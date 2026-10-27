//! Owner state, the line, and the reconcile step. Every event mutates
//! [`State`] under one lock and wakes [`run`], which turns the state into
//! actions: admissions, signals, `start`/`stop` to the arbiter.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{watch, Notify};

use crate::protocol::{now_unix, ArbiterCmd, Census, JobState, Mode, Placement, RunnerEvent, Sig};
use crate::switch::config::{secs, SwitchConfig};
use crate::switch::room::{self, Holder};
use crate::switch::state::*;
use crate::switch::token::Secret;

pub struct Daemon {
    pub config: SwitchConfig,
    pub config_path: PathBuf,
    pub secret: Secret,
    state_dir: Option<PathBuf>,
    state: Mutex<State>,
    bump: watch::Sender<u64>,
    sample: Notify,
    pub journal: super::journal::Journal,
}

impl Daemon {
    pub fn new(config: SwitchConfig, config_path: PathBuf) -> Result<Arc<Daemon>, String> {
        let backoff = secs(config.restart_backoff_initial_secs);
        let state_dir = config.state_dir.clone().or_else(|| {
            std::env::var_os("STATE_DIRECTORY").and_then(|dirs| std::env::split_paths(&dirs).next())
        });
        let secret = Secret::load_or_create(state_dir.as_deref())?;
        let journal_dir = state_dir.clone().unwrap_or_else(|| {
            config_path
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .join("gpu-queue-state")
        });
        let journal = super::journal::Journal::open(&journal_dir)?;
        Ok(Arc::new(Daemon {
            config,
            config_path,
            secret,
            state_dir,
            state: Mutex::new(State::new(backoff)),
            bump: watch::channel(0).0,
            sample: Notify::new(),
            journal,
        }))
    }

    pub fn state_dir(&self) -> Option<&std::path::Path> {
        self.state_dir.as_deref()
    }

    /// A decision waits on a card sample newer than the last one.
    pub fn want_sample(&self) {
        self.sample.notify_one();
    }

    /// Until someone wants a card sample.
    pub async fn sample_wanted(&self) {
        self.sample.notified().await;
    }

    pub fn with<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut g)
    }

    /// Mutate and wake the reconcile loop and every waiter.
    pub fn update<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let r = self.with(f);
        self.bump.send_modify(|v| *v = v.wrapping_add(1));
        r
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.bump.subscribe()
    }

    pub async fn wait_for<T>(&self, mut f: impl FnMut(&mut State) -> Option<T>) -> T {
        let mut rx = self.bump.subscribe();
        loop {
            rx.borrow_and_update();
            if let Some(v) = self.with(&mut f) {
                return v;
            }
            if rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

pub async fn run(d: Arc<Daemon>) {
    let mut rx = d.subscribe();
    let tick = Duration::from_millis(100);
    loop {
        rx.borrow_and_update();
        let now = Instant::now();
        d.with(|s| reconcile(&d, s, now));
        let _ = tokio::time::timeout(tick, rx.changed()).await;
    }
}

/// The arbiter link has been down longer than the arbiter keeps NInfer.
fn link_gone(s: &State, now: Instant, grace: Duration) -> bool {
    s.arbiter.is_none() && s.ninfer.link_lost_at.is_none_or(|at| now - at >= grace)
}

/// Jobs whose clients did not come back after a restart of gpu-switch.
fn abandon_detached(d: &Arc<Daemon>, s: &mut State, now: Instant) {
    let gone: Vec<(Side, u64, JobState)> = s
        .jobs()
        .filter(|j| j.active() && !j.departing && j.detached().is_some_and(|until| until <= now))
        .map(|j| (j.side, j.id, j.state))
        .collect();
    for (side, id, state) in gone {
        let (end, why) = match state {
            JobState::Queued => (JobState::Cancelled, "its client did not come back"),
            _ => (
                JobState::Failed,
                "its client did not come back after gpu-switch restarted",
            ),
        };
        if state == JobState::Queued {
            end_job(d, s, side, id, end, why.into());
        } else {
            let job = s.job_mut(side, id).expect("collected job");
            job.departing = true;
            job.end_reason.get_or_insert((end, why.into()));
            super::enforcement::signal(d, job, Sig::Term);
            let d = d.clone();
            tokio::spawn(async move {
                super::departure::settle(&d, side, id).await;
                d.update(|s| finish_run(&d, s, side, id, None, None));
            });
        }
    }
}

fn reconcile(d: &Arc<Daemon>, s: &mut State, now: Instant) {
    super::fairness::reconcile(d, s, now);
    let cfg = &d.config;
    let grace = secs(cfg.reattach_grace_secs);
    // Scheduled signals, yield requests.
    let exclusive_waiting = s
        .line()
        .iter()
        .any(|(_, e)| s.entry_mode(*e) == Mode::Exclusive);
    let all_cooperative = s.running().all(|j| j.spec.cooperative);
    let min_hold = secs(cfg.coop_min_hold_secs);
    for side in s.sides.values_mut() {
        for job in side.jobs.values_mut() {
            let mut due = Vec::new();
            if let Some(kill) = &mut job.kill {
                while kill.steps.front().is_some_and(|(t, _)| *t <= now) {
                    due.push(kill.steps.pop_front().expect("front").1);
                }
            }
            for sig in due {
                super::enforcement::signal(d, job, sig);
            }
            // tools/gpu_cooperative.py: yield only if that frees the card.
            if job.state == JobState::Running
                && job.spec.cooperative
                && !job.yield_sent
                && exclusive_waiting
                && all_cooperative
                && job.admitted_at.is_some_and(|t| now - t >= min_hold)
            {
                job.yield_sent = true;
                if job.shutdown_grace_secs.is_some() {
                    job.end_reason = Some((
                        JobState::Cancelled,
                        "cooperative yield to a waiting exclusive job".into(),
                    ));
                    super::enforcement::signal(d, job, Sig::Int);
                } else {
                    job.runner(RunnerEvent::Yield);
                }
            }
        }
    }

    abandon_detached(d, s, now);
    let empty = s.queue_empty();
    s.empty_since = if empty {
        Some(s.empty_since.unwrap_or(now))
    } else {
        None
    };

    match s.ninfer.phase {
        NPhase::Idle => {
            let ready =
                s.serve_wanted && s.arbiter.is_some() && s.ninfer.retry_at.is_none_or(|t| now >= t);
            let lingered = empty
                && (s.skip_linger
                    || s.empty_since
                        .is_some_and(|t| now - t >= secs(cfg.linger_secs)));
            if ready && (lingered || s.switch_to_ninfer) {
                s.ninfer.key = if s.switch_to_ninfer {
                    // Ahead of every waiter, fresh or re-queued.
                    Placement { class: 0, ns: 0 }
                } else {
                    Placement {
                        class: 1,
                        ns: monotonic_ns(),
                    }
                };
                s.ninfer.phase = NPhase::Queued;
                s.skip_linger = false;
                eprintln!("gpu-switch: NInfer queued");
            }
        }
        NPhase::Queued => {
            if !s.serve_wanted || s.arbiter.is_none() {
                s.ninfer.phase = NPhase::Idle;
            }
        }
        NPhase::Starting | NPhase::Serving => {
            let stop = !s.serve_wanted || s.switch_requested || s.restart_requested;
            if link_gone(s, now, grace) {
                // The arbiter stops NInfer itself when its link stays down.
                s.ninfer.phase = NPhase::Releasing;
                s.ninfer.stopped_at = Some(now);
                s.ninfer.restart = false;
            } else if stop && s.arbiter.is_some() {
                s.ninfer.restart = s.restart_requested && s.serve_wanted && !s.switch_requested;
                s.switch_requested = false;
                s.restart_requested = false;
                s.ninfer.phase = NPhase::Stopping;
                s.ninfer.draining = false;
                if let Some(a) = &s.arbiter {
                    let _ = a.tx.send(ArbiterCmd::Stop {
                        drain_timeout_secs: cfg.drain_timeout_secs,
                    });
                }
                eprintln!("gpu-switch: stopping NInfer");
            }
        }
        NPhase::Stopping => {
            if link_gone(s, now, grace) {
                s.ninfer.phase = NPhase::Releasing;
                s.ninfer.stopped_at = Some(now);
                s.ninfer.restart = false;
            }
        }
        NPhase::Releasing => {
            if !ninfer_off_gpu(d, s) {
                d.want_sample();
            } else {
                let restart =
                    s.ninfer.restart && s.serve_wanted && s.arbiter.is_some() && !s.ninfer.failed;
                s.ninfer.restart = false;
                s.ninfer.host_pid = None;
                s.ninfer.ns_pid = None;
                if restart {
                    start_ninfer(d, s);
                } else {
                    s.ninfer.phase = NPhase::Idle;
                    s.ninfer.failed = false;
                    s.left_gpu_at = Some(now);
                    eprintln!("gpu-switch: NInfer off the GPU, it belongs to the line");
                }
            }
        }
    }
    schedule(d, s);
}

fn start_ninfer(d: &Daemon, s: &mut State) {
    let profile = s.profile_next(&d.config.profile);
    let host_cache = s.host_cache_next(d.config.host_context_mib);
    s.profile_current = profile.clone();
    s.host_cache_current = host_cache;
    s.switch_requested = false;
    s.switch_to_ninfer = false;
    s.restart_requested = false;
    s.ninfer.phase = NPhase::Starting;
    s.ninfer.failed = false;
    s.ninfer.draining = false;
    if let Some(a) = &s.arbiter {
        let _ = a.tx.send(ArbiterCmd::Start {
            profile,
            host_context_mib: host_cache,
        });
    }
    eprintln!("gpu-switch: NInfer admitted, start sent");
}

/// NInfer's pid no longer listed by a GPU sample taken after it stopped.
fn ninfer_off_gpu(d: &Daemon, s: &State) -> bool {
    if d.config.nvidia_smi.is_none() {
        return true;
    }
    let Some(pid) = s.ninfer.host_pid else {
        return true;
    };
    let (Some(at), Some(stopped)) = (s.view.gpu_at, s.ninfer.stopped_at) else {
        return false;
    };
    at > stopped
        && s.view
            .gpu
            .as_ref()
            .is_some_and(|g| !g.compute.iter().any(|(p, _)| *p == pid))
}

/// Compute pids on the card that belong to no job of ours and no NInfer,
/// minus idle graphics/compute clients (as tools/_gpu_card.py:22-30).
/// `except`: the candidate's own tree.
fn foreign_busy(s: &State, except: &HashSet<u32>) -> usize {
    let Some(g) = &s.view.gpu else { return 0 };
    let mut ours: HashSet<u32> = except.clone();
    for j in s.running() {
        ours.extend(s.tree(j.pid_host));
    }
    ours.extend(s.tree(s.ninfer.host_pid));
    let graphics: HashSet<u32> = g
        .pmon
        .iter()
        .filter(|p| p.graphics())
        .map(|p| p.pid)
        .collect();
    let active: HashSet<u32> = g
        .pmon
        .iter()
        .filter(|p| p.computing())
        .map(|p| p.pid)
        .collect();
    g.compute
        .iter()
        .filter(|(_, memory)| *memory != Some(0))
        .map(|(p, _)| *p)
        .filter(|p| !ours.contains(p))
        .filter(|p| !(graphics.contains(p) && !active.contains(p)))
        .count()
}

/// The room rule against the running jobs (see room.rs). Without a card
/// sample: admit if nvidia-smi is off, else wait for one.
fn room_for(d: &Daemon, s: &State, need: u64) -> Result<(), String> {
    if s.running().next().is_none() {
        return Ok(());
    }
    if d.config.nvidia_smi.is_none() {
        return Ok(());
    }
    let Some(g) = &s.view.gpu else {
        return Err("waiting for a GPU sample".into());
    };
    let Some((total, free)) = g.memory else {
        return Err("waiting for a GPU sample".into());
    };
    let mut parties: HashMap<Party, Holder> = HashMap::new();
    for j in s.running() {
        let tree = s.tree(j.pid_host);
        let mut actual = 0u64;
        for (pid, mem) in &g.compute {
            if tree.contains(pid) {
                match mem {
                    Some(m) => actual += m,
                    // tools/_gpu_card.py:79-80: unverifiable means no.
                    None => return Err("GPU memory of a running job cannot be verified".into()),
                }
            }
        }
        let holder = parties.entry(j.party()).or_insert(Holder {
            declared: 0,
            actual: Some(0),
        });
        holder.declared = holder.declared.max(j.spec.vram);
        holder.actual = holder.actual.map(|a| a + actual);
    }
    let others: Vec<Holder> = parties.into_values().collect();
    let used = total.saturating_sub(free);
    if room::admits(total, used, &others, false, need) {
        Ok(())
    } else {
        let avail = room::available(total, used, &others).max(0) as u64;
        Err(format!(
            "waiting for {} of VRAM beside {} job(s): {} available",
            mib(need),
            others.len(),
            mib(avail)
        ))
    }
}

fn mib(b: u64) -> String {
    format!("{} MiB", b >> 20)
}

fn admit(s: &mut State, side: Side, id: u64, now: Instant) {
    let Some(job) = s.job_mut(side, id) else {
        return;
    };
    job.state = JobState::Running;
    job.started_at = Some(now_unix());
    job.runs += 1;
    job.admitted_at = Some(now);
    job.timing
        .get_or_insert_with(Default::default)
        .begin(monotonic_ns());
    job.police = Police::default();
    job.shutdown = None;
    job.yield_sent = false;
    job.reason = None;
    job.requeued = false;
    job.go();
    eprintln!("gpu-switch: job {} admitted", job.handle);
}

/// Admit fitting jobs in line order. A blocked waiter does not prevent a later
/// shared job from using spare capacity. An admitted exclusive remains exclusive.
fn schedule(d: &Daemon, s: &mut State) {
    let now = Instant::now();
    admit_members(s, now);
    let line = s.line();
    let switching = s.switch_to_ninfer && s.ninfer.phase == NPhase::Queued;
    let fixed = if s.ninfer_owns() {
        Some("NInfer holds the GPU; NOT running until 'gpu-run switch-gpu'")
    } else if switching {
        Some("the GPU is switching to NInfer; NOT running until 'gpu-run switch-gpu'")
    } else {
        None
    };
    let mut reasons: Vec<(Entry, Wait)> = Vec::new();
    let mut blocked = s.ninfer_owns();
    let mut bypassing = false;
    // Decide on a card sample newer than the last departure, or a fast
    // successor could be admitted beside processes the sample never saw.
    let fresh = d.config.nvidia_smi.is_none()
        || match (s.view.gpu_at, s.left_gpu_at) {
            (_, None) => true,
            (Some(at), Some(left)) => at > left,
            (None, Some(_)) => false,
        };
    for (pos, (_, entry)) in line.iter().enumerate() {
        let behind = || match fixed {
            Some(t) => t.to_string(),
            None => format!("position {} in line", pos + 1),
        };
        if blocked {
            reasons.push((*entry, Wait::Event(behind())));
            continue;
        }
        if !fresh {
            reasons.push((
                *entry,
                Wait::Card("waiting for a GPU sample after the last job left".into()),
            ));
            blocked = true;
            continue;
        }
        let running = s.running().count();
        let exclusive_holder = s.running().any(|j| j.spec.mode == Mode::Exclusive);
        match *entry {
            Entry::Ninfer => {
                match (running, foreign_busy(s, &HashSet::new())) {
                    (0, 0) => start_ninfer(d, s),
                    (0, n) => reasons.push((
                        Entry::Ninfer,
                        Wait::Card(format!("waiting for {n} foreign GPU process(es) to exit")),
                    )),
                    _ => reasons.push((
                        Entry::Ninfer,
                        Wait::Event(format!("waiting for {running} job(s) on the GPU to finish")),
                    )),
                }
                blocked = true;
            }
            Entry::Job(side, id) => {
                let (mode, vram, root, detached) = match s.job(side, id) {
                    Some(j) => (j.spec.mode, j.spec.vram, j.pid_host, j.detached().is_some()),
                    None => continue,
                };
                let verdict = match mode {
                    _ if detached => Err(Wait::Event(
                        "waiting for its client to come back after gpu-switch restarted".into(),
                    )),
                    Mode::Exclusive if bypassing => {
                        Err(Wait::Event("waiting behind an earlier job".into()))
                    }
                    Mode::Shared if bypassing && !super::backfill::fits(s, side, id, now) => {
                        Err(Wait::Event(
                            "expected runtime does not fit the running jobs' remaining time".into(),
                        ))
                    }
                    Mode::Exclusive if running > 0 => Err(Wait::Event(format!(
                        "waiting for {running} job(s) on the GPU to finish"
                    ))),
                    Mode::Exclusive => match foreign_busy(s, &s.tree(root)) {
                        0 => Ok(()),
                        n => Err(Wait::Card(format!(
                            "waiting for {n} foreign GPU process(es) to exit"
                        ))),
                    },
                    Mode::Shared if exclusive_holder => Err(Wait::Event(
                        "waiting for an exclusive job on the GPU to finish".into(),
                    )),
                    Mode::Shared => room_for(d, s, vram).map_err(Wait::Card),
                };
                match verdict {
                    Ok(()) => {
                        admit(s, side, id, now);
                        blocked = mode == Mode::Exclusive;
                    }
                    Err(why) => {
                        let why = match fixed {
                            Some(t) => Wait::Event(t.to_string()),
                            None => why,
                        };
                        reasons.push((Entry::Job(side, id), why));
                        bypassing = true;
                    }
                }
            }
        }
    }
    if reasons.iter().any(|(_, why)| matches!(why, Wait::Card(_))) {
        d.want_sample();
    }
    for (entry, why) in reasons {
        if let Entry::Job(side, id) = entry {
            if let Some(j) = s.job_mut(side, id) {
                j.waiting(why.reason());
            }
        }
    }
}

/// A shared lease whose group already runs joins it at once: the group's room
/// is reserved, and its process may wait on that member (tools/_gpu_room.py).
fn admit_members(s: &mut State, now: Instant) {
    let running: HashSet<Party> = s
        .running()
        .filter(|j| j.spec.mode == Mode::Shared && j.group.is_some())
        .map(Job::party)
        .collect();
    let members: Vec<(Side, u64)> = s
        .jobs()
        .filter(|j| {
            j.state == JobState::Queued
                && j.spec.mode == Mode::Shared
                && j.detached().is_none()
                && running.contains(&j.party())
        })
        .map(|j| (j.side, j.id))
        .collect();
    for (side, id) in members {
        admit(s, side, id, now);
    }
}

/// Counts for a lease client of `group` on `side`: no across-side details.
pub fn census(s: &State, side: Side, group: Option<&str>) -> Census {
    let own = group.map(|g| Party::Group(side, g.to_string()));
    let mut others: HashMap<Party, bool> = HashMap::new();
    for j in s.running().filter(|j| Some(j.party()) != own) {
        *others.entry(j.party()).or_insert(true) &= j.spec.cooperative;
    }
    Census {
        exclusive_waiting: s
            .line()
            .iter()
            .any(|(_, e)| s.entry_mode(*e) == Mode::Exclusive),
        others: others.len(),
        non_cooperative_others: others.values().filter(|cooperative| !**cooperative).count(),
    }
}

/// Why an entry waits: a card fact (a newer sample may admit it) or an event.
enum Wait {
    Card(String),
    Event(String),
}

impl Wait {
    fn reason(self) -> String {
        match self {
            Wait::Card(why) | Wait::Event(why) => why,
        }
    }
}

/// Where a new entry of priority `p` sorts: from the back of the line it
/// passes waiting entries of lower priority q, at most p - q per level,
/// never one of equal or higher priority. Fresh entries are class 1.
pub fn placement(s: &State, p: i8) -> Placement {
    let line: Vec<(Placement, i8)> = s
        .line()
        .into_iter()
        .map(|(k, e)| (k, s.entry_priority(e)))
        .collect();
    place_in(&line, p, monotonic_ns())
}

pub fn place_in(line: &[(Placement, i8)], p: i8, now: u64) -> Placement {
    let back = Placement { class: 1, ns: now };
    let mut used = [0usize; 21];
    let mut idx = line.len();
    while idx > 0 {
        let q = line[idx - 1].1;
        if q >= p {
            break;
        }
        let level = (q - crate::protocol::PRIORITY_MIN) as usize;
        if used[level] >= (p - q) as usize {
            break;
        }
        used[level] += 1;
        idx -= 1;
    }
    // A key strictly between line[idx-1] and line[idx]; pass one fewer if
    // there is no integer gap.
    while idx < line.len() {
        let b = line[idx].0;
        if b.ns > 0 {
            let c = Placement {
                class: b.class,
                ns: b.ns - 1,
            };
            if idx == 0 || line[idx - 1].0 < c {
                return c;
            }
        }
        idx += 1;
    }
    back
}

/// Key of a re-queued entry: right behind the head, after every entry
/// re-queued before it, before every fresh waiter (tools/_gpu_line.py:3-6,
/// GPU_LOCK_RECIPE.md "Queue order").
pub fn behind_head(s: &State) -> Placement {
    let line: Vec<(Placement, bool)> = s
        .line()
        .into_iter()
        .map(|(k, e)| {
            let requeued = match e {
                Entry::Job(side, id) => s.job(side, id).is_some_and(|j| j.requeued),
                Entry::Ninfer => false,
            };
            (k, requeued)
        })
        .collect();
    let mut idx = 1.min(line.len());
    while idx < line.len() && line[idx].1 {
        idx += 1;
    }
    after(&line.iter().map(|(k, _)| *k).collect::<Vec<_>>(), idx)
}

/// A key sorting after `keys[idx-1]` and before `keys[idx]`, or the first
/// gap further back.
fn after(keys: &[Placement], mut idx: usize) -> Placement {
    loop {
        let a = idx.checked_sub(1).map(|i| keys[i]);
        let b = keys.get(idx).copied();
        let c = match (a, b) {
            (None, None) => {
                return Placement {
                    class: 0,
                    ns: monotonic_ns(),
                }
            }
            (None, Some(b)) if b.ns > 0 => Some(Placement {
                class: b.class,
                ns: b.ns - 1,
            }),
            (None, Some(_)) => None,
            (Some(a), None) => {
                return Placement {
                    class: a.class,
                    ns: a.ns + 1,
                }
            }
            (Some(a), Some(b)) => {
                let c = Placement {
                    class: a.class,
                    ns: a.ns + 1,
                };
                (c < b).then_some(c)
            }
        };
        if let Some(c) = c {
            return c;
        }
        idx += 1;
    }
}

/// Schedule signals for a running job: `(delay from the previous, signal)`.
pub fn schedule_kill(job: &mut Job, steps: &[(Duration, Sig)]) {
    let now = Instant::now();
    let mut acc = Duration::ZERO;
    let mut q = VecDeque::new();
    for (delay, sig) in steps {
        acc += *delay;
        q.push_back((now + acc, *sig));
    }
    job.kill = Some(Kill { steps: q });
}

/// A run ended: decide the job's state, or re-queue a cooperative yield.
pub fn finish_run(
    d: &Daemon,
    s: &mut State,
    side: Side,
    id: u64,
    exit: Option<i32>,
    sig: Option<i32>,
) {
    s.left_gpu_at = Some(Instant::now());
    let behind = behind_head(s);
    let Some(job) = s.job_mut(side, id) else {
        return;
    };
    if let Some(timing) = &mut job.timing {
        timing.stop(monotonic_ns());
    }
    let yielded = job.spec.cooperative && exit == Some(76) && job.end_reason.is_none();
    job.exit_code = exit;
    job.signal = sig.or(job.signal);
    job.kill = None;
    if yielded {
        job.state = JobState::Queued;
        job.key = behind;
        job.requeued = true;
        eprintln!("gpu-switch: job {} yielded (76), re-queued", job.handle);
        return;
    }
    let (state, error) = match job.end_reason.take() {
        Some((st, why)) => (st, Some(why)),
        None if exit == Some(0) => (JobState::Done, None),
        None if job.is_lease() => (JobState::Done, None),
        None => (
            JobState::Failed,
            Some(format!("process exited: code {exit:?}, signal {sig:?}")),
        ),
    };
    job.state = state;
    if error.is_some() {
        job.error = error;
    }
    job.finished_at = Some(now_unix());
    if state != JobState::Done {
        if let Err(error) = d.journal.record("terminal_failure", job) {
            eprintln!("gpu-queue: {error}");
        }
    }
    job.runner(RunnerEvent::Done);
    eprintln!(
        "gpu-switch: job {} {}: exit {exit:?} signal {sig:?}",
        job.handle,
        state.as_str()
    );
    prune(s, side);
}

/// End a job that never runs again (cancelled, connection gone).
pub fn end_job(d: &Daemon, s: &mut State, side: Side, id: u64, state: JobState, error: String) {
    let Some(job) = s.job_mut(side, id) else {
        return;
    };
    if job.state.is_terminal() {
        return;
    }
    if let Some(timing) = &mut job.timing {
        timing.stop(monotonic_ns());
    }
    let was_running = job.state == JobState::Running;
    job.state = state;
    job.error = Some(error);
    job.finished_at = Some(now_unix());
    job.kill = None;
    if let Err(error) = d.journal.record("terminal_failure", job) {
        eprintln!("gpu-queue: {error}");
    }
    if was_running {
        s.left_gpu_at = Some(Instant::now());
    }
    prune(s, side);
}

const KEEP_FINISHED: usize = 200;

fn prune(s: &mut State, side: Side) {
    let Some(sj) = s.sides.get_mut(&side) else {
        return;
    };
    let done: Vec<u64> = sj
        .jobs
        .values()
        .filter(|j| j.state.is_terminal())
        .map(|j| j.id)
        .collect();
    if done.len() > KEEP_FINISHED {
        for id in &done[..done.len() - KEEP_FINISHED] {
            sj.jobs.remove(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(ns: u64) -> Placement {
        Placement { class: 1, ns }
    }

    #[test]
    fn skip_rule() {
        let line = [(k(10), 0), (k(20), 0), (k(30), 0), (k(40), 0)];
        assert_eq!(place_in(&line, 0, 99), k(99));
        // Priority 2 passes two priority-0 entries, not a third.
        assert_eq!(place_in(&line, 2, 99), k(29));
        // Any waiting entry may be passed, the first too.
        assert_eq!(place_in(&line, 10, 99), k(9));
        // Levels counted separately: one priority-1 and two priority-0.
        let line = [(k(10), 0), (k(20), 0), (k(30), 0), (k(40), 1), (k(50), 0)];
        assert_eq!(place_in(&line, 2, 99), k(29));
        // Priority 0 passes negative priorities too: p - q for every q < p.
        let line = [(k(10), 0), (k(20), -1), (k(30), -3)];
        assert_eq!(place_in(&line, 0, 99), k(19));
        // Equal priority is never passed.
        let line = [(k(10), 0), (k(20), 3), (k(30), 0)];
        assert_eq!(place_in(&line, 3, 99), k(29));
        // No integer gap: one fewer.
        let line = [(k(10), 0), (k(11), 0), (k(30), 0)];
        assert_eq!(place_in(&line, 2, 99), k(29));
        // A re-queued entry (class 0) ahead of a fresh one.
        let line = [(Placement { class: 0, ns: 99 }, 0), (k(5), 0)];
        assert_eq!(place_in(&line, 1, 200), k(4));
        // Re-queued: right behind the head, or one further on no gap.
        assert_eq!(after(&[k(10), k(20)], 1), k(11));
        assert_eq!(after(&[k(10), k(11), k(20)], 1), k(12));
        assert_eq!(after(&[k(10)], 1), k(11));
        // switch-ninfer's key sorts first of all.
        assert!(Placement { class: 0, ns: 0 } < Placement { class: 0, ns: 1 });
    }
}
