//! Owner state, the line, and the reconcile step. Every event mutates
//! [`State`] under one lock and wakes [`run`], which turns the state into
//! actions: admissions, signals, `start`/`stop` to the arbiter.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{watch, Notify};

use crate::gpu::Gpu;
use crate::protocol::{
    now_unix, ArbiterCmd, Blocker, BlockerKind, Census, JobState, Mode, Placement, RunnerEvent, Sig,
};
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
    /// Bumped by a change worth saving at once; an observation bumps only `bump`.
    kept: watch::Sender<u64>,
    sample: Notify,
    pub journal: super::journal::Journal,
    /// The card's telemetry; none when the configuration names no NVML library.
    pub gpu: Option<Gpu>,
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
        let gpu = config.nvml.as_deref().map(Gpu::open).transpose()?;
        Ok(Arc::new(Daemon {
            config,
            config_path,
            secret,
            state_dir,
            state: Mutex::new(State::new(backoff)),
            bump: watch::channel(0).0,
            kept: watch::channel(0).0,
            sample: Notify::new(),
            journal,
            gpu,
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

    /// Mutate and wake the reconcile loop and every waiter; the change is saved shortly after.
    pub fn update<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let r = self.observe(f);
        self.kept.send_modify(|v| *v = v.wrapping_add(1));
        r
    }

    /// Mutate with what a poll observed and wake the reconcile loop and every waiter; the observation is saved
    /// with the next change worth keeping or by the periodic save (`persist`), so a poll ten times a second does
    /// not write the state as often.
    pub fn observe<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let r = self.with(f);
        self.bump.send_modify(|v| *v = v.wrapping_add(1));
        r
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.bump.subscribe()
    }

    /// Changes worth saving at once.
    pub fn subscribe_kept(&self) -> watch::Receiver<u64> {
        self.kept.subscribe()
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
    super::runtime::reconcile(d, s);
    super::retention::reconcile(d, s);
    let cfg = &d.config;
    let grace = secs(cfg.reattach_grace_secs);
    // Scheduled signals, yield requests.
    let exclusive_waiting = s.exclusive_waiting();
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
    if d.gpu.is_none() {
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
    let graphics: HashSet<u32> = g.graphics.iter().map(|(p, _)| *p).collect();
    g.compute
        .iter()
        .filter(|(_, memory)| *memory != Some(0))
        .map(|(p, _)| *p)
        .filter(|p| !ours.contains(p))
        .filter(|p| !(graphics.contains(p) && !g.computing.contains(p)))
        .count()
}

/// The room rule against the running jobs (see room.rs). Without a card
/// sample: admit if nvidia-smi is off, else wait for one.
fn room_for(d: &Daemon, s: &State, need: u64) -> Result<(), String> {
    if s.running().next().is_none() {
        return Ok(());
    }
    if d.gpu.is_none() {
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
        if j.retention.is_some() {
            // Only card-wide actual free memory accounts for retained VRAM.
            parties
                .entry(j.party())
                .or_insert(Holder {
                    declared: 0,
                    actual: None,
                })
                .actual = None;
            continue;
        }
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
        holder.declared = holder.declared.max(j.reserved_vram());
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
    job.gpu_processes = Vec::new();
    job.terminal_status = None;
    job.shutdown = None;
    job.yield_sent = false;
    job.reason = None;
    job.blocker = None;
    job.requeued = false;
    job.go();
    eprintln!("gpu-switch: job {} admitted", job.handle);
}

/// The running jobs as admission weighs them, recomputed after each admission.
struct Holders {
    count: usize,
    exclusive: bool,
    /// The shares of every running job, and of the running exclusive jobs.
    shares: Vec<String>,
    exclusive_shares: Vec<String>,
}

impl Holders {
    fn of(s: &State) -> Holders {
        let shares = |exclusive_only: bool| {
            s.running()
                .filter(|j| !exclusive_only || j.mode() == Mode::Exclusive)
                .map(|j| j.share.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect()
        };
        Holders {
            count: s.running().count(),
            exclusive: s.running().any(|j| j.mode() == Mode::Exclusive),
            shares: shares(false),
            exclusive_shares: shares(true),
        }
    }
    fn blocker(kind: BlockerKind, shares: &[String]) -> Option<Blocker> {
        Some(Blocker {
            kind,
            shares: shares.to_vec(),
        })
    }
}

/// Admit fitting jobs in line order. A blocked waiter does not prevent a later
/// shared job from using spare capacity. An admitted exclusive remains exclusive.
/// Places not yet prepared are told first, then skipped: they block nothing.
fn schedule(d: &Daemon, s: &mut State) {
    let now = Instant::now();
    admit_members(s, now);
    let untold = super::preparation::tell(d, s);
    let line = s.line();
    let switching = s.switch_to_ninfer && s.ninfer.phase == NPhase::Queued;
    let fixed = if s.ninfer_owns() {
        Some("NInfer holds the GPU; NOT running until 'gpu-run switch-gpu'")
    } else if switching {
        Some("the GPU is switching to NInfer; NOT running until 'gpu-run switch-gpu'")
    } else {
        None
    };
    let ninfer = || Holders::blocker(BlockerKind::Ninfer, &[]);
    let mut reasons: Vec<(Entry, Wait, Option<Blocker>)> = Vec::new();
    // Why every later entry waits once one is reached that holds the line.
    let mut blocked = s.ninfer_owns().then(ninfer);
    // What the first entry that could not be admitted waits for, once one could not.
    let mut bypassing: Option<Option<Blocker>> = None;
    let mut holders = Holders::of(s);
    // Decide on a card sample newer than the last departure, or a fast
    // successor could be admitted beside processes the sample never saw.
    let fresh = d.gpu.is_none()
        || match (s.view.gpu_at, s.left_gpu_at) {
            (_, None) => true,
            (Some(at), Some(left)) => at > left,
            (None, Some(_)) => false,
        };
    for (pos, (_, entry)) in line.iter().enumerate() {
        if let Entry::Job(side, id) = *entry {
            if let Some(place) = s.job(side, id).filter(|j| !j.admissible()).and_then(|j| j.place.as_ref()) {
                let reason = Wait::Event(place.reason(pos + 1));
                reasons.push((*entry, reason, untold.get(&(side, id)).cloned()));
                continue;
            }
        }
        let behind = || match fixed {
            Some(t) => t.to_string(),
            None => format!("position {} in line", pos + 1),
        };
        if let Some(blocker) = &blocked {
            reasons.push((*entry, Wait::Event(behind()), blocker.clone()));
            continue;
        }
        if !fresh {
            reasons.push((
                *entry,
                Wait::Card("waiting for a GPU sample after the last job left".into()),
                None,
            ));
            blocked = Some(None);
            continue;
        }
        let running = holders.count;
        match *entry {
            Entry::Ninfer => {
                match (running, foreign_busy(s, &HashSet::new())) {
                    (0, 0) => start_ninfer(d, s),
                    (0, n) => reasons.push((
                        Entry::Ninfer,
                        Wait::Card(format!("waiting for {n} foreign GPU process(es) to exit")),
                        None,
                    )),
                    _ => reasons.push((
                        Entry::Ninfer,
                        Wait::Event(format!("waiting for {running} job(s) on the GPU to finish")),
                        None,
                    )),
                }
                blocked = Some(ninfer());
            }
            Entry::Job(side, id) => {
                let (mode, vram, root, detached, share) = match s.job(side, id) {
                    Some(j) => (
                        j.mode(),
                        j.reserved_vram(),
                        j.pid_host,
                        j.detached().is_some(),
                        j.share.clone(),
                    ),
                    None => continue,
                };
                let room = Holders::blocker(BlockerKind::VramRoom, &holders.shares);
                let exclusive = Holders::blocker(BlockerKind::ExclusiveRunning, &holders.exclusive_shares);
                let verdict = match mode {
                    _ if detached => Err((
                        Wait::Event(
                            "waiting for its client to come back after gpu-switch restarted".into(),
                        ),
                        None,
                    )),
                    Mode::Exclusive if bypassing.is_some() => Err((
                        Wait::Event("waiting behind an earlier job".into()),
                        bypassing.clone().flatten(),
                    )),
                    Mode::Shared
                        if bypassing.is_some() && !super::backfill::fits(s, side, id, now) =>
                    {
                        Err((
                            Wait::Event(
                                "expected runtime does not fit the running jobs' remaining time"
                                    .into(),
                            ),
                            bypassing.clone().flatten(),
                        ))
                    }
                    Mode::Exclusive if running > 0 => Err((
                        Wait::Event(format!("waiting for {running} job(s) on the GPU to finish")),
                        match holders.exclusive {
                            true => exclusive,
                            false => room,
                        },
                    )),
                    Mode::Exclusive => match foreign_busy(s, &s.tree(root)) {
                        0 => Ok(()),
                        n => Err((
                            Wait::Card(format!("waiting for {n} foreign GPU process(es) to exit")),
                            None,
                        )),
                    },
                    Mode::Shared if holders.exclusive => Err((
                        Wait::Event("waiting for an exclusive job on the GPU to finish".into()),
                        exclusive,
                    )),
                    Mode::Shared => room_for(d, s, vram).map_err(|why| (Wait::Card(why), room)),
                };
                match verdict {
                    Ok(()) => {
                        admit(s, side, id, now);
                        holders = Holders::of(s);
                        if mode == Mode::Exclusive {
                            blocked = Some(Holders::blocker(BlockerKind::ExclusiveRunning, &[share]));
                        }
                    }
                    Err((why, blocker)) => {
                        let (why, blocker) = match fixed {
                            Some(t) => (Wait::Event(t.to_string()), ninfer()),
                            None => (why, blocker),
                        };
                        if bypassing.is_none() {
                            bypassing = Some(match mode {
                                Mode::Exclusive => Holders::blocker(BlockerKind::ExclusiveAhead, &[share]),
                                Mode::Shared => blocker.clone(),
                            });
                        }
                        reasons.push((Entry::Job(side, id), why, blocker));
                    }
                }
            }
        }
    }
    if reasons.iter().any(|(_, why, _)| matches!(why, Wait::Card(_))) {
        d.want_sample();
    }
    for (entry, why, blocker) in reasons {
        if let Entry::Job(side, id) = entry {
            if let Some(j) = s.job_mut(side, id) {
                j.waiting(why.reason(), blocker);
            }
        }
    }
}

/// A shared lease whose group already runs joins it at once: the group's room
/// is reserved, and its process may wait on that member (tools/_gpu_room.py).
fn admit_members(s: &mut State, now: Instant) {
    let running: HashSet<Party> = s
        .running()
        .filter(|j| j.mode() == Mode::Shared && j.retention.is_none() && j.group.is_some())
        .map(Job::party)
        .collect();
    let members: Vec<(Side, u64)> = s
        .jobs()
        .filter(|j| {
            j.state == JobState::Queued
                && j.mode() == Mode::Shared
                && j.admissible()
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
        exclusive_waiting: s.exclusive_waiting(),
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

/// A waiting entry as a fresh entry's placement sees it; NInfer has no share.
pub struct Waiting<'a> {
    pub key: Placement,
    pub priority: i8,
    pub share: Option<&'a str>,
}

/// Where a new entry of priority `p` and share `share` sorts (`place_in`).
pub fn placement(s: &State, p: i8, share: &str) -> Placement {
    let line: Vec<Waiting> = s
        .line()
        .into_iter()
        .map(|(key, e)| Waiting {
            key,
            priority: s.entry_priority(e),
            share: s.entry_share(e),
        })
        .collect();
    place_in(&line, p, share, monotonic_ns())
}

/// One backward walk from the back of the line. A fresh entry of priority p passes a waiting entry e of
/// priority q while either holds:
/// - priority: e is not re-queued (class 0), q < p, and fewer than p - q entries of level q were passed;
/// - turn: e is a fresh job entry, q = p, of another share, with more entries of its share ahead of it
///   than the new entry's share has waiting.
/// It stops at the first entry neither lets it pass. So shares of one priority take turns, a fresh entry
/// never passes a higher priority or a re-queued entry, and with one share this is the priority rule alone.
/// Fresh entries are class 1.
pub fn place_in(line: &[Waiting], p: i8, share: &str, now: u64) -> Placement {
    let back = Placement { class: 1, ns: now };
    let mut counted = std::collections::HashMap::<Option<&str>, usize>::new();
    let ahead: Vec<usize> = line
        .iter()
        .map(|e| {
            let n = counted.entry(e.share).or_default();
            *n += 1;
            *n - 1
        })
        .collect();
    let waiting = counted.get(&Some(share)).copied().unwrap_or(0);
    let mut used = [0usize; 21];
    let mut idx = line.len();
    while idx > 0 {
        let e = &line[idx - 1];
        let q = e.priority;
        let level = (q - crate::protocol::PRIORITY_MIN) as usize;
        let by_priority = e.key.class != 0 && q < p && used[level] < (p - q) as usize;
        let by_turn = e.key.class == 1
            && q == p
            && e.share.is_some_and(|other| other != share)
            && ahead[idx - 1] > waiting;
        if !(by_priority || by_turn) {
            break;
        }
        used[level] += usize::from(by_priority);
        idx -= 1;
    }
    // A key strictly between line[idx-1] and line[idx]; pass one fewer if
    // there is no integer gap. The key is the gap's middle, not the integer
    // just below line[idx]: that would leave no gap behind the new entry, and
    // a later entry the walk stops at the same place would land one further.
    while idx < line.len() {
        let b = line[idx].key;
        let floor = idx
            .checked_sub(1)
            .map(|a| line[a].key)
            .filter(|a| a.class == b.class)
            .map_or(0, |a| a.ns + 1);
        if floor < b.ns {
            return Placement {
                class: b.class,
                ns: floor + (b.ns - floor) / 2,
            };
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
    run_ended(d, job);
    let yielded = job.spec.cooperative && exit == Some(76) && job.end_reason.is_none();
    if job.end_reason.is_none() && exit.is_some_and(|code| code != 0) && !yielded {
        job.terminal_status = Some(crate::protocol::TerminalStatus::NonZeroExit);
    }
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
        None if job.is_lease() && exit.is_none() && sig.is_none() => (JobState::Done, None),
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

/// The run stops holding its grant: its timing stops and the contract judges its working phase.
fn run_ended(d: &Daemon, job: &mut Job) {
    if let Some(timing) = &mut job.timing {
        timing.stop(monotonic_ns());
    }
    super::contract::work_ends(d, job);
}

/// End a job that never runs again (cancelled, connection gone).
pub fn end_job(d: &Daemon, s: &mut State, side: Side, id: u64, state: JobState, error: String) {
    let Some(job) = s.job_mut(side, id) else {
        return;
    };
    if job.state.is_terminal() {
        return;
    }
    run_ended(d, job);
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

    /// Entries of one share, `a`.
    fn alone(line: &[(Placement, i8)]) -> Vec<Waiting<'static>> {
        line.iter()
            .map(|&(key, priority)| Waiting {
                key,
                priority,
                share: Some("a"),
            })
            .collect()
    }

    #[test]
    fn skip_rule() {
        // Keys fall in the middle of the gap the walk stopped at.
        let line = alone(&[(k(10), 0), (k(20), 0), (k(30), 0), (k(40), 0)]);
        assert_eq!(place_in(&line, 0, "a", 99), k(99));
        // Priority 2 passes two priority-0 entries, not a third.
        assert_eq!(place_in(&line, 2, "a", 99), k(25));
        // Any waiting entry may be passed, the first too.
        assert_eq!(place_in(&line, 10, "a", 99), k(5));
        // Levels counted separately: one priority-1 and two priority-0.
        let line = alone(&[(k(10), 0), (k(20), 0), (k(30), 0), (k(40), 1), (k(50), 0)]);
        assert_eq!(place_in(&line, 2, "a", 99), k(25));
        // Priority 0 passes negative priorities too: p - q for every q < p.
        let line = alone(&[(k(10), 0), (k(20), -1), (k(30), -3)]);
        assert_eq!(place_in(&line, 0, "a", 99), k(15));
        // Equal priority is never passed.
        let line = alone(&[(k(10), 0), (k(20), 3), (k(30), 0)]);
        assert_eq!(place_in(&line, 3, "a", 99), k(25));
        // No integer gap: one fewer.
        let line = alone(&[(k(10), 0), (k(11), 0), (k(30), 0)]);
        assert_eq!(place_in(&line, 2, "a", 99), k(21));
        // A re-queued entry (class 0) ahead of a fresh one.
        let line = alone(&[(Placement { class: 0, ns: 99 }, 0), (k(5), 0)]);
        assert_eq!(place_in(&line, 1, "a", 200), k(2));
        // Re-queued: right behind the head, or one further on no gap.
        assert_eq!(after(&[k(10), k(20)], 1), k(11));
        assert_eq!(after(&[k(10), k(11), k(20)], 1), k(12));
        assert_eq!(after(&[k(10)], 1), k(11));
        // switch-ninfer's key sorts first of all.
        assert!(Placement { class: 0, ns: 0 } < Placement { class: 0, ns: 1 });
    }

    /// A line built by placing entries one after another, each named by its share and its number in it.
    #[derive(Default)]
    struct Placed {
        entries: Vec<(Placement, i8, Option<String>, String)>,
        clock: u64,
    }
    impl Placed {
        fn place(&mut self, share: &str, priority: i8) -> Placement {
            let line: Vec<Waiting> = self
                .entries
                .iter()
                .map(|(key, priority, share, _)| Waiting {
                    key: *key,
                    priority: *priority,
                    share: share.as_deref(),
                })
                .collect();
            self.clock += 1000;
            let key = place_in(&line, priority, share, self.clock);
            let number = self.entries.iter().filter(|e| e.2.as_deref() == Some(share)).count();
            self.insert(key, priority, Some(share.into()), format!("{share}{number}"));
            key
        }
        fn insert(&mut self, key: Placement, priority: i8, share: Option<String>, name: String) {
            assert!(self.entries.iter().all(|e| e.0 != key), "keys stay distinct");
            self.entries.push((key, priority, share, name));
            self.entries.sort_by_key(|e| e.0);
        }
        fn names(&self) -> Vec<&str> {
            self.entries.iter().map(|e| e.3.as_str()).collect()
        }
    }

    #[test]
    fn shares_of_one_priority_take_turns() {
        let mut line = Placed::default();
        for _ in 0..4 {
            line.place("A", 0);
        }
        line.place("B", 0);
        assert_eq!(line.names(), ["A0", "B0", "A1", "A2", "A3"]);
        line.place("B", 0);
        assert_eq!(line.names(), ["A0", "B0", "A1", "B1", "A2", "A3"]);
        line.place("C", 0);
        assert_eq!(line.names(), ["A0", "B0", "C0", "A1", "B1", "A2", "A3"]);
    }

    #[test]
    fn a_turn_never_passes_a_higher_priority_and_priority_passes_at_most_p_minus_q() {
        let mut line = Placed::default();
        for _ in 0..3 {
            line.place("A", 5);
        }
        line.place("B", -10);
        assert_eq!(line.names(), ["A0", "A1", "A2", "B0"]);
        let mut line = Placed::default();
        for _ in 0..20 {
            line.place("A", 0);
        }
        line.place("B", 5);
        assert_eq!(line.names().iter().position(|n| *n == "B0"), Some(15), "B passes exactly five");
        line.place("B", 5);
        assert_eq!(line.names().iter().position(|n| *n == "B1"), Some(16), "the second B stops behind the first");
    }

    #[test]
    fn a_fresh_entry_never_passes_a_requeued_entry_or_a_forced_ninfer_and_passes_ninfer_only_by_priority() {
        let requeued = Placement { class: 0, ns: 7 };
        let forced = Placement { class: 0, ns: 0 };
        for head in [requeued, forced] {
            let line = [Waiting { key: head, priority: -10, share: None }, Waiting { key: k(50), priority: 0, share: Some("A") }];
            let key = place_in(&line, 10, "B", 99);
            assert!(key > head && key.class == 1, "{head:?} -> {key:?}");
        }
        let ordinary = [Waiting { key: k(50), priority: 0, share: None }];
        assert_eq!(place_in(&ordinary, 0, "B", 99), k(99), "no turn passes NInfer");
        assert_eq!(place_in(&ordinary, 1, "B", 99), k(25), "priority does");
    }

    #[test]
    fn a_departed_entry_leaves_a_gap_the_next_placement_falls_strictly_inside() {
        let mut line = Placed::default();
        for _ in 0..3 {
            line.place("A", 0);
        }
        line.entries.remove(1);
        let key = line.place("B", 0);
        assert!(line.entries[0].0 < key && key < line.entries[2].0, "{:?}", line.names());
        assert_eq!(line.names(), ["A0", "B0", "A2"]);
    }

    /// Where `key` sorts in `line`: how many entries precede it.
    fn rank(line: &[(Placement, i8)], key: Placement) -> usize {
        assert!(line.iter().all(|(other, _)| *other != key), "a placement takes a free key");
        line.iter().filter(|(other, _)| *other < key).count()
    }

    /// `place_in` at d179805, before shares: the oracle for one share. It took the integer just below its
    /// successor, so its keys differ; where they sort does not.
    fn place_in_d179805(line: &[(Placement, i8)], p: i8, now: u64) -> Placement {
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

    #[test]
    fn with_one_share_every_placement_is_the_priority_rule_and_stops_before_requeued_entries() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut random = |bound: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % bound
        };
        for case in 0..20_000 {
            let requeued = if case % 2 == 0 { 0 } else { random(3) };
            let mut ns = 1 + random(3);
            let mut line = Vec::new();
            for _ in 0..requeued {
                line.push((Placement { class: 0, ns }, random(21) as i8 - 10));
                ns += 1 + random(3);
            }
            for _ in 0..random(12) {
                line.push((k(ns), random(21) as i8 - 10));
                ns += 1 + random(3);
            }
            let p = random(21) as i8 - 10;
            let now = ns + 1000;
            let fresh: Vec<_> = line.iter().copied().filter(|(key, _)| key.class == 1).collect();
            let placed = place_in(&alone(&line), p, "a", now);
            assert_eq!(
                rank(&line, placed),
                rank(&line, place_in_d179805(&fresh, p, now)),
                "{line:?} at {p}"
            );
            if requeued == 0 {
                assert_eq!(rank(&line, placed), rank(&line, place_in_d179805(&line, p, now)));
            }
        }
    }

    /// Places in the line: telling, preparation, parking and the share floor (S-1).
    mod places {
        use super::*;
        use crate::protocol::{ClientMsg, JobSpec, ReadyFigures};
        use crate::switch::control::tests::{daemon, enter, spec};
        use tokio::sync::mpsc::UnboundedReceiver;

        const SIDE: Side = Side::Host(1000);

        struct Line {
            d: Arc<Daemon>,
            dir: std::path::PathBuf,
            clients: Vec<UnboundedReceiver<ClientMsg>>,
        }
        impl Line {
            fn new(config: serde_json::Value) -> Line {
                let (d, dir) = daemon(config);
                Line { d, dir, clients: Vec::new() }
            }
            fn enter(&mut self, spec: JobSpec, prepares: bool) -> u64 {
                let (id, client) = enter(&self.d, SIDE, spec, prepares, None);
                self.clients.push(client);
                id
            }
            fn place(&mut self, share: &str) -> u64 {
                self.enter(spec(Some(share)), true)
            }
            fn schedule(&self) {
                self.d.with(|s| schedule(&self.d, s));
            }
            fn job<R>(&self, id: u64, f: impl FnOnce(&Job) -> R) -> R {
                self.d.with(|s| f(s.job(SIDE, id).expect("job")))
            }
            fn told(&self, id: u64) -> bool {
                self.job(id, |j| j.place.as_ref().is_some_and(|p| p.told()))
            }
            fn told_ids(&self, ids: &[u64]) -> Vec<u64> {
                ids.iter().copied().filter(|id| self.told(*id)).collect()
            }
            fn state(&self, id: u64) -> JobState {
                self.job(id, |j| j.state)
            }
            fn ready(&self, id: u64) -> Result<(), String> {
                let vram = self.job(id, |j| j.spec.vram);
                self.d.update(|s| {
                    crate::switch::preparation::ready(
                        s.job_mut(SIDE, id).unwrap(),
                        ReadyFigures { vram, expected_runtime_secs: Some(60), max_runtime_secs: None, shutdown_grace_secs: Some(5.0) },
                    )
                })
            }
            fn wait(&self, id: u64, reason: &str, parked: bool) -> Result<(), String> {
                self.d.update(|s| crate::switch::preparation::wait(s.job_mut(SIDE, id).unwrap(), reason.into(), parked))
            }
            fn order(&self) -> Vec<u64> {
                self.d.with(|s| {
                    s.line()
                        .into_iter()
                        .filter_map(|(_, e)| match e {
                            Entry::Job(_, id) => Some(id),
                            Entry::Ninfer => None,
                        })
                        .collect()
                })
            }
            /// The told records the journal kept.
            fn told_records(&self) -> Vec<serde_json::Value> {
                std::fs::read_to_string(self.dir.join("failures.jsonl"))
                    .unwrap_or_default()
                    .lines()
                    .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                    .filter(|record| record["event"] == "told")
                    .collect()
            }
        }
        impl Drop for Line {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.dir);
            }
        }
        fn exclusive(share: &str) -> JobSpec {
            JobSpec { mode: Mode::Exclusive, vram: 0, ..spec(Some(share)) }
        }

        #[test]
        fn the_first_n_places_are_told_and_an_admission_tells_the_next() {
            let mut line = Line::new(serde_json::json!({}));
            let places: Vec<u64> = (0..6).map(|_| line.place("a")).collect();
            line.schedule();
            assert_eq!(line.told_ids(&places), places[..4]);
            assert!(matches!(line.clients[0].try_recv(), Ok(ClientMsg::Prepare { prepare: true })));
            assert!(line.clients[4].try_recv().is_err() || line.job(places[4], |j| j.reason.clone()).unwrap().starts_with("placed: position"));
            line.ready(places[0]).unwrap();
            line.schedule();
            assert_eq!(line.state(places[0]), JobState::Running);
            line.schedule();
            assert_eq!(line.told_ids(&places), places[..5], "telling is never taken back; the fifth is told");
            for record in line.told_records() {
                crate::switch::preparation::Telling::obeyed(&record).unwrap();
            }
            assert_eq!(line.told_records().len(), 5);
        }

        #[test]
        fn a_ready_entry_passes_a_preparing_place_and_an_unready_place_is_never_admitted() {
            let mut line = Line::new(serde_json::json!({}));
            let place = line.enter(exclusive("a"), true);
            let ready = line.enter(spec(Some("b")), false);
            line.schedule();
            assert!(line.told(place));
            assert_eq!(line.state(place), JobState::Queued, "a preparing place is skipped");
            assert_eq!(line.state(ready), JobState::Running, "the ready entry behind it is admitted");
            assert_eq!(line.job(place, |j| j.reason.clone()).as_deref(), Some("preparing"));
            line.ready(place).unwrap();
            line.schedule();
            assert_eq!(line.state(place), JobState::Queued, "once ready, the exclusive waits for the GPU");
            assert_eq!(line.job(place, |j| j.reason.clone()).as_deref(), Some("waiting for 1 job(s) on the GPU to finish"));
            // A shared place of a group that runs does not join it before its Ready.
            let (running, _a) = enter(&line.d, SIDE, spec(Some("g")), false, Some("group"));
            let (member, _b) = enter(&line.d, SIDE, spec(Some("g")), true, Some("group"));
            line.d.with(|s| {
                s.job_mut(SIDE, place).unwrap().state = JobState::Cancelled;
                s.job_mut(SIDE, ready).unwrap().state = JobState::Done;
            });
            line.schedule();
            assert_eq!(line.state(running), JobState::Running);
            line.schedule();
            assert!(line.told(member));
            assert_eq!(line.state(member), JobState::Queued, "admit_members skips an unready place");
        }

        #[test]
        fn entries_ready_on_arrival_are_admitted_as_before_and_count_towards_telling() {
            let mut line = Line::new(serde_json::json!({}));
            let holder = line.enter(exclusive("a"), false);
            line.schedule();
            assert_eq!(line.state(holder), JobState::Running);
            let waiting: Vec<u64> = (0..4).map(|_| line.enter(exclusive("a"), false)).collect();
            let place = line.place("a");
            line.schedule();
            assert!(waiting.iter().all(|id| line.state(*id) == JobState::Queued));
            assert!(!line.told(place), "four counted entries of its own share stand ahead");
            assert_eq!(line.job(place, |j| j.reason.clone()).as_deref(), Some("placed: position 5"));
            let blocker = line.job(place, |j| j.blocker.clone()).unwrap();
            assert_eq!((blocker.kind, blocker.shares), (BlockerKind::PreparingAhead, vec!["a".to_string()]));
        }

        #[test]
        fn parked_places_count_for_nothing_and_count_again_once_unparked() {
            let mut line = Line::new(serde_json::json!({}));
            let places: Vec<u64> = (0..20).map(|_| line.place("a")).collect();
            line.schedule();
            assert_eq!(line.told_ids(&places).len(), 4);
            for parking in 0..10 {
                line.wait(places[parking], "waiting for host memory: 12884901888 bytes", true).unwrap();
                line.schedule();
                assert_eq!(line.told_ids(&places).len(), 5 + parking, "each parked place gives its count to the next");
            }
            assert_eq!(line.job(places[0], |j| j.reason.clone()).as_deref(), Some("parked: waiting for host memory: 12884901888 bytes"));
            line.wait(places[10], "waiting for host memory: 2147483648 bytes", false).unwrap();
            line.schedule();
            assert_eq!(line.told_ids(&places).len(), 14, "an unparked waiting place stays counted");
            assert_eq!(line.job(places[10], |j| j.reason.clone()).as_deref(), Some("preparing: waiting: waiting for host memory: 2147483648 bytes"));
            line.wait(places[0], "", false).unwrap();
            line.schedule();
            assert_eq!(line.told_ids(&places).len(), 14, "an unparked place is counted again; none is told while N or more are");
            assert!(line.told_records().iter().all(|r| crate::switch::preparation::Telling::obeyed(r).is_ok()));
        }

        #[test]
        fn a_new_share_is_told_whatever_parked_places_or_an_old_clients_backlog_stand_ahead() {
            let mut line = Line::new(serde_json::json!({}));
            let parked: Vec<u64> = (0..4).map(|_| line.place("a")).collect();
            line.schedule();
            for id in &parked {
                line.wait(*id, "large claim", true).unwrap();
            }
            let newcomer = line.place("b");
            line.schedule();
            assert!(line.told(newcomer));
            let mut line = Line::new(serde_json::json!({}));
            let backlog: Vec<u64> = (0..100).map(|_| line.enter(exclusive("uid-1000"), false)).collect();
            let campaign = line.place("kernel-x");
            assert_eq!(line.order()[..3], [backlog[0], campaign, backlog[1]]);
            line.schedule();
            assert!(line.told(campaign), "entries behind a place never delay it");
        }

        #[test]
        fn later_arrivals_ahead_of_told_places_are_told_and_telling_is_never_taken_back() {
            let mut line = Line::new(serde_json::json!({}));
            let a: Vec<u64> = (0..4).map(|_| line.place("a")).collect();
            line.schedule();
            let b: Vec<u64> = (0..4).map(|_| line.enter(JobSpec { priority: 4, ..spec(Some("b")) }, true)).collect();
            assert_eq!(line.order(), [b.clone(), a.clone()].concat(), "the higher-priority places pass them");
            line.schedule();
            assert_eq!(line.told_ids(&[a.clone(), b.clone()].concat()).len(), 8, "all eight are told");
            let ninth = line.place("a");
            line.schedule();
            assert!(!line.told(ninth));
            line.wait(a[0], "large claim", true).unwrap();
            line.schedule();
            line.wait(a[0], "", false).unwrap();
            line.schedule();
            assert!(!line.told(ninth), "a parked place that unparks is counted again");
            assert_eq!(line.told_ids(&[a, b].concat()).len(), 8, "no told place is displaced");
        }

        #[test]
        fn the_share_floor_tells_each_shares_first_places_whatever_stands_ahead() {
            let mut line = Line::new(serde_json::json!({"preparation_places": 4, "preparation_places_per_share": 2}));
            let x: Vec<u64> = (0..4).map(|_| line.place("x")).collect();
            line.schedule();
            assert_eq!(line.told_ids(&x), x, "X0-X3 are told");
            let y: Vec<u64> = (0..6).map(|_| line.place("y")).collect();
            assert_eq!(line.order(), [x[0], y[0], x[1], y[1], x[2], y[2], x[3], y[3], y[4], y[5]]);
            line.schedule();
            assert_eq!(line.told_ids(&y), y[..2], "Y0 and Y1 by the line window");
            for id in &y[..2] {
                line.ready(*id).unwrap();
            }
            line.schedule();
            assert!(y[..2].iter().all(|id| line.state(*id) == JobState::Running));
            line.schedule();
            assert_eq!(line.told_ids(&y), y[..4], "Y2 by the window, Y3 by the floor alone");
            let records = line.told_records();
            let record = |id: u64| {
                let handle = line.job(id, |j| j.handle.clone());
                records.iter().find(|r| r["handle"] == handle).unwrap().clone()
            };
            assert_eq!(record(y[2])["counted_ahead"], 3);
            assert_eq!(record(y[3])["counted_ahead"], 5, "only the floor told Y3");
            assert_eq!(record(y[3])["counted_share_ahead"], 1);
            for id in &y[2..4] {
                line.ready(*id).unwrap();
            }
            line.schedule();
            line.schedule();
            for id in &y[4..] {
                line.ready(*id).unwrap();
            }
            line.schedule();
            assert!(y.iter().all(|id| line.state(*id) == JobState::Running), "every Y entry is admitted");
            assert!(x.iter().all(|id| line.state(*id) == JobState::Queued), "while X's told places still wait");
            for record in line.told_records() {
                crate::switch::preparation::Telling::obeyed(&record).unwrap();
            }
            // The priority case: a third Y place told by the floor alone.
            let mut line = Line::new(serde_json::json!({}));
            let x: Vec<u64> = (0..4).map(|_| line.place("x")).collect();
            let y: Vec<u64> = (0..2).map(|_| line.enter(JobSpec { priority: -10, ..spec(Some("y")) }, true)).collect();
            line.schedule();
            assert_eq!(line.told_ids(&x), x);
            assert_eq!(line.told_ids(&y), y, "the floor tells both");
            let passing = line.enter(JobSpec { priority: -9, ..spec(Some("y")) }, true);
            assert_eq!(line.order(), [x.clone(), vec![y[0], passing, y[1]]].concat());
            line.schedule();
            assert!(line.told(passing), "three Y places are told by the floor alone");
            // One share alone gets the whole window; parked places count for neither condition.
            let mut line = Line::new(serde_json::json!({}));
            let alone: Vec<u64> = (0..6).map(|_| line.place("a")).collect();
            line.schedule();
            assert_eq!(line.told_ids(&alone), alone[..4]);
            let mut line = Line::new(serde_json::json!({}));
            let lead: Vec<u64> = (0..4).map(|_| line.place("x")).collect();
            line.schedule();
            let parking: Vec<u64> = (0..4).map(|_| line.place("y")).collect();
            line.schedule();
            assert_eq!(line.told_ids(&parking), parking[..2]);
            for id in &parking[..2] {
                line.wait(*id, "large claim", true).unwrap();
            }
            line.schedule();
            assert!(line.told(parking[2]) && line.told(parking[3]), "a share whose K places park tells its next places");
            assert_eq!(line.told_ids(&lead), lead);
        }

        #[test]
        fn a_config_whose_floor_exceeds_its_window_is_refused() {
            let dir = std::env::temp_dir().join(format!("gpu-switch-config-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("switch.json");
            std::fs::write(&path, br#"{"listen":[],"preparation_places":2,"preparation_places_per_share":3}"#).unwrap();
            let refused = crate::switch::config::SwitchConfig::load(&path).unwrap_err();
            assert!(refused.contains("preparation_places_per_share"), "{refused}");
            std::fs::write(&path, br#"{"listen":[]}"#).unwrap();
            let defaults = crate::switch::config::SwitchConfig::load(&path).unwrap();
            assert_eq!((defaults.preparation_places, defaults.preparation_places_per_share), (4, 2));
            std::fs::remove_dir_all(dir).unwrap();
        }

        #[test]
        fn preparation_requests_are_for_told_places_and_ready_figures_are_final() {
            let mut line = Line::new(serde_json::json!({"preparation_places": 1, "preparation_places_per_share": 1}));
            let told = line.place("a");
            let untold = line.place("a");
            let ready = line.enter(spec(Some("b")), false);
            line.schedule();
            assert!(line.told(told) && !line.told(untold));
            assert!(line.ready(untold).unwrap_err().contains("told place"));
            assert!(line.wait(untold, "claim", false).unwrap_err().contains("told place"));
            assert!(line.ready(ready).unwrap_err().contains("told place"));
            let invalid = ReadyFigures { vram: 0, expected_runtime_secs: Some(60), max_runtime_secs: None, shutdown_grace_secs: None };
            let refused = line.d.update(|s| crate::switch::preparation::ready(s.job_mut(SIDE, told).unwrap(), invalid));
            assert!(refused.unwrap_err().contains("--vram"), "a shared place declares its VRAM");
            line.ready(told).unwrap();
            line.ready(told).unwrap();
            let changed = ReadyFigures { vram: 2 << 30, expected_runtime_secs: Some(60), max_runtime_secs: None, shutdown_grace_secs: Some(5.0) };
            let refused = line.d.update(|s| crate::switch::preparation::ready(s.job_mut(SIDE, told).unwrap(), changed));
            assert!(refused.unwrap_err().contains("cannot change"));
            line.job(told, |j| assert_eq!((j.spec.expected_runtime_secs, j.shutdown_grace_secs), (Some(60), Some(5.0))));
        }

        #[test]
        fn a_preparing_exclusive_place_is_no_waiting_exclusive() {
            let mut line = Line::new(serde_json::json!({}));
            let place = line.enter(exclusive("a"), true);
            line.schedule();
            assert!(line.told(place));
            assert!(!line.d.with(|s| census(s, SIDE, None).exclusive_waiting));
            line.ready(place).unwrap();
            assert!(line.d.with(|s| census(s, SIDE, None).exclusive_waiting));
        }

        #[test]
        fn places_survive_a_restore() {
            let mut line = Line::new(serde_json::json!({}));
            let ready = line.place("a");
            let parked = line.place("a");
            let untold: Vec<u64> = (0..4).map(|_| line.place("a")).collect();
            line.schedule();
            line.ready(ready).unwrap();
            line.wait(parked, "large claim", true).unwrap();
            crate::switch::persist::save(&line.d);
            let (successor, other) = daemon(serde_json::json!({"state_dir": line.dir}));
            crate::switch::persist::restore(&successor);
            successor.with(|s| {
                let place = |id: u64| s.job(SIDE, id).unwrap().place.clone().unwrap();
                assert!(place(ready).told() && place(ready).ready.is_some());
                assert!(place(parked).parked && place(parked).waiting.as_deref() == Some("large claim"));
                assert!(!place(untold[3]).told());
                assert_eq!(s.job(SIDE, ready).unwrap().share, "a");
            });
            std::fs::remove_dir_all(other).unwrap();
        }

        #[test]
        fn a_thousand_places_are_scheduled_and_shown_within_fifty_milliseconds() {
            let mut line = Line::new(serde_json::json!({}));
            for index in 0..1000 {
                line.place(&format!("share-{}", index % 7));
            }
            let started = Instant::now();
            line.schedule();
            let status = line.d.with(|s| crate::switch::view::status(&line.d, s, SIDE, None, Instant::now()));
            let elapsed = started.elapsed();
            assert_eq!(status.jobs.len(), 1000);
            assert!(elapsed < Duration::from_millis(50), "{elapsed:?}");
        }

        #[test]
        fn status_shows_each_entrys_share_reason_and_blocker() {
            // NInfer owning the GPU.
            let mut line = Line::new(serde_json::json!({}));
            let waiting = line.enter(spec(Some("y")), false);
            line.d.with(|s| s.ninfer.phase = NPhase::Serving);
            line.schedule();
            let own = line.d.with(|s| crate::switch::view::own_job_of(s, SIDE, s.job(SIDE, waiting).unwrap()));
            assert_eq!(own.share.as_deref(), Some("y"));
            assert!(own.reason.unwrap().starts_with("NInfer holds the GPU"));
            assert_eq!(own.blocker.unwrap().kind, BlockerKind::Ninfer);
            // A foreign exclusive job running.
            let mut line = Line::new(serde_json::json!({}));
            let holder = line.enter(exclusive("x"), false);
            line.schedule();
            let shared = line.enter(spec(Some("y")), false);
            line.schedule();
            assert_eq!(line.state(holder), JobState::Running);
            let blocker = line.job(shared, |j| j.blocker.clone()).unwrap();
            assert_eq!((blocker.kind, blocker.shares), (BlockerKind::ExclusiveRunning, vec!["x".to_string()]));
            // A foreign exclusive entry ahead.
            let mut line = Line::new(serde_json::json!({}));
            let running = line.enter(spec(Some("z")), false);
            line.schedule();
            let ahead = line.enter(exclusive("x"), false);
            let behind = line.enter(JobSpec { expected_runtime_secs: None, ..spec(Some("y")) }, false);
            line.schedule();
            assert_eq!(line.state(running), JobState::Running);
            assert_eq!(line.job(ahead, |j| j.blocker.clone()).unwrap().kind, BlockerKind::VramRoom);
            let blocker = line.job(behind, |j| j.blocker.clone()).unwrap();
            assert_eq!((blocker.kind, blocker.shares), (BlockerKind::ExclusiveAhead, vec!["x".to_string()]));
            // A fitting foreign shared lease sets none.
            let fitting = line.enter(spec(Some("y")), false);
            line.d.with(|s| s.job_mut(SIDE, ahead).unwrap().state = JobState::Cancelled);
            line.schedule();
            assert_eq!(line.state(fitting), JobState::Running);
            assert_eq!(line.job(fitting, |j| j.blocker.clone()), None);
        }

        #[test]
        fn a_foreign_vram_shortage_is_a_vram_room_blocker() {
            let mock = std::env::current_exe().unwrap().parent().unwrap().parent().unwrap().join("examples/libnvml_mock.so");
            assert!(mock.exists(), "cargo test builds the mock NVML beside the tests: {}", mock.display());
            let mut line = Line::new(serde_json::json!({"nvml": mock}));
            let holder = line.enter(JobSpec { vram: 20 << 30, ..spec(Some("x")) }, false);
            line.schedule();
            assert_eq!(line.state(holder), JobState::Running);
            line.d.with(|s| {
                s.view.gpu = Some(crate::gpu::Sample {
                    compute: vec![(std::process::id(), Some(20 << 30))],
                    graphics: Vec::new(),
                    computing: HashSet::new(),
                    memory: Some((24 << 30, 4 << 30)),
                    card: Default::default(),
                });
                s.view.gpu_at = Some(Instant::now());
            });
            let short = line.enter(JobSpec { vram: 8 << 30, ..spec(Some("y")) }, false);
            line.schedule();
            assert_eq!(line.state(short), JobState::Queued);
            let blocker = line.job(short, |j| j.blocker.clone()).unwrap();
            assert_eq!((blocker.kind, blocker.shares), (BlockerKind::VramRoom, vec!["x".to_string()]));
        }

        #[test]
        fn a_fabricated_told_record_that_meets_neither_condition_is_rejected() {
            let record = |ahead: u64, share: u64| serde_json::json!({"event":"told","counted_ahead":ahead,
                "counted_share_ahead":share,"preparation_places":4,"preparation_places_per_share":2});
            assert!(crate::switch::preparation::Telling::obeyed(&record(3, 3)).is_ok());
            assert!(crate::switch::preparation::Telling::obeyed(&record(5, 1)).is_ok());
            assert!(crate::switch::preparation::Telling::obeyed(&record(4, 2)).is_err());
            assert!(crate::switch::preparation::Telling::obeyed(&serde_json::json!({"event":"told"})).is_err());
        }
    }
}
