//! Owner state, the line, and the reconcile step. Every event mutates
//! [`State`] under one lock and wakes [`run`], which turns the state into
//! actions: admissions, signals, `start`/`stop` to the arbiter.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{watch, Notify};

use crate::clock::Clock;
use crate::gpu::{Driver, Nvml};
use crate::process_identity::Instance;
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
    /// `kept` and `bump` as the daemon was built, never read: the saver starts from them, so it sees every change
    /// since, also one published before it first ran.
    unsaved: (watch::Receiver<u64>, watch::Receiver<u64>),
    /// The saves' requests and how far the ended saves reach (persist).
    pub(crate) saves: super::persist::Saves,
    sample: Notify,
    pub journal: super::journal::Journal,
    /// The card, reached through NVML; unconfigured when the configuration names no NVML library.
    pub card: Arc<super::card_reader::CardReader>,
    /// This switch process in its boot: what identifies the losses it publishes.
    pub incarnation: Instance,
    /// Serving, then stopping: published by the service before it saves and before its runtime drops.
    pub lifecycle: super::lifecycle::Lifecycle,
    /// The one source of every monotonic reading of the daemon: every instant its state holds and its decisions
    /// compare.
    pub clock: Clock,
    #[cfg(test)]
    pub hooks: super::hooks::Hooks,
}

impl Daemon {
    /// The daemon of `config`, reading the card through NVML from its configured library. It never fails because
    /// of NVML: a library that cannot open is a lost card.
    pub fn new(config: SwitchConfig, config_path: PathBuf) -> Result<Arc<Daemon>, String> {
        let driver = Daemon::configured_driver(&config);
        Daemon::reading(config, config_path, driver, Instance::current()?, Clock::system())
    }

    /// The driver `config` names: NVML from its configured library, or none.
    pub fn configured_driver(config: &SwitchConfig) -> Option<Box<dyn Driver>> {
        config
            .nvml
            .clone()
            .map(|library| Box::new(Nvml::new(library)) as Box<dyn Driver>)
    }

    /// The daemon of `config` reading the card through `driver`, as incarnation `incarnation`, on `clock`; no driver
    /// is a host without a card.
    pub fn reading(
        config: SwitchConfig,
        config_path: PathBuf,
        driver: Option<Box<dyn Driver>>,
        incarnation: Instance,
        clock: Clock,
    ) -> Result<Arc<Daemon>, String> {
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
        let card = Arc::new(super::card_reader::CardReader::new(
            driver,
            secs(config.gpu_reopen_interval_secs),
            clock.clone(),
        ));
        let (bump, observed) = watch::channel(0);
        let (kept, worth_keeping) = watch::channel(0);
        Ok(Arc::new(Daemon {
            config,
            config_path,
            secret,
            state_dir,
            state: Mutex::new(State::new(backoff, card.configured())),
            bump,
            kept,
            unsaved: (worth_keeping, observed),
            saves: super::persist::Saves::new(),
            sample: Notify::new(),
            journal,
            card,
            incarnation,
            lifecycle: super::lifecycle::Lifecycle::new(),
            clock,
            #[cfg(test)]
            hooks: Default::default(),
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
        self.keep();
        r
    }

    /// A change worth saving at once: the short save debounce (`persist`).
    pub fn keep(&self) {
        self.kept.send_modify(|v| *v = v.wrapping_add(1));
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

    /// The changes worth keeping and the observations, each as unseen since the daemon was built: the saver's, so
    /// a change published before the saver first runs still wakes it.
    pub fn unsaved(&self) -> (watch::Receiver<u64>, watch::Receiver<u64>) {
        self.unsaved.clone()
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
        pass(&d);
        let _ = tokio::time::timeout(tick, rx.changed()).await;
    }
}

/// One reconcile pass. Its decision instant, read here before the state lock, decides which steps are due, which
/// detached jobs are abandoned and what a rule judges.
fn pass(d: &Arc<Daemon>) {
    let now = d.clock.monotonic();
    d.with(|s| reconcile(d, s, now));
}

/// The arbiter link has been down longer than the arbiter keeps NInfer.
fn link_gone(s: &State, now: Duration, grace: Duration) -> bool {
    s.arbiter.is_none() && s.ninfer.link_lost_at.is_none_or(|at| now.saturating_sub(at) >= grace)
}

/// Jobs whose clients did not come back after a restart of gpu-switch.
fn abandon_detached(d: &Arc<Daemon>, s: &mut State, now: Duration) {
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

fn reconcile(d: &Arc<Daemon>, s: &mut State, now: Duration) {
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
                && job.admitted_at.is_some_and(|t| now.saturating_sub(t) >= min_hold)
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
                        .is_some_and(|t| now.saturating_sub(t) >= secs(cfg.linger_secs)));
            if ready && (lingered || s.switch_to_ninfer) {
                s.ninfer.key = if s.switch_to_ninfer {
                    // Ahead of every waiter, fresh or re-queued.
                    Placement { class: 0, ns: 0 }
                } else {
                    Placement {
                        class: 1,
                        ns: d.clock.monotonic_ns(),
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
            let restart = s.ninfer.restart && s.serve_wanted && s.arbiter.is_some() && !s.ninfer.failed;
            match (ninfer_off_gpu(d, s), restart) {
                (false, _) => d.want_sample(),
                // A restart starts work: it asks the card again, at the decision.
                (true, true) if !matches!(super::card::usable(d, s), super::card::Usable::Card(_)) => {
                    d.want_sample()
                }
                (true, restart) => {
                    s.ninfer.restart = false;
                    s.ninfer.host_pid = None;
                    s.ninfer.ns_pid = None;
                    match restart {
                        true => start_ninfer(d, s),
                        false => {
                            s.ninfer.phase = NPhase::Idle;
                            s.ninfer.failed = false;
                            s.left_gpu_at = Some(now);
                            eprintln!("gpu-switch: NInfer off the GPU, it belongs to the line");
                        }
                    }
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

/// NInfer is off the GPU only on a usable sample taken after it stopped that no longer lists its pid, whether or
/// not its pid is known; on a host without a card at once.
fn ninfer_off_gpu(d: &Daemon, s: &State) -> bool {
    match super::card::usable(d, s) {
        super::card::Usable::Card(card) if s.view.card.fresh_after(s.ninfer.stopped_at) => {
            card.is_none_or(|g| s.ninfer.host_pid.is_none_or(|pid| !g.lists(pid)))
        }
        _ => false,
    }
}

/// Compute pids on the card that belong to no job of ours and no NInfer,
/// minus idle graphics/compute clients (as tools/_gpu_card.py:22-30).
/// `except`: the candidate's own tree. No card, on a host without one: none.
fn foreign_busy(s: &State, card: Option<&crate::gpu::Sample>, except: &HashSet<u32>) -> usize {
    let Some(g) = card else { return 0 };
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

/// The room rule against the running jobs (see room.rs), on the usable sample `card`; on a host without a card,
/// admit.
fn room_for(s: &State, card: Option<&crate::gpu::Sample>, need: u64) -> Result<(), String> {
    if s.running().next().is_none() {
        return Ok(());
    }
    let Some(g) = card else {
        return Ok(());
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

fn admit(clock: &Clock, s: &mut State, side: Side, id: u64, now: Duration) {
    let Some(job) = s.job_mut(side, id) else {
        return;
    };
    job.state = JobState::Running;
    job.started_at = Some(now_unix());
    job.runs += 1;
    job.admitted_at = Some(now);
    job.timing
        .get_or_insert_with(Default::default)
        .begin(clock.monotonic_ns());
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
pub(crate) fn schedule(d: &Daemon, s: &mut State) {
    let now = d.clock.monotonic();
    admit_members(d, s, now);
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
        // Every entry asks the card at its verdict; a lost card's text comes before every other reason. Decide on a
        // usable sample newer than the last departure, or a fast successor could be admitted beside processes the
        // sample never saw.
        let card = match (super::card::usable(d, s), blocked.clone()) {
            (super::card::Usable::Lost(text), _) => {
                reasons.push((*entry, Wait::Event(text), None));
                continue;
            }
            (_, Some(blocker)) => {
                reasons.push((*entry, Wait::Event(behind()), blocker));
                continue;
            }
            (super::card::Usable::Unanswered(text), None) => {
                reasons.push((*entry, Wait::Card(text), None));
                blocked = Some(None);
                continue;
            }
            (super::card::Usable::Card(_), None) if !s.view.card.fresh_after(s.left_gpu_at) => {
                reasons.push((
                    *entry,
                    Wait::Card("waiting for a GPU sample after the last job left".into()),
                    None,
                ));
                blocked = Some(None);
                continue;
            }
            (super::card::Usable::Card(card), None) => card,
        };
        let running = holders.count;
        match *entry {
            Entry::Ninfer => {
                match (running, foreign_busy(s, card, &HashSet::new())) {
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
                        if bypassing.is_some() && !super::backfill::fits(&d.clock, s, side, id, now) =>
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
                    Mode::Exclusive => match foreign_busy(s, card, &s.tree(root)) {
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
                    Mode::Shared => room_for(s, card, vram).map_err(|why| (Wait::Card(why), room)),
                };
                match verdict {
                    Ok(()) => {
                        admit(&d.clock, s, side, id, now);
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
/// It needs no departure freshness, but it asks the card before each admission: a member stays queued unless the
/// card is usable, and the line walk gives it its reason.
fn admit_members(d: &Daemon, s: &mut State, now: Duration) {
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
        match super::card::usable(d, s) {
            super::card::Usable::Card(_) => admit(&d.clock, s, side, id, now),
            super::card::Usable::Unanswered(_) | super::card::Usable::Lost(_) => {}
        }
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
pub fn placement(clock: &Clock, s: &State, p: i8, share: &str) -> Placement {
    let line: Vec<Waiting> = s
        .line()
        .into_iter()
        .map(|(key, e)| Waiting {
            key,
            priority: s.entry_priority(e),
            share: s.entry_share(e),
        })
        .collect();
    place_in(&line, p, share, clock.monotonic_ns())
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
pub fn behind_head(clock: &Clock, s: &State) -> Placement {
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
    after(clock, &line.iter().map(|(k, _)| *k).collect::<Vec<_>>(), idx)
}

/// A key sorting after `keys[idx-1]` and before `keys[idx]`, or the first
/// gap further back.
fn after(clock: &Clock, keys: &[Placement], mut idx: usize) -> Placement {
    loop {
        let a = idx.checked_sub(1).map(|i| keys[i]);
        let b = keys.get(idx).copied();
        let c = match (a, b) {
            (None, None) => {
                return Placement {
                    class: 0,
                    ns: clock.monotonic_ns(),
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

/// Schedule signals for a running job: `(delay from the previous, signal)`, from `clock`'s reading as it is built.
pub fn schedule_kill(clock: &Clock, job: &mut Job, steps: &[(Duration, Sig)]) {
    let now = clock.monotonic();
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
    s.left_gpu_at = Some(d.clock.monotonic());
    let behind = behind_head(&d.clock, s);
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
        timing.stop(d.clock.monotonic_ns());
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
        s.left_gpu_at = Some(d.clock.monotonic());
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
        assert_eq!(after(&Clock::system(), &[k(10), k(20)], 1), k(11));
        assert_eq!(after(&Clock::system(), &[k(10), k(11), k(20)], 1), k(12));
        assert_eq!(after(&Clock::system(), &[k(10)], 1), k(11));
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
        use crate::test_support::scaling::{self, Measured};
        use crate::thread_instructions::{counted, Count};
        /// The base count of 1,000 places of 7 shares, measured at the base in each profile (G3c).
        #[cfg(debug_assertions)]
        const PLACES_BASE: u64 = 71_944_298;
        #[cfg(not(debug_assertions))]
        const PLACES_BASE: u64 = 16_359_414;
        use crate::protocol::{ClientMsg, JobSpec, ReadyFigures};
        use crate::switch::control::tests::{daemon, daemon_on, enter, spec, StateDir};
        use tokio::sync::mpsc::UnboundedReceiver;

        const SIDE: Side = Side::Host(1000);

        struct Line {
            d: Arc<Daemon>,
            dir: StateDir,
            clients: Vec<UnboundedReceiver<ClientMsg>>,
        }
        impl Line {
            fn new(config: serde_json::Value) -> Line {
                Line::on(crate::clock::Clock::system(), config)
            }
            /// The line of a daemon on `clock`.
            fn on(clock: crate::clock::Clock, config: serde_json::Value) -> Line {
                let (d, dir) = daemon_on(clock, config);
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
                std::fs::read_to_string(self.dir.path().join("failures.jsonl"))
                    .unwrap_or_default()
                    .lines()
                    .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                    .filter(|record| record["event"] == "told")
                    .collect()
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
            let dir = StateDir::new();
            let path = dir.path().join("switch.json");
            std::fs::write(&path, br#"{"listen":[],"preparation_places":2,"preparation_places_per_share":3}"#).unwrap();
            let refused = crate::switch::config::SwitchConfig::load(&path).unwrap_err();
            assert!(refused.contains("preparation_places_per_share"), "{refused}");
            std::fs::write(&path, br#"{"listen":[]}"#).unwrap();
            let defaults = crate::switch::config::SwitchConfig::load(&path).unwrap();
            assert_eq!((defaults.preparation_places, defaults.preparation_places_per_share), (4, 2));
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
            let (successor, _other) = daemon(serde_json::json!({"state_dir": line.dir.path()}));
            crate::switch::persist::restore(&successor).expect("the saved state restores");
            successor.with(|s| {
                let place = |id: u64| s.job(SIDE, id).unwrap().place.clone().unwrap();
                assert!(place(ready).told() && place(ready).ready.is_some());
                assert!(place(parked).parked && place(parked).waiting.as_deref() == Some("large claim"));
                assert!(!place(untold[3]).told());
                assert_eq!(s.job(SIDE, ready).unwrap().share, "a");
            });
        }

        /// `count` places of `shares` shares arriving in turn on a fresh line, then one schedule and one status, which
        /// alone are counted: the line, its places, the status and the instructions the two retired on this thread.
        fn scheduled_and_shown(count: usize, shares: usize) -> (Line, Vec<u64>, crate::protocol::Status, Count) {
            let mut line = Line::new(serde_json::json!({}));
            let places: Vec<u64> = (0..count).map(|index| line.place(&format!("share-{}", index % shares))).collect();
            let (status, counted) = counted(|| {
                line.schedule();
                line.d.with(|s| crate::switch::view::status(&line.d, s, SIDE, None, line.d.clock.monotonic()))
            });
            (line, places, status, counted)
        }

        /// The cost of scheduling and showing `count` places of `shares` shares: three fresh lines' counts.
        fn places_cost(count: usize, shares: usize) -> Measured {
            let counts: Vec<u64> = (0..3).map(|_| scheduled_and_shown(count, shares).3.instructions).collect();
            Measured { size: (count as u64, shares as u64), count: counts[0], spread: scaling::spread(&counts) }
        }

        /// Scheduling and showing 1,000 places of 7 shares stays within its instruction budget, linear in the places
        /// and in the shares; both run on the test thread alone. Arriving in turn, the places keep their arrival
        /// order. The window (N = 4) tells places 1-4 and the share floor (K = 2) places 5-14, each share's first two;
        /// the other 986 wait untold behind all seven shares. No place is ready, so none is admitted.
        #[test]
        fn a_thousand_places_are_scheduled_and_shown_within_their_budget() {
            let (line, places, status, _) = scheduled_and_shown(1000, 7);
            let in_places = [places_cost(1000, 7), places_cost(2000, 7), places_cost(4000, 7)];
            let in_shares = [in_places[0], places_cost(1000, 14), places_cost(1000, 28)];
            eprintln!("places scheduled and shown: {in_places:?} {in_shares:?}");
            scaling::assert_linear("scheduling and showing in places", in_places, 2.6);
            scaling::assert_linear("scheduling and showing in shares", in_shares, 2.6);
            scaling::assert_budget("1,000 places of 7 shares", in_places[0], PLACES_BASE, 1.5);
            assert_eq!(status.jobs.len(), 1000);
            let shown: HashMap<u64, _> = status.jobs.iter().map(|job| (job.id, job)).collect();
            assert_eq!(shown.len(), 1000, "the status shows 1,000 distinct entries");
            for (index, id) in places.iter().enumerate() {
                assert_eq!((shown[id].state, shown[id].position), (JobState::Queued, Some(index + 1)));
            }
            let (told, placed): (Vec<u64>, Vec<u64>) = places
                .iter()
                .copied()
                .partition(|id| shown[id].preparation.as_ref().is_some_and(|place| place.told_at.is_some()));
            assert_eq!(told, places[..14], "the window tells four, the share floor each share's first two");
            assert_eq!(placed, places[14..]);
            for id in &told {
                assert_eq!((shown[id].reason.as_deref(), &shown[id].blocker), (Some("preparing"), &None));
            }
            let ahead = Blocker {
                kind: BlockerKind::PreparingAhead,
                shares: (0..7).map(|share| format!("share-{share}")).collect(),
            };
            for (index, id) in placed.iter().enumerate() {
                let reason = format!("placed: position {}", index + 15);
                assert_eq!((shown[id].reason.as_ref(), shown[id].blocker.as_ref()), (Some(&reason), Some(&ahead)));
            }
            assert_eq!(line.told_records().len(), 14);
        }

        #[test]
        fn status_shows_each_entrys_share_reason_and_blocker() {
            // NInfer owning the GPU.
            let mut line = Line::new(serde_json::json!({}));
            let waiting = line.enter(spec(Some("y")), false);
            line.d.with(|s| s.ninfer.phase = NPhase::Serving);
            line.schedule();
            let own = line.d.with(|s| crate::switch::view::own_job_of(&line.d.clock, s, SIDE, s.job(SIDE, waiting).unwrap()));
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

        /// Q1's producer: a run that ends after the last sample holds the next admission for a new sample and wants
        /// one. The sample and the end are both read on the manual source, 1 ns apart, so the order is by construction.
        #[test]
        fn a_departure_after_the_last_sample_holds_admission_for_a_new_sample_and_wants_one() {
            let origin = crate::clock::Readings {
                monotonic: Duration::from_secs(16_777_216),
                wall: Duration::from_secs(1_800_000_000),
            };
            let (manual, clock) = crate::clock::Manual::starting_at(origin);
            let mock = crate::roles::nvml_mock();
            let mut line = Line::on(clock, serde_json::json!({"nvml": mock}));
            let observe = |line: &Line, sample: crate::gpu::Sample| {
                let observation = crate::switch::card::tests::sampled(sample, line.d.clock.monotonic());
                line.d.observe(|s| crate::switch::card::observe(&line.d, s, observation));
            };
            observe(&line, crate::gpu::Sample { memory: Some((24 << 30, 24 << 30)), ..Default::default() });
            let a = line.enter(exclusive("a"), false);
            line.schedule();
            assert_eq!(line.state(a), JobState::Running);
            let b = line.enter(exclusive("b"), false);
            manual.advance(Duration::from_nanos(1));
            line.d.update(|s| finish_run(&line.d, s, SIDE, a, Some(0), None));
            line.schedule();
            assert_eq!(line.state(b), JobState::Queued);
            assert_eq!(
                line.job(b, |j| j.reason.clone()).as_deref(),
                Some("waiting for a GPU sample after the last job left")
            );
            let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
            let wanted = runtime.block_on(async { tokio::time::timeout(Duration::ZERO, line.d.sample_wanted()).await });
            assert!(wanted.is_ok(), "the admission walk stored a wanted permit");
        }

        #[test]
        fn a_foreign_vram_shortage_is_a_vram_room_blocker() {
            let mock = crate::roles::nvml_mock();
            let mut line = Line::new(serde_json::json!({"nvml": mock}));
            // A configured card starts unread: an initial sample with no foreign process admits the holder.
            let observe = |line: &Line, sample: crate::gpu::Sample| {
                let observation = crate::switch::card::tests::sampled(sample, line.d.clock.monotonic());
                line.d.observe(|s| crate::switch::card::observe(&line.d, s, observation));
            };
            observe(&line, crate::gpu::Sample { memory: Some((24 << 30, 24 << 30)), ..Default::default() });
            let holder = line.enter(JobSpec { vram: 20 << 30, ..spec(Some("x")) }, false);
            line.schedule();
            assert_eq!(line.state(holder), JobState::Running);
            observe(
                &line,
                crate::gpu::Sample {
                    compute: vec![(std::process::id(), Some(20 << 30))],
                    memory: Some((24 << 30, 4 << 30)),
                    ..Default::default()
                },
            );
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

    /// The gate: every admission path asks the card at its decision (PLAN-L §3.3; T-4, T-9, T-10, T-11, T-16, T-17).
    mod gate {
        use super::*;
        use crate::protocol::{ClientMsg, JobSpec, Telemetry};
        use crate::switch::card::tests::{known, readable_began};
        use crate::switch::card::{self as owner, Heard, Usable};
        use crate::switch::card::tests::next_loss;
        use crate::switch::card_reader::tests::{lost, timeout, unknown, Act, Call, Script};
        use crate::switch::card_reader::Observation;
        use crate::switch::control::tests::{daemon, enter, incarnation, reading, spec, StateDir};
        use tokio::sync::mpsc::UnboundedReceiver;

        const SIDE: Side = Side::Host(1000);

        fn block<F: std::future::Future>(future: F) -> F::Output {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(future)
        }

        fn exclusive(share: &str) -> JobSpec {
            JobSpec { mode: Mode::Exclusive, vram: 0, ..spec(Some(share)) }
        }

        /// A daemon reading a scripted card, with the clients of the entries a test made.
        struct Gate {
            d: Arc<Daemon>,
            script: Script,
            _dir: StateDir,
            clients: Vec<UnboundedReceiver<ClientMsg>>,
        }

        impl Gate {
            fn new(config: serde_json::Value) -> Gate {
                let script = Script::new();
                let (d, dir) = reading(config, Some(script.driver()), incarnation(1));
                Gate { d, script, _dir: dir, clients: Vec::new() }
            }
            fn readable(config: serde_json::Value) -> Gate {
                let gate = Gate::new(config);
                assert_eq!(gate.read(), Heard::Read(()));
                gate
            }
            fn publish<T: owner::Reading>(&self, observation: Observation<T>) -> Heard<()> {
                self.d.observe(|s| owner::observe(&self.d, s, observation)).map_value()
            }
            fn read(&self) -> Heard<()> {
                self.publish(block(self.d.card.sample()))
            }
            async fn read_now(&self) -> Heard<()> {
                self.publish(self.d.card.sample().await)
            }
            fn lose(&self) -> String {
                self.script.then(Call::Sample, Act::Fail(lost()));
                assert_eq!(self.read(), Heard::Failed(lost()));
                self.d.with(|s| s.view.card.loss().unwrap().reason())
            }
            fn enter(&mut self, spec: JobSpec, group: Option<&str>) -> u64 {
                let (id, client) = enter(&self.d, SIDE, spec, false, group);
                self.clients.push(client);
                id
            }
            fn schedule(&self) {
                self.d.with(|s| schedule(&self.d, s));
            }
            fn state(&self, id: u64) -> JobState {
                self.d.with(|s| s.job(SIDE, id).unwrap().state)
            }
            fn reason(&self, id: u64) -> Option<String> {
                self.d.with(|s| s.job(SIDE, id).unwrap().reason.clone())
            }
            fn usable(&self) -> String {
                self.d.with(|s| match owner::usable(&self.d, s) {
                    Usable::Card(card) => format!("card {}", card.is_some()),
                    Usable::Unanswered(text) | Usable::Lost(text) => text,
                })
            }
            /// No client of the test's entries heard a grant.
            fn none_granted(&mut self) -> bool {
                self.clients.iter_mut().all(|client| {
                    std::iter::from_fn(|| client.try_recv().ok()).all(|msg| !matches!(msg, ClientMsg::Granted { .. }))
                })
            }
        }

        #[test]
        fn while_lost_nothing_is_granted_and_every_entry_waits_with_the_loss_text() {
            // (a) an idle loss and an exclusive job; (b) a shared job with nothing running.
            let mut gate = Gate::readable(serde_json::json!({}));
            let reason = gate.lose();
            assert!(reason.contains(lost().text()) && reason.contains("found lost at 20"), "{reason}");
            let ids = [gate.enter(exclusive("a"), None), gate.enter(spec(Some("b")), None)];
            gate.schedule();
            for id in ids {
                assert_eq!((gate.state(id), gate.reason(id)), (JobState::Queued, Some(reason.clone())));
            }
            assert!(gate.none_granted());
            // (c) a shared job beside a running one with spare VRAM; (d) a member of a running group; (g) a
            // departure, then the loss.
            let mut gate = Gate::readable(serde_json::json!({}));
            let running = gate.enter(spec(Some("c")), Some("group"));
            gate.schedule();
            assert_eq!(gate.state(running), JobState::Running);
            gate.clients.clear();
            let reason = gate.lose();
            let ids = [gate.enter(spec(Some("c")), None), gate.enter(spec(Some("c")), Some("group"))];
            gate.schedule();
            for id in ids {
                assert_eq!((gate.state(id), gate.reason(id)), (JobState::Queued, Some(reason.clone())));
            }
            gate.d.with(|s| finish_run(&gate.d, s, SIDE, running, Some(0), None));
            gate.schedule();
            for id in ids {
                assert_eq!(gate.reason(id), Some(reason.clone()), "the loss text, not the departure's");
            }
            assert!(gate.none_granted());
            // (e) NInfer's line entry; (f) NInfer releasing with a restart and an unknown host pid.
            let gate = Gate::readable(serde_json::json!({}));
            let (tx, mut arbiter) = tokio::sync::mpsc::unbounded_channel();
            gate.d.with(|s| {
                s.serve_wanted = true;
                s.arbiter = Some(Arbiter { conn: 1, tx, host_pid: 1 });
                s.ninfer.phase = NPhase::Queued;
            });
            gate.lose();
            gate.schedule();
            assert_eq!(gate.d.with(|s| s.ninfer.phase), NPhase::Queued);
            gate.d.with(|s| {
                s.ninfer.phase = NPhase::Releasing;
                s.ninfer.restart = true;
                s.ninfer.host_pid = None;
                s.ninfer.stopped_at = Some(gate.d.clock.monotonic());
                reconcile(&gate.d, s, gate.d.clock.monotonic());
            });
            assert_eq!(gate.d.with(|s| s.ninfer.phase), NPhase::Releasing, "while lost, NInfer stays releasing");
            assert!(arbiter.try_recv().is_err(), "no start is sent");
        }

        #[test]
        fn a_group_member_stays_queued_while_the_card_is_unanswered() {
            // (h) after an unanswered read.
            let mut gate = Gate::readable(serde_json::json!({}));
            let leader = gate.enter(spec(Some("g")), Some("group"));
            gate.schedule();
            assert_eq!(gate.state(leader), JobState::Running);
            gate.clients.clear();
            gate.script.then(Call::Sample, Act::Fail(timeout()));
            gate.read();
            let member = gate.enter(spec(Some("g")), Some("group"));
            gate.schedule();
            let failed = format!("waiting for a GPU sample: the last read failed: {}", timeout().text());
            assert_eq!((gate.state(member), gate.reason(member)), (JobState::Queued, Some(failed)));
            // (i) while a read is outstanding past the deadline.
            let mut gate = Gate::readable(serde_json::json!({"gpu_read_deadline_secs": 0.2}));
            let leader = gate.enter(spec(Some("g")), Some("group"));
            gate.schedule();
            assert_eq!(gate.state(leader), JobState::Running);
            gate.clients.clear();
            gate.script.then(Call::Compute, Act::Hold);
            let card = gate.d.card.clone();
            let inflight = std::thread::spawn(move || block(card.compute()));
            let held = block(gate.script.held(Call::Compute));
            std::thread::sleep(Duration::from_millis(250));
            let member = gate.enter(spec(Some("g")), Some("group"));
            gate.schedule();
            assert_eq!(gate.state(member), JobState::Queued);
            assert!(gate.reason(member).unwrap().starts_with("waiting for a GPU read outstanding "));
            gate.script.release(held, Act::Answer);
            gate.publish(inflight.join().unwrap());
            // (j) at a configured start, before the first sample: a group restored running.
            let mut gate = Gate::new(serde_json::json!({}));
            let leader = gate.enter(spec(Some("g")), Some("group"));
            gate.d.with(|s| s.job_mut(SIDE, leader).unwrap().state = JobState::Running);
            let member = gate.enter(spec(Some("g")), Some("group"));
            gate.schedule();
            assert_eq!((gate.state(member), gate.reason(member)), (JobState::Queued, Some("waiting for a GPU sample".into())));
            assert!(gate.none_granted());
        }

        #[test]
        fn an_unanswered_read_refuses_the_old_sample_and_the_next_good_read_admits() {
            let mut gate = Gate::readable(serde_json::json!({}));
            let leader = gate.enter(spec(Some("g")), Some("group"));
            gate.schedule();
            gate.script.then(Call::Sample, Act::Fail(timeout()));
            assert_eq!(gate.read(), Heard::Failed(timeout()));
            let member = gate.enter(spec(Some("g")), Some("group"));
            gate.schedule();
            assert_eq!((gate.state(leader), gate.state(member)), (JobState::Running, JobState::Queued));
            assert_eq!(gate.script.count(Call::Close), 0, "an unanswered read closes nothing");
            assert_eq!(gate.read(), Heard::Read(()), "a read begun after the failure returned");
            gate.schedule();
            assert_eq!(gate.state(member), JobState::Running);
            let mut gate = Gate::readable(serde_json::json!({}));
            gate.script.then(Call::Sample, Act::Fail(timeout()));
            gate.read();
            let job = gate.enter(exclusive("x"), None);
            gate.schedule();
            assert_eq!(gate.state(job), JobState::Queued);
            gate.read();
            gate.schedule();
            assert_eq!(gate.state(job), JobState::Running);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_held_read_inside_the_deadline_never_closes_admission_and_counts_from_its_dispatch() {
            let gate = Gate::new(serde_json::json!({"gpu_read_deadline_secs": 3600}));
            gate.read_now().await;
            gate.script.then(Call::Sample, Act::Hold);
            let dispatched = gate.d.clock.monotonic();
            let read = tokio::spawn({
                let card = gate.d.card.clone();
                async move { card.sample().await }
            });
            let held = gate.script.held(Call::Sample).await;
            assert_eq!(gate.usable(), "card true", "a held read inside the deadline closes nothing");
            gate.script.release(held, Act::Answer);
            let observation = read.await.unwrap();
            assert!(observation.began >= dispatched && observation.began < observation.returned);
            let began = observation.began;
            gate.publish(observation);
            assert_eq!(gate.d.with(|s| readable_began(s)), Some(began), "the sample is valid from its dispatch");
            assert_eq!(gate.usable(), "card true");
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_read_held_past_the_deadline_closes_admission_and_status_until_a_success_or_its_failure_is_applied() {
            let gate = Gate::new(serde_json::json!({"gpu_read_deadline_secs": 0.2}));
            gate.read_now().await;
            for (release, under_the_lock) in [
                (Act::Answer, "card true"),
                (Act::Fail(timeout()), "waiting for gpu-switch to apply a failed GPU read"),
            ] {
                gate.script.then(Call::Sample, Act::Hold);
                gate.script.pause("sample");
                let read = tokio::spawn({
                    let card = gate.d.card.clone();
                    async move { card.sample().await }
                });
                let held = gate.script.held(Call::Sample).await;
                tokio::time::sleep(Duration::from_millis(250)).await;
                let closed = gate.usable();
                assert!(closed.starts_with("waiting for a GPU read outstanding "), "{closed}");
                let telemetry = gate.d.with(|s| owner::telemetry(&gate.d, s));
                assert!(matches!(&telemetry, Telemetry::Unavailable { error } if error.starts_with(&closed)), "{telemetry:?}");
                let (paused, answered) = gate.d.with(|s| {
                    gate.script.release(held, release.clone());
                    let paused = tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(gate.script.paused())
                    });
                    let answered = match owner::usable(&gate.d, s) {
                        Usable::Card(_) => "card true".to_string(),
                        Usable::Unanswered(text) | Usable::Lost(text) => text,
                    };
                    (paused, answered)
                });
                assert_eq!(answered, under_the_lock);
                paused.release();
                gate.publish(read.await.unwrap());
                assert_eq!(gate.usable(), match release {
                    Act::Answer => "card true".to_string(),
                    _ => format!("waiting for a GPU sample: the last read failed: {}", timeout().text()),
                });
            }
        }

        /// A runtime whose one blocking thread a test can occupy.
        fn narrow() -> tokio::runtime::Runtime {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .max_blocking_threads(1)
                .enable_all()
                .build()
                .unwrap()
        }

        /// Occupies the runtime's one blocking thread until the answered sender is used.
        fn occupy(runtime: &tokio::runtime::Runtime) -> std::sync::mpsc::Sender<()> {
            let (occupied, occupying) = std::sync::mpsc::channel();
            let (free, freed) = std::sync::mpsc::channel::<()>();
            runtime.spawn_blocking(move || {
                occupied.send(()).unwrap();
                let _ = freed.recv();
            });
            occupying.recv().unwrap();
            free
        }

        #[test]
        fn a_read_waiting_for_a_blocked_pool_is_outstanding_from_its_dispatch() {
            let gate = Gate::new(serde_json::json!({"gpu_read_deadline_secs": 0.2}));
            let runtime = narrow();
            runtime.block_on(gate.read_now());
            let free = occupy(&runtime);
            let dispatched = gate.d.clock.monotonic();
            let read = runtime.spawn({
                let card = gate.d.card.clone();
                async move { card.sample().await }
            });
            // The task has started and the reader registered its read: the read is outstanding from then.
            let registered = loop {
                match gate.d.card.pending().outstanding {
                    Some(since) => break since,
                    None => std::thread::sleep(Duration::from_millis(1)),
                }
            };
            assert!(registered >= dispatched);
            assert_eq!(gate.script.count(Call::Sample), 1, "registered, its blocking call waits for the pool");
            std::thread::sleep(Duration::from_millis(250).saturating_sub(gate.d.clock.monotonic().saturating_sub(registered)));
            assert_eq!(gate.d.card.pending().outstanding, Some(registered));
            assert_eq!(gate.script.count(Call::Sample), 1, "the read still waits for the pool");
            assert!(gate.usable().starts_with("waiting for a GPU read outstanding "));
            free.send(()).unwrap();
            gate.publish(runtime.block_on(read).unwrap());
            assert_eq!(gate.usable(), "card true");
        }

        #[test]
        fn a_slow_process_walk_keeps_admission_open_and_its_late_publication_is_accepted() {
            let gate = Gate::new(serde_json::json!({"gpu_read_deadline_secs": 0.2}));
            let runtime = narrow();
            runtime.block_on(gate.read_now());
            let first = gate.d.with(|s| readable_began(s)).unwrap();
            gate.d.hooks.before_walk.arm();
            let poller = runtime.spawn(super::super::super::poller::run(gate.d.clone()));
            let reached = runtime.block_on(gate.d.hooks.before_walk.reached());
            let free = occupy(&runtime);
            reached.release();
            std::thread::sleep(Duration::from_millis(250));
            assert_eq!(gate.usable(), "card true", "the walk is no card read");
            free.send(()).unwrap();
            runtime.block_on(gate.d.wait_for(|s| readable_began(s).filter(|began| *began > first)));
            poller.abort();
            let _ = runtime.block_on(poller);
        }

        #[test]
        fn a_failed_sample_is_published_before_the_poller_dispatches_its_walk() {
            let gate = Gate::new(serde_json::json!({}));
            let runtime = narrow();
            runtime.block_on(gate.read_now());
            gate.d.hooks.before_walk.arm();
            gate.script.then(Call::Sample, Act::Fail(timeout()));
            let poller = runtime.spawn(super::super::super::poller::run(gate.d.clone()));
            let reached = runtime.block_on(gate.d.hooks.before_walk.reached());
            assert_eq!(gate.d.with(|s| known(s)), "unread");
            assert!(!gate.d.card.pending().failed, "published and acknowledged before the walk");
            reached.release();
            poller.abort();
            let _ = runtime.block_on(poller);
        }

        #[test]
        fn a_pass_held_at_its_journal_write_asks_the_card_at_the_grant_after_it() {
            for case in ["deadline", "lost", "timeout"] {
                let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
                let mut gate = Gate::new(serde_json::json!({"gpu_read_deadline_secs": 0.2}));
                runtime.block_on(gate.read_now());
                let (place, _client) = enter(&gate.d, SIDE, spec(Some("p")), true, None);
                let ready = gate.enter(spec(Some("r")), None);
                let failure = match case {
                    "deadline" => None,
                    "lost" => Some(lost()),
                    _ => Some(timeout()),
                };
                gate.script.then(Call::Compute, Act::Hold);
                failure.iter().for_each(|_| gate.script.pause("compute"));
                let read = runtime.spawn({
                    let card = gate.d.card.clone();
                    async move { card.compute().await }
                });
                let held = runtime.block_on(gate.script.held(Call::Compute));
                gate.d.hooks.preparation.arm();
                let journal = gate.d.journal.hold();
                let pass = std::thread::spawn({
                    let d = gate.d.clone();
                    move || d.with(|s| schedule(&d, s))
                });
                let reached = runtime.block_on(gate.d.hooks.preparation.reached());
                let (held, paused, refused) = match failure {
                    None => {
                        std::thread::sleep(Duration::from_millis(250));
                        (Some(held), None, "waiting for a GPU read outstanding ")
                    }
                    Some(failure) => {
                        gate.script.release(held, Act::Fail(failure));
                        let paused = runtime.block_on(gate.script.paused());
                        let pending = gate.d.card.pending();
                        assert!(pending.outstanding.is_none() && pending.failed, "{case}: no longer outstanding but pending");
                        (None, Some(paused), "waiting for gpu-switch to apply a failed GPU read")
                    }
                };
                drop(journal);
                reached.release();
                pass.join().unwrap();
                assert!(gate.d.with(|s| s.job(SIDE, place).unwrap().place.as_ref().unwrap().told()), "{case}");
                assert_eq!(gate.state(ready), JobState::Queued, "{case}: the grant after the write is refused");
                assert!(gate.reason(ready).unwrap().starts_with(refused), "{case}: {:?}", gate.reason(ready));
                held.into_iter().for_each(|held| gate.script.release(held, Act::Answer));
                paused.into_iter().for_each(|paused| paused.release());
                gate.publish(runtime.block_on(read).unwrap());
            }
        }

        #[test]
        fn an_unconfigured_daemon_admits_without_a_sample_settles_at_once_and_shows_the_default_card() {
            let (d, _dir) = daemon(serde_json::json!({}));
            let (job, _client) = enter(&d, SIDE, exclusive("a"), false, None);
            d.with(|s| schedule(&d, s));
            assert_eq!(d.with(|s| s.job(SIDE, job).unwrap().state), JobState::Running);
            block(super::super::super::departure::settle(&d, SIDE, job));
            d.with(|s| {
                let job = s.job(SIDE, job).unwrap();
                assert_eq!((job.state, job.end_reason.is_none()), (JobState::Running, true), "settled at once, no kill");
            });
            let journal = std::fs::read_to_string(_dir.path().join("failures.jsonl")).unwrap_or_default();
            assert!(!journal.contains("unclean_release"));
            d.with(|s| finish_run(&d, s, SIDE, job, Some(0), None));
            let (shared, _other) = enter(&d, SIDE, spec(Some("b")), false, None);
            d.with(|s| schedule(&d, s));
            assert_eq!(d.with(|s| s.job(SIDE, shared).unwrap().state), JobState::Running);
            let status = d.with(|s| crate::switch::view::status(&d, s, SIDE, None, d.clock.monotonic()));
            assert_eq!((status.card, status.gpu_processes), (Telemetry::default(), 0));
            assert!(status.summary.starts_with("OFF: serving is off"), "{}", status.summary);
            d.with(|s| finish_run(&d, s, SIDE, shared, Some(0), None));
            let (tx, mut arbiter) = tokio::sync::mpsc::unbounded_channel();
            d.with(|s| {
                s.serve_wanted = true;
                s.arbiter = Some(Arbiter { conn: 1, tx, host_pid: 1 });
                s.ninfer.phase = NPhase::Queued;
                schedule(&d, s);
            });
            assert!(matches!(arbiter.try_recv(), Ok(ArbiterCmd::Start { .. })), "NInfer starts without a sample");
        }

        /// The service in process (PLAN-L L-7, T-12): the real poller, reconcile loop and allocation observer on a
        /// scripted card. Every wait is an event wait.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn the_service_in_process_tells_a_holder_and_a_waiter_of_a_loss_and_grants_again_after_recovery() {
            let gate = Gate::new(serde_json::json!({"gpu_reopen_interval_secs": 0, "startup_idle_secs": 3600}));
            let tasks = [
                tokio::spawn(super::super::super::poller::run(gate.d.clone())),
                tokio::spawn(super::super::run(gate.d.clone())),
                tokio::spawn(super::super::super::startup::run(gate.d.clone())),
            ];
            async fn heard(client: &mut UnboundedReceiver<ClientMsg>, wanted: impl Fn(&ClientMsg) -> bool) -> ClientMsg {
                loop {
                    let msg = client.recv().await.expect("the session's channel");
                    if wanted(&msg) {
                        return msg;
                    }
                }
            }
            let (first, mut holder) = enter(&gate.d, SIDE, exclusive("a"), false, None);
            heard(&mut holder, |msg| matches!(msg, ClientMsg::Granted { .. })).await;
            let (second, mut waiter) = enter(&gate.d, SIDE, exclusive("b"), false, None);
            gate.script.always(Call::Sample, Act::Fail(lost()));
            let ClientMsg::DeviceLost { device_lost } = heard(&mut holder, |msg| matches!(msg, ClientMsg::DeviceLost { .. })).await else {
                unreachable!()
            };
            assert_eq!(device_lost.run, 1);
            let reason = device_lost.loss.reason();
            heard(&mut waiter, |msg| matches!(msg, ClientMsg::Waiting { waiting } if *waiting == reason)).await;
            let status = gate.d.with(|s| crate::switch::view::status(&gate.d, s, SIDE, None, gate.d.clock.monotonic()));
            assert!(status.summary.starts_with("GPU LOST, found at "), "{}", status.summary);
            assert_eq!(status.device_lost, Some(device_lost.loss));
            gate.script.always(Call::Sample, Act::Answer);
            gate.d.wait_for(|s| (known(s) == "readable").then_some(())).await;
            crate::switch::departure::settle(&gate.d, SIDE, first).await;
            gate.d.update(|s| {
                drop(s.job_mut(SIDE, first).unwrap().final_report());
                finish_run(&gate.d, s, SIDE, first, None, None)
            });
            heard(&mut waiter, |msg| matches!(msg, ClientMsg::Granted { .. })).await;
            assert_eq!(gate.state(second), JobState::Running);
            tasks.iter().for_each(tokio::task::JoinHandle::abort);
        }

        /// Startup during a loss, persistence and recovery (PLAN-L §3.5, §3.6; T-5, T-8, T-16 (c)).
        mod restore {
            use super::*;
            use crate::process_identity::Instance;

            fn successor(gate: &Gate, config: serde_json::Value, incarnation: Instance) -> Gate {
                let script = Script::new();
                let mut config = config;
                config["state_dir"] = serde_json::json!(gate.d.state_dir().expect("a state directory"));
                let (d, dir) = reading(config, Some(script.driver()), incarnation);
                crate::switch::persist::restore(&d).expect("the saved state restores");
                Gate { d, script, _dir: dir, clients: Vec::new() }
            }

            fn another_boot() -> Instance {
                Instance { boot_id: "another boot".into(), ..incarnation(9) }
            }

            fn records(dir: &std::path::Path, event: &str) -> Vec<serde_json::Value> {
                std::fs::read_to_string(dir.join("failures.jsonl"))
                    .unwrap_or_default()
                    .lines()
                    .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                    .filter(|record| record["event"] == event)
                    .collect()
            }

            fn identity(gate: &Gate, id: u64) -> Vec<(u64, u64)> {
                gate.d.with(|s| {
                    s.job(SIDE, id).unwrap().device_lost.iter().map(|grant| (grant.loss.incarnation.start_ticks, grant.loss.number)).collect()
                })
            }

            #[test]
            fn a_configured_nvml_that_cannot_open_is_a_lost_card_and_the_daemon_runs() {
                let (d, _dir) = daemon(serde_json::json!({"nvml": "/nonexistent/libnvidia-ml.so.1"}));
                let heard = d.observe(|s| owner::observe(&d, s, block(d.card.sample()))).map_value();
                assert!(matches!(&heard, Heard::Failed(crate::gpu::Failure::Lost(text)) if text.starts_with("nvmlInit_v2")), "{heard:?}");
                let (job, _client) = enter(&d, SIDE, exclusive("a"), false, None);
                d.with(|s| schedule(&d, s));
                let reason = d.with(|s| s.job(SIDE, job).unwrap().reason.clone()).unwrap();
                assert!(reason.starts_with("the GPU was found lost at "), "{reason}");
                let status = d.with(|s| crate::switch::view::status(&d, s, SIDE, None, d.clock.monotonic()));
                assert!(status.summary.starts_with("GPU LOST, found at "), "the summary names the loss: {}", status.summary);
                assert!(status.summary.ends_with("NInfer returns once it is empty") || status.summary.contains("OFF: "), "{}", status.summary);
                assert_eq!(status.device_lost, d.with(|s| s.view.card.loss().cloned()));
                assert!(matches!(&status.card, Telemetry::Unavailable { error } if error.starts_with("found lost at ")), "{:?}", status.card);
            }

            /// The role of the recovery test above, whose stderr its parent counts: twenty polls of a lost card that
            /// answers as another device.
            #[test]
            #[ignore]
            fn a_lost_card_that_answers_as_another_device() {
                if std::env::var_os("GQ_IDENTITY_ROLE").is_none_or(|role| role.is_empty()) {
                    return;
                }
                let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
                let gate = Gate::new(serde_json::json!({"gpu_reopen_interval_secs": 0}));
                runtime.block_on(gate.read_now());
                gate.script.then(Call::Sample, Act::Fail(lost()));
                runtime.block_on(gate.read_now());
                gate.script.identity(&["GPU-b"]);
                for _ in 0..20 {
                    runtime.block_on(gate.read_now());
                }
                assert_eq!(gate.d.with(|s| known(s)), "lost");
            }

            #[test]
            fn an_absent_card_on_the_real_reader_admits_no_exclusive_job() {
                let mock = crate::switch::control::tests::StateDir::new();
                let role = crate::roles::Role::start(
                    "switch::daemon::tests::gate::restore::the_real_reader_on_an_absent_card",
                    &[("NINFER_MOCK_GPU_DIR", mock.path().as_os_str())],
                );
                assert!(role.wait().success());
            }

            /// The role of the test above, in its own process: the mock's control directory is process-wide.
            #[test]
            #[ignore]
            fn the_real_reader_on_an_absent_card() {
                let Some(mock) = std::env::var_os("NINFER_MOCK_GPU_DIR").map(std::path::PathBuf::from) else {
                    return;
                };
                std::fs::write(mock.join("absent"), "").unwrap();
                let (d, _state) = daemon(serde_json::json!({"nvml": crate::roles::nvml_mock()}));
                let heard = d.observe(|s| owner::observe(&d, s, block(d.card.sample()))).map_value();
                assert_eq!(heard, Heard::Failed(crate::gpu::Failure::Lost("NVML reports no GPU".into())));
                let (job, _client) = enter(&d, SIDE, exclusive("a"), false, None);
                d.with(|s| schedule(&d, s));
                assert_eq!(d.with(|s| s.job(SIDE, job).unwrap().state), JobState::Queued);
            }

            #[test]
            fn a_healthy_configured_start_admits_nothing_before_its_first_sample_and_admits_on_it() {
                let mut gate = Gate::new(serde_json::json!({}));
                let job = gate.enter(exclusive("a"), None);
                gate.schedule();
                assert_eq!((gate.state(job), gate.reason(job)), (JobState::Queued, Some("waiting for a GPU sample".into())));
                gate.read();
                gate.schedule();
                assert_eq!(gate.state(job), JobState::Running);
            }

            #[test]
            fn a_restored_running_lease_is_marked_by_a_lost_first_read_and_a_restored_loss_keeps_its_one_association() {
                let mut before = Gate::readable(serde_json::json!({}));
                let lease = before.enter(spec(Some("l")), None);
                before.schedule();
                crate::switch::persist::save(&before.d);
                // (c) a restored running lease and a lost first read.
                let after = successor(&before, serde_json::json!({}), incarnation(2));
                assert_eq!(after.d.with(|s| known(s)), "unread");
                after.lose();
                assert_eq!(identity(&after, lease), [(2, 1)]);
                // (d) a saved loss of this boot restores lost, with its identity and times.
                crate::switch::persist::save(&after.d);
                let loss = after.d.with(|s| s.view.card.loss().cloned()).unwrap();
                let third = successor(&after, serde_json::json!({}), incarnation(3));
                assert_eq!(third.d.with(|s| s.view.card.loss().cloned()), Some(loss.clone()));
                third.script.then(Call::Sample, Act::Fail(lost()));
                assert_eq!(third.read(), Heard::Failed(lost()));
                assert_eq!(identity(&third, lease), [(2, 1)], "the restored run keeps its one association");
                // T-16 (c): compute before the first open answers the restored loss.
                let fourth = successor(&third, serde_json::json!({}), incarnation(4));
                let early = block(fourth.d.card.compute());
                assert_eq!(early.generation, 0);
                assert_eq!(fourth.publish(early), Heard::Failed(crate::gpu::Failure::Lost(loss.cause.clone())));
            }

            #[test]
            fn an_unconfigured_start_restores_nothing_and_a_new_boot_starts_unread() {
                let before = Gate::readable(serde_json::json!({}));
                before.lose();
                crate::switch::persist::save(&before.d);
                // (f) the same boot with no NVML: unconfigured, and an exclusive job runs without a sample.
                let (d, _dir) = reading(serde_json::json!({"state_dir": before._dir.path()}), None, incarnation(2));
                crate::switch::persist::restore(&d).expect("the saved state restores");
                let (job, _client) = enter(&d, SIDE, exclusive("a"), false, None);
                d.with(|s| schedule(&d, s));
                assert_eq!(d.with(|s| (known(s), s.job(SIDE, job).unwrap().state)), ("unconfigured", JobState::Running));
                // (g) a new boot: unread, then admission on the first sample.
                let mut after = successor(&before, serde_json::json!({}), another_boot());
                assert_eq!(after.d.with(|s| known(s)), "unread");
                let job = after.enter(exclusive("b"), None);
                after.read();
                after.schedule();
                assert_eq!(after.state(job), JobState::Running);
            }

            #[test]
            fn a_loss_takes_the_short_save_and_an_older_snapshot_restores_its_jobs_unread() {
                let mut gate = Gate::readable(serde_json::json!({}));
                let lease = gate.enter(spec(Some("l")), None);
                gate.schedule();
                let (mut kept, _) = gate.d.unsaved();
                kept.borrow_and_update();
                gate.lose();
                assert!(kept.has_changed().unwrap(), "the transition requested the short save");
                crate::switch::persist::save(&gate.d);
                let after = successor(&gate, serde_json::json!({}), incarnation(2));
                assert_eq!(after.d.with(|s| s.view.card.loss().cloned()), gate.d.with(|s| s.view.card.loss().cloned()));
                // A snapshot of a gpu-switch before card losses: no card, no associations, no report flag.
                let file = crate::switch::persist::path(gate._dir.path());
                let mut old: serde_json::Value = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
                old.as_object_mut().unwrap().remove("card");
                for side in old["sides"].as_array_mut().unwrap() {
                    for job in side["jobs"].as_array_mut().unwrap() {
                        job.as_object_mut().unwrap().remove("device_lost");
                        job["police"].as_object_mut().unwrap().remove("reported");
                    }
                }
                std::fs::write(&file, serde_json::to_vec(&old).unwrap()).unwrap();
                let older = successor(&gate, serde_json::json!({}), incarnation(3));
                assert_eq!(older.d.with(|s| known(s)), "unread");
                assert_eq!(older.state(lease), JobState::Running, "its jobs restore");
            }

            /// The saver of `gate`'s daemon, first run after everything the test published: polled once, then again
            /// once the short debounce has passed. What the state file then holds.
            fn saved_at_the_short_debounce(gate: &Gate) -> serde_json::Value {
                use std::future::Future;
                let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                runtime.block_on(async {
                    let (_stop, stopped) = tokio::sync::oneshot::channel();
                    let mut saver = std::pin::pin!(crate::switch::persist::run(gate.d.clone(), stopped));
                    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
                    assert!(saver.as_mut().poll(&mut cx).is_pending(), "the saver waits for its debounce");
                    tokio::time::sleep(crate::switch::persist::DEBOUNCE).await;
                    assert!(saver.as_mut().poll(&mut cx).is_pending(), "the saver saved and waits again");
                });
                let file = crate::switch::persist::path(gate.d.state_dir().unwrap());
                serde_json::from_slice(&std::fs::read(file).expect("the saver saved the state")).unwrap()
            }

            fn saved_loss(gate: &Gate) -> serde_json::Value {
                serde_json::to_value(gate.d.with(|s| s.view.card.loss().cloned()).unwrap()).unwrap()
            }

            #[test]
            fn a_first_read_loss_published_before_the_saver_first_runs_takes_the_short_save() {
                let gate = Gate::new(serde_json::json!({}));
                gate.lose();
                assert_eq!(saved_at_the_short_debounce(&gate)["card"]["loss"], saved_loss(&gate));
            }

            #[test]
            fn a_same_boot_restore_then_a_first_read_loss_before_the_saver_first_runs_takes_the_short_save() {
                let mut before = Gate::readable(serde_json::json!({}));
                let lease = before.enter(spec(Some("l")), None);
                before.schedule();
                crate::switch::persist::save(&before.d);
                let after = successor(&before, serde_json::json!({}), incarnation(2));
                assert_eq!(after.d.with(|s| known(s)), "unread");
                after.lose();
                let saved = saved_at_the_short_debounce(&after);
                assert_eq!(saved["card"]["loss"], saved_loss(&after));
                let handle = after.d.with(|s| s.job(SIDE, lease).unwrap().handle.clone());
                let sides = saved["sides"].as_array().unwrap();
                let jobs = sides.iter().flat_map(|side| side["jobs"].as_array().unwrap());
                let job = jobs.into_iter().find(|job| job["handle"] == handle.as_str()).unwrap();
                assert_eq!(job["device_lost"][0]["loss"], saved_loss(&after), "the restored run's mark is saved");
            }

            #[test]
            fn a_crash_before_the_snapshot_publishes_a_new_loss_of_the_new_incarnation() {
                let mut first = Gate::readable(serde_json::json!({}));
                let lease = first.enter(spec(Some("l")), None);
                first.schedule();
                crate::switch::persist::save(&first.d);
                let file = crate::switch::persist::path(first._dir.path());
                let before = std::fs::read(&file).unwrap();
                first.lose();
                assert_eq!(identity(&first, lease), [(1, 1)]);
                std::fs::write(&file, before).unwrap();
                let second = successor(&first, serde_json::json!({}), incarnation(2));
                second.lose();
                assert_eq!(identity(&second, lease), [(2, 1)]);
                let pairs: Vec<(u64, u64)> = records(first._dir.path(), "card_lost")
                    .iter()
                    .map(|record| (record["loss"]["incarnation"]["start_ticks"].as_u64().unwrap(), record["loss"]["number"].as_u64().unwrap()))
                    .collect();
                assert_eq!(pairs, [(1, 1), (2, 1)], "the journal holds both pairs");
            }

            #[test]
            fn an_association_survives_an_unconfigured_start_and_a_later_incarnation_counts_from_one() {
                let mut first = Gate::readable(serde_json::json!({}));
                let lease = first.enter(spec(Some("l")), None);
                first.schedule();
                first.lose();
                crate::switch::persist::save(&first.d);
                let (unconfigured, _dir) = reading(serde_json::json!({"state_dir": first._dir.path()}), None, incarnation(2));
                crate::switch::persist::restore(&unconfigured).expect("the saved state restores");
                let kept = unconfigured.with(|s| s.job(SIDE, lease).unwrap().device_lost.clone());
                assert_eq!(kept.len(), 1, "the job record keeps it");
                crate::switch::persist::save(&unconfigured);
                let third = successor(&first, serde_json::json!({}), incarnation(3));
                assert_eq!(identity(&third, lease), [(1, 1)], "and so does the snapshot");
                third.read();
                third.d.with(|s| finish_run(&third.d, s, SIDE, lease, Some(0), None));
                third.read();
                let mut third = third;
                let next = third.enter(spec(Some("n")), None);
                third.schedule();
                assert_eq!(third.state(next), JobState::Running);
                third.lose();
                assert_eq!(identity(&third, next), [(3, 1)], "a loss under I3 is (I3, 1)");
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn the_card_is_read_again_on_the_same_identity_and_stays_lost_on_another_or_an_unknown_answer() {
                let mut gate = Gate::new(serde_json::json!({"gpu_reopen_interval_secs": 0}));
                gate.read_now().await;
                let (_running, _client) = enter(&gate.d, SIDE, spec(Some("r")), false, None);
                gate.schedule();
                gate.d.with(|s| finish_run(&gate.d, s, SIDE, _running, Some(0), None));
                let stale = {
                    gate.script.then(Call::Compute, Act::Hold);
                    let card = gate.d.card.clone();
                    let held = tokio::spawn(async move { card.compute().await });
                    (held, gate.script.held(Call::Compute).await)
                };
                gate.script.then(Call::Sample, Act::Fail(lost()));
                gate.read_now().await;
                let job = gate.enter(exclusive("q"), None);
                gate.script.release(stale.1, Act::Answer);
                let stale = stale.0.await.unwrap();
                assert_eq!(gate.read_now().await, Heard::Read(()), "re-opened on the same identity");
                assert_eq!(records(gate._dir.path(), "card_read_again").len(), 1);
                gate.schedule();
                assert_eq!(gate.state(job), JobState::Running, "admitted on the recovery sample, with no second");
                assert_eq!(gate.publish(stale), Heard::Superseded, "a success of the retired generation");
                assert_eq!(gate.d.with(|s| known(s)), "readable");
                // Another identity, or an unknown answer, keeps the card lost; each change of the error is logged once.
                let log = crate::switch::control::tests::StateDir::new();
                let role = crate::roles::Role::logged(
                    "switch::daemon::tests::gate::restore::a_lost_card_that_answers_as_another_device",
                    &[("GQ_IDENTITY_ROLE", std::ffi::OsStr::new("1"))],
                    &log.path().join("role.log"),
                );
                assert!(role.wait().success());
                let written = std::fs::read_to_string(log.path().join("role.log")).unwrap();
                assert_eq!(written.matches("gpu-switch: the GPU is still lost: the GPU answers as another device").count(), 1, "{written}");
                for answer in ["another identity", "unknown"] {
                    let gate = Gate::new(serde_json::json!({"gpu_reopen_interval_secs": 0}));
                    gate.read_now().await;
                    gate.script.then(Call::Sample, Act::Fail(lost()));
                    gate.read_now().await;
                    match answer {
                        "another identity" => gate.script.identity(&["GPU-b"]),
                        _ => gate.script.always(Call::Open, Act::Fail(unknown())),
                    }
                    for _ in 0..20 {
                        gate.read_now().await;
                        assert_eq!(gate.d.with(|s| known(s)), "lost", "{answer}");
                    }
                    assert_eq!(gate.d.with(|s| next_loss(s)), 2, "{answer}: no second loss");
                    let latest = gate.d.with(|s| crate::switch::card::tests::latest(s));
                    match answer {
                        "another identity" => assert!(latest.starts_with("the GPU answers as another device"), "{latest}"),
                        _ => assert_eq!(latest, unknown().text()),
                    }
                }
            }
        }

        /// Marking the open grants at a loss (PLAN-L §3.4; T-6, T-7, T-15, T-17 (a)).
        mod marks {
            use super::*;
            use crate::protocol::{GrantLoss, RunnerEvent, Rule};
            use crate::switch::control::tests::submit;

            fn journal(gate: &Gate) -> Vec<serde_json::Value> {
                std::fs::read_to_string(gate._dir.path().join("failures.jsonl"))
                    .unwrap_or_default()
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect()
            }

            fn events<'a>(records: &'a [serde_json::Value], event: &str) -> Vec<&'a serde_json::Value> {
                records.iter().filter(|record| record["event"] == event).collect()
            }

            fn associations(gate: &Gate, id: u64) -> Vec<GrantLoss> {
                gate.d.with(|s| s.job(SIDE, id).unwrap().device_lost.clone())
            }

            #[test]
            fn a_loss_marks_each_open_grant_once_with_its_run_and_tells_its_holder() {
                let mut gate = Gate::readable(serde_json::json!({}));
                let lease = gate.enter(spec(Some("l")), None);
                let (runner, mut events_of_runner) = submit(&gate.d, SIDE, spec(Some("r")));
                gate.schedule();
                let stopping = gate.enter(spec(Some("s")), None);
                gate.schedule();
                gate.d.with(|s| s.job_mut(SIDE, stopping).unwrap().state = JobState::Stopping);
                let queued = gate.enter(exclusive("q"), None);
                gate.schedule();
                assert_eq!((gate.state(lease), gate.state(runner), gate.state(queued)), (JobState::Running, JobState::Running, JobState::Queued));
                gate.lose();
                let loss = gate.d.with(|s| s.view.card.loss().cloned()).unwrap();
                assert_eq!((loss.number, &loss.incarnation), (1, &incarnation(1)));
                assert!(loss.last_good_began.is_some_and(|good| good <= loss.read_began));
                assert!(loss.read_began <= loss.observed && loss.observed <= loss.published);
                for id in [lease, runner] {
                    assert_eq!(associations(&gate, id), [GrantLoss { run: 1, loss: loss.clone() }]);
                }
                for id in [stopping, queued] {
                    assert!(associations(&gate, id).is_empty(), "a stopping and a queued job hold no grant");
                }
                let heard: Vec<ClientMsg> = std::iter::from_fn(|| gate.clients[0].try_recv().ok()).collect();
                assert!(heard.iter().any(|msg| matches!(msg, ClientMsg::DeviceLost { device_lost } if device_lost.loss == loss)));
                let told: Vec<RunnerEvent> = std::iter::from_fn(|| events_of_runner.try_recv().ok()).collect();
                assert!(told.iter().any(|event| matches!(event, RunnerEvent::DeviceLost { device_lost } if device_lost.run == 1)));
                let own = gate.d.with(|s| crate::switch::view::own_job_of(&gate.d.clock, s, SIDE, s.job(SIDE, lease).unwrap()));
                assert_eq!(own.device_lost.len(), 1);
                assert_eq!(own.warnings("holder").last().unwrap(), &loss.notice("holder", 1));
                let records = journal(&gate);
                assert_eq!(events(&records, "card_lost").len(), 1);
                assert_eq!(events(&records, "card_lost")[0]["marked"].as_array().unwrap().len(), 2);
                assert_eq!(events(&records, "device_lost").len(), 2, "one per affected run");
                gate.d.with(|s| {
                    let job = s.job_mut(SIDE, lease).unwrap();
                    assert!(job.police.concluded);
                    crate::switch::contract::violated(&gate.d, job, Rule::NoVram);
                    assert!(job.violations.is_empty(), "no rule is judged over a span a loss ended");
                });
                // The associations survive a save and a restore, and a reattached holder hears them again.
                crate::switch::persist::save(&gate.d);
                let (successor, _other) = reading(
                    serde_json::json!({"state_dir": gate._dir.path()}),
                    Some(Script::new().driver()),
                    incarnation(2),
                );
                crate::switch::persist::restore(&successor).expect("the saved state restores");
                let (tx, mut reattached) = tokio::sync::mpsc::unbounded_channel();
                successor.with(|s| {
                    let job = s.job_mut(SIDE, lease).unwrap();
                    assert_eq!(job.device_lost, [GrantLoss { run: 1, loss: loss.clone() }]);
                    job.conn = Conn::Client(tx);
                    job.replay_notices();
                });
                assert!(matches!(reattached.try_recv(), Ok(ClientMsg::DeviceLost { device_lost }) if device_lost.loss == loss));
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn a_departure_that_meets_the_lost_card_concludes_without_a_kill() {
                for released in [false, true] {
                    let mut gate = Gate::new(serde_json::json!({}));
                    gate.read_now().await;
                    let mut child = std::process::Command::new("sleep").arg("30").spawn().unwrap();
                    let id = gate.enter(spec(Some("l")), None);
                    gate.d.with(|s| {
                        let job = s.job_mut(SIDE, id).unwrap();
                        job.pid_host = Some(child.id());
                        job.process_started = crate::procs::started(child.id());
                    });
                    gate.schedule();
                    assert_eq!(gate.state(id), JobState::Running);
                    if released {
                        gate.d.with(|s| drop(s.job_mut(SIDE, id).unwrap().final_report()));
                    }
                    gate.script.then(Call::Compute, Act::Fail(lost()));
                    crate::switch::departure::settle(&gate.d, SIDE, id).await;
                    let records = journal(&gate);
                    assert!(events(&records, "unclean_release").is_empty(), "released {released}");
                    assert!(gate.d.with(|s| s.job(SIDE, id).unwrap().end_reason.is_none()), "no Killed end");
                    assert!(child.try_wait().unwrap().is_none(), "the child was not killed");
                    let lost = events(&records, "card_lost");
                    match released {
                        false => assert_eq!(associations(&gate, id).len(), 1, "after EOF the grant is marked"),
                        true => {
                            assert!(associations(&gate, id).is_empty(), "the grant ended at its final report");
                            assert_eq!(lost[0]["released"].as_array().unwrap().len(), 1, "listed as released");
                        }
                    }
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn each_run_of_a_cooperative_job_takes_its_own_association_and_an_open_grant_keeps_its_first() {
                let gate = Gate::new(serde_json::json!({"gpu_reopen_interval_secs": 0}));
                gate.read_now().await;
                let (cooperative, _runner) = submit(&gate.d, SIDE, JobSpec { cooperative: true, ..spec(Some("c")) });
                let (steady, _other) = submit(&gate.d, SIDE, spec(Some("s")));
                gate.schedule();
                gate.script.then(Call::Sample, Act::Fail(lost()));
                gate.read_now().await;
                gate.d.with(|s| finish_run(&gate.d, s, SIDE, cooperative, Some(76), None));
                assert_eq!(gate.state(cooperative), JobState::Queued, "it yielded and is re-queued");
                assert_eq!(gate.read_now().await, Heard::Read(()), "recovered");
                gate.schedule();
                assert_eq!(gate.state(cooperative), JobState::Running);
                gate.script.then(Call::Sample, Act::Fail(lost()));
                gate.read_now().await;
                let numbers = |id| associations(&gate, id).iter().map(|grant| (grant.run, grant.loss.number)).collect::<Vec<_>>();
                assert_eq!(numbers(cooperative), [(1, 1), (2, 2)], "two runs, two losses of one incarnation");
                assert_eq!(numbers(steady), [(1, 1)], "one grant open across two losses keeps its first");
                let records = journal(&gate);
                let further = events(&records, "device_lost")
                    .into_iter()
                    .filter(|record| record["handle"] == gate.d.with(|s| s.job(SIDE, steady).unwrap().handle.clone()))
                    .count();
                assert_eq!(further, 2, "the further interval is journalled for the open grant");
                assert_eq!(events(&records, "card_lost")[1]["already_marked"].as_array().unwrap().len(), 1);
            }

            #[test]
            fn a_panel_loss_seen_by_a_sample_and_an_absent_card_seen_by_departure_each_mark_the_open_grant() {
                let mock = crate::switch::control::tests::StateDir::new();
                let role = crate::roles::Role::start(
                    "switch::daemon::tests::gate::marks::the_real_reader_marks_on_the_mock",
                    &[("NINFER_MOCK_GPU_DIR", mock.path().as_os_str())],
                );
                assert!(role.wait().success());
            }

            /// The role of the test above, in its own process: the mock's control directory is process-wide.
            #[test]
            #[ignore]
            fn the_real_reader_marks_on_the_mock() {
                let Some(mock) = std::env::var_os("NINFER_MOCK_GPU_DIR").map(std::path::PathBuf::from) else {
                    return;
                };
                let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
                for (control, text, departure) in [("fail", "nvmlDeviceGetName", false), ("absent", "", true)] {
                    let (d, _state) = daemon(serde_json::json!({"nvml": crate::roles::nvml_mock()}));
                    let publish = |observation| d.observe(|s| owner::observe(&d, s, observation)).map_value();
                    assert_eq!(publish(runtime.block_on(d.card.sample())), Heard::Read(()));
                    let (id, _client) = enter(&d, SIDE, spec(Some("m")), false, None);
                    d.with(|s| schedule(&d, s));
                    assert_eq!(d.with(|s| s.job(SIDE, id).unwrap().state), JobState::Running);
                    std::fs::write(mock.join(control), text).unwrap();
                    match departure {
                        false => drop(publish(runtime.block_on(d.card.sample()))),
                        true => runtime.block_on(crate::switch::departure::settle(&d, SIDE, id)),
                    }
                    let marked = d.with(|s| s.job(SIDE, id).unwrap().device_lost.clone());
                    assert_eq!(marked.len(), 1, "{control}: {marked:?}");
                    std::fs::remove_file(mock.join(control)).unwrap();
                }
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn the_marks_and_the_save_request_precede_the_close_of_the_lost_handle() {
                let gate = Gate::new(serde_json::json!({"gpu_reopen_interval_secs": 0}));
                gate.read_now().await;
                let (lease, _client) = enter(&gate.d, SIDE, spec(Some("l")), false, None);
                gate.schedule();
                let (mut kept, _) = gate.d.unsaved();
                kept.borrow_and_update();
                gate.script.then(Call::Sample, Act::Fail(lost()));
                gate.script.then(Call::Close, Act::Hold);
                gate.read_now().await;
                let closing = tokio::spawn({
                    let card = gate.d.card.clone();
                    async move { card.sample().await }
                });
                let held = gate.script.held(Call::Close).await;
                assert_eq!(associations(&gate, lease).len(), 1, "marked before the close");
                let status = gate.d.with(|s| crate::switch::view::status(&gate.d, s, SIDE, None, gate.d.clock.monotonic()));
                assert!(status.summary.starts_with("GPU LOST, found at "), "the summary before the close: {}", status.summary);
                assert!(kept.has_changed().unwrap(), "the short save was requested before the close");
                assert_eq!(gate.script.count(Call::Open), 1, "no open while the close is held");
                gate.script.release(held, Act::Answer);
                assert_eq!(closing.await.unwrap().generation, 2);
            }
        }

        /// Publication races (PLAN-L §3.3, T-16, T-17 (d)): real observations, completed in a forced order by the
        /// scripted reader's barrier and published in a forced order by the completion pause.
        mod races {
            use super::*;
            use crate::switch::card_reader::tests::Paused;
            use crate::switch::card_reader::{Answer, Processes};
            use crate::gpu::{Failure, Sample};

            async fn compute(gate: &Gate, act: Act) -> (tokio::task::JoinHandle<Observation<Processes>>, Paused) {
                gate.script.then(Call::Compute, act);
                gate.script.pause("compute");
                let read = tokio::spawn({
                    let card = gate.d.card.clone();
                    async move { card.compute().await }
                });
                (read, gate.script.paused().await)
            }

            async fn sample(gate: &Gate, act: Act) -> (tokio::task::JoinHandle<Observation<Sample>>, Paused) {
                gate.script.then(Call::Sample, act);
                gate.script.pause("sample");
                let read = tokio::spawn({
                    let card = gate.d.card.clone();
                    async move { card.sample().await }
                });
                (read, gate.script.paused().await)
            }

            async fn taken<T>(read: (tokio::task::JoinHandle<Observation<T>>, Paused)) -> Observation<T> {
                read.1.release();
                read.0.await.unwrap()
            }

            fn loss_count(gate: &Gate) -> u64 {
                gate.d.with(|s| s.view.card.loss().map_or(0, |loss| loss.number))
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn a_sample_and_a_later_failure_in_each_publication_order_end_closed() {
                for (first, failure) in [("sample", timeout()), ("failure", timeout()), ("failure", lost())] {
                    let gate = Gate::new(serde_json::json!({}));
                    gate.read_now().await;
                    let success = taken(sample(&gate, Act::Answer).await).await;
                    let failed = taken(compute(&gate, Act::Fail(failure.clone())).await).await;
                    assert!(failed.returned > success.began);
                    match first {
                        "sample" => {
                            assert_eq!(gate.publish(success), Heard::Read(()));
                            assert_eq!(gate.d.with(|s| known(s)), "readable");
                            gate.publish(failed);
                        }
                        _ => {
                            gate.publish(failed);
                            let heard = gate.publish(success);
                            let expected = match failure {
                                Failure::Lost(_) => Heard::Failed(failure.clone()),
                                Failure::Unanswered(_) => Heard::Superseded,
                            };
                            assert_eq!(heard, expected);
                        }
                    }
                    assert!(matches!(gate.d.with(|s| known(s)), "unread" | "lost"), "both orders end closed");
                    assert_ne!(gate.usable(), "card true");
                }
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn a_loss_seen_first_by_compute_discards_the_sample_that_held_the_handle_and_recovers_only_on_a_new_generation() {
                let gate = Gate::new(serde_json::json!({"gpu_reopen_interval_secs": 0}));
                gate.read_now().await;
                gate.script.then(Call::Sample, Act::Hold);
                let holding = tokio::spawn({
                    let card = gate.d.card.clone();
                    async move { card.sample().await }
                });
                let held = gate.script.held(Call::Sample).await;
                let (read, paused) = compute(&gate, Act::Fail(lost())).await;
                paused.release();
                gate.publish(read.await.unwrap());
                gate.script.release(held, Act::Answer);
                let stale = holding.await.unwrap();
                assert_eq!(stale.generation, 1);
                assert_eq!(gate.publish(stale), Heard::Failed(lost()), "its later success is discarded");
                assert_eq!((gate.d.with(|s| known(s)), gate.script.count(Call::Close)), ("lost", 0), "the held read kept it open");
                let after = gate.d.card.sample().await;
                assert_eq!(after.generation, 2, "a sample after the retirement reads a new generation, never the retired one");
                let order = gate.script.order();
                assert_eq!(order[order.len() - 3..], [Call::Close, Call::Open, Call::Sample], "the next due sample closes, then opens");
                assert_eq!(gate.publish(after), Heard::Read(()));
                assert_eq!(gate.d.with(|s| known(s)), "readable");
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn compute_before_the_first_open_is_discarded_and_certifies_nothing() {
                let gate = Gate::new(serde_json::json!({}));
                let early = gate.d.card.compute().await;
                assert_eq!(early.generation, 0);
                assert_eq!(gate.publish(early), Heard::Superseded);
                gate.script.then(Call::Open, Act::Hold);
                let opening = tokio::spawn({
                    let card = gate.d.card.clone();
                    async move { card.sample().await }
                });
                let held = gate.script.held(Call::Open).await;
                assert_eq!(gate.publish(gate.d.card.compute().await), Heard::Superseded);
                assert_eq!(gate.d.with(|s| known(s)), "unread");
                gate.script.release(held, Act::Answer);
                gate.publish(opening.await.unwrap());
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn a_second_failure_of_an_applied_loss_keeps_admission_closed_through_the_recovery_until_discarded() {
                let gate = Gate::new(serde_json::json!({"gpu_reopen_interval_secs": 0}));
                gate.read_now().await;
                gate.script.then(Call::Compute, Act::Hold);
                let second = tokio::spawn({
                    let card = gate.d.card.clone();
                    async move { card.compute().await }
                });
                let held = gate.script.held(Call::Compute).await;
                gate.script.then(Call::Compute, Act::Fail(lost()));
                gate.publish(gate.d.card.compute().await);
                gate.script.pause("compute");
                gate.script.release(held, Act::Fail(timeout()));
                let paused = gate.script.paused().await;
                assert_eq!(gate.read_now().await, Heard::Read(()), "every call on the handle returned: close, re-open");
                assert_eq!(gate.usable(), "waiting for gpu-switch to apply a failed GPU read");
                paused.release();
                assert_eq!(gate.publish(second.await.unwrap()), Heard::Superseded);
                assert_eq!((gate.d.with(|s| known(s)), gate.usable()), ("readable", "card true".to_string()));
                assert_eq!(loss_count(&gate), 0, "recovered: no loss stands");
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn two_failures_and_a_sample_published_b_a_s_discard_the_sample() {
                let gate = Gate::new(serde_json::json!({}));
                gate.read_now().await;
                let a = compute(&gate, Act::Fail(timeout())).await;
                let s = sample(&gate, Act::Answer).await;
                gate.script.then(Call::Compute, Act::Hold);
                let b = tokio::spawn({
                    let card = gate.d.card.clone();
                    async move { card.compute().await }
                });
                let held = gate.script.held(Call::Compute).await;
                gate.script.release(held, Act::Fail(timeout()));
                let b = b.await.unwrap();
                let (a, s) = (taken(a).await, taken(s).await);
                assert!(a.returned < s.began && s.began < b.returned);
                gate.publish(b);
                gate.publish(a);
                assert_eq!(gate.publish(s), Heard::Superseded);
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn a_loss_and_its_replay_in_either_order_publish_one_loss_with_the_originals_times() {
                for replay_first in [true, false] {
                    let gate = Gate::new(serde_json::json!({}));
                    gate.read_now().await;
                    let poll = sample(&gate, Act::Answer).await;
                    let original = taken(compute(&gate, Act::Fail(lost())).await).await;
                    let replay = gate.d.card.compute().await;
                    assert_eq!((replay.read, replay.began, replay.returned), (original.read, original.began, original.returned));
                    let (first, second) = match replay_first {
                        true => (replay, original.clone()),
                        false => (original.clone(), replay),
                    };
                    gate.publish(first);
                    gate.publish(second);
                    let loss = gate.d.with(|s| s.view.card.loss().cloned()).unwrap();
                    assert_eq!((loss.number, loss.read_began, loss.observed), (1, original.began_unix, original.returned_unix));
                    assert_eq!(gate.d.card.registered(), 0, "no pending entry remains");
                    gate.publish(taken(poll).await);
                    assert_eq!(gate.d.with(|s| next_loss(s)), 2, "one loss in each run");
                }
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn a_late_unanswered_read_of_a_lost_generation_never_replaces_the_loss_or_the_recovery() {
                for run in ["unanswered first", "loss first", "held through the recovery"] {
                    let gate = Gate::new(serde_json::json!({"gpu_reopen_interval_secs": 0}));
                    gate.read_now().await;
                    gate.script.then(Call::Compute, Act::Hold);
                    gate.script.pause("compute");
                    let late = tokio::spawn({
                        let card = gate.d.card.clone();
                        async move { card.compute().await }
                    });
                    let held = gate.script.held(Call::Compute).await;
                    let l = compute(&gate, Act::Fail(lost())).await;
                    gate.script.release(held, Act::Fail(timeout()));
                    let u_paused = gate.script.paused().await;
                    let replay = gate.d.card.sample().await;
                    assert!(matches!(replay.answer, Answer::Failed(Failure::Lost(_))));
                    let l = taken(l).await;
                    assert_eq!((replay.read, replay.returned), (l.read, l.returned), "the replay is L's, not the later U's");
                    u_paused.release();
                    let u = late.await.unwrap();
                    assert!(u.returned > l.returned);
                    match run {
                        "unanswered first" => {
                            gate.publish(u);
                            assert_eq!(gate.d.with(|s| known(s)), "unread");
                            assert_eq!(gate.usable(), "waiting for gpu-switch to apply a failed GPU read", "L keeps it closed");
                            gate.publish(l);
                            assert_eq!(gate.d.with(|s| known(s)), "lost");
                        }
                        "loss first" => {
                            gate.publish(l);
                            assert_eq!(gate.publish(u), Heard::Failed(lost()), "rule 1 discards U");
                            assert_eq!(gate.d.card.registered(), 0, "rule 6 removed U's entry");
                        }
                        _ => {
                            gate.publish(l);
                            assert_eq!(gate.read_now().await, Heard::Read(()), "closed and recovered");
                            assert_eq!(gate.usable(), "waiting for gpu-switch to apply a failed GPU read", "U keeps it closed");
                            assert_eq!(gate.publish(u), Heard::Superseded);
                            assert_eq!((gate.d.with(|s| known(s)), gate.usable()), ("readable", "card true".to_string()));
                        }
                    }
                    gate.publish(replay);
                    assert_eq!(gate.d.with(|s| next_loss(s)), 2, "{run}: one loss");
                }
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn a_withheld_loss_and_its_replay_open_nothing_until_either_copy_is_acknowledged() {
                for replay_first in [true, false] {
                    let gate = Gate::new(serde_json::json!({"gpu_reopen_interval_secs": 0}));
                    gate.read_now().await;
                    let original = compute(&gate, Act::Fail(lost())).await;
                    let replay = sample(&gate, Act::Answer).await;
                    assert_eq!(gate.script.count(Call::Open), 1, "no open before any acknowledgement");
                    let (original, replay) = (taken(original).await, taken(replay).await);
                    match replay_first {
                        true => drop(gate.publish(replay)),
                        false => drop(gate.publish(original)),
                    }
                    assert_eq!(gate.read_now().await, Heard::Read(()));
                    let order = gate.script.order();
                    assert_eq!(order[order.len() - 3..], [Call::Close, Call::Open, Call::Sample]);
                    assert_eq!((gate.script.count(Call::Close), gate.script.count(Call::Open)), (1, 2));
                }
            }
        }
    }

    /// L2's shutdown precondition and the stages and admissions that follow it, on a manual source (DECISION-W-GQ-5
    /// ruling 5): no reconcile loop runs, and every pass is the test's.
    mod shutdown {
        use super::*;
        use crate::clock::{Manual, Readings};
        use crate::protocol::{ClientMsg, JobSpec};
        use crate::switch::config::secs;
        use crate::switch::control::tests::{cancel, daemon_on, enter, spec, StateDir};

        const SIDE: Side = Side::Host(1000);
        const NS: Duration = Duration::from_nanos(1);
        const ORIGIN: Readings = Readings {
            monotonic: Duration::from_secs(16_777_216),
            wall: Duration::from_secs(1_800_000_000),
        };

        /// L2's precondition on job `id` of `d`, entered and not yet admitted: `admitted`, then `requested`. Returns t0.
        fn precondition(d: &Arc<Daemon>, id: u64, grace: f64) -> Duration {
            let t0 = admitted(d, id, grace);
            requested(d, id);
            t0
        }

        /// The precondition's first half: the client grace `grace` recorded as `lease_session` records it, and the
        /// admission by `schedule` at the source's reading t0. Returns t0.
        fn admitted(d: &Arc<Daemon>, id: u64, grace: f64) -> Duration {
            d.update(|s| s.job_mut(SIDE, id).unwrap().shutdown_grace_secs = Some(grace));
            let t0 = d.clock.monotonic();
            d.with(|s| schedule(d, s));
            assert_eq!(d.with(|s| s.job(SIDE, id).unwrap().admitted_at), Some(t0), "admitted at t0");
            t0
        }

        /// The precondition's second half: the end reason through the real `cancel`, and one controlled pass that pops
        /// the cancel's due SIGTERM.
        fn requested(d: &Arc<Daemon>, id: u64) {
            cancel(d, SIDE, id);
            pass(d);
        }

        /// The journal's entries of `event` for job `id`.
        fn entries(d: &Daemon, id: u64, event: &str) -> usize {
            let handle = d.with(|s| s.job(SIDE, id).unwrap().handle.clone());
            let journal = std::fs::read_to_string(d.state_dir().unwrap().join("failures.jsonl")).unwrap_or_default();
            journal
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .filter(|entry| entry["event"] == event && entry["handle"] == handle.as_str())
                .count()
        }

        /// What was delivered to job `id`: its recorded signal and its `signal` entries.
        fn delivered(d: &Daemon, id: u64) -> (Option<i32>, usize) {
            (d.with(|s| s.job(SIDE, id).unwrap().signal), entries(d, id, "signal"))
        }

        /// The precondition's results, asserted before any stage: the request, its schedule from `from`, one
        /// `shutdown_requested` entry and no signal delivered. Returns the request's id.
        fn assert_requested(d: &Daemon, id: u64, from: Duration, grace: Duration) -> String {
            let tail = secs(d.config.sigterm_to_sigkill_secs);
            let request_id = d.with(|s| {
                let job = s.job(SIDE, id).unwrap();
                let request = job.shutdown.as_ref().expect("a shutdown request");
                assert_eq!(request.reason, "cancelled while running");
                assert!(!request.permanent, "a cancel's request is not permanent");
                let steps: Vec<(Duration, Sig)> = job.kill.as_ref().unwrap().steps.iter().copied().collect();
                assert_eq!(steps, [(from + grace, Sig::Term), (from + grace + tail, Sig::Kill)], "the schedule");
                request.request_id.clone()
            });
            assert_eq!(entries(d, id, "shutdown_requested"), 1, "one shutdown_requested entry");
            assert_eq!(delivered(d, id), (None, 0), "no signal delivered");
            request_id
        }

        /// The messages the owner's receiver holds.
        fn told(client: &mut tokio::sync::mpsc::UnboundedReceiver<ClientMsg>) -> Vec<ClientMsg> {
            std::iter::from_fn(|| client.try_recv().ok()).collect()
        }

        fn notified(told: &[ClientMsg], request_id: &str) -> bool {
            told.iter().any(|m| matches!(m, ClientMsg::Shutdown { shutdown } if shutdown.request_id == request_id))
        }

        /// L2a's stage case: the request first, published to its owner, then SIGTERM exactly at t0 + g and SIGKILL
        /// exactly at t0 + g + T, each stage 1 ns early delivering nothing.
        #[test]
        fn a_shutdown_request_comes_first_then_sigterm_at_its_grace_and_sigkill_after_its_tail() {
            let (manual, clock) = Manual::starting_at(ORIGIN);
            let (d, _dir) = daemon_on(clock, serde_json::json!({}));
            let (id, mut client) = enter(&d, SIDE, spec(None), false, None);
            let t0 = precondition(&d, id, 60.0);
            let (grace, tail) = (secs(60.0), secs(d.config.sigterm_to_sigkill_secs));
            let request_id = assert_requested(&d, id, t0, grace);
            assert_eq!(d.with(|s| s.job(SIDE, id).unwrap().state), JobState::Running, "no startup rule fired");
            let told = told(&mut client);
            assert!(notified(&told, &request_id), "the pass published the request to its owner: {told:?}");
            assert!(!told.iter().any(|m| matches!(m, ClientMsg::Signal { .. })), "and no signal: {told:?}");
            manual.advance(grace - NS);
            pass(&d);
            assert_eq!(delivered(&d, id), (None, 0), "nothing 1 ns before the grace ends");
            manual.advance(NS);
            pass(&d);
            assert_eq!(delivered(&d, id), (Some(libc::SIGTERM), 1), "SIGTERM at t0 + g");
            manual.advance(tail - NS);
            pass(&d);
            assert_eq!(delivered(&d, id), (Some(libc::SIGTERM), 1), "nothing 1 ns before the tail ends");
            manual.advance(NS);
            pass(&d);
            assert_eq!(delivered(&d, id), (Some(libc::SIGKILL), 2), "SIGKILL at t0 + g + T");
        }

        /// A source whose monotonic reading is t0 plus 65 s for each `shutdown_requested` entry in one journal:
        /// that journal write takes 65 s of source time.
        struct Journaled {
            journal: std::path::PathBuf,
        }

        impl crate::clock::Source for Journaled {
            fn monotonic(&self) -> Duration {
                let journal = std::fs::read_to_string(&self.journal).unwrap_or_default();
                let written = journal
                    .lines()
                    .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                    .filter(|entry| entry["event"] == "shutdown_requested")
                    .count();
                ORIGIN.monotonic + Duration::from_secs(65) * written as u32
            }
            fn wall(&self) -> Duration {
                ORIGIN.wall
            }
        }

        /// L2a's delayed pre-schedule write (DECISION-W-GQ-3 ruling 3): the grace starts when its schedule is built,
        /// after the journal write that precedes it, never at the pass's decision instant.
        #[test]
        fn a_grace_starts_when_its_schedule_is_built_after_the_journal_write_before_it() {
            let state = StateDir::new();
            let source = Journaled { journal: state.path().join("failures.jsonl") };
            let clock = crate::clock::Clock::new(Arc::new(source));
            let (d, _dir) = daemon_on(clock, serde_json::json!({"state_dir": state.path()}));
            let (id, mut client) = enter(&d, SIDE, spec(None), false, None);
            let t0 = precondition(&d, id, 60.0);
            assert_eq!(t0, ORIGIN.monotonic, "the cancel read t0: no entry was written yet");
            let request_id = assert_requested(&d, id, t0 + Duration::from_secs(65), secs(60.0));
            assert!(notified(&told(&mut client), &request_id), "the notice reached its owner in the same pass");
        }

        /// L6's admission beside a stopping grant without VRAM: once the startup rule stops the requested grant, an
        /// exclusive submitted after it is admitted, with no further reconcile pass.
        #[test]
        fn an_exclusive_is_admitted_beside_a_grant_stopped_without_vram() {
            let (manual, clock) = Manual::starting_at(ORIGIN);
            let (d, _dir) = daemon_on(clock, serde_json::json!({"startup_idle_secs": 0.15}));
            let exclusive = JobSpec { mode: Mode::Exclusive, vram: 0, ..spec(None) };
            let (stopped, _client) = enter(&d, SIDE, exclusive.clone(), false, None);
            let t0 = precondition(&d, stopped, 2.0);
            assert_requested(&d, stopped, t0, secs(2.0));
            assert_eq!(d.with(|s| s.job(SIDE, stopped).unwrap().state), JobState::Running, "no startup rule fired");
            manual.advance(Duration::from_secs_f64(0.15));
            let (observed_at, tick) = (d.clock.monotonic(), d.clock.monotonic_ns());
            d.update(|s| crate::switch::startup::pass(&d, s, &Vec::new(), &HashMap::new(), observed_at, tick));
            assert_eq!(d.with(|s| s.job(SIDE, stopped).unwrap().state), JobState::Stopping, "stopped without VRAM");
            manual.advance(NS);
            let (next, _next_client) = enter(&d, SIDE, exclusive, false, None);
            d.with(|s| schedule(&d, s));
            assert_eq!(d.with(|s| s.job(SIDE, next).unwrap().state), JobState::Running, "the exclusive is admitted");
        }

        /// L2b's daemon role, by the test path it is started as and names its line by.
        const ENFORCING_DAEMON: &str = "switch::daemon::tests::shutdown::enforcing_daemon";

        /// Job `id` as D reports it to T: the request, its schedule from `t0` in nanoseconds, the state, the end
        /// reason, the signal, and the journal's entries of the request and of signals.
        fn report(d: &Daemon, id: u64, t0: Duration) -> serde_json::Value {
            let (requested, signals) = (entries(d, id, "shutdown_requested"), entries(d, id, "signal"));
            d.with(|s| {
                let job = s.job(SIDE, id).unwrap();
                let steps: Vec<(u64, Sig)> = job
                    .kill
                    .iter()
                    .flat_map(|kill| kill.steps.iter().map(|(at, sig)| (crate::clock::ns(at.saturating_sub(t0)), *sig)))
                    .collect();
                serde_json::json!({
                    "reason": job.shutdown.as_ref().map(|request| request.reason.clone()),
                    "permanent": job.shutdown.as_ref().map(|request| request.permanent),
                    "steps": steps, "state": job.state, "end": job.end_reason, "signal": job.signal,
                    "requested": requested, "signals": signals,
                })
            })
        }

        /// D for L2b (R-d): a daemon on a manual source at t0 that runs no service and passes only on T's command. It
        /// registers H's grant for H's pid and start time (H is T's other child, outside D's tree), with the client
        /// grace 60 s, admits it at t0, marks its startup rule fired and reaches the request through L2's
        /// precondition. It returns at once without its environment.
        #[test]
        #[ignore]
        fn enforcing_daemon() {
            let Some(holder) = std::env::var_os("GQ_L2B_HOLDER") else {
                return;
            };
            let holder: u32 = holder.to_str().unwrap().parse().unwrap();
            let birth: u64 = std::env::var("GQ_L2B_BIRTH").unwrap().parse().unwrap();
            let (link, mut inbox) = crate::switch::sibling::Link::connect(ENFORCING_DAEMON);
            let (manual, clock) = Manual::starting_at(ORIGIN);
            let (d, _dir) = daemon_on(clock, serde_json::json!({}));
            let (id, _client) = enter(&d, SIDE, spec(None), false, None);
            d.update(|s| {
                let job = s.job_mut(SIDE, id).unwrap();
                job.pid_host = Some(holder);
                job.process_started = Some(birth);
            });
            let t0 = admitted(&d, id, 60.0);
            d.update(|s| s.job_mut(SIDE, id).unwrap().police.startup_fired = true);
            requested(&d, id);
            link.send(serde_json::json!({"ack": "requested", "job": report(&d, id, t0)}));
            loop {
                let command = inbox.next();
                match command["cmd"].as_str() {
                    Some("observe") => {
                        let held = vec![(holder, Some(1u64 << 20))];
                        let parents = crate::procs::parents();
                        d.update(|s| crate::switch::startup::pass(&d, s, &held, &parents, t0, crate::clock::ns(t0)));
                    }
                    Some("grace") => {
                        manual.advance(secs(60.0));
                        pass(&d);
                    }
                    Some("tail") => {
                        manual.advance(secs(d.config.sigterm_to_sigkill_secs));
                        pass(&d);
                    }
                    other => panic!("no such command: {other:?}"),
                }
                link.send(serde_json::json!({"ack": "done", "job": report(&d, id, t0)}));
            }
        }

        /// T's side of L2b: H, started first, and D on H's identity, whose precondition T asserts first.
        fn enforcing(holder: crate::roles::Role) -> (crate::roles::Role, crate::switch::sibling::Sibling) {
            let birth = crate::procs::started(holder.pid()).expect("H's start time").to_string();
            let pid = holder.pid().to_string();
            let environment: Vec<(&str, &std::ffi::OsStr)> =
                vec![("GQ_L2B_HOLDER", pid.as_ref()), ("GQ_L2B_BIRTH", birth.as_ref())];
            let mut d = crate::switch::sibling::Sibling::start(ENFORCING_DAEMON, &environment);
            let job = d.expect("requested")["job"].clone();
            let defaults: SwitchConfig = serde_json::from_value(serde_json::json!({"listen": []})).unwrap();
            let (grace, tail) = (secs(60.0), secs(defaults.sigterm_to_sigkill_secs));
            assert_eq!(job["reason"], "cancelled while running", "the precondition's request: {job}");
            assert_eq!(job["permanent"], false, "{job}");
            assert_eq!(
                job["steps"],
                serde_json::json!([[crate::clock::ns(grace), "TERM"], [crate::clock::ns(grace + tail), "KILL"]]),
                "SIGTERM at t0 + g and SIGKILL at t0 + g + T: {job}"
            );
            assert_eq!((&job["requested"], &job["signals"], &job["signal"]), (&serde_json::json!(1), &serde_json::json!(0), &serde_json::Value::Null));
            assert_eq!(job["state"], serde_json::json!(JobState::Stopping), "the startup rule had fired: {job}");
            assert_eq!(job["end"], serde_json::json!([JobState::Cancelled, "cancelled while running"]), "{job}");
            (holder, d)
        }

        /// L2b's first case: with the source at t0 and no escalation pass, the observer that sees H's reallocation is
        /// the only cause: the end reason becomes exactly the request's, permanent, signal 9, and H dies by SIGKILL.
        #[test]
        fn a_reallocation_after_a_shutdown_request_is_killed_by_the_observer_with_its_requests_reason() {
            let mut sleeper = std::process::Command::new("sleep");
            sleeper.arg("3600");
            let (mut holder, mut d) = enforcing(crate::roles::Role::spawn(sleeper));
            d.send(serde_json::json!({"cmd": "observe"}));
            let job = d.expect("done")["job"].clone();
            assert_eq!(job["end"], serde_json::json!([JobState::Killed, "shutdown deadline exceeded: cancelled while running"]), "{job}");
            assert_eq!((&job["permanent"], &job["signal"]), (&serde_json::json!(true), &serde_json::json!(libc::SIGKILL)), "{job}");
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(holder.status().signal(), Some(libc::SIGKILL), "H dies by SIGKILL");
        }

        /// L2b's second case: H ignores SIGTERM and marks it; the grace's pass delivers SIGTERM, whose marker T waits
        /// for (or H's death), and only the tail's pass delivers SIGKILL.
        #[test]
        fn a_stopping_grants_sigterm_reaches_its_holder_before_the_sigkill_of_the_tail() {
            let marks = StateDir::new();
            let (installed, marker) = (marks.path().join("installed"), marks.path().join("term"));
            let mut shell = std::process::Command::new("sh");
            shell.arg("-c").arg(format!(
                "trap 'echo term >> {}' TERM; echo > {}; while :; do sleep 0.02; done",
                marker.display(),
                installed.display()
            ));
            let holder = crate::roles::Role::spawn(shell);
            crate::test_support::wait::wait_until("H's SIGTERM disposition", || installed.exists());
            let (mut holder, mut d) = enforcing(holder);
            d.send(serde_json::json!({"cmd": "grace"}));
            assert_eq!(d.expect("done")["job"]["signal"], serde_json::json!(libc::SIGTERM));
            crate::test_support::wait::wait_until("H's SIGTERM marker or its end", || marker.exists() || !holder.running());
            assert!(marker.exists(), "H met SIGTERM before any SIGKILL");
            d.send(serde_json::json!({"cmd": "tail"}));
            assert_eq!(d.expect("done")["job"]["signal"], serde_json::json!(libc::SIGKILL));
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(holder.status().signal(), Some(libc::SIGKILL), "H dies by SIGKILL");
        }
    }

    /// Q3c: a restart keeps a running job's run and a queued job's place, and abandons only the job whose client did
    /// not come back, at the successor's reattachment grace exactly. Predecessor and successor share one manual
    /// source, as the kernel's monotonic clock is one clock for both processes across a restart.
    mod reattach {
        use super::*;
        use crate::clock::{Manual, Readings};
        use crate::protocol::JobSpec;
        use crate::switch::config::secs;
        use crate::switch::control::tests::{daemon_on, resume, spec, submit};

        const SIDE: Side = Side::Host(1000);
        const NS: Duration = Duration::from_nanos(1);

        #[test]
        fn a_restore_keeps_the_running_job_and_the_queued_place_and_abandons_the_absent_client_at_its_grace() {
            let origin = Readings { monotonic: Duration::from_secs(16_777_216), wall: Duration::from_secs(1_800_000_000) };
            let (manual, clock) = Manual::starting_at(origin);
            let (predecessor, dir) = daemon_on(clock, serde_json::json!({}));
            let exclusive = JobSpec { mode: Mode::Exclusive, vram: 0, ..spec(None) };
            let (a, _a_runner) = submit(&predecessor, SIDE, exclusive.clone());
            predecessor.with(|s| schedule(&predecessor, s));
            manual.advance(NS);
            let (b, _b_runner) = submit(&predecessor, SIDE, exclusive.clone());
            manual.advance(NS);
            let (c, _c_runner) = submit(&predecessor, SIDE, exclusive);
            predecessor.with(|s| schedule(&predecessor, s));
            let state = |d: &Daemon, id: u64| d.with(|s| s.job(SIDE, id).unwrap().state);
            assert_eq!([state(&predecessor, a), state(&predecessor, b), state(&predecessor, c)], [
                JobState::Running,
                JobState::Queued,
                JobState::Queued
            ]);
            crate::switch::persist::save(&predecessor);
            let source = crate::clock::Clock::new(manual.clone());
            let (successor, _successor_dir) = daemon_on(source, serde_json::json!({"state_dir": dir.path()}));
            crate::switch::persist::restore(&successor).expect("the saved state restores");
            let t0 = successor.clock.monotonic();
            let grace = secs(successor.config.reattach_grace_secs);
            for id in [a, b, c] {
                assert_eq!(successor.with(|s| s.job(SIDE, id).unwrap().detached()), Some(t0 + grace), "job {id}");
            }
            assert_eq!(resume(&successor, SIDE, a, std::process::id()), Ok(a), "A reattached");
            assert_eq!(resume(&successor, SIDE, b, std::process::id()), Ok(b), "B reattached");
            manual.advance(grace - NS);
            pass(&successor);
            assert_eq!(state(&successor, c), JobState::Queued, "nothing is abandoned before the grace");
            manual.advance(NS);
            pass(&successor);
            let ended = successor.with(|s| s.job(SIDE, c).unwrap().error.clone());
            assert_eq!((state(&successor, c), ended.as_deref()), (JobState::Cancelled, Some("its client did not come back")));
            assert_eq!(state(&successor, a), JobState::Running, "A keeps its run");
            let position =
                successor.with(|s| crate::switch::view::own_job_of(&successor.clock, s, SIDE, s.job(SIDE, b).unwrap()).position);
            assert_eq!((state(&successor, b), position), (JobState::Queued, Some(1)), "B keeps its place behind A");
        }
    }

    /// L3's owner test: the scheduler never grants while NInfer holds the card, and grants once the line has it back,
    /// both read from the lease's receiver in the scheduling thread.
    mod ninfer {
        use super::*;
        use crate::protocol::ClientMsg;
        use crate::switch::control::tests::{daemon, enter, spec};

        const SIDE: Side = Side::Container;
        const HELD: &str = "NInfer holds the GPU; NOT running until 'gpu-run switch-gpu'";

        #[test]
        fn a_lease_is_never_granted_while_ninfer_holds_the_card_and_is_granted_once_the_line_has_it_back() {
            let (d, _dir) = daemon(serde_json::json!({}));
            d.update(|s| {
                s.serve_wanted = true;
                s.ninfer.phase = NPhase::Serving;
            });
            let (id, mut client) = enter(&d, SIDE, spec(None), false, None);
            d.with(|s| schedule(&d, s));
            let told: Vec<ClientMsg> = std::iter::from_fn(|| client.try_recv().ok()).collect();
            assert!(told.iter().any(|m| matches!(m, ClientMsg::Waiting { waiting } if waiting == HELD)), "{told:?}");
            assert!(!told.iter().any(|m| matches!(m, ClientMsg::Granted { .. })), "no grant while NInfer holds it: {told:?}");
            assert_eq!(d.with(|s| s.job(SIDE, id).unwrap().state), JobState::Queued);
            // What switch-gpu ends in: the line holds the card.
            d.update(|s| s.ninfer.phase = NPhase::Idle);
            d.with(|s| schedule(&d, s));
            let told: Vec<ClientMsg> = std::iter::from_fn(|| client.try_recv().ok()).collect();
            assert!(told.iter().any(|m| matches!(m, ClientMsg::Granted { .. })), "granted once the line has it: {told:?}");
        }
    }
}
