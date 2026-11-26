use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::protocol::{
    ArbiterCmd, ClientMsg, JobSpec, JobState, Mode, Placement, RunnerEvent, Sig,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Side {
    Host(u32),
    Container,
}

impl Side {
    /// The share of work that names none: its user's pool, or the container's. The OS session would not do:
    /// `gpu-run submit` starts each runner in a session of its own, and so does a campaign for each worker.
    pub fn pool(self) -> String {
        match self {
            Side::Host(uid) => format!("uid-{uid}"),
            Side::Container => "container".into(),
        }
    }
}

/// A process on the card, as the host numbers it, with the start time that tells it from a later process
/// given the same pid, and its memory in bytes (unknown as None).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobGpuProcess {
    pub pid: u32,
    pub birth: u64,
    pub bytes: Option<u64>,
}

pub struct SideJobs {
    pub prefix: String,
    pub next_id: u64,
    pub jobs: BTreeMap<u64, Job>,
}

/// Who acts on a job's grant: a gpu-run runner, or a lease client itself;
/// after a restart of gpu-switch, nobody until the client resumes it.
pub enum Conn {
    Runner(mpsc::UnboundedSender<RunnerEvent>),
    Client(mpsc::UnboundedSender<ClientMsg>),
    Detached { until: Instant },
}

/// Signals still to send, at absolute times.
pub struct Kill {
    pub steps: VecDeque<(Instant, Sig)>,
}

#[derive(Default)]
pub struct Police {
    pub compute_watchdog: super::compute_watchdog::ComputeWatchdog,
    pub allocated: bool,
    pub used_compute: bool,
    pub startup_since: Option<Instant>,
    pub startup_fired: bool,
    pub idle_since: Option<Instant>,
    /// 0 none, 1 SIGINT sent, 2 SIGTERM sent, 3 SIGKILL sent.
    pub idle_stage: u8,
    pub idle_stage_at: Option<Instant>,
    pub shared_spent: f64,
    pub shared_sample_at: Option<Instant>,
    /// The holder ended its session and was answered with the grant's violations, or the card was lost under the
    /// grant; nothing after is judged.
    pub concluded: bool,
    /// The grant's final report was taken: the grant ended for its holder.
    pub reported: bool,
}

/// How a loss reached a job (`Job::device_lost`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Association {
    /// The open grant's run is linked to the loss, and its holder is told.
    Marked,
    /// The open grant's run already has a loss from an earlier interval: it keeps its first.
    AlreadyMarked,
    /// The grant ended at its final report.
    Ended,
    /// The job holds no grant.
    NotHeld,
}

/// What ends a grant for its holder: its run's broken rules and its loss, at the final report's snapshot, and the
/// card as sampled over its grants.
pub struct FinalReport {
    pub violations: Vec<crate::protocol::Violation>,
    pub device_lost: Option<crate::protocol::GrantLoss>,
    pub telemetry: Option<crate::protocol::JobTelemetry>,
}

pub struct Job {
    pub retention: Option<super::retention::Retention>,
    pub terminal_status: Option<crate::protocol::TerminalStatus>,
    /// The GPU usage contract's broken rules, once each (`super::contract`).
    pub violations: Vec<crate::protocol::Violation>,
    /// The card losses linked to its runs, one association per run (`super::card`).
    pub device_lost: Vec<crate::protocol::GrantLoss>,
    /// The GPU processes of the job's tree in the latest card sample (`super::poller`).
    pub gpu_processes: Vec<JobGpuProcess>,
    pub timing: Option<super::timing::Timing>,
    /// The card as sampled while it held its grants (`super::poller`); sealed once it never runs again.
    pub telemetry: super::telemetry::Telemetry,
    pub id: u64,
    pub handle: String,
    pub side: Side,
    pub spec: JobSpec,
    /// The share a fresh entry of this job took its turn by: `spec.share`, or the side's pool.
    pub share: String,
    pub cmd: Vec<String>,
    pub label: Option<String>,
    /// A lease client's job: its shared members never queue.
    pub group: Option<String>,
    /// A lease client rather than a gpu-run job.
    pub lease: bool,
    /// Lets the client resume the job after gpu-switch restarts.
    pub token: String,
    pub cwd: String,
    pub log: String,
    pub state: JobState,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub error: Option<String>,
    /// SO_PEERCRED pid of the runner or the lease client (host namespace).
    pub runner_pid: u32,
    pub conn: Conn,
    /// Teardown was started after the client's reattachment window expired.
    pub departing: bool,
    pub pid_inner: Option<u32>,
    /// Root of the job's process tree on the host.
    pub pid_host: Option<u32>,
    pub submitted_at: f64,
    pub started_at: Option<f64>,
    pub finished_at: Option<f64>,
    /// Place in the line while queued.
    pub key: Placement,
    /// Re-queued after a yield: behind the head, before fresh waiters.
    pub requeued: bool,
    pub runs: u32,
    pub admitted_at: Option<Instant>,
    pub yield_sent: bool,
    pub kill: Option<Kill>,
    /// How the run ends if it ends now: set by cancel, kill and policing.
    pub end_reason: Option<(JobState, String)>,
    pub police: Police,
    pub shutdown_grace_secs: Option<f64>,
    pub shutdown: Option<crate::protocol::ShutdownRequest>,
    /// Process birth identity, fencing enforcement against PID reuse.
    pub process_started: Option<u64>,
    /// The scheduler's current waiting reason; a lease client hears each change, unless it is a told place
    /// still preparing.
    pub reason: Option<String>,
    /// What the scheduler found blocking it, with `reason`.
    pub blocker: Option<crate::protocol::Blocker>,
    /// A place (`super::preparation`); none for an entry ready on arrival.
    pub place: Option<super::preparation::Place>,
    /// The owner a place was transferred from: its resent transfer is acknowledged again, and its resume
    /// is answered with the place's new owner.
    pub transferred_from: Option<crate::protocol::ProcessIdentity>,
}

/// Jobs that count as one: a lease group, or a job on its own.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Party {
    Group(Side, String),
    Alone(Side, u64),
}

impl Job {
    pub fn mode(&self) -> Mode {
        match self.retention {
            Some(_) => Mode::Shared,
            None => self.spec.mode,
        }
    }

    pub fn reserved_vram(&self) -> u64 {
        match self.retention {
            Some(_) => 0,
            None => self.spec.vram,
        }
    }

    pub fn party(&self) -> Party {
        match &self.group {
            Some(group) => Party::Group(self.side, group.clone()),
            None => Party::Alone(self.side, self.id),
        }
    }

    pub fn active(&self) -> bool {
        !self.state.is_terminal()
    }

    /// The scheduler may admit it: it arrived ready, or it is a place that sent `Ready`.
    pub fn admissible(&self) -> bool {
        self.place.as_ref().is_none_or(|place| place.ready.is_some())
    }

    /// It counts towards the places told: it arrived ready, or it is a told place that is not parked.
    pub fn counted(&self) -> bool {
        self.place.as_ref().is_none_or(|place| place.told() && !place.parked)
    }

    pub fn is_lease(&self) -> bool {
        self.lease
    }

    /// Waiting for its client to resume it, until when.
    pub fn detached(&self) -> Option<Instant> {
        match self.conn {
            Conn::Detached { until } => Some(until),
            _ => None,
        }
    }

    pub fn go(&self) {
        let _ = match &self.conn {
            Conn::Runner(tx) => tx.send(RunnerEvent::Go).map_err(|_| ()),
            Conn::Client(tx) => tx
                .send(ClientMsg::Granted { granted: true })
                .map_err(|_| ()),
            Conn::Detached { .. } => Ok(()),
        };
    }

    pub fn signal(&self, sig: Sig) {
        let _ = match &self.conn {
            Conn::Runner(tx) => tx.send(RunnerEvent::Signal { sig }).map_err(|_| ()),
            Conn::Client(tx) => tx.send(ClientMsg::Signal { signal: sig }).map_err(|_| ()),
            Conn::Detached { .. } => Ok(()),
        };
    }

    pub fn shutdown_notice(&self, request: crate::protocol::ShutdownRequest) {
        if let Conn::Client(tx) = &self.conn {
            let _ = tx.send(ClientMsg::Shutdown { shutdown: request });
        }
    }

    /// The holder reports that its run failed, before its own teardown: the run ends when it releases or exits.
    pub fn failure_reported(&mut self, reason: &str) {
        self.end_reason
            .get_or_insert((JobState::Failed, reason.chars().take(4096).collect()));
    }

    /// The rules the current grant broke: what a releasing lease client is answered.
    pub fn grant_violations(&self) -> Vec<crate::protocol::Violation> {
        self.violations
            .iter()
            .filter(|violation| violation.run == self.runs)
            .cloned()
            .collect()
    }

    /// Every recorded violation and every loss association again, for a holder that reattached; it ignores those
    /// it heard.
    pub fn replay_notices(&self) {
        for violation in &self.violations {
            self.violation_notice(violation.clone());
        }
        for grant in &self.device_lost {
            self.loss_notice(grant.clone());
        }
    }

    /// Links `loss` to the open grant's run: a grant is open while it runs and before its final report. Its holder is
    /// told, and no rule is judged over a span that a hardware loss ended.
    pub fn device_lost(&mut self, loss: &crate::protocol::DeviceLoss) -> Association {
        let open = self.state == JobState::Running;
        let linked = self.device_lost.iter().any(|grant| grant.run == self.runs);
        match (open, self.police.reported, linked) {
            (false, _, _) => Association::NotHeld,
            (true, true, _) => Association::Ended,
            (true, false, true) => Association::AlreadyMarked,
            (true, false, false) => {
                let grant = crate::protocol::GrantLoss {
                    run: self.runs,
                    loss: loss.clone(),
                };
                self.device_lost.push(grant.clone());
                self.police.concluded = true;
                self.loss_notice(grant);
                Association::Marked
            }
        }
    }

    /// The start of the open grant being timed: it runs and its final report was not taken.
    pub fn open_grant(&self) -> Option<u64> {
        self.timing
            .as_ref()
            .and_then(super::timing::Timing::window)
            .filter(|_| !self.police.reported)
    }

    /// The grant ends for its holder: one snapshot of its run's broken rules and its loss, after which nothing is
    /// judged and no later loss is linked.
    pub fn final_report(&mut self) -> FinalReport {
        self.police.reported = true;
        self.police.concluded = true;
        FinalReport {
            violations: self.grant_violations(),
            device_lost: self.device_lost.iter().find(|grant| grant.run == self.runs).cloned(),
            telemetry: self.telemetry.summary(),
        }
    }

    /// The holder hears of a card loss linked to one of its runs: a lease client itself, a gpu-run job through its
    /// runner.
    fn loss_notice(&self, device_lost: crate::protocol::GrantLoss) {
        let _ = match &self.conn {
            Conn::Runner(tx) => tx.send(RunnerEvent::DeviceLost { device_lost }).map_err(|_| ()),
            Conn::Client(tx) => tx.send(ClientMsg::DeviceLost { device_lost }).map_err(|_| ()),
            Conn::Detached { .. } => Ok(()),
        };
    }

    /// The holder hears of each broken contract rule: a lease client itself, a gpu-run job through its runner.
    pub fn violation_notice(&self, violation: crate::protocol::Violation) {
        let _ = match &self.conn {
            Conn::Runner(tx) => tx
                .send(RunnerEvent::Violation { violation })
                .map_err(|_| ()),
            Conn::Client(tx) => tx.send(ClientMsg::Violation { violation }).map_err(|_| ()),
            Conn::Detached { .. } => Ok(()),
        };
    }

    pub fn runner(&self, ev: RunnerEvent) {
        if let Conn::Runner(tx) = &self.conn {
            let _ = tx.send(ev);
        }
    }

    /// Before its `Ready`, a told place hears only replays, signals and shutdown requests.
    pub fn waiting(&mut self, why: String, blocker: Option<crate::protocol::Blocker>) {
        let preparing = self
            .place
            .as_ref()
            .is_some_and(|place| place.told() && place.ready.is_none());
        if let Conn::Client(tx) = &self.conn {
            if self.reason.as_ref() != Some(&why) && !preparing {
                let _ = tx.send(ClientMsg::Waiting {
                    waiting: why.clone(),
                });
            }
        }
        self.reason = Some(why);
        self.blocker = blocker;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NPhase {
    /// Not in the line.
    Idle,
    /// Waiting in the line.
    Queued,
    Starting,
    Serving,
    /// `stop` sent.
    Stopping,
    /// NInfer stopped; waiting for its pid to leave the GPU.
    Releasing,
}

pub struct Ninfer {
    pub phase: NPhase,
    pub key: Placement,
    pub draining: bool,
    pub ns_pid: Option<u32>,
    pub host_pid: Option<u32>,
    pub error: Option<String>,
    pub failed: bool,
    pub retry_at: Option<Instant>,
    pub backoff: Duration,
    /// After the stop, start again without letting the line in (`--apply-now`).
    pub restart: bool,
    pub stopped_at: Option<Instant>,
    /// The arbiter link dropped (or gpu-switch restarted): NInfer keeps the
    /// GPU for `reattach_grace_secs` while the arbiter reconnects.
    pub link_lost_at: Option<Instant>,
}

pub struct Arbiter {
    pub conn: u64,
    pub tx: mpsc::UnboundedSender<ArbiterCmd>,
    pub host_pid: u32,
}

pub struct View {
    /// What the switch knows of the card (`super::card`).
    pub card: super::card::CardState,
    pub parents: HashMap<u32, u32>,
}

/// One waiting entry of the line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Entry {
    Job(Side, u64),
    Ninfer,
}

pub struct State {
    pub serve_wanted: bool,
    pub switch_requested: bool,
    pub restart_requested: bool,
    /// `switch-ninfer`: NInfer's entry goes to the front of the line.
    pub switch_to_ninfer: bool,
    pub ninfer: Ninfer,
    pub arbiter: Option<Arbiter>,
    pub next_conn: u64,
    pub sides: BTreeMap<Side, SideJobs>,
    pub view: View,
    pub empty_since: Option<Instant>,
    pub skip_linger: bool,
    /// Last time a job or NInfer left the GPU: admissions need a newer sample.
    pub left_gpu_at: Option<Instant>,
    pub profile_override: Option<String>,
    pub host_cache_override: Option<u64>,
    pub profile_current: Option<String>,
    pub host_cache_current: Option<u64>,
}

impl State {
    /// The state of a daemon whose card is `configured`, or a host without one.
    pub fn new(backoff: Duration, configured: bool) -> State {
        State {
            serve_wanted: false,
            switch_requested: false,
            restart_requested: false,
            switch_to_ninfer: false,
            ninfer: Ninfer {
                phase: NPhase::Idle,
                key: Placement { class: 1, ns: 0 },
                draining: false,
                ns_pid: None,
                host_pid: None,
                error: None,
                failed: false,
                retry_at: None,
                backoff,
                restart: false,
                stopped_at: None,
                link_lost_at: None,
            },
            arbiter: None,
            next_conn: 1,
            sides: BTreeMap::new(),
            view: View {
                card: super::card::CardState::new(configured),
                parents: HashMap::new(),
            },
            empty_since: None,
            skip_linger: false,
            left_gpu_at: None,
            profile_override: None,
            host_cache_override: None,
            profile_current: None,
            host_cache_current: None,
        }
    }

    pub fn job(&self, side: Side, id: u64) -> Option<&Job> {
        self.sides.get(&side)?.jobs.get(&id)
    }

    pub fn job_mut(&mut self, side: Side, id: u64) -> Option<&mut Job> {
        self.sides.get_mut(&side)?.jobs.get_mut(&id)
    }

    /// A healthy running lease, asked about by its authenticated owner.
    pub fn owned_lease(
        &mut self,
        side: Side,
        peer_pid: u32,
        resume: &crate::protocol::Resume,
        what: &str,
    ) -> Result<&mut Job, String> {
        let job = self.job_mut(side, resume.id).ok_or("no such GPU lease")?;
        if !job.lease
            || job.runner_pid != peer_pid
            || job.token != resume.token
            || job.process_started != crate::procs::started(peer_pid)
        {
            return Err(format!("{what} requires the authenticated lease owner"));
        }
        if job.state != JobState::Running
            || job.kill.is_some()
            || job.end_reason.is_some()
            || job.shutdown.is_some()
            || job.departing
        {
            return Err(format!("{what} requires a healthy running lease"));
        }
        Ok(job)
    }

    pub fn jobs(&self) -> impl Iterator<Item = &Job> {
        self.sides.values().flat_map(|s| s.jobs.values())
    }

    /// The GPU belongs to NInfer from its admission until it is off the card.
    pub fn ninfer_owns(&self) -> bool {
        matches!(
            self.ninfer.phase,
            NPhase::Starting | NPhase::Serving | NPhase::Stopping | NPhase::Releasing
        )
    }

    /// Waiting entries in line order.
    pub fn line(&self) -> Vec<(Placement, Entry)> {
        let mut v: Vec<(Placement, Entry)> = self
            .jobs()
            .filter(|j| j.state == JobState::Queued)
            .map(|j| (j.key, Entry::Job(j.side, j.id)))
            .collect();
        if self.ninfer.phase == NPhase::Queued {
            v.push((self.ninfer.key, Entry::Ninfer));
        }
        v.sort_by_key(|(k, _)| *k);
        v
    }

    pub fn entry_priority(&self, e: Entry) -> i8 {
        match e {
            Entry::Job(side, id) => self.job(side, id).map(|j| j.spec.priority).unwrap_or(0),
            Entry::Ninfer => 0,
        }
    }

    /// The share a waiting entry took its turn by; NInfer has none.
    pub fn entry_share(&self, e: Entry) -> Option<&str> {
        match e {
            Entry::Job(side, id) => self.job(side, id).map(|j| j.share.as_str()),
            Entry::Ninfer => None,
        }
    }

    /// An admissible exclusive entry or NInfer waits in the line; a place still preparing does not count.
    pub fn exclusive_waiting(&self) -> bool {
        self.line().iter().any(|(_, e)| {
            self.entry_mode(*e) == Mode::Exclusive
                && match e {
                    Entry::Job(side, id) => self.job(*side, *id).is_some_and(Job::admissible),
                    Entry::Ninfer => true,
                }
        })
    }

    pub fn entry_mode(&self, e: Entry) -> Mode {
        match e {
            Entry::Job(side, id) => self.job(side, id).map(Job::mode).unwrap_or(Mode::Exclusive),
            Entry::Ninfer => Mode::Exclusive,
        }
    }

    /// No job queued or running.
    pub fn queue_empty(&self) -> bool {
        !self
            .jobs()
            .any(|j| matches!(j.state, JobState::Queued | JobState::Running))
    }

    pub fn running(&self) -> impl Iterator<Item = &Job> {
        self.jobs().filter(|j| j.state == JobState::Running)
    }

    /// Host pids of a job's process tree, from the last poll.
    pub fn tree(&self, root: Option<u32>) -> HashSet<u32> {
        root.map(|r| crate::procs::job_tree(r, &self.view.parents))
            .unwrap_or_default()
    }

    pub fn profile_next(&self, default: &Option<String>) -> Option<String> {
        self.profile_override.clone().or_else(|| default.clone())
    }

    pub fn host_cache_next(&self, default: Option<u64>) -> Option<u64> {
        self.host_cache_override.or(default)
    }
}

pub fn monotonic_ns() -> u64 {
    crate::posix_clock::monotonic().as_nanos() as u64
}

/// The monotonic deadline of a positive, finite, representable duration.
pub fn deadline_after(now_ns: u64, seconds: f64, what: &str) -> Result<u64, String> {
    let duration = Duration::try_from_secs_f64(seconds)
        .map_err(|_| format!("{what} requires positive finite seconds"))?;
    let nanos =
        u64::try_from(duration.as_nanos()).map_err(|_| format!("{what} duration is too large"))?;
    if nanos == 0 {
        return Err(format!("{what} requires positive finite seconds"));
    }
    now_ns
        .checked_add(nanos)
        .ok_or_else(|| format!("{what} deadline is too large"))
}
