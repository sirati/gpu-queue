use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::nvsmi::Sample;
use crate::protocol::{
    ArbiterCmd, ClientMsg, JobSpec, JobState, Mode, Placement, RunnerEvent, Sig,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Side {
    Host(u32),
    Container,
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
}

pub struct Job {
    pub retention: Option<super::retention::Retention>,
    pub terminal_status: Option<crate::protocol::TerminalStatus>,
    pub timing: Option<super::timing::Timing>,
    pub id: u64,
    pub handle: String,
    pub side: Side,
    pub spec: JobSpec,
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
    /// Last waiting reason sent to a lease client.
    pub reason: Option<String>,
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

    pub fn runner(&self, ev: RunnerEvent) {
        if let Conn::Runner(tx) = &self.conn {
            let _ = tx.send(ev);
        }
    }

    pub fn waiting(&mut self, why: String) {
        if let Conn::Client(tx) = &self.conn {
            if self.reason.as_ref() != Some(&why) {
                let _ = tx.send(ClientMsg::Waiting {
                    waiting: why.clone(),
                });
                self.reason = Some(why);
            }
        }
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

#[derive(Default)]
pub struct View {
    pub gpu: Option<Sample>,
    pub gpu_at: Option<Instant>,
    pub gpu_error: Option<String>,
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
    pub fn new(backoff: Duration) -> State {
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
            view: View::default(),
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
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: valid out pointer.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}
