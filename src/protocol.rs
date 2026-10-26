//! Wire formats. Every connection carries one JSON object per line in each
//! direction.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Exclusive,
    Shared,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Exclusive => "exclusive",
            Mode::Shared => "shared",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Category {
    Train,
    Benchmark,
    Accuracy,
    Test,
    Eval,
    Inference,
    Other,
}

impl Category {
    pub const ALL: [Category; 7] = [
        Category::Train,
        Category::Benchmark,
        Category::Accuracy,
        Category::Test,
        Category::Eval,
        Category::Inference,
        Category::Other,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            Category::Train => "train",
            Category::Benchmark => "benchmark",
            Category::Accuracy => "accuracy",
            Category::Test => "test",
            Category::Eval => "eval",
            Category::Inference => "inference",
            Category::Other => "other",
        }
    }
    pub fn parse(s: &str) -> Option<Category> {
        Category::ALL.into_iter().find(|c| c.as_str() == s)
    }
}

pub const PRIORITY_MIN: i8 = -10;
pub const PRIORITY_MAX: i8 = 10;

/// What a submit declares; shared by the CLI, the runner and the daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSpec {
    pub mode: Mode,
    /// Declared peak VRAM in bytes; 0 for exclusive.
    pub vram: u64,
    pub category: Category,
    pub cooperative: bool,
    pub priority: i8,
    /// Wall-clock limit on the job's running time, counted by the runner:
    /// SIGTERM at the limit, SIGKILL after a grace period.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_runtime_secs: Option<u64>,
}

impl JobSpec {
    pub fn validate(&self) -> Result<(), String> {
        match self.mode {
            Mode::Shared if self.vram == 0 => {
                return Err(
                    "a --shared job declares its peak VRAM with --vram SIZE (e.g. 12G)".into(),
                )
            }
            Mode::Exclusive if self.vram != 0 => {
                return Err("--vram belongs to --shared only".into())
            }
            Mode::Exclusive if self.cooperative => {
                return Err("--cooperative needs --shared".into())
            }
            _ => {}
        }
        if self.max_runtime_secs == Some(0) {
            return Err("the run-time limit must be at least one second".into());
        }
        if !(PRIORITY_MIN..=PRIORITY_MAX).contains(&self.priority) {
            return Err(format!(
                "priority must be in {PRIORITY_MIN}..={PRIORITY_MAX}"
            ));
        }
        Ok(())
    }
}

// ---- gpu-run <-> gpu-switch ----

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Status,
    SwitchGpu,
    /// Hold the line, let the GPU-resident jobs finish, then start NInfer.
    SwitchNinfer,
    Cancel {
        id: u64,
    },
    Wait {
        id: u64,
    },
    Kill {
        handle: String,
    },
    SetHostCache {
        mib: u64,
        apply_now: bool,
    },
    SetProfile {
        profile: String,
        apply_now: bool,
    },
    Serve {
        on: bool,
    },
    ServeStatus,
    /// Turns the connection into a runner session.
    Submit(SubmitRequest),
    /// Turns the connection into a lease client: a grant for the peer's own
    /// process tree, held while the connection lasts.
    Lease(LeaseRequest),
    /// Counts for a lease client's yield decisions, never across-side details.
    Census {
        #[serde(default)]
        group: Option<String>,
    },
    /// Lease session: give the grant back.
    Release,
    /// Lease session: cleanup has begun for this exact shutdown request.
    ShutdownStarted {
        request_id: String,
    },
    Failed {
        reason: String,
    },
    /// Container socket only: turns the connection into the arbiter link.
    ArbiterAttach,
}

/// Runner -> daemon, after a successful submit.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum RunnerReport {
    /// `pid` in the runner's pid namespace.
    Started {
        pid: u32,
    },
    SpawnFailed {
        error: String,
    },
    Finished {
        exit_code: Option<i32>,
        signal: Option<i32>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Sig {
    Int,
    Term,
    Kill,
}

impl Sig {
    pub fn number(self) -> i32 {
        match self {
            Sig::Int => libc::SIGINT,
            Sig::Term => libc::SIGTERM,
            Sig::Kill => libc::SIGKILL,
        }
    }
}

/// Daemon -> runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum RunnerEvent {
    Go,
    /// Cancelled while queued: exit without running anything.
    Cancel,
    /// Deliver to the job's process group.
    Signal {
        sig: Sig,
    },
    /// Cooperative job: create `$ML_GPU_YIELD`.
    Yield,
    /// After `finished`: nothing more, exit.
    Done,
}

/// What a gpu-run runner asks for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitRequest {
    pub cmd: Vec<String>,
    pub cwd: String,
    pub spec: JobSpec,
    /// The runner's log directory; the daemon names `<dir>/<id>.log` in it.
    pub log_dir: String,
    #[serde(default)]
    pub resume: Option<Resume>,
}

/// A job a client held before gpu-switch restarted: re-attached if gpu-switch
/// kept it, re-registered at its place if the token proves that place.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resume {
    pub id: u64,
    pub token: String,
    pub key: Placement,
    /// Admitted, and its processes still use the grant.
    pub running: bool,
}

/// What a lease client asks for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseRequest {
    /// Opt in to typed shutdown with a bounded checkpoint interval.
    #[serde(default)]
    pub shutdown_grace_secs: Option<f64>,
    pub mode: Mode,
    #[serde(default)]
    pub vram: Option<u64>,
    /// Shown to the owner only.
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub priority: i8,
    /// One job's processes: a shared member of a running group never
    /// queues, and the group's VRAM counts once.
    #[serde(default)]
    pub group: Option<String>,
    /// Released and asking again: behind the head, before fresh waiters.
    #[serde(default)]
    pub requeued: bool,
    #[serde(default)]
    pub cooperative: bool,
    #[serde(default)]
    pub resume: Option<Resume>,
}

/// Running jobs and waiting exclusives as a lease client's group sees them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Census {
    /// An exclusive job or NInfer waits in the line.
    pub exclusive_waiting: bool,
    /// Running jobs outside the asking group.
    pub others: usize,
    /// Of those, the ones not cooperative.
    pub non_cooperative_others: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Reply {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<Status>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<OwnJob>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serve: Option<ServeStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub census: Option<Census>,
    /// A submitted job's resume token and place in the line.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume: Option<Resume>,
}

impl Reply {
    pub fn ok() -> Reply {
        Reply {
            ok: true,
            ..Default::default()
        }
    }
    pub fn message(m: impl Into<String>) -> Reply {
        Reply {
            ok: true,
            message: Some(m.into()),
            ..Default::default()
        }
    }
    pub fn error(e: impl Into<String>) -> Reply {
        Reply {
            ok: false,
            error: Some(e.into()),
            ..Default::default()
        }
    }
}

/// What `serve_status` and `serve` reveal: nothing about jobs, requests,
/// sizes or errors.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServeStatus {
    pub phase: String,
    pub profile_current: Option<String>,
    pub profile_pending: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Queued,
    /// No GPU grant remains; bounded CPU cleanup is still supervised.
    Stopping,
    Running,
    Done,
    Failed,
    Cancelled,
    Killed,
}

impl JobState {
    pub fn is_terminal(self) -> bool {
        !matches!(
            self,
            JobState::Queued | JobState::Running | JobState::Stopping
        )
    }
    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Stopping => "stopping",
            JobState::Running => "running",
            JobState::Done => "done",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
            JobState::Killed => "killed",
        }
    }
}

/// A job of the viewer's own side: everything.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwnJob {
    pub id: u64,
    pub handle: String,
    pub state: JobState,
    pub mode: Mode,
    pub vram: u64,
    pub category: Category,
    pub cooperative: bool,
    pub priority: i8,
    /// 1-based place in the joint line while waiting.
    pub position: Option<usize>,
    /// Waiting behind NInfer's lease: runs only after `switch-gpu`.
    pub behind_ninfer: bool,
    /// gpu-run job: the command. Lease client: empty, see `label`.
    pub cmd: Vec<String>,
    pub label: Option<String>,
    /// A lease client rather than a gpu-run job.
    pub lease: bool,
    pub cwd: String,
    pub log: String,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub error: Option<String>,
    pub runner_pid: u32,
    pub pid: Option<u32>,
    pub submitted_at: f64,
    pub started_at: Option<f64>,
    pub finished_at: Option<f64>,
    pub runs: u32,
}

/// Another side's job: only this.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtherJob {
    pub handle: String,
    pub state: JobState,
    pub position: Option<usize>,
    pub mode: Mode,
    pub vram: u64,
    pub cooperative: bool,
    pub priority: i8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LineKind {
    Own,
    Other,
    Ninfer,
}

/// One waiting ticket of the joint line, in order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineEntry {
    pub position: usize,
    pub kind: LineKind,
    /// Own jobs: the id. Other side: the handle. NInfer: null.
    pub job: Option<String>,
    pub priority: i8,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Counts {
    pub queued: usize,
    pub running: usize,
    /// Queued behind NInfer's lease, i.e. waiting for `switch-gpu`.
    pub waiting: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    pub summary: String,
    pub serve: ServeStatus,
    pub serve_wanted: bool,
    pub owner: String,
    /// Configured profile names.
    pub profiles: Vec<String>,
    pub host_cache_current_mib: Option<u64>,
    pub host_cache_pending_mib: Option<u64>,
    pub gpu_processes: usize,
    pub own: Counts,
    pub others: Counts,
    pub line: Vec<LineEntry>,
    pub jobs: Vec<OwnJob>,
    pub other_jobs: Vec<OtherJob>,
    /// Container side only: NInfer's last error.
    pub ninfer_error: Option<String>,
}

// ---- gpu-switch <-> gpu-arbiter ----

/// Daemon -> arbiter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum ArbiterCmd {
    Start {
        profile: Option<String>,
        host_context_mib: Option<u64>,
    },
    Stop {
        drain_timeout_secs: Option<f64>,
    },
}

/// Arbiter -> daemon. Pids are in the arbiter's namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ArbiterEvent {
    Starting {
        pid: u32,
    },
    Serving {
        pid: u32,
    },
    Draining,
    Stopped {
        pid: Option<u32>,
    },
    /// Start failed, or NInfer exited by itself. NInfer is not running.
    Failed {
        error: String,
    },
}

// ---- lease clients ----

/// A line key: lower sorts first. Fresh entries are `(1, arrival ns)`,
/// re-queued ones `(0, ns)`; `switch-ninfer` puts NInfer at `(0, 0)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Placement {
    pub class: u8,
    pub ns: u64,
}

/// What a lease client is told, each its own line.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ClientMsg {
    /// Why it still waits; sent whenever the reason changes.
    Waiting {
        waiting: String,
    },
    /// Admitted: the grant lasts until `release` or EOF.
    Granted {
        granted: bool,
    },
    /// Deliver to itself (policing, cross-side kill).
    Signal {
        signal: Sig,
    },
    Shutdown {
        shutdown: ShutdownRequest,
    },
}

/// The deadline is fixed when enforcement starts. An acknowledgement does not renew it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownRequest {
    pub request_id: String,
    pub reason: String,
    pub deadline: f64,
    pub permanent: bool,
    #[serde(default)]
    pub handler_started: bool,
}

/// The job a lease reply names, and how to resume it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseJob {
    pub id: u64,
    pub handle: String,
    pub resume: Resume,
}

pub fn now_unix() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Python's `vram_bytes` (tools/gpu_exclusive.py:21-25).
pub fn parse_vram(text: &str) -> Option<u64> {
    let t = text.trim();
    let digits_end = t
        .char_indices()
        .find(|(_, c)| !(c.is_ascii_digit() || *c == '.'))
        .map(|(i, _)| i)
        .unwrap_or(t.len());
    let (num, rest) = t.split_at(digits_end);
    let mut parts = num.splitn(2, '.');
    let int = parts.next()?;
    if int.is_empty() || !int.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if let Some(frac) = parts.next() {
        if frac.is_empty() || !frac.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
    }
    let lower = rest.to_ascii_lowercase();
    let (unit, suffix) = match lower.chars().next() {
        Some(c @ ('k' | 'm' | 'g' | 't')) => (c, &lower[1..]),
        _ => (' ', lower.as_str()),
    };
    if !matches!(suffix, "" | "b" | "ib") {
        return None;
    }
    let mult: u64 = match unit {
        'k' => 1 << 10,
        'm' => 1 << 20,
        'g' => 1 << 30,
        't' => 1 << 40,
        _ => 1,
    };
    let value: f64 = num.parse().ok()?;
    Some((value * mult as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vram_like_python() {
        assert_eq!(parse_vram("12G"), Some(12 << 30));
        assert_eq!(parse_vram("1536M"), Some(1536 << 20));
        assert_eq!(parse_vram("2.5GiB"), Some(5 << 29));
        assert_eq!(parse_vram("2.5gib"), Some(5 << 29));
        assert_eq!(parse_vram("1000"), Some(1000));
        assert_eq!(parse_vram("1000B"), Some(1000));
        assert_eq!(parse_vram(" 4k "), Some(4096));
        assert_eq!(parse_vram("12GB"), Some(12 << 30));
        for bad in ["", "G", "1.G", ".5G", "12X", "12GiBs", "-1G", "1e3"] {
            assert_eq!(parse_vram(bad), None, "{bad}");
        }
    }
}
