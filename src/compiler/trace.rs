//! Proof-build instrumentation (branch proof/actor-stall-diagnosis-20261118): times the compile actor's busy spans
//! and writes every span longer than THRESHOLD_MS to standard error as one `stall-trace: {json}` line. It changes no
//! decision: callers mark phases around the work they already do.
//!
//! Every line carries the span's start on CLOCK_MONOTONIC (`mono_start`, comparable with Python's `time.monotonic()`)
//! and on the wall clock (`wall_start`, comparable with `time.time()`), its wall duration, and the current thread's
//! scheduler figures over the span from /proc/thread-self/schedstat: `run_ms` on a CPU, `run_delay_ms` runnable but
//! waiting for a CPU, `off_cpu_ms` neither (sleeping or blocked: awaiting another thread, a pipe, or the disk).
use serde_json::{json, Map, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub const PREFIX: &str = "stall-trace: ";
const THRESHOLD_MS: f64 = 20.0;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub fn monotonic() -> f64 {
    let mut t: libc::timespec = unsafe { std::mem::zeroed() };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut t) };
    t.tv_sec as f64 + t.tv_nsec as f64 / 1e9
}

fn milliseconds(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1e3
}

/// The current thread's time on a CPU and time runnable on a run queue, in nanoseconds.
#[derive(Clone, Copy, Default)]
pub struct Sched {
    run: u64,
    delay: u64,
}
impl Sched {
    pub fn now() -> Self {
        Self::of("/proc/thread-self/schedstat")
    }
    /// A thread's figures since it was created, e.g. a just-spawned child's main thread.
    pub fn of(path: &str) -> Self {
        let text = std::fs::read_to_string(path).unwrap_or_default();
        let mut fields = text.split_whitespace().map(|f| f.parse::<u64>().unwrap_or(0));
        Self {
            run: fields.next().unwrap_or(0),
            delay: fields.next().unwrap_or(0),
        }
    }
    pub fn run_ms(&self) -> f64 {
        self.run as f64 / 1e6
    }
    pub fn delay_ms(&self) -> f64 {
        self.delay as f64 / 1e6
    }
    fn since(&self, earlier: &Sched) -> Sched {
        Sched {
            run: self.run.saturating_sub(earlier.run),
            delay: self.delay.saturating_sub(earlier.delay),
        }
    }
}

/// Identifies a request from its connection to the actor: the connection's sequence number and the moment the
/// request entered the actor's channel.
#[derive(Clone, Copy)]
pub struct Sent {
    pub sequence: u64,
    pub at: Instant,
}
impl Sent {
    pub fn now(sequence: u64) -> Self {
        Self { sequence, at: Instant::now() }
    }
}
pub fn next_sequence() -> u64 {
    SEQUENCE.fetch_add(1, Ordering::Relaxed)
}

/// One timed span. Phases are closed in order by `mark` or added as measured durations by `add`; the span is
/// written when dropped if its wall time exceeds THRESHOLD_MS, or always when `force`d.
pub struct Span {
    kind: &'static str,
    label: String,
    begun: Instant,
    mono: f64,
    wall: f64,
    sched: Sched,
    last: Instant,
    phases: Vec<(&'static str, f64)>,
    notes: Map<String, Value>,
    forced: bool,
}
impl Span {
    pub fn begin(kind: &'static str, label: impl Into<String>) -> Self {
        let sched = Sched::now();
        let begun = Instant::now();
        Self {
            kind,
            label: label.into(),
            begun,
            mono: monotonic(),
            wall: crate::protocol::now_unix(),
            sched,
            last: begun,
            phases: Vec::new(),
            notes: Map::new(),
            forced: false,
        }
    }
    pub fn relabel(&mut self, label: impl Into<String>) {
        self.label = label.into();
    }
    /// Closes the phase since the previous mark (or the span's start) under `phase`.
    pub fn mark(&mut self, phase: &'static str) {
        let now = Instant::now();
        self.phases.push((phase, milliseconds(now - self.last)));
        self.last = now;
    }
    /// Adds a phase measured by the caller and restarts the next phase from now.
    pub fn add(&mut self, phase: &'static str, duration: std::time::Duration) {
        self.phases.push((phase, milliseconds(duration)));
        self.last = Instant::now();
    }
    pub fn note(&mut self, key: &str, value: impl Into<Value>) {
        self.notes.insert(key.to_owned(), value.into());
    }
    pub fn force(&mut self) {
        self.forced = true;
    }
}
impl Drop for Span {
    fn drop(&mut self) {
        let wall = milliseconds(self.begun.elapsed());
        if wall <= THRESHOLD_MS && !self.forced {
            return;
        }
        let sched = Sched::now().since(&self.sched);
        let phases: Vec<Value> = self.phases.iter().map(|(name, ms)| json!([name, round(*ms)])).collect();
        let line = json!({
            "kind": self.kind,
            "label": self.label,
            "mono_start": self.mono,
            "mono_end": self.mono + wall / 1e3,
            "wall_start": self.wall,
            "wall_ms": round(wall),
            "run_ms": round(sched.run_ms()),
            "run_delay_ms": round(sched.delay_ms()),
            "off_cpu_ms": round((wall - sched.run_ms() - sched.delay_ms()).max(0.0)),
            "tid": unsafe { libc::gettid() },
            "phases": phases,
            "notes": self.notes,
        });
        eprintln!("{PREFIX}{line}");
    }
}

fn round(ms: f64) -> f64 {
    (ms * 1000.0).round() / 1000.0
}

/// What a blocking-pool call cost: how long it waited for a pool thread after submission, the call itself, and the
/// pool thread's time on a CPU and runnable during the call.
#[derive(Clone, Copy, Default)]
pub struct Blocking {
    pub queued_ms: f64,
    pub call_ms: f64,
    pub run_ms: f64,
    pub run_delay_ms: f64,
}
/// Runs `call` on the current (blocking-pool) thread and measures it; `submitted` is when the actor handed it over.
pub fn blocking<T>(submitted: Instant, call: impl FnOnce() -> T) -> (T, Blocking) {
    let started = Instant::now();
    let before = Sched::now();
    let value = call();
    let after = Sched::now().since(&before);
    (
        value,
        Blocking {
            queued_ms: milliseconds(started - submitted),
            call_ms: milliseconds(started.elapsed()),
            run_ms: after.run_ms(),
            run_delay_ms: after.delay_ms(),
        },
    )
}
/// The worst of a batch of blocking calls, for one span's notes.
#[derive(Default)]
pub struct Worst {
    queued_ms: f64,
    call_ms: f64,
    run_delay_ms: f64,
    calls: usize,
}
impl Worst {
    pub fn add(&mut self, cost: &Blocking) {
        self.queued_ms = self.queued_ms.max(cost.queued_ms);
        self.call_ms = self.call_ms.max(cost.call_ms);
        self.run_delay_ms = self.run_delay_ms.max(cost.run_delay_ms);
        self.calls += 1;
    }
    pub fn note(&self, span: &mut Span, prefix: &str) {
        span.note(&format!("{prefix}_calls"), self.calls);
        span.note(&format!("{prefix}_queued_max_ms"), round(self.queued_ms));
        span.note(&format!("{prefix}_call_max_ms"), round(self.call_ms));
        span.note(&format!("{prefix}_run_delay_max_ms"), round(self.run_delay_ms));
    }
}
