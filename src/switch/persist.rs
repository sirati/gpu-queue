//! gpu-switch's state on disk: written after changes and at shutdown, read
//! at start, so a restart (a rebuild switch) keeps the line and the grants.
//! Restored jobs are detached until their clients resume them.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::protocol::{JobSpec, JobState, Placement, Sig};
use crate::switch::config::secs;
use crate::switch::daemon::Daemon;
use crate::switch::state::*;

const VERSION: u32 = 1;
const FILE: &str = "state.json";
const DEBOUNCE: Duration = Duration::from_millis(100);

#[derive(Serialize, Deserialize)]
struct Saved {
    version: u32,
    boot_id: String,
    serve_wanted: bool,
    switch_requested: bool,
    restart_requested: bool,
    switch_to_ninfer: bool,
    profile_override: Option<String>,
    host_cache_override: Option<u64>,
    profile_current: Option<String>,
    host_cache_current: Option<u64>,
    ninfer: SavedNinfer,
    sides: Vec<SavedSide>,
}

#[derive(Serialize, Deserialize)]
struct SavedNinfer {
    phase: NPhase,
    key: Placement,
    ns_pid: Option<u32>,
    host_pid: Option<u32>,
    error: Option<String>,
    failed: bool,
    restart: bool,
}

#[derive(Serialize, Deserialize)]
struct SavedSide {
    side: Side,
    prefix: String,
    next_id: u64,
    jobs: Vec<SavedJob>,
}

#[derive(Serialize, Deserialize)]
struct SavedPolice {
    #[serde(default = "super::compute_watchdog::ComputeWatchdog::legacy")]
    compute_watchdog: super::compute_watchdog::ComputeWatchdog,
    #[serde(default)]
    shared_spent: f64,
    #[serde(default)]
    shared_sample_at: Option<u64>,
    #[serde(default)]
    allocated: bool,
    used_compute: bool,
    startup_since: Option<u64>,
    startup_fired: bool,
    idle_since: Option<u64>,
    idle_stage: u8,
    idle_stage_at: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct SavedJob {
    #[serde(default)]
    terminal_status: Option<crate::protocol::TerminalStatus>,
    #[serde(default)]
    timing: Option<super::timing::Timing>,
    id: u64,
    handle: String,
    spec: JobSpec,
    cmd: Vec<String>,
    label: Option<String>,
    group: Option<String>,
    lease: bool,
    token: String,
    cwd: String,
    log: String,
    state: JobState,
    exit_code: Option<i32>,
    signal: Option<i32>,
    error: Option<String>,
    runner_pid: u32,
    pid_inner: Option<u32>,
    pid_host: Option<u32>,
    submitted_at: f64,
    started_at: Option<f64>,
    finished_at: Option<f64>,
    key: Placement,
    requeued: bool,
    runs: u32,
    admitted_at: Option<u64>,
    yield_sent: bool,
    end_reason: Option<(JobState, String)>,
    police: SavedPolice,
    #[serde(default)]
    shutdown_grace_secs: Option<f64>,
    #[serde(default)]
    shutdown: Option<crate::protocol::ShutdownRequest>,
    #[serde(default)]
    process_started: Option<u64>,
    kill: Vec<(u64, Sig)>,
}

/// CLOCK_MONOTONIC instants: the same clock across a restart of this process.
struct Clock {
    now: Instant,
    now_ns: u64,
}

impl Clock {
    fn new() -> Clock {
        Clock {
            now: Instant::now(),
            now_ns: monotonic_ns(),
        }
    }

    fn ns(&self, at: Instant) -> u64 {
        match at.checked_duration_since(self.now) {
            Some(ahead) => self.now_ns + ahead.as_nanos() as u64,
            None => self
                .now_ns
                .saturating_sub((self.now - at).as_nanos() as u64),
        }
    }

    fn instant(&self, ns: u64) -> Instant {
        match ns.checked_sub(self.now_ns) {
            Some(ahead) => self.now + Duration::from_nanos(ahead),
            None => self
                .now
                .checked_sub(Duration::from_nanos(self.now_ns - ns))
                .unwrap_or(self.now),
        }
    }
}

pub fn path(dir: &Path) -> PathBuf {
    dir.join(FILE)
}

fn boot_id() -> String {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn saved(s: &State) -> Saved {
    let clock = Clock::new();
    let ns = |at: Option<Instant>| at.map(|i| clock.ns(i));
    Saved {
        version: VERSION,
        boot_id: boot_id(),
        serve_wanted: s.serve_wanted,
        switch_requested: s.switch_requested,
        restart_requested: s.restart_requested,
        switch_to_ninfer: s.switch_to_ninfer,
        profile_override: s.profile_override.clone(),
        host_cache_override: s.host_cache_override,
        profile_current: s.profile_current.clone(),
        host_cache_current: s.host_cache_current,
        ninfer: SavedNinfer {
            phase: s.ninfer.phase,
            key: s.ninfer.key,
            ns_pid: s.ninfer.ns_pid,
            host_pid: s.ninfer.host_pid,
            error: s.ninfer.error.clone(),
            failed: s.ninfer.failed,
            restart: s.ninfer.restart,
        },
        sides: s
            .sides
            .iter()
            .map(|(side, sj)| SavedSide {
                side: *side,
                prefix: sj.prefix.clone(),
                next_id: sj.next_id,
                jobs: sj
                    .jobs
                    .values()
                    .map(|j| SavedJob {
                        terminal_status: j.terminal_status,
                        id: j.id,
                        handle: j.handle.clone(),
                        spec: j.spec.clone(),
                        cmd: j.cmd.clone(),
                        label: j.label.clone(),
                        group: j.group.clone(),
                        lease: j.lease,
                        token: j.token.clone(),
                        cwd: j.cwd.clone(),
                        log: j.log.clone(),
                        state: j.state,
                        exit_code: j.exit_code,
                        signal: j.signal,
                        error: j.error.clone(),
                        runner_pid: j.runner_pid,
                        pid_inner: j.pid_inner,
                        pid_host: j.pid_host,
                        submitted_at: j.submitted_at,
                        started_at: j.started_at,
                        finished_at: j.finished_at,
                        key: j.key,
                        requeued: j.requeued,
                        runs: j.runs,
                        admitted_at: ns(j.admitted_at),
                        yield_sent: j.yield_sent,
                        end_reason: j.end_reason.clone(),
                        shutdown_grace_secs: j.shutdown_grace_secs,
                        shutdown: j.shutdown.clone(),
                        process_started: j.process_started,
                        police: SavedPolice {
                            compute_watchdog: j.police.compute_watchdog.clone(),
                            shared_spent: j.police.shared_spent,
                            shared_sample_at: ns(j.police.shared_sample_at),
                            allocated: j.police.allocated,
                            used_compute: j.police.used_compute,
                            startup_since: ns(j.police.startup_since),
                            startup_fired: j.police.startup_fired,
                            idle_since: ns(j.police.idle_since),
                            idle_stage: j.police.idle_stage,
                            idle_stage_at: ns(j.police.idle_stage_at),
                        },
                        timing: j.timing.clone(),
                        kill: j
                            .kill
                            .iter()
                            .flat_map(|k| k.steps.iter().map(|(at, sig)| (clock.ns(*at), *sig)))
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Atomically, so a crash mid-write leaves the previous state.
pub fn save(d: &Daemon) {
    let Some(dir) = d.state_dir() else {
        return;
    };
    let text = d.with(|s| serde_json::to_vec(&saved(s)).expect("serializable"));
    let target = path(dir);
    let temporary = dir.join(format!("{FILE}.tmp"));
    let written = std::fs::File::create(&temporary)
        .and_then(|mut f| f.write_all(&text).and_then(|()| f.sync_all()))
        .and_then(|()| std::fs::rename(&temporary, &target));
    if let Err(e) = written {
        eprintln!("gpu-switch: saving {}: {e}", target.display());
    }
}

/// Restore what an earlier gpu-switch of this boot saved.
pub fn restore(d: &Daemon) {
    let Some(dir) = d.state_dir() else {
        return;
    };
    let file = path(dir);
    let saved: Saved = match std::fs::read(&file).map(|t| serde_json::from_slice::<Saved>(&t)) {
        Ok(Ok(saved)) if saved.version == VERSION => saved,
        Ok(Ok(_)) | Ok(Err(_)) => {
            eprintln!(
                "gpu-switch: {} is unreadable; starting empty",
                file.display()
            );
            return;
        }
        Err(_) => return,
    };
    let grace = secs(d.config.reattach_grace_secs);
    let same_boot = saved.boot_id == boot_id();
    d.update(|s| apply(s, saved, same_boot, grace));
    eprintln!("gpu-switch: state restored from {}", file.display());
}

fn apply(s: &mut State, saved: Saved, same_boot: bool, grace: Duration) {
    let clock = Clock::new();
    let at = |ns: Option<u64>| ns.map(|n| clock.instant(n));
    s.serve_wanted = saved.serve_wanted;
    s.profile_override = saved.profile_override;
    s.host_cache_override = saved.host_cache_override;
    s.profile_current = saved.profile_current;
    s.host_cache_current = saved.host_cache_current;
    if !same_boot {
        return;
    }
    s.switch_requested = saved.switch_requested;
    s.restart_requested = saved.restart_requested;
    s.switch_to_ninfer = saved.switch_to_ninfer;
    s.left_gpu_at = Some(clock.now);
    let n = saved.ninfer;
    s.ninfer.phase = n.phase;
    s.ninfer.key = n.key;
    s.ninfer.ns_pid = n.ns_pid;
    s.ninfer.host_pid = n.host_pid;
    s.ninfer.error = n.error;
    s.ninfer.failed = n.failed;
    s.ninfer.restart = n.restart;
    s.ninfer.link_lost_at = Some(clock.now);
    s.ninfer.stopped_at = (n.phase == NPhase::Releasing).then_some(clock.now);
    let until = clock.now + grace;
    for side in saved.sides {
        let jobs = side
            .jobs
            .into_iter()
            .map(|j| {
                let job = Job {
                    terminal_status: j.terminal_status,
                    id: j.id,
                    handle: j.handle,
                    side: side.side,
                    spec: j.spec,
                    cmd: j.cmd,
                    label: j.label,
                    group: j.group,
                    lease: j.lease,
                    token: j.token,
                    cwd: j.cwd,
                    log: j.log,
                    state: j.state,
                    exit_code: j.exit_code,
                    signal: j.signal,
                    error: j.error,
                    runner_pid: j.runner_pid,
                    conn: Conn::Detached { until },
                    departing: false,
                    pid_inner: j.pid_inner,
                    pid_host: j.pid_host,
                    submitted_at: j.submitted_at,
                    started_at: j.started_at,
                    finished_at: j.finished_at,
                    key: j.key,
                    requeued: j.requeued,
                    runs: j.runs,
                    admitted_at: at(j.admitted_at),
                    yield_sent: j.yield_sent,
                    kill: (!j.kill.is_empty()).then(|| Kill {
                        steps: j
                            .kill
                            .iter()
                            .map(|(ns, sig)| (clock.instant(*ns), *sig))
                            .collect(),
                    }),
                    end_reason: j.end_reason,
                    shutdown_grace_secs: j.shutdown_grace_secs,
                    shutdown: j.shutdown,
                    process_started: j.process_started,
                    police: Police {
                        compute_watchdog: j.police.compute_watchdog,
                        shared_spent: j.police.shared_spent,
                        shared_sample_at: at(j.police.shared_sample_at),
                        allocated: j.police.allocated,
                        used_compute: j.police.used_compute,
                        startup_since: at(j.police.startup_since),
                        startup_fired: j.police.startup_fired,
                        idle_since: at(j.police.idle_since),
                        idle_stage: j.police.idle_stage,
                        idle_stage_at: at(j.police.idle_stage_at),
                    },
                    timing: j.timing,
                    reason: None,
                };
                (job.id, job)
            })
            .collect();
        s.sides.insert(
            side.side,
            SideJobs {
                prefix: side.prefix,
                next_id: side.next_id,
                jobs,
            },
        );
    }
}

/// Saves at most every [`DEBOUNCE`] while the state changes.
pub async fn run(d: Arc<Daemon>) {
    let mut rx = d.subscribe();
    loop {
        if rx.changed().await.is_err() {
            return;
        }
        tokio::time::sleep(DEBOUNCE).await;
        rx.borrow_and_update();
        save(&d);
    }
}
