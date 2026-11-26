//! gpu-switch's state on disk: one task saves it while gpu-switch serves, after changes and when asked, and the
//! shutdown stops that task and saves last; read at start, so a restart (a rebuild switch) keeps the line and the
//! grants. Restored jobs are detached until their clients resume them.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, watch};

use crate::protocol::{JobSpec, JobState, Placement, Sig};
use crate::saved_file::{self, Optional};
use crate::switch::config::secs;
use crate::switch::daemon::Daemon;
use crate::switch::state::*;

const VERSION: u32 = 1;
const FILE: &str = "state.json";
pub(crate) const DEBOUNCE: Duration = Duration::from_millis(100);
/// How long what the polls observed may wait for a save.
const OBSERVED: Duration = Duration::from_secs(5);

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
    /// What the card's owner keeps across a restart; none from a gpu-switch before card losses.
    #[serde(default)]
    card: Option<super::card::SavedCard>,
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
    #[serde(default)]
    concluded: bool,
    #[serde(default)]
    reported: bool,
}

#[derive(Serialize, Deserialize)]
struct SavedJob {
    #[serde(default)]
    retention: Option<super::retention::Retention>,
    #[serde(default)]
    terminal_status: Option<crate::protocol::TerminalStatus>,
    #[serde(default)]
    violations: Vec<crate::protocol::Violation>,
    #[serde(default)]
    device_lost: Vec<crate::protocol::GrantLoss>,
    #[serde(default)]
    timing: Option<super::timing::Timing>,
    /// Kept only while it can still grow: a sealed job's figures are final and were reported, and every save
    /// rewrites the whole state.
    #[serde(default, skip_serializing_if = "super::telemetry::Record::is_empty")]
    telemetry: super::telemetry::Record,
    id: u64,
    handle: String,
    spec: JobSpec,
    /// Saved by gpu-switch builds before shares as none: the side's pool.
    #[serde(default)]
    share: Option<String>,
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
    #[serde(default)]
    place: Option<super::preparation::Place>,
    #[serde(default)]
    transferred_from: Option<crate::protocol::ProcessIdentity>,
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

/// What `d`'s state saves, in the boot of its incarnation.
fn saved(s: &State, boot_id: &str) -> Saved {
    let clock = Clock::new();
    let ns = |at: Option<Instant>| at.map(|i| clock.ns(i));
    Saved {
        version: VERSION,
        boot_id: boot_id.to_string(),
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
                        retention: j.retention.clone(),
                        terminal_status: j.terminal_status,
                        violations: j.violations.clone(),
                        device_lost: j.device_lost.clone(),
                        id: j.id,
                        handle: j.handle.clone(),
                        spec: j.spec.clone(),
                        share: Some(j.share.clone()),
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
                        place: j.place.clone(),
                        transferred_from: j.transferred_from,
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
                            concluded: j.police.concluded,
                            reported: j.police.reported,
                        },
                        timing: j.timing.clone(),
                        telemetry: j.telemetry.recording().cloned().unwrap_or_default(),
                        kill: j
                            .kill
                            .iter()
                            .flat_map(|k| k.steps.iter().map(|(at, sig)| (clock.ns(*at), *sig)))
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
        card: s.view.card.saved(),
    }
}

/// Draws each save's temporary name. With the pid, no two saves of one process and no two live processes of one pid
/// namespace draw the same name; `create_new` refuses a name that exists all the same.
static STAGED: AtomicU64 = AtomicU64::new(0);

/// One save's own temporary file beside the saved state, created exclusively, so no other save writes through it.
struct Staged {
    file: std::fs::File,
    path: PathBuf,
}

impl Staged {
    /// Creates this save's file in `dir`, under a name no other save uses.
    fn create(dir: &Path) -> std::io::Result<Staged> {
        let path = dir.join(format!("{FILE}.{}.{}.tmp", std::process::id(), STAGED.fetch_add(1, Ordering::Relaxed)));
        let file = std::fs::OpenOptions::new().write(true).create_new(true).open(&path)?;
        Ok(Staged { file, path })
    }

    /// Writes `bytes` through this save's own file, syncs it and renames it over `target`. A failure removes the file.
    fn publish(mut self, bytes: &[u8], target: &Path) -> std::io::Result<()> {
        let published = self
            .file
            .write_all(bytes)
            .and_then(|()| self.file.sync_all())
            .and_then(|()| std::fs::rename(&self.path, target));
        published.inspect_err(|_| {
            let _ = std::fs::remove_file(&self.path);
        })
    }
}

/// Atomically, so a crash mid-write leaves the previous state. Through a file of this save's own, claimed before the
/// state is read, so every save in flight holds a name in the directory.
pub(crate) fn save(d: &Daemon) {
    #[cfg(test)]
    super::service::shutdown::save_entry(d);
    let Some(dir) = d.state_dir() else {
        return;
    };
    let target = path(dir);
    let written = Staged::create(dir).and_then(|staged| {
        let text = d.with(|s| serde_json::to_vec(&saved(s, &d.incarnation.boot_id)).expect("serializable"));
        staged.publish(&text, &target)
    });
    if let Err(e) = written {
        eprintln!("gpu-switch: saving {}: {e}", target.display());
    }
}

/// Restore what an earlier gpu-switch of this boot saved. A saved state that cannot be read, does not decode or names
/// another version refuses the start, before gpu-switch serves or saves anything; the file stays as it is.
pub fn restore(d: &Daemon) -> Result<(), String> {
    let Some(dir) = d.state_dir() else {
        return Ok(());
    };
    let file = path(dir);
    let saved = match saved_file::read(&file).decode(&file, decoded) {
        Optional::Present(saved) => saved,
        Optional::Absent => {
            eprintln!("gpu-switch: no saved state at {}; starting empty", file.display());
            return Ok(());
        }
        Optional::Failed(error) => return Err(error),
    };
    let grace = secs(d.config.reattach_grace_secs);
    let same_boot = saved.boot_id == d.incarnation.boot_id;
    d.update(|s| apply(s, saved, same_boot, grace));
    eprintln!("gpu-switch: state restored from {}", file.display());
    Ok(())
}

/// The saved state, in the version this build writes.
fn decoded(bytes: Vec<u8>) -> Result<Saved, String> {
    let saved: Saved = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    match saved.version {
        VERSION => Ok(saved),
        other => Err(format!("version {other}; this gpu-switch reads version {VERSION}")),
    }
}

fn apply(s: &mut State, saved: Saved, same_boot: bool, grace: Duration) {
    let clock = Clock::new();
    let at = |ns: Option<u64>| ns.map(|n| clock.instant(n));
    s.serve_wanted = saved.serve_wanted;
    s.profile_override = saved.profile_override;
    s.host_cache_override = saved.host_cache_override;
    s.profile_current = saved.profile_current;
    s.host_cache_current = saved.host_cache_current;
    s.view.card.restore(saved.card, same_boot);
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
                    retention: j.retention,
                    terminal_status: j.terminal_status,
                    violations: j.violations,
                    device_lost: j.device_lost,
                    gpu_processes: Vec::new(),
                    id: j.id,
                    handle: j.handle,
                    side: side.side,
                    spec: j.spec,
                    share: j.share.unwrap_or_else(|| side.side.pool()),
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
                        concluded: j.police.concluded,
                        reported: j.police.reported,
                    },
                    timing: j.timing,
                    telemetry: super::telemetry::Telemetry::Recording(j.telemetry),
                    reason: None,
                    blocker: None,
                    place: j.place,
                    transferred_from: j.transferred_from,
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

/// Requests for a save now, and how far the ended saves reach. The daemon holds it; only this module reads it.
pub(crate) struct Saves {
    /// Bumped by each `durable` call.
    asked: watch::Sender<u64>,
    /// `asked` as the daemon was built, never read: the saver starts from a clone, so it sees every request since,
    /// also one made before it first ran.
    unanswered: watch::Receiver<u64>,
    /// The requests that the last ended save covered.
    ended: watch::Sender<u64>,
}

impl Saves {
    pub(crate) fn new() -> Saves {
        let (asked, unanswered) = watch::channel(0);
        Saves { asked, unanswered, ended: watch::channel(0).0 }
    }
}

/// Returns once a save that began after this call has ended: what changed before the call is in that save's snapshot,
/// and on disk unless that save failed, which it prints. The saver answers while gpu-switch serves; the last save
/// answers what it covers.
pub(crate) async fn durable(d: &Daemon) {
    let mut ask = 0;
    d.saves.asked.send_modify(|asked| {
        *asked += 1;
        ask = *asked;
    });
    let _ = d.saves.ended.subscribe().wait_for(|ended| *ended >= ask).await;
}

/// The one task that saves gpu-switch's state while it serves. `finish` ends it and writes the last save.
pub(crate) struct Saver {
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl Saver {
    pub(crate) fn spawn(d: Arc<Daemon>) -> Saver {
        let (stop, stopped) = oneshot::channel();
        Saver { stop, task: tokio::spawn(run(d, stopped)) }
    }

    /// Stops the saver, waits until a save it is writing has ended, then saves once more. Nothing saves after it.
    pub(crate) async fn finish(self, d: &Daemon) {
        let Saver { stop, task } = self;
        let _ = stop.send(());
        let _ = task.await;
        save_covering(d);
    }
}

/// One save, covering every `durable` request made before it began. `ended` advances when the save has ended, also
/// after a failed save, which `save` prints.
fn save_covering(d: &Daemon) {
    let covered = *d.saves.asked.borrow();
    save(d);
    d.saves.ended.send_replace(covered);
}

/// Saves at most every [`DEBOUNCE`] while the state changes.
/// Saves a change worth keeping `DEBOUNCE` after it, what the polls observed at most `OBSERVED` after it, and at once
/// when asked, until stopped. It starts from receivers kept since the daemon was built, so a change or a request made
/// before its first poll is seen at that poll. The stop is seen only between saves.
pub(crate) async fn run(d: Arc<Daemon>, mut stopped: oneshot::Receiver<()>) {
    let ((mut kept, mut any), mut asked) = (d.unsaved(), d.saves.unanswered.clone());
    loop {
        let first = tokio::select! {
            biased;
            _ = &mut stopped => return,
            changed = asked.changed() => changed.map(|()| Duration::ZERO),
            changed = kept.changed() => changed.map(|()| DEBOUNCE),
            changed = any.changed() => changed.map(|()| OBSERVED),
        };
        let Ok(wait) = first else {
            return;
        };
        let mut due = tokio::time::Instant::now() + wait;
        // A change worth keeping while observations wait brings the save forward; a request brings it to now.
        loop {
            tokio::select! {
                biased;
                _ = &mut stopped => return,
                _ = tokio::time::sleep_until(due) => break,
                changed = asked.changed() => match changed {
                    Ok(()) => due = tokio::time::Instant::now(),
                    Err(_) => return,
                },
                changed = kept.changed() => match changed {
                    Ok(()) => due = due.min(tokio::time::Instant::now() + DEBOUNCE),
                    Err(_) => return,
                },
            }
        }
        asked.borrow_and_update();
        kept.borrow_and_update();
        any.borrow_and_update();
        save_covering(&d);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Placement;
    use crate::saved_file::WriteOnly;
    use crate::switch::control::tests::{daemon, register, spec};
    use serde_json::json;
    use std::future::Future;
    use std::sync::mpsc;
    use std::task::{Context, Poll, Waker};

    /// One job as a restore must give it back: its side, id, token and key.
    type Kept = (Side, u64, String, Placement);

    /// `d`'s jobs, in the order the state walks its sides and their jobs.
    fn jobs(d: &Daemon) -> Vec<Kept> {
        d.with(|s| s.jobs().map(|j| (j.side, j.id, j.token.clone(), j.key)).collect::<Vec<_>>())
    }

    /// The jobs a saved snapshot holds, in file order, which is `jobs`'s order.
    fn saved_jobs(bytes: &[u8]) -> Vec<Kept> {
        let saved = serde_json::from_slice::<Saved>(bytes).expect("a whole snapshot");
        saved
            .sides
            .iter()
            .flat_map(|side| side.jobs.iter().map(|job| (side.side, job.id, job.token.clone(), job.key)))
            .collect()
    }

    /// The saves' temporary files in `dir`, by name, sorted.
    fn temporaries(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| name.starts_with(&format!("{FILE}.")) && name.ends_with(".tmp"))
            .collect();
        names.sort();
        names
    }

    /// The runtime of the tests that drive a saver: two workers, so one blocked in a held save leaves the timers
    /// running.
    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap()
    }

    /// One poll of `future` on this thread, with a waker that wakes nothing: the caller polls again.
    fn polled<F: Future>(future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    /// Whether `ready` holds within 10 s, asked every 20 ms on this thread.
    fn within(mut ready: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if ready() {
                return true;
            }
            if Instant::now() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// `d`'s state lock, held by a thread of the test until this guard drops, so every exit of the test ends the hold.
    struct Hold {
        release: Option<mpsc::Sender<()>>,
        holder: Option<std::thread::JoinHandle<()>>,
    }

    impl Hold {
        /// Holds `d`'s state lock on a thread of its own; returns once the lock is held.
        fn take(d: &Arc<Daemon>) -> Hold {
            let (held, holding) = mpsc::channel();
            let (release, released) = mpsc::channel::<()>();
            let holder = std::thread::spawn({
                let d = d.clone();
                move || {
                    d.with(|_| {
                        held.send(()).unwrap();
                        let _ = released.recv();
                    })
                }
            });
            let hold = Hold { release: Some(release), holder: Some(holder) };
            holding.recv_timeout(Duration::from_secs(10)).expect("the state lock is held within 10 s");
            hold
        }
    }

    impl Drop for Hold {
        fn drop(&mut self) {
            self.release.take();
            let _ = self.holder.take().map(std::thread::JoinHandle::join);
        }
    }

    /// What the thread that drives `finish` reports, in this order.
    #[derive(Debug, PartialEq)]
    enum Driven {
        Polling,
        FirstPoll { pending: bool },
        Done,
    }

    #[test]
    fn a_save_never_writes_through_a_file_another_save_published() {
        let (d, dir) = daemon(json!({}));
        register(&d, Side::Host(1000), spec(None));
        let older = d.with(|s| serde_json::to_vec(&saved(s, &d.incarnation.boot_id)).unwrap());
        register(&d, Side::Host(1000), spec(Some("kernel-a")));
        let newer = d.with(|s| serde_json::to_vec(&saved(s, &d.incarnation.boot_id)).unwrap());
        assert!(newer.len() > older.len(), "the newer snapshot is the longer one");
        let target = path(dir.path());
        let p = Staged::create(dir.path()).expect("1. p claims its file");
        let q = Staged::create(dir.path()).expect("1. q claims its file");
        q.publish(&newer, &target).expect("2. q publishes");
        assert_eq!(std::fs::read(&target).unwrap(), newer, "2. q's snapshot is published");
        let p_published = p.publish(&older, &target);
        let published = std::fs::read(&target).unwrap();
        assert!(
            published == older || published == newer,
            "4. the published file is one whole snapshot, not a mix of two: {} bytes, older {}, newer {}",
            published.len(),
            older.len(),
            newer.len()
        );
        assert_eq!(published, older, "4. the save published last holds the file");
        saved_jobs(&published);
        assert_eq!(p_published.map_err(|e| e.to_string()), Ok(()), "5. p published through its own file");
        assert_eq!(temporaries(dir.path()), Vec::<String>::new(), "5. no temporary file stays");
    }

    #[test]
    fn the_last_save_and_a_durable_request_wait_for_the_save_in_flight() {
        let (d, dir) = daemon(json!({}));
        register(&d, Side::Host(1000), spec(None));
        let rt = runtime();
        let hold = Hold::take(&d);
        let saver = {
            let _entered = rt.enter();
            Saver::spawn(d.clone())
        };
        assert!(
            within(|| temporaries(dir.path()).len() == 1),
            "3. the saver's save claims its temporary file within 10 s and blocks on the held state: {:?}",
            temporaries(dir.path())
        );
        let in_flight = temporaries(dir.path());
        let mut asked = std::pin::pin!(durable(&d));
        assert!(polled(asked.as_mut()).is_pending(), "4. a durable request waits: the save in flight began before it");
        let (report, reports) = mpsc::channel();
        let driver = std::thread::spawn({
            let d = d.clone();
            move || {
                let _ = report.send(Driven::Polling);
                let mut finish = std::pin::pin!(saver.finish(&d));
                let mut pending = polled(finish.as_mut()).is_pending();
                let _ = report.send(Driven::FirstPoll { pending });
                while pending {
                    std::thread::sleep(Duration::from_millis(20));
                    pending = polled(finish.as_mut()).is_pending();
                }
                let _ = report.send(Driven::Done);
            }
        });
        assert_eq!(reports.recv_timeout(Duration::from_secs(10)), Ok(Driven::Polling), "5. the driver of finish starts");
        let mut first_poll = Err(mpsc::TryRecvError::Empty);
        within(|| {
            first_poll = reports.try_recv();
            first_poll.is_ok() || temporaries(dir.path()) != in_flight
        });
        assert_eq!(temporaries(dir.path()), in_flight, "6 (a). no other save claims a file while the save in flight is held");
        assert_eq!(
            first_poll,
            Ok(Driven::FirstPoll { pending: true }),
            "6 (b). finish's first poll returned while the save in flight is held, pending on the saver's end"
        );
        assert!(polled(asked.as_mut()).is_pending(), "6 (c). the durable request still waits");
        drop(hold);
        assert_eq!(
            reports.recv_timeout(Duration::from_secs(10)),
            Ok(Driven::Done),
            "8 (a). finish ends within 10 s of the release"
        );
        assert!(within(|| polled(asked.as_mut()).is_ready()), "8 (b). the durable request is answered within 10 s");
        assert_eq!(saved_jobs(&std::fs::read(path(dir.path())).unwrap()), jobs(&d), "9. the last save holds the state");
        assert_eq!(temporaries(dir.path()), Vec::<String>::new(), "9. no temporary file stays");
        driver.join().unwrap();
    }

    #[test]
    fn a_durable_request_made_before_the_saver_first_runs_is_answered_with_nothing_else_published() {
        let (d, dir) = daemon(json!({}));
        let (kept, any) = d.unsaved();
        assert_eq!(
            (kept.has_changed().ok(), any.has_changed().ok()),
            (Some(false), Some(false)),
            "nothing was published since the daemon was built"
        );
        assert_eq!(
            std::fs::read(path(dir.path())).map_err(|e| e.kind()).err(),
            Some(std::io::ErrorKind::NotFound),
            "nothing is saved yet"
        );
        assert_eq!(temporaries(dir.path()), Vec::<String>::new());
        let rt = runtime();
        let mut asked = std::pin::pin!(durable(&d));
        assert!(polled(asked.as_mut()).is_pending(), "1. the request waits: no saver exists");
        let saver = {
            let _entered = rt.enter();
            Saver::spawn(d.clone())
        };
        assert!(
            within(|| polled(asked.as_mut()).is_ready()),
            "3. the saver answers a request made before it first ran, within 10 s"
        );
        assert_eq!(saved_jobs(&std::fs::read(path(dir.path())).unwrap()), Vec::new(), "4. one whole snapshot");
        assert_eq!(temporaries(dir.path()), Vec::<String>::new(), "4. no temporary file stays");
        rt.block_on(saver.finish(&d));
        assert_eq!(temporaries(dir.path()), Vec::<String>::new(), "5. no temporary file stays");
    }

    #[test]
    fn a_state_that_cannot_be_read_refuses_restore_and_keeps_every_job() {
        let (d, dir) = daemon(json!({}));
        register(&d, Side::Host(1000), spec(None));
        register(&d, Side::Host(1000), spec(Some("kernel-a")));
        save(&d);
        let file = path(dir.path());
        let kept = std::fs::read(&file).unwrap();
        let fault = WriteOnly::at(&file);
        let (successor, _a) = daemon(json!({"state_dir": dir.path()}));
        assert_eq!(
            restore(&successor),
            Err(format!("{}: Permission denied (os error 13)", file.display())),
            "1. a state that cannot be read refuses the restore"
        );
        assert_eq!(jobs(&successor), Vec::new(), "2. the refused restore applied nothing");
        fault.lift();
        assert_eq!(std::fs::read(&file).unwrap(), kept, "3. the file stays as it was");
        let (healthy, _b) = daemon(json!({"state_dir": dir.path()}));
        assert_eq!(restore(&healthy), Ok(()), "4. a healthy restart restores");
        assert_eq!(jobs(&healthy), jobs(&d), "5. it keeps every job");
    }

    #[test]
    fn a_state_that_does_not_decode_or_names_another_version_refuses_restore_and_stays() {
        let (d, dir) = daemon(json!({}));
        register(&d, Side::Host(1000), spec(None));
        save(&d);
        let file = path(dir.path());
        let whole = std::fs::read(&file).unwrap();
        let successor = || daemon(json!({"state_dir": dir.path()}));
        let half = whole[..whole.len() / 2].to_vec();
        std::fs::write(&file, &half).unwrap();
        let undecodable = serde_json::from_slice::<Saved>(&half).map(|_| ()).unwrap_err();
        let (a, _a) = successor();
        assert_eq!(
            restore(&a),
            Err(format!("{}: {undecodable}", file.display())),
            "1 (a). a state that does not decode refuses the restore"
        );
        assert_eq!(jobs(&a), Vec::new(), "1 (a). the refused restore applied nothing");
        assert_eq!(std::fs::read(&file).unwrap(), half, "1 (a). the file stays as it was");
        let mut other: serde_json::Value = serde_json::from_slice(&whole).unwrap();
        other["version"] = json!(VERSION + 1);
        let another = serde_json::to_vec(&other).unwrap();
        std::fs::write(&file, &another).unwrap();
        let (b, _b) = successor();
        assert_eq!(
            restore(&b),
            Err(format!("{}: version {}; this gpu-switch reads version {}", file.display(), VERSION + 1, VERSION)),
            "2 (b). a state of another version refuses the restore"
        );
        assert_eq!(jobs(&b), Vec::new(), "2 (b). the refused restore applied nothing");
        assert_eq!(std::fs::read(&file).unwrap(), another, "2 (b). the file stays as it was");
        std::fs::remove_file(&file).unwrap();
        let (c, _c) = successor();
        assert_eq!(restore(&c), Ok(()), "3 (c). an absent state starts empty");
        assert_eq!(jobs(&c), Vec::new(), "3 (c). nothing to restore");
    }
}
