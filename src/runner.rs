//! Detached job runner (`gpu-run __runner SOCKET -- CMD...`, spec in
//! `GPU_RUN_SPEC`, removed before the job sees the environment). Spawned by
//! `gpu-run submit` with the caller's environment, cwd and umask, in its own
//! session, stdout a pipe back to the CLI. It runs as the submitter and does
//! everything that needs the submitter's rights: the log file, signals to
//! the job's process group, the yield file.
//!
//! The job gets `GPU_SWITCH_GRANTED=<id>`: a gpu-switch lease client inside
//! it (tools/_gpu_switch.py) takes no grant of its own, which would wait
//! behind this very job. It also gets a private ml-project lock
//! (`ML_GPU_LOCK`, plus `ML_GPU_LEASE_MODE`/`ML_GPU_VRAM` matching the
//! submit): Python inside the job is alone on it and passes at once, instead
//! of waiting on the caller's lock, which another process may hold while it
//! waits for gpu-switch behind this job.

use std::ffi::OsString;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{ExitCode, Stdio};

use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;
use tokio::time::{sleep, sleep_until};

use crate::protocol::{JobSpec, Mode, Reply, Request, RunnerEvent, RunnerReport, SubmitRequest};

pub const NAME_ENV: &str = "GPU_RUN_NAME";
pub const SPEC_ENV: &str = "GPU_RUN_SPEC";

/// Between the SIGTERM a run-time limit sends and the SIGKILL that follows.
pub const TIMEOUT_GRACE: Duration = Duration::from_secs(10);

/// `${XDG_STATE_HOME:-$HOME/.local/state}`
pub fn state_home() -> PathBuf {
    match std::env::var_os("XDG_STATE_HOME").filter(|v| !v.is_empty()) {
        Some(d) => PathBuf::from(d),
        None => PathBuf::from(std::env::var_os("HOME").unwrap_or_else(|| "/".into()))
            .join(".local/state"),
    }
}

fn private_lock_dir(id: u64) -> PathBuf {
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .filter(|p| p.is_dir())
        .unwrap_or_else(state_home);
    base.join("gpu-run").join(id.to_string())
}

pub fn main(args: Vec<OsString>) -> ExitCode {
    let spec: Option<JobSpec> = std::env::var(SPEC_ENV)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());
    std::env::remove_var(SPEC_ENV);
    let (socket, cmd, spec) = match (args.as_slice(), spec) {
        ([socket, dashes, cmd @ ..], Some(spec)) if dashes == "--" && !cmd.is_empty() => {
            (PathBuf::from(socket), cmd.to_vec(), spec)
        }
        _ => {
            report_to_cli(&Reply::error("runner: bad arguments"));
            return ExitCode::from(2);
        }
    };
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            report_to_cli(&Reply::error(format!("runner: tokio runtime: {e}")));
            return ExitCode::FAILURE;
        }
    };
    rt.block_on(run(socket, cmd, spec))
}

/// Write the submit result to the CLI, then detach stdout from its pipe.
fn report_to_cli(reply: &Reply) {
    let mut line = serde_json::to_string(reply).expect("serializable");
    line.push('\n');
    // SAFETY: raw fd writes and dup2 on fds this process owns.
    unsafe {
        libc::write(1, line.as_ptr().cast(), line.len());
        let null = libc::open(c"/dev/null".as_ptr(), libc::O_WRONLY);
        if null >= 0 {
            libc::dup2(null, 1);
            libc::close(null);
        }
    }
}

async fn send(w: &mut tokio::net::unix::OwnedWriteHalf, report: &RunnerReport) -> bool {
    let mut line = serde_json::to_vec(report).expect("serializable");
    line.push(b'\n');
    w.write_all(&line).await.is_ok()
}

fn signal_group(pgid: i32, sig: i32) {
    if pgid > 0 {
        // SAFETY: signals our own child's process group.
        unsafe { libc::kill(-pgid, sig) };
    }
}

/// A line of the runner's own in the job's log, for whoever reads it later.
fn note_in_log(path: &str, line: &str) {
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new().append(true).open(path) {
        let _ = writeln!(f, "{line}");
    }
}

/// A deadline for `sleep_until`; a branch without one is disabled by its guard.
fn at(t: Option<Instant>) -> tokio::time::Instant {
    tokio::time::Instant::from_std(t.unwrap_or_else(|| Instant::now() + Duration::from_secs(86400)))
}

fn open_log(path: &Path, first: bool) -> std::io::Result<std::fs::File> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(!first)
        .truncate(first)
        .open(path)
}

/// How long the runner waits for a restarted gpu-switch.
const RESUME_PATIENCE: Duration = Duration::from_secs(600);

/// The runner's session with gpu-switch, resumed when gpu-switch restarts.
struct Link {
    socket: PathBuf,
    submit: SubmitRequest,
    lines: Lines<BufReader<OwnedReadHalf>>,
    w: OwnedWriteHalf,
}

/// What the session brings next.
enum LinkEvent {
    Line(String),
    /// gpu-switch came back and holds the job again.
    Resumed,
    /// gpu-switch is gone or refused the job.
    Gone,
}

type Opened = (Lines<BufReader<OwnedReadHalf>>, OwnedWriteHalf, Reply);

impl Link {
    async fn open(socket: &Path, submit: &SubmitRequest) -> Result<Opened, String> {
        let stream = UnixStream::connect(socket)
            .await
            .map_err(|e| format!("cannot reach gpu-switch at {}: {e}", socket.display()))?;
        let (r, mut w) = stream.into_split();
        let mut lines = BufReader::new(r).lines();
        let mut line = serde_json::to_vec(&Request::Submit(submit.clone())).expect("serializable");
        line.push(b'\n');
        w.write_all(&line)
            .await
            .map_err(|e| format!("writing to gpu-switch: {e}"))?;
        let reply = match lines.next_line().await {
            Ok(Some(l)) => serde_json::from_str(&l)
                .unwrap_or_else(|e| Reply::error(format!("bad reply from gpu-switch: {e}"))),
            _ => return Err("gpu-switch closed the connection".into()),
        };
        Ok((lines, w, reply))
    }

    async fn resume(&mut self, running: bool) -> bool {
        if let Some(resume) = &mut self.submit.resume {
            resume.running = running;
        }
        let deadline = Instant::now() + RESUME_PATIENCE;
        let mut wait = Duration::from_millis(50);
        loop {
            match Link::open(&self.socket, &self.submit).await {
                Ok((lines, w, reply)) if reply.ok => {
                    self.lines = lines;
                    self.w = w;
                    self.submit.resume = reply.resume.or(self.submit.resume.take());
                    return true;
                }
                Ok((_, _, reply)) => {
                    eprintln!("gpu-run: gpu-switch refused the job: {:?}", reply.error);
                    return false;
                }
                Err(_) if Instant::now() < deadline => {
                    sleep(wait).await;
                    wait = (wait * 2).min(Duration::from_secs(1));
                }
                Err(_) => return false,
            }
        }
    }

    async fn event(&mut self, running: bool) -> LinkEvent {
        match self.lines.next_line().await {
            Ok(Some(line)) => LinkEvent::Line(line),
            _ => match self.resume(running).await {
                true => LinkEvent::Resumed,
                false => LinkEvent::Gone,
            },
        }
    }

    async fn send(&mut self, report: &RunnerReport) -> bool {
        send(&mut self.w, report).await
    }
}

async fn run(socket: PathBuf, cmd: Vec<OsString>, spec: JobSpec) -> ExitCode {
    let name = std::env::var(NAME_ENV).ok();
    std::env::remove_var(NAME_ENV);
    let submit = SubmitRequest {
        name,
        cmd: cmd
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect(),
        cwd: std::env::current_dir()
            .map(|d| d.display().to_string())
            .unwrap_or_default(),
        spec: spec.clone(),
        log_dir: state_home().join("gpu-run").display().to_string(),
        resume: None,
    };
    let (lines, w, reply) = match Link::open(&socket, &submit).await {
        Ok(opened) => opened,
        Err(e) => {
            report_to_cli(&Reply::error(e));
            return ExitCode::FAILURE;
        }
    };
    report_to_cli(&reply);
    let Some(job) = reply.job.filter(|_| reply.ok) else {
        return ExitCode::FAILURE;
    };
    let mut link = Link {
        socket,
        submit: SubmitRequest {
            resume: reply.resume,
            ..submit
        },
        lines,
        w,
    };
    let lock_dir = private_lock_dir(job.id);
    let yield_file = lock_dir.join("yield");
    let mut runs = 0u32;
    // Running time spent by earlier runs of a cooperative job.
    let mut used = Duration::ZERO;
    loop {
        // Wait for go (also again after a cooperative yield).
        loop {
            let ev = match link.event(false).await {
                LinkEvent::Line(l) => serde_json::from_str::<RunnerEvent>(&l).ok(),
                LinkEvent::Resumed => continue,
                LinkEvent::Gone => {
                    let _ = std::fs::remove_dir_all(&lock_dir);
                    return ExitCode::FAILURE;
                }
            };
            match ev {
                Some(RunnerEvent::Go) => break,
                Some(RunnerEvent::Cancel | RunnerEvent::Done) => {
                    let _ = std::fs::remove_dir_all(&lock_dir);
                    return ExitCode::SUCCESS;
                }
                _ => {}
            }
        }
        runs += 1;
        let _ = std::fs::remove_file(&yield_file);
        let spawned = (|| -> std::io::Result<tokio::process::Child> {
            std::fs::create_dir_all(&lock_dir)?;
            let log = open_log(Path::new(&job.log), runs == 1)?;
            let err = log.try_clone()?;
            let mut c = tokio::process::Command::new(&cmd[0]);
            c.args(&cmd[1..])
                .stdin(Stdio::null())
                .stdout(log)
                .stderr(err)
                .process_group(0)
                .env("GPU_SWITCH_GRANTED", job.id.to_string())
                .env("ML_GPU_LOCK", lock_dir.join("ml-gpu.lock"))
                .env("ML_GPU_LEASE_MODE", spec.mode.as_str())
                .env(
                    "ML_GPU_VRAM",
                    if spec.mode == Mode::Shared {
                        spec.vram
                    } else {
                        0
                    }
                    .to_string(),
                );
            if spec.cooperative {
                c.env("ML_GPU_YIELD", &yield_file);
            }
            c.spawn()
        })();
        let mut child = match spawned {
            Ok(c) => c,
            Err(e) => {
                let error = format!("spawning {}: {e}", cmd[0].to_string_lossy());
                link.send(&RunnerReport::SpawnFailed { error }).await;
                let _ = std::fs::remove_dir_all(&lock_dir);
                return ExitCode::FAILURE;
            }
        };
        let pgid = child.id().unwrap_or(0) as i32;
        let started = RunnerReport::Started { pid: pgid as u32 };
        let mut connected = link.send(&started).await;
        let mut killed = false;
        let run_started = Instant::now();
        let mut term_at = spec
            .max_runtime_secs
            .map(|s| run_started + Duration::from_secs(s).saturating_sub(used));
        let mut kill_at: Option<Instant> = None;
        let mut timed_out = false;
        let status = loop {
            tokio::select! {
                status = child.wait() => break status,
                _ = sleep_until(at(term_at)), if term_at.is_some() => {
                    timed_out = true;
                    term_at = None;
                    kill_at = Some(Instant::now() + TIMEOUT_GRACE);
                    note_in_log(
                        &job.log,
                        &format!(
                            "gpu-run: run-time limit of {}s reached; sending SIGTERM",
                            spec.max_runtime_secs.unwrap_or(0)
                        ),
                    );
                    signal_group(pgid, libc::SIGTERM);
                }
                _ = sleep_until(at(kill_at)), if kill_at.is_some() => {
                    kill_at = None;
                    killed = true;
                    note_in_log(
                        &job.log,
                        "gpu-run: still running after the grace period; sending SIGKILL",
                    );
                    signal_group(pgid, libc::SIGKILL);
                }
                ev = link.event(true), if connected => match ev {
                    LinkEvent::Line(l) => match serde_json::from_str::<RunnerEvent>(&l) {
                        Ok(RunnerEvent::Signal { sig }) => {
                            killed |= sig == crate::protocol::Sig::Kill;
                            signal_group(pgid, sig.number());
                        }
                        Ok(RunnerEvent::Yield) => {
                            let _ = std::fs::write(&yield_file, b"");
                        }
                        _ => {}
                    },
                    // A gpu-switch that lost the job learns its pid again.
                    LinkEvent::Resumed => connected = link.send(&started).await,
                    LinkEvent::Gone => {
                        connected = false;
                        killed = true;
                        note_in_log(&job.log, "gpu-run: GPU queue authority lost; terminating the command");
                        signal_group(pgid, libc::SIGTERM);
                        kill_at = Some(Instant::now() + Duration::from_secs(10));
                    },
                },
            }
        };
        used += run_started.elapsed();
        killed |= timed_out;
        if killed {
            // Nothing of the group may outlive a kill.
            signal_group(pgid, libc::SIGKILL);
        }
        // Before reporting: whoever waits on the job may look at once.
        let _ = std::fs::remove_dir_all(&lock_dir);
        let (exit_code, signal) = match &status {
            Ok(s) => (s.code(), s.signal()),
            Err(_) => (None, None),
        };
        let finished = RunnerReport::Finished { exit_code, signal };
        while connected && !link.send(&finished).await {
            connected = link.resume(true).await;
        }
        if !connected {
            return ExitCode::SUCCESS;
        }
    }
}
