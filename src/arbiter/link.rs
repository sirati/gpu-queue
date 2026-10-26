//! The control link to gpu-switch and the NInfer child. NInfer runs only
//! while the link is up, or for `link_grace_secs` after it drops (gpu-switch
//! restarting); the arbiter reconnects meanwhile and reports what runs.
//! After the grace NInfer is stopped. The proxy holds requests while not ready.

use std::os::unix::process::ExitStatusExt;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;
use tokio::sync::watch;
use tokio::time::{sleep, timeout, Instant};

use crate::arbiter::config::secs;
use crate::arbiter::state::Shared;
use crate::protocol::{ArbiterCmd, ArbiterEvent, Reply, Request};

/// The NInfer child and what it is doing, across links.
struct Ninfer {
    child: Option<Child>,
    phase: Phase,
}

pub async fn run(shared: Arc<Shared>) {
    let initial = secs(shared.config.reconnect_initial_secs);
    let max = secs(shared.config.reconnect_max_secs);
    let grace = secs(shared.config.link_grace_secs);
    let mut backoff = initial;
    let mut ninfer = Ninfer {
        child: None,
        phase: Phase::Idle,
    };
    let mut lost: Option<Instant> = None;
    loop {
        match connect(&shared).await {
            Ok((lines, mut w)) => {
                backoff = initial;
                eprintln!("gpu-arbiter: attached to gpu-switch");
                if report(&mut ninfer, &mut w).await {
                    session(&shared, &mut ninfer, lines, w).await;
                }
                eprintln!("gpu-arbiter: link to gpu-switch lost");
                lost = Some(Instant::now());
            }
            Err(e) => eprintln!("gpu-arbiter: {e}"),
        }
        let held = ninfer.child.is_some();
        if held && lost.is_some_and(|at| at.elapsed() >= grace) {
            // Fail-safe: no NInfer without a live grant.
            shared.set_ready(false);
            shared.abort_in_flight();
            if let Some(mut c) = ninfer.child.take() {
                terminate(&shared, &mut c).await;
            }
            ninfer.phase = Phase::Idle;
        }
        // While NInfer waits for gpu-switch to come back, retry quickly.
        let wait = match ninfer.child.is_some() {
            true => backoff.min(Duration::from_secs(1)),
            false => backoff,
        };
        sleep(wait).await;
        backoff = (backoff * 2).min(max);
    }
}

/// Tell a (restarted) gpu-switch what runs, first thing on a new link.
async fn report(ninfer: &mut Ninfer, w: &mut OwnedWriteHalf) -> bool {
    let exited = ninfer.child.as_ref().and_then(|c| c.exit.borrow().clone());
    let event = match (&ninfer.child, &ninfer.phase, exited) {
        (Some(_), _, Some(exit)) => {
            ninfer.child = None;
            ninfer.phase = Phase::Idle;
            ArbiterEvent::Failed {
                error: format!("NInfer exited while gpu-switch was away: {exit}"),
            }
        }
        (Some(c), Phase::Serving, None) => ArbiterEvent::Serving { pid: c.pid },
        (Some(c), _, None) => ArbiterEvent::Starting { pid: c.pid },
        (None, _, _) => ArbiterEvent::Stopped { pid: None },
    };
    write_line(w, &event).await.is_ok()
}

async fn connect(
    shared: &Shared,
) -> Result<(Lines<BufReader<OwnedReadHalf>>, OwnedWriteHalf), String> {
    let path = &shared.config.switch_socket;
    let s = UnixStream::connect(path)
        .await
        .map_err(|e| format!("connecting {}: {e}", path.display()))?;
    let (r, mut w) = s.into_split();
    let mut lines = BufReader::new(r).lines();
    write_line(&mut w, &Request::ArbiterAttach)
        .await
        .map_err(|e| e.to_string())?;
    let reply: Reply = match lines.next_line().await {
        Ok(Some(l)) => serde_json::from_str(&l).map_err(|e| e.to_string())?,
        _ => return Err("gpu-switch closed the link".into()),
    };
    if !reply.ok {
        return Err(reply.error.unwrap_or_else(|| "attach refused".into()));
    }
    Ok((lines, w))
}

async fn write_line<T: serde::Serialize>(w: &mut OwnedWriteHalf, v: &T) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(v).expect("serializable");
    line.push(b'\n');
    w.write_all(&line).await
}

struct Child {
    pid: u32,
    exit: watch::Receiver<Option<String>>,
}

impl Child {
    async fn exited(&mut self) -> String {
        loop {
            if let Some(e) = self.exit.borrow_and_update().clone() {
                return e;
            }
            if self.exit.changed().await.is_err() {
                return "lost".into();
            }
        }
    }
}

fn describe(status: ExitStatus) -> String {
    match (status.code(), status.signal()) {
        (Some(c), _) => format!("exit code {c}"),
        (None, Some(s)) => format!("signal {s}"),
        _ => "unknown status".into(),
    }
}

fn spawn(
    shared: &Arc<Shared>,
    profile: Option<&str>,
    host_cache: Option<u64>,
) -> Result<Child, String> {
    let cfg = &shared.config.ninfer;
    if !cfg.model.exists() {
        return Err(format!("model file missing: {}", cfg.model.display()));
    }
    let mut args = vec![cfg.model.display().to_string()];
    args.extend(cfg.args.iter().cloned());
    if let Some(p) = profile {
        let extra = cfg
            .profiles
            .get(p)
            .ok_or_else(|| format!("profile {p:?} is not configured in the arbiter"))?;
        args.extend(extra.iter().cloned());
    }
    args.extend([
        "--host".into(),
        "127.0.0.1".into(),
        "--port".into(),
        cfg.port.to_string(),
    ]);
    if let Some(mib) = host_cache {
        args.extend(["--host-context-mib".into(), mib.to_string()]);
    }
    eprintln!(
        "gpu-arbiter: starting {} {}",
        cfg.executable.display(),
        args.join(" ")
    );
    let mut cmd = tokio::process::Command::new(&cfg.executable);
    cmd.args(&args).stdin(Stdio::null()).process_group(0);
    // SAFETY: prctl is async-signal-safe.
    unsafe {
        cmd.pre_exec(|| {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            Ok(())
        });
    }
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("spawning {}: {e}", cfg.executable.display()))?;
    let pid = child.id().unwrap_or(0);
    shared.with(|i| i.ninfer_pid = Some(pid));
    let (tx, rx) = watch::channel(None);
    let owner = shared.clone();
    tokio::spawn(async move {
        let exit = match child.wait().await {
            Ok(s) => describe(s),
            Err(e) => format!("wait failed: {e}"),
        };
        eprintln!("gpu-arbiter: NInfer (pid {pid}) exited: {exit}");
        owner.with(|i| {
            if i.ninfer_pid == Some(pid) {
                i.ninfer_pid = None;
            }
        });
        let _ = tx.send(Some(exit));
    });
    Ok(Child { pid, exit: rx })
}

/// SIGTERM, then SIGKILL after the stop timeout.
async fn terminate(shared: &Shared, child: &mut Child) {
    // SAFETY: signal to our own child, not yet reaped.
    unsafe { libc::kill(child.pid as i32, libc::SIGTERM) };
    let limit = secs(shared.config.ninfer.stop_timeout_secs);
    if timeout(limit, child.exited()).await.is_err() {
        eprintln!("gpu-arbiter: NInfer ignored SIGTERM, sending SIGKILL");
        unsafe { libc::kill(child.pid as i32, libc::SIGKILL) };
        child.exited().await;
    }
}

async fn health_ok(port: u16) -> bool {
    let headers = hyper::HeaderMap::new();
    let req = crate::http::send_upstream(
        port,
        hyper::Method::GET,
        "/health",
        &headers,
        bytes::Bytes::new(),
    );
    matches!(timeout(Duration::from_secs(5), req).await, Ok(Ok(r)) if r.status() == hyper::StatusCode::OK)
}

#[derive(PartialEq, Eq)]
enum Phase {
    Idle,
    Starting(Instant),
    Serving,
}

async fn session(
    shared: &Arc<Shared>,
    ninfer: &mut Ninfer,
    mut lines: Lines<BufReader<OwnedReadHalf>>,
    mut w: OwnedWriteHalf,
) {
    let mut child = ninfer.child.take();
    let mut phase = std::mem::replace(&mut ninfer.phase, Phase::Idle);
    let poll = secs(shared.config.ninfer.health_poll_interval_secs);
    let port = shared.config.ninfer.port;
    loop {
        let exited = async {
            match child.as_mut() {
                Some(c) => c.exited().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            line = lines.next_line() => {
                let Ok(Some(line)) = line else { break };
                let cmd: ArbiterCmd = match serde_json::from_str(&line) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("gpu-arbiter: bad command: {e}");
                        continue;
                    }
                };
                match cmd {
                    ArbiterCmd::Start { profile, host_context_mib } => {
                        if let Some(c) = &child {
                            if phase == Phase::Serving {
                                let _ = write_line(&mut w, &ArbiterEvent::Serving { pid: c.pid }).await;
                            }
                            continue;
                        }
                        match spawn(shared, profile.as_deref(), host_context_mib) {
                            Ok(c) => {
                                let pid = c.pid;
                                child = Some(c);
                                phase = Phase::Starting(Instant::now() + secs(shared.config.ninfer.startup_timeout_secs));
                                if write_line(&mut w, &ArbiterEvent::Starting { pid }).await.is_err() {
                                    break;
                                }
                            }
                            Err(error) => {
                                eprintln!("gpu-arbiter: {error}");
                                if write_line(&mut w, &ArbiterEvent::Failed { error }).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    ArbiterCmd::Stop { drain_timeout_secs } => {
                        let pid = child.as_ref().map(|c| c.pid);
                        if !stop(shared, &mut child, &mut phase, drain_timeout_secs, &mut lines, &mut w).await {
                            break;
                        }
                        if write_line(&mut w, &ArbiterEvent::Stopped { pid }).await.is_err() {
                            break;
                        }
                    }
                }
            }
            exit = exited => {
                shared.set_ready(false);
                let error = match phase {
                    Phase::Starting(_) => format!("NInfer exited during startup: {exit}"),
                    _ => format!("NInfer exited unexpectedly: {exit}"),
                };
                child = None;
                phase = Phase::Idle;
                eprintln!("gpu-arbiter: {error}");
                if write_line(&mut w, &ArbiterEvent::Failed { error }).await.is_err() {
                    break;
                }
            }
            _ = sleep(poll), if matches!(phase, Phase::Starting(_)) => {
                let Phase::Starting(deadline) = phase else { continue };
                let pid = child.as_ref().map(|c| c.pid).unwrap_or(0);
                if health_ok(port).await {
                    phase = Phase::Serving;
                    shared.set_ready(true);
                    eprintln!("gpu-arbiter: NInfer serving");
                    if write_line(&mut w, &ArbiterEvent::Serving { pid }).await.is_err() {
                        break;
                    }
                } else if Instant::now() >= deadline {
                    if let Some(mut c) = child.take() {
                        terminate(shared, &mut c).await;
                    }
                    phase = Phase::Idle;
                    let error = format!(
                        "NInfer not healthy after {}s",
                        shared.config.ninfer.startup_timeout_secs
                    );
                    if write_line(&mut w, &ArbiterEvent::Failed { error }).await.is_err() {
                        break;
                    }
                }
            }
        }
    }
    // NInfer outlives the link for the grace (see run).
    ninfer.child = child;
    ninfer.phase = phase;
}

/// Drain, then terminate. False if the link dropped meanwhile (NInfer is
/// still stopped, without waiting for the drain).
async fn stop(
    shared: &Arc<Shared>,
    child: &mut Option<Child>,
    phase: &mut Phase,
    drain_timeout: Option<f64>,
    lines: &mut Lines<BufReader<OwnedReadHalf>>,
    w: &mut OwnedWriteHalf,
) -> bool {
    shared.set_ready(false);
    let mut link = true;
    if *phase == Phase::Serving {
        let _ = write_line(w, &ArbiterEvent::Draining).await;
        let drained = async {
            match drain_timeout {
                None => shared.wait_idle().await,
                Some(t) => {
                    if timeout(secs(t), shared.wait_idle()).await.is_err() {
                        eprintln!(
                            "gpu-arbiter: drain timeout after {t}s, aborting in-flight requests"
                        );
                        shared.abort_in_flight();
                        shared.wait_idle().await;
                    }
                }
            }
        };
        tokio::pin!(drained);
        loop {
            tokio::select! {
                _ = &mut drained => break,
                line = lines.next_line() => {
                    if !matches!(line, Ok(Some(_))) {
                        link = false;
                        shared.abort_in_flight();
                        break;
                    }
                }
            }
        }
    }
    if let Some(mut c) = child.take() {
        terminate(shared, &mut c).await;
    }
    *phase = Phase::Idle;
    link
}

/// Daemon shutdown: stop a running NInfer and wait for it.
pub async fn shutdown(shared: &Shared) {
    let Some(pid) = shared.with(|i| i.ninfer_pid) else {
        return;
    };
    shared.set_ready(false);
    // SAFETY: our own child.
    unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    let deadline = Instant::now() + secs(shared.config.ninfer.stop_timeout_secs);
    while shared.with(|i| i.ninfer_pid == Some(pid)) {
        if Instant::now() >= deadline {
            unsafe { libc::kill(pid as i32, libc::SIGKILL) };
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
}
