//! gpu-switch from the caller's side, blocking: one request per connection,
//! and the submit path that starts a detached runner. Shared by the CLI and
//! the MCP server.

use std::ffi::OsString;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use crate::protocol::{JobSpec, OwnJob, Reply, Request, Status};
use crate::runner::SPEC_ENV;

pub fn request(socket: &Path, req: &Request) -> Result<Reply, String> {
    let mut stream = UnixStream::connect(socket).map_err(|e| unreachable(socket, e))?;
    stream
        .write_all(&line_of(req))
        .map_err(|e| format!("writing to gpu-switch: {e}"))?;
    let mut reply = String::new();
    BufReader::new(stream)
        .read_line(&mut reply)
        .map_err(|e| format!("reading from gpu-switch: {e}"))?;
    reply_of(&reply)
}

/// Like `request`, but connecting, sending and the whole answer share one deadline, `timeout` from now; a
/// daemon that does not accept (its backlog full) is an error at once. Not for a caller inside a tokio
/// runtime: it runs one of its own.
pub fn request_within(socket: &Path, req: &Request, timeout: Duration) -> Result<Reply, String> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|e| format!("gpu-switch connection: {e}"))?;
    runtime.block_on(async {
        let exchanged = async {
            let mut stream = tokio::net::UnixStream::connect(socket)
                .await
                .map_err(|e| unreachable(socket, e))?;
            stream
                .write_all(&line_of(req))
                .await
                .map_err(|e| format!("writing to gpu-switch: {e}"))?;
            let mut reply = String::new();
            tokio::io::BufReader::new(stream)
                .read_line(&mut reply)
                .await
                .map_err(|e| format!("reading from gpu-switch: {e}"))?;
            reply_of(&reply)
        };
        tokio::time::timeout(timeout, exchanged)
            .await
            .map_err(|_| {
                format!(
                    "gpu-switch gave no answer within {} s",
                    timeout.as_secs_f64()
                )
            })?
    })
}

fn unreachable(socket: &Path, error: std::io::Error) -> String {
    format!("cannot reach gpu-switch at {}: {error}", socket.display())
}

/// A request as the daemon reads it: one line of JSON.
fn line_of(req: &Request) -> Vec<u8> {
    let mut line = serde_json::to_vec(req).expect("serializable");
    line.push(b'\n');
    line
}

/// The daemon's answer line as a reply; no line at all means it closed the connection.
fn reply_of(line: &str) -> Result<Reply, String> {
    if line.is_empty() {
        return Err("gpu-switch closed the connection".into());
    }
    serde_json::from_str(line).map_err(|e| format!("bad reply from gpu-switch: {e}"))
}

pub fn checked(reply: Reply) -> Result<Reply, String> {
    if reply.ok {
        Ok(reply)
    } else {
        Err(reply.error.unwrap_or_else(|| "request failed".into()))
    }
}

pub fn get_status(socket: &Path) -> Result<Status, String> {
    status_of(request(socket, &Request::Status)?)
}

/// `get_status`, or an error when the daemon does not answer within `timeout`.
pub fn get_status_within(socket: &Path, timeout: Duration) -> Result<Status, String> {
    status_of(request_within(socket, &Request::Status, timeout)?)
}

fn status_of(reply: Reply) -> Result<Status, String> {
    checked(reply)?
        .status
        .ok_or_else(|| "reply without status".into())
}

pub fn find_job(socket: &Path, id: u64) -> Result<OwnJob, String> {
    get_status(socket)?
        .jobs
        .into_iter()
        .find(|j| j.id == id)
        .ok_or(format!("no job {id}"))
}

/// Queue `cmd` and return gpu-switch's reply. The runner is this binary
/// re-executed: env and umask carry over unchanged, cwd is `cwd` when given.
/// setsid detaches it from the caller's session and terminal.
pub fn submit(
    socket: &Path,
    spec: &JobSpec,
    cmd: &[OsString],
    cwd: Option<&Path>,
) -> Result<Reply, String> {
    submit_named(socket, spec, cmd, cwd, None)
}

pub fn submit_named(
    socket: &Path,
    spec: &JobSpec,
    cmd: &[OsString],
    cwd: Option<&Path>,
    name: Option<&str>,
) -> Result<Reply, String> {
    let mut runner = std::process::Command::new("/proc/self/exe");
    runner
        .arg0("gpu-run-runner")
        .arg("__runner")
        .arg(socket)
        .arg("--")
        .args(cmd)
        .env(SPEC_ENV, serde_json::to_string(spec).expect("serializable"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    runner.env_remove(crate::runner::NAME_ENV);
    if let Some(name) = name {
        runner.env(crate::runner::NAME_ENV, name);
    }
    if let Some(cwd) = cwd {
        runner.current_dir(cwd);
    }
    // SAFETY: setsid is async-signal-safe.
    unsafe {
        runner.pre_exec(|| {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = runner
        .spawn()
        .map_err(|e| format!("starting runner: {e}"))?;
    let mut out = String::new();
    // The runner closes its stdout once registered (or on failure).
    child
        .stdout
        .take()
        .expect("piped")
        .read_to_string(&mut out)
        .map_err(|e| format!("reading runner: {e}"))?;
    // A long-lived caller (the MCP server) reaps the runner when it ends.
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    let reply: Reply = serde_json::from_str(out.trim())
        .map_err(|_| "runner exited before registering with gpu-switch".to_string())?;
    checked(reply)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;
    use std::time::Instant;

    #[test]
    fn a_daemon_that_never_answers_is_an_error_within_the_timeout() {
        let dir = std::env::temp_dir().join(format!("client-silent-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let socket = dir.join("gpu-switch.sock");
        let listening = UnixListener::bind(&socket).unwrap();
        let started = Instant::now();
        let error = get_status_within(&socket, Duration::from_millis(200)).unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(error, "gpu-switch gave no answer within 0.2 s");
        drop(listening);
        std::fs::remove_file(&socket).unwrap();
        // A daemon that trickles an answer it never ends keeps no read alive past the deadline.
        let socket = dir.join("trickling.sock");
        let listening = UnixListener::bind(&socket).unwrap();
        let trickling = std::thread::spawn(move || {
            let (mut connection, _) = listening.accept().unwrap();
            let until = Instant::now() + Duration::from_secs(2);
            while Instant::now() < until && connection.write_all(b" ").is_ok() {
                std::thread::sleep(Duration::from_millis(20));
            }
            listening
        });
        let started = Instant::now();
        let error = get_status_within(&socket, Duration::from_millis(300)).unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(error, "gpu-switch gave no answer within 0.3 s");
        drop(trickling.join().unwrap());
        std::fs::remove_file(&socket).unwrap();
        std::fs::remove_dir(&dir).unwrap();
    }
}
