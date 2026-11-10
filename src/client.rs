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
    exchange(connected(socket)?, req)
}

/// Like `request`, but a daemon that does not take the request, or does not answer, within `timeout` is an
/// error.
pub fn request_within(socket: &Path, req: &Request, timeout: Duration) -> Result<Reply, String> {
    let stream = connected(socket)?;
    stream
        .set_read_timeout(Some(timeout))
        .and_then(|()| stream.set_write_timeout(Some(timeout)))
        .map_err(|e| format!("gpu-switch connection: {e}"))?;
    exchange(stream, req)
}

fn connected(socket: &Path) -> Result<UnixStream, String> {
    UnixStream::connect(socket)
        .map_err(|e| format!("cannot reach gpu-switch at {}: {e}", socket.display()))
}

fn exchange(mut stream: UnixStream, req: &Request) -> Result<Reply, String> {
    let mut line = serde_json::to_vec(req).expect("serializable");
    line.push(b'\n');
    stream
        .write_all(&line)
        .map_err(|e| format!("writing to gpu-switch: {e}"))?;
    let mut reply = String::new();
    BufReader::new(stream)
        .read_line(&mut reply)
        .map_err(|e| format!("reading from gpu-switch: {e}"))?;
    if reply.is_empty() {
        return Err("gpu-switch closed the connection".into());
    }
    serde_json::from_str(&reply).map_err(|e| format!("bad reply from gpu-switch: {e}"))
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
        assert!(error.starts_with("reading from gpu-switch"), "{error}");
        drop(listening);
        std::fs::remove_file(&socket).unwrap();
        std::fs::remove_dir(&dir).unwrap();
    }
}
