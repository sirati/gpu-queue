//! gpu-switch from the caller's side, blocking: one request per connection,
//! and the submit path that starts a detached runner. Shared by the CLI and
//! the MCP server.

use std::ffi::OsString;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::Stdio;

use crate::protocol::{JobSpec, OwnJob, Reply, Request, Status};
use crate::runner::SPEC_ENV;

pub fn request(socket: &Path, req: &Request) -> Result<Reply, String> {
    let mut stream = UnixStream::connect(socket)
        .map_err(|e| format!("cannot reach gpu-switch at {}: {e}", socket.display()))?;
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
    checked(request(socket, &Request::Status)?)?
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
