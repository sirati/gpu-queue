//! Native transport shared by Rust callers and PyO3. No Python socket client.
use super::{endpoint, model::Group};
use serde_json::{json, Value};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::linux::net::SocketAddrExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

/// `deadline` bounds one request only for service control and probes. Scheduling
/// clients pass `None`: a busy or restarting daemon delays an answer, never fails it.
#[derive(Clone, Debug)]
pub struct Client {
    state: PathBuf,
    deadline: Option<Duration>,
}
impl Client {
    pub fn new(state: &Path, deadline: Option<Duration>) -> Result<Self, String> {
        if deadline.is_some_and(|limit| limit.is_zero()) {
            return Err("compiler request deadline must be positive".into());
        }
        let state = std::fs::canonicalize(state).map_err(|e| format!("compiler state: {e}"))?;
        Ok(Self { state, deadline })
    }
    pub fn state(&self) -> &Path {
        &self.state
    }
    pub async fn request(&self, request: Value) -> Result<Value, String> {
        match self.deadline {
            Some(limit) => tokio::time::timeout(limit, self.exchange(request))
                .await
                .map_err(|_| "compiler service request exceeded its probe deadline".to_owned())?,
            None => self.exchange(request).await,
        }
    }
    /// The daemon answers once the awaited state is reached; the request is
    /// sent again only when a replaced daemon closes the connection unanswered.
    pub async fn wait(&self, request: Value) -> Result<Value, String> {
        self.exchange(request).await
    }
    async fn exchange(&self, mut request: Value) -> Result<Value, String> {
        request["protocol"] = json!(1);
        loop {
            let mut stream = connect(&self.state).await?;
            let peer = stream.peer_cred().map_err(|e| e.to_string())?;
            if peer.uid() != unsafe { libc::getuid() } {
                return Err("compiler daemon belongs to another user".into());
            }
            let mut bytes = serde_json::to_vec(&request).map_err(|e| e.to_string())?;
            bytes.push(b'\n');
            if bytes.len() > 16 * 1024 * 1024 {
                return Err("compiler declaration exceeds protocol limit".into());
            }
            if stream.write_all(&bytes).await.is_err() {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            let mut line = String::new();
            let read = BufReader::new(stream)
                .take(16 * 1024 * 1024)
                .read_line(&mut line)
                .await;
            if !matches!(read,Ok(size) if size > 0) {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            let answer: Value = serde_json::from_str(&line)
                .map_err(|e| format!("invalid compiler response: {e}"))?;
            if answer["ok"] != true {
                return Err(answer["error"]
                    .as_str()
                    .unwrap_or("compiler request failed")
                    .to_owned());
            }
            return Ok(answer["value"].clone());
        }
    }
    pub async fn health(&self) -> Result<Value, String> {
        self.request(json!({"op":"health"})).await
    }
    pub async fn submit(&self, group: &Group) -> Result<Value, String> {
        group.validate()?;
        self.request(json!({"op":"submit_group","group":group}))
            .await
    }
    pub async fn status(&self, group: &str) -> Result<Value, String> {
        self.request(json!({"op":"group_status","group_id":group}))
            .await
    }
    pub async fn wait_group(&self, group: &str) -> Result<Value, String> {
        self.wait(json!({"op":"group_wait","group_id":group})).await
    }
    pub async fn cancel(
        &self,
        group: &str,
        job: Option<&str>,
        reason: &str,
        include_running: bool,
    ) -> Result<Value, String> {
        if reason.trim().is_empty() {
            return Err("cancellation requires a reason".into());
        }
        self.request(json!({"op":"cancel","group_id":group,"job_id":job,"reason":reason,"include_running":include_running})).await
    }
}

async fn connect(state: &Path) -> Result<tokio::net::UnixStream, String> {
    let address = endpoint(state)?;
    let name = address
        .as_abstract_name()
        .ok_or("expected abstract compiler endpoint")?;
    let mut raw: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    raw.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (slot, byte) in raw.sun_path[1..].iter_mut().zip(name) {
        *slot = *byte as libc::c_char;
    }
    let length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + 1 + name.len();
    loop {
        let fd = unsafe {
            libc::socket(
                libc::AF_UNIX,
                libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let connected = unsafe {
            libc::connect(
                fd.as_raw_fd(),
                &raw as *const _ as *const libc::sockaddr,
                length as libc::socklen_t,
            )
        };
        if connected == 0 {
            return tokio::net::UnixStream::from_std(std::os::unix::net::UnixStream::from(fd))
                .map_err(|e| e.to_string());
        }
        let error = std::io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EAGAIN | libc::ECONNREFUSED | libc::ENOENT) => {
                // A full Unix accept queue has no pending connect. Retry with a
                // fresh fd, yielding to Tokio and the enclosing request deadline.
                drop(fd);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Some(libc::EINPROGRESS) => {
                let stream =
                    tokio::net::UnixStream::from_std(std::os::unix::net::UnixStream::from(fd))
                        .map_err(|e| e.to_string())?;
                stream.writable().await.map_err(|e| e.to_string())?;
                if let Some(error) = stream.take_error().map_err(|e| e.to_string())? {
                    return Err(error.to_string());
                }
                return Ok(stream);
            }
            _ => return Err(format!("compiler daemon connection: {error}")),
        }
    }
}
