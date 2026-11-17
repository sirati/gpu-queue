//! Native transport shared by Rust callers and PyO3. No Python socket client.
use super::{endpoint, model::Group};
use serde_json::{json, Value};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::linux::net::SocketAddrExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

/// Why a request has no answer value.
#[derive(Debug)]
pub enum Failure {
    /// No service answered within the caller's patience, or it lacks what the caller needs: an outage of the
    /// authority, never its verdict.
    Unavailable(String),
    /// The service answered with an error.
    Refused(String),
}
impl Failure {
    pub fn reason(self) -> String {
        match self {
            Self::Unavailable(reason) | Self::Refused(reason) => reason,
        }
    }
}

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
        .map_err(Failure::reason)
    }
    /// The daemon answers once the awaited state is reached; the request is
    /// sent again only when a replaced daemon closes the connection unanswered.
    pub async fn wait(&self, request: Value) -> Result<Value, String> {
        self.exchange(request).await.map_err(Failure::reason)
    }
    /// An answer within `patience`, through service restarts; past it the service is unavailable.
    pub async fn within(&self, request: Value, patience: Duration) -> Result<Value, Failure> {
        tokio::time::timeout(patience, self.exchange(request))
            .await
            .unwrap_or_else(|_| {
                Err(Failure::Unavailable(format!(
                    "the compile service did not answer within {} s",
                    patience.as_secs()
                )))
            })
    }
    /// The service answers within `patience` and names every capability in `required`.
    pub async fn require(&self, required: &[&str], patience: Duration) -> Result<Value, Failure> {
        let health = self.within(json!({"op":"health"}), patience).await?;
        let offered = health["capabilities"].as_array().cloned().unwrap_or_default();
        match required.iter().find(|name| !offered.contains(&json!(name))) {
            Some(missing) => Err(Failure::Unavailable(format!(
                "the compile service lacks the {missing} capability; use the updated service"
            ))),
            None => Ok(health),
        }
    }
    async fn exchange(&self, mut request: Value) -> Result<Value, Failure> {
        request["protocol"] = json!(1);
        loop {
            let mut stream = connect(&self.state).await.map_err(Failure::Unavailable)?;
            let peer = stream
                .peer_cred()
                .map_err(|e| Failure::Unavailable(e.to_string()))?;
            if peer.uid() != unsafe { libc::getuid() } {
                return Err(Failure::Unavailable(
                    "compiler daemon belongs to another user".into(),
                ));
            }
            let mut bytes =
                serde_json::to_vec(&request).map_err(|e| Failure::Refused(e.to_string()))?;
            bytes.push(b'\n');
            if bytes.len() > 16 * 1024 * 1024 {
                return Err(Failure::Refused(
                    "compiler declaration exceeds protocol limit".into(),
                ));
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
                .map_err(|e| Failure::Refused(format!("invalid compiler response: {e}")))?;
            if answer["ok"] != true {
                return Err(Failure::Refused(
                    answer["error"]
                        .as_str()
                        .unwrap_or("compiler request failed")
                        .to_owned(),
                ));
            }
            return Ok(answer["value"].clone());
        }
    }
    pub async fn health(&self) -> Result<Value, String> {
        self.request(json!({"op":"health"})).await
    }
    pub async fn submit(&self, group: &Group, priority: i8) -> Result<Value, String> {
        group.validate()?;
        self.request(json!({"op":"submit_group","group":group,"priority":priority}))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_service_that_never_answers_is_unavailable_once_the_patience_passes() {
        let state = std::env::temp_dir().join(format!(
            "compiler-unanswered-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        std::fs::create_dir(&state).unwrap();
        let client = Client::new(&state, None).unwrap();
        let started = std::time::Instant::now();
        let answer = client
            .within(json!({"op":"health"}), Duration::from_millis(300))
            .await;
        assert!(matches!(answer, Err(Failure::Unavailable(_))), "{answer:?}");
        assert!(started.elapsed() < Duration::from_secs(2));
        let required = client.require(&["transfer"], Duration::from_millis(300)).await;
        assert!(matches!(required, Err(Failure::Unavailable(_))), "{required:?}");
        std::fs::remove_dir_all(state).unwrap();
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
