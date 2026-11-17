//! Native transport shared by Rust callers and PyO3. No Python socket client. It owns all transport-failure
//! detection: framing, peer validation, parsing, retries and the deadlines its callers' contexts supply.
use super::{endpoint, model::Group};
use crate::outage::Outage;
use serde_json::{json, Value};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::linux::net::SocketAddrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

/// How long the compile service may leave a caller unanswered before the caller reports the authority
/// unavailable, an outage rather than a verdict. It outlasts a restart of the service.
pub const AUTHORITY_PATIENCE: Duration = Duration::from_secs(60);
/// The authority every outage of this transport names.
const SOURCE: &str = "compile service";
/// A request or an answer is one line of at most this many bytes.
const PROTOCOL_LIMIT: usize = 16 * 1024 * 1024;
/// The pause before a lost connection or a failed write is tried again.
const RETRY: Duration = Duration::from_millis(50);

/// An absolute point on the monotonic clock by which an exchange must be answered. The caller's context
/// supplies it, so an operation's name never selects its patience.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Deadline(Instant);
impl Deadline {
    pub fn after(limit: Duration) -> Self {
        Self(Instant::now() + limit)
    }
    /// A scheduling snapshot's deadline: the whole exchange within `AUTHORITY_PATIENCE` from now.
    pub fn snapshot() -> Self {
        Self::after(AUTHORITY_PATIENCE)
    }
    pub fn at(self) -> Instant {
        self.0
    }
    pub fn remaining(self) -> Duration {
        self.0.saturating_duration_since(Instant::now())
    }
}

/// Why a request has no answer value.
#[derive(Debug)]
pub enum Failure {
    /// No service answered by the caller's deadline, or it lacks what the caller needs: an outage of the
    /// authority, never its verdict.
    Unavailable(Outage),
    /// The service answered with an error.
    Refused(String),
    /// The operation was invalid before anything was sent.
    Invalid(String),
}
impl std::fmt::Display for Failure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(outage) => outage.fmt(formatter),
            Self::Refused(reason) | Self::Invalid(reason) => formatter.write_str(reason),
        }
    }
}
fn unavailable(operation: &str, reason: impl Into<String>) -> Failure {
    Failure::Unavailable(Outage::new(SOURCE, operation, reason))
}

/// One request as it crosses the socket: the operation that names its outages, and its framed line.
struct Framed {
    operation: String,
    bytes: Vec<u8>,
}
impl Framed {
    fn of(mut request: Value) -> Result<Self, Failure> {
        request["protocol"] = json!(1);
        let operation = request["op"].as_str().unwrap_or_default().to_owned();
        let mut bytes = serde_json::to_vec(&request).map_err(|e| Failure::Invalid(e.to_string()))?;
        bytes.push(b'\n');
        if bytes.len() > PROTOCOL_LIMIT {
            return Err(Failure::Invalid(
                "compiler declaration exceeds protocol limit".into(),
            ));
        }
        Ok(Self { operation, bytes })
    }
    fn unavailable(&self, reason: impl Into<String>) -> Failure {
        unavailable(&self.operation, reason)
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    state: PathBuf,
}
impl Client {
    /// The client of the service whose state directory is `state`. A missing state directory is an outage: no
    /// service has run there.
    pub fn new(state: &Path) -> Result<Self, Failure> {
        let state = std::fs::canonicalize(state)
            .map_err(|e| unavailable("state_dir", format!("compiler state: {e}")))?;
        Ok(Self { state })
    }
    pub fn state(&self) -> &Path {
        &self.state
    }
    /// One answer by `deadline`, through service restarts: the whole exchange, connection and retries
    /// included. Past it the service is unavailable; once it has passed, nothing is sent.
    pub async fn request(&self, request: Value, deadline: Deadline) -> Result<Value, Failure> {
        let framed = Framed::of(request)?;
        let limit = deadline.remaining();
        if limit.is_zero() {
            return Err(framed.unavailable(format!(
                "the deadline of {} had passed, so nothing was sent",
                framed.operation
            )));
        }
        tokio::time::timeout_at(deadline.at().into(), self.exchange(&framed))
            .await
            .unwrap_or_else(|_| {
                Err(framed.unavailable(format!(
                    "the compile service did not answer {} within {limit:.1?}",
                    framed.operation
                )))
            })
    }
    /// The daemon answers once the awaited state is reached; the request is
    /// sent again only when a replaced daemon closes the connection unanswered.
    pub async fn wait(&self, request: Value) -> Result<Value, Failure> {
        self.exchange(&Framed::of(request)?).await
    }
    /// The service answers by `deadline` and names every capability in `required`.
    pub async fn require(&self, required: &[&str], deadline: Deadline) -> Result<Value, Failure> {
        let health = self.health(deadline).await?;
        let offered = health["capabilities"].as_array().cloned().unwrap_or_default();
        match required.iter().find(|name| !offered.contains(&json!(name))) {
            Some(missing) => Err(unavailable(
                "health",
                format!("the compile service lacks the {missing} capability; use the updated service"),
            )),
            None => Ok(health),
        }
    }
    async fn exchange(&self, framed: &Framed) -> Result<Value, Failure> {
        loop {
            let stream = self.deliver(framed).await?;
            match answer(stream).await? {
                Some(value) => return Ok(value),
                None => tokio::time::sleep(RETRY).await,
            }
        }
    }
    /// The connection on which the whole framed request was written: connections that fail and writes that
    /// fail are tried again; a peer of another user is never written to.
    async fn deliver(&self, framed: &Framed) -> Result<tokio::net::UnixStream, Failure> {
        loop {
            let mut stream = connect(&self.state).await.map_err(|e| framed.unavailable(e))?;
            let peer = stream
                .peer_cred()
                .map_err(|e| framed.unavailable(e.to_string()))?;
            if peer.uid() != unsafe { libc::getuid() } {
                return Err(framed.unavailable("compiler daemon belongs to another user"));
            }
            match stream.write_all(&framed.bytes).await {
                Ok(()) => return Ok(stream),
                Err(_) => tokio::time::sleep(RETRY).await,
            }
        }
    }
    pub async fn health(&self, deadline: Deadline) -> Result<Value, Failure> {
        self.request(json!({"op":"health"}), deadline).await
    }
    pub async fn submit(
        &self,
        group: &Group,
        priority: i8,
        deadline: Deadline,
    ) -> Result<Value, Failure> {
        group.validate().map_err(Failure::Invalid)?;
        self.request(
            json!({"op":"submit_group","group":group,"priority":priority}),
            deadline,
        )
        .await
    }
    pub async fn status(&self, group: &str, deadline: Deadline) -> Result<Value, Failure> {
        self.request(json!({"op":"group_status","group_id":group}), deadline)
            .await
    }
    pub async fn wait_group(&self, group: &str) -> Result<Value, Failure> {
        self.wait(json!({"op":"group_wait","group_id":group})).await
    }
    pub async fn cancel(
        &self,
        group: &str,
        job: Option<&str>,
        reason: &str,
        include_running: bool,
        deadline: Deadline,
    ) -> Result<Value, Failure> {
        if reason.trim().is_empty() {
            return Err(Failure::Invalid("cancellation requires a reason".into()));
        }
        self.request(json!({"op":"cancel","group_id":group,"job_id":job,"reason":reason,"include_running":include_running}), deadline).await
    }
}

/// The answer on a connection that carries an issued request, or none once the connection broke unanswered.
async fn answer(stream: tokio::net::UnixStream) -> Result<Option<Value>, Failure> {
    let mut line = String::new();
    let read = BufReader::new(stream)
        .take(PROTOCOL_LIMIT as u64)
        .read_line(&mut line)
        .await;
    match read {
        Ok(size) if size > 0 => decoded(&line).map(Some),
        _ => Ok(None),
    }
}

fn decoded(line: &str) -> Result<Value, Failure> {
    let answer: Value = serde_json::from_str(line)
        .map_err(|e| Failure::Refused(format!("invalid compiler response: {e}")))?;
    if answer["ok"] != true {
        return Err(Failure::Refused(
            answer["error"]
                .as_str()
                .unwrap_or("compiler request failed")
                .to_owned(),
        ));
    }
    Ok(answer["value"].clone())
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
        let client = Client::new(&state).unwrap();
        let started = std::time::Instant::now();
        let answer = client.health(Deadline::after(Duration::from_millis(300))).await;
        assert!(
            matches!(&answer, Err(Failure::Unavailable(outage)) if outage.operation == "health" && outage.source == SOURCE),
            "{answer:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
        let required = client
            .require(&["transfer"], Deadline::after(Duration::from_millis(300)))
            .await;
        assert!(matches!(required, Err(Failure::Unavailable(_))), "{required:?}");
        std::fs::remove_dir_all(state).unwrap();
    }

    #[tokio::test]
    async fn nothing_is_sent_once_the_deadline_has_passed() {
        let state = std::env::temp_dir().join(format!(
            "compiler-passed-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        std::fs::create_dir(&state).unwrap();
        let listener =
            std::os::unix::net::UnixListener::bind_addr(&endpoint(&std::fs::canonicalize(&state).unwrap()).unwrap())
                .unwrap();
        listener.set_nonblocking(true).unwrap();
        let client = Client::new(&state).unwrap();
        let passed = Deadline::after(Duration::ZERO);
        let answer = client.health(passed).await;
        assert!(matches!(answer, Err(Failure::Unavailable(_))), "{answer:?}");
        assert_eq!(
            listener.accept().map_err(|e| e.kind()).err(),
            Some(std::io::ErrorKind::WouldBlock),
            "an exchange past its deadline connects to nobody"
        );
        std::fs::remove_dir_all(state).unwrap();
    }

    #[test]
    fn a_missing_state_directory_is_an_outage_and_local_errors_are_invalid() {
        let missing = std::env::temp_dir().join("compiler-state-that-was-never-made");
        let built = Client::new(&missing);
        assert!(
            matches!(&built, Err(Failure::Unavailable(outage)) if outage.operation == "state_dir"),
            "{built:?}"
        );
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let client = Client::new(&std::env::temp_dir()).unwrap();
        let cancelled = runtime.block_on(client.cancel("g", None, " ", false, Deadline::snapshot()));
        assert!(matches!(cancelled, Err(Failure::Invalid(_))), "{cancelled:?}");
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
