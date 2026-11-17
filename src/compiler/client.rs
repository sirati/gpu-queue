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
/// How often a parked wait probes the service's health: one probe at once, then one each interval, never two
/// at a time. Detecting an outage that needs a probe takes up to one interval beyond the patience.
pub const PROBE_INTERVAL: Duration = Duration::from_secs(5);

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

/// Why a parked wait ended unavailable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Expiry {
    /// The service stayed unanswered through a whole outage episode.
    Episode,
    /// The wait was not written whole within its delivery phase.
    Delivery,
}
impl Expiry {
    fn reason(self, operation: &str) -> String {
        let patience = AUTHORITY_PATIENCE.as_secs();
        match self {
            Self::Episode => format!(
                "the compile service answered no health probe for {patience} s while {operation} waited"
            ),
            Self::Delivery => {
                format!("{operation} could not be written to the compile service within {patience} s")
            }
        }
    }
}

/// A parked wait's patience as pure state: when its outage episode and its delivery phase end.
///
/// An episode begins at a broken wait connection, or when a probe starts after the last health reply; that
/// probe's time counts. Its deadline is absolute, so every probe and retry inside it shares what remains, and
/// a health reply ends it. A delivery phase begins at the first attempt to issue the wait and ends once the
/// whole request is written; a health reply never extends it. The parked reply itself is never timed.
#[derive(Debug, Default)]
struct Patience {
    episode: Option<Instant>,
    delivery: Option<Instant>,
}
impl Patience {
    /// A probe starts at `now`: it may take what remains of the episode it begins or continues.
    fn probing(&mut self, now: Instant) -> Deadline {
        Deadline(*self.episode.get_or_insert(now + AUTHORITY_PATIENCE))
    }
    /// The service answered a probe: the episode ends.
    fn answered(&mut self) {
        self.episode = None;
    }
    /// The first attempt to issue the wait, at `now`, begins a delivery phase.
    fn delivering(&mut self, now: Instant) {
        self.delivery = Some(now + AUTHORITY_PATIENCE);
    }
    /// The whole request was written: its delivery phase ends.
    fn delivered(&mut self) {
        self.delivery = None;
    }
    /// The issued wait's connection broke unanswered at `now`: an episode begins unless one runs, and the
    /// wait is issued again in a new delivery phase.
    fn lost(&mut self, now: Instant) {
        self.episode.get_or_insert(now + AUTHORITY_PATIENCE);
        self.delivering(now);
    }
    /// The earliest deadline that ends the wait unavailable, and why.
    fn expiry(&self) -> Option<(Instant, Expiry)> {
        [
            self.episode.map(|at| (at, Expiry::Episode)),
            self.delivery.map(|at| (at, Expiry::Delivery)),
        ]
        .into_iter()
        .flatten()
        .min_by_key(|(at, _)| *at)
    }
}

/// What one leg of a parked wait's connection came to.
enum Leg {
    /// The whole request was written on this connection.
    Delivered(tokio::net::UnixStream),
    /// The service answered.
    Answered(Value),
    /// The connection broke before an answer.
    Lost,
}

/// What the probe side of a parked wait came to.
enum Probed {
    /// The next probe is due.
    Due,
    /// The pending probe finished.
    Finished(Result<Value, Failure>),
}

type Pending<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// A leg that waits for an answer on the connection that carries the issued request.
fn parked(stream: tokio::net::UnixStream) -> Pending<'static, Result<Leg, Failure>> {
    Box::pin(async move { Ok(answer(stream).await?.map_or(Leg::Lost, Leg::Answered)) })
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
    /// Holds a parked request until the daemon answers it once the awaited state is reached; a healthy long
    /// queue never times out. Beside it, health is probed (`PROBE_INTERVAL`), and `Patience` decides when the
    /// service is unavailable: within `AUTHORITY_PATIENCE` of an outage episode's start, plus one probe
    /// interval when only a probe detects it. A connection that breaks unanswered (a restart or hand-over)
    /// issues the same keyed request again, which the daemon answers idempotently.
    pub async fn waiting(&self, request: Value) -> Result<Value, Failure> {
        let framed = Framed::of(request)?;
        let mut patience = Patience::default();
        patience.delivering(Instant::now());
        let mut leg = self.issued(&framed, Duration::ZERO);
        let mut probe: Option<Pending<'_, Result<Value, Failure>>> = None;
        let mut next_probe = Instant::now();
        loop {
            let expiry = patience.expiry();
            tokio::select! {
                step = &mut leg => match step? {
                    Leg::Delivered(stream) => {
                        patience.delivered();
                        leg = parked(stream);
                    }
                    Leg::Answered(value) => return Ok(value),
                    Leg::Lost => {
                        patience.lost(Instant::now());
                        leg = self.issued(&framed, RETRY);
                    }
                },
                probed = async {
                    match probe.as_mut() {
                        Some(pending) => Probed::Finished(pending.await),
                        None => {
                            tokio::time::sleep_until(next_probe.into()).await;
                            Probed::Due
                        }
                    }
                } => match probed {
                    Probed::Due => {
                        let now = Instant::now();
                        probe = Some(Box::pin(self.health(patience.probing(now))));
                        next_probe = now + PROBE_INTERVAL;
                    }
                    Probed::Finished(Ok(_)) => {
                        patience.answered();
                        probe = None;
                    }
                    Probed::Finished(Err(_)) => probe = None,
                },
                expired = async {
                    match expiry {
                        Some((at, why)) => {
                            tokio::time::sleep_until(at.into()).await;
                            why
                        }
                        None => std::future::pending().await,
                    }
                } => return Err(framed.unavailable(expired.reason(&framed.operation))),
            }
        }
    }
    /// A leg that writes the whole request after `pause`, retrying failed connections and writes.
    fn issued<'a>(&'a self, framed: &'a Framed, pause: Duration) -> Pending<'a, Result<Leg, Failure>> {
        Box::pin(async move {
            tokio::time::sleep(pause).await;
            self.deliver(framed).await.map(Leg::Delivered)
        })
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
        self.waiting(json!({"op":"group_wait","group_id":group}))
            .await
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

    fn seconds(start: Instant, offset: u64) -> Instant {
        start + Duration::from_secs(offset)
    }

    #[test]
    fn health_replies_never_extend_a_delivery_phase() {
        let start = Instant::now();
        let mut patience = Patience::default();
        patience.delivering(start);
        for probe in (0..60).step_by(5) {
            patience.probing(seconds(start, probe));
            patience.answered();
        }
        assert_eq!(patience.expiry(), Some((seconds(start, 60), Expiry::Delivery)));
        patience.delivered();
        assert_eq!(patience.expiry(), None, "a written wait has no deadline while health answers");
    }

    #[test]
    fn a_pending_first_probe_ends_its_episode_after_one_patience() {
        let start = Instant::now();
        let mut patience = Patience::default();
        patience.delivering(start);
        patience.delivered();
        assert_eq!(patience.probing(start), Deadline(seconds(start, 60)));
        assert_eq!(patience.expiry(), Some((seconds(start, 60), Expiry::Episode)));
    }

    #[test]
    fn probes_and_losses_inside_an_episode_share_its_deadline() {
        let start = Instant::now();
        let mut patience = Patience::default();
        patience.delivering(start);
        patience.delivered();
        patience.probing(seconds(start, 10));
        patience.lost(seconds(start, 12));
        assert_eq!(patience.expiry(), Some((seconds(start, 70), Expiry::Episode)));
        assert_eq!(
            patience.probing(seconds(start, 15)),
            Deadline(seconds(start, 70)),
            "a later probe has what remains, not a fresh patience"
        );
        patience.answered();
        assert_eq!(
            patience.expiry(),
            Some((seconds(start, 72), Expiry::Delivery)),
            "the reissue's own delivery phase still ends it"
        );
    }

    #[test]
    fn a_health_reply_ends_the_episode_and_a_later_failure_begins_a_new_one() {
        let start = Instant::now();
        let mut patience = Patience::default();
        patience.delivering(start);
        patience.delivered();
        patience.probing(start);
        patience.answered();
        assert_eq!(patience.expiry(), None);
        assert_eq!(patience.probing(seconds(start, 59)), Deadline(seconds(start, 119)));
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
