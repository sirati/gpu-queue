//! Blocking native lease client. This module owns the wire session, not queue
//! policy. A caller receives a typed shutdown notification plus an explicit
//! acknowledgement API. All enforcement remains in the existing daemon engine.
use crate::notices::{Notice, Notices};
use crate::protocol::{
    ClientMsg, DeviceLossReport, LeaseJob, LeaseRequest, ProcessIdentity, ReadyFigures, Request, ShutdownRequest,
};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub struct Lease {
    owner_pid: u32,
    socket: std::path::PathBuf,
    identity: crate::protocol::Resume,
    retention_supported: bool,
    cancel_supported: bool,
    writer_fd: RawFd,
    stream: Arc<UnixStream>,
    writer: Arc<Mutex<()>>,
    connected: Arc<AtomicBool>,
    released: Arc<AtomicBool>,
    /// The requests the session's outcome depends on (a failure report, the release or cancellation that ended
    /// it), in order, sent again to a service that went away before handling them.
    decisive: Arc<Mutex<Vec<Request>>>,
    /// The attached service answers the end of a session with every rule the grant broke
    /// (`contract_protocol`); a reattachment, also to an upgraded service, renews it.
    answers_release: Arc<AtomicBool>,
    /// Ends the watcher; set once the release answer arrived or was waited for.
    stop: Arc<AtomicBool>,
    pub pending: Arc<Mutex<Option<ShutdownRequest>>>,
    pub notices: Arc<Mutex<Notices>>,
    watcher: Option<JoinHandle<()>>,
    pub id: u64,
}

/// What a lease update means for the session.
#[derive(Clone, Copy, PartialEq)]
enum Update {
    Notice,
    /// The session's outcome depends on it; a service that went away first hears it again.
    Decisive,
    /// Decisive, and it ends the session.
    Ending,
}

struct ReadStream {
    stream: Arc<UnixStream>,
    prefix: Cursor<Vec<u8>>,
}
impl Read for ReadStream {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let size = self.prefix.read(bytes)?;
        if size > 0 {
            return Ok(size);
        }
        (&*self.stream).read(bytes)
    }
}

fn send(mut stream: &UnixStream, request: &Request) -> Result<(), String> {
    let mut line = serde_json::to_vec(request).map_err(|e| e.to_string())?;
    line.push(b'\n');
    stream.write_all(&line).map_err(|e| e.to_string())
}

enum SessionError {
    Retry(String),
    Refused(String),
    Interrupted(String),
    /// The resuming process gave its place away; the place belongs to this process now.
    Transferred(ProcessIdentity),
}

impl SessionError {
    fn reason(self) -> String {
        match self {
            Self::Retry(reason) | Self::Refused(reason) | Self::Interrupted(reason) => reason,
            Self::Transferred(owner) => format!("the place was transferred to pid {}", owner.pid),
        }
    }
}

/// Why a lease session ended before its grant.
#[derive(Debug, Clone)]
pub enum PlaceError {
    /// gpu-switch is unreachable, lacks the protocol the request needs, or did not come back within the
    /// reattachment window: an outage of the authority, never a verdict.
    Unavailable(String),
    Refused(String),
    Interrupted(String),
    /// gpu-switch lost a told place and registered it again untold: what was prepared holds no place.
    Lost,
}

impl PlaceError {
    pub fn reason(&self) -> String {
        match self {
            Self::Unavailable(reason) | Self::Refused(reason) | Self::Interrupted(reason) => reason.clone(),
            Self::Lost => "gpu-switch lost the told place and registered it again untold".into(),
        }
    }
}

impl From<SessionError> for PlaceError {
    fn from(error: SessionError) -> Self {
        match error {
            SessionError::Retry(reason) => Self::Unavailable(reason),
            SessionError::Interrupted(reason) => Self::Interrupted(reason),
            other => Self::Refused(other.reason()),
        }
    }
}

fn read_checked(
    reader: &mut BufReader<ReadStream>,
    line: &mut String,
    check: &impl Fn() -> Result<(), String>,
) -> Result<usize, SessionError> {
    loop {
        check().map_err(SessionError::Interrupted)?;
        match reader.read_line(line) {
            Ok(size) => return Ok(size),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(SessionError::Retry(error.to_string())),
        }
    }
}

// Match the runner's retained-state reattachment window. No fallback queue and
// no fresh submission are allowed after a grant has been issued.
const RESUME_PATIENCE: Duration = Duration::from_secs(600);
type Opened = (Arc<UnixStream>, BufReader<ReadStream>, LeaseJob, serde_json::Value);

fn open(
    socket: &Path,
    request: &LeaseRequest,
    check: &impl Fn() -> Result<(), String>,
) -> Result<Opened, SessionError> {
    check().map_err(SessionError::Interrupted)?;
    let io = |error: std::io::Error| SessionError::Retry(error.to_string());
    let stream = Arc::new(UnixStream::connect(socket).map_err(io)?);
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .map_err(io)?;
    stream
        .set_write_timeout(Some(Duration::from_secs(1)))
        .map_err(io)?;
    send(&stream, &Request::Lease(request.clone())).map_err(SessionError::Retry)?;
    let mut reader = BufReader::new(ReadStream {
        stream: stream.clone(),
        prefix: Cursor::new(Vec::new()),
    });
    let mut line = String::new();
    if read_checked(&mut reader, &mut line, check)? == 0 {
        return Err(SessionError::Retry(
            "daemon disconnected before lease reply".into(),
        ));
    }
    let reply: serde_json::Value = serde_json::from_str(&line)
        .map_err(|error| SessionError::Refused(format!("invalid lease reply: {error}")))?;
    if reply["ok"] != true {
        return Err(SessionError::Refused(format!(
            "GPU queue refused lease: {}",
            reply["error"]
        )));
    }
    if let Ok(owner) = serde_json::from_value::<ProcessIdentity>(reply["transferred"].clone()) {
        return Err(SessionError::Transferred(owner));
    }
    if let Some(warning) = reply["warning"].as_str() {
        eprintln!("gpu-queue: warning: {warning}");
    }
    if request.category != crate::protocol::Category::Other && reply["lease_category_protocol"] != 1
    {
        return Err(SessionError::Refused(
            "daemon does not support native lease task categories; upgrade it before using a category".into(),
        ));
    }
    if request.max_runtime_secs.is_some() && reply["lease_runtime_protocol"] != 1 {
        return Err(SessionError::Refused(
            "daemon does not support native lease runtime limits; upgrade it before using a limit"
                .into(),
        ));
    }
    if request.shutdown_grace_secs.is_some() && reply["shutdown_protocol"] != 1 {
        return Err(SessionError::Refused(
            "daemon does not support cooperative shutdown; upgrade it before using this API".into(),
        ));
    }
    let job: LeaseJob = serde_json::from_value(reply["job"].clone())
        .map_err(|error| SessionError::Refused(format!("invalid lease reply: {error}")))?;
    Ok((stream, reader, job, reply))
}

fn reconnect(
    socket: &Path,
    request: &LeaseRequest,
    check: &impl Fn() -> Result<(), String>,
) -> Result<Opened, SessionError> {
    if request.resume.is_none() {
        return Err(SessionError::Refused("lease reply missing resume token".into()));
    }
    let deadline = Instant::now() + RESUME_PATIENCE;
    let mut wait = Duration::from_millis(50);
    loop {
        check().map_err(SessionError::Interrupted)?;
        // Bound a server that accepts a socket but never replies as well.
        let bounded = || {
            check()?;
            if Instant::now() >= deadline {
                Err("reattachment timed out".into())
            } else {
                Ok(())
            }
        };
        match open(socket, request, &bounded) {
            Ok(opened) => return Ok(opened),
            Err(SessionError::Retry(error)) => {
                if Instant::now() >= deadline {
                    return Err(SessionError::Retry(error));
                }
                std::thread::sleep(wait);
                wait = (wait * 2).min(Duration::from_millis(100));
            }
            Err(error) => return Err(error),
        }
    }
}

fn replace(
    stream: &Arc<UnixStream>,
    reader: &mut BufReader<ReadStream>,
    next: Arc<UnixStream>,
    next_reader: BufReader<ReadStream>,
) -> Result<(), String> {
    // Keep the descriptor stable across reconnects so the post-fork child can
    // close its inherited session without locking or releasing the parent grant.
    if unsafe { libc::dup3(next.as_raw_fd(), stream.as_raw_fd(), libc::O_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    *reader = BufReader::new(ReadStream {
        stream: stream.clone(),
        prefix: Cursor::new(next_reader.buffer().to_vec()),
    });
    Ok(())
}

/// One authenticated owner request that answers with a fixed Unix deadline.
fn deadline_update(socket: &Path, request: &Request, what: &str) -> Result<f64, SessionError> {
    let stream = UnixStream::connect(socket).map_err(|e| SessionError::Retry(e.to_string()))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| SessionError::Retry(e.to_string()))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| SessionError::Retry(e.to_string()))?;
    send(&stream, request).map_err(SessionError::Retry)?;
    let mut reply = String::new();
    let size = BufReader::new(stream)
        .read_line(&mut reply)
        .map_err(|e| SessionError::Retry(e.to_string()))?;
    if size == 0 {
        return Err(SessionError::Retry(format!(
            "daemon disconnected before {what} acknowledgement"
        )));
    }
    let reply: crate::protocol::Reply =
        serde_json::from_str(&reply).map_err(|e| match e.is_eof() {
            true => SessionError::Retry(e.to_string()),
            false => SessionError::Refused(e.to_string()),
        })?;
    if !reply.ok {
        return Err(SessionError::Refused(
            reply.error.unwrap_or_else(|| format!("{what} refused")),
        ));
    }
    reply
        .message
        .ok_or_else(|| SessionError::Refused(format!("{what} acknowledgement missing deadline")))?
        .parse()
        .map_err(|_| SessionError::Refused(format!("invalid {what} deadline")))
}

/// Prints a notice the first time the holder hears it.
fn hear(notices: &Mutex<Notices>, notice: Notice, holder: &str) {
    let heard = notices.lock().unwrap_or_else(|e| e.into_inner()).hear(notice, holder);
    heard.into_iter().for_each(|text| eprintln!("{text}"));
}

/// A lease session between its placement and its grant.
struct Session {
    socket: PathBuf,
    request: LeaseRequest,
    stream: Arc<UnixStream>,
    reader: BufReader<ReadStream>,
    job: LeaseJob,
    /// Lines read while waiting for something else, still to be read.
    kept: VecDeque<String>,
    /// gpu-switch told this place to prepare.
    told: bool,
    /// Why the last preparation report failed.
    failure: Option<PlaceError>,
    /// What a told place last declared, declared again to a gpu-switch that comes back without it.
    waiting: Option<(String, bool)>,
    ready: Option<ReadyFigures>,
    /// What the session heard, and whether every connection could report a card loss.
    notices: Notices,
}

/// How a reattachment ended.
enum Reattached {
    Attached,
    /// This process had given the place away: it belongs to that process.
    Transferred(ProcessIdentity),
}

impl Session {
    /// The next line, reattaching across a restart of gpu-switch.
    fn line(&mut self, check: &impl Fn() -> Result<(), String>) -> Result<String, PlaceError> {
        if let Some(line) = self.kept.pop_front() {
            return Ok(line);
        }
        loop {
            let mut line = String::new();
            match read_checked(&mut self.reader, &mut line, check) {
                Ok(0) | Err(SessionError::Retry(_)) => match self.reattach(check)? {
                    Reattached::Attached => continue,
                    Reattached::Transferred(owner) => {
                        return Err(PlaceError::Refused(format!(
                            "the place was transferred to pid {}",
                            owner.pid
                        )))
                    }
                },
                Err(error) => return Err(error.into()),
                Ok(_) => return Ok(line),
            }
        }
    }

    /// Sends `request`, reattaching first if gpu-switch went away; what a told place declared is declared
    /// again by the reattachment itself.
    fn send(&mut self, request: &Request, check: &impl Fn() -> Result<(), String>) -> Result<(), PlaceError> {
        match send(&self.stream, request) {
            Ok(()) => Ok(()),
            Err(_) => match self.reattach(check)? {
                Reattached::Attached => Ok(()),
                Reattached::Transferred(owner) => Err(PlaceError::Refused(format!(
                    "the place was transferred to pid {}",
                    owner.pid
                ))),
            },
        }
    }

    /// Resumes the session. A told place that gpu-switch registered again untold is lost; one it kept
    /// declares its wait again, and its `Ready` if gpu-switch did not keep it.
    fn reattach(&mut self, check: &impl Fn() -> Result<(), String>) -> Result<Reattached, PlaceError> {
        let (next, next_reader, job, reply) = match reconnect(&self.socket, &self.request, check) {
            Ok(opened) => opened,
            Err(SessionError::Transferred(owner)) => return Ok(Reattached::Transferred(owner)),
            Err(error) => return Err(error.into()),
        };
        replace(&self.stream, &mut self.reader, next, next_reader).map_err(PlaceError::Unavailable)?;
        self.request.resume = Some(job.resume.clone());
        self.notices.connected(job.device_loss_protocol);
        self.job = job;
        if self.told && reply["reregistered"] == true {
            return Err(PlaceError::Lost);
        }
        let waiting = self.waiting.clone().map(|(reason, parked)| Request::PreparationWait { reason, parked });
        let ready = self
            .ready
            .clone()
            .filter(|_| reply["preparation"]["ready"] != true)
            .map(Request::Ready);
        for request in waiting.iter().chain(ready.iter()) {
            send(&self.stream, request).map_err(PlaceError::Unavailable)?;
        }
        Ok(Reattached::Attached)
    }

    /// A refusal gpu-switch answered on the session.
    fn refusal(line: &str) -> Option<PlaceError> {
        let value: serde_json::Value = serde_json::from_str(line).ok()?;
        (value["ok"] == false).then(|| {
            PlaceError::Refused(value["error"].as_str().unwrap_or("GPU queue refused the request").into())
        })
    }

    /// The granted session becomes the lease its watcher keeps.
    fn lease(self, notify: impl Fn(ShutdownRequest) + Send + 'static) -> Lease {
        let Session {
            socket,
            mut request,
            stream,
            mut reader,
            job,
            notices,
            ..
        } = self;
        let id = job.id;
        let cancel_supported = job.cancel_protocol == 1;
        let answers_release = Arc::new(AtomicBool::new(job.contract_protocol == 2));
        let retention_supported = job.vram_retention_protocol == 2;
        let mut line = String::new();
        request.resume.as_mut().expect("resume token").running = true;
        let identity = request.resume.clone().expect("resume token");
        let client_socket = socket.clone();
        let writer_fd = stream.as_raw_fd();
        let writer = Arc::new(Mutex::new(()));
        let connected = Arc::new(AtomicBool::new(true));
        let released = Arc::new(AtomicBool::new(false));
        let pending = Arc::new(Mutex::new(None));
        let notices = Arc::new(Mutex::new(notices));
        let heard = notices.clone();
        let holder = request.label.clone().unwrap_or_else(|| format!("job {}", job.id));
        let stop = Arc::new(AtomicBool::new(false));
        let ending = stop.clone();
        let ended = released.clone();
        let decisive = Arc::new(Mutex::new(Vec::<Request>::new()));
        let decided = decisive.clone();
        let state = pending.clone();
        let watch_stream = stream.clone();
        let watch_writer = writer.clone();
        let watch_connected = connected.clone();
        let answering = answers_release.clone();
        let watcher = std::thread::spawn(move || {
            loop {
                line.clear();
                let check = || {
                    if stop.load(Ordering::Acquire) {
                        Err("lease released".into())
                    } else {
                        Ok(())
                    }
                };
                if read_checked(&mut reader, &mut line, &check).unwrap_or(0) == 0 {
                    watch_connected.store(false, Ordering::Release);
                    // A released session ends at its EOF, unless the service owes it an answer: then the
                    // service went away first, and the resumed one is asked again.
                    if ended.load(Ordering::Acquire) && !answering.load(Ordering::Acquire) {
                        return;
                    }
                    let resumed = reconnect(&socket, &request, &check).map_err(SessionError::reason).and_then(
                        |(next, next_reader, job, _)| {
                            let _guard = watch_writer.lock().unwrap_or_else(|e| e.into_inner());
                            check()?;
                            replace(&watch_stream, &mut reader, next, next_reader)?;
                            answering.store(job.contract_protocol == 2, Ordering::Release);
                            heard.lock().unwrap_or_else(|e| e.into_inner()).connected(job.device_loss_protocol);
                            watch_connected.store(true, Ordering::Release);
                            request.resume = Some(job.resume);
                            // The local process is already admitted. Never let a later
                            // reconnect represent it as a fresh queued acquisition.
                            request.resume.as_mut().expect("resume token").running = true;
                            let resent = decided
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .iter()
                                .try_for_each(|request| send(&watch_stream, request));
                            // A connection that took them only in part is ended, so the next read
                            // reattaches and sends them again.
                            if resent.is_err() {
                                let _ = watch_stream.shutdown(std::net::Shutdown::Both);
                            }
                            Ok(())
                        },
                    );
                    match resumed {
                        Ok(()) => continue,
                        Err(_) if ended.load(Ordering::Acquire) => return,
                        Err(reason) => {
                            eprintln!("gpu-queue: GPU queue reattachment failed: {reason}");
                            notify(ShutdownRequest {
                                request_id: "authority-lost".into(),
                                reason: format!("GPU queue reattachment failed: {reason}"),
                                deadline: crate::protocol::now_unix(),
                                permanent: true,
                                handler_started: false,
                            });
                            unsafe {
                                libc::kill(std::process::id() as i32, libc::SIGTERM);
                            }
                            return;
                        }
                    }
                }
                match serde_json::from_str::<ClientMsg>(&line) {
                    Ok(ClientMsg::Shutdown { shutdown }) => {
                        let mut pending = state.lock().unwrap_or_else(|e| e.into_inner());
                        let duplicate =
                            pending.as_ref().is_some_and(|request: &ShutdownRequest| {
                                request.request_id == shutdown.request_id
                            });
                        *pending = Some(shutdown.clone());
                        drop(pending);
                        // Replaying a retained request must not interrupt its own
                        // checkpoint callback or start a second callback.
                        if !duplicate {
                            notify(shutdown);
                        }
                    }
                    Ok(ClientMsg::Signal { signal }) => unsafe {
                        libc::kill(std::process::id() as i32, signal.number());
                    },
                    Ok(ClientMsg::Violation { violation }) => {
                        hear(&heard, Notice::Violation(violation), &holder);
                    }
                    Ok(ClientMsg::DeviceLost { device_lost }) => {
                        hear(&heard, Notice::DeviceLost(device_lost), &holder);
                    }
                    // The final report: what it carries is printed once, also if its notice was heard first.
                    Ok(ClientMsg::Violations { violations, device_lost }) => {
                        let notices = violations.into_iter().map(Notice::Violation);
                        for notice in notices.chain(device_lost.map(Notice::DeviceLost)) {
                            hear(&heard, notice, &holder);
                        }
                        return;
                    }
                    _ => {}
                }
            }
        });
        Lease {
            owner_pid: std::process::id(),
            socket: client_socket,
            identity,
            retention_supported,
            cancel_supported,
            writer_fd,
            stream,
            writer,
            connected,
            released,
            decisive,
            answers_release,
            stop: ending,
            pending,
            notices,
            watcher: Some(watcher),
            id,
        }
    }
}


/// Placed in the line, not yet told to prepare.
pub struct Placed(Session);

/// Told to prepare (a place), or ready on arrival.
pub struct Told(Session);

/// A told place handed to another process across an exec or a fork: its session descriptor, which stays
/// open without close-on-exec, and the bytes read from it but not consumed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaceHandover {
    pub fd: RawFd,
    pub socket: PathBuf,
    pub job: LeaseJob,
    pub request: LeaseRequest,
    pub unread: Vec<u8>,
}

/// The environment variable through which `gpu-run place` hands its told place to the program it executes.
pub const PLACE_HANDOVER: &str = "GPU_QUEUE_PLACE";

impl Placed {
    /// Waits until gpu-switch tells the place to prepare, reporting each change of its waiting reason. An
    /// entry ready on arrival is told at once, and nothing is read, so a grant already sent stays unread.
    pub fn told(
        self,
        check: &impl Fn() -> Result<(), String>,
        report: &mut dyn FnMut(&str),
    ) -> Result<Told, PlaceError> {
        let mut session = self.0;
        if !session.request.prepares {
            return Ok(Told(session));
        }
        loop {
            let line = session.line(check)?;
            match serde_json::from_str::<ClientMsg>(&line) {
                Ok(ClientMsg::Prepare { prepare: true }) => {
                    session.told = true;
                    return Ok(Told(session));
                }
                Ok(ClientMsg::Waiting { waiting }) => report(&waiting),
                _ => {
                    if let Some(refused) = Session::refusal(&line) {
                        return Err(refused);
                    }
                }
            }
        }
    }
}

impl Told {
    /// The place gpu-switch handed this process (`PLACE_HANDOVER`), as one of its processes left it.
    pub fn inherited(handover: PlaceHandover) -> Result<Told, String> {
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(handover.fd, &mut stat) } != 0 || stat.st_mode & libc::S_IFMT != libc::S_IFSOCK {
            return Err(format!("the handed-over place descriptor {} is not a socket", handover.fd));
        }
        // SAFETY: the descriptor is this process's, handed over open for exactly this session.
        let stream = unsafe { UnixStream::from_raw_fd(handover.fd) };
        if unsafe { libc::fcntl(handover.fd, libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .map_err(|e| e.to_string())?;
        stream
            .set_write_timeout(Some(Duration::from_secs(1)))
            .map_err(|e| e.to_string())?;
        let stream = Arc::new(stream);
        let reader = BufReader::new(ReadStream {
            stream: stream.clone(),
            prefix: Cursor::new(handover.unread),
        });
        Ok(Told(Session {
            socket: handover.socket,
            request: handover.request,
            stream,
            reader,
            notices: Notices::of(handover.job.device_loss_protocol),
            job: handover.job,
            kept: VecDeque::new(),
            told: true,
            failure: None,
            waiting: None,
            ready: None,
        }))
    }

    /// The place for another process: its descriptor stays open and is no longer closed on exec, with every
    /// byte read from it but not consumed.
    pub fn handover(self) -> PlaceHandover {
        let Session {
            socket,
            request,
            stream,
            reader,
            job,
            kept,
            ..
        } = self.0;
        let mut unread: Vec<u8> = kept.into_iter().flat_map(String::into_bytes).collect();
        unread.extend_from_slice(reader.buffer());
        let rest = reader.into_inner();
        unread.extend_from_slice(&rest.prefix.get_ref()[rest.prefix.position() as usize..]);
        drop(rest);
        let stream = Arc::try_unwrap(stream).expect("the session holds its stream alone");
        let fd = stream.into_raw_fd();
        unsafe {
            libc::fcntl(fd, libc::F_SETFD, 0);
        }
        PlaceHandover {
            fd,
            socket,
            job,
            request,
            unread,
        }
    }

    /// Why the last preparation report through `PreparationWait` failed.
    pub fn failure(&self) -> Option<&PlaceError> {
        self.0.failure.as_ref()
    }

    pub fn job(&self) -> &LeaseJob {
        &self.0.job
    }

    pub fn request(&self) -> &LeaseRequest {
        &self.0.request
    }

    pub fn socket(&self) -> &Path {
        &self.0.socket
    }

    /// Why preparation waits; an empty reason ends the wait. Reads nothing, so every line stays unread.
    pub fn preparation_wait(&mut self, reason: &str, parked: bool) -> Result<(), PlaceError> {
        self.0.waiting = Some((reason.to_owned(), parked));
        let request = Request::PreparationWait {
            reason: reason.to_owned(),
            parked,
        };
        self.0.send(&request, &|| Ok(()))
    }

    /// Gives the place to `to_pid`, a live descendant of this process, before its `Ready`. Returns once
    /// gpu-switch acknowledged it durably: on this session, or by answering this process's resume with the
    /// place's new owner. Lines read meanwhile stay unread for the new owner.
    pub fn transfer(&mut self, to_pid: u32) -> Result<(), PlaceError> {
        let to = ProcessIdentity {
            pid: to_pid,
            birth: crate::procs::started(to_pid)
                .ok_or_else(|| PlaceError::Refused(format!("transfer target {to_pid} is gone")))?,
        };
        let request = Request::TransferPlace { to };
        let check = || Ok(());
        let mut sent = send(&self.0.stream, &request).is_ok();
        loop {
            let mut line = String::new();
            let read = match sent {
                true => read_checked(&mut self.0.reader, &mut line, &check),
                false => Ok(0),
            };
            match read {
                Ok(0) | Err(SessionError::Retry(_)) => match self.0.reattach(&check)? {
                    Reattached::Transferred(owner) if owner == to => return Ok(()),
                    Reattached::Transferred(owner) => {
                        return Err(PlaceError::Refused(format!(
                            "the place was transferred to pid {}, not {to_pid}",
                            owner.pid
                        )))
                    }
                    Reattached::Attached => {
                        sent = send(&self.0.stream, &request).is_ok();
                    }
                },
                Err(error) => return Err(error.into()),
                Ok(_) => match serde_json::from_str::<ClientMsg>(&line) {
                    Ok(ClientMsg::Transferred { transferred }) if transferred == to => return Ok(()),
                    _ => {
                        if let Some(refused) = Session::refusal(&line) {
                            return Err(refused);
                        }
                        self.0.kept.push_back(line);
                    }
                },
            }
        }
    }

    /// Sends a place's `Ready` with the final figures of its run, then waits for the grant. An entry ready
    /// on arrival sends nothing and ignores `ready`.
    pub fn granted(
        self,
        ready: ReadyFigures,
        notify: impl Fn(ShutdownRequest) + Send + 'static,
        check: &impl Fn() -> Result<(), String>,
    ) -> Result<Lease, PlaceError> {
        let mut session = self.0;
        if session.request.prepares {
            session.ready = Some(ready.clone());
            session.send(&Request::Ready(ready), check)?;
        }
        loop {
            let line = session.line(check)?;
            if matches!(serde_json::from_str(&line), Ok(ClientMsg::Granted { granted: true })) {
                return Ok(session.lease(notify));
            }
            if let Some(refused) = Session::refusal(&line) {
                return Err(refused);
            }
        }
    }

    /// In a forked child only: closes the inherited descriptor without a word to gpu-switch, so the
    /// parent's place stays the parent's.
    pub fn inherited_close(self) {
        let fd = self.0.stream.as_raw_fd();
        std::mem::forget(self);
        unsafe {
            libc::close(fd);
        }
    }

    /// Ends the place before its grant.
    pub fn leave(mut self, reason: &str) -> Result<(), PlaceError> {
        self.0.send(
            &Request::Cancelled {
                reason: reason.to_owned(),
            },
            &|| Ok(()),
        )?;
        let _ = self.0.stream.shutdown(std::net::Shutdown::Both);
        Ok(())
    }
}

impl crate::compiler::memory_client::PreparationWait for Told {
    fn preparation_wait(&mut self, reason: &str, parked: bool) -> Result<(), String> {
        Told::preparation_wait(self, reason, parked).map_err(|error| {
            let reason = error.reason();
            self.0.failure = Some(error);
            reason
        })
    }
}

impl Lease {
    pub fn acquire(
        socket: &Path,
        request: LeaseRequest,
        notify: impl Fn(ShutdownRequest) + Send + 'static,
    ) -> Result<Self, String> {
        Self::acquire_checked(socket, request, notify, || Ok(()))
    }

    /// Places, waits to be told (at once for an entry ready on arrival), and waits for the grant.
    pub fn acquire_checked(
        socket: &Path,
        request: LeaseRequest,
        notify: impl Fn(ShutdownRequest) + Send + 'static,
        check: impl Fn() -> Result<(), String>,
    ) -> Result<Self, String> {
        let ready = ReadyFigures::of(&request);
        Self::place(socket, request, &check)
            .and_then(|placed| placed.told(&check, &mut |_| {}))
            .and_then(|told| told.granted(ready, notify, &check))
            .map_err(|error| error.reason())
    }

    /// Enters the line. A place needs a gpu-switch that speaks the preparation protocol; without one it is
    /// refused before any `Ready`.
    pub fn place(
        socket: &Path,
        request: LeaseRequest,
        check: &impl Fn() -> Result<(), String>,
    ) -> Result<Placed, PlaceError> {
        let mut request = request;
        let (stream, reader, job, reply) = open(socket, &request, check)?;
        if request.prepares && reply["preparation_protocol"] != 1 {
            return Err(PlaceError::Unavailable(
                "gpu-switch does not tell places when to prepare (preparation_protocol); update it".into(),
            ));
        }
        if job.watchdog != request.watchdog {
            return Err(PlaceError::Refused(format!(
                "gpu-switch cannot apply the {} watchdog policy; upgrade the service",
                request.watchdog.as_str()
            )));
        }
        request.resume = Some(job.resume.clone());
        Ok(Placed(Session {
            socket: socket.to_owned(),
            request,
            stream,
            reader,
            notices: Notices::of(job.device_loss_protocol),
            job,
            kept: VecDeque::new(),
            told: false,
            failure: None,
            waiting: None,
            ready: None,
        }))
    }

    /// End exclusivity permanently for a fixed no-compute VRAM retention phase.
    /// The independent authenticated RPC needs no session writer lock.
    pub fn retain_vram(&self, seconds: f64) -> Result<f64, String> {
        if !self.retention_supported {
            return Err("daemon lacks bounded VRAM retention support".into());
        }
        let resume = self.identity.clone();
        self.owner_deadline(&Request::RetainVram { resume, seconds }, "VRAM retention")
    }

    fn owner_deadline(&self, request: &Request, what: &str) -> Result<f64, String> {
        self.check_owner()?;
        if self.is_released() {
            return Err("lease released".into());
        }
        let deadline = Instant::now() + RESUME_PATIENCE;
        loop {
            if self.is_released() {
                return Err("lease released".into());
            }
            match deadline_update(&self.socket, request, what) {
                Ok(deadline) => return Ok(deadline),
                Err(SessionError::Retry(error)) => {
                    if Instant::now() >= deadline {
                        return Err(error);
                    }
                    // Same identity and duration: a lost acknowledgement cannot
                    // create a fresh phase or renew an existing deadline.
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(error) => return Err(error.reason()),
            }
        }
    }

    pub fn shutdown_started(&self, request_id: &str) -> Result<(), String> {
        self.check_owner()?;
        let pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        match &*pending {
            Some(request) if request.request_id == request_id => {
                drop(pending);
                self.send_active(
                    &Request::ShutdownStarted {
                        request_id: request_id.into(),
                    },
                    Update::Notice,
                )?;
                if let Some(request) = self
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_mut()
                {
                    if request.request_id == request_id {
                        request.handler_started = true;
                    }
                }
                Ok(())
            }
            _ => Err("stale or unknown shutdown request".into()),
        }
    }

    /// The session's card loss: the first association heard, else unreported if a connection reached a gpu-switch
    /// that cannot report one, else none.
    pub fn device_loss(&self) -> DeviceLossReport {
        self.notices.lock().unwrap_or_else(|e| e.into_inner()).report()
    }

    pub fn is_released(&self) -> bool {
        self.released.load(Ordering::Acquire)
    }

    pub fn cancel(&self, reason: String) -> Result<(), String> {
        self.check_owner()?;
        if !self.cancel_supported {
            return Err("daemon lacks explicit lease cancellation; upgrade it".into());
        }
        if reason.trim().is_empty() {
            return Err("cancellation requires a reason".into());
        }
        self.send_active(&Request::Cancelled { reason }, Update::Ending)
    }

    /// Report the run failed. The grant stays held, and judged, until the release that ends the session.
    pub fn fail(&self, reason: String) -> Result<(), String> {
        self.check_owner()?;
        self.send_active(&Request::Failed { reason }, Update::Decisive)
    }

    fn send_active(&self, request: &Request, update: Update) -> Result<(), String> {
        let deadline = Instant::now() + RESUME_PATIENCE;
        loop {
            let guard = self.writer.lock().unwrap_or_else(|e| e.into_inner());
            if self.released.load(Ordering::Acquire) {
                return Err("lease released".into());
            }
            if self.connected.load(Ordering::Acquire) {
                match send(&self.stream, request) {
                    Ok(()) => {
                        // Recorded under the writer lock, so no reattachment comes between the
                        // send and the record of what it must send again.
                        if update != Update::Notice {
                            self.decisive.lock().unwrap_or_else(|e| e.into_inner()).push(request.clone());
                        }
                        if update == Update::Ending {
                            self.released.store(true, Ordering::Release);
                        }
                        return Ok(());
                    }
                    Err(_) => self.connected.store(false, Ordering::Release),
                }
            }
            drop(guard);
            if Instant::now() >= deadline {
                return Err("GPU queue reattachment timed out while sending lease update".into());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn release(&mut self) {
        if self.owner_pid != std::process::id() {
            return;
        }
        if self.released.load(Ordering::Acquire) && self.watcher.is_none() {
            return;
        }
        // Complete the release through the resumed authority instead of leaving
        // a detached retained grant behind when teardown races a restart.
        if !self.released.load(Ordering::Acquire) {
            let _ = self.send_active(&Request::Release, Update::Ending);
        }
        self.released.store(true, Ordering::Release);
        // The answer lists every rule the grant broke; the watcher ends on it, reattaching to ask again if the
        // service went away first, as patient as any other lease update.
        let answered = Instant::now() + RESUME_PATIENCE;
        while self.answers_release.load(Ordering::Acquire)
            && self.watcher.as_ref().is_some_and(|watcher| !watcher.is_finished())
            && Instant::now() < answered
        {
            std::thread::sleep(Duration::from_millis(5));
        }
        self.stop.store(true, Ordering::Release);
        let guard = self.writer.lock().unwrap_or_else(|e| e.into_inner());
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
        drop(guard);
        if let Some(watcher) = self.watcher.take() {
            let _ = watcher.join();
        }
    }

    pub fn check_owner(&self) -> Result<(), String> {
        if self.owner_pid == std::process::id() {
            Ok(())
        } else {
            Err("a forked child must acquire its own GPU lease".into())
        }
    }

    /// In the post-fork child only: close duplicated descriptors without
    /// shutdown(2), release messages, mutex acquisition or joining vanished threads.
    pub fn inherited_close(self) -> Result<(), String> {
        if self.owner_pid == std::process::id() {
            return Err("inherited_close is only valid in a forked child".into());
        }
        let inherited = std::mem::ManuallyDrop::new(self);
        unsafe {
            libc::close(inherited.writer_fd);
        }
        Ok(())
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod retention_tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    #[test]
    fn missing_or_truncated_acknowledgements_are_retryable() {
        let directory = std::env::temp_dir().join(format!(
            "gpu-retention-ack-{}-{}",
            std::process::id(),
            crate::protocol::now_unix()
        ));
        std::fs::create_dir(&directory).unwrap();
        for (index, answer) in [b"".as_slice(), b"{\"ok\":true".as_slice()]
            .into_iter()
            .enumerate()
        {
            let socket = directory.join(index.to_string());
            let listener = UnixListener::bind(&socket).unwrap();
            let worker = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = String::new();
                BufReader::new(stream.try_clone().unwrap())
                    .read_line(&mut request)
                    .unwrap();
                stream.write_all(answer).unwrap();
            });
            let identity = crate::protocol::Resume {
                id: 1,
                token: "token".into(),
                key: crate::protocol::Placement { class: 1, ns: 0 },
                running: true,
            };
            assert!(matches!(
                deadline_update(
                    &socket,
                    &Request::RetainVram {
                        resume: identity,
                        seconds: 10.0
                    },
                    "VRAM retention"
                ),
                Err(SessionError::Retry(_))
            ));
            worker.join().unwrap();
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[cfg(test)]
mod place_tests {
    use super::*;
    use crate::protocol::{Category, Mode, WatchdogPolicy};
    use serde_json::{json, Value};
    use std::os::unix::net::UnixListener;

    /// A gpu-switch that answers each connection with `script`, in turn: what it heard is returned.
    fn fake(
        name: &str,
        scripts: Vec<Box<dyn FnOnce(&mut BufReader<UnixStream>, &mut UnixStream) + Send>>,
    ) -> (PathBuf, std::thread::JoinHandle<()>) {
        let directory = std::env::temp_dir().join(format!(
            "gpu-place-{name}-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let socket = directory.join("switch.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let serving = std::thread::spawn(move || {
            for script in scripts {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                script(&mut reader, &mut stream);
            }
        });
        (socket, serving)
    }

    fn heard(reader: &mut BufReader<UnixStream>) -> Value {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        serde_json::from_str(&line).unwrap_or(Value::Null)
    }

    fn reply(preparation_protocol: bool) -> Value {
        let mut reply = json!({"ok": true, "lease_protocol": 2, "job": {"id": 7, "handle": "s7",
            "resume": {"id": 7, "token": "ab", "key": {"class": 1, "ns": 9}, "running": false}}});
        if preparation_protocol {
            reply["preparation_protocol"] = json!(1);
        }
        reply
    }

    fn write(stream: &mut UnixStream, lines: &[Value]) {
        let text: String = lines.iter().map(|line| format!("{line}\n")).collect();
        stream.write_all(text.as_bytes()).unwrap();
    }

    fn request(prepares: bool) -> LeaseRequest {
        LeaseRequest {
            category: Category::Other,
            max_runtime_secs: None,
            expected_runtime_secs: Some(60),
            shutdown_grace_secs: None,
            mode: Mode::Shared,
            vram: Some(1 << 30),
            label: Some("place test".into()),
            priority: 0,
            group: None,
            requeued: false,
            cooperative: false,
            resume: None,
            watchdog: WatchdogPolicy::Standard,
            share: Some("kernel-test".into()),
            prepares,
        }
    }

    #[test]
    fn an_entry_ready_on_arrival_keeps_a_grant_sent_with_an_old_format_reply() {
        let (socket, serving) = fake(
            "old-reply",
            vec![Box::new(|reader, stream| {
                assert_eq!(heard(reader)["op"], "lease");
                write(stream, &[reply(false), json!({"granted": true})]);
                assert_eq!(heard(reader)["op"], "release");
            })],
        );
        let mut lease = Lease::acquire(&socket, request(false), |_| {}).unwrap();
        assert_eq!(lease.id, 7);
        lease.release();
        serving.join().unwrap();
    }

    #[test]
    fn a_place_is_refused_by_a_gpu_switch_without_places_before_any_ready() {
        let (socket, serving) = fake(
            "old-daemon",
            vec![Box::new(|reader, stream| {
                assert_eq!(heard(reader)["prepares"], true);
                write(stream, &[reply(false)]);
                assert_eq!(heard(reader), Value::Null, "the client said nothing more, no Ready");
            })],
        );
        let refused = Lease::place(&socket, request(true), &|| Ok(())).err().unwrap();
        assert!(matches!(refused, PlaceError::Unavailable(_)), "{refused:?}");
        serving.join().unwrap();
    }

    #[test]
    fn unread_lines_of_a_told_place_survive_its_hand_over() {
        let (socket, serving) = fake(
            "unread",
            vec![Box::new(|reader, stream| {
                heard(reader);
                write(stream, &[reply(true), json!({"waiting": "placed: position 2"}), json!({"prepare": true}),
                    json!({"granted": true})]);
                assert_eq!(heard(reader)["op"], "preparation_wait");
                assert_eq!(heard(reader)["op"], "ready");
                assert_eq!(heard(reader)["op"], "release");
            })],
        );
        let mut reasons = Vec::new();
        let told = Lease::place(&socket, request(true), &|| Ok(()))
            .unwrap()
            .told(&|| Ok(()), &mut |reason| reasons.push(reason.to_owned()))
            .unwrap();
        assert_eq!(reasons, ["placed: position 2"]);
        let mut told = told;
        told.preparation_wait("waiting for host memory: 1 bytes", false).unwrap();
        let handover = told.handover();
        assert!(String::from_utf8_lossy(&handover.unread).contains("\"granted\":true"));
        assert_eq!(unsafe { libc::fcntl(handover.fd, libc::F_GETFD) } & libc::FD_CLOEXEC, 0, "it survives an exec");
        let fd = handover.fd;
        let told = Told::inherited(serde_json::from_str(&serde_json::to_string(&handover).unwrap()).unwrap()).unwrap();
        assert_ne!(unsafe { libc::fcntl(fd, libc::F_GETFD) } & libc::FD_CLOEXEC, 0, "an adopted place closes on exec");
        // The grant was read before the hand-over, and gpu-switch sends nothing after the Ready: the adopter
        // is granted from the bytes the hand-over carried.
        let mut lease = told
            .granted(ReadyFigures::of(&request(true)), |_| {}, &|| Ok(()))
            .unwrap();
        lease.release();
        serving.join().unwrap();
    }

    #[test]
    fn a_told_place_registered_again_untold_is_lost_and_a_kept_one_declares_its_wait_again() {
        let (socket, serving) = fake(
            "lost",
            vec![
                Box::new(|reader, stream| {
                    heard(reader);
                    write(stream, &[reply(true), json!({"prepare": true})]);
                }),
                Box::new(|reader, stream| {
                    assert_eq!(heard(reader)["resume"]["id"], 7);
                    let mut kept = reply(true);
                    kept["preparation"] = json!({"told": true, "ready": false, "parked": true});
                    write(stream, &[kept]);
                    let resent = heard(reader);
                    assert_eq!((resent["op"].clone(), resent["parked"].clone()), (json!("preparation_wait"), json!(true)));
                }),
                Box::new(|reader, stream| {
                    heard(reader);
                    let mut again = reply(true);
                    again["reregistered"] = json!(true);
                    write(stream, &[again]);
                }),
            ],
        );
        let mut told = Lease::place(&socket, request(true), &|| Ok(()))
            .unwrap()
            .told(&|| Ok(()), &mut |_| {})
            .unwrap();
        // gpu-switch went away after the first connection: the report reattaches and is declared again.
        std::thread::sleep(Duration::from_millis(100));
        told.preparation_wait("waiting for host memory: 12884901888 bytes", true).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        let lost = told.preparation_wait("", false).unwrap_err();
        assert!(matches!(lost, PlaceError::Lost), "{lost:?}");
        assert_eq!(crate::front_end::place_exit(&lost), crate::front_end::PLACE_LOST);
        serving.join().unwrap();
    }

    #[test]
    fn a_transfer_acknowledged_by_the_resume_of_its_sender_is_done() {
        let child = std::process::Command::new("sleep").arg("30").spawn().unwrap();
        let to = ProcessIdentity { pid: child.id(), birth: crate::procs::started(child.id()).unwrap() };
        let (socket, serving) = fake(
            "transfer",
            vec![
                Box::new(|reader, stream| {
                    heard(reader);
                    write(stream, &[reply(true), json!({"prepare": true})]);
                    assert_eq!(heard(reader)["op"], "transfer_place");
                    // Saved, then replaced before it answered.
                }),
                Box::new(move |reader, stream| {
                    let resume = heard(reader);
                    assert_eq!(resume["resume"]["id"], 7);
                    write(stream, &[json!({"ok": true, "transferred": to})]);
                }),
            ],
        );
        let mut told = Lease::place(&socket, request(true), &|| Ok(()))
            .unwrap()
            .told(&|| Ok(()), &mut |_| {})
            .unwrap();
        told.transfer(child.id()).unwrap();
        serving.join().unwrap();
        let mut child = child;
        child.kill().unwrap();
        child.wait().unwrap();
    }

    /// `reply` declaring card-loss protocol `protocol`, or none as an ed650d4 switch.
    fn declaring(mut reply: Value, protocol: Option<u64>) -> Value {
        if let Some(protocol) = protocol {
            reply["job"]["device_loss_protocol"] = json!(protocol);
        }
        reply
    }

    #[test]
    fn a_lease_reply_without_the_loss_capability_reports_unreported() {
        let (socket, serving) = fake(
            "loss-unreported",
            vec![Box::new(|reader, stream| {
                assert_eq!(heard(reader)["op"], "lease");
                write(stream, &[reply(false), json!({"granted": true})]);
                assert_eq!(heard(reader)["op"], "release");
            })],
        );
        let mut lease = Lease::acquire(&socket, request(false), |_| {}).unwrap();
        assert_eq!(lease.device_loss(), DeviceLossReport::Unreported);
        assert_eq!(lease.device_loss().text(), r#"{"state":"unreported"}"#);
        lease.release();
        serving.join().unwrap();
    }

    #[test]
    fn a_reattachment_that_changes_the_capability_leaves_the_session_unreported_from_then_on() {
        for (first, second) in [(Some(1), None), (None, Some(1))] {
            // Before the grant: the first connection ends unanswered, and the session reattaches.
            let (socket, serving) = fake(
                "loss-pre-grant",
                vec![
                    Box::new(move |reader, stream| {
                        heard(reader);
                        write(stream, &[declaring(reply(false), first)]);
                    }),
                    Box::new(move |reader, stream| {
                        assert!(heard(reader)["resume"].is_object());
                        write(stream, &[declaring(reply(false), second), json!({"granted": true})]);
                        assert_eq!(heard(reader)["op"], "release");
                    }),
                ],
            );
            let mut lease = Lease::acquire(&socket, request(false), |_| {}).unwrap();
            assert_eq!(lease.device_loss(), DeviceLossReport::Unreported, "{first:?} then {second:?}");
            lease.release();
            serving.join().unwrap();
            // After the grant: the watcher reconnects; a notice after the reply shows that it read the reply.
            let (socket, serving) = fake(
                "loss-watcher",
                vec![
                    Box::new(move |reader, stream| {
                        heard(reader);
                        write(stream, &[declaring(reply(false), first), json!({"granted": true})]);
                    }),
                    Box::new(move |reader, stream| {
                        assert_eq!(heard(reader)["resume"]["running"], true);
                        let violation = json!({"violation": {"rule": "no_vram", "run": 1, "text": "t", "at": 1.0}});
                        write(stream, &[declaring(reply(false), second), violation]);
                        assert_eq!(heard(reader)["op"], "release");
                    }),
                ],
            );
            let mut lease = Lease::acquire(&socket, request(false), |_| {}).unwrap();
            while lease.notices.lock().unwrap().warnings.is_empty() {
                std::thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(lease.device_loss(), DeviceLossReport::Unreported, "{first:?} then {second:?}");
            lease.release();
            serving.join().unwrap();
        }
    }
}
