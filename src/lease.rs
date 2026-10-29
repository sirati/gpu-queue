//! Blocking native lease client. This module owns the wire session, not queue
//! policy. A caller receives a typed shutdown notification plus an explicit
//! acknowledgement API. All enforcement remains in the existing daemon engine.
use crate::protocol::{ClientMsg, LeaseJob, LeaseRequest, Request, ShutdownRequest};
use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::Path;
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
    pub pending: Arc<Mutex<Option<ShutdownRequest>>>,
    watcher: Option<JoinHandle<()>>,
    pub id: u64,
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
}

impl SessionError {
    fn reason(self) -> String {
        match self {
            Self::Retry(reason) | Self::Refused(reason) | Self::Interrupted(reason) => reason,
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
type Opened = (Arc<UnixStream>, BufReader<ReadStream>, LeaseJob);

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
    Ok((stream, reader, job))
}

fn reconnect(
    socket: &Path,
    request: &LeaseRequest,
    check: &impl Fn() -> Result<(), String>,
) -> Result<Opened, String> {
    if request.resume.is_none() {
        return Err("lease reply missing resume token".into());
    }
    let deadline = Instant::now() + RESUME_PATIENCE;
    let mut wait = Duration::from_millis(50);
    loop {
        check()?;
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
                    return Err(error);
                }
                std::thread::sleep(wait);
                wait = (wait * 2).min(Duration::from_millis(100));
            }
            Err(error) => return Err(error.reason()),
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

fn retention_update(
    socket: &Path,
    identity: &crate::protocol::Resume,
    seconds: f64,
) -> Result<f64, SessionError> {
    let stream = UnixStream::connect(socket).map_err(|e| SessionError::Retry(e.to_string()))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| SessionError::Retry(e.to_string()))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| SessionError::Retry(e.to_string()))?;
    send(
        &stream,
        &Request::RetainVram {
            resume: identity.clone(),
            seconds,
        },
    )
    .map_err(SessionError::Retry)?;
    let mut reply = String::new();
    let size = BufReader::new(stream)
        .read_line(&mut reply)
        .map_err(|e| SessionError::Retry(e.to_string()))?;
    if size == 0 {
        return Err(SessionError::Retry(
            "daemon disconnected before VRAM retention acknowledgement".into(),
        ));
    }
    let reply: crate::protocol::Reply =
        serde_json::from_str(&reply).map_err(|e| match e.is_eof() {
            true => SessionError::Retry(e.to_string()),
            false => SessionError::Refused(e.to_string()),
        })?;
    if !reply.ok {
        return Err(SessionError::Refused(
            reply
                .error
                .unwrap_or_else(|| "VRAM retention refused".into()),
        ));
    }
    reply
        .message
        .ok_or_else(|| {
            SessionError::Refused("VRAM retention acknowledgement missing deadline".into())
        })?
        .parse()
        .map_err(|_| SessionError::Refused("invalid VRAM retention deadline".into()))
}

impl Lease {
    pub fn acquire(
        socket: &Path,
        request: LeaseRequest,
        notify: impl Fn(ShutdownRequest) + Send + 'static,
    ) -> Result<Self, String> {
        Self::acquire_checked(socket, request, notify, || Ok(()))
    }

    pub fn acquire_checked(
        socket: &Path,
        request: LeaseRequest,
        notify: impl Fn(ShutdownRequest) + Send + 'static,
        check: impl Fn() -> Result<(), String>,
    ) -> Result<Self, String> {
        let mut request = request;
        let (stream, mut reader, job) =
            open(socket, &request, &check).map_err(SessionError::reason)?;
        let cancel_supported = job.cancel_protocol == 1;
        let retention_supported = job.vram_retention_protocol == 1;
        request.resume = Some(job.resume);
        let mut id = job.id;
        let mut line = String::new();
        loop {
            line.clear();
            match read_checked(&mut reader, &mut line, &check) {
                Ok(0) | Err(SessionError::Retry(_)) => {
                    let (next, next_reader, job) = reconnect(socket, &request, &check)?;
                    id = job.id;
                    request.resume = Some(job.resume);
                    replace(&stream, &mut reader, next, next_reader)?;
                    continue;
                }
                Err(error) => return Err(error.reason()),
                Ok(_) => {}
            }
            if matches!(
                serde_json::from_str(&line),
                Ok(ClientMsg::Granted { granted: true })
            ) {
                break;
            }
        }
        request.resume.as_mut().expect("resume token").running = true;
        let identity = request.resume.clone().expect("resume token");
        let client_socket = socket.to_owned();
        let writer_fd = stream.as_raw_fd();
        let writer = Arc::new(Mutex::new(()));
        let connected = Arc::new(AtomicBool::new(true));
        let released = Arc::new(AtomicBool::new(false));
        let pending = Arc::new(Mutex::new(None));
        let stop = released.clone();
        let state = pending.clone();
        let socket = socket.to_owned();
        let watch_stream = stream.clone();
        let watch_writer = writer.clone();
        let watch_connected = connected.clone();
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
                    if stop.load(Ordering::Acquire) {
                        return;
                    }
                    let resumed = reconnect(&socket, &request, &check).and_then(
                        |(next, next_reader, job)| {
                            let _guard = watch_writer.lock().unwrap_or_else(|e| e.into_inner());
                            check()?;
                            replace(&watch_stream, &mut reader, next, next_reader)?;
                            watch_connected.store(true, Ordering::Release);
                            request.resume = Some(job.resume);
                            // The local process is already admitted. Never let a later
                            // reconnect represent it as a fresh queued acquisition.
                            request.resume.as_mut().expect("resume token").running = true;
                            Ok(())
                        },
                    );
                    match resumed {
                        Ok(()) => continue,
                        Err(_) if stop.load(Ordering::Acquire) => return,
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
                    _ => {}
                }
            }
        });
        Ok(Self {
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
            pending,
            watcher: Some(watcher),
            id,
        })
    }

    /// End exclusivity permanently for a fixed no-compute VRAM retention phase.
    /// The independent authenticated RPC needs no session writer lock.
    pub fn retain_vram(&self, seconds: f64) -> Result<f64, String> {
        self.check_owner()?;
        if self.is_released() {
            return Err("lease released".into());
        }
        if !self.retention_supported {
            return Err("daemon lacks bounded VRAM retention support".into());
        }
        let deadline = Instant::now() + RESUME_PATIENCE;
        loop {
            if self.is_released() {
                return Err("lease released".into());
            }
            match retention_update(&self.socket, &self.identity, seconds) {
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
                    false,
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
        self.send_active(&Request::Cancelled { reason }, true)
    }

    pub fn fail(&self, reason: String) -> Result<(), String> {
        self.check_owner()?;
        self.send_active(&Request::Failed { reason }, true)
    }

    fn send_active(&self, request: &Request, closing: bool) -> Result<(), String> {
        let deadline = Instant::now() + RESUME_PATIENCE;
        loop {
            let guard = self.writer.lock().unwrap_or_else(|e| e.into_inner());
            if self.released.load(Ordering::Acquire) {
                return Err("lease released".into());
            }
            if self.connected.load(Ordering::Acquire) {
                match send(&self.stream, request) {
                    Ok(()) => {
                        if closing {
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
            let _ = self.send_active(&Request::Release, true);
        }
        self.released.store(true, Ordering::Release);
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
                retention_update(&socket, &identity, 10.0),
                Err(SessionError::Retry(_))
            ));
            worker.join().unwrap();
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
}
