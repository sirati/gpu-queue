//! Blocking native lease client. This module owns the wire session, not queue
//! policy. A caller receives a typed shutdown notification plus an explicit
//! acknowledgement API. All enforcement remains in the existing daemon engine.
use crate::protocol::{ClientMsg, LeaseRequest, Request, ShutdownRequest};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

pub struct Lease {
    owner_pid: u32,
    writer_fd: RawFd,
    reader_fd: RawFd,
    _reader: Arc<UnixStream>,
    writer: Mutex<UnixStream>,
    released: Arc<AtomicBool>,
    pub pending: Arc<Mutex<Option<ShutdownRequest>>>,
    watcher: Option<JoinHandle<()>>,
    pub id: u64,
}

struct ReadStream(Arc<UnixStream>);
impl Read for ReadStream {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        (&*self.0).read(bytes)
    }
}

fn send(stream: &mut UnixStream, request: &Request) -> Result<(), String> {
    let mut line = serde_json::to_vec(request).map_err(|e| e.to_string())?;
    line.push(b'\n');
    stream.write_all(&line).map_err(|e| e.to_string())
}

fn read_checked(
    reader: &mut BufReader<ReadStream>,
    line: &mut String,
    check: &impl Fn() -> Result<(), String>,
) -> Result<usize, String> {
    loop {
        check()?;
        match reader.read_line(line) {
            Ok(size) => return Ok(size),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error.to_string()),
        }
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

    pub fn acquire_checked(
        socket: &Path,
        request: LeaseRequest,
        notify: impl Fn(ShutdownRequest) + Send + 'static,
        check: impl Fn() -> Result<(), String>,
    ) -> Result<Self, String> {
        let cooperative_shutdown = request.shutdown_grace_secs.is_some();
        let mut stream = UnixStream::connect(socket).map_err(|e| e.to_string())?;
        stream
            .set_read_timeout(Some(std::time::Duration::from_millis(100)))
            .map_err(|e| e.to_string())?;
        send(&mut stream, &Request::Lease(request))?;
        let reader_stream = Arc::new(stream.try_clone().map_err(|e| e.to_string())?);
        let mut reader = BufReader::new(ReadStream(reader_stream.clone()));
        let reader_fd = reader_stream.as_raw_fd();
        let writer_fd = stream.as_raw_fd();
        let mut line = String::new();
        read_checked(&mut reader, &mut line, &check)?;
        let reply: serde_json::Value = serde_json::from_str(&line).map_err(|e| e.to_string())?;
        if reply["ok"] != true {
            return Err(reply.to_string());
        }
        if let Some(warning) = reply["warning"].as_str() {
            eprintln!("gpu-queue: warning: {warning}");
        }
        if cooperative_shutdown && reply["shutdown_protocol"] != 1 {
            return Err(
                "daemon does not support cooperative shutdown; upgrade it before using this API"
                    .into(),
            );
        }
        let id = reply["job"]["id"]
            .as_u64()
            .ok_or("lease reply missing job id")?;
        loop {
            line.clear();
            if read_checked(&mut reader, &mut line, &check)? == 0 {
                return Err("daemon disconnected while awaiting the GPU grant".into());
            }
            if matches!(
                serde_json::from_str(&line),
                Ok(ClientMsg::Granted { granted: true })
            ) {
                break;
            }
        }
        stream.set_read_timeout(None).map_err(|e| e.to_string())?;
        let released = Arc::new(AtomicBool::new(false));
        let pending = Arc::new(Mutex::new(None));
        let stop = released.clone();
        let state = pending.clone();
        let watcher = std::thread::spawn(move || {
            loop {
                line.clear();
                if reader.read_line(&mut line).unwrap_or(0) == 0 {
                    if !stop.load(Ordering::Acquire) {
                        // Never silently become an independent queue after losing authority.
                        notify(ShutdownRequest {
                            request_id: "authority-lost".into(),
                            reason: "GPU queue authority disconnected".into(),
                            deadline: crate::protocol::now_unix(),
                            permanent: true,
                            handler_started: false,
                        });
                        unsafe {
                            libc::kill(std::process::id() as i32, libc::SIGTERM);
                        }
                    }
                    return;
                }
                match serde_json::from_str::<ClientMsg>(&line) {
                    Ok(ClientMsg::Shutdown { shutdown }) => {
                        *state.lock().unwrap_or_else(|e| e.into_inner()) = Some(shutdown.clone());
                        notify(shutdown);
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
            writer_fd,
            reader_fd,
            _reader: reader_stream,
            writer: Mutex::new(stream),
            released,
            pending,
            watcher: Some(watcher),
            id,
        })
    }

    pub fn shutdown_started(&self, request_id: &str) -> Result<(), String> {
        self.check_owner()?;
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        match &mut *pending {
            Some(request) if request.request_id == request_id => {
                send(
                    &mut self.writer.lock().unwrap_or_else(|e| e.into_inner()),
                    &Request::ShutdownStarted {
                        request_id: request_id.into(),
                    },
                )?;
                request.handler_started = true;
                Ok(())
            }
            _ => Err("stale or unknown shutdown request".into()),
        }
    }

    pub fn fail(&self, reason: String) -> Result<(), String> {
        self.check_owner()?;
        self.released.store(true, Ordering::Release);
        send(
            &mut self.writer.lock().unwrap_or_else(|e| e.into_inner()),
            &Request::Failed { reason },
        )
    }

    pub fn release(&mut self) {
        if self.owner_pid != std::process::id() {
            return;
        }
        if self.released.swap(true, Ordering::AcqRel) && self.watcher.is_none() {
            return;
        }
        let mut stream = self.writer.lock().unwrap_or_else(|e| e.into_inner());
        let _ = send(&mut stream, &Request::Release);
        let _ = stream.shutdown(std::net::Shutdown::Both);
        drop(stream);
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
            libc::close(inherited.reader_fd);
        }
        Ok(())
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.release();
    }
}
