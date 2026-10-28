//! Compiler authority: one Tokio actor owns all mutable queue state.
//! Connections only send commands through mpsc and await oneshot replies.
pub mod client;
mod engine;
mod fairness;
pub mod model;
#[cfg(feature = "python")]
pub(crate) mod python;
mod resources;
mod store;
pub mod user_service;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::os::linux::net::SocketAddrExt;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};

pub struct Config {
    pub state: PathBuf,
    pub minimum_ram: u64,
    pub average_ram: u64,
}

pub fn endpoint(state: &std::path::Path) -> Result<std::os::unix::net::SocketAddr, String> {
    use std::os::unix::ffi::OsStrExt;
    let digest = Sha256::digest(state.as_os_str().as_bytes());
    let suffix: String = digest[..12].iter().map(|b| format!("{b:02x}")).collect();
    std::os::unix::net::SocketAddr::from_abstract_name(
        format!("rust-compile-{}-{suffix}", unsafe { libc::getuid() }).as_bytes(),
    )
    .map_err(|e| e.to_string())
}

enum Message {
    Request(Value, oneshot::Sender<Result<Value, String>>),
    Resources(resources::Snapshot),
}

async fn connection(stream: UnixStream, sender: mpsc::Sender<Message>) -> Result<(), String> {
    let credentials = stream.peer_cred().map_err(|e| e.to_string())?;
    if credentials.uid() != unsafe { libc::getuid() } {
        return Err("compiler endpoint only accepts its own user".into());
    }
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader).take(16 * 1024 * 1024);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .map_err(|e| e.to_string())?;
    let mut request: Value = serde_json::from_str(&line).map_err(|e| e.to_string())?;
    request["_peer_pid"] = serde_json::json!(credentials.pid());
    let (reply, receive) = oneshot::channel();
    sender
        .send(Message::Request(request, reply))
        .await
        .map_err(|e| e.to_string())?;
    let answer = match receive.await.map_err(|e| e.to_string())? {
        Ok(value) => json!({"ok": true, "value": value}),
        Err(error) => json!({"ok": false, "error": error}),
    };
    let mut encoded = serde_json::to_vec(&answer).map_err(|e| e.to_string())?;
    encoded.push(b'\n');
    writer.write_all(&encoded).await.map_err(|e| e.to_string())
}

pub async fn run(mut config: Config) -> Result<(), String> {
    tokio::fs::create_dir_all(&config.state)
        .await
        .map_err(|e| e.to_string())?;
    config.state = std::fs::canonicalize(&config.state).map_err(|e| e.to_string())?;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let metadata = std::fs::metadata(&config.state).map_err(|e| e.to_string())?;
    if metadata.uid() != unsafe { libc::getuid() } {
        return Err("compiler state must belong to its service user".into());
    }
    std::fs::set_permissions(&config.state, std::fs::Permissions::from_mode(0o700))
        .map_err(|e| e.to_string())?;
    // The endpoint itself is the live service. No filesystem ownership artifact
    // exists to survive a crash, and bind is atomic for concurrent starters.
    let listener = std::os::unix::net::UnixListener::bind_addr(&endpoint(&config.state)?)
        .map_err(|e| format!("compiler endpoint: {e}"))?;
    listener.set_nonblocking(true).map_err(|e| e.to_string())?;
    let listener = UnixListener::from_std(listener).map_err(|e| e.to_string())?;
    let (sender, mut messages) = mpsc::channel(1024);
    let samples = sender.clone();
    tokio::spawn(async move {
        let mut sampler = resources::Sampler::default();
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(250));
        loop {
            tick.tick().await;
            if let Ok(sample) = sampler.sample() {
                if samples.send(Message::Resources(sample)).await.is_err() {
                    break;
                }
            }
        }
    });
    let mut engine = engine::Engine::restore(config).await?;
    notify("READY=1\nSTATUS=Serving compiler requests through the channel actor");
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| e.to_string())?;
    loop {
        tokio::select! {
            incoming = listener.accept() => {
                let (stream, _) = incoming.map_err(|e| e.to_string())?;
                let tx = sender.clone();
                tokio::spawn(async move {
                    let _ = tokio::time::timeout(std::time::Duration::from_secs(30), connection(stream, tx)).await;
                });
            }
            message = messages.recv() => match message {
                Some(Message::Request(request, reply)) => { let _ = reply.send(engine.request(request).await); }
                Some(Message::Resources(snapshot)) => {
                    engine.tick(snapshot).await?;
                    notify("WATCHDOG=1");
                    if engine.drained() { break; }
                }
                None => break,
            },
            _ = terminate.recv() => { engine.drain(); notify("STOPPING=1\nSTATUS=Draining compiler commands"); }
            _ = tokio::signal::ctrl_c() => { engine.drain(); notify("STOPPING=1\nSTATUS=Draining compiler commands"); }
        }
    }
    // Graceful replacement drains direct children and retains exact exit codes.
    // A crash leaves durable custody, never an automatic duplicate execution.
    Ok(())
}

/// systemd supervises the actor, not its workers. No owner polling is needed.
fn notify(message: &str) {
    let Some(address) = std::env::var_os("NOTIFY_SOCKET") else {
        return;
    };
    let Ok(socket) = std::os::unix::net::UnixDatagram::unbound() else {
        return;
    };
    use std::os::unix::ffi::OsStrExt;
    let bytes = address.as_os_str().as_bytes();
    if bytes.first() == Some(&b'@') {
        if let Ok(address) = std::os::unix::net::SocketAddr::from_abstract_name(&bytes[1..]) {
            let _ = socket.send_to_addr(message.as_bytes(), &address);
        }
    } else {
        let _ = socket.send_to(message.as_bytes(), std::path::Path::new(&address));
    }
}
