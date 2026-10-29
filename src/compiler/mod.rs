//! Compiler authority: one Tokio actor owns all mutable queue state.
//! Connections only send commands through mpsc and await oneshot replies.
pub mod client;
mod engine;
mod fairness;
mod handoff;
mod host_memory;
pub mod memory_client;
#[cfg(feature = "python")]
mod memory_python;
pub mod model;
#[cfg(feature = "python")]
pub(crate) mod python;
mod resources;
mod store;
pub mod user_service;

pub fn handoff_capabilities() -> serde_json::Value {
    handoff::capabilities()
}
pub fn service_generation() -> u64 {
    handoff::service_generation()
}

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
    Exit(String, i32),
    ReaperGone,
    CommitUpgrade(PathBuf),
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
    let upgrade = request["op"] == "upgrade";
    let upgrade_path = request["executable"].as_str().map(PathBuf::from);
    // Reject incompatible successors outside the actor, before they restore
    // the durable job catalogue or pause any queue operation.
    if upgrade {
        let answer = match &upgrade_path {
            Some(path) => handoff::validate_successor(path).await,
            None => Err("upgrade requires a successor executable".into()),
        };
        if let Err(error) = answer {
            let mut encoded = serde_json::to_vec(&json!({"ok":false,"error":error}))
                .map_err(|e| e.to_string())?;
            encoded.push(b'\n');
            writer
                .write_all(&encoded)
                .await
                .map_err(|e| e.to_string())?;
            return Ok(());
        }
    }
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
    writer
        .write_all(&encoded)
        .await
        .map_err(|e| e.to_string())?;
    if upgrade && answer["ok"] == true {
        if let Some(path) = upgrade_path {
            sender
                .send(Message::CommitUpgrade(path))
                .await
                .map_err(|e| e.to_string())?;
        }
    }
    Ok(())
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
    let inherited = handoff::inherited(&config).await?;
    let (listener, reaper, inherited_ids) = if let Some((listener, stream, ids)) = inherited {
        (listener, Some(stream), ids)
    } else {
        let listener = std::os::unix::net::UnixListener::bind_addr(&endpoint(&config.state)?)
            .map_err(|e| format!("compiler endpoint: {e}"))?;
        listener.set_nonblocking(true).map_err(|e| e.to_string())?;
        (
            UnixListener::from_std(listener).map_err(|e| e.to_string())?,
            None,
            std::collections::BTreeSet::new(),
        )
    };
    let (sender, mut messages) = mpsc::channel(1024);
    let (sampled, mut samples) = mpsc::channel(1);
    let sampler_task = tokio::spawn(async move {
        let mut sampler = resources::Sampler::default();
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(250));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let at = std::time::Instant::now();
            let measured = tokio::task::spawn_blocking(move || {
                let result = sampler.sample();
                (sampler, result)
            })
            .await;
            let Ok((next, result)) = measured else { break };
            sampler = next;
            if let Ok(sample) = result {
                match sampled.try_send((at, sample)) {
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                    _ => {}
                }
            }
        }
    });
    let mut engine = engine::Engine::restore(config).await?;
    engine.await_reapers(&inherited_ids);
    if let Some(stream) = reaper {
        handoff::ready(stream, sender.clone()).await?;
    }
    notify("READY=1\nSTATUS=Serving compiler requests through the channel actor");
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| e.to_string())?;
    let mut pending_upgrade = None;
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
                Some(Message::Request(request, reply)) => {
                    if request["op"] == "upgrade" {
                        let path=request["executable"].as_str().map(PathBuf::from);
                        let answer=if let Some(path)=path.filter(|p|p.is_absolute() && p.is_file()) {
                            match std::env::current_exe().map_err(|e|e.to_string()).and_then(|current|handoff::same_code(&current,&path)) {
                                Ok(true)=>Ok(json!({"already_current":true})),
                                Err(error)=>Err(error),
                                Ok(false)=> if pending_upgrade.as_ref().is_some_and(|current|current!=&path) {
                                    Err("another compiler handover is already pending".into())
                                } else {pending_upgrade=Some(path);Ok(json!({"accepted":true}))}
                            }
                        }else{Err("upgrade requires an absolute successor executable".into())};
                        let _=reply.send(answer);
                    } else { let _=reply.send(engine.request(request).await); }
                }
                Some(Message::Exit(id,code))=>engine.external_exit(&id,code).await?,
                Some(Message::ReaperGone)=>engine.reaper_gone(),
                Some(Message::CommitUpgrade(path))=>{
                    if pending_upgrade.as_ref()==Some(&path) {
                        pending_upgrade=None;
                        engine.publish_reaper_custody().await?;
                        match handoff::successor(&path,&engine.handoff_config(),&listener,&engine.reaping_ids()).await {
                            Ok((mut process,mut stream))=>{
                                let pid=process.id().ok_or("successor PID missing")?;
                                if let Err(error)=handoff::activate(&mut stream,pid).await {
                                    notify(&format!("MAINPID={}\nSTATUS=Original compiler retains scheduling",std::process::id()));
                                    let _=process.kill().await;
                                    eprintln!("compiler handover activation refused: {error}");
                                    continue;
                                }
                                sampler_task.abort();
                                let state=engine.handoff_config().state;
                                let children=engine.take_children();
                                let inherited=engine.inherited_reaping_ids();
                                drop(listener);drop(engine);
                                return handoff::reap_only(stream,children,inherited,messages,state).await;
                            },
                            Err(error)=>eprintln!("compiler handover refused; original authority retained: {error}"),
                        }
                    }
                }
                None => break,
            },
            Some((at, snapshot)) = samples.recv() => {
                if at.elapsed() <= std::time::Duration::from_secs(1) {
                    engine.tick(snapshot).await?;
                    notify("WATCHDOG=1");
                    if engine.drained() { break; }
                }
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
