//! Compiler authority: one Tokio actor owns all mutable queue state.
//! Connections only send commands through mpsc and await oneshot replies.
mod budget;
pub mod client;
mod containment;
mod contract;
mod decision;
mod engine;
mod fairness;
mod handoff;
mod host_memory;
mod memory_growth;
pub mod memory_client;
#[cfg(feature = "python")]
mod memory_python;
pub mod model;
mod party;
mod precedence;
mod progress;
#[cfg(feature = "python")]
pub(crate) mod python;
mod resources;
mod store;
mod trace;
pub mod user_service;
mod watch;

pub use party::bounded as bounded_priority;

pub fn handoff_capabilities() -> serde_json::Value {
    handoff::capabilities()
}
pub fn service_generation() -> u64 {
    handoff::service_generation()
}
/// Checks the admission records in a compile service's standard error against the admission rules; returns how
/// many it checked.
pub fn check_admission_records(journal: &str) -> Result<usize, String> {
    let records = decision::parse(journal)?;
    decision::check(&records)?;
    Ok(records.len())
}

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::os::linux::net::SocketAddrExt;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};

// Bounds receiving a request and delivering its reply, never the awaited work.
const REQUEST_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

pub struct Config {
    pub state: PathBuf,
    pub minimum_ram: u64,
    pub average_ram: u64,
}

/// The state directory of this user's compile service when none is named.
pub fn default_state() -> PathBuf {
    PathBuf::from(format!("/var/tmp/rust-compile-{}", unsafe { libc::getuid() }))
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
    Request(Value, oneshot::Sender<Result<Value, String>>, trace::Sent),
    Exit(String, i32),
    ReaperGone,
    CommitUpgrade(PathBuf),
}

async fn connection(stream: UnixStream, sender: mpsc::Sender<Message>, mut span: trace::Span) -> Result<(), String> {
    let sequence = trace::next_sequence();
    span.note("seq", sequence);
    span.mark("spawned");
    let credentials = stream.peer_cred().map_err(|e| e.to_string())?;
    if credentials.uid() != unsafe { libc::getuid() } {
        return Err("compiler endpoint only accepts its own user".into());
    }
    span.note("peer_pid", credentials.pid());
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader).take(16 * 1024 * 1024);
    let mut line = String::new();
    tokio::time::timeout(REQUEST_DEADLINE, reader.read_line(&mut line))
        .await
        .map_err(|_| "compiler request was not received in time")?
        .map_err(|e| e.to_string())?;
    span.mark("read");
    let mut request: Value = serde_json::from_str(&line).map_err(|e| e.to_string())?;
    span.relabel(request["op"].as_str().unwrap_or("?").to_owned());
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
            return deliver(&mut writer, &json!({"ok":false,"error":error})).await;
        }
    }
    let (reply, receive) = oneshot::channel();
    span.mark("parse");
    sender
        .send(Message::Request(request, reply, trace::Sent::now(sequence)))
        .await
        .map_err(|e| e.to_string())?;
    span.mark("send");
    // A parked wait is answered when its state is reached. A client that leaves
    // first drops the receiver, and the actor prunes the parked reply.
    let answer = tokio::select! {
        answer = receive => answer.map_err(|e| e.to_string())?,
        _ = departed(&mut reader) => {
            span.mark("departed");
            return Ok(());
        }
    };
    span.mark("answer");
    let answer = match answer {
        Ok(value) => json!({"ok": true, "value": value}),
        Err(error) => json!({"ok": false, "error": error}),
    };
    deliver(&mut writer, &answer).await?;
    span.mark("deliver");
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

async fn deliver(writer: &mut (impl AsyncWrite + Unpin), answer: &Value) -> Result<(), String> {
    let mut encoded = serde_json::to_vec(answer).map_err(|e| e.to_string())?;
    encoded.push(b'\n');
    tokio::time::timeout(REQUEST_DEADLINE, writer.write_all(&encoded))
        .await
        .map_err(|_| "compiler reply was not accepted in time")?
        .map_err(|e| e.to_string())
}

/// End of stream means the client no longer awaits this reply.
async fn departed(reader: &mut (impl AsyncRead + Unpin)) {
    let mut discarded = [0u8; 64];
    while matches!(reader.read(&mut discarded).await, Ok(count) if count > 0) {}
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
    let (sampled, mut samples) = tokio::sync::watch::channel(None);
    let sampler_task = tokio::spawn(async move {
        let mut sampler = resources::Sampler::default();
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(250));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let at = std::time::Instant::now();
            let mut span = trace::Span::begin("sampler", "sample");
            let measured = tokio::task::spawn_blocking(move || {
                trace::blocking(at, || {
                    let result = sampler.sample();
                    (sampler, result)
                })
            })
            .await;
            let Ok(((next, result), cost)) = measured else { break };
            span.mark("sample");
            span.note("queued_ms", cost.queued_ms);
            span.note("call_ms", cost.call_ms);
            span.note("pool_run_ms", cost.run_ms);
            span.note("pool_run_delay_ms", cost.run_delay_ms);
            if let Ok(sample) = result.as_ref() {
                span.note("processes", sample.processes.len());
            }
            drop(span);
            sampler = next;
            // The actor reads only the newest sample, so a stale one never
            // displaces it while a long request runs.
            if let Ok(sample) = result {
                if sampled.send(Some((at, sample))).is_err() {
                    break;
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
    let mut alive = std::time::Instant::now();
    loop {
        tokio::select! {
            incoming = listener.accept() => {
                let (stream, _) = incoming.map_err(|e| e.to_string())?;
                let tx = sender.clone();
                let span = trace::Span::begin("connection", "?");
                tokio::spawn(async move {
                    let _ = connection(stream, tx, span).await;
                });
            }
            message = messages.recv() => match message {
                Some(Message::Request(request, reply, sent)) => {
                    let mut span = trace::Span::begin("message", format!("request:{}", request["op"].as_str().unwrap_or("?")));
                    span.note("seq", sent.sequence);
                    span.note("queue_ms", sent.at.elapsed().as_secs_f64() * 1e3);
                    span.note("channel_backlog", messages.len());
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
                    } else { engine.serve(request, reply).await; }
                    span.mark("serve");
                }
                Some(Message::Exit(id,code))=>{
                    let mut span = trace::Span::begin("message", "exit");
                    span.note("job", id.as_str());
                    engine.external_exit(&id,code).await?;
                    span.mark("external_exit");
                }
                Some(Message::ReaperGone)=>{
                    let _span = trace::Span::begin("message", "reaper_gone");
                    engine.reaper_gone();
                }
                Some(Message::CommitUpgrade(path))=>{
                    let _span = trace::Span::begin("message", "commit_upgrade");
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
            Ok(()) = samples.changed() => {
                let mut span = trace::Span::begin("message", "tick");
                let latest = samples.borrow_and_update().clone();
                if let Some((at, _)) = latest.as_ref() {
                    span.note("sample_age_ms", at.elapsed().as_secs_f64() * 1e3);
                }
                span.note("channel_backlog", messages.len());
                span.mark("sample");
                if latest.as_ref().is_some_and(|(at, _)| at.elapsed() > std::time::Duration::from_secs(1)) {
                    span.note("stale", true);
                    span.force();
                }
                if let Some((_, snapshot)) = latest.filter(|(at, _)| at.elapsed() <= std::time::Duration::from_secs(1)) {
                    engine.tick(snapshot).await?;
                    span.mark("tick");
                    if engine.drained() { break; }
                }
            },
            _ = terminate.recv() => { engine.drain(); notify("STOPPING=1\nSTATUS=Draining compiler commands"); }
            _ = tokio::signal::ctrl_c() => { engine.drain(); notify("STOPPING=1\nSTATUS=Draining compiler commands"); }
        }
        // Liveness is progress through the loop: a request awaiting slow disk
        // still returns here, a hung actor never does.
        if alive.elapsed() >= std::time::Duration::from_secs(1) {
            notify("WATCHDOG=1");
            alive = std::time::Instant::now();
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
