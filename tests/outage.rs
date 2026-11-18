//! Outage gates of the compile-service transport (PLAN-I §8), on disposable services and fake endpoints only.
//! The long gates are ignored by default: `cargo test --test outage -- --ignored --nocapture`.
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use gpu_arbiter::compiler::client::{CancelToken, Client, Deadline, Failure, AUTHORITY_PATIENCE, PROBE_INTERVAL};
use gpu_arbiter::compiler::memory_client::{HostMemoryClaim, CLEANUP_PATIENCE};
use gpu_arbiter::compiler::model::{Group, Purpose};
use serde_json::{json, Value};

mod compile_service;
use compile_service::{directory, Service};

/// Ordinary scheduling tolerance for timing assertions on this loaded host.
const TOLERANCE: Duration = Duration::from_secs(3);

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

/// `future` within `limit`, timed only once the runtime polls it.
async fn timed<T>(limit: Duration, future: impl std::future::Future<Output = T>) -> Result<T, tokio::time::error::Elapsed> {
    tokio::time::timeout(limit, future).await
}

fn listener(state: &Path) -> UnixListener {
    UnixListener::bind_addr(&gpu_arbiter::compiler::endpoint(&std::fs::canonicalize(state).unwrap()).unwrap()).unwrap()
}

/// An endpoint that accepts every connection and never answers.
fn silent(state: &Path) {
    let listener = listener(state);
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming() {
            held.push(stream);
        }
    });
}

/// One job that is never admitted: it declares all the memory the service admits against.
fn unadmitted(service: &Service, group_id: &str) -> Group {
    serde_json::from_value(json!({"group_id": group_id, "jobs": [{
        "job_id": format!("never-{group_id}"), "argv": ["/bin/sh", "-c", "exit 0"], "cwd": service.root(),
        "memory_bytes": service.capacity(), "expected_runtime_secs": 60}]}))
    .unwrap()
}

fn cancelled(service: &Service, group_id: &str) {
    runtime()
        .block_on(Client::new(&service.state).unwrap().cancel(group_id, None, "the gate ends its wait", true, Deadline::snapshot()))
        .unwrap();
}

fn eventually(what: &str, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !predicate() {
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn parked(service: &Service) -> (u64, u64) {
    let health = service.health();
    (health["parked_group_waits"].as_u64().unwrap(), health["host_memory"]["parked_waits"].as_u64().unwrap())
}

fn claim(state: &Path, bytes: u64, label: &str) -> HostMemoryClaim {
    HostMemoryClaim::new(Client::new(state).unwrap(), bytes, label.into(), 3, Some(Purpose::HostOnly)).unwrap()
}

fn unavailable_during(answer: &Result<Value, Failure>, operation: &str) -> bool {
    matches!(answer, Err(Failure::Unavailable(outage)) if outage.operation == operation && outage.source == "compile service")
}

/// Each request an endpoint of a gate received, in order: `None` for one it did not read whole.
type Received = Arc<Mutex<Vec<Option<Value>>>>;

/// The requests for `operation` among those received, in order.
fn issued(received: &Received, operation: &str) -> Vec<Value> {
    received.lock().unwrap().iter().flatten().filter(|request| request["op"] == operation).cloned().collect()
}

/// The most of one request a stand-in endpoint reads. A longer request is closed with the rest unread, which
/// fails its writer.
const STAND_IN_READ: u64 = 64 * 1024;

/// The request on `stream`, unless it was not read whole within `STAND_IN_READ`.
fn request(stream: &UnixStream) -> Option<Value> {
    let mut line = String::new();
    let _ = BufReader::new(stream).take(STAND_IN_READ).read_line(&mut line);
    serde_json::from_str(&line).ok()
}

/// What a stand-in endpoint does with one request.
enum Reply {
    /// Answers with this value.
    Answer(Value),
    /// Sends the start of an answer frame, then closes the connection.
    Truncate,
    /// Keeps the connection open and never answers.
    Hold,
}

/// Does `reply` on `stream`; the stream comes back when it is to be held open.
fn replied(stream: UnixStream, reply: Reply) -> Option<UnixStream> {
    let frame = match reply {
        Reply::Answer(value) => format!("{}\n", json!({"ok": true, "value": value})),
        Reply::Truncate => r#"{"ok":true,"value":"#.to_owned(),
        Reply::Hold => return Some(stream),
    };
    let _ = (&stream).write_all(frame.as_bytes());
    None
}

/// A stand-in endpoint that answers each connection in a thread of its own, as `reply` decides from its
/// request; a request it did not read whole is closed unanswered. Every request is recorded.
fn answering(state: &Path, reply: impl Fn(&Value) -> Reply + Send + Sync + 'static) -> Received {
    let listener = listener(state);
    let received = Received::default();
    let recorder = received.clone();
    let reply = Arc::new(reply);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let (recorder, reply) = (recorder.clone(), reply.clone());
            std::thread::spawn(move || {
                let request = request(&stream);
                recorder.lock().unwrap().push(request.clone());
                let Some(request) = request else { return };
                if let Some(_held) = replied(stream, reply(&request)) {
                    std::thread::sleep(Duration::from_secs(3600));
                }
            });
        }
    });
    received
}

/// An in-process proxy from the endpoint of `front` to the service of `back`. The daemon sees this process as
/// the peer, so claims keep their owner. It records each request, can lose the next reply to one operation,
/// and can sever every connection it carries.
#[derive(Clone, Default)]
struct Proxy {
    requests: Received,
    lose: Arc<Mutex<Option<String>>>,
    carried: Arc<Mutex<Vec<UnixStream>>>,
}
impl Proxy {
    fn start(front: &Path, back: &Path) -> Self {
        let proxy = Self::default();
        let listener = listener(front);
        let address = gpu_arbiter::compiler::endpoint(&std::fs::canonicalize(back).unwrap()).unwrap();
        let carrier = proxy.clone();
        std::thread::spawn(move || {
            for client in listener.incoming().flatten() {
                let carrier = carrier.clone();
                let address = address.clone();
                std::thread::spawn(move || carrier.carry(client, &address));
            }
        });
        proxy
    }
    fn carry(&self, client: UnixStream, address: &std::os::unix::net::SocketAddr) {
        self.carried.lock().unwrap().push(client.try_clone().unwrap());
        let mut reader = BufReader::new(client.try_clone().unwrap());
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        self.requests.lock().unwrap().push(serde_json::from_str(&line).ok());
        let Ok(service) = UnixStream::connect_addr(address) else { return };
        (&service).write_all(line.as_bytes()).unwrap();
        // A client that leaves is a departure the daemon sees too.
        let departing = service.try_clone().unwrap();
        std::thread::spawn(move || {
            let mut rest = Vec::new();
            let _ = std::io::Read::read_to_end(&mut reader, &mut rest);
            let _ = departing.shutdown(std::net::Shutdown::Both);
        });
        let mut reply = String::new();
        let answered = BufReader::new(&service).read_line(&mut reply).unwrap_or(0) > 0;
        let operation: Value = serde_json::from_str(&line).unwrap();
        let lost = {
            let mut lose = self.lose.lock().unwrap();
            let matches = lose.as_deref() == operation["op"].as_str();
            if matches {
                *lose = None;
            }
            matches
        };
        if answered && !lost {
            let _ = (&client).write_all(reply.as_bytes());
        }
        let _ = client.shutdown(std::net::Shutdown::Both);
    }
    fn lose_next(&self, operation: &str) {
        *self.lose.lock().unwrap() = Some(operation.into());
    }
    fn sever(&self) {
        for stream in self.carried.lock().unwrap().drain(..) {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }
    fn requests_for(&self, operation: &str) -> Vec<Value> {
        issued(&self.requests, operation)
    }
}

fn front_state(name: &str) -> PathBuf {
    let front = directory(name).join("front");
    std::fs::create_dir(&front).unwrap();
    front
}

#[test]
fn a_lost_reply_after_a_grant_is_reissued_whole_and_granted_once() {
    let service = Service::start("lost-grant");
    let front = front_state("lost-grant");
    let proxy = Proxy::start(&front, &service.state);
    proxy.lose_next("host_memory_wait");
    let mut held = claim(&front, 8 << 20, "reissued");
    let runtime = runtime();
    runtime.block_on(held.register()).unwrap();
    runtime
        .block_on(timed(Duration::from_secs(30), held.acquire(&CancelToken::new())))
        .expect("granted after the reissue")
        .unwrap();
    let issued = proxy.requests_for("host_memory_wait");
    assert_eq!(issued.len(), 2, "{issued:?}");
    assert_eq!(issued[0], issued[1], "the reissue carries the whole original declaration");
    let claims = service.health()["host_memory"]["claims"].clone();
    let [only] = claims.as_array().unwrap().as_slice() else { panic!("{claims}") };
    assert_eq!(only["claim_id"], held.id());
    assert_eq!(only["state"], "granted");
    assert_eq!(only["owner_pid"], std::process::id());
    assert_eq!(only["memory_bytes"], 8 << 20);
    assert_eq!(only["label"], "reissued");
    assert_eq!(only["priority"], 3);
    assert_eq!(only["purpose"], "host_only");
    runtime.block_on(held.release()).unwrap();
    assert_eq!(service.health()["host_memory"]["claims"], json!([]));
    std::fs::remove_dir_all(front.parent().unwrap()).unwrap();
}

#[test]
fn a_reply_cut_off_by_a_disconnect_is_a_lost_connection_after_which_both_waits_reissue_and_succeed() {
    let state = directory("cut-reply");
    let cut = Mutex::new(std::collections::BTreeSet::new());
    let received = answering(&state, move |request| {
        let operation = request["op"].as_str().unwrap_or_default();
        match operation {
            "group_wait" | "host_memory_wait" if cut.lock().unwrap().insert(operation.to_owned()) => Reply::Truncate,
            "group_wait" => Reply::Answer(json!({"group_id": "cut", "complete": true, "jobs": []})),
            "host_memory_wait" => Reply::Answer(json!({"state": "granted"})),
            _ => Reply::Answer(capable()),
        }
    });
    let client = Client::new(&state).unwrap();
    let runtime = runtime();
    let status = runtime
        .block_on(timed(Duration::from_secs(10), client.wait_group("cut", &CancelToken::new())))
        .expect("answered after the reissue");
    assert_eq!(status.unwrap()["complete"], true);
    let mut held = claim(&state, 8 << 20, "cut");
    runtime.block_on(held.register()).unwrap();
    runtime
        .block_on(timed(Duration::from_secs(10), held.acquire(&CancelToken::new())))
        .expect("granted after the reissue")
        .unwrap();
    runtime.block_on(held.release()).unwrap();
    for operation in ["group_wait", "host_memory_wait"] {
        let waits = issued(&received, operation);
        assert_eq!(waits.len(), 2, "{operation}: {waits:?}");
        assert_eq!(waits[0], waits[1], "{operation} is reissued whole");
    }
    std::fs::remove_dir_all(state).unwrap();
}

#[test]
fn a_pending_claim_that_reconnects_reissues_its_whole_declaration() {
    let service = Service::start("reconnect");
    let front = front_state("reconnect");
    let proxy = Proxy::start(&front, &service.state);
    let capacity = service.capacity();
    let waiting = std::thread::spawn(move || {
        let mut pending = claim(&front, capacity, "pending");
        let runtime = runtime();
        runtime.block_on(pending.register()).unwrap();
        let outcome = runtime.block_on(timed(Duration::from_secs(15), pending.acquire(&CancelToken::new())));
        runtime.block_on(pending.release()).unwrap();
        std::fs::remove_dir_all(front.parent().unwrap()).unwrap();
        outcome.map(|answer| answer.map_err(|failure| failure.to_string()))
    });
    eventually("the claim parks", || parked(&service).1 == 1);
    proxy.sever();
    eventually("the claim parks again", || proxy.requests_for("host_memory_wait").len() == 2 && parked(&service).1 == 1);
    let issued = proxy.requests_for("host_memory_wait");
    assert_eq!(issued[0], issued[1], "the reissue carries the whole original declaration");
    let claims = service.health()["host_memory"]["claims"].clone();
    let [only] = claims.as_array().unwrap().as_slice() else { panic!("{claims}") };
    assert_eq!(only["state"], "pending");
    assert_eq!(only["owner_pid"], std::process::id());
    assert_eq!(only["label"], "pending");
    assert!(waiting.join().unwrap().is_err(), "still pending, with no outage, when the gate stops waiting");
}

#[test]
fn a_hand_over_during_parked_waits_reissues_them_without_an_outage() {
    let mut service = Service::start("restart");
    let group = unadmitted(&service, "restarted");
    let state = service.state.clone();
    let client = Client::new(&state).unwrap();
    runtime().block_on(client.submit(&group, 0, Deadline::snapshot())).unwrap();
    let group_wait = std::thread::spawn(move || runtime().block_on(client.wait_group("restarted", &CancelToken::new())));
    let capacity = service.capacity();
    let claim_wait = std::thread::spawn(move || {
        let mut pending = claim(&state, capacity, "restarted");
        let runtime = runtime();
        runtime.block_on(pending.register()).unwrap();
        let outcome = runtime.block_on(timed(Duration::from_secs(25), pending.acquire(&CancelToken::new())));
        runtime.block_on(pending.release()).unwrap();
        outcome.map(|answer| answer.map_err(|failure| failure.to_string()))
    });
    eventually("both waits park", || parked(&service) == (1, 1));
    service.hand_over();
    eventually("both waits park again on the successor", || parked(&service) == (1, 1));
    let claims = service.health()["host_memory"]["claims"].clone();
    let [only] = claims.as_array().unwrap().as_slice() else { panic!("{claims}") };
    assert_eq!((only["state"].as_str(), only["label"].as_str()), (Some("pending"), Some("restarted")));
    cancelled(&service, "restarted");
    let status = group_wait.join().unwrap().expect("the group wait outlived the restart");
    assert_eq!(status["complete"], true);
    let claim_outcome = claim_wait.join().unwrap();
    assert!(claim_outcome.is_err(), "the claim was still pending, with no outage: {claim_outcome:?}");
}

#[test]
#[ignore = "outage gate 5: about 70 s"]
fn a_restart_longer_than_patience_stops_resumably() {
    let mut service = Service::start("long-restart");
    let group = unadmitted(&service, "resumed");
    let state = service.state.clone();
    let client = Client::new(&state).unwrap();
    runtime().block_on(client.submit(&group, 0, Deadline::snapshot())).unwrap();
    let waiter = client.clone();
    let group_wait = std::thread::spawn(move || {
        let answer = runtime().block_on(waiter.wait_group("resumed", &CancelToken::new()));
        (answer, Instant::now())
    });
    let capacity = service.capacity();
    let (granted, claim_ended) = std::sync::mpsc::channel();
    let (resumed, resume) = std::sync::mpsc::channel::<()>();
    let claimant = std::thread::spawn(move || {
        let mut pending = claim(&state, capacity, "resumed");
        let runtime = runtime();
        runtime.block_on(pending.register()).unwrap();
        granted.send((runtime.block_on(pending.acquire(&CancelToken::new())), Instant::now())).unwrap();
        resume.recv().unwrap();
        // Resumed: the same claim waits again with its whole declaration, and is pending, not refused.
        let again = runtime.block_on(timed(Duration::from_secs(5), pending.acquire(&CancelToken::new())));
        runtime.block_on(pending.release()).unwrap();
        again.is_err()
    });
    eventually("both waits park", || parked(&service) == (1, 1));
    service.crash();
    let stopped = Instant::now();
    let (group_answer, group_ended) = group_wait.join().unwrap();
    let (claim_answer, claim_ended) = claim_ended.recv().unwrap();
    for (answer, ended, operation) in [
        (&group_answer, group_ended, "group_wait"),
        (&claim_answer.map(|()| Value::Null), claim_ended, "host_memory_wait"),
    ] {
        let after = ended - stopped;
        eprintln!("{operation}: unavailable {after:?} after the service crashed");
        assert!(unavailable_during(answer, operation), "{answer:?}");
        assert!(after >= AUTHORITY_PATIENCE - Duration::from_millis(500), "{after:?}");
        assert!(after <= AUTHORITY_PATIENCE + PROBE_INTERVAL + TOLERANCE, "{after:?}");
    }
    service.resume();
    let reattached = std::thread::spawn(move || runtime().block_on(client.wait_group("resumed", &CancelToken::new())));
    eventually("the reattached wait parks", || parked(&service).0 == 1);
    cancelled(&service, "resumed");
    assert_eq!(reattached.join().unwrap().unwrap()["complete"], true);
    resumed.send(()).unwrap();
    assert!(claimant.join().unwrap(), "the resumed claim waited again");
}

#[test]
#[ignore = "outage gate 3: about 60 s"]
fn a_silent_endpoint_ends_a_parked_wait_within_patience_and_one_interval() {
    let state = directory("silent");
    silent(&state);
    let client = Client::new(&state).unwrap();
    let started = Instant::now();
    let answer = runtime().block_on(client.wait_group("unanswered", &CancelToken::new()));
    let elapsed = started.elapsed();
    eprintln!("silent endpoint: unavailable after {elapsed:?}");
    assert!(unavailable_during(&answer, "group_wait"), "{answer:?}");
    assert!(elapsed >= AUTHORITY_PATIENCE - Duration::from_millis(500), "{elapsed:?}");
    assert!(elapsed <= AUTHORITY_PATIENCE + PROBE_INTERVAL + TOLERANCE, "{elapsed:?}");
    assert!(elapsed < 2 * AUTHORITY_PATIENCE);
    std::fs::remove_dir_all(state).unwrap();
}

#[test]
#[ignore = "outage gate 4: about 200 s"]
fn healthy_parked_waits_outlast_three_patiences_without_an_outage() {
    const WAITS: usize = 8;
    let longer = 3 * AUTHORITY_PATIENCE + Duration::from_secs(15);
    let service = Service::start("long");
    let capacity = service.capacity();
    let client = Client::new(&service.state).unwrap();
    let groups: Vec<String> = (0..WAITS).map(|index| format!("long-{index}")).collect();
    for group in &groups {
        runtime().block_on(client.submit(&unadmitted(&service, group), 0, Deadline::snapshot())).unwrap();
    }
    // Each wait in a thread with its own runtime, as each Python call has.
    let group_waits: Vec<_> = groups
        .iter()
        .map(|group| {
            let (client, group) = (client.clone(), group.clone());
            std::thread::spawn(move || {
                let outcome = runtime().block_on(timed(longer, client.wait_group(&group, &CancelToken::new())));
                outcome.map(|answer| answer.map_err(|failure| failure.to_string()))
            })
        })
        .collect();
    // One claim per process: this process waits for one, and front-ends wait for the others.
    let state = service.state.clone();
    let claim_wait = std::thread::spawn(move || {
        let mut pending = claim(&state, capacity, "long");
        let runtime = runtime();
        runtime.block_on(pending.register()).unwrap();
        let outcome = runtime.block_on(timed(longer, pending.acquire(&CancelToken::new())));
        runtime.block_on(pending.release()).unwrap();
        outcome.map(|answer| answer.map(|()| Value::Null).map_err(|failure| failure.to_string()))
    });
    let mut front_ends: Vec<_> = (1..WAITS)
        .map(|index| {
            std::process::Command::new(env!("CARGO_BIN_EXE_host-claim"))
                .args(["--bytes", &capacity.to_string(), "--host-only", "--label", &format!("long {index}")])
                .args(["--state-dir", service.state.to_str().unwrap(), "--", "true"])
                .env_remove("GPU_QUEUE_PLACE")
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    let sampling = Instant::now();
    let mut latencies = Vec::new();
    let sampler = runtime();
    while sampling.elapsed() < longer {
        let started = Instant::now();
        sampler.block_on(client.health(Deadline::snapshot())).unwrap();
        latencies.push(started.elapsed());
        std::thread::sleep(Duration::from_secs(1));
    }
    for wait in group_waits {
        let outcome = wait.join().unwrap();
        assert!(outcome.is_err(), "group_wait ended before {longer:?}: {outcome:?}");
    }
    let outcome = claim_wait.join().unwrap();
    assert!(outcome.is_err(), "host_memory_wait ended before {longer:?}: {outcome:?}");
    for front_end in &mut front_ends {
        let exited = front_end.try_wait().unwrap();
        assert_eq!(exited, None, "a waiting front-end ended before {longer:?}");
        front_end.kill().unwrap();
        front_end.wait().unwrap();
    }
    latencies.sort();
    let at = |fraction: f64| latencies[((latencies.len() - 1) as f64 * fraction) as usize];
    eprintln!(
        "health latency beside {} parked waits, load {}: n {}, median {:?}, p95 {:?}, max {:?}",
        2 * WAITS,
        std::fs::read_to_string("/proc/loadavg").unwrap().trim(),
        latencies.len(),
        at(0.5),
        at(0.95),
        at(1.0)
    );
    for group in &groups {
        cancelled(&service, group);
    }
}

fn granted(service: &Service, label: &str) -> HostMemoryClaim {
    let mut held = claim(&service.state, 8 << 20, label);
    let runtime = runtime();
    runtime.block_on(held.register()).unwrap();
    runtime.block_on(held.acquire(&CancelToken::new())).unwrap();
    held
}

/// An endpoint whose listen queue is full: a connection is neither accepted nor queued. The returned
/// descriptors keep it so.
fn saturated(state: &Path) -> Vec<std::os::fd::OwnedFd> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::linux::net::SocketAddrExt;
    let address = gpu_arbiter::compiler::endpoint(&std::fs::canonicalize(state).unwrap()).unwrap();
    let name = address.as_abstract_name().unwrap().to_vec();
    let mut raw: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    raw.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (slot, byte) in raw.sun_path[1..].iter_mut().zip(&name) {
        *slot = *byte as libc::c_char;
    }
    let length = (std::mem::offset_of!(libc::sockaddr_un, sun_path) + 1 + name.len()) as libc::socklen_t;
    let socket = || unsafe {
        OwnedFd::from_raw_fd(libc::socket(libc::AF_UNIX, libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, 0))
    };
    let listener = socket();
    assert_eq!(unsafe { libc::bind(listener.as_raw_fd(), &raw as *const _ as *const libc::sockaddr, length) }, 0);
    assert_eq!(unsafe { libc::listen(listener.as_raw_fd(), 0) }, 0);
    let mut held = vec![listener];
    loop {
        let client = socket();
        let connected = unsafe { libc::connect(client.as_raw_fd(), &raw as *const _ as *const libc::sockaddr, length) };
        if connected != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EAGAIN) {
            return held;
        }
        held.push(client);
    }
}

fn within_cleanup(elapsed: Duration) {
    assert!(elapsed >= CLEANUP_PATIENCE - Duration::from_millis(500), "{elapsed:?}");
    assert!(elapsed <= CLEANUP_PATIENCE + TOLERANCE, "{elapsed:?}");
}

#[test]
fn a_dropped_claim_is_abandoned_through_the_shared_client() {
    let service = Service::start("abandoned");
    let held = granted(&service, "abandoned");
    runtime().block_on(async move { drop(held) });
    eventually("the abandoned claim is withdrawn", || service.health()["host_memory"]["claims"] == json!([]));
}

#[test]
fn a_failed_release_then_a_drop_in_a_live_runtime_end_within_one_cleanup_deadline() {
    let mut service = Service::start("release-outage");
    let mut held = granted(&service, "released");
    service.crash();
    let started = Instant::now();
    let released = runtime().block_on(async move {
        let released = held.release().await;
        // Inside the live runtime, as host-claim drops a claim: the abandon has nothing left to spend.
        drop(held);
        released
    });
    let elapsed = started.elapsed();
    eprintln!("failed release and drop: {elapsed:?}");
    assert!(
        matches!(&released, Err(Failure::Unavailable(outage)) if outage.operation == "host_memory_release"),
        "{released:?}"
    );
    within_cleanup(elapsed);
}

#[test]
fn a_claim_dropped_in_a_live_runtime_without_its_service_returns_within_the_cleanup_deadline() {
    let mut service = Service::start("drop-outage");
    let held = granted(&service, "dropped");
    service.crash();
    let started = Instant::now();
    runtime().block_on(async move { drop(held) });
    let elapsed = started.elapsed();
    eprintln!("drop without a service: {elapsed:?}");
    within_cleanup(elapsed);
}

#[test]
fn a_claim_dropped_beside_a_saturated_endpoint_returns_within_the_cleanup_deadline() {
    let mut service = Service::start("saturated");
    let held = granted(&service, "saturated");
    service.crash();
    let _queue = saturated(&service.state);
    let started = Instant::now();
    runtime().block_on(async move { drop(held) });
    let elapsed = started.elapsed();
    eprintln!("drop beside a saturated endpoint: {elapsed:?}");
    within_cleanup(elapsed);
}

fn cancelled_locally<T: std::fmt::Debug>(answer: &Result<T, Failure>) -> bool {
    matches!(answer, Err(Failure::Cancelled))
}

#[test]
fn a_token_cancelled_before_its_wait_starts_ends_it_at_once_and_parks_nothing() {
    let service = Service::start("latched");
    let client = Client::new(&service.state).unwrap();
    let runtime = runtime();
    runtime.block_on(client.submit(&unadmitted(&service, "latched"), 0, Deadline::snapshot())).unwrap();
    let token = CancelToken::new();
    token.cancel();
    let started = Instant::now();
    let answer = runtime.block_on(client.wait_group("latched", &token));
    assert!(cancelled_locally(&answer), "{answer:?}");
    let mut pending = claim(&service.state, service.capacity(), "latched");
    runtime.block_on(pending.register()).unwrap();
    let acquired = runtime.block_on(pending.acquire(&token));
    assert!(cancelled_locally(&acquired), "{acquired:?}");
    assert!(started.elapsed() < Duration::from_millis(500), "{:?}", started.elapsed());
    assert_eq!(parked(&service), (0, 0));
    runtime.block_on(pending.release()).unwrap();
    cancelled(&service, "latched");
}

#[test]
fn a_token_ends_a_wait_while_its_probe_is_pending() {
    let state = directory("cancel-probe");
    silent(&state);
    let client = Client::new(&state).unwrap();
    let token = CancelToken::new();
    let signal = token.clone();
    let waiting = std::thread::spawn(move || {
        let answer = runtime().block_on(client.wait_group("unanswered", &token));
        (answer, Instant::now())
    });
    std::thread::sleep(Duration::from_secs(1));
    let signalled = Instant::now();
    signal.cancel();
    let (answer, ended) = waiting.join().unwrap();
    assert!(cancelled_locally(&answer), "{answer:?}");
    assert!(ended - signalled < Duration::from_millis(500), "{:?}", ended - signalled);
    std::fs::remove_dir_all(state).unwrap();
}

#[test]
fn independent_tokens_end_only_their_own_waits_and_the_service_keeps_the_work() {
    let service = Service::start("independent");
    let client = Client::new(&service.state).unwrap();
    runtime().block_on(client.submit(&unadmitted(&service, "kept"), 0, Deadline::snapshot())).unwrap();
    let wait = |token: &CancelToken| {
        let (client, token) = (client.clone(), token.clone());
        std::thread::spawn(move || runtime().block_on(client.wait_group("kept", &token)))
    };
    let (first, second) = (CancelToken::new(), CancelToken::new());
    let (cancelled_wait, kept_wait) = (wait(&first), wait(&second));
    eventually("both waits park", || parked(&service).0 == 2);
    first.cancel();
    first.cancel();
    let answer = cancelled_wait.join().unwrap();
    assert!(cancelled_locally(&answer), "{answer:?}");
    eventually("one wait stays parked", || parked(&service).0 == 1);
    let status = runtime().block_on(client.status("kept", Deadline::snapshot())).unwrap();
    assert_eq!(status["complete"], false, "a local cancellation withdraws nothing");
    let reattached = wait(&CancelToken::new());
    eventually("the reattached wait parks", || parked(&service).0 == 2);
    cancelled(&service, "kept");
    for waited in [kept_wait, reattached] {
        assert_eq!(waited.join().unwrap().unwrap()["complete"], true);
    }
    // A RAM wait cancelled locally leaves its registered claim to its owner, who waits for it again.
    let capacity = service.capacity();
    let state = service.state.clone();
    let token = CancelToken::new();
    let signal = token.clone();
    let claimant = std::thread::spawn(move || {
        let mut pending = claim(&state, capacity, "kept");
        let runtime = runtime();
        runtime.block_on(pending.register()).unwrap();
        let first = runtime.block_on(pending.acquire(&token));
        let again = runtime.block_on(timed(Duration::from_secs(3), pending.acquire(&CancelToken::new())));
        runtime.block_on(pending.release()).unwrap();
        (first.map_err(|failure| failure.to_string()), again.is_err())
    });
    eventually("the claim parks", || parked(&service).1 == 1);
    signal.cancel();
    let (first, waited_again) = claimant.join().unwrap();
    assert_eq!(first, Err("the wait was cancelled locally".into()));
    assert!(waited_again, "the same claim waited again, still pending");
}

#[test]
fn a_token_racing_an_answer_gives_the_answer_or_a_local_cancellation() {
    let service = Service::start("race");
    let client = Client::new(&service.state).unwrap();
    let runtime = runtime();
    let finished: Group = serde_json::from_value(json!({"group_id": "finished", "jobs": []})).unwrap();
    runtime.block_on(client.submit(&finished, 0, Deadline::snapshot())).unwrap();
    for _ in 0..50 {
        let token = CancelToken::new();
        let signal = token.clone();
        let racer = std::thread::spawn(move || signal.cancel());
        let started = Instant::now();
        let answer = runtime.block_on(client.wait_group("finished", &token));
        racer.join().unwrap();
        assert!(cancelled_locally(&answer) || answer.as_ref().is_ok_and(|status| status["complete"] == true), "{answer:?}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }
}

/// An endpoint that does `replies` to its first connections in order, then refuses every later connection;
/// held connections stay open meanwhile.
fn scripted(state: &Path, replies: Vec<Reply>) {
    let listener = listener(state);
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for reply in replies {
            let Ok((stream, _)) = listener.accept() else { return };
            request(&stream);
            held.extend(replied(stream, reply));
        }
        drop(listener);
        std::thread::sleep(Duration::from_secs(3600));
    });
}

fn capable() -> Value {
    json!({"protocol": 1, "host_memory": {"protocol": 1}, "capabilities": ["transfer", "purpose"]})
}

/// `host-claim` for `bytes`, recording its outage beside its state.
fn front_end(state: &Path, bytes: u64) -> (std::process::Child, PathBuf) {
    let recorded = state.parent().unwrap().join(format!("{}.outage.json", state.file_name().unwrap().to_string_lossy()));
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_host-claim"))
        .args(["--bytes", &bytes.to_string(), "--host-only", "--state-dir", state.to_str().unwrap()])
        .args(["--outage-file", recorded.to_str().unwrap(), "--", "true"])
        .env_remove("GPU_QUEUE_PLACE")
        .env_remove("COMPILE_QUEUE_JOB")
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    (child, recorded)
}

/// The front-end exits 75 within the transport bound plus the cleanup deadline after `lost`, its outage
/// recorded with `operation` and the reason its stderr names.
fn exits_unavailable(front_end: std::process::Child, recorded: &Path, lost: Instant, operation: &str) {
    let output = front_end.wait_with_output().unwrap();
    let after = lost.elapsed();
    let errors = String::from_utf8_lossy(&output.stderr);
    eprintln!("host-claim lost the service during {operation}: exited after {after:?}");
    assert_eq!(output.status.code(), Some(75), "{errors}");
    assert!(after >= AUTHORITY_PATIENCE - Duration::from_millis(500), "{after:?}");
    assert!(after <= AUTHORITY_PATIENCE + PROBE_INTERVAL + CLEANUP_PATIENCE + TOLERANCE, "{after:?}");
    let outage = gpu_arbiter::outage::Outage::read(recorded).unwrap().expect("the outage was handed on");
    assert_eq!((outage.source.as_str(), outage.operation.as_str()), ("compile service", operation));
    assert!(errors.contains(&format!("host-claim: {}", outage.reason)), "{errors}");
}

#[test]
#[ignore = "outage gate 6: about 70 s"]
fn host_claim_losing_the_service_between_probe_and_registration_exits_75_with_its_outage() {
    let root = directory("lost-registration");
    let state = root.join("state");
    std::fs::create_dir(&state).unwrap();
    scripted(&state, vec![Reply::Answer(capable())]);
    let lost = Instant::now();
    let (child, recorded) = front_end(&state, 8 << 20);
    exits_unavailable(child, &recorded, lost, "host_memory_register");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
#[ignore = "outage gate 6: about 60 s"]
fn host_claim_whose_registration_reply_is_cut_off_exits_75_with_its_outage() {
    let root = directory("cut-registration");
    let state = root.join("state");
    std::fs::create_dir(&state).unwrap();
    let received = answering(&state, |request| match request["op"].as_str() {
        Some("host_memory_register") => Reply::Truncate,
        _ => Reply::Answer(capable()),
    });
    let lost = Instant::now();
    let (child, recorded) = front_end(&state, 8 << 20);
    exits_unavailable(child, &recorded, lost, "host_memory_register");
    let registrations = issued(&received, "host_memory_register").len();
    assert!(registrations > 1, "a cut-off registration is retried, never refused: {registrations}");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
#[ignore = "outage gate 6: about 70 s"]
fn host_claim_losing_the_service_during_its_acquisition_health_exits_75_with_its_outage() {
    let root = directory("lost-health");
    let state = root.join("state");
    std::fs::create_dir(&state).unwrap();
    scripted(&state, vec![Reply::Answer(capable()), Reply::Answer(json!({"state": "pending", "warnings": []})), Reply::Hold]);
    let lost = Instant::now();
    let (child, recorded) = front_end(&state, 8 << 20);
    exits_unavailable(child, &recorded, lost, "health");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
#[ignore = "outage gate 6: about 70 s"]
fn host_claim_losing_the_service_during_its_wait_exits_75_with_its_outage() {
    let mut service = Service::start("lost-wait");
    let (child, recorded) = front_end(&service.state, service.capacity());
    eventually("the front-end's claim parks", || parked(&service).1 == 1);
    service.crash();
    exits_unavailable(child, &recorded, Instant::now(), "host_memory_wait");
}
