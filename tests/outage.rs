//! Outage gates of the compile-service transport (PLAN-I §8), on disposable services and fake endpoints only.
//! The long gates are ignored by default: `cargo test --test outage -- --ignored --nocapture`.
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use gpu_arbiter::compiler::client::{Client, Deadline, Failure, AUTHORITY_PATIENCE, PROBE_INTERVAL};
use gpu_arbiter::compiler::memory_client::HostMemoryClaim;
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

/// An in-process proxy from the endpoint of `front` to the service of `back`. The daemon sees this process as
/// the peer, so claims keep their owner. It records each request line, can lose the next reply to one
/// operation, and can sever every connection it carries.
#[derive(Clone, Default)]
struct Proxy {
    requests: Arc<Mutex<Vec<String>>>,
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
        self.requests.lock().unwrap().push(line.clone());
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
    fn requests_for(&self, operation: &str) -> Vec<String> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|line| serde_json::from_str::<Value>(line).unwrap()["op"] == operation)
            .cloned()
            .collect()
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
        .block_on(timed(Duration::from_secs(30), held.acquire()))
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
fn a_pending_claim_that_reconnects_reissues_its_whole_declaration() {
    let service = Service::start("reconnect");
    let front = front_state("reconnect");
    let proxy = Proxy::start(&front, &service.state);
    let capacity = service.capacity();
    let waiting = std::thread::spawn(move || {
        let mut pending = claim(&front, capacity, "pending");
        let runtime = runtime();
        runtime.block_on(pending.register()).unwrap();
        let outcome = runtime.block_on(timed(Duration::from_secs(15), pending.acquire()));
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
    let group_wait = std::thread::spawn(move || runtime().block_on(client.wait_group("restarted")));
    let capacity = service.capacity();
    let claim_wait = std::thread::spawn(move || {
        let mut pending = claim(&state, capacity, "restarted");
        let runtime = runtime();
        runtime.block_on(pending.register()).unwrap();
        let outcome = runtime.block_on(timed(Duration::from_secs(25), pending.acquire()));
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
        let answer = runtime().block_on(waiter.wait_group("resumed"));
        (answer, Instant::now())
    });
    let capacity = service.capacity();
    let (granted, claim_ended) = std::sync::mpsc::channel();
    let (resumed, resume) = std::sync::mpsc::channel::<()>();
    let claimant = std::thread::spawn(move || {
        let mut pending = claim(&state, capacity, "resumed");
        let runtime = runtime();
        runtime.block_on(pending.register()).unwrap();
        granted.send((runtime.block_on(pending.acquire()), Instant::now())).unwrap();
        resume.recv().unwrap();
        // Resumed: the same claim waits again with its whole declaration, and is pending, not refused.
        let again = runtime.block_on(timed(Duration::from_secs(5), pending.acquire()));
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
    let reattached = std::thread::spawn(move || runtime().block_on(client.wait_group("resumed")));
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
    let answer = runtime().block_on(client.wait_group("unanswered"));
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
                let outcome = runtime().block_on(timed(longer, client.wait_group(&group)));
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
        let outcome = runtime.block_on(timed(longer, pending.acquire()));
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
