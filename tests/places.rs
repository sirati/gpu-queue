//! Places over gpu-switch's lease socket, across restarts of gpu-switch: what a place hears, and what a
//! reattaching client is told of its preparation.
mod common;

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use common::{Harness, Opts, Who};
use serde_json::{json, Value};

const T: Duration = Duration::from_secs(10);

/// A lease session spoken line by line, as a client of the protocol would.
struct Session {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
}

impl Session {
    fn open(h: &Harness, request: &Value) -> (Session, Value) {
        let writer = UnixStream::connect(h.sock(Who::Host)).unwrap();
        writer.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
        let mut session = Session {
            reader: BufReader::new(writer.try_clone().unwrap()),
            writer,
        };
        let mut request = request.clone();
        request["op"] = json!("lease");
        session.send(&request);
        let reply = session.next(T).expect("lease reply");
        (session, reply)
    }
    fn send(&mut self, line: &Value) {
        let mut bytes = serde_json::to_vec(line).unwrap();
        bytes.push(b'\n');
        self.writer.write_all(&bytes).unwrap();
    }
    fn next(&mut self, within: Duration) -> Option<Value> {
        let deadline = Instant::now() + within;
        let mut line = String::new();
        while Instant::now() < deadline {
            match self.reader.read_line(&mut line) {
                Ok(0) => return None,
                Ok(_) => return Some(serde_json::from_str(&line).unwrap()),
                Err(_) => continue,
            }
        }
        None
    }
    /// The first line `wanted` accepts, within the timeout.
    fn until(&mut self, what: &str, wanted: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + T;
        while Instant::now() < deadline {
            if let Some(line) = self.next(deadline - Instant::now()).filter(|line| wanted(line)) {
                return line;
            }
        }
        panic!("no {what} line");
    }
    /// Lines that arrive within `within`.
    fn quiet(&mut self, within: Duration) -> Vec<Value> {
        let deadline = Instant::now() + within;
        let mut heard = Vec::new();
        while let Some(line) = self.next(deadline.saturating_duration_since(Instant::now())) {
            heard.push(line);
        }
        heard
    }
}

fn place() -> Value {
    json!({"mode": "shared", "vram": 1 << 30, "expected_runtime_secs": 60, "prepares": true,
           "share": "kernel-places", "label": "place"})
}

fn ready() -> Value {
    json!({"op": "ready", "vram": 1 << 30, "expected_runtime_secs": 60, "max_runtime_secs": null, "shutdown_grace_secs": 5})
}

fn resumed(reply: &Value) -> Value {
    let mut request = place();
    request["resume"] = reply["job"]["resume"].clone();
    request
}

fn restart(h: &mut Harness) {
    h.stop_switch();
    h.start_switch();
}

#[test]
fn a_place_hears_prepare_once_told_and_nothing_else_before_its_ready() {
    let h = Harness::start("places-told", Opts { arbiter: false, ..Opts::default() });
    let (mut session, reply) = Session::open(&h, &place());
    assert_eq!(reply["ok"], true);
    assert_eq!(reply["preparation_protocol"], 1);
    assert_eq!(reply["preparation"], json!({"told": false, "ready": false, "parked": false}));
    session.until("prepare", |line| line["prepare"] == true);
    let job = h.wait_status(Who::Host, "the place shows its preparation", |s| {
        s["jobs"][0]["preparation"]["told_at"].is_number()
    })["jobs"][0]
        .clone();
    assert_eq!(job["share"], "kernel-places");
    assert_eq!(job["reason"], "preparing");
    session.send(&json!({"op": "preparation_wait", "reason": "waiting for host memory: 2147483648 bytes", "parked": false}));
    let heard = session.quiet(Duration::from_millis(500));
    assert!(heard.iter().all(|line| line.get("waiting").is_none()), "a preparing place hears no waiting reasons: {heard:?}");
    h.wait_status(Who::Host, "its wait is shown", |s| {
        s["jobs"][0]["reason"] == "preparing: waiting: waiting for host memory: 2147483648 bytes"
    });
    session.send(&json!({"op": "preparation_wait", "reason": "", "parked": false}));
    session.send(&ready());
    session.until("granted", |line| line["granted"] == true);
}

#[test]
fn requests_for_a_told_place_are_refused_on_an_entry_ready_on_arrival() {
    let h = Harness::start("places-refused", Opts { arbiter: false, ..Opts::default() });
    let mut request = place();
    request["prepares"] = json!(false);
    let (mut session, reply) = Session::open(&h, &request);
    assert!(reply.get("preparation").is_none());
    session.until("granted", |line| line["granted"] == true);
    session.send(&ready());
    let refused = session.until("refusal", |line| line["ok"] == false);
    assert!(refused["error"].as_str().unwrap().contains("told place"), "{refused}");
}

#[test]
fn a_reattaching_place_hears_prepare_again_until_it_is_ready() {
    let mut h = Harness::start("places-replay", Opts { arbiter: false, ..Opts::default() });
    let (mut session, reply) = Session::open(&h, &place());
    session.until("prepare", |line| line["prepare"] == true);
    // gpu-switch restarts before the client's Ready reached it; the client's connection stays open.
    restart(&mut h);
    drop(session);
    let (mut session, again) = Session::open(&h, &resumed(&reply));
    assert_eq!(again["ok"], true, "{again}");
    assert_eq!(again["preparation"], json!({"told": true, "ready": false, "parked": false}));
    assert!(again.get("reregistered").is_none());
    session.until("prepare replayed", |line| line["prepare"] == true);
    session.send(&ready());
    session.send(&ready());
    session.until("granted", |line| line["granted"] == true);
}

#[test]
fn a_place_that_sent_ready_or_waits_on_a_large_claim_is_told_so_after_a_restart() {
    let mut h = Harness::start("places-ready", Opts { arbiter: false, ..Opts::default() });
    // An exclusive lease holds the GPU, so the prepared place stays in line.
    let (mut holder, _) = Session::open(&h, &json!({"mode": "exclusive", "expected_runtime_secs": 60}));
    holder.until("granted", |line| line["granted"] == true);
    let (mut session, reply) = Session::open(&h, &place());
    session.until("prepare", |line| line["prepare"] == true);
    session.send(&ready());
    h.wait_status(Who::Host, "the place is ready", |s| {
        s["jobs"].as_array().unwrap().iter().any(|j| j["preparation"]["ready_at"].is_number())
    });
    let (mut waiting, waiting_reply) = Session::open(&h, &place());
    waiting.until("prepare", |line| line["prepare"] == true);
    waiting.send(&json!({"op": "preparation_wait", "reason": "waiting for host memory: 12884901888 bytes", "parked": true}));
    h.wait_status(Who::Host, "the second place is parked", |s| {
        s["jobs"].as_array().unwrap().iter().any(|j| j["preparation"]["parked"] == true)
    });
    // The holder's grant is kept, detached, through the restart, so the GPU stays held.
    restart(&mut h);
    drop((session, waiting));
    let (mut session, again) = Session::open(&h, &resumed(&reply));
    assert_eq!(again["preparation"], json!({"told": true, "ready": true, "parked": false}));
    let (mut waiting, waiting_again) = Session::open(&h, &resumed(&waiting_reply));
    assert_eq!(waiting_again["preparation"], json!({"told": true, "ready": false, "parked": true}));
    waiting.until("prepare replayed", |line| line["prepare"] == true);
    waiting.send(&json!({"op": "preparation_wait", "reason": "", "parked": false}));
    waiting.send(&ready());
    let heard = session.quiet(Duration::from_millis(300));
    assert!(heard.iter().all(|line| line.get("prepare").is_none()), "a ready place hears no Prepare again");
}

fn gpu_run_place(h: &Harness, args: &[&str]) -> std::process::Command {
    let mut command = h.gpu_run(Who::Host);
    command.arg("place").args(args).env_remove("GPU_QUEUE_PLACE");
    command
}

#[test]
fn gpu_run_place_executes_its_program_in_its_own_process_once_told() {
    let h = Harness::start("places-front-end", Opts { arbiter: false, ..Opts::default() });
    let output = gpu_run_place(&h, &["--shared", "--vram", "1G", "--expected-secs", "60", "--share", "kernel-x",
        "--label", "front-end", "--", "/bin/sh", "-c", "echo \"$$ $GPU_QUEUE_PLACE\""])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let text = String::from_utf8_lossy(&output.stdout).into_owned();
    let (_, handover) = text.trim().split_once(' ').unwrap();
    let handover: Value = serde_json::from_str(handover).unwrap();
    assert!(handover["fd"].as_i64().unwrap() > 2);
    assert_eq!(handover["request"]["prepares"], true);
    assert_eq!(handover["request"]["share"], "kernel-x");
    assert_eq!(handover["request"]["label"], "front-end");
    // The program held the place and left: the place ends with it.
    h.wait_status(Who::Host, "the place ended with its program", |s| {
        s["jobs"].as_array().unwrap().iter().all(|j| j["state"] != "queued")
    });
}

#[test]
fn gpu_run_place_exits_125_on_its_cancel_file_and_2_on_a_usage_error() {
    let h = Harness::start("places-cancel", Opts { arbiter: false, ..Opts::default() });
    let (mut holder, _) = Session::open(&h, &json!({"mode": "exclusive", "expected_runtime_secs": 60}));
    holder.until("granted", |line| line["granted"] == true);
    // Four counted entries of the user's pool stand ahead, so the pool's place stays untold.
    let _waiting: Vec<_> = (0..4)
        .map(|_| Session::open(&h, &json!({"mode": "exclusive", "expected_runtime_secs": 60})))
        .collect();
    let cancel = h.dir.join("cancel-pending.json");
    let mut front_end = gpu_run_place(&h, &["--exclusive", "--expected-secs", "60", "--cancel-file",
        cancel.to_str().unwrap(), "--", "true"])
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    h.wait_status(Who::Host, "the place waits untold", |s| {
        s["jobs"].as_array().unwrap().iter().any(|j| {
            j["reason"].as_str().is_some_and(|r| r.starts_with("placed: position"))
        })
    });
    std::fs::write(&cancel, b"{}").unwrap();
    let written = Instant::now();
    let status = loop {
        if let Some(status) = front_end.try_wait().unwrap() {
            break status;
        }
        assert!(written.elapsed() < Duration::from_secs(1), "the front-end outlived its cancel file");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(status.code(), Some(125));
    let usage = gpu_run_place(&h, &["--exclusive", "--", ]).output().unwrap();
    assert_eq!(usage.status.code(), Some(2));
    let unknown = gpu_run_place(&h, &["--exclusive", "--bogus", "--", "true"]).output().unwrap();
    assert_eq!(unknown.status.code(), Some(2));
}

#[test]
fn gpu_run_place_exits_75_without_a_gpu_switch_or_one_without_places() {
    let dir = std::env::temp_dir().join(format!("places-outage-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let place = |socket: &std::path::Path| {
        std::process::Command::new(common::GPU_RUN)
            .args(["--socket", socket.to_str().unwrap(), "place", "--exclusive", "--expected-secs", "60", "--", "true"])
            .env_remove("GPU_QUEUE_PLACE")
            .output()
            .unwrap()
    };
    assert_eq!(place(&dir.join("absent.sock")).status.code(), Some(75));
    // A gpu-switch of an earlier build: it answers the lease without a preparation protocol.
    let socket = dir.join("old.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    let old = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut line = String::new();
        BufReader::new(&stream).read_line(&mut line).unwrap();
        let reply = json!({"ok": true, "lease_protocol": 2, "job": {"id": 1, "handle": "s1",
            "resume": {"id": 1, "token": "ab", "key": {"class": 1, "ns": 1}, "running": false}}});
        (&stream).write_all(format!("{reply}\n").as_bytes()).unwrap();
        line
    });
    let output = place(&socket);
    assert_eq!(output.status.code(), Some(75), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(old.join().unwrap().contains("\"prepares\":true"));
    std::fs::remove_dir_all(dir).unwrap();
}
