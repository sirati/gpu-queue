//! The real gpu-switch on the mock NVML while the card is lost (PLAN-L T-13). The switch's state lives on tmpfs,
//! removed when the test ends. Every wait is an event wait: the run's scope is the hang guard.

mod common;

use common::*;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Duration;

/// The switch's state directory on tmpfs, removed when its owner drops, also when the test fails.
struct Tmpfs(PathBuf);

impl Tmpfs {
    fn new(name: &str) -> Tmpfs {
        let dir = PathBuf::from(format!("/dev/shm/gpu-card-loss-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        Tmpfs(dir)
    }
}

impl Drop for Tmpfs {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A raw protocol `status` request on the host socket.
fn status(h: &Harness) -> Value {
    let stream = UnixStream::connect(&h.host_sock).expect("gpu-switch answers");
    (&stream).write_all(b"{\"op\":\"status\"}\n").unwrap();
    let mut line = String::new();
    BufReader::new(&stream).read_line(&mut line).unwrap();
    let reply: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(reply["ok"], true, "{reply}");
    reply["status"].clone()
}

/// Until the status satisfies `wanted`.
fn until(h: &Harness, wanted: impl Fn(&Value) -> bool) -> Value {
    loop {
        let status = status(h);
        if wanted(&status) {
            return status;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn job(status: &Value, id: u64) -> Value {
    status["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|job| job["id"] == id)
        .cloned()
        .unwrap_or(Value::Null)
}

fn lost_while(control: &str) {
    let state = Tmpfs::new(control);
    let mut h = Harness::start(
        &format!("card-loss-{control}"),
        Opts {
            arbiter: false,
            switch: json!({"state_dir": state.0, "gpu_reopen_interval_secs": 0.2}),
            ..Opts::default()
        },
    );
    // A same-boot restart: the control file is in place before the switch reads the card.
    h.stop_switch();
    std::fs::write(h.gpu_dir.join(control), "").unwrap();
    h.start_switch();
    let lost = until(&h, |status| status["device_lost"].is_object());
    let summary = lost["summary"].as_str().unwrap();
    assert!(summary.starts_with("GPU LOST, found at "), "{summary}");
    let id = h.submit(Who::Host, EXCL, &["true"]);
    let waiting = until(&h, |status| {
        job(status, id)["reason"].as_str().is_some_and(|reason| reason.starts_with("the GPU was found lost at "))
    });
    assert_eq!(job(&waiting, id)["state"], "queued", "{waiting}");
    std::fs::remove_file(h.gpu_dir.join(control)).unwrap();
    until(&h, |status| job(status, id)["state"] == "done");
    assert!(status(&h)["device_lost"].is_null(), "read again");
}

#[test]
fn a_lost_card_keeps_the_switch_running_names_the_loss_and_holds_the_line_until_it_is_read_again() {
    lost_while("lost");
}

#[test]
fn an_absent_card_is_a_lost_card() {
    lost_while("absent");
}

#[test]
fn a_missing_nvml_library_is_a_lost_card_and_the_switch_keeps_answering() {
    let state = Tmpfs::new("missing");
    let h = Harness::start(
        "card-loss-missing",
        Opts {
            arbiter: false,
            switch: json!({"state_dir": state.0, "nvml": "/nonexistent/libnvidia-ml.so.1"}),
            ..Opts::default()
        },
    );
    let lost = until(&h, |status| status["device_lost"].is_object());
    let cause = lost["device_lost"]["cause"].as_str().unwrap();
    assert!(cause.starts_with("nvmlInit_v2"), "{cause}");
    assert!(status(&h)["device_lost"].is_object(), "the switch keeps answering");
}
