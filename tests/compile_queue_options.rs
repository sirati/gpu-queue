//! The compile-queue executable's options, checked against the built executable on a disposable state directory.
mod compile_service;

use compile_service::{directory, COMPILE_QUEUE};
use gpu_arbiter::compiler::client::Client;
use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Q-C2: an overcommit share above its maximum refuses to start before the state directory, the endpoint or any state
/// is touched.
#[test]
fn compile_queue_refuses_to_start_above_the_maximum() {
    let tmp = directory("overcommit-option");
    let state = tmp.join("s");
    let mut child = Command::new(COMPILE_QUEUE)
        .arg("--state-dir")
        .arg(&state)
        .args(["--overcommit-percent", "51"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        match child.try_wait().unwrap() {
            Some(status) => break Some(status),
            None if Instant::now() >= deadline => break None,
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    };
    let Some(status) = status else {
        let _ = child.kill();
        let _ = child.wait();
        let manifest = state.join("format.json");
        panic!("(a) the service still ran 10 s after its start; {} exists: {}", manifest.display(), manifest.exists());
    };
    assert_eq!(status.code(), Some(2), "(a)");
    let mut stderr = String::new();
    child.stderr.take().unwrap().read_to_string(&mut stderr).unwrap();
    assert!(stderr.contains("maximum of 50 percent"), "(b) {stderr}");
    assert!(!state.exists(), "(c) the state directory was created");
    assert!(Client::new(&state).is_err(), "(d) a client finds a service state");
    std::fs::remove_dir_all(tmp).unwrap();
}
