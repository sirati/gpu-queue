//! The place-aware acquisition and the `host-claim` front-end against a disposable compile service.
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use gpu_arbiter::compiler::client::Client;
use gpu_arbiter::compiler::memory_client::{HostMemoryClaim, PreparationWait};
use gpu_arbiter::compiler::model::Purpose;
use serde_json::{json, Value};

mod compile_service;
use compile_service::{directory, Service};

const HOST_CLAIM: &str = env!("CARGO_BIN_EXE_host-claim");

/// A fake GPU place: what a waiting claim reported to it.
#[derive(Default)]
struct Reported(Vec<(String, bool)>);
impl PreparationWait for Reported {
    fn preparation_wait(&mut self, reason: &str, parked: bool) -> Result<(), String> {
        self.0.push((reason.to_owned(), parked));
        Ok(())
    }
}

fn claim(service: &Service, bytes: u64) -> HostMemoryClaim {
    HostMemoryClaim::new(
        Client::new(&service.state).unwrap(),
        bytes,
        "place fixture".into(),
        0,
        Some(Purpose::GpuPlace),
    )
    .unwrap()
}

#[test]
fn a_waiting_claim_reports_to_its_place_and_parks_only_when_large() {
    let service = Service::start("report");
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(reports(&service));
}

async fn reports(service: &Service) {
    // The fake place holds every line it was sent; the acquisition reads none of the place's own.
    let mut small = claim(service, 2 << 30);
    small.register().await.unwrap();
    let mut place = Reported::default();
    small.acquire_reporting(&mut place).await.unwrap();
    assert_eq!(
        place.0,
        [(format!("waiting for host memory: {} bytes", 2u64 << 30), false), (String::new(), false)]
    );
    small.release().await.unwrap();
    let mut large = claim(service, 12 << 30);
    large.register().await.unwrap();
    let mut place = Reported::default();
    let _ = tokio::time::timeout(Duration::from_millis(500), large.acquire_reporting(&mut place)).await;
    assert_eq!(place.0[0], (format!("waiting for host memory: {} bytes", 12u64 << 30), true));
    assert!(place.0[1..].iter().all(|(reason, parked)| reason.is_empty() && !parked));
    large.release().await.unwrap();
}

fn host_claim(args: &[&str], env: &[(&str, &str)]) -> Command {
    let mut command = Command::new(HOST_CLAIM);
    command.args(args).env_remove("GPU_QUEUE_PLACE").env_remove("COMPILE_QUEUE_JOB");
    for (name, value) in env {
        command.env(name, value);
    }
    command
}

#[test]
fn usage_errors_exit_two() {
    let cases: [(&[&str], &[(&str, &str)]); 5] = [
        (&["--bytes", "1", "--", "true"], &[]),
        (&["--bytes", "1", "--gpu-place", "--host-only", "--", "true"], &[]),
        (&["--bytes", "1", "--host-only", "--"], &[]),
        (&["--bytes", "0", "--host-only", "--", "true"], &[]),
        (&["--bytes", "1", "--host-only", "--", "true"], &[("GPU_QUEUE_PLACE", "{}")]),
    ];
    for (args, env) in cases {
        let output = host_claim(args, env).output().unwrap();
        assert_eq!(output.status.code(), Some(2), "{args:?} {env:?}: {}", String::from_utf8_lossy(&output.stderr));
    }
    let placed = host_claim(&["--bytes", "1", "--host-only", "--", "true"], &[("GPU_QUEUE_PLACE", "{}")])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&placed.stderr).contains("GPU_QUEUE_PLACE is set"));
}

#[test]
fn a_waiting_front_end_exits_125_within_a_second_of_its_cancel_file() {
    // No service answers this state directory, so the front-end waits for one.
    let dir = directory("cancel");
    let cancel = dir.join("cancel-pending.json");
    let mut front_end = host_claim(
        &["--bytes", "1048576", "--host-only", "--state-dir", dir.to_str().unwrap(),
          "--cancel-file", cancel.to_str().unwrap(), "--", "true"],
        &[],
    )
    .stderr(Stdio::null())
    .spawn()
    .unwrap();
    std::thread::sleep(Duration::from_millis(500));
    assert!(front_end.try_wait().unwrap().is_none(), "it waits for the service");
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
    std::fs::remove_dir_all(dir).unwrap();
}

/// A compile service of an earlier generation: it answers health without the capabilities a front-end needs.
fn lacking_service(state: &Path) -> std::thread::JoinHandle<()> {
    let address = gpu_arbiter::compiler::endpoint(&std::fs::canonicalize(state).unwrap()).unwrap();
    let listener = std::os::unix::net::UnixListener::bind_addr(&address).unwrap();
    std::thread::spawn(move || {
        use std::io::{BufRead, Write};
        if let Ok((stream, _)) = listener.accept() {
            let mut line = String::new();
            std::io::BufReader::new(&stream).read_line(&mut line).unwrap();
            let answer = json!({"ok":true,"value":{"protocol":1,"host_memory":{"protocol":1}}});
            (&stream).write_all(format!("{answer}\n").as_bytes()).unwrap();
        }
    })
}

#[test]
fn a_service_without_the_transfer_and_purpose_capabilities_is_an_outage() {
    let dir = directory("lacking");
    let answered = lacking_service(&dir);
    let output = host_claim(
        &["--bytes", "1048576", "--gpu-place", "--state-dir", dir.to_str().unwrap(), "--", "true"],
        &[],
    )
    .output()
    .unwrap();
    answered.join().unwrap();
    assert_eq!(output.status.code(), Some(75), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(String::from_utf8_lossy(&output.stderr).contains("lacks the transfer capability"));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn the_granted_claim_is_handed_to_the_program_in_the_same_process() {
    let service = Service::start("exec");
    let output = host_claim(
        &["--bytes", "1048576", "--gpu-place", "--label", "handed", "--state-dir", service.state.to_str().unwrap(),
          "--", "/bin/sh", "-c", "echo \"$$ $HOST_MEMORY_CLAIM\"; read line; exit 0"],
        &[],
    )
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .spawn()
    .unwrap();
    let pid = output.id();
    let mut child = output;
    let mut line = String::new();
    std::io::BufRead::read_line(&mut std::io::BufReader::new(child.stdout.as_mut().unwrap()), &mut line).unwrap();
    let (shell, handover) = line.trim().split_once(' ').unwrap();
    assert_eq!(shell.parse::<u32>().unwrap(), pid, "the program keeps the front-end's pid");
    let handover: Value = serde_json::from_str(handover).unwrap();
    assert_eq!(handover["memory_bytes"], 1048576);
    assert_eq!(handover["state_dir"], json!(std::fs::canonicalize(&service.state).unwrap()));
    let claims = service.health()["host_memory"]["claims"].clone();
    let claim = claims.as_array().unwrap().iter().find(|c| c["claim_id"] == handover["claim_id"]).unwrap().clone();
    assert_eq!(claim["state"], "granted");
    assert_eq!(claim["owner_pid"], pid);
    assert_eq!(claim["purpose"], "gpu_place");
    assert_eq!(claim["label"], "handed");
    drop(child.stdin.take());
    assert!(child.wait().unwrap().success());
}
