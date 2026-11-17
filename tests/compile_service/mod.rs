//! A disposable compile service for integration tests: its own state directory, never the user's service.
#![allow(dead_code)]
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use gpu_arbiter::compiler::client::{Client, Deadline};
use serde_json::Value;

const COMPILE_QUEUE: &str = env!("CARGO_BIN_EXE_compile-queue");

pub fn directory(name: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let path = std::env::temp_dir().join(format!(
        "host-claim-{name}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

pub struct Service {
    child: Option<Child>,
    /// Successors a hand-over started; they are no children of this process.
    successors: Vec<u32>,
    pub state: PathBuf,
}
impl Service {
    pub fn start(name: &str) -> Self {
        let state = directory(name).join("state");
        std::fs::create_dir(&state).unwrap();
        let mut service = Self { child: None, successors: Vec::new(), state };
        service.resume();
        service
    }
    /// Starts the daemon again on the same state, once it answers health.
    pub fn resume(&mut self) {
        self.child = Some(
            Command::new(COMPILE_QUEUE)
                .args(["--state-dir", self.state.to_str().unwrap(), "--minimum-ram", "0", "--average-ram", "1048576"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let probe = Client::new(&self.state).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while runtime.block_on(probe.health(Deadline::after(Duration::from_millis(500)))).is_err() {
            assert!(Instant::now() < deadline, "the compile service did not start");
        }
    }
    /// Ends the daemon as a crash does (SIGKILL): its durable state stays for the next start. A graceful stop
    /// would wait for every claim to end first.
    pub fn crash(&mut self) {
        self.successors.drain(..).for_each(|pid| unsafe {
            libc::kill(pid as i32, libc::SIGKILL);
        });
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
    /// Hands the service over to a successor executable of the same build, as an upgrade does, once the
    /// successor answers health.
    pub fn hand_over(&mut self) {
        let previous = self.health()["pid"].as_u64().unwrap();
        let successor = self.root().join(format!("compile-queue-successor-{}", self.successors.len()));
        let mut image = std::fs::read(COMPILE_QUEUE).unwrap();
        // Different bytes, the same program: the service hands over only to code that differs.
        image.push(0);
        std::fs::write(&successor, image).unwrap();
        std::fs::set_permissions(&successor, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
        let client = Client::new(&self.state).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime
            .block_on(client.request(serde_json::json!({"op":"upgrade","executable":successor}), Deadline::snapshot()))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let pid = runtime.block_on(client.health(Deadline::snapshot())).unwrap()["pid"].as_u64().unwrap();
            if pid != previous {
                self.successors.push(pid as u32);
                return;
            }
            assert!(Instant::now() < deadline, "the service did not hand over");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    pub fn health(&self) -> Value {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime
            .block_on(Client::new(&self.state).unwrap().health(Deadline::after(Duration::from_secs(5))))
            .unwrap()
    }
    /// The memory capacity the daemon admits against, once it sampled its resources.
    pub fn capacity(&self) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(capacity) = self.health()["resources"]["memory_capacity_bytes"].as_u64() {
                return capacity;
            }
            assert!(Instant::now() < deadline, "the compile service sampled no resources");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    pub fn root(&self) -> &Path {
        self.state.parent().unwrap()
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.crash();
        let _ = std::fs::remove_dir_all(self.state.parent().unwrap());
    }
}
