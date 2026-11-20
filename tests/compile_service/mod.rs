//! A disposable compile service for integration tests: its own state directory and memory ceiling, never the user's
//! service. The test's directory keeps what its services wrote to standard error, their admission records among it.
#![allow(dead_code)]
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use gpu_arbiter::compiler::client::{Client, Deadline};
use gpu_arbiter::procs::Process;
use serde_json::Value;

const COMPILE_QUEUE: &str = env!("CARGO_BIN_EXE_compile-queue");

/// The memory ceiling of the service's own scope. It lies above the large class (`LARGE_REQUEST`), so that a large
/// claim is admissible whatever ceiling the test itself runs under. It is only a ceiling: a claim allocates nothing.
const CEILING: u64 = 16 << 30;

/// The unit of the user's service manager that the test runs in. There is none where no such manager knows the
/// test, as in the Nix build sandbox, which has no user bus and mounts no `/sys`, so the service reads no cgroup
/// ceiling there.
fn own_unit() -> Option<String> {
    let output = Command::new("systemctl").args(["--user", "whoami"]).stderr(Stdio::null()).output().ok()?;
    output.status.success().then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// The service's command. Under the user's service manager the service runs in a transient scope of its own, in
/// the test unit's slice, with the fixture's ceiling instead of the test's. The scope is bound to the test's unit,
/// so whatever ends that unit ends the service too.
fn command() -> Command {
    own_unit().map_or_else(|| Command::new(COMPILE_QUEUE), scoped)
}

fn scoped(unit: String) -> Command {
    let mut command = Command::new("systemd-run");
    command
        .args(["--user", "--scope", "--quiet", "--collect", "--slice-inherit"])
        .arg(format!("--property=MemoryMax={CEILING}"))
        .arg(format!("--property=BindsTo={unit}"))
        .args(["--", COMPILE_QUEUE]);
    command
}

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
    /// Successors a hand-over started, each as its health reported it; they are no children of this process.
    successors: Vec<Process>,
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
    /// Starts the daemon again on the same state, once it answers health. Its standard error, and a successor's it
    /// starts, is appended to the test directory's `compile-queue.stderr`.
    pub fn resume(&mut self) {
        let stderr = std::fs::OpenOptions::new().create(true).append(true).open(self.stderr()).unwrap();
        self.child = Some(
            command()
                .args(["--state-dir", self.state.to_str().unwrap(), "--minimum-ram", "0", "--average-ram", "1048576"])
                .stdout(Stdio::null())
                .stderr(stderr)
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
        self.successors.drain(..).for_each(|successor| {
            successor.signal(libc::SIGKILL);
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
        std::fs::create_dir_all(self.executables()).unwrap();
        let successor = self.executables().join(format!("compile-queue-successor-{}", self.successors.len()));
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
            let health = runtime.block_on(client.health(Deadline::snapshot())).unwrap();
            let pid = health["pid"].as_u64().unwrap();
            if pid != previous {
                self.successors.push(Process {
                    pid: pid as u32,
                    birth: health["start_ticks"].as_u64().unwrap(),
                });
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
    /// What the test's services wrote to standard error, kept after the test.
    fn stderr(&self) -> PathBuf {
        self.root().join("compile-queue.stderr")
    }
    /// Where hand-overs put their successors' executables.
    fn executables(&self) -> PathBuf {
        self.root().join("successors")
    }
}
impl Drop for Service {
    /// The test's directory stays with its services' standard error; their state and the successors' executables go.
    fn drop(&mut self) {
        self.crash();
        let _ = std::fs::remove_dir_all(&self.state);
        let _ = std::fs::remove_dir_all(self.executables());
    }
}
