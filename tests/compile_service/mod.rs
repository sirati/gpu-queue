//! A disposable compile service for integration tests: its own state directory and memory ceiling, never the user's
//! service. The test's directory keeps what its services wrote to standard error, their admission records among it.
#![allow(dead_code)]
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use gpu_arbiter::compiler::client::{Client, Deadline};
use gpu_arbiter::compiler::resources;
use gpu_arbiter::procs::Process;
use serde_json::Value;

#[path = "../../src/test_support/wait.rs"]
mod wait;
use wait::wait_until;

pub const COMPILE_QUEUE: &str = env!("CARGO_BIN_EXE_compile-queue");

/// The memory ceiling of the service's own scope. It lies above the large class (`LARGE_REQUEST`), so that a large
/// claim is admissible whatever ceiling the test itself runs under. It is only a ceiling: a claim allocates nothing.
const CEILING: u64 = 16 << 30;

/// The headroom the service keeps (`--minimum-ram`): none, so that a claim of its whole capacity is admissible.
const HEADROOM: u64 = 0;

/// The unit of the user's service manager that the test runs in. There is none where no such manager answers the
/// test, as in the Nix build sandbox, which has no user bus.
fn own_unit() -> Option<String> {
    let output = Command::new("systemctl").args(["--user", "whoami"]).stderr(Stdio::null()).output().ok()?;
    output.status.success().then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// How the service runs.
#[derive(Debug, PartialEq, Eq)]
pub enum Launch {
    /// In a transient scope of the user's service manager, in the test unit's slice, with the fixture's ceiling
    /// instead of the test's. The scope is bound to the test's unit, so whatever ends that unit ends the service too.
    Scoped(String),
    /// As the test's child, under the test's own ceilings, which admit the test's request.
    Child,
}

/// The prerequisite a launch lacks: containment in a scope of its own, or a capacity where the test runs that
/// admits the test's request after the service's headroom.
#[derive(Debug, PartialEq, Eq)]
pub struct Uncontained {
    pub request: u64,
    pub headroom: u64,
    /// The service's memory reading where the test runs: available and capacity bytes, as `resources::memory`.
    pub reading: Result<(u64, u64), String>,
}

impl std::fmt::Display for Uncontained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let capacity = match &self.reading {
            Ok((_, capacity)) => format!("{capacity} bytes"),
            Err(error) => format!("unreadable ({error})"),
        };
        write!(
            f,
            "prerequisite unavailable, containment or capacity: no user service manager gives the disposable compile \
             service a scope of its own, and the capacity where the test runs, {capacity}, does not admit the \
             test's request of {} bytes after the service's headroom of {} bytes",
            self.request, self.headroom
        )
    }
}

impl Launch {
    /// The launch for a test whose largest fixed claim is `request`. `unit` is the user service manager's unit the
    /// test runs in, and `reading` the service's memory reading where the test runs (`resources::memory`), whose
    /// capacity is `MemTotal` bounded by every cgroup ancestor's `memory.max`. A scope of its own is taken wherever
    /// there is a unit. Without one, the service runs as the test's child only where that capacity admits the
    /// request after `headroom`, as the service's registration does.
    pub fn select(
        unit: Option<String>,
        reading: Result<(u64, u64), String>,
        request: u64,
        headroom: u64,
    ) -> Result<Launch, Uncontained> {
        match (unit, reading) {
            (Some(unit), _) => Ok(Launch::Scoped(unit)),
            (None, Ok((_, capacity))) if request + headroom <= capacity => Ok(Launch::Child),
            (None, reading) => Err(Uncontained { request, headroom, reading }),
        }
    }

    /// The launch where the test runs. A child reads what the test reads: it shares the test's host and cgroup.
    fn here(request: u64) -> Launch {
        Launch::select(own_unit(), resources::memory(), request, HEADROOM).unwrap_or_else(|missing| panic!("{missing}"))
    }

    fn command(&self) -> Command {
        match self {
            Launch::Scoped(unit) => scoped(unit),
            Launch::Child => Command::new(COMPILE_QUEUE),
        }
    }
}

fn scoped(unit: &str) -> Command {
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
    launch: Launch,
    pub state: PathBuf,
}
impl Service {
    /// Starts the service for a test whose largest fixed claim is `request`, as `Launch::select` decides. Where the
    /// test has neither a scope of its own for the service nor a capacity that admits `request`, it fails naming
    /// that prerequisite.
    pub fn start(name: &str, request: u64) -> Self {
        let launch = Launch::here(request);
        let state = directory(name).join("state");
        std::fs::create_dir(&state).unwrap();
        let mut service = Self { child: None, successors: Vec::new(), launch, state };
        service.resume();
        service
    }
    /// Starts the daemon again on the same state, and returns once it answers health; a daemon that exits first fails
    /// the call. Its standard error, and a successor's it starts, is appended to the test directory's
    /// `compile-queue.stderr`.
    pub fn resume(&mut self) {
        let stderr = std::fs::OpenOptions::new().create(true).append(true).open(self.stderr()).unwrap();
        self.child = Some(
            self.launch
                .command()
                .args(["--state-dir", self.state.to_str().unwrap(), "--minimum-ram", &HEADROOM.to_string(), "--average-ram", "1048576"])
                .stdout(Stdio::null())
                .stderr(stderr)
                .spawn()
                .unwrap(),
        );
        let probe = Client::new(&self.state).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let child = self.child.as_mut().expect("started above");
        let mut exited = None;
        wait_until("the compile service's health", || {
            if runtime.block_on(probe.health(Deadline::after(Duration::from_millis(500)))).is_ok() {
                return true;
            }
            exited = child.try_wait().unwrap();
            exited.is_some()
        });
        if let Some(status) = exited {
            panic!("the compile service exited before it answered health: {status}");
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
        let mut successor = None;
        wait_until("the hand-over to the successor", || {
            let health = runtime.block_on(client.health(Deadline::snapshot())).unwrap();
            let pid = health["pid"].as_u64().unwrap();
            successor = (pid != previous).then(|| Process { pid: pid as u32, birth: health["start_ticks"].as_u64().unwrap() });
            successor.is_some()
        });
        self.successors.extend(successor);
    }
    pub fn health(&self) -> Value {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime
            .block_on(Client::new(&self.state).unwrap().health(Deadline::after(Duration::from_secs(5))))
            .unwrap()
    }
    /// The memory capacity the daemon admits against, once it sampled its resources.
    pub fn capacity(&self) -> u64 {
        let mut capacity = None;
        wait_until("the compile service's resource sample", || {
            capacity = self.health()["resources"]["memory_capacity_bytes"].as_u64();
            capacity.is_some()
        });
        capacity.expect("the wait ends once a capacity is read")
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
