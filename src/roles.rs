//! Test-only: this test binary re-executed into one `#[ignore]` role test, as a child process with an environment
//! of its own. A process-wide setting, such as the mock NVML's `$NINFER_MOCK_GPU_DIR`, then belongs to one role, and
//! no test serializes on it. A role test returns at once without the environment its parent gives it.

use std::ffi::OsStr;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};

/// A running role. Dropped unwaited, on failure too, it is killed by its pid and reaped.
pub struct Role {
    child: Option<Child>,
}

impl Role {
    /// The role test `name`, by its full path, with `env` set in its process only.
    pub fn start(name: &str, env: &[(&str, &OsStr)]) -> Role {
        Role::spawn(Role::command(name, env))
    }

    /// As `start`, with the role's stderr written to `log`.
    pub fn logged(name: &str, env: &[(&str, &OsStr)], log: &Path) -> Role {
        let mut command = Role::command(name, env);
        let file = std::fs::File::create(log).expect("the role's log");
        command.stdout(file.try_clone().expect("the role's log")).stderr(file);
        Role::spawn(command)
    }

    /// Any child process this owner kills and reaps when it drops unwaited.
    pub fn spawn(mut command: Command) -> Role {
        Role {
            child: Some(command.spawn().expect("starting a child")),
        }
    }

    fn command(name: &str, env: &[(&str, &OsStr)]) -> Command {
        let mut command = Command::new(std::env::current_exe().expect("the test binary"));
        command
            .args(["--ignored", "--exact", name, "--nocapture", "--test-threads=1"])
            .stdin(Stdio::null());
        for (key, value) in env {
            command.env(key, value);
        }
        command
    }

    /// Waits for the child to end and reaps it, keeping its owner.
    pub fn status(&mut self) -> ExitStatus {
        self.child.as_mut().expect("not waited").wait().expect("waiting for a child")
    }

    /// Whether the child still runs; an exited one is reaped.
    pub fn running(&mut self) -> bool {
        self.child
            .as_mut()
            .expect("not waited")
            .try_wait()
            .expect("try_wait")
            .is_none()
    }

    pub fn pid(&self) -> u32 {
        self.child.as_ref().expect("running").id()
    }

    /// Waits for the role to end and reaps it.
    pub fn wait(mut self) -> ExitStatus {
        self.child
            .take()
            .expect("running")
            .wait()
            .expect("waiting for a role")
    }
}

impl Drop for Role {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// The mock NVML library that `cargo test` builds beside the test binaries.
pub fn nvml_mock() -> std::path::PathBuf {
    let mock = std::env::current_exe()
        .expect("the test binary")
        .parent()
        .and_then(std::path::Path::parent)
        .expect("the target directory")
        .join("examples/libnvml_mock.so");
    assert!(
        mock.exists(),
        "cargo test builds the mock NVML beside the tests: {}",
        mock.display()
    );
    mock
}
