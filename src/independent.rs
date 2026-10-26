//! Independent operation starts the existing daemon engine in a local helper.
//! A file lock serializes startup for one local authority; clients never select
//! this mode as a fallback from an unreachable configured daemon.
use serde_json::json;
use std::fs::OpenOptions;
use std::os::fd::AsRawFd;
use std::os::unix::{fs::PermissionsExt, process::CommandExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub fn socket(directory: &Path, executable: &Path, nvidia_smi: &Path) -> Result<PathBuf, String> {
    let directory = directory.to_path_buf();
    let system_socket = std::env::var_os("GPU_SWITCH_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/run/gpu-switch/host.sock"));
    if system_socket.exists() {
        return Err("a system GPU queue exists; use client mode to share its authority".into());
    }
    std::fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(&directory).map_err(|e| e.to_string())?;
    if !metadata.is_dir() || metadata.uid() != unsafe { libc::getuid() } {
        return Err("independent runtime directory must be owned by this user".into());
    }
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
        .map_err(|e| e.to_string())?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join("startup.lock"))
        .map_err(|e| e.to_string())?;
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    let socket = directory.join("queue.sock");
    if crate::client::get_status(&socket).is_ok() {
        return Ok(socket);
    }
    let config = directory.join("config.json");
    let content = json!({"listen": [{"name": "host", "path": socket}],
        "nvidia_smi": nvidia_smi, "state_dir": directory.join("state"),
        "startup_idle_secs": 1, "idle_secs": 90});
    std::fs::write(&config, serde_json::to_vec(&content).unwrap()).map_err(|e| e.to_string())?;
    let log = OpenOptions::new()
        .append(true)
        .create(true)
        .open(directory.join("daemon.log"))
        .map_err(|e| e.to_string())?;
    let mut command = Command::new(executable);
    command
        .args(["--config"])
        .arg(&config)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log);
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            let forked = libc::fork();
            if forked < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if forked > 0 {
                libc::_exit(0);
            }
            Ok(())
        });
    }
    let mut child = command.spawn().map_err(|e| e.to_string())?;
    let status = child.wait().map_err(|e| e.to_string())?;
    if !status.success() {
        return Err(format!("local queue launcher failed: {status}"));
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if crate::client::get_status(&socket).is_ok() {
            return Ok(socket);
        }
        if Instant::now() >= deadline {
            return Err("local GPU queue did not start within five seconds".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}
