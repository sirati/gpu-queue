//! `gpu-switch`: the host daemon owning the joint queue (DESIGN.md).

pub mod backfill;
pub mod config;
pub mod control;
pub mod daemon;
pub mod departure;
pub mod enforcement;
pub mod fairness;
pub mod journal;
pub mod persist;
pub mod poller;
pub mod room;
pub mod runtime;
pub mod service;
pub mod startup;
pub mod state;
pub mod timing;
pub mod token;
pub mod view;

use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::Path;

/// `LISTEN_FDS`/`LISTEN_FDNAMES` from systemd, by name; cleared from the
/// environment so children never see them.
pub fn listen_fds() -> Vec<(String, OwnedFd)> {
    let mine = std::env::var("LISTEN_PID")
        .ok()
        .and_then(|p| p.parse::<u32>().ok())
        == Some(std::process::id());
    let count: i32 = std::env::var("LISTEN_FDS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    let names = std::env::var("LISTEN_FDNAMES").unwrap_or_default();
    for v in ["LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"] {
        std::env::remove_var(v);
    }
    if !mine {
        return Vec::new();
    }
    let names: Vec<&str> = names.split(':').collect();
    (0..count)
        .map(|i| {
            let fd = 3 + i;
            // SAFETY: systemd passes these descriptors to this process.
            unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
            let name = names
                .get(i as usize)
                .copied()
                .unwrap_or("unknown")
                .to_string();
            (name, unsafe { OwnedFd::from_raw_fd(fd) })
        })
        .collect()
}

/// Bind `path` (tests and manual runs), replacing a stale socket.
pub fn bind(path: &Path) -> Result<std::os::unix::net::UnixListener, String> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("creating {}: {e}", dir.display()))?;
    }
    if let Ok(meta) = std::fs::symlink_metadata(path) {
        if !meta.file_type().is_socket() {
            return Err(format!("{} exists and is not a socket", path.display()));
        }
        if std::os::unix::net::UnixStream::connect(path).is_ok() {
            return Err(format!("{} is in use", path.display()));
        }
        std::fs::remove_file(path).map_err(|e| format!("removing {}: {e}", path.display()))?;
    }
    let l = std::os::unix::net::UnixListener::bind(path)
        .map_err(|e| format!("binding {}: {e}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660))
        .map_err(|e| format!("chmod {}: {e}", path.display()))?;
    Ok(l)
}
