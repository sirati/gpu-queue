//! Apply the packaged user unit. systemd owns lifetime beyond any ML terminal.
use std::os::unix::fs::{symlink, MetadataExt};
use std::path::{Path, PathBuf};

fn publish(unit: &Path, runtime: &Path) -> Result<bool, String> {
    let directory = runtime.join("systemd/user");
    std::fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
    let destination = directory.join("doggy-compile.service");
    if std::fs::read_link(&destination).is_ok_and(|current| current == unit) {
        return Ok(false);
    }
    let temporary = directory.join(format!(
        ".doggy-compile-{}-{}-{}.tmp",
        std::process::id(),
        unsafe { libc::syscall(libc::SYS_gettid) },
        crate::protocol::now_unix().to_bits()
    ));
    symlink(unit, &temporary).map_err(|e| e.to_string())?;
    if let Err(error) = std::fs::rename(&temporary, &destination) {
        let _ = std::fs::remove_file(temporary);
        return Err(error.to_string());
    }
    Ok(true)
}

fn publish_authority(unit: &Path, runtime: &Path) -> Result<(), String> {
    let directory = runtime.join("systemd/user/doggy-compile.service.d");
    std::fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
    let image = executable(unit)?;
    let image = image
        .to_string_lossy()
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('%', "%%");
    let contents =
        format!("[Unit]\nRefuseManualStop=yes\n[Service]\nExecStart=\nExecStart=\"{image}\"\n");
    let destination = directory.join("authority.conf");
    if std::fs::read_to_string(&destination).is_ok_and(|current| current == contents) {
        return Ok(());
    }
    let temporary = directory.join(format!(
        ".authority-{}-{}.tmp",
        std::process::id(),
        crate::protocol::now_unix().to_bits()
    ));
    std::fs::write(&temporary, contents).map_err(|e| e.to_string())?;
    if let Err(error) = std::fs::rename(&temporary, &destination) {
        let _ = std::fs::remove_file(temporary);
        return Err(error.to_string());
    }
    Ok(())
}

fn systemctl(arguments: &[&str]) -> Result<(), String> {
    let executable = option_env!("COMPILER_SYSTEMCTL").unwrap_or("systemctl");
    let result = std::process::Command::new(executable)
        .arg("--user")
        .args(arguments)
        .output()
        .map_err(|e| e.to_string())?;
    if !result.status.success() {
        return Err(format!(
            "compiler user service: {}",
            String::from_utf8_lossy(&result.stderr).trim()
        ));
    }
    Ok(())
}

fn executable(unit: &Path) -> Result<PathBuf, String> {
    unit.ancestors()
        .nth(4)
        .map(|root| root.join("bin/compile-queue"))
        .ok_or_else(|| "invalid compiler unit path".into())
}

pub fn activate(unit: &Path) -> Result<(), String> {
    if std::env::var_os("COMPILE_QUEUE_JOB").is_some() {
        return Err("a compilation cannot activate or replace its own compiler service".into());
    }
    if !unit.is_file() {
        return Err(format!(
            "packaged compiler unit is missing: {}",
            unit.display()
        ));
    }
    let uid = unsafe { libc::getuid() };
    let runtime = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(format!("/run/user/{uid}")));
    if std::fs::metadata(&runtime)
        .map_err(|e| e.to_string())?
        .uid()
        != uid
    {
        return Err("compiler user runtime belongs to another user".into());
    }
    let previous = std::fs::read_link(runtime.join("systemd/user/doggy-compile.service")).ok();
    let changed = publish(unit, &runtime)?;
    systemctl(&["daemon-reload"])?;
    let identical = changed
        && previous.as_ref().is_some_and(|old| {
            executable(old)
                .and_then(|old| super::handoff::same_code(&old, &executable(unit)?))
                .unwrap_or(false)
        });
    if changed && !identical {
        let state = PathBuf::from(format!("/var/tmp/rust-compile-{uid}"));
        let control_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let current = control_runtime.block_on(async {
            let client = super::client::Client::new(&state, std::time::Duration::from_secs(2))?;
            client.health().await.map(|health| (client, health))
        });
        match current {
            Ok((client, health)) => {
                if !health["features"]
                    .as_array()
                    .is_some_and(|items| items.iter().any(|v| v == "separate-process-handoff"))
                {
                    if let Some(previous) = &previous {
                        publish(previous, &runtime)?;
                        systemctl(&["daemon-reload"])?;
                    }
                    return Err("running compiler lacks handover protocol; current service and work retained; no restart performed".into());
                }
                let executable = executable(unit)?;
                if let Err(error) = control_runtime.block_on(
                    client.request(serde_json::json!({"op":"upgrade","executable":executable})),
                ) {
                    if let Some(previous) = &previous {
                        publish(previous, &runtime)?;
                        systemctl(&["daemon-reload"])?;
                    }
                    return Err(error);
                }
            }
            Err(_) => systemctl(&["start", "--no-block", "doggy-compile.service"])?,
        }
    } else {
        systemctl(&["start", "--no-block", "doggy-compile.service"])?;
    }
    publish_authority(unit, &runtime)?;
    systemctl(&["daemon-reload"])?;
    // A project can immediately construct its native client after activation.
    std::fs::create_dir_all(format!("/var/tmp/rust-compile-{uid}")).map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stale_unit_publication_cannot_change_authoritative_executable() {
        let runtime = std::env::temp_dir().join(format!(
            "compiler-authority-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        let current = Path::new("/nix/store/current/share/systemd/user/doggy-compile.service");
        let stale = Path::new("/nix/store/stale/share/systemd/user/doggy-compile.service");
        publish(current, &runtime).unwrap();
        publish_authority(current, &runtime).unwrap();
        publish(stale, &runtime).unwrap();
        let contents = std::fs::read_to_string(
            runtime.join("systemd/user/doggy-compile.service.d/authority.conf"),
        )
        .unwrap();
        assert!(contents.contains("ExecStart=\"/nix/store/current/bin/compile-queue\""));
        assert!(contents.contains("RefuseManualStop=yes"));
        publish_authority(current, &runtime).unwrap();
        assert_eq!(
            std::fs::read_dir(runtime.join("systemd/user/doggy-compile.service.d"))
                .unwrap()
                .count(),
            1
        );
        std::fs::remove_dir_all(runtime).unwrap();
    }
    #[test]
    fn applying_an_unchanged_declaration_needs_no_restart() {
        let runtime = std::env::temp_dir().join(format!(
            "compiler-unit-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        std::fs::create_dir_all(&runtime).unwrap();
        let first = Path::new("/nix/store/first/share/systemd/user/doggy-compile.service");
        let second = Path::new("/nix/store/second/share/systemd/user/doggy-compile.service");
        assert!(publish(first, &runtime).unwrap());
        assert!(!publish(first, &runtime).unwrap());
        assert!(publish(second, &runtime).unwrap());
        assert_eq!(
            std::fs::read_link(runtime.join("systemd/user/doggy-compile.service")).unwrap(),
            second
        );
        assert_eq!(
            std::fs::read_dir(runtime.join("systemd/user"))
                .unwrap()
                .count(),
            1
        );
        std::fs::remove_dir_all(runtime).unwrap();
    }
}
