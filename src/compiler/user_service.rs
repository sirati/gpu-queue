//! Apply the packaged user unit. systemd owns lifetime beyond any ML terminal.
use super::client::{Client, Deadline};
use serde_json::json;
use std::os::unix::fs::{symlink, MetadataExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

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

/// A drop-in is rewritten only when its contents change.
fn publish_dropin(runtime: &Path, name: &str, contents: &str) -> Result<(), String> {
    let directory = runtime.join("systemd/user/doggy-compile.service.d");
    std::fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
    let destination = directory.join(name);
    if std::fs::read_to_string(&destination).is_ok_and(|current| current == contents) {
        return Ok(());
    }
    let temporary = directory.join(format!(
        ".{name}-{}-{}.tmp",
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

fn publish_authority(unit: &Path, runtime: &Path) -> Result<(), String> {
    let image = executable(unit)?;
    let image = image
        .to_string_lossy()
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('%', "%%");
    publish_dropin(
        runtime,
        "zz-native-authority.conf",
        &format!("[Unit]\nRefuseManualStop=yes\n[Service]\nExecStart=\nExecStart=\"{image}\"\n"),
    )
}

/// Delegation lives outside the unit file: a shell of an older build briefly
/// publishes its own unit, and reloading an undelegated service clears the
/// controllers of its memory cells.
fn publish_delegation(runtime: &Path) -> Result<(), String> {
    publish_dropin(
        runtime,
        "memory-cells.conf",
        &format!("[Service]\nDelegate=memory\nDelegateSubgroup={}\n", super::containment::DAEMON),
    )
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
    publish_delegation(&runtime)?;
    publish(unit, &runtime)?;
    systemctl(&["daemon-reload"])?;
    systemctl(&["start", "--no-block", "doggy-compile.service"])?;
    let state = super::default_state();
    std::fs::create_dir_all(&state).map_err(|e| e.to_string())?;
    let desired = executable(unit)?;
    let activation = handed_over(&state, &desired, Deadline::after(ACTIVATION_BUDGET));
    if let Err(error) = activation {
        if let Some(previous) = &previous {
            publish(previous, &runtime)?;
            systemctl(&["daemon-reload"])?;
        }
        return Err(error);
    }
    publish_authority(unit, &runtime)?;
    systemctl(&["daemon-reload"])?;
    // A project can immediately construct its native client after activation.
    std::fs::create_dir_all(super::default_state()).map_err(|e| e.to_string())?;
    Ok(())
}

/// How long activation's whole hand-over may take. Its upgrade and every verification health exchange share
/// this one budget, never restarted per exchange.
const ACTIVATION_BUDGET: Duration = Duration::from_secs(120);

/// The service of `state` was asked to hand over to `desired` and now runs it, by `deadline`.
fn handed_over(state: &Path, desired: &Path, deadline: Deadline) -> Result<(), String> {
    let control_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    let client = Client::new(state).map_err(|e| e.to_string())?;
    control_runtime.block_on(async {
        tokio::time::timeout_at(deadline.at().into(), async {
            client
                .request(json!({"op":"upgrade","executable":desired}), deadline)
                .await
                .map_err(|e| e.to_string())?;
            loop {
                let health = client.health(deadline).await.map_err(|e| e.to_string())?;
                let pid = health["pid"]
                    .as_u64()
                    .ok_or("compiler health lacks its process identity")?;
                let image = PathBuf::from(format!("/proc/{pid}/exe"));
                if super::handoff::same_code(desired, &image)? {
                    return Ok::<(), String>(());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|_| {
            "compiler activation did not finish its handover before the deadline".to_owned()
        })?
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, Write};

    /// A service that answers `upgrade` at once and each verification health after `delay`, naming this
    /// process as the one that runs the desired executable.
    fn slow_verifier(state: &Path, delay: Duration) -> std::thread::JoinHandle<()> {
        let address = super::super::endpoint(&std::fs::canonicalize(state).unwrap()).unwrap();
        let listener = std::os::unix::net::UnixListener::bind_addr(&address).unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                std::thread::spawn(move || {
                    let mut line = String::new();
                    if std::io::BufReader::new(&stream).read_line(&mut line).is_err() {
                        return;
                    }
                    let request: serde_json::Value = serde_json::from_str(&line).unwrap();
                    let value = match request["op"].as_str() {
                        Some("health") => {
                            std::thread::sleep(delay);
                            json!({"pid": std::process::id()})
                        }
                        _ => json!({}),
                    };
                    let _ = (&stream).write_all(format!("{}\n", json!({"ok":true,"value":value})).as_bytes());
                });
            }
        })
    }

    fn activation_state(name: &str) -> PathBuf {
        let state = std::env::temp_dir().join(format!(
            "compiler-activation-{name}-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        std::fs::create_dir_all(&state).unwrap();
        state
    }

    #[test]
    #[ignore = "outage gate 13: about 65 s"]
    fn a_verification_health_slower_than_a_snapshot_finishes_within_the_activation_budget() {
        let state = activation_state("slow");
        let _service = slow_verifier(&state, Duration::from_secs(65));
        let started = std::time::Instant::now();
        let desired = std::env::current_exe().unwrap();
        let activated = handed_over(&state, &desired, Deadline::after(ACTIVATION_BUDGET));
        assert_eq!(activated, Ok(()));
        assert!(started.elapsed() > Duration::from_secs(65));
        std::fs::remove_dir_all(state).unwrap();
    }

    #[test]
    #[ignore = "outage gate 13: about 120 s"]
    fn an_activation_past_its_whole_budget_fails() {
        let state = activation_state("past");
        let _service = slow_verifier(&state, Duration::from_secs(3600));
        let started = std::time::Instant::now();
        let desired = std::env::current_exe().unwrap();
        let activated = handed_over(&state, &desired, Deadline::after(ACTIVATION_BUDGET));
        let elapsed = started.elapsed();
        assert!(activated.is_err(), "{activated:?}");
        assert!(elapsed >= ACTIVATION_BUDGET - Duration::from_millis(100), "{elapsed:?}");
        assert!(elapsed < ACTIVATION_BUDGET + Duration::from_secs(3), "{elapsed:?}");
        std::fs::remove_dir_all(state).unwrap();
    }

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
        std::fs::write(
            runtime.join("systemd/user/doggy-compile.service.d/authority.conf"),
            "[Service]\nExecStart=\nExecStart=/nix/store/stale/bin/compile-queue\n",
        )
        .unwrap();
        let contents = std::fs::read_to_string(
            runtime.join("systemd/user/doggy-compile.service.d/zz-native-authority.conf"),
        )
        .unwrap();
        assert!(contents.contains("ExecStart=\"/nix/store/current/bin/compile-queue\""));
        assert!(contents.contains("RefuseManualStop=yes"));
        publish_authority(current, &runtime).unwrap();
        assert_eq!(
            std::fs::read_dir(runtime.join("systemd/user/doggy-compile.service.d"))
                .unwrap()
                .count(),
            2
        );
        std::fs::remove_dir_all(runtime).unwrap();
    }
    #[test]
    fn delegation_is_published_once_beside_any_unit() {
        let runtime = std::env::temp_dir().join(format!(
            "compiler-delegation-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        publish_delegation(&runtime).unwrap();
        let path = runtime.join("systemd/user/doggy-compile.service.d/memory-cells.conf");
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "[Service]\nDelegate=memory\nDelegateSubgroup=daemon\n"
        );
        let written = std::fs::metadata(&path).unwrap().modified().unwrap();
        publish(Path::new("/nix/store/older/share/systemd/user/doggy-compile.service"), &runtime).unwrap();
        publish_delegation(&runtime).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), written);
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
