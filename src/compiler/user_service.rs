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
    let changed = publish(unit, &runtime)?;
    systemctl(&["daemon-reload"])?;
    systemctl(&[
        if changed { "restart" } else { "start" },
        "--no-block",
        "doggy-compile.service",
    ])?;
    // A project can immediately construct its native client after activation.
    std::fs::create_dir_all(format!("/var/tmp/rust-compile-{uid}")).map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
