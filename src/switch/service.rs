use std::path::PathBuf;

use crate::switch::config::SwitchConfig;
use crate::switch::control::{self, Role};
use crate::switch::daemon::{self, Daemon};
use crate::switch::{listen_fds, persist, poller, startup};
use tokio::signal::unix::{signal, SignalKind};

pub async fn run(config: SwitchConfig, config_path: PathBuf) -> Result<(), String> {
    let mut fds = listen_fds();
    let mut listeners = Vec::new();
    for l in &config.listen {
        let role = Role::of(&l.name).ok_or_else(|| format!("listen name {:?}", l.name))?;
        let std_listener = match fds.iter().position(|(n, _)| *n == l.name) {
            Some(i) => std::os::unix::net::UnixListener::from(fds.remove(i).1),
            None => match &l.path {
                Some(p) => crate::switch::bind(p)?,
                None => return Err(format!("no socket for listen {:?}: no fd, no path", l.name)),
            },
        };
        std_listener
            .set_nonblocking(true)
            .map_err(|e| format!("socket {}: {e}", l.name))?;
        let listener = tokio::net::UnixListener::from_std(std_listener)
            .map_err(|e| format!("socket {}: {e}", l.name))?;
        listeners.push((listener, role, l.name.clone()));
    }
    let mut term = signal(SignalKind::terminate()).map_err(|e| e.to_string())?;
    let mut int = signal(SignalKind::interrupt()).map_err(|e| e.to_string())?;
    let d = Daemon::new(config, config_path)?;
    persist::restore(&d);
    for (listener, role, name) in listeners {
        eprintln!("gpu-switch: listening ({name})");
        tokio::spawn(control::serve(d.clone(), listener, role));
    }
    tokio::spawn(poller::run(d.clone()));
    tokio::spawn(startup::run(d.clone()));
    tokio::spawn(daemon::run(d.clone()));
    tokio::spawn(persist::run(d.clone()));
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
    // Saved before any connection closes: the clients resume what it holds.
    persist::save(&d);
    eprintln!("gpu-switch: shutting down");
    // Leases end with their helpers; the arbiter stops NInfer when its link drops.
    for l in &d.config.listen {
        if let Some(p) = &l.path {
            let _ = std::fs::remove_file(p);
        }
    }
    Ok(())
}
