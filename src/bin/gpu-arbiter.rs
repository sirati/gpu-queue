use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::ExitCode;

use gpu_arbiter::arbiter::config::ArbiterConfig;
use gpu_arbiter::arbiter::link;
use gpu_arbiter::arbiter::state::Shared;
use gpu_arbiter::proxy;
use gpu_arbiter::switch::{bind, listen_fds};
use tokio::signal::unix::{signal, SignalKind};

const USAGE: &str = "usage: gpu-arbiter --config FILE [--check]";

fn main() -> ExitCode {
    let mut config_path: Option<PathBuf> = None;
    let mut check = false;
    let mut args = std::env::args_os().skip(1);
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--config") => config_path = args.next().map(PathBuf::from),
            Some("--check") => check = true,
            Some("-h" | "--help") => {
                println!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            _ => {
                eprintln!("{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    let Some(config_path) = config_path else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let config = match ArbiterConfig::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("gpu-arbiter: {e}");
            return ExitCode::from(2);
        }
    };
    if check {
        return ExitCode::SUCCESS;
    }
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("gpu-arbiter: tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("gpu-arbiter: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(config: ArbiterConfig) -> Result<(), String> {
    let tcp = match config.proxy.listen {
        Some(addr) => Some(
            tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|e| format!("binding {addr}: {e}"))?,
        ),
        None => None,
    };
    let mut fds = listen_fds();
    let mut unix = Vec::new();
    for l in &config.proxy.listeners {
        let std_listener = match fds.iter().position(|(n, _)| *n == l.name) {
            Some(i) => std::os::unix::net::UnixListener::from(fds.remove(i).1),
            None => match &l.path {
                Some(p) => {
                    let s = bind(p)?;
                    if let Some(mode) = l.socket_mode()? {
                        std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode))
                            .map_err(|e| format!("chmod {}: {e}", p.display()))?;
                    }
                    s
                }
                None => {
                    return Err(format!(
                        "no socket for listener {:?}: no fd, no path",
                        l.name
                    ))
                }
            },
        };
        std_listener
            .set_nonblocking(true)
            .map_err(|e| format!("socket {}: {e}", l.name))?;
        let listener = tokio::net::UnixListener::from_std(std_listener)
            .map_err(|e| format!("socket {}: {e}", l.name))?;
        unix.push((listener, proxy::Policy::of(l), l.name.clone()));
    }
    let mut term = signal(SignalKind::terminate()).map_err(|e| e.to_string())?;
    let mut int = signal(SignalKind::interrupt()).map_err(|e| e.to_string())?;
    let names: Vec<String> = config
        .proxy
        .listen
        .iter()
        .map(|a| a.to_string())
        .chain(unix.iter().map(|(_, _, n)| n.clone()))
        .collect();
    eprintln!(
        "gpu-arbiter: proxy on {}, gpu-switch at {}",
        names.join(", "),
        config.switch_socket.display()
    );
    let shared = Shared::new(config);
    if let Some(listener) = tcp {
        tokio::spawn(proxy::serve(shared.clone(), listener));
    }
    for (listener, policy, _) in unix {
        tokio::spawn(proxy::serve_unix(shared.clone(), listener, policy));
    }
    let link_task = tokio::spawn(link::run(shared.clone()));
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
    eprintln!("gpu-arbiter: shutting down");
    // Stop NInfer first; dropping the link then tells gpu-switch.
    link::shutdown(&shared).await;
    link_task.abort();
    let _ = link_task.await;
    Ok(())
}
