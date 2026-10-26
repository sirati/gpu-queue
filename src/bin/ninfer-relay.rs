use std::process::ExitCode;
use std::time::Duration;

use gpu_arbiter::relay::{self, Relay};

const USAGE: &str = "usage: ninfer-relay --listen 127.0.0.1:PORT --socket PATH [--hold-secs N]";

fn main() -> ExitCode {
    let mut listen = None;
    let mut socket = None;
    let mut hold = Duration::from_secs(3600);
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = args.next();
        match (arg.as_str(), value) {
            ("--listen", Some(v)) => match v.parse() {
                Ok(a) => listen = Some(a),
                Err(e) => {
                    eprintln!("ninfer-relay: --listen {v}: {e}");
                    return ExitCode::from(2);
                }
            },
            ("--socket", Some(v)) => socket = Some(v.into()),
            ("--hold-secs", Some(v)) => match v.parse::<u64>() {
                Ok(n) => hold = Duration::from_secs(n),
                Err(e) => {
                    eprintln!("ninfer-relay: --hold-secs {v}: {e}");
                    return ExitCode::from(2);
                }
            },
            _ => {
                eprintln!("{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    let (Some(listen), Some(socket)) = (listen, socket) else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("ninfer-relay: tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(relay::run(Relay {
        listen,
        socket,
        hold,
    })) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ninfer-relay: {e}");
            ExitCode::FAILURE
        }
    }
}
