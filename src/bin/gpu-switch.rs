use std::path::PathBuf;
use std::process::ExitCode;

use gpu_arbiter::switch::config::SwitchConfig;

const USAGE: &str = "usage: gpu-switch --config FILE [--check]";

fn main() -> ExitCode {
    let args: Vec<std::ffi::OsString> = std::env::args_os().skip(1).collect();
    let mut config_path: Option<PathBuf> = None;
    let mut check = false;
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        match a.to_str() {
            Some("--config") => config_path = it.next().map(PathBuf::from),
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
    let config_path = std::fs::canonicalize(&config_path).unwrap_or(config_path);
    let config = match SwitchConfig::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("gpu-switch: {e}");
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
            eprintln!("gpu-switch: tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(gpu_arbiter::switch::service::run(config, config_path)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("gpu-switch: {e}");
            ExitCode::FAILURE
        }
    }
}
