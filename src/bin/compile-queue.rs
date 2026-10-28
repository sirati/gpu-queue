//! The compiler authority. All state lives in a Tokio actor, never a file lock.
use gpu_arbiter::compiler::{self, Config};
use std::path::PathBuf;

fn main() {
    if std::env::args().nth(1).as_deref() == Some("--activate-user-service") {
        let executable = std::env::current_exe().expect("compiler executable path");
        let unit = executable
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("share/systemd/user/doggy-compile.service");
        if let Err(error) = compiler::user_service::activate(&unit) {
            eprintln!("compile-queue: {error}");
            std::process::exit(1);
        }
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("compiler runtime");
    runtime.block_on(run());
}

async fn run() {
    let mut args = std::env::args().skip(1);
    let mut config = Config {
        state: PathBuf::from(format!("/var/tmp/rust-compile-{}", unsafe {
            libc::getuid()
        })),
        minimum_ram: 10 << 30,
        average_ram: 1 << 30,
    };
    while let Some(arg) = args.next() {
        let value = args.next().unwrap_or_default();
        match arg.as_str() {
            "--state-dir" => config.state = value.into(),
            "--minimum-ram" => config.minimum_ram = value.parse().expect("minimum RAM in bytes"),
            "--average-ram" => config.average_ram = value.parse().expect("average RAM in bytes"),
            _ => {
                eprintln!("unknown compiler option {arg}");
                std::process::exit(2);
            }
        }
    }
    if let Err(error) = compiler::run(config).await {
        eprintln!("compile-queue: {error}");
        std::process::exit(1);
    }
}
