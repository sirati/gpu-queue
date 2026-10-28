//! The compiler authority. All state lives in a Tokio actor, never a file lock.
use gpu_arbiter::compiler::{self, Config};
use std::path::PathBuf;

#[tokio::main]
async fn main() {
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
