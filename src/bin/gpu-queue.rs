//! Operating modes reuse the existing daemon and native lease session.
use gpu_arbiter::protocol::{LeaseRequest, Mode, WatchdogPolicy};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, ExitCode};
use std::time::Duration;

fn run() -> Result<i32, String> {
    let mut args = std::env::args().skip(1);
    let mode = args
        .next()
        .ok_or("usage: gpu-queue daemon|client|independent [OPTIONS] -- COMMAND")?;
    let mut socket = PathBuf::from(
        std::env::var_os("GPU_SWITCH_SOCKET").unwrap_or_else(|| "/run/gpu-switch/host.sock".into()),
    );
    let mut directory =
        std::env::temp_dir().join(format!("gpu-queue-{}", unsafe { libc::getuid() }));
    let mut config = None;
    let mut shared = false;
    let mut vram = 0;
    let mut expected_runtime_secs = None;
    let mut watchdog = WatchdogPolicy::Standard;
    let mut command = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config = args.next().map(PathBuf::from),
            "--socket" => socket = args.next().ok_or("--socket needs a path")?.into(),
            "--runtime-dir" => directory = args.next().ok_or("--runtime-dir needs a path")?.into(),
            "--shared" => {
                shared = true;
                vram = gpu_arbiter::protocol::parse_vram(
                    &args.next().ok_or("--shared needs VRAM size")?,
                )
                .ok_or("invalid VRAM")?;
            }
            "--exclusive" => shared = false,
            "--watchdog" => {
                watchdog = WatchdogPolicy::parse(&args.next().ok_or("--watchdog needs a policy")?)?
            }
            "--expected-secs" => {
                expected_runtime_secs = Some(
                    args.next()
                        .ok_or("--expected-secs needs seconds")?
                        .parse::<i64>()
                        .map_err(|_| "expected runtime must be positive seconds or -1")?,
                )
            }
            "--" => {
                command.extend(args);
                break;
            }
            _ => return Err(format!("unknown option {arg}")),
        }
    }
    if mode == "daemon" {
        let path = config.ok_or("daemon requires --config FILE")?;
        let config = gpu_arbiter::switch::config::SwitchConfig::load(&path)?;
        let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        rt.block_on(gpu_arbiter::switch::service::run(config, path))?;
        return Ok(0);
    }
    if mode == "independent" {
        let daemon = std::env::current_exe()
            .map_err(|e| e.to_string())?
            .with_file_name("gpu-switch");
        socket = gpu_arbiter::independent::socket(
            &directory,
            &daemon,
            std::path::Path::new("nvidia-smi"),
        )?;
    } else if mode != "client" {
        return Err("mode must be daemon, client or independent".into());
    }
    if command.is_empty() {
        return Err("a command is required after --".into());
    }
    let mut lease = gpu_arbiter::lease::Lease::acquire(
        &socket,
        LeaseRequest {
            category: gpu_arbiter::protocol::Category::Other,
            max_runtime_secs: None,
            expected_runtime_secs,
            mode: if shared {
                Mode::Shared
            } else {
                Mode::Exclusive
            },
            vram: Some(vram),
            label: Some(command.join(" ")),
            priority: 0,
            group: None,
            requeued: false,
            cooperative: false,
            resume: None,
            shutdown_grace_secs: None,
            watchdog,
        },
        |reason| eprintln!("gpu-queue: {}", reason.reason),
    )?;
    let mut child_command = Command::new(&command[0]);
    child_command.args(&command[1..]);
    unsafe {
        child_command.pre_exec(|| {
            // A dead lease owner must not leave an unsupervised command behind.
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = child_command.spawn().map_err(|e| e.to_string())?;
    loop {
        if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
            if !status.success() {
                let _ = lease.fail(format!("command failed: {status}"));
            }
            lease.release();
            use std::os::unix::process::ExitStatusExt;
            return Ok(status.code().unwrap_or(128 + status.signal().unwrap_or(9)));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}
fn main() -> ExitCode {
    match run() {
        Ok(code) => ExitCode::from(code as u8),
        Err(error) => {
            eprintln!("gpu-queue: {error}");
            ExitCode::from(2)
        }
    }
}
