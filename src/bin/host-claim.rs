//! host-claim: waits for a host memory claim in a process that holds nothing else, then executes the program
//! that uses the memory, in the same process, with the granted claim in `HOST_MEMORY_CLAIM`.
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use gpu_arbiter::compiler::client::{Client, Failure};
use gpu_arbiter::compiler::memory_client::{
    HostMemoryClaim, PreparationWait, Unplaced, AUTHORITY_PATIENCE, HANDOVER,
};
use gpu_arbiter::compiler::model::Purpose;
use gpu_arbiter::front_end::{self, CancelFile};

const USAGE: &str = "\
usage: host-claim --bytes N [--label L] [--priority P] [--state-dir D] (--gpu-place | --host-only)
                  [--cancel-file PATH] -- ARGV
  Waits for a host memory claim of N bytes, then executes ARGV in this process with
  HOST_MEMORY_CLAIM={\"claim_id\",\"memory_bytes\",\"state_dir\"} set.
  --gpu-place   the claim is held for work next on the GPU
  --host-only   the claim is held only for host work, which ends
exit 125 if PATH appears first, 75 if the compile service is unreachable or lacks
the transfer or purpose capability, 2 on a usage error.";

/// The process that consumes a GPU place hands it on in this variable (`gpu-run place`).
const PLACE: &str = "GPU_QUEUE_PLACE";

/// How often a waiting front-end looks for its cancel file: within a second of its appearance it exits.
const CANCEL_POLL: Duration = Duration::from_millis(200);

struct Arguments {
    bytes: u64,
    label: String,
    priority: i8,
    state: PathBuf,
    purpose: Purpose,
    cancel: CancelFile,
    argv: Vec<OsString>,
}

impl Arguments {
    fn parse(mut args: impl Iterator<Item = OsString>) -> Result<Self, String> {
        let mut bytes = None;
        let mut label = String::new();
        let mut priority = 0;
        let mut state = gpu_arbiter::compiler::default_state();
        let mut purposes = Vec::new();
        let mut cancel = None;
        loop {
            let Some(flag) = args.next() else {
                return Err("missing -- ARGV".into());
            };
            let flag = flag.to_string_lossy().into_owned();
            let mut value = || {
                args.next()
                    .map(|value| value.to_string_lossy().into_owned())
                    .ok_or(format!("{flag} needs a value"))
            };
            match flag.as_str() {
                "--bytes" => {
                    bytes = Some(
                        value()?
                            .parse::<u64>()
                            .ok()
                            .filter(|bytes| *bytes > 0)
                            .ok_or("--bytes takes positive bytes")?,
                    )
                }
                "--label" => label = value()?,
                "--priority" => {
                    let given = value()?.parse::<i64>().map_err(|e| e.to_string())?;
                    priority = gpu_arbiter::compiler::bounded_priority(given)?;
                }
                "--state-dir" => state = PathBuf::from(value()?),
                "--cancel-file" => cancel = Some(PathBuf::from(value()?)),
                "--gpu-place" => purposes.push(Purpose::GpuPlace),
                "--host-only" => purposes.push(Purpose::HostOnly),
                "--" => break,
                other => return Err(format!("unknown option {other}")),
            }
        }
        let argv: Vec<OsString> = args.collect();
        let [purpose] = purposes[..] else {
            return Err("give exactly one of --gpu-place and --host-only".into());
        };
        if std::env::var_os(PLACE).is_some() && purpose != Purpose::GpuPlace {
            return Err(format!("{PLACE} is set: a claim beside a GPU place is --gpu-place"));
        }
        if argv.is_empty() {
            return Err("missing ARGV after --".into());
        }
        Ok(Self {
            bytes: bytes.ok_or("--bytes is required")?,
            label,
            priority,
            state,
            purpose,
            cancel: CancelFile::new(cancel),
            argv,
        })
    }
}

/// Why the front-end ends without executing its program.
enum Ended {
    Cancelled,
    Unavailable(String),
    Refused(String),
    NotExecuted(String),
}

impl Ended {
    fn exit(self) -> ExitCode {
        let (code, reason) = match self {
            Ended::Cancelled => (front_end::CANCELLED, "the cancel file appeared".to_owned()),
            Ended::Unavailable(reason) => (front_end::UNAVAILABLE, reason),
            Ended::Refused(reason) => (1, reason),
            Ended::NotExecuted(reason) => (front_end::NOT_EXECUTED, reason),
        };
        eprintln!("host-claim: {reason}");
        ExitCode::from(code)
    }
}

impl From<Failure> for Ended {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Unavailable(reason) => Ended::Unavailable(reason),
            Failure::Refused(reason) => Ended::Refused(reason),
        }
    }
}

fn main() -> ExitCode {
    let arguments = match Arguments::parse(std::env::args_os().skip(1)) {
        Ok(arguments) => arguments,
        Err(error) => {
            eprintln!("host-claim: {error}\n{USAGE}");
            return ExitCode::from(front_end::USAGE);
        }
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("host-claim runtime");
    match runtime.block_on(claim(&arguments, &mut Unplaced)) {
        Ok(claim) => {
            let handover = serde_json::to_string(&claim.handover()).expect("serializable");
            // Executed, the program holds the claim in this same process; only a failed exec returns, and
            // dropping the claim then withdraws it.
            let error = front_end::execute(&arguments.argv, HANDOVER, &handover);
            Ended::NotExecuted(format!("{:?}: {error}", arguments.argv[0])).exit()
        }
        Err(ended) => ended.exit(),
    }
}

/// The granted claim, unless the cancel file appears first. A claim given up is dropped, which withdraws it
/// within a bounded request; the service also withdraws it once this process has exited.
async fn claim(
    arguments: &Arguments,
    place: &mut dyn PreparationWait,
) -> Result<HostMemoryClaim, Ended> {
    let client = Client::new(&arguments.state, None).map_err(Ended::Unavailable)?;
    let mut claim = HostMemoryClaim::new(
        client.clone(),
        arguments.bytes,
        arguments.label.clone(),
        arguments.priority,
        Some(arguments.purpose),
    )
    .map_err(Ended::Refused)?;
    tokio::select! {
        acquired = acquire(&client, &mut claim, place) => acquired,
        () = cancelled(&arguments.cancel) => Err(Ended::Cancelled),
    }?;
    Ok(claim)
}

async fn acquire(
    client: &Client,
    claim: &mut HostMemoryClaim,
    place: &mut dyn PreparationWait,
) -> Result<(), Ended> {
    client
        .require(&["transfer", "purpose"], AUTHORITY_PATIENCE)
        .await?;
    let registered = claim.register().await.map_err(Ended::Refused)?;
    for warning in registered["warnings"].as_array().into_iter().flatten() {
        eprintln!("host-claim: warning: {}", warning.as_str().unwrap_or_default());
    }
    claim.acquire_reporting(place).await.map_err(Ended::Refused)
}

async fn cancelled(cancel: &CancelFile) {
    while !cancel.requested() {
        tokio::time::sleep(CANCEL_POLL).await;
    }
}
