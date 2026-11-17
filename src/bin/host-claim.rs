//! host-claim: waits for a host memory claim in a process that holds nothing else, then executes the program
//! that uses the memory, in the same process, with the granted claim in `HOST_MEMORY_CLAIM`.
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use gpu_arbiter::compiler::client::{Client, Deadline, Failure};
use gpu_arbiter::compiler::memory_client::{HostMemoryClaim, PreparationWait, Unplaced, HANDOVER};
use gpu_arbiter::compiler::model::Purpose;
use gpu_arbiter::front_end::{self, CancelFile};
use gpu_arbiter::lease::{PlaceHandover, Told, PLACE_HANDOVER};

const USAGE: &str = "\
usage: host-claim --bytes N [--label L] [--priority P] [--state-dir D] (--gpu-place | --host-only)
                  [--cancel-file PATH] -- ARGV
  Waits for a host memory claim of N bytes, then executes ARGV in this process with
  HOST_MEMORY_CLAIM={\"claim_id\",\"memory_bytes\",\"state_dir\"} set.
  --gpu-place   the claim is held for work next on the GPU; a place handed over in
                GPU_QUEUE_PLACE hears why its preparation waits, parked for a large
                claim, and is handed on to ARGV
  --host-only   the claim is held only for host work, which ends
exit 125 if PATH appears first, 75 if the compile service or gpu-switch is
unreachable or the compile service lacks the transfer or purpose capability, 74
if the handed-over place was lost, 2 on a usage error.";

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
        if std::env::var_os(PLACE_HANDOVER).is_some() && purpose != Purpose::GpuPlace {
            return Err(format!("{PLACE_HANDOVER} is set: a claim beside a GPU place is --gpu-place"));
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
            Failure::Unavailable(outage) => Ended::Unavailable(outage.reason),
            Failure::Refused(reason) | Failure::Invalid(reason) => Ended::Refused(reason),
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
    let mut place = match handed_place() {
        Ok(place) => place,
        Err(error) => {
            eprintln!("host-claim: {error}");
            return ExitCode::from(front_end::USAGE);
        }
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("host-claim runtime");
    let reporting: &mut dyn PreparationWait = match place.as_mut() {
        Some(place) => place,
        None => &mut Unplaced,
    };
    match runtime.block_on(claim(&arguments, reporting)) {
        Ok(claim) => {
            let claimed = serde_json::to_string(&claim.handover()).expect("serializable");
            // The place session keeps one reader: this process until the exec, then the program.
            let placed = place.map(|place| serde_json::to_string(&place.handover()).expect("serializable"));
            let mut variables = vec![(HANDOVER, claimed.as_str())];
            variables.extend(placed.as_deref().map(|placed| (PLACE_HANDOVER, placed)));
            // Executed, the program holds the claim in this same process; only a failed exec returns, and
            // dropping the claim then withdraws it.
            let error = front_end::execute(&arguments.argv, &variables);
            Ended::NotExecuted(format!("{:?}: {error}", arguments.argv[0])).exit()
        }
        Err(ended) => match place.as_ref().and_then(Told::failure) {
            Some(failure) => {
                eprintln!("host-claim: place: {}", failure.reason());
                ExitCode::from(front_end::place_exit(failure))
            }
            None => ended.exit(),
        },
    }
}

/// The told place `gpu-run place` handed this process, if any.
fn handed_place() -> Result<Option<Told>, String> {
    std::env::var(PLACE_HANDOVER)
        .ok()
        .map(|text| {
            let handover: PlaceHandover =
                serde_json::from_str(&text).map_err(|e| format!("{PLACE_HANDOVER}: {e}"))?;
            Told::inherited(handover)
        })
        .transpose()
}

/// The granted claim, unless the cancel file appears first. A claim given up is dropped, which withdraws it
/// within a bounded request; the service also withdraws it once this process has exited.
async fn claim(
    arguments: &Arguments,
    place: &mut dyn PreparationWait,
) -> Result<HostMemoryClaim, Ended> {
    let client = Client::new(&arguments.state)?;
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
        .require(&["transfer", "purpose"], Deadline::snapshot())
        .await?;
    let registered = claim.register().await?;
    for warning in registered["warnings"].as_array().into_iter().flatten() {
        eprintln!("host-claim: warning: {}", warning.as_str().unwrap_or_default());
    }
    Ok(claim.acquire_reporting(place).await?)
}

async fn cancelled(cancel: &CancelFile) {
    while !cancel.requested() {
        tokio::time::sleep(CANCEL_POLL).await;
    }
}
