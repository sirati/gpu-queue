use std::ffi::OsString;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use gpu_arbiter::cli;
use gpu_arbiter::client::{self, checked, find_job, get_status, request};
use gpu_arbiter::protocol::{
    parse_vram, Category, JobSpec, JobState, LineKind, Mode, OwnJob, Request, PRIORITY_MAX,
    PRIORITY_MIN,
};

const USAGE: &str = "\
usage: gpu-run [--socket PATH] COMMAND
  submit (--exclusive | --shared --vram SIZE) [--category C] [--cooperative]
         [--priority N] [--timeout SECS] [--expected-secs SECS] [--] CMD...
                                 queue CMD in the joint GPU line; it runs with this
                                 env/cwd/umask. SIZE like 12G, 1536M, 2.5GiB or bytes.
                                 C: train benchmark accuracy test eval inference other.
                                 N: -10..10, default 0
                                 SECS: SIGTERM the job after this long running, SIGKILL
                                 10 s later (default: no limit)
  status [ID] [--json]           line, owner, serving state, your jobs, others as counts
  switch-gpu                     stop NInfer and hand the GPU to the queued work
  switch-ninfer                  hold the line, let resident jobs finish, start NInfer
  wait ID [--report]             block until job ID ends; exit with its exit code
                                 (--report: print how it went, with its log's end on failure)
  logs ID [-f]                   print (or follow) the job's combined stdout/stderr
  cancel ID                      remove a queued job, or SIGTERM a running one
  kill HANDLE                    host side: SIGINT/SIGTERM/SIGKILL a container job
  set-host-cache MIB [--apply-now]
  set-profile NAME [--apply-now] NInfer settings for its next start
  serve on|off|status            NInfer on or off; status shows only state and profile
  completions bash               print a bash completion script
  mcp [--max-timeout-secs N]     stdio MCP server for Claude Code: only calls that return at once
  hook                           Claude Code PreToolUse hook (JSON on stdin): refuses a blocking
                                 gpu-run in a foreground Bash call
The socket comes from --socket or $GPU_SWITCH_SOCKET.";

fn main() -> ExitCode {
    let mut args: Vec<OsString> = std::env::args_os().skip(1).collect();
    if args.first().is_some_and(|a| a == "__runner") {
        return gpu_arbiter::runner::main(args.split_off(1));
    }
    if args.first().is_some_and(|a| a == "__complete") {
        // `__complete CWORD WORDS...`: candidates one per line, never an error.
        let words: Vec<String> = args[1..]
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        if let Some((cword, words)) = words.split_first() {
            if let Ok(cword) = cword.parse() {
                let env = std::env::var_os("GPU_SWITCH_SOCKET").map(PathBuf::from);
                for c in cli::complete(words, cword, env, &cli::live_status) {
                    println!("{c}");
                }
            }
        }
        return ExitCode::SUCCESS;
    }
    if args.first().is_some_and(|a| a == cli::COMPLETIONS) {
        return match args.get(1).and_then(|s| s.to_str()) {
            Some("bash") if args.len() == 2 => {
                print!("{}", cli::BASH);
                ExitCode::SUCCESS
            }
            _ => usage(),
        };
    }
    let mut socket = std::env::var_os("GPU_SWITCH_SOCKET").map(PathBuf::from);
    if args.first().is_some_and(|a| a == cli::SOCKET) {
        if args.len() < 2 {
            return usage();
        }
        socket = Some(PathBuf::from(args.remove(1)));
        args.remove(0);
    }
    let Some((cmd, rest)) = args.split_first() else {
        return usage();
    };
    let cmd = cmd.to_string_lossy().into_owned();
    if cmd == "help" || cmd == "-h" || cmd == "--help" {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    if cmd == "hook" && rest.is_empty() {
        return gpu_arbiter::hook::run();
    }
    if cmd == "mcp" {
        let rest: Vec<String> = rest
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        return gpu_arbiter::mcp::run(socket, &rest);
    }
    let Some(socket) = socket else {
        eprintln!("gpu-run: no gpu-switch socket; pass --socket PATH or set GPU_SWITCH_SOCKET");
        return ExitCode::from(2);
    };
    let rest_str: Vec<String> = rest
        .iter()
        .map(|a| a.to_string_lossy().into_owned())
        .collect();
    let result = match cmd.as_str() {
        cli::SUBMIT => submit(&socket, rest),
        cli::STATUS => status(&socket, &rest_str),
        cli::SWITCH_GPU if rest.is_empty() => simple(&socket, Request::SwitchGpu),
        cli::SWITCH_NINFER if rest.is_empty() => simple(&socket, Request::SwitchNinfer),
        cli::WAIT => {
            let (id, report) = match rest_str.as_slice() {
                [id] => (id, false),
                [id, flag] if flag == "--report" => (id, true),
                _ => return usage(),
            };
            match id.parse() {
                Ok(id) => wait(&socket, id, report),
                Err(_) => return usage(),
            }
        }
        cli::LOGS => logs(&socket, &rest_str),
        cli::CANCEL => match one_id(&rest_str) {
            Some(id) => simple(&socket, Request::Cancel { id }),
            None => return usage(),
        },
        cli::KILL => match rest_str.as_slice() {
            [h] => simple(&socket, Request::Kill { handle: h.clone() }),
            _ => return usage(),
        },
        cli::SET_HOST_CACHE => match with_apply_now(&rest_str) {
            Some((m, apply_now)) => match m.parse() {
                Ok(mib) => simple(&socket, Request::SetHostCache { mib, apply_now }),
                Err(_) => return usage(),
            },
            None => return usage(),
        },
        cli::SET_PROFILE => match with_apply_now(&rest_str) {
            Some((p, apply_now)) => simple(
                &socket,
                Request::SetProfile {
                    profile: p.clone(),
                    apply_now,
                },
            ),
            None => return usage(),
        },
        cli::SERVE => match rest_str.as_slice() {
            [a] if a == cli::SERVE_ON => serve(&socket, Request::Serve { on: true }),
            [a] if a == cli::SERVE_OFF => serve(&socket, Request::Serve { on: false }),
            [a] if a == cli::SERVE_STATUS => serve(&socket, Request::ServeStatus),
            _ => return usage(),
        },
        _ => return usage(),
    };
    match result {
        Ok(code) => code,
        Err(e) => {
            eprintln!("gpu-run: {e}");
            ExitCode::FAILURE
        }
    }
}

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::from(2)
}

fn one_id(args: &[String]) -> Option<u64> {
    match args {
        [id] => id.parse().ok(),
        _ => None,
    }
}

fn with_apply_now(args: &[String]) -> Option<(&String, bool)> {
    match args {
        [v] => Some((v, false)),
        [v, f] | [f, v] if f == cli::APPLY_NOW => Some((v, true)),
        _ => None,
    }
}

fn simple(socket: &Path, req: Request) -> Result<ExitCode, String> {
    let reply = checked(request(socket, &req)?)?;
    if let Some(m) = reply.message {
        println!("{m}");
    }
    Ok(ExitCode::SUCCESS)
}

fn profile_line(current: Option<&str>, pending: Option<&str>) -> String {
    let current = current.unwrap_or("-");
    match pending {
        Some(p) => format!("profile: {current} current, {p} pending (next NInfer start)"),
        None => format!("profile: {current}"),
    }
}

/// For the host user: the daemon's message (phase words only), the phase,
/// the profiles. Nothing else from the reply is printed.
fn serve(socket: &Path, req: Request) -> Result<ExitCode, String> {
    let reply = checked(request(socket, &req)?)?;
    let s = reply.serve.ok_or("reply without serve status")?;
    if let Some(m) = reply.message {
        println!("{m}");
    }
    println!("{}", s.phase);
    println!(
        "{}",
        profile_line(s.profile_current.as_deref(), s.profile_pending.as_deref())
    );
    Ok(ExitCode::SUCCESS)
}

fn parse_spec(args: &[OsString]) -> Result<(JobSpec, &[OsString]), String> {
    let mut mode = None;
    let mut vram = None;
    let mut category = Category::Other;
    let mut cooperative = false;
    let mut priority = 0i8;
    let mut max_runtime_secs = None;
    let mut expected_runtime_secs = None;
    let mut i = 0;
    while i < args.len() {
        let a = args[i].to_string_lossy();
        let mut value = || -> Result<String, String> {
            i += 1;
            args.get(i)
                .map(|v| v.to_string_lossy().into_owned())
                .ok_or_else(|| format!("{a} needs a value"))
        };
        match a.as_ref() {
            cli::END => {
                i += 1;
                break;
            }
            cli::EXCLUSIVE | cli::SHARED if mode.is_some() => {
                return Err("name one mode: --exclusive or --shared".into())
            }
            cli::EXCLUSIVE => mode = Some(Mode::Exclusive),
            cli::SHARED => mode = Some(Mode::Shared),
            cli::VRAM => {
                let v = value()?;
                vram =
                    Some(parse_vram(&v).ok_or_else(|| {
                        format!("not a VRAM size: {v:?} (e.g. 12G, 1536M, 2.5GiB)")
                    })?);
            }
            cli::CATEGORY => {
                let v = value()?;
                category = Category::parse(&v).ok_or_else(|| format!("unknown category {v:?}"))?;
            }
            cli::COOPERATIVE => cooperative = true,
            cli::PRIORITY => {
                let v = value()?;
                priority = v
                    .parse()
                    .ok()
                    .filter(|p| (PRIORITY_MIN..=PRIORITY_MAX).contains(p))
                    .ok_or_else(|| format!("priority must be {PRIORITY_MIN}..{PRIORITY_MAX}"))?;
            }
            cli::TIMEOUT => {
                let v = value()?;
                max_runtime_secs = Some(
                    v.parse()
                        .ok()
                        .filter(|s| *s > 0)
                        .ok_or_else(|| format!("not a number of seconds: {v:?}"))?,
                );
            }
            cli::EXPECTED => {
                let v = value()?;
                expected_runtime_secs = Some(
                    v.parse::<u64>()
                        .ok()
                        .filter(|n| *n > 0)
                        .ok_or("--expected-secs needs a positive number of seconds")?,
                );
            }
            s if s.starts_with("--") => return Err(format!("unknown option {s}")),
            _ => break,
        }
        i += 1;
    }
    let mode = mode.ok_or("every submit names its mode: --exclusive or --shared --vram SIZE")?;
    let spec = JobSpec {
        mode,
        vram: vram.unwrap_or(0),
        category,
        cooperative,
        priority,
        max_runtime_secs,
        expected_runtime_secs,
    };
    spec.validate()?;
    Ok((spec, &args[i..]))
}

fn submit(socket: &Path, args: &[OsString]) -> Result<ExitCode, String> {
    let (spec, cmd) = parse_spec(args)?;
    if cmd.is_empty() {
        return Err("a command is required".into());
    }
    let reply = client::submit(socket, &spec, cmd, None)?;
    let job = reply.job.ok_or("reply without job")?;
    println!("{}", job.id);
    if let Some(m) = reply.message {
        eprintln!("{m}");
    }
    Ok(ExitCode::SUCCESS)
}

fn exit_of(j: &OwnJob) -> String {
    match (j.exit_code, j.signal) {
        (Some(c), _) => c.to_string(),
        (None, Some(s)) => format!("sig {s}"),
        _ => "-".into(),
    }
}

fn duration(seconds: Option<f64>) -> String {
    seconds
        .map(|s| format!("{s:.1}s"))
        .unwrap_or_else(|| "-".into())
}

pub fn size(bytes: u64) -> String {
    if bytes == 0 {
        "-".into()
    } else if bytes >= 1 << 30 {
        format!("{:.1}G", bytes as f64 / (1u64 << 30) as f64)
    } else {
        format!("{}M", bytes >> 20)
    }
}

/// A gpu-run job's command, or a lease client's label.
fn what(j: &OwnJob) -> String {
    match (j.lease, &j.label) {
        (true, Some(label)) => command_line(std::slice::from_ref(label), 60),
        _ => command_line(&j.cmd, 60),
    }
}

fn command_line(cmd: &[String], max: usize) -> String {
    let s = cmd.join(" ");
    if s.chars().count() > max {
        let cut: String = s.chars().take(max.saturating_sub(3)).collect();
        format!("{cut}...")
    } else {
        s
    }
}

fn pos(p: Option<usize>) -> String {
    p.map(|p| p.to_string()).unwrap_or_else(|| "-".into())
}

fn status(socket: &Path, args: &[String]) -> Result<ExitCode, String> {
    let json = args.iter().any(|a| a == cli::JSON);
    let ids: Vec<&String> = args.iter().filter(|a| *a != cli::JSON).collect();
    let id = match ids.as_slice() {
        [] => None,
        [id] => Some(
            id.parse::<u64>()
                .map_err(|_| "status takes your own job id")?,
        ),
        _ => return Ok(usage()),
    };
    let s = get_status(socket)?;
    if let Some(id) = id {
        let j = s
            .jobs
            .iter()
            .find(|j| j.id == id)
            .ok_or(format!("no job {id}"))?;
        if json {
            println!("{}", serde_json::to_string_pretty(j).expect("serializable"));
        } else {
            print_job(j);
        }
        return Ok(ExitCode::SUCCESS);
    }
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&s).expect("serializable")
        );
        return Ok(ExitCode::SUCCESS);
    }
    println!("{}", s.summary);
    println!(
        "serving: {}, GPU owner: {}",
        if s.serve_wanted { "on" } else { "off" },
        s.owner
    );
    println!(
        "{}",
        profile_line(
            s.serve.profile_current.as_deref(),
            s.serve.profile_pending.as_deref()
        )
    );
    let mib = |v: Option<u64>| {
        v.map(|m| format!("{m} MiB"))
            .unwrap_or_else(|| "unset".into())
    };
    match s.host_cache_pending_mib {
        Some(p) => println!(
            "host cache: {} current, {} pending (next NInfer start)",
            mib(s.host_cache_current_mib),
            mib(Some(p))
        ),
        None => println!("host cache: {}", mib(s.host_cache_current_mib)),
    }
    println!("GPU processes: {}", s.gpu_processes);
    if let Some(e) = &s.ninfer_error {
        println!("NInfer last error: {e}");
    }
    if !s.line.is_empty() {
        println!("line:");
        for e in &s.line {
            let who = match e.kind {
                LineKind::Own => format!("job {} (yours)", e.job.as_deref().unwrap_or("?")),
                LineKind::Other => e.job.clone().unwrap_or_else(|| "other".into()),
                LineKind::Ninfer => "ninfer".into(),
            };
            println!("  {:>3}  {:<16} prio {}", e.position, who, e.priority);
        }
    }
    let active: Vec<&OwnJob> = s.jobs.iter().filter(|j| !j.state.is_terminal()).collect();
    let finished: Vec<&OwnJob> = s.jobs.iter().filter(|j| j.state.is_terminal()).collect();
    let recent = &finished[finished.len().saturating_sub(10)..];
    if !active.is_empty() || !recent.is_empty() {
        println!("your jobs:");
        println!(
            "  {:>4}  {:<9}  {:>3}  {:<9}  {:>6}  {:>4}  {:<9}  {:>5}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  COMMAND",
            "ID", "STATE", "POS", "MODE", "VRAM", "PRIO", "CATEGORY", "EXIT", "EXPECT", "PREP", "GPU", "NO-VRAM", "NO-UTIL"
        );
        for j in recent.iter().chain(active.iter()) {
            let state = if j.behind_ninfer {
                "waiting".to_string()
            } else {
                j.state.as_str().to_string()
            };
            println!(
                "  {:>4}  {:<9}  {:>3}  {:<9}  {:>6}  {:>4}  {:<9}  {:>5}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {}",
                j.id,
                state,
                pos(j.position),
                j.mode.as_str(),
                size(j.vram),
                j.priority,
                j.category.as_str(),
                exit_of(j),
                duration(j.expected_runtime_secs.map(|n| n as f64)),
                duration(j.timing.as_ref().map(|t| t.preparation_seconds)),
                duration(j.timing.as_ref().map(|t| t.gpu_seconds)),
                duration(j.timing.as_ref().map(|t| t.no_vram_seconds)),
                duration(j.timing.as_ref().map(|t| t.no_utilisation_seconds)),
                what(j)
            );
        }
        if active.iter().any(|j| j.behind_ninfer) {
            println!("  (waiting: behind NInfer, NOT running until 'gpu-run switch-gpu')");
        }
    }
    if !s.other_jobs.is_empty() {
        println!("other side's jobs:");
        println!(
            "  {:<8}  {:<9}  {:>3}  {:<9}  {:>6}  {:>4}  COOP",
            "HANDLE", "STATE", "POS", "MODE", "VRAM", "PRIO"
        );
        for j in &s.other_jobs {
            println!(
                "  {:<8}  {:<9}  {:>3}  {:<9}  {:>6}  {:>4}  {}",
                j.handle,
                j.state.as_str(),
                pos(j.position),
                j.mode.as_str(),
                size(j.vram),
                j.priority,
                if j.cooperative { "yes" } else { "no" }
            );
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn print_job(j: &OwnJob) {
    println!(
        "expected GPU runtime: {}",
        duration(j.expected_runtime_secs.map(|n| n as f64))
    );
    if let Some(t) = &j.timing {
        println!("GPU preparation: {:.3}s, GPU grant time: {:.3}s, without VRAM: {:.3}s, without utilisation: {:.3}s",
            t.preparation_seconds, t.gpu_seconds, t.no_vram_seconds, t.no_utilisation_seconds);
    }
    println!("job {} ({}): {}", j.id, j.handle, j.state.as_str());
    if let Some(p) = j.position {
        println!("line position: {p}");
    }
    if j.behind_ninfer {
        println!("behind NInfer: NOT running until 'gpu-run switch-gpu'");
    }
    println!(
        "mode: {}, VRAM: {}, priority: {}, category: {}, cooperative: {}",
        j.mode.as_str(),
        size(j.vram),
        j.priority,
        j.category.as_str(),
        j.cooperative
    );
    println!("command: {}", command_line(&j.cmd, usize::MAX));
    println!("cwd: {}", j.cwd);
    println!("log: {}", j.log);
    println!("exit: {}", exit_of(j));
    if let Some(e) = &j.error {
        println!("reason: {e}");
    }
    println!("runs: {}", j.runs);
    let pid = j.pid.map(|p| p.to_string()).unwrap_or_else(|| "-".into());
    println!("runner pid: {}, job pid: {pid}", j.runner_pid);
}

fn wait(socket: &Path, id: u64, report: bool) -> Result<ExitCode, String> {
    let job = checked(request(socket, &Request::Wait { id })?)?
        .job
        .ok_or("reply without job")?;
    // For an agent that waits in the background: how it went, on stdout.
    if report {
        print!(
            "{}",
            gpu_arbiter::mcp::job_report(&job, job.state != JobState::Done)
        );
    }
    let code = match (job.exit_code, job.signal) {
        (Some(c), _) => c,
        (None, Some(s)) => 128 + s,
        _ => 1,
    };
    if job.state != JobState::Done {
        let why = job
            .error
            .as_deref()
            .map(|e| format!(" ({e})"))
            .unwrap_or_default();
        eprintln!(
            "gpu-run: job {id} {}: exit {}{why}",
            job.state.as_str(),
            exit_of(&job)
        );
    }
    Ok(ExitCode::from((code & 0xff) as u8))
}

fn logs(socket: &Path, args: &[String]) -> Result<ExitCode, String> {
    let (id, follow) = match args {
        [id] => (id, false),
        [id, f] | [f, id] if f == cli::FOLLOW => (id, true),
        _ => return Ok(usage()),
    };
    let Ok(id) = id.parse::<u64>() else {
        return Ok(usage());
    };
    let mut job = find_job(socket, id)?;
    let mut stdout = std::io::stdout().lock();
    let mut offset = 0u64;
    loop {
        let terminal = job.state.is_terminal();
        let copied = match std::fs::File::open(&job.log) {
            Ok(mut f) => {
                f.seek(SeekFrom::Start(offset)).map_err(|e| e.to_string())?;
                let n = std::io::copy(&mut f, &mut stdout).map_err(|e| e.to_string())?;
                offset += n;
                stdout.flush().map_err(|e| e.to_string())?;
                n
            }
            Err(_) if job.state == JobState::Queued && !follow => {
                eprintln!("gpu-run: job {id} is queued; no output yet");
                return Ok(ExitCode::SUCCESS);
            }
            Err(e) if job.started_at.is_some() && !follow => {
                return Err(format!("{}: {e}", job.log));
            }
            Err(_) => 0,
        };
        if !follow || (terminal && copied == 0) {
            return Ok(ExitCode::SUCCESS);
        }
        if copied == 0 {
            std::thread::sleep(Duration::from_millis(250));
        }
        job = find_job(socket, id)?;
    }
}
