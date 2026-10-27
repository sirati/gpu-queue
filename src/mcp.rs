//! `gpu-run mcp`: gpu-run as an MCP server over stdio, for the Claude Code
//! that lives next to NInfer.
//!
//! That agent's model is NInfer. While a queued job runs, NInfer is down; while
//! NInfer serves, queued jobs sit behind it. So anything that waits for the GPU
//! from inside a tool call waits for something that only happens once the
//! agent's own next request is served: a deadlock. No tool here waits for the
//! GPU. What the CLI has beyond them is left out on purpose:
//!
//! - `wait`, `logs -f`: block on the line;
//! - `switch-ninfer`, `serve off`: take the agent's own model away for good;
//! - `set-profile`/`set-host-cache --apply-now`: restart the model under it;
//! - `kill`: a host-side tool.
//!
//! `gpu_run_queue` is the one tool that costs the agent its model. It hands the
//! GPU to the line, which runs every queued job and gives it back to NInfer by
//! itself when the queue is empty. The agent cannot read anything before its
//! model is back, so the call returns only once its jobs have ended, with how
//! each went; it waits for the jobs, never for the GPU, and stops waiting when
//! the model takes the GPU back with jobs of the agent's still queued.
//!
//! JSON-RPC 2.0, one message per line (MCP's stdio transport).

use std::ffi::OsString;
use std::io::{BufRead, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::client;
use crate::protocol::{
    now_unix, parse_vram, Category, JobSpec, JobState, LineKind, Mode, OwnJob, Request, Status,
    PRIORITY_MAX, PRIORITY_MIN,
};

/// The longest `timeout_secs` a job may ask for, unless `--max-timeout-secs`.
pub const DEFAULT_MAX_TIMEOUT_SECS: u64 = 86_400;

const SUPPORTED_VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

/// Log tail: lines returned by default, the most allowed, and how much of the
/// file's end is read to find them.
const TAIL_DEFAULT: usize = 100;
const TAIL_MAX: usize = 2000;
const TAIL_READ_BYTES: u64 = 512 * 1024;
const TEXT_MAX_BYTES: usize = 32 * 1024;

/// gpu_run_queue: how often it looks at the jobs, how long the queue may be
/// unreachable, how many looks show the model holding the GPU with jobs of ours
/// still queued before it stops waiting for them, and the log lines of a failed
/// job it reports.
const POLL: Duration = Duration::from_secs(2);
const UNREACHABLE_MAX: Duration = Duration::from_secs(120);
const HELD_POLLS: u32 = 3;
const FAILED_TAIL: usize = 20;

const INSTRUCTIONS: &str = "\
GPU jobs for this machine. The model you are talking to runs on the same GPU: \
while queued jobs run, it is stopped. Queue with gpu_submit; nothing runs \
until gpu_run_queue, which stops the model, runs everything queued and \
returns once your jobs have ended, with how each went; the model is back \
when you read that. Jobs must not use the local model: it is down while they \
run.";

pub fn run(socket: Option<PathBuf>, args: &[String]) -> ExitCode {
    let mut max_timeout_secs = DEFAULT_MAX_TIMEOUT_SECS;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match (a.as_str(), it.next().and_then(|v| v.parse::<u64>().ok())) {
            ("--max-timeout-secs", Some(n)) if n > 0 => max_timeout_secs = n,
            _ => {
                eprintln!("usage: gpu-run mcp [--max-timeout-secs N]");
                return ExitCode::from(2);
            }
        }
    }
    let server = Server {
        socket,
        max_timeout_secs,
        submitted: Default::default(),
    };
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        if let Some(reply) = server.handle_line(&line) {
            let mut out = reply.to_string();
            out.push('\n');
            if stdout.write_all(out.as_bytes()).is_err() || stdout.flush().is_err() {
                break;
            }
        }
    }
    ExitCode::SUCCESS
}

pub struct Server {
    pub socket: Option<PathBuf>,
    pub max_timeout_secs: u64,
    /// Jobs queued through this server, i.e. by this one Claude Code session.
    pub submitted: std::cell::RefCell<Vec<u64>>,
}

fn error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn tool_result(r: Result<String, String>) -> Value {
    let (text, is_error) = match r {
        Ok(t) => (t, false),
        Err(e) => (e, true),
    };
    json!({"content": [{"type": "text", "text": text}], "isError": is_error})
}

impl Server {
    pub fn handle_line(&self, line: &str) -> Option<Value> {
        match serde_json::from_str::<Value>(line) {
            Ok(msg) => self.handle(&msg),
            Err(_) => Some(error(Value::Null, -32700, "parse error")),
        }
    }

    /// The reply to one message; none for a notification.
    pub fn handle(&self, msg: &Value) -> Option<Value> {
        let Some(obj) = msg.as_object() else {
            return Some(error(
                Value::Null,
                -32600,
                "a JSON-RPC message is an object",
            ));
        };
        let id = obj.get("id").cloned();
        let method = obj.get("method").and_then(Value::as_str);
        let Some(id) = id else {
            // A notification (notifications/initialized, /cancelled): no reply.
            return None;
        };
        let Some(method) = method else {
            // A response to a request of ours; we send none.
            return None;
        };
        let params = obj.get("params").cloned().unwrap_or(Value::Null);
        let result = match method {
            "initialize" => {
                let asked = params.get("protocolVersion").and_then(Value::as_str);
                let version = asked
                    .filter(|v| SUPPORTED_VERSIONS.contains(v))
                    .unwrap_or(SUPPORTED_VERSIONS[1]);
                json!({
                    "protocolVersion": version,
                    "capabilities": {"tools": {"listChanged": false}},
                    "serverInfo": {"name": "gpu-run", "version": env!("CARGO_PKG_VERSION")},
                    "instructions": INSTRUCTIONS,
                })
            }
            "ping" => json!({}),
            "tools/list" => json!({"tools": self.tools()}),
            "tools/call" => {
                let Some(name) = params.get("name").and_then(Value::as_str) else {
                    return Some(error(id, -32602, "tools/call needs a tool name"));
                };
                let args = params.get("arguments").cloned().unwrap_or(json!({}));
                if !args.is_object() {
                    return Some(error(id, -32602, "arguments must be an object"));
                }
                match self.call(name, &args) {
                    Some(r) => tool_result(r),
                    None => {
                        return Some(error(id, -32602, &format!("unknown tool {name:?}")));
                    }
                }
            }
            _ => return Some(error(id, -32601, &format!("method not found: {method}"))),
        };
        Some(json!({"jsonrpc": "2.0", "id": id, "result": result}))
    }

    pub fn tools(&self) -> Vec<Value> {
        let max = self.max_timeout_secs;
        vec![
            json!({
                "name": "gpu_submit",
                "description": format!(
                    "Queue a command that needs the GPU (training, CUDA programs, \
                     anything that allocates VRAM) and return at once with its job id. \
                     The job does NOT start now: while the local model holds the GPU it \
                     waits, and it runs only after gpu_run_queue. It runs with your \
                     current environment and working directory (or `cwd`). It is \
                     stopped (SIGTERM, then SIGKILL) after `timeout_secs`, at most {max}. \
                     The job must not call the local model (this server, the proxy at \
                     127.0.0.1): the model is stopped while jobs run, so such a call \
                     would hang the job. `command` is an argv array, no shell; use \
                     [\"sh\", \"-c\", \"...\"] for shell syntax."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "array", "items": {"type": "string"}, "minItems": 1,
                            "description": "argv of the command"
                        },
                        "mode": {
                            "type": "string", "enum": ["exclusive", "shared"],
                            "description": "exclusive: the whole GPU. shared: beside other \
                                            jobs, needs `vram`."
                        },
                        "vram": {
                            "type": "string",
                            "description": "peak VRAM of a shared job, e.g. \"12G\", \"1536M\""
                        },
                        "timeout_secs": {
                            "type": "integer", "minimum": 1, "maximum": max,
                            "description": "wall-clock limit on the running time"
                        },
                        "expected_runtime_secs": {
                            "type": "integer", "minimum": 1,
                            "description": "estimated GPU duration; required to backfill ahead of blocked jobs"
                        },
                        "category": {
                            "type": "string",
                            "enum": Category::ALL.iter().map(|c| c.as_str()).collect::<Vec<_>>(),
                            "description": "default other"
                        },
                        "priority": {
                            "type": "integer", "minimum": PRIORITY_MIN, "maximum": PRIORITY_MAX,
                            "description": "higher goes first in the line, default 0"
                        },
                        "cwd": {
                            "type": "string",
                            "description": "working directory, default the current one"
                        }
                    },
                    "required": ["command", "mode", "timeout_secs"]
                },
                "annotations": {"title": "Queue a GPU job", "readOnlyHint": false,
                                "destructiveHint": false, "idempotentHint": false,
                                "openWorldHint": false}
            }),
            json!({
                "name": "gpu_status",
                "description": "Show who holds the GPU, whether the local model is serving, \
                                the line and your jobs (state, place in the line, whether a \
                                job waits for gpu_run_queue). With `id`, one job in detail. \
                                Returns at once.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"id": {"type": "integer", "description": "one job"}}
                },
                "annotations": {"title": "GPU and job status", "readOnlyHint": true,
                                "openWorldHint": false}
            }),
            json!({
                "name": "gpu_logs",
                "description": "The last lines of a job's combined stdout/stderr. Returns \
                                at once with what is there now; it never follows the file.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "tail": {
                            "type": "integer", "minimum": 1, "maximum": TAIL_MAX,
                            "description": format!("lines from the end, default {TAIL_DEFAULT}")
                        }
                    },
                    "required": ["id"]
                },
                "annotations": {"title": "Tail a job's log", "readOnlyHint": true,
                                "openWorldHint": false}
            }),
            json!({
                "name": "gpu_cancel",
                "description": "Remove a queued job, or SIGTERM a running one.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"id": {"type": "integer"}},
                    "required": ["id"]
                },
                "annotations": {"title": "Cancel a GPU job", "readOnlyHint": false,
                                "destructiveHint": true, "idempotentHint": true,
                                "openWorldHint": false}
            }),
            json!({
                "name": "gpu_run_queue",
                "description": "Run the queue now: the local model stops, every queued job \
                                runs, and the model comes back by itself once the queue is \
                                empty. The result is a report of your jobs from this run: \
                                which finished, which failed or crashed (with the end of \
                                their logs), and any the model took the GPU back before. \
                                Queue everything you need with gpu_submit first. If \
                                nothing this session queued is waiting (another agent \
                                already ran the queue, say), it does not switch and says \
                                so.",
                "inputSchema": {"type": "object", "properties": {
                    "force": {"type": "boolean", "description": "Switch even though nothing \
                              this session queued is waiting. Not for normal use."}
                }},
                "annotations": {"title": "Run the GPU queue (takes the model offline)",
                                "readOnlyHint": false, "destructiveHint": false,
                                "idempotentHint": false, "openWorldHint": false}
            }),
        ]
    }

    /// `None` for a tool that does not exist; otherwise its outcome.
    pub fn call(&self, name: &str, args: &Value) -> Option<Result<String, String>> {
        Some(match name {
            "gpu_submit" => self.submit(args),
            "gpu_status" => self.status(args),
            "gpu_logs" => self.logs(args),
            "gpu_cancel" => self.cancel(args),
            "gpu_run_queue" => self.run_queue(args),
            _ => return None,
        })
    }

    fn socket(&self) -> Result<&Path, String> {
        self.socket.as_deref().ok_or_else(|| {
            "no gpu-switch socket: GPU_SWITCH_SOCKET is not set for this server".to_string()
        })
    }

    fn submit(&self, args: &Value) -> Result<String, String> {
        let socket = self.socket()?;
        let command: Vec<OsString> = match args.get("command").and_then(Value::as_array) {
            Some(a) if !a.is_empty() => a
                .iter()
                .map(|v| v.as_str().map(OsString::from))
                .collect::<Option<_>>()
                .ok_or("command is an array of strings")?,
            _ => return Err("command is a non-empty array of strings".into()),
        };
        let mode = match args.get("mode").and_then(Value::as_str) {
            Some("exclusive") => Mode::Exclusive,
            Some("shared") => Mode::Shared,
            _ => return Err("mode is \"exclusive\" or \"shared\"".into()),
        };
        let vram = match args.get("vram") {
            None | Some(Value::Null) => 0,
            Some(Value::String(v)) => parse_vram(v)
                .ok_or_else(|| format!("not a VRAM size: {v:?} (e.g. \"12G\", \"1536M\")"))?,
            Some(_) => return Err("vram is a string like \"12G\"".into()),
        };
        let timeout = args
            .get("timeout_secs")
            .and_then(Value::as_u64)
            .ok_or("timeout_secs is required: how many seconds the job may run (an integer)")?;
        if timeout == 0 || timeout > self.max_timeout_secs {
            return Err(format!(
                "timeout_secs must be 1..={} (seconds of running time), got {timeout}",
                self.max_timeout_secs
            ));
        }
        let category = match args.get("category") {
            None | Some(Value::Null) => Category::Other,
            Some(v) => v
                .as_str()
                .and_then(Category::parse)
                .ok_or_else(|| format!("unknown category {v}"))?,
        };
        let priority = match args.get("priority") {
            None | Some(Value::Null) => 0,
            Some(v) => v
                .as_i64()
                .filter(|p| (PRIORITY_MIN as i64..=PRIORITY_MAX as i64).contains(p))
                .ok_or_else(|| format!("priority is {PRIORITY_MIN}..{PRIORITY_MAX}"))?
                as i8,
        };
        let cwd = match args.get("cwd") {
            None | Some(Value::Null) => None,
            Some(v) => {
                let p = Path::new(v.as_str().ok_or("cwd is a path")?);
                if !p.is_dir() {
                    return Err(format!("cwd {} is not a directory", p.display()));
                }
                Some(p.to_path_buf())
            }
        };
        let spec = JobSpec {
            mode,
            vram,
            category,
            cooperative: false,
            priority,
            max_runtime_secs: Some(timeout),
            expected_runtime_secs: args
                .get("expected_runtime_secs")
                .map(|v| {
                    v.as_u64()
                        .filter(|n| *n > 0)
                        .ok_or("expected_runtime_secs must be a positive integer")
                })
                .transpose()?,
        };
        spec.validate()?;
        let reply = client::submit(socket, &spec, &command, cwd.as_deref())?;
        let id = reply.job.ok_or("reply without job")?.id;
        self.submitted.borrow_mut().push(id);
        let job = client::find_job(socket, id)?;
        let mut out = format!(
            "Queued job {id} ({}, {}, time limit {timeout}s).\n",
            mode.as_str(),
            category.as_str()
        );
        out.push_str(&describe_state(&job));
        out.push_str(&format!("\nLog: {}\n", job.log));
        out.push_str(&format!(
            "To be woken when it has ended, start `gpu-run wait {id} --report` now as a \
             background Bash task (run_in_background: true). It ends when the job does and \
             prints how it went; that is your notification, whoever runs the queue.\n"
        ));
        Ok(out)
    }

    fn status(&self, args: &Value) -> Result<String, String> {
        let socket = self.socket()?;
        let s = client::get_status(socket)?;
        if let Some(id) = args.get("id").filter(|v| !v.is_null()) {
            let id = id.as_u64().ok_or("id is a job id")?;
            let job = s
                .jobs
                .iter()
                .find(|j| j.id == id)
                .ok_or(format!("no job {id}"))?;
            return Ok(job_detail(job));
        }
        Ok(overview(&s))
    }

    fn logs(&self, args: &Value) -> Result<String, String> {
        let socket = self.socket()?;
        let id = args
            .get("id")
            .and_then(Value::as_u64)
            .ok_or("id is a job id")?;
        let tail = match args.get("tail") {
            None | Some(Value::Null) => TAIL_DEFAULT,
            Some(v) => v
                .as_u64()
                .filter(|t| (1..=TAIL_MAX as u64).contains(t))
                .ok_or_else(|| format!("tail is 1..{TAIL_MAX}"))? as usize,
        };
        let job = client::find_job(socket, id)?;
        let text = match tail_of(Path::new(&job.log), tail) {
            Ok(t) => t,
            Err(_) if job.started_at.is_none() => {
                return Ok(format!(
                    "Job {id} is {}; it has not started, so there is no output yet.",
                    job.state.as_str()
                ))
            }
            Err(e) => return Err(format!("{}: {e}", job.log)),
        };
        let head = format!(
            "Job {id} is {}; last lines of its log:\n",
            job.state.as_str()
        );
        Ok(if text.is_empty() {
            format!("{head}(no output yet)")
        } else {
            format!("{head}{text}")
        })
    }

    fn cancel(&self, args: &Value) -> Result<String, String> {
        let socket = self.socket()?;
        let id = args
            .get("id")
            .and_then(Value::as_u64)
            .ok_or("id is a job id")?;
        let reply = client::checked(client::request(socket, &Request::Cancel { id })?)?;
        Ok(reply
            .message
            .unwrap_or_else(|| format!("Cancelled job {id}.")))
    }

    fn run_queue(&self, args: &Value) -> Result<String, String> {
        let force = match args.get("force") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(b)) => *b,
            Some(_) => return Err("force is true or false".into()),
        };
        let socket = self.socket()?;
        let s = client::get_status(socket)?;
        let submitted = self.submitted.borrow().clone();
        let session: Vec<&OwnJob> = s
            .jobs
            .iter()
            .filter(|j| submitted.contains(&j.id))
            .collect();
        let waiting = session
            .iter()
            .filter(|j| j.state == JobState::Queued && j.behind_ninfer)
            .count();
        // Agents that agreed that one of them runs the queue may all call this;
        // only the first switch is real, and the others arrive once it is over.
        if waiting == 0 && !force {
            let mut out = String::from(
                "Nothing you queued in this session is waiting to run, so this did not switch \
                 the GPU. Maybe this call was an accident, or another agent ran the queue at \
                 the same time and your jobs have already run.\n",
            );
            if session.is_empty() {
                out += "This session has not queued any job; queue with gpu_submit first.\n";
            }
            for j in &session {
                out += &job_report(j, j.state.is_terminal() && j.state != JobState::Done);
            }
            out += "Only if you really mean to hand the GPU to the queue anyway, call \
                    gpu_run_queue with force: true. That is not for normal use.";
            return Err(out);
        }
        if s.own.waiting == 0 && s.others.waiting == 0 {
            return Err("Nothing waits behind the local model, so there is nothing to run.".into());
        }
        // This session's jobs; with force and none of them left, everything of
        // this side's that waits.
        let mut ids: Vec<u64> = session
            .iter()
            .filter(|j| !j.state.is_terminal())
            .map(|j| j.id)
            .collect();
        if ids.is_empty() {
            ids = s
                .jobs
                .iter()
                .filter(|j| j.behind_ninfer)
                .map(|j| j.id)
                .collect();
        }
        client::checked(client::request(socket, &Request::SwitchGpu)?)?;

        // The model is down until the queue is empty, so nobody reads this
        // reply before then: it waits for the jobs and reports how they went.
        let mut unreachable_since: Option<Instant> = None;
        let mut held_polls = 0;
        loop {
            std::thread::sleep(POLL);
            let s = match client::get_status(socket) {
                Ok(s) => s,
                Err(e) => {
                    if unreachable_since.get_or_insert_with(Instant::now).elapsed()
                        > UNREACHABLE_MAX
                    {
                        return Err(format!(
                            "Lost the GPU queue while your jobs ran: {e}. See gpu_status."
                        ));
                    }
                    continue;
                }
            };
            unreachable_since = None;
            let mine: Vec<&OwnJob> = ids
                .iter()
                .filter_map(|id| s.jobs.iter().find(|j| j.id == *id))
                .collect();
            if mine.iter().all(|j| j.state.is_terminal()) {
                return Ok(run_report(&ids, &mine, &[]));
            }
            // The other side can hand the GPU back to the model early
            // (switch-ninfer); what is left then waits for the next run.
            let held: Vec<&OwnJob> = mine
                .iter()
                .copied()
                .filter(|j| j.state == JobState::Queued && j.behind_ninfer)
                .collect();
            held_polls = if held.is_empty() { 0 } else { held_polls + 1 };
            if held_polls >= HELD_POLLS
                && mine
                    .iter()
                    .all(|j| j.state.is_terminal() || j.behind_ninfer)
            {
                let ran: Vec<&OwnJob> = mine
                    .iter()
                    .copied()
                    .filter(|j| j.state.is_terminal())
                    .collect();
                return Ok(run_report(&ids, &ran, &held));
            }
        }
    }
}

/// How the jobs of one gpu_run_queue went, for the agent when it is back.
/// One job in a line: how it ended (or where it is), and the end of its log
/// when `with_log`.
pub fn job_report(j: &OwnJob, with_log: bool) -> String {
    let took = match (j.started_at, j.finished_at) {
        (Some(a), Some(b)) => format!(", ran {}", span((b - a).max(0.0) as u64)),
        _ => String::new(),
    };
    let how = match j.state {
        JobState::Done => "done".to_string(),
        s if s.is_terminal() => s.as_str().to_uppercase(),
        s => s.as_str().to_string(),
    };
    let ended = if j.state.is_terminal() {
        format!(", {}", exit_of(j))
    } else {
        String::new()
    };
    let mut out = format!("- Job {} ({}): {how}{ended}{took}", j.id, what(j));
    if let Some(e) = &j.error {
        out += &format!(" ({e})");
    }
    out.push('\n');
    if with_log {
        if let Ok(tail) = tail_of(Path::new(&j.log), FAILED_TAIL) {
            out += &format!("  Last lines of its log ({}):\n", j.log);
            for l in tail.lines() {
                out += &format!("    {l}\n");
            }
        }
    }
    out
}

fn run_report(ids: &[u64], ran: &[&OwnJob], held: &[&OwnJob]) -> String {
    let failed: Vec<&&OwnJob> = ran.iter().filter(|j| j.state != JobState::Done).collect();
    let mut out = if failed.is_empty() && held.is_empty() && ran.len() == ids.len() {
        format!(
            "The queue has run. All {} of your job(s) finished successfully.\n",
            ran.len()
        )
    } else {
        format!(
            "The queue has run. Of your {} job(s): {} finished successfully, {} did not{}.\n",
            ids.len(),
            ran.len() - failed.len(),
            failed.len(),
            if held.is_empty() {
                String::new()
            } else {
                format!(", {} did not get to run", held.len())
            }
        )
    };
    for j in ran {
        out += &job_report(j, j.state != JobState::Done);
    }
    for j in held {
        out += &format!(
            "- Job {} ({}): still queued. The local model took the GPU back before it ran; \
             gpu_run_queue runs it.\n",
            j.id,
            what(j)
        );
    }
    let missing = ids.len() - ran.len() - held.len();
    if missing > 0 {
        out += &format!("{missing} job(s) are no longer listed; see gpu_status.\n");
    }
    out += "gpu_logs shows any job's output.";
    out
}

fn span(s: u64) -> String {
    match s {
        0..=59 => format!("{s}s"),
        60..=3599 => format!("{}m{:02}s", s / 60, s % 60),
        _ => format!("{}h{:02}m", s / 3600, (s % 3600) / 60),
    }
}

fn describe_state(j: &OwnJob) -> String {
    match j.state {
        JobState::Queued if j.behind_ninfer => {
            "State: waiting. The local model holds the GPU; the job runs only after \
             gpu_run_queue (which takes the model offline until the queue is empty)."
                .into()
        }
        JobState::Queued => format!(
            "State: queued{}.",
            j.position
                .map(|p| format!(", place {p} in the line"))
                .unwrap_or_default()
        ),
        JobState::Running => "State: running.".into(),
        s => format!("State: {}.", s.as_str()),
    }
}

fn exit_of(j: &OwnJob) -> String {
    match (j.exit_code, j.signal) {
        (Some(c), _) => format!("exit {c}"),
        (None, Some(s)) => format!("signal {s}"),
        _ => "no exit status".into(),
    }
}

fn ago(t: f64) -> String {
    span((now_unix() - t).max(0.0) as u64)
}

fn what(j: &OwnJob) -> String {
    let s = match (j.lease, &j.label) {
        (true, Some(label)) => label.clone(),
        _ => j.cmd.join(" "),
    };
    if s.chars().count() > 70 {
        let cut: String = s.chars().take(67).collect();
        format!("{cut}...")
    } else {
        s
    }
}

fn job_detail(j: &OwnJob) -> String {
    let mut out = format!("Job {} ({}): {}\n", j.id, j.handle, j.state.as_str());
    out.push_str(&describe_state(j));
    out.push('\n');
    if j.state.is_terminal() {
        out.push_str(&format!("Ended: {}", exit_of(j)));
        if let Some(e) = &j.error {
            out.push_str(&format!(" ({e})"));
        }
        out.push('\n');
    }
    out.push_str(&format!(
        "Mode {}, category {}, priority {}\nCommand: {}\nCwd: {}\nLog: {}\nSubmitted {} ago",
        j.mode.as_str(),
        j.category.as_str(),
        j.priority,
        j.cmd.join(" "),
        j.cwd,
        j.log,
        ago(j.submitted_at)
    ));
    if let Some(t) = j.started_at {
        out.push_str(&format!(", started {} ago", ago(t)));
    }
    if let Some(t) = j.finished_at {
        out.push_str(&format!(", finished {} ago", ago(t)));
    }
    out.push('\n');
    out
}

fn overview(s: &Status) -> String {
    let mut out = format!("{}\n", s.summary);
    out.push_str(&format!(
        "Local model: {} (serving wanted: {}), GPU owner: {}\n",
        s.serve.phase,
        if s.serve_wanted { "yes" } else { "no" },
        s.owner
    ));
    let waiting: Vec<&OwnJob> = s
        .jobs
        .iter()
        .filter(|j| j.behind_ninfer && !j.state.is_terminal())
        .collect();
    if !s.line.is_empty() {
        out.push_str("Line:");
        for e in &s.line {
            let who = match e.kind {
                LineKind::Own => format!("your job {}", e.job.as_deref().unwrap_or("?")),
                LineKind::Other => format!("other {}", e.job.as_deref().unwrap_or("job")),
                LineKind::Ninfer => "the local model".into(),
            };
            out.push_str(&format!(" {}. {who};", e.position));
        }
        out.push('\n');
    }
    let active: Vec<&OwnJob> = s.jobs.iter().filter(|j| !j.state.is_terminal()).collect();
    let finished: Vec<&OwnJob> = s.jobs.iter().filter(|j| j.state.is_terminal()).collect();
    let recent = &finished[finished.len().saturating_sub(10)..];
    if active.is_empty() && recent.is_empty() {
        out.push_str("Your jobs: none.\n");
    } else {
        out.push_str("Your jobs:\n");
        for j in recent.iter().chain(active.iter()) {
            let state = if j.behind_ninfer && !j.state.is_terminal() {
                "waiting for gpu_run_queue".to_string()
            } else if j.state.is_terminal() {
                format!("{} ({})", j.state.as_str(), exit_of(j))
            } else {
                j.state.as_str().to_string()
            };
            out.push_str(&format!(
                "  {}: {state}, {}, {} ago: {}\n",
                j.id,
                j.mode.as_str(),
                ago(j.submitted_at),
                what(j)
            ));
        }
    }
    let others = s.others.queued + s.others.running;
    if others > 0 {
        out.push_str(&format!(
            "Other users' jobs: {} queued, {} running.\n",
            s.others.queued, s.others.running
        ));
    }
    if !waiting.is_empty() {
        out.push_str(&format!(
            "{} of your job(s) wait behind the local model. They start after gpu_run_queue, \
             which takes the model (and you) offline until the queue is empty.\n",
            waiting.len()
        ));
    }
    if let Some(e) = &s.ninfer_error {
        out.push_str(&format!("The local model's last error: {e}\n"));
    }
    out
}

/// The last `lines` lines of the file, from at most its last `TAIL_READ_BYTES`.
fn tail_of(path: &Path, lines: usize) -> std::io::Result<String> {
    let mut f = std::fs::File::open(path)?;
    let len = f.metadata()?.len();
    let start = len.saturating_sub(TAIL_READ_BYTES);
    f.seek(SeekFrom::Start(start))?;
    let mut buf = Vec::new();
    f.take(TAIL_READ_BYTES).read_to_end(&mut buf)?;
    let text = String::from_utf8_lossy(&buf);
    let mut all: Vec<&str> = text.lines().collect();
    let mut cut_front = start > 0;
    if cut_front && !all.is_empty() {
        // The first line is probably a fragment.
        all.remove(0);
    }
    if all.len() > lines {
        all.drain(..all.len() - lines);
        cut_front = true;
    }
    let mut out = all.join("\n");
    if out.len() > TEXT_MAX_BYTES {
        let mut from = out.len() - TEXT_MAX_BYTES;
        while !out.is_char_boundary(from) {
            from += 1;
        }
        out = out[from..].to_string();
        cut_front = true;
    }
    if cut_front {
        out = format!("[earlier output omitted]\n{out}");
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server() -> Server {
        Server {
            socket: None,
            max_timeout_secs: 3600,
            submitted: Default::default(),
        }
    }

    fn rpc(s: &Server, msg: Value) -> Value {
        s.handle(&msg).expect("a reply")
    }

    fn tool_names(s: &Server) -> Vec<String> {
        s.tools()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn initialize_negotiates_the_version() {
        let s = server();
        let r = rpc(
            &s,
            json!({"jsonrpc":"2.0","id":1,"method":"initialize",
                   "params":{"protocolVersion":"2025-03-26","capabilities":{},
                             "clientInfo":{"name":"t","version":"1"}}}),
        );
        assert_eq!(r["id"], 1);
        assert_eq!(r["result"]["protocolVersion"], "2025-03-26");
        assert_eq!(r["result"]["serverInfo"]["name"], "gpu-run");
        assert!(r["result"]["capabilities"]["tools"].is_object());
        assert!(r["result"]["instructions"]
            .as_str()
            .unwrap()
            .contains("Queue with gpu_submit"));
        let r = rpc(
            &s,
            json!({"jsonrpc":"2.0","id":"x","method":"initialize",
                   "params":{"protocolVersion":"1999-01-01"}}),
        );
        assert_eq!(r["id"], "x");
        assert_eq!(r["result"]["protocolVersion"], "2025-06-18");
    }

    #[test]
    fn notifications_get_no_reply_and_ping_answers() {
        let s = server();
        assert!(s
            .handle(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .is_none());
        assert!(s
            .handle(&json!({"jsonrpc":"2.0","method":"notifications/cancelled",
                            "params":{"requestId":3}}))
            .is_none());
        assert_eq!(
            rpc(&s, json!({"jsonrpc":"2.0","id":7,"method":"ping"}))["result"],
            json!({})
        );
    }

    #[test]
    fn the_tool_set_cannot_wait_on_the_gpu() {
        let s = server();
        assert_eq!(
            tool_names(&s),
            [
                "gpu_submit",
                "gpu_status",
                "gpu_logs",
                "gpu_cancel",
                "gpu_run_queue"
            ]
        );
        for name in [
            "gpu_wait",
            "gpu_switch_ninfer",
            "gpu_serve",
            "gpu_kill",
            "gpu_set_profile",
            "gpu_set_host_cache",
        ] {
            assert!(s.call(name, &json!({})).is_none(), "{name}");
        }
        let r = rpc(&s, json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}));
        for t in r["result"]["tools"].as_array().unwrap() {
            assert_eq!(t["inputSchema"]["type"], "object");
            assert!(t["description"].as_str().unwrap().len() > 40);
        }
        // No follow flag on logs, no wait-ish argument anywhere.
        let logs = &s.tools()[2];
        assert!(logs["inputSchema"]["properties"]["follow"].is_null());
        // The cap is in the schema the model sees.
        assert_eq!(
            s.tools()[0]["inputSchema"]["properties"]["timeout_secs"]["maximum"],
            3600
        );
    }

    #[test]
    fn errors_are_jsonrpc_errors() {
        let s = server();
        assert_eq!(
            rpc(&s, json!({"jsonrpc":"2.0","id":1,"method":"nope"}))["error"]["code"],
            -32601
        );
        assert_eq!(
            rpc(
                &s,
                json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"gpu_wait"}})
            )["error"]["code"],
            -32602
        );
        assert_eq!(
            rpc(
                &s,
                json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{}})
            )["error"]["code"],
            -32602
        );
        assert_eq!(s.handle_line("{nope").unwrap()["error"]["code"], -32700);
        assert_eq!(s.handle_line("[1]").unwrap()["error"]["code"], -32600);
    }

    #[test]
    fn bad_arguments_are_tool_errors_before_any_socket_is_touched() {
        let s = Server {
            socket: Some(PathBuf::from("/nonexistent/gpu-switch.sock")),
            max_timeout_secs: 3600,
            submitted: Default::default(),
        };
        let err = |name: &str, args: Value| -> String {
            s.call(name, &args).unwrap().expect_err("an error")
        };
        let ok = json!({"command": ["true"], "mode": "exclusive", "timeout_secs": 60});
        for (patch, needle) in [
            (json!({"command": []}), "non-empty"),
            (json!({"command": "true"}), "non-empty"),
            (json!({"command": [1]}), "array of strings"),
            (json!({"mode": "both"}), "exclusive"),
            (json!({"mode": "shared"}), "VRAM"),
            (json!({"vram": "12X"}), "not a VRAM size"),
            (json!({"timeout_secs": null}), "timeout_secs is required"),
            (json!({"timeout_secs": 0}), "1..=3600"),
            (json!({"timeout_secs": 3601}), "1..=3600"),
            (json!({"timeout_secs": "60"}), "timeout_secs is required"),
            (json!({"category": "fun"}), "category"),
            (json!({"priority": 11}), "priority"),
            (json!({"cwd": "/nonexistent-dir"}), "not a directory"),
        ] {
            let mut args = ok.clone();
            for (k, v) in patch.as_object().unwrap() {
                args[k] = v.clone();
            }
            // `shared` without vram is the spec's own refusal; the rest are ours.
            let e = err("gpu_submit", args);
            assert!(e.contains(needle), "{patch}: {e}");
        }
        let mut no_timeout = ok.clone();
        no_timeout.as_object_mut().unwrap().remove("timeout_secs");
        assert!(err("gpu_submit", no_timeout).contains("timeout_secs is required"));
        // Valid arguments get as far as the socket (submit goes through a
        // runner process, which the integration tests drive).
        assert!(err("gpu_status", json!({})).contains("cannot reach gpu-switch"));
        assert!(err("gpu_logs", json!({"id": 1, "tail": 0})).contains("tail"));
        assert!(err("gpu_logs", json!({})).contains("id"));
        assert!(err("gpu_cancel", json!({})).contains("id"));
        assert!(err("gpu_run_queue", json!({})).contains("cannot reach gpu-switch"));
    }

    #[test]
    fn no_socket_is_a_tool_error_not_a_crash() {
        let s = server();
        for name in ["gpu_status", "gpu_run_queue"] {
            let e = s.call(name, &json!({})).unwrap().unwrap_err();
            assert!(e.contains("GPU_SWITCH_SOCKET"), "{e}");
        }
    }

    #[test]
    fn tail_keeps_the_last_lines() {
        let dir = std::env::temp_dir().join(format!("gpu-mcp-tail-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("log");
        let body: String = (1..=300).map(|i| format!("line {i}\n")).collect();
        std::fs::write(&f, &body).unwrap();
        let t = tail_of(&f, 3).unwrap();
        assert_eq!(t, "[earlier output omitted]\nline 298\nline 299\nline 300");
        let all = tail_of(&f, 1000).unwrap();
        assert!(all.starts_with("line 1\n") && all.ends_with("line 300"));
        std::fs::write(&f, "").unwrap();
        assert_eq!(tail_of(&f, 5).unwrap(), "");
        // A file larger than the read window starts at a whole line.
        let big: String = (0..200_000).map(|i| format!("row {i}\n")).collect();
        std::fs::write(&f, &big).unwrap();
        let t = tail_of(&f, 1_000_000).unwrap();
        assert!(t.len() <= TEXT_MAX_BYTES + 40);
        assert!(t.starts_with("[earlier output omitted]\nrow ") || t.starts_with("[earlier"));
        assert!(t.ends_with("row 199999"));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
