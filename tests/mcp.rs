//! `gpu-run mcp` and `gpu-run hook` against the real gpu-switch, arbiter and
//! ninfer-mock, plus the run-time limit they rely on.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Stdio};
use std::sync::mpsc::{channel, Receiver};
use std::time::{Duration, Instant};

use common::*;
use serde_json::{json, Value};

/// The MCP server over its stdio, as Claude Code drives it.
struct Mcp {
    child: Child,
    stdin: ChildStdin,
    lines: Receiver<String>,
    next_id: u64,
}

impl Mcp {
    fn start(h: &Harness, max_timeout_secs: u64) -> Mcp {
        let mut c = h.gpu_run(Who::Cont);
        let mut child = c
            .args(["mcp", "--max-timeout-secs", &max_timeout_secs.to_string()])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let out = child.stdout.take().unwrap();
        let (tx, lines) = channel();
        std::thread::spawn(move || {
            for l in BufReader::new(out).lines() {
                let Ok(l) = l else { break };
                if tx.send(l).is_err() {
                    break;
                }
            }
        });
        let mut m = Mcp {
            child,
            stdin,
            lines,
            next_id: 1,
        };
        let init = m.request(
            "initialize",
            json!({"protocolVersion": "2025-06-18", "capabilities": {},
                   "clientInfo": {"name": "test", "version": "0"}}),
        );
        assert_eq!(init["result"]["serverInfo"]["name"], "gpu-run");
        m.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        m
    }

    fn send(&mut self, msg: Value) {
        writeln!(self.stdin, "{msg}").unwrap();
        self.stdin.flush().unwrap();
    }

    /// Every reply must come promptly: a tool that waits is the bug.
    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        let line = self
            .lines
            .recv_timeout(Duration::from_secs(30))
            .unwrap_or_else(|_| panic!("no reply to {method} within 30s"));
        let reply: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(reply["id"], id);
        reply
    }

    /// (is_error, text)
    fn call(&mut self, tool: &str, args: Value) -> (bool, String) {
        let r = self.request("tools/call", json!({"name": tool, "arguments": args}));
        let result = &r["result"];
        assert!(result.is_object(), "{r}");
        (
            result["isError"].as_bool().unwrap(),
            result["content"][0]["text"].as_str().unwrap().to_string(),
        )
    }

    fn ok(&mut self, tool: &str, args: Value) -> String {
        let (err, text) = self.call(tool, args);
        assert!(!err, "{tool} failed: {text}");
        text
    }

    fn err(&mut self, tool: &str, args: Value) -> String {
        let (err, text) = self.call(tool, args);
        assert!(err, "{tool} should have failed: {text}");
        text
    }

    fn submit(&mut self, command: &[&str], timeout_secs: u64) -> u64 {
        let text = self.ok(
            "gpu_submit",
            json!({"command": command, "mode": "exclusive", "timeout_secs": timeout_secs}),
        );
        job_id(&text)
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn job_id(text: &str) -> u64 {
    text.strip_prefix("Queued job ")
        .and_then(|t| t.split_whitespace().next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("no job id in {text:?}"))
}

fn serve_on(h: &Harness) {
    h.run_ok(Who::Host, &["serve", "on"]);
    h.wait_word("SERVING");
}

#[test]
fn the_server_speaks_mcp_and_offers_only_calls_that_return() {
    let h = Harness::start("mcp-proto", Opts::default());
    let mut m = Mcp::start(&h, 600);
    let tools = m.request("tools/list", json!({}));
    let names: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "gpu_submit",
            "gpu_status",
            "gpu_logs",
            "gpu_cancel",
            "gpu_run_queue"
        ]
    );
    assert_eq!(m.request("ping", json!({}))["result"], json!({}));
    let r = m.request(
        "tools/call",
        json!({"name": "gpu_wait", "arguments": {"id": 1}}),
    );
    assert_eq!(r["error"]["code"], -32602, "no wait tool: {r}");
    // The cap travels into the schema.
    assert_eq!(
        tools["result"]["tools"][0]["inputSchema"]["properties"]["timeout_secs"]["maximum"],
        600
    );
}

#[test]
fn queue_run_reports_how_the_jobs_went() {
    let h = Harness::start("mcp-flow", Opts::default());
    serve_on(&h);
    let mut m = Mcp::start(&h, 600);

    // Nothing queued: refused.
    let e = m.err("gpu_run_queue", json!({}));
    assert!(e.contains("This session has not queued any job"), "{e}");

    let text = m.ok(
        "gpu_submit",
        json!({"command": ["sh", "-c", "echo job-ran-here; echo second line"],
               "mode": "exclusive", "timeout_secs": 60, "category": "train",
               "cwd": h.dir.display().to_string()}),
    );
    let id = job_id(&text);
    assert!(text.contains("exclusive, train"), "{text}");
    assert!(text.contains("gpu_run_queue"), "{text}");
    assert!(text.contains("waiting"), "{text}");
    assert!(
        text.contains(&format!(
            "`gpu-run wait {id} --report` now as a background Bash task"
        )),
        "{text}"
    );

    // While the model serves, the job sits behind it, and every call returns.
    let status = m.ok("gpu_status", json!({}));
    assert!(status.contains("SERVING"), "{status}");
    assert!(status.contains("waiting for gpu_run_queue"), "{status}");
    let detail = m.ok("gpu_status", json!({"id": id}));
    assert!(detail.contains("waiting"), "{detail}");
    let logs = m.ok("gpu_logs", json!({"id": id}));
    assert!(logs.contains("not started"), "{logs}");
    assert_eq!(h.word(), "SERVING");

    // It returns once the job has ended, with how it went.
    let out = m.ok("gpu_run_queue", json!({}));
    assert!(
        out.contains("All 1 of your job(s) finished successfully"),
        "{out}"
    );
    assert!(
        out.contains(&format!(
            "- Job {id} (sh -c echo job-ran-here; echo second line): done, exit 0, ran "
        )),
        "{out}"
    );
    assert_eq!(h.job(Who::Cont, id)["state"], "done");

    // The model returns on its own.
    h.wait_word("SERVING");
    let logs = m.ok("gpu_logs", json!({"id": id}));
    assert!(logs.contains("job-ran-here\nsecond line"), "{logs}");
    let status = m.ok("gpu_status", json!({}));
    assert!(status.contains(&format!("{id}: done (exit 0)")), "{status}");
    assert_eq!(
        h.job(Who::Cont, id)["cwd"].as_str().unwrap(),
        h.dir.display().to_string()
    );
    // Run again, as a second agent would after the first ran the queue.
    let e = m.err("gpu_run_queue", json!({}));
    assert!(e.contains("did not switch the GPU"), "{e}");
    assert!(
        e.contains(&format!(
            "- Job {id} (sh -c echo job-ran-here; echo second line): done, exit 0"
        )),
        "{e}"
    );
    assert!(e.contains("force: true"), "{e}");
}

#[test]
fn run_queue_refuses_what_it_cannot_switch() {
    let h = Harness::start("mcp-refuse", Opts::default());
    h.wait_word("OFF");
    let mut m = Mcp::start(&h, 600);
    // Serving is off: the job starts by itself, there is no model to stop.
    let id = m.submit(&["sleep", "30"], 60);
    h.wait_job_state(Who::Cont, id, "running");
    let e = m.err("gpu_run_queue", json!({}));
    assert!(e.contains("did not switch the GPU"), "{e}");
    let out = m.ok("gpu_cancel", json!({"id": id}));
    assert!(!out.is_empty());
    h.wait_job_state(Who::Cont, id, "cancelled");
}

#[test]
fn cancel_a_queued_job() {
    let h = Harness::start("mcp-cancel", Opts::default());
    serve_on(&h);
    let mut m = Mcp::start(&h, 600);
    let id = m.submit(&["sleep", "30"], 60);
    m.ok("gpu_cancel", json!({"id": id}));
    h.wait_job_state(Who::Cont, id, "cancelled");
    let detail = m.ok("gpu_status", json!({"id": id}));
    assert!(detail.contains("cancelled"), "{detail}");
    assert!(m
        .err("gpu_status", json!({"id": 999}))
        .contains("no job 999"));
    assert!(m.err("gpu_cancel", json!({"id": 999})).contains("999"));
    // Cancelling it left the model where it was.
    assert_eq!(h.word(), "SERVING");
}

#[test]
fn the_timeout_is_required_and_capped() {
    let h = Harness::start("mcp-cap", Opts::default());
    let mut m = Mcp::start(&h, 120);
    for (args, needle) in [
        (
            json!({"command": ["true"], "mode": "exclusive"}),
            "timeout_secs is required",
        ),
        (
            json!({"command": ["true"], "mode": "exclusive", "timeout_secs": 121}),
            "1..=120",
        ),
        (
            json!({"command": ["true"], "mode": "shared", "timeout_secs": 5}),
            "VRAM",
        ),
        (
            json!({"command": ["true"], "mode": "exclusive", "timeout_secs": 5, "vram": "1G"}),
            "shared only",
        ),
    ] {
        let e = m.err("gpu_submit", args.clone());
        assert!(e.contains(needle), "{args}: {e}");
    }
    // Nothing reached the line.
    assert!(h.status(Who::Cont)["jobs"].as_array().unwrap().is_empty());
    // A shared job that names its VRAM is fine.
    let text = m.ok(
        "gpu_submit",
        json!({"command": ["true"], "mode": "shared", "vram": "1G", "timeout_secs": 5}),
    );
    let id = job_id(&text);
    h.wait_job_state(Who::Cont, id, "done");
}

#[test]
fn logs_return_the_tail() {
    let h = Harness::start("mcp-tail", Opts::default());
    h.wait_word("OFF");
    let mut m = Mcp::start(&h, 600);
    let id = m.submit(&["seq", "1", "300"], 30);
    h.wait_job_state(Who::Cont, id, "done");
    let five = m.ok("gpu_logs", json!({"id": id, "tail": 5}));
    assert!(
        five.ends_with("[earlier output omitted]\n296\n297\n298\n299\n300"),
        "{five}"
    );
    let default = m.ok("gpu_logs", json!({"id": id}));
    assert!(
        default.contains("\n201\n") && !default.contains("\n200\n"),
        "{default}"
    );
    assert!(default.ends_with("300"), "{default}");
    let all = m.ok("gpu_logs", json!({"id": id, "tail": 2000}));
    assert!(
        all.contains("\n1\n2\n3\n") && !all.contains("omitted"),
        "{all}"
    );
}

#[test]
fn a_dead_gpu_switch_is_an_error_not_a_hang() {
    let mut h = Harness::start("mcp-down", Opts::default());
    let mut m = Mcp::start(&h, 600);
    h.stop_switch();
    for (tool, args) in [
        ("gpu_status", json!({})),
        ("gpu_run_queue", json!({})),
        ("gpu_cancel", json!({"id": 1})),
        ("gpu_logs", json!({"id": 1})),
    ] {
        let e = m.err(tool, args);
        assert!(e.contains("cannot reach gpu-switch"), "{tool}: {e}");
    }
    let e = m.err(
        "gpu_submit",
        json!({"command": ["true"], "mode": "exclusive", "timeout_secs": 5}),
    );
    assert!(e.contains("gpu-switch"), "{e}");
}

#[test]
fn the_run_time_limit_stops_a_job() {
    let h = Harness::start("mcp-limit", Opts::default());
    h.wait_word("OFF");
    let mut m = Mcp::start(&h, 600);
    let started = Instant::now();
    let command = h.hold("--ms 120000 --mem 1 --sm 50");
    let id = m.submit(&["sh", "-c", &command], 1);
    let job = wait_ended(&h, id);
    assert!(started.elapsed() < Duration::from_secs(15));
    assert_eq!(job["signal"], 15, "{job:#}");
    let logs = m.ok("gpu_logs", json!({"id": id}));
    assert!(logs.contains("run-time limit of 1s reached"), "{logs}");
    assert!(!logs.contains("SIGKILL"), "{logs}");
}

#[test]
fn a_job_that_ignores_sigterm_is_killed_after_the_grace_period() {
    let h = Harness::start("cli-limit", Opts::default());
    h.wait_word("OFF");
    let id = h.submit(
        Who::Cont,
        &["--exclusive", "--timeout", "1"],
        &[
            "sh",
            "-c",
            &format!(
                "trap '' TERM; {}; while :; do sleep 1; done",
                h.hold("--ms 120000 --mem 1 --sm 50")
            ),
        ],
    );
    let started = Instant::now();
    let job = wait_ended(&h, id);
    let took = started.elapsed();
    assert_eq!(job["signal"], 9, "{job:#}");
    assert!(
        took >= Duration::from_secs(10) && took < Duration::from_secs(25),
        "{took:?}"
    );
    let log = std::fs::read_to_string(job["log"].as_str().unwrap()).unwrap();
    assert!(log.contains("run-time limit of 1s reached"), "{log}");
    assert!(log.contains("sending SIGKILL"), "{log}");
    // A limit of nothing is refused up front.
    let out = h.run(
        Who::Cont,
        &["submit", "--exclusive", "--timeout", "0", "--", "true"],
    );
    assert!(!out.status.success());
}

#[test]
fn a_job_inside_its_limit_is_left_alone() {
    let h = Harness::start("cli-inside", Opts::default());
    h.wait_word("OFF");
    let id = h.submit(
        Who::Cont,
        &["--exclusive", "--timeout", "30"],
        &[
            "sh",
            "-c",
            &format!("{}; echo fine", h.hold("--ms 1000 --sm 50")),
        ],
    );
    assert_eq!(h.wait_job(Who::Cont, id), 0);
    let job = h.job(Who::Cont, id);
    assert_eq!(job["state"], "done");
    let log = std::fs::read_to_string(job["log"].as_str().unwrap()).unwrap();
    assert_eq!(log, "fine\n");
}

fn wait_ended(h: &Harness, id: u64) -> Value {
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let j = h.job(Who::Cont, id);
        if matches!(
            j["state"].as_str(),
            Some("done" | "failed" | "killed" | "cancelled")
        ) {
            return j;
        }
        assert!(Instant::now() < deadline, "job {id} never ended: {j:#}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ---- the hook, as its real binary ----

fn hook(h: &Harness, input: &str) -> (bool, String) {
    let mut child = h
        .gpu_run(Who::Cont)
        .arg("hook")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    (out.status.success(), String::from_utf8(out.stdout).unwrap())
}

#[test]
fn the_hook_turns_the_cli_away_in_the_foreground_only() {
    // The hook needs no gpu-switch at all.
    let h = Harness::start(
        "hook",
        Opts {
            arbiter: false,
            ..Opts::default()
        },
    );
    let call = |tool: &str, command: &str, background: Option<bool>| {
        let mut ti = json!({"command": command});
        if let Some(b) = background {
            ti["run_in_background"] = json!(b);
        }
        json!({"session_id": "s", "hook_event_name": "PreToolUse",
               "tool_name": tool, "tool_input": ti})
        .to_string()
    };
    let (ok, out) = hook(&h, &call("Bash", "gpu-run wait 3", None));
    assert!(ok);
    let v: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(v["hookSpecificOutput"]["permissionDecision"], "deny");
    assert_eq!(
        v["hookSpecificOutput"]["permissionDecisionReason"],
        "Please use the MCP tool, otherwise you can deadlock yourself."
    );
    for (tool, command, background) in [
        ("Bash", "gpu-run wait 3", Some(true)),
        ("Bash", "ls -la", None),
        ("Bash", "echo gpu-run", Some(false)),
        ("Read", "gpu-run wait 3", None),
    ] {
        let (ok, out) = hook(&h, &call(tool, command, background));
        assert!(ok);
        assert_eq!(out, "", "{tool} {command}");
    }
    for (command, denied) in [
        ("cd /x && nix run .#gpu-run -- wait 1", true),
        ("bash -c 'gpu-run logs 1 -f'", true),
        ("echo $(gpu-run status)", true),
        ("grep -rn gpu-run docs", false),
    ] {
        let (_, out) = hook(&h, &call("Bash", command, None));
        assert_eq!(out.contains("deny"), denied, "{command}: {out}");
    }
    // Garbage in, nothing out, success: the call is not blocked by a broken hook.
    for junk in ["", "nope", "{}"] {
        let (ok, out) = hook(&h, junk);
        assert!(ok);
        assert_eq!(out, "");
    }
}

#[test]
fn queue_run_reports_failures_with_their_logs() {
    let h = Harness::start("mcp-fail", Opts::default());
    serve_on(&h);
    let mut m = Mcp::start(&h, 600);
    let good = job_id(&m.ok(
        "gpu_submit",
        json!({"command": ["true"], "mode": "exclusive", "timeout_secs": 60}),
    ));
    let bad = job_id(&m.ok(
        "gpu_submit",
        json!({"command": ["sh", "-c", "echo before-the-crash; exit 3"],
               "mode": "exclusive", "timeout_secs": 60}),
    ));
    let out = m.ok("gpu_run_queue", json!({}));
    assert!(
        out.contains("Of your 2 job(s): 1 finished successfully, 1 did not."),
        "{out}"
    );
    assert!(
        out.contains(&format!("- Job {good} (true): done, exit 0")),
        "{out}"
    );
    assert!(
        out.contains(&format!(
            "- Job {bad} (sh -c echo before-the-crash; exit 3): FAILED, exit 3"
        )),
        "{out}"
    );
    assert!(out.contains("    before-the-crash"), "{out}");
    h.wait_word("SERVING");
}

#[test]
fn queue_run_stops_waiting_when_the_model_takes_the_gpu_back() {
    let h = Harness::start("mcp-held", Opts::default());
    serve_on(&h);
    let mut m = Mcp::start(&h, 600);
    let command = h.hold("--ms 3000 --mem 1 --sm 50");
    let first = job_id(&m.ok(
        "gpu_submit",
        json!({"command": ["sh", "-c", &command], "mode": "exclusive", "timeout_secs": 60}),
    ));
    let second = job_id(&m.ok(
        "gpu_submit",
        json!({"command": ["true"], "mode": "exclusive", "timeout_secs": 60}),
    ));
    let out = std::thread::scope(|s| {
        s.spawn(|| {
            h.wait_job_state(Who::Cont, first, "running");
            h.run_ok(Who::Host, &["switch-ninfer"]);
        });
        m.ok("gpu_run_queue", json!({}))
    });
    assert!(out.contains("1 did not get to run"), "{out}");
    assert!(out.contains(&format!("- Job {first} (sh -c ")), "{out}");
    assert!(
        out.contains(&format!("- Job {second} (true): still queued")),
        "{out}"
    );
    assert_eq!(h.job(Who::Cont, second)["state"], "queued");
}

#[test]
fn a_second_agent_running_the_queue_too_is_told_its_jobs_already_ran() {
    let h = Harness::start("mcp-race", Opts::default());
    serve_on(&h);
    let mut a = Mcp::start(&h, 600);
    let mut b = Mcp::start(&h, 600);
    let ja = a.submit(&["true"], 60);
    let jb = b.submit(&["sh", "-c", "exit 4"], 60);
    // Both agreed that one runs the queue; a does, and runs b's job with its own.
    let out = a.ok("gpu_run_queue", json!({}));
    assert!(out.contains(&format!("- Job {ja} (true): done")), "{out}");
    assert!(
        !out.contains(&format!("Job {jb} ")),
        "a reports its own jobs only: {out}"
    );
    h.wait_word("SERVING");
    // b's call arrives after the run: no new switch, and how its job went.
    let e = b.err("gpu_run_queue", json!({}));
    assert!(e.contains("did not switch the GPU"), "{e}");
    assert!(
        e.contains(&format!("- Job {jb} (sh -c exit 4): FAILED, exit 4")),
        "{e}"
    );
    assert_eq!(h.word(), "SERVING");
    // force switches even so, but only when anything waits at all.
    let e = b.err("gpu_run_queue", json!({"force": true}));
    assert!(e.contains("Nothing waits behind the local model"), "{e}");
    assert_eq!(h.word(), "SERVING");
}

#[test]
fn wait_report_is_the_monitor_of_an_agent_that_did_not_switch() {
    let h = Harness::start("mcp-monitor", Opts::default());
    serve_on(&h);
    let mut m = Mcp::start(&h, 600);
    let id = m.submit(&["sh", "-c", "echo the-last-words; exit 2"], 60);
    let mut waiter = h
        .gpu_run(Who::Cont)
        .args(["wait", &id.to_string(), "--report"])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        waiter.try_wait().unwrap().is_none(),
        "waits while the job waits"
    );
    h.run_ok(Who::Host, &["switch-gpu"]);
    let out = waiter.wait_with_output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(
        text.contains(&format!(
            "- Job {id} (sh -c echo the-last-words; exit 2): FAILED, exit 2"
        )),
        "{text}"
    );
    assert!(text.contains("    the-last-words"), "{text}");
}
