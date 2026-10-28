//! The joint queue: ownership, line order, priorities, privacy, policing.

mod common;

use std::time::Duration;

use common::*;
use serde_json::{json, Value};

fn order(h: &Harness) -> Vec<String> {
    std::fs::read_to_string(h.dir.join("order"))
        .unwrap_or_default()
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

fn mark(label: &str, then: &str) -> Vec<String> {
    vec![
        "sh".into(),
        "-c".into(),
        format!("echo {label} >> order; {then}"),
    ]
}

fn submit_cmd(h: &Harness, who: Who, spec: &[&str], cmd: &[String]) -> u64 {
    let refs: Vec<&str> = cmd.iter().map(String::as_str).collect();
    h.submit(who, spec, &refs)
}

/// An exclusive job that computes until cancelled, holding the line still.
fn blocker(h: &Harness, who: Who) -> u64 {
    let cmd = h.hold("--sm 50");
    let id = h.submit(who, EXCL, &["sh", "-c", &format!("exec {cmd}")]);
    h.wait_job_state(who, id, "running");
    id
}

fn line_kinds(s: &Value) -> Vec<String> {
    s["line"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            format!(
                "{}:{}",
                e["kind"].as_str().unwrap(),
                e["job"].as_str().unwrap_or("-")
            )
        })
        .collect()
}

#[test]
fn off_by_default_and_jobs_run_without_switch() {
    let h = Harness::start("off", Opts::default());
    h.wait_word("OFF");
    let id = h.submit(Who::Host, EXCL, &["true"]);
    assert_eq!(h.wait_job(Who::Host, id), 0);
    let out = h.run_ok(Who::Host, &["switch-gpu"]);
    assert!(out.contains("no switch needed"), "{out}");
    assert!(h.starts().is_empty());
    h.run_ok(Who::Host, &["serve", "on"]);
    h.wait_word("SERVING");
    assert_eq!(h.starts().len(), 1);
}

#[test]
fn submit_requires_a_mode() {
    let h = Harness::start("mode", Opts::default());
    for (args, needle) in [
        (vec!["submit", "--", "true"], "names its mode"),
        (vec!["submit", "--shared", "--", "true"], "--vram SIZE"),
        (
            vec!["submit", "--exclusive", "--vram", "1G", "--", "true"],
            "--shared only",
        ),
        (
            vec!["submit", "--shared", "--vram", "12X", "--", "true"],
            "not a VRAM size",
        ),
        (
            vec!["submit", "--exclusive", "--cooperative", "--", "true"],
            "--cooperative needs --shared",
        ),
        (
            vec!["submit", "--exclusive", "--priority", "11", "--", "true"],
            "priority",
        ),
        (
            vec!["submit", "--exclusive", "--category", "nap", "--", "true"],
            "unknown category",
        ),
    ] {
        let out = h.run(Who::Host, &args);
        assert!(!out.status.success(), "{args:?}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains(needle), "{args:?}: {err}");
    }
}

#[test]
fn jobs_wait_behind_ninfer_until_switch_then_ninfer_returns() {
    let h = Harness::start("switch", Opts::default());
    h.run_ok(Who::Host, &["serve", "on"]);
    h.wait_word("SERVING");
    let out = h.run(Who::Host, &["switch-gpu"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("nothing queued"));

    let a = submit_cmd(&h, Who::Host, EXCL, &mark("A", "true"));
    let b = submit_cmd(
        &h,
        Who::Cont,
        &["--shared", "--vram", "1G"],
        &mark("B", "true"),
    );
    let s = h.status(Who::Host);
    assert!(
        s["summary"]
            .as_str()
            .unwrap()
            .contains("NOT running until 'gpu-run switch-gpu'"),
        "{s:#}"
    );
    assert_eq!(s["own"]["waiting"], 1);
    assert_eq!(s["others"]["waiting"], 1);
    assert!(s["jobs"][0]["behind_ninfer"].as_bool().unwrap());
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        order(&h).is_empty(),
        "nothing runs while NInfer owns the GPU"
    );
    let text = h.run_ok(Who::Host, &["status"]);
    assert!(text.contains("waiting"), "{text}");

    h.run_ok(Who::Cont, &["switch-gpu"]);
    assert_eq!(h.wait_job(Who::Host, a), 0);
    assert_eq!(h.wait_job(Who::Cont, b), 0);
    assert_eq!(order(&h), ["A", "B"], "arrival order across sides");
    h.wait_word("SERVING");
    assert_eq!(h.starts().len(), 2);
}

#[test]
fn caller_env_cwd_umask_and_private_lock() {
    let h = Harness::start("env", Opts::default());
    let work = h.dir.join("work dir");
    std::fs::create_dir_all(&work).unwrap();
    let id = h.submit_with(
        Who::Cont,
        &["--shared", "--vram", "1536M"],
        &[
            "sh",
            "-c",
            "env > env.txt; pwd > pwd.txt; umask > umask.txt",
        ],
        |c| {
            c.current_dir(&work).env("GA_TEST_MARKER", "xyz 123");
            with_umask(c, 0o027);
        },
    );
    assert_eq!(h.wait_job(Who::Cont, id), 0);
    let env = std::fs::read_to_string(work.join("env.txt")).unwrap();
    let get = |k: &str| {
        env.lines()
            .find_map(|l| l.strip_prefix(&format!("{k}=")))
            .unwrap_or_else(|| panic!("{k} missing: {env}"))
            .to_string()
    };
    assert_eq!(get("GA_TEST_MARKER"), "xyz 123");
    let state = h.dir.join("state-cont");
    let lock = state.join(format!("gpu-run/{id}/ml-gpu.lock"));
    assert_eq!(get("ML_GPU_LOCK"), lock.display().to_string());
    assert_eq!(get("ML_GPU_LEASE_MODE"), "shared");
    assert_eq!(get("ML_GPU_VRAM"), (1536u64 << 20).to_string());
    assert!(!env.contains("GPU_RUN_SPEC"));
    assert!(
        !lock.parent().unwrap().exists(),
        "private lock dir removed after the job"
    );
    let pwd = std::fs::read_to_string(work.join("pwd.txt")).unwrap();
    assert_eq!(
        pwd.trim(),
        work.canonicalize().unwrap().display().to_string()
    );
    assert_eq!(
        std::fs::read_to_string(work.join("umask.txt"))
            .unwrap()
            .trim(),
        "0027"
    );
    let j = h.job(Who::Cont, id);
    assert_eq!(
        j["log"],
        state
            .join(format!("gpu-run/{id}.log"))
            .display()
            .to_string()
    );
    assert_eq!(j["category"], "other");
}

#[test]
fn shared_jobs_share_and_exclusive_waits() {
    let h = Harness::start("shared", Opts::default());
    let hold = h.hold("--ms 1500 --mem 100 --sm 50");
    let a = h.submit(
        Who::Host,
        &["--shared", "--vram", "1G"],
        &["sh", "-c", &hold],
    );
    let b = h.submit(
        Who::Cont,
        &["--shared", "--vram", "1G"],
        &["sh", "-c", &hold],
    );
    h.wait_job_state(Who::Host, a, "running");
    h.wait_job_state(Who::Cont, b, "running");
    let e = submit_cmd(&h, Who::Host, EXCL, &mark("E", "true"));
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        h.job(Who::Host, e)["state"],
        "queued",
        "exclusive waits for the shared holders"
    );
    assert_eq!(h.job(Who::Host, a)["state"], "running");
    assert_eq!(h.wait_job(Who::Host, e), 0);
    assert_eq!(h.job(Who::Host, a)["state"], "done");
    assert_eq!(h.job(Who::Cont, b)["state"], "done");
}

#[test]
fn both_command_modes_register_their_expected_gpu_duration() {
    let h = Harness::start("command-estimates", Opts::default());
    for mode in [vec!["--exclusive"], vec!["--shared", "--vram", "1G"]] {
        let mut spec = mode;
        spec.extend(["--expected-secs", "12"]);
        let command = h.hold("--ms 150 --mem 100 --sm 50");
        let id = h.submit(Who::Host, &spec, &["sh", "-c", &command]);
        assert_eq!(h.job(Who::Host, id)["expected_runtime_secs"], 12);
        assert_eq!(h.wait_job(Who::Host, id), 0);
    }
}

#[test]
fn cli_estimate_warning_is_on_stderr_and_explicit_unknown_suppresses_it() {
    let h = Harness::start("cli-estimate-warning", Opts::default());
    for mode in [vec!["--exclusive"], vec!["--shared", "--vram", "1G"]] {
        for estimate in [None, Some("-1"), Some("60")] {
            let mut args = vec!["submit"];
            args.extend(mode.clone());
            if let Some(value) = estimate {
                args.extend(["--expected-secs", value]);
            }
            args.extend([
                "--", MOCK, "gpu-hold", "--mem", "1", "--sm", "50", "--ms", "100",
            ]);
            let result = h.run(Who::Host, &args);
            assert!(result.status.success(), "{:?}", result);
            let stdout = String::from_utf8(result.stdout).unwrap();
            let stderr = String::from_utf8(result.stderr).unwrap();
            let id: u64 = stdout
                .trim()
                .parse()
                .expect("stdout remains only the job id");
            assert_eq!(
                stderr.contains("warning: GPU duration estimate omitted"),
                estimate.is_none(),
                "{stderr}"
            );
            assert_eq!(h.wait_job(Who::Host, id), 0);
        }
    }
}

#[test]
fn room_rule_counts_max_of_declared_and_actual() {
    let h = Harness::start(
        "room",
        Opts {
            gpu_total_mib: Some(10000),
            ..Opts::default()
        },
    );
    // Declared 6000, using 2000: occupies 6000; 4000 left.
    let hold = h.hold("--mem 2000 --sm 50");
    let a = h.submit(
        Who::Host,
        &["--shared", "--vram", "6000M"],
        &["sh", "-c", &format!("exec {hold}")],
    );
    h.wait_job_state(Who::Host, a, "running");
    let fits = h.submit(Who::Cont, &["--shared", "--vram", "4000M"], &["true"]);
    assert_eq!(h.wait_job(Who::Cont, fits), 0, "4000M fits beside it");
    let big = h.submit(Who::Cont, &["--shared", "--vram", "4001M"], &["true"]);
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(h.job(Who::Cont, big)["state"], "queued", "4001M does not");
    h.run_ok(Who::Host, &["cancel", &a.to_string()]);
    assert_eq!(
        h.wait_job(Who::Cont, big),
        0,
        "admitted once the card is free"
    );
}

#[test]
fn line_positions_and_fifo_across_sides() {
    let h = Harness::start("fifo", Opts::default());
    let blk = blocker(&h, Who::Host);
    let n1 = submit_cmd(&h, Who::Cont, EXCL, &mark("n1", "true"));
    let s2 = submit_cmd(
        &h,
        Who::Host,
        &["--shared", "--vram", "1G"],
        &mark("s2", "true"),
    );
    let n2 = submit_cmd(&h, Who::Cont, EXCL, &mark("n2", "true"));
    let host = h.status(Who::Host);
    assert_eq!(line_kinds(&host), ["other:n1", "own:2", "other:n2"]);
    assert_eq!(host["jobs"][1]["position"], 2);
    assert_eq!(host["other_jobs"][0]["position"], 1);
    let cont = h.status(Who::Cont);
    let s = host_prefix();
    assert_eq!(
        line_kinds(&cont),
        ["own:1", &format!("other:{s}2"), "own:2"]
    );
    h.run_ok(Who::Host, &["cancel", &blk.to_string()]);
    for (who, id) in [(Who::Cont, n1), (Who::Host, s2), (Who::Cont, n2)] {
        assert_eq!(h.wait_job(who, id), 0);
    }
    assert_eq!(order(&h), ["n1", "s2", "n2"]);
}

#[test]
fn priority_passes_lower_within_its_budget() {
    let h = Harness::start("prio", Opts::default());
    let blk = blocker(&h, Who::Host);
    let a = submit_cmd(&h, Who::Cont, EXCL, &mark("a0", "true"));
    let b = submit_cmd(&h, Who::Host, EXCL, &mark("b0", "true"));
    let c = submit_cmd(&h, Who::Cont, EXCL, &mark("c0", "true"));
    let d = submit_cmd(
        &h,
        Who::Host,
        &["--exclusive", "--priority", "1"],
        &mark("d1", "true"),
    );
    let p = submit_cmd(
        &h,
        Who::Cont,
        &["--exclusive", "--priority", "2"],
        &mark("p2", "true"),
    );
    let hp = host_prefix();
    // d1 passed c0 (its one level-0 pass). p2 passes c0 and b0 (two level-0
    // passes) and d1 (one level-1 pass); never the first ticket, a0.
    let s = h.status(Who::Cont);
    assert_eq!(
        line_kinds(&s),
        [
            "own:1",
            "own:3",
            &format!("other:{hp}2"),
            &format!("other:{hp}3"),
            "own:2"
        ]
    );
    assert_eq!(s["line"][1]["priority"], 2);
    let host = h.status(Who::Host);
    let other: Vec<&Value> = host["other_jobs"].as_array().unwrap().iter().collect();
    assert!(other
        .iter()
        .any(|j| j["handle"] == "n3" && j["priority"] == 2));
    h.run_ok(Who::Host, &["cancel", &blk.to_string()]);
    for (who, id) in [
        (Who::Cont, a),
        (Who::Cont, p),
        (Who::Host, b),
        (Who::Host, d),
        (Who::Cont, c),
    ] {
        assert_eq!(h.wait_job(who, id), 0);
    }
    assert_eq!(order(&h), ["a0", "p2", "b0", "d1", "c0"]);
}

#[test]
fn cancel_queued_and_running() {
    let h = Harness::start("cancel", Opts::default());
    let a = h.submit(Who::Host, EXCL, &["sleep", "30"]);
    let b = h.submit(Who::Host, EXCL, &["sleep", "30"]);
    h.wait_job_state(Who::Host, a, "running");
    let runner = h.job(Who::Host, b)["runner_pid"].as_i64().unwrap() as i32;
    h.run_ok(Who::Host, &["cancel", &b.to_string()]);
    assert_eq!(h.job(Who::Host, b)["state"], "cancelled");
    wait_until("queued runner exits", Duration::from_secs(5), || {
        !alive(runner)
    });
    wait_until("job pid", Duration::from_secs(5), || {
        !h.job(Who::Host, a)["pid"].is_null()
    });
    let pid = h.job(Who::Host, a)["pid"].as_i64().unwrap() as i32;
    h.run_ok(Who::Host, &["cancel", &a.to_string()]);
    assert_eq!(h.wait_job(Who::Host, a), 128 + libc::SIGTERM);
    assert_eq!(h.job(Who::Host, a)["state"], "cancelled");
    wait_until("job killed", Duration::from_secs(5), || !alive(pid));
    let out = h.run(Who::Cont, &["cancel", &a.to_string()]);
    assert!(
        !out.status.success(),
        "ids are per side: the container has no job {a}"
    );
}

#[test]
fn host_kills_container_job_by_handle() {
    let h = Harness::start(
        "kill",
        Opts {
            switch: json!({"sigint_to_sigterm_secs": 0.3, "sigterm_to_sigkill_secs": 0.3}),
            ..Opts::default()
        },
    );
    let hold = h.hold("--sm 50 --ignore-int --ignore-term");
    let n = h.submit(Who::Cont, EXCL, &["sh", "-c", &format!("exec {hold}")]);
    h.wait_job_state(Who::Cont, n, "running");
    // Its handlers are installed once it shows on the card.
    wait_until("job on the card", Duration::from_secs(5), || {
        !h.gpu_pids().is_empty()
    });
    let out = h.run(Who::Cont, &["kill", &format!("{}1", host_prefix())]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("only the host side"));
    let out = h.run_ok(Who::Host, &["kill", &format!("n{n}")]);
    assert!(out.contains("SIGINT"), "{out}");
    assert_eq!(h.wait_job(Who::Cont, n), 128 + libc::SIGKILL);
    let j = h.job(Who::Cont, n);
    assert_eq!(j["state"], "killed");
    let user = gpu_arbiter::procs::username(unsafe { libc::getuid() }).unwrap();
    assert_eq!(j["error"], format!("killed by {user}"));
    let host = h.run_ok(Who::Host, &["status", "--json"]);
    assert!(
        !host.contains("killed by"),
        "kill reasons stay with the owner"
    );
}

#[test]
fn runner_death_frees_the_slot() {
    let h = Harness::start("runner-death", Opts::default());
    let blk = blocker(&h, Who::Host);
    let q = h.submit(Who::Cont, EXCL, &["sleep", "30"]);
    let runner = h.job(Who::Cont, q)["runner_pid"].as_i64().unwrap() as i32;
    signal(runner, libc::SIGKILL);
    let j = h.wait_job_state(Who::Cont, q, "failed");
    assert!(j["error"]
        .as_str()
        .unwrap()
        .contains("before the job started"));
    assert!(
        h.status(Who::Cont)["line"].as_array().unwrap().is_empty(),
        "its ticket left the line"
    );
    wait_until("blocker on the card", Duration::from_secs(5), || {
        !h.gpu_pids().is_empty()
    });
    let runner = h.job(Who::Host, blk)["runner_pid"].as_i64().unwrap() as i32;
    signal(runner, libc::SIGKILL);
    let j = h.wait_job_state(Who::Host, blk, "killed");
    assert!(j["error"]
        .as_str()
        .unwrap()
        .contains("VRAM remained allocated"));
    let next = h.submit(Who::Cont, EXCL, &["true"]);
    // A lost runner cannot leave GPU work behind after its grant is released.
    assert!(h.gpu_pids().is_empty());
    assert_eq!(h.wait_job(Who::Cont, next), 0);
}

#[test]
fn wait_exit_code_and_logs() {
    let h = Harness::start("logs", Opts::default());
    let a = h.submit(
        Who::Host,
        EXCL,
        &["sh", "-c", "echo out; echo err >&2; exit 7"],
    );
    assert_eq!(h.wait_job(Who::Host, a), 7);
    let j = h.job(Who::Host, a);
    assert_eq!(j["state"], "failed");
    let log = h.run_ok(Who::Host, &["logs", &a.to_string()]);
    assert!(log.contains("out\n") && log.contains("err\n"), "{log:?}");
    let b = h.submit(
        Who::Host,
        EXCL,
        &[
            "sh",
            "-c",
            &format!("echo one; {}; echo two", h.hold("--ms 1100 --sm 50")),
        ],
    );
    h.wait_job_state(Who::Host, b, "running");
    assert_eq!(
        h.run_ok(Who::Host, &["logs", &b.to_string(), "-f"]),
        "one\ntwo\n"
    );
    assert!(!h.run(Who::Cont, &["logs", &a.to_string()]).status.success());
}

const SECRET: &str = "SECRET-MARKER";

#[test]
fn other_side_sees_only_the_allowed_fields() {
    let h = Harness::start("privacy", Opts::default());
    let work = h.dir.join(format!("{SECRET}-cwd"));
    std::fs::create_dir_all(&work).unwrap();
    let hold = h.hold("--sm 50");
    let run = h.submit_with(
        Who::Host,
        &[
            "--shared",
            "--vram",
            "2G",
            "--category",
            "train",
            "--cooperative",
            "--priority",
            "3",
        ],
        &["sh", "-c", &format!("exec {hold} # {SECRET}")],
        |c| {
            c.current_dir(&work).env("SECRET_ENV", SECRET);
        },
    );
    h.wait_job_state(Who::Host, run, "running");
    let blk = h.submit(Who::Host, EXCL, &["true", SECRET]);
    let cont = h.run_ok(Who::Cont, &["status", "--json"]);
    assert!(!cont.contains(SECRET), "{cont}");
    assert!(!cont.contains("train"), "categories are private: {cont}");
    assert!(
        !cont.contains("state-host"),
        "log paths are private: {cont}"
    );
    let s: Value = serde_json::from_str(&cont).unwrap();
    assert!(s["jobs"].as_array().unwrap().is_empty());
    for j in s["other_jobs"].as_array().unwrap() {
        let mut keys: Vec<&String> = j.as_object().unwrap().keys().collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                "cooperative",
                "handle",
                "mode",
                "position",
                "priority",
                "state",
                "vram"
            ]
        );
    }
    let first = &s["other_jobs"][0];
    assert_eq!(first["handle"], format!("{}{run}", host_prefix()));
    assert_eq!(first["mode"], "shared");
    assert_eq!(first["vram"], 2u64 << 30);
    assert_eq!(first["cooperative"], true);
    assert_eq!(first["priority"], 3);
    assert_eq!(s["others"]["running"], 1);
    assert_eq!(s["others"]["queued"], 1);
    let text = h.run_ok(Who::Cont, &["status"]);
    assert!(!text.contains(SECRET), "{text}");
    // The owner sees everything.
    let own = h.run_ok(Who::Host, &["status", "--json"]);
    assert!(own.contains(SECRET) && own.contains("train"));
    // Serve output names phases and profiles only.
    for out in [
        h.run_ok(Who::Cont, &["serve", "status"]),
        h.run_ok(Who::Cont, &["serve", "on"]),
        h.run_ok(Who::Host, &["serve", "status"]),
        h.run_ok(Who::Host, &["serve", "off"]),
    ] {
        assert!(!out.contains(SECRET), "{out}");
        assert!(
            !out.replace("dflash2", "")
                .chars()
                .any(|c| c.is_ascii_digit()),
            "{out}"
        );
    }
    let _ = blk;
}

fn policing() -> Opts {
    Opts {
        switch: json!({
            "startup_idle_secs": 0.5,
            "idle_secs": 0.5,
            "sigint_to_sigterm_secs": 0.3,
            "sigterm_to_sigkill_secs": 0.3
        }),
        ..Opts::default()
    }
}

#[test]
fn startup_rule_terminates_a_job_that_never_allocates() {
    let h = Harness::start("startup", policing());
    let hold = h.hold("--mem 0 --sm 0");
    let id = h.submit(Who::Cont, EXCL, &["sh", "-c", &format!("exec {hold}")]);
    assert_eq!(h.wait_job(Who::Cont, id), 128 + libc::SIGINT);
    let j = h.job(Who::Cont, id);
    assert_eq!(j["state"], "killed");
    assert_eq!(
        j["error"],
        "killed: no GPU VRAM allocation within 0.5 s of grant"
    );
    // A different shared job's compute does not satisfy this job's deadline.
    let busy = h.hold("--ms 1500 --sm 50");
    let idle = h.hold("--ms 1500 --mem 0 --sm 0");
    let a = h.submit(
        Who::Host,
        &["--shared", "--vram", "1G"],
        &["sh", "-c", &busy],
    );
    let b = h.submit(
        Who::Host,
        &["--shared", "--vram", "1G"],
        &["sh", "-c", &idle],
    );
    assert_eq!(h.wait_job(Who::Host, b), 128 + libc::SIGINT);
    assert_eq!(h.wait_job(Who::Host, a), 0);
}

#[test]
fn watchdog_escalation_is_permanent_even_if_compute_resumes() {
    let h = Harness::start("idle", policing());
    let stuck = h.hold("--sm 50 --sm-ms 300 --ignore-int --ignore-term");
    let id = h.submit(Who::Host, EXCL, &["sh", "-c", &format!("exec {stuck}")]);
    assert_eq!(h.wait_job(Who::Host, id), 128 + libc::SIGKILL);
    let j = h.job(Who::Host, id);
    assert_eq!(j["state"], "killed");
    assert_eq!(j["error"], "killed, compute idle >=50%");
    let wakes = h.hold("--ms 12000 --sm 50 --sm-ms 300 --busy-on-int");
    let id = h.submit(Who::Host, EXCL, &["sh", "-c", &format!("exec {wakes}")]);
    assert_eq!(
        h.wait_job(Who::Host, id),
        128 + libc::SIGTERM,
        "resuming compute must not erase a permanent watchdog failure"
    );
    let j = h.job(Who::Host, id);
    assert_eq!(j["state"], "killed");
    assert_eq!(j["terminal_status"], "low_compute_duty");
}

#[test]
fn cooperative_job_yields_to_an_exclusive_waiter_and_requeues() {
    let h = Harness::start(
        "coop",
        Opts {
            switch: json!({"coop_min_hold_secs": 0.3}),
            ..Opts::default()
        },
    );
    let coop = h.hold("--sm 50 --mem 100 --exit-on-yield");
    let c = h.submit(
        Who::Cont,
        &["--shared", "--vram", "1G", "--cooperative"],
        &[
            "sh",
            "-c",
            &format!("echo run >> {}/runs; exec {coop}", h.dir.display()),
        ],
    );
    h.wait_job_state(Who::Cont, c, "running");
    let e = submit_cmd(&h, Who::Host, EXCL, &mark("E", "true"));
    assert_eq!(h.wait_job(Who::Host, e), 0, "the yield freed the card");
    wait_until("second run", Duration::from_secs(10), || {
        std::fs::read_to_string(h.dir.join("runs"))
            .unwrap_or_default()
            .lines()
            .count()
            == 2
    });
    let j = h.wait_job_state(Who::Cont, c, "running");
    assert_eq!(j["runs"], 2);
    h.run_ok(Who::Cont, &["cancel", &c.to_string()]);
}

#[test]
fn ninfer_failure_releases_the_gpu_and_retries() {
    let h = Harness::start(
        "failed",
        Opts {
            model_present: false,
            ..Opts::default()
        },
    );
    h.run_ok(Who::Host, &["serve", "on"]);
    wait_until("FAILED", Duration::from_secs(10), || h.word() == "FAILED");
    let cont = h.status(Who::Cont);
    assert!(
        cont["ninfer_error"]
            .as_str()
            .unwrap()
            .contains("model file missing"),
        "{cont}"
    );
    let id = h.submit(Who::Host, EXCL, &["true"]);
    assert_eq!(
        h.wait_job(Who::Host, id),
        0,
        "a failed NInfer does not hold the GPU"
    );
    std::fs::write(&h.model, b"weights").unwrap();
    h.wait_word("SERVING");
    h.run_ok(Who::Host, &["serve", "off"]);
    h.wait_word("OFF");
}

#[test]
fn profile_and_host_cache_pending_versus_apply_now() {
    let h = Harness::start("profile", Opts::default());
    let msg = h.run_ok(Who::Host, &["set-profile", "dflash2", "--apply-now"]);
    assert!(msg.contains("not serving"), "{msg}");
    assert_eq!(
        h.run_ok(Who::Host, &["serve", "status"]),
        "OFF\nprofile: - current, dflash2 pending (next NInfer start)\n"
    );
    h.run_ok(Who::Host, &["set-profile", "mtp"]);
    h.run_ok(Who::Host, &["serve", "on"]);
    h.wait_word("SERVING");
    let has = |argv: &[String], a: &str, b: &str| argv.windows(2).any(|w| w[0] == a && w[1] == b);
    assert!(has(&h.starts()[0], "--spec", "mtp"));
    let msg = h.run_ok(Who::Cont, &["set-profile", "dflash2"]);
    assert!(msg.contains("pending"), "{msg}");
    let s = h.status(Who::Cont);
    assert_eq!(s["serve"]["profile_current"], "mtp");
    assert_eq!(s["serve"]["profile_pending"], "dflash2");
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(h.starts().len(), 1);
    let out = h.run(Who::Host, &["set-profile", "nope"]);
    assert!(String::from_utf8_lossy(&out.stderr).contains("unknown profile"));
    h.run_ok(Who::Host, &["set-profile", "dflash2", "--apply-now"]);
    wait_until("restart", Duration::from_secs(10), || h.starts().len() == 2);
    assert!(has(&h.starts()[1], "--spec", "dflash2"));
    h.wait_word("SERVING");
    let s = h.status(Who::Host);
    assert_eq!(s["serve"]["profile_current"], "dflash2");
    assert!(s["serve"]["profile_pending"].is_null());
    h.run_ok(Who::Host, &["set-host-cache", "2048"]);
    assert_eq!(h.status(Who::Host)["host_cache_pending_mib"], 2048);
}

#[test]
fn serve_on_during_a_run_starts_after_it() {
    let h = Harness::start("on-mid-run", Opts::default());
    let id = h.submit(Who::Host, EXCL, &["sleep", "1"]);
    h.wait_job_state(Who::Host, id, "running");
    let out = h.run_ok(Who::Host, &["serve", "on"]);
    assert!(
        out.contains("once the queue is empty (RUNNING_JOBS)"),
        "{out}"
    );
    assert!(h.starts().is_empty());
    assert_eq!(h.wait_job(Who::Host, id), 0);
    h.wait_word("SERVING");
    assert_eq!(h.starts().len(), 1);
}

#[test]
fn serve_off_while_serving_stops_ninfer() {
    let h = Harness::start("serve-off", Opts::default());
    h.run_ok(Who::Cont, &["serve", "on"]);
    h.wait_word("SERVING");
    let pid = h.ninfer_pid().unwrap();
    let out = h.run_ok(Who::Host, &["serve", "off"]);
    assert!(out.starts_with("serving off: stopping NInfer"), "{out}");
    h.wait_word("OFF");
    assert!(!alive(pid));
    assert!(h
        .run_ok(Who::Host, &["serve", "off"])
        .starts_with("serving already off"));
    let id = h.submit(Who::Cont, EXCL, &["true"]);
    assert_eq!(h.wait_job(Who::Cont, id), 0);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(h.word(), "OFF");
    assert_eq!(h.starts().len(), 1, "never started again while off");
}

fn complete(h: &Harness, who: Option<Who>, line: &[&str]) -> Vec<String> {
    let mut c = std::process::Command::new(GPU_RUN);
    c.env_remove("GPU_SWITCH_SOCKET");
    if let Some(w) = who {
        c.env("GPU_SWITCH_SOCKET", h.sock(w));
    }
    let out = c
        .arg("__complete")
        .arg((line.len() - 1).to_string())
        .args(line)
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(out.stderr.is_empty(), "Tab must stay quiet");
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

#[test]
fn completion_offers_own_ids_and_other_handles() {
    let h = Harness::start("complete", Opts::default());
    let blk = blocker(&h, Who::Host);
    let queued = h.submit(Who::Host, EXCL, &["true"]);
    let done = h.submit(Who::Cont, EXCL, &["sleep", "30"]);
    let ids = complete(&h, Some(Who::Host), &["gpu-run", "cancel", ""]);
    assert_eq!(ids, [blk.to_string(), queued.to_string()]);
    let handles = complete(&h, Some(Who::Host), &["gpu-run", "kill", ""]);
    assert_eq!(handles, [format!("n{done}")]);
    let theirs = complete(&h, Some(Who::Cont), &["gpu-run", "wait", ""]);
    assert_eq!(theirs, [done.to_string()]);
    let host = h.host_sock.display().to_string();
    let via_flag = complete(&h, None, &["gpu-run", "--socket", &host, "kill", "n"]);
    assert_eq!(via_flag, [format!("n{done}")]);
    let profiles = complete(&h, Some(Who::Host), &["gpu-run", "set-profile", ""]);
    assert_eq!(profiles, ["mtp", "dflash2", "--apply-now"]);
    let script = h.run_ok(Who::Host, &["completions", "bash"]);
    assert!(script.contains("complete -o default -F _gpu_run gpu-run"));
    let gone = h.dir.join("nothing.sock").display().to_string();
    assert!(complete(&h, None, &["gpu-run", "--socket", &gone, "kill", ""]).is_empty());
}

#[test]
fn switch_ninfer_lets_resident_jobs_finish_and_keeps_the_line() {
    let h = Harness::start("to-ninfer", Opts::default());
    let resident = h.hold("--ms 1500 --sm 50");
    let r = h.submit(Who::Host, EXCL, &["sh", "-c", &format!("exec {resident}")]);
    h.wait_job_state(Who::Host, r, "running");
    let a = submit_cmd(&h, Who::Cont, EXCL, &mark("A", "true"));
    let b = submit_cmd(
        &h,
        Who::Host,
        &["--shared", "--vram", "1G"],
        &mark("B", "true"),
    );
    let c = submit_cmd(&h, Who::Cont, EXCL, &mark("C", "true"));
    let before = line_kinds(&h.status(Who::Host));
    let out = h.run_ok(Who::Cont, &["switch-ninfer"]);
    assert!(out.starts_with("serving on; switching to NInfer"), "{out}");
    wait_until("switching", Duration::from_secs(5), || {
        h.word() == "SWITCHING_TO_NINFER"
    });
    let s = h.status(Who::Host);
    assert!(
        s["summary"]
            .as_str()
            .unwrap()
            .contains("waiting for 1 GPU-resident job(s) to finish; 3 queued, kept in order"),
        "{s:#}"
    );
    assert_eq!(h.wait_job(Who::Host, r), 0);
    h.wait_word("SERVING");
    assert!(
        order(&h).is_empty(),
        "nothing waiting was admitted before NInfer"
    );
    let host = h.status(Who::Host);
    assert_eq!(line_kinds(&host), before, "waiting order unchanged");
    assert_eq!(host["others"]["waiting"], 2);
    let again = h.run_ok(Who::Host, &["switch-ninfer"]);
    assert!(again.starts_with("NInfer already holds the GPU"), "{again}");
    h.run_ok(Who::Host, &["switch-gpu"]);
    for (who, id) in [(Who::Cont, a), (Who::Host, b), (Who::Cont, c)] {
        assert_eq!(h.wait_job(who, id), 0);
    }
    assert_eq!(order(&h), ["A", "B", "C"]);
    h.wait_word("SERVING");
}

#[test]
fn a_departure_is_sampled_at_once_not_on_the_next_tick() {
    let h = Harness::start(
        "departure",
        Opts {
            switch: json!({"gpu_poll_interval_secs": 60.0}),
            ..Opts::default()
        },
    );
    let hold = h.hold("--ms 1500 --mem 100 --sm 50");
    let first = submit_cmd(&h, Who::Host, EXCL, &mark("A", &format!("exec {hold}")));
    h.wait_job_state(Who::Host, first, "running");
    let second = submit_cmd(&h, Who::Host, EXCL, &mark("B", "true"));
    assert_eq!(h.wait_job(Who::Host, first), 0);
    let left = std::time::Instant::now();
    assert_eq!(h.wait_job(Who::Host, second), 0);
    let gap = left.elapsed();
    assert_eq!(order(&h), ["A", "B"]);
    assert!(
        gap < Duration::from_millis(1500),
        "the successor waited {gap:?}, a poll interval is 60 s"
    );
}

fn queued_positions(h: &Harness) -> Vec<(u64, u64)> {
    h.status(Who::Host)["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|j| j["state"] == "queued")
        .map(|j| (j["id"].as_u64().unwrap(), j["position"].as_u64().unwrap()))
        .collect()
}

#[test]
fn a_restart_keeps_running_and_queued_jobs_in_order() {
    let mut h = Harness::start("restart-kept", Opts::default());
    let hold = h.hold("--ms 2500 --sm 50");
    let running = submit_cmd(&h, Who::Host, EXCL, &mark("A", &format!("exec {hold}")));
    h.wait_job_state(Who::Host, running, "running");
    let queued = submit_cmd(&h, Who::Host, EXCL, &mark("B", "true"));
    assert_eq!(queued_positions(&h), [(queued, 1)]);
    h.stop_switch();
    h.start_switch();
    wait_until("both resumed", Duration::from_secs(10), || {
        h.log("switch.log").matches("resumed by pid").count() >= 2
    });
    assert_eq!(h.job(Who::Host, running)["state"], "running");
    assert_eq!(queued_positions(&h), [(queued, 1)]);
    assert_eq!(h.wait_job(Who::Host, running), 0);
    assert_eq!(h.wait_job(Who::Host, queued), 0);
    assert_eq!(order(&h), ["A", "B"]);
}

#[test]
fn lost_state_restores_queue_order_without_inventing_a_running_grant() {
    let mut h = Harness::start("restart-lost", Opts::default());
    let hold = h.hold("--ms 3000 --sm 50");
    let running = submit_cmd(&h, Who::Host, EXCL, &mark("A", &format!("exec {hold}")));
    h.wait_job_state(Who::Host, running, "running");
    submit_cmd(&h, Who::Host, EXCL, &mark("B", "true"));
    h.stop_switch();
    std::fs::remove_file(h.dir.join("switch-state/state.json")).unwrap();
    h.start_switch();
    wait_until(
        "queued job registered again",
        Duration::from_secs(10),
        || h.log("switch.log").contains("registered again"),
    );
    submit_cmd(&h, Who::Host, EXCL, &mark("C", "true"));
    wait_until("all ran", Duration::from_secs(20), || order(&h).len() == 3);
    assert_eq!(order(&h), ["A", "B", "C"], "B kept its place ahead of C");
}

#[test]
fn a_client_cannot_claim_a_running_grant_after_lost_state() {
    use std::io::{BufRead, BufReader, Write};
    let h = Harness::start("forged-running", Opts::default());
    let _blocker = blocker(&h, Who::Host);
    let mut stream = std::os::unix::net::UnixStream::connect(h.sock(Who::Cont)).unwrap();
    writeln!(
        stream,
        "{}",
        json!({"op": "lease", "mode": "exclusive", "resume": {
            "id": 9, "token": "00".repeat(32), "key": {"class": 0, "ns": 1}, "running": true
        }})
    )
    .unwrap();
    let mut line = String::new();
    BufReader::new(&stream).read_line(&mut line).unwrap();
    let reply: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(reply["ok"], false, "{reply}");
    assert!(reply["error"]
        .as_str()
        .unwrap()
        .contains("retained daemon state"));
    assert_eq!(h.status(Who::Host)["others"]["running"], 0);
}

#[test]
fn a_forged_place_is_not_honoured() {
    use std::io::{BufRead, BufReader, Write};
    let h = Harness::start("forged", Opts::default());
    let mut stream = std::os::unix::net::UnixStream::connect(h.sock(Who::Cont)).unwrap();
    let resume =
        json!({"id": 9, "token": "00".repeat(32), "key": {"class": 0, "ns": 1}, "running": false});
    writeln!(
        stream,
        "{}",
        json!({"op": "lease", "mode": "exclusive", "resume": resume})
    )
    .unwrap();
    let mut reply = String::new();
    BufReader::new(&stream).read_line(&mut reply).unwrap();
    let reply: Value = serde_json::from_str(&reply).unwrap();
    assert_eq!(reply["ok"], true);
    let key = &reply["job"]["resume"]["key"];
    assert_eq!(key["class"], 1, "{reply}");
    assert!(key["ns"].as_u64().unwrap() > 1, "{reply}");
}

#[test]
fn a_proven_place_survives_lost_state() {
    use std::io::{BufRead, BufReader, Write};
    let mut h = Harness::start("proven", Opts::default());
    let _blocker_id = blocker(&h, Who::Host);
    let socket = h.sock(Who::Cont).to_path_buf();
    let lease = |resume: Option<&Value>| {
        let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
        let mut request = json!({"op": "lease", "mode": "exclusive", "label": "waiter"});
        if let Some(r) = resume {
            request["resume"] = r.clone();
        }
        writeln!(stream, "{request}").unwrap();
        let mut reply = String::new();
        BufReader::new(&stream).read_line(&mut reply).unwrap();
        (stream, serde_json::from_str::<Value>(&reply).unwrap())
    };
    let (waiting, first) = lease(None);
    let resume = first["job"]["resume"].clone();
    assert_eq!(resume["running"], false);
    h.stop_switch();
    drop(waiting);
    std::fs::remove_file(h.dir.join("switch-state/state.json")).unwrap();
    h.start_switch();
    h.run_ok(Who::Host, &["serve", "on"]);
    h.wait_word("SERVING");
    let (_again, second) = lease(Some(&resume));
    assert_eq!(second["ok"], true, "{second}");
    assert_eq!(second["job"]["resume"]["key"], resume["key"], "{second}");
    h.run_ok(Who::Cont, &["cancel", &second["job"]["id"].to_string()]);
}

#[test]
fn status_names_commands_and_nonzero_exit_are_distinct() {
    let h = Harness::start("named-status", Opts::default());
    let id = h.submit(
        Who::Host,
        &["--exclusive", "--name", "example execution"],
        &["sh", "-c", "exit 7 # private-command-text"],
    );
    assert_eq!(h.wait_job(Who::Host, id), 7);
    let job = h.job(Who::Host, id);
    assert_eq!(job["state"], "failed"); // Historical clients retain this lifecycle.
    assert_eq!(job["terminal_status"], "non_zero_exit");
    assert_eq!(job["label"], "example execution");
    let table = h.run_ok(Who::Host, &["status"]);
    assert!(table.contains("example execution"));
    assert!(table.contains("QUEUED"));
    assert!(table.contains("non-zero exit code"));
    assert!(!table.contains("private-command-text"));
    let detail = h.run_ok(Who::Host, &["status", &id.to_string()]);
    assert!(!detail.contains("private-command-text"));
    let time = detail
        .lines()
        .find_map(|line| line.strip_prefix("queued: "))
        .unwrap();
    assert_eq!(time.len(), 8);
    assert_eq!(time.as_bytes()[2], b':');
    assert_eq!(time.as_bytes()[5], b':');
    let explicit = h.run_ok(Who::Host, &["status", &id.to_string(), "--command"]);
    assert!(explicit.contains("private-command-text"));
    let explicit = h.run_ok(Who::Host, &["status", "--command"]);
    assert!(explicit.contains("private-command-text"));
    let id = h.submit(Who::Host, EXCL, &["true"]);
    assert_eq!(h.wait_job(Who::Host, id), 0);
    assert!(h.job(Who::Host, id)["label"]
        .as_str()
        .unwrap()
        .starts_with("execution-"));
}
