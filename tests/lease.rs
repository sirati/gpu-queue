//! Lease clients: processes holding a grant for themselves, in the same
//! line as gpu-run jobs. Driven through `ninfer-mock lease-client`.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use common::*;
use serde_json::{json, Value};

struct Client {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: mpsc::Receiver<String>,
    seen: Vec<String>,
}

impl Client {
    fn start(h: &Harness, who: Who, args: &[&str]) -> Client {
        let mut child = Command::new(MOCK)
            .arg("lease-client")
            // Ordinary fixtures represent GPU users. Deadline tests override these.
            .args(["--mem", "1", "--sm", "50"])
            .args(args)
            .env("GPU_SWITCH_SOCKET", h.sock(who))
            .env("NINFER_MOCK_GPU_DIR", &h.gpu_dir)
            .env_remove("GPU_SWITCH_GRANTED")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let out = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for l in BufReader::new(out).lines().map_while(Result::ok) {
                let _ = tx.send(l);
            }
        });
        let stdin = child.stdin.take();
        Client {
            child,
            stdin,
            lines: rx,
            seen: Vec::new(),
        }
    }

    fn pid(&self) -> i32 {
        self.child.id() as i32
    }

    /// The first line (seen or new) satisfying `f`.
    fn find(&mut self, timeout: Duration, f: impl Fn(&str) -> bool) -> Option<String> {
        if let Some(l) = self.seen.iter().find(|l| f(l)) {
            return Some(l.clone());
        }
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(left) {
                Ok(l) => {
                    self.seen.push(l.clone());
                    if f(&l) {
                        return Some(l);
                    }
                }
                Err(_) => return None,
            }
        }
    }

    fn json(&mut self, timeout: Duration, key: &str) -> Option<Value> {
        self.find(timeout, |l| {
            serde_json::from_str::<Value>(l).is_ok_and(|v| v.get(key).is_some())
        })
        .map(|l| serde_json::from_str(&l).unwrap())
    }

    fn granted(&mut self, timeout: Duration) -> bool {
        self.find(timeout, |l| l == "granted").is_some()
    }

    fn release(&mut self) {
        if let Some(mut s) = self.stdin.take() {
            let _ = writeln!(s);
        }
        assert!(self
            .find(Duration::from_secs(10), |l| l == "released")
            .is_some());
        let _ = self.child.wait();
    }

    fn exited(&mut self) -> std::process::ExitStatus {
        self.child.wait().unwrap()
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

const T: Duration = Duration::from_secs(10);

#[test]
fn estimates_warn_only_when_omitted_and_accept_explicit_unknown_in_both_modes() {
    let h = Harness::start("estimate-warnings", Opts::default());
    for mode in [vec!["--exclusive"], vec!["--shared", "--vram", "1048576"]] {
        for estimate in [None, Some("-1"), Some("60"), Some("0"), Some("-2")] {
            let mut args = mode.clone();
            if let Some(value) = estimate {
                args.extend(["--expected-secs", value]);
            }
            let mut client = Client::start(&h, Who::Host, &args);
            let reply = client.json(T, "ok").unwrap();
            if matches!(estimate, Some("0" | "-2")) {
                assert_eq!(reply["ok"], false, "{reply}");
                assert!(!client.exited().success());
                continue;
            }
            assert_eq!(reply["ok"], true, "{reply}");
            assert_eq!(reply["warning"].is_string(), estimate.is_none(), "{reply}");
            assert!(client.granted(T));
            client.release();
        }
    }
}

#[test]
fn status_reports_independent_job_timings_instead_of_age() {
    let h = Harness::start("status-timing", Opts::default());
    let mut client = Client::start(&h, Who::Host, &["--exclusive", "--expected-secs", "60"]);
    assert!(client.granted(T));
    std::thread::sleep(Duration::from_millis(200));
    let claim = h.gpu_dir.join(client.pid().to_string());
    std::fs::write(&claim, "mem=1 sm=0 type=C").unwrap();
    std::thread::sleep(Duration::from_millis(300));
    std::fs::write(&claim, "mem=0 sm=0 type=C").unwrap();
    std::thread::sleep(Duration::from_millis(300));
    let job = h.job(Who::Host, 1);
    assert_eq!(job["expected_runtime_secs"], 60);
    assert!(
        job["timing"]["gpu_seconds"].as_f64().unwrap() >= 0.7,
        "{job}"
    );
    assert!(
        job["timing"]["no_vram_seconds"].as_f64().unwrap() > 0.05,
        "{job}"
    );
    assert!(
        job["timing"]["no_utilisation_seconds"].as_f64().unwrap() > 0.05,
        "{job}"
    );
    let output = h.run_ok(Who::Host, &["status"]);
    assert!(!output.contains("AGE"));
    assert!(output.contains("PREP") && output.contains("NO-VRAM") && output.contains("NO-UTIL"));
    client.release();
    let done = h.wait_job_state(Who::Host, 1, "done");
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(done["timing"], h.job(Who::Host, 1)["timing"]);
}

#[test]
fn fitting_shared_job_backfills_past_shared_and_exclusive_waiters_within_longest_remaining_estimate(
) {
    let h = Harness::start(
        "backfill-horizon",
        Opts {
            gpu_total_mib: Some(32768),
            ..Opts::default()
        },
    );
    let mut first = Client::start(
        &h,
        Who::Host,
        &[
            "--shared",
            "--vram",
            &(27_u64 << 30).to_string(),
            "--expected-secs",
            "60",
        ],
    );
    assert!(first.granted(T));
    let mut second = Client::start(
        &h,
        Who::Host,
        &[
            "--shared",
            "--vram",
            &(1_u64 << 30).to_string(),
            "--expected-secs",
            "120",
        ],
    );
    assert!(second.granted(T));
    let mut large = Client::start(
        &h,
        Who::Cont,
        &[
            "--shared",
            "--vram",
            &(13_u64 << 30).to_string(),
            "--expected-secs",
            "30",
        ],
    );
    large.json(T, "waiting").expect("large job blocked on VRAM");
    let mut exclusive = Client::start(&h, Who::Cont, &["--exclusive", "--expected-secs", "60"]);
    exclusive
        .json(T, "waiting")
        .expect("exclusive waits for the GPU");
    let mut long = Client::start(
        &h,
        Who::Cont,
        &[
            "--shared",
            "--vram",
            &(1_u64 << 30).to_string(),
            "--expected-secs",
            "180",
        ],
    );
    let wait = long.json(T, "waiting").unwrap();
    assert!(
        wait["waiting"]
            .as_str()
            .unwrap()
            .contains("expected runtime"),
        "{wait}"
    );
    let mut small = Client::start(
        &h,
        Who::Cont,
        &[
            "--shared",
            "--vram",
            &(1536_u64 << 20).to_string(),
            "--expected-secs",
            "90",
        ],
    );
    assert!(
        small.granted(T),
        "must fit the longest remaining estimate, not the shortest"
    );
    assert!(!large.granted(Duration::from_millis(50)));
    assert!(!exclusive.granted(Duration::from_millis(50)));
    assert!(!long.granted(Duration::from_millis(50)));
    small.release();
    drop(long);
    drop(exclusive);
    drop(large);
    second.release();
    first.release();
}

#[test]
fn expired_or_unknown_estimates_do_not_authorise_backfill() {
    for estimate in ["1", "0", "-1"] {
        let h = Harness::start(
            "backfill-no-horizon",
            Opts {
                gpu_total_mib: Some(32768),
                ..Opts::default()
            },
        );
        let mut args = vec!["--shared", "--vram", "28991029248"];
        if estimate != "0" {
            args.extend(["--expected-secs", estimate]);
        }
        let mut running = Client::start(&h, Who::Host, &args);
        assert!(running.granted(T));
        if estimate == "1" {
            std::thread::sleep(Duration::from_millis(1100));
        }
        let mut exclusive = Client::start(&h, Who::Cont, &["--exclusive", "--expected-secs", "1"]);
        exclusive.json(T, "waiting").unwrap();
        let mut small = Client::start(
            &h,
            Who::Cont,
            &["--shared", "--vram", "1048576", "--expected-secs", "1"],
        );
        let waiting = small.json(T, "waiting").unwrap();
        assert!(waiting["waiting"]
            .as_str()
            .unwrap()
            .contains("expected runtime"));
        assert!(!small.granted(Duration::from_millis(100)));
        drop(small);
        drop(exclusive);
        running.release();
    }
}

#[test]
fn expired_reattachment_reclaims_live_gpu_work_before_next_exclusive() {
    let mut h = Harness::start(
        "expired-reattachment",
        Opts {
            switch: json!({"reattach_grace_secs": 0.2}),
            ..Opts::default()
        },
    );
    let mut abandoned = Client::start(&h, Who::Host, &["--exclusive"]);
    assert!(abandoned.granted(T));
    wait_until("allocation visible", T, || !h.gpu_pids().is_empty());
    h.stop_switch();
    h.start_switch();
    let mut next = Client::start(&h, Who::Cont, &["--exclusive"]);
    assert!(next.granted(T));
    assert!(!gpu_arbiter::procs::alive(abandoned.pid() as u32));
    let reason = h.wait_job_state(Who::Host, 1, "failed");
    assert!(reason["error"]
        .as_str()
        .unwrap()
        .contains("did not come back"));
    next.release();
}

#[test]
fn native_shared_policy_requests_cleanup_for_budget_and_sole_holder_turn() {
    for (name, policy, waiter, expected) in [
        (
            "native-budget",
            json!({"shared_budget_secs": 0.5}),
            false,
            "budget",
        ),
        (
            "native-turn",
            json!({"shared_turn_secs": 0.3}),
            true,
            "turn",
        ),
    ] {
        let mut config = json!({"coop_min_hold_secs": 1000});
        config
            .as_object_mut()
            .unwrap()
            .extend(policy.as_object().unwrap().clone());
        let h = Harness::start(
            name,
            Opts {
                switch: config,
                ..Opts::default()
            },
        );
        let mut shared = Client::start(
            &h,
            Who::Host,
            &[
                "--shared",
                "--vram",
                "1048576",
                "--cooperative",
                "--shutdown-grace",
                "0.1",
            ],
        );
        assert!(shared.granted(T));
        let mut exclusive = waiter.then(|| Client::start(&h, Who::Cont, &["--exclusive"]));
        let shutdown = shared
            .json(T, "shutdown")
            .expect("Rust fairness requested cleanup");
        assert!(
            shutdown["shutdown"]["reason"]
                .as_str()
                .unwrap()
                .contains(expected),
            "{shutdown}"
        );
        assert_eq!(shutdown["shutdown"]["permanent"], false);
        shared.release();
        if let Some(exclusive) = &mut exclusive {
            assert!(exclusive.granted(T));
            exclusive.release();
        }
    }
}

fn own_jobs(h: &Harness, who: Who) -> Vec<Value> {
    h.status(who)["jobs"].as_array().unwrap().clone()
}

#[test]
fn grant_on_connect_then_release() {
    let h = Harness::start("lease-grant", Opts::default());
    let mut c = Client::start(&h, Who::Host, &["--exclusive", "--label", "train-x"]);
    let ok = c.json(T, "ok").unwrap();
    assert_eq!(ok["ok"], true);
    assert_eq!(ok["lease_protocol"], 2);
    assert_eq!(ok["job"]["id"], 1);
    assert!(ok["job"]["handle"].as_str().unwrap().ends_with('1'));
    let g = c.json(T, "granted").unwrap();
    assert_eq!(g, json!({"granted": true, "lease_protocol": 2}));
    assert!(c.granted(T));
    let jobs = own_jobs(&h, Who::Host);
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["lease"], true);
    assert_eq!(jobs[0]["label"], "train-x");
    assert_eq!(jobs[0]["state"], "running");
    assert_eq!(jobs[0]["pid"], c.pid());
    c.release();
    h.wait_job_state(Who::Host, 1, "done");
}

#[test]
fn refused_request_carries_the_protocol_marker() {
    let h = Harness::start("lease-bad", Opts::default());
    let mut c = Client::start(&h, Who::Cont, &["--shared"]);
    let r = c.json(T, "ok").unwrap();
    assert_eq!(r["ok"], false);
    assert_eq!(r["lease_protocol"], 2);
    assert!(r["error"].as_str().unwrap().contains("--vram"));
    assert!(!c.exited().success());
}

#[test]
fn waits_behind_ninfer_until_switch_gpu() {
    let h = Harness::start("lease-ninfer", Opts::default());
    h.run_ok(Who::Host, &["serve", "on"]);
    h.wait_word("SERVING");
    let mut c = Client::start(
        &h,
        Who::Cont,
        &["--shared", "--vram", &(1u64 << 30).to_string()],
    );
    let w = c.json(T, "waiting").unwrap();
    assert_eq!(
        w["waiting"],
        "NInfer holds the GPU; NOT running until 'gpu-run switch-gpu'"
    );
    assert_eq!(w["lease_protocol"], 2);
    assert!(!c.granted(Duration::from_millis(500)));
    assert_eq!(h.status(Who::Host)["others"]["waiting"], 1);
    h.run_ok(Who::Host, &["switch-gpu"]);
    assert!(c.granted(T));
    assert_eq!(h.word(), "RUNNING_JOBS");
    c.release();
    h.wait_word("SERVING");
    assert_eq!(h.starts().len(), 2);
}

#[test]
fn shared_room_by_max_of_declared_and_observed() {
    let h = Harness::start(
        "lease-room",
        Opts {
            gpu_total_mib: Some(10000),
            ..Opts::default()
        },
    );
    let m = |mib: u64| (mib << 20).to_string();
    let mut a = Client::start(
        &h,
        Who::Host,
        &[
            "--shared",
            "--vram",
            &m(6000),
            "--mem",
            "2000",
            "--sm",
            "50",
        ],
    );
    assert!(a.granted(T));
    wait_until("on the card", T, || !h.gpu_pids().is_empty());
    std::thread::sleep(Duration::from_millis(200));
    let mut fits = Client::start(
        &h,
        Who::Cont,
        &["--shared", "--vram", &m(4000), "--hold-ms", "10"],
    );
    assert!(fits.granted(T), "10000 - max(6000, 2000) = 4000 fits");
    assert!(fits.exited().success());
    h.wait_job_state(Who::Cont, 1, "done");
    let mut big = Client::start(&h, Who::Cont, &["--shared", "--vram", &m(4001)]);
    let w = big
        .find(T, |l| l.contains("\"waiting\"") && l.contains("VRAM"))
        .expect("waits for room");
    assert!(
        w.contains("4001 MiB of VRAM beside 1 job(s): 4000 MiB available"),
        "{w}"
    );
    assert!(!big.granted(Duration::from_millis(400)));
    a.release();
    assert!(big.granted(T));
    big.release();
}

#[test]
fn dropped_connection_frees_the_grant() {
    let h = Harness::start("lease-drop", Opts::default());
    let mut a = Client::start(&h, Who::Host, &["--exclusive"]);
    assert!(a.granted(T));
    let mut b = Client::start(&h, Who::Cont, &["--exclusive"]);
    let w = b.json(T, "waiting").unwrap();
    assert!(
        w["waiting"].as_str().unwrap().contains("job(s) on the GPU"),
        "{w}"
    );
    signal(a.pid(), libc::SIGKILL);
    assert!(b.granted(T), "a dead client's grant is gone");
    h.wait_job_state(Who::Host, 1, "done");
    b.release();
}

#[test]
fn signals_go_to_the_client() {
    let h = Harness::start(
        "lease-signal",
        Opts {
            switch: json!({"startup_idle_secs": 0.5, "sigint_to_sigterm_secs": 0.3, "sigterm_to_sigkill_secs": 0.3}),
            ..Opts::default()
        },
    );
    // Cross-side kill: the client delivers SIGINT to itself and dies.
    let mut c = Client::start(&h, Who::Cont, &["--exclusive", "--sm", "50"]);
    assert!(c.granted(T));
    wait_until("on the card", T, || !h.gpu_pids().is_empty());
    let handle = own_jobs(&h, Who::Cont)[0]["handle"]
        .as_str()
        .unwrap()
        .to_string();
    h.run_ok(Who::Host, &["kill", &handle]);
    let s = c.json(T, "signal").unwrap();
    assert_eq!(s, json!({"signal": "INT", "lease_protocol": 2}));
    let st = c.exited();
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&st),
        Some(libc::SIGINT)
    );
    let j = h.wait_job_state(Who::Cont, 1, "killed");
    assert!(j["error"].as_str().unwrap().starts_with("killed by "));
}

#[test]
fn allocation_deadline_kills_lease_clients_in_both_modes() {
    for mode in ["exclusive", "shared"] {
        let h = Harness::start(&format!("lease-startup-{mode}"), Opts::default());
        let args = match mode {
            "shared" => vec![
                "--shared",
                "--vram",
                "1073741824",
                "--mem",
                "0",
                "--sm",
                "0",
            ],
            _ => vec!["--exclusive", "--mem", "0", "--sm", "0"],
        };
        let mut c = Client::start(&h, Who::Host, &args);
        assert!(c.granted(T));
        let started = Instant::now();
        assert_eq!(
            std::os::unix::process::ExitStatusExt::signal(&c.exited()),
            Some(libc::SIGKILL)
        );
        assert!(started.elapsed() < Duration::from_secs(2));
        let j = h.wait_job_state(Who::Host, 1, "killed");
        assert_eq!(
            j["error"],
            "killed: no GPU VRAM allocation within 1 s of grant"
        );
    }
}

#[test]
fn allocation_deadline_is_not_suppressed_by_another_shared_holder() {
    let h = Harness::start("lease-startup-shared", Opts::default());
    let mut busy = Client::start(&h, Who::Host, &["--shared", "--vram", "1073741824"]);
    assert!(busy.granted(T));
    let mut empty = Client::start(
        &h,
        Who::Host,
        &[
            "--shared",
            "--vram",
            "1073741824",
            "--mem",
            "0",
            "--sm",
            "0",
        ],
    );
    assert!(empty.granted(T));
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&empty.exited()),
        Some(libc::SIGKILL)
    );
    h.wait_job_state(Who::Host, 2, "killed");
    assert_eq!(h.job(Who::Host, 1)["state"], "running");
    busy.release();
}

#[test]
fn allocated_lease_clients_are_policed_for_idle_compute_in_both_modes() {
    for mode in ["exclusive", "shared"] {
        let h = Harness::start(
            &format!("lease-idle-{mode}"),
            Opts {
                switch: json!({"idle_secs": 0.3, "sigint_to_sigterm_secs": 0.3,
                           "sigterm_to_sigkill_secs": 0.3}),
                ..Opts::default()
            },
        );
        let args = match mode {
            "shared" => vec![
                "--shared",
                "--vram",
                "1073741824",
                "--mem",
                "100",
                "--sm",
                "0",
                "--ignore-int",
            ],
            _ => vec!["--exclusive", "--mem", "100", "--sm", "0", "--ignore-int"],
        };
        let mut c = Client::start(&h, Who::Host, &args);
        assert!(c.granted(T));
        assert_eq!(c.json(T, "signal").unwrap()["signal"], "INT");
        assert_eq!(
            std::os::unix::process::ExitStatusExt::signal(&c.exited()),
            Some(libc::SIGTERM)
        );
        let j = h.wait_job_state(Who::Host, 1, "killed");
        assert!(j["error"].as_str().unwrap().starts_with("killed: idle"));
    }
}

#[test]
fn a_client_inside_a_gpu_run_job_takes_no_grant() {
    let h = Harness::start("lease-nested", Opts::default());
    let id = h.submit(
        Who::Host,
        EXCL,
        &[
            "sh",
            "-c",
            &format!("{MOCK} lease-client --exclusive --hold-ms 200"),
        ],
    );
    assert_eq!(
        h.wait_job(Who::Host, id),
        0,
        "it would deadlock behind its own job otherwise"
    );
    let log = h.run_ok(Who::Host, &["logs", &id.to_string()]);
    assert_eq!(log.trim(), format!("nested {id}"));
    assert_eq!(own_jobs(&h, Who::Host).len(), 1, "no lease job was created");
}

#[test]
fn other_side_sees_a_lease_like_any_job() {
    let h = Harness::start("lease-cross", Opts::default());
    let two_g = (2u64 << 30).to_string();
    let mut c = Client::start(
        &h,
        Who::Host,
        &["--shared", "--vram", &two_g, "--label", "SECRET-LABEL"],
    );
    assert!(c.granted(T));
    let cont = h.run_ok(Who::Cont, &["status", "--json"]);
    assert!(
        !cont.contains("SECRET-LABEL"),
        "labels stay with the owner: {cont}"
    );
    let s: Value = serde_json::from_str(&cont).unwrap();
    assert_eq!(s["others"]["running"], 1);
    assert!(s["jobs"].as_array().unwrap().is_empty());
    let o = &s["other_jobs"][0];
    assert_eq!(o["handle"], format!("{}1", host_prefix()));
    assert_eq!(o["mode"], "shared");
    assert_eq!(o["vram"], 2u64 << 30);
    assert_eq!(o["state"], "running");
    // A gpu-run job behind an exclusive lease sees it as a running job.
    let mut ex = Client::start(&h, Who::Cont, &["--exclusive"]);
    let w = ex.json(T, "waiting").unwrap();
    assert!(
        w["waiting"]
            .as_str()
            .unwrap()
            .contains("1 job(s) on the GPU"),
        "{w}"
    );
    c.release();
    assert!(ex.granted(T));
    let host = h.status(Who::Host);
    assert_eq!(host["others"]["running"], 1);
    ex.release();
}

fn strs(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

fn census(h: &Harness, who: Who, group: &str) -> Value {
    let mut stream = std::os::unix::net::UnixStream::connect(h.sock(who)).unwrap();
    writeln!(stream, "{}", json!({"op": "census", "group": group})).unwrap();
    let mut reply = String::new();
    BufReader::new(stream).read_line(&mut reply).unwrap();
    serde_json::from_str::<Value>(&reply).unwrap()["census"].clone()
}

#[test]
fn a_shared_member_of_a_running_group_never_queues_and_counts_once() {
    let h = Harness::start(
        "lease-group",
        Opts {
            gpu_total_mib: Some(10000),
            ..Opts::default()
        },
    );
    let m = |mib: u64| (mib << 20).to_string();
    let shared = |vram: u64, mem: &str, group: &str| {
        vec![
            "--shared".to_string(),
            "--vram".into(),
            m(vram),
            "--mem".into(),
            mem.into(),
            "--sm".into(),
            "50".into(),
            "--group".into(),
            group.into(),
        ]
    };
    let mut a = Client::start(&h, Who::Host, &strs(&shared(6000, "2000", "job-a")));
    assert!(a.granted(T));
    wait_until("on the card", T, || !h.gpu_pids().is_empty());
    let mut blocker = Client::start(&h, Who::Cont, &["--exclusive"]);
    blocker.json(T, "waiting").unwrap();
    let mut member = Client::start(&h, Who::Host, &strs(&shared(6000, "1000", "job-a")));
    assert!(
        member.granted(T),
        "a member passes the waiting exclusive and ignores room"
    );
    drop(blocker);
    wait_until("both on the card", T, || h.gpu_pids().len() == 2);
    std::thread::sleep(Duration::from_millis(200));
    let mut beside = Client::start(&h, Who::Cont, &["--shared", "--vram", &m(3000)]);
    assert!(
        beside.granted(T),
        "the group counts once: 10000 - max(6000, 3000) = 4000 >= 3000"
    );
    beside.release();
    member.release();
    a.release();
}

#[test]
fn a_requeued_lease_goes_behind_the_head_before_fresh_waiters() {
    let h = Harness::start("lease-requeued", Opts::default());
    let mut holder = Client::start(&h, Who::Host, &["--exclusive", "--label", "holder"]);
    assert!(holder.granted(T));
    let mut first = Client::start(&h, Who::Host, &["--exclusive", "--label", "first"]);
    first.json(T, "waiting").unwrap();
    let mut fresh = Client::start(&h, Who::Host, &["--exclusive", "--label", "fresh"]);
    fresh.json(T, "waiting").unwrap();
    let mut again = Client::start(
        &h,
        Who::Host,
        &["--exclusive", "--label", "again", "--requeued"],
    );
    again.json(T, "waiting").unwrap();
    let positions: Vec<(String, u64)> = own_jobs(&h, Who::Host)
        .iter()
        .filter(|j| j["state"] == "queued")
        .map(|j| {
            (
                j["label"].as_str().unwrap().to_string(),
                j["position"].as_u64().unwrap(),
            )
        })
        .collect();
    let at = |label: &str| positions.iter().find(|(l, _)| l == label).unwrap().1;
    assert_eq!(
        (at("first"), at("again"), at("fresh")),
        (1, 2, 3),
        "{positions:?}"
    );
    holder.release();
    first.release();
    again.release();
    fresh.release();
}

#[test]
fn census_counts_other_parties_and_waiting_exclusives() {
    let h = Harness::start("lease-census", Opts::default());
    let gib = (1u64 << 30).to_string();
    let mut mine = Client::start(
        &h,
        Who::Host,
        &["--shared", "--vram", &gib, "--group", "mine"],
    );
    assert!(mine.granted(T));
    let mut sibling = Client::start(
        &h,
        Who::Host,
        &["--shared", "--vram", &gib, "--group", "mine"],
    );
    assert!(sibling.granted(T));
    let mut coop = Client::start(
        &h,
        Who::Cont,
        &[
            "--shared",
            "--vram",
            &gib,
            "--group",
            "theirs",
            "--cooperative",
        ],
    );
    assert!(coop.granted(T));
    assert_eq!(
        census(&h, Who::Host, "mine"),
        json!({"exclusive_waiting": false, "others": 1, "non_cooperative_others": 0})
    );
    let mut timed = Client::start(&h, Who::Host, &["--exclusive"]);
    timed.json(T, "waiting").unwrap();
    assert_eq!(
        census(&h, Who::Cont, "theirs"),
        json!({"exclusive_waiting": true, "others": 1, "non_cooperative_others": 1})
    );
    coop.release();
    sibling.release();
    mine.release();
    assert!(timed.granted(T));
    timed.release();
}

#[test]
fn shutdown_acknowledgement_does_not_extend_the_deadline() {
    let h = Harness::start(
        "shutdown-deadline",
        Opts {
            switch: json!({"startup_idle_secs": 0.15, "sigterm_to_sigkill_secs": 0.1}),
            ..Opts::default()
        },
    );
    h.wait_word("OFF");
    let mut client = Client::start(
        &h,
        Who::Host,
        &[
            "--exclusive",
            "--mem",
            "0",
            "--sm",
            "0",
            "--shutdown-grace",
            "0.4",
            "--ack-shutdown",
            "--ignore-int",
            "--ignore-term",
        ],
    );
    assert!(client.granted(T));
    let start = Instant::now();
    let notice = client.json(T, "shutdown").expect("typed shutdown");
    assert_eq!(notice["shutdown"]["permanent"], true);
    assert!(notice["shutdown"]["reason"]
        .as_str()
        .unwrap()
        .contains("VRAM"));
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(client.exited().signal(), Some(9));
    assert!(start.elapsed() < Duration::from_secs(2));
    let journal = std::fs::read_to_string(h.dir.join("switch-state/failures.jsonl")).unwrap();
    let events: Vec<Value> = journal
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(events
        .iter()
        .any(|entry| entry["event"] == "shutdown_handler_started"));
    assert!(events
        .iter()
        .any(|entry| entry["event"] == "shutdown_requested"));
}

#[test]
fn cooperative_cleanup_preserves_the_permanent_failure_reason() {
    let h = Harness::start(
        "shutdown-cleanup",
        Opts {
            switch: json!({"startup_idle_secs": 0.15}),
            ..Opts::default()
        },
    );
    h.wait_word("OFF");
    let mut client = Client::start(
        &h,
        Who::Host,
        &[
            "--shared",
            "--vram",
            "1000000",
            "--mem",
            "0",
            "--sm",
            "0",
            "--shutdown-grace",
            "2",
            "--ack-shutdown",
        ],
    );
    assert!(client.granted(T));
    let notice = client.json(T, "shutdown").expect("typed shutdown");
    client.release();
    let id = own_jobs(&h, Who::Host)
        .into_iter()
        .find(|job| job["pid"] == client.pid())
        .unwrap()["id"]
        .as_u64()
        .unwrap();
    h.wait_job_state(Who::Host, id, "killed");
    let job = h.job(Who::Host, id);
    assert_eq!(job["error"], notice["shutdown"]["reason"]);
}

#[test]
fn zero_vram_after_prior_use_is_also_enforced() {
    let h = Harness::start(
        "zero-after-use",
        Opts {
            switch: json!({"startup_idle_secs": 0.2}),
            ..Opts::default()
        },
    );
    h.wait_word("OFF");
    let mut client = Client::start(&h, Who::Host, &["--exclusive"]);
    assert!(client.granted(T));
    std::thread::sleep(Duration::from_millis(300));
    std::fs::write(
        h.gpu_dir.join(client.pid().to_string()),
        "mem=0 sm=0 type=C",
    )
    .unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(client.exited().signal(), Some(9));
}

#[test]
fn zero_vram_cleanup_does_not_keep_a_gpu_grant() {
    let h = Harness::start(
        "shutdown-no-grant",
        Opts {
            switch: json!({"startup_idle_secs": 0.15}),
            ..Opts::default()
        },
    );
    h.wait_word("OFF");
    let mut stopped = Client::start(
        &h,
        Who::Host,
        &[
            "--exclusive",
            "--mem",
            "0",
            "--sm",
            "0",
            "--shutdown-grace",
            "2",
        ],
    );
    assert!(stopped.granted(T));
    stopped.json(T, "shutdown").expect("shutdown request");
    assert_eq!(h.job(Who::Host, 1)["state"], "stopping");
    let mut next = Client::start(&h, Who::Host, &["--exclusive"]);
    assert!(
        next.granted(Duration::from_secs(1)),
        "CPU cleanup blocked a GPU grant"
    );
    stopped.release();
    next.release();
}

#[test]
fn shutdown_cleanup_cannot_reallocate_the_gpu_after_revocation() {
    let h = Harness::start(
        "shutdown-reallocate",
        Opts {
            switch: json!({"startup_idle_secs": 0.15}),
            ..Opts::default()
        },
    );
    h.wait_word("OFF");
    let mut stopped = Client::start(
        &h,
        Who::Host,
        &[
            "--exclusive",
            "--mem",
            "0",
            "--sm",
            "0",
            "--shutdown-grace",
            "2",
        ],
    );
    assert!(stopped.granted(T));
    stopped.json(T, "shutdown").expect("shutdown request");
    std::fs::write(
        h.gpu_dir.join(stopped.pid().to_string()),
        "mem=1 sm=50 type=C",
    )
    .unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(stopped.exited().signal(), Some(9));
    let job = h.wait_job_state(Who::Host, 1, "killed");
    assert!(job["error"].as_str().unwrap().contains("grant was revoked"));
}
