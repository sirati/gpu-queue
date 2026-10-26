//! ml-project's real gpu-switch client (tools/_gpu_switch.py, lease protocol
//! 2) against the real daemon. Opt-in:
//!   GA_ML_TOOLS  tree root holding that client's tools/
//!   GA_PYTHON    python3
//! Without them each test prints a notice and passes. No Python lock is
//! involved: the client is driven through `usable()` and `lease()` alone.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use common::*;

struct Env {
    tools: PathBuf,
    python: PathBuf,
}

fn env() -> Option<Env> {
    let get = |k: &str| std::env::var_os(k).map(PathBuf::from);
    Some(Env {
        tools: get("GA_ML_TOOLS")?,
        python: get("GA_PYTHON")?,
    })
}

macro_rules! need_env {
    () => {
        match env() {
            Some(e) => e,
            None => {
                eprintln!("interop: GA_ML_TOOLS/GA_PYTHON unset, skipped");
                return;
            }
        }
    };
}

/// One `switch_client.py`; stdout and stderr lines on one channel.
struct Py {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: mpsc::Receiver<String>,
    seen: Vec<String>,
}

impl Py {
    fn start(
        e: &Env,
        socket: Option<&std::path::Path>,
        mode: &str,
        vram: u64,
        label: &str,
        priority: i8,
        f: impl FnOnce(&mut Command),
    ) -> Py {
        let driver =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/interop/switch_client.py");
        let mut c = Command::new(&e.python);
        c.arg(driver)
            .arg(&e.tools)
            .arg(mode)
            .arg(vram.to_string())
            .arg(label)
            .arg(priority.to_string())
            .env_remove("GPU_SWITCH_GRANTED")
            .env_remove("ML_GPU_SWITCH")
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        match socket {
            Some(s) => c.env("GPU_SWITCH_SOCKET", s),
            None => c.env_remove("GPU_SWITCH_SOCKET"),
        };
        f(&mut c);
        let mut child = c.spawn().unwrap();
        let (tx, rx) = mpsc::channel();
        for out in [
            Box::new(child.stdout.take().unwrap()) as Box<dyn std::io::Read + Send>,
            Box::new(child.stderr.take().unwrap()),
        ] {
            let tx = tx.clone();
            std::thread::spawn(move || {
                for l in BufReader::new(out).lines().map_while(Result::ok) {
                    let _ = tx.send(l);
                }
            });
        }
        let stdin = child.stdin.take();
        Py {
            child,
            stdin,
            lines: rx,
            seen: Vec::new(),
        }
    }

    fn find(&mut self, timeout: Duration, f: impl Fn(&str) -> bool) -> Option<String> {
        if let Some(l) = self.seen.iter().find(|l| f(l)) {
            return Some(l.clone());
        }
        let deadline = Instant::now() + timeout;
        loop {
            match self
                .lines
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            {
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

    fn granted(&mut self, t: Duration) -> bool {
        self.find(t, |l| l == "granted").is_some()
    }

    fn release(&mut self) {
        if let Some(mut s) = self.stdin.take() {
            let _ = writeln!(s);
        }
        assert!(
            self.find(Duration::from_secs(10), |l| l == "released")
                .is_some(),
            "{:?}",
            self.seen
        );
        let _ = self.child.wait();
    }
}

impl Drop for Py {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

const T: Duration = Duration::from_secs(10);

#[test]
fn python_client_is_granted_and_releases() {
    let e = need_env!();
    let h = Harness::start("py-grant", Opts::default());
    let mut p = Py::start(&e, Some(&h.host_sock), "exclusive", 0, "py-job", 0, |_| {});
    assert!(p.granted(T), "{:?}", p.seen);
    let s = h.status(Who::Host);
    let j = &s["jobs"][0];
    assert_eq!(j["lease"], true);
    assert_eq!(j["label"], "py-job");
    assert_eq!(j["state"], "running");
    assert_eq!(j["pid"], p.child.id());
    p.release();
    h.wait_job_state(Who::Host, 1, "done");
}

#[test]
fn python_client_waits_behind_ninfer_until_switch_gpu() {
    let e = need_env!();
    let h = Harness::start("py-ninfer", Opts::default());
    h.run_ok(Who::Host, &["serve", "on"]);
    h.wait_word("SERVING");
    let mut p = Py::start(&e, Some(&h.cont_sock), "shared", 1 << 30, "py", 0, |_| {});
    let w = p
        .find(T, |l| l.starts_with("GPU-SWITCH: waiting:"))
        .expect("the client reports why it waits");
    assert!(
        w.contains("NInfer holds the GPU; NOT running until 'gpu-run switch-gpu'"),
        "{w}"
    );
    assert!(!p.granted(Duration::from_millis(500)));
    h.run_ok(Who::Host, &["switch-gpu"]);
    assert!(p.granted(T));
    p.release();
    h.wait_word("SERVING");
}

#[test]
fn python_client_delivers_the_signal_to_itself() {
    let e = need_env!();
    let h = Harness::start("py-signal", Opts::default());
    let mut p = Py::start(&e, Some(&h.cont_sock), "exclusive", 0, "py", 0, |_| {});
    assert!(p.granted(T));
    let handle = h.status(Who::Cont)["jobs"][0]["handle"]
        .as_str()
        .unwrap()
        .to_string();
    h.run_ok(Who::Host, &["kill", &handle]);
    assert!(
        p.find(T, |l| l.contains("gpu-switch sends SIGINT"))
            .is_some(),
        "{:?}",
        p.seen
    );
    let st = p.child.wait().unwrap();
    assert!(!st.success(), "SIGINT ended the waiting client");
    let j = h.wait_job_state(Who::Cont, 1, "killed");
    assert!(j["error"].as_str().unwrap().starts_with("killed by "));
}

#[test]
fn python_client_under_a_gpu_run_job_takes_no_grant() {
    let e = need_env!();
    let h = Harness::start("py-nested", Opts::default());
    let mut p = Py::start(&e, Some(&h.host_sock), "exclusive", 0, "py", 0, |c| {
        c.env("GPU_SWITCH_GRANTED", "7");
    });
    assert!(p.find(T, |l| l == "none").is_some(), "{:?}", p.seen);
    assert!(h.status(Who::Host)["jobs"].as_array().unwrap().is_empty());
}

#[test]
fn python_priority_passes_within_its_budget() {
    let e = need_env!();
    let h = Harness::start("py-prio", Opts::default());
    let hold = h.hold("--sm 50");
    let blk = h.submit(Who::Host, EXCL, &["sh", "-c", &format!("exec {hold}")]);
    h.wait_job_state(Who::Host, blk, "running");
    let zero = h.submit(Who::Cont, EXCL, &["true"]);
    let neg = h.submit(Who::Cont, &["--exclusive", "--priority", "-1"], &["true"]);
    // Priority 0 passes the -1 job (budget 1) but not the 0 job.
    let mut p = Py::start(&e, Some(&h.host_sock), "exclusive", 0, "py", 0, |_| {});
    wait_until("python in line", T, || {
        h.status(Who::Host)["line"].as_array().unwrap().len() == 3
    });
    let kinds: Vec<String> = h.status(Who::Cont)["line"]
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
        .collect();
    assert_eq!(
        kinds,
        [
            format!("own:{zero}"),
            format!("other:{}2", host_prefix()),
            format!("own:{neg}")
        ]
    );
    h.run_ok(Who::Host, &["cancel", &blk.to_string()]);
    assert_eq!(h.wait_job(Who::Cont, zero), 0);
    assert!(p.granted(T));
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        h.job(Who::Cont, neg)["state"],
        "queued",
        "the -1 job waits behind the grant"
    );
    p.release();
    assert_eq!(h.wait_job(Who::Cont, neg), 0);
}

#[test]
fn old_daemon_without_leases_is_not_used() {
    let e = need_env!();
    // A socket that answers like a gpu-switch predating leases.
    let dir = std::env::temp_dir().join(format!("ga-py-old-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("old.sock");
    let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
    std::thread::spawn(move || {
        if let Ok((s, _)) = listener.accept() {
            let mut r = BufReader::new(s.try_clone().unwrap());
            let mut l = String::new();
            let _ = r.read_line(&mut l);
            let _ = (&s)
                .write_all(b"{\"ok\":false,\"error\":\"bad request: unknown variant lease\"}\n");
        }
    });
    let mut p = Py::start(&e, Some(&path), "exclusive", 0, "py", 0, |_| {});
    assert!(p.find(T, |l| l == "none").is_some(), "{:?}", p.seen);
    let _ = std::fs::remove_dir_all(&dir);
}

fn queued_lease(h: &Harness, who: Who, label: &str) -> Option<serde_json::Value> {
    h.status(who)["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|j| j["label"] == label && j["state"] == "queued")
        .cloned()
}

fn running_id(h: &Harness, who: Who) -> u64 {
    h.status(who)["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|j| j["state"] == "running")
        .map(|j| j["id"].as_u64().unwrap())
        .expect("a running job")
}

#[test]
fn python_lease_waits_in_the_line_and_shows_in_status() {
    let e = need_env!();
    let h = Harness::start("py-line", Opts::default());
    let blocker = h.submit(
        Who::Host,
        EXCL,
        &["sh", "-c", &format!("exec {}", h.hold("--sm 50"))],
    );
    h.wait_job_state(Who::Host, blocker, "running");
    let mut p = Py::start(
        &e,
        Some(&h.host_sock),
        "exclusive",
        0,
        "tree: pytest",
        0,
        |_| {},
    );
    wait_until("python in line", T, || {
        queued_lease(&h, Who::Host, "tree: pytest").is_some()
    });
    assert_eq!(
        queued_lease(&h, Who::Host, "tree: pytest").unwrap()["position"],
        1
    );
    assert_eq!(
        h.status(Who::Cont)["others"]["queued"],
        1,
        "the other side counts it"
    );
    h.run_ok(Who::Host, &["cancel", &blocker.to_string()]);
    assert!(p.granted(T), "{:?}", p.seen);
    p.release();
}

#[test]
fn python_lease_survives_restarts_while_waiting_and_holding() {
    let e = need_env!();
    let mut h = Harness::start("py-restart", Opts::default());
    let blocker = h.submit(
        Who::Host,
        EXCL,
        &["sh", "-c", &format!("exec {}", h.hold("--sm 50"))],
    );
    h.wait_job_state(Who::Host, blocker, "running");
    let mut p = Py::start(
        &e,
        Some(&h.cont_sock),
        "exclusive",
        0,
        "py-restart",
        0,
        |_| {},
    );
    wait_until("python in line", T, || {
        queued_lease(&h, Who::Cont, "py-restart").is_some()
    });
    let before = queued_lease(&h, Who::Cont, "py-restart").unwrap()["id"].clone();
    h.stop_switch();
    h.start_switch();
    wait_until("python resumed", T, || {
        queued_lease(&h, Who::Cont, "py-restart").is_some()
    });
    assert_eq!(
        queued_lease(&h, Who::Cont, "py-restart").unwrap()["id"],
        before
    );
    h.run_ok(Who::Host, &["cancel", &blocker.to_string()]);
    assert!(p.granted(T), "{:?}", p.seen);
    h.stop_switch();
    h.start_switch();
    wait_until("the grant resumed", T, || {
        h.log("switch.log").matches("resumed by pid").count() >= 2
    });
    assert_eq!(running_id(&h, Who::Cont).to_string(), before.to_string());
    p.release();
}

#[test]
fn python_lease_keeps_its_place_when_gpu_switch_lost_its_state() {
    let e = need_env!();
    let mut h = Harness::start("py-lost", Opts::default());
    let blocker = h.submit(
        Who::Host,
        EXCL,
        &["sh", "-c", &format!("exec {}", h.hold("--sm 50"))],
    );
    h.wait_job_state(Who::Host, blocker, "running");
    let mut p = Py::start(
        &e,
        Some(&h.cont_sock),
        "exclusive",
        0,
        "py-first",
        0,
        |_| {},
    );
    wait_until("python in line", T, || {
        queued_lease(&h, Who::Cont, "py-first").is_some()
    });
    h.stop_switch();
    std::fs::remove_file(h.dir.join("switch-state/state.json")).unwrap();
    h.start_switch();
    wait_until("python registered again", T, || {
        queued_lease(&h, Who::Cont, "py-first").is_some()
    });
    let later = h.submit(Who::Cont, EXCL, &["true"]);
    wait_until("blocker registered again", T, || {
        h.status(Who::Host)["jobs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|j| j["state"] == "running")
    });
    h.run_ok(
        Who::Host,
        &["cancel", &running_id(&h, Who::Host).to_string()],
    );
    assert!(p.granted(T), "{:?}", p.seen);
    assert_eq!(
        h.job(Who::Cont, later)["state"],
        "queued",
        "the later job waits behind it"
    );
    p.release();
    assert_eq!(h.wait_job(Who::Cont, later), 0);
}
