#![allow(dead_code)]

use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use bytes::Bytes;
use gpu_arbiter::procs::{Pidfd, Process};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{HeaderMap, Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};

#[path = "../../src/bin/mock/gpu_claims.rs"]
pub mod gpu_claims;
#[path = "../../src/test_support/held.rs"]
pub mod held;
#[path = "../../src/test_support/wait.rs"]
pub mod wait;
pub use wait::wait_until;

pub const SWITCH: &str = env!("CARGO_BIN_EXE_gpu-switch");
pub const ARBITER: &str = env!("CARGO_BIN_EXE_gpu-arbiter");
pub const GPU_RUN: &str = env!("CARGO_BIN_EXE_gpu-run");
pub const MOCK: &str = env!("CARGO_BIN_EXE_ninfer-mock");

/// The mock NVML library (`examples/nvml_mock.rs`), which `cargo test` builds beside the binaries.
pub fn nvml_mock() -> PathBuf {
    Path::new(MOCK)
        .with_file_name("examples")
        .join("libnvml_mock.so")
}

pub const REPLY: &str = "Hello from the mock model";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Who {
    Host,
    Cont,
}

pub struct Opts {
    pub mock_args: Vec<String>,
    pub linger: f64,
    pub drain_timeout: Option<f64>,
    pub model_present: bool,
    pub fake_gpu: bool,
    pub arbiter: bool,
    /// Extra gpu-switch config fields.
    pub switch: Value,
    /// Extra gpu-arbiter config fields.
    pub arbiter_config: Value,
    pub gpu_total_mib: Option<u64>,
}

impl Default for Opts {
    fn default() -> Opts {
        Opts {
            mock_args: Vec::new(),
            linger: 0.3,
            drain_timeout: None,
            model_present: true,
            fake_gpu: true,
            arbiter: true,
            switch: json!({}),
            arbiter_config: json!({}),
            gpu_total_mib: None,
        }
    }
}

pub struct Harness {
    pub dir: PathBuf,
    pub host_sock: PathBuf,
    pub cont_sock: PathBuf,
    /// gpu-switch's socket for the arbiter link.
    pub arb_sock: PathBuf,
    pub proxy: SocketAddr,
    /// The proxy's container listener: Messages only, an open socket.
    pub proxy_cont: PathBuf,
    pub argv_file: PathBuf,
    pub model: PathBuf,
    pub gpu_dir: PathBuf,
    pub switch_config: PathBuf,
    pub arbiter_config: PathBuf,
    /// The held commands this harness made, in `<dir>/held`.
    holds: held::Holds,
    switch: Option<Child>,
    arbiter: Option<Child>,
    fake_gpu: bool,
}

/// A free port not handed to another harness of this test process.
fn free_port() -> u16 {
    static TAKEN: std::sync::Mutex<Vec<u16>> = std::sync::Mutex::new(Vec::new());
    let mut taken = TAKEN.lock().unwrap();
    loop {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        if !taken.contains(&port) {
            taken.push(port);
            return port;
        }
    }
}

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn merge(base: &mut Value, extra: &Value) {
    if let (Some(b), Some(e)) = (base.as_object_mut(), extra.as_object()) {
        for (k, v) in e {
            b.insert(k.clone(), v.clone());
        }
    }
}

impl Harness {
    pub fn start(name: &str, opts: Opts) -> Harness {
        let dir = std::env::temp_dir().join(format!(
            "ga-{name}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        for d in ["bin", "gpu", "state-host", "state-cont"] {
            std::fs::create_dir_all(dir.join(d)).unwrap();
        }
        let gpu_dir = dir.join("gpu");
        if let Some(t) = opts.gpu_total_mib {
            std::fs::write(gpu_dir.join("total"), t.to_string()).unwrap();
        }
        let model = dir.join("model.ninfer");
        if opts.model_present {
            std::fs::write(&model, b"mock weights").unwrap();
        }
        let argv_file = dir.join("argv.jsonl");
        let host_sock = dir.join("host.sock");
        let cont_sock = dir.join("cont.sock");
        let arb_sock = dir.join("arb.sock");
        let proxy: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
        let proxy_cont = dir.join("proxy-cont.sock");

        let mut switch = json!({
            "listen": [
                {"name": "host", "path": host_sock},
                {"name": "container", "path": cont_sock},
                {"name": "arbiter", "path": arb_sock}
            ],
            "nvml": if opts.fake_gpu { json!(nvml_mock()) } else { Value::Null },
            "linger_secs": opts.linger,
            "gpu_poll_interval_secs": 0.05,
            "job_kill_grace_secs": 1,
            "profiles": ["mtp", "dflash2"],
            "profile": "mtp",
            "host_context_mib": 8192,
            "drain_timeout_secs": opts.drain_timeout,
            "restart_backoff_initial_secs": 0.2,
            "restart_backoff_max_secs": 1,
            "state_dir": dir.join("switch-state")
        });
        merge(&mut switch, &opts.switch);
        let switch_config = dir.join("switch.json");
        std::fs::write(&switch_config, serde_json::to_vec_pretty(&switch).unwrap()).unwrap();

        let mut mock_args = vec!["--reply".to_string(), REPLY.into()];
        mock_args.extend(["--argv-file".into(), argv_file.display().to_string()]);
        mock_args.extend(opts.mock_args.iter().cloned());
        let mut arbiter = json!({
            "proxy": {
                "listen": proxy.to_string(),
                "listeners": [{
                    "name": "container",
                    "path": proxy_cont,
                    "messages_only": true,
                    "mode": "0666"
                }],
                "keepalive_interval_secs": 0.2
            },
            "ninfer": {
                "executable": MOCK,
                "model": model,
                "port": free_port(),
                "args": mock_args,
                "profiles": {"mtp": ["--spec", "mtp"], "dflash2": ["--spec", "dflash2"]},
                "health_poll_interval_secs": 0.05,
                "startup_timeout_secs": 30,
                "stop_timeout_secs": 5
            },
            "switch_socket": arb_sock,
            "reconnect_initial_secs": 0.1,
            "reconnect_max_secs": 0.5
        });
        merge(&mut arbiter, &opts.arbiter_config);
        let arbiter_config = dir.join("arbiter.json");
        std::fs::write(
            &arbiter_config,
            serde_json::to_vec_pretty(&arbiter).unwrap(),
        )
        .unwrap();

        let holds = held::Holds::new(dir.join("held"));
        let mut h = Harness {
            dir,
            host_sock,
            cont_sock,
            arb_sock,
            proxy,
            proxy_cont,
            argv_file,
            model,
            gpu_dir,
            switch_config,
            arbiter_config,
            holds,
            switch: None,
            arbiter: None,
            fake_gpu: opts.fake_gpu,
        };
        h.start_switch();
        if opts.arbiter {
            h.start_arbiter();
        }
        h
    }

    fn spawn_logged(&self, bin: &str, config: &Path, log: &str) -> Child {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(log))
            .unwrap();
        let mut cmd = Command::new(bin);
        cmd.arg("--config")
            .arg(config)
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        if self.fake_gpu {
            cmd.env("NINFER_MOCK_GPU_DIR", &self.gpu_dir);
        }
        cmd.spawn().unwrap()
    }

    pub fn start_switch(&mut self) {
        self.switch = Some(self.spawn_logged(SWITCH, &self.switch_config.clone(), "switch.log"));
        wait_until("gpu-switch sockets", || {
            std::os::unix::net::UnixStream::connect(&self.host_sock).is_ok()
                && std::os::unix::net::UnixStream::connect(&self.cont_sock).is_ok()
                && std::os::unix::net::UnixStream::connect(&self.arb_sock).is_ok()
        });
    }

    /// Starts gpu-switch on the harness's configuration, a start that must refuse, and waits for it to exit.
    pub fn start_switch_refused(&self) -> std::process::ExitStatus {
        let mut child = self.spawn_logged(SWITCH, &self.switch_config, "switch.log");
        child.wait().unwrap()
    }

    pub fn start_arbiter(&mut self) {
        self.arbiter =
            Some(self.spawn_logged(ARBITER, &self.arbiter_config.clone(), "arbiter.log"));
        wait_until("arbiter attached", || {
            self.log("switch.log").contains("arbiter attached")
        });
    }

    pub fn stop_switch(&mut self) -> std::process::ExitStatus {
        stop_child(self.switch.take().expect("gpu-switch running"))
    }

    pub fn stop_arbiter(&mut self) -> std::process::ExitStatus {
        stop_child(self.arbiter.take().expect("gpu-arbiter running"))
    }

    pub fn arbiter_pid(&self) -> i32 {
        self.arbiter.as_ref().unwrap().id() as i32
    }

    pub fn sock(&self, who: Who) -> &Path {
        match who {
            Who::Host => &self.host_sock,
            Who::Cont => &self.cont_sock,
        }
    }

    pub fn gpu_run(&self, who: Who) -> Command {
        let mut c = Command::new(GPU_RUN);
        c.env("GPU_SWITCH_SOCKET", self.sock(who))
            .env(
                "XDG_STATE_HOME",
                self.dir.join(match who {
                    Who::Host => "state-host",
                    Who::Cont => "state-cont",
                }),
            )
            .env_remove("XDG_RUNTIME_DIR")
            .current_dir(&self.dir);
        if self.fake_gpu {
            c.env("NINFER_MOCK_GPU_DIR", &self.gpu_dir);
        }
        c
    }

    pub fn run(&self, who: Who, args: &[&str]) -> Output {
        self.gpu_run(who).args(args).output().unwrap()
    }

    pub fn run_ok(&self, who: Who, args: &[&str]) -> String {
        let out = self.run(who, args);
        assert!(
            out.status.success(),
            "gpu-run {args:?} ({who:?}) failed: {}\n{}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }

    pub fn status(&self, who: Who) -> Value {
        serde_json::from_str(&self.run_ok(who, &["status", "--json"])).unwrap()
    }

    pub fn word(&self) -> String {
        let out = self.run_ok(Who::Host, &["serve", "status"]);
        out.lines().next().unwrap_or("").to_string()
    }

    pub fn wait_word(&self, word: &str) {
        wait_until(&format!("the serve word {word}"), || self.word() == word);
    }

    pub fn wait_status(&self, who: Who, what: &str, f: impl Fn(&Value) -> bool) -> Value {
        let mut status = Value::Null;
        wait_until(what, || {
            status = self.status(who);
            f(&status)
        });
        status
    }

    pub fn job(&self, who: Who, id: u64) -> Value {
        serde_json::from_str(&self.run_ok(who, &["status", &id.to_string(), "--json"])).unwrap()
    }

    pub fn wait_job_state(&self, who: Who, id: u64, state: &str) -> Value {
        let mut job = Value::Null;
        wait_until(&format!("job {id} ({who:?}) {state}"), || {
            job = self.job(who, id);
            job["state"] == state
        });
        job
    }

    pub fn submit_with(
        &self,
        who: Who,
        spec: &[&str],
        cmd: &[&str],
        f: impl FnOnce(&mut Command),
    ) -> u64 {
        let mut c = self.gpu_run(who);
        c.arg("submit").args(spec).arg("--").args(cmd);
        f(&mut c);
        let out = c.output().unwrap();
        assert!(
            out.status.success(),
            "submit failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout)
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }

    pub fn submit(&self, who: Who, spec: &[&str], cmd: &[&str]) -> u64 {
        self.submit_with(who, spec, cmd, |_| {})
    }

    pub fn wait_job(&self, who: Who, id: u64) -> i32 {
        self.run(who, &["wait", &id.to_string()])
            .status
            .code()
            .unwrap()
    }

    pub fn starts(&self) -> Vec<Vec<String>> {
        std::fs::read_to_string(&self.argv_file)
            .unwrap_or_default()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    pub fn log(&self, name: &str) -> String {
        std::fs::read_to_string(self.dir.join(name)).unwrap_or_default()
    }

    /// Each file of the fake GPU named by a pid, with that pid.
    fn gpu_files(&self) -> Vec<(i32, PathBuf)> {
        std::fs::read_dir(&self.gpu_dir)
            .map(|d| {
                d.flatten()
                    .filter_map(|e| Some((e.file_name().to_str()?.parse().ok()?, e.path())))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Pids registered in the fake GPU.
    pub fn gpu_pids(&self) -> Vec<i32> {
        self.gpu_files()
            .into_iter()
            .map(|(pid, _)| pid)
            .filter(|p| alive(*p))
            .collect()
    }

    /// The processes that registered themselves in the fake GPU, each as the start time its file records.
    fn gpu_registrants(&self) -> Vec<Process> {
        self.gpu_files()
            .into_iter()
            .filter_map(|(pid, path)| {
                Some(Process {
                    pid: u32::try_from(pid).ok()?,
                    birth: gpu_claims::registered_birth(&std::fs::read_to_string(path).ok()?)?,
                })
            })
            .collect()
    }

    /// `ninfer-mock gpu-hold` with these arguments, as a job command.
    pub fn hold(&self, args: &str) -> String {
        gpu_hold(args).join(" ")
    }

    /// `ninfer-mock gpu-hold` with these arguments, held until the test releases it or the harness drops.
    pub fn held(&self, args: &str) -> held::Held {
        let mut words = gpu_hold(args);
        words.push("--until-file".into());
        self.holds.held(held::Waiter { words })
    }

    /// The arbiter's NInfer child, if one runs: its pid and start time are read together with its parent, this
    /// harness's arbiter, which this process has not waited for.
    pub fn ninfer(&self) -> Option<Process> {
        let me = self.arbiter.as_ref()?.id();
        let processes = gpu_arbiter::procs::processes();
        processes
            .parents
            .iter()
            .find(|(_, ppid)| **ppid == me)
            .map(|(pid, _)| Process {
                pid: *pid,
                birth: processes.births[pid],
            })
    }

    /// The runner of job `id`, as the process its pid names when the harness reads the job.
    pub fn runner(&self, who: Who, id: u64) -> Process {
        named(&self.job(who, id)["runner_pid"]).expect("a runner that runs")
    }

    /// The runners and job process groups of the running and queued entries on both sides, held from the moment
    /// the harness reads them.
    fn entries(&self) -> Entries {
        let entries: Vec<Value> = [Who::Host, Who::Cont]
            .iter()
            .filter_map(|w| {
                let out = self.gpu_run(*w).args(["status", "--json"]).output().ok()?;
                serde_json::from_slice::<Value>(&out.stdout).ok()
            })
            .flat_map(|s| s["jobs"].as_array().cloned().unwrap_or_default())
            .filter(|j| j["state"] == "running" || j["state"] == "queued")
            .collect();
        Entries {
            runners: entries
                .iter()
                .filter_map(|j| held(named(&j["runner_pid"])?))
                .collect(),
            groups: entries
                .iter()
                .filter_map(|j| held(named(&j["pid"])?))
                .collect(),
        }
    }
}

/// What the harness ends once its daemons have stopped: each runner alone, and the process group each job leads.
struct Entries {
    runners: Vec<Pidfd>,
    groups: Vec<Pidfd>,
}

/// The process a status names by `pid`. A status reports no start time, so its identity is the one the pid has when
/// the harness reads it.
fn named(pid: &Value) -> Option<Process> {
    Process::now(u32::try_from(pid.as_u64()?).ok()?)
}

/// The words of `ninfer-mock gpu-hold` with these arguments: the one prefix of `Harness::hold` and `Harness::held`.
fn gpu_hold(args: &str) -> Vec<String> {
    [MOCK, "gpu-hold", "--mem", "1"].into_iter().map(String::from).chain(args.split_whitespace().map(String::from)).collect()
}

/// A process the harness may end, held so that no later process given its pid is: never the test process itself,
/// which raw lease protocol tests make a peer.
fn held(process: Process) -> Option<Pidfd> {
    (process.pid != std::process::id())
        .then(|| process.pidfd())
        .flatten()
}

/// A child of this process: its pid names it until it is waited for.
pub fn spawned(child: &Child) -> Process {
    Process::now(child.id()).expect("a child not yet waited for")
}

fn stop_child(mut child: Child) -> std::process::ExitStatus {
    spawned(&child).signal(libc::SIGTERM);
    child.wait().unwrap()
}

impl Drop for Harness {
    /// Releases its held commands first, then stops its daemons and ends what it keeps, and deletes its directory
    /// last.
    fn drop(&mut self) {
        self.holds.release_all();
        let entries = self.entries();
        if let Some(a) = self.arbiter.take() {
            stop_child(a);
        }
        if let Some(s) = self.switch.take() {
            stop_child(s);
        }
        for runner in &entries.runners {
            runner.signal(libc::SIGKILL);
        }
        for group in &entries.groups {
            group.signal_group(libc::SIGKILL);
        }
        for registrant in self.gpu_registrants().into_iter().filter_map(held) {
            registrant.signal(libc::SIGKILL);
        }
        if std::thread::panicking() || std::env::var_os("GA_TEST_LOG").is_some() {
            eprintln!(
                "---- {} ----\n-- switch --\n{}\n-- arbiter --\n{}",
                self.dir.display(),
                self.log("switch.log"),
                self.log("arbiter.log")
            );
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub fn alive(pid: i32) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(s) => !s
            .rsplit(')')
            .next()
            .unwrap_or("")
            .trim_start()
            .starts_with('Z'),
        Err(_) => false,
    }
}


pub fn with_umask(c: &mut Command, mask: libc::mode_t) {
    unsafe {
        c.pre_exec(move || {
            libc::umask(mask);
            Ok(())
        });
    }
}

pub fn now() -> f64 {
    gpu_arbiter::protocol::now_unix()
}

// ---- HTTP client ----

pub async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<Value>,
) -> (StatusCode, HeaderMap, Incoming) {
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(conn);
    let mut b = Request::builder()
        .method(method)
        .uri(path)
        .header("host", addr.to_string());
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    let body = body.map(|v| Bytes::from(v.to_string())).unwrap_or_default();
    if !body.is_empty() {
        b = b.header("content-type", "application/json");
    }
    let resp = sender
        .send_request(b.body(Full::new(body)).unwrap())
        .await
        .unwrap();
    let (parts, body) = resp.into_parts();
    (parts.status, parts.headers, body)
}

pub async fn text(body: Incoming) -> String {
    String::from_utf8(body.collect().await.unwrap().to_bytes().to_vec()).unwrap()
}

pub async fn chunks(mut body: Incoming) -> Vec<(Instant, String)> {
    let mut out = Vec::new();
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame.unwrap().into_data() {
            out.push((Instant::now(), String::from_utf8(data.to_vec()).unwrap()));
        }
    }
    out
}

pub fn message(stream: bool) -> Value {
    json!({
        "model": "local",
        "max_tokens": 64,
        "stream": stream,
        "messages": [{"role": "user", "content": "hi"}],
    })
}

pub fn events(sse: &str) -> Vec<String> {
    sse.lines()
        .filter_map(|l| l.strip_prefix("event: "))
        .map(str::to_string)
        .collect()
}

pub const EXCL: &[&str] = &["--exclusive"];

/// The host side's handle prefix: the user's initial, or `u<uid>-` when that
/// is `n` (the container's) or not a letter. Mirrors gpu-switch.
pub fn host_prefix() -> String {
    let uid = unsafe { libc::getuid() };
    match gpu_arbiter::procs::username(uid)
        .and_then(|n| n.chars().next())
        .filter(|c| c.is_ascii_alphabetic())
        .map(|c| c.to_ascii_lowercase())
    {
        Some(c) if c != 'n' => c.to_string(),
        _ => format!("u{uid}-"),
    }
}

pub const RELAY: &str = env!("CARGO_BIN_EXE_ninfer-relay");

/// One request over a unix socket of the proxy.
pub async fn request_unix(
    path: &Path,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, HeaderMap, Incoming) {
    let stream = tokio::net::UnixStream::connect(path).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::spawn(conn);
    let mut b = Request::builder()
        .method(method)
        .uri(uri)
        .header("host", "localhost");
    let body = body.map(|v| Bytes::from(v.to_string())).unwrap_or_default();
    if !body.is_empty() {
        b = b.header("content-type", "application/json");
    }
    let resp = sender
        .send_request(b.body(Full::new(body)).unwrap())
        .await
        .unwrap();
    let (parts, body) = resp.into_parts();
    (parts.status, parts.headers, body)
}

/// A user message under a system prompt.
pub fn asking(system: &str, user: &str) -> Value {
    json!({
        "model": "local",
        "max_tokens": 64,
        "system": system,
        "messages": [{"role": "user", "content": user}],
    })
}
