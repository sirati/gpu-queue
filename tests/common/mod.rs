#![allow(dead_code)]

use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{HeaderMap, Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};

pub const SWITCH: &str = env!("CARGO_BIN_EXE_gpu-switch");
pub const ARBITER: &str = env!("CARGO_BIN_EXE_gpu-arbiter");
pub const GPU_RUN: &str = env!("CARGO_BIN_EXE_gpu-run");
pub const MOCK: &str = env!("CARGO_BIN_EXE_ninfer-mock");

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
    pub nvidia_smi: PathBuf,
    pub switch_config: PathBuf,
    pub arbiter_config: PathBuf,
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
        let nvidia_smi = dir.join("bin/nvidia-smi");
        std::os::unix::fs::symlink(MOCK, &nvidia_smi).unwrap();
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
            "nvidia_smi": if opts.fake_gpu { json!(nvidia_smi) } else { Value::Null },
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
            nvidia_smi,
            switch_config,
            arbiter_config,
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
        wait_until("gpu-switch sockets", Duration::from_secs(10), || {
            std::os::unix::net::UnixStream::connect(&self.host_sock).is_ok()
                && std::os::unix::net::UnixStream::connect(&self.cont_sock).is_ok()
                && std::os::unix::net::UnixStream::connect(&self.arb_sock).is_ok()
        });
    }

    pub fn start_arbiter(&mut self) {
        self.arbiter =
            Some(self.spawn_logged(ARBITER, &self.arbiter_config.clone(), "arbiter.log"));
        wait_until("arbiter attached", Duration::from_secs(10), || {
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
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let w = self.word();
            if w == word {
                return;
            }
            if Instant::now() > deadline {
                panic!("timed out waiting for {word}, now {w}");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    pub fn wait_status(&self, who: Who, what: &str, f: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let s = self.status(who);
            if f(&s) {
                return s;
            }
            if Instant::now() > deadline {
                panic!("timed out waiting for {what}; last status: {s:#}");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    pub fn job(&self, who: Who, id: u64) -> Value {
        serde_json::from_str(&self.run_ok(who, &["status", &id.to_string(), "--json"])).unwrap()
    }

    pub fn wait_job_state(&self, who: Who, id: u64, state: &str) -> Value {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let j = self.job(who, id);
            if j["state"] == state {
                return j;
            }
            if Instant::now() > deadline {
                panic!("timed out waiting for job {id} ({who:?}) {state}: {j:#}");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
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

    /// Pids registered in the fake GPU.
    pub fn gpu_pids(&self) -> Vec<i32> {
        std::fs::read_dir(&self.gpu_dir)
            .map(|d| {
                d.flatten()
                    .filter_map(|e| e.file_name().to_str()?.parse().ok())
                    .filter(|p| alive(*p))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `ninfer-mock gpu-hold` with these arguments, as a job command.
    pub fn hold(&self, args: &str) -> String {
        format!("{MOCK} gpu-hold --mem 1 {args}")
    }

    /// The arbiter's NInfer child, if one runs.
    pub fn ninfer_pid(&self) -> Option<i32> {
        let me = self.arbiter.as_ref()?.id();
        gpu_arbiter::procs::parents()
            .into_iter()
            .find(|(_, ppid)| *ppid == me)
            .map(|(pid, _)| pid as i32)
    }
}

fn stop_child(mut child: Child) -> std::process::ExitStatus {
    signal(child.id() as i32, libc::SIGTERM);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("child ignored SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let jobs: Vec<i64> = [Who::Host, Who::Cont]
            .iter()
            .filter_map(|w| {
                let out = self.gpu_run(*w).args(["status", "--json"]).output().ok()?;
                serde_json::from_slice::<Value>(&out.stdout).ok()
            })
            .flat_map(|s| {
                s["jobs"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|j| j["state"] == "running" || j["state"] == "queued")
                    .flat_map(|j| [j["runner_pid"].as_i64(), j["pid"].as_i64().map(|p| -p)])
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .collect();
        if let Some(a) = self.arbiter.take() {
            stop_child(a);
        }
        if let Some(s) = self.switch.take() {
            stop_child(s);
        }
        for pid in jobs {
            // Raw lease protocol tests use the test process as their peer.
            if pid.unsigned_abs() == u64::from(std::process::id()) {
                continue;
            }
            signal(pid as i32, libc::SIGKILL);
        }
        for pid in self.gpu_pids() {
            signal(pid, libc::SIGKILL);
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

pub fn signal(pid: i32, sig: i32) {
    unsafe { libc::kill(pid, sig) };
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

pub fn wait_until(what: &str, timeout: Duration, f: impl Fn() -> bool) {
    let deadline = Instant::now() + timeout;
    while !f() {
        if Instant::now() > deadline {
            panic!("timed out waiting for {what}");
        }
        std::thread::sleep(Duration::from_millis(20));
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
