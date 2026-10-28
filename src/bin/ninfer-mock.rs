//! Test double for ninfer-serve: same HTTP surface (/health, /v1/messages,
//! /v1/messages/count_tokens, /v1/models), no GPU.
//!
//! Also a fake GPU, kept in `$NINFER_MOCK_GPU_DIR`: one file per process
//! (`<pid>`: `mem=<MiB> sm=<pct> type=<C|G|C+G>`) and `total` (MiB, default
//! 32768).
//! - Invoked as `nvidia-smi` (argv[0] basename) or `ninfer-mock nvidia-smi`,
//!   it answers the queries gpu-switch makes (compute apps, memory, pmon).
//! - `ninfer-mock gpu-hold [--ms N] [--mem MiB] [--sm PCT] [--sm-ms N]
//!   [--type T] [--ignore-int] [--ignore-term] [--busy-on-int]
//!   [--exit-on-yield]` is a fake GPU job.
//! - A serving mock registers itself (`--gpu-mem MiB`, default 1000).
//!
//! `ninfer-mock lease-client` is a gpu-switch lease client (see the fn).

use std::convert::Infallible;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use gpu_arbiter::http::{self, ProxyBody};
use http_body_util::BodyExt;
use hyper::body::{Frame, Incoming, SizeHint};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::json;

/// Requests refused for carrying deferred tools; see `messages`.
static REFUSED: AtomicU64 = AtomicU64::new(0);
const USAGE: &str = "\
usage: ninfer-mock [MODEL] [--host H] [--port N] [--health-delay-ms N] [--reply TEXT]
                   [--token-delay-ms N] [--error-status N] [--argv-file PATH]
                   [--exit-after-ms N] [--gpu-mem MiB] [other ninfer-serve flags are ignored]
       ninfer-mock gpu-hold [--ms N] [--mem MiB] [--sm PCT] [--sm-ms N] [--type T]
                            [--ignore-int] [--ignore-term] [--busy-on-int] [--exit-on-yield]
       ninfer-mock lease-client (--exclusive | --shared --vram BYTES) [--label L] [--priority N] [--hold-ms N]
                                [--group G] [--requeued] [--cooperative]
                                [--mem MiB] [--sm PCT] [--ignore-int]
       ninfer-mock nvidia-smi ...";

struct Opts {
    host: String,
    port: u16,
    health_delay: Duration,
    reply: String,
    token_delay: Duration,
    error_status: Option<StatusCode>,
    argv_file: Option<PathBuf>,
    exit_after: Option<Duration>,
    gpu_mem: u64,
}

fn parse(args: &[String]) -> Result<Opts, String> {
    let mut o = Opts {
        host: "127.0.0.1".into(),
        port: 8080,
        health_delay: Duration::ZERO,
        reply: "Hello from ninfer-mock.".into(),
        token_delay: Duration::ZERO,
        error_status: None,
        argv_file: None,
        exit_after: None,
        gpu_mem: 1000,
    };
    let ms = |v: &str| {
        v.parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|e| format!("{v}: {e}"))
    };
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if !a.starts_with("--") {
            continue;
        }
        let known = matches!(
            a.as_str(),
            "--host"
                | "--port"
                | "--health-delay-ms"
                | "--reply"
                | "--token-delay-ms"
                | "--error-status"
                | "--argv-file"
                | "--exit-after-ms"
                | "--gpu-mem"
        );
        if !known {
            continue;
        }
        let v = it.next().ok_or(format!("{a} needs a value"))?;
        match a.as_str() {
            "--host" => o.host = v.clone(),
            "--port" => o.port = v.parse().map_err(|e| format!("--port: {e}"))?,
            "--health-delay-ms" => o.health_delay = ms(v)?,
            "--reply" => o.reply = v.clone(),
            "--token-delay-ms" => o.token_delay = ms(v)?,
            "--error-status" => {
                let n: u16 = v.parse().map_err(|e| format!("--error-status: {e}"))?;
                o.error_status = Some(StatusCode::from_u16(n).map_err(|e| e.to_string())?);
            }
            "--argv-file" => o.argv_file = Some(PathBuf::from(v)),
            "--exit-after-ms" => o.exit_after = Some(ms(v)?),
            "--gpu-mem" => o.gpu_mem = v.parse().map_err(|e| format!("--gpu-mem: {e}"))?,
            _ => unreachable!(),
        }
    }
    Ok(o)
}

fn gpu_dir() -> Option<PathBuf> {
    std::env::var_os("NINFER_MOCK_GPU_DIR").map(PathBuf::from)
}

/// One fake GPU process: `mem=<MiB> sm=<pct> type=<C|G|C+G>` in
/// `$NINFER_MOCK_GPU_DIR/<pid>`; an empty file is `mem=0 sm=0 type=C`.
#[derive(Clone)]
struct GpuProc {
    mem: u64,
    sm: u32,
    kind: String,
}

impl GpuProc {
    fn parse(text: &str) -> GpuProc {
        let mut p = GpuProc {
            mem: 0,
            sm: 0,
            kind: "C".into(),
        };
        for kv in text.split_whitespace() {
            match kv.split_once('=') {
                Some(("mem", v)) => p.mem = v.parse().unwrap_or(0),
                Some(("sm", v)) => p.sm = v.parse().unwrap_or(0),
                Some(("type", v)) => p.kind = v.into(),
                _ => {}
            }
        }
        p
    }
    fn line(&self) -> String {
        format!("mem={} sm={} type={}", self.mem, self.sm, self.kind)
    }
}

/// Registry entry removed on drop (normal exit or SIGTERM path).
struct GpuClaim(Option<PathBuf>);

impl GpuClaim {
    fn new(p: &GpuProc) -> GpuClaim {
        GpuClaim(gpu_dir().map(|d| {
            let path = d.join(std::process::id().to_string());
            let _ = std::fs::create_dir_all(&d);
            let _ = std::fs::write(&path, p.line());
            path
        }))
    }
    fn set(&self, p: &GpuProc) {
        if let Some(path) = &self.0 {
            let _ = std::fs::write(path, p.line());
        }
    }
}

impl Drop for GpuClaim {
    fn drop(&mut self) {
        if let Some(p) = &self.0 {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// Live fake processes, `total` MiB (default 32768).
fn gpu_state() -> (Vec<(u32, GpuProc)>, u64) {
    let Some(dir) = gpu_dir() else {
        return (Vec::new(), 32768);
    };
    let total = std::fs::read_to_string(dir.join("total"))
        .ok()
        .and_then(|t| t.trim().parse().ok())
        .unwrap_or(32768);
    let mut procs = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            let Ok(pid) = name.parse::<u32>() else {
                continue;
            };
            let alive = std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .ok()
                .and_then(|s| s.get(s.rfind(')')? + 2..).map(|r| !r.starts_with('Z')))
                .unwrap_or(false);
            if alive {
                let text = std::fs::read_to_string(e.path()).unwrap_or_default();
                procs.push((pid, GpuProc::parse(&text)));
            } else {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }
    procs.sort_by_key(|(p, _)| *p);
    (procs, total)
}

/// The subset of nvidia-smi that gpu-switch uses.
fn nvidia_smi(args: &[String]) -> ExitCode {
    let (procs, total) = gpu_state();
    let mut out = std::io::stdout().lock();
    let compute = || procs.iter().filter(|(_, p)| p.kind.contains('C'));
    let has = |a: &str| args.iter().any(|x| x == a);
    if has("pmon") {
        let _ = writeln!(
            out,
            "# gpu         pid   type     sm    mem    enc    dec    jpg    ofa    command"
        );
        let _ = writeln!(
            out,
            "# Idx           #    C/G      %      %      %      %      %      %    name"
        );
        if procs.is_empty() {
            let _ = writeln!(
                out,
                "    0          -     -      -      -      -      -      -      -    -"
            );
        }
        for (pid, p) in &procs {
            let _ = writeln!(
                out,
                "    0 {pid:>10} {:>5} {:>6}      0      -      -      -      -    mock",
                p.kind, p.sm
            );
        }
    } else if has("--query-gpu=utilization.gpu") {
        let _ = writeln!(
            out,
            "{}",
            if procs.iter().any(|(_, p)| p.sm > 0) {
                50
            } else {
                0
            }
        );
    } else if has("--query-compute-apps=pid,used_memory") {
        for (pid, p) in compute() {
            let _ = writeln!(out, "{pid}, {}", p.mem);
        }
    } else if has("--query-compute-apps=pid") {
        for (pid, _) in compute() {
            let _ = writeln!(out, "{pid}");
        }
    } else if has("--query-gpu=memory.total,memory.free") {
        let used: u64 = procs.iter().map(|(_, p)| p.mem).sum();
        let _ = writeln!(out, "{total}, {}", total.saturating_sub(used));
    } else {
        eprintln!("ninfer-mock nvidia-smi: unsupported arguments {args:?}");
        return ExitCode::from(2);
    }
    ExitCode::SUCCESS
}

static INT_SEEN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

extern "C" fn on_int(_: libc::c_int) {
    INT_SEEN.store(true, std::sync::atomic::Ordering::SeqCst);
}

/// `gpu-hold`: a fake GPU job for policing and admission tests.
fn gpu_hold(args: &[String]) -> ExitCode {
    let mut ms: u64 = 0;
    let mut p = GpuProc {
        mem: 0,
        sm: 0,
        kind: "C".into(),
    };
    let mut sm_ms: Option<u64> = None;
    let mut ignore_int = false;
    let mut ignore_term = false;
    let mut busy_on_int = false;
    let mut exit_on_yield = false;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let mut num = || it.next().and_then(|v| v.parse::<u64>().ok());
        match a.as_str() {
            "--ms" => ms = num().unwrap_or(0),
            "--mem" => p.mem = num().unwrap_or(0),
            "--sm" => p.sm = num().unwrap_or(0) as u32,
            "--sm-ms" => sm_ms = num(),
            "--type" => p.kind = it.next().cloned().unwrap_or_else(|| "C".into()),
            "--ignore-int" => ignore_int = true,
            "--ignore-term" => ignore_term = true,
            "--busy-on-int" => busy_on_int = true,
            "--exit-on-yield" => exit_on_yield = true,
            _ => {
                eprintln!("{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    // SAFETY: installing simple handlers before any threads exist.
    unsafe {
        if ignore_term {
            libc::signal(libc::SIGTERM, libc::SIG_IGN);
        }
        if ignore_int || busy_on_int {
            libc::signal(libc::SIGINT, on_int as *const () as libc::sighandler_t);
        }
    }
    let claim = GpuClaim::new(&p);
    let started = Instant::now();
    let yield_file = std::env::var_os("ML_GPU_YIELD").map(PathBuf::from);
    loop {
        if ms > 0 && started.elapsed() >= Duration::from_millis(ms) {
            return ExitCode::SUCCESS;
        }
        if let Some(limit) = sm_ms {
            if p.sm > 0 && started.elapsed() >= Duration::from_millis(limit) {
                p.sm = 0;
                claim.set(&p);
            }
        }
        if INT_SEEN.swap(false, std::sync::atomic::Ordering::SeqCst) && busy_on_int {
            p.sm = 50;
            sm_ms = None;
            claim.set(&p);
        }
        if exit_on_yield && yield_file.as_ref().is_some_and(|f| f.exists()) {
            return ExitCode::from(76);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// `lease-client`: a gpu-switch lease client, the protocol of
/// tools/_gpu_switch.py. Prints every daemon line, then `granted`; delivers
/// `signal` lines to itself; releases on a stdin line, stdin EOF or after
/// `--hold-ms`. Under `$GPU_SWITCH_GRANTED` it prints `nested <id>` and
/// takes no grant.
fn lease_client(args: &[String]) -> ExitCode {
    use std::io::{BufRead, BufReader};
    let mut mode = None;
    let mut vram: Option<u64> = None;
    let mut label = String::from("mock");
    let mut priority: i64 = 0;
    let mut group: Option<String> = None;
    let mut requeued = false;
    let mut cooperative = false;
    let mut hold_ms: Option<u64> = None;
    let mut p = GpuProc {
        mem: 0,
        sm: 0,
        kind: "C".into(),
    };
    let mut ignore_int = false;
    let mut ignore_term = false;
    let mut shutdown_grace: Option<f64> = None;
    let mut expected_runtime: Option<i64> = None;
    let mut ack_shutdown = false;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--exclusive" => mode = Some("exclusive"),
            "--shared" => mode = Some("shared"),
            "--vram" => vram = it.next().and_then(|v| v.parse().ok()),
            "--label" => label = it.next().cloned().unwrap_or_default(),
            "--priority" => priority = it.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            "--group" => group = it.next().cloned(),
            "--requeued" => requeued = true,
            "--cooperative" => cooperative = true,
            "--hold-ms" => hold_ms = it.next().and_then(|v| v.parse().ok()),
            "--mem" => p.mem = it.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            "--sm" => p.sm = it.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            "--ignore-int" => ignore_int = true,
            "--ignore-term" => ignore_term = true,
            "--shutdown-grace" => shutdown_grace = it.next().and_then(|v| v.parse().ok()),
            "--expected-secs" => expected_runtime = it.next().and_then(|v| v.parse().ok()),
            "--ack-shutdown" => ack_shutdown = true,
            _ => {
                eprintln!("{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    let Some(mode) = mode else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    if ignore_int {
        // SAFETY: before any threads.
        unsafe { libc::signal(libc::SIGINT, libc::SIG_IGN) };
    }
    if ignore_term {
        unsafe {
            libc::signal(libc::SIGTERM, libc::SIG_IGN);
        }
    }
    let out = |line: &str| {
        let mut o = std::io::stdout().lock();
        let _ = writeln!(o, "{line}");
        let _ = o.flush();
    };
    let hold = |claim: &GpuClaim| {
        let _ = claim;
        match hold_ms {
            Some(ms) => std::thread::sleep(Duration::from_millis(ms)),
            None => {
                let mut l = String::new();
                let _ = std::io::stdin().lock().read_line(&mut l);
            }
        }
    };
    if let Ok(id) = std::env::var("GPU_SWITCH_GRANTED") {
        out(&format!("nested {id}"));
        let claim = GpuClaim::new(&p);
        hold(&claim);
        return ExitCode::SUCCESS;
    }
    let Some(socket) = std::env::var_os("GPU_SWITCH_SOCKET") else {
        eprintln!("lease-client: GPU_SWITCH_SOCKET unset");
        return ExitCode::from(2);
    };
    let mut stream = match std::os::unix::net::UnixStream::connect(&socket) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("lease-client: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut req = json!({"op": "lease", "mode": mode, "label": label, "priority": priority,
                         "group": group, "requeued": requeued, "cooperative": cooperative});
    req["shutdown_grace_secs"] = json!(shutdown_grace);
    req["expected_runtime_secs"] = json!(expected_runtime);
    if let Some(v) = vram {
        req["vram"] = json!(v);
    }
    if writeln!(stream, "{req}").is_err() {
        return ExitCode::FAILURE;
    }
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let reader = stream.try_clone().expect("socket clone");
    let mut acknowledger = stream.try_clone().expect("socket clone");
    std::thread::spawn(move || {
        for line in BufReader::new(reader).lines().map_while(Result::ok) {
            {
                let mut o = std::io::stdout().lock();
                let _ = writeln!(o, "{line}");
                let _ = o.flush();
            }
            let v: serde_json::Value = serde_json::from_str(&line).unwrap_or_default();
            if v["granted"] == true {
                let _ = tx.send(());
            }
            if ack_shutdown && v["shutdown"]["request_id"].is_string() {
                let _ = writeln!(
                    acknowledger,
                    "{}",
                    json!({"op": "shutdown_started",
                    "request_id": v["shutdown"]["request_id"]})
                );
            }
            let sig = match v["signal"].as_str() {
                Some("INT") => libc::SIGINT,
                Some("TERM") => libc::SIGTERM,
                Some("KILL") => libc::SIGKILL,
                _ => continue,
            };
            // SAFETY: signal to ourselves.
            unsafe { libc::kill(libc::getpid(), sig) };
        }
    });
    if rx.recv().is_err() {
        out("closed");
        return ExitCode::FAILURE;
    }
    let claim = GpuClaim::new(&p);
    out("granted");
    hold(&claim);
    let _ = writeln!(stream, "{}", json!({"op": "release"}));
    drop(claim);
    out("released");
    ExitCode::SUCCESS
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().collect();
    let base = argv
        .first()
        .and_then(|a| a.rsplit('/').next())
        .unwrap_or("");
    if base == "nvidia-smi" {
        return nvidia_smi(&argv[1..]);
    }
    if argv.get(1).is_some_and(|a| a == "nvidia-smi") {
        return nvidia_smi(&argv[2..]);
    }
    if argv.get(1).is_some_and(|a| a == "lease-client") {
        return lease_client(&argv[2..]);
    }
    if argv.get(1).is_some_and(|a| a == "gpu-hold") {
        return gpu_hold(&argv[2..]);
    }
    if argv.get(1).is_some_and(|a| a == "-h" || a == "--help") {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let opts = match parse(&argv[1..]) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("ninfer-mock: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    if let Some(f) = &opts.argv_file {
        let line = serde_json::to_string(&argv).expect("serializable");
        let appended = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(f)
            .and_then(|mut file| writeln!(file, "{line}"));
        if let Err(e) = appended {
            eprintln!("ninfer-mock: {}: {e}", f.display());
            return ExitCode::FAILURE;
        }
    }
    let _claim = GpuClaim::new(&GpuProc {
        mem: opts.gpu_mem,
        sm: 30,
        kind: "C".into(),
    });
    rt.block_on(serve(Arc::new(opts)))
}

async fn serve(opts: Arc<Opts>) -> ExitCode {
    let listener = match tokio::net::TcpListener::bind((opts.host.as_str(), opts.port)).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("ninfer-mock: bind {}:{}: {e}", opts.host, opts.port);
            return ExitCode::FAILURE;
        }
    };
    let started = Instant::now();
    let mut term =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("signal");
    let exit_after = async {
        match opts.exit_after {
            Some(d) => tokio::time::sleep(d).await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(exit_after);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue };
                let opts = opts.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |req| {
                        let opts = opts.clone();
                        async move { Ok::<_, Infallible>(handle(&opts, started, req).await) }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
            _ = term.recv() => return ExitCode::SUCCESS,
            _ = &mut exit_after => {
                eprintln!("ninfer-mock: --exit-after-ms reached, exiting with 3");
                return ExitCode::from(3);
            }
        }
    }
}

async fn handle(opts: &Opts, started: Instant, req: Request<Incoming>) -> Response<ProxyBody> {
    let ready = started.elapsed() >= opts.health_delay;
    let echo = req.headers().get("x-mock-echo").cloned();
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|p| p.to_string())
        .unwrap_or_default();
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let body = req
        .into_body()
        .collect()
        .await
        .map(|b| b.to_bytes())
        .unwrap_or_default();
    let mut resp = route(opts, ready, &method, &path, &body).await;
    let h = resp.headers_mut();
    h.insert("x-mock-path", path_and_query.parse().expect("valid header"));
    if let Some(v) = echo {
        h.insert("x-mock-echo", v);
    }
    resp
}

async fn route(
    opts: &Opts,
    ready: bool,
    method: &Method,
    path: &str,
    body: &Bytes,
) -> Response<ProxyBody> {
    if path == "/health" && method == Method::GET {
        return if ready {
            http::json_response(StatusCode::OK, &json!({"status": "ok"}))
        } else {
            http::json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                &json!({"status": "loading"}),
            )
        };
    }
    if !ready {
        return http::json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &http::anthropic_error_body("api_error", "model loading"),
        );
    }
    match (method, path) {
        (&Method::GET, "/v1/models") => http::json_response(
            StatusCode::OK,
            &json!({
                "data": [{"type": "model", "id": "ninfer-mock", "display_name": "ninfer-mock",
                          "created_at": "2026-01-01T00:00:00Z"}],
                "has_more": false, "first_id": "ninfer-mock", "last_id": "ninfer-mock",
            }),
        ),
        (&Method::POST, "/v1/messages/count_tokens") => {
            http::json_response(StatusCode::OK, &json!({"input_tokens": body.len() / 4}))
        }
        (&Method::POST, "/v1/messages") => messages(opts, body),
        // How many requests it refused as ninfer-serve would, for tests.
        (&Method::GET, "/mock/refused") => http::json_response(
            StatusCode::OK,
            &json!({"refused": REFUSED.load(Ordering::SeqCst)}),
        ),
        _ => http::json_response(
            StatusCode::NOT_FOUND,
            &http::anthropic_error_body("not_found_error", &format!("no route {method} {path}")),
        ),
    }
}

fn messages(opts: &Opts, body: &Bytes) -> Response<ProxyBody> {
    let req: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            return http::json_response(
                StatusCode::BAD_REQUEST,
                &http::anthropic_error_body("invalid_request_error", &format!("bad json: {e}")),
            )
        }
    };
    if let Some(status) = opts.error_status {
        return http::json_response(
            status,
            &http::anthropic_error_body(
                http::error_type_for_status(status),
                &format!("ninfer-mock forced HTTP {}", status.as_u16()),
            ),
        );
    }
    // As ninfer-serve, which has no tool search.
    let deferred = req
        .get("tools")
        .and_then(|t| t.as_array())
        .is_some_and(|tools| {
            tools
                .iter()
                .any(|t| t.get("defer_loading") == Some(&json!(true)))
        });
    if deferred {
        REFUSED.fetch_add(1, Ordering::SeqCst);
        return http::json_response(
            StatusCode::BAD_REQUEST,
            &http::anthropic_error_body(
                "invalid_request_error",
                "deferred tool loading is not supported",
            ),
        );
    }
    let model = req
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or("ninfer-mock")
        .to_string();
    let stream = req.get("stream").and_then(|s| s.as_bool()).unwrap_or(false);
    let tokens: Vec<String> = opts
        .reply
        .split_inclusive(' ')
        .map(str::to_string)
        .collect();
    let id = format!("msg_mock_{}", std::process::id());
    if !stream {
        return http::json_response(
            StatusCode::OK,
            &json!({
                "id": id, "type": "message", "role": "assistant", "model": model,
                "content": [{"type": "text", "text": opts.reply}],
                "stop_reason": "end_turn", "stop_sequence": null,
                "usage": {"input_tokens": body.len() / 4, "output_tokens": tokens.len()},
            }),
        );
    }
    let frame = |event: &str, data: serde_json::Value| -> Bytes {
        Bytes::from(format!("event: {event}\ndata: {data}\n\n"))
    };
    let mut events = vec![
        frame(
            "message_start",
            json!({"type": "message_start", "message": {
                "id": id, "type": "message", "role": "assistant", "model": model, "content": [],
                "stop_reason": null, "stop_sequence": null,
                "usage": {"input_tokens": body.len() / 4, "output_tokens": 0}}}),
        ),
        frame(
            "content_block_start",
            json!({"type": "content_block_start", "index": 0,
                   "content_block": {"type": "text", "text": ""}}),
        ),
    ];
    let first_delta = events.len();
    for t in &tokens {
        events.push(frame(
            "content_block_delta",
            json!({"type": "content_block_delta", "index": 0,
                   "delta": {"type": "text_delta", "text": t}}),
        ));
    }
    events.push(frame(
        "content_block_stop",
        json!({"type": "content_block_stop", "index": 0}),
    ));
    events.push(frame(
        "message_delta",
        json!({"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": null},
               "usage": {"output_tokens": tokens.len()}}),
    ));
    events.push(frame("message_stop", json!({"type": "message_stop"})));

    let (tx, out) = http::channel_body(SizeHint::default());
    let delay = opts.token_delay;
    tokio::spawn(async move {
        for (n, ev) in events.into_iter().enumerate() {
            if n >= first_delta && !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            if tx.send(Ok(Frame::data(ev))).await.is_err() {
                return;
            }
        }
    });
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(out)
        .expect("static response")
}
