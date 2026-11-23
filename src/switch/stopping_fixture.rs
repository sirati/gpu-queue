//! Test-only (PLAN-L §5; AMEND-L6-4 §5): the real service, a holder and the test as three processes in separate trees,
//! in an order the test fixes by acknowledgements. The test process T re-executes itself into the daemon role D and
//! the holder role H, owns both by PID, and talks to each over its own Unix stream, one JSON line per acknowledgement
//! or command. A daemon cannot kill a holder in its own tree, so the kill controls need these sibling processes.
//! Every wait is an event wait; the run's scope is the hang guard. T writes each acknowledgement's name to stderr as
//! it arrives, so the last one names the seam of a run that never ended.

use std::ffi::OsStr;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::task::{Poll, Waker};
use std::time::Duration;

use serde_json::{json, Value};

use crate::process_identity::Instance;
use crate::protocol::{JobSpec, Mode, Request, RunnerReport, SubmitRequest};
use crate::roles::Role;
use crate::switch::card_reader::tests::{timeout, Act, Call, Script};
use crate::switch::config::SwitchConfig;
use crate::switch::control::tests::{spec, StateDir};
use crate::switch::control::Role as Listening;
use crate::switch::daemon::Daemon;
use crate::switch::departure::{Teardown, Verdict};
use crate::switch::hooks::{Choice, Hold, Settlement};
use crate::switch::lifecycle::Life;
use crate::switch::service;
use crate::switch::sibling::{Link, Sibling};

/// The payload of a read that the fixture makes panic.
const PANIC: &str = "the read panicked in the stopping fixture";
const CANCELLED: &str = "gpu-switch's runtime cancelled the read before it ran";
const UNVERIFIED: &str = "teardown unverified: gpu-switch was stopping";
const RETAINED: &str = "lease connection closed while GPU VRAM remained allocated";
const ABANDONED: &str = "its client did not come back after gpu-switch restarted";
/// The two roles, by the test path each is started as and names its line by.
const DAEMON_ROLE: &str = "switch::stopping_fixture::daemon_role";
const HOLDER_ROLE: &str = "switch::stopping_fixture::holder_role";

fn env(key: &str) -> String {
    std::env::var(key).unwrap_or_default()
}

// ---- the daemon role, D ----

/// Order (iii)'s pre-dispatch rendezvous: `Pending` until T's wake arrives from the controller thread, outside the
/// runtime; then the poll blocks its thread, not the runtime, until T releases it.
#[derive(Default)]
struct Rendezvous {
    ready: AtomicBool,
    waker: Mutex<Option<Waker>>,
    released: Mutex<bool>,
    release: Condvar,
}

impl Rendezvous {
    fn wake(&self) {
        self.ready.store(true, Ordering::Release);
        let waker = self.waker.lock().unwrap_or_else(PoisonError::into_inner).take();
        waker.into_iter().for_each(Waker::wake);
    }

    fn release(&self) {
        *self.released.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.release.notify_all();
    }

    fn hold(&self) {
        let mut released = self.released.lock().unwrap_or_else(PoisonError::into_inner);
        while !*released {
            released = self.release.wait(released).unwrap_or_else(PoisonError::into_inner);
        }
    }
}

/// Order (iii)'s held poll: it acknowledges `held` with its budget, then blocks its thread, not the runtime, until T
/// releases it. The hold runs in `block_in_place`, which hands the thread's scheduler core, with the task in its LIFO
/// slot, to another thread, so the runtime's other tasks keep running. It needs a multi-threaded runtime.
struct Meeting {
    rendezvous: Arc<Rendezvous>,
    link: Link,
    id: u64,
    started_ns: u64,
    announced: bool,
}

impl std::future::Future for Meeting {
    type Output = ();

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        let rendezvous = self.rendezvous.clone();
        if !rendezvous.ready.load(Ordering::Acquire) {
            *rendezvous.waker.lock().unwrap_or_else(PoisonError::into_inner) = Some(cx.waker().clone());
            if !self.announced {
                self.announced = true;
                let (id, started_ns) = (self.id, self.started_ns);
                self.link.send(json!({"ack": "rendezvous", "id": id, "pass": 1, "started_ns": started_ns}));
            }
            if !rendezvous.ready.load(Ordering::Acquire) {
                return Poll::Pending;
            }
        }
        self.link.send(json!({"ack": "held", "budget": tokio::task::coop::has_budget_remaining()}));
        tokio::task::block_in_place(|| rendezvous.hold());
        Poll::Ready(())
    }
}

/// Settlement's points in D, acknowledged to T.
struct Points {
    link: Link,
    rendezvous: Option<Arc<Rendezvous>>,
    hold_choice: bool,
    chosen: AtomicBool,
    choice_released: Arc<tokio::sync::Notify>,
}

fn verdict_word(verdict: &Verdict) -> (&'static str, Option<&str>) {
    match verdict {
        Verdict::Wait => ("wait", None),
        Verdict::Conclude => ("conclude", None),
        Verdict::Enforce(reason) => ("enforce", Some(reason)),
        Verdict::EndUnverified(cause) => ("end_unverified", Some(cause)),
    }
}

fn teardown_word(teardown: &Teardown) -> &'static str {
    match teardown {
        Teardown::NothingToVerify => "nothing_to_verify",
        Teardown::Released => "released",
        Teardown::Retained => "retained",
        Teardown::Unverified(_) => "unverified",
        Teardown::Panicked(_) => "panicked",
    }
}

fn life_word(life: Life) -> &'static str {
    match life {
        Life::Serving => "serving",
        Life::Stopping => "stopping",
    }
}

impl Settlement for Points {
    fn dispatch(&self, id: u64, started_ns: u64, pass: u32) -> Hold {
        match (&self.rendezvous, pass) {
            (Some(rendezvous), 1) => Box::pin(Meeting {
                rendezvous: rendezvous.clone(),
                link: self.link.clone(),
                id,
                started_ns,
                announced: false,
            }),
            _ => {
                self.link.send(json!({"ack": "dispatch", "id": id, "pass": pass, "started_ns": started_ns}));
                Box::pin(std::future::ready(()))
            }
        }
    }

    fn choice(&self, id: u64, choice: Choice) -> Hold {
        let (verdict, text) = verdict_word(&choice.verdict);
        self.link.send(json!({"ack": "choice", "id": id, "verdict": verdict, "text": text,
            "teardown": teardown_word(&choice.teardown), "life": life_word(choice.life),
            "job_shutdown": choice.job_shutdown, "since_ms": choice.since_ms as u64}));
        match self.hold_choice && !self.chosen.swap(true, Ordering::AcqRel) {
            true => {
                let released = self.choice_released.clone();
                Box::pin(async move { released.notified().await })
            }
            false => Box::pin(std::future::ready(())),
        }
    }

    fn end(&self, id: u64, returned: bool) {
        self.link.send(json!({"ack": "end", "id": id, "returned": returned}));
    }
}

/// The write end of D's SIGTERM pipe.
static TERM_PIPE: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(-1);

extern "C" fn on_term(_: libc::c_int) {
    let byte = 1u8;
    // SAFETY: write(2) is async-signal-safe; the pipe outlives the process's handlers.
    unsafe { libc::write(TERM_PIPE.load(Ordering::Relaxed), (&byte as *const u8).cast(), 1) };
}

/// D's SIGTERM as a future completed from outside the runtime. A handler writes to a pipe, and a thread of D's own
/// completes the stop, as the controller thread wakes the rendezvous. So the stop does not depend on the runtime
/// driving Tokio's signal stream while order (iii) holds a thread mid-poll.
fn terminated() -> tokio::sync::oneshot::Receiver<()> {
    let mut fds = [0; 2];
    // SAFETY: a pipe and a handler installed once, before T can signal D.
    unsafe {
        assert_eq!(libc::pipe(fds.as_mut_ptr()), 0);
        TERM_PIPE.store(fds[1], Ordering::Relaxed);
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = on_term as usize;
        libc::sigemptyset(&mut action.sa_mask);
        action.sa_flags = libc::SA_RESTART;
        assert_eq!(libc::sigaction(libc::SIGTERM, &action, std::ptr::null_mut()), 0);
    }
    let (stopped, stop) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let mut byte = 0u8;
        // SAFETY: a blocking read of one byte into a local.
        unsafe { libc::read(fds[0], (&mut byte as *mut u8).cast(), 1) };
        let _ = stopped.send(());
    });
    stop
}

/// What T's answer makes every held read do.
fn act(command: &Value, script: &Script) -> Act {
    match command["act"].as_str() {
        Some("timeout") => Act::Fail(timeout()),
        Some("panic") => Act::Panic(PANIC),
        Some("read") => {
            let pids = command["pids"].as_array().expect("pids");
            let compute = pids
                .iter()
                .map(|pair| (pair[0].as_u64().unwrap() as u32, pair[1].as_u64()))
                .collect();
            script.answers(compute, Some((24 << 30, 20 << 30)));
            Act::Answer
        }
        other => panic!("no such answer: {other:?}"),
    }
}

/// D: the real service on a scripted card, with settlement's points and its own SIGTERM. It returns at once without
/// its environment.
#[test]
#[ignore]
fn daemon_role() {
    let Some(dir) = std::env::var_os("GQ_L6_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let order = env("GQ_L6_ORDER");
    let (link, mut inbox) = Link::connect(DAEMON_ROLE);
    let script = Script::new();
    let grace = env("GQ_L6_GRACE").parse::<f64>().unwrap_or(60.0);
    let config: SwitchConfig = serde_json::from_value(json!({
        "listen": [{"name": "host", "path": dir.join("host.sock")}],
        "state_dir": dir, "startup_idle_secs": 3600.0, "idle_secs": 3600.0, "reattach_grace_secs": grace,
    }))
    .expect("the role's configuration");
    let d = Daemon::reading(config, dir.join("switch.json"), Some(script.driver()), Instance::current().unwrap(), crate::clock::Clock::system())
        .expect("the role's daemon");
    let rendezvous = Arc::new(Rendezvous::default());
    let choice_released = Arc::new(tokio::sync::Notify::new());
    let points = Points {
        link: link.clone(),
        rendezvous: (order == "cancelled").then(|| rendezvous.clone()),
        hold_choice: env("GQ_L6_HOLD_CHOICE") == "1",
        chosen: AtomicBool::new(false),
        choice_released: choice_released.clone(),
    };
    assert!(d.hooks.settlement.set(Box::new(points)).is_ok());
    if env("GQ_L6_ARM") == "1" {
        script.always(Call::Compute, Act::Hold);
    }
    let (release_save, save_released) = std::sync::mpsc::channel();
    std::thread::spawn({
        let (link, script) = (link.clone(), script.clone());
        move || loop {
            let command = inbox.next();
            match command["cmd"].as_str() {
                Some("arm") => {
                    script.always(Call::Compute, Act::Hold);
                    link.send(json!({"ack": "armed"}));
                }
                Some("answer") => script.answer_held(Call::Compute, act(&command, &script)),
                Some("wake") => rendezvous.wake(),
                Some("release_hold") => rendezvous.release(),
                Some("release_choice") => choice_released.notify_one(),
                Some("release_save") => release_save.send(()).expect("the stop waits"),
                Some("exit") => std::process::exit(0),
                other => panic!("no such command: {other:?}"),
            }
        }
    });
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    let std_listener = crate::switch::bind(&dir.join("host.sock")).expect("the host socket");
    std_listener.set_nonblocking(true).unwrap();
    let listener = {
        let _entered = runtime.enter();
        tokio::net::UnixListener::from_std(std_listener).unwrap()
    };
    let _armed = service::shutdown::arm(
        &d,
        {
            let link = link.clone();
            move |entry| {
                link.send(json!({"ack": "save", "invocation": entry.invocation, "life": life_word(entry.life)}))
            }
        },
        (env("GQ_L6_HOLD_SAVE") == "1").then_some(save_released),
    );
    let stop = terminated();
    link.send(json!({"ack": "ready"}));
    runtime.block_on(service::serve(d.clone(), vec![(listener, Listening::Host, "host".into())], async move {
        let _ = stop.await;
    }))
    .expect("the daemon role's saved state restores");
    match order.as_str() {
        // A held worker would make the runtime's drop wait forever: D shuts down in the background and exits on T's
        // word, after its hooks wrote their acknowledgements.
        "cancelled" => {
            runtime.shutdown_background();
            link.send(json!({"ack": "shutdown"}));
            loop {
                std::thread::park();
            }
        }
        _ => drop(runtime),
    }
}

// ---- the holder role, H ----

fn lease_request() -> crate::protocol::LeaseRequest {
    serde_json::from_value(json!({"mode": "exclusive", "expected_runtime_secs": 60, "label": "l6"})).unwrap()
}

/// H: a lease client outside D's tree. `raw` holds a test-owned protocol session on one stream, which closes without
/// `Release` or waits for D's EOF; `lease` holds the crate's `Lease` and releases it. It returns at once without its
/// environment.
#[test]
#[ignore]
fn holder_role() {
    let Some(_) = std::env::var_os("GQ_L6_HOLDER") else {
        return;
    };
    let (link, mut inbox) = Link::connect(HOLDER_ROLE);
    if env("GQ_L6_IGNORE_TERM") == "1" {
        // SAFETY: setting a signal's disposition to ignore has no handler to race with.
        unsafe { libc::signal(libc::SIGTERM, libc::SIG_IGN) };
    }
    let socket = PathBuf::from(env("GQ_L6_SOCKET"));
    match env("GQ_L6_HOLDER").as_str() {
        "raw" => {
            let stream = UnixStream::connect(&socket).expect("D's socket");
            let mut line = serde_json::to_vec(&Request::Lease(lease_request())).unwrap();
            line.push(b'\n');
            (&stream).write_all(&line).unwrap();
            link.send(json!({"ack": "wrote", "lines": 1}));
            let mut lines = BufReader::new(stream.try_clone().unwrap()).lines();
            let mut job = Value::Null;
            for line in lines.by_ref() {
                let message: Value = serde_json::from_str(&line.unwrap()).unwrap();
                job = message.get("job").cloned().unwrap_or(job);
                if message["granted"] == true {
                    break;
                }
            }
            link.send(json!({"ack": "granted", "id": job["id"], "handle": job["handle"]}));
            std::thread::spawn({
                let link = link.clone();
                move || {
                    lines.map_while(Result::ok).for_each(drop);
                    link.send(json!({"ack": "eof"}));
                }
            });
            loop {
                let command = inbox.next();
                assert_eq!(command["cmd"], "close");
                let _ = stream.shutdown(std::net::Shutdown::Both);
                link.send(json!({"ack": "closed"}));
            }
        }
        "lease" => {
            let mut lease = crate::lease::Lease::acquire(&socket, lease_request(), |_| {}).expect("the grant");
            link.send(json!({"ack": "granted", "id": lease.id}));
            loop {
                let command = inbox.next();
                assert_eq!(command["cmd"], "release");
                lease.release();
                link.send(json!({"ack": "released"}));
            }
        }
        other => panic!("no such holder: {other}"),
    }
}

// ---- the test process, T ----

/// The orders of T-22 and T-23.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Order {
    /// (i): the runtime alive, an unanswered read answered after the publication.
    Live,
    /// (ii): the same with a panic.
    Panic,
    /// (iii): D13's order: the dispatch cancelled by the runtime's shutdown.
    Cancelled,
    /// (iv): a choice that read `Serving`, held across the publication.
    ServingChoice,
    /// T-23 (a) to (e): a timeout while serving.
    Serving,
    /// T-23 (f): a panic while serving.
    ServingPanic,
    /// T-23 (g): a verified retention while stopping.
    Retained,
}

/// The callers of `settle`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Caller {
    /// (a) a lease after EOF, through H's raw session.
    Eof,
    /// (b) a lease whose release was answered, through the crate's `Lease`.
    Release,
    /// (c) a runner's `Finished`, with H as T's direct child.
    Finished,
    /// (d) a runner session's tail after T closed its connection.
    RunnerTail,
    /// (e) an abandoned detached job, restored from a predecessor's snapshot.
    Abandoned,
}

impl Order {
    fn word(self) -> &'static str {
        match self {
            Order::Cancelled => "cancelled",
            Order::Live | Order::Panic | Order::ServingChoice | Order::Serving | Order::ServingPanic | Order::Retained => {
                "live"
            }
        }
    }

    /// The stop's save is held, so the settlement's choice falls between the publication and the save's end.
    fn holds_save(self) -> bool {
        matches!(self, Order::Live | Order::Panic | Order::ServingChoice | Order::Retained)
    }

    fn stopping(self) -> bool {
        !matches!(self, Order::Serving | Order::ServingPanic)
    }

    /// Settlement returns to its caller on a live runtime, which finalizes the job and answers a runner's `Finished`
    /// with `Done`. (iii)'s settling task is mid-poll while the runtime shuts down, so it may be dropped before its
    /// caller resumes: it promises no `Done` (PLAN-L section 3.10.3).
    fn promises_done(self) -> bool {
        !matches!(self, Order::Cancelled)
    }
}

/// How the holder ended, as T's `waitpid` saw it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HolderEnd {
    Alive,
    Killed(i32),
    Exited(Option<i32>),
}

/// Everything a run observed.
#[derive(Debug)]
pub(crate) struct Outcome {
    pub choices: Vec<Value>,
    pub end: Value,
    pub saves: Vec<Value>,
    pub daemon: Vec<Value>,
    pub holder: Vec<Value>,
    pub holder_end: HolderEnd,
    pub journal: Vec<Value>,
    pub log: String,
    /// The holder's pid: the job's `pid_host`, by which the journal names it.
    pub holder_pid: u32,
    pub done: bool,
    pub hold_budget: Option<bool>,
}

impl Outcome {
    /// The run's whole record, for a failed assertion: the choices, the end, the holder's end, the kills and the endings.
    pub fn record(&self) -> String {
        let terminal: Vec<(&Value, &Value, &Value)> = self
            .records("terminal_failure")
            .into_iter()
            .map(|record| (&record["state"], &record["terminal_status"], &record["reason"]))
            .collect();
        let choices: Vec<(&Value, &Value, &Value)> =
            self.choices.iter().map(|choice| (&choice["verdict"], &choice["life"], &choice["text"])).collect();
        format!(
            "choices {choices:?}; end {}; holder {:?}; unclean_release {}; terminal_failure {terminal:?}; done {}",
            self.end,
            self.holder_end,
            self.records("unclean_release").len(),
            self.done
        )
    }

    /// The journal's records of the run's job.
    pub fn records(&self, event: &str) -> Vec<&Value> {
        self.journal
            .iter()
            .filter(|record| record["event"] == event && record["pid"] == self.holder_pid)
            .collect()
    }
}

/// The holder of a run, and how T reaps it.
enum Holder {
    Role(Sibling),
    Runner { child: Role, stream: UnixStream, lines: BufReader<UnixStream> },
}

impl Holder {
    fn reap(&mut self) -> HolderEnd {
        let status = match self {
            Holder::Role(holder) => holder.status(),
            Holder::Runner { child, .. } => wait(child),
        };
        match (status.signal(), status.code()) {
            (Some(signal), _) => HolderEnd::Killed(signal),
            (None, code) => HolderEnd::Exited(code),
        }
    }

    fn alive(&mut self) -> bool {
        match self {
            Holder::Role(holder) => holder.running(),
            Holder::Runner { child, .. } => child.running(),
        }
    }

    fn pid(&self) -> u32 {
        match self {
            Holder::Role(holder) => holder.pid(),
            Holder::Runner { child, .. } => child.pid(),
        }
    }
}

/// Waits for a child without giving up its owner.
fn wait(role: &mut Role) -> std::process::ExitStatus {
    role.status()
}

struct Run {
    dir: StateDir,
}

impl Run {
    fn new() -> Run {
        Run { dir: StateDir::new() }
    }

    fn socket(&self) -> PathBuf {
        self.dir.path().join("host.sock")
    }

    fn daemon(&self, log: &str, order: &str, extra: &[(&str, &str)]) -> Sibling {
        let mut env: Vec<(&str, &OsStr)> = vec![
            ("GQ_L6_DIR", self.dir.path().as_os_str()),
            ("GQ_L6_ORDER", OsStr::new(order)),
        ];
        env.extend(extra.iter().map(|(key, value)| (*key, OsStr::new(*value))));
        let mut daemon = Sibling::logged(DAEMON_ROLE, &env, &self.dir.path().join(log));
        daemon.expect("ready");
        daemon
    }

    fn holder(&self, kind: &str, ignore_term: bool) -> Sibling {
        let socket = self.socket();
        let env: Vec<(&str, &OsStr)> = vec![
            ("GQ_L6_SOCKET", socket.as_os_str()),
            ("GQ_L6_HOLDER", OsStr::new(kind)),
            ("GQ_L6_IGNORE_TERM", OsStr::new(if ignore_term { "1" } else { "0" })),
        ];
        Sibling::logged(HOLDER_ROLE, &env, &self.dir.path().join("h.log"))
    }

    /// T as the authenticated runner: it submits, waits for `Go`, starts H as its direct child and reports it.
    fn runner(&self) -> Holder {
        let stream = UnixStream::connect(self.socket()).unwrap();
        let mut lines = BufReader::new(stream.try_clone().unwrap());
        let submit = Request::Submit(SubmitRequest {
            name: Some("l6".into()),
            cmd: vec!["sleep".into(), "3600".into()],
            cwd: "/".into(),
            spec: JobSpec { mode: Mode::Exclusive, vram: 0, ..spec(None) },
            log_dir: self.dir.path().to_string_lossy().into_owned(),
            resume: None,
        });
        send_line(&stream, &serde_json::to_value(&submit).unwrap());
        let reply = read_line(&mut lines);
        assert!(reply["job"]["handle"].is_string(), "a submitted job: {reply}");
        while read_line(&mut lines)["event"] != "go" {}
        let mut sleeper = std::process::Command::new("sleep");
        sleeper.arg("3600");
        let child = Role::spawn(sleeper);
        send_line(&stream, &serde_json::to_value(RunnerReport::Started { pid: child.pid() }).unwrap());
        Holder::Runner { child, stream, lines }
    }

    /// The predecessor D0 grants H's raw session and stops with the job active; H keeps its socket through D0's
    /// save and runtime drop and acknowledges D0's EOF. The snapshot's job then carries a terminal status.
    fn predecessor(&self) -> Holder {
        let mut d0 = self.daemon("d0.log", "predecessor", &[]);
        let mut holder = self.holder("raw", true);
        let handle = holder.expect("granted")["handle"].as_str().unwrap().to_string();
        // SAFETY: a signal to the child T owns, by its pid.
        unsafe { libc::kill(d0.pid() as i32, libc::SIGTERM) };
        d0.expect("save");
        holder.expect("eof");
        assert!(d0.status().success(), "D0 exits 0");
        let file = self.dir.path().join("state.json");
        let mut state: Value = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
        for side in state["sides"].as_array_mut().unwrap() {
            for job in side["jobs"].as_array_mut().unwrap() {
                if job["handle"] == handle.as_str() {
                    job["terminal_status"] = json!("no_vram");
                }
            }
        }
        let temporary = self.dir.path().join("state.json.tmp");
        std::fs::write(&temporary, serde_json::to_vec(&state).unwrap()).unwrap();
        std::fs::rename(&temporary, &file).unwrap();
        Holder::Role(holder)
    }
}

fn send_line(stream: &UnixStream, value: &Value) {
    let mut line = serde_json::to_vec(value).unwrap();
    line.push(b'\n');
    let mut writer = stream;
    writer.write_all(&line).unwrap();
}

fn read_line(lines: &mut BufReader<UnixStream>) -> Value {
    let mut line = String::new();
    assert!(lines.read_line(&mut line).unwrap() > 0, "the runner's connection closed");
    serde_json::from_str(&line).unwrap()
}

/// Sleeps until 250 ms have passed since `started_ns` on the kernel's monotonic clock: a lower bound that load only
/// lengthens.
fn past_escalation(started_ns: u64) {
    let due = started_ns + 250_000_000;
    let now = crate::clock::Clock::system().monotonic_ns();
    std::thread::sleep(Duration::from_nanos(due.saturating_sub(now)));
}

/// One run of `order` on `caller`, observed whole.
pub(crate) fn scenario(order: Order, caller: Caller) -> Outcome {
    let run = Run::new();
    let predecessor = (caller == Caller::Abandoned).then(|| run.predecessor());
    let mut extra = vec![];
    extra.extend(order.holds_save().then_some(("GQ_L6_HOLD_SAVE", "1")));
    extra.extend((order == Order::ServingChoice).then_some(("GQ_L6_HOLD_CHOICE", "1")));
    extra.extend((caller == Caller::Abandoned).then_some(("GQ_L6_ARM", "1")));
    extra.extend((caller == Caller::Abandoned).then_some(("GQ_L6_GRACE", "0.3")));
    let mut d = run.daemon("d.log", order.word(), &extra);
    let mut holder = match (caller, predecessor) {
        (Caller::Abandoned, Some(predecessor)) => predecessor,
        (Caller::Eof | Caller::Release, _) => {
            let kind = match caller {
                Caller::Eof => "raw",
                _ => "lease",
            };
            let mut holder = run.holder(kind, false);
            holder.expect("granted");
            Holder::Role(holder)
        }
        (Caller::Finished | Caller::RunnerTail, _) => run.runner(),
        (Caller::Abandoned, None) => unreachable!("the predecessor ran"),
    };
    if caller != Caller::Abandoned {
        d.send(json!({"cmd": "arm"}));
        d.expect("armed");
    }
    match &mut holder {
        Holder::Role(holder) if caller == Caller::Eof => holder.send(json!({"cmd": "close"})),
        Holder::Role(holder) if caller == Caller::Release => holder.send(json!({"cmd": "release"})),
        Holder::Runner { stream, .. } if caller == Caller::Finished => send_line(
            stream,
            &serde_json::to_value(RunnerReport::Finished { exit_code: Some(0), signal: None }).unwrap(),
        ),
        Holder::Runner { stream, .. } => {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
        Holder::Role(_) => {}
    }
    let answer = match order {
        Order::Live | Order::ServingChoice | Order::Serving => json!({"cmd": "answer", "act": "timeout"}),
        Order::Panic | Order::ServingPanic => json!({"cmd": "answer", "act": "panic"}),
        Order::Retained => json!({"cmd": "answer", "act": "read", "pids": [[holder.pid(), 1u64 << 20]]}),
        Order::Cancelled => Value::Null,
    };
    let terminate = |daemon: &Sibling| {
        // SAFETY: a signal to the child T owns, by its pid.
        unsafe { libc::kill(daemon.pid() as i32, libc::SIGTERM) };
    };
    let holder_pid = holder.pid();
    let mut hold_budget = None;
    let mut holder_end = None;
    match order {
        Order::Cancelled => {
            let met = d.expect("rendezvous");
            d.send(json!({"cmd": "wake"}));
            hold_budget = d.expect("held")["budget"].as_bool();
            past_escalation(met["started_ns"].as_u64().unwrap());
            terminate(&d);
            d.expect("shutdown");
            d.send(json!({"cmd": "release_hold"}));
        }
        Order::ServingChoice => {
            let dispatched = d.expect("dispatch");
            past_escalation(dispatched["started_ns"].as_u64().unwrap());
            d.send(answer);
            d.expect("choice");
            terminate(&d);
            d.expect("save");
            d.send(json!({"cmd": "release_choice"}));
        }
        Order::Serving | Order::ServingPanic => {
            let dispatched = d.expect("dispatch");
            past_escalation(dispatched["started_ns"].as_u64().unwrap());
            d.send(answer);
        }
        Order::Live | Order::Panic | Order::Retained => {
            let dispatched = d.expect("dispatch");
            past_escalation(dispatched["started_ns"].as_u64().unwrap());
            terminate(&d);
            d.expect("save");
            d.send(answer);
        }
    }
    // Checked on arrival: an enforcement kills H, whom T reaps at once, since settlement sees an unreaped zombie as
    // alive; an unverified end in a serving control fails at once, as no kill will come.
    let end = loop {
        let ack = d.next();
        match (ack["ack"].as_str(), ack["verdict"].as_str()) {
            (Some("end"), _) => break ack,
            (Some("choice"), Some("enforce")) if holder_end.is_none() => holder_end = Some(holder.reap()),
            (Some("choice"), Some("end_unverified")) if !order.stopping() => {
                panic!("a serving control ended unverified: {:?}", d.heard())
            }
            _ => {}
        }
    };
    let done = match &mut holder {
        Holder::Runner { lines, .. } if caller == Caller::Finished && order.promises_done() => {
            std::iter::from_fn(|| Some(read_line(lines))).any(|event| event["event"] == "done")
        }
        Holder::Runner { .. } | Holder::Role(_) => false,
    };
    match order {
        Order::Cancelled => d.send(json!({"cmd": "exit"})),
        Order::Serving | Order::ServingPanic => terminate(&d),
        Order::Live | Order::Panic | Order::ServingChoice | Order::Retained => {
            d.send(json!({"cmd": "release_save"}))
        }
    }
    assert!(d.status().success(), "D exits 0: {:?}", d.heard());
    // The absence of a kill is judged only now, with D reaped, and before the guard kills H.
    let holder_end = holder_end.unwrap_or_else(|| match holder.alive() {
        true => HolderEnd::Alive,
        false => holder.reap(),
    });
    let holder_heard = match &mut holder {
        Holder::Role(holder) => holder.take_heard(),
        Holder::Runner { .. } => Vec::new(),
    };
    let journal = std::fs::read_to_string(run.dir.path().join("failures.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let log = std::fs::read_to_string(run.dir.path().join("d.log")).unwrap_or_default();
    let choices = d.heard().iter().filter(|ack| ack["ack"] == "choice").cloned().collect();
    let saves = d.heard().iter().filter(|ack| ack["ack"] == "save").cloned().collect();
    Outcome {
        choices,
        end,
        saves,
        daemon: d.take_heard(),
        holder: holder_heard,
        holder_end,
        journal,
        log,
        holder_pid,
        done,
        hold_budget,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn last_choice(outcome: &Outcome) -> &Value {
        outcome.choices.last().expect("a choice")
    }

    fn ordered(log: &str, first: &str, then: &str) -> bool {
        log.find(first).zip(log.find(then)).is_some_and(|(first, then)| first < then)
    }

    /// T-22 (i), (ii) and (iii) on one caller.
    fn ends_unverified(order: Order, caller: Caller) {
        let outcome = scenario(order, caller);
        let cause = match order {
            Order::Live => timeout().text().to_string(),
            Order::Panic => PANIC.to_string(),
            _ => CANCELLED.to_string(),
        };
        let choice = last_choice(&outcome);
        assert_eq!(
            (choice["verdict"].as_str(), choice["life"].as_str(), choice["text"].as_str()),
            (Some("end_unverified"), Some("stopping"), Some(cause.as_str())),
            "{order:?} {caller:?}: {}",
            outcome.record()
        );
        assert_eq!(outcome.end["returned"], true, "{order:?} {caller:?}: settlement returned");
        assert_eq!(outcome.holder_end, HolderEnd::Alive, "{order:?} {caller:?}: no kill");
        assert!(outcome.records("unclean_release").is_empty(), "{order:?} {caller:?}: {:?}", outcome.journal);
        assert!(ordered(&outcome.log, "gpu-switch: stopping", "gpu-switch: shutting down"), "{}", outcome.log);
        assert!(outcome.log.contains("ends unverified after"), "{}", outcome.log);
        match order {
            Order::Cancelled => assert_eq!(outcome.hold_budget, Some(true), "the forced entry held with budget"),
            _ => {
                let terminal = outcome.records("terminal_failure");
                assert_eq!(terminal.len(), 1, "{order:?} {caller:?}: {:?}", outcome.journal);
                let (status, reason) = match caller {
                    Caller::Abandoned => (json!("no_vram"), ABANDONED),
                    _ => (Value::Null, UNVERIFIED),
                };
                assert_eq!(
                    (&terminal[0]["state"], &terminal[0]["terminal_status"], terminal[0]["reason"].as_str()),
                    (&json!("failed"), &status, Some(reason)),
                    "{order:?} {caller:?}"
                );
                assert!(caller != Caller::Finished || outcome.done, "the runner received Done");
            }
        }
        match caller {
            Caller::Eof => assert_eq!(
                outcome.holder.iter().filter(|ack| ack["ack"] == "wrote").map(|ack| ack["lines"].clone()).collect::<Vec<_>>(),
                [json!(1)],
                "one written line: the session took its EOF arm"
            ),
            Caller::Abandoned => {
                let signals = outcome.records("signal");
                assert_eq!(signals.len(), 1, "abandonment's SIGTERM only: {:?}", outcome.journal);
                assert!(outcome.holder.iter().any(|ack| ack["ack"] == "eof"), "H acknowledged D0's EOF");
            }
            Caller::Release | Caller::Finished | Caller::RunnerTail => {}
        }
    }

    /// T-23 and T-22 (iv): an enforcement that kills H.
    fn enforces(order: Order, caller: Caller, life: &str, reason: &str) {
        let outcome = scenario(order, caller);
        let enforced: Vec<&Value> = outcome.choices.iter().filter(|choice| choice["verdict"] == "enforce").collect();
        assert!(!enforced.is_empty(), "{order:?} {caller:?}: {}", outcome.record());
        assert_eq!(enforced[0]["life"].as_str(), Some(life), "{order:?} {caller:?}");
        assert_eq!(outcome.holder_end, HolderEnd::Killed(libc::SIGKILL), "{order:?} {caller:?}");
        let unclean = outcome.records("unclean_release");
        assert_eq!(unclean.len(), 1, "{order:?} {caller:?}: {:?}", outcome.journal);
        let terminal = outcome.records("terminal_failure");
        match caller {
            Caller::Abandoned => {
                assert_eq!(unclean[0]["reason"].as_str(), Some(ABANDONED), "the earlier reason stays");
                assert_eq!((&terminal[0]["state"], terminal[0]["reason"].as_str()), (&json!("failed"), Some(ABANDONED)));
            }
            _ => {
                assert_eq!(unclean[0]["reason"].as_str(), Some(reason), "{order:?} {caller:?}");
                assert_eq!(terminal.last().map(|record| &record["state"]), Some(&json!("killed")), "{:?}", outcome.journal);
            }
        }
        assert!(caller != Caller::Finished || outcome.done, "the runner received Done");
        match order {
            Order::ServingChoice => {
                let choice = outcome.daemon.iter().position(|ack| ack["ack"] == "choice").unwrap();
                let save = outcome.daemon.iter().position(|ack| ack["ack"] == "save").unwrap();
                assert!(choice < save, "the choice read Serving before the save's entry");
                assert_eq!((outcome.saves[0]["invocation"].as_u64(), outcome.saves[0]["life"].as_str()), (Some(1), Some("stopping")));
            }
            _ => {}
        }
    }

    #[test]
    fn t22_i_a_lease_after_eof_ends_unverified_without_a_kill() {
        ends_unverified(Order::Live, Caller::Eof);
    }

    #[test]
    fn t22_i_an_answered_release_ends_unverified_without_a_kill() {
        ends_unverified(Order::Live, Caller::Release);
    }

    #[test]
    fn t22_i_a_runners_finished_ends_unverified_without_a_kill() {
        ends_unverified(Order::Live, Caller::Finished);
    }

    #[test]
    fn t22_i_a_runner_sessions_tail_ends_unverified_without_a_kill() {
        ends_unverified(Order::Live, Caller::RunnerTail);
    }

    #[test]
    fn t22_i_an_abandoned_detached_job_ends_unverified_without_a_kill_and_keeps_its_terminal_status() {
        ends_unverified(Order::Live, Caller::Abandoned);
    }

    #[test]
    fn t22_ii_a_panicked_read_while_stopping_ends_unverified_without_a_kill() {
        ends_unverified(Order::Panic, Caller::Eof);
    }

    #[test]
    fn t22_iii_a_cancelled_dispatch_after_a_lease_eof_ends_unverified_without_a_kill() {
        ends_unverified(Order::Cancelled, Caller::Eof);
    }

    #[test]
    fn t22_iii_a_cancelled_dispatch_at_an_answered_release_ends_unverified_without_a_kill() {
        ends_unverified(Order::Cancelled, Caller::Release);
    }

    #[test]
    fn t22_iii_a_cancelled_dispatch_at_a_runners_finished_ends_unverified_without_a_kill() {
        ends_unverified(Order::Cancelled, Caller::Finished);
    }

    #[test]
    fn t22_iii_a_cancelled_dispatch_in_a_runner_sessions_tail_ends_unverified_without_a_kill() {
        ends_unverified(Order::Cancelled, Caller::RunnerTail);
    }

    #[test]
    fn t22_iii_a_cancelled_dispatch_of_an_abandoned_detached_job_ends_unverified_without_a_kill() {
        ends_unverified(Order::Cancelled, Caller::Abandoned);
    }

    #[test]
    fn t22_iv_a_choice_that_read_serving_is_enforced_after_the_publication() {
        enforces(Order::ServingChoice, Caller::Eof, "serving", RETAINED);
    }

    #[test]
    fn t23_a_an_unanswered_read_after_a_lease_eof_while_serving_is_enforced_with_a_real_sigkill() {
        enforces(Order::Serving, Caller::Eof, "serving", RETAINED);
    }

    #[test]
    fn t23_b_an_unanswered_read_at_an_answered_release_while_serving_is_enforced_with_a_real_sigkill() {
        enforces(Order::Serving, Caller::Release, "serving", RETAINED);
    }

    #[test]
    fn t23_c_an_unanswered_read_at_a_runners_finished_while_serving_is_enforced_with_a_real_sigkill() {
        enforces(Order::Serving, Caller::Finished, "serving", RETAINED);
    }

    #[test]
    fn t23_d_an_unanswered_read_in_a_runner_sessions_tail_while_serving_is_enforced_with_a_real_sigkill() {
        enforces(Order::Serving, Caller::RunnerTail, "serving", RETAINED);
    }

    #[test]
    fn t23_e_an_unanswered_read_of_an_abandoned_detached_job_while_serving_is_enforced_with_a_real_sigkill() {
        enforces(Order::Serving, Caller::Abandoned, "serving", RETAINED);
    }

    #[test]
    fn t23_f_a_panicked_read_while_serving_is_enforced_with_its_own_reason() {
        let reason = format!("gpu-switch's GPU read panicked, so the teardown could not be verified: {PANIC}");
        enforces(Order::ServingPanic, Caller::Eof, "serving", &reason);
    }

    #[test]
    fn t23_g_a_verified_retention_while_stopping_is_enforced() {
        enforces(Order::Retained, Caller::Eof, "stopping", RETAINED);
    }

    /// Case 4's regression (R-d): the serving daemon D on its scripted card and the lease holder H, siblings of T, with
    /// settlement's start acknowledged and every compute read held until T answers it. A release is answered before
    /// its departure settles; a holder that keeps VRAM is killed by the ordinary rule; one that freed it completes.
    mod settling {
        use super::*;
        use crate::switch::sibling::Sibling;

        /// D's answer to a held read: the card lists `pids`, each with its bytes.
        fn read(pids: &[(u32, u64)]) -> Value {
            json!({"cmd": "answer", "act": "read", "pids": pids.iter().map(|(pid, bytes)| json!([pid, bytes])).collect::<Vec<_>>()})
        }

        /// A request on D's host socket, as `gpu-run` sends it: its one reply.
        fn ask(run: &Run, request: Value) -> Value {
            let stream = UnixStream::connect(run.socket()).unwrap();
            send_line(&stream, &request);
            read_line(&mut BufReader::new(stream))
        }

        /// D serving, and H holding its granted lease; then every compute read is held until T answers it.
        fn released(run: &Run) -> (Sibling, Sibling, u64) {
            let mut d = run.daemon("d.log", "serving", &[]);
            let mut h = run.holder("lease", false);
            let id = h.expect("granted")["id"].as_u64().unwrap();
            d.send(json!({"cmd": "arm"}));
            d.expect("armed");
            h.send(json!({"cmd": "release"}));
            (d, h, id)
        }

        /// D's end, by T's word.
        fn exits(d: Sibling) {
            d.send(json!({"cmd": "exit"}));
            assert!(d.wait().success(), "D exits 0");
        }

        #[test]
        fn a_released_holder_that_keeps_its_vram_is_killed_by_the_serving_control_past_the_rule_bound() {
            let run = Run::new();
            let (mut d, mut h, _id) = released(&run);
            let started = d.expect("dispatch")["started_ns"].as_u64().unwrap();
            let retained = read(&[(h.pid(), 1 << 20)]);
            d.send(retained.clone());
            loop {
                let ack = d.next();
                match (ack["ack"].as_str(), ack["verdict"].as_str()) {
                    (Some("dispatch"), _) => d.send(retained.clone()),
                    (Some("choice"), Some("enforce")) => break,
                    (Some("end"), _) => panic!("settlement ended without enforcing: {:?}", d.heard()),
                    _ => {}
                }
            }
            let status = h.status();
            let ended = crate::clock::Clock::system().monotonic_ns();
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(libc::SIGKILL), "H dies by SIGKILL");
            assert!(ended >= started + 250_000_000, "the kill came at least 250 ms after settlement began");
            d.expect("end");
            let journal = std::fs::read_to_string(run.dir.path().join("failures.jsonl")).unwrap_or_default();
            let unclean = journal
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .filter(|entry| entry["event"] == "unclean_release" && entry["pid"] == h.pid())
                .count();
            assert_eq!(unclean, 1, "unclean_release is recorded once");
            exits(d);
        }

        #[test]
        fn a_release_is_answered_while_its_settlement_still_holds_the_job() {
            let run = Run::new();
            let (mut d, mut h, id) = released(&run);
            d.expect("dispatch");
            h.expect("released");
            let status = ask(&run, json!({"op": "status"}));
            let job = status["status"]["jobs"].as_array().unwrap().iter().find(|job| job["id"] == id).cloned();
            let state: crate::protocol::JobState = serde_json::from_value(job.expect("the job is listed")["state"].clone()).unwrap();
            assert!(!state.is_terminal(), "the job is not terminal while settlement holds its read: {state:?}");
            d.send(read(&[]));
            d.expect("end");
            exits(d);
        }

        #[test]
        fn a_living_holder_that_freed_its_vram_completes_its_job() {
            let run = Run::new();
            let (mut d, h, id) = released(&run);
            let holder = crate::procs::Process::now(h.pid()).expect("H runs").open().expect("H's pidfd");
            let freed = read(&[(h.pid(), 0)]);
            loop {
                let ack = d.next();
                match (ack["ack"].as_str(), ack["verdict"].as_str()) {
                    (Some("dispatch"), _) => d.send(freed.clone()),
                    (Some("choice"), Some("enforce")) => panic!("settlement enforced on a holder that freed its VRAM: {ack}"),
                    (Some("end"), _) => break,
                    _ => {}
                }
            }
            let waited = ask(&run, json!({"op": "wait", "id": id}));
            assert_eq!(waited["job"]["state"], json!(crate::protocol::JobState::Done), "{waited}");
            assert!(!holder.exited(), "H is still alive");
            exits(d);
        }
    }
}
