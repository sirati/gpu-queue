//! The listening sockets. `host`: each peer uid is its own side.
//! `container`: every peer is the container side. `arbiter`: the link of
//! gpu-arbiter, which runs NInfer, and nothing else.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

use crate::procs;
use crate::protocol::{
    now_unix, ArbiterEvent, ClientMsg, JobSpec, JobState, LeaseJob, LeaseRequest, Placement, Reply, Request,
    Resume, RunnerEvent, RunnerReport, Sig, SubmitRequest,
};
use crate::switch::config::secs;
use crate::switch::daemon::{
    behind_head, census, end_job, finish_run, placement, schedule_kill, Daemon,
};
use crate::switch::state::{Arbiter, Conn, Job, NPhase, Police, Side, SideJobs, State};
use crate::switch::token::{Claim, Secret};
use crate::switch::view;

/// Carried by every line of a lease session; a client that misses it on
/// the first reply talks to a gpu-switch without leases.
pub const LEASE_PROTOCOL: u64 = 2;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Host,
    Container,
    Arbiter,
}

impl Role {
    pub fn of(name: &str) -> Option<Role> {
        match name {
            "host" => Some(Role::Host),
            "container" => Some(Role::Container),
            "arbiter" => Some(Role::Arbiter),
            _ => None,
        }
    }
}

pub async fn serve(d: Arc<Daemon>, listener: UnixListener, role: Role) {
    loop {
        let stream = match listener.accept().await {
            Ok((s, _)) => s,
            Err(e) => {
                eprintln!("gpu-switch: accept: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let cred = match stream.peer_cred() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("gpu-switch: peer credentials: {e}");
                continue;
            }
        };
        // SO_PEERCRED reports the pid in our (the host's) namespace.
        let pid = cred.pid().unwrap_or(0).max(0) as u32;
        let side = match role {
            Role::Host => Side::Host(cred.uid()),
            Role::Container => Side::Container,
            Role::Arbiter => {
                tokio::spawn(arbiter_connection(d.clone(), stream, pid));
                continue;
            }
        };
        // The connecting process's pid namespace, as it was when it connected: the socket may outlive it.
        let namespace = procs::PidNamespace::of(pid);
        tokio::spawn(connection(d.clone(), stream, side, pid, namespace));
    }
}

async fn write_line<T: serde::Serialize>(w: &mut OwnedWriteHalf, v: &T) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(v).expect("serializable");
    line.push(b'\n');
    w.write_all(&line).await
}

async fn write_lease<T: serde::Serialize>(w: &mut OwnedWriteHalf, v: &T) -> std::io::Result<()> {
    let mut v = serde_json::to_value(v).expect("serializable");
    if let Some(o) = v.as_object_mut() {
        o.insert("lease_protocol".into(), LEASE_PROTOCOL.into());
    }
    write_line(w, &v).await
}

async fn connection(
    d: Arc<Daemon>,
    stream: UnixStream,
    side: Side,
    peer_pid: u32,
    namespace: Option<procs::PidNamespace>,
) {
    let (r, mut w) = stream.into_split();
    let mut lines = BufReader::new(r).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let req: Request = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                let _ = write_line(&mut w, &Reply::error(format!("bad request: {e}"))).await;
                continue;
            }
        };
        let reply = match req {
            Request::Submit(request) => {
                runner_session(d, side, peer_pid, request, lines, w).await;
                return;
            }
            Request::Lease(request) => {
                lease_session(d, side, peer_pid, request, lines, w).await;
                return;
            }
            Request::Census { group } => Reply {
                census: Some(d.with(|s| census(s, side, group.as_deref()))),
                ..Reply::ok()
            },
            Request::RetainVram { resume, seconds } => {
                d.update(|s| super::retention::request(&d, s, side, peer_pid, &resume, seconds))
            }
            Request::Release
            | Request::ShutdownStarted { .. }
            | Request::Failed { .. }
            | Request::Cancelled { .. }
            | Request::Ready(_)
            | Request::PreparationWait { .. }
            | Request::TransferPlace { .. } => Reply::error("request requires a lease session"),
            Request::ArbiterAttach => {
                Reply::error("arbiter_attach is for gpu-arbiter's own socket")
            }
            Request::Status => Reply {
                status: Some(
                    d.with(|s| view::status(&d, s, side, namespace.as_ref(), Instant::now())),
                ),
                ..Reply::ok()
            },
            Request::ServeStatus => Reply {
                serve: Some(d.with(|s| view::serve_status(&d, s, Instant::now()))),
                ..Reply::ok()
            },
            Request::Serve { on } => d.update(|s| set_serve(&d, s, on)),
            Request::SwitchGpu => d.update(switch_gpu),
            Request::SwitchNinfer => d.update(switch_ninfer),
            Request::Cancel { id } => d.update(|s| cancel(&d, s, side, id)),
            Request::Kill { handle } => d.update(|s| kill(&d, s, side, &handle)),
            Request::Wait { id } => wait(&d, side, id).await,
            Request::SetHostCache { mib, apply_now } => d.update(|s| {
                s.host_cache_override = Some(mib);
                let active = s.ninfer_owns() && s.host_cache_current == Some(mib);
                pending_change(s, &format!("host cache {mib} MiB"), active, apply_now)
            }),
            Request::SetProfile { profile, apply_now } => {
                if !d.config.profiles.contains(&profile) {
                    Reply::error(format!(
                        "unknown profile {profile:?}; configured: {}",
                        d.config.profiles.join(", ")
                    ))
                } else {
                    d.update(|s| {
                        let active =
                            s.ninfer_owns() && s.profile_current.as_ref() == Some(&profile);
                        let what = format!("profile {profile}");
                        s.profile_override = Some(profile);
                        pending_change(s, &what, active, apply_now)
                    })
                }
            }
        };
        if write_line(&mut w, &reply).await.is_err() {
            return;
        }
    }
}

/// Phase words only: the host user reads this.
fn set_serve(d: &Daemon, s: &mut State, on: bool) -> Reply {
    let was = s.serve_wanted;
    s.serve_wanted = on;
    let now = Instant::now();
    let word = view::serve_word(s, now);
    let message = match (on, was) {
        (true, true) => format!("serving already on ({word})"),
        (false, false) => format!("serving already off ({word})"),
        (true, false) => {
            s.ninfer.retry_at = None;
            if s.ninfer.phase == NPhase::Idle && s.queue_empty() {
                s.skip_linger = true;
                "serving on: NInfer queues for the GPU now".into()
            } else {
                format!("serving on: NInfer queues for the GPU once the queue is empty ({word})")
            }
        }
        (false, true) => {
            s.switch_to_ninfer = false;
            if s.ninfer_owns() {
                format!("serving off: stopping NInfer ({word})")
            } else {
                format!("serving off ({word})")
            }
        }
    };
    eprintln!("gpu-switch: {message}");
    Reply {
        message: Some(message),
        serve: Some(view::serve_status(d, s, now)),
        ..Reply::ok()
    }
}

fn switch_gpu(s: &mut State) -> Reply {
    if !s.serve_wanted {
        return Reply::message("serving is off: jobs already run in line order, no switch needed");
    }
    match s.ninfer.phase {
        NPhase::Starting | NPhase::Serving => {
            let waiting = s.jobs().filter(|j| j.state == JobState::Queued).count();
            if waiting == 0 {
                return Reply::error(
                    "nothing queued; switch-gpu hands the GPU to queued work. Submit with 'gpu-run submit' first",
                );
            }
            s.switch_requested = true;
            Reply::message(format!(
                "switching: NInfer stops after its in-flight requests, then {waiting} queued job(s) run in line order"
            ))
        }
        NPhase::Stopping | NPhase::Releasing => Reply::message("NInfer is already stopping"),
        NPhase::Queued => Reply::message("the GPU belongs to the line; NInfer waits in it"),
        NPhase::Idle => Reply::message("the GPU already belongs to the line"),
    }
}

/// The exact guarantee, also in the README and the status text.
pub const NINFER_FRONT_GUARANTEE: &str = "nothing waiting starts before NInfer";

fn switch_ninfer(s: &mut State) -> Reply {
    let was = s.serve_wanted;
    s.serve_wanted = true;
    s.ninfer.retry_at = None;
    if s.ninfer_owns() {
        return Reply::message(format!(
            "NInfer already holds the GPU ({})",
            view::serve_word(s, Instant::now())
        ));
    }
    if s.switch_to_ninfer {
        return Reply::message("already switching to NInfer");
    }
    s.switch_to_ninfer = true;
    if s.ninfer.phase == NPhase::Queued {
        s.ninfer.key = crate::protocol::Placement { class: 0, ns: 0 };
    }
    let resident = s.running().count();
    let queued = s.jobs().filter(|j| j.state == JobState::Queued).count();
    let on = if was { "" } else { "serving on; " };
    Reply::message(format!(
        "{on}switching to NInfer: it starts once the {resident} GPU-resident job(s) finish; \
         {queued} queued keep their order ({NINFER_FRONT_GUARANTEE})"
    ))
}

/// Common tail of set-host-cache and set-profile.
fn pending_change(s: &mut State, what: &str, active: bool, apply_now: bool) -> Reply {
    if active {
        return Reply::message(format!("{what} is already active"));
    }
    if apply_now && s.serve_wanted && matches!(s.ninfer.phase, NPhase::Starting | NPhase::Serving) {
        s.restart_requested = true;
        return Reply::message(format!(
            "{what}: restarting NInfer now; the line stays behind it"
        ));
    }
    let when = if apply_now {
        "NInfer is not serving, it applies at its next start"
    } else {
        "applies at the next NInfer start ('--apply-now' restarts a serving NInfer)"
    };
    Reply::message(format!("{what} pending; {when}"))
}

/// A queued lease client is turned away by closing its connection.
fn hang_up(job: &mut Job) {
    if job.is_lease() {
        job.conn = Conn::Client(mpsc::unbounded_channel().0);
    }
}

fn cancel(d: &Daemon, s: &mut State, side: Side, id: u64) -> Reply {
    let grace = secs(d.config.job_kill_grace_secs);
    let Some(job) = s.job_mut(side, id) else {
        return Reply::error(format!("no job {id}"));
    };
    match job.state {
        JobState::Queued => {
            job.runner(RunnerEvent::Cancel);
            hang_up(job);
            end_job(
                &d,
                s,
                side,
                id,
                JobState::Cancelled,
                "cancelled while queued".into(),
            );
            Reply::message(format!("job {id} removed from the line"))
        }
        JobState::Running => {
            job.end_reason = Some((JobState::Cancelled, "cancelled while running".into()));
            schedule_kill(job, &[(Duration::ZERO, Sig::Term), (grace, Sig::Kill)]);
            Reply::message(format!(
                "job {id}: SIGTERM, SIGKILL after {}s",
                grace.as_secs_f64()
            ))
        }
        st => Reply::error(format!("job {id} already {}", st.as_str())),
    }
}

/// A host user kills a container job by handle.
fn kill(d: &Daemon, s: &mut State, viewer: Side, handle: &str) -> Reply {
    let Side::Host(uid) = viewer else {
        return Reply::error("only the host side may kill another side's jobs");
    };
    let found = s
        .sides
        .get(&Side::Container)
        .and_then(|sj| sj.jobs.values().find(|j| j.handle == handle && j.active()))
        .map(|j| j.id);
    let Some(id) = found else {
        return Reply::error(format!("no active job {handle} on the container side"));
    };
    let by = procs::username(uid).unwrap_or_else(|| format!("uid {uid}"));
    let reason = format!("killed by {by}");
    let int_term = secs(d.config.sigint_to_sigterm_secs);
    let term_kill = secs(d.config.sigterm_to_sigkill_secs);
    let job = s.job_mut(Side::Container, id).expect("found");
    match job.state {
        JobState::Queued => {
            job.runner(RunnerEvent::Cancel);
            hang_up(job);
            end_job(&d, s, Side::Container, id, JobState::Killed, reason);
            Reply::message(format!("{handle} removed from the line"))
        }
        _ => {
            job.end_reason = Some((JobState::Killed, reason));
            schedule_kill(
                job,
                &[
                    (Duration::ZERO, Sig::Int),
                    (int_term, Sig::Term),
                    (term_kill, Sig::Kill),
                ],
            );
            Reply::message(format!(
                "{handle}: SIGINT, SIGTERM after {}s, SIGKILL {}s later",
                int_term.as_secs_f64(),
                term_kill.as_secs_f64()
            ))
        }
    }
}

async fn wait(d: &Arc<Daemon>, side: Side, id: u64) -> Reply {
    let done = d
        .wait_for(|s| match s.job(side, id) {
            None => Some(false),
            Some(j) if j.state.is_terminal() => Some(true),
            Some(_) => None,
        })
        .await;
    if !done {
        return Reply::error(format!("no job {id}"));
    }
    d.with(|s| match s.job(side, id) {
        Some(j) => Reply {
            job: Some(view::own_job_of(s, side, j)),
            ..Reply::ok()
        },
        None => Reply::error(format!("no job {id}")),
    })
}

fn side_prefix(s: &State, side: Side) -> String {
    match side {
        Side::Container => "n".into(),
        Side::Host(uid) => {
            let first = procs::username(uid)
                .and_then(|n| n.chars().next())
                .filter(|c| c.is_ascii_alphabetic())
                .map(|c| c.to_ascii_lowercase());
            let taken = |p: &str| s.sides.iter().any(|(o, sj)| *o != side && sj.prefix == p);
            match first {
                Some(c) if c != 'n' && !taken(&c.to_string()) => c.to_string(),
                _ => format!("u{uid}-"),
            }
        }
    }
}

/// A new queued job at its place in the line.
#[allow(clippy::too_many_arguments)]
/// Where a new job enters the line.
#[derive(Clone, Copy)]
enum Arrival {
    Fresh,
    /// Released and asking again: behind the head, before fresh waiters.
    Requeued,
    /// Resumed at the place its token proves.
    At(Placement),
}

/// What a new job brings to the line.
struct NewJob<'a> {
    spec: JobSpec,
    cmd: Vec<String>,
    label: Option<String>,
    group: Option<String>,
    cwd: String,
    log_dir: Option<&'a str>,
    arrival: Arrival,
    /// A place: it prepares only once told.
    prepares: bool,
}

fn new_job(
    s: &mut State,
    secret: &Secret,
    side: Side,
    peer_pid: u32,
    conn: Conn,
    new: NewJob,
) -> u64 {
    let NewJob {
        spec,
        cmd,
        label,
        group,
        cwd,
        log_dir,
        arrival,
        prepares,
    } = new;
    if !s.sides.contains_key(&side) {
        let prefix = side_prefix(s, side);
        s.sides.insert(
            side,
            SideJobs {
                prefix,
                next_id: 1,
                jobs: BTreeMap::new(),
            },
        );
    }
    let share = spec.share.clone().unwrap_or_else(|| side.pool());
    let key = match arrival {
        Arrival::Requeued => behind_head(s),
        Arrival::At(key) => key,
        Arrival::Fresh => placement(s, spec.priority, &share),
    };
    let requeued = key.class == 0;
    let token = secret.token(&Claim {
        side,
        spec: &spec,
        group: group.as_deref(),
        key,
    });
    let lease = matches!(conn, Conn::Client(_));
    let sj = s.sides.get_mut(&side).expect("inserted");
    let id = sj.next_id;
    sj.next_id += 1;
    let handle = format!("{}{id}", sj.prefix);
    let log = log_dir
        .map(|d| format!("{}/{id}.log", d.trim_end_matches('/')))
        .unwrap_or_default();
    let label = Some(
        label
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| format!("execution-{handle}")),
    );
    sj.jobs.insert(
        id,
        Job {
            retention: None,
            terminal_status: None,
            violations: Vec::new(),
            device_lost: Vec::new(),
            gpu_processes: Vec::new(),
            id,
            handle,
            side,
            spec,
            share,
            cmd,
            label,
            group,
            lease,
            token,
            cwd,
            log,
            state: JobState::Queued,
            exit_code: None,
            signal: None,
            error: None,
            runner_pid: peer_pid,
            conn,
            departing: false,
            pid_inner: None,
            // A lease client's grant covers its own process tree.
            pid_host: lease.then_some(peer_pid),
            submitted_at: now_unix(),
            started_at: None,
            finished_at: None,
            key,
            requeued,
            runs: 0,
            admitted_at: None,
            yield_sent: false,
            kill: None,
            end_reason: None,
            shutdown_grace_secs: None,
            shutdown: None,
            process_started: crate::procs::started(peer_pid),
            police: Police::default(),
            timing: Some(super::timing::Timing::default()),
            reason: None,
            blocker: None,
            place: prepares.then(super::preparation::Place::default),
            transferred_from: None,
        },
    );
    id
}

/// Why a lease session attaches to no job.
enum Gone {
    Refused(String),
    /// Its client gave the place to this process.
    Transferred(crate::protocol::ProcessIdentity),
}
impl From<String> for Gone {
    fn from(error: String) -> Self {
        Gone::Refused(error)
    }
}

/// How a resuming client's job came back.
struct Resumed {
    id: u64,
    /// gpu-switch had lost the job and registered it again at the place its token proves, untold.
    reregistered: bool,
    /// The client gave its place away: the place's owner now; nothing was attached.
    transferred: Option<crate::protocol::ProcessIdentity>,
}

/// A resuming client's job: re-attached if gpu-switch kept it, else
/// registered again, at the place its token proves.
fn resume_job(
    d: &Daemon,
    s: &mut State,
    side: Side,
    peer_pid: u32,
    conn: Conn,
    new: NewJob,
    resume: &Resume,
) -> Result<Resumed, String> {
    if let Some(job) = s
        .job_mut(side, resume.id)
        .filter(|j| j.token == resume.token)
    {
        if let Some(owner) = super::preparation::transferred_from(job, peer_pid) {
            return Ok(Resumed {
                id: job.id,
                reregistered: false,
                transferred: Some(owner),
            });
        }
        if !job.active() {
            return Err(format!("job {} has ended: {}", job.id, job.state.as_str()));
        }
        if job.departing {
            return Err(format!(
                "job {} is terminating after its reattachment deadline",
                job.id
            ));
        }
        if job.detached().is_none() {
            return Err(format!("job {} is attached to another client", job.id));
        }
        job.conn = conn;
        job.runner_pid = peer_pid;
        job.pid_host = job
            .pid_host
            .filter(|_| !job.lease)
            .or(job.lease.then_some(peer_pid));
        job.reason = None;
        eprintln!("gpu-switch: job {} resumed by pid {peer_pid}", job.handle);
        return Ok(Resumed {
            id: job.id,
            reregistered: false,
            transferred: None,
        });
    }
    let claim = Claim {
        side,
        spec: &new.spec,
        group: new.group.as_deref(),
        key: resume.key,
    };
    // A place token proves queue order, not a grant. Never reconstruct a live
    // grant from the client's mutable `running` flag after losing authority state.
    if resume.running {
        return Err("cannot restore a running GPU grant without retained daemon state".into());
    }
    let arrival = match d.secret.proves(&claim, &resume.token) {
        true => Arrival::At(resume.key),
        false => Arrival::Fresh,
    };
    let id = new_job(
        s,
        &d.secret,
        side,
        peer_pid,
        conn,
        NewJob { arrival, ..new },
    );
    eprintln!("gpu-switch: job of pid {peer_pid} registered again as {id}");
    Ok(Resumed {
        id,
        reregistered: true,
        transferred: None,
    })
}

/// The resume block a client keeps for job `id`.
fn resume_of(s: &State, side: Side, id: u64) -> Option<Resume> {
    s.job(side, id).map(|j| Resume {
        id,
        token: j.token.clone(),
        key: j.key,
        running: j.state == JobState::Running,
    })
}

async fn runner_session(
    d: Arc<Daemon>,
    side: Side,
    runner_pid: u32,
    request: SubmitRequest,
    mut lines: Lines<BufReader<OwnedReadHalf>>,
    mut w: OwnedWriteHalf,
) {
    let SubmitRequest {
        name,
        cmd,
        cwd,
        spec,
        log_dir,
        resume,
    } = request;
    if cmd.is_empty() {
        let _ = write_line(&mut w, &Reply::error("empty command")).await;
        return;
    }
    if let Err(e) = spec.validate() {
        let _ = write_line(&mut w, &Reply::error(e)).await;
        return;
    }
    let warning = crate::estimate::warning(spec.expected_runtime_secs)
        .filter(|_| resume.is_none())
        .map(str::to_owned);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let new = NewJob {
        spec,
        cmd,
        label: name,
        group: None,
        cwd,
        log_dir: Some(&log_dir),
        arrival: Arrival::Fresh,
        prepares: false,
    };
    let created = d.update(|s| {
        let id = match &resume {
            Some(resume) => {
                resume_job(&d, s, side, runner_pid, Conn::Runner(tx), new, resume)?.id
            }
            None => new_job(s, &d.secret, side, runner_pid, Conn::Runner(tx), new),
        };
        let j = s.job(side, id).expect("inserted");
        // A resumed runner may have missed its go; a running one ignores it.
        if j.state == JobState::Running {
            j.go();
        }
        j.replay_notices();
        let job = view::own_job_of(s, side, j);
        let note = if !s.serve_wanted {
            format!("job {id} queued; serving is off, it runs in line order")
        } else if job.behind_ninfer {
            format!("job {id} queued behind NInfer; NOT running until 'gpu-run switch-gpu'")
        } else {
            format!("job {id} queued; it runs in line order")
        };
        Ok::<_, String>((job, note, resume_of(s, side, id)))
    });
    let (job, note, resume) = match created {
        Ok(created) => created,
        Err(e) => {
            let _ = write_line(&mut w, &Reply::error(e)).await;
            return;
        }
    };
    let id = job.id;
    eprintln!(
        "gpu-switch: job {} submitted by runner pid {runner_pid}",
        job.handle
    );
    let reply = Reply {
        warning,
        job: Some(job),
        message: Some(note),
        resume,
        ..Reply::ok()
    };
    if write_line(&mut w, &reply).await.is_ok() {
        loop {
            tokio::select! {
                ev = rx.recv() => match ev {
                    Some(ev) => if write_line(&mut w, &ev).await.is_err() { break },
                    None => break,
                },
                line = lines.next_line() => match line {
                    Ok(Some(line)) => report(&d, side, id, &line).await,
                    _ => break,
                },
            }
        }
    }
    super::departure::settle(&d, side, id).await;
    d.update(|s| {
        if s.job(side, id)
            .is_some_and(|j| j.active() && j.end_reason.is_some())
        {
            finish_run(&d, s, side, id, None, None);
            return;
        }
        match s.job(side, id).map(|j| j.state) {
            Some(JobState::Queued) => end_job(
                &d,
                s,
                side,
                id,
                JobState::Failed,
                "runner exited before the job started".into(),
            ),
            Some(JobState::Running) => end_job(
                &d,
                s,
                side,
                id,
                JobState::Failed,
                "runner connection lost".into(),
            ),
            _ => {}
        }
    });
}

async fn report(d: &Arc<Daemon>, side: Side, id: u64, line: &str) {
    let r: RunnerReport = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("gpu-switch: bad runner report: {e}");
            return;
        }
    };
    if matches!(&r, RunnerReport::Finished { .. }) {
        super::departure::settle(d, side, id).await;
    }
    d.update(|s| {
        let Some(job) = s.job_mut(side, id) else {
            return;
        };
        match r {
            RunnerReport::Started { pid } => {
                job.pid_inner = Some(pid);
                // Started is untrusted wire input. Only a direct child of the
                // authenticated runner may become a kernel-signal target.
                job.pid_host = procs::child_by_inner_pid(job.runner_pid, pid);
                job.process_started = job
                    .pid_host
                    .and_then(procs::started)
                    .or(job.process_started);
            }
            RunnerReport::SpawnFailed { error } => {
                job.runner(RunnerEvent::Done);
                end_job(&d, s, side, id, JobState::Failed, error);
            }
            RunnerReport::Failed { reason } => job.failure_reported(&reason),
            RunnerReport::Finished { exit_code, signal } => {
                if job.state.is_terminal() {
                    job.runner(RunnerEvent::Done);
                } else {
                    finish_run(&d, s, side, id, exit_code, signal);
                }
            }
        }
    });
}

/// A process asking for a grant for itself (tools/_gpu_switch.py): a job in
/// the same line, held while the connection lasts.
async fn lease_session(
    d: Arc<Daemon>,
    side: Side,
    peer_pid: u32,
    request: LeaseRequest,
    mut lines: Lines<BufReader<OwnedReadHalf>>,
    mut w: OwnedWriteHalf,
) {
    // A grant is only safe when the authority can enforce its final deadline.
    if unsafe { libc::kill(peer_pid as i32, 0) } != 0 {
        let _ = write_lease(
            &mut w,
            &Reply::error("GPU manager cannot signal this client; the service requires CAP_KILL"),
        )
        .await;
        return;
    }
    let LeaseRequest {
        category,
        max_runtime_secs,
        expected_runtime_secs,
        shutdown_grace_secs,
        mode,
        vram,
        label,
        priority,
        group,
        requeued,
        cooperative,
        resume,
        watchdog,
        share,
        prepares,
    } = request;
    if shutdown_grace_secs.is_some_and(|s| !s.is_finite() || !(0.0..=60.0).contains(&s)) {
        let _ = write_lease(
            &mut w,
            &Reply::error("shutdown grace must be 0..60 seconds"),
        )
        .await;
        return;
    }
    let spec = JobSpec {
        mode,
        vram: vram.unwrap_or(0),
        category,
        cooperative,
        priority,
        max_runtime_secs,
        expected_runtime_secs,
        watchdog,
        share,
    };
    if let Err(e) = spec.validate() {
        let _ = write_lease(&mut w, &Reply::error(e)).await;
        return;
    }
    let warning = crate::estimate::warning(spec.expected_runtime_secs).filter(|_| resume.is_none());
    // One line, bounded: it is shown to the owner only.
    let label = label.map(|l| l.lines().next().unwrap_or("").chars().take(200).collect());
    let (tx, mut rx) = mpsc::unbounded_channel();
    let new = NewJob {
        spec,
        cmd: Vec::new(),
        label,
        group,
        cwd: String::new(),
        log_dir: None,
        arrival: match requeued {
            true => Arrival::Requeued,
            false => Arrival::Fresh,
        },
        prepares,
    };
    let created = d.update(|s| {
        let Resumed { id, reregistered, transferred } = match &resume {
            Some(resume) => resume_job(&d, s, side, peer_pid, Conn::Client(tx), new, resume)?,
            None => Resumed {
                id: new_job(s, &d.secret, side, peer_pid, Conn::Client(tx), new),
                reregistered: false,
                transferred: None,
            },
        };
        if let Some(owner) = transferred {
            return Err(Gone::Transferred(owner));
        }
        let job = s.job_mut(side, id).expect("inserted");
        // A prepared place's grace is its Ready's; a resume keeps it.
        if job.place.as_ref().is_none_or(|place| place.ready.is_none()) {
            job.shutdown_grace_secs = shutdown_grace_secs;
        }
        if job.state == JobState::Running {
            job.go();
        }
        if job.place.as_ref().is_some_and(|place| place.told() && place.ready.is_none()) {
            super::preparation::prepare(job);
        }
        if let Some(shutdown) = &job.shutdown {
            job.shutdown_notice(shutdown.clone());
        }
        job.replay_notices();
        let handle = job.handle.clone();
        let watchdog = job.spec.watchdog;
        let preparation = super::preparation::summary(job);
        let resume = resume_of(s, side, id).expect("inserted");
        Ok::<_, Gone>((
            LeaseJob {
                id,
                handle,
                resume,
                contract_protocol: 2,
                cancel_protocol: 1,
                vram_retention_protocol: 2,
                watchdog,
                device_loss_protocol: 1,
            },
            preparation,
            reregistered,
        ))
    });
    let (job, preparation, reregistered) = match created {
        Ok(created) => created,
        Err(Gone::Refused(e)) => {
            let _ = write_lease(&mut w, &Reply::error(e)).await;
            return;
        }
        Err(Gone::Transferred(owner)) => {
            let _ = write_lease(&mut w, &serde_json::json!({"ok": true, "transferred": owner})).await;
            return;
        }
    };
    let id = job.id;
    eprintln!("gpu-switch: lease {} for pid {peer_pid}", job.handle);
    let mut reply = serde_json::json!({"ok": true, "job": job, "shutdown_protocol": 1, "lease_runtime_protocol": 1,
        "lease_category_protocol": 1, "preparation_protocol": 1});
    if let Some(preparation) = preparation {
        reply["preparation"] = preparation;
    }
    if reregistered {
        reply["reregistered"] = true.into();
    }
    if let Some(warning) = warning {
        reply["warning"] = warning.into();
    }
    if write_lease(&mut w, &reply).await.is_ok() {
        loop {
            tokio::select! {
                msg = rx.recv() => match msg {
                    Some(msg) => if write_lease(&mut w, &msg).await.is_err() { break },
                    None => break,
                },
                line = lines.next_line() => match line {
                    Ok(Some(line)) => {
                        match serde_json::from_str(&line) {
                            Ok(Request::Release) => {
                                #[cfg(test)]
                                d.hooks.release_arm.pass().await;
                                answer_release(&d, side, id, &mut w).await;
                                break;
                            }
                            Ok(Request::Cancelled { reason }) => {
                                d.update(|s| {
                                    if let Some(job) = s.job_mut(side, id) {
                                        job.end_reason.get_or_insert((JobState::Cancelled,
                                            reason.chars().take(4096).collect()));
                                    }
                                });
                                answer_release(&d, side, id, &mut w).await;
                                break;
                            }
                            // A failing process reports before its own teardown;
                            // its grant ends when it releases or exits.
                            Ok(Request::Failed { reason }) => {
                                d.update(|s| {
                                    if let Some(job) = s.job_mut(side, id) {
                                        job.failure_reported(&reason);
                                    }
                                });
                            }
                            Ok(Request::Ready(figures)) => {
                                let answer = d.update(|s| {
                                    super::preparation::ready(s.job_mut(side, id).expect("session job"), figures)
                                });
                                if let Err(error) = answer {
                                    if write_lease(&mut w, &Reply::error(error)).await.is_err() { break; }
                                }
                            }
                            Ok(Request::PreparationWait { reason, parked }) => {
                                let answer = d.update(|s| {
                                    super::preparation::wait(s.job_mut(side, id).expect("session job"), reason, parked)
                                });
                                if let Err(error) = answer {
                                    if write_lease(&mut w, &Reply::error(error)).await.is_err() { break; }
                                }
                            }
                            Ok(Request::TransferPlace { to }) => {
                                let answer = d.update(|s| {
                                    super::preparation::transfer(s.job_mut(side, id).expect("session job"), peer_pid, to)
                                });
                                // Durable before it is acknowledged: a successor answers a resent transfer.
                                let line = match answer {
                                    Ok(()) => {
                                        super::persist::save(&d);
                                        serde_json::to_value(ClientMsg::Transferred { transferred: to }).expect("serializable")
                                    }
                                    Err(error) => serde_json::to_value(Reply::error(error)).expect("serializable"),
                                };
                                if write_lease(&mut w, &line).await.is_err() { break; }
                            }
                            Ok(Request::ShutdownStarted { request_id }) => {
                                let answer = d.update(|s| {
                                    let job = s.job_mut(side, id).expect("session job");
                                    crate::switch::enforcement::acknowledge(&d, job, &request_id)
                                });
                                if write_lease(&mut w, &answer).await.is_err() { break; }
                            }
                            _ => {}
                        }
                    }
                    _ => break,
                },
            }
        }
    }
    super::departure::settle(&d, side, id).await;
    d.update(|s| match s.job(side, id).map(|j| j.state) {
        Some(JobState::Queued) => end_job(
            &d,
            s,
            side,
            id,
            JobState::Cancelled,
            "lease client left before the grant".into(),
        ),
        Some(JobState::Running | JobState::Stopping) => finish_run(&d, s, side, id, None, None),
        _ => {}
    });
}

/// A client ending its session (release or cancellation) hears every rule its grant broke and its run's card loss,
/// including any whose notice was still queued. The final report's snapshot ends the grant, so the answer is final.
async fn answer_release(d: &Daemon, side: Side, id: u64, w: &mut OwnedWriteHalf) {
    let (violations, device_lost) = d.update(|s| {
        s.job_mut(side, id)
            .map(|job| {
                super::contract::work_ends(d, job);
                let report = job.final_report();
                (report.violations, report.device_lost)
            })
            .unwrap_or_default()
    });
    let _ = write_lease(w, &ClientMsg::Violations { violations, device_lost }).await;
}

/// The arbiter socket takes `arbiter_attach` and nothing else: no side's
/// jobs, status or serve control go through it.
async fn arbiter_connection(d: Arc<Daemon>, stream: UnixStream, peer_pid: u32) {
    let (r, mut w) = stream.into_split();
    let mut lines = BufReader::new(r).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let reply = match serde_json::from_str::<Request>(&line) {
            Ok(Request::ArbiterAttach) => {
                arbiter_session(d, peer_pid, lines, w).await;
                return;
            }
            Ok(_) => Reply::error("this socket is gpu-arbiter's: arbiter_attach only"),
            Err(e) => Reply::error(format!("bad request: {e}")),
        };
        if write_line(&mut w, &reply).await.is_err() {
            return;
        }
    }
}

async fn arbiter_session(
    d: Arc<Daemon>,
    host_pid: u32,
    mut lines: Lines<BufReader<OwnedReadHalf>>,
    mut w: OwnedWriteHalf,
) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let conn = d.update(|s| {
        let conn = s.next_conn;
        s.next_conn += 1;
        s.arbiter = Some(Arbiter { conn, tx, host_pid });
        s.ninfer.link_lost_at = None;
        conn
    });
    eprintln!("gpu-switch: arbiter attached (pid {host_pid})");
    let _ = write_line(&mut w, &Reply::ok()).await;
    loop {
        tokio::select! {
            cmd = rx.recv() => match cmd {
                Some(cmd) => if write_line(&mut w, &cmd).await.is_err() { break },
                None => break,
            },
            line = lines.next_line() => match line {
                Ok(Some(line)) => match serde_json::from_str::<ArbiterEvent>(&line) {
                    Ok(ev) => d.update(|s| arbiter_event(&d, s, host_pid, ev)),
                    Err(e) => eprintln!("gpu-switch: bad arbiter event: {e}"),
                },
                _ => break,
            },
        }
    }
    eprintln!("gpu-switch: arbiter detached");
    d.update(|s| {
        if s.arbiter.as_ref().is_some_and(|a| a.conn == conn) {
            s.arbiter = None;
            s.ninfer.link_lost_at = Some(Instant::now());
        }
    });
}

/// A re-attached arbiter runs an NInfer gpu-switch had not kept track of:
/// it holds the GPU, so it owns it.
fn adopt(s: &mut State, phase: NPhase) {
    if matches!(
        s.ninfer.phase,
        NPhase::Idle | NPhase::Queued | NPhase::Releasing
    ) {
        eprintln!("gpu-switch: the arbiter runs NInfer; it owns the GPU");
        s.ninfer.phase = phase;
        s.ninfer.stopped_at = None;
    }
}

fn arbiter_event(d: &Daemon, s: &mut State, arbiter_pid: u32, ev: ArbiterEvent) {
    let now = Instant::now();
    match ev {
        ArbiterEvent::Starting { pid } => {
            s.ninfer.ns_pid = Some(pid);
            s.ninfer.host_pid = procs::child_by_inner_pid(arbiter_pid, pid);
            adopt(s, NPhase::Starting);
        }
        ArbiterEvent::Serving { pid } => {
            s.ninfer.ns_pid = Some(pid);
            if s.ninfer.host_pid.is_none() {
                s.ninfer.host_pid = procs::child_by_inner_pid(arbiter_pid, pid);
            }
            adopt(s, NPhase::Serving);
            if s.ninfer.phase == NPhase::Starting {
                s.ninfer.phase = NPhase::Serving;
                s.ninfer.backoff = secs(d.config.restart_backoff_initial_secs);
                s.ninfer.retry_at = None;
                eprintln!("gpu-switch: NInfer serving");
            }
        }
        ArbiterEvent::Draining => s.ninfer.draining = true,
        ArbiterEvent::Stopped { .. } => {
            if matches!(
                s.ninfer.phase,
                NPhase::Starting | NPhase::Serving | NPhase::Stopping
            ) {
                s.ninfer.phase = NPhase::Releasing;
                s.ninfer.stopped_at = Some(now);
            }
        }
        ArbiterEvent::Failed { error } => {
            eprintln!("gpu-switch: NInfer failed: {error}");
            s.ninfer.error = Some(error);
            s.ninfer.failed = true;
            s.ninfer.restart = false;
            s.ninfer.retry_at = Some(now + s.ninfer.backoff);
            s.ninfer.backoff = (s.ninfer.backoff * 2).min(secs(d.config.restart_backoff_max_secs));
            s.skip_linger = true;
            if s.ninfer.phase != NPhase::Idle && s.ninfer.phase != NPhase::Queued {
                s.ninfer.phase = NPhase::Releasing;
                s.ninfer.stopped_at = Some(now);
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::protocol::{Category, Mode, WatchdogPolicy};

    /// A test's own state directory, read by nothing else. It is on tmpfs, which the nix build sandbox mounts at
    /// /dev/shm too, so no test's journal sync waits on a disk. It is removed when its owner drops, also when the test
    /// fails.
    pub(crate) struct StateDir(std::path::PathBuf);

    impl StateDir {
        pub(crate) fn new() -> StateDir {
            static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let dir = std::path::Path::new("/dev/shm").join(format!(
                "gpu-switch-unit-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            StateDir(dir)
        }

        pub(crate) fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    impl Drop for StateDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// `config` with no sockets and the state directory `dir`.
    fn config_in(config: serde_json::Value, dir: &StateDir) -> crate::switch::config::SwitchConfig {
        let mut full = serde_json::json!({"listen": [], "state_dir": dir.path()});
        for (key, value) in config.as_object().into_iter().flatten() {
            full[key] = value.clone();
        }
        serde_json::from_value(full).unwrap()
    }

    /// A daemon of its own state directory, with no sockets and no card.
    pub(crate) fn daemon(config: serde_json::Value) -> (Arc<Daemon>, StateDir) {
        let dir = StateDir::new();
        let config = config_in(config, &dir);
        (Daemon::new(config, dir.path().join("switch.json")).unwrap(), dir)
    }

    /// A daemon of its own state directory reading the card through `driver`, as `incarnation`.
    pub(crate) fn reading(
        config: serde_json::Value,
        driver: Option<Box<dyn crate::gpu::Driver>>,
        incarnation: crate::process_identity::Instance,
    ) -> (Arc<Daemon>, StateDir) {
        let dir = StateDir::new();
        let config = config_in(config, &dir);
        (Daemon::reading(config, dir.path().join("switch.json"), driver, incarnation).unwrap(), dir)
    }

    /// An incarnation of this test process, told apart from others of it by `start_ticks`.
    pub(crate) fn incarnation(start_ticks: u64) -> crate::process_identity::Instance {
        crate::process_identity::Instance {
            boot_id: crate::process_identity::Instance::current().unwrap().boot_id,
            pid: std::process::id(),
            start_ticks,
        }
    }

    pub(crate) fn spec(share: Option<&str>) -> JobSpec {
        JobSpec {
            mode: Mode::Shared,
            vram: 1 << 30,
            category: Category::Other,
            cooperative: false,
            priority: 0,
            max_runtime_secs: None,
            expected_runtime_secs: Some(60),
            watchdog: WatchdogPolicy::Standard,
            share: share.map(String::from),
        }
    }

    /// A fresh lease entry ready on arrival, whose client hears nothing.
    pub(crate) fn register(d: &Daemon, side: Side, spec: JobSpec) -> u64 {
        enter(d, side, spec, false, None).0
    }

    /// A fresh gpu-run job ready on arrival; the receiver hears what its runner would.
    pub(crate) fn submit(d: &Daemon, side: Side, spec: JobSpec) -> (u64, mpsc::UnboundedReceiver<RunnerEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let id = d.update(|s| {
            new_job(
                s,
                &d.secret,
                side,
                std::process::id(),
                Conn::Runner(tx),
                NewJob {
                    spec,
                    cmd: vec!["true".into()],
                    label: None,
                    group: None,
                    cwd: String::new(),
                    log_dir: None,
                    arrival: Arrival::Fresh,
                    prepares: false,
                },
            )
        });
        (id, rx)
    }

    /// A fresh lease entry, a place when `prepares`; the receiver hears what its client would.
    pub(crate) fn enter(
        d: &Daemon,
        side: Side,
        spec: JobSpec,
        prepares: bool,
        group: Option<&str>,
    ) -> (u64, mpsc::UnboundedReceiver<ClientMsg>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let id = d.update(|s| {
            new_job(
                s,
                &d.secret,
                side,
                std::process::id(),
                Conn::Client(tx),
                NewJob {
                    spec,
                    cmd: Vec::new(),
                    label: None,
                    group: group.map(String::from),
                    cwd: String::new(),
                    log_dir: None,
                    arrival: Arrival::Fresh,
                    prepares,
                },
            )
        });
        (id, rx)
    }

    #[test]
    fn a_saved_transfer_is_acknowledged_again_and_its_previous_owner_resumes_nothing() {
        let (d, dir) = daemon(serde_json::json!({}));
        let side = Side::Host(1000);
        let owner = std::process::id();
        let (id, _client) = enter(&d, side, spec(Some("kernel-a")), true, None);
        let mut sibling = std::process::Command::new("sleep").arg("30").spawn().unwrap();
        let mut child = std::process::Command::new("sleep").arg("30").spawn().unwrap();
        let to = crate::protocol::ProcessIdentity { pid: child.id(), birth: procs::started(child.id()).unwrap() };
        let transfer = |d: &Daemon, requester: u32, to| {
            d.update(|s| super::super::preparation::transfer(s.job_mut(side, id).unwrap(), requester, to))
        };
        assert!(transfer(&d, owner, to).unwrap_err().contains("told place"), "an untold place stays");
        d.with(|s| s.job_mut(side, id).unwrap().place.as_mut().unwrap().told_at = Some(1.0));
        let init = crate::protocol::ProcessIdentity { pid: 1, birth: procs::started(1).unwrap() };
        assert!(transfer(&d, owner, init).unwrap_err().contains("live descendant"));
        assert!(transfer(&d, sibling.id(), to).unwrap_err().contains("by its owner"));
        transfer(&d, owner, to).unwrap();
        crate::switch::persist::save(&d);
        let resume = d.with(|s| resume_of(s, side, id)).unwrap();
        // Replaced before it answered: the successor has the transfer.
        let (successor, _other) = daemon(serde_json::json!({"state_dir": dir.path()}));
        crate::switch::persist::restore(&successor);
        transfer(&successor, owner, to).unwrap();
        successor.with(|s| assert_eq!(s.job(side, id).unwrap().runner_pid, child.id(), "a resent transfer changes nothing"));
        let new = || NewJob {
            spec: spec(Some("kernel-a")),
            cmd: Vec::new(),
            label: None,
            group: None,
            cwd: String::new(),
            log_dir: None,
            arrival: Arrival::Fresh,
            prepares: true,
        };
        let conn = || Conn::Client(mpsc::unbounded_channel().0);
        let previous = successor
            .update(|s| resume_job(&successor, s, side, owner, conn(), new(), &resume))
            .unwrap();
        assert_eq!(previous.transferred, Some(to), "the previous owner's resume names the new owner");
        let adopted = successor
            .update(|s| resume_job(&successor, s, side, child.id(), conn(), new(), &resume))
            .unwrap();
        assert_eq!((adopted.id, adopted.transferred, adopted.reregistered), (id, None, false));
        successor.update(|s| {
            let job = s.job_mut(side, id).unwrap();
            job.state = JobState::Running;
            let running = Resume { running: true, ..resume.clone() };
            assert!(s.owned_lease(side, child.id(), &running, "retention").is_ok());
            assert!(s.owned_lease(side, owner, &running, "retention").is_err(), "the new owner only");
        });
        for process in [&mut sibling, &mut child] {
            process.kill().unwrap();
            process.wait().unwrap();
        }
    }

    /// A told place lost with gpu-switch's state comes back untold, at its token's place, reregistered.
    #[test]
    fn a_place_lost_with_the_state_is_registered_again_untold_at_its_proven_place() {
        let (d, dir) = daemon(serde_json::json!({}));
        let side = Side::Host(1000);
        let (id, _client) = enter(&d, side, spec(Some("kernel-a")), true, None);
        d.with(|s| s.job_mut(side, id).unwrap().place.as_mut().unwrap().told_at = Some(1.0));
        let resume = d.with(|s| resume_of(s, side, id)).unwrap();
        let (successor, _other) = daemon(serde_json::json!({"state_dir": dir.path()}));
        let new = || NewJob {
            spec: spec(Some("kernel-a")),
            cmd: Vec::new(),
            label: None,
            group: None,
            cwd: String::new(),
            log_dir: None,
            arrival: Arrival::Fresh,
            prepares: true,
        };
        let conn = || Conn::Client(mpsc::unbounded_channel().0);
        let again = successor
            .update(|s| resume_job(&successor, s, side, std::process::id(), conn(), new(), &resume))
            .unwrap();
        assert!(again.reregistered);
        successor.with(|s| {
            let job = s.job(side, again.id).unwrap();
            assert!(!job.place.as_ref().unwrap().told(), "re-registered untold");
            assert_eq!(job.key, resume.key);
        });
        let kept = d
            .update(|s| {
                s.job_mut(side, id).unwrap().conn = Conn::Detached { until: Instant::now() };
                resume_job(&d, s, side, std::process::id(), conn(), new(), &resume)
            })
            .unwrap();
        assert!(!kept.reregistered && kept.id == id, "a kept job is attached, not registered again");
    }

    fn line_ids(d: &Daemon) -> Vec<u64> {
        d.with(|s| {
            s.line()
                .into_iter()
                .filter_map(|(_, e)| match e {
                    crate::switch::state::Entry::Job(_, id) => Some(id),
                    crate::switch::state::Entry::Ninfer => None,
                })
                .collect()
        })
    }

    #[test]
    fn work_naming_no_share_forms_its_users_pool_and_a_campaign_takes_turns_with_it() {
        let (d, _dir) = daemon(serde_json::json!({}));
        let side = Side::Host(1000);
        let pool: Vec<u64> = (0..900).map(|_| register(&d, side, spec(None))).collect();
        assert!(d.with(|s| s.jobs().all(|j| j.share == "uid-1000")));
        let campaign = register(&d, side, spec(Some("kernel-campaign")));
        assert_eq!(&line_ids(&d)[..3], [pool[0], campaign, pool[1]]);
        let container = register(&d, Side::Container, spec(None));
        assert_eq!(d.with(|s| s.job(Side::Container, container).unwrap().share.clone()), "container");
    }

    #[test]
    fn a_lease_request_without_a_share_joins_its_users_pool() {
        let request: LeaseRequest = serde_json::from_value(serde_json::json!({"mode": "exclusive"})).unwrap();
        assert_eq!(request.share, None);
        let (d, _dir) = daemon(serde_json::json!({}));
        let id = register(&d, Side::Host(4242), JobSpec { mode: Mode::Exclusive, vram: 0, share: request.share, ..spec(None) });
        assert_eq!(d.with(|s| s.job(Side::Host(4242), id).unwrap().share.clone()), "uid-4242");
        assert!(spec(Some("")).validate().is_err());
        assert!(spec(Some(&"x".repeat(129))).validate().is_err());
        assert!(spec(Some("kernel/campaign")).validate().is_err());
        assert!(spec(Some(&format!("kernel-{}.a_b", "x".repeat(110)))).validate().is_ok());
    }

    #[test]
    fn a_resumed_entry_keeps_its_token_proven_place_and_share() {
        let (d, dir) = daemon(serde_json::json!({}));
        let side = Side::Host(1000);
        for _ in 0..3 {
            register(&d, side, spec(None));
        }
        let id = register(&d, side, spec(Some("kernel-campaign")));
        let resume = d.with(|s| resume_of(s, side, id)).unwrap();
        // A successor that kept no state but the secret.
        let (successor, _other) = daemon(serde_json::json!({"state_dir": dir.path()}));
        let again = successor
            .update(|s| {
                resume_job(
                    &successor,
                    s,
                    side,
                    std::process::id(),
                    Conn::Client(mpsc::unbounded_channel().0),
                    NewJob {
                        spec: spec(Some("kernel-campaign")),
                        cmd: Vec::new(),
                        label: None,
                        group: None,
                        cwd: String::new(),
                        log_dir: None,
                        arrival: Arrival::Fresh,
                        prepares: false,
                    },
                    &resume,
                )
            })
            .unwrap()
            .id;
        successor.with(|s| {
            let job = s.job(side, again).unwrap();
            assert_eq!(job.key, resume.key);
            assert_eq!(job.share, "kernel-campaign");
        });
    }
}

/// The final report and the holder's view of a loss (PLAN-L §3.4.4; T-14 (a) to (c)): the real session on a socket,
/// the native `Lease` as its client.
#[cfg(test)]
mod final_report {
    use super::tests::{incarnation, reading, StateDir};
    use super::*;
    use crate::protocol::{DeviceLossReport, LeaseRequest};
    use crate::switch::card::{self, Heard};
    use crate::switch::card_reader::tests::{lost, Act, Call, Script};
    use crate::switch::card_reader::Observation;

    struct Service {
        d: Arc<Daemon>,
        script: Script,
        dir: StateDir,
        tasks: Vec<tokio::task::JoinHandle<()>>,
    }

    impl Service {
        async fn start() -> Service {
            let script = Script::new();
            let (d, dir) = reading(serde_json::json!({}), Some(script.driver()), incarnation(1));
            let mut service = Service { d, script, dir, tasks: Vec::new() };
            assert_eq!(service.publish(service.d.card.sample().await), Heard::Read(()));
            let listener = UnixListener::bind(service.socket()).unwrap();
            service.tasks = vec![
                tokio::spawn(serve(service.d.clone(), listener, Role::Host)),
                tokio::spawn(crate::switch::daemon::run(service.d.clone())),
            ];
            service
        }

        fn socket(&self) -> std::path::PathBuf {
            self.dir.path().join("host.sock")
        }

        fn publish<T: card::Reading>(&self, observation: Observation<T>) -> Heard<()> {
            self.d.observe(|s| card::observe(&self.d, s, observation)).map_value()
        }

        async fn lease(&self) -> crate::lease::Lease {
            let socket = self.socket();
            tokio::task::spawn_blocking(move || {
                let request: LeaseRequest =
                    serde_json::from_value(serde_json::json!({"mode": "shared", "vram": 1024, "expected_runtime_secs": 60}))
                        .unwrap();
                crate::lease::Lease::acquire(&socket, request, |_| {}).unwrap()
            })
            .await
            .unwrap()
        }

        /// The session job's associations.
        fn device_lost(&self) -> Vec<crate::protocol::GrantLoss> {
            self.d.with(|s| s.jobs().next().expect("the session's job").device_lost.clone())
        }

        fn journal(&self, event: &str) -> Vec<serde_json::Value> {
            std::fs::read_to_string(self.dir.path().join("failures.jsonl"))
                .unwrap_or_default()
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .filter(|record| record["event"] == event)
                .collect()
        }

        /// Until the session's settlement has finished its run.
        async fn ended(&self) {
            self.d.wait_for(|s| s.jobs().next().filter(|job| job.state.is_terminal()).map(|_| ())).await;
        }
    }

    impl Drop for Service {
        fn drop(&mut self) {
            self.tasks.iter().for_each(tokio::task::JoinHandle::abort);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_release_that_wins_over_its_queued_loss_notice_carries_the_loss_in_its_final_report() {
        let service = Service::start().await;
        let mut lease = service.lease().await;
        service.d.hooks.release_arm.arm();
        let releasing = tokio::task::spawn_blocking(move || {
            lease.release();
            lease
        });
        let reached = service.d.hooks.release_arm.reached().await;
        service.script.then(Call::Sample, Act::Fail(lost()));
        assert_eq!(service.publish(service.d.card.sample().await), Heard::Failed(lost()), "marked, its notice queued");
        reached.release();
        let lease = releasing.await.unwrap();
        let DeviceLossReport::Lost(grant) = lease.device_loss() else {
            panic!("{:?}", lease.device_loss())
        };
        assert_eq!(grant.run, 1);
        service.ended().await;
        assert_eq!(service.device_lost(), [grant], "the job record carries it");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_loss_first_published_by_settlement_after_a_release_lies_outside_the_grant() {
        let service = Service::start().await;
        let mut lease = service.lease().await;
        service.script.then(Call::Compute, Act::Fail(lost()));
        let lease = tokio::task::spawn_blocking(move || {
            lease.release();
            lease
        })
        .await
        .unwrap();
        service.ended().await;
        assert_eq!(lease.device_loss(), DeviceLossReport::None);
        assert!(service.device_lost().is_empty(), "the grant ended at its final report");
        let lost = service.journal("card_lost");
        assert_eq!(lost[0]["released"].as_array().unwrap().len(), 1, "listed as released: {lost:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_same_loss_after_an_eof_marks_the_grant() {
        let service = Service::start().await;
        service.script.then(Call::Compute, Act::Fail(lost()));
        let socket = service.socket();
        tokio::task::spawn_blocking(move || {
            use std::io::{BufRead, Write};
            let stream = std::os::unix::net::UnixStream::connect(socket).unwrap();
            let request: LeaseRequest =
                serde_json::from_value(serde_json::json!({"mode": "shared", "vram": 1024, "expected_runtime_secs": 60}))
                    .unwrap();
            let mut line = serde_json::to_vec(&Request::Lease(request)).unwrap();
            line.push(b'\n');
            (&stream).write_all(&line).unwrap();
            let granted = std::io::BufReader::new(&stream)
                .lines()
                .map_while(Result::ok)
                .any(|line| serde_json::from_str::<serde_json::Value>(&line).unwrap()["granted"] == true);
            assert!(granted);
        })
        .await
        .unwrap();
        service.ended().await;
        assert_eq!(service.device_lost().len(), 1, "after EOF the open grant is marked");
        assert_eq!(service.journal("card_lost")[0]["marked"].as_array().unwrap().len(), 1);
    }
}

/// A holder that reattaches across restarts of gpu-switch (PLAN-L §3.8; T-14 (e), (f)). Each gpu-switch is an
/// incarnation of the real service in process, on a runtime of its own, and stops as a crash stops it: its runtime
/// goes and its connections with it. A successor restores the snapshot the test left. The holder is a role process,
/// the native `Lease` or gpu-run's runner, and what it printed is read from its stderr.
#[cfg(test)]
mod reattachment {
    use super::tests::{incarnation, reading, StateDir};
    use super::*;
    use crate::protocol::{Rule, Violation};
    use crate::switch::card::{self, Heard};
    use crate::switch::card_reader::tests::{lost, Act, Call, Script};

    const LEASE: &str = "switch::control::reattachment::a_lease_held_across_restarts";
    const RUNNER: &str = "runner::tests::a_runner_held_across_restarts";
    /// What every loss notice of the holder's run begins with.
    const NOTICE: &str = "THE GPU WAS FOUND LOST during run 1 of ";

    /// The socket every incarnation serves.
    fn socket(dir: &StateDir) -> std::path::PathBuf {
        dir.path().join("host.sock")
    }

    /// The file whose presence keeps the holder holding.
    fn hold(dir: &std::path::Path) -> std::path::PathBuf {
        dir.join("hold")
    }

    /// One incarnation of gpu-switch on the test's state directory.
    struct Incarnation {
        d: Arc<Daemon>,
        script: Script,
        runtime: tokio::runtime::Runtime,
        _own: StateDir,
    }

    impl Incarnation {
        /// Incarnation `ticks` of this boot, restored from what `dir` holds; it serves nothing yet.
        fn restored(dir: &StateDir, ticks: u64) -> Incarnation {
            let script = Script::new();
            let (d, own) = reading(serde_json::json!({"state_dir": dir.path()}), Some(script.driver()), incarnation(ticks));
            crate::switch::persist::restore(&d);
            let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
            Incarnation { d, script, runtime, _own: own }
        }

        /// The next read of the card, published.
        fn read(&self) -> Heard<()> {
            let observation = self.runtime.block_on(self.d.card.sample());
            self.d.observe(|s| card::observe(&self.d, s, observation)).map_value()
        }

        /// The card is found lost by the next read.
        fn lose(&self) {
            self.script.then(Call::Sample, Act::Fail(lost()));
            assert_eq!(self.read(), Heard::Failed(lost()));
        }

        /// Serves the test's socket, and runs the queue.
        fn listen(&self, dir: &StateDir) {
            let _ = std::fs::remove_file(socket(dir));
            let _entered = self.runtime.enter();
            let listener = UnixListener::bind(socket(dir)).unwrap();
            self.runtime.spawn(serve(self.d.clone(), listener, Role::Host));
            self.runtime.spawn(crate::switch::daemon::run(self.d.clone()));
        }

        /// Tells the holder marker `n`, a notice that the holder prints and that no replay repeats.
        fn mark(&self, n: u32) {
            let marker = Violation { rule: Rule::NoVram, run: 100 + n, text: format!("marker {n}"), at: 0.0 };
            self.d.with(|s| s.jobs().next().expect("the holder's job").violation_notice(marker));
        }

        fn save(&self) {
            crate::switch::persist::save(&self.d);
        }

        /// The crash: the runtime goes, and every connection with it.
        fn stop(self) {
            self.runtime.shutdown_background();
        }
    }

    /// The holder, the role process `role`, holding while the hold file exists. Dropped, also on failure, its hold
    /// file goes with the test's directory and the role is killed.
    struct Holder {
        role: crate::roles::Role,
        log: std::path::PathBuf,
    }

    impl Holder {
        fn start(role: &str, dir: &StateDir) -> Holder {
            std::fs::write(hold(dir.path()), b"").unwrap();
            let log = dir.path().join("holder.log");
            let role = crate::roles::Role::logged(role, &[("GQ_REATTACH_DIR", dir.path().as_os_str())], &log);
            Holder { role, log }
        }

        fn printed(&self) -> String {
            std::fs::read_to_string(&self.log).unwrap_or_default()
        }

        /// Until `done`, while the holder runs: a holder that ended fails the test with what it printed.
        fn until(&mut self, what: &str, done: impl Fn(&Holder) -> bool) {
            while !done(self) {
                assert!(self.role.running(), "the holder ended before {what}:\n{}", self.printed());
                std::thread::sleep(Duration::from_millis(20));
            }
        }

        /// Until the holder has printed `text`.
        fn until_printed(&mut self, text: &str) {
            self.until(text, |holder| holder.printed().contains(text));
        }

        /// Until the holder's job at `at` runs.
        fn running(&mut self, at: &Incarnation) {
            self.until("its grant", |_| at.d.with(|s| s.jobs().next().is_some_and(|job| job.state == JobState::Running)));
        }

        /// Until the holder has resumed its job at `at`. The resume and the session's replay are one state change,
        /// so a notice sent afterwards follows the replay on the holder's channel.
        fn attached(&mut self, at: &Incarnation) {
            self.until("its resume", |_| at.d.with(|s| s.jobs().next().is_some_and(|job| job.detached().is_none())));
        }

        /// How many loss notices of its run the holder printed.
        fn losses(&self) -> usize {
            self.printed().matches(NOTICE).count()
        }

        /// The holder ends its grant and exits, its own checks passed.
        fn end(mut self, dir: &StateDir) -> String {
            std::fs::remove_file(hold(dir.path())).unwrap();
            assert!(self.role.status().success(), "{}", self.printed());
            self.printed()
        }
    }

    /// T-14 (e): the holder reattaches after a loss that it never heard, published by a successor restored from a
    /// snapshot that precedes the loss, while the holder was away. Only the session's replay can tell it; a second
    /// reattachment replays it again, and it prints it once.
    fn a_loss_missed_while_away(role: &str) {
        let dir = StateDir::new();
        let first = Incarnation::restored(&dir, 1);
        assert_eq!(first.read(), Heard::Read(()));
        first.listen(&dir);
        let mut holder = Holder::start(role, &dir);
        holder.running(&first);
        first.save();
        first.stop();
        let second = Incarnation::restored(&dir, 2);
        second.lose();
        second.listen(&dir);
        holder.attached(&second);
        second.mark(1);
        holder.until_printed("marker 1");
        assert_eq!(holder.losses(), 1, "the replay told the loss:\n{}", holder.printed());
        second.save();
        second.stop();
        let third = Incarnation::restored(&dir, 3);
        third.listen(&dir);
        holder.attached(&third);
        third.mark(2);
        holder.until_printed("marker 2");
        assert_eq!(holder.losses(), 1, "its replay printed nothing:\n{}", holder.printed());
        let printed = holder.end(&dir);
        assert_eq!(printed.matches(NOTICE).count(), 1, "{printed}");
        third.stop();
    }

    /// T-14 (f): the holder heard incarnation 1's loss (I1, 1) of its run, then reattaches to incarnation 2, restored
    /// from the snapshot taken before that loss. Incarnation 2's first loss (I2, 1) marks the run again, and the
    /// holder prints it as new; incarnation 3's replay of it prints nothing.
    fn a_new_incarnations_loss_after_a_heard_one(role: &str) {
        let dir = StateDir::new();
        let first = Incarnation::restored(&dir, 1);
        assert_eq!(first.read(), Heard::Read(()));
        first.listen(&dir);
        let mut holder = Holder::start(role, &dir);
        holder.running(&first);
        // The snapshot before the loss: nothing saves after it.
        first.save();
        first.lose();
        holder.until_printed(NOTICE);
        first.stop();
        let second = Incarnation::restored(&dir, 2);
        assert!(second.d.with(|s| s.jobs().next().unwrap().device_lost.is_empty()), "restored from before the loss");
        second.lose();
        second.listen(&dir);
        holder.attached(&second);
        second.mark(1);
        holder.until_printed("marker 1");
        assert_eq!(holder.losses(), 2, "(I2, 1) is new to the holder:\n{}", holder.printed());
        second.save();
        second.stop();
        let third = Incarnation::restored(&dir, 3);
        third.listen(&dir);
        holder.attached(&third);
        third.mark(2);
        holder.until_printed("marker 2");
        assert_eq!(holder.losses(), 2, "its replay printed nothing:\n{}", holder.printed());
        let printed = holder.end(&dir);
        assert_eq!(printed.matches(NOTICE).count(), 2, "{printed}");
        third.stop();
    }

    #[test]
    fn a_reattached_lease_hears_a_loss_it_missed_once() {
        a_loss_missed_while_away(LEASE);
    }

    #[test]
    fn a_reattached_runner_hears_a_loss_it_missed_once() {
        a_loss_missed_while_away(RUNNER);
    }

    #[test]
    fn a_lease_hears_a_new_incarnations_loss_as_new_and_its_replay_once() {
        a_new_incarnations_loss_after_a_heard_one(LEASE);
    }

    #[test]
    fn a_runner_hears_a_new_incarnations_loss_as_new_and_its_replay_once() {
        a_new_incarnations_loss_after_a_heard_one(RUNNER);
    }

    /// The lease holder's role: a native lease on the test's socket, held while the hold file exists, then released.
    /// Its final report prints nothing it heard before.
    #[test]
    #[ignore]
    fn a_lease_held_across_restarts() {
        let Some(dir) = std::env::var_os("GQ_REATTACH_DIR").map(std::path::PathBuf::from) else {
            return;
        };
        let request: LeaseRequest = serde_json::from_value(serde_json::json!({"mode": "shared", "vram": 1024,
            "expected_runtime_secs": 60, "label": "holder"}))
        .unwrap();
        let mut lease = crate::lease::Lease::acquire(&dir.join("host.sock"), request, |_| {}).unwrap();
        while hold(&dir).exists() {
            std::thread::sleep(Duration::from_millis(20));
        }
        lease.release();
        assert!(matches!(lease.device_loss(), crate::protocol::DeviceLossReport::Lost(_)), "{:?}", lease.device_loss());
    }
}
