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
    now_unix, ArbiterEvent, Category, JobSpec, JobState, LeaseJob, LeaseRequest, Placement, Reply,
    Request, Resume, RunnerEvent, RunnerReport, Sig, SubmitRequest,
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
        tokio::spawn(connection(d.clone(), stream, side, pid));
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

async fn connection(d: Arc<Daemon>, stream: UnixStream, side: Side, peer_pid: u32) {
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
            Request::Release | Request::ShutdownStarted { .. } | Request::Failed { .. } => {
                Reply::error("request requires a lease session")
            }
            Request::ArbiterAttach => {
                Reply::error("arbiter_attach is for gpu-arbiter's own socket")
            }
            Request::Status => Reply {
                status: Some(d.with(|s| view::status(&d, s, side, Instant::now()))),
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
    let key = match arrival {
        Arrival::Requeued => behind_head(s),
        Arrival::At(key) => key,
        Arrival::Fresh => placement(s, spec.priority),
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
    sj.jobs.insert(
        id,
        Job {
            id,
            handle,
            side,
            spec,
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
        },
    );
    id
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
) -> Result<u64, String> {
    if let Some(job) = s
        .job_mut(side, resume.id)
        .filter(|j| j.token == resume.token)
    {
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
        return Ok(job.id);
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
    Ok(id)
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
        label: None,
        group: None,
        cwd,
        log_dir: Some(&log_dir),
        arrival: Arrival::Fresh,
    };
    let created = d.update(|s| {
        let id = match &resume {
            Some(resume) => resume_job(&d, s, side, runner_pid, Conn::Runner(tx), new, resume)?,
            None => new_job(s, &d.secret, side, runner_pid, Conn::Runner(tx), new),
        };
        let j = s.job(side, id).expect("inserted");
        // A resumed runner may have missed its go; a running one ignores it.
        if j.state == JobState::Running {
            j.go();
        }
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
        category: Category::Other,
        cooperative,
        priority,
        max_runtime_secs,
        expected_runtime_secs,
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
    };
    let created = d.update(|s| {
        let id = match &resume {
            Some(resume) => resume_job(&d, s, side, peer_pid, Conn::Client(tx), new, resume)?,
            None => new_job(s, &d.secret, side, peer_pid, Conn::Client(tx), new),
        };
        let job = s.job_mut(side, id).expect("inserted");
        job.shutdown_grace_secs = shutdown_grace_secs;
        if job.state == JobState::Running {
            job.go();
        }
        if let Some(shutdown) = &job.shutdown {
            job.shutdown_notice(shutdown.clone());
        }
        let handle = job.handle.clone();
        let resume = resume_of(s, side, id).expect("inserted");
        Ok::<_, String>(LeaseJob { id, handle, resume })
    });
    let job = match created {
        Ok(job) => job,
        Err(e) => {
            let _ = write_lease(&mut w, &Reply::error(e)).await;
            return;
        }
    };
    let id = job.id;
    eprintln!("gpu-switch: lease {} for pid {peer_pid}", job.handle);
    let mut reply = serde_json::json!({"ok": true, "job": job, "shutdown_protocol": 1, "lease_runtime_protocol": 1});
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
                            Ok(Request::Release) => break,
                            Ok(Request::Failed { reason }) => {
                                d.update(|s| {
                                    if let Some(job) = s.job_mut(side, id) {
                                        job.end_reason.get_or_insert((JobState::Failed,
                                            reason.chars().take(4096).collect()));
                                    }
                                });
                                break;
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
