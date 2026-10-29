//! What a viewer may see. Their own jobs in full; any other side's jobs as
//! handle, state, position, mode, VRAM, cooperative, priority.

use std::collections::HashMap;
use std::time::Instant;

use crate::protocol::{
    Counts, JobState, LineEntry, LineKind, OtherJob, OwnJob, ServeStatus, Status,
};
use crate::switch::daemon::Daemon;
use crate::switch::state::{Entry, Job, NPhase, Side, State};

pub fn serve_word(s: &State, now: Instant) -> &'static str {
    match s.ninfer.phase {
        NPhase::Starting => "STARTING",
        NPhase::Serving => "SERVING",
        NPhase::Stopping if s.ninfer.draining => "DRAINING",
        NPhase::Stopping => "STOPPING",
        NPhase::Releasing if s.ninfer.failed => "FAILED",
        NPhase::Releasing => "STOPPING",
        NPhase::Idle | NPhase::Queued if s.switch_to_ninfer && s.arbiter.is_some() => {
            "SWITCHING_TO_NINFER"
        }
        NPhase::Queued => "WAITING_GPU",
        NPhase::Idle if !s.serve_wanted => "OFF",
        NPhase::Idle if s.arbiter.is_none() => "WAITING_ARBITER",
        NPhase::Idle if s.ninfer.retry_at.is_some_and(|t| t > now) => "FAILED",
        NPhase::Idle if !s.queue_empty() => "RUNNING_JOBS",
        NPhase::Idle => "LINGERING",
    }
}

pub fn serve_status(d: &Daemon, s: &State, now: Instant) -> ServeStatus {
    let next = s.profile_next(&d.config.profile);
    ServeStatus {
        phase: serve_word(s, now).into(),
        profile_current: s.profile_current.clone(),
        profile_pending: if next != s.profile_current {
            next
        } else {
            None
        },
    }
}

struct Line {
    entries: Vec<LineEntry>,
    position: HashMap<(Side, u64), usize>,
    ninfer_at: Option<usize>,
}

fn line(s: &State, viewer: Side) -> Line {
    let mut out = Line {
        entries: Vec::new(),
        position: HashMap::new(),
        ninfer_at: None,
    };
    for (i, (_, entry)) in s.line().into_iter().enumerate() {
        let position = i + 1;
        let (kind, job) = match entry {
            Entry::Ninfer => {
                out.ninfer_at = Some(position);
                (LineKind::Ninfer, None)
            }
            Entry::Job(side, id) => {
                out.position.insert((side, id), position);
                if side == viewer {
                    (LineKind::Own, Some(id.to_string()))
                } else {
                    (LineKind::Other, s.job(side, id).map(|j| j.handle.clone()))
                }
            }
        };
        out.entries.push(LineEntry {
            position,
            kind,
            job,
            priority: s.entry_priority(entry),
        });
    }
    out
}

fn behind_ninfer(s: &State, l: &Line, job: &Job) -> bool {
    if job.state != JobState::Queued {
        return false;
    }
    if s.ninfer_owns() {
        return true;
    }
    match (l.ninfer_at, l.position.get(&(job.side, job.id))) {
        (Some(n), Some(p)) => n < *p,
        _ => false,
    }
}

fn count(s: &State, l: &Line, jobs: &[&Job]) -> Counts {
    Counts {
        queued: jobs.iter().filter(|j| j.state == JobState::Queued).count(),
        running: jobs.iter().filter(|j| j.state == JobState::Running).count(),
        waiting: jobs.iter().filter(|j| behind_ninfer(s, l, j)).count(),
    }
}

fn own_job(s: &State, l: &Line, j: &Job) -> OwnJob {
    OwnJob {
        vram_retention_deadline: j.retention.as_ref().map(|r| r.deadline),
        terminal_status: j.terminal_status,
        timing: j
            .timing
            .as_ref()
            .map(|timing| timing.snapshot(super::state::monotonic_ns())),
        expected_runtime_secs: crate::estimate::seconds(j.spec.expected_runtime_secs),
        id: j.id,
        handle: j.handle.clone(),
        state: j.state,
        mode: j.mode(),
        vram: j.reserved_vram(),
        category: j.spec.category,
        cooperative: j.spec.cooperative,
        priority: j.spec.priority,
        position: l.position.get(&(j.side, j.id)).copied(),
        behind_ninfer: behind_ninfer(s, l, j),
        cmd: j.cmd.clone(),
        label: j.label.clone(),
        lease: j.is_lease(),
        cwd: j.cwd.clone(),
        log: j.log.clone(),
        exit_code: j.exit_code,
        signal: j.signal,
        error: j.error.clone(),
        runner_pid: j.runner_pid,
        pid: if j.is_lease() {
            j.pid_host
        } else {
            j.pid_inner
        },
        submitted_at: j.submitted_at,
        started_at: j.started_at,
        finished_at: j.finished_at,
        runs: j.runs,
    }
}

pub fn own_job_of(s: &State, viewer: Side, j: &Job) -> OwnJob {
    own_job(s, &line(s, viewer), j)
}

pub fn status(d: &Daemon, s: &State, viewer: Side, now: Instant) -> Status {
    let l = line(s, viewer);
    let own: Vec<&Job> = s.jobs().filter(|j| j.side == viewer).collect();
    let others: Vec<&Job> = s
        .jobs()
        .filter(|j| j.side != viewer && j.active())
        .collect();
    let own_counts = count(s, &l, &own);
    let other_counts = count(s, &l, &others);
    let serve = serve_status(d, s, now);
    let next_cache = s.host_cache_next(d.config.host_context_mib);
    let summary = summary(d, s, &serve.phase, &own_counts, &other_counts, now);
    Status {
        summary,
        serve,
        serve_wanted: s.serve_wanted,
        owner: if s.ninfer_owns() { "ninfer" } else { "queue" }.into(),
        profiles: d.config.profiles.clone(),
        host_cache_current_mib: s.host_cache_current,
        host_cache_pending_mib: if next_cache != s.host_cache_current {
            next_cache
        } else {
            None
        },
        gpu_processes: s.view.gpu.as_ref().map(|g| g.compute.len()).unwrap_or(0),
        jobs: own.iter().map(|j| own_job(s, &l, j)).collect(),
        other_jobs: others
            .iter()
            .map(|j| OtherJob {
                handle: j.handle.clone(),
                state: j.state,
                position: l.position.get(&(j.side, j.id)).copied(),
                mode: j.mode(),
                vram: j.reserved_vram(),
                cooperative: j.spec.cooperative,
                priority: j.spec.priority,
            })
            .collect(),
        own: own_counts,
        others: other_counts,
        line: l.entries,
        ninfer_error: if viewer == Side::Container {
            s.ninfer.error.clone()
        } else {
            None
        },
    }
}

fn summary(
    d: &Daemon,
    s: &State,
    word: &str,
    own: &Counts,
    other: &Counts,
    now: Instant,
) -> String {
    let queued = format!("queued: {} yours, {} others'", own.queued, other.queued);
    let running = format!("running: {} yours, {} others'", own.running, other.running);
    let waiting = own.queued + other.queued;
    match word {
        "SERVING" | "STARTING" if waiting == 0 => {
            format!("{word}: NInfer holds the GPU; nothing queued")
        }
        "SERVING" | "STARTING" => format!(
            "{word}: NInfer holds the GPU; {queued}; NOT running until 'gpu-run switch-gpu'"
        ),
        "DRAINING" | "STOPPING" => format!("{word}: NInfer is handing the GPU back; {queued}"),
        "SWITCHING_TO_NINFER" => format!(
            "SWITCHING_TO_NINFER: waiting for {} GPU-resident job(s) to finish; {waiting} queued, kept in order ({})",
            own.running + other.running,
            crate::switch::control::NINFER_FRONT_GUARANTEE
        ),
        "WAITING_GPU" => format!("WAITING_GPU: NInfer waits in line behind earlier work; {running}; {queued}"),
        "LINGERING" => {
            let left = s
                .empty_since
                .map(|t| d.config.linger_secs - (now - t).as_secs_f64())
                .unwrap_or(d.config.linger_secs)
                .max(0.0)
                .ceil();
            format!("LINGERING: queue empty, starting NInfer in {left}s unless more work arrives")
        }
        "RUNNING_JOBS" => format!(
            "RUNNING_JOBS: the GPU belongs to the line; {running}; {queued}; NInfer returns once it is empty"
        ),
        "OFF" => format!(
            "OFF: serving is off, jobs run in line order without 'switch-gpu'; {running}; {queued}"
        ),
        "FAILED" => format!("FAILED: NInfer failed to serve, retrying; {running}; {queued}"),
        "WAITING_ARBITER" => format!(
            "WAITING_ARBITER: serving is on, gpu-arbiter (NInfer's supervisor) is not connected; {running}; {queued}"
        ),
        _ => word.to_string(),
    }
}
