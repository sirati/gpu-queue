//! Periodic facts: the card (nvidia-smi) and the host process tree; and
//! idle policing of our jobs.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::nvsmi::Sample;
use crate::procs;
use crate::protocol::{JobState, Sig};
use crate::switch::config::secs;
use crate::switch::daemon::Daemon;
use crate::switch::state::{Job, State};

/// Samples every `gpu_poll_interval_secs`, and at once whenever a decision
/// waits on the card (at most every `gpu_wanted_poll_interval_secs`). A
/// sample is dated when it starts: it may have missed a later departure.
pub async fn run(d: Arc<Daemon>) {
    let interval = secs(d.config.gpu_poll_interval_secs);
    let wanted = secs(d.config.gpu_wanted_poll_interval_secs);
    loop {
        let started = Instant::now();
        let tick = super::state::monotonic_ns();
        let smi = d.config.nvidia_smi.clone();
        let facts = tokio::task::spawn_blocking(move || {
            (smi.as_deref().map(Sample::take), procs::parents())
        })
        .await;
        if let Ok((gpu, parents)) = facts {
            d.update(|s| apply(&d, s, gpu, parents, started, tick));
        }
        tokio::time::sleep(wanted).await;
        let _ = tokio::time::timeout(interval.saturating_sub(wanted), d.sample_wanted()).await;
    }
}

fn apply(
    d: &Daemon,
    s: &mut State,
    gpu: Option<Result<Sample, String>>,
    parents: HashMap<u32, u32>,
    now: Instant,
    tick: u64,
) {
    s.view.parents = parents;
    match gpu {
        Some(Ok(sample)) => {
            s.view.gpu = Some(sample);
            s.view.gpu_at = Some(now);
            s.view.gpu_error = None;
        }
        Some(Err(e)) => s.view.gpu_error = Some(e),
        None => {}
    }
    if let Some(sample) = s.view.gpu.clone().filter(|_| s.view.gpu_at == Some(now)) {
        let parents = std::mem::take(&mut s.view.parents);
        police(d, s, &sample, &parents, now, tick);
        s.view.parents = parents;
    }
}

/// Every running job, including lease clients, is subject to idle enforcement
/// after allocating VRAM or computing. Compute cancels an ongoing escalation.
fn police(
    d: &Daemon,
    s: &mut State,
    sample: &Sample,
    parents: &HashMap<u32, u32>,
    now: Instant,
    tick: u64,
) {
    let cfg = &d.config;
    let computing = sample.computing();
    let idle = secs(cfg.idle_secs);
    let int_term = secs(cfg.sigint_to_sigterm_secs);
    let term_kill = secs(cfg.sigterm_to_sigkill_secs);
    for side in s.sides.values_mut() {
        for job in side.jobs.values_mut() {
            if job.state != JobState::Running {
                continue;
            }
            let Some(root) = job.pid_host else { continue };
            let owned = procs::job_tree(root, parents);
            let rows: Vec<_> = sample
                .pmon
                .iter()
                .filter(|p| owned.contains(&p.pid))
                .collect();
            let use_compute = if rows.iter().any(|p| p.computing()) {
                Some(true)
            } else if rows.iter().any(|p| p.sm.is_none()) {
                None
            } else if !rows.is_empty() || !sample.compute.iter().any(|(pid, _)| owned.contains(pid))
            {
                Some(false)
            } else {
                None
            };
            if let Some(timing) = &mut job.timing {
                timing.compute(
                    tick,
                    use_compute,
                    (cfg.gpu_poll_interval_secs.max(1.0) * 3e9) as u64,
                );
            }
            if job.kill.is_some() {
                continue;
            }
            let busy = procs::job_tree(root, parents)
                .iter()
                .any(|p| computing.get(p).copied().unwrap_or(false));
            police_job(d, job, busy, now, idle, int_term, term_kill);
        }
    }
}

fn police_job(
    d: &Daemon,
    job: &mut Job,
    busy: bool,
    now: Instant,
    idle: Duration,
    int_term: Duration,
    term_kill: Duration,
) {
    let p = &mut job.police;
    if busy {
        p.used_compute = true;
        p.startup_since = None;
        p.idle_since = None;
        if p.idle_stage > 0 && p.idle_stage < 3 {
            p.idle_stage = 0;
            p.idle_stage_at = None;
            if job
                .end_reason
                .as_ref()
                .is_some_and(|(_, r)| r.starts_with("killed: idle"))
            {
                job.end_reason = None;
            }
        }
        return;
    }
    if !p.used_compute && !p.allocated {
        return;
    }
    let since = *p.idle_since.get_or_insert(now);
    let (next, wait) = match p.idle_stage {
        0 => (Sig::Int, idle),
        1 => (Sig::Term, int_term),
        2 => (Sig::Kill, term_kill),
        _ => return,
    };
    let from = p.idle_stage_at.unwrap_or(since);
    if now - from >= wait {
        p.idle_stage += 1;
        p.idle_stage_at = Some(now);
        if next == Sig::Int {
            job.end_reason = Some((
                JobState::Killed,
                format!(
                    "killed: idle {} s (SIGINT, then SIGTERM/SIGKILL)",
                    idle.as_secs_f64()
                ),
            ));
        }
        super::enforcement::signal(d, job, next);
    }
}
