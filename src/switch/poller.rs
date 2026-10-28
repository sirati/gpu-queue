//! Periodic facts: the card (nvidia-smi) and the host process tree; and
//! idle policing of our jobs.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::nvsmi::Sample;
use crate::procs;
use crate::protocol::JobState;
use crate::switch::config::secs;
use crate::switch::daemon::Daemon;
use crate::switch::state::State;

/// Samples every `gpu_poll_interval_secs`, and at once whenever a decision
/// waits on the card (at most every `gpu_wanted_poll_interval_secs`). A
/// sample is dated when it starts: it may have missed a later departure.
pub async fn run(d: Arc<Daemon>) {
    let interval = secs(d.config.gpu_poll_interval_secs.min(1.0));
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
        let _ = tokio::time::timeout(
            interval.saturating_sub(started.elapsed()),
            d.sample_wanted(),
        )
        .await;
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
/// using per-grant sampled evidence. Watchdog failures are permanent.
fn police(
    d: &Daemon,
    s: &mut State,
    sample: &Sample,
    parents: &HashMap<u32, u32>,
    now: Instant,
    tick: u64,
) {
    let cfg = &d.config;
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
            } else if rows.iter().any(|p| p.sm.is_none())
                || sample.compute.iter().any(|(pid, _)| {
                    owned.contains(pid) && !rows.iter().any(|p| p.pid == *pid && p.sm.is_some())
                })
            {
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
            if job.end_reason.is_some() {
                continue;
            }
            let granted = tick.saturating_sub(
                job.admitted_at
                    .map(|at| now.saturating_duration_since(at).as_nanos() as u64)
                    .unwrap_or(0),
            );
            if let Some(reason) = job.police.compute_watchdog.observe(
                tick,
                granted,
                use_compute,
                (cfg.gpu_poll_interval_secs.max(1.0) * 3e9) as u64,
                cfg.idle_secs,
            ) {
                job.terminal_status = Some(reason);
                job.end_reason = Some((JobState::Killed, reason.as_str().into()));
                super::enforcement::watchdog(
                    job,
                    reason,
                    secs(cfg.sigint_to_sigterm_secs),
                    secs(cfg.sigterm_to_sigkill_secs),
                );
            }
            if use_compute == Some(true) {
                job.police.used_compute = true;
                job.police.idle_since = None;
            } else if use_compute == Some(false) {
                job.police.idle_since.get_or_insert(now);
            }
        }
    }
}
