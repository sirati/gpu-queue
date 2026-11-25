//! Periodic facts: the card (NVML), the host process tree, each
//! job's processes on the card and its grants' telemetry; and idle policing of our jobs.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use crate::gpu::Sample;
use crate::procs;
use crate::switch::card::{self, Heard};
use crate::switch::card_reader::{Answer, Observation};
use crate::protocol::JobState;
use crate::switch::compute_watchdog::Thresholds;
use crate::switch::config::{secs, SwitchConfig};
use crate::switch::daemon::Daemon;
use crate::switch::state::{JobGpuProcess, State};
use crate::switch::telemetry::Reading;

/// Samples every `gpu_poll_interval_secs`, and at once whenever a decision
/// waits on the card (at most every `gpu_wanted_poll_interval_secs`). A
/// sample is dated when it starts: it may have missed a later departure. A failed read is published at once,
/// before the process walk; a sample is applied with the walk's tree.
pub async fn run(d: Arc<Daemon>) {
    let interval = secs(d.config.gpu_poll_interval_secs.min(1.0));
    let wanted = secs(d.config.gpu_wanted_poll_interval_secs);
    loop {
        let started = Instant::now();
        let tick = super::state::monotonic_ns();
        let sampled = d.card.sample().await;
        let sample = match sampled.answer {
            Answer::Read(_) | Answer::Unconfigured => Some(sampled),
            Answer::Failed(_) | Answer::Panicked(_) => {
                d.observe(|s| card::observe(&d, s, sampled));
                None
            }
        };
        #[cfg(test)]
        d.hooks.before_walk.pass().await;
        if let Ok(processes) = tokio::task::spawn_blocking(procs::processes).await {
            d.observe(|s| apply(&d, s, sample, processes, started, tick));
        }
        tokio::time::sleep(wanted).await;
        let _ = tokio::time::timeout(
            interval.saturating_sub(started.elapsed()),
            d.sample_wanted(),
        )
        .await;
    }
}

/// The walk's tree, then the sample: policing and attribution see a sample only when the card's owner applied
/// it.
fn apply(
    d: &Daemon,
    s: &mut State,
    sample: Option<Observation<Sample>>,
    processes: procs::Processes,
    now: Instant,
    tick: u64,
) {
    s.view.parents = processes.parents;
    let Some(Heard::Read(sample)) = sample.map(|sample| card::observe(d, s, sample)) else {
        return;
    };
    let parents = std::mem::take(&mut s.view.parents);
    police(d, s, &sample, &parents, now, tick);
    attribute(s, &sample, &parents, &processes.births);
    record(s, &sample, tick, max_gap(&d.config));
    s.view.parents = parents;
}

/// The longest interval between two samples that still counts as observed: three polls.
fn max_gap(cfg: &SwitchConfig) -> u64 {
    (cfg.gpu_poll_interval_secs.max(1.0) * 3e9) as u64
}

/// Each job holding an open grant adds the sample to its telemetry, as its processes on the card in this sample see
/// it.
fn record(s: &mut State, sample: &Sample, tick: u64, max_gap: u64) {
    for job in s.sides.values_mut().flat_map(|side| side.jobs.values_mut()) {
        let Some(window) = job.open_grant() else {
            continue;
        };
        let own: HashSet<u32> = job.gpu_processes.iter().map(|process| process.pid).collect();
        let uses = || own.iter().filter_map(|pid| sample.utilisation.get(pid));
        let reading = Reading {
            card: &sample.figures,
            vram: job.gpu_processes.iter().map(|process| process.bytes).sum(),
            sm: uses().map(|used| used.sm).sum(),
            memory: uses().map(|used| used.memory).sum(),
            shared: sample.computing.iter().any(|pid| !own.contains(pid)),
        };
        job.telemetry.observe(window, tick, &reading, max_gap);
    }
}

/// Each running or stopping job's processes on the card in this sample, compute and graphics alike, each
/// with its start time from the same read as its place in the tree, so a status view can tell it from a later
/// process given the same pid. A job's tree counts only while its root is the process the job started with:
/// a root's reused pid heads another's tree. Every other job holds none.
fn attribute(
    s: &mut State,
    sample: &Sample,
    parents: &HashMap<u32, u32>,
    births: &HashMap<u32, u64>,
) {
    let processes = sample.processes();
    for job in s.sides.values_mut().flat_map(|side| side.jobs.values_mut()) {
        let owned = job
            .pid_host
            .zip(job.process_started)
            .filter(|_| matches!(job.state, JobState::Running | JobState::Stopping))
            .map(|(root, birth)| tree_while_root_lives(root, birth, parents, births))
            .unwrap_or_default();
        job.gpu_processes = processes
            .iter()
            .filter(|(pid, _)| owned.contains(pid))
            .filter_map(|(pid, bytes)| {
                Some(JobGpuProcess {
                    pid: *pid,
                    birth: *births.get(pid)?,
                    bytes: *bytes,
                })
            })
            .collect();
    }
}

/// The job tree of `root` while `root` is still the process born at `birth`; nothing once its pid is another
/// process's.
fn tree_while_root_lives(
    root: u32,
    birth: u64,
    parents: &HashMap<u32, u32>,
    births: &HashMap<u32, u64>,
) -> HashSet<u32> {
    match births.get(&root) == Some(&birth) {
        true => procs::job_tree(root, parents),
        false => HashSet::new(),
    }
}

/// Every running job, including lease clients, keeps the GPU usage contract,
/// judged on per-grant sampled evidence. A broken rule stays broken for the grant.
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
            let use_compute = Some(sample.computes(&owned));
            if let Some(timing) = &mut job.timing {
                timing.compute(tick, use_compute, max_gap(cfg));
            }
            if job.kill.is_some() {
                continue;
            }
            let granted = tick.saturating_sub(
                job.admitted_at
                    .map(|at| now.saturating_duration_since(at).as_nanos() as u64)
                    .unwrap_or(0),
            );
            let limits = Thresholds::of(cfg);
            // An end the queue ordered, or the holder's own failure report, ends the grant's work.
            let working = job.end_reason.is_none();
            for rule in job.police.compute_watchdog.observe(
                tick,
                granted,
                use_compute,
                working,
                max_gap(cfg),
                &limits,
            ) {
                super::contract::violated(d, job, rule);
            }
            // Fairness yields judge only grants still running their work.
            if job.end_reason.is_some() || job.retention.is_some() {
                continue;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_root_whose_pid_is_another_processs_heads_no_tree_of_the_job() {
        let parents = HashMap::from([(101, 100), (102, 101), (201, 200)]);
        let births = HashMap::from([(100, 5), (101, 6), (102, 7), (200, 8), (201, 9)]);
        assert_eq!(
            tree_while_root_lives(100, 5, &parents, &births),
            HashSet::from([100, 101, 102])
        );
        assert!(tree_while_root_lives(100, 4, &parents, &births).is_empty());
        assert!(tree_while_root_lives(300, 5, &parents, &births).is_empty());
    }
}
