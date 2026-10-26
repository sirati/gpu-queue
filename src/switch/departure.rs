//! A connection closing does not prove that its GPU allocation disappeared.
//! Keep the grant until telemetry confirms teardown, terminating retained
//! process identities when a client leaves its GPU contexts behind.
use super::daemon::Daemon;
use super::state::Side;
use crate::protocol::JobState;
use crate::{nvsmi, procs};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub async fn settle(d: &Arc<Daemon>, side: Side, id: u64) {
    let Some(smi) = d.config.nvidia_smi.clone() else {
        return;
    };
    let Some((root, birth, last_parents)) = d.with(|s| {
        let job = s.job(side, id)?;
        if !matches!(job.state, JobState::Running | JobState::Stopping) {
            return None;
        }
        Some((job.pid_host?, job.process_started?, s.view.parents.clone()))
    }) else {
        return;
    };
    let mut identities: HashMap<u32, u64> = procs::job_tree(root, &last_parents)
        .into_iter()
        .filter_map(|pid| procs::started(pid).map(|at| (pid, at)))
        .collect();
    identities.insert(root, birth);
    let started = Instant::now();
    let mut recorded = false;
    loop {
        if procs::started(root) == Some(birth) {
            identities.extend(
                procs::job_tree(root, &procs::parents())
                    .into_iter()
                    .filter_map(|pid| procs::started(pid).map(|at| (pid, at))),
            );
        }
        identities.retain(|pid, at| procs::started(*pid) == Some(*at));
        if identities.is_empty() {
            return;
        }
        let query = smi.clone();
        let facts = tokio::task::spawn_blocking(move || nvsmi::compute_apps(&query)).await;
        match facts {
            Ok(Ok(apps)) => {
                if !apps.iter().any(|(pid, memory)| {
                    identities.contains_key(pid) && memory.is_none_or(|bytes| bytes > 0)
                }) {
                    return;
                }
            }
            _ => {
                // A driver failure cannot certify a clean release.
                eprintln!("gpu-queue: cannot verify GPU teardown for {id}");
            }
        }
        let shutting_down = d.with(|s| s.job(side, id).is_some_and(|job| job.shutdown.is_some()));
        // Cooperative shutdown already has a fixed daemon-owned escalation.
        // Keep allocated VRAM attributed during normal interpreter teardown.
        if !shutting_down && started.elapsed() >= Duration::from_millis(250) {
            if !recorded {
                d.update(|s| {
                    if let Some(job) = s.job_mut(side, id) {
                        job.end_reason.get_or_insert((
                            JobState::Killed,
                            "lease connection closed while GPU VRAM remained allocated".into(),
                        ));
                        if let Err(e) = d.journal.record("unclean_release", job) {
                            eprintln!("gpu-queue: {e}");
                        }
                    }
                });
                recorded = true;
            }
            // Capture each known birth identity; orphaned children still belong
            // to the grant even after their parent has already exited.
            for (pid, at) in &identities {
                let _ = procs::terminate_tree(*pid, *at, libc::SIGKILL);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
