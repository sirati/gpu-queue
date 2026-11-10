//! The grant-to-allocation deadline belongs to host enforcement. The daemon
//! supplies admitted jobs; this observer supplies process-tree VRAM evidence.
//! A grant with no allocation by its deadline breaks the GPU usage contract
//! (`super::contract`); one shutting down without an allocation gives up its
//! grant. This fast observer does not wait for the slower SM-activity sample.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::nvsmi;
use crate::procs;
use crate::protocol::{JobState, Sig};
use crate::switch::compute_watchdog::Thresholds;
use crate::switch::config::secs;
use crate::switch::daemon::Daemon;
use crate::switch::state::Job;

fn awaiting_allocation(job: &Job) -> bool {
    job.state == JobState::Stopping
        || (job.state == JobState::Running
            && job.retention.is_none()
            && (job.kill.is_none() || job.shutdown.is_some()))
}

fn observed(job: &Job) -> bool {
    matches!(job.state, JobState::Running | JobState::Stopping)
}

pub async fn run(d: Arc<Daemon>) {
    let Some(smi) = d.config.nvidia_smi.clone() else {
        return;
    };
    let interval = Duration::from_millis(50);
    let mut changed = d.subscribe();
    loop {
        changed.borrow_and_update();
        if !d.with(|s| s.jobs().any(observed)) {
            if changed.changed().await.is_err() {
                return;
            }
            continue;
        }
        let observed_at = Instant::now();
        let tick = super::state::monotonic_ns();
        let smi = smi.clone();
        let facts =
            tokio::task::spawn_blocking(move || (nvsmi::compute_apps(&smi), procs::parents()))
                .await;
        if let Ok((Ok(compute), parents)) = facts {
            d.update(|s| {
                for side in s.sides.values_mut() {
                    for job in side.jobs.values_mut().filter(|job| observed(job)) {
                        // A sample begun before admission cannot prove anything about this grant.
                        let Some(admitted) = job.admitted_at.filter(|at| *at <= observed_at) else {
                            continue;
                        };
                        let allocated = job.pid_host.is_some_and(|root| {
                            let owned = procs::job_tree(root, &parents);
                            compute.iter().any(|(pid, memory)| {
                                owned.contains(pid) && memory.is_none_or(|bytes| bytes > 0)
                            })
                        });
                        let memory_use = job.pid_host.and_then(|root| {
                            let owned = procs::job_tree(root, &parents);
                            let apps: Vec<_> = compute
                                .iter()
                                .filter(|(pid, _)| owned.contains(pid))
                                .collect();
                            if apps.iter().any(|(_, bytes)| bytes.is_some_and(|n| n > 0)) {
                                Some(true)
                            } else if apps.iter().any(|(_, bytes)| bytes.is_none()) {
                                None
                            } else {
                                Some(false)
                            }
                        });
                        if let Some(timing) = &mut job.timing {
                            timing.memory(tick, memory_use);
                        }
                        if job.state == JobState::Stopping {
                            let unauthorized = job.pid_host.is_some_and(|root| {
                                let owned = procs::job_tree(root, &parents);
                                compute.iter().any(|(pid, memory)| {
                                    owned.contains(pid) && memory.is_some_and(|bytes| bytes > 0)
                                })
                            });
                            if unauthorized {
                                job.end_reason = Some((
                                    JobState::Killed,
                                    "GPU allocation after the grant was revoked for shutdown"
                                        .into(),
                                ));
                                super::enforcement::signal(&d, job, Sig::Kill);
                            }
                            continue;
                        }
                        if awaiting_allocation(job) {
                            let limit = secs(Thresholds::of(&d.config).vram);
                            observe(&d, job, allocated, observed_at, admitted, limit);
                        }
                    }
                }
            });
        }
        tokio::time::sleep(interval).await;
    }
}

fn observe(
    d: &Daemon,
    job: &mut Job,
    allocated: bool,
    now: Instant,
    admitted: Instant,
    limit: Duration,
) {
    if allocated {
        job.police.allocated = true;
        job.police.startup_since = None;
        job.police.startup_fired = false;
        return;
    }
    let since = *job.police.startup_since.get_or_insert_with(|| {
        if job.police.allocated || job.police.used_compute {
            now
        } else {
            admitted
        }
    });
    if now.duration_since(since) >= limit {
        job.police.startup_fired = true;
        if job.shutdown.is_some() {
            job.state = JobState::Stopping;
            if let Some(timing) = &mut job.timing {
                timing.stop(super::state::monotonic_ns());
            }
            return;
        }
        super::contract::violated(d, job, crate::protocol::Rule::NoVram);
    }
}
