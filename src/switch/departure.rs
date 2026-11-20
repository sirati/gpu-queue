//! A connection closing does not prove that its GPU allocation disappeared.
//! Keep the grant until telemetry confirms teardown, terminating retained
//! process identities when a client leaves its GPU contexts behind.
//! Each pass reads through the card's owner, converts what it heard into a teardown fact ([`teardown`]) and
//! decides on that fact alone ([`verdict`]).
use super::card::{self, Heard};
use super::card_reader::Processes;
use super::daemon::Daemon;
use super::state::Side;
use crate::gpu::Failure;
use crate::protocol::JobState;
use crate::procs;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// When a teardown that is not certified clean is enforced, unless the job has a shutdown request.
const ESCALATION: Duration = Duration::from_millis(250);

/// What one pass's read says about the departed grant's allocation.
#[derive(Debug, Clone, PartialEq)]
enum Teardown {
    /// No card holds anything to verify: a host without one, or a lost card.
    NothingToVerify,
    /// No captured identity holds VRAM.
    Released,
    /// A captured identity holds VRAM, or VRAM the driver does not report.
    Retained,
    /// The read certified nothing.
    Unverified(String),
    /// The read panicked: it certified nothing.
    Panicked(String),
}

/// What decides a pass besides its fact.
struct Context {
    /// The job has a shutdown request, whose escalation its own owner fixed.
    job_shutdown: bool,
    since: Duration,
}

#[derive(Debug, Clone, PartialEq)]
enum Verdict {
    Wait,
    Conclude,
    Enforce(String),
}

/// The read's outcome as a teardown fact; it reads nothing else.
fn teardown(heard: Heard<Processes>, identities: &HashMap<u32, u64>) -> Teardown {
    match heard {
        Heard::Unconfigured | Heard::Failed(Failure::Lost(_)) => Teardown::NothingToVerify,
        Heard::Read(apps) => match apps
            .iter()
            .any(|(pid, memory)| identities.contains_key(pid) && memory.is_none_or(|bytes| bytes > 0))
        {
            true => Teardown::Retained,
            false => Teardown::Released,
        },
        Heard::Failed(Failure::Unanswered(error)) => Teardown::Unverified(error),
        Heard::Superseded => Teardown::Unverified("a GPU read failure applied before this read".into()),
        Heard::Panicked(payload) => Teardown::Panicked(payload),
    }
}

/// The pass's verdict: pure.
fn verdict(teardown: &Teardown, context: &Context) -> Verdict {
    let due = !context.job_shutdown && context.since >= ESCALATION;
    match (teardown, due) {
        (Teardown::NothingToVerify | Teardown::Released, _) => Verdict::Conclude,
        (_, false) => Verdict::Wait,
        (Teardown::Retained | Teardown::Unverified(_), true) => {
            Verdict::Enforce("lease connection closed while GPU VRAM remained allocated".into())
        }
        (Teardown::Panicked(payload), true) => Verdict::Enforce(format!(
            "gpu-switch's GPU read panicked, so the teardown could not be verified: {payload}"
        )),
    }
}

pub async fn settle(d: &Arc<Daemon>, side: Side, id: u64) {
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
        // Every observation, a loss included, is published before the verdict.
        let observation = d.card.compute().await;
        let heard = d.observe(|s| card::observe(d, s, observation));
        let teardown = teardown(heard, &identities);
        match &teardown {
            // A read that certifies nothing cannot certify a clean release.
            Teardown::Unverified(cause) | Teardown::Panicked(cause) => {
                eprintln!("gpu-queue: cannot verify GPU teardown for {id}: {cause}")
            }
            Teardown::NothingToVerify | Teardown::Released | Teardown::Retained => {}
        }
        // Cooperative shutdown already has a fixed daemon-owned escalation.
        // Keep allocated VRAM attributed during normal interpreter teardown.
        let job_shutdown = d.with(|s| s.job(side, id).is_some_and(|job| job.shutdown.is_some()));
        let context = Context {
            job_shutdown,
            since: started.elapsed(),
        };
        match verdict(&teardown, &context) {
            Verdict::Conclude => return,
            Verdict::Wait => {}
            Verdict::Enforce(reason) => {
                if !recorded {
                    d.update(|s| {
                        if let Some(job) = s.job_mut(side, id) {
                            job.end_reason.get_or_insert((JobState::Killed, reason));
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
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
