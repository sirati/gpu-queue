//! A connection closing does not prove that its GPU allocation disappeared.
//! Keep the grant until telemetry confirms teardown, terminating retained
//! process identities when a client leaves its GPU contexts behind.
//! Each pass reads through the card's owner, converts what it heard into a teardown fact ([`teardown`]) and
//! decides on that fact and on the daemon's life, loaded once at the choice ([`verdict`]). While the daemon stops, a
//! fact that certifies nothing ends the settlement unverified, without a signal (PLAN-L §3.10).
use super::card::{self, Heard};
use super::card_reader::Processes;
use super::daemon::Daemon;
use super::lifecycle::Life;
use super::state::Side;
use crate::gpu::Failure;
use crate::protocol::JobState;
use crate::procs;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// When a teardown that is not certified clean is enforced, unless the job has a shutdown request.
const ESCALATION: Duration = Duration::from_millis(250);

/// What one pass's read says about the departed grant's allocation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Teardown {
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
pub(crate) struct Context {
    /// The daemon's life, loaded once at the choice.
    pub(crate) life: Life,
    /// The job has a shutdown request, whose escalation its own owner fixed.
    pub(crate) job_shutdown: bool,
    pub(crate) since: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Verdict {
    Wait,
    Conclude,
    Enforce(String),
    /// While the daemon stops, a fact that certifies nothing: settlement ends without a signal.
    EndUnverified(String),
}

/// The end reason of a settlement that ended unverified while the daemon stopped: nothing was killed.
const UNVERIFIED: &str = "teardown unverified: gpu-switch was stopping";

impl Life {
    fn word(self) -> &'static str {
        match self {
            Life::Serving => "serving",
            Life::Stopping => "stopping",
        }
    }
}

impl Teardown {
    /// The fact, as a decision line names it.
    fn fact(&self) -> String {
        match self {
            Teardown::NothingToVerify => "nothing to verify".into(),
            Teardown::Released => "released".into(),
            Teardown::Retained => "GPU VRAM remained allocated".into(),
            Teardown::Unverified(cause) => cause.clone(),
            Teardown::Panicked(payload) => format!("the GPU read panicked: {payload}"),
        }
    }
}

/// The read's outcome as a teardown fact; it reads nothing else.
pub(crate) fn teardown(heard: Heard<Processes>, identities: &HashMap<u32, u64>) -> Teardown {
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

/// The pass's verdict: pure. It consults the life in one place: where enforcement is chosen for a fact that
/// certifies nothing.
pub(crate) fn verdict(teardown: &Teardown, context: &Context) -> Verdict {
    let due = !context.job_shutdown && context.since >= ESCALATION;
    match (teardown, due, context.life) {
        (Teardown::NothingToVerify | Teardown::Released, _, _) => Verdict::Conclude,
        (_, false, _) => Verdict::Wait,
        (Teardown::Unverified(cause) | Teardown::Panicked(cause), true, Life::Stopping) => {
            Verdict::EndUnverified(cause.clone())
        }
        (Teardown::Retained | Teardown::Unverified(_), true, _) => {
            Verdict::Enforce("lease connection closed while GPU VRAM remained allocated".into())
        }
        (Teardown::Panicked(payload), true, Life::Serving) => Verdict::Enforce(format!(
            "gpu-switch's GPU read panicked, so the teardown could not be verified: {payload}"
        )),
    }
}

/// Settles a departed grant: returns once its teardown is concluded. The callers then end the job as they do after
/// any settlement.
pub async fn settle(d: &Arc<Daemon>, side: Side, id: u64) {
    #[cfg(test)]
    let ended = super::hooks::Ended::new(d, id);
    settled(d, side, id).await;
    #[cfg(test)]
    ended.returned();
}

async fn settled(d: &Arc<Daemon>, side: Side, id: u64) {
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
    let started = d.clock.monotonic();
    #[cfg(test)]
    let started_ns = d.clock.monotonic_ns();
    #[cfg(test)]
    let mut pass = 0;
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
        #[cfg(test)]
        {
            pass += 1;
            super::hooks::dispatch(d, id, started_ns, pass).await;
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
        // The one load of the daemon's life for this pass: everything the pass does follows from its verdict.
        let context = Context {
            life: d.lifecycle.life(),
            job_shutdown,
            since: d.clock.monotonic().saturating_sub(started),
        };
        let verdict = verdict(&teardown, &context);
        #[cfg(test)]
        super::hooks::choice(d, id, &teardown, &context, &verdict).await;
        let since = context.since.as_millis();
        match verdict {
            Verdict::Conclude => return,
            Verdict::Wait => {}
            Verdict::EndUnverified(cause) => {
                eprintln!(
                    "gpu-queue: settlement of {id} ends unverified after {since} ms ({cause}; gpu-switch {}): no \
                     signal",
                    context.life.word()
                );
                d.update(|s| {
                    if let Some(job) = s.job_mut(side, id) {
                        job.end_reason.get_or_insert((JobState::Failed, UNVERIFIED.into()));
                    }
                });
                return;
            }
            Verdict::Enforce(reason) => {
                let targets: Vec<String> = identities.keys().map(u32::to_string).collect();
                eprintln!(
                    "gpu-queue: settlement of {id} enforces after {since} ms ({}; gpu-switch {}): SIGKILL to {}",
                    teardown.fact(),
                    context.life.word(),
                    targets.join(", ")
                );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::switch::card_reader::tests::{lost, timeout};

    const CANCELLED: &str = "gpu-switch's runtime cancelled the read before it ran";

    #[test]
    fn every_heard_is_its_teardown_fact() {
        let identities = HashMap::from([(10, 1), (11, 2)]);
        let cases = [
            (Heard::Unconfigured, Teardown::NothingToVerify),
            (Heard::Read(vec![]), Teardown::Released),
            (Heard::Read(vec![(99, Some(5 << 20)), (98, None)]), Teardown::Released),
            (Heard::Read(vec![(10, Some(0)), (11, Some(0))]), Teardown::Released),
            (Heard::Read(vec![(10, Some(1))]), Teardown::Retained),
            (Heard::Read(vec![(11, None)]), Teardown::Retained),
            (Heard::Failed(lost()), Teardown::NothingToVerify),
            (Heard::Failed(Failure::Lost("the loss's cause, by rule 4".into())), Teardown::NothingToVerify),
            (Heard::Failed(timeout()), Teardown::Unverified(timeout().text().into())),
            (Heard::Failed(Failure::Unanswered(CANCELLED.into())), Teardown::Unverified(CANCELLED.into())),
            (Heard::Superseded, Teardown::Unverified("a GPU read failure applied before this read".into())),
            (Heard::Panicked("p".into()), Teardown::Panicked("p".into())),
        ];
        for (heard, fact) in cases {
            assert_eq!(teardown(heard.clone(), &identities), fact, "{heard:?}");
        }
    }

    /// L5's accepted settlement (PLAN-L §3.4.3), the baseline: a read that certifies nothing, a panic included, is
    /// enforced at 250 ms unless the job has a shutdown request; a loss and a release conclude.
    fn l5(teardown: &Teardown, job_shutdown: bool, since: Duration) -> Verdict {
        let enforce = || match teardown {
            Teardown::Panicked(payload) => Verdict::Enforce(format!(
                "gpu-switch's GPU read panicked, so the teardown could not be verified: {payload}"
            )),
            _ => Verdict::Enforce("lease connection closed while GPU VRAM remained allocated".into()),
        };
        match teardown {
            Teardown::NothingToVerify | Teardown::Released => Verdict::Conclude,
            _ if job_shutdown || since < Duration::from_millis(250) => Verdict::Wait,
            _ => enforce(),
        }
    }

    #[test]
    fn the_verdict_table_is_l5s_but_for_exactly_two_stopping_cells() {
        let facts = [
            Teardown::NothingToVerify,
            Teardown::Released,
            Teardown::Retained,
            Teardown::Unverified(timeout().text().into()),
            Teardown::Panicked("p".into()),
        ];
        let mut changed = Vec::new();
        for fact in &facts {
            for life in [Life::Serving, Life::Stopping] {
                for job_shutdown in [false, true] {
                    for since in [Duration::from_millis(249), Duration::from_millis(250)] {
                        let context = Context { life, job_shutdown, since };
                        let verdict = verdict(fact, &context);
                        let baseline = l5(fact, job_shutdown, since);
                        if verdict != baseline {
                            changed.push((fact.clone(), life, job_shutdown, since, verdict, baseline));
                        }
                    }
                }
            }
        }
        let cause = |fact: &Teardown| match fact {
            Teardown::Unverified(cause) | Teardown::Panicked(cause) => cause.clone(),
            _ => unreachable!(),
        };
        assert_eq!(changed.len(), 2, "{changed:?}");
        for (fact, life, job_shutdown, since, verdict, baseline) in changed {
            assert_eq!((life, job_shutdown, since), (Life::Stopping, false, Duration::from_millis(250)));
            assert_eq!(verdict, Verdict::EndUnverified(cause(&fact)));
            assert!(matches!(baseline, Verdict::Enforce(_)), "L5 enforces there");
        }
        let at = |life| Context { life, job_shutdown: false, since: ESCALATION };
        assert_eq!(
            verdict(&Teardown::Panicked("p".into()), &at(Life::Serving)),
            Verdict::Enforce("gpu-switch's GPU read panicked, so the teardown could not be verified: p".into()),
            "Q11's text: L5's action with the panic's own reason"
        );
        assert!(matches!(verdict(&Teardown::Retained, &at(Life::Stopping)), Verdict::Enforce(_)), "Q12");
        assert_eq!(verdict(&Teardown::NothingToVerify, &at(Life::Stopping)), Verdict::Conclude, "a loss concludes");
    }
}
