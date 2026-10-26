//! Shared fairness is accounted by admitted party, not by process count.
//! Only cooperative shared work participates in the turn/budget policy.
use super::{daemon::Daemon, state::State};
use crate::protocol::{JobState, Mode, RunnerEvent, Sig};
use std::collections::HashSet;
use std::time::Instant;

fn reason(
    spent: f64,
    held: f64,
    idle: f64,
    others: usize,
    waiting: bool,
    budget: f64,
    turn: f64,
    idle_limit: f64,
    grace: f64,
) -> Option<&'static str> {
    if others == 0 && waiting && held >= turn {
        Some("shared GPU turn ended with an exclusive job waiting")
    } else if others == 0 && waiting && idle >= idle_limit {
        Some("shared GPU idle turn ended with an exclusive job waiting")
    } else if spent + grace >= budget {
        Some("weighted shared GPU budget exhausted")
    } else {
        None
    }
}

pub fn reconcile(d: &Daemon, s: &mut State, now: Instant) {
    let parties: HashSet<_> = s.running().map(|j| j.party()).collect();
    let waiting = s
        .line()
        .iter()
        .any(|(_, e)| s.entry_mode(*e) == Mode::Exclusive);
    for side in s.sides.values_mut() {
        for job in side.jobs.values_mut() {
            if job.state != JobState::Running
                || job.spec.mode != Mode::Shared
                || !job.spec.cooperative
                || job.yield_sent
                || job.kill.is_some()
            {
                continue;
            }
            let Some(admitted) = job.admitted_at else {
                continue;
            };
            let others = parties.len().saturating_sub(1);
            let previous = job.police.shared_sample_at.replace(now).unwrap_or(admitted);
            job.police.shared_spent += now.saturating_duration_since(previous).as_secs_f64()
                / 1.25_f64.powf(others as f64);
            let idle = job
                .police
                .idle_since
                .map(|at| now.saturating_duration_since(at).as_secs_f64())
                .unwrap_or(0.0);
            let grace = job
                .shutdown_grace_secs
                .unwrap_or(d.config.job_kill_grace_secs);
            if let Some(reason) = reason(
                job.police.shared_spent,
                now.saturating_duration_since(admitted).as_secs_f64(),
                idle,
                others,
                waiting,
                d.config.shared_budget_secs,
                d.config.shared_turn_secs,
                d.config.shared_idle_yield_secs,
                grace,
            ) {
                job.yield_sent = true;
                if job.shutdown_grace_secs.is_some() {
                    job.end_reason = Some((JobState::Cancelled, reason.into()));
                    super::enforcement::signal(d, job, Sig::Int);
                } else {
                    job.runner(RunnerEvent::Yield);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::reason;

    #[test]
    fn sole_holder_yields_for_turn_or_idle_only_with_waiter() {
        assert!(reason(0., 301., 0., 0, true, 600., 300., 60., 30.).is_some());
        assert!(reason(0., 70., 61., 0, true, 600., 300., 60., 30.).is_some());
        assert!(reason(0., 301., 61., 0, false, 600., 300., 60., 30.).is_none());
        assert!(reason(0., 301., 61., 1, true, 600., 300., 60., 30.).is_none());
    }

    #[test]
    fn budget_reserves_cleanup_time_without_a_waiter() {
        assert!(reason(569., 0., 0., 2, false, 600., 300., 60., 30.).is_none());
        assert!(reason(570., 0., 0., 2, false, 600., 300., 60., 30.).is_some());
    }
}
