//! The GPU usage contract every grant keeps: it allocates VRAM and computes as soon as it is granted, keeps the
//! GPU busy while it holds it, and frees retained VRAM by the deadline it declared. A grant that breaks a rule is
//! not stopped: the rule is recorded once per grant, journalled, shown in status and sent to the holder, whose run
//! ends as it would have and who must move its host work out of the grant.
use super::compute_watchdog::Thresholds;
use super::daemon::Daemon;
use super::state::Job;
use crate::protocol::{now_unix, JobState, Rule, TerminalStatus, Violation};

pub fn violated(d: &Daemon, job: &mut Job, rule: Rule) {
    let violation = Violation {
        rule,
        run: job.runs,
        text: Thresholds::of(&d.config).describe(rule),
        at: now_unix(),
    };
    let relieved = job.retention.is_some() && !rule.binds_retention();
    if relieved || job.police.concluded || job.violations.iter().any(|recorded| recorded.same(&violation)) {
        return;
    }
    job.violations.push(violation.clone());
    if let Err(error) = d.journal.record("contract_violation", job) {
        eprintln!("gpu-queue: journal of a contract violation failed: {error}; {}", job.handle);
    }
    job.violation_notice(violation);
}

/// The rule an older daemon was ending a still-running grant for, before any shutdown notice reached its holder.
fn interrupted_by_contract(state: JobState, status: Option<TerminalStatus>, notified: bool) -> Option<Rule> {
    status.and_then(TerminalStatus::rule).filter(|_| state == JobState::Running && !notified)
}

/// A grant restored from an older daemon that was ending it for a contract rule keeps running: the escalation
/// stops and the rule is recorded broken, told to the holder when it reattaches.
pub fn restored(job: &mut Job, limits: &Thresholds) {
    let Some(rule) = interrupted_by_contract(job.state, job.terminal_status, job.shutdown.is_some()) else {
        return;
    };
    job.terminal_status = None;
    job.end_reason = None;
    job.kill = None;
    job.violations.push(Violation {
        rule,
        run: job.runs,
        text: limits.describe(rule),
        at: now_unix(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_running_contract_kill_without_a_delivered_notice_is_taken_back() {
        let idle = Some(TerminalStatus::LowComputeDuty);
        assert_eq!(interrupted_by_contract(JobState::Running, idle, false), Some(Rule::LowComputeDuty));
        assert_eq!(interrupted_by_contract(JobState::Running, idle, true), None);
        assert_eq!(interrupted_by_contract(JobState::Killed, idle, false), None);
        assert_eq!(interrupted_by_contract(JobState::Running, Some(TerminalStatus::NonZeroExit), false), None);
        assert_eq!(interrupted_by_contract(JobState::Running, None, false), None);
    }
}
