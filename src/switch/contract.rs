//! The GPU usage contract every grant keeps: it allocates VRAM and computes as soon as it is granted, keeps the
//! GPU busy while it holds it, and frees retained VRAM by the deadline it declared. A grant that breaks a rule is
//! not stopped: the rule is recorded once per grant, journalled, shown in status and sent to the holder, whose run
//! ends as it would have and who must move its host work out of the grant. What an older daemon decided before
//! an upgrade (a shutdown, its escalation, an acknowledged retention) stands, as it would have without the upgrade:
//! its holder may already act on it.
use super::compute_watchdog::Thresholds;
use super::daemon::Daemon;
use super::state::Job;
use crate::protocol::{now_unix, Rule, Violation};

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

/// The grant's working phase ends: its run ends, it releases its lease or it begins VRAM retention. The rules
/// that judge the whole phase are judged now, over everything measured.
pub fn work_ends(d: &Daemon, job: &mut Job) {
    if let Some(rule) = job.police.compute_watchdog.end_span() {
        violated(d, job, rule);
    }
}
