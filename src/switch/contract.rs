//! The GPU usage contract every grant keeps: it allocates VRAM and computes as soon as it is granted, keeps the
//! GPU busy while it holds it, and frees retained VRAM by the deadline it declared. A grant that breaks a rule is
//! not stopped: the rule is recorded once per grant, journalled, shown in status and sent to the holder, whose run
//! ends as it would have and who must move its host work out of the grant.
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
    if job.violations.iter().any(|recorded| recorded.same(&violation)) {
        return;
    }
    job.violations.push(violation.clone());
    if let Err(error) = d.journal.record("contract_violation", job) {
        eprintln!("gpu-queue: journal of a contract violation failed: {error}; {}", job.handle);
    }
    job.violation_notice(violation);
}
