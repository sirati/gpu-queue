//! Irreversible, bounded VRAM-only retention. Admission uses effective shared
//! mode with zero reservation; measured card usage remains accounted normally.
use super::config::secs;
use super::daemon::Daemon;
use super::state::{deadline_after, monotonic_ns, Side, State};
use crate::protocol::{now_unix, JobState, Reply, Resume, TerminalStatus};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Retention {
    pub seconds: f64,
    pub deadline: f64,
    pub deadline_ns: u64,
}

impl Retention {
    pub fn new(seconds: f64, now_ns: u64, now: f64) -> Result<Self, String> {
        let deadline_ns = deadline_after(now_ns, seconds, "VRAM retention")?;
        Ok(Self {
            seconds,
            deadline: now + seconds,
            deadline_ns,
        })
    }
}

pub fn request(s: &mut State, side: Side, peer_pid: u32, resume: &Resume, seconds: f64) -> Reply {
    match begin(s, side, peer_pid, resume, seconds) {
        Ok(deadline) => Reply::message(deadline.to_string()),
        Err(error) => Reply::error(error),
    }
}

fn begin(
    s: &mut State,
    side: Side,
    peer_pid: u32,
    resume: &Resume,
    seconds: f64,
) -> Result<f64, String> {
    let job = s.owned_lease(side, peer_pid, resume, "VRAM retention")?;
    // Retransmission can acknowledge the same phase, never renew its deadline.
    if let Some(retention) = &job.retention {
        return match retention.seconds == seconds {
            true => Ok(retention.deadline),
            false => Err("VRAM retention is irreversible and cannot be renewed".into()),
        };
    }
    let retention = Retention::new(seconds, monotonic_ns(), now_unix())?;
    let deadline = retention.deadline;
    job.retention = Some(retention);
    // Admission must see memory allocated before this transition.
    s.left_gpu_at = Some(std::time::Instant::now());
    Ok(deadline)
}

pub fn reconcile(d: &Daemon, s: &mut State) {
    let now = monotonic_ns();
    for job in s.sides.values_mut().flat_map(|side| side.jobs.values_mut()) {
        if job.state == JobState::Running
            && job.end_reason.is_none()
            && job.retention.as_ref().is_some_and(|r| now >= r.deadline_ns)
        {
            let reason = TerminalStatus::VramRetentionExpired;
            job.end_reason = Some((JobState::Killed, reason.as_str().into()));
            super::enforcement::watchdog(
                job,
                reason,
                secs(d.config.sigint_to_sigterm_secs),
                secs(d.config.sigterm_to_sigkill_secs),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_has_a_fixed_monotonic_deadline() {
        let phase = Retention::new(1.25, 100, 200.0).unwrap();
        assert_eq!(phase.deadline_ns, 1_250_000_100);
        assert_eq!(phase.deadline, 201.25);
        let restored: Retention =
            serde_json::from_str(&serde_json::to_string(&phase).unwrap()).unwrap();
        assert_eq!(restored.deadline_ns, phase.deadline_ns);
    }

    #[test]
    fn rejects_unbounded_nonpositive_or_overflowing_durations() {
        for value in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e300, 1e-20] {
            assert!(Retention::new(value, 100, 200.0).is_err());
        }
        assert!(Retention::new(1.0, u64::MAX, 200.0).is_err());
    }
}
