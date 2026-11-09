//! Irreversible, bounded VRAM-only retention. Admission uses effective shared
//! mode with zero reservation; measured card usage remains accounted normally.
use super::daemon::Daemon;
use super::state::{deadline_after, monotonic_ns, Side, State};
use crate::protocol::{now_unix, JobState, Reply, Resume, Rule};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Retention {
    pub seconds: f64,
    pub deadline: f64,
    pub deadline_ns: u64,
}

/// Long enough for the teardown of GPU objects after the last GPU operation.
pub const MAX_SECONDS: f64 = 300.0;

impl Retention {
    pub fn new(seconds: f64, now_ns: u64, now: f64) -> Result<Self, String> {
        if !(seconds <= MAX_SECONDS) {
            return Err(format!("VRAM retention lasts at most {MAX_SECONDS} s"));
        }
        let deadline_ns = deadline_after(now_ns, seconds, "VRAM retention")?;
        Ok(Self {
            seconds,
            deadline: now + seconds,
            deadline_ns,
        })
    }

    /// A phase an older daemon acknowledged for longer ends where the bound ends it.
    pub fn bounded(self) -> Self {
        let excess = (self.seconds - MAX_SECONDS).max(0.0);
        Self {
            seconds: self.seconds - excess,
            deadline: self.deadline - excess,
            deadline_ns: self.deadline_ns.saturating_sub((excess * 1e9) as u64),
        }
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
            && job.kill.is_none()
            && job.retention.as_ref().is_some_and(|r| now >= r.deadline_ns)
        {
            super::contract::violated(d, job, Rule::VramRetentionExpired);
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
        for value in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e300, 1e-20, MAX_SECONDS + 1.0] {
            assert!(Retention::new(value, 100, 200.0).is_err());
        }
        assert!(Retention::new(MAX_SECONDS, 100, 200.0).is_ok());
    }

    #[test]
    fn a_longer_phase_from_an_older_daemon_ends_at_the_bound() {
        let older = Retention { seconds: 3600.0, deadline: 3800.0, deadline_ns: 3_600_000_000_100 };
        let bounded = older.bounded();
        assert_eq!((bounded.seconds, bounded.deadline, bounded.deadline_ns), (MAX_SECONDS, 500.0, 300_000_000_100));
        let within = Retention::new(30.0, 100, 200.0).unwrap();
        assert_eq!(within.clone().bounded().deadline_ns, within.deadline_ns);
        assert!(Retention::new(1.0, u64::MAX, 200.0).is_err());
    }
}
