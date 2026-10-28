//! Lease runtime limits use the same grant clock and shutdown path as policing.
use super::daemon::Daemon;
use super::state::State;
use crate::protocol::{JobState, Sig};

fn expired(limit: Option<u64>, held: f64) -> bool {
    limit.is_some_and(|seconds| held >= seconds as f64)
}

pub fn reconcile(d: &Daemon, s: &mut State) {
    let at = super::state::monotonic_ns();
    for side in s.sides.values_mut() {
        for job in side.jobs.values_mut() {
            if job.lease
                && job.state == JobState::Running
                && job.end_reason.is_none()
                && expired(
                    job.spec.max_runtime_secs,
                    job.timing
                        .as_ref()
                        .map(|timing| timing.snapshot(at).gpu_seconds)
                        .unwrap_or(0.0),
                )
            {
                job.end_reason = Some((
                    JobState::Failed,
                    format!(
                        "GPU runtime limit exceeded: {} seconds",
                        job.spec.max_runtime_secs.unwrap_or(0)
                    ),
                ));
                super::enforcement::signal(d, job, Sig::Int);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_admitted_time_exhausts_an_explicit_limit() {
        assert!(!expired(None, 1000.0));
        assert!(!expired(Some(10), 0.0));
        assert!(!expired(Some(10), 9.99));
        assert!(expired(Some(10), 10.0));
    }
}
