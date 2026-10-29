//! Queue jumps may consume spare memory only within the currently running
//! workload's declared time horizon. Unknown or expired estimates give no room.
use super::state::{Side, State};
use std::time::Instant;

pub fn fits(s: &State, side: Side, id: u64, now: Instant) -> bool {
    let Some(need) = s
        .job(side, id)
        .and_then(|j| crate::estimate::seconds(j.spec.expected_runtime_secs))
    else {
        return false;
    };
    let remaining = s
        .running()
        .filter_map(|j| {
            if let Some(retention) = &j.retention {
                return Some(
                    retention
                        .deadline_ns
                        .saturating_sub(super::state::monotonic_ns()) as f64
                        / 1e9,
                );
            }
            let expected = crate::estimate::seconds(j.spec.expected_runtime_secs)? as f64;
            let elapsed = now.saturating_duration_since(j.admitted_at?).as_secs_f64();
            Some((expected - elapsed).max(0.0))
        })
        .fold(0.0_f64, f64::max);
    need as f64 <= remaining
}
