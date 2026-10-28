//! Per-grant sampled compute policy. Unknown samples never prove idleness.
use crate::protocol::TerminalStatus;
use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct ComputeWatchdog {
    previous: Option<(u64, bool)>,
    busy_since: Option<u64>,
    idle_since: Option<u64>,
    ever_busy: bool,
    sustained: bool,
    measured: u64,
    idle: u64,
}

const SECOND: u64 = 1_000_000_000;
impl ComputeWatchdog {
    pub fn observe(
        &mut self,
        at: u64,
        granted: u64,
        activity: Option<bool>,
        max_gap: u64,
        sustained_idle_secs: f64,
    ) -> Option<TerminalStatus> {
        let Some(busy) = activity else {
            self.previous = None;
            self.busy_since = None;
            self.idle_since = None;
            return None;
        };
        if !self.ever_busy && at.saturating_sub(granted) > 3 * SECOND {
            return Some(TerminalStatus::NoInitialCompute);
        }
        let previous = self.previous.replace((at, busy));
        let connected = previous.is_some_and(|(prev, _)| at >= prev && at - prev <= max_gap);
        if !connected {
            self.busy_since = None;
            self.idle_since = None;
        }
        if let Some((prev, was_busy)) = previous.filter(|_| connected) {
            let duration = at.saturating_sub(prev.max(granted.saturating_add(6 * SECOND)));
            // Only intervals with matching endpoints are evidence of sustained activity.
            if was_busy == busy {
                self.measured += duration;
                if !busy {
                    self.idle += duration;
                }
            }
        }
        if busy {
            self.ever_busy = true;
            self.idle_since = None;
            let since = *self.busy_since.get_or_insert(at);
            self.sustained |= at.saturating_sub(since) > 90 * SECOND;
        } else {
            self.busy_since = None;
            let since = *self.idle_since.get_or_insert(at);
            if !self.ever_busy && at.saturating_sub(granted) >= 3 * SECOND {
                return Some(TerminalStatus::NoInitialCompute);
            }
            let limit = if self.sustained {
                sustained_idle_secs
            } else {
                30.0
            };
            if at.saturating_sub(since) as f64 > limit * SECOND as f64 {
                return Some(if self.sustained {
                    TerminalStatus::IdleCompute90
                } else {
                    TerminalStatus::IdleCompute30
                });
            }
        }
        if !self.sustained && self.measured > 0 && self.idle as u128 * 2 >= self.measured as u128 {
            return Some(TerminalStatus::LowComputeDuty);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample(w: &mut ComputeWatchdog, second: u64, busy: Option<bool>) -> Option<TerminalStatus> {
        w.observe(second * SECOND, 0, busy, 2 * SECOND, 90.)
    }
    #[test]
    fn initial_deadline_is_three_seconds() {
        let mut w = ComputeWatchdog::default();
        for t in 0..3 {
            assert_eq!(sample(&mut w, t, Some(false)), None);
        }
        assert_eq!(
            sample(&mut w, 3, Some(false)),
            Some(TerminalStatus::NoInitialCompute)
        );
    }
    #[test]
    fn late_first_compute_cannot_erase_the_startup_violation() {
        let mut w = ComputeWatchdog::default();
        assert_eq!(sample(&mut w, 0, Some(false)), None);
        assert_eq!(
            sample(&mut w, 4, Some(true)),
            Some(TerminalStatus::NoInitialCompute)
        );
    }

    #[test]
    fn ratio_excludes_first_six_seconds_and_triggers_at_half() {
        let mut w = ComputeWatchdog::default();
        for t in 0..=8 {
            assert_eq!(sample(&mut w, t, Some(true)), None);
        }
        assert_eq!(sample(&mut w, 9, Some(false)), None);
        assert_eq!(sample(&mut w, 10, Some(false)), None);
        assert_eq!(
            sample(&mut w, 11, Some(false)),
            Some(TerminalStatus::LowComputeDuty)
        );
    }
    #[test]
    fn short_compute_gets_thirty_second_idle_limit() {
        let mut w = ComputeWatchdog::default();
        // Build a busy history large enough to avoid the separate ratio rule.
        for t in 0..=80 {
            assert_eq!(sample(&mut w, t, Some(true)), None);
        }
        for t in 81..=111 {
            assert_eq!(sample(&mut w, t, Some(false)), None);
        }
        assert_eq!(
            sample(&mut w, 112, Some(false)),
            Some(TerminalStatus::IdleCompute30)
        );
    }
    #[test]
    fn sustained_compute_earns_only_ninety_second_idle_rule() {
        let mut w = ComputeWatchdog::default();
        for t in 0..=91 {
            assert_eq!(sample(&mut w, t, Some(true)), None);
        }
        // Restart retains the allowance and accumulated evidence.
        let mut w: ComputeWatchdog =
            serde_json::from_str(&serde_json::to_string(&w).unwrap()).unwrap();
        for t in 92..=182 {
            assert_eq!(sample(&mut w, t, Some(false)), None);
        }
        assert_eq!(
            sample(&mut w, 183, Some(false)),
            Some(TerminalStatus::IdleCompute90)
        );
    }
    #[test]
    fn unknown_and_missing_samples_neither_prove_idle_nor_continuity() {
        let mut w = ComputeWatchdog::default();
        assert_eq!(sample(&mut w, 0, Some(true)), None);
        assert_eq!(sample(&mut w, 90, Some(true)), None);
        assert!(!w.sustained);
        assert_eq!(sample(&mut w, 91, None), None);
        assert_eq!(sample(&mut w, 200, Some(false)), None);
    }
}
