//! Per-grant sampled compute policy. Unknown samples never prove idleness, and
//! neither does time the owner declared idle in advance.
use super::config::SwitchConfig;
use crate::protocol::{TerminalStatus, WatchdogPolicy};
use serde::{Deserialize, Serialize};

const SECOND: u64 = 1_000_000_000;

/// What a watchdog policy tolerates; standard is the long-standing policy.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Thresholds {
    pub policy: WatchdogPolicy,
    /// Seconds a running grant may hold no VRAM allocation.
    pub vram: f64,
    first_compute: u64,
    idle: f64,
    sustained_idle: f64,
    /// The rule that at least half of the measured time is busy.
    duty: bool,
}

impl Thresholds {
    pub fn of(policy: WatchdogPolicy, config: &SwitchConfig) -> Self {
        match policy {
            WatchdogPolicy::Standard => Self {
                policy,
                vram: config.startup_idle_secs,
                first_compute: 3 * SECOND,
                idle: 30.0,
                sustained_idle: config.idle_secs,
                duty: true,
            },
            WatchdogPolicy::Lenient => Self {
                policy,
                vram: config.startup_idle_secs.max(30.0),
                first_compute: 120 * SECOND,
                idle: 300.0,
                sustained_idle: config.idle_secs.max(300.0),
                duty: false,
            },
        }
    }
    /// A lenient kill names the thresholds it applied; standard text is unchanged.
    pub fn describe(&self, reason: TerminalStatus) -> String {
        match (self.policy, reason) {
            (WatchdogPolicy::Lenient, TerminalStatus::NoInitialCompute) => format!(
                "killed, no compute in first {}s (lenient watchdog)",
                self.first_compute / SECOND
            ),
            (WatchdogPolicy::Lenient, TerminalStatus::IdleCompute30) => {
                format!("killed, no compute >{}s (lenient watchdog)", self.idle)
            }
            (WatchdogPolicy::Lenient, TerminalStatus::IdleCompute90) => format!(
                "killed, no compute >{}s (lenient watchdog)",
                self.sustained_idle
            ),
            _ => reason.as_str().into(),
        }
    }
}

/// A declared idle window on the monotonic clock, and its wall-clock deadline.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Excuse {
    from: u64,
    until: u64,
    seconds: f64,
    deadline: f64,
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct ComputeWatchdog {
    previous: Option<(u64, bool)>,
    busy_since: Option<u64>,
    idle_since: Option<u64>,
    ever_busy: bool,
    sustained: bool,
    measured: u64,
    idle: u64,
    #[serde(default)]
    excused: Option<Excuse>,
}

impl ComputeWatchdog {
    /// Once per grant. A retransmission acknowledges the same window and can
    /// never renew it.
    pub fn excuse(&mut self, now_ns: u64, now: f64, seconds: f64) -> Result<f64, String> {
        if let Some(excuse) = self.excused {
            return match excuse.seconds == seconds {
                true => Ok(excuse.deadline),
                false => Err("an idle expectation is declared once per grant".into()),
            };
        }
        let until = super::state::deadline_after(now_ns, seconds, "an idle expectation")?;
        self.excused = Some(Excuse {
            from: now_ns,
            until,
            seconds,
            deadline: now + seconds,
        });
        Ok(now + seconds)
    }
    pub fn excused_at(&self, at: u64) -> bool {
        self.excused
            .is_some_and(|excuse| (excuse.from..excuse.until).contains(&at))
    }
    pub fn expected_idle_until(&self) -> Option<f64> {
        self.excused.map(|excuse| excuse.deadline)
    }
    fn excused_within(&self, from: u64, at: u64) -> u64 {
        self.excused.map_or(0, |excuse| {
            at.min(excuse.until).saturating_sub(from.max(excuse.from))
        })
    }
    /// Old checkpoints cannot prove historic duty or a continuous-compute streak.
    /// Preserve their old 90-second idle policy until the next grant.
    pub fn legacy() -> Self {
        Self {
            ever_busy: true,
            sustained: true,
            ..Self::default()
        }
    }

    pub fn observe(
        &mut self,
        at: u64,
        granted: u64,
        activity: Option<bool>,
        max_gap: u64,
        limits: &Thresholds,
    ) -> Option<TerminalStatus> {
        let Some(busy) = activity.filter(|_| !self.excused_at(at)) else {
            self.previous = None;
            self.busy_since = None;
            self.idle_since = None;
            return None;
        };
        // Declared idle time runs neither the first-compute clock nor the
        // ratio's warm-up.
        let excused = self.excused_within(granted, at);
        let started = at.saturating_sub(granted).saturating_sub(excused);
        if !self.ever_busy && started > limits.first_compute {
            return Some(TerminalStatus::NoInitialCompute);
        }
        let previous = self.previous.replace((at, busy));
        let connected = previous.is_some_and(|(prev, _)| at >= prev && at - prev <= max_gap);
        if !connected {
            self.busy_since = None;
            self.idle_since = None;
        }
        if let Some((prev, was_busy)) = previous.filter(|_| connected) {
            let warm = granted.saturating_add(6 * SECOND).saturating_add(excused);
            let duration = at.saturating_sub(prev.max(warm));
            // Integrate the previous valid observation across its bounded interval.
            // Dropping transitions would let alternating busy/idle evade the ratio.
            self.measured += duration;
            if !was_busy {
                self.idle += duration;
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
            if !self.ever_busy && started >= limits.first_compute {
                return Some(TerminalStatus::NoInitialCompute);
            }
            let limit = if self.sustained {
                limits.sustained_idle
            } else {
                limits.idle
            };
            if at.saturating_sub(since) as f64 > limit * SECOND as f64 {
                return Some(if self.sustained {
                    TerminalStatus::IdleCompute90
                } else {
                    TerminalStatus::IdleCompute30
                });
            }
        }
        if limits.duty
            && !self.sustained
            && self.measured > 0
            && self.idle as u128 * 2 >= self.measured as u128
        {
            return Some(TerminalStatus::LowComputeDuty);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn limits(policy: WatchdogPolicy) -> Thresholds {
        Thresholds::of(
            policy,
            &serde_json::from_str::<SwitchConfig>(r#"{"listen":[]}"#).unwrap(),
        )
    }
    fn sample(w: &mut ComputeWatchdog, second: u64, busy: Option<bool>) -> Option<TerminalStatus> {
        w.observe(
            second * SECOND,
            0,
            busy,
            2 * SECOND,
            &limits(WatchdogPolicy::Standard),
        )
    }
    #[test]
    fn legacy_grants_do_not_acquire_retroactive_startup_or_ratio_failures() {
        let mut w = ComputeWatchdog::legacy();
        for t in 500..=590 {
            assert_eq!(sample(&mut w, t, Some(false)), None);
        }
        assert_eq!(
            sample(&mut w, 591, Some(false)),
            Some(TerminalStatus::IdleCompute90)
        );
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
        assert_eq!(sample(&mut w, 11, Some(false)), None);
        assert_eq!(
            sample(&mut w, 12, Some(false)),
            Some(TerminalStatus::LowComputeDuty)
        );
    }
    #[test]
    fn alternating_activity_cannot_evade_the_half_idle_rule() {
        let mut w = ComputeWatchdog::default();
        for t in 0..=6 {
            assert_eq!(sample(&mut w, t, Some(true)), None);
        }
        assert_eq!(sample(&mut w, 7, Some(false)), None);
        assert_eq!(
            sample(&mut w, 8, Some(true)),
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
    #[test]
    fn a_lenient_grant_has_raised_limits_and_no_duty_rule() {
        let lenient = limits(WatchdogPolicy::Lenient);
        let observe = |w: &mut ComputeWatchdog, second: u64, busy: bool| {
            w.observe(second * SECOND, 0, Some(busy), 2 * SECOND, &lenient)
        };
        let mut w = ComputeWatchdog::default();
        for t in 0..120 {
            assert_eq!(observe(&mut w, t, false), None);
        }
        assert_eq!(
            observe(&mut w, 120, false),
            Some(TerminalStatus::NoInitialCompute)
        );
        let mut w = ComputeWatchdog::default();
        for t in 0..=8 {
            assert_eq!(observe(&mut w, t, true), None);
        }
        for t in 9..=309 {
            assert_eq!(observe(&mut w, t, false), None, "second {t}");
        }
        assert_eq!(
            observe(&mut w, 310, false),
            Some(TerminalStatus::IdleCompute30)
        );
        assert_eq!(
            lenient.describe(TerminalStatus::IdleCompute30),
            "killed, no compute >300s (lenient watchdog)"
        );
        assert_eq!(
            limits(WatchdogPolicy::Standard).describe(TerminalStatus::NoInitialCompute),
            TerminalStatus::NoInitialCompute.as_str()
        );
        assert_eq!(lenient.vram, 30.0);
    }
    #[test]
    fn declared_idle_time_is_neither_evidence_nor_startup_time() {
        let mut w = ComputeWatchdog::default();
        assert_eq!(sample(&mut w, 0, Some(false)), None);
        assert_eq!(w.excuse(SECOND, 100.0, 10.0), Ok(110.0));
        for t in 1..=12 {
            assert_eq!(sample(&mut w, t, Some(false)), None, "second {t}");
        }
        assert_eq!(
            sample(&mut w, 13, Some(false)),
            Some(TerminalStatus::NoInitialCompute),
            "the first-compute clock resumes after the declared window"
        );
        assert_eq!(w.excuse(5 * SECOND, 104.0, 10.0), Ok(110.0));
        assert!(w.excuse(5 * SECOND, 104.0, 20.0).is_err());
        assert_eq!(w.expected_idle_until(), Some(110.0));
        let restored: ComputeWatchdog =
            serde_json::from_str(&serde_json::to_string(&w).unwrap()).unwrap();
        assert!(restored.excused_at(5 * SECOND) && !restored.excused_at(11 * SECOND));
    }
}
