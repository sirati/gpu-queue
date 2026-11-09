//! Per-grant sampled compute policy. Unknown samples never prove idleness, and
//! neither does time the owner declared idle in advance.
use super::config::SwitchConfig;
use crate::protocol::Rule;
use serde::{Deserialize, Serialize};

const SECOND: u64 = 1_000_000_000;

/// What the GPU usage contract tolerates: one set of limits for every grant, whatever watchdog policy its
/// client still declares.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Thresholds {
    /// Seconds a running grant may hold no VRAM allocation.
    pub vram: f64,
    first_compute: u64,
    idle: f64,
    sustained_idle: f64,
}

impl Thresholds {
    pub fn of(config: &SwitchConfig) -> Self {
        Self {
            vram: config.startup_idle_secs,
            first_compute: 3 * SECOND,
            idle: 30.0,
            sustained_idle: config.idle_secs,
        }
    }
    /// The broken rule, as the grant's holder reads it.
    pub fn describe(&self, rule: Rule) -> String {
        match rule {
            Rule::NoVram => format!("no GPU VRAM allocation within {} s of the grant", self.vram),
            Rule::NoInitialCompute => {
                format!("no GPU compute in the first {} s of the grant", self.first_compute / SECOND)
            }
            Rule::LowComputeDuty => "the GPU was idle for at least half of the grant".into(),
            Rule::IdleCompute30 => format!("no GPU compute for more than {} s", self.idle),
            Rule::IdleCompute90 => {
                format!("no GPU compute for more than {} s", self.sustained_idle)
            }
            Rule::VramRetentionExpired => "VRAM retained past its declared deadline".into(),
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
    ) -> Vec<Rule> {
        let mut broken = Vec::new();
        let Some(busy) = activity.filter(|_| !self.excused_at(at)) else {
            self.previous = None;
            self.busy_since = None;
            self.idle_since = None;
            return broken;
        };
        // Declared idle time runs neither the first-compute clock nor the
        // ratio's warm-up.
        let excused = self.excused_within(granted, at);
        let started = at.saturating_sub(granted).saturating_sub(excused);
        // Compute first seen after the deadline breaks the rule too; the grant goes on and is judged further.
        if !self.ever_busy
            && (started > limits.first_compute || !busy && started >= limits.first_compute)
        {
            broken.push(Rule::NoInitialCompute);
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
            let limit = if self.sustained {
                limits.sustained_idle
            } else {
                limits.idle
            };
            if at.saturating_sub(since) as f64 > limit * SECOND as f64 {
                broken.push(if self.sustained {
                    Rule::IdleCompute90
                } else {
                    Rule::IdleCompute30
                });
            }
        }
        if !self.sustained
            && self.measured > 0
            && self.idle as u128 * 2 >= self.measured as u128
        {
            broken.push(Rule::LowComputeDuty);
        }
        broken
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn limits() -> Thresholds {
        Thresholds::of(&serde_json::from_str::<SwitchConfig>(r#"{"listen":[]}"#).unwrap())
    }
    fn sample(w: &mut ComputeWatchdog, second: u64, busy: Option<bool>) -> Vec<Rule> {
        w.observe(
            second * SECOND,
            0,
            busy,
            2 * SECOND,
            &limits(),
        )
    }
    #[test]
    fn legacy_grants_do_not_acquire_retroactive_startup_or_ratio_failures() {
        let mut w = ComputeWatchdog::legacy();
        for t in 500..=590 {
            assert_eq!(sample(&mut w, t, Some(false)), []);
        }
        assert_eq!(
            sample(&mut w, 591, Some(false)),
            [Rule::IdleCompute90]
        );
    }

    #[test]
    fn initial_deadline_is_three_seconds() {
        let mut w = ComputeWatchdog::default();
        for t in 0..3 {
            assert_eq!(sample(&mut w, t, Some(false)), []);
        }
        assert_eq!(
            sample(&mut w, 3, Some(false)),
            [Rule::NoInitialCompute]
        );
    }
    #[test]
    fn late_first_compute_cannot_erase_the_startup_violation() {
        let mut w = ComputeWatchdog::default();
        assert_eq!(sample(&mut w, 0, Some(false)), []);
        assert_eq!(
            sample(&mut w, 4, Some(true)),
            [Rule::NoInitialCompute]
        );
    }

    #[test]
    fn ratio_excludes_first_six_seconds_and_triggers_at_half() {
        let mut w = ComputeWatchdog::default();
        for t in 0..=8 {
            assert_eq!(sample(&mut w, t, Some(true)), []);
        }
        assert_eq!(sample(&mut w, 9, Some(false)), []);
        assert_eq!(sample(&mut w, 10, Some(false)), []);
        assert_eq!(sample(&mut w, 11, Some(false)), []);
        assert_eq!(
            sample(&mut w, 12, Some(false)),
            [Rule::LowComputeDuty]
        );
    }
    #[test]
    fn alternating_activity_cannot_evade_the_half_idle_rule() {
        let mut w = ComputeWatchdog::default();
        for t in 0..=6 {
            assert_eq!(sample(&mut w, t, Some(true)), []);
        }
        assert_eq!(sample(&mut w, 7, Some(false)), []);
        assert_eq!(
            sample(&mut w, 8, Some(true)),
            [Rule::LowComputeDuty]
        );
    }

    #[test]
    fn short_compute_gets_thirty_second_idle_limit() {
        let mut w = ComputeWatchdog::default();
        // Build a busy history large enough to avoid the separate ratio rule.
        for t in 0..=80 {
            assert_eq!(sample(&mut w, t, Some(true)), []);
        }
        for t in 81..=111 {
            assert_eq!(sample(&mut w, t, Some(false)), []);
        }
        assert_eq!(
            sample(&mut w, 112, Some(false)),
            [Rule::IdleCompute30]
        );
    }
    #[test]
    fn sustained_compute_earns_only_ninety_second_idle_rule() {
        let mut w = ComputeWatchdog::default();
        for t in 0..=91 {
            assert_eq!(sample(&mut w, t, Some(true)), []);
        }
        // Restart retains the allowance and accumulated evidence.
        let mut w: ComputeWatchdog =
            serde_json::from_str(&serde_json::to_string(&w).unwrap()).unwrap();
        for t in 92..=182 {
            assert_eq!(sample(&mut w, t, Some(false)), []);
        }
        assert_eq!(
            sample(&mut w, 183, Some(false)),
            [Rule::IdleCompute90]
        );
    }
    #[test]
    fn unknown_and_missing_samples_neither_prove_idle_nor_continuity() {
        let mut w = ComputeWatchdog::default();
        assert_eq!(sample(&mut w, 0, Some(true)), []);
        assert_eq!(sample(&mut w, 90, Some(true)), []);
        assert!(!w.sustained);
        assert_eq!(sample(&mut w, 91, None), []);
        assert_eq!(sample(&mut w, 200, Some(false)), []);
    }
    #[test]
    fn a_missed_first_compute_leaves_the_later_rules_in_force() {
        let mut w = ComputeWatchdog::default();
        for t in 0..3 {
            assert_eq!(sample(&mut w, t, Some(false)), []);
        }
        assert_eq!(sample(&mut w, 3, Some(false)), [Rule::NoInitialCompute]);
        assert_eq!(sample(&mut w, 4, Some(true)), [Rule::NoInitialCompute]);
        for t in 5..=20 {
            assert_eq!(sample(&mut w, t, Some(true)), [], "second {t}");
        }
        for t in 21..=35 {
            assert_eq!(sample(&mut w, t, Some(false)), [], "second {t}");
        }
        assert_eq!(sample(&mut w, 36, Some(false)), [Rule::LowComputeDuty]);
    }
    #[test]
    fn each_broken_rule_reads_with_the_service_limits() {
        let limits = limits();
        assert_eq!(
            limits.describe(Rule::NoVram),
            "no GPU VRAM allocation within 1 s of the grant"
        );
        assert_eq!(
            limits.describe(Rule::NoInitialCompute),
            "no GPU compute in the first 3 s of the grant"
        );
        assert_eq!(
            limits.describe(Rule::LowComputeDuty),
            "the GPU was idle for at least half of the grant"
        );
        assert_eq!(
            limits.describe(Rule::IdleCompute30),
            "no GPU compute for more than 30 s"
        );
    }
    #[test]
    fn declared_idle_time_is_neither_evidence_nor_startup_time() {
        let mut w = ComputeWatchdog::default();
        assert_eq!(sample(&mut w, 0, Some(false)), []);
        assert_eq!(w.excuse(SECOND, 100.0, 10.0), Ok(110.0));
        for t in 1..=12 {
            assert_eq!(sample(&mut w, t, Some(false)), [], "second {t}");
        }
        assert_eq!(
            sample(&mut w, 13, Some(false)),
            [Rule::NoInitialCompute],
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
