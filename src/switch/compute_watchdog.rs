//! Per-grant sampled compute policy. Unknown samples never prove idleness.
use super::config::SwitchConfig;
use crate::protocol::{Activity, ComputeEvidence, DutyVerdict, FirstCompute, Judgement, Rule};
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

/// What the job owner knows of a grant's work, the watchdog's structured input (`Job::work`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GrantWork {
    pub working: bool,
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
    /// The span the duty rule covers has ended and was judged.
    #[serde(default)]
    duty_judged: bool,
    /// The duty rule's judgement, recorded when the span was judged; a checkpoint written before it restores none.
    #[serde(default)]
    judgement: Option<Judgement>,
    /// The first-compute judgement, recorded by `observe`.
    #[serde(default)]
    first_compute: FirstCompute,
}

impl ComputeWatchdog {
    /// Old checkpoints cannot prove historic duty or a continuous-compute streak.
    /// Preserve their old 90-second idle policy until the next grant.
    pub fn legacy() -> Self {
        Self {
            ever_busy: true,
            sustained: true,
            ..Self::default()
        }
    }

    /// `working`: the grant still does its own work, false once the queue ordered it to end or its holder reported
    /// a failure.
    pub fn observe(
        &mut self,
        at: u64,
        granted: u64,
        activity: Option<bool>,
        working: bool,
        max_gap: u64,
        limits: &Thresholds,
    ) -> Vec<Rule> {
        let mut broken = Vec::new();
        if !working {
            // Its shutdown is not its work: no later sample counts for the duty rule.
            broken.extend(self.end_span());
        }
        let Some(busy) = activity else {
            self.previous = None;
            self.busy_since = None;
            self.idle_since = None;
            return broken;
        };
        let started = at.saturating_sub(granted);
        // Compute first seen after the deadline breaks the rule too; the grant goes on and is judged further.
        if !self.ever_busy
            && (started > limits.first_compute || !busy && started >= limits.first_compute)
        {
            broken.push(Rule::NoInitialCompute);
            self.first_compute = FirstCompute::Broken;
        }
        let previous = self.previous.replace((at, busy));
        let connected = previous.is_some_and(|(prev, _)| at >= prev && at - prev <= max_gap);
        if !connected {
            self.busy_since = None;
            self.idle_since = None;
        }
        if let Some((prev, was_busy)) = previous.filter(|_| connected) {
            let warm = granted.saturating_add(6 * SECOND);
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
            if self.first_compute == FirstCompute::Pending {
                self.first_compute = FirstCompute::Seen { at_seconds: seconds(at) };
            }
            self.idle_since = None;
            let since = *self.busy_since.get_or_insert(at);
            if at.saturating_sub(since) > 90 * SECOND {
                // Sustained compute ends the span the duty rule covers.
                broken.extend(self.end_span());
                self.sustained = true;
            }
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
        broken
    }

    /// Ends the span the duty rule covers and judges it over everything measured, once: the span ends when the
    /// grant stops working or first computes for 90 s without a pause, whichever comes first. Part of the span
    /// proves nothing about the duty of the whole, so the rule is never judged before the span ends.
    pub fn end_span(&mut self) -> Option<Rule> {
        let open = !self.sustained && !self.duty_judged;
        self.duty_judged = true;
        let low = self.measured > 0 && self.idle as u128 * 2 >= self.measured as u128;
        if open {
            self.judgement = Some(Judgement {
                verdict: match (self.measured, low) {
                    (0, _) => DutyVerdict::Unmeasured,
                    (_, true) => DutyVerdict::Low,
                    (_, false) => DutyVerdict::NotLow,
                },
                measured_seconds: seconds(self.measured),
                idle_seconds: seconds(self.idle),
            });
        }
        (open && low).then_some(Rule::LowComputeDuty)
    }

    /// The watchdog's evidence for its viewer, with the job owner's `work`.
    pub fn evidence(&self, work: GrantWork) -> ComputeEvidence {
        ComputeEvidence {
            measured_seconds: seconds(self.measured),
            idle_seconds: seconds(self.idle),
            first_compute: self.first_compute,
            last_activity: self.previous.map(|(at, busy)| Activity { at_seconds: seconds(at), busy }),
            sustained: self.sustained,
            judgement: self.judgement,
            working: work.working,
        }
    }
}

/// Nanoseconds of the monotonic clock in seconds.
fn seconds(ns: u64) -> f64 {
    ns as f64 / SECOND as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    fn limits() -> Thresholds {
        Thresholds::of(&serde_json::from_str::<SwitchConfig>(r#"{"listen":[]}"#).unwrap())
    }
    fn observed(
        w: &mut ComputeWatchdog,
        second: u64,
        busy: Option<bool>,
        working: bool,
    ) -> Vec<Rule> {
        w.observe(second * SECOND, 0, busy, working, 2 * SECOND, &limits())
    }
    fn sample(w: &mut ComputeWatchdog, second: u64, busy: Option<bool>) -> Vec<Rule> {
        observed(w, second, busy, true)
    }
    /// A sample once the queue ordered the grant to end.
    fn after_end_order(w: &mut ComputeWatchdog, second: u64, busy: Option<bool>) -> Vec<Rule> {
        observed(w, second, busy, false)
    }
    /// The duty verdict if the span ended now.
    fn verdict_now(w: &ComputeWatchdog) -> Option<Rule> {
        w.clone().end_span()
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
        assert_eq!(w.end_span(), None);
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
        for t in 9..=11 {
            assert_eq!(sample(&mut w, t, Some(false)), []);
        }
        assert_eq!(verdict_now(&w), None);
        assert_eq!(sample(&mut w, 12, Some(false)), []);
        assert_eq!(verdict_now(&w), Some(Rule::LowComputeDuty));
    }
    #[test]
    fn alternating_activity_cannot_evade_the_half_idle_rule() {
        let mut w = ComputeWatchdog::default();
        for t in 0..=6 {
            assert_eq!(sample(&mut w, t, Some(true)), []);
        }
        assert_eq!(sample(&mut w, 7, Some(false)), []);
        assert_eq!(verdict_now(&w), None);
        assert_eq!(sample(&mut w, 8, Some(true)), []);
        assert_eq!(verdict_now(&w), Some(Rule::LowComputeDuty));
    }
    #[test]
    fn an_idle_sample_at_the_end_of_the_warm_up_is_not_the_grants_duty() {
        let mut w = ComputeWatchdog::default();
        for t in 0..=4 {
            assert_eq!(sample(&mut w, t, Some(true)), []);
        }
        for t in 5..=6 {
            assert_eq!(sample(&mut w, t, Some(false)), []);
        }
        for t in 7..=40 {
            assert_eq!(sample(&mut w, t, Some(true)), [], "second {t}");
        }
        assert_eq!(verdict_now(&w), None);
    }
    #[test]
    fn sustained_compute_ends_the_phase_the_duty_rule_judges() {
        let mut w = ComputeWatchdog::default();
        for t in 0..=6 {
            assert_eq!(sample(&mut w, t, Some(true)), []);
        }
        // Idle 29 of every 30 seconds keeps within the 30-second idle rule.
        for block in 0..4 {
            let start = 7 + 30 * block;
            for t in start..start + 29 {
                assert_eq!(sample(&mut w, t, Some(false)), [], "second {t}");
            }
            assert_eq!(sample(&mut w, start + 29, Some(true)), []);
        }
        for t in 127..217 {
            assert_eq!(sample(&mut w, t, Some(true)), [], "second {t}");
        }
        assert_eq!(sample(&mut w, 217, Some(true)), [Rule::LowComputeDuty]);
        assert_eq!(sample(&mut w, 218, Some(true)), []);
        assert_eq!(verdict_now(&w), None);
    }
    #[test]
    fn the_duty_span_is_judged_once() {
        let mut w = ComputeWatchdog::default();
        for t in 0..=6 {
            assert_eq!(sample(&mut w, t, Some(true)), []);
        }
        for t in 7..=9 {
            assert_eq!(sample(&mut w, t, Some(false)), []);
        }
        assert_eq!(w.end_span(), Some(Rule::LowComputeDuty));
        assert_eq!(w.end_span(), None);
        // A restart keeps the span ended.
        let mut w: ComputeWatchdog =
            serde_json::from_str(&serde_json::to_string(&w).unwrap()).unwrap();
        for t in 10..=20 {
            assert_eq!(sample(&mut w, t, Some(false)), [], "second {t}");
        }
        assert_eq!(w.end_span(), None);
    }
    #[test]
    fn an_order_to_end_ends_the_duty_span_before_later_samples_count() {
        let dozed = || {
            let mut w = ComputeWatchdog::default();
            for t in 0..=2 {
                assert_eq!(sample(&mut w, t, Some(true)), []);
            }
            for t in 3..=8 {
                assert_eq!(sample(&mut w, t, Some(false)), []);
            }
            w
        };
        let mut w = dozed();
        assert_eq!(
            after_end_order(&mut w, 9, Some(true)),
            [Rule::LowComputeDuty]
        );
        for t in 10..=30 {
            assert_eq!(after_end_order(&mut w, t, Some(true)), [], "second {t}");
        }
        assert_eq!(w.end_span(), None);
        // An unknown sample at the order does not defer it.
        let mut w = dozed();
        assert_eq!(after_end_order(&mut w, 9, None), [Rule::LowComputeDuty]);
    }
    #[test]
    fn a_state_saved_before_the_duty_span_flag_keeps_its_span_open() {
        let mut w = ComputeWatchdog::default();
        for t in 0..=6 {
            assert_eq!(sample(&mut w, t, Some(true)), []);
        }
        for t in 7..=9 {
            assert_eq!(sample(&mut w, t, Some(false)), []);
        }
        let mut saved = serde_json::to_value(&w).unwrap();
        saved
            .as_object_mut()
            .unwrap()
            .remove("duty_judged")
            .unwrap();
        let mut w: ComputeWatchdog = serde_json::from_value(saved).unwrap();
        assert_eq!(w.end_span(), Some(Rule::LowComputeDuty));
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
        assert_eq!(verdict_now(&w), None);
        assert_eq!(sample(&mut w, 36, Some(false)), []);
        assert_eq!(verdict_now(&w), Some(Rule::LowComputeDuty));
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

    /// The record of a judged span is the verdict `end_span` returns, with the accumulators as they were then; a
    /// later observation never changes it, while the accumulators go on.
    #[test]
    fn the_recorded_judgement_agrees_with_end_span_and_never_changes_after() {
        let mut w = ComputeWatchdog::default();
        sample(&mut w, 1, Some(true));
        (2..=20).for_each(|second| {
            sample(&mut w, second, Some(false));
        });
        let before = w.evidence(GrantWork { working: true });
        assert_eq!(w.end_span(), Some(Rule::LowComputeDuty));
        let judged = w.evidence(GrantWork { working: false });
        assert_eq!(
            judged.judgement,
            Some(Judgement { verdict: DutyVerdict::Low, measured_seconds: before.measured_seconds, idle_seconds: before.idle_seconds })
        );
        assert_eq!(judged.first_compute, FirstCompute::Seen { at_seconds: 1.0 });
        assert_eq!(judged.last_activity, Some(Activity { at_seconds: 20.0, busy: false }));
        assert!(!judged.working);
        (21..=40).for_each(|second| {
            sample(&mut w, second, Some(true));
        });
        let later = w.evidence(GrantWork { working: false });
        assert_eq!(later.judgement, judged.judgement, "a later observation never changes the record");
        assert!(later.measured_seconds > judged.measured_seconds, "while the accumulators go on");
        assert_eq!(w.end_span(), None, "a span is judged once");
    }

    #[test]
    fn a_span_with_nothing_measured_is_unmeasured_and_breaks_no_rule() {
        let mut w = ComputeWatchdog::default();
        assert_eq!(w.end_span(), None);
        assert_eq!(
            w.evidence(GrantWork { working: false }).judgement,
            Some(Judgement { verdict: DutyVerdict::Unmeasured, measured_seconds: 0.0, idle_seconds: 0.0 })
        );
    }

    #[test]
    fn a_busy_span_is_judged_not_low() {
        let mut w = ComputeWatchdog::default();
        (1..=20).for_each(|second| {
            sample(&mut w, second, Some(true));
        });
        assert_eq!(w.end_span(), None);
        assert_eq!(w.evidence(GrantWork { working: false }).judgement.map(|j| j.verdict), Some(DutyVerdict::NotLow));
    }

    #[test]
    fn a_compute_first_seen_after_its_deadline_records_the_rule_broken() {
        let mut w = ComputeWatchdog::default();
        assert_eq!(sample(&mut w, 4, Some(true)), [Rule::NoInitialCompute]);
        assert_eq!(w.evidence(GrantWork { working: true }).first_compute, FirstCompute::Broken);
    }
}
