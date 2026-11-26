//! A job's card figures over its grants. Each sample is weighted by the interval before it, since NVML's power and
//! utilisation readings average the period before the read; an interval counts only inside one grant and no longer
//! than the poll's gap bound, and a peak counts every sample. A figure's 90th percentile comes from the seconds
//! spent at each of its values, rounded to the figure's quantum. The energy counter's growth counts inside one grant
//! across any gap: the counter itself spans it.
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::gpu::Figures;
use crate::protocol::{Energy, Figure, JobTelemetry};

const MIB: f64 = (1u64 << 20) as f64;

/// The value step a figure's percentile is exact to.
trait Step {
    const STEP: f64;
}

#[derive(Clone, Default)]
struct Watt;
#[derive(Clone, Default)]
struct Percent;
#[derive(Clone, Default)]
struct Celsius;
#[derive(Clone, Default)]
struct Mhz;
/// Device memory, in MiB.
#[derive(Clone, Default)]
struct VramMib;

impl Step for Watt {
    const STEP: f64 = 1.0;
}
impl Step for Percent {
    const STEP: f64 = 1.0;
}
impl Step for Celsius {
    const STEP: f64 = 1.0;
}
impl Step for Mhz {
    const STEP: f64 = 1.0;
}
impl Step for VramMib {
    const STEP: f64 = 16.0;
}

/// One sample as one job holding a grant sees it.
pub struct Reading<'a> {
    pub card: &'a Figures,
    /// The device memory its processes hold; unknown where the driver does not report a process's.
    pub vram: Option<u64>,
    /// Its processes' utilisation shares, summed.
    pub sm: u32,
    pub memory: u32,
    /// Whether a process other than its own computes.
    pub shared: bool,
}

fn seconds(ns: u64) -> f64 {
    ns as f64 / 1_000_000_000.0
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(bound = "")]
struct Weighted<S: Step> {
    seconds: f64,
    integral: f64,
    peak: Option<f64>,
    /// Seconds spent at each value, in steps of `S`.
    #[serde(default)]
    spent: BTreeMap<i64, f32>,
    #[serde(skip)]
    step: PhantomData<S>,
}

impl<S: Step> Weighted<S> {
    fn observe(&mut self, value: Option<f64>, interval: Option<f64>) {
        let Some(value) = value else { return };
        self.peak = Some(self.peak.map_or(value, |peak| peak.max(value)));
        if let Some(seconds) = interval {
            self.seconds += seconds;
            self.integral += value * seconds;
            *self.spent.entry((value / S::STEP).round() as i64).or_default() += seconds as f32;
        }
    }

    /// The least value at which at least `share` of the weighted time was spent at it or below.
    fn percentile(&self, share: f64) -> Option<f64> {
        let total: f64 = self.spent.values().map(|&seconds| f64::from(seconds)).sum();
        let mut below = 0.0;
        self.spent
            .iter()
            .find(|(_, &seconds)| {
                below += f64::from(seconds);
                below >= share * total
            })
            .map(|(&step, _)| step as f64 * S::STEP)
    }

    fn figure(&self) -> Option<Figure> {
        self.peak.map(|peak| Figure {
            mean: (self.seconds > 0.0).then(|| self.integral / self.seconds),
            p90: self.percentile(0.9),
            peak,
        })
    }
}

/// The energy counter's growth between readings of one grant; a reading below the last (a driver reload) restarts
/// the count from it.
#[derive(Clone, Default, Serialize, Deserialize)]
struct Counter {
    joules: f64,
    seconds: f64,
    /// The last reading: its time and millijoules.
    last: Option<(u64, u64)>,
}

impl Counter {
    fn observe(&mut self, at: u64, millijoules: Option<u64>) {
        let Some(millijoules) = millijoules else { return };
        if let Some((then, before)) = self.last.filter(|&(_, before)| millijoules >= before) {
            self.joules += (millijoules - before) as f64 / 1000.0;
            self.seconds += seconds(at - then);
        }
        self.last = Some((at, millijoules));
    }

    fn energy(&self) -> Option<Energy> {
        (self.seconds > 0.0).then_some(Energy {
            joules: self.joules,
            seconds: self.seconds,
        })
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Record {
    /// The start of the grant the last sample was in, and the sample's time.
    window: Option<u64>,
    at: Option<u64>,
    samples: u64,
    seconds: f64,
    power: Weighted<Watt>,
    power_limit: Weighted<Watt>,
    utilization: Weighted<Percent>,
    memory_utilization: Weighted<Percent>,
    temperature: Weighted<Celsius>,
    sm_clock: Weighted<Mhz>,
    process_sm: Weighted<Percent>,
    process_memory: Weighted<Percent>,
    vram: Weighted<VramMib>,
    energy: Counter,
    throttle: BTreeMap<String, f64>,
    #[serde(default)]
    throttle_causes: BTreeMap<String, f64>,
    shared: f64,
}

impl Record {
    /// Adds the sample read at `at` to the grant that began at `window`; a sample read before the grant began is
    /// not its.
    pub fn observe(&mut self, window: u64, at: u64, reading: &Reading, max_gap: u64) {
        if at < window || self.at.is_some_and(|previous| self.window == Some(window) && at < previous) {
            return;
        }
        if self.window != Some(window) {
            self.window = Some(window);
            self.at = None;
            self.energy.last = None;
        }
        let interval = self
            .at
            .filter(|&previous| at - previous <= max_gap)
            .map(|previous| seconds(at - previous));
        self.at = Some(at);
        self.samples += 1;
        let card = reading.card;
        self.power.observe(card.power_draw, interval);
        self.power_limit.observe(card.power_limit, interval);
        self.utilization.observe(card.utilization.map(f64::from), interval);
        self.memory_utilization.observe(card.memory_utilization.map(f64::from), interval);
        self.temperature.observe(card.temperature.map(f64::from), interval);
        self.sm_clock.observe(card.sm_clock.map(f64::from), interval);
        self.process_sm.observe(Some(f64::from(reading.sm)), interval);
        self.process_memory.observe(Some(f64::from(reading.memory)), interval);
        self.vram.observe(reading.vram.map(|bytes| bytes as f64 / MIB), interval);
        self.energy.observe(at, card.energy);
        let Some(interval) = interval else { return };
        self.seconds += interval;
        let reasons = card.throttle.iter().flatten();
        for reason in reasons.clone() {
            *self.throttle.entry(reason.name.to_string()).or_default() += interval;
        }
        for cause in reasons.map(|reason| reason.cause.as_str()).collect::<BTreeSet<_>>() {
            *self.throttle_causes.entry(cause.to_string()).or_default() += interval;
        }
        self.shared += if reading.shared { interval } else { 0.0 };
    }

    /// No sample yet.
    pub fn is_empty(&self) -> bool {
        self.samples == 0
    }

    /// The figures so far; none before the first sample.
    pub fn summary(&self) -> Option<JobTelemetry> {
        (self.samples > 0).then(|| JobTelemetry {
            seconds: self.seconds,
            samples: self.samples,
            power_watts: self.power.figure(),
            power_limit_watts: self.power_limit.figure(),
            energy: self.energy.energy(),
            gpu_utilization: self.utilization.figure(),
            memory_utilization: self.memory_utilization.figure(),
            temperature_celsius: self.temperature.figure(),
            sm_clock_mhz: self.sm_clock.figure(),
            process_sm_utilization: self.process_sm.figure(),
            process_memory_utilization: self.process_memory.figure(),
            vram_mib: self.vram.figure(),
            throttle_seconds: self.throttle.clone(),
            throttle_cause_seconds: self.throttle_causes.clone(),
            shared_seconds: self.shared,
        })
    }
}

/// A job's telemetry: recorded while the job may still hold a grant, then only its figures, so the finished
/// history keeps no distributions.
#[derive(Clone)]
pub enum Telemetry {
    Recording(Record),
    Sealed(Option<JobTelemetry>),
}

impl Default for Telemetry {
    fn default() -> Telemetry {
        Telemetry::Recording(Record::default())
    }
}

impl Telemetry {
    /// As `Record::observe`; a sealed job holds no grant.
    pub fn observe(&mut self, window: u64, at: u64, reading: &Reading, max_gap: u64) {
        if let Telemetry::Recording(record) = self {
            record.observe(window, at, reading, max_gap);
        }
    }

    pub fn summary(&self) -> Option<JobTelemetry> {
        match self {
            Telemetry::Recording(record) => record.summary(),
            Telemetry::Sealed(summary) => summary.clone(),
        }
    }

    /// The job never holds a grant again: only its figures are kept.
    pub fn seal(&mut self) {
        if let Telemetry::Recording(record) = self {
            *self = Telemetry::Sealed(record.summary());
        }
    }

    /// The record while it can still grow.
    pub fn recording(&self) -> Option<&Record> {
        match self {
            Telemetry::Recording(record) => Some(record),
            Telemetry::Sealed(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gpu::ThrottleReason;
    use crate::protocol::ThrottleCause;
    const S: u64 = 1_000_000_000;
    const GAP: u64 = 3 * S;
    const IDLE: ThrottleReason = ThrottleReason { name: "gpu_idle", cause: ThrottleCause::Idle };
    const POWER_CAP: ThrottleReason = ThrottleReason { name: "sw_power_cap", cause: ThrottleCause::Power };

    fn card(power: f64, energy: u64, throttle: Vec<ThrottleReason>) -> Figures {
        Figures {
            power_draw: Some(power),
            power_limit: Some(575.0),
            temperature: Some(70),
            utilization: Some(90),
            memory_utilization: Some(40),
            sm_clock: Some(2_700),
            throttle: Some(throttle),
            energy: Some(energy),
        }
    }

    fn reading(card: &Figures, sm: u32, shared: bool) -> Reading<'_> {
        Reading {
            card,
            vram: Some(2 << 30),
            sm,
            memory: sm / 2,
            shared,
        }
    }

    #[test]
    fn each_sample_weighs_the_interval_before_it_and_the_first_counts_only_its_peak() {
        let mut record = Record::default();
        let (idle, busy) = (card(100.0, 1_000, vec![IDLE]), card(500.0, 1_500, vec![POWER_CAP]));
        record.observe(S, S, &reading(&idle, 0, false), GAP);
        record.observe(S, 2 * S, &reading(&busy, 80, false), GAP);
        record.observe(S, 5 * S, &reading(&idle, 20, true), GAP);
        let summary = record.summary().unwrap();
        assert_eq!((summary.samples, summary.seconds), (3, 4.0));
        assert_eq!(
            summary.power_watts,
            Some(Figure { mean: Some((500.0 + 3.0 * 100.0) / 4.0), p90: Some(500.0), peak: 500.0 })
        );
        assert_eq!(
            summary.process_sm_utilization,
            Some(Figure { mean: Some((80.0 + 3.0 * 20.0) / 4.0), p90: Some(80.0), peak: 80.0 })
        );
        assert_eq!(summary.vram_mib, Some(Figure { mean: Some(2048.0), p90: Some(2048.0), peak: 2048.0 }));
        assert_eq!(summary.throttle_seconds, BTreeMap::from([("gpu_idle".into(), 3.0), ("sw_power_cap".into(), 1.0)]));
        assert_eq!(summary.throttle_cause_seconds, BTreeMap::from([("idle".into(), 3.0), ("power".into(), 1.0)]));
        assert_eq!(summary.shared_seconds, 3.0);
        assert_eq!(
            summary.energy,
            Some(Energy { joules: 0.5, seconds: 1.0 }),
            "a reading below the last restarts the count from it"
        );
    }

    #[test]
    fn a_gap_beyond_the_bound_weighs_nothing_and_a_new_grant_starts_its_own_intervals() {
        let mut record = Record::default();
        let figures = |energy| card(400.0, energy, Vec::new());
        record.observe(S, S, &reading(&figures(10_000), 50, false), GAP);
        record.observe(S, 2 * S, &reading(&figures(10_400), 50, false), GAP);
        record.observe(S, 10 * S, &reading(&figures(13_600), 50, false), GAP);
        record.observe(20 * S, 21 * S, &reading(&figures(20_000), 50, false), GAP);
        record.observe(20 * S, 22 * S, &reading(&figures(20_400), 50, false), GAP);
        let summary = record.summary().unwrap();
        assert_eq!((summary.samples, summary.seconds), (5, 2.0));
        assert_eq!(summary.energy, Some(Energy { joules: 4.0, seconds: 10.0 }), "the counter spans the gap, not the break between grants");
    }

    #[test]
    fn a_sample_from_before_the_grant_or_out_of_order_is_not_its_and_unsupported_figures_stay_absent() {
        let mut record = Record::default();
        assert!(record.summary().is_none());
        let figures = Figures { power_draw: Some(300.0), ..Figures::default() };
        record.observe(5 * S, 4 * S, &reading(&figures, 10, false), GAP);
        assert!(record.summary().is_none());
        record.observe(5 * S, 6 * S, &reading(&figures, 10, false), GAP);
        record.observe(5 * S, 5 * S + 1, &reading(&figures, 99, false), GAP);
        let summary = record.summary().unwrap();
        assert_eq!(summary.samples, 1);
        assert_eq!(summary.power_watts, Some(Figure { mean: None, p90: None, peak: 300.0 }));
        assert_eq!((summary.sm_clock_mhz, summary.energy, summary.gpu_utilization), (None, None, None));
        assert!(summary.throttle_seconds.is_empty());
    }

    #[test]
    fn the_ninetieth_percentile_weighs_time_not_samples() {
        let mut record = Record::default();
        let watts = |power| card(power, 0, Vec::new());
        record.observe(S, S, &reading(&watts(100.0), 0, false), u64::MAX);
        record.observe(S, S + 9 * S + S / 2, &reading(&watts(100.0), 0, false), u64::MAX);
        for step in 1..=5 {
            record.observe(S, 10 * S + S / 2 + step * S / 10, &reading(&watts(500.4), 0, false), u64::MAX);
        }
        let power = record.summary().unwrap().power_watts.unwrap();
        assert_eq!((power.p90, power.peak), (Some(100.0), 500.4), "9.5 s at 100 W, five samples over 0.5 s at 500 W");
        let mut steps = Record::default();
        steps.observe(0, 0, &reading(&watts(0.0), 0, false), GAP);
        for watt in 1..=10 {
            steps.observe(0, watt * S, &reading(&watts(watt as f64), 0, false), GAP);
        }
        assert_eq!(steps.summary().unwrap().power_watts.unwrap().p90, Some(9.0), "the least value 9 s of 10 reach");
    }

    #[test]
    fn a_cause_counts_once_for_all_its_reasons_at_once() {
        let mut record = Record::default();
        let heat = |name| ThrottleReason { name, cause: ThrottleCause::Heat };
        let hot = card(
            450.0,
            0,
            vec![
                ThrottleReason { name: "hw_slowdown", cause: ThrottleCause::Hardware },
                heat("sw_thermal_slowdown"),
                heat("hw_thermal_slowdown"),
            ],
        );
        record.observe(S, S, &reading(&hot, 90, false), GAP);
        record.observe(S, 3 * S, &reading(&hot, 90, false), GAP);
        let summary = record.summary().unwrap();
        assert_eq!(summary.throttle_cause_seconds, BTreeMap::from([("hardware".into(), 2.0), ("heat".into(), 2.0)]));
        assert_eq!(summary.throttle_seconds.values().sum::<f64>(), 6.0, "each reason its own seconds");
    }

    #[test]
    fn a_sealed_telemetry_keeps_its_figures_and_neither_grows_nor_is_saved() {
        let mut telemetry = Telemetry::default();
        let figures = card(300.0, 0, Vec::new());
        telemetry.observe(S, S, &reading(&figures, 40, false), GAP);
        telemetry.observe(S, 2 * S, &reading(&figures, 40, false), GAP);
        let before = telemetry.summary();
        assert!(telemetry.recording().is_some());
        telemetry.seal();
        telemetry.observe(S, 3 * S, &reading(&figures, 99, false), GAP);
        assert_eq!(telemetry.summary(), before);
        assert!(telemetry.recording().is_none());
        let mut empty = Telemetry::default();
        empty.seal();
        assert_eq!(empty.summary(), None);
    }
}
