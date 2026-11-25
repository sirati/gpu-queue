//! A job's card figures over its grants. Each sample is weighted by the interval before it, since NVML's power and
//! utilisation readings average the period before the read; an interval counts only inside one grant and no longer
//! than the poll's gap bound, and a peak counts every sample. The energy counter's growth counts inside one grant
//! across any gap: the counter itself spans it.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::gpu::Figures;
use crate::protocol::{Energy, Figure, JobTelemetry};

const MIB: f64 = (1u64 << 20) as f64;

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
struct Weighted {
    seconds: f64,
    integral: f64,
    peak: Option<f64>,
}

impl Weighted {
    fn observe(&mut self, value: Option<f64>, interval: Option<f64>) {
        let Some(value) = value else { return };
        self.peak = Some(self.peak.map_or(value, |peak| peak.max(value)));
        if let Some(seconds) = interval {
            self.seconds += seconds;
            self.integral += value * seconds;
        }
    }

    fn figure(&self) -> Option<Figure> {
        self.peak.map(|peak| Figure {
            mean: (self.seconds > 0.0).then(|| self.integral / self.seconds),
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
    power: Weighted,
    power_limit: Weighted,
    utilization: Weighted,
    memory_utilization: Weighted,
    temperature: Weighted,
    sm_clock: Weighted,
    process_sm: Weighted,
    process_memory: Weighted,
    vram: Weighted,
    energy: Counter,
    throttle: BTreeMap<String, f64>,
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
        for reason in card.throttle.iter().flatten() {
            *self.throttle.entry(reason.to_string()).or_default() += interval;
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
            shared_seconds: self.shared,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const S: u64 = 1_000_000_000;
    const GAP: u64 = 3 * S;

    fn card(power: f64, energy: u64, throttle: Vec<&'static str>) -> Figures {
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
        let (idle, busy) = (card(100.0, 1_000, vec!["gpu_idle"]), card(500.0, 1_500, vec!["sw_power_cap"]));
        record.observe(S, S, &reading(&idle, 0, false), GAP);
        record.observe(S, 2 * S, &reading(&busy, 80, false), GAP);
        record.observe(S, 5 * S, &reading(&idle, 20, true), GAP);
        let summary = record.summary().unwrap();
        assert_eq!((summary.samples, summary.seconds), (3, 4.0));
        assert_eq!(summary.power_watts, Some(Figure { mean: Some((500.0 + 3.0 * 100.0) / 4.0), peak: 500.0 }));
        assert_eq!(summary.process_sm_utilization, Some(Figure { mean: Some((80.0 + 3.0 * 20.0) / 4.0), peak: 80.0 }));
        assert_eq!(summary.vram_mib, Some(Figure { mean: Some(2048.0), peak: 2048.0 }));
        assert_eq!(summary.throttle_seconds, BTreeMap::from([("gpu_idle".into(), 3.0), ("sw_power_cap".into(), 1.0)]));
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
        assert_eq!(summary.power_watts, Some(Figure { mean: None, peak: 300.0 }));
        assert_eq!((summary.sm_clock_mhz, summary.energy, summary.gpu_utilization), (None, None, None));
        assert!(summary.throttle_seconds.is_empty());
    }
}
