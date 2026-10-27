//! Per-job grant accounting. VRAM and SM evidence have separate clocks, so a
//! job holding allocated but idle memory is not mistaken for a zero-VRAM job.
use crate::protocol::JobTiming;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Serialize, Deserialize)]
struct ZeroTime {
    seconds: f64,
    at: Option<u64>,
    zero: Option<bool>,
}

impl ZeroTime {
    fn observe(&mut self, at: u64, used: Option<bool>, max_gap: u64) {
        if let Some(previous) = self.at {
            if at < previous {
                return;
            }
            if self.zero == Some(true) && used == Some(false) && at - previous <= max_gap {
                self.seconds += seconds(at - previous);
            }
        }
        self.at = Some(at);
        self.zero = used.map(|used| !used);
    }

    fn reset(&mut self) {
        self.at = None;
        self.zero = None;
    }
}

fn seconds(ns: u64) -> f64 {
    ns as f64 / 1_000_000_000.0
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Timing {
    gpu_seconds: f64,
    preparation_seconds: f64,
    running_since: Option<u64>,
    preparation_since: Option<u64>,
    memory: ZeroTime,
    compute: ZeroTime,
}

impl Timing {
    pub fn begin(&mut self, at: u64) {
        self.running_since = Some(at);
        self.preparation_since = Some(at);
        self.memory.reset();
        self.compute.reset();
    }

    pub fn memory(&mut self, at: u64, used: Option<bool>) {
        if !self.running_since.is_some_and(|start| at >= start) {
            return;
        }
        self.memory.observe(at, used, 2_000_000_000);
        if used == Some(true) {
            if let Some(start) = self.preparation_since.take() {
                self.preparation_seconds += seconds(at.saturating_sub(start));
            }
        }
    }

    pub fn compute(&mut self, at: u64, used: Option<bool>, max_gap: u64) {
        if !self.running_since.is_some_and(|start| at >= start) {
            return;
        }
        self.compute.observe(at, used, max_gap);
    }

    pub fn stop(&mut self, at: u64) {
        if let Some(start) = self.running_since.take() {
            self.gpu_seconds += seconds(at.saturating_sub(start));
        }
        if let Some(start) = self.preparation_since.take() {
            self.preparation_seconds += seconds(at.saturating_sub(start));
        }
        self.memory.reset();
        self.compute.reset();
    }

    pub fn snapshot(&self, at: u64) -> JobTiming {
        JobTiming {
            preparation_seconds: self.preparation_seconds
                + self
                    .preparation_since
                    .map(|start| seconds(at.saturating_sub(start)))
                    .unwrap_or(0.0),
            gpu_seconds: self.gpu_seconds
                + self
                    .running_since
                    .map(|start| seconds(at.saturating_sub(start)))
                    .unwrap_or(0.0),
            no_vram_seconds: self.memory.seconds,
            no_utilisation_seconds: self.compute.seconds,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const S: u64 = 1_000_000_000;

    #[test]
    fn preparation_runtime_and_idle_are_independent_and_freeze_on_departure() {
        let mut timing = Timing::default();
        timing.begin(S);
        timing.memory(S, Some(false));
        timing.memory(2 * S, Some(false));
        timing.memory(3 * S, Some(true));
        timing.compute(3 * S, Some(false), 3 * S);
        timing.compute(5 * S, Some(false), 3 * S);
        timing.stop(6 * S);
        let stats = timing.snapshot(100 * S);
        assert_eq!(stats.preparation_seconds, 2.0);
        assert_eq!(stats.gpu_seconds, 5.0);
        assert_eq!(stats.no_vram_seconds, 1.0);
        assert_eq!(stats.no_utilisation_seconds, 2.0);
    }

    #[test]
    fn unknown_samples_and_restart_gaps_do_not_count_as_idle() {
        let mut timing = Timing::default();
        timing.begin(S);
        timing.memory(S, Some(false));
        timing.memory(2 * S, None);
        timing.memory(3 * S, Some(false));
        timing.memory(9 * S, Some(false));
        assert_eq!(timing.snapshot(10 * S).no_vram_seconds, 0.0);
        let mut restored: Timing =
            serde_json::from_str(&serde_json::to_string(&timing).unwrap()).unwrap();
        restored.stop(10 * S);
        restored.begin(20 * S);
        restored.memory(21 * S, Some(true));
        restored.stop(22 * S);
        assert_eq!(restored.snapshot(30 * S).gpu_seconds, 11.0);
        assert_eq!(restored.snapshot(30 * S).preparation_seconds, 10.0);
    }
}
