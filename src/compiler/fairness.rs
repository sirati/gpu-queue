//! Service time, rather than command count, determines each subqueue's turns.
use std::collections::BTreeMap;

#[derive(Default)]
struct History {
    seconds: f64,
    count: u64,
}
#[derive(Default)]
pub struct Fairness {
    history: BTreeMap<String, History>,
    total: History,
}
impl Fairness {
    pub fn observe(&mut self, group: &str, seconds: f64) {
        if !seconds.is_finite() || seconds <= 0.0 {
            return;
        }
        let history = self.history.entry(group.to_owned()).or_default();
        history.seconds += seconds;
        history.count += 1;
        self.total.seconds += seconds;
        self.total.count += 1;
    }
    pub fn average(&self, group: &str) -> f64 {
        let history = self
            .history
            .get(group)
            .filter(|h| h.count > 0)
            .unwrap_or(&self.total);
        if history.count == 0 {
            1.0
        } else {
            history.seconds / history.count as f64
        }
    }
    pub fn served(&self, group: &str) -> f64 {
        self.history.get(group).map(|h| h.seconds).unwrap_or(0.0)
    }
    pub fn score(&self, group: &str, baseline: f64, active: impl Iterator<Item = f64>) -> f64 {
        let average = self.average(group);
        baseline + self.served(group) + active.map(|elapsed| elapsed.max(average)).sum::<f64>()
    }
}

/// One compiler is indivisible on a one-slot machine. Otherwise floor the cap.
pub fn core_limit(eligible: usize) -> usize {
    if eligible == 0 {
        0
    } else {
        (eligible * 2 / 3).max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn duration_corrects_turns_instead_of_counting_commands() {
        let mut f = Fairness::default();
        f.observe("short", 1.0);
        f.observe("long", 10.0);
        // One completed and one running command in each queue: the long queue
        // has consumed ten times the service, even with equal command counts.
        assert_eq!(f.score("short", 0.0, [0.0].into_iter()), 2.0);
        assert_eq!(f.score("long", 0.0, [0.0].into_iter()), 20.0);
        for _ in 0..18 {
            f.observe("short", 1.0);
        }
        assert_eq!(f.score("short", 0.0, [0.0].into_iter()), 20.0);
        // Overrunning work immediately counts its elapsed time.
        assert_eq!(f.score("short", 0.0, [7.0].into_iter()), 26.0);
    }
    #[test]
    fn exact_completion_replaces_the_running_estimate() {
        let mut f = Fairness::default();
        f.observe("queue", 10.0);
        assert_eq!(f.score("queue", 0.0, [0.0].into_iter()), 20.0);
        f.observe("queue", 2.0);
        assert_eq!(f.average("queue"), 6.0);
        assert_eq!(f.score("queue", 0.0, [].into_iter()), 12.0);
    }
    #[test]
    fn queue_cap_rounds_down_against_compiler_eligible_cores() {
        assert_eq!(
            (0..7).map(core_limit).collect::<Vec<_>>(),
            [0, 1, 1, 2, 2, 3, 4]
        );
        assert_eq!(core_limit(31), 20);
    }
}
