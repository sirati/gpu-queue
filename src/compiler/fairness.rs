//! Service time, rather than command count, determines each subqueue's turns.
//! Priority weighs a party's share: work is charged at its cost divided by the
//! weight 1+p, or multiplied by 1-p below zero, so a priority-10 party receives
//! eleven times the share of a priority-0 one and nothing starves.
use std::collections::{BTreeMap, BTreeSet};

// Divisible by every weight 1..=11, so space charges stay exact integers.
const SCALE: u64 = 27720;

fn weight(priority: i8) -> (u64, u64) {
    let priority = i64::from(priority);
    (1 + priority.max(0) as u64, 1 + (-priority).max(0) as u64)
}
/// Space held at a priority, in comparable units.
pub fn charge(amount: u64, priority: i8) -> u64 {
    let (above, below) = weight(priority);
    amount.saturating_mul(SCALE / above * below)
}
/// Service time at a priority, in comparable seconds.
pub fn charge_seconds(seconds: f64, priority: i8) -> f64 {
    let (above, below) = weight(priority);
    seconds * below as f64 / above as f64
}

#[derive(Default)]
struct History {
    seconds: f64,
    service: f64,
    count: u64,
}
#[derive(Default)]
pub struct Fairness {
    history: BTreeMap<String, History>,
    total: History,
}
impl Fairness {
    pub fn observe(&mut self, group: &str, seconds: f64, priority: i8) {
        if !seconds.is_finite() || seconds <= 0.0 {
            return;
        }
        let history = self.history.entry(group.to_owned()).or_default();
        history.seconds += seconds;
        history.service += charge_seconds(seconds, priority);
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
        self.history.get(group).map(|h| h.service).unwrap_or(0.0)
    }
    /// Each running command counts at least the average, at its own priority.
    pub fn score(
        &self,
        group: &str,
        baseline: f64,
        active: impl Iterator<Item = (f64, i8)>,
    ) -> f64 {
        let average = self.average(group);
        baseline
            + self.served(group)
            + active
                .map(|(elapsed, priority)| charge_seconds(elapsed.max(average), priority))
                .sum::<f64>()
    }
}

/// A space-shared resource, held concurrently rather than served over time,
/// goes next to the party holding the least of it. A party's own requests keep
/// their order; equal holdings go to the earliest request.
pub struct Shares<T> {
    held: BTreeMap<String, u64>,
    waiting: BTreeMap<String, BTreeSet<T>>,
}
impl<T: Ord> Shares<T> {
    pub fn new(held: BTreeMap<String, u64>) -> Self {
        Self {
            held,
            waiting: BTreeMap::new(),
        }
    }
    pub fn wait(&mut self, party: &str, request: T) {
        self.waiting
            .entry(party.to_owned())
            .or_default()
            .insert(request);
    }
    pub fn next(&mut self) -> Option<(String, T)> {
        let party = self
            .waiting
            .iter()
            .filter_map(|(party, requests)| {
                Some((
                    (
                        self.held.get(party).copied().unwrap_or(0),
                        requests.first()?,
                    ),
                    party,
                ))
            })
            .min()?
            .1
            .clone();
        let request = self.waiting.get_mut(&party)?.pop_first()?;
        Some((party, request))
    }
    pub fn grant(&mut self, party: &str, amount: u64) {
        let held = self.held.entry(party.to_owned()).or_default();
        *held = held.saturating_add(amount);
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
        f.observe("short", 1.0, 0);
        f.observe("long", 10.0, 0);
        // One completed and one running command in each queue: the long queue
        // has consumed ten times the service, even with equal command counts.
        assert_eq!(f.score("short", 0.0, [(0.0, 0)].into_iter()), 2.0);
        assert_eq!(f.score("long", 0.0, [(0.0, 0)].into_iter()), 20.0);
        for _ in 0..18 {
            f.observe("short", 1.0, 0);
        }
        assert_eq!(f.score("short", 0.0, [(0.0, 0)].into_iter()), 20.0);
        // Overrunning work immediately counts its elapsed time.
        assert_eq!(f.score("short", 0.0, [(7.0, 0)].into_iter()), 26.0);
    }
    #[test]
    fn exact_completion_replaces_the_running_estimate() {
        let mut f = Fairness::default();
        f.observe("queue", 10.0, 0);
        assert_eq!(f.score("queue", 0.0, [(0.0, 0)].into_iter()), 20.0);
        f.observe("queue", 2.0, 0);
        assert_eq!(f.average("queue"), 6.0);
        assert_eq!(f.score("queue", 0.0, [].into_iter()), 12.0);
    }
    #[test]
    fn priority_weighs_service_and_space_symmetrically() {
        let mut f = Fairness::default();
        f.observe("urgent", 11.0, 10);
        f.observe("ordinary", 1.0, 0);
        f.observe("background", 1.0, -10);
        assert_eq!(f.served("urgent"), f.served("ordinary"));
        assert_eq!(f.served("background"), 11.0);
        assert_eq!(f.average("urgent"), 11.0, "averages stay in real seconds");
        assert_eq!(f.score("urgent", 0.0, [(22.0, 10)].into_iter()), 3.0);
        assert_eq!(charge(11, 10), charge(1, 0));
        assert_eq!(charge(1, -10), charge(11, 0));
        assert_eq!(charge(5 << 40, 0), (5 << 40) * SCALE);
    }
    #[test]
    fn the_least_holding_party_goes_next_and_keeps_its_own_order() {
        let mut shares = Shares::new(BTreeMap::from([("campaign".to_owned(), 60)]));
        for order in [1, 2, 3] {
            shares.wait("campaign", order);
        }
        shares.wait("small", 5);
        shares.wait("small", 4);
        assert_eq!(shares.next(), Some(("small".to_owned(), 4)));
        shares.grant("small", 40);
        assert_eq!(shares.next(), Some(("small".to_owned(), 5)));
        shares.grant("small", 40);
        assert_eq!(shares.next(), Some(("campaign".to_owned(), 1)));
        // A request that is skipped rather than granted leaves holdings as they were.
        assert_eq!(shares.next(), Some(("campaign".to_owned(), 2)));
        shares.grant("campaign", 30);
        assert_eq!(shares.next(), Some(("campaign".to_owned(), 3)));
        assert_eq!(shares.next(), None);
        let mut tied = Shares::new(BTreeMap::new());
        tied.wait("late", 9);
        tied.wait("early", 2);
        assert_eq!(tied.next(), Some(("early".to_owned(), 2)));
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
