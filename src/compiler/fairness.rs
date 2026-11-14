//! Compile slots and RAM are held concurrently, so both go by share: the next
//! one goes to the party holding the least, and equal holdings rotate by turn.
//! What a party was served before never counts, so a long-running large
//! campaign keeps its share while a new small one takes its turn.
//! Priority weighs a party's share: holdings are charged at their amount divided
//! by the weight 1+p, or multiplied by 1-p below zero, so a priority-10 party
//! holds eleven times the share of a priority-0 one and nothing starves.
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

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
/// When a party last took a slot, in Unix seconds; one that never did is oldest.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LastTurn(pub f64);
impl Eq for LastTurn {}
impl Ord for LastTurn {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}
impl PartialOrd for LastTurn {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
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
    fn priority_weighs_holdings_symmetrically() {
        assert_eq!(charge(11, 10), charge(1, 0));
        assert_eq!(charge(1, -10), charge(11, 0));
        assert_eq!(charge(5 << 40, 0), (5 << 40) * SCALE);
    }
    #[test]
    fn equal_slot_holdings_go_to_the_oldest_turn_whatever_was_served_before() {
        let mut slots = Shares::new(BTreeMap::from([
            ("campaign".to_owned(), charge(1, 0)),
            ("recent".to_owned(), charge(1, 0)),
        ]));
        slots.wait("recent", (LastTurn(200.0), 0));
        slots.wait("campaign", (LastTurn(100.0), 1));
        slots.wait("new", (LastTurn(0.0), 2));
        assert_eq!(slots.next(), Some(("new".to_owned(), (LastTurn(0.0), 2))));
        assert_eq!(
            slots.next(),
            Some(("campaign".to_owned(), (LastTurn(100.0), 1))),
            "with equal holdings the longer-waiting turn goes first"
        );
        assert!(LastTurn(-0.0) < LastTurn(0.0) && LastTurn(1.0) < LastTurn(f64::INFINITY));
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
