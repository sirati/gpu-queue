//! One tick's admissible memory. It is computed once, kept signed so a deficit is never discarded, and every
//! admission of the tick takes from it in order: reservations, grants, compiler turns and launches. The overcommit
//! budget B lends part of the floor to attempts admitted at their expected memory; what they hold of it is added back.
use super::model::Cell;
use serde::{Deserialize, Serialize};

/// Bytes that may still be admitted this tick; negative while more is held than is available.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Budget(i64);

fn signed(bytes: u64) -> i64 {
    i64::try_from(bytes).unwrap_or(i64::MAX)
}

impl Budget {
    pub fn of(bytes: u64) -> Self {
        Self(signed(bytes))
    }
    pub fn less(self, bytes: u64) -> Self {
        Self(self.0.saturating_sub(signed(bytes)))
    }
    pub fn plus(self, bytes: u64) -> Self {
        Self(self.0.saturating_add(signed(bytes)))
    }
    /// A request is admitted only from a positive budget, and only whole.
    pub fn fits(self, bytes: u64) -> bool {
        signed(bytes) <= self.0
    }
    /// What may still be admitted, floored at nothing.
    pub fn floor(self) -> u64 {
        u64::try_from(self.0).unwrap_or(0)
    }
}

/// What one attempt holds: of the budget, and of the overcommit budget. Its cap is the sum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Charge {
    pub ordinary: u64,
    pub lending: u64,
}

impl Charge {
    /// A whole cell: all of it ordinary.
    pub fn whole(cap: u64) -> Self {
        Self { ordinary: cap, lending: 0 }
    }
    /// A running attempt's holding: its cap, of which `lending` is lent; `lending` is at most `cap`.
    pub fn held(cap: u64, lending: u64) -> Self {
        Self { ordinary: cap - lending, lending }
    }
    pub fn cap(&self) -> u64 {
        self.ordinary + self.lending
    }
    /// Funds a raise of `bytes` from this released holding: from its ordinary reservation first, then from its
    /// lending. Returns the part of the raise that moves as lending; the rest of the raise moves as ordinary. What
    /// the holding keeps afterwards returns to its own budget.
    pub fn fund(&mut self, bytes: u64) -> u64 {
        let from_ordinary = bytes.min(self.ordinary);
        let moved = (bytes - from_ordinary).min(self.lending);
        self.ordinary -= from_ordinary;
        self.lending -= moved;
        moved
    }
}

/// The overcommit budget an instance lends, and the lending its running attempts hold.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Lending {
    /// B.
    pub lent: u64,
    /// Σ lending over the running attempts.
    pub held: u64,
}

impl Lending {
    /// What the budget adds back: the lending, at most B.
    pub fn added_back(&self) -> u64 {
        self.held.min(self.lent)
    }
    /// What new launches may still lend.
    pub fn lendable(&self) -> u64 {
        self.lent.saturating_sub(self.held)
    }
    /// Lending above B, charged as ordinary until its attempts end.
    pub fn excess(&self) -> u64 {
        self.held.saturating_sub(self.lent)
    }
    /// The effective bound, as restore states it.
    pub fn describe(&self) -> String {
        format!(
            "the overcommit budget is {} bytes; running attempts hold {} bytes of lending, {} bytes added back, {} bytes charged as ordinary until their attempts end",
            self.lent,
            self.held,
            self.added_back(),
            self.excess()
        )
    }
}

/// The terms of the admissible budget: available memory, less running compiles' unused cells, the headroom and
/// granted claims' unused declarations, plus the lending running attempts hold, at most B. Every reader of the
/// budget computes it here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Terms {
    pub available: u64,
    pub compiler_reserved: u64,
    pub minimum_ram: u64,
    pub claims_unused: u64,
    pub lending: Lending,
}

impl Terms {
    /// The budget, kept signed.
    pub fn budget(&self) -> Budget {
        Budget::of(self.available)
            .less(self.compiler_reserved)
            .less(self.minimum_ram)
            .less(self.claims_unused)
            .plus(self.lending.added_back())
    }
}

/// What the launches of a tick take from: the budget, the bytes reserved for the aged head, and what the overcommit
/// budget may still lend.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Purse {
    pub memory: Budget,
    pub reserved: u64,
    /// What the overcommit budget may still lend this tick.
    pub lendable: u64,
}

impl Purse {
    /// The charge a launch of `cell` takes: its expected figure from the budget and the rest as lending when the
    /// lendable remainder holds the rest and the budget holds the expected figure; otherwise its whole cell from the
    /// budget; none when neither fits. A job's budget is the memory left plus what is reserved for it; a job that
    /// may run ahead into the reservation may use it for either.
    pub fn admits(&self, cell: Cell, reserved_for_it: bool) -> Option<Charge> {
        let budget = match reserved_for_it {
            true => self.memory.plus(self.reserved),
            false => self.memory,
        };
        [Charge { ordinary: cell.expected, lending: cell.rest() }, Charge::whole(cell.limit)]
            .into_iter()
            .find(|charge| charge.lending <= self.lendable && budget.fits(charge.ordinary))
    }
    /// Takes `charge` once: its ordinary part as a whole cell is taken, its lending from `lendable`. The aged head
    /// pays from its reservation first, then from memory; every other job pays from memory first, and a job running
    /// ahead pays the rest from the reservation. Returns what the reservation paid.
    pub fn charge(&mut self, charge: Charge, head: bool) -> u64 {
        let cell = charge.ordinary;
        self.lendable = self.lendable.saturating_sub(charge.lending);
        let from_reserved = match head {
            true => cell.min(self.reserved),
            false => cell.saturating_sub(self.memory.floor()).min(self.reserved),
        };
        self.memory = self.memory.less(cell - from_reserved);
        self.reserved = match head {
            true => 0,
            false => self.reserved - from_reserved,
        };
        from_reserved
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const GIB: u64 = 1 << 30;

    #[test]
    fn a_deficit_is_kept_and_nothing_is_admitted_from_it() {
        let budget = Budget::of(6 * GIB).less(2 * GIB).less(8 * GIB);
        assert_eq!(budget, Budget::of(0).less(4 * GIB));
        assert!(!budget.fits(0) && !budget.fits(1));
        assert_eq!(budget.floor(), 0);
        assert_eq!(budget.plus(10 * GIB).floor(), 6 * GIB, "ending 10 GiB of declared work leaves 6 GiB, not 10");
        assert!(Budget::of(GIB).fits(GIB) && !Budget::of(GIB).fits(GIB + 1));
        assert_eq!(Budget::of(u64::MAX).less(u64::MAX), Budget::default());
    }

    #[test]
    fn the_budget_terms_have_one_formula() {
        let terms = Terms { available: 20 * GIB, compiler_reserved: 3 * GIB, minimum_ram: 10 * GIB, claims_unused: 2 * GIB, lending: Lending::default() };
        assert_eq!(terms.budget(), Budget::of(5 * GIB), "(a)");
        let short = Terms { available: 12 * GIB, ..terms };
        assert_eq!(short.budget(), Budget::of(0).less(3 * GIB), "(b) the deficit is kept");
        assert_eq!(short.budget().floor(), 0, "(b) and admits nothing");
    }

    #[test]
    fn the_head_pays_from_its_reservation_and_a_job_running_ahead_pays_the_rest_from_it() {
        let mut purse = Purse { memory: Budget::of(GIB), reserved: 5 * GIB, lendable: 0 };
        assert!(purse.admits(Cell::whole(6 * GIB), true).is_some() && purse.admits(Cell::whole(6 * GIB), false).is_none());
        assert_eq!(purse.charge(Charge::whole(5 * GIB), true), 5 * GIB);
        assert_eq!(purse, Purse { memory: Budget::of(GIB), reserved: 0, lendable: 0 }, "the head's reservation is spent");

        let mut purse = Purse { memory: Budget::of(GIB).less(2 * GIB), reserved: 5 * GIB, lendable: 0 };
        assert_eq!(purse.charge(Charge::whole(2 * GIB), false), 2 * GIB, "nothing is left in memory");
        assert_eq!(purse, Purse { memory: Budget::of(GIB).less(2 * GIB), reserved: 3 * GIB, lendable: 0 });

        let mut purse = Purse { memory: Budget::of(GIB), reserved: 5 * GIB, lendable: 0 };
        assert_eq!(purse.charge(Charge::whole(3 * GIB), false), 2 * GIB);
        assert_eq!(purse, Purse { memory: Budget::default(), reserved: 3 * GIB, lendable: 0 });
        assert_eq!(purse.charge(Charge::whole(0), false), 0);
    }

    const MIB: u64 = 1 << 20;
    fn cell(limit: u64, expected: u64) -> Cell {
        Cell { limit, expected }
    }
    fn charge(ordinary: u64, lending: u64) -> Charge {
        Charge { ordinary, lending }
    }

    /// Q-B1.
    #[test]
    fn the_lending_path_is_taken_whenever_it_fits() {
        let mut purse = Purse { memory: Budget::of(8 * GIB), reserved: 0, lendable: 3 * GIB };
        let admitted = purse.admits(cell(4 * GIB, GIB), false);
        assert_eq!(admitted, Some(charge(GIB, 3 * GIB)), "the lending path, although the whole cell would fit");
        purse.charge(admitted.unwrap(), false);
        assert_eq!(purse, Purse { memory: Budget::of(7 * GIB), reserved: 0, lendable: 0 });
    }

    /// Q-B2.
    #[test]
    fn the_whole_cell_when_the_lendable_remainder_cannot_hold_the_rest() {
        let purse = Purse { memory: Budget::of(8 * GIB), reserved: 0, lendable: 2 * GIB };
        assert_eq!(purse.admits(cell(4 * GIB, GIB), false), Some(Charge::whole(4 * GIB)), "(a)");
        let purse = Purse { memory: Budget::of(3 * GIB), reserved: 0, lendable: 2 * GIB };
        assert_eq!(purse.admits(cell(4 * GIB, GIB), false), None, "(b)");
    }

    /// Q-B3.
    #[test]
    fn a_one_figure_cell_is_charged_as_today() {
        for lendable in [0, 3 * GIB, 8 * GIB] {
            let purse = Purse { memory: Budget::of(10 * GIB), reserved: 0, lendable };
            assert_eq!(purse.admits(Cell::whole(4 * GIB), false), Some(Charge::whole(4 * GIB)), "remainder {lendable}");
        }
    }

    /// Q-B4.
    #[test]
    fn the_head_and_a_run_ahead_pay_their_ordinary_part_from_the_reservation() {
        let mut head = Purse { memory: Budget::of(0), reserved: 4 * GIB, lendable: 3 * GIB };
        let admitted = head.admits(cell(4 * GIB, GIB), true);
        assert_eq!(admitted, Some(charge(GIB, 3 * GIB)), "the head");
        assert_eq!(head.charge(admitted.unwrap(), true), GIB);
        assert_eq!(head, Purse { memory: Budget::of(0), reserved: 0, lendable: 0 }, "the head's purse");
        let mut ahead = Purse { memory: Budget::of(512 * MIB), reserved: 4 * GIB, lendable: 3 * GIB };
        assert_eq!(ahead.charge(charge(GIB, 3 * GIB), false), 512 * MIB, "a run-ahead");
        assert_eq!(ahead, Purse { memory: Budget::of(0), reserved: 3 * GIB + 512 * MIB, lendable: 0 }, "the run-ahead's purse");
    }

    /// Q-B5.
    #[test]
    fn the_remainder_falls_within_a_tick() {
        let mut purse = Purse { memory: Budget::of(100 * GIB), reserved: 0, lendable: 3 * GIB };
        let first = purse.admits(cell(4 * GIB, GIB), false).unwrap();
        purse.charge(first, false);
        let second = purse.admits(cell(4 * GIB, GIB), false).unwrap();
        purse.charge(second, false);
        assert_eq!((first, second), (charge(GIB, 3 * GIB), Charge::whole(4 * GIB)));
        assert_eq!(first.lending + second.lending, 3 * GIB, "Σ lending");
    }

    /// Q-B6.
    #[test]
    fn the_add_back_is_the_lending_at_most_b() {
        let terms = |available: u64, compiler_reserved: u64, lent: u64, held: u64| {
            Terms { available, compiler_reserved, minimum_ram: 10 * GIB, claims_unused: 0, lending: Lending { lent, held } }
        };
        assert_eq!(terms(20 * GIB, 4 * GIB, 3 * GIB, 3 * GIB).budget(), Budget::of(9 * GIB), "(a)");
        assert_eq!(terms(16 * GIB, 0, 3 * GIB, 3 * GIB).budget(), Budget::of(9 * GIB), "(b) the attempt at its cap");
        assert_eq!(terms(20 * GIB, 4 * GIB, 3 * GIB, 5 * GIB).budget(), Budget::of(9 * GIB), "(c) the excess is charged");
        assert_eq!(terms(20 * GIB, 4 * GIB, 3 * GIB, GIB).budget(), Budget::of(7 * GIB), "(d)");
    }

    /// Q-B7.
    #[test]
    fn a_released_holding_funds_raises_ordinary_first() {
        let mut released = Charge::held(64 * MIB, 48 * MIB);
        assert_eq!(released.fund(32 * MIB), 16 * MIB, "(a) the first raise");
        assert_eq!(released.fund(32 * MIB), 32 * MIB, "(a) the second raise");
        assert_eq!(released, charge(0, 0), "(a)");
        let mut released = Charge::held(4 * GIB, 3 * GIB);
        assert_eq!(released.fund(512 * MIB), 0, "(b)");
        assert_eq!(released, charge(512 * MIB, 3 * GIB), "(b)");
        // (c) Every holding of at most 16 units of 64 MiB, and every sequence of raises whose sum is at most its cap.
        const UNIT: u64 = 64 * MIB;
        fn every(holding: Charge, cap: u64, lent: u64, raised: u64, moved: u64) -> u64 {
            let mut prefixes = 1;
            for raise in (1..=(cap - raised) / UNIT).map(|units| units * UNIT) {
                let mut after = holding;
                let returned = after.fund(raise);
                assert!(returned <= raise, "{holding:?}: a raise of {raise} moves {returned} as lending");
                assert!(moved + returned <= lent, "{holding:?}: the raises move more lending than the {lent} it held");
                assert_eq!(after.cap() + raised + raise, cap, "{holding:?}: what remains plus the raises is not its cap");
                prefixes += every(after, cap, lent, raised + raise, moved + returned);
            }
            prefixes
        }
        let mut prefixes = 0;
        for cap in (0..=16).map(|units| units * UNIT) {
            for lending in (0..=cap / UNIT).map(|units| units * UNIT) {
                prefixes += every(Charge::held(cap, lending), cap, lending, 0, 0);
            }
        }
        assert_eq!(prefixes, 2_097_153, "(c) every prefix of every sequence, the empty one included");
    }
}
