//! One tick's admissible memory. It is computed once, kept signed so a deficit is never discarded, and every
//! admission of the tick takes from it in order: reservations, grants, compiler turns and launches.
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

/// The terms of the admissible budget: available memory, less running compiles' unused cells, the headroom and
/// granted claims' unused declarations. Every reader of the budget computes it here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Terms {
    pub available: u64,
    pub compiler_reserved: u64,
    pub minimum_ram: u64,
    pub claims_unused: u64,
}

impl Terms {
    /// The budget, kept signed.
    pub fn budget(&self) -> Budget {
        Budget::of(self.available)
            .less(self.compiler_reserved)
            .less(self.minimum_ram)
            .less(self.claims_unused)
    }
}

/// What the launches of a tick take from: the budget, and the bytes reserved for the aged head.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Purse {
    pub memory: Budget,
    pub reserved: u64,
}

impl Purse {
    /// A job's budget is the memory left plus what is reserved for it; a job that may run ahead into the
    /// reservation may use it too.
    pub fn fits(&self, cell: u64, reserved_for_it: bool) -> bool {
        match reserved_for_it {
            true => self.memory.plus(self.reserved).fits(cell),
            false => self.memory.fits(cell),
        }
    }
    /// Charges a launch its whole cell once. The aged head pays from its reservation first, then from memory;
    /// every other job pays from memory first, and a job running ahead pays the rest from the reservation.
    /// Returns what the reservation paid.
    pub fn charge(&mut self, cell: u64, head: bool) -> u64 {
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
        let terms = Terms { available: 20 * GIB, compiler_reserved: 3 * GIB, minimum_ram: 10 * GIB, claims_unused: 2 * GIB };
        assert_eq!(terms.budget(), Budget::of(5 * GIB), "(a)");
        let short = Terms { available: 12 * GIB, ..terms };
        assert_eq!(short.budget(), Budget::of(0).less(3 * GIB), "(b) the deficit is kept");
        assert_eq!(short.budget().floor(), 0, "(b) and admits nothing");
    }

    #[test]
    fn the_head_pays_from_its_reservation_and_a_job_running_ahead_pays_the_rest_from_it() {
        let mut purse = Purse { memory: Budget::of(GIB), reserved: 5 * GIB };
        assert!(purse.fits(6 * GIB, true) && !purse.fits(6 * GIB, false));
        assert_eq!(purse.charge(5 * GIB, true), 5 * GIB);
        assert_eq!(purse, Purse { memory: Budget::of(GIB), reserved: 0 }, "the head's reservation is spent");

        let mut purse = Purse { memory: Budget::of(GIB).less(2 * GIB), reserved: 5 * GIB };
        assert_eq!(purse.charge(2 * GIB, false), 2 * GIB, "nothing is left in memory");
        assert_eq!(purse, Purse { memory: Budget::of(GIB).less(2 * GIB), reserved: 3 * GIB });

        let mut purse = Purse { memory: Budget::of(GIB), reserved: 5 * GIB };
        assert_eq!(purse.charge(3 * GIB, false), 2 * GIB);
        assert_eq!(purse, Purse { memory: Budget::default(), reserved: 3 * GIB });
        assert_eq!(purse.charge(0, false), 0);
    }
}
