//! A compilation killed for outgrowing its memory cell runs again, and its
//! subqueue learns from the kill: running siblings share the memory it held,
//! each gaining at most one step, and every compilation not yet started,
//! itself included, gains one step.
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const STEP: u64 = 512 << 20;

/// Raised limits by job; job identities are unique across subqueues.
#[derive(Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Growth(BTreeMap<String, u64>);

impl Growth {
    pub fn limit(&self, job: &str, declared: u64) -> u64 {
        self.0.get(job).copied().unwrap_or(declared).max(declared)
    }
    /// `freed` is the killed attempt's limit; both lists carry current limits.
    /// Returns the running siblings' new limits.
    pub fn out_of_memory(
        &mut self,
        freed: u64,
        running: &[(String, u64)],
        waiting: &[(String, u64)],
    ) -> Vec<(String, u64)> {
        let share = (freed / running.len().max(1) as u64).min(STEP);
        let raised: Vec<_> = running
            .iter()
            .map(|(job, limit)| (job.clone(), limit.saturating_add(share)))
            .collect();
        for (job, limit) in raised.iter().cloned().chain(
            waiting
                .iter()
                .map(|(job, limit)| (job.clone(), limit.saturating_add(STEP))),
        ) {
            self.0.insert(job, limit);
        }
        raised
    }
    pub fn forget<'a>(&mut self, jobs: impl IntoIterator<Item = &'a str>) {
        for job in jobs {
            self.0.remove(job);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const GIB: u64 = 1 << 30;
    #[test]
    fn a_kill_spreads_its_memory_over_running_siblings_and_steps_the_waiting() {
        let mut growth = Growth::default();
        let owned = |jobs: &[(&str, u64)]| -> Vec<(String, u64)> {
            jobs.iter().map(|&(job, limit)| (job.to_owned(), limit)).collect()
        };
        let raised = growth.out_of_memory(
            GIB,
            &owned(&[("a", 4 * GIB), ("b", 4 * GIB), ("c", 4 * GIB), ("d", 4 * GIB)]),
            &owned(&[("killed", 4 * GIB), ("next", 2 * GIB)]),
        );
        assert_eq!(raised, owned(&[("a", 4 * GIB + GIB / 4), ("b", 4 * GIB + GIB / 4), ("c", 4 * GIB + GIB / 4), ("d", 4 * GIB + GIB / 4)]));
        assert_eq!(growth.limit("killed", 4 * GIB), 4 * GIB + STEP);
        assert_eq!(growth.limit("next", 2 * GIB), 2 * GIB + STEP);
        assert_eq!(growth.limit("next", 8 * GIB), 8 * GIB, "a raise never lowers a declaration");
        assert_eq!(growth.limit("unrelated", GIB), GIB);
        let capped = growth.out_of_memory(8 * GIB, &owned(&[("a", 4 * GIB)]), &[]);
        assert_eq!(capped, owned(&[("a", 4 * GIB + STEP)]), "a sibling gains at most one step");
        growth.forget(["a", "killed"]);
        assert_eq!(growth.limit("killed", 4 * GIB), 4 * GIB);
        assert_eq!(growth.limit("next", 2 * GIB), 2 * GIB + STEP);
    }
}
