//! Shared admission. Every job on the card occupies max(declared, actual).
//! Jobs we can attribute (ours, by host pid) count individually; the rest
//! (Python leases, whose pids we may not inspect) count as one group whose
//! usage is whatever the card uses beyond the attributable processes:
//!
//! available = capacity − Σ_attr max(declared_i, actual_i)
//!                      − max(Σ_unattr declared, used_total − Σ_attr actual)
//!
//! Otherwise as tools/_gpu_room.py: a job alone on the card, or one whose job
//! is already on it, always enters.

#[derive(Debug, Clone, Copy)]
pub struct Holder {
    pub declared: u64,
    /// Measured usage of the job's processes; `None`: not attributable.
    pub actual: Option<u64>,
}

pub fn available(capacity: u64, used_total: u64, others: &[Holder]) -> i128 {
    let mut attr = 0i128;
    let mut attr_actual = 0i128;
    let mut unattr = 0i128;
    for h in others {
        match h.actual {
            Some(a) => {
                attr += h.declared.max(a) as i128;
                attr_actual += a as i128;
            }
            None => unattr += h.declared as i128,
        }
    }
    capacity as i128 - attr - unattr.max(used_total as i128 - attr_actual)
}

pub fn admits(
    capacity: u64,
    used_total: u64,
    others: &[Holder],
    same_job: bool,
    need: u64,
) -> bool {
    same_job || others.is_empty() || available(capacity, used_total, others) >= need as i128
}

#[cfg(test)]
mod tests {
    use super::*;

    const G: u64 = 1 << 30;

    #[test]
    fn alone_and_same_job_enter() {
        assert!(admits(32 * G, 31 * G, &[], false, 30 * G));
        let h = [Holder {
            declared: 30 * G,
            actual: Some(30 * G),
        }];
        assert!(admits(32 * G, 31 * G, &h, true, 30 * G));
    }

    #[test]
    fn attributable_counts_max_of_declared_and_actual() {
        // Ours: declared 12, using 4. Card: 4 used. 32 - 12 - max(0, 4-4) = 20.
        let h = [Holder {
            declared: 12 * G,
            actual: Some(4 * G),
        }];
        assert_eq!(available(32 * G, 4 * G, &h), (20 * G) as i128);
        // Over its declaration: the actual counts.
        let h = [Holder {
            declared: 2 * G,
            actual: Some(6 * G),
        }];
        assert_eq!(available(32 * G, 6 * G, &h), (26 * G) as i128);
    }

    #[test]
    fn residual_usage_belongs_to_the_unattributable_group() {
        // Python declared 10, card uses 7 of which 3 are ours (declared 5).
        // Residual 4 < 10 declared: 32 - 5 - 10 = 17.
        let h = [
            Holder {
                declared: 5 * G,
                actual: Some(3 * G),
            },
            Holder {
                declared: 10 * G,
                actual: None,
            },
        ];
        assert_eq!(available(32 * G, 7 * G, &h), (17 * G) as i128);
        // Residual 14 > 10 declared (a Python job above its declaration, or
        // stray processes): 32 - 5 - 14 = 13.
        assert_eq!(available(32 * G, 17 * G, &h), (13 * G) as i128);
        assert!(admits(32 * G, 17 * G, &h, false, 13 * G));
        assert!(!admits(32 * G, 17 * G, &h, false, 13 * G + 1));
    }

    #[test]
    fn no_double_count_of_our_usage() {
        // Ours declared 6 using 2, nothing else: free 30 of 32; a 26 G request fits.
        let h = [Holder {
            declared: 6 * G,
            actual: Some(2 * G),
        }];
        assert!(admits(32 * G, 2 * G, &h, false, 26 * G));
        assert!(!admits(32 * G, 2 * G, &h, false, 26 * G + 1));
    }
}
