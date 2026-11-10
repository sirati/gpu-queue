//! The card and its compute processes as a viewer may see them: each process of one of the viewer's running
//! jobs, found in the job's process tree, with its memory; everything else (the desktop, another side's jobs,
//! which stay private) as one `other` amount. Telemetry that cannot be read is said so: status never fails
//! for it.
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::nvsmi::{self, Card};
use crate::procs;
use crate::protocol::{JobState, OwnJob};

/// A GPU process of one of the viewer's jobs.
#[derive(Debug, Clone, PartialEq)]
pub struct JobProcess {
    pub pid: u32,
    pub memory_mib: u64,
    pub job: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GpuView {
    pub card: Result<Card, String>,
    pub processes: Vec<JobProcess>,
    /// The card's used memory no process of the viewer's jobs holds.
    pub other_mib: u64,
}

impl GpuView {
    pub fn read(nvidia_smi: &Path, jobs: &[OwnJob]) -> GpuView {
        let read =
            nvsmi::card(nvidia_smi).and_then(|card| Ok((card, nvsmi::compute_apps(nvidia_smi)?)));
        match read {
            Ok((card, apps)) => GpuView::attributed(card, &apps, jobs, &procs::parents()),
            Err(error) => GpuView {
                card: Err(error),
                processes: Vec::new(),
                other_mib: 0,
            },
        }
    }

    pub fn attributed(
        card: Card,
        apps: &[(u32, Option<u64>)],
        jobs: &[OwnJob],
        parents: &HashMap<u32, u32>,
    ) -> GpuView {
        let trees: Vec<(u64, HashSet<u32>)> = jobs
            .iter()
            .filter(|j| matches!(j.state, JobState::Running | JobState::Stopping))
            .filter_map(|j| j.pid.map(|pid| (j.id, procs::job_tree(pid, parents))))
            .collect();
        let processes: Vec<JobProcess> = apps
            .iter()
            .filter_map(|(pid, bytes)| {
                trees
                    .iter()
                    .find(|(_, tree)| tree.contains(pid))
                    .map(|(job, _)| JobProcess {
                        pid: *pid,
                        memory_mib: bytes.unwrap_or(0) >> 20,
                        job: *job,
                    })
            })
            .collect();
        let own: u64 = processes.iter().map(|p| p.memory_mib).sum();
        GpuView {
            other_mib: card.memory_used_mib.saturating_sub(own),
            card: Ok(card),
            processes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::own;

    #[test]
    fn a_process_in_a_running_jobs_tree_is_that_jobs_and_the_rest_is_other() {
        let card =
            nvsmi::parse_card("NVIDIA GeForce RTX 5090, 598.40, 600.00, 75, 100, 18289, 32607\n")
                .unwrap();
        let mut running = own(5, JobState::Running);
        running.pid = Some(100);
        let mut done = own(6, JobState::Done);
        done.pid = Some(200);
        let parents = HashMap::from([(101, 100), (102, 101), (201, 200), (300, 1)]);
        let apps = [
            (102, Some(17958 << 20)),
            (201, Some(100 << 20)),
            (300, Some(55 << 20)),
        ];
        let view = GpuView::attributed(card, &apps, &[running, done], &parents);
        assert_eq!(
            view.processes,
            [JobProcess {
                pid: 102,
                memory_mib: 17958,
                job: 5
            }]
        );
        assert_eq!(view.other_mib, 18289 - 17958);
    }

    #[test]
    fn an_unreadable_card_line_is_an_error_not_a_panel() {
        assert!(nvsmi::parse_card("NVIDIA, [N/A], 600, 75, 100, 1, 2\n").is_err());
        assert!(nvsmi::parse_card("").is_err());
    }
}
