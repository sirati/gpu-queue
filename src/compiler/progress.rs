//! Remaining work per compiler group, so scheduling never rescans the catalogue.
use super::model::Group;
use std::collections::{BTreeMap, BTreeSet};

struct Remaining {
    pending: BTreeSet<usize>,
    open: BTreeSet<usize>,
}
#[derive(Default)]
pub struct Progress {
    jobs: BTreeMap<String, (String, usize)>,
    groups: BTreeMap<String, Remaining>,
}
impl Progress {
    pub fn owner(&self, job: &str) -> Option<&str> {
        self.jobs.get(job).map(|(group, _)| group.as_str())
    }
    pub fn admit(&mut self, group: &Group, terminal: impl Fn(&str) -> bool) {
        let open: BTreeSet<usize> = group
            .jobs
            .iter()
            .enumerate()
            .filter(|(_, job)| !terminal(&job.job_id))
            .map(|(index, _)| index)
            .collect();
        for (index, job) in group.jobs.iter().enumerate() {
            self.jobs
                .insert(job.job_id.clone(), (group.group_id.clone(), index));
        }
        self.groups.insert(
            group.group_id.clone(),
            Remaining {
                pending: open.clone(),
                open,
            },
        );
    }
    /// A launched job is no longer pending but stays open until it settles.
    pub fn start(&mut self, job: &str) {
        if let Some((group, index)) = self.jobs.get(job) {
            if let Some(remaining) = self.groups.get_mut(group) {
                remaining.pending.remove(index);
            }
        }
    }
    /// A job whose attempt runs again is pending once more.
    pub fn requeue(&mut self, job: &str) {
        if let Some((group, index)) = self.jobs.get(job) {
            if let Some(remaining) = self.groups.get_mut(group) {
                remaining.pending.insert(*index);
            }
        }
    }
    /// Returns the group whose last open job just settled.
    pub fn settle(&mut self, job: &str) -> Option<String> {
        let (group, index) = self.jobs.get(job)?;
        let remaining = self.groups.get_mut(group)?;
        remaining.pending.remove(index);
        (remaining.open.remove(index) && remaining.open.is_empty()).then(|| group.clone())
    }
    pub fn pending(&self, group: &str) -> impl Iterator<Item = usize> + '_ {
        self.groups
            .get(group)
            .into_iter()
            .flat_map(|remaining| remaining.pending.iter().copied())
    }
    pub fn complete(&self, group: &str) -> bool {
        self.groups
            .get(group)
            .is_some_and(|remaining| remaining.open.is_empty())
    }
    pub fn active(&self) -> impl Iterator<Item = &str> {
        self.groups
            .iter()
            .filter(|(_, remaining)| !remaining.open.is_empty())
            .map(|(id, _)| id.as_str())
    }
    /// A retired group releases its job identities.
    pub fn forget(&mut self, group: &Group) {
        for job in &group.jobs {
            self.jobs.remove(&job.job_id);
        }
        self.groups.remove(&group.group_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn group(id: &str, jobs: &[&str]) -> Group {
        serde_json::from_value(serde_json::json!({"group_id":id,"jobs":jobs.iter()
            .map(|job| serde_json::json!({"job_id":job,"argv":["true"],"cwd":"/"})).collect::<Vec<_>>()}))
        .unwrap()
    }
    #[test]
    fn settling_the_last_open_job_completes_its_group_exactly_once() {
        let mut progress = Progress::default();
        progress.admit(&group("g", &["a", "b", "c"]), |job| job == "a");
        assert_eq!(progress.owner("b"), Some("g"));
        assert_eq!(progress.pending("g").collect::<Vec<_>>(), [1, 2]);
        progress.start("b");
        assert_eq!(progress.pending("g").collect::<Vec<_>>(), [2]);
        assert!(!progress.complete("g"));
        assert_eq!(progress.settle("c"), None);
        assert_eq!(progress.settle("c"), None);
        assert_eq!(progress.active().collect::<Vec<_>>(), ["g"]);
        assert_eq!(progress.settle("b"), Some("g".into()));
        assert_eq!(progress.settle("b"), None);
        assert!(progress.complete("g"));
        assert_eq!(progress.active().count(), 0);
    }
    #[test]
    fn a_forgotten_group_releases_its_identities() {
        let mut progress = Progress::default();
        let retired = group("g", &["a", "b"]);
        progress.admit(&retired, |_| true);
        progress.forget(&retired);
        assert_eq!(progress.owner("a"), None);
        assert!(!progress.complete("g"));
        progress.admit(&group("again", &["a"]), |_| false);
        assert_eq!(progress.owner("a"), Some("again"));
    }
    #[test]
    fn a_group_admitted_with_every_job_terminal_is_already_complete() {
        let mut progress = Progress::default();
        assert_eq!(progress.settle("unknown"), None);
        progress.admit(&group("done", &["x"]), |_| true);
        assert!(progress.complete("done"));
        assert_eq!(progress.pending("done").count(), 0);
    }
}
