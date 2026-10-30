//! Fairness accounts per submitting party, not per group: a campaign that splits
//! its work into many groups still receives one share. A party is the
//! submitter's OS session unless the submission names one.
use super::resources::Snapshot;
use std::collections::{BTreeMap, BTreeSet};

/// Groups admitted before parties were recorded share one party.
pub const UNATTRIBUTED: &str = "unattributed";

/// A session id cannot be recycled while the session has members; the
/// leader's birth separates later reuse.
fn name(sid: u32, leader_birth: Option<u64>) -> String {
    format!("session-{sid}-{}", leader_birth.unwrap_or(0))
}

/// The session of an authenticated submitter.
pub fn session(pid: u32) -> Option<String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let sid: u32 = stat[stat.rfind(')')? + 2..]
        .split_whitespace()
        .nth(3)?
        .parse()
        .ok()?;
    Some(name(sid, crate::procs::started(sid)))
}

/// A process belongs to the party of its nearest ancestor session, so workers
/// that start their own session still count towards the campaign that spawned
/// them. Processes outside every party share one.
pub fn attribute(sample: &Snapshot, pid: u32, parties: &BTreeSet<String>) -> String {
    let mut seen = BTreeSet::new();
    std::iter::successors(Some(pid), |pid| {
        sample.processes.get(pid).map(|&(parent, _)| parent)
    })
    .take_while(|pid| seen.insert(*pid))
    .filter_map(|pid| sample.sessions.get(&pid))
    .map(|&(sid, _)| name(sid, sample.sessions.get(&sid).map(|&(_, birth)| birth)))
    .find(|party| parties.contains(party))
    .unwrap_or_else(|| UNATTRIBUTED.to_owned())
}

/// Priorities share the GPU line's range.
pub fn bounded(priority: i64) -> Result<i8, String> {
    i8::try_from(priority)
        .ok()
        .filter(|p| (-10..=10).contains(p))
        .ok_or_else(|| "priority must be an integer between -10 and 10".into())
}
/// A submission's priority; unstated means 0.
pub fn priority(request: &serde_json::Value) -> Result<i8, String> {
    match &request["priority"] {
        serde_json::Value::Null => Ok(0),
        value => bounded(value.as_i64().ok_or("priority must be an integer")?),
    }
}

#[derive(Default)]
pub struct Parties {
    of: BTreeMap<String, (String, i8)>,
}
impl Parties {
    pub fn assign(&mut self, group: &str, party: &str, priority: i8) {
        self.of
            .insert(group.to_owned(), (party.to_owned(), priority));
    }
    pub fn of(&self, group: &str) -> &str {
        self.of.get(group).map_or(UNATTRIBUTED, |(party, _)| party)
    }
    pub fn priority(&self, group: &str) -> i8 {
        self.of.get(group).map_or(0, |&(_, priority)| priority)
    }
    pub fn forget(&mut self, group: &str) {
        self.of.remove(group);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_submitter_belongs_to_its_session_and_unknown_groups_share_one_party() {
        let own = session(std::process::id()).unwrap();
        assert!(own.starts_with("session-"));
        assert_eq!(session(std::process::id()), Some(own));
        let mut parties = Parties::default();
        parties.assign("g", "campaign", 10);
        assert_eq!(parties.of("g"), "campaign");
        assert_eq!((parties.priority("g"), parties.priority("legacy")), (10, 0));
        assert_eq!(priority(&serde_json::json!({})), Ok(0));
        assert_eq!(priority(&serde_json::json!({"priority": -10})), Ok(-10));
        for invalid in [
            serde_json::json!(11),
            serde_json::json!(2.5),
            serde_json::json!("10"),
        ] {
            assert!(priority(&serde_json::json!({ "priority": invalid })).is_err());
        }
        assert_eq!(parties.of("legacy"), UNATTRIBUTED);
        parties.forget("g");
        assert_eq!(parties.of("g"), UNATTRIBUTED);
    }
    #[test]
    fn a_worker_in_its_own_session_belongs_to_the_nearest_party_ancestor() {
        // driver 10 leads session 10; worker 21 leads session 21 under it.
        let sample = Snapshot {
            processes: BTreeMap::from([(10, (1, 0)), (20, (10, 0)), (21, (20, 0)), (30, (1, 0))]),
            sessions: BTreeMap::from([(10, (10, 7)), (20, (10, 8)), (21, (21, 9)), (30, (30, 4))]),
            ..Snapshot::default()
        };
        let parties = BTreeSet::from([name(10, Some(7))]);
        assert_eq!(attribute(&sample, 21, &parties), "session-10-7");
        assert_eq!(attribute(&sample, 10, &parties), "session-10-7");
        assert_eq!(attribute(&sample, 30, &parties), UNATTRIBUTED);
        assert_eq!(attribute(&sample, 99, &parties), UNATTRIBUTED);
        let own = session(std::process::id()).unwrap();
        let sid = own.split('-').nth(1).unwrap().parse().unwrap();
        let live = Snapshot {
            sessions: BTreeMap::from([
                (std::process::id(), (sid, 0)),
                (sid, (sid, crate::procs::started(sid).unwrap_or(0))),
            ]),
            ..Snapshot::default()
        };
        assert_eq!(
            attribute(&live, std::process::id(), &BTreeSet::from([own.clone()])),
            own,
            "sampled and requested session names agree"
        );
    }
}
