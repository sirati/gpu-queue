//! Fairness accounts per submitting party, not per group: a campaign that splits
//! its work into many groups still receives one share. A party is the
//! submitter's OS session unless the submission names one.
use std::collections::BTreeMap;

/// Groups admitted before parties were recorded share one party.
pub const UNATTRIBUTED: &str = "unattributed";

/// The session of an authenticated submitter. A session id cannot be recycled
/// while the session has members; the leader's birth separates later reuse.
pub fn session(pid: u32) -> Option<String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let sid: u32 = stat[stat.rfind(')')? + 2..]
        .split_whitespace()
        .nth(3)?
        .parse()
        .ok()?;
    Some(format!(
        "session-{sid}-{}",
        crate::procs::started(sid).unwrap_or(0)
    ))
}

#[derive(Default)]
pub struct Parties {
    of: BTreeMap<String, String>,
}
impl Parties {
    pub fn assign(&mut self, group: &str, party: &str) {
        self.of.insert(group.to_owned(), party.to_owned());
    }
    pub fn of(&self, group: &str) -> &str {
        self.of.get(group).map_or(UNATTRIBUTED, String::as_str)
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
        parties.assign("g", "campaign");
        assert_eq!(parties.of("g"), "campaign");
        assert_eq!(parties.of("legacy"), UNATTRIBUTED);
        parties.forget("g");
        assert_eq!(parties.of("g"), UNATTRIBUTED);
    }
}
