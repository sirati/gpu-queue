//! Replies parked until the state a client waits for is reached. Clients send
//! one request and the actor answers it once; nobody polls.
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Instant;
use tokio::sync::oneshot;

pub type Reply = oneshot::Sender<Result<Value, String>>;

#[derive(Default)]
pub struct Watches {
    parked: BTreeMap<String, Vec<(Reply, Option<Instant>)>>,
}
impl Watches {
    /// A deadline bounds how long a client that cannot wait longer is held.
    pub fn park(&mut self, key: &str, reply: Reply, deadline: Option<Instant>) {
        self.parked
            .entry(key.to_owned())
            .or_default()
            .push((reply, deadline));
    }
    pub fn waits_for(&self, key: &str) -> bool {
        self.parked.contains_key(key)
    }
    pub fn resolve(&mut self, key: &str, answer: Result<Value, String>) {
        for (reply, _) in self.parked.remove(key).unwrap_or_default() {
            let _ = reply.send(answer.clone());
        }
    }
    /// Replies whose deadline passed; their owner answers with the current state.
    pub fn expire(&mut self, now: Instant) -> Vec<(String, Reply)> {
        let mut expired = Vec::new();
        self.parked.retain(|key, replies| {
            let (due, kept) = std::mem::take(replies)
                .into_iter()
                .partition(|(_, deadline)| deadline.is_some_and(|deadline| deadline <= now));
            *replies = kept;
            expired.extend(
                due.into_iter()
                    .map(|(reply, _): (Reply, _)| (key.clone(), reply)),
            );
            !replies.is_empty()
        });
        expired
    }
    /// A departed client closes its receiver; its parked reply is dropped.
    pub fn prune(&mut self) {
        self.parked.retain(|_, replies| {
            replies.retain(|(reply, _)| !reply.is_closed());
            !replies.is_empty()
        });
    }
    pub fn len(&self) -> usize {
        self.parked.values().map(Vec::len).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_parked_reply_receives_the_answer_once_and_departures_are_pruned() {
        let mut watches = Watches::default();
        let (first, mut first_answer) = oneshot::channel();
        let (second, second_answer) = oneshot::channel();
        let (other, mut other_answer) = oneshot::channel();
        watches.park("group", first, None);
        watches.park("group", second, None);
        watches.park("other", other, None);
        drop(second_answer);
        watches.prune();
        assert_eq!(watches.len(), 2);
        watches.resolve("group", Ok(serde_json::json!({"complete":true})));
        assert_eq!(first_answer.try_recv().unwrap().unwrap()["complete"], true);
        assert!(!watches.waits_for("group"));
        assert!(other_answer.try_recv().is_err());
        assert!(watches.waits_for("other"));
    }
    #[test]
    fn only_replies_past_their_deadline_expire() {
        let mut watches = Watches::default();
        let now = Instant::now();
        let (held, _held_answer) = oneshot::channel();
        let (due, _due_answer) = oneshot::channel();
        let (unbounded, _unbounded_answer) = oneshot::channel();
        watches.park(
            "claim",
            held,
            Some(now + std::time::Duration::from_secs(20)),
        );
        watches.park("claim", due, Some(now));
        watches.park("other", unbounded, None);
        let expired = watches.expire(now);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, "claim");
        assert_eq!(watches.len(), 2);
        assert!(watches.expire(now).is_empty());
    }
}
