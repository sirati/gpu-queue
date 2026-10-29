//! Replies parked until the state a client waits for is reached. Clients send
//! one request and the actor answers it once; nobody polls.
use serde_json::Value;
use std::collections::BTreeMap;
use tokio::sync::oneshot;

pub type Reply = oneshot::Sender<Result<Value, String>>;

#[derive(Default)]
pub struct Watches {
    parked: BTreeMap<String, Vec<Reply>>,
}
impl Watches {
    pub fn park(&mut self, key: &str, reply: Reply) {
        self.parked.entry(key.to_owned()).or_default().push(reply);
    }
    pub fn waits_for(&self, key: &str) -> bool {
        self.parked.contains_key(key)
    }
    pub fn resolve(&mut self, key: &str, answer: Result<Value, String>) {
        for reply in self.parked.remove(key).unwrap_or_default() {
            let _ = reply.send(answer.clone());
        }
    }
    /// A departed client closes its receiver; its parked reply is dropped.
    pub fn prune(&mut self) {
        self.parked.retain(|_, replies| {
            replies.retain(|reply| !reply.is_closed());
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
        watches.park("group", first);
        watches.park("group", second);
        watches.park("other", other);
        drop(second_answer);
        watches.prune();
        assert_eq!(watches.len(), 2);
        watches.resolve("group", Ok(serde_json::json!({"complete":true})));
        assert_eq!(first_answer.try_recv().unwrap().unwrap()["complete"], true);
        assert!(!watches.waits_for("group"));
        assert!(other_answer.try_recv().is_err());
        assert!(watches.waits_for("other"));
    }
}
