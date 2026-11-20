//! Durable enforcement history, independent of the bounded status snapshot.
//! The enforcement boundary appends before it notifies or signals a process.
use super::state::Job;
use serde_json::json;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

pub struct Journal(Mutex<File>);
impl Journal {
    pub fn open(directory: &Path) -> Result<Self, String> {
        std::fs::create_dir_all(directory).map_err(|e| e.to_string())?;
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(directory.join("failures.jsonl"))
            .map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        File::open(directory)
            .and_then(|d| d.sync_all())
            .map_err(|e| e.to_string())?;
        Ok(Self(Mutex::new(file)))
    }
    pub fn record(&self, event: &str, job: &Job) -> Result<(), String> {
        self.append(Self::entry(event, job))
    }
    /// The places one scheduling pass told, durable together: each with what told it, the counted entries
    /// ahead, those of its own share, N and K.
    pub fn told(&self, decisions: Vec<serde_json::Value>) -> Result<(), String> {
        self.append_all(decisions)
    }
    pub fn told_entry(job: &Job, telling: &super::preparation::Telling) -> serde_json::Value {
        let mut value = Self::entry("told", job);
        value["share"] = json!(job.share);
        value["counted_ahead"] = json!(telling.counted_ahead);
        value["counted_share_ahead"] = json!(telling.counted_share_ahead);
        value["preparation_places"] = json!(telling.places);
        value["preparation_places_per_share"] = json!(telling.per_share);
        value
    }
    fn entry(event: &str, job: &Job) -> serde_json::Value {
        json!({"time": crate::protocol::now_unix(), "event": event,
            "handle": job.handle, "side": job.side, "pid": job.pid_host,
            "state": job.state, "terminal_status": job.terminal_status, "violations": job.violations,
            "exit_code": job.exit_code, "signal": job.signal,
            "permanent": matches!(job.state, crate::protocol::JobState::Failed | crate::protocol::JobState::Killed)
                || job.end_reason.as_ref().is_some_and(|(state, _)| matches!(state, crate::protocol::JobState::Failed | crate::protocol::JobState::Killed))
                || job.shutdown.as_ref().is_some_and(|s| s.permanent),
            "reason": job.end_reason.as_ref().map(|(_, reason)| reason).or(job.error.as_ref()),
            "shutdown": job.shutdown})
    }
    /// Holds the journal's file, so a write waits until the guard drops.
    #[cfg(test)]
    pub(crate) fn hold(&self) -> std::sync::MutexGuard<'_, File> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn append(&self, value: serde_json::Value) -> Result<(), String> {
        self.append_all(vec![value])
    }
    /// One write and one sync for every value, so a batch costs one commit; an empty batch costs none.
    fn append_all(&self, values: Vec<serde_json::Value>) -> Result<(), String> {
        if values.is_empty() {
            return Ok(());
        }
        let mut line = Vec::new();
        for value in values {
            line.extend(serde_json::to_vec(&value).map_err(|e| e.to_string())?);
            line.push(b'\n');
        }
        let mut file = self.0.lock().unwrap_or_else(|e| e.into_inner());
        file.write_all(&line)
            .and_then(|()| file.sync_all())
            .map_err(|e| e.to_string())
    }
}
