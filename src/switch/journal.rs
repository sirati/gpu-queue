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
        let value = json!({"time": crate::protocol::now_unix(), "event": event,
            "handle": job.handle, "side": job.side, "pid": job.pid_host,
            "state": job.state, "terminal_status": job.terminal_status, "exit_code": job.exit_code, "signal": job.signal,
            "permanent": matches!(job.state, crate::protocol::JobState::Failed | crate::protocol::JobState::Killed)
                || job.end_reason.as_ref().is_some_and(|(state, _)| matches!(state, crate::protocol::JobState::Failed | crate::protocol::JobState::Killed))
                || job.shutdown.as_ref().is_some_and(|s| s.permanent),
            "reason": job.end_reason.as_ref().map(|(_, reason)| reason).or(job.error.as_ref()),
            "shutdown": job.shutdown});
        let mut line = serde_json::to_vec(&value).map_err(|e| e.to_string())?;
        line.push(b'\n');
        let mut file = self.0.lock().unwrap_or_else(|e| e.into_inner());
        file.write_all(&line)
            .and_then(|()| file.sync_all())
            .map_err(|e| e.to_string())
    }
}
