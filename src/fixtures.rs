//! Status data for tests: one own job, and a status around given jobs.
use crate::protocol::{
    Category as Cat, Counts, JobState, Mode, OtherJob, OwnJob, ServeStatus, Status,
};

pub fn own(id: u64, state: JobState) -> OwnJob {
    OwnJob {
        vram_retention_deadline: None,
        watchdog: Default::default(),
        terminal_status: None,
        violations: Vec::new(),
        timing: None,
        expected_runtime_secs: None,
        id,
        handle: format!("s{id}"),
        state,
        mode: Mode::Exclusive,
        vram: 0,
        category: Cat::Other,
        cooperative: false,
        priority: 0,
        position: None,
        behind_ninfer: false,
        cmd: vec!["true".into()],
        label: None,
        lease: false,
        cwd: "/".into(),
        log: "/l".into(),
        exit_code: None,
        signal: None,
        error: None,
        runner_pid: 1,
        pid: None,
        submitted_at: 0.0,
        started_at: None,
        finished_at: None,
        runs: 0,
    }
}

pub fn status(jobs: Vec<OwnJob>, other_jobs: Vec<OtherJob>) -> Status {
    Status {
        summary: String::new(),
        serve: ServeStatus {
            phase: "OFF".into(),
            profile_current: None,
            profile_pending: None,
        },
        serve_wanted: false,
        owner: "queue".into(),
        profiles: vec!["alpha".into(), "beta".into()],
        host_cache_current_mib: None,
        host_cache_pending_mib: None,
        gpu_processes: 0,
        own: Counts::default(),
        others: Counts::default(),
        line: Vec::new(),
        jobs,
        other_jobs,
        ninfer_error: None,
    }
}
