//! The fake card of the tests: one file per fake GPU process in `$NINFER_MOCK_GPU_DIR`, written by
//! `ninfer-mock` and read by the mock NVML library, and the card's total memory in its `total` file. Both include
//! this file, and each uses its own half of it.
#![allow(dead_code)]
use std::path::PathBuf;

pub fn gpu_dir() -> Option<PathBuf> {
    std::env::var_os("NINFER_MOCK_GPU_DIR").map(PathBuf::from)
}

/// One fake GPU process: `mem=<MiB> sm=<pct> type=<C|G|C+G>` in
/// `$NINFER_MOCK_GPU_DIR/<pid>` (see `registration`); an empty file is `mem=0 sm=0 type=C`.
#[derive(Clone)]
pub struct GpuProc {
    pub mem: u64,
    pub sm: u32,
    pub kind: String,
}

impl GpuProc {
    pub fn parse(text: &str) -> GpuProc {
        let mut p = GpuProc {
            mem: 0,
            sm: 0,
            kind: "C".into(),
        };
        for kv in text.split_whitespace() {
            match kv.split_once('=') {
                Some(("mem", v)) => p.mem = v.parse().unwrap_or(0),
                Some(("sm", v)) => p.sm = v.parse().unwrap_or(0),
                Some(("type", v)) => p.kind = v.into(),
                _ => {}
            }
        }
        p
    }
    pub fn line(&self) -> String {
        format!("mem={} sm={} type={}", self.mem, self.sm, self.kind)
    }
}

/// The file of a process that registers itself: its use, and its start time (`birth=<ticks>`), which tells it apart
/// from a later process given its pid. A file a test writes for some pid carries none.
pub fn registration(usage: &GpuProc, birth: u64) -> String {
    format!("{} birth={birth}", usage.line())
}

/// The start time of the process that registered itself in a file's `text`.
pub fn registered_birth(text: &str) -> Option<u64> {
    text.split_whitespace()
        .find_map(|kv| kv.strip_prefix("birth="))?
        .parse()
        .ok()
}

/// Live fake processes, `total` MiB (default 32768).
pub fn gpu_state() -> (Vec<(u32, GpuProc)>, u64) {
    let Some(dir) = gpu_dir() else {
        return (Vec::new(), 32768);
    };
    let total = std::fs::read_to_string(dir.join("total"))
        .ok()
        .and_then(|t| t.trim().parse().ok())
        .unwrap_or(32768);
    let mut procs = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            let Ok(pid) = name.parse::<u32>() else {
                continue;
            };
            let alive = std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .ok()
                .and_then(|s| s.get(s.rfind(')')? + 2..).map(|r| !r.starts_with('Z')))
                .unwrap_or(false);
            if alive {
                let text = std::fs::read_to_string(e.path()).unwrap_or_default();
                procs.push((pid, GpuProc::parse(&text)));
            } else {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }
    procs.sort_by_key(|(p, _)| *p);
    (procs, total)
}
