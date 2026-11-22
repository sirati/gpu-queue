use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

fn yes() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub job_id: String,
    pub argv: Vec<String>,
    pub cwd: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Retained only so persisted declarations deserialize; no compilation ever
    /// ends because time elapsed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<f64>,
    #[serde(default)]
    pub memory_bytes: u64,
    /// The memory the submitter expects the job to use, at most its cell (`memory_bytes`). A job that declares it
    /// declares its cell.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_memory_bytes: Option<u64>,
    /// How long one run takes; required of every submission. Groups persisted
    /// before it was required restore without one and run uncontracted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_runtime_secs: Option<u64>,
    /// Using more is an error: the compilation ends without a retry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_memory_bytes: Option<u64>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(default = "yes")]
    pub inherit_environment: bool,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Group {
    pub group_id: String,
    pub jobs: Vec<Job>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub max_running: usize,
    /// A directory the submitter owns: the group's terminal results are written to
    /// `<sink>/<group_id>.results.json` at completion, so they outlive the group's retention.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_sink: Option<std::path::PathBuf>,
}

/// What a host memory claim holds its bytes for. GPU-place claims are ordinary (`Class::of`); host-only claims count
/// as work that ends in what the service's own work can free.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Purpose {
    /// Held for work next on the GPU.
    GpuPlace,
    /// Held only for host work, which ends.
    HostOnly,
}

/// The memory of a job's attempt: its cell's limit, and the part of it the job is expected to use, at most the limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cell {
    pub limit: u64,
    pub expected: u64,
}

impl Cell {
    /// A cell the job is expected to use whole.
    pub fn whole(limit: u64) -> Self {
        Self { limit, expected: limit }
    }
    /// The part of the cell the job is not expected to use.
    pub fn rest(&self) -> u64 {
        self.limit - self.expected
    }
    /// The cell at most `ceiling`, its expected part at most its limit.
    pub fn capped(self, ceiling: Option<u64>) -> Self {
        let limit = self.limit.min(ceiling.unwrap_or(u64::MAX));
        Self { limit, expected: self.expected.min(limit) }
    }
}

impl Job {
    /// The cell its declarations give before any growth: `memory_bytes`, or `average` when it declares none, expected
    /// at `expected_memory_bytes`, else whole.
    pub fn cell(&self, average: u64) -> Cell {
        let limit = if self.memory_bytes == 0 {
            average
        } else {
            self.memory_bytes
        };
        Cell { limit, expected: self.expected_memory_bytes.unwrap_or(limit).min(limit) }
    }
}

pub fn identifier(id: &str) -> Result<(), String> {
    if id.is_empty()
        || id.len() > 160
        || !id.as_bytes()[0].is_ascii_alphanumeric()
        || !id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
    {
        return Err("invalid queue identity".into());
    }
    Ok(())
}
impl Group {
    pub fn validate(&self) -> Result<(), String> {
        identifier(&self.group_id)?;
        if self.result_sink.as_ref().is_some_and(|sink| !sink.is_absolute()) {
            return Err("a compile group's result sink must be an absolute directory".into());
        }
        let mut ids = BTreeSet::new();
        for job in &self.jobs {
            identifier(&job.job_id)?;
            if !ids.insert(job.job_id.as_str()) {
                return Err("duplicate job identity".into());
            }
            if !std::path::Path::new(&job.cwd).is_absolute()
                || job.cwd.contains('\0')
                || job.argv.is_empty()
                || job.argv.iter().any(|s| s.contains('\0'))
                || job.argv[0].is_empty()
                || job
                    .timeout_seconds
                    .is_some_and(|seconds| !seconds.is_finite() || seconds <= 0.0)
                || job.expected_runtime_secs == Some(0)
                || job
                    .error_memory_bytes
                    .is_some_and(|limit| limit < job.memory_bytes.max(1))
                || job
                    .env
                    .iter()
                    .any(|(k, v)| k.is_empty() || k.contains(['=', '\0']) || v.contains('\0'))
            {
                return Err("invalid compiler command/environment".into());
            }
            if job
                .expected_memory_bytes
                .is_some_and(|expected| expected == 0 || job.memory_bytes == 0 || expected > job.memory_bytes)
            {
                return Err("a job that declares its expected memory declares its cell, and expects more than nothing and at most its cell".into());
            }
        }
        for job in &self.jobs {
            let mut deps = BTreeSet::new();
            for dependency in &job.dependencies {
                if !ids.contains(dependency.as_str()) || !deps.insert(dependency) {
                    return Err("unknown or duplicate compile dependency".into());
                }
            }
        }
        let mut resolved = BTreeSet::new();
        loop {
            let before = resolved.len();
            for job in &self.jobs {
                if job
                    .dependencies
                    .iter()
                    .all(|id| resolved.contains(id.as_str()))
                {
                    resolved.insert(job.job_id.as_str());
                }
            }
            if resolved.len() == self.jobs.len() {
                return Ok(());
            }
            if resolved.len() == before {
                return Err("cyclic compile dependencies".into());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const GIB: u64 = 1 << 30;

    fn job(memory_bytes: u64, expected_memory_bytes: Option<u64>) -> Group {
        Group {
            group_id: "g".into(),
            jobs: vec![Job {
                job_id: "j".into(),
                argv: vec!["true".into()],
                cwd: "/".into(),
                env: BTreeMap::new(),
                timeout_seconds: None,
                memory_bytes,
                expected_memory_bytes,
                expected_runtime_secs: Some(60),
                error_memory_bytes: None,
                dependencies: Vec::new(),
                metadata: BTreeMap::new(),
                inherit_environment: true,
            }],
            metadata: BTreeMap::new(),
            max_running: 0,
            result_sink: None,
        }
    }

    /// Q-J1.
    #[test]
    fn an_expected_figure_is_validated() {
        assert!(job(4 * GIB, Some(5 * GIB)).validate().is_err(), "(a) above its cell");
        assert!(job(4 * GIB, Some(0)).validate().is_err(), "(b) nothing");
        assert!(job(0, Some(GIB)).validate().is_err(), "(c) without a declared cell");
        assert_eq!(job(4 * GIB, Some(GIB)).validate(), Ok(()), "(d) below its cell");
        assert_eq!(job(4 * GIB, Some(4 * GIB)).validate(), Ok(()), "(d) its whole cell");
    }

    /// Q-J2: stored descriptors read and write back as they were.
    #[test]
    fn a_descriptor_without_the_field_round_trips() {
        let without = r#"{"group_id":"g","jobs":[{"argv":["true"],"cwd":"/","dependencies":[],"env":{},"expected_runtime_secs":60,"inherit_environment":true,"job_id":"j","memory_bytes":4294967296,"metadata":{}}],"max_running":0,"metadata":{}}"#;
        let group: Group = serde_json::from_str(without).unwrap();
        assert_eq!(group.jobs[0].expected_memory_bytes, None);
        assert_eq!(serde_json::to_value(&group).unwrap().to_string(), without, "written back byte-equal");
        let with = without.replace(r#""env":{},"#, r#""env":{},"expected_memory_bytes":1073741824,"#);
        let group: Group = serde_json::from_str(&with).unwrap();
        assert_eq!(group.jobs[0].expected_memory_bytes, Some(GIB));
        assert_eq!(serde_json::to_value(&group).unwrap().to_string(), with, "written back with it");
    }
}
