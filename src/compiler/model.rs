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
