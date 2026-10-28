use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

fn timeout() -> f64 {
    1200.0
}
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
    #[serde(default = "timeout")]
    pub timeout_seconds: f64,
    #[serde(default)]
    pub memory_bytes: u64,
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
                || !job.timeout_seconds.is_finite()
                || job.timeout_seconds <= 0.0
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

/// Direct native command continuations. GPU admission remains owned by Lease
/// inside each command, after its CPU preparation has completed.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Pipeline {
    pub compile_group: Group,
    pub gpu_jobs: Vec<Job>,
    #[serde(default)]
    pub max_running: usize,
}
impl Pipeline {
    pub fn validate(&self) -> Result<(), String> {
        self.compile_group.validate()?;
        let compile_ids: BTreeSet<_> = self
            .compile_group
            .jobs
            .iter()
            .map(|j| j.job_id.as_str())
            .collect();
        let mut ids = compile_ids.clone();
        for job in &self.gpu_jobs {
            if !ids.insert(job.job_id.as_str()) {
                return Err("duplicate pipeline job identity".into());
            }
            let mut command = job.clone();
            command.dependencies.clear();
            Group {
                group_id: "validation".into(),
                jobs: vec![command],
                metadata: BTreeMap::new(),
                max_running: 0,
            }
            .validate()?;
            if job
                .dependencies
                .iter()
                .any(|id| !compile_ids.contains(id.as_str()))
            {
                return Err("unknown native command prerequisite".into());
            }
        }
        Ok(())
    }
}
