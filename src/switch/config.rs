use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listen {
    /// `host`, `container` or `arbiter`.
    pub name: String,
    /// Bound when no `LISTEN_FDNAMES` fd of that name was passed.
    #[serde(default)]
    pub path: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwitchConfig {
    pub listen: Vec<Listen>,
    #[serde(default)]
    pub nvidia_smi: Option<PathBuf>,
    #[serde(default = "d_linger")]
    pub linger_secs: f64,
    #[serde(default = "d_poll")]
    pub gpu_poll_interval_secs: f64,
    /// Card samples while an admission waits on one: at most this often.
    #[serde(default = "d_wanted_poll")]
    pub gpu_wanted_poll_interval_secs: f64,
    #[serde(default = "d_grace")]
    pub job_kill_grace_secs: f64,
    #[serde(default)]
    pub profiles: Vec<String>,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub host_context_mib: Option<u64>,
    #[serde(default)]
    pub drain_timeout_secs: Option<f64>,
    #[serde(default = "d_backoff_initial")]
    pub restart_backoff_initial_secs: f64,
    #[serde(default = "d_backoff_max")]
    pub restart_backoff_max_secs: f64,
    /// Startup rule: every GPU grant must allocate VRAM within this long.
    #[serde(default = "d_startup")]
    pub startup_idle_secs: f64,
    /// Idle rule: no compute this long after having used it -> SIGINT.
    #[serde(default = "d_idle")]
    pub idle_secs: f64,
    /// SIGINT (idle rule, kill) -> SIGTERM.
    #[serde(default = "d_int_term")]
    pub sigint_to_sigterm_secs: f64,
    /// SIGTERM -> SIGKILL (all escalations).
    #[serde(default = "d_term_kill")]
    pub sigterm_to_sigkill_secs: f64,
    /// ML_GPU_COOP_MIN_HOLD: a cooperative job yields only after this long.
    #[serde(default = "d_coop")]
    pub coop_min_hold_secs: f64,
    /// Weighted GPU time of a cooperative shared admission, including yield grace.
    #[serde(default = "d_shared_budget")]
    pub shared_budget_secs: f64,
    #[serde(default = "d_shared_turn")]
    pub shared_turn_secs: f64,
    #[serde(default = "d_shared_idle")]
    pub shared_idle_yield_secs: f64,
    /// Where the line survives a restart; else `$STATE_DIRECTORY` (systemd).
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
    /// After a restart, or a dropped arbiter link: how long a job's client,
    /// or the arbiter, has to come back.
    #[serde(default = "d_reattach")]
    pub reattach_grace_secs: f64,
}

fn d_linger() -> f64 {
    60.0
}
fn d_poll() -> f64 {
    2.0
}
fn d_wanted_poll() -> f64 {
    0.05
}
fn d_grace() -> f64 {
    10.0
}
fn d_backoff_initial() -> f64 {
    2.0
}
fn d_backoff_max() -> f64 {
    60.0
}
fn d_startup() -> f64 {
    1.0
}
fn d_idle() -> f64 {
    90.0
}
fn d_int_term() -> f64 {
    60.0
}
fn d_term_kill() -> f64 {
    10.0
}
fn d_coop() -> f64 {
    180.0
}
fn d_shared_budget() -> f64 {
    600.0
}
fn d_shared_turn() -> f64 {
    300.0
}
fn d_shared_idle() -> f64 {
    60.0
}
fn d_reattach() -> f64 {
    60.0
}

pub fn secs(v: f64) -> Duration {
    Duration::from_secs_f64(v.max(0.0))
}

impl SwitchConfig {
    pub fn load(path: &Path) -> Result<SwitchConfig, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("reading {}: {e}", path.display()))?;
        let c: SwitchConfig =
            serde_json::from_str(&text).map_err(|e| format!("parsing {}: {e}", path.display()))?;
        c.validate()?;
        Ok(c)
    }

    fn validate(&self) -> Result<(), String> {
        for l in &self.listen {
            if !["host", "container", "arbiter"].contains(&l.name.as_str()) {
                return Err(format!(
                    "listen name {:?}: must be host, container or arbiter",
                    l.name
                ));
            }
        }
        if let Some(p) = &self.profile {
            if !self.profiles.contains(p) {
                return Err(format!("profile {p:?} is not in profiles"));
            }
        }
        let all = [
            ("linger_secs", self.linger_secs),
            ("gpu_poll_interval_secs", self.gpu_poll_interval_secs),
            (
                "gpu_wanted_poll_interval_secs",
                self.gpu_wanted_poll_interval_secs,
            ),
            ("job_kill_grace_secs", self.job_kill_grace_secs),
            (
                "restart_backoff_initial_secs",
                self.restart_backoff_initial_secs,
            ),
            ("restart_backoff_max_secs", self.restart_backoff_max_secs),
            ("startup_idle_secs", self.startup_idle_secs),
            ("idle_secs", self.idle_secs),
            ("sigint_to_sigterm_secs", self.sigint_to_sigterm_secs),
            ("sigterm_to_sigkill_secs", self.sigterm_to_sigkill_secs),
            ("coop_min_hold_secs", self.coop_min_hold_secs),
            ("shared_budget_secs", self.shared_budget_secs),
            ("shared_turn_secs", self.shared_turn_secs),
            ("shared_idle_yield_secs", self.shared_idle_yield_secs),
            ("reattach_grace_secs", self.reattach_grace_secs),
            ("drain_timeout_secs", self.drain_timeout_secs.unwrap_or(0.0)),
        ];
        for (name, v) in all {
            if !v.is_finite() || v < 0.0 {
                return Err(format!("{name} must be a non-negative number"));
            }
        }
        if self.gpu_poll_interval_secs == 0.0 {
            return Err("gpu_poll_interval_secs must be > 0".into());
        }
        Ok(())
    }
}
