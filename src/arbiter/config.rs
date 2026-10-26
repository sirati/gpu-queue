use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

pub use crate::switch::config::secs;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArbiterConfig {
    pub proxy: ProxyConfig,
    pub ninfer: NinferConfig,
    /// gpu-switch's container socket.
    pub switch_socket: PathBuf,
    #[serde(default = "d_reconnect_initial")]
    pub reconnect_initial_secs: f64,
    #[serde(default = "d_reconnect_max")]
    pub reconnect_max_secs: f64,
    /// NInfer keeps serving this long after the link drops (a restart of
    /// gpu-switch); then it is stopped: no NInfer without a live grant.
    #[serde(default = "d_link_grace")]
    pub link_grace_secs: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyConfig {
    /// A TCP listener with the whole API (tests, manual runs).
    #[serde(default)]
    pub listen: Option<SocketAddr>,
    /// Unix socket listeners, each with its own policy.
    #[serde(default)]
    pub listeners: Vec<Listener>,
    #[serde(default = "d_keepalive")]
    pub keepalive_interval_secs: f64,
}

/// A unix socket of the proxy: the `LISTEN_FDNAMES` fd of `name` (systemd),
/// otherwise `path`, bound here.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listener {
    pub name: String,
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// Octal mode of a socket bound here (default 0660); who reaches it is
    /// then up to its directory.
    #[serde(default)]
    pub mode: Option<String>,
    /// Anthropic Messages only: `POST /v1/messages`, `POST
    /// /v1/messages/count_tokens` and `GET /v1/models`; anything else is 404.
    #[serde(default)]
    pub messages_only: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NinferConfig {
    pub executable: PathBuf,
    pub model: PathBuf,
    pub port: u16,
    #[serde(default)]
    pub args: Vec<String>,
    /// Profile name -> extra args; the daemon names the profile in `start`.
    #[serde(default)]
    pub profiles: BTreeMap<String, Vec<String>>,
    #[serde(default = "d_health_poll")]
    pub health_poll_interval_secs: f64,
    #[serde(default = "d_startup_timeout")]
    pub startup_timeout_secs: f64,
    #[serde(default = "d_stop_timeout")]
    pub stop_timeout_secs: f64,
}

fn d_reconnect_initial() -> f64 {
    1.0
}
fn d_reconnect_max() -> f64 {
    30.0
}
fn d_link_grace() -> f64 {
    30.0
}
fn d_keepalive() -> f64 {
    5.0
}
fn d_health_poll() -> f64 {
    1.0
}
fn d_startup_timeout() -> f64 {
    1800.0
}
fn d_stop_timeout() -> f64 {
    120.0
}

impl Listener {
    pub fn socket_mode(&self) -> Result<Option<u32>, String> {
        match &self.mode {
            None => Ok(None),
            Some(m) => match u32::from_str_radix(m, 8) {
                Ok(v) if v <= 0o777 => Ok(Some(v)),
                _ => Err(format!(
                    "proxy listener {}: mode {m:?} is not an octal mode",
                    self.name
                )),
            },
        }
    }
}

impl ArbiterConfig {
    pub fn load(path: &Path) -> Result<ArbiterConfig, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("reading {}: {e}", path.display()))?;
        let c: ArbiterConfig =
            serde_json::from_str(&text).map_err(|e| format!("parsing {}: {e}", path.display()))?;
        for (name, v) in [
            ("reconnect_initial_secs", c.reconnect_initial_secs),
            ("reconnect_max_secs", c.reconnect_max_secs),
            ("link_grace_secs", c.link_grace_secs),
            (
                "proxy.keepalive_interval_secs",
                c.proxy.keepalive_interval_secs,
            ),
            (
                "ninfer.health_poll_interval_secs",
                c.ninfer.health_poll_interval_secs,
            ),
            ("ninfer.startup_timeout_secs", c.ninfer.startup_timeout_secs),
            ("ninfer.stop_timeout_secs", c.ninfer.stop_timeout_secs),
        ] {
            if !v.is_finite() || v < 0.0 {
                return Err(format!("{name} must be a non-negative number"));
            }
        }
        if c.proxy.keepalive_interval_secs == 0.0 || c.ninfer.health_poll_interval_secs == 0.0 {
            return Err("poll/keepalive intervals must be > 0".into());
        }
        if c.proxy.listen.is_none() && c.proxy.listeners.is_empty() {
            return Err("proxy: no listen address and no listeners".into());
        }
        for l in &c.proxy.listeners {
            l.socket_mode()?;
        }
        Ok(c)
    }
}
