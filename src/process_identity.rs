//! A process's identity for all time: its boot, its id and its start, which the kernel never gives two processes
//! of one boot.
use serde::{Deserialize, Serialize};

/// The engine instance that writes a journal: the service process, in its boot. Each one starts from the empty
/// admission state, so a journal's records are checked per instance.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Instance {
    pub boot_id: String,
    pub pid: u32,
    pub start_ticks: u64,
}
impl Instance {
    /// This process, in this boot.
    pub fn current() -> Result<Self, String> {
        let pid = std::process::id();
        Ok(Self {
            boot_id: boot()?,
            pid,
            start_ticks: identity(pid)?.0,
        })
    }
}

impl std::fmt::Display for Instance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "instance {} of boot {} started at {}", self.pid, self.boot_id, self.start_ticks)
    }
}

fn boot() -> Result<String, String> {
    Ok(std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map_err(|e| e.to_string())?
        .trim()
        .to_owned())
}

/// A process's start ticks and its state.
pub fn identity(pid: u32) -> Result<(u64, char), String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).map_err(|e| e.to_string())?;
    let (_, tail) = stat.rsplit_once(')').ok_or("bad process stat")?;
    let fields: Vec<&str> = tail.split_whitespace().collect();
    Ok((
        fields
            .get(19)
            .ok_or("missing process birth")?
            .parse::<u64>()
            .map_err(|e| e.to_string())?,
        fields[0].chars().next().unwrap_or('X'),
    ))
}
