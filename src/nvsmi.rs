//! Host GPU facts from the `nvidia-smi` CLI. Stands in for the NVML calls of
//! tools/_gpu_nvml.py: compute and graphics processes, per-pid memory, SM
//! activity, free memory.

use std::collections::HashMap;
use std::path::Path;
use std::process::{Command, Stdio};

const MIB: u64 = 1 << 20;

fn run(nvidia_smi: &Path, args: &[&str]) -> Result<String, String> {
    let out = Command::new(nvidia_smi)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("running {}: {e}", nvidia_smi.display()))?;
    if !out.status.success() {
        return Err(format!(
            "{} {}: {}",
            nvidia_smi.display(),
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Compute processes with used memory in bytes; `None` where the driver
/// reports it unavailable.
pub fn compute_apps(nvidia_smi: &Path) -> Result<Vec<(u32, Option<u64>)>, String> {
    let text = run(
        nvidia_smi,
        &[
            "--query-compute-apps=pid,used_memory",
            "--format=csv,noheader,nounits",
        ],
    )?;
    let mut out = Vec::new();
    for line in text.lines().map(str::trim).filter(|l| !l.is_empty()) {
        let mut f = line.split(',').map(str::trim);
        let pid = f
            .next()
            .and_then(|p| p.parse().ok())
            .ok_or_else(|| format!("unexpected nvidia-smi line {line:?}"))?;
        let mem = f
            .next()
            .and_then(|m| m.parse::<u64>().ok())
            .map(|m| m * MIB);
        out.push((pid, mem));
    }
    Ok(out)
}

/// `(total, free)` bytes of the device with least free memory
/// (tools/_gpu_nvml.py:114-124).
pub fn memory(nvidia_smi: &Path) -> Result<(u64, u64), String> {
    let text = run(
        nvidia_smi,
        &[
            "--query-gpu=memory.total,memory.free",
            "--format=csv,noheader,nounits",
        ],
    )?;
    text.lines()
        .filter_map(|l| {
            let mut f = l.split(',').map(|v| v.trim().parse::<u64>().ok());
            Some((f.next()??, f.next()??))
        })
        .min_by_key(|(_, free)| *free)
        .map(|(t, f)| (t * MIB, f * MIB))
        .ok_or_else(|| format!("no memory.total,memory.free in {text:?}"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pmon {
    pub pid: u32,
    /// `C`, `G` or `C+G`.
    pub kind: String,
    /// SM utilisation in percent; `None` when not sampled (`-`).
    pub sm: Option<u32>,
}

impl Pmon {
    pub fn graphics(&self) -> bool {
        self.kind.contains('G')
    }
    pub fn computing(&self) -> bool {
        self.sm.is_some_and(|s| s > 0)
    }
}

/// One `nvidia-smi pmon -c 1 -s u` sample.
pub fn pmon(nvidia_smi: &Path) -> Result<Vec<Pmon>, String> {
    let text = run(nvidia_smi, &["pmon", "-c", "1", "-s", "u"])?;
    Ok(parse_pmon(&text))
}

pub fn parse_pmon(text: &str) -> Vec<Pmon> {
    text.lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .filter_map(|l| {
            let f: Vec<&str> = l.split_whitespace().collect();
            Some(Pmon {
                pid: f.get(1)?.parse().ok()?,
                kind: f.get(2)?.to_string(),
                sm: f.get(3).and_then(|s| s.parse().ok()),
            })
        })
        .collect()
}

#[derive(Debug, Clone, Default)]
pub struct Sample {
    pub compute: Vec<(u32, Option<u64>)>,
    pub pmon: Vec<Pmon>,
    /// `(total, free)` bytes.
    pub memory: Option<(u64, u64)>,
}

impl Sample {
    pub fn take(nvidia_smi: &Path) -> Result<Sample, String> {
        Ok(Sample {
            compute: compute_apps(nvidia_smi)?,
            pmon: pmon(nvidia_smi)?,
            memory: Some(memory(nvidia_smi)?),
        })
    }

    pub fn computing(&self) -> HashMap<u32, bool> {
        self.pmon.iter().map(|p| (p.pid, p.computing())).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pmon_rows() {
        let text = "\
# gpu         pid   type     sm    mem    enc    dec    jpg    ofa    command
# Idx           #    C/G      %      %      %      %      %      %    name
    0       1234     C     45     10      -      -      -      -    python
    0       5678     G      -      -      -      -      -      -    Xorg
    0       9012   C+G      0      0      -      -      -      -    firefox
    0          -     -      -      -      -      -      -      -    -
";
        let rows = parse_pmon(text);
        assert_eq!(rows.len(), 3);
        assert!(rows[0].computing() && !rows[0].graphics());
        assert!(rows[1].graphics() && rows[1].sm.is_none());
        assert!(rows[2].graphics() && !rows[2].computing());
    }
}
