//! Host GPU facts from the `nvidia-smi` CLI. Stands in for the NVML calls of
//! tools/_gpu_nvml.py: compute and graphics processes, per-pid memory, SM
//! activity, free memory.

use std::collections::{HashMap, HashSet};
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

/// Zero on every device proves inactivity even if per-process SM is unavailable.
fn device_activity(nvidia_smi: &Path) -> Result<bool, String> {
    let text = run(
        nvidia_smi,
        &[
            "--query-gpu=utilization.gpu",
            "--format=csv,noheader,nounits",
        ],
    )?;
    let values: Result<Vec<u32>, _> = text.lines().map(|line| line.trim().parse()).collect();
    let values = values.map_err(|_| "GPU utilisation unavailable".to_string())?;
    if values.is_empty() {
        return Err("no GPU utilisation samples".into());
    }
    Ok(values.iter().any(|value| *value > 0))
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
    /// Any device computing, or None when the device query is unavailable.
    pub device_active: Option<bool>,
    /// `(total, free)` bytes.
    pub memory: Option<(u64, u64)>,
}

impl Sample {
    pub fn take(nvidia_smi: &Path) -> Result<Sample, String> {
        Ok(Sample {
            compute: compute_apps(nvidia_smi)?,
            pmon: pmon(nvidia_smi)?,
            memory: Some(memory(nvidia_smi)?),
            device_active: device_activity(nvidia_smi).ok(),
        })
    }

    /// Shared by accounting and enforcement so unavailable pmon rows cannot
    /// freeze idle accounting while another path declares the same job idle.
    pub fn activity(&self, owned: &HashSet<u32>) -> Option<bool> {
        let rows: Vec<_> = self
            .pmon
            .iter()
            .filter(|p| owned.contains(&p.pid))
            .collect();
        if rows.iter().any(|p| p.computing()) {
            return Some(true);
        }
        if self.device_active == Some(false) {
            return Some(false);
        }
        let unknown = rows.iter().any(|p| p.sm.is_none())
            || self.compute.iter().any(|(pid, _)| {
                owned.contains(pid) && !rows.iter().any(|p| p.pid == *pid && p.sm.is_some())
            });
        if unknown {
            None
        } else {
            Some(false)
        }
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
    #[test]
    fn unavailable_process_sm_uses_only_proven_device_inactivity() {
        let owned = HashSet::from([1234]);
        let mut sample = Sample {
            compute: vec![(1234, Some(100))],
            pmon: vec![Pmon {
                pid: 1234,
                kind: "C".into(),
                sm: None,
            }],
            ..Sample::default()
        };
        assert_eq!(sample.activity(&owned), None);
        sample.device_active = Some(true);
        assert_eq!(sample.activity(&owned), None);
        sample.device_active = Some(false);
        assert_eq!(sample.activity(&owned), Some(false));
        sample.pmon[0].sm = Some(20);
        assert_eq!(sample.activity(&owned), Some(true));
    }
    #[test]
    fn another_shared_job_cannot_supply_compute_for_an_idle_owner() {
        let sample = Sample {
            device_active: Some(true),
            pmon: vec![
                Pmon {
                    pid: 1,
                    kind: "C".into(),
                    sm: Some(0),
                },
                Pmon {
                    pid: 2,
                    kind: "C".into(),
                    sm: Some(90),
                },
            ],
            ..Sample::default()
        };
        assert_eq!(sample.activity(&HashSet::from([1])), Some(false));
        assert_eq!(sample.activity(&HashSet::from([2])), Some(true));
    }
}
