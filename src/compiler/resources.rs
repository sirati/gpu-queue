//! Sampling never owns queue state and never holds a GPU grant.
use std::collections::{BTreeMap, BTreeSet};
use std::fs;

#[derive(Clone, Default, Debug)]
pub struct Snapshot {
    pub available: u64,
    pub capacity: u64,
    pub idle: usize,
    pub cores: Vec<usize>,
    pub processes: BTreeMap<u32, (u32, u64)>,
    pub groups: BTreeMap<u32, (u32, u64)>,
    /// pid -> (session, start time), zombies included.
    pub sessions: BTreeMap<u32, (u32, u64)>,
}
impl Snapshot {
    pub fn group_members(&self, leader: u32) -> Vec<(u32, u64)> {
        if leader == 0 {
            return Vec::new();
        }
        self.groups
            .iter()
            .filter(|(_, (group, _))| *group == leader)
            .map(|(&pid, &(_, birth))| (pid, birth))
            .collect()
    }

    pub fn rss(&self, roots: &BTreeSet<u32>) -> u64 {
        let mut owned = roots.clone();
        loop {
            let before = owned.len();
            for (&pid, &(parent, _)) in &self.processes {
                if owned.contains(&parent) {
                    owned.insert(pid);
                }
            }
            if owned.len() == before {
                break;
            }
        }
        owned
            .iter()
            .filter_map(|pid| self.processes.get(pid))
            .map(|(_, rss)| rss)
            .sum()
    }
}
#[derive(Default)]
pub struct Sampler {
    previous: BTreeMap<usize, (u64, u64)>,
}
impl Sampler {
    pub fn sample(&mut self) -> Result<Snapshot, String> {
        let mut affinity: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        // Tokio tasks can move between runtime threads. The service leader's
        // affinity is the stable scheduling boundary, independent of that move.
        if unsafe {
            libc::sched_getaffinity(
                libc::getpid(),
                std::mem::size_of_val(&affinity),
                &mut affinity,
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error().to_string());
        }
        let cores: Vec<usize> = (0..libc::CPU_SETSIZE as usize)
            .filter(|&i| unsafe { libc::CPU_ISSET(i, &affinity) })
            .collect();
        let (available, capacity) = memory()?;
        let stat = fs::read_to_string("/proc/stat").map_err(|e| e.to_string())?;
        let mut next = BTreeMap::new();
        let mut idle = 0.0;
        for line in stat.lines() {
            let mut fields = line.split_whitespace();
            let Some(cpu) = fields
                .next()
                .and_then(|s| s.strip_prefix("cpu"))
                .and_then(|s| s.parse::<usize>().ok())
            else {
                continue;
            };
            if !cores.contains(&cpu) {
                continue;
            }
            let values: Vec<u64> = fields.take(8).filter_map(|s| s.parse().ok()).collect();
            if values.len() < 5 {
                continue;
            }
            let total = values.iter().sum::<u64>();
            let free = values[3] + values[4];
            if let Some((old_total, old_free)) = self.previous.get(&cpu) {
                if total > *old_total {
                    idle += (free.saturating_sub(*old_free) as f64 / (total - old_total) as f64)
                        .clamp(0.0, 1.0);
                }
            }
            next.insert(cpu, (total, free));
        }
        self.previous = next;
        let mut processes = BTreeMap::new();
        let mut groups = BTreeMap::new();
        let mut sessions = BTreeMap::new();
        for entry in fs::read_dir("/proc").map_err(|e| e.to_string())?.flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|s| s.parse::<u32>().ok())
            else {
                continue;
            };
            if let Ok(stat) = fs::read_to_string(entry.path().join("stat")) {
                if let Some((_, tail)) = stat.rsplit_once(')') {
                    let fields: Vec<&str> = tail.split_whitespace().collect();
                    if fields.len() > 21 {
                        if !matches!(fields[0], "Z" | "X") {
                            if let (Ok(group), Ok(birth)) = (fields[2].parse(), fields[19].parse())
                            {
                                groups.insert(pid, (group, birth));
                            }
                        }
                        if let (Ok(session), Ok(birth)) = (fields[3].parse(), fields[19].parse()) {
                            sessions.insert(pid, (session, birth));
                        }
                        if let (Ok(parent), Ok(rss)) =
                            (fields[1].parse(), fields[21].parse::<u64>())
                        {
                            processes.insert(
                                pid,
                                (
                                    parent,
                                    rss * unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64,
                                ),
                            );
                        }
                    }
                }
            }
        }
        Ok(Snapshot {
            available,
            capacity,
            // Fractional spare capacity must make progress too. Flooring e.g.
            // 0.9 idle cores to zero can starve every short compilation forever.
            idle: idle.ceil() as usize,
            cores,
            processes,
            groups,
            sessions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_spawn_pid_never_claims_process_group_zero() {
        let sample = Snapshot {
            groups: BTreeMap::from([(50, (0, 123)), (51, (30, 456))]),
            ..Snapshot::default()
        };
        assert!(sample.group_members(0).is_empty());
        assert_eq!(sample.group_members(30), vec![(51, 456)]);
    }
}

/// Current RAM headroom, respecting all service cgroup ceilings.
pub fn memory() -> Result<(u64, u64), String> {
    let mem = fs::read_to_string("/proc/meminfo").map_err(|e| e.to_string())?;
    let mut available = mem
        .lines()
        .find(|l| l.starts_with("MemAvailable:"))
        .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok())
        .ok_or("no MemAvailable")?
        * 1024;
    let mut capacity = mem
        .lines()
        .find(|l| l.starts_with("MemTotal:"))
        .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok())
        .ok_or("no MemTotal")?
        * 1024;
    // Respect the current service cgroup, including parent memory ceilings.
    if let Ok(cgroup) = fs::read_to_string("/proc/self/cgroup") {
        if let Some(relative) = cgroup.lines().find_map(|line| line.strip_prefix("0::")) {
            let base = std::path::Path::new("/sys/fs/cgroup");
            let path = base.join(relative.trim_start_matches('/'));
            for parent in path.ancestors().take_while(|p| p.starts_with(base)) {
                let ceiling = fs::read_to_string(parent.join("memory.max"))
                    .ok()
                    .and_then(|s| s.trim().parse::<u64>().ok());
                let current = fs::read_to_string(parent.join("memory.current"))
                    .ok()
                    .and_then(|s| s.trim().parse::<u64>().ok());
                if let (Some(max), Some(used)) = (ceiling, current) {
                    available = available.min(max.saturating_sub(used));
                    capacity = capacity.min(max);
                }
            }
        }
    }
    Ok((available, capacity))
}
