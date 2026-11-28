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

    /// The sample's processes by parent, indexed in one pass for any number of descendant queries.
    pub fn process_tree(&self) -> ProcessTree<'_> {
        let mut children: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        for (&pid, &(parent, _)) in &self.processes {
            children.entry(parent).or_default().push(pid);
        }
        ProcessTree { sample: self, children }
    }
}

/// A sample's process tree: a query visits only the processes it returns, never the whole table.
pub struct ProcessTree<'a> {
    sample: &'a Snapshot,
    children: BTreeMap<u32, Vec<u32>>,
}
impl ProcessTree<'_> {
    /// `roots` and every process of the sample that descends from one of them.
    pub fn descendants(&self, roots: impl IntoIterator<Item = u32>) -> BTreeSet<u32> {
        let mut owned = BTreeSet::new();
        let mut frontier: Vec<u32> = roots.into_iter().collect();
        while let Some(pid) = frontier.pop() {
            if owned.insert(pid) {
                frontier.extend(self.children.get(&pid).into_iter().flatten());
            }
        }
        owned
    }

    /// The resident memory of `roots` and their descendants, as the sample read it.
    pub fn rss(&self, roots: impl IntoIterator<Item = u32>) -> u64 {
        self.descendants(roots)
            .iter()
            .filter_map(|pid| self.sample.processes.get(pid))
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

    /// The descendants of roots are every process whose parent chain reaches one of them, at any depth and in any
    /// table order, the roots themselves included even when the sample does not list them.
    #[test]
    fn a_tree_query_finds_every_descendant_and_only_those() {
        let sample = Snapshot {
            processes: BTreeMap::from([
                (10, (1, 100)),
                (11, (10, 20)),
                (12, (11, 3)),
                (5, (12, 4)),
                (20, (1, 1000)),
                (21, (20, 7)),
                (31, (30, 50)),
            ]),
            ..Snapshot::default()
        };
        let tree = sample.process_tree();
        assert_eq!(tree.descendants([10]), BTreeSet::from([10, 11, 12, 5]), "a child listed before its parent is found");
        assert_eq!(tree.descendants([11, 21]), BTreeSet::from([11, 12, 5, 21]));
        assert_eq!(tree.descendants([30]), BTreeSet::from([30, 31]), "an unlisted root keeps its listed children");
        assert_eq!(tree.descendants(None), BTreeSet::new());
        assert_eq!(tree.rss([10]), 127);
        assert_eq!(tree.rss([30]), 50, "an unlisted root counts nothing itself");
        assert_eq!(tree.rss(None), 0);
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
