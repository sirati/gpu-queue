//! Every compilation runs in its own memory cgroup sized to its declaration: a
//! compilation that outgrows it is killed alone instead of exhausting the host.
//! Containment exists only inside the daemon's delegated service cgroup;
//! anywhere else compilations run unconfined.
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::{Path, PathBuf};

const SERVICE: &str = "doggy-compile.service";
/// The unit's `DelegateSubgroup`: the daemon's own leaf beside the cells.
pub const DAEMON: &str = "daemon";

pub enum Containment {
    Cgroups(PathBuf),
    Unconfined(String),
}

/// What a finished attempt used.
#[derive(Debug, Default, PartialEq)]
pub struct Usage {
    pub peak: Option<u64>,
    pub out_of_memory: bool,
}

/// The cell a child moves itself into between fork and exec.
pub struct Placement(Option<OwnedFd>);
impl Placement {
    pub fn descriptor(&self) -> Option<i32> {
        self.0.as_ref().map(AsRawFd::as_raw_fd)
    }
    /// Async-signal-safe: runs in the forked child.
    pub fn join(descriptor: Option<i32>) -> std::io::Result<()> {
        match descriptor {
            Some(fd) if unsafe { libc::write(fd, b"0".as_ptr().cast(), 1) } != 1 => {
                Err(std::io::Error::last_os_error())
            }
            _ => Ok(()),
        }
    }
}

fn write(path: &Path, value: &str) -> Result<(), String> {
    std::fs::write(path, value).map_err(|e| format!("{}: {e}", path.display()))
}

fn own_cgroup() -> Result<PathBuf, String> {
    let line = std::fs::read_to_string("/proc/self/cgroup").map_err(|e| e.to_string())?;
    let path = line
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .ok_or("no unified cgroup")?;
    Ok(Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/')))
}

impl Containment {
    pub fn establish() -> Self {
        match own_cgroup().and_then(|own| Self::service_root(&own)).and_then(Self::adopt) {
            Ok(root) => Self::Cgroups(root),
            Err(reason) => Self::Unconfined(reason),
        }
    }
    fn service_root(own: &Path) -> Result<PathBuf, String> {
        let root = match own.file_name().and_then(|name| name.to_str()) {
            Some(DAEMON) => own.parent().unwrap_or(own),
            _ => own,
        };
        match root.file_name().and_then(|name| name.to_str()) {
            Some(SERVICE) => Ok(root.to_owned()),
            _ => Err(format!("not running in {SERVICE}")),
        }
    }
    /// Processes of the root move into the daemon leaf, so the root may give
    /// its children their own memory controller.
    pub fn adopt(root: PathBuf) -> Result<PathBuf, String> {
        let leaf = root.join(DAEMON);
        std::fs::create_dir_all(&leaf).map_err(|e| e.to_string())?;
        // A predecessor may fork into the root while its processes move.
        for _ in 0..10 {
            let procs =
                std::fs::read_to_string(root.join("cgroup.procs")).map_err(|e| e.to_string())?;
            for pid in procs.split_whitespace() {
                // A process may exit between listing and moving.
                if let Err(error) = std::fs::write(leaf.join("cgroup.procs"), pid) {
                    if Path::new(&format!("/proc/{pid}")).exists() {
                        return Err(format!("cannot adopt process {pid}: {error}"));
                    }
                }
            }
            if write(&root.join("cgroup.subtree_control"), "+memory").is_ok() {
                return Ok(root);
            }
        }
        write(&root.join("cgroup.subtree_control"), "+memory").map(|()| root)
    }
    fn cell(root: &Path, job: &str) -> PathBuf {
        root.join(format!("job-{job}"))
    }
    /// A fresh cell for one attempt.
    pub fn enter(&self, job: &str, limit: u64) -> Result<Placement, String> {
        let Self::Cgroups(root) = self else {
            return Ok(Placement(None));
        };
        let cell = Self::cell(root, job);
        match std::fs::remove_dir(&cell) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                return Err(format!("stale memory cell {}: {error}", cell.display()))
            }
            _ => {}
        }
        std::fs::create_dir(&cell).map_err(|e| e.to_string())?;
        write(&cell.join("memory.max"), &limit.to_string())?;
        write(&cell.join("memory.swap.max"), "0")?;
        write(&cell.join("memory.oom.group"), "1")?;
        let procs = std::fs::OpenOptions::new()
            .write(true)
            .open(cell.join("cgroup.procs"))
            .map_err(|e| e.to_string())?;
        Ok(Placement(Some(procs.into())))
    }
    pub fn limit(&self, job: &str, limit: u64) -> Result<(), String> {
        match self {
            Self::Cgroups(root) => write(&Self::cell(root, job).join("memory.max"), &limit.to_string()),
            Self::Unconfined(_) => Ok(()),
        }
    }
    /// The attempt's usage; its cell and any process left in it are gone afterwards.
    pub fn release(&self, job: &str) -> Usage {
        let Self::Cgroups(root) = self else {
            return Usage::default();
        };
        let cell = Self::cell(root, job);
        let read = |name: &str| std::fs::read_to_string(cell.join(name)).unwrap_or_default();
        let usage = Usage {
            peak: read("memory.peak").trim().parse().ok(),
            out_of_memory: read("memory.events")
                .lines()
                .filter_map(|line| line.strip_prefix("oom_kill "))
                .any(|count| count.trim() != "0"),
        };
        let _ = std::fs::write(cell.join("cgroup.kill"), "1");
        let _ = std::fs::remove_dir(&cell);
        usage
    }
    /// Cells whose job no longer runs.
    pub fn prune(&self, running: impl Fn(&str) -> bool) {
        let Self::Cgroups(root) = self else {
            return;
        };
        for entry in std::fs::read_dir(root).into_iter().flatten().flatten() {
            if let Some(job) = entry.file_name().to_str().and_then(|n| n.strip_prefix("job-")) {
                if !running(job) {
                    self.release(job);
                }
            }
        }
    }
    /// A test that runs in its own delegated scope contains within it.
    #[cfg(test)]
    pub fn delegated_scope() -> Self {
        Self::Cgroups(Self::adopt(own_cgroup().unwrap()).unwrap())
    }
    pub fn describe(&self) -> String {
        match self {
            Self::Cgroups(root) => format!("cgroup {}", root.display()),
            Self::Unconfined(reason) => format!("unconfined: {reason}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_the_service_cgroup_is_contained() {
        let units = Path::new("/sys/fs/cgroup/user.slice/app.slice");
        for own in [units.join(SERVICE), units.join(SERVICE).join(DAEMON)] {
            assert_eq!(Containment::service_root(&own), Ok(units.join(SERVICE)));
        }
        for own in [units.join("other.service"), units.join("other.service").join(DAEMON), units.to_owned()] {
            assert!(Containment::service_root(&own).is_err());
        }
        assert_eq!(Containment::Unconfined("x".into()).release("job"), Usage::default());
        assert_eq!(Placement::join(None).ok(), Some(()));
    }

    /// Needs a delegated cgroup of its own:
    /// `systemd-run --user --scope -p Delegate=memory cargo test -- --ignored contained`
    #[test]
    #[ignore]
    fn a_contained_attempt_reports_its_peak_and_its_out_of_memory_kill() {
        use std::os::unix::process::CommandExt;
        let containment = Containment::delegated_scope();
        let Containment::Cgroups(root) = &containment else { unreachable!() };
        let run = |job: &str, limit: u64, bytes: u64| {
            let placement = containment.enter(job, limit).unwrap();
            let descriptor = placement.descriptor();
            let mut command = std::process::Command::new("sh");
            command.args(["-c", &format!("dd if=/dev/zero of=/dev/null bs={bytes} count=1 status=none")]);
            unsafe { command.pre_exec(move || Placement::join(descriptor)) };
            let status = command.status().unwrap();
            drop(placement);
            (status, containment.release(job))
        };
        let (status, usage) = run("fits", 256 << 20, 64 << 20);
        assert!(status.success() && !usage.out_of_memory);
        assert!(usage.peak.is_some_and(|peak| peak > 0 && peak <= 256 << 20));
        let (status, usage) = run("exceeds", 64 << 20, 512 << 20);
        assert!(!status.success() && usage.out_of_memory, "{status:?} {usage:?}");
        assert!(!Containment::cell(root, "exceeds").exists());
    }
}
