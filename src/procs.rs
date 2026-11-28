//! Host process facts from world-readable /proc files.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::MetadataExt;

/// `_IO(0xb7, 0x2)`: a namespace's parent, as a new descriptor (ioctl_ns(2)).
const NS_GET_PARENT: u64 = 0xb702;

/// A pid namespace, as this process reads `/proc`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PidNamespace {
    /// The one this process reads `/proc` in: it holds every process `/proc` lists, each by the pid read there.
    /// Knowing it takes no namespace file, which only a process allowed to trace the other may open.
    Reading,
    /// One below it: its depth and its identity (the device and inode of its `ns/pid` file).
    Nested { depth: usize, identity: (u64, u64) },
}

impl PidNamespace {
    /// The pid namespace `pid` lives in.
    pub fn of(pid: u32) -> Option<PidNamespace> {
        match nspid(pid)?.len().checked_sub(1)? {
            0 => Some(PidNamespace::Reading),
            depth => Some(PidNamespace::Nested {
                depth,
                identity: identity(&namespace_file(pid)?)?,
            }),
        }
    }

    /// The pid this namespace gives the process `pid` that started at `birth`: none once that process has
    /// ended (the pid may be another process's by now), and none for a process outside this namespace.
    pub fn sees(&self, pid: u32, birth: u64) -> Option<u32> {
        let alive = || started(pid) == Some(birth);
        match *self {
            PidNamespace::Reading => alive().then_some(pid),
            PidNamespace::Nested {
                depth,
                identity: own,
            } => {
                let pids = nspid(pid).filter(|_| alive())?;
                let above = pids.len().checked_sub(1 + depth)?;
                let namespace = ancestor(namespace_file(pid)?, above)?;
                let seen = pids.get(depth).copied();
                seen.filter(|_| identity(&namespace) == Some(own) && alive())
            }
        }
    }
}

fn namespace_file(pid: u32) -> Option<File> {
    File::open(format!("/proc/{pid}/ns/pid")).ok()
}

/// The namespace `levels` above the one `namespace` refers to.
fn ancestor(namespace: File, levels: usize) -> Option<File> {
    (0..levels).try_fold(namespace, |namespace, _| {
        // SAFETY: NS_GET_PARENT on a namespace descriptor returns a new descriptor, or -1.
        let parent = unsafe { libc::ioctl(namespace.as_raw_fd(), NS_GET_PARENT as _) };
        // SAFETY: a descriptor the kernel just opened for this call, owned by nothing else.
        (parent >= 0).then(|| unsafe { File::from_raw_fd(parent) })
    })
}

fn identity(namespace: &File) -> Option<(u64, u64)> {
    namespace.metadata().ok().map(|m| (m.dev(), m.ino()))
}

/// pid -> parent pid, from `/proc/<pid>/stat`.
pub fn parents() -> HashMap<u32, u32> {
    processes().parents
}

/// Every process's parent and start time, each pair from one read of its stat file, so a process's place in
/// the tree and the identity it is known by belong to the same process.
#[derive(Debug, Default)]
pub struct Processes {
    pub parents: HashMap<u32, u32>,
    pub births: HashMap<u32, u64>,
}

pub fn processes() -> Processes {
    let mut out = Processes::default();
    let Ok(dir) = std::fs::read_dir("/proc") else {
        return out;
    };
    for e in dir.flatten() {
        let Some(pid) = e.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else {
            continue;
        };
        if let Some((ppid, birth)) = lineage(pid) {
            out.parents.insert(pid, ppid);
            out.births.insert(pid, birth);
        }
    }
    out
}

/// Why a process's fact could not be read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Unread {
    /// Proven: the kernel answered ESRCH or ENOENT for the process, or its pid now names a process of another birth.
    Gone,
    /// Any other failure, naming where it happened.
    Failed(String),
}

/// The one classification of an errno met while reading a process: only ESRCH and ENOENT prove it gone.
fn unread(at: &str, error: std::io::Error) -> Unread {
    match error.raw_os_error() {
        Some(libc::ESRCH | libc::ENOENT) => Unread::Gone,
        _ => Unread::Failed(format!("{at}: {error}")),
    }
}

/// The whole of the file at `path`, read into `buffer`: one read takes a /proc file whole and one finds its end, where
/// `fs::read_to_string` asks its size and then probes the unknown size of a /proc file in small growing reads.
pub fn read_whole<'b>(path: &std::path::Path, buffer: &'b mut Vec<u8>) -> std::io::Result<&'b [u8]> {
    use std::io::Read;
    let mut file = File::open(path)?;
    let mut filled = 0;
    loop {
        if filled == buffer.len() {
            buffer.resize((2 * buffer.len()).max(4096), 0);
        }
        match file.read(&mut buffer[filled..]) {
            Ok(0) => return Ok(&buffer[..filled]),
            Ok(read) => filled += read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

/// What `parse` takes from the fields of `/proc/<pid>/stat` after its command name, its state first, from one read.
fn stat_fields<T>(pid: u32, parse: impl FnOnce(&[&str]) -> Option<T>) -> Result<T, Unread> {
    let at = format!("/proc/{pid}/stat");
    let mut buffer = Vec::new();
    let read = read_whole(std::path::Path::new(&at), &mut buffer).map_err(|error| unread(&at, error))?;
    let stat = std::str::from_utf8(read).map_err(|error| Unread::Failed(format!("{at}: {error}")))?;
    let fields: Option<Vec<&str>> = stat.rfind(')').and_then(|end| stat.get(end + 2..)).map(|tail| tail.split_whitespace().collect());
    fields.and_then(|fields| parse(&fields)).ok_or_else(|| Unread::Failed(format!("{at}: unparsable")))
}

/// The parent and the start time of `pid`, from one read of `/proc/<pid>/stat`.
fn stat(pid: u32) -> Result<(u32, u64), Unread> {
    stat_fields(pid, |fields| Some((fields.get(1)?.parse().ok()?, fields.get(19)?.parse().ok()?)))
}

/// The start time of `pid` while it is not a zombie, from one read of `/proc/<pid>/stat`: what `started` and `alive`
/// together answer.
pub fn living(pid: u32) -> Option<u64> {
    stat_fields(pid, |fields| Some((!zombie(fields), fields.get(19)?.parse::<u64>().ok()?)))
        .ok()
        .and_then(|(living, birth)| living.then_some(birth))
}

fn zombie(fields: &[&str]) -> bool {
    fields.first().is_some_and(|state| state.starts_with('Z'))
}

/// The parent and the start time of `pid`, from one read of `/proc/<pid>/stat`.
fn lineage(pid: u32) -> Option<(u32, u64)> {
    stat(pid).ok()
}

pub fn parent(pid: u32) -> Option<u32> {
    lineage(pid).map(|(parent, _)| parent)
}

/// `NSpid:` of `/proc/<pid>/status`: the pid in each namespace, outermost first.
pub fn nspid(pid: u32) -> Option<Vec<u32>> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let line = status.lines().find(|l| l.starts_with("NSpid:"))?;
    line["NSpid:".len()..]
        .split_whitespace()
        .map(|p| p.parse().ok())
        .collect()
}

/// `VmHWM:` of `/proc/<pid>/status`: the largest resident set the process has had, in bytes.
pub fn high_water(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let line = status.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kib: u64 = line["VmHWM:".len()..].split_whitespace().next()?.parse().ok()?;
    kib.checked_mul(1024)
}

/// What one read of `/proc/<pid>/status` counts of a process, in bytes: kernel counters, no page-table walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resident {
    /// `RssAnon:`, its resident anonymous pages, each counted whole however many processes share it.
    pub anonymous: u64,
    /// `VmHWM:`, the largest resident set it has had.
    pub high_water: u64,
}

/// The resident counters of `pid`; none for a process gone or without an address space.
pub fn resident(pid: u32) -> Option<Resident> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let field = |name: &str| -> Option<u64> {
        let line = status.lines().find(|l| l.starts_with(name))?;
        line[name.len()..].split_whitespace().next()?.parse::<u64>().ok()?.checked_mul(1024)
    };
    Some(Resident { anonymous: field("RssAnon:")?, high_water: field("VmHWM:")? })
}

/// `Pss_Anon:` of `/proc/<pid>/smaps_rollup`, in bytes: its anonymous pages, a page its sharers share divided between
/// them. The kernel walks the process's page tables for it, a cost that grows with its resident set.
pub fn proportional_anonymous(pid: u32) -> Option<u64> {
    let text = std::fs::read_to_string(format!("/proc/{pid}/smaps_rollup")).ok()?;
    let kib: u64 = text.lines().find_map(|line| line.strip_prefix("Pss_Anon:")?.split_whitespace().next()?.parse().ok())?;
    kib.checked_mul(1024)
}

/// When the running system booted, in whole seconds since the Unix epoch: `btime` of `/proc/stat`.
pub fn booted_at() -> Option<u64> {
    std::fs::read_to_string("/proc/stat").ok()?.lines().find_map(|line| line.strip_prefix("btime ")?.trim().parse().ok())
}

/// Host pid of `parent`'s child whose innermost pid is `inner`.
pub fn child_by_inner_pid(parent: u32, inner: u32) -> Option<u32> {
    parents()
        .into_iter()
        .filter(|(_, p)| *p == parent)
        .map(|(pid, _)| pid)
        .find(|pid| nspid(*pid).and_then(|v| v.last().copied()) == Some(inner))
}

/// `root` and all its descendants.
pub fn tree(root: u32, parents: &HashMap<u32, u32>) -> HashSet<u32> {
    let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
    for (pid, ppid) in parents {
        children.entry(*ppid).or_default().push(*pid);
    }
    let mut out = HashSet::new();
    let mut stack = vec![root];
    while let Some(p) = stack.pop() {
        if out.insert(p) {
            stack.extend(children.get(&p).into_iter().flatten().copied());
        }
    }
    out
}

pub fn alive(pid: u32) -> bool {
    stat_fields(pid, |fields| Some(!zombie(fields))).unwrap_or(false)
}

pub fn username(uid: u32) -> Option<String> {
    let mut buf = vec![0u8; 4096];
    let mut pw: libc::passwd = unsafe { std::mem::zeroed() };
    let mut result: *mut libc::passwd = std::ptr::null_mut();
    // SAFETY: getpwuid_r with a caller-owned buffer.
    let rc = unsafe {
        libc::getpwuid_r(
            uid,
            &mut pw,
            buf.as_mut_ptr().cast(),
            buf.len(),
            &mut result,
        )
    };
    if rc != 0 || result.is_null() {
        return None;
    }
    // SAFETY: pw_name points into buf, NUL-terminated.
    let name = unsafe { std::ffi::CStr::from_ptr(pw.pw_name) };
    Some(name.to_string_lossy().into_owned())
}

/// Linux starttime is stable for one process, including across daemon restarts.
pub fn started(pid: u32) -> Option<u64> {
    lineage(pid).map(|(_, birth)| birth)
}

/// Kill a verified process tree using pidfds, never reused integer PIDs.
pub fn terminate_tree(root: u32, birth: u64, sig: i32) -> Result<(), String> {
    use std::os::fd::OwnedFd;
    if started(root) != Some(birth) {
        return Ok(());
    }
    let parents = parents();
    let mut targets = Vec::new();
    for pid in job_tree(root, &parents) {
        let before = started(pid);
        let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if raw < 0 {
            continue;
        }
        let descriptor = unsafe { OwnedFd::from_raw_fd(raw as i32) };
        if before.is_some() && before == started(pid) && started(root) == Some(birth) {
            targets.push(descriptor);
        }
    }
    for descriptor in targets {
        let rc = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                descriptor.as_raw_fd(),
                sig,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        };
        if rc < 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
            return Err(std::io::Error::last_os_error().to_string());
        }
    }
    Ok(())
}

/// The manager and its helpers never belong to a submitted job's GPU tree.
pub fn job_tree(root: u32, parents: &HashMap<u32, u32>) -> HashSet<u32> {
    let mut owned = tree(root, parents);
    for pid in tree(std::process::id(), parents) {
        owned.remove(&pid);
    }
    owned
}

/// A process as it was recorded: its pid and its start time, which no later process given the same pid shares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Process {
    pub pid: u32,
    pub birth: u64,
}

impl Process {
    /// The process `pid` names as this reads it, or why it cannot be read.
    pub fn read(pid: u32) -> Result<Process, Unread> {
        stat(pid).map(|(_, birth)| Process { pid, birth })
    }

    /// The process `pid` names as this reads it.
    pub fn now(pid: u32) -> Option<Process> {
        Process::read(pid).ok()
    }

    /// Whether its pid still names it.
    fn named(&self) -> bool {
        started(self.pid) == Some(self.birth)
    }

    /// Whether it still runs: its pid names it on both sides of the read that finds it has not ended, so that read
    /// was of this process.
    pub fn running(&self) -> bool {
        self.named() && alive(self.pid) && self.named()
    }

    /// A descriptor that refers to this process alone until it is waited for: opened before its pid is read to
    /// still name this birth, so it never refers to a later process given the same pid (Gone when it does).
    pub fn open(&self) -> Result<Pidfd, Unread> {
        let opened = pidfd_open(self.pid)?;
        let (_, birth) = stat(self.pid)?;
        (birth == self.birth).then_some(opened).ok_or(Unread::Gone)
    }

    /// A descriptor for this process, as `open` gives it.
    pub fn pidfd(&self) -> Option<Pidfd> {
        self.open().ok()
    }

    /// Its `0::` cgroup path: `open`, then the read of /proc/<pid>/cgroup, then `exists` through the descriptor. A pid
    /// cannot be reused while the process the descriptor refers to exists, zombie included, so the read was of it.
    pub fn cgroup(&self) -> Result<String, Unread> {
        let pidfd = self.open()?;
        let at = format!("/proc/{}/cgroup", self.pid);
        let lines = std::fs::read_to_string(&at).map_err(|error| unread(&at, error))?;
        let path = lines
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .map(str::to_owned)
            .ok_or_else(|| Unread::Failed(format!("{at}: no unified cgroup")))?;
        pidfd.exists()?;
        Ok(path)
    }

    /// Sends `sig` to this process alone; false once it has been waited for, whichever process its pid names by then.
    pub fn signal(&self, sig: i32) -> bool {
        self.pidfd().is_some_and(|pidfd| pidfd.signal(sig))
    }
}

/// A descriptor for whatever process `pid` names now.
fn pidfd_open(pid: u32) -> Result<Pidfd, Unread> {
    // SAFETY: pidfd_open returns a new descriptor, or -1.
    let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    (raw >= 0)
        // SAFETY: a descriptor the kernel just opened for this call, owned by nothing else.
        .then(|| Pidfd {
            descriptor: unsafe { std::os::fd::OwnedFd::from_raw_fd(raw as i32) },
            pid,
        })
        .ok_or_else(|| unread(&format!("pidfd_open({pid})"), std::io::Error::last_os_error()))
}

/// A descriptor for one process (pidfd_open(2)): what is sent through it reaches that process, or the process group
/// it leads, and never a later process given the same pid.
pub struct Pidfd {
    descriptor: std::os::fd::OwnedFd,
    /// The pid the process had when the descriptor was obtained, which the descriptor's errors name.
    pid: u32,
}

impl Pidfd {
    /// Sends `sig` to the process; false once it has been waited for.
    pub fn signal(&self, sig: i32) -> bool {
        self.send(sig, 0).is_ok()
    }

    /// Sends `sig` to the process group the process leads: the group it created, which its members keep after it
    /// has ended. False when it leads none or none of the group is left.
    pub fn signal_group(&self, sig: i32) -> bool {
        self.send(sig, libc::PIDFD_SIGNAL_PROCESS_GROUP).is_ok()
    }

    /// Whether the process still exists (a zombie does): Gone on ESRCH, Failed on any other error.
    pub fn exists(&self) -> Result<(), Unread> {
        self.send(0, 0).map_err(|error| unread(&format!("pidfd_send_signal({}, 0)", self.pid), error))
    }

    fn send(&self, sig: i32, scope: libc::c_uint) -> std::io::Result<()> {
        // SAFETY: pidfd_send_signal on a descriptor this value owns, with no siginfo.
        let rc = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.descriptor.as_raw_fd(),
                sig,
                std::ptr::null::<libc::siginfo_t>(),
                scope,
            )
        };
        (rc == 0).then_some(()).ok_or_else(std::io::Error::last_os_error)
    }
}

/// The process at the other end of a connected Unix socket: a descriptor for it, which holds the pid the descriptor
/// named when it was obtained, or why none was obtained.
pub struct Peer(Result<Pidfd, Unread>);

impl Peer {
    /// `SO_PEERPIDFD` of `socket`, then the descriptor's `Pid:` from `/proc/self/fdinfo/<fd>`: Gone on ESRCH or on
    /// `Pid: -1` (a peer already reaped), Failed on any other error and on `Pid: 0` (a peer outside this pid
    /// namespace).
    pub fn of(socket: std::os::fd::BorrowedFd<'_>) -> Peer {
        Peer(Self::obtained(socket))
    }

    fn obtained(socket: std::os::fd::BorrowedFd<'_>) -> Result<Pidfd, Unread> {
        let mut raw: libc::c_int = -1;
        let mut length = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        // SAFETY: getsockopt into a caller-owned int of the length it is given.
        let rc = unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERPIDFD,
                (&mut raw as *mut libc::c_int).cast(),
                &mut length,
            )
        };
        let descriptor = (rc == 0)
            // SAFETY: a descriptor the kernel just opened for this call, owned by nothing else.
            .then(|| unsafe { std::os::fd::OwnedFd::from_raw_fd(raw) })
            .ok_or_else(|| unread("SO_PEERPIDFD", std::io::Error::last_os_error()))?;
        let at = format!("/proc/self/fdinfo/{}", descriptor.as_raw_fd());
        let fdinfo = std::fs::read_to_string(&at).map_err(|error| unread(&at, error))?;
        let pid: i64 = fdinfo
            .lines()
            .find_map(|line| line.strip_prefix("Pid:"))
            .and_then(|pid| pid.trim().parse().ok())
            .ok_or_else(|| Unread::Failed(format!("{at}: no Pid: line")))?;
        match pid {
            -1 => Err(Unread::Gone),
            named => u32::try_from(named)
                .ok()
                .filter(|pid| *pid > 0)
                .map(|pid| Pidfd { descriptor, pid })
                .ok_or_else(|| Unread::Failed(format!("{at}: Pid: {named}, a process outside this pid namespace"))),
        }
    }

    /// The sender: the acquisition's error, or its birth read from /proc/<pid>/stat and then `Pidfd::exists` through
    /// the descriptor. A pid cannot be reused while the descriptor's process exists, zombie included, so the birth
    /// read was the sender's.
    pub fn process(&self) -> Result<Process, Unread> {
        let pidfd = self.0.as_ref().map_err(Unread::clone)?;
        let process = Process::read(pidfd.pid)?;
        pidfd.exists()?;
        Ok(process)
    }
}

#[cfg(test)]
impl Peer {
    /// pidfd_open(pid): the kernel object SO_PEERPIDFD gives, as a connection takes it at accept; ESRCH: Gone.
    pub fn opened(pid: u32) -> Peer {
        Peer(pidfd_open(pid))
    }

    pub fn failed(unread: Unread) -> Peer {
        Peer(Err(unread))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Child, Command};
    use std::time::{Duration, Instant};

    /// A file is read whole whatever the buffer held before and however many reads it takes.
    #[test]
    fn a_file_is_read_whole_into_a_reused_buffer() {
        let directory = std::env::temp_dir().join(format!("procs-read-whole-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("file");
        let long: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &long).unwrap();
        let mut buffer = Vec::new();
        assert_eq!(read_whole(&path, &mut buffer).unwrap(), &long[..], "(a) longer than the first buffer");
        std::fs::write(&path, b"short").unwrap();
        assert_eq!(read_whole(&path, &mut buffer).unwrap(), b"short", "(b) none of the longer content remains");
        let own = read_whole(std::path::Path::new("/proc/self/stat"), &mut buffer).unwrap().to_vec();
        assert!(own.starts_with(format!("{} (", std::process::id()).as_bytes()), "(c) a /proc file");
        assert!(read_whole(&directory.join("missing"), &mut buffer).is_err(), "(d) a missing file is an error");
        std::fs::remove_dir_all(&directory).unwrap();
    }

    /// A process lives with its birth until it is a zombie; one gone, or a pid never used, lives with none.
    #[test]
    fn a_process_lives_with_its_birth_until_it_is_a_zombie() {
        let own = std::process::id();
        assert_eq!(living(own), started(own), "(a) this process");
        assert!(living(own).is_some());
        let mut child = Command::new("true").spawn().unwrap();
        let pid = child.id();
        let birth = started(pid).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while alive(pid) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!((alive(pid), started(pid)), (false, Some(birth)), "(b) an unreaped exit is a zombie");
        assert_eq!(living(pid), None, "(b) a zombie does not live");
        child.wait().unwrap();
        assert_eq!(living(pid), None, "(c) a reaped process does not live");
        assert_eq!(living(u32::MAX), None, "(c) nor does a pid never used");
    }

    #[test]
    fn a_namespace_sees_its_own_process_while_it_lives_and_nothing_deeper_sees_it() {
        let own = std::process::id();
        let birth = started(own).unwrap();
        let namespace = PidNamespace::of(own).unwrap();
        assert_eq!(namespace, PidNamespace::Reading);
        assert_eq!(namespace.sees(own, birth), Some(own));
        assert_eq!(
            namespace.sees(own, birth + 1),
            None,
            "another process given the same pid"
        );
        let deeper = PidNamespace::Nested {
            depth: 1,
            identity: identity(&namespace_file(own).unwrap()).unwrap(),
        };
        assert_eq!(deeper.sees(own, birth), None);
        assert_eq!(namespace.sees(u32::MAX, birth), None);
    }

    #[test]
    fn the_reading_namespace_numbers_another_users_process_without_its_namespace_file() {
        let unreadable = processes().births.into_iter().find(|(pid, _)| {
            nspid(*pid).is_some_and(|pids| pids.len() == 1) && namespace_file(*pid).is_none()
        });
        let Some((pid, birth)) = unreadable else {
            eprintln!("no process here hides its namespace file; nothing to check");
            return;
        };
        assert_eq!(PidNamespace::of(pid), Some(PidNamespace::Reading));
        assert_eq!(PidNamespace::Reading.sees(pid, birth), Some(pid));
    }

    #[test]
    fn the_snapshot_gives_each_process_its_parent_and_start_time_together() {
        let own = std::process::id();
        let snapshot = processes();
        assert_eq!(snapshot.parents.get(&own).copied(), parent(own));
        assert_eq!(snapshot.births.get(&own).copied(), started(own));
        assert_eq!(snapshot.parents.len(), snapshot.births.len());
    }

    /// A process in a pid namespace of its own, and its pid as this process numbers it.
    fn nested() -> (Child, u32) {
        let unshared = Command::new("unshare")
            .args([
                "--user",
                "--map-root-user",
                "--pid",
                "--fork",
                "--kill-child",
                "sleep",
                "30",
            ])
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let child = parents()
                .into_iter()
                .find(|(_, parent)| *parent == unshared.id())
                .map(|(pid, _)| pid);
            match child {
                Some(pid) => return (unshared, pid),
                None if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(20)),
                None => panic!("unshare started no process"),
            }
        }
    }

    fn ends_within(process: Process, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while process.running() {
            if Instant::now() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        true
    }

    #[test]
    fn a_recorded_process_is_signalled_only_while_its_pid_names_it() {
        use std::os::unix::process::ExitStatusExt;
        let mut sleeper = Command::new("sleep").arg("30").spawn().unwrap();
        let recorded = Process::now(sleeper.id()).unwrap();
        let another = Process {
            birth: recorded.birth + 1,
            ..recorded
        };
        assert!(!another.running(), "another process given the same pid");
        assert!(!another.signal(libc::SIGKILL));
        assert!(sleeper.try_wait().unwrap().is_none());
        assert!(recorded.running());
        assert!(recorded.signal(libc::SIGKILL));
        assert_eq!(sleeper.wait().unwrap().signal(), Some(libc::SIGKILL));
        assert!(!recorded.running());
        assert!(!recorded.signal(libc::SIGKILL), "waited for");
    }

    /// A shell leading a group of its own, running `script`, and the process whose pid the script prints first.
    fn group_with_member(script: &str) -> (Child, Process) {
        use std::io::{BufRead, BufReader};
        use std::os::unix::process::CommandExt;
        let mut leader = Command::new("sh")
            .args(["-c", script])
            .process_group(0)
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(leader.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        (leader, Process::now(line.trim().parse().unwrap()).unwrap())
    }

    #[test]
    fn a_group_is_signalled_through_its_leader_even_after_the_leader_was_waited_for() {
        let (mut leader, member) = group_with_member("sleep 30 & echo $!");
        let pidfd = Process::now(leader.id()).unwrap().pidfd().unwrap();
        leader.wait().unwrap();
        assert!(!pidfd.signal(0), "the leader was waited for");
        assert!(member.running());
        assert!(pidfd.signal_group(libc::SIGKILL));
        assert!(ends_within(member, Duration::from_secs(10)));
    }

    #[test]
    fn a_process_that_leads_no_group_signals_none_and_spares_the_group_it_is_in() {
        let (mut leader, member) = group_with_member("sleep 30 & echo $!; wait");
        assert!(!member.pidfd().unwrap().signal_group(libc::SIGKILL));
        assert!(member.running());
        assert!(leader.try_wait().unwrap().is_none());
        let group = Process::now(leader.id()).unwrap().pidfd().unwrap();
        assert!(group.signal_group(libc::SIGKILL));
        leader.wait().unwrap();
        assert!(ends_within(member, Duration::from_secs(10)));
    }

    /// The `0::` line of this process's own cgroup file.
    fn own_cgroup() -> String {
        std::fs::read_to_string("/proc/self/cgroup")
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .unwrap()
            .to_owned()
    }

    #[test]
    fn a_cgroup_is_read_for_the_identity_that_holds_the_pid() {
        let mut sleeper = Command::new("sleep").arg("60").spawn().unwrap();
        let recorded = Process::read(sleeper.id()).unwrap();
        assert_eq!(recorded.cgroup(), Ok(own_cgroup()), "a child inherits its parent's cgroup");
        let another = Process {
            birth: recorded.birth + 1,
            ..recorded
        };
        assert_eq!(another.cgroup(), Err(Unread::Gone), "a birth read and found different");
        sleeper.kill().unwrap();
        sleeper.wait().unwrap();
        assert_eq!(recorded.cgroup(), Err(Unread::Gone), "ESRCH from pidfd_open");
        assert_eq!(Process::read(recorded.pid), Err(Unread::Gone), "ENOENT from the stat read");
    }

    #[test]
    #[ignore = "needs a process of another user, which a build sandbox does not have; run with --ignored"]
    fn a_refused_final_check_is_failed_not_gone() {
        let init = Process::read(1).unwrap();
        assert!(init.open().is_ok(), "pidfd_open needs no permission");
        let refused = init.cgroup();
        assert_eq!(
            refused,
            Err(Unread::Failed("pidfd_send_signal(1, 0): Operation not permitted (os error 1)".into())),
            "the final check failed, and the process is not gone"
        );
    }

    #[test]
    #[ignore = "needs unprivileged user namespaces; run with --ignored"]
    fn a_nested_namespace_numbers_its_own_processes_and_a_sibling_sees_none_of_them() {
        let ((mut a, inner), (mut b, sibling)) = (nested(), nested());
        let birth = started(inner).unwrap();
        let ours = PidNamespace::of(std::process::id()).unwrap();
        assert_eq!(ours.sees(inner, birth), Some(inner));
        assert_eq!(PidNamespace::of(inner).unwrap().sees(inner, birth), Some(1));
        assert_eq!(PidNamespace::of(sibling).unwrap().sees(inner, birth), None);
        let own = std::process::id();
        assert_eq!(
            PidNamespace::of(inner)
                .unwrap()
                .sees(own, started(own).unwrap()),
            None
        );
        for unshared in [&mut a, &mut b] {
            unshared.kill().unwrap();
            unshared.wait().unwrap();
        }
    }
}
