//! Host process facts from world-readable /proc files.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::MetadataExt;

/// `_IO(0xb7, 0x2)`: a namespace's parent, as a new descriptor (ioctl_ns(2)).
const NS_GET_PARENT: u64 = 0xb702;

/// A pid namespace: its depth below the one this process reads `/proc` in, and its identity (the device and
/// inode of its `ns/pid` file).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PidNamespace {
    depth: usize,
    identity: (u64, u64),
}

impl PidNamespace {
    /// The pid namespace `pid` lives in.
    pub fn of(pid: u32) -> Option<PidNamespace> {
        Some(PidNamespace {
            depth: nspid(pid)?.len().checked_sub(1)?,
            identity: identity(&namespace_file(pid)?)?,
        })
    }

    /// The pid this namespace gives the process `pid` that started at `birth`: none once that process has
    /// ended (the pid may be another process's by now), and none for a process outside this namespace.
    pub fn sees(&self, pid: u32, birth: u64) -> Option<u32> {
        let alive = || started(pid) == Some(birth);
        let pids = nspid(pid).filter(|_| alive())?;
        let above = pids.len().checked_sub(1 + self.depth)?;
        let namespace = ancestor(namespace_file(pid)?, above)?;
        let seen = pids.get(self.depth).copied();
        seen.filter(|_| identity(&namespace) == Some(self.identity) && alive())
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
    let mut out = HashMap::new();
    let Ok(dir) = std::fs::read_dir("/proc") else {
        return out;
    };
    for e in dir.flatten() {
        let Some(pid) = e.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else {
            continue;
        };
        if let Some(ppid) = parent(pid) {
            out.insert(pid, ppid);
        }
    }
    out
}

pub fn parent(pid: u32) -> Option<u32> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rest = &stat[stat.rfind(')')? + 2..];
    rest.split_whitespace().nth(1)?.parse().ok()
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
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|s| s.get(s.rfind(')')? + 2..).map(|r| !r.starts_with('Z')))
        .unwrap_or(false)
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
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat[stat.rfind(')')? + 2..]
        .split_whitespace()
        .nth(19)?
        .parse()
        .ok()
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Child, Command};
    use std::time::{Duration, Instant};

    #[test]
    fn a_namespace_sees_its_own_process_while_it_lives_and_nothing_deeper_sees_it() {
        let own = std::process::id();
        let birth = started(own).unwrap();
        let namespace = PidNamespace::of(own).unwrap();
        assert_eq!(namespace.sees(own, birth), Some(own));
        assert_eq!(
            namespace.sees(own, birth + 1),
            None,
            "another process given the same pid"
        );
        let deeper = PidNamespace {
            depth: namespace.depth + 1,
            ..namespace
        };
        assert_eq!(deeper.sees(own, birth), None);
        assert_eq!(namespace.sees(u32::MAX, birth), None);
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
