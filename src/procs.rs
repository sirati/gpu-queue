//! Host process facts from world-readable /proc files.

use std::collections::{HashMap, HashSet};

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
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
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
