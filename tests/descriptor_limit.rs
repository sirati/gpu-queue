//! A process's reads under a lowered descriptor limit fail, naming where; they never prove the process gone.
//! The parent runs this executable again as a child, which lowers only its own soft RLIMIT_NOFILE.
mod descriptors;

use descriptors::soft_limit;
use gpu_arbiter::procs::{Process, Unread};
use std::os::fd::AsRawFd;
use std::process::Command;

/// The sleeper's pid and birth, in the child's environment.
const SLEEPER: &str = "DESCRIPTOR_LIMIT_SLEEPER";

fn main() {
    match std::env::var(SLEEPER) {
        Ok(sleeper) => child(&sleeper),
        Err(_) => parent(),
    }
}

fn parent() {
    let mut sleeper = Command::new("sleep").arg("60").spawn().unwrap();
    let recorded = Process::read(sleeper.id()).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .env(SLEEPER, format!("{} {}", recorded.pid, recorded.birth))
        .output()
        .unwrap();
    let still = recorded.running();
    let _ = sleeper.kill();
    let _ = sleeper.wait();
    let printed = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "the child exited {}: {printed}{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(printed.lines().collect::<Vec<_>>(), ["open", "stat read", "restored"], "every case held");
    assert!(still, "the sleeper still ran after every case");
    println!("descriptor limit: 3 of 3 cases held");
}

fn child(sleeper: &str) {
    let (pid, birth) = sleeper.split_once(' ').unwrap();
    let recorded = Process {
        pid: pid.parse().unwrap(),
        birth: birth.parse().unwrap(),
    };
    let path = std::fs::read_to_string("/proc/self/cgroup")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .unwrap()
        .to_owned();
    let lowest = std::fs::File::open("/dev/null").unwrap().as_raw_fd() as u64;
    let exhausted = "Too many open files (os error 24)";
    let (stat, open) = (format!("/proc/{pid}/stat"), format!("pidfd_open({pid})"));

    let kept = soft_limit(lowest);
    assert_eq!(Process::read(recorded.pid), Err(Unread::Failed(format!("{stat}: {exhausted}"))), "open: the stat read");
    assert_eq!(recorded.open().err(), Some(Unread::Failed(format!("{open}: {exhausted}"))), "open: pidfd_open");
    assert_eq!(recorded.cgroup(), Err(Unread::Failed(format!("{open}: {exhausted}"))), "open: the cgroup read");
    println!("open");

    soft_limit(lowest + 1);
    assert_eq!(
        recorded.cgroup(),
        Err(Unread::Failed(format!("{stat}: {exhausted}"))),
        "stat read: the descriptor takes the last slot"
    );
    println!("stat read");

    soft_limit(kept);
    assert_eq!(recorded.cgroup(), Ok(path), "restored: the sleeper's path");
    println!("restored");
}
