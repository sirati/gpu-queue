//! Readings of the POSIX clocks the crate uses. This is the one place that calls `clock_gettime`; callers convert the
//! reading into the unit they keep.
use std::time::Duration;

/// The kernel's monotonic clock: time since boot, never set back.
pub fn monotonic() -> Duration {
    read(libc::CLOCK_MONOTONIC)
}

/// The CPU time the calling thread has used.
pub fn thread_cpu() -> Duration {
    read(libc::CLOCK_THREAD_CPUTIME_ID)
}

/// Both clocks always exist on Linux, so the call's status is not consulted, as before this module existed.
fn read(clock: libc::clockid_t) -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: valid out pointer.
    unsafe { libc::clock_gettime(clock, &mut ts) };
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}
