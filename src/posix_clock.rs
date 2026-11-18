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

/// A failed read panics: a clock that reads zero would pass a CPU-time bound and corrupt every monotonic deadline.
/// Both clocks always exist on Linux, so the panic is never reached there.
fn read(clock: libc::clockid_t) -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: valid out pointer.
    let status = unsafe { libc::clock_gettime(clock, &mut ts) };
    assert_eq!(status, 0, "clock_gettime({clock}): {}", std::io::Error::last_os_error());
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}
