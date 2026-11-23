//! Readings of the POSIX clocks the crate uses. This is the one place that calls `clock_gettime`; callers convert the
//! reading into the unit they keep.
use std::time::Duration;

/// The kernel's monotonic clock: time since boot, never set back.
pub fn monotonic() -> Duration {
    read(libc::CLOCK_MONOTONIC)
}

/// A failed read panics: a clock that reads zero would corrupt every monotonic deadline. The monotonic clock always
/// exists on Linux, so the readings production takes never reach the panic.
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
