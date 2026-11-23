//! The user-mode instructions the calling thread retires while it runs some work: a count that load does not change,
//! for the tests' cost checks. One `perf_event_open(2)` counter of `PERF_COUNT_HW_INSTRUCTIONS` follows the calling
//! thread only (`pid 0, cpu -1`), excluding the kernel and the hypervisor. It never returns a guess: a counter that
//! cannot be opened, or that shared the PMU with other events and so was multiplexed, panics with its cause.

use std::future::Future;

/// What the counter read for one piece of work: the instructions retired, and the nanoseconds the counter was enabled
/// and running. Running below enabled means the count was scaled from a sample, and [`counted`] refuses it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Count {
    pub instructions: u64,
    pub enabled: u64,
    pub running: u64,
}

/// `work`'s result and the instructions the calling thread retired while it ran.
pub fn counted<R>(work: impl FnOnce() -> R) -> (R, Count) {
    let counter = Counter::open();
    counter.start();
    let result = work();
    (result, counter.stop())
}

/// `work`'s output and the instructions the calling thread retired while it was polled to completion. On a
/// current-thread runtime that is the work of every task the runtime polled meanwhile; work on other threads, such as
/// `spawn_blocking`'s, is not in the count.
pub async fn counted_async<R>(work: impl Future<Output = R>) -> (R, Count) {
    let counter = Counter::open();
    counter.start();
    let result = work.await;
    (result, counter.stop())
}

/// `struct perf_event_attr` up to `bp_type`/`config1`: the kernel's first published layout, `PERF_ATTR_SIZE_VER0`.
#[repr(C)]
struct Attributes {
    kind: u32,
    size: u32,
    config: u64,
    sample_period: u64,
    sample_type: u64,
    read_format: u64,
    flags: u64,
    wakeup_events: u32,
    bp_type: u32,
    config1: u64,
}

const PERF_TYPE_HARDWARE: u32 = 0;
const PERF_COUNT_HW_INSTRUCTIONS: u64 = 1;
const PERF_FORMAT_TOTAL_TIME_ENABLED: u64 = 1;
const PERF_FORMAT_TOTAL_TIME_RUNNING: u64 = 2;
const DISABLED: u64 = 1 << 0;
const EXCLUDE_KERNEL: u64 = 1 << 5;
const EXCLUDE_HV: u64 = 1 << 6;
const PERF_FLAG_FD_CLOEXEC: libc::c_ulong = 8;
const PERF_EVENT_IOC_ENABLE: libc::c_ulong = 0x2400;
const PERF_EVENT_IOC_DISABLE: libc::c_ulong = 0x2401;
const PERF_EVENT_IOC_RESET: libc::c_ulong = 0x2403;

struct Counter(std::os::fd::OwnedFd);

impl Counter {
    fn open() -> Counter {
        let attributes = Attributes {
            kind: PERF_TYPE_HARDWARE,
            size: std::mem::size_of::<Attributes>() as u32,
            config: PERF_COUNT_HW_INSTRUCTIONS,
            sample_period: 0,
            sample_type: 0,
            read_format: PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING,
            flags: DISABLED | EXCLUDE_KERNEL | EXCLUDE_HV,
            wakeup_events: 0,
            bp_type: 0,
            config1: 0,
        };
        // SAFETY: a valid attribute block of the size it declares; pid 0 and cpu -1 name the calling thread.
        let fd = unsafe {
            libc::syscall(libc::SYS_perf_event_open, &attributes as *const Attributes, 0, -1, -1, PERF_FLAG_FD_CLOEXEC)
        };
        if fd < 0 {
            let error = std::io::Error::last_os_error();
            let paranoid = std::fs::read_to_string("/proc/sys/kernel/perf_event_paranoid").unwrap_or_default();
            panic!("the instruction counter cannot be opened: {error} (perf_event_paranoid {})", paranoid.trim());
        }
        // SAFETY: the kernel returned this descriptor to this process, and nothing else owns it.
        Counter(unsafe { std::os::fd::FromRawFd::from_raw_fd(fd as i32) })
    }

    fn control(&self, request: libc::c_ulong) {
        // SAFETY: a perf event descriptor and one of its argument-free requests.
        let status = unsafe { libc::ioctl(std::os::fd::AsRawFd::as_raw_fd(&self.0), request as _, 0) };
        assert_eq!(status, 0, "perf event ioctl {request:#x}: {}", std::io::Error::last_os_error());
    }

    fn start(&self) {
        self.control(PERF_EVENT_IOC_RESET);
        self.control(PERF_EVENT_IOC_ENABLE);
    }

    fn stop(self) -> Count {
        self.control(PERF_EVENT_IOC_DISABLE);
        let mut values = [0u64; 3];
        // SAFETY: reads at most the buffer's own size into it.
        let read = unsafe {
            libc::read(std::os::fd::AsRawFd::as_raw_fd(&self.0), values.as_mut_ptr().cast(), std::mem::size_of_val(&values))
        };
        assert_eq!(read, std::mem::size_of_val(&values) as isize, "perf event read: {}", std::io::Error::last_os_error());
        let [instructions, enabled, running] = values;
        assert_eq!(
            running, enabled,
            "the instruction counter was multiplexed (running {running} ns of {enabled} ns enabled), so its count is a guess"
        );
        Count { instructions, enabled, running }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The same work counts the same, and twice the work counts about twice as much.
    #[test]
    fn a_fixed_loop_counts_a_stable_number_of_instructions() {
        let work = |n: u64| (0..n).fold(0u64, |sum, i| std::hint::black_box(sum.wrapping_add(i * i)));
        let (_, once) = counted(|| work(1_000_000));
        let (_, again) = counted(|| work(1_000_000));
        let (_, twice) = counted(|| work(2_000_000));
        assert!(once.instructions > 1_000_000, "{once:?}");
        assert!(once.instructions.abs_diff(again.instructions) * 100 < once.instructions, "{once:?} {again:?}");
        let ratio = twice.instructions as f64 / once.instructions as f64;
        assert!((1.9..2.1).contains(&ratio), "{once:?} {twice:?}");
    }
}
