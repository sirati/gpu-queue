//! This process's soft descriptor limit, which the standalone descriptor tests lower in the children they run.
/// Sets this process's soft descriptor limit; returns the one it replaced.
pub fn soft_limit(soft: u64) -> u64 {
    // SAFETY: getrlimit and setrlimit with a caller-owned rlimit.
    let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
    assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) }, 0);
    let kept = limit.rlim_cur;
    limit.rlim_cur = soft;
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) }, 0);
    kept
}
