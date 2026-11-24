//! The service's heap. glibc keeps memory the service freed in its arena, at the arena's high-water mark; the
//! service hands it back to the system.

/// Blocks of 128 KiB and more are mapped and unmapped on their own. A fixed threshold stops glibc from raising it
/// after each large free, which would keep later large transient buffers in the arena. Called once, before the
/// service starts any thread.
pub fn configure() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    // SAFETY: mallopt only tunes glibc's allocator, and no other thread exists yet.
    unsafe {
        libc::mallopt(libc::M_MMAP_THRESHOLD, 128 << 10);
    }
}

/// Returns the heap's free pages to the system; one line on standard error when that gave back at least 64 MiB.
pub fn trim() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        let own = std::process::id();
        let resident = || crate::procs::resident(own).map_or(0, |resident| resident.anonymous);
        let before = resident();
        // SAFETY: malloc_trim only releases free memory of glibc's arenas.
        unsafe {
            libc::malloc_trim(0);
        }
        let returned = before.saturating_sub(resident());
        if returned >= 64 << 20 {
            eprintln!("compile-queue: returned {} MiB of free heap to the system", returned >> 20);
        }
    }
}
