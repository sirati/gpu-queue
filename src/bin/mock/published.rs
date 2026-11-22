//! A file of the mock that another process reads, published whole: written in full under a staged name in its own
//! directory, then renamed to its final name. A rename within one directory is atomic, so a final name that exists
//! holds a complete file. A staged name begins with a dot, so it parses as no PID and as no sequence number, and no
//! reader takes it; a writer ended before its rename leaves only a staged file. The mock binary and `tests/common`
//! include this file by `#[path]`, and each uses part of it.
#![allow(dead_code)]
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// A file written whole under its staged name, not yet published.
pub struct Staged {
    path: PathBuf,
}

/// Stages `content` in `dir` as `.<pid>-<n>.tmp`, where `<n>` counts this process's stagings. Each chunk read is
/// written before the next is read.
pub fn stage(dir: &Path, mut content: impl std::io::Read) -> std::io::Result<Staged> {
    static STAGINGS: AtomicU64 = AtomicU64::new(0);
    let path = dir.join(format!(".{}-{}.tmp", std::process::id(), STAGINGS.fetch_add(1, Ordering::Relaxed)));
    let mut file = std::fs::File::create(&path)?;
    std::io::copy(&mut content, &mut file)?;
    Ok(Staged { path })
}

impl Staged {
    /// Publishes the staged file as `path`, replacing what is there.
    pub fn replace(&self, path: &Path) -> std::io::Result<()> {
        std::fs::rename(&self.path, path)
    }

    /// Publishes the staged file as `path` only if nothing is there (RENAME_NOREPLACE). A taken `path` gives
    /// `ErrorKind::AlreadyExists` and leaves the staged file, so the caller can try another name.
    pub fn create(&self, path: &Path) -> std::io::Result<()> {
        let (from, to) = (c_path(&self.path)?, c_path(path)?);
        // SAFETY: two NUL-terminated paths that outlive the call, which only reads them.
        let renamed = unsafe {
            libc::renameat2(libc::AT_FDCWD, from.as_ptr(), libc::AT_FDCWD, to.as_ptr(), libc::RENAME_NOREPLACE)
        };
        (renamed == 0).then_some(()).ok_or_else(std::io::Error::last_os_error)
    }
}

fn c_path(path: &Path) -> std::io::Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;
    std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{}: {e}", path.display())))
}
