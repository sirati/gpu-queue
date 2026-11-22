//! A saved file that may be absent: one read, three outcomes. Nothing checks existence before the read. ENOENT alone
//! is absence, and every other error is a failure. What the bytes mean stays with each reader.
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

/// What one read of a saved file that may be absent found.
#[derive(Debug, PartialEq)]
pub enum Optional<T> {
    /// Read: its bytes, or what its reader decoded from them.
    Present(T),
    /// ENOENT alone: nothing is saved there.
    Absent,
    /// Any other read error, or the reader's decoding error, as "<path>: <error>".
    Failed(String),
}

impl<T> Optional<T> {
    /// Decodes a present value. A decoding error is a failure, named by `path`.
    pub fn decode<U, E: std::fmt::Display>(self, path: &Path, decode: impl FnOnce(T) -> Result<U, E>) -> Optional<U> {
        match self {
            Optional::Present(value) => decode(value).map_or_else(|error| Optional::Failed(named(path, error)), Optional::Present),
            Optional::Absent => Optional::Absent,
            Optional::Failed(error) => Optional::Failed(error),
        }
    }
}

/// One blocking read.
pub fn read(path: &Path) -> Optional<Vec<u8>> {
    found(path, std::fs::read(path))
}

/// One read on tokio's blocking pool.
pub async fn read_async(path: &Path) -> Optional<Vec<u8>> {
    found(path, tokio::fs::read(path).await)
}

fn found(path: &Path, read: std::io::Result<Vec<u8>>) -> Optional<Vec<u8>> {
    match read {
        Ok(bytes) => Optional::Present(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Optional::Absent,
        Err(error) => Optional::Failed(named(path, error)),
    }
}

fn named(path: &Path, error: impl std::fmt::Display) -> String {
    format!("{}: {error}", path.display())
}

/// A read fault on one saved file that leaves it writable: mode 0200, so an open for reading fails with EACCES and
/// an open for writing succeeds.
#[cfg(test)]
pub struct WriteOnly {
    path: PathBuf,
}

#[cfg(test)]
impl WriteOnly {
    /// Sets the mode, then asserts that `std::fs::read(path)` fails with `PermissionDenied`. A root user bypasses the
    /// mode; the assertion then fails, naming that precondition.
    pub fn at(path: &Path) -> WriteOnly {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o200)).unwrap();
        assert_eq!(
            std::fs::read(path).err().map(|e| e.kind()),
            Some(std::io::ErrorKind::PermissionDenied),
            "the fault is in place: reading {} fails with EACCES; a root user, or a filesystem that ignores permission \
             bits, cannot run this fixture",
            path.display()
        );
        WriteOnly {
            path: path.to_path_buf(),
        }
    }

    /// Restores mode 0600.
    pub fn lift(self) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A fresh directory under the system's temporary directory, named by this process and a counter, removed when it
    /// drops, also when the test fails.
    struct Scratch(PathBuf);

    impl Scratch {
        /// The cases (a) to (e) laid out: the file `a` holding `x\n` and the directory `e`.
        fn laid_out() -> Scratch {
            use std::sync::atomic::{AtomicU64, Ordering};
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "saved-file-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&dir).unwrap();
            std::fs::write(dir.join("a"), b"x\n").unwrap();
            std::fs::create_dir(dir.join("e")).unwrap();
            Scratch(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Each case of a read: its step, its path in `dir` and the outcome every read must give there, written out.
    fn cases(dir: &Path) -> Vec<(&'static str, PathBuf, Optional<Vec<u8>>)> {
        let failed = |path: &Path, error: &str| Optional::Failed(format!("{}: {error}", path.display()));
        vec![
            ("1. (a) a present file", dir.join("a"), Optional::Present(b"x\n".to_vec())),
            ("2. (b) a missing file", dir.join("b"), Optional::Absent),
            ("3. (c) a missing parent", dir.join("missing/c"), Optional::Absent),
            ("4. (d) a path through a file", dir.join("a/d"), failed(&dir.join("a/d"), "Not a directory (os error 20)")),
            ("5. (e) a directory", dir.join("e"), failed(&dir.join("e"), "Is a directory (os error 21)")),
        ]
    }

    /// The outcome of step 6 while the fault is in place.
    fn denied(path: &Path) -> Optional<Vec<u8>> {
        Optional::Failed(format!("{}: Permission denied (os error 13)", path.display()))
    }

    #[test]
    fn a_blocking_read_tells_a_present_file_an_absent_one_and_a_failed_read_apart() {
        let dir = Scratch::laid_out();
        for (step, path, outcome) in cases(&dir.0) {
            assert_eq!(read(&path), outcome, "{step}");
        }
        let a = dir.0.join("a");
        let fault = WriteOnly::at(&a);
        assert_eq!(read(&a), denied(&a), "6. a file that cannot be read");
        fault.lift();
        assert_eq!(read(&a), Optional::Present(b"x\n".to_vec()), "6. the lifted fault leaves the file");
    }

    #[tokio::test]
    async fn a_read_on_tokios_blocking_pool_tells_the_same_outcomes_apart() {
        let dir = Scratch::laid_out();
        for (step, path, outcome) in cases(&dir.0) {
            assert_eq!(read_async(&path).await, outcome, "{step}");
        }
        let a = dir.0.join("a");
        let fault = WriteOnly::at(&a);
        assert_eq!(read_async(&a).await, denied(&a), "6. a file that cannot be read");
        fault.lift();
        assert_eq!(read_async(&a).await, Optional::Present(b"x\n".to_vec()), "6. the lifted fault leaves the file");
    }

    #[test]
    fn a_decoding_error_is_a_failure_named_by_its_path_and_absence_stays_absence() {
        let p = Path::new("/saved/x");
        let decode = |b: Vec<u8>| serde_json::from_slice::<serde_json::Value>(&b);
        assert_eq!(Optional::Present(b"1".to_vec()).decode(p, decode), Optional::Present(json!(1)), "1. it decodes");
        let error = serde_json::from_slice::<serde_json::Value>(b"{").unwrap_err();
        assert_eq!(
            Optional::Present(b"{".to_vec()).decode(p, decode),
            Optional::Failed(format!("/saved/x: {error}")),
            "2. a decoding error is a failure named by its path"
        );
        assert_eq!(Optional::<Vec<u8>>::Absent.decode(p, decode), Optional::Absent, "3. absence stays absence");
        assert_eq!(
            Optional::<Vec<u8>>::Failed("m".into()).decode(p, decode),
            Optional::Failed("m".into()),
            "4. a failure stays as it is"
        );
    }
}
