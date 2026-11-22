//! A command that runs until its test releases it: the one held-command owner of the Rust tests. A held command has
//! no lifetime of its own; it ends when its release file exists, or when its test's teardown ends it.
//!
//! The file uses only `std`, so the library's unit tests and each integration crate (through `#[path]`) compile the
//! same meaning. The waiting is the caller's program; this file reads no environment and starts no process.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

/// The words of a command that runs until the path appended as its last word exists. The caller owns this program.
pub struct Waiter {
    pub words: Vec<String>,
}

/// The held commands one fixture made in `directory`, numbered from 0.
pub struct Holds {
    directory: PathBuf,
    count: AtomicUsize,
}

impl Holds {
    /// Creates `directory`, where the release files of these held commands go.
    pub fn new(directory: PathBuf) -> Holds {
        std::fs::create_dir_all(&directory).unwrap_or_else(|e| panic!("creating {}: {e}", directory.display()));
        Holds { directory, count: AtomicUsize::new(0) }
    }

    /// The next held command: `waiter`'s words, then the release file `<directory>/<number>.release`.
    pub fn held(&self, waiter: Waiter) -> Held {
        let number = self.count.fetch_add(1, Ordering::Relaxed);
        let release = self.release_file(number);
        let mut argv = waiter.words;
        argv.push(release.to_string_lossy().into_owned());
        Held { argv, release }
    }

    /// Writes the release file of every held command made here, released ones again included.
    pub fn release_all(&self) {
        (0..self.count.load(Ordering::Relaxed)).for_each(|number| write_release(&self.release_file(number)));
    }

    fn release_file(&self, number: usize) -> PathBuf {
        self.directory.join(format!("{number}.release"))
    }
}

/// One held command.
pub struct Held {
    argv: Vec<String>,
    release: PathBuf,
}

impl Held {
    pub fn argv(&self) -> &[String] {
        &self.argv
    }

    /// The same words, each single-quoted for a shell and joined by spaces.
    pub fn shell(&self) -> String {
        self.argv.iter().map(|word| format!("'{}'", word.replace('\'', "'\\''"))).collect::<Vec<_>>().join(" ")
    }

    /// Writes this command's release file.
    pub fn release(&self) {
        write_release(&self.release);
    }
}

/// An empty release file; writing it again changes nothing.
fn write_release(path: &Path) {
    std::fs::write(path, b"").unwrap_or_else(|e| panic!("releasing {}: {e}", path.display()));
}
