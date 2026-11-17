//! What the admission front-ends share: their exit statuses, the cancel file that stops one while it waits,
//! and the hand-over to the program it admits, which keeps the front-end's pid.
use std::ffi::OsString;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;

pub const USAGE: u8 = 2;
/// An authority is unreachable or lacks a capability the front-end needs: an outage, never a verdict.
pub const UNAVAILABLE: u8 = 75;
/// The cancel file appeared while the front-end waited.
pub const CANCELLED: u8 = 125;
/// The admitted program could not be executed.
pub const NOT_EXECUTED: u8 = 127;
/// A told place was registered again untold after gpu-switch lost it.
pub const PLACE_LOST: u8 = 74;

/// How a front-end that holds a place ends when the place's session failed.
pub fn place_exit(error: &crate::lease::PlaceError) -> u8 {
    match error {
        crate::lease::PlaceError::Unavailable(_) => UNAVAILABLE,
        crate::lease::PlaceError::Interrupted(_) => CANCELLED,
        crate::lease::PlaceError::Lost => PLACE_LOST,
        crate::lease::PlaceError::Refused(_) => 1,
    }
}

/// The campaign's launch-stop marker: once it exists, a waiting front-end gives up.
pub struct CancelFile(Option<PathBuf>);
impl CancelFile {
    pub fn new(path: Option<PathBuf>) -> Self {
        Self(path)
    }
    pub fn requested(&self) -> bool {
        self.0.as_ref().is_some_and(|path| path.exists())
    }
}

/// Executes `argv` in this process, with `variables` set; returns only if that fails.
pub fn execute(argv: &[OsString], variables: &[(&str, &str)]) -> std::io::Error {
    std::process::Command::new(&argv[0])
        .args(&argv[1..])
        .envs(variables.iter().copied())
        .exec()
}
