//! An authority's outage: which authority did not answer, the operation it did not answer, and why. An outage
//! stops work to be resumed later; it is never a verdict. This is also the one hand-off format by which a
//! front-end tells whoever launched it why it exits unavailable: `write` and `read` are its only codec.
use crate::saved_file::Optional;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outage {
    /// The authority that did not answer, such as the compile service.
    pub source: String,
    /// What was asked of it: its wire operation.
    pub operation: String,
    /// The original reason, as the caller observed it.
    pub reason: String,
}
impl Outage {
    pub fn new(source: &str, operation: &str, reason: impl Into<String>) -> Self {
        Self {
            source: source.to_owned(),
            operation: operation.to_owned(),
            reason: reason.into(),
        }
    }
    /// Records the outage at `path` by one durable write: a sibling written and synced, renamed into place,
    /// and its directory synced. A reader finds the whole record or none.
    pub fn write(&self, path: &Path) -> Result<(), String> {
        let directory = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let name = path
            .file_name()
            .ok_or_else(|| format!("outage file {} names no file", path.display()))?;
        let temporary = directory.join(format!(
            ".{}.{}.tmp",
            name.to_string_lossy(),
            std::process::id()
        ));
        let mut bytes = serde_json::to_vec(self).map_err(|e| e.to_string())?;
        bytes.push(b'\n');
        let written = (|| {
            let mut file = std::fs::File::create(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&temporary, path)?;
            std::fs::File::open(directory)?.sync_all()
        })();
        if written.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        written.map_err(|e| format!("outage file {}: {e}", path.display()))
    }
    /// The outage recorded at `path`, or none when nothing was recorded there.
    pub fn read(path: &Path) -> Result<Option<Self>, String> {
        match crate::saved_file::read(path).decode(path, |bytes| serde_json::from_slice(&bytes)) {
            Optional::Present(outage) => Ok(Some(outage)),
            Optional::Absent => Ok(None),
            Optional::Failed(error) => Err(format!("outage file {error}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh directory of this test process for `purpose`.
    fn fresh_directory(purpose: &str) -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "outage-{purpose}-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        std::fs::create_dir(&directory).unwrap();
        directory
    }

    #[test]
    fn a_written_outage_reads_back_whole_and_an_absent_one_reads_as_none() {
        let directory = fresh_directory("codec");
        let path = directory.join("attempt.outage.json");
        assert_eq!(Outage::read(&path), Ok(None));
        let outage = Outage::new("compile service", "host_memory_register", "no answer within 60 s");
        outage.write(&path).unwrap();
        assert_eq!(Outage::read(&path), Ok(Some(outage)));
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1, "no temporary file stays");
        std::fs::write(&path, b"{\"source\":").unwrap();
        assert!(Outage::read(&path).is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn an_outage_file_that_cannot_be_read_or_decoded_is_an_error_naming_it() {
        let directory = fresh_directory("unreadable");
        let path = directory.join("attempt.outage.json");
        std::fs::create_dir(&path).unwrap();
        assert_eq!(
            Outage::read(&path),
            Err(format!("outage file {}: Is a directory (os error 21)", path.display())),
            "1. a directory at the path"
        );
        std::fs::remove_dir(&path).unwrap();
        std::fs::write(&path, b"{\"source\":").unwrap();
        let error = serde_json::from_slice::<Outage>(b"{\"source\":").unwrap_err();
        assert_eq!(
            Outage::read(&path),
            Err(format!("outage file {}: {error}", path.display())),
            "2. bytes that do not decode"
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
}
impl std::fmt::Display for Outage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.reason)
    }
}
