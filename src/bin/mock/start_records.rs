//! NInfer's start records: one file per start in a directory, named by its sequence number and holding the start's
//! argv as a JSON array. The mock binary includes this file for its writer half and `tests/common` for its reader
//! half, each beside `published.rs`, through which a record is published whole: a final name that exists names a
//! whole record.
#![allow(dead_code)]
use std::path::Path;

use super::published;

/// Records one start: `argv`, published under the first free sequence number in `dir`.
pub fn record(dir: &Path, argv: &[String]) -> std::io::Result<()> {
    publish(dir, serde_json::to_vec(argv).expect("serializable").as_slice())
}

/// Stages `record` in `dir` and publishes it under the first free decimal name, counting from 0, which is its
/// sequence number. A taken name is passed over: each name is tried once, and the walk ends at the first free one.
pub fn publish(dir: &Path, record: impl std::io::Read) -> std::io::Result<()> {
    let staged = published::stage(dir, record)?;
    let mut sequence = 0u64;
    loop {
        match staged.create(&dir.join(sequence.to_string())) {
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => sequence += 1,
            published => return published,
        }
    }
}

/// Every published record in `dir`, in sequence order: the names that parse as a sequence number, each a whole JSON
/// array of strings. A published record that does not parse is `InvalidData`, with its name.
pub fn read(dir: &Path) -> std::io::Result<Vec<Vec<String>>> {
    let mut records = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let Some(sequence) = entry.file_name().to_str().and_then(|name| name.parse::<u64>().ok()) else {
            continue;
        };
        records.push((sequence, entry.path()));
    }
    records.sort_by_key(|(sequence, _)| *sequence);
    records
        .into_iter()
        .map(|(sequence, path)| {
            serde_json::from_slice(&std::fs::read(&path)?).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, format!("start record {sequence}: {e}"))
            })
        })
        .collect()
}
