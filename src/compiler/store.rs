//! Durable metadata. Only the compiler actor calls mutating operations.
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

// Concurrent fsyncs join one filesystem journal commit; the bound only limits
// blocking threads.
const CONCURRENT_SYNCS: usize = 64;

pub async fn write(path: &Path, value: &Value) -> Result<(), String> {
    write_all(&[(path, value)]).await
}
/// Every entry is synced before it is renamed into place, so a reader sees a
/// complete old or new file. Syncing a batch concurrently costs about one
/// commit, where syncing entry by entry cost one commit each.
pub async fn write_all(entries: &[(&Path, &Value)]) -> Result<(), String> {
    let mut directories = BTreeSet::new();
    let mut staged = Vec::with_capacity(entries.len());
    for (path, value) in entries {
        let directory = path.parent().ok_or("metadata needs a parent directory")?;
        if directories.insert(directory.to_path_buf()) {
            tokio::fs::create_dir_all(directory)
                .await
                .map_err(|e| e.to_string())?;
        }
        let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
        let mut bytes = serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?;
        bytes.push(b'\n');
        tokio::fs::write(&temporary, bytes)
            .await
            .map_err(|e| e.to_string())?;
        staged.push((temporary, path.to_path_buf()));
    }
    for chunk in staged.chunks(CONCURRENT_SYNCS) {
        let mut syncs = tokio::task::JoinSet::new();
        for (temporary, _) in chunk {
            let temporary = temporary.clone();
            syncs.spawn_blocking(move || std::fs::File::open(temporary)?.sync_all());
        }
        while let Some(synced) = syncs.join_next().await {
            synced
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
        }
    }
    for (temporary, path) in &staged {
        tokio::fs::rename(temporary, path)
            .await
            .map_err(|e| e.to_string())?;
    }
    for directory in directories {
        sync_directory(&directory).await?;
    }
    Ok(())
}
pub async fn sync_directory(directory: &Path) -> Result<(), String> {
    tokio::fs::File::open(directory)
        .await
        .map_err(|e| e.to_string())?
        .sync_all()
        .await
        .map_err(|e| e.to_string())
}
/// An absent path counts as removed, so an interrupted removal can be repeated.
pub async fn remove_all(paths: Vec<PathBuf>) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        for path in paths {
            if let Err(error) = std::fs::remove_file(&path) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    return Err(format!("{}: {error}", path.display()));
                }
            }
        }
        Ok(())
    })
    .await
    .map_err(|e| e.to_string())?
}
/// A writer that died between staging and rename leaves its staging file
/// behind. A live writer's, such as a handover predecessor's, is kept.
pub async fn discard_abandoned(directory: &Path) -> Result<(), String> {
    let mut entries = tokio::fs::read_dir(directory)
        .await
        .map_err(|e| e.to_string())?;
    let mut abandoned = Vec::new();
    while let Some(entry) = entries.next_entry().await.map_err(|e| e.to_string())? {
        let path = entry.path();
        if path.extension().is_some_and(|s| s == "tmp")
            && path
                .file_stem()
                .map(Path::new)
                .and_then(Path::extension)
                .and_then(|pid| pid.to_str()?.parse().ok())
                .is_some_and(|pid| !crate::procs::alive(pid))
        {
            abandoned.push(path);
        }
    }
    remove_all(abandoned).await
}
pub async fn read(path: &Path) -> Result<Value, String> {
    serde_json::from_slice(&tokio::fs::read(path).await.map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())
}
pub async fn descriptors(directory: &Path) -> Result<Vec<PathBuf>, String> {
    tokio::fs::create_dir_all(directory)
        .await
        .map_err(|e| e.to_string())?;
    let mut entries = tokio::fs::read_dir(directory)
        .await
        .map_err(|e| e.to_string())?;
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await.map_err(|e| e.to_string())? {
        if entry.path().extension().is_some_and(|s| s == "json") {
            paths.push(entry.path());
        }
    }
    paths.sort();
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn a_batch_is_published_complete_without_staging_files() {
        let root = std::env::temp_dir().join(format!(
            "compiler-store-batch-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        let values: Vec<(PathBuf, Value)> = (0..100)
            .map(|i| {
                (
                    root.join("results").join(format!("{i}.json")),
                    serde_json::json!({"index": i}),
                )
            })
            .collect();
        let entries: Vec<_> = values
            .iter()
            .map(|(path, value)| (path.as_path(), value))
            .collect();
        write_all(&entries).await.unwrap();
        for (path, value) in &values {
            assert_eq!(&read(path).await.unwrap(), value);
        }
        assert_eq!(
            std::fs::read_dir(root.join("results")).unwrap().count(),
            100,
            "no staging file remains"
        );
        write_all(&[]).await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn only_staging_files_of_dead_writers_are_discarded() {
        let root = std::env::temp_dir().join(format!(
            "compiler-store-abandoned-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let mut writer = std::process::Command::new("true").spawn().unwrap();
        let dead = writer.id();
        writer.wait().unwrap();
        let abandoned = root.join(format!("job.{dead}.tmp"));
        let live = root.join(format!("job.{}.tmp", std::process::id()));
        let published = root.join("job.json");
        for path in [&abandoned, &live, &published] {
            std::fs::write(path, b"{}\n").unwrap();
        }
        discard_abandoned(&root).await.unwrap();
        assert!(!abandoned.exists());
        assert!(live.exists() && published.exists());
        std::fs::remove_dir_all(root).unwrap();
    }
}
