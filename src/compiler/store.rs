//! Durable metadata. Only the compiler actor calls mutating operations.
use serde_json::Value;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

pub async fn write(path: &Path, value: &Value) -> Result<(), String> {
    let directory = path.parent().ok_or("metadata needs a parent directory")?;
    tokio::fs::create_dir_all(directory)
        .await
        .map_err(|e| e.to_string())?;
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    let mut file = tokio::fs::File::create(&temporary)
        .await
        .map_err(|e| e.to_string())?;
    let mut bytes = serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?;
    bytes.push(b'\n');
    file.write_all(&bytes).await.map_err(|e| e.to_string())?;
    file.sync_all().await.map_err(|e| e.to_string())?;
    drop(file);
    tokio::fs::rename(&temporary, path)
        .await
        .map_err(|e| e.to_string())?;
    tokio::fs::File::open(directory)
        .await
        .map_err(|e| e.to_string())?
        .sync_all()
        .await
        .map_err(|e| e.to_string())
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
