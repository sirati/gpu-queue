//! Durable metadata. Every mutating operation goes through a `Store` handle.
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

// Concurrent fsyncs join one filesystem journal commit; the bound only limits
// blocking threads.
const CONCURRENT_SYNCS: usize = 64;

/// One durable operation's identity: what it does, and to what. Two operations on one file, such as a launch's
/// intent and its PID, differ.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Operation {
    kind: &'static str,
    subject: String,
}
impl Operation {
    pub fn new(kind: &'static str, subject: impl std::fmt::Display) -> Self {
        Self {
            kind,
            subject: subject.to_string(),
        }
    }
}

/// An operation held before it runs: it reports that it was entered, then waits for its release.
struct Gate {
    entered: oneshot::Sender<()>,
    released: oneshot::Receiver<()>,
}

/// The handle every durable write, removal, directory creation, permission change and directory sync of the service,
/// its engine, its host memory and the handover reaper goes through.
/// Each runs the store's operation below unchanged; a test may hold a named operation before it runs.
#[derive(Clone, Default)]
pub struct Store {
    holds: Arc<Mutex<BTreeMap<Operation, Gate>>>,
}

impl Store {
    async fn enter(&self, operation: &Operation) {
        let gate = self.holds.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).remove(operation);
        let Some(gate) = gate else {
            return;
        };
        let _ = gate.entered.send(());
        let _ = gate.released.await;
    }
    pub async fn write(&self, operation: Operation, path: &Path, value: &Value) -> Result<(), String> {
        self.enter(&operation).await;
        write(path, value).await
    }
    pub async fn write_all(&self, operation: Operation, entries: &[(&Path, &Value)]) -> Result<(), String> {
        self.enter(&operation).await;
        write_all(entries).await
    }
    /// Creates `directory` and its missing parents; an existing one is kept.
    pub async fn create_directory(&self, operation: Operation, directory: &Path) -> Result<(), String> {
        self.enter(&operation).await;
        create_directory(directory).await
    }
    /// Sets `path`'s permissions.
    pub async fn set_permissions(
        &self,
        operation: Operation,
        path: &Path,
        permissions: std::fs::Permissions,
    ) -> Result<(), String> {
        self.enter(&operation).await;
        set_permissions(path, permissions).await
    }
    pub async fn sync_directory(&self, operation: Operation, directory: &Path) -> Result<(), String> {
        self.enter(&operation).await;
        sync_directory(directory).await
    }
    /// Idempotent: an absent path counts as removed.
    pub async fn remove_all(&self, operation: Operation, paths: Vec<PathBuf>) -> Result<(), String> {
        self.enter(&operation).await;
        remove_all(paths).await
    }
    /// Strict: an absent file is an error.
    pub async fn remove_file(&self, operation: Operation, path: &Path) -> Result<(), String> {
        self.enter(&operation).await;
        remove_file(path).await
    }
    pub async fn discard_abandoned(&self, operation: Operation, directory: &Path) -> Result<(), String> {
        self.enter(&operation).await;
        discard_abandoned(directory).await
    }
}

/// A test's hold on one operation.
#[cfg(test)]
pub struct Hold {
    entered: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
}

#[cfg(test)]
impl Store {
    /// Holds `operation` the next time it runs.
    pub fn hold(&self, operation: Operation) -> Hold {
        let (entered, entered_receiver) = oneshot::channel();
        let (release, released) = oneshot::channel();
        self.holds.lock().unwrap().insert(operation, Gate { entered, released });
        Hold {
            entered: entered_receiver,
            release,
        }
    }
}

#[cfg(test)]
impl Hold {
    /// Returns once the held operation has been entered.
    pub async fn entered(&mut self) {
        (&mut self.entered).await.expect("the held operation was entered");
    }
    /// Lets the held operation run the call it was entered with.
    pub fn release(self) {
        let _ = self.release.send(());
    }
}

async fn write(path: &Path, value: &Value) -> Result<(), String> {
    write_all(&[(path, value)]).await
}
/// Every entry is synced before it is renamed into place, so a reader sees a
/// complete old or new file. Syncing a batch concurrently costs about one
/// commit, where syncing entry by entry cost one commit each.
async fn write_all(entries: &[(&Path, &Value)]) -> Result<(), String> {
    let mut directories = BTreeSet::new();
    let mut staged = Vec::with_capacity(entries.len());
    for (path, value) in entries {
        let directory = path.parent().ok_or("metadata needs a parent directory")?;
        if directories.insert(directory.to_path_buf()) {
            create_directory(directory).await?;
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
async fn create_directory(directory: &Path) -> Result<(), String> {
    tokio::fs::create_dir_all(directory)
        .await
        .map_err(|e| e.to_string())
}
async fn set_permissions(path: &Path, permissions: std::fs::Permissions) -> Result<(), String> {
    tokio::fs::set_permissions(path, permissions)
        .await
        .map_err(|e| e.to_string())
}
async fn sync_directory(directory: &Path) -> Result<(), String> {
    tokio::fs::File::open(directory)
        .await
        .map_err(|e| e.to_string())?
        .sync_all()
        .await
        .map_err(|e| e.to_string())
}
/// An absent path counts as removed, so an interrupted removal can be repeated.
async fn remove_all(paths: Vec<PathBuf>) -> Result<(), String> {
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
/// One unlink, whose result is returned as it is: an absent file is an error.
async fn remove_file(path: &Path) -> Result<(), String> {
    tokio::fs::remove_file(path).await.map_err(|e| e.to_string())
}
/// A writer that died between staging and rename leaves its staging file
/// behind. A live writer's, such as a handover predecessor's, is kept.
async fn discard_abandoned(directory: &Path) -> Result<(), String> {
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
/// The `.json` descriptors in `directory`, in path order. It only reads: the directory is prepared beforehand,
/// through the handle's `create_directory`.
pub async fn descriptors(directory: &Path) -> Result<Vec<PathBuf>, String> {
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

    fn scratch(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "compiler-store-{name}-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        std::fs::create_dir_all(root.join("directory")).unwrap();
        std::fs::write(root.join("present"), b"{}\n").unwrap();
        root
    }

    #[tokio::test]
    async fn a_strict_removal_fails_on_a_missing_file_and_an_idempotent_one_does_not() {
        let root = scratch("removals");
        let store = Store::default();
        let strict = |path: PathBuf| {
            let store = store.clone();
            async move { store.remove_file(Operation::new("strict", path.display()), &path).await }
        };
        strict(root.join("present")).await.unwrap();
        assert!(!root.join("present").exists());
        let not_found = std::fs::remove_file(root.join("present")).unwrap_err();
        assert_eq!(not_found.kind(), std::io::ErrorKind::NotFound);
        assert_eq!(strict(root.join("present")).await.unwrap_err(), not_found.to_string(), "formatted as today");
        let directory = strict(root.join("directory")).await.unwrap_err();
        assert_eq!(directory, std::fs::remove_file(root.join("directory")).unwrap_err().to_string());

        std::fs::write(root.join("present"), b"{}\n").unwrap();
        let idempotent = |paths: Vec<PathBuf>| store.remove_all(Operation::new("idempotent", "paths"), paths);
        idempotent(vec![root.join("present")]).await.unwrap();
        assert!(!root.join("present").exists());
        idempotent(vec![root.join("present")]).await.unwrap();
        let directory = idempotent(vec![root.join("directory")]).await.unwrap_err();
        assert!(directory.starts_with(&format!("{}: ", root.join("directory").display())), "{directory}");
        assert!(root.join("directory").is_dir());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn a_directory_is_created_through_the_handle_alone_and_descriptors_only_read() {
        let root = scratch("directories");
        let store = Store::default();
        let missing = root.join("missing").join("nested");
        let mut hold = store.hold(Operation::new("directory", "missing"));
        let creation = store.create_directory(Operation::new("directory", "missing"), &missing);
        let control = async {
            hold.entered().await;
            assert!(!root.join("missing").exists(), "a held creation has not run");
            hold.release();
        };
        let (created, ()) = tokio::join!(creation, control);
        created.unwrap();
        assert!(missing.is_dir());
        store.create_directory(Operation::new("directory", "missing"), &missing).await.unwrap();
        // A file where a directory belongs: its own error, formatted as today.
        let blocked = root.join("present").join("directory");
        let error = store.create_directory(Operation::new("directory", "blocked"), &blocked).await.unwrap_err();
        assert_eq!(error, std::fs::create_dir_all(&blocked).unwrap_err().to_string());
        let absent = root.join("absent");
        let error = descriptors(&absent).await.unwrap_err();
        assert_eq!(error, std::fs::read_dir(&absent).unwrap_err().to_string());
        assert!(!absent.exists(), "reading descriptors creates nothing");
        std::fs::remove_dir_all(root).unwrap();
    }

    fn mode(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }
    fn private() -> std::fs::Permissions {
        use std::os::unix::fs::PermissionsExt;
        std::fs::Permissions::from_mode(0o700)
    }

    #[tokio::test]
    async fn a_held_permission_change_reports_its_entry_and_runs_once_released() {
        use std::os::unix::fs::PermissionsExt;
        let root = scratch("permissions");
        let store = Store::default();
        let directory = root.join("directory");
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut hold = store.hold(Operation::new("permissions", "directory"));
        let change = store.set_permissions(Operation::new("permissions", "directory"), &directory, private());
        let control = async {
            hold.entered().await;
            assert_eq!(mode(&directory), 0o755, "a held change has not run");
            hold.release();
        };
        let (changed, ()) = tokio::time::timeout(std::time::Duration::from_secs(10), async { tokio::join!(change, control) })
            .await
            .expect("the change is held and released through the handle");
        changed.unwrap();
        assert_eq!(mode(&directory), 0o700);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn a_permission_change_returns_its_own_error_formatted_as_today() {
        let root = scratch("permission-error");
        let store = Store::default();
        let absent = root.join("absent");
        let error = store.set_permissions(Operation::new("permissions", "absent"), &absent, private()).await.unwrap_err();
        assert_eq!(error, std::fs::set_permissions(&absent, private()).unwrap_err().to_string());
        assert!(!absent.exists(), "a failed change creates nothing");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn a_held_operation_reports_its_entry_and_resumes_the_same_call_with_its_own_result() {
        let root = scratch("holds");
        let store = Store::default();
        let path = root.join("present");
        let intent = Operation::new("intent", "job");
        let mut hold = store.hold(Operation::new("removal", "job"));
        // Another operation on the same file is not held.
        store.write(intent, &path, &serde_json::json!({"pid": null})).await.unwrap();
        let removal = store.remove_file(Operation::new("removal", "job"), &path);
        let control = async {
            hold.entered().await;
            assert!(path.exists(), "a held removal has not run");
            std::fs::remove_file(&path).unwrap();
            hold.release();
        };
        let (removed, ()) = tokio::join!(removal, control);
        let not_found = std::fs::remove_file(&path).unwrap_err().to_string();
        assert_eq!(removed.unwrap_err(), not_found, "the strict result is kept, never replaced by success");
        // A hold applies once.
        std::fs::write(&path, b"{}\n").unwrap();
        store.remove_file(Operation::new("removal", "job"), &path).await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
