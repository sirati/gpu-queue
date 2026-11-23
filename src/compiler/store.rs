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
    /// What the operation does.
    #[cfg(test)]
    pub fn kind(&self) -> &'static str {
        self.kind
    }
}

/// The operations a store entered, in the order it entered them: a test's record of the durable work that leaves the
/// calling thread.
#[derive(Clone, Default)]
pub struct Entered(Arc<Mutex<Vec<Operation>>>);
impl Entered {
    /// The operations entered since the last take.
    #[cfg(test)]
    pub fn take(&self) -> Vec<Operation> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner()))
    }
    fn add(&self, operation: &Operation) {
        self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).push(operation.clone());
    }
}

/// How far a failed durable write got.
#[derive(Debug, PartialEq)]
pub enum Failure {
    /// No entry was renamed into place: every destination holds what it held.
    Unpublished(String),
    /// An entry was renamed into place, then a later rename or a directory sync failed: every later reader sees the
    /// new bytes of every renamed entry, and whether they survive a crash of the host is unknown.
    Unsynced(String),
}
impl std::fmt::Display for Failure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::Unpublished(message) | Failure::Unsynced(message) => formatter.write_str(message),
        }
    }
}
impl From<Failure> for String {
    fn from(failure: Failure) -> String {
        failure.to_string()
    }
}

/// An operation held before it runs: it reports that it was entered, then waits for its release.
struct Gate {
    entered: oneshot::Sender<()>,
    released: oneshot::Receiver<()>,
}

/// What a publication published: its record's modification time in seconds since the Unix epoch, read through the
/// staged file's descriptor after its data sync and before its rename, which keeps it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Published {
    pub modified: f64,
}

/// The handle every durable write, removal, directory creation, permission change and directory sync of the service,
/// its engine, its host memory and the handover reaper goes through.
/// Each runs the store's operation below unchanged; a test may hold a named operation before it runs, or record the
/// operations entered. Once a publication could not be made durable the store has stopped, and every later operation
/// refuses before it runs.
#[derive(Clone, Default)]
pub struct Store {
    holds: Arc<Mutex<BTreeMap<Operation, Gate>>>,
    entered: Option<Entered>,
    stopped: Arc<std::sync::OnceLock<String>>,
}

impl Store {
    /// The operation's record and its hold, if a test set them, then the stop's refusal.
    async fn enter(&self, operation: &Operation) -> Result<(), String> {
        self.entered.iter().for_each(|entered| entered.add(operation));
        let gate = self.holds.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).remove(operation);
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            let _ = gate.released.await;
        }
        self.serving()
    }
    /// Ok until a publication stopped the store; then its message, with which every later operation refuses.
    pub fn serving(&self) -> Result<(), String> {
        self.stopped.get().map_or(Ok(()), |stopped| Err(stopped.clone()))
    }
    pub async fn write(&self, operation: Operation, path: &Path, value: &Value) -> Result<(), Failure> {
        self.enter(&operation).await.map_err(Failure::Unpublished)?;
        write(path, value).await
    }
    pub async fn write_all(&self, operation: Operation, entries: &[(&Path, &Value)]) -> Result<(), Failure> {
        self.enter(&operation).await.map_err(Failure::Unpublished)?;
        write_all(entries).await
    }
    /// One record of state the service answers from: written; after `Unsynced`, written again under `<kind>`
    /// `resave <subject>`, a fresh publication whose own syncs retry; when that fails too, the store stops.
    pub async fn publish(&self, operation: Operation, path: &Path, value: &Value) -> Result<Published, Failure> {
        let first = match self.published(&operation, path, value).await {
            Err(Failure::Unsynced(first)) => first,
            done => return done,
        };
        let resave = Operation::new(operation.kind, format!("resave {}", operation.subject));
        self.published(&resave, path, value).await.map_err(|second| {
            let stopped =
                format!("state was published but could not be synced: {first}; saving it again: {second}; the service stops");
            let _ = self.stopped.set(stopped.clone());
            Failure::Unsynced(stopped)
        })
    }
    /// One publication of one record, through its operation's hold and the stop's refusal.
    async fn published(&self, operation: &Operation, path: &Path, value: &Value) -> Result<Published, Failure> {
        self.enter(operation).await.map_err(Failure::Unpublished)?;
        let modified = written(&[(path, value)]).await?;
        Ok(Published { modified: modified[0] })
    }
    /// Creates `directory` and its missing parents; an existing one is kept.
    pub async fn create_directory(&self, operation: Operation, directory: &Path) -> Result<(), String> {
        self.enter(&operation).await?;
        create_directory(directory).await
    }
    /// Sets `path`'s permissions.
    pub async fn set_permissions(
        &self,
        operation: Operation,
        path: &Path,
        permissions: std::fs::Permissions,
    ) -> Result<(), String> {
        self.enter(&operation).await?;
        set_permissions(path, permissions).await
    }
    pub async fn sync_directory(&self, operation: Operation, directory: &Path) -> Result<(), String> {
        self.enter(&operation).await?;
        sync_directory(directory).await
    }
    /// Idempotent: an absent path counts as removed.
    pub async fn remove_all(&self, operation: Operation, paths: Vec<PathBuf>) -> Result<(), String> {
        self.enter(&operation).await?;
        remove_all(paths).await
    }
    /// Strict: an absent file is an error.
    pub async fn remove_file(&self, operation: Operation, path: &Path) -> Result<(), String> {
        self.enter(&operation).await?;
        remove_file(path).await
    }
    pub async fn discard_abandoned(&self, operation: Operation, directory: &Path) -> Result<(), String> {
        self.enter(&operation).await?;
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
    /// A store that records every operation it enters, and its record.
    pub fn recording() -> (Store, Entered) {
        let entered = Entered::default();
        (Store { entered: Some(entered.clone()), ..Store::default() }, entered)
    }
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

/// EACCES on every read through one saved path.
#[cfg(test)]
pub struct Unreadable {
    path: PathBuf,
    kept: PathBuf,
}

#[cfg(test)]
impl Unreadable {
    /// Faults `path`; asserts the fault, which root bypasses.
    pub fn at(path: &Path) -> Unreadable {
        use std::os::unix::fs::PermissionsExt;
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "compiler-store-unreadable-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let kept = directory.join(path.file_name().expect("a saved file has a name"));
        std::fs::rename(path, &kept).expect("the saved file moves to the fault's directory on the same filesystem");
        std::os::unix::fs::symlink(&kept, path).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o600)).unwrap();
        for (call, error) in [("metadata", std::fs::metadata(path).err()), ("read", std::fs::read(path).err())] {
            assert_eq!(
                error.map(|e| e.kind()),
                Some(std::io::ErrorKind::PermissionDenied),
                "the fault is in place: {call} through {} fails with EACCES; a root user, or a filesystem that ignores \
                 permission bits, cannot run this fixture",
                path.display()
            );
        }
        Unreadable {
            path: path.to_path_buf(),
            kept,
        }
    }
    /// Restores the file; asserts nothing replaced the link.
    pub fn lift(self) {
        use std::os::unix::fs::PermissionsExt;
        let directory = self.kept.parent().expect("the fault's directory");
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(
            std::fs::read_link(&self.path).ok(),
            Some(self.kept.clone()),
            "{} is still the fault's link: nothing was published over it",
            self.path.display()
        );
        std::fs::rename(&self.kept, &self.path).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }
}

/// A directory's permission bits set for a test, restored when dropped.
#[cfg(test)]
pub struct Mode {
    directory: PathBuf,
    kept: u32,
}

#[cfg(test)]
impl Mode {
    /// Sets `directory`'s mode; asserts a user other than root, which bypasses permission bits.
    pub fn set(directory: &Path, mode: u32) -> Mode {
        use std::os::unix::fs::PermissionsExt;
        // SAFETY: geteuid has no preconditions.
        assert_ne!(unsafe { libc::geteuid() }, 0, "root bypasses permission bits and cannot run this fixture");
        let kept = std::fs::metadata(directory).unwrap().permissions().mode() & 0o7777;
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(mode)).unwrap();
        Mode {
            directory: directory.to_path_buf(),
            kept,
        }
    }
}

#[cfg(test)]
impl Drop for Mode {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&self.directory, std::fs::Permissions::from_mode(self.kept));
    }
}

async fn write(path: &Path, value: &Value) -> Result<(), Failure> {
    write_all(&[(path, value)]).await
}
async fn write_all(entries: &[(&Path, &Value)]) -> Result<(), Failure> {
    written(entries).await.map(drop)
}
/// A modification time in seconds since the Unix epoch: the one conversion of a publication's and of a saved
/// record's time.
fn seconds(time: std::io::Result<std::time::SystemTime>) -> Result<f64, String> {
    time.map_err(|e| e.to_string())?
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_secs_f64())
        .map_err(|e| e.to_string())
}
/// Every entry is synced before it is renamed into place, so a reader sees a
/// complete old or new file. Syncing a batch concurrently costs about one
/// commit, where syncing entry by entry cost one commit each. Returns each entry's modification time, read
/// through the descriptor it synced, in entry order.
async fn written(entries: &[(&Path, &Value)]) -> Result<Vec<f64>, Failure> {
    let mut directories = BTreeSet::new();
    let mut staged = Vec::with_capacity(entries.len());
    for (path, value) in entries {
        let directory = path
            .parent()
            .ok_or_else(|| Failure::Unpublished("metadata needs a parent directory".into()))?;
        if directories.insert(directory.to_path_buf()) {
            create_directory(directory).await.map_err(Failure::Unpublished)?;
        }
        let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
        let mut bytes = serde_json::to_vec_pretty(value).map_err(|e| Failure::Unpublished(e.to_string()))?;
        bytes.push(b'\n');
        tokio::fs::write(&temporary, bytes)
            .await
            .map_err(|e| Failure::Unpublished(e.to_string()))?;
        staged.push((temporary, path.to_path_buf()));
    }
    let mut modified = vec![0.0; staged.len()];
    let indexed: Vec<(usize, PathBuf)> = staged.iter().map(|(temporary, _)| temporary.clone()).enumerate().collect();
    for chunk in indexed.chunks(CONCURRENT_SYNCS) {
        let mut syncs = tokio::task::JoinSet::new();
        for (index, temporary) in chunk.iter().cloned() {
            syncs.spawn_blocking(move || -> Result<(usize, f64), String> {
                let file = std::fs::File::open(temporary).map_err(|e| e.to_string())?;
                file.sync_all().map_err(|e| e.to_string())?;
                Ok((index, seconds(file.metadata().and_then(|metadata| metadata.modified()))?))
            });
        }
        while let Some(synced) = syncs.join_next().await {
            let (index, time) = synced
                .map_err(|e| Failure::Unpublished(e.to_string()))?
                .map_err(Failure::Unpublished)?;
            modified[index] = time;
        }
    }
    // How far the batch got: once an entry is renamed into place, every later failure leaves it published.
    let mut reached: fn(String) -> Failure = Failure::Unpublished;
    for (temporary, path) in &staged {
        tokio::fs::rename(temporary, path)
            .await
            .map_err(|e| reached(e.to_string()))?;
        reached = Failure::Unsynced;
    }
    for directory in directories {
        sync_directory(&directory).await.map_err(reached)?;
    }
    Ok(modified)
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
/// ENOENT alone: no file at the path.
fn absent(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound
}
/// One path's error, as "<path>: <error>".
fn named(path: &Path, error: impl std::fmt::Display) -> String {
    format!("{}: {error}", path.display())
}
/// An absent path counts as removed, so an interrupted removal can be repeated.
async fn remove_all(paths: Vec<PathBuf>) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        for path in paths {
            if let Err(error) = std::fs::remove_file(&path) {
                if !absent(&error) {
                    return Err(named(&path, error));
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
/// The store's one parse of saved bytes.
fn value(bytes: &[u8]) -> Result<Value, String> {
    serde_json::from_slice(bytes).map_err(|e| e.to_string())
}
pub async fn read(path: &Path) -> Result<Value, String> {
    value(&tokio::fs::read(path).await.map_err(|e| e.to_string())?)
}
/// What one read of a possibly absent file found: the store's JSON value.
pub type Optional = crate::saved_file::Optional<Value>;
/// One read, with no existence check first.
pub async fn optional(path: &Path) -> Optional {
    crate::saved_file::read_async(path).await.decode(path, |bytes| value(&bytes))
}
/// A saved record's modification time, converted as a publication's is: what restore reads of a group record.
pub fn modified(path: &Path) -> Result<f64, String> {
    seconds(std::fs::metadata(path).and_then(|metadata| metadata.modified()))
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

    /// A `Failed` outcome's message; otherwise `case` fails.
    fn failed(outcome: Optional, case: &str) -> String {
        match outcome {
            Optional::Failed(message) => message,
            other => panic!("{case}: {other:?} where Failed is expected"),
        }
    }

    #[tokio::test]
    async fn an_optional_read_tells_an_absent_file_from_a_failed_read() {
        let root = scratch("optional");
        let saved = root.join("a.json");
        std::fs::write(&saved, br#"{"a":1}"#).unwrap();
        assert_eq!(optional(&saved).await, Optional::Present(serde_json::json!({"a":1})), "(a) a saved file");
        let missing = root.join("missing.json");
        assert_eq!(optional(&missing).await, Optional::Absent, "(b) no file at the path");
        let through = saved.join("x");
        let message = failed(optional(&through).await, "(c) a path through a file, ENOTDIR");
        assert!(message.starts_with(&format!("{}: ", through.display())), "(c) {message}");
        let unparsable = root.join("bad.json");
        std::fs::write(&unparsable, b"{").unwrap();
        let parse = serde_json::from_slice::<Value>(b"{").unwrap_err();
        assert_eq!(
            failed(optional(&unparsable).await, "(d) a file that does not parse"),
            format!("{}: {parse}", unparsable.display()),
            "(d)"
        );
        let directory = root.join("dir.json");
        std::fs::create_dir(&directory).unwrap();
        failed(optional(&directory).await, "(e) a directory, EISDIR");
        let fault = Unreadable::at(&saved);
        assert_eq!(
            failed(optional(&saved).await, "(f) a read fault, EACCES"),
            format!("{}: Permission denied (os error 13)", saved.display()),
            "(f)"
        );
        fault.lift();
        assert_eq!(optional(&saved).await, Optional::Present(serde_json::json!({"a":1})), "(f) once lifted");
        let not_found = std::fs::read(&missing).unwrap_err().to_string();
        assert_eq!(read(&missing).await.unwrap_err(), not_found, "(g) read's error, formatted as today");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn a_write_reports_how_far_it_published() {
        let root = scratch("published");
        let directory = root.join("directory");
        let saved = directory.join("a.json");
        std::fs::write(&saved, b"{\"a\": 0}\n").unwrap();
        let before = std::fs::read(&saved).unwrap();
        let store = Store::default();
        let denied = "Permission denied (os error 13)".to_owned();

        let unwritable = Mode::set(&directory, 0o500);
        let staged = store.write(Operation::new("test", "a"), &saved, &serde_json::json!({"a": 1})).await;
        drop(unwritable);
        assert_eq!(staged, Err(Failure::Unpublished(denied.clone())), "(a) the staging write fails before any rename");
        assert_eq!(std::fs::read(&saved).unwrap(), before, "(a) a.json is unchanged");

        let unreadable = Mode::set(&directory, 0o300);
        let renamed = store.write(Operation::new("test", "a"), &saved, &serde_json::json!({"a": 2})).await;
        drop(unreadable);
        assert_eq!(renamed, Err(Failure::Unsynced(denied)), "(b) the rename succeeds and the directory open fails");
        assert_eq!(read(&saved).await.unwrap(), serde_json::json!({"a": 2}), "(b) a.json holds the new value");
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1, "(b) no staging file remains");

        let blocked = directory.join("b.json");
        std::fs::create_dir(&blocked).unwrap();
        let (three, four) = (serde_json::json!({"a": 3}), serde_json::json!({"a": 4}));
        let later = store.write_all(Operation::new("test", "batch"), &[(&saved, &three), (&blocked, &three)]).await;
        assert!(matches!(later, Err(Failure::Unsynced(_))), "(c) a rename fails after an earlier one: {later:?}");
        assert_eq!(read(&saved).await.unwrap(), three, "(c) a.json holds the new value");
        assert!(blocked.is_dir(), "(c) b.json is still a directory");
        let first = store.write_all(Operation::new("test", "batch"), &[(&blocked, &four), (&saved, &four)]).await;
        assert!(matches!(first, Err(Failure::Unpublished(_))), "(c) the first rename fails: {first:?}");
        assert_eq!(read(&saved).await.unwrap(), three, "(c) a.json is unchanged");

        for failure in [Failure::Unpublished("m".into()), Failure::Unsynced("m".into())] {
            assert_eq!(String::from(failure), "m", "(d) a failure converts to the message it carries");
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn a_publication_is_published_again_once_and_otherwise_stops_the_store() {
        let root = scratch("publication");
        let directory = root.join("directory");
        let saved = directory.join("a.json");
        std::fs::write(&saved, b"{\"a\": 0}\n").unwrap();
        let within = std::time::Duration::from_secs(10);
        let store = Store::default();

        let mut hold = store.hold(Operation::new("test", "resave a"));
        let unreadable = Mode::set(&directory, 0o300);
        let one = serde_json::json!({"a": 1});
        let publication = store.publish(Operation::new("test", "a"), &saved, &one);
        let control = async {
            tokio::time::timeout(within, hold.entered()).await.expect("(a) the re-publication is entered within 10 s");
            drop(unreadable);
            hold.release();
        };
        let (published, ()) = tokio::join!(publication, control);
        let published = published.expect("(a) the re-publication publishes");
        assert_eq!(read(&saved).await.unwrap(), one, "(a) a.json holds the new value");
        let modified = std::fs::metadata(&saved).unwrap().modified();
        assert_eq!(Ok(published.modified), seconds(modified), "(a) the time a later reader finds");
        assert_eq!(store.serving(), Ok(()), "(a) the store still serves");

        let kept_off = Mode::set(&directory, 0o300);
        let two = serde_json::json!({"a": 2});
        let message = match store.publish(Operation::new("test", "a"), &saved, &two).await {
            Err(Failure::Unsynced(message)) => message,
            other => panic!("(b) {other:?} where Unsynced is expected"),
        };
        assert!(message.contains("could not be synced") && message.contains("the service stops"), "(b) {message}");
        assert_eq!(store.serving(), Err(message.clone()), "(b) serving refuses with the stop's message");
        let later = store.write(Operation::new("test", "a"), &saved, &serde_json::json!({"a": 3})).await;
        assert_eq!(later, Err(Failure::Unpublished(message.clone())), "(b) a later write is refused before it runs");
        let removal = store.remove_all(Operation::new("test", "removal"), vec![saved.clone()]).await;
        drop(kept_off);
        assert_eq!(read(&saved).await.unwrap(), two, "(b) a.json still holds the stopping publication's value");
        assert_eq!(removal, Err(message), "(b) a removal is refused");
        assert!(saved.exists(), "(b) the file remains");

        let fresh = Store::default();
        let mut never = fresh.hold(Operation::new("test", "resave a"));
        let unwritable = Mode::set(&directory, 0o500);
        let refused = fresh.publish(Operation::new("test", "a"), &saved, &serde_json::json!({"a": 4})).await;
        drop(unwritable);
        assert!(matches!(refused, Err(Failure::Unpublished(_))), "(c) {refused:?}");
        assert!(never.entered.try_recv().is_err(), "(c) the re-publication is never entered");
        assert_eq!(fresh.serving(), Ok(()), "(c) the store still serves");
        std::fs::remove_dir_all(root).unwrap();
    }
}
