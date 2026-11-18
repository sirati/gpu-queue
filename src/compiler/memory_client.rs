//! Native host-memory claims. No Python admission state or process wrappers. Every exchange of a claim's own
//! operations is a scheduling snapshot (`Deadline::snapshot`); its teardown shares one cleanup deadline.
use super::client::{blocking, CancelToken, Client, Deadline, Failure};
use super::model::Purpose;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

/// Claims above this are in the large class: admission may delay them indefinitely, and a GPU place waiting
/// on one parks.
pub const LARGE_REQUEST: u64 = super::contract::LARGE_REQUEST;

/// How long a claim's final cleanup may take in all: release, a failed acquisition's release and the abandon
/// of a dropped claim share it. Past it nothing more is sent; the service withdraws a claim whose owner and
/// workers are gone, so process lifetime stays the fallback.
pub const CLEANUP_PATIENCE: Duration = Duration::from_secs(10);

/// The environment variable through which a front-end hands its granted claim to the program it executes.
pub const HANDOVER: &str = "HOST_MEMORY_CLAIM";

/// A granted claim as a front-end hands it on.
#[derive(Debug, Serialize, Deserialize)]
pub struct Handover {
    pub claim_id: String,
    pub memory_bytes: u64,
    pub state_dir: PathBuf,
}
impl Handover {
    pub fn of_environment() -> Result<Self, String> {
        let text = std::env::var(HANDOVER).map_err(|_| format!("{HANDOVER} is not set"))?;
        serde_json::from_str(&text).map_err(|e| format!("{HANDOVER}: {e}"))
    }
}

/// Where a waiting claim reports its wait: the GPU place its holder has been told to prepare for.
pub trait PreparationWait {
    /// Why preparation waits, or an empty reason once it no longer does. `parked` holds only while a large
    /// claim waits, in a process that holds nothing else.
    fn preparation_wait(&mut self, reason: &str, parked: bool) -> Result<(), String>;
}

/// A claim acquired without a GPU place reports its wait to nobody.
pub struct Unplaced;
impl PreparationWait for Unplaced {
    fn preparation_wait(&mut self, _reason: &str, _parked: bool) -> Result<(), String> {
        Ok(())
    }
}

pub struct HostMemoryClaim {
    client: Client,
    id: String,
    bytes: u64,
    label: String,
    priority: i8,
    purpose: Option<Purpose>,
    owner: u32,
    attempted: bool,
    checked: bool,
    granted: bool,
    released: bool,
    /// Fixed when the claim's final cleanup begins; every later cleanup step spends what remains of it.
    cleanup: Option<Deadline>,
}
impl HostMemoryClaim {
    /// `purpose` none is unknown, what every gen-16 client declares.
    pub fn new(
        client: Client,
        bytes: u64,
        label: String,
        priority: i8,
        purpose: Option<Purpose>,
    ) -> Result<Self, String> {
        if bytes == 0 {
            return Err("host memory claim must be positive bytes".into());
        }
        if std::env::var_os("COMPILE_QUEUE_JOB").is_some() {
            return Err(
                "compilations cannot acquire preparation RAM or wait for another admission".into(),
            );
        }
        let id = std::fs::read_to_string("/proc/sys/kernel/random/uuid")
            .map_err(|e| e.to_string())?
            .trim()
            .to_owned();
        Ok(Self {
            client,
            id,
            bytes,
            label: label.chars().take(200).collect(),
            priority,
            purpose,
            owner: std::process::id(),
            attempted: false,
            checked: false,
            granted: false,
            released: false,
            cleanup: None,
        })
    }
    /// The granted claim a front-end handed on, now held by this process: checked to be granted, owned by
    /// this process and of the stated bytes.
    pub async fn inherited(handover: &Handover) -> Result<Self, Failure> {
        let client = Client::new(&handover.state_dir)?;
        let status = client
            .request(
                json!({"op":"host_memory_status","claim_id":handover.claim_id}),
                Deadline::snapshot(),
            )
            .await?;
        let own = std::process::id();
        if status["state"] != "granted"
            || status["owner_pid"] != own
            || status["memory_bytes"] != handover.memory_bytes
        {
            return Err(Failure::Refused(format!(
                "inherited host memory claim {} is not a granted claim of {} bytes owned by pid {own}: {status}",
                handover.claim_id, handover.memory_bytes
            )));
        }
        Ok(Self {
            client,
            id: handover.claim_id.clone(),
            bytes: handover.memory_bytes,
            label: status["label"].as_str().unwrap_or("").to_owned(),
            priority: super::party::priority(&status).map_err(Failure::Refused)?,
            purpose: serde_json::from_value(status["purpose"].clone())
                .map_err(|e| Failure::Refused(e.to_string()))?,
            owner: own,
            attempted: true,
            checked: true,
            granted: true,
            released: false,
            cleanup: None,
        })
    }
    /// What a front-end hands to the program it executes.
    pub fn handover(&self) -> Handover {
        Handover {
            claim_id: self.id.clone(),
            memory_bytes: self.bytes,
            state_dir: self.client.state().to_owned(),
        }
    }
    /// Gives the claim to `pid`, a live descendant of this process that owns no claim. The service saves the
    /// transfer before it answers, and answers a resent transfer again, so a reply lost to a restart is safe.
    /// This process holds the claim no longer. Its capability probe and the transfer share one snapshot deadline.
    pub async fn transfer(&mut self, pid: u32) -> Result<(), Failure> {
        self.check().map_err(Failure::Invalid)?;
        let birth = crate::procs::started(pid)
            .ok_or_else(|| Failure::Invalid(format!("transfer target {pid} is gone")))?;
        let deadline = Deadline::snapshot();
        self.client.require(&["transfer"], deadline).await?;
        self.client
            .request(
                json!({"op":"host_memory_transfer","claim_id":self.id,"to":{"pid":pid,"birth":birth}}),
                deadline,
            )
            .await?;
        self.owner = pid;
        Ok(())
    }
    pub fn memory_bytes(&self) -> u64 {
        self.bytes
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    /// The claim's declaration under `op`: one of the acquisition operations.
    fn declaration(&self, op: &str) -> Value {
        json!({"op":op,"claim_id":self.id,"memory_bytes":self.bytes,"label":self.label,
            "priority":self.priority,"purpose":self.purpose})
    }
    fn check(&self) -> Result<(), String> {
        if self.owner != std::process::id() {
            return Err("only the original process may operate its host memory claim".into());
        }
        if self.released {
            return Err("host memory claim was already released".into());
        }
        Ok(())
    }
    /// Registers the claim without waiting for it; the answer carries the
    /// daemon's warnings about the request.
    pub async fn register(&mut self) -> Result<Value, Failure> {
        self.check().map_err(Failure::Invalid)?;
        self.attempted = true;
        self.client
            .request(self.declaration("host_memory_register"), Deadline::snapshot())
            .await
    }
    /// Returns once the daemon grants the claim. Call before allocating
    /// preparation inputs; the daemon answers, the claim never polls. `cancel` ends the capability check and
    /// the wait; the claim stays registered for its owner's cleanup.
    pub async fn acquire(&mut self, cancel: &CancelToken) -> Result<(), Failure> {
        self.check().map_err(Failure::Invalid)?;
        if !self.checked {
            let health = cancel
                .or_cancelled(self.client.health(Deadline::snapshot()))
                .await?;
            if health["host_memory"]["protocol"] != 1 {
                return Err(Failure::Refused(
                    "native daemon does not support host memory admission; use the updated service"
                        .into(),
                ));
            }
            self.checked = true;
        }
        self.attempted = true;
        // Every reissue carries the whole declaration from this owner, as the daemon requires of a replay.
        let status = self
            .client
            .waiting(self.declaration("host_memory_wait"), cancel)
            .await?;
        if status["state"] != "granted" {
            return Err(Failure::Refused(
                status["error"]
                    .as_str()
                    .unwrap_or("host memory admission failed")
                    .into(),
            ));
        }
        self.granted = true;
        Ok(())
    }
    /// A front-end's acquisition: reports the claim's wait to `place` (parked only for a large claim), waits
    /// for the grant as `acquire` does, and reports that preparation no longer waits. Lines the place has not
    /// read stay its own. A place that fails ends the acquisition as a refusal; its holder reads the place's
    /// own failure.
    pub async fn acquire_reporting(
        &mut self,
        place: &mut dyn PreparationWait,
        cancel: &CancelToken,
    ) -> Result<(), Failure> {
        place
            .preparation_wait(
                &format!("waiting for host memory: {} bytes", self.bytes),
                self.bytes > LARGE_REQUEST,
            )
            .map_err(Failure::Refused)?;
        self.acquire(cancel).await?;
        place.preparation_wait("", false).map_err(Failure::Refused)
    }
    pub async fn track_worker(&self, pid: u32) -> Result<(), Failure> {
        self.check().map_err(Failure::Invalid)?;
        if !self.granted {
            return Err(Failure::Invalid(
                "acquire host memory before registering workers".into(),
            ));
        }
        self.client
            .request(
                json!({"op":"host_memory_track","claim_id":self.id,"worker_pid":pid}),
                Deadline::snapshot(),
            )
            .await?;
        Ok(())
    }
    /// Releases the claim with this process's final high-water mark. The answer carries what admission
    /// observed of the claim; a claim never registered has none. Its exchange spends the claim's cleanup
    /// deadline. An answer, granted or refused, ends that cleanup: a refused release leaves the claim held, and
    /// its next cleanup has a deadline of its own. Only an outage keeps spending this one.
    pub async fn release(&mut self) -> Result<Option<Value>, Failure> {
        if self.released {
            return Ok(None);
        }
        self.check().map_err(Failure::Invalid)?;
        let mut answer = None;
        if self.attempted {
            let deadline = self.cleanup();
            let released = self
                .client
                .request(
                    json!({"op":"host_memory_release","claim_id":self.id,
                    "high_water_bytes":crate::procs::high_water(std::process::id())}),
                    deadline,
                )
                .await;
            if !matches!(released, Err(Failure::Unavailable(_))) {
                self.cleanup = None;
            }
            answer = Some(released?);
        }
        self.released = true;
        Ok(answer)
    }
    /// The claim's cleanup deadline, fixed by the first cleanup step that asks for it.
    fn cleanup(&mut self) -> Deadline {
        *self
            .cleanup
            .get_or_insert_with(|| Deadline::after(CLEANUP_PATIENCE))
    }
}
impl Drop for HostMemoryClaim {
    /// A dropped claim the service still holds is abandoned, which marks it retiring; the service still waits
    /// for its registered workers. The abandon runs on the synchronous facade within what remains of the
    /// cleanup deadline, so nothing is sent once it has passed, and the claim and its error are logged once.
    fn drop(&mut self) {
        if self.owner == std::process::id() && self.attempted && !self.released {
            let client = self.client.clone();
            let abandon = json!({"op":"host_memory_abandon","claim_id":self.id});
            let deadline = self.cleanup();
            let abandoned = blocking(async move { client.request(abandon, deadline).await })
                .and_then(|answer| answer.map_err(|failure| failure.to_string()));
            if let Err(error) = abandoned {
                eprintln!(
                    "host memory claim {} remains bound to its process lifetime: {error}",
                    self.id
                );
            }
        }
    }
}
