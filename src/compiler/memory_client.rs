//! Native host-memory claims. No Python admission state or process wrappers.
use super::client::{Client, Failure};
use super::model::Purpose;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::{BufRead, Write};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::time::Duration;

/// Claims above this are in the large class: admission may delay them indefinitely, and a GPU place waiting
/// on one parks.
pub const LARGE_REQUEST: u64 = super::contract::LARGE_REQUEST;

/// How long adopting or transferring a claim waits for the compile service before it reports the authority
/// unavailable, an outage rather than a verdict. It outlasts a restart of the service.
pub const AUTHORITY_PATIENCE: Duration = Duration::from_secs(60);

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
        })
    }
    /// The granted claim a front-end handed on, now held by this process: checked to be granted, owned by
    /// this process and of the stated bytes.
    pub async fn inherited(handover: &Handover) -> Result<Self, Failure> {
        let client = Client::new(&handover.state_dir, None).map_err(Failure::Unavailable)?;
        let status = client
            .within(
                json!({"op":"host_memory_status","claim_id":handover.claim_id}),
                AUTHORITY_PATIENCE,
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
    /// This process holds the claim no longer.
    pub async fn transfer(&mut self, pid: u32) -> Result<(), Failure> {
        self.check().map_err(Failure::Refused)?;
        let birth = crate::procs::started(pid)
            .ok_or_else(|| Failure::Refused(format!("transfer target {pid} is gone")))?;
        self.client.require(&["transfer"], AUTHORITY_PATIENCE).await?;
        self.client
            .within(
                json!({"op":"host_memory_transfer","claim_id":self.id,"to":{"pid":pid,"birth":birth}}),
                AUTHORITY_PATIENCE,
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
    pub async fn register(&mut self) -> Result<Value, String> {
        self.check()?;
        self.attempted = true;
        self.client
            .request(self.declaration("host_memory_register"))
            .await
    }
    /// Returns once the daemon grants the claim. Call before allocating
    /// preparation inputs; the daemon answers, the claim never polls.
    pub async fn acquire(&mut self) -> Result<(), String> {
        self.check()?;
        if !self.checked {
            let health = self.client.health().await?;
            if health["host_memory"]["protocol"] != 1 {
                return Err(
                    "native daemon does not support host memory admission; use the updated service"
                        .into(),
                );
            }
            self.checked = true;
        }
        self.attempted = true;
        let status = self
            .client
            .wait(self.declaration("host_memory_wait"))
            .await?;
        if status["state"] != "granted" {
            return Err(status["error"]
                .as_str()
                .unwrap_or("host memory admission failed")
                .into());
        }
        self.granted = true;
        Ok(())
    }
    /// A front-end's acquisition: reports the claim's wait to `place` (parked only for a large claim), waits
    /// for the grant as `acquire` does, and reports that preparation no longer waits. Lines the place has not
    /// read stay its own.
    pub async fn acquire_reporting(&mut self, place: &mut dyn PreparationWait) -> Result<(), String> {
        place.preparation_wait(
            &format!("waiting for host memory: {} bytes", self.bytes),
            self.bytes > LARGE_REQUEST,
        )?;
        self.acquire().await?;
        place.preparation_wait("", false)
    }
    pub async fn track_worker(&self, pid: u32) -> Result<(), String> {
        self.check()?;
        if !self.granted {
            return Err("acquire host memory before registering workers".into());
        }
        self.client
            .request(json!({"op":"host_memory_track","claim_id":self.id,"worker_pid":pid}))
            .await?;
        Ok(())
    }
    /// Releases the claim with this process's final high-water mark. The answer carries what admission
    /// observed of the claim; a claim never registered has none.
    pub async fn release(&mut self) -> Result<Option<Value>, String> {
        if self.released {
            return Ok(None);
        }
        self.check()?;
        let mut answer = None;
        if self.attempted {
            answer = Some(
                self.client
                    .request(json!({"op":"host_memory_release","claim_id":self.id,
                        "high_water_bytes":crate::procs::high_water(std::process::id())}))
                    .await?,
            );
        }
        self.released = true;
        Ok(answer)
    }
    // Drop cannot borrow a caller's Tokio runtime. This bounded native RPC marks
    // a dropped claim retiring; the actor still waits for registered workers.
    fn abandon(&self) -> Result<(), String> {
        let mut stream =
            std::os::unix::net::UnixStream::connect_addr(&super::endpoint(self.client.state())?)
                .map_err(|e| e.to_string())?;
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .map_err(|e| e.to_string())?;
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .map_err(|e| e.to_string())?;
        let mut peer: libc::ucred = unsafe { std::mem::zeroed() };
        let mut size = std::mem::size_of_val(&peer) as libc::socklen_t;
        if unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut peer as *mut libc::ucred).cast(),
                &mut size,
            )
        } != 0
            || peer.uid != unsafe { libc::getuid() }
        {
            return Err("host memory endpoint belongs to another user".into());
        }
        let mut bytes = serde_json::to_vec(
            &json!({"protocol":1,"op":"host_memory_abandon","claim_id":self.id}),
        )
        .map_err(|e| e.to_string())?;
        bytes.push(b'\n');
        stream.write_all(&bytes).map_err(|e| e.to_string())?;
        let mut line = String::new();
        std::io::BufReader::new(stream)
            .read_line(&mut line)
            .map_err(|e| e.to_string())?;
        let reply: Value = serde_json::from_str(&line).map_err(|e| e.to_string())?;
        if reply["ok"] != true {
            return Err(reply["error"]
                .as_str()
                .unwrap_or("host memory cleanup failed")
                .into());
        }
        Ok(())
    }
}
impl Drop for HostMemoryClaim {
    fn drop(&mut self) {
        if self.owner == std::process::id() && self.attempted && !self.released {
            if let Err(error) = self.abandon() {
                eprintln!(
                    "host memory claim {} remains bound to its process lifetime: {error}",
                    self.id
                );
            }
        }
    }
}
