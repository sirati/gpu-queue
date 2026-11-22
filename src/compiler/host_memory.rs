//! Atomic host RAM admission, independent of compiler slots and CPU affinity.
//! Claims and compiler demand take turns by party: the party holding the least
//! RAM goes next.
//! Only the channel actor mutates claims. Durable PID birth identities survive
//! handover and cannot be kept alive by recycled PIDs or forked token holders.
use crate::clock::Clock;
use super::{
    budget::{Budget, Lending},
    decision::{self, Counted, Decision, Decisions, Entry, Gpu, GpuTurn, Persisting, Phase, Record, Subject},
    fairness::{self, Shares},
    model::Purpose,
    party::{self, Attribution},
    precedence::{self, At, Candidate, Catalogue, Class, Figures, Registration, Head, Lacks, Precedence, RequestId, Standing, Work},
    resources::Snapshot,
    store::{self, Failure, Operation, Optional, Store},
    watch::{Reply, Watches},
};
use crate::procs::{Peer, Process, Unread};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

/// A requester or a named worker that is proven gone.
const OWNER_GONE: &str = "host memory owner is gone";
/// Why a requester's or a named worker's identity is unknown, as a request's answer says it.
fn unread_owner(unread: Unread) -> String {
    match unread {
        Unread::Gone => OWNER_GONE.to_owned(),
        Unread::Failed(error) => format!("host memory owner's identity cannot be read: {error}"),
    }
}

// Old clients re-send acquisitions under a 30 s request deadline.
const ACQUIRE_HOLD: std::time::Duration = std::time::Duration::from_secs(20);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Identity {
    pid: u32,
    birth: u64,
}
impl Identity {
    /// The process `pid` names now: a worker a request names by pid.
    fn new(pid: u32) -> Result<Self, String> {
        Process::read(pid).map(Self::from).map_err(unread_owner)
    }
    /// The process a transfer names: `{pid, birth}`.
    fn named(value: &Value) -> Result<Self, String> {
        serde_json::from_value(value.clone())
            .map_err(|_| "a host memory transfer names its target as {pid, birth}".into())
    }
    fn alive(&self) -> bool {
        crate::procs::started(self.pid) == Some(self.birth) && crate::procs::alive(self.pid)
    }
}
impl From<Process> for Identity {
    fn from(process: Process) -> Self {
        Self {
            pid: process.pid,
            birth: process.birth,
        }
    }
}
/// A claim, its party of type `P`: the party it was registered with, or, saved by generation 19 or earlier, none.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Claim<P = String> {
    id: String,
    owner: Identity,
    bytes: u64,
    label: String,
    order: u64,
    granted: bool,
    baseline_anonymous: u64,
    failure: Option<String>,
    retiring: bool,
    workers: BTreeMap<u32, Identity>,
    #[serde(default)]
    priority: i8,
    #[serde(default)]
    requested_at: f64,
    #[serde(default)]
    observed: Observed,
    /// Unknown for claims of gen-16 clients and for state they saved.
    #[serde(default)]
    purpose: Option<Purpose>,
    /// The owner before the last transfer: its resent transfer is answered again, anything else it asks
    /// is refused.
    #[serde(default)]
    previous_owner: Option<Identity>,
    /// The unit of the process that registered it, read once at its registration; never changed.
    party: P,
}
impl<P> Claim<P> {
    /// The claim with `party` as its party.
    fn with<Q>(self, party: Q) -> Claim<Q> {
        Claim {
            id: self.id,
            owner: self.owner,
            bytes: self.bytes,
            label: self.label,
            order: self.order,
            granted: self.granted,
            baseline_anonymous: self.baseline_anonymous,
            failure: self.failure,
            retiring: self.retiring,
            workers: self.workers,
            priority: self.priority,
            requested_at: self.requested_at,
            observed: self.observed,
            purpose: self.purpose,
            previous_owner: self.previous_owner,
            party,
        }
    }
}
impl Claim<Option<String>> {
    /// The claim with its party: the saved one, or, for a claim saved without one, the unit of the first readable of
    /// its recorded processes, each read with its saved pid and birth: its owner, its workers by pid, its previous
    /// owner. A process proven gone is passed over; one that cannot be read refuses the restore; a claim none of
    /// whose processes is readable is `unit-unknown`, an ending claim the first tick drops.
    fn attributed(self, attribution: &Attribution) -> Result<Claim, String> {
        let party = match self.party.clone() {
            Some(saved) => saved,
            None => std::iter::once(&self.owner)
                .chain(self.workers.values())
                .chain(self.previous_owner.iter())
                .find_map(|identity| match attribution.party(identity.pid, identity.birth) {
                    Err(Unread::Gone) => None,
                    Ok(party) => Some(Ok(party)),
                    Err(Unread::Failed(error)) => Some(Err(format!(
                        "host memory claim {} saved without a party: process {} cannot be read: {error}",
                        self.id, identity.pid
                    ))),
                })
                .unwrap_or_else(|| Ok(party::UNIT_UNKNOWN.to_owned()))?,
        };
        Ok(self.with(party))
    }
}
impl Claim {
    /// Its request: its registration.
    fn request(&self) -> RequestId {
        RequestId::Claim(Registration { claim: self.id.clone(), order: self.order })
    }
    /// One tick's view of a granted claim: what admission charges it, and its processes' high-water marks.
    fn observe(&mut self, anonymous: u64, tree: &BTreeSet<u32>) {
        self.observed
            .charged(anonymous.saturating_sub(self.baseline_anonymous));
        for &pid in tree {
            if let (Some(birth), Some(bytes)) =
                (crate::procs::started(pid), crate::procs::high_water(pid))
            {
                self.observed.high_water(pid, birth, bytes);
            }
        }
    }
}
/// What admission saw of a granted claim at its ticks, and what its releasing process reported of itself.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Observed {
    /// The largest growth over the grant-time baseline that admission charged.
    peak: Option<u64>,
    /// Each observed process's largest resident set.
    processes: Vec<HighWater>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct HighWater {
    pid: u32,
    birth: u64,
    bytes: u64,
}
impl Observed {
    fn charged(&mut self, bytes: u64) {
        self.peak = Some(self.peak.unwrap_or(0).max(bytes));
    }
    fn high_water(&mut self, pid: u32, birth: u64, bytes: u64) {
        match self
            .processes
            .iter_mut()
            .find(|process| process.pid == pid && process.birth == birth)
        {
            Some(process) => process.bytes = process.bytes.max(bytes),
            None => self.processes.push(HighWater { pid, birth, bytes }),
        }
    }
    fn report(&self, declared: u64) -> Value {
        super::contract::claim_report(super::contract::Claim {
            declared,
            peak: self.peak,
            high_water: (!self.processes.is_empty())
                .then(|| self.processes.iter().fold(0u64, |sum, p| sum.saturating_add(p.bytes))),
        })
    }
}
/// RAM demand in party share order. A compiler party asks for its next job;
/// on equal holdings, short compilations go before long-held claims.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub enum Turn {
    Compiler(u64, i8),
    Claim(u64, String),
}
#[derive(Clone, Default, Serialize, Deserialize)]
struct Claims<P = String> {
    boot: String,
    sequence: u64,
    claims: BTreeMap<String, Claim<P>>,
}
pub struct HostMemory {
    path: PathBuf,
    state: Claims,
    allocated: BTreeMap<String, u64>,
    /// Restore gave claims saved without a party their party, which activation saves before the engine serves.
    migrated: bool,
    watches: Watches,
    /// Memory the last tick left for claims, after every grant it made.
    admissible: Option<u64>,
    /// What this tick's `open` found admissible.
    opened: Option<Opened>,
    /// The work counted in this tick's projection.
    live: BTreeSet<Work>,
    /// What this tick's GPU pass decided for each pending GPU-place claim.
    gpu: BTreeMap<String, Gpu>,
    precedence: Precedence,
    clock: Clock,
    store: Store,
}
/// What running compiles charge the budget as a tick opens: their unused caps, and the lending they hold beside the
/// overcommit budget the instance lends.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Compiles {
    pub reserved: u64,
    pub lending: Lending,
}

/// What a tick's `open` found: the budget the aged head is judged by, and S, what would be admissible once the
/// service's own work that ends has ended.
#[derive(Clone, Copy, Debug, Default)]
pub struct Opened {
    pub left: Budget,
    pub serviceable: u64,
}
impl HostMemory {
    /// The saved claims, a claim saved without a party given one (`Claim::attributed`); it writes nothing.
    pub async fn restore(
        directory: &std::path::Path,
        boot: &str,
        clock: Clock,
        store: Store,
        attribution: &Attribution,
    ) -> Result<Self, String> {
        let path = directory.join("host-memory.json");
        let mut saved: Claims<Option<String>> = match store::optional(&path).await {
            Optional::Present(value) => serde_json::from_value(value).map_err(|e| e.to_string())?,
            Optional::Absent => Claims::default(),
            Optional::Failed(error) => return Err(error),
        };
        if saved.boot != boot {
            saved = Claims {
                boot: boot.into(),
                ..Claims::default()
            };
        }
        let migrated = saved.claims.values().any(|claim| claim.party.is_none());
        let claims = saved
            .claims
            .into_iter()
            .map(|(id, claim)| Ok((id, claim.attributed(attribution)?)))
            .collect::<Result<_, String>>()?;
        let state = Claims {
            boot: saved.boot,
            sequence: saved.sequence,
            claims,
        };
        Ok(Self {
            path,
            state,
            allocated: BTreeMap::new(),
            migrated,
            watches: Watches::default(),
            admissible: None,
            opened: None,
            live: BTreeSet::new(),
            gpu: BTreeMap::new(),
            precedence: Precedence::default(),
            clock,
            store,
        })
    }
    /// Saves the parties restore migrated, under operation `claims` `migrate`, once: restore's last step, after the
    /// service holds authority and before it serves.
    pub async fn activated(&mut self) -> Result<(), String> {
        match std::mem::take(&mut self.migrated) {
            true => self.save(Operation::new("claims", "migrate")).await,
            false => Ok(()),
        }
    }
    async fn save(&self, operation: Operation) -> Result<(), String> {
        self.store.write(
            operation,
            &self.path,
            &serde_json::to_value(&self.state).map_err(|e| e.to_string())?,
        )
        .await
        .map_err(String::from)
    }
    /// A request's change, applied and then published. It stands once published, the store's re-publication
    /// included, and also when the publication was not synced (the store has stopped, and memory matches what every
    /// later reader finds); a copy of the claims before it is put back when the change was refused or not published.
    async fn committed<T>(
        &mut self,
        subject: String,
        change: impl FnOnce(&mut Claims) -> Result<T, String>,
    ) -> Result<T, String> {
        let kept = self.state.clone();
        let published = match change(&mut self.state) {
            Ok(answer) => self.published(subject).await.map(|()| answer),
            Err(refused) => Err(Failure::Unpublished(refused)),
        };
        match published {
            Ok(answer) => Ok(answer),
            Err(Failure::Unpublished(error)) => {
                self.state = kept;
                Err(error)
            }
            Err(Failure::Unsynced(error)) => Err(error),
        }
    }
    /// The claims published through the store, under operation `claims` `subject`.
    async fn published(&self, subject: String) -> Result<(), Failure> {
        let value = serde_json::to_value(&self.state).map_err(|e| Failure::Unpublished(e.to_string()))?;
        self.store.publish(Operation::new("claims", subject), &self.path, &value).await.map(drop)
    }
    /// The claim `id`, when `requester`, by pid and birth, is its live owner.
    fn owned(&self, id: &str, requester: &Identity) -> Result<&Claim, String> {
        let claim = self
            .state
            .claims
            .get(id)
            .ok_or("unknown or released host memory claim")?;
        if claim.owner != *requester || !claim.owner.alive() {
            return Err(match &claim.previous_owner {
                Some(previous) if previous == requester => {
                    format!("host memory claim was transferred to {}", claim.owner.pid)
                }
                _ => "host memory claim belongs to a different process identity".into(),
            });
        }
        Ok(claim)
    }
    fn status(claim: &Claim) -> Value {
        let mut status = json!({"claim_id":claim.id,"state":if claim.failure.is_some() {"failed"} else if claim.granted {"granted"} else {"pending"},
            "error":claim.failure,"memory_bytes":claim.bytes,"owner_pid":claim.owner.pid,"label":claim.label,"priority":claim.priority,
            "purpose":claim.purpose,"worker_pids":claim.workers.keys().collect::<Vec<_>>()});
        for (key, value) in claim.observed.report(claim.bytes).as_object().into_iter().flatten() {
            status[key] = value.clone();
        }
        status
    }
    pub fn health(&self, sample: Option<&Snapshot>) -> Value {
        json!({"protocol":1,"granted":self.state.claims.values().filter(|c|c.granted).count(),
            "pending":self.state.claims.values().filter(|c|!c.granted && c.failure.is_none()).count(),
            "claimed_bytes":self.state.claims.values().filter(|c|c.granted).fold(0u64,|n,c|n.saturating_add(c.bytes)),"parked_waits":self.watches.len(),
            "unallocated_bytes":sample.map(|s|self.reserved(s)),
            "admissible_bytes":self.admissible,
            "claims":self.state.claims.values().map(|claim| {
                let mut status = Self::status(claim);
                status["party"] = json!(claim.party);
                status["order"] = json!(claim.order);
                if !claim.granted && claim.failure.is_none() {
                    status["pending_seconds"] = json!((claim.requested_at > 0.0)
                        .then(|| self.clock.wall() - claim.requested_at));
                    status["waiting"] = json!(self.admissible.map(|left| self.waiting(claim, left)));
                }
                status
            }).collect::<Vec<_>>()})
    }
    /// Why a pending claim waits.
    fn waiting(&self, claim: &Claim, left: u64) -> String {
        let id = claim.request();
        let serviceable = self.opened.unwrap_or_default().serviceable;
        let aged = precedence::aged(claim.requested_at, self.clock.wall());
        let gpu = self.gpu.get(&claim.id).map(|decision| match decision {
            Gpu::Head { .. } => format!("first GPU-place claim: {} bytes reserved until the service's own work frees them", claim.bytes),
            Gpu::BehindGpuHead => "GPU-place claim: behind the GPU head".into(),
            Gpu::Granted | Gpu::EndedInVain { .. } | Gpu::GivingWay | Gpu::NotServiceable => {
                "GPU-place claim: memory held outside the service's work keeps it waiting; nothing reserved".into()
            }
        });
        if let Some(gpu) = gpu {
            return gpu;
        }
        match (self.precedence.head().filter(|head| head.id == id), Class::of(claim.bytes, claim.purpose).waiting()) {
            (Some(head), _) => format!("aged head, party {}'s turn: {} bytes reserved", head.party, head.bytes),
            (None, Some(large)) => large.into(),
            (None, None) if aged && claim.bytes <= serviceable && !self.precedence.gives_way(&id) => {
                "aged: waits for its party's turn".into()
            }
            (None, None) => format!(
                "waiting for RAM: needs {} bytes, {left} bytes left for claims after headroom, granted claims' unused declarations and compilations",
                claim.bytes
            ),
        }
    }
    /// The claim a claim request names: of its ID and its registration order.
    fn registered(&self, id: &RequestId) -> Option<&Claim> {
        match id {
            RequestId::Claim(registration) => self.state.claims.get(&registration.claim).filter(|claim| claim.order == registration.order),
            RequestId::Compile { .. } => None,
        }
    }
    pub fn reserved(&self, _sample: &Snapshot) -> u64 {
        self.state
            .claims
            .values()
            .filter(|c| c.granted)
            .fold(0u64, |sum, claim| {
                let allocated = self
                    .allocated
                    .get(&claim.id)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(claim.baseline_anonymous);
                sum.saturating_add(claim.bytes.saturating_sub(allocated))
            })
    }
    /// The claim's live owner and workers, and their descendants in the sample.
    fn tree(claim: &Claim, sample: &Snapshot) -> BTreeSet<u32> {
        let mut roots = BTreeSet::new();
        if claim.owner.alive() {
            roots.insert(claim.owner.pid);
        }
        roots.extend(claim.workers.values().filter(|p| p.alive()).map(|p| p.pid));
        loop {
            let before = roots.len();
            for (&pid, &(parent, _)) in &sample.processes {
                if roots.contains(&parent) {
                    roots.insert(pid);
                }
            }
            if roots.len() == before {
                break;
            }
        }
        roots
    }
    fn anonymous(tree: &BTreeSet<u32>, sample: &Snapshot) -> u64 {
        // PSS counts fork-shared anonymous pages once, unlike summed RSS.
        // Missing/inaccessible evidence receives no allocation credit.
        let mut total = 0u64;
        for &pid in tree {
            if sample
                .groups
                .get(&pid)
                .is_some_and(|(_, birth)| crate::procs::started(pid) != Some(*birth))
            {
                return 0;
            }
            let Ok(text) = std::fs::read_to_string(format!("/proc/{pid}/smaps_rollup")) else {
                return 0;
            };
            let Some(kib) = text.lines().find_map(|line| {
                line.strip_prefix("Pss_Anon:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            }) else {
                return 0;
            };
            total = total.saturating_add(kib.saturating_mul(1024));
        }
        total
    }
    fn descends(mut pid: u32, root: u32, parent: impl Fn(u32) -> Option<u32>) -> bool {
        let mut seen = BTreeSet::new();
        while seen.insert(pid) {
            if pid == root {
                return true;
            }
            let Some(next) = parent(pid) else {
                return false;
            };
            pid = next;
        }
        false
    }
    pub fn empty(&self) -> bool {
        self.state.claims.is_empty()
    }
    pub async fn request(
        &mut self,
        request: &Value,
        peer: &Peer,
        sample: Option<&Snapshot>,
        headroom: u64,
        attribution: &Attribution,
    ) -> Result<Value, String> {
        let requester = Identity::from(peer.process().map_err(unread_owner)?);
        let id = request["claim_id"]
            .as_str()
            .filter(|s| {
                !s.is_empty()
                    && s.len() <= 128
                    && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
            .ok_or("invalid host memory claim identity")?;
        match request["op"].as_str().unwrap_or("") {
            "host_memory_acquire" | "host_memory_wait" | "host_memory_register" => {
                let bytes = request["memory_bytes"]
                    .as_u64()
                    .filter(|n| *n > 0)
                    .ok_or("host memory claim must be positive bytes")?;
                let label = request["label"]
                    .as_str()
                    .unwrap_or("")
                    .chars()
                    .take(200)
                    .collect::<String>();
                let priority = party::priority(request)?;
                let purpose: Option<Purpose> = serde_json::from_value(request["purpose"].clone())
                    .map_err(|_| "a host memory claim's purpose is gpu_place or host_only")?;
                if let Some(claim) = self.state.claims.get(id) {
                    self.owned(id, &requester)?;
                    if claim.bytes != bytes
                        || claim.label != label
                        || claim.priority != priority
                        || claim.purpose != purpose
                    {
                        return Err("conflicting host memory claim".into());
                    }
                    return Ok(Self::status(claim));
                }
                if self
                    .state
                    .claims
                    .values()
                    .any(|c| c.owner == requester && c.owner.alive())
                {
                    return Err("one aggregate host memory claim per process; release it before requesting another".into());
                }
                if sample
                    .is_some_and(|s| s.capacity > 0 && bytes > s.capacity.saturating_sub(headroom))
                {
                    return Err("host memory requirement exceeds host/cgroup capacity after required headroom".into());
                }
                let party = attribution.party(requester.pid, requester.birth).map_err(|unread| match unread {
                    Unread::Gone => OWNER_GONE.to_owned(),
                    Unread::Failed(error) => format!("host memory owner's cgroup cannot be read: {error}"),
                })?;
                let owner = requester.clone();
                let requested_at = self.clock.wall();
                self.committed(format!("register {id}"), |state| {
                    state.sequence = state
                        .sequence
                        .checked_add(1)
                        .ok_or("host memory sequence exhausted")?;
                    let claim = Claim {
                        id: id.into(),
                        owner,
                        bytes,
                        label,
                        priority,
                        order: state.sequence,
                        granted: false,
                        baseline_anonymous: 0,
                        failure: None,
                        retiring: false,
                        workers: BTreeMap::new(),
                        requested_at,
                        observed: Observed::default(),
                        purpose,
                        previous_owner: None,
                        party,
                    };
                    let result = Self::status(&claim);
                    state.claims.insert(id.into(), claim);
                    Ok(result)
                })
                .await
            }
            "host_memory_status" => Ok(Self::status(self.owned(id, &requester)?)),
            "host_memory_transfer" => self.transfer(id, &requester, Identity::named(&request["to"])?).await,
            "host_memory_track" => {
                let claim = self.owned(id, &requester)?;
                if !claim.granted {
                    return Err("host memory must be granted before registering workers".into());
                }
                let worker = request["worker_pid"]
                    .as_u64()
                    .and_then(|p| u32::try_from(p).ok())
                    .ok_or("invalid host memory worker")?;
                let identity = Identity::new(worker)?;
                if worker == requester.pid || !Self::descends(worker, requester.pid, crate::procs::parent) {
                    return Err(
                        "host memory worker must be a descendant of its authenticated owner".into(),
                    );
                }
                self.committed(format!("track {id}"), |state| {
                    state.claims.get_mut(id).unwrap().workers.insert(worker, identity);
                    Ok(())
                })
                .await?;
                Ok(Self::status(self.owned(id, &requester)?))
            }
            "host_memory_abandon" => {
                if !self.state.claims.contains_key(id) {
                    return Ok(json!({"released":true}));
                }
                self.owned(id, &requester)?;
                self.committed(format!("abandon {id}"), |state| {
                    state.claims.get_mut(id).unwrap().retiring = true;
                    Ok(())
                })
                .await?;
                Ok(json!({"retiring":true}))
            }
            "host_memory_release" => {
                // Release is idempotent after an interrupted RPC response.
                if !self.state.claims.contains_key(id) {
                    return Ok(json!({"released":true}));
                }
                let claim = self.owned(id, &requester)?;
                if claim.workers.values().any(Identity::alive) {
                    return Err(
                        "join all registered workers before releasing their host memory claim"
                            .into(),
                    );
                }
                let mut claim = self
                    .committed(format!("release {id}"), |state| Ok(state.claims.remove(id).expect("owned claim")))
                    .await?;
                // The releasing owner's own final high-water mark, read after its last allocation.
                if let Some(bytes) = request["high_water_bytes"].as_u64() {
                    claim
                        .observed
                        .high_water(claim.owner.pid, claim.owner.birth, bytes);
                }
                self.watches
                    .resolve(id, Err("host memory claim was released".into()));
                let mut answer = claim.observed.report(claim.bytes);
                answer["released"] = json!(true);
                answer["claim_id"] = json!(claim.id);
                Ok(answer)
            }
            _ => Err("unknown host memory operation".into()),
        }
    }
    /// The owner gives its claim to `to`, a live descendant that owns no claim. The transfer is durable before
    /// it is answered. A resent transfer from the previous owner naming the current one is answered again.
    async fn transfer(&mut self, id: &str, requester: &Identity, to: Identity) -> Result<Value, String> {
        let claim = self
            .state
            .claims
            .get(id)
            .ok_or("unknown or released host memory claim")?;
        if claim.previous_owner.as_ref() == Some(requester) && claim.owner == to {
            return Ok(Self::status(claim));
        }
        self.owned(id, requester)?;
        if to.pid == requester.pid || !to.alive() || !Self::descends(to.pid, requester.pid, crate::procs::parent) {
            return Err(
                "a host memory claim is transferred only to a live descendant of its owner".into(),
            );
        }
        if self
            .state
            .claims
            .values()
            .any(|c| c.owner.pid == to.pid && c.owner.alive())
        {
            return Err("the transfer's target already owns a host memory claim".into());
        }
        self.committed(format!("transfer {id}"), |state| {
            let claim = state.claims.get_mut(id).expect("owned claim");
            claim.previous_owner = Some(std::mem::replace(&mut claim.owner, to));
            Ok(Self::status(claim))
        })
        .await
    }
    /// Registers the claim and answers once it is decided. Clients re-send
    /// host_memory_acquire until granted under a 30 s request deadline, so a
    /// pending answer is held at most ACQUIRE_HOLD instead of their polling.
    pub async fn wait(
        &mut self,
        request: &Value,
        peer: &Peer,
        sample: Option<&Snapshot>,
        headroom: u64,
        attribution: &Attribution,
        reply: Reply,
    ) {
        let answer = self.request(request, peer, sample, headroom, attribution).await;
        match answer
            .as_ref()
            .ok()
            .filter(|status| status["state"] == "pending")
            .and_then(|status| status["claim_id"].as_str())
        {
            Some(id) => {
                let deadline = (request["op"] == "host_memory_acquire")
                    .then(|| std::time::Instant::now() + ACQUIRE_HOLD);
                self.watches.park(id, reply, deadline)
            }
            None => {
                let _ = reply.send(answer);
            }
        }
    }
    /// The tick's first phase: ended claims retire, allocations are sampled, and the budget is computed once, signed. `fresh` is a reading of available memory taken now, beside the
    /// sample's; `charged` is what running compiles charge the budget; `compiles` are the running compiles the
    /// service's projection counts. Returns the budget the aged head is judged by and S, what the service's own work
    /// can free.
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        &mut self,
        sample: &Snapshot,
        fresh: u64,
        charged: Compiles,
        headroom: u64,
        compiles: Vec<Counted>,
        record: &mut Record,
    ) -> Result<Opened, String> {
        let mut changed = false;
        let mut ended = Vec::new();
        self.state.claims.retain(|id, claim| {
            let keep = (claim.owner.alive() && !claim.retiring)
                || (claim.granted && claim.workers.values().any(Identity::alive));
            if !keep {
                ended.push(id.clone());
            }
            changed |= !keep;
            keep
        });
        self.allocated = BTreeMap::new();
        for (id, claim) in self.state.claims.iter_mut() {
            let tree = Self::tree(claim, sample);
            let anonymous = Self::anonymous(&tree, sample);
            if claim.granted {
                claim.observe(anonymous, &tree);
            }
            self.allocated.insert(id.clone(), anonymous);
        }
        record.available = fresh.min(sample.available);
        record.compiler_reserved = charged.reserved;
        record.minimum_ram = headroom;
        record.claims_unused = self.reserved(sample);
        record.overcommit = charged.lending.lent;
        record.lending = charged.lending.held;
        record.own_work = compiles;
        record.own_work.extend(self.state.claims.values().filter(|c| c.granted && c.purpose == Some(Purpose::HostOnly)).map(
            |claim| Counted::Claim {
                grant: Registration {
                    claim: claim.id.clone(),
                    order: claim.order,
                },
                bytes: claim.bytes,
            },
        ));
        self.live = record.live();
        self.precedence.settle(&self.live);
        let own = record.own();
        let mut decided = Vec::new();
        let left = self.gpu_pass(sample, headroom, own, record, &mut decided);
        let opened = Opened {
            left,
            serviceable: decision::serviceable(left, own),
        };
        record.left = opened.left;
        record.serviceable = opened.serviceable;
        self.opened = Some(opened);
        self.settle(Operation::new("claims", "open"), changed || !decided.is_empty(), ended, decided).await?;
        Ok(opened)
    }
    /// The GPU pass, before every other admission: pending GPU-place claims in request order, each granted if it
    /// fits the budget at its turn. The first that does not fit but whose bytes the service's own work can free
    /// (at most that budget plus `own`), and that is not giving way, is the GPU head: its bytes are reserved,
    /// and later claims are granted only from what is left after them. Returns what the pass leaves.
    fn gpu_pass(
        &mut self,
        sample: &Snapshot,
        headroom: u64,
        own: u64,
        record: &mut Record,
        decided: &mut Vec<String>,
    ) -> Budget {
        let mut pending: Vec<(At, u64, String)> = self
            .state
            .claims
            .values()
            .filter(|c| !c.granted && c.failure.is_none() && Class::of(c.bytes, c.purpose) == Class::Gpu)
            .map(|c| (At(c.requested_at), c.order, c.id.clone()))
            .collect();
        pending.sort();
        let mut budget = record.budget();
        let mut head = false;
        self.gpu = BTreeMap::new();
        for (_, _, id) in pending {
            if let Some(refusal) = self.refusal(&id, sample, headroom) {
                self.state.claims.get_mut(&id).unwrap().failure = Some(refusal.into());
                decided.push(id);
                continue;
            }
            let bytes = self.state.claims[&id].bytes;
            let request = self.state.claims[&id].request();
            let decision = self.precedence.gpu_turn(&request, bytes, budget, own, head, &self.live);
            record.gpu.push(GpuTurn {
                claim: id.clone(),
                order: self.state.claims[&id].order,
                bytes,
                budget,
                decision: decision.clone(),
            });
            match &decision {
                Gpu::Granted => {
                    budget = budget.less(bytes);
                    self.grant(&id);
                    decided.push(id.clone());
                }
                Gpu::Head { .. } => {
                    budget = budget.less(bytes);
                    record.gpu_reserved = bytes;
                    head = true;
                }
                Gpu::EndedInVain { .. } | Gpu::GivingWay | Gpu::BehindGpuHead | Gpu::NotServiceable => {}
            }
            self.gpu.insert(id, decision);
        }
        if !head {
            self.precedence.no_gpu();
        }
        budget
    }
    /// The aged head of an earlier tick, if any.
    pub fn head(&self) -> Option<&Head> {
        self.precedence.head()
    }
    pub fn gives_way(&self, id: &RequestId) -> bool {
        self.precedence.gives_way(id)
    }
    /// The requests giving way after their head ended in vain.
    pub fn giving_way(&self) -> impl Iterator<Item = &RequestId> {
        self.precedence.giving_way()
    }
    /// A compile ran ahead into the aged head's bytes: the head waits for it too.
    pub fn ran_ahead(&mut self, work: Work) {
        self.precedence.ran_ahead(work)
    }
    /// Asks precedence for the aged head among the engine's compile candidates and the claims that are
    /// candidates: ordinary, pending, aged, at most S and not giving way. `compile` is how a compile head of an
    /// earlier tick stands, as the engine reports it; a compile head it does not report is no longer pending.
    #[allow(clippy::too_many_arguments)]
    pub fn choose(
        &mut self,
        sample: &Snapshot,
        headroom: u64,
        compiles: Vec<Candidate>,
        compile: Option<Standing>,
        draining: bool,
        now: f64,
        record: &mut Record,
    ) {
        let opened = self.opened.unwrap_or_default();
        let mut candidates = compiles;
        let waits = |claim: &Claim, serviceable: u64| self.waits(claim, serviceable, sample, headroom);
        candidates.extend(self.state.claims.values().filter(|claim| waits(claim, opened.serviceable)).filter_map(|claim| {
            let id = claim.request();
            (precedence::aged(claim.requested_at, now) && !self.precedence.gives_way(&id)).then(|| Candidate {
                id,
                party: claim.party.clone(),
                eligible_at: At(claim.requested_at),
                catalogue: Catalogue::Claim { order: claim.order },
                bytes: claim.bytes,
                lacks: Lacks::Nothing,
            })
        }));
        let standing = self.precedence.head().map(|head| match &head.id {
            RequestId::Claim(_) => Standing::of(
                self.registered(&head.id).is_some_and(|claim| waits(claim, u64::MAX)),
                self.registered(&head.id).map_or(head.bytes, |claim| claim.bytes),
                Lacks::Nothing,
            ),
            RequestId::Compile { .. } => compile.clone().unwrap_or(Standing::Ended {
                bytes: head.bytes,
                capacity: None,
            }),
        });
        let figures = Figures {
            left: opened.left,
            serviceable: opened.serviceable,
            live: &self.live,
            draining,
        };
        record.draining = draining;
        record.aged = self.precedence.choose(figures, candidates, standing);
    }
    /// Whether a claim is pending, ordinary, at most `serviceable` and would not be refused: what candidacy and
    /// a claim head's standing require.
    fn waits(&self, claim: &Claim, serviceable: u64, sample: &Snapshot, headroom: u64) -> bool {
        !claim.granted
            && claim.failure.is_none()
            && Class::of(claim.bytes, claim.purpose) == Class::Ordinary
            && claim.bytes <= serviceable
            && self.refusal(&claim.id, sample, headroom).is_none()
    }
    /// The tick's serving phase: a fitting claim head is granted at once, any other head reserves its bytes, and
    /// ordinary claims and compiler turns are served by party share from what is left. Returns the budget for
    /// the launches that follow: the turns' bytes go back to them.
    pub async fn serve(
        &mut self,
        sample: &Snapshot,
        headroom: u64,
        turns: Shares<Turn>,
        record: &mut Record,
    ) -> Result<Budget, String> {
        let mut budget = self.opened.unwrap_or_default().left;
        let mut decided = Vec::new();
        let mut changed = false;
        let head = self.precedence.head().map(|head| (head.id.clone(), head.bytes, head.fits));
        match head {
            Some((RequestId::Claim(Registration { claim, order }), bytes, true)) => {
                record.admitted.push(Entry::Granted {
                    phase: Persisting::Serve,
                    claim: claim.clone(),
                    order,
                    party: self.state.claims[&claim].party.clone(),
                    class: Class::of(bytes, None),
                    bytes,
                    budget,
                    head: true,
                });
                budget = budget.less(bytes);
                self.grant(&claim);
                self.precedence.served();
                changed = true;
                decided.push(claim);
            }
            Some((request, bytes, _)) => {
                record.admitted.push(Entry::Reserved { request, bytes, budget });
                budget = budget.less(bytes);
                record.aged_reserved = bytes;
            }
            None => {}
        }
        let mut shares = turns;
        self.wait_by_share(&mut shares, Class::Ordinary);
        let (served, kept) = self.share_pass(sample, headroom, shares, &mut budget, Persisting::Serve, record, &mut decided);
        record.admitted.push(Entry::Returned { bytes: kept, budget });
        budget = budget.plus(kept);
        self.settle(Operation::new("claims", "serve"), changed || served, Vec::new(), decided).await?;
        Ok(budget)
    }
    /// After the tick's ordinary launches, claims of the class `phase` admits still pending are offered what is
    /// left, by party share: ordinary claims what the ordinary launches left (`Persisting::Close`), large claims what
    /// every smaller request left (`Persisting::LargeClose`). `held` is each party's holding of running compiles.
    /// Returns what the pass leaves.
    pub async fn close(
        &mut self,
        sample: &Snapshot,
        headroom: u64,
        held: BTreeMap<String, u64>,
        phase: Persisting,
        mut budget: Budget,
        record: &mut Record,
    ) -> Result<Budget, String> {
        let mut shares = Shares::new(held);
        let mut decided = Vec::new();
        self.wait_by_share(&mut shares, Phase::from(phase).class());
        let (changed, _) = self.share_pass(sample, headroom, shares, &mut budget, phase, record, &mut decided);
        self.admissible = Some(budget.floor());
        let operation = Operation::new("claims", format!("{phase:?}"));
        self.settle(operation, changed, Vec::new(), decided).await?;
        Ok(budget)
    }
    /// Granted claims count towards their party's holding; pending claims of `class` other than the aged head
    /// wait for their turn.
    fn wait_by_share(&self, shares: &mut Shares<Turn>, class: Class) {
        let head = self.precedence.head().map(|head| &head.id);
        for claim in self.state.claims.values() {
            let party = &claim.party;
            let is_head = head == Some(&claim.request());
            match (claim.granted, &claim.failure) {
                (true, _) => shares.grant(party, fairness::charge(claim.bytes, claim.priority)),
                (false, None) if Class::of(claim.bytes, claim.purpose) == class && !is_head => {
                    shares.wait(party, Turn::Claim(claim.order, claim.id.clone()))
                }
                (false, _) => {}
            }
        }
    }
    /// Why a pending claim is refused: its process tree overlaps a granted claim's, or it exceeds the capacity.
    fn refusal(&self, id: &str, sample: &Snapshot, headroom: u64) -> Option<&'static str> {
        let owner = self.state.claims[id].owner.pid;
        let overlaps = self.state.claims.values().any(|c| {
            c.granted
                && (Self::descends(owner, c.owner.pid, |p| {
                    sample.processes.get(&p).map(|(parent, _)| *parent)
                }) || Self::descends(c.owner.pid, owner, |p| {
                    sample.processes.get(&p).map(|(parent, _)| *parent)
                }))
        });
        let claim = &self.state.claims[id];
        match (overlaps, claim.bytes > sample.capacity.saturating_sub(headroom) && sample.capacity > 0) {
            (true, _) => Some("overlapping process-tree host memory claims are forbidden; declare one aggregate claim"),
            (false, true) => Some("host memory requirement exceeds host/cgroup capacity after required headroom"),
            (false, false) => None,
        }
    }
    fn grant(&mut self, id: &str) {
        let baseline = self.allocated.get(id).copied().unwrap_or(0);
        let claim = self.state.claims.get_mut(id).expect("a pending claim");
        claim.granted = true;
        claim.baseline_anonymous = baseline;
    }
    /// Serves the turns in party share order from `budget`. Returns whether any claim was decided, and the
    /// bytes the compiler turns kept.
    #[allow(clippy::too_many_arguments)]
    fn share_pass(
        &mut self,
        sample: &Snapshot,
        headroom: u64,
        mut shares: Shares<Turn>,
        budget: &mut Budget,
        phase: Persisting,
        record: &mut Record,
        decided: &mut Vec<String>,
    ) -> (bool, u64) {
        let mut changed = false;
        let mut kept = 0u64;
        while let Some((party, turn)) = shares.next() {
            let id = match turn {
                // Compiler admission follows this phase and finds the RAM its
                // turn kept from claims.
                Turn::Compiler(bytes, priority) => {
                    if budget.fits(bytes) {
                        record.admitted.push(Entry::Turn { party: party.clone(), bytes, budget: *budget });
                        *budget = budget.less(bytes);
                        kept += bytes;
                        shares.grant(&party, fairness::charge(bytes, priority));
                    }
                    continue;
                }
                Turn::Claim(_, id) => id,
            };
            if let Some(refusal) = self.refusal(&id, sample, headroom) {
                self.state.claims.get_mut(&id).unwrap().failure = Some(refusal.into());
                changed = true;
                decided.push(id);
                continue;
            }
            let claim = &self.state.claims[&id];
            if !budget.fits(claim.bytes) {
                // A large claim next in turn must not block smaller claims
                // that can fit in the currently available RAM.  Keep the
                // pending claim ordered for a later tick, while continuing the
                // scan so admission makes progress without overcommitting RAM.
                continue;
            }
            let (bytes, priority) = (claim.bytes, claim.priority);
            record.admitted.push(Entry::Granted {
                phase,
                claim: id.clone(),
                order: claim.order,
                party: party.clone(),
                class: Class::of(bytes, None),
                bytes,
                budget: *budget,
                head: false,
            });
            *budget = budget.less(bytes);
            self.grant(&id);
            shares.grant(&party, fairness::charge(bytes, priority));
            changed = true;
            decided.push(id);
        }
        (changed, kept)
    }
    /// Every pending claim's decision.
    pub fn decisions(&self) -> BTreeMap<Subject, Decisions> {
        let serviceable = self.opened.unwrap_or_default().serviceable;
        let head = self.precedence.head();
        self.state
            .claims
            .values()
            .filter(|claim| !claim.granted && claim.failure.is_none())
            .map(|claim| {
                let id = claim.request();
                let decision = match (head.filter(|head| head.id == id), self.gpu.get(&claim.id)) {
                    (Some(head), _) => Decision::head(head.fits),
                    (None, Some(gpu)) => Decision::gpu(gpu),
                    (None, None) => Decision::waiting(
                        Class::of(claim.bytes, claim.purpose),
                        claim.bytes,
                        serviceable,
                        self.precedence.gives_way(&id),
                    ),
                };
                (Subject::from(&id), Decisions::One(decision))
            })
            .collect()
    }
    /// The aged head and the turn, as health shows them.
    pub fn heads(&self) -> Value {
        let aged = self.precedence.head().map(|head| {
            json!({"request":head.id,"party":head.party,"bytes":head.bytes,"fits":head.fits,"reserves":true})
        });
        let gpu = self.precedence.gpu_head().and_then(|id| {
            let claim = self.registered(id)?;
            Some(json!({"request":id,"party":claim.party,"bytes":claim.bytes,"reserves":true}))
        });
        json!({"gpu":gpu,"aged":aged,"turn":self.precedence.turn()})
    }
    /// Decisions become durable, then their waiters learn them.
    async fn settle(
        &mut self,
        operation: Operation,
        changed: bool,
        ended: Vec<String>,
        decided: Vec<String>,
    ) -> Result<(), String> {
        if changed {
            self.save(operation).await?;
        }
        // Waiters learn an outcome only once it is durable.
        for id in ended {
            self.watches
                .resolve(&id, Err("host memory claim ended before admission".into()));
        }
        for id in decided {
            let status = Self::status(&self.state.claims[&id]);
            self.watches.resolve(&id, Ok(status));
        }
        self.answer_expired(std::time::Instant::now());
        self.watches.prune();
        Ok(())
    }
    /// Moves a claim's request `seconds` into the past.
    #[cfg(test)]
    pub fn backdate(&mut self, id: &str, seconds: f64) {
        self.state.claims.get_mut(id).unwrap().requested_at -= seconds;
    }
    /// A held acquisition is answered with its still-pending state.
    fn answer_expired(&mut self, now: std::time::Instant) {
        for (id, reply) in self.watches.expire(now) {
            let status = self.state.claims.get(&id).map(Self::status);
            let _ =
                reply.send(status.ok_or_else(|| "host memory claim ended before admission".into()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{decision::Journal, party::Units};
    use crate::process_identity::Instance;
    use super::*;
    /// The unit this process and every `Owner` are in, unless a test places one elsewhere.
    const SHARED: &str = "/user.slice/test.slice/host-memory-tests.scope";
    /// Where the tests' processes are, by pid and birth; this process is in `SHARED`.
    fn units() -> &'static Units {
        static UNITS: std::sync::OnceLock<Units> = std::sync::OnceLock::new();
        UNITS.get_or_init(|| {
            let units = Units::default();
            let own = std::process::id();
            units.place(own, crate::procs::started(own).unwrap(), SHARED);
            units
        })
    }
    fn attribution() -> Attribution {
        Attribution::reading(units().reader())
    }
    struct Owner(std::process::Child);
    impl Owner {
        /// A `sleep` in `SHARED`.
        fn new() -> Self {
            let owner = Self(std::process::Command::new("sleep").arg("60").spawn().unwrap());
            owner.place(SHARED);
            owner
        }
        fn birth(&self) -> u64 {
            crate::procs::started(self.pid()).unwrap()
        }
        /// It is in the cgroup `path` from now on.
        fn place(&self, path: &str) {
            units().place(self.pid(), self.birth(), path);
        }
        fn pid(&self) -> u32 {
            self.0.id()
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    /// A state directory of its own: tests running at once in one process never share one.
    fn directory() -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "host-memory-{}-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
    fn sample(owners: &[u32]) -> Snapshot {
        Snapshot {
            available: 1000,
            capacity: 1000,
            processes: owners
                .iter()
                .map(|p| (*p, (std::process::id(), 0)))
                .collect(),
            ..Snapshot::default()
        }
    }
    /// One admission tick with no compile candidates, every phase in order; each compiler turn's job launches,
    /// as the engine's launches would. `journal` writes its record as the engine's does, and every record so far
    /// must replay.
    #[allow(clippy::too_many_arguments)]
    async fn tick(
        m: &mut HostMemory,
        journal: &mut Journal,
        s: &Snapshot,
        compiler_reserved: u64,
        headroom: u64,
        turns: Shares<Turn>,
    ) -> Result<(), String> {
        let now = crate::protocol::now_unix();
        let mut record = journal.begin(now);
        let charged = Compiles { reserved: compiler_reserved, lending: Lending::default() };
        m.open(s, u64::MAX, charged, headroom, Vec::new(), &mut record).await?;
        m.choose(s, headroom, Vec::new(), None, false, now, &mut record);
        let mut budget = m.serve(s, headroom, turns, &mut record).await?;
        record.persisted(Persisting::Serve, 0.0, budget);
        let kept: Vec<(String, u64)> = record
            .admitted
            .iter()
            .filter_map(|entry| match entry {
                Entry::Turn { party, bytes, .. } => Some((party.clone(), *bytes)),
                _ => None,
            })
            .collect();
        for (party, bytes) in kept {
            record.admitted.push(Entry::Launched {
                phase: decision::Launching::Launch,
                group: party.clone(),
                job: party,
                class: Class::of(bytes, None),
                bytes,
                expected: bytes,
                lent: 0,
                budget,
                reserved: 0,
                head: false,
                ahead: None,
                admitted_at: 0.0,
                declared: Some(60),
                settled_at: 0.0,
                running: true,
            });
            record.slots += 1;
            budget = budget.less(bytes);
        }
        record.left_after_launch = budget.floor();
        let budget = m.close(s, headroom, BTreeMap::new(), Persisting::Close, budget, &mut record).await?;
        record.persisted(Persisting::Close, 0.0, budget);
        let left = m.close(s, headroom, BTreeMap::new(), Persisting::LargeClose, budget, &mut record).await?;
        record.persisted(Persisting::LargeClose, 0.0, left);
        journal.finish(record, m.decisions());
        decision::replays(journal)
    }
    /// A request and its sender's descriptor, as a connection posts them.
    type Asked = (Value, Peer);
    fn acquire(pid: u32, id: &str, bytes: u64) -> Asked {
        (json!({"op":"host_memory_acquire","claim_id":id,"memory_bytes":bytes,"label":"fixture batch"}), Peer::opened(pid))
    }
    fn operation(pid: u32, id: &str, op: &str) -> Asked {
        (json!({"op":op,"claim_id":id}), Peer::opened(pid))
    }
    impl HostMemory {
        /// `request` of a request with its sender.
        async fn asked(&mut self, (request, peer): &Asked, sample: Option<&Snapshot>, headroom: u64) -> Result<Value, String> {
            self.request(request, peer, sample, headroom, &attribution()).await
        }
        /// `wait` of a request with its sender.
        async fn waited(&mut self, (request, peer): &Asked, sample: Option<&Snapshot>, headroom: u64, reply: Reply) {
            self.wait(request, peer, sample, headroom, &attribution(), reply).await
        }
    }
    #[tokio::test]
    async fn aggregate_claims_are_atomic_and_never_consume_cpu_slots() {
        let a = Owner::new();
        let b = Owner::new();
        let dir = directory();
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        let s = sample(&[a.pid(), b.pid()]);
        m.asked(&acquire(a.pid(), "a", 600), Some(&s), 100)
            .await
            .unwrap();
        m.asked(&acquire(b.pid(), "b", 400), Some(&s), 100)
            .await
            .unwrap();
        tick(&mut m, &mut journal, &s, 400, 100, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(
            !m.state.claims["a"].granted,
            "compiler RAM reservations count, even with no CPU admission"
        );
        assert!(m.state.claims["b"].granted);
        assert!(
            s.cores.is_empty() && s.idle == 0,
            "RAM claims do not require compiler cores"
        );
        m.asked(
            &operation(b.pid(), "b", "host_memory_release"),
            Some(&s),
            100,
        )
        .await
        .unwrap();
        tick(&mut m, &mut journal, &s, 0, 100, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(m.state.claims["a"].granted);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test]
    async fn a_waiting_claim_is_answered_when_admission_decides_it() {
        let a = Owner::new();
        let b = Owner::new();
        let dir = directory();
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        let s = sample(&[a.pid(), b.pid()]);
        let wait = |pid, id, bytes| {
            let mut request = acquire(pid, id, bytes);
            request.0["op"] = json!("host_memory_wait");
            request
        };
        let (reply, mut granted) = tokio::sync::oneshot::channel();
        m.waited(&wait(a.pid(), "a", 600), Some(&s), 0, reply).await;
        let (reply, mut released) = tokio::sync::oneshot::channel();
        m.waited(&wait(b.pid(), "b", 600), Some(&s), 0, reply).await;
        assert!(granted.try_recv().is_err() && released.try_recv().is_err());
        tick(&mut m, &mut journal, &s, 500, 0, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(
            granted.try_recv().is_err(),
            "a claim that does not fit stays parked"
        );
        tick(&mut m, &mut journal, &s, 0, 0, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert_eq!(granted.try_recv().unwrap().unwrap()["state"], "granted");
        let (reply, mut held) = tokio::sync::oneshot::channel();
        m.waited(&acquire(b.pid(), "b", 600), Some(&s), 0, reply)
            .await;
        assert!(
            held.try_recv().is_err(),
            "a re-sent acquisition is held, not polled"
        );
        m.answer_expired(std::time::Instant::now() + ACQUIRE_HOLD);
        assert_eq!(held.try_recv().unwrap().unwrap()["state"], "pending");
        let (reply, mut held) = tokio::sync::oneshot::channel();
        m.waited(&acquire(b.pid(), "b", 600), Some(&s), 0, reply)
            .await;
        m.asked(&operation(b.pid(), "b", "host_memory_release"), Some(&s), 0)
            .await
            .unwrap();
        assert!(released
            .try_recv()
            .unwrap()
            .unwrap_err()
            .contains("released"));
        assert!(held.try_recv().unwrap().unwrap_err().contains("released"));
        let (reply, answered) = tokio::sync::oneshot::channel();
        m.waited(&wait(a.pid(), "a", 600), Some(&s), 0, reply).await;
        assert_eq!(answered.await.unwrap().unwrap()["state"], "granted");
        assert_eq!(m.watches.len(), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test]
    async fn a_large_pending_claim_does_not_block_later_fitting_claims() {
        let a = Owner::new();
        let b = Owner::new();
        let dir = directory();
        let s = sample(&[a.pid(), b.pid()]);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        m.asked(&acquire(a.pid(), "large", 700), Some(&s), 0)
            .await
            .unwrap();
        m.asked(&acquire(b.pid(), "small", 400), Some(&s), 0)
            .await
            .unwrap();

        // The large claim cannot fit after compiler reservations, but the
        // later claim can.  It must be granted without exceeding capacity.
        tick(&mut m, &mut journal, &s, 400, 0, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(!m.state.claims["large"].granted);
        assert!(m.state.claims["small"].granted);
        assert_eq!(m.reserved(&s), 400);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test]
    async fn a_later_small_party_is_not_queued_behind_a_whole_campaign() {
        let campaign = [Owner::new(), Owner::new(), Owner::new()];
        let small = Owner::new();
        let dir = directory();
        let mut s = sample(&[
            campaign[0].pid(),
            campaign[1].pid(),
            campaign[2].pid(),
            small.pid(),
        ]);
        let owners: Vec<_> = campaign
            .iter()
            .map(|o| (o.pid(), 100))
            .chain([(small.pid(), 200)])
            .collect();
        attributed(&mut s, &owners);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        for (i, owner) in campaign.iter().enumerate() {
            m.asked(&acquire(owner.pid(), &format!("c{i}"), 300), Some(&s), 0)
                .await
                .unwrap();
        }
        m.asked(&acquire(small.pid(), "small", 300), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &mut journal, &s, 400, 0, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(m.state.claims["c0"].granted);
        assert!(
            m.state.claims["small"].granted,
            "the party holding no RAM goes before the campaign's second claim"
        );
        assert!(!m.state.claims["c1"].granted && !m.state.claims["c2"].granted);
        assert_eq!(m.health(Some(&s))["claims"][3]["party"], driven(200));
        std::fs::remove_dir_all(dir).unwrap();
    }
    /// Each claim owner was spawned by its party's driver, in the driver's unit.
    fn attributed(sample: &mut Snapshot, owners: &[(u32, u32)]) {
        for &(owner, driver) in owners {
            sample.processes.insert(owner, (driver, 0));
            units().place(owner, crate::procs::started(owner).unwrap(), &driven(driver));
        }
    }
    /// The unit of driver `driver`.
    fn driven(driver: u32) -> String {
        format!("/user.slice/test.slice/driver-{driver}.service")
    }
    #[tokio::test]
    async fn a_priority_party_holds_a_weighted_share() {
        let owners = [Owner::new(), Owner::new(), Owner::new(), Owner::new()];
        let dir = directory();
        let mut s = sample(&owners.iter().map(Owner::pid).collect::<Vec<_>>());
        let drivers = [100, 200, 100, 200];
        let pairs: Vec<_> = owners.iter().map(Owner::pid).zip(drivers).collect();
        attributed(&mut s, &pairs);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        let claim = |owner: &Owner, id: &str, bytes: u64, priority: i8| {
            let mut request = acquire(owner.pid(), id, bytes);
            request.0["priority"] = json!(priority);
            request
        };
        m.asked(&claim(&owners[0], "ordinary-held", 100, 0), Some(&s), 0)
            .await
            .unwrap();
        m.asked(&claim(&owners[1], "urgent-held", 500, 10), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &mut journal, &s, 200, 0, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        m.asked(&claim(&owners[2], "ordinary-next", 200, 0), Some(&s), 0)
            .await
            .unwrap();
        m.asked(&claim(&owners[3], "urgent-next", 200, 10), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &mut journal, &s, 200, 0, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(
            m.state.claims["urgent-next"].granted,
            "500 bytes at priority 10 weigh less than 100 bytes at priority 0"
        );
        assert!(!m.state.claims["ordinary-next"].granted);
        assert!(m
            .asked(&claim(&owners[3], "urgent-next", 200, 0), Some(&s), 0)
            .await
            .unwrap_err()
            .contains("conflicting"));
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test]
    async fn a_compiler_party_holding_less_ram_is_not_starved_by_claims() {
        let a = Owner::new();
        let b = Owner::new();
        let dir = directory();
        let s = sample(&[a.pid(), b.pid()]);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        let none = || Shares::new(BTreeMap::new());
        m.asked(&acquire(a.pid(), "held", 300), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &mut journal, &s, 400, 0, none()).await.unwrap();
        m.asked(&acquire(b.pid(), "next", 300), Some(&s), 0)
            .await
            .unwrap();
        let mut compilers = none();
        compilers.wait("compiles", Turn::Compiler(300, 0));
        tick(&mut m, &mut journal, &s, 400, 0, compilers)
            .await
            .unwrap();
        assert!(
            !m.state.claims["next"].granted,
            "RAM freed for a party holding none goes to its compilation first"
        );
        let mut compilers = Shares::new(BTreeMap::from([(
            "compiles".to_owned(),
            fairness::charge(300, 0),
        )]));
        compilers.wait("compiles", Turn::Compiler(300, 0));
        tick(&mut m, &mut journal, &s, 100, 0, compilers)
            .await
            .unwrap();
        assert!(
            m.state.claims["next"].granted,
            "a claim is granted from RAM left after the compiler's turn"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test]
    async fn identities_are_authenticated_and_handover_preserves_claims() {
        let a = Owner::new();
        let b = Owner::new();
        let dir = directory();
        let s = sample(&[a.pid(), b.pid()]);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        m.asked(&acquire(a.pid(), "a", 600), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &mut journal, &s, 0, 0, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(m
            .asked(&acquire(a.pid(), "b", 1), Some(&s), 0)
            .await
            .unwrap_err()
            .contains("aggregate"));
        assert!(m
            .asked(&operation(b.pid(), "a", "host_memory_release"), Some(&s), 0)
            .await
            .is_err());
        drop(m);
        let m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        assert!(m.state.claims["a"].granted);
        assert_eq!(m.reserved(&s), 600);
        let m = HostMemory::restore(&dir, "different-boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        assert!(m.empty());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test]
    async fn impossible_claims_fail_before_any_preparation() {
        let a = Owner::new();
        let dir = directory();
        let s = sample(&[a.pid()]);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        assert!(m
            .asked(&acquire(a.pid(), "a", 901), Some(&s), 100)
            .await
            .unwrap_err()
            .contains("capacity"));
        m.asked(&acquire(a.pid(), "a", 901), None, 100)
            .await
            .unwrap();
        tick(&mut m, &mut journal, &s, 0, 100, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        let state = m
            .asked(
                &operation(a.pid(), "a", "host_memory_status"),
                Some(&s),
                100,
            )
            .await
            .unwrap();
        assert_eq!(state["state"], "failed");
        assert_eq!(m.reserved(&s), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test]
    async fn tracked_workers_must_join_before_release_or_abandoned_cleanup() {
        let mut worker = Owner::new();
        let owner = std::process::id();
        let dir = directory();
        let s = sample(&[worker.pid()]);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        m.asked(&acquire(owner, "a", 600), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &mut journal, &s, 0, 0, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        let mut track = operation(owner, "a", "host_memory_track");
        track.0["worker_pid"] = json!(worker.pid());
        m.asked(&track, Some(&s), 0).await.unwrap();
        assert!(m
            .asked(&operation(owner, "a", "host_memory_release"), Some(&s), 0)
            .await
            .unwrap_err()
            .contains("join"));
        m.asked(&operation(owner, "a", "host_memory_abandon"), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &mut journal, &s, 0, 0, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(!m.empty());
        worker.0.kill().unwrap();
        worker.0.wait().unwrap();
        tick(&mut m, &mut journal, &s, 0, 0, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(m.empty());
        std::fs::remove_dir_all(dir).unwrap();
    }
    const MIB: u64 = 1 << 20;

    /// A Python owner that runs `script` once its directory holds `go`; the script writes `done` when it has
    /// allocated what it holds, and the owner then waits for `stop`.
    struct Allocating {
        owner: std::process::Child,
        directory: PathBuf,
    }
    impl Allocating {
        fn new(directory: &std::path::Path, script: &str) -> Self {
            let program = std::env::var("PATH")
                .unwrap()
                .split(':')
                .map(|p| PathBuf::from(p).join("python3"))
                .find(|p| p.is_file())
                .unwrap();
            let code = format!(
                "import pathlib, time\nwhile not pathlib.Path('go').exists(): time.sleep(0.005)\n{script}\n\
                 pathlib.Path('done').write_text('')\nwhile not pathlib.Path('stop').exists(): time.sleep(0.01)"
            );
            let owner = std::process::Command::new(program)
                .args(["-S", "-c", &code])
                .current_dir(directory)
                .spawn()
                .unwrap();
            units().place(owner.id(), crate::procs::started(owner.id()).unwrap(), SHARED);
            Self {
                owner,
                directory: directory.to_owned(),
            }
        }
        fn pid(&self) -> u32 {
            self.owner.id()
        }
        fn run(&self) {
            std::fs::write(self.directory.join("go"), b"").unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            while !self.directory.join("done").exists() {
                assert!(std::time::Instant::now() < deadline, "the owner never allocated");
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
    }
    impl Drop for Allocating {
        fn drop(&mut self) {
            let _ = self.owner.kill();
            let _ = self.owner.wait();
        }
    }
    fn roomy(owners: &[u32]) -> Snapshot {
        Snapshot {
            available: 64 << 30,
            capacity: 64 << 30,
            ..sample(owners)
        }
    }
    async fn ticks(m: &mut HostMemory, journal: &mut Journal, s: &Snapshot, count: usize) {
        for _ in 0..count {
            tick(m, journal, s, 0, 0, Shares::new(BTreeMap::new()))
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }

    #[tokio::test]
    async fn the_observed_peak_is_what_admission_charged_above_the_grant_time_baseline() {
        let dir = directory();
        let owner = Allocating::new(&dir, "held = bytearray(b'x') * (300 << 20)");
        let s = roomy(&[owner.pid()]);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        m.asked(&acquire(owner.pid(), "a", 1000 * MIB), Some(&s), 0)
            .await
            .unwrap();
        ticks(&mut m, &mut journal, &s, 1).await;
        let pending = m
            .asked(&operation(owner.pid(), "a", "host_memory_status"), Some(&s), 0)
            .await
            .unwrap();
        assert_eq!(pending["state"], "granted");
        owner.run();
        ticks(&mut m, &mut journal, &s, 4).await;
        let status = m
            .asked(&operation(owner.pid(), "a", "host_memory_status"), Some(&s), 0)
            .await
            .unwrap();
        let peak = status["observed_peak_bytes"].as_u64().unwrap();
        assert!((300 * MIB..=310 * MIB).contains(&peak), "{status}");
        let warnings = status["warnings"].as_array().unwrap();
        assert_eq!(warnings.len(), 1, "300 of 1,000 is used within its declaration: {status}");
        assert!(warnings[0]
            .as_str()
            .unwrap()
            .starts_with(&format!("declared {} bytes; observed high-water", 1000 * MIB)));
        let health = m.health(Some(&s));
        assert_eq!(health["claims"][0]["observed_peak_bytes"], peak);
        assert_eq!(health["protocol"], 1);
        drop(owner);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_release_carries_the_owners_final_high_water_mark_and_labels_its_figures_observed() {
        let dir = directory();
        let owner = Allocating::new(
            &dir,
            "burst = bytearray(b'x') * (600 << 20)\ndel burst\nheld = bytearray(b'x') * (100 << 20)\n\
             line = next(l for l in open('/proc/self/status') if l.startswith('VmHWM:'))\n\
             pathlib.Path('hwm').write_text(str(int(line.split()[1]) * 1024))",
        );
        let s = roomy(&[owner.pid()]);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        m.asked(&acquire(owner.pid(), "a", 400 * MIB), Some(&s), 0)
            .await
            .unwrap();
        ticks(&mut m, &mut journal, &s, 1).await;
        owner.run();
        ticks(&mut m, &mut journal, &s, 1).await;
        let reported: u64 = std::fs::read_to_string(dir.join("hwm"))
            .unwrap()
            .parse()
            .unwrap();
        let mut release = operation(owner.pid(), "a", "host_memory_release");
        release.0["high_water_bytes"] = json!(reported);
        let answer = m.asked(&release, Some(&s), 0).await.unwrap();
        assert_eq!(answer["released"], true);
        let high_water = answer["observed_high_water_bytes"].as_u64().unwrap();
        assert!(high_water >= 600 * MIB, "{answer}");
        assert!(
            answer["observed_peak_bytes"].as_u64().is_some(),
            "a process that peaks between ticks can be missed, so the figures are labelled observed: {answer}"
        );
        assert!(m.empty());
        assert_eq!(
            m.asked(&release, Some(&s), 0).await.unwrap(),
            json!({"released":true}),
            "a repeated release still answers released"
        );
        drop(owner);
        std::fs::remove_dir_all(dir).unwrap();
    }

    fn transfer(pid: u32, id: &str, to: u32) -> Asked {
        let mut request = operation(pid, id, "host_memory_transfer");
        request.0["to"] = json!({"pid": to, "birth": crate::procs::started(to).unwrap()});
        request
    }

    #[tokio::test]
    async fn a_claim_is_transferred_only_to_a_live_descendant_that_owns_no_claim() {
        let owner = std::process::id();
        let child = Owner::new();
        let claimant = Owner::new();
        let dir = directory();
        let s = sample(&[child.pid(), claimant.pid()]);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        m.asked(&acquire(owner, "a", 300), Some(&s), 0).await.unwrap();
        m.asked(&acquire(claimant.pid(), "b", 300), Some(&s), 0)
            .await
            .unwrap();
        ticks(&mut m, &mut journal, &s, 1).await;
        let sibling = m
            .asked(&transfer(claimant.pid(), "b", child.pid()), Some(&s), 0)
            .await
            .unwrap_err();
        assert!(sibling.contains("live descendant"), "{sibling}");
        let owning = m
            .asked(&transfer(owner, "a", claimant.pid()), Some(&s), 0)
            .await
            .unwrap_err();
        assert!(owning.contains("already owns"), "{owning}");
        let foreign = m
            .asked(&transfer(child.pid(), "a", child.pid()), Some(&s), 0)
            .await
            .unwrap_err();
        assert!(foreign.contains("different process identity"), "{foreign}");
        let moved = m
            .asked(&transfer(owner, "a", child.pid()), Some(&s), 0)
            .await
            .unwrap();
        assert_eq!(moved["owner_pid"], child.pid());
        assert_eq!(moved["state"], "granted");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_transfer_saved_before_its_answer_is_answered_again_by_a_successor() {
        let owner = std::process::id();
        let child = Owner::new();
        let dir = directory();
        let s = sample(&[child.pid()]);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        m.asked(&acquire(owner, "a", 300), Some(&s), 0).await.unwrap();
        ticks(&mut m, &mut journal, &s, 1).await;
        m.asked(&transfer(owner, "a", child.pid()), Some(&s), 0)
            .await
            .unwrap();
        // The connection closed before the answer; a successor restores the saved transfer.
        drop(m);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let resent = m
            .asked(&transfer(owner, "a", child.pid()), Some(&s), 0)
            .await
            .unwrap();
        assert_eq!(resent["owner_pid"], child.pid(), "the resent transfer succeeds");
        for op in ["host_memory_status", "host_memory_release", "host_memory_track"] {
            let refused = m
                .asked(&operation(owner, "a", op), Some(&s), 0)
                .await
                .unwrap_err();
            assert_eq!(refused, format!("host memory claim was transferred to {}", child.pid()), "{op}");
        }
        let again = m
            .asked(&acquire(owner, "a", 300), Some(&s), 0)
            .await
            .unwrap_err();
        assert!(again.contains("transferred to"), "{again}");
        let released = m
            .asked(&operation(child.pid(), "a", "host_memory_release"), Some(&s), 0)
            .await
            .unwrap();
        assert_eq!(released["released"], true);
        assert!(m.empty());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_claim_shows_its_declared_purpose_and_claims_of_every_purpose_are_served_alike() {
        let owners = [Owner::new(), Owner::new(), Owner::new(), Owner::new()];
        let dir = directory();
        let s = sample(&owners.iter().map(Owner::pid).collect::<Vec<_>>());
        let declared = |owner: &Owner, id: &str, bytes: u64, purpose: Value| {
            let mut request = acquire(owner.pid(), id, bytes);
            request.0["purpose"] = purpose;
            request
        };
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let placed = m
            .asked(&declared(&owners[0], "placed", 100, json!("gpu_place")), Some(&s), 0)
            .await
            .unwrap();
        assert_eq!(placed["purpose"], "gpu_place");
        let host = m
            .asked(&declared(&owners[1], "host", 100, json!("host_only")), Some(&s), 0)
            .await
            .unwrap();
        assert_eq!(host["purpose"], "host_only");
        let unknown = m
            .asked(&acquire(owners[2].pid(), "unknown", 100), Some(&s), 0)
            .await
            .unwrap();
        assert!(unknown["purpose"].is_null(), "gen-16 clients declare none");
        let conflicting = m
            .asked(&declared(&owners[0], "placed", 100, json!("host_only")), Some(&s), 0)
            .await
            .unwrap_err();
        assert!(conflicting.contains("conflicting"), "{conflicting}");
        assert!(m
            .asked(&declared(&owners[3], "bad", 100, json!("gpu")), Some(&s), 0)
            .await
            .unwrap_err()
            .contains("purpose"));
        let health = m.health(Some(&s));
        let purposes: BTreeMap<String, Value> = health["claims"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| (c["claim_id"].as_str().unwrap().to_owned(), c["purpose"].clone()))
            .collect();
        assert_eq!(purposes["placed"], "gpu_place");
        assert_eq!(purposes["host"], "host_only");
        assert!(purposes["unknown"].is_null());
        std::fs::remove_dir_all(&dir).unwrap();

        // The same requests with every purpose, none and mixed are granted in the same order at the same ticks.
        let assignments: [[Value; 4]; 4] = [
            [json!("gpu_place"), json!("gpu_place"), json!("gpu_place"), json!("gpu_place")],
            [json!("host_only"), json!("host_only"), json!("host_only"), json!("host_only")],
            [Value::Null, Value::Null, Value::Null, Value::Null],
            [json!("gpu_place"), Value::Null, json!("host_only"), json!("gpu_place")],
        ];
        let mut outcomes = Vec::new();
        for purposes in assignments {
            let dir = directory();
            let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
            let mut journal = Journal::new(Instance::default());
            for ((owner, purpose), (id, bytes)) in owners
                .iter()
                .zip(purposes)
                .zip([("w", 300), ("x", 500), ("y", 200), ("z", 400)])
            {
                m.asked(&declared(owner, id, bytes, purpose), Some(&s), 0)
                    .await
                    .unwrap();
            }
            let mut granted = Vec::new();
            for reserved in [600, 300, 0] {
                tick(&mut m, &mut journal, &s, reserved, 0, Shares::new(BTreeMap::new()))
                    .await
                    .unwrap();
                granted.push(
                    m.state
                        .claims
                        .values()
                        .filter(|c| c.granted)
                        .map(|c| c.id.clone())
                        .collect::<Vec<_>>(),
                );
            }
            outcomes.push(granted);
            std::fs::remove_dir_all(dir).unwrap();
        }
        assert!(outcomes.windows(2).all(|pair| pair[0] == pair[1]), "{outcomes:?}");
        assert_ne!(outcomes[0][0], outcomes[0][2], "the replay grants something along the way");
    }

    #[tokio::test]
    async fn a_claim_saved_by_generation_16_restores_with_no_purpose() {
        let owner = Owner::new();
        let dir = directory();
        let birth = crate::procs::started(owner.pid()).unwrap();
        std::fs::write(
            dir.join("host-memory.json"),
            serde_json::to_vec(&json!({"boot":"boot","sequence":1,"claims":{"a":{
                "id":"a","owner":{"pid":owner.pid(),"birth":birth},"bytes":600,"label":"gen 16","order":1,
                "granted":true,"baseline_anonymous":0,"failure":null,"retiring":false,"workers":{},
                "priority":0,"requested_at":0.0}}}))
            .unwrap(),
        )
        .unwrap();
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let s = sample(&[owner.pid()]);
        let status = m
            .asked(&operation(owner.pid(), "a", "host_memory_status"), Some(&s), 0)
            .await
            .unwrap();
        assert_eq!(status["state"], "granted");
        assert!(status["purpose"].is_null());
        assert!(status["observed_peak_bytes"].is_null());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn dead_owner_does_not_leave_a_stale_ram_reservation() {
        let mut owner = Owner::new();
        let dir = directory();
        let s = sample(&[owner.pid()]);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        m.asked(&acquire(owner.pid(), "a", 600), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &mut journal, &s, 0, 0, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        owner.0.kill().unwrap();
        owner.0.wait().unwrap();
        tick(&mut m, &mut journal, &s, 0, 0, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(m.empty());
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// An 8 GiB GPU-place claim gpu-8 of `owner`, registered.
    async fn place(m: &mut HostMemory, owner: &Owner, s: &Snapshot) {
        let mut request = acquire(owner.pid(), "gpu-8", 8 << 30);
        request.0["purpose"] = json!("gpu_place");
        m.asked(&request, Some(s), 0).await.unwrap();
    }
    /// gpu-8 released, then registered again under its ID.
    async fn replace(m: &mut HostMemory, owner: &Owner, s: &Snapshot) {
        m.asked(&operation(owner.pid(), "gpu-8", "host_memory_release"), Some(s), 0)
            .await
            .unwrap();
        place(m, owner, s).await;
    }
    /// One tick with nothing else pending; every record of `journal` so far must replay.
    async fn quiet_tick(m: &mut HostMemory, journal: &mut Journal, s: &Snapshot) {
        tick(m, journal, s, 0, 0, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
    }
    /// The ticks `journal` wrote, each with the subjects it listed.
    fn listed(journal: &Journal) -> Vec<(u64, Vec<Subject>)> {
        journal
            .history
            .iter()
            .filter(|(_, written)| *written)
            .map(|(record, _)| (record.tick, record.decisions.iter().map(|decided| decided.subject.clone()).collect()))
            .collect()
    }
    /// An instance that grants gpu-8 at three consecutive ticks, releasing it and registering it again before the
    /// second and the third: `history`'s ticks, each written, each listing the admission of that tick's registration.
    async fn regrant(instance: Instance, owner: &Owner, s: &Snapshot) -> Journal {
        let dir = directory();
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(instance);
        place(&mut m, owner, s).await;
        quiet_tick(&mut m, &mut journal, s).await;
        for _ in 0..2 {
            replace(&mut m, owner, s).await;
            quiet_tick(&mut m, &mut journal, s).await;
            assert!(m.state.claims["gpu-8"].granted);
        }
        std::fs::remove_dir_all(dir).unwrap();
        journal
    }

    #[tokio::test]
    async fn a_gpu_claim_registered_again_at_consecutive_ticks_lists_each_registrations_admission() {
        // Review round 6's finding 1, with registrations as subjects: each registration of gpu-8 is admitted once and
        // listed once; the replay carries every tick's admission, a GPU-place claim's grant by the share pass among them.
        let owner = Owner::new();
        let s = roomy(&[owner.pid()]);
        let journal = regrant(Instance::default(), &owner, &s).await;
        let gpu = |order| Subject::Claim(precedence::Registration { claim: "gpu-8".into(), order });
        assert_eq!(listed(&journal), vec![(1, vec![gpu(1)]), (2, vec![gpu(2)]), (3, vec![gpu(3)])]);
        decision::replays(&journal).unwrap();
    }

    #[tokio::test]
    async fn a_gpu_claim_granted_again_after_a_silent_tick_lists_its_admission_again() {
        let owner = Owner::new();
        let s = roomy(&[owner.pid()]);
        let dir = directory();
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        place(&mut m, &owner, &s).await;
        quiet_tick(&mut m, &mut journal, &s).await;
        // Released, it is pending nowhere at tick 2, which writes nothing; registered again, tick 3 grants it.
        m.asked(&operation(owner.pid(), "gpu-8", "host_memory_release"), Some(&s), 0)
            .await
            .unwrap();
        quiet_tick(&mut m, &mut journal, &s).await;
        for _ in 0..2 {
            place(&mut m, &owner, &s).await;
            quiet_tick(&mut m, &mut journal, &s).await;
            m.asked(&operation(owner.pid(), "gpu-8", "host_memory_release"), Some(&s), 0)
                .await
                .unwrap();
        }
        let gpu = |order| Subject::Claim(precedence::Registration { claim: "gpu-8".into(), order });
        assert_eq!(listed(&journal), vec![(1, vec![gpu(1)]), (3, vec![gpu(2)]), (4, vec![gpu(3)])]);
        decision::replays(&journal).unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn gpu_claims_granted_by_two_instances_replay_merged() {
        // Two instances each grant their gpu-8 at three consecutive ticks; merged tick by tick, each instance's
        // records carry only its own admissions.
        let owner = Owner::new();
        let s = roomy(&[owner.pid()]);
        let first = regrant(Instance::default(), &owner, &s).await;
        let second = regrant(Instance { pid: 2, ..Instance::default() }, &owner, &s).await;
        let written = |journal: &Journal| -> Vec<Record> {
            journal.history.iter().filter(|(_, written)| *written).map(|(record, _)| record.clone()).collect()
        };
        let merged: Vec<Record> = written(&first).into_iter().zip(written(&second)).flat_map(|(a, b)| [a, b]).collect();
        assert_eq!(merged.len(), 6);
        decision::check(&merged).unwrap();
    }

    /// Each claim's ID, owner, order and grant.
    fn catalogue(m: &HostMemory) -> Vec<(String, Identity, u64, bool)> {
        m.state.claims.values().map(|c| (c.id.clone(), c.owner.clone(), c.order, c.granted)).collect()
    }

    #[tokio::test]
    async fn a_failed_catalogue_read_refuses_restore_and_keeps_every_claim() {
        let (a, b, e) = (Owner::new(), Owner::new(), Owner::new());
        let dir = directory();
        let s = sample(&[a.pid(), b.pid(), e.pid()]);
        let (unit_w, unit_u, unit_v) = ("/user.slice/test.slice/w.scope", "/user.slice/test.slice/u.scope", "/user.slice/test.slice/v.scope");
        a.place(unit_w);
        b.place(unit_u);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        m.asked(&acquire(a.pid(), "c", 600), Some(&s), 0).await.unwrap();
        m.asked(&acquire(b.pid(), "d", 600), Some(&s), 0).await.unwrap();
        ticks(&mut m, &mut journal, &s, 1).await;
        let path = dir.join("host-memory.json");
        // c as generation 19 saved it, d as this generation saves it; b moves to another unit.
        let mut saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!((&saved["claims"]["c"]["party"], &saved["claims"]["d"]["party"]), (&json!(unit_w), &json!(unit_u)));
        saved["claims"]["c"].as_object_mut().unwrap().remove("party");
        std::fs::write(&path, serde_json::to_vec(&saved).unwrap()).unwrap();
        b.place(unit_v);
        let (bytes, kept) = (std::fs::read(&path).unwrap(), catalogue(&m));
        let owner = |o: &Owner| Identity::new(o.pid()).unwrap();
        assert_eq!((kept.clone(), m.state.sequence), (vec![("c".into(), owner(&a), 1, true), ("d".into(), owner(&b), 2, false)], 2));
        drop(m);

        let fault = store::Unreadable::at(&path);
        let refused = match HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await {
            Ok(m) => panic!("the faulted restore answered Ok with {} claims and sequence {}", m.state.claims.len(), m.state.sequence),
            Err(refused) => refused,
        };
        assert_eq!(refused, format!("{}: Permission denied (os error 13)", path.display()));
        fault.lift();
        assert_eq!(std::fs::read(&path).unwrap(), bytes, "the saved catalogue is intact");

        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        assert_eq!((catalogue(&m), m.state.sequence), (kept, 2), "the healthy restore keeps every claim and the sequence");
        let parties = (m.state.claims["c"].party.clone(), m.state.claims["d"].party.clone());
        assert_eq!(parties, (unit_w.to_owned(), unit_u.to_owned()), "d keeps its party; c, saved without one, takes a's unit");
        m.activated().await.unwrap();
        assert_eq!(saved_party(&dir, "c"), json!(unit_w), "activation saves the migrated party");
        let retried = m.asked(&acquire(b.pid(), "d", 600), Some(&s), 0).await.unwrap();
        assert_eq!((&retried["claim_id"], m.state.claims["d"].party.as_str()), (&json!("d"), unit_u), "the keyed retry finds d, its party U");
        let resent = m.asked(&acquire(a.pid(), "c", 600), Some(&s), 0).await.unwrap();
        assert_eq!(resent["state"], "granted", "the keyed retry finds its claim");
        let saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!((saved["claims"].as_object().unwrap().len(), &saved["sequence"]), (2, &json!(2)));
        m.asked(&acquire(e.pid(), "f", 100), Some(&s), 0).await.unwrap();
        assert_eq!(m.state.claims["f"].order, 3, "the restored instance kept the sequence");
        std::fs::remove_dir_all(dir).unwrap();

        let fresh = directory();
        let mut m = HostMemory::restore(&fresh, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        assert!(m.empty() && m.state.sequence == 0, "an absent catalogue restores empty, as today");
        m.asked(&acquire(a.pid(), "g", 100), Some(&s), 0).await.unwrap();
        assert_eq!(m.state.claims["g"].order, 1);
        std::fs::remove_dir_all(fresh).unwrap();
    }

    /// What a restart or a hand-over successor must restore: each claim's ID, owner, order, grant and previous owner,
    /// and the sequence.
    type Kept = (Vec<(String, Identity, u64, bool, Option<Identity>)>, u64);
    fn kept(m: &HostMemory) -> Kept {
        let claims = m.state.claims.values().map(|c| (c.id.clone(), c.owner.clone(), c.order, c.granted, c.previous_owner.clone()));
        (claims.collect(), m.state.sequence)
    }
    /// The state a successor restores from `dir`.
    async fn restored(dir: &std::path::Path) -> Kept {
        kept(&HostMemory::restore(dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap())
    }
    /// The saved file's claims and sequence, or null when nothing is saved.
    fn saved(dir: &std::path::Path) -> Value {
        std::fs::read(dir.join("host-memory.json")).ok().and_then(|bytes| serde_json::from_slice(&bytes).ok()).unwrap_or(Value::Null)
    }
    const WITHIN: std::time::Duration = std::time::Duration::from_secs(10);

    #[tokio::test]
    async fn a_registration_whose_save_is_not_published_is_undone_and_its_retry_is_saved() {
        let a = Owner::new();
        let dir = directory();
        let s = sample(&[a.pid()]);
        let store = Store::default();
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), store.clone(), &attribution()).await.unwrap();
        let mut hold = store.hold(Operation::new("claims", "register c"));
        let mut unwritable = None;
        let register = acquire(a.pid(), "c", 600);
        let (refused, ()) = tokio::join!(m.asked(&register, Some(&s), 0), async {
            hold.entered().await;
            unwritable = Some(store::Mode::set(&dir, 0o500));
            hold.release();
        });
        drop(unwritable);
        let refused = refused.unwrap_err();
        assert!(refused.contains("Permission denied"), "the staging write fails before any rename: {refused}");
        let pending = m.asked(&acquire(a.pid(), "c", 600), Some(&s), 0).await.unwrap();
        assert_eq!(pending["state"], "pending");
        let file = saved(&dir);
        assert_eq!(file["claims"]["c"]["order"], 1, "the file's claims holds c with order 1: {file}");
        assert_eq!(file["sequence"], 1);
        assert_eq!(restored(&dir).await, kept(&m), "a successor restores the instance's claims at its last answer");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_transfer_whose_save_is_not_published_is_undone_and_its_retry_is_saved() {
        let owner = std::process::id();
        let child = Owner::new();
        let dir = directory();
        let s = sample(&[child.pid()]);
        let store = Store::default();
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), store.clone(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        m.asked(&acquire(owner, "c", 300), Some(&s), 0).await.unwrap();
        ticks(&mut m, &mut journal, &s, 1).await;
        let mut hold = store.hold(Operation::new("claims", "transfer c"));
        let mut unwritable = None;
        let moving = transfer(owner, "c", child.pid());
        let (refused, ()) = tokio::join!(m.asked(&moving, Some(&s), 0), async {
            hold.entered().await;
            unwritable = Some(store::Mode::set(&dir, 0o500));
            hold.release();
        });
        drop(unwritable);
        assert!(refused.is_err(), "the transfer answers an error: {refused:?}");
        let status = m.asked(&operation(owner, "c", "host_memory_status"), Some(&s), 0).await;
        assert_eq!(status.map(|status| status["owner_pid"].clone()), Ok(json!(owner)), "o still owns c");
        let moved = m.asked(&transfer(owner, "c", child.pid()), Some(&s), 0).await.unwrap();
        assert_eq!(moved["owner_pid"], child.pid());
        assert_eq!(saved(&dir)["claims"]["c"]["owner"]["pid"], child.pid(), "the file's owner is k");
        assert_eq!(restored(&dir).await, kept(&m), "a successor restores the instance's claims at its last answer");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_registration_published_but_not_synced_is_kept_and_saved_again() {
        let a = Owner::new();
        let dir = directory();
        let s = sample(&[a.pid()]);
        let store = Store::default();
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), store.clone(), &attribution()).await.unwrap();
        let mut hold = store.hold(Operation::new("claims", "resave register c"));
        let unreadable = store::Mode::set(&dir, 0o300);
        let register = acquire(a.pid(), "c", 600);
        let (pending, ()) = tokio::join!(m.asked(&register, Some(&s), 0), async {
            tokio::time::timeout(WITHIN, hold.entered()).await.expect("the re-save is entered within 10 s");
            assert_eq!(saved(&dir)["claims"]["c"]["order"], 1, "the renamed file holds c, read through the search bit");
            drop(unreadable);
            hold.release();
        });
        assert_eq!(pending.unwrap()["state"], "pending");
        let file = saved(&dir);
        assert_eq!((&file["claims"]["c"]["order"], &file["sequence"]), (&json!(1), &json!(1)), "{file}");
        assert_eq!(store.serving(), Ok(()));
        assert_eq!(restored(&dir).await, kept(&m), "a successor restores the instance's claims at its last answer");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_published_state_that_cannot_be_synced_stops_the_store() {
        let (o, mut e, f) = (Owner::new(), Owner::new(), Owner::new());
        let dir = directory();
        let s = sample(&[o.pid(), e.pid(), f.pid()]);
        let store = Store::default();
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), store.clone(), &attribution()).await.unwrap();
        let mut journal = Journal::new(Instance::default());
        m.asked(&acquire(o.pid(), "c", 600), Some(&s), 0).await.unwrap();
        m.asked(&acquire(e.pid(), "d", 600), Some(&s), 0).await.unwrap();
        ticks(&mut m, &mut journal, &s, 1).await;
        assert!(m.state.claims["c"].granted && !m.state.claims["d"].granted);
        let unreadable = store::Mode::set(&dir, 0o300);
        let stopped = m.asked(&operation(o.pid(), "c", "host_memory_release"), Some(&s), 0).await.unwrap_err();
        assert!(stopped.contains("could not be synced"), "{stopped}");
        assert_eq!(store.serving(), Err(stopped.clone()), "the store has stopped");
        let before = kept(&m);
        let refused = m.asked(&acquire(f.pid(), "g", 100), Some(&s), 0).await;
        assert_eq!(refused, Err(stopped.clone()), "a registration's publication is refused before it runs");
        let answered = kept(&m);
        assert_eq!(answered, before, "the refused registration changed neither the claims nor the sequence");
        e.0.kill().unwrap();
        e.0.wait().unwrap();
        let opened = tick(&mut m, &mut journal, &s, 0, 0, Shares::new(BTreeMap::new())).await;
        assert_eq!(opened, Err(stopped), "open drops d, and its save is refused");
        drop(unreadable);
        let file = saved(&dir);
        assert!(file["claims"]["d"] != Value::Null && file["claims"]["c"] == Value::Null, "the file holds d and no c: {file}");
        assert_eq!(restored(&dir).await, answered, "a successor restores the claims of the instance's last answer");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_request_without_a_readable_sender_is_refused_by_its_reason() {
        let dir = directory();
        let s = sample(&[]);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let registration = |peer| (json!({"op":"host_memory_acquire","claim_id":"c","memory_bytes":100,"label":"fixture batch"}), peer);
        let unread = [
            (Peer::failed(Unread::Gone), "host memory owner is gone"),
            (Peer::failed(Unread::Failed("x".into())), "host memory owner's identity cannot be read: x"),
        ];
        for (peer, refusal) in unread {
            assert_eq!(m.asked(&registration(peer), Some(&s), 0).await, Err(refusal.into()));
            assert!(m.empty() && m.state.sequence == 0, "no claim exists and the sequence is unchanged");
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
    /// The party `id` has in the saved file.
    fn saved_party(dir: &std::path::Path, id: &str) -> Value {
        saved(dir)["claims"][id]["party"].clone()
    }

    #[tokio::test]
    async fn a_registration_whose_owner_cannot_be_read_is_refused() {
        let (gone, failed) = (Owner::new(), Owner::new());
        units().refuse(gone.pid(), gone.birth(), Unread::Gone);
        units().refuse(failed.pid(), failed.birth(), Unread::Failed("x".into()));
        let dir = directory();
        let s = sample(&[gone.pid(), failed.pid()]);
        let mut m = HostMemory::restore(&dir, "boot", Clock::system(), Store::default(), &attribution()).await.unwrap();
        let refused = m.asked(&acquire(gone.pid(), "c", 100), Some(&s), 0).await;
        assert_eq!(refused, Err("host memory owner is gone".into()));
        let refused = m.asked(&acquire(failed.pid(), "d", 100), Some(&s), 0).await;
        assert_eq!(refused, Err("host memory owner's cgroup cannot be read: x".into()));
        assert!(m.empty() && m.state.sequence == 0, "no claim exists and the sequence is unchanged");
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Pids no process has: above the kernel's pid_max.
    fn unused_pid(offset: u32) -> u32 {
        let maximum: u32 = std::fs::read_to_string("/proc/sys/kernel/pid_max").unwrap().trim().parse().unwrap();
        maximum + 1000 + offset
    }
    /// A claim as generation 19 saved it: no party.
    fn generation_19(id: &str, owner: (u32, u64), granted: bool, workers: &[(u32, u64)], previous: Option<(u32, u64)>) -> Value {
        let identity = |(pid, birth): (u32, u64)| json!({"pid": pid, "birth": birth});
        let workers: serde_json::Map<String, Value> = workers.iter().map(|&worker| (worker.0.to_string(), identity(worker))).collect();
        json!({"id":id,"owner":identity(owner),"bytes":600,"label":"generation 19","order":1,"granted":granted,"baseline_anonymous":0,
            "failure":null,"retiring":false,"workers":workers,"priority":0,"requested_at":0.0,"observed":{"peak":null,"processes":[]},
            "purpose":null,"previous_owner":previous.map(identity)})
    }
    /// Writes `claims` as generation 19's catalogue in `dir`; returns its bytes.
    fn saved_by_generation_19(dir: &std::path::Path, claims: &[Value]) -> Vec<u8> {
        let claims: serde_json::Map<String, Value> = claims.iter().map(|claim| (claim["id"].as_str().unwrap().to_owned(), claim.clone())).collect();
        let bytes = serde_json::to_vec(&json!({"boot":"boot","sequence":claims.len(),"claims":claims})).unwrap();
        std::fs::write(dir.join("host-memory.json"), &bytes).unwrap();
        bytes
    }
    /// A restore of `dir` whose processes are read in `units`.
    async fn migrating(dir: &std::path::Path, units: &Units) -> Result<HostMemory, String> {
        HostMemory::restore(dir, "boot", Clock::system(), Store::default(), &Attribution::reading(units.reader())).await
    }

    #[tokio::test]
    async fn a_claim_saved_without_a_party_takes_its_current_owners_unit() {
        let (owner, previous) = ((unused_pid(100), 7), (unused_pid(50), 3));
        let dir = directory();
        let bytes = saved_by_generation_19(&dir, &[generation_19("c", owner, false, &[], Some(previous))]);
        let units = Units::default();
        units.place(owner.0, owner.1, "/u.scope");
        units.place(previous.0, previous.1, "/v.scope");
        let mut m = migrating(&dir, &units).await.unwrap();
        assert_eq!(m.state.claims["c"].party, "/u.scope");
        assert_eq!(std::fs::read(dir.join("host-memory.json")).unwrap(), bytes, "restore writes nothing");
        m.activated().await.unwrap();
        assert_eq!(saved_party(&dir, "c"), json!("/u.scope"), "activation saves the migrated party");
        let saved = std::fs::read(dir.join("host-memory.json")).unwrap();
        units.place(owner.0, owner.1, "/w.scope");
        let mut again = migrating(&dir, &units).await.unwrap();
        assert_eq!(again.state.claims["c"].party, "/u.scope", "a saved party is read, never migrated again");
        again.activated().await.unwrap();
        assert_eq!(std::fs::read(dir.join("host-memory.json")).unwrap(), saved, "the second activation writes nothing");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn repeated_transfers_migrate_to_the_current_owners_unit() {
        let (current, previous) = ((unused_pid(200), 9), (unused_pid(150), 8));
        let dir = directory();
        saved_by_generation_19(&dir, &[generation_19("c", current, true, &[], Some(previous))]);
        let units = Units::default();
        units.place(current.0, current.1, "/u.scope");
        units.place(previous.0, previous.1, "/a.scope");
        assert_eq!(migrating(&dir, &units).await.unwrap().state.claims["c"].party, "/u.scope");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_claim_whose_owner_is_gone_migrates_to_its_first_readable_identity() {
        let units = Units::default();
        let worker = (unused_pid(1), 11);
        let previous = (unused_pid(2), 12);
        units.place(worker.0, worker.1, "/w.scope");
        units.place(previous.0, previous.1, "/v.scope");
        let dir = directory();
        saved_by_generation_19(&dir, &[
            generation_19("a", (unused_pid(3), 13), true, &[worker], None),
            generation_19("b", (unused_pid(4), 14), false, &[], Some(previous)),
            generation_19("c", (unused_pid(5), 15), false, &[], None),
        ]);
        let mut m = migrating(&dir, &units).await.unwrap();
        let party = |m: &HostMemory, id: &str| m.state.claims[id].party.clone();
        assert_eq!(party(&m, "a"), "/w.scope", "(a) a granted claim's live worker");
        assert_eq!(party(&m, "b"), "/v.scope", "(b) its previous owner");
        assert_eq!(party(&m, "c"), party::UNIT_UNKNOWN, "(c) nothing readable");
        let mut journal = Journal::new(Instance::default());
        tick(&mut m, &mut journal, &sample(&[]), 0, 0, Shares::new(BTreeMap::new())).await.unwrap();
        assert!(!m.state.claims.contains_key("c"), "(c) the first open drops it");
        std::fs::remove_dir_all(dir).unwrap();

        let live = Owner::new();
        units.refuse(live.pid(), live.birth(), Unread::Failed("x".into()));
        let dir = directory();
        saved_by_generation_19(&dir, &[generation_19("d", (live.pid(), live.birth()), false, &[], None)]);
        let refused = migrating(&dir, &units).await.err().unwrap();
        assert!(refused.contains("claim d") && refused.contains(&live.pid().to_string()) && refused.ends_with("x"), "(d) {refused}");
        std::fs::remove_dir_all(dir).unwrap();

        let working = Owner::new();
        units.refuse(working.pid(), working.birth(), Unread::Failed("y".into()));
        let dir = directory();
        saved_by_generation_19(&dir, &[generation_19("e", (unused_pid(6), 16), true, &[(working.pid(), working.birth())], None)]);
        let refused = migrating(&dir, &units).await.err().unwrap();
        assert!(refused.contains("claim e") && refused.contains(&working.pid().to_string()) && refused.ends_with("y"), "(e) {refused}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_reused_owner_pid_never_lends_its_unit() {
        let (pid, previous) = (unused_pid(100), (unused_pid(50), 3));
        let units = Units::default();
        units.place(pid, 9, "/x.scope");
        units.place(previous.0, previous.1, "/v.scope");
        let dir = directory();
        saved_by_generation_19(&dir, &[generation_19("c", (pid, 7), false, &[], Some(previous))]);
        assert_eq!(migrating(&dir, &units).await.unwrap().state.claims["c"].party, "/v.scope", "not /x.scope");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
