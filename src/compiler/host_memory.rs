//! Atomic host RAM admission, independent of compiler slots and CPU affinity.
//! Claims and compiler demand take turns by party: the party holding the least
//! RAM goes next.
//! Only the channel actor mutates claims. Durable PID birth identities survive
//! handover and cannot be kept alive by recycled PIDs or forked token holders.
use super::{
    budget::Budget,
    decision::{self, Counted, Decision, Decisions, Entry, Phase, Record, Subject},
    fairness::{self, Shares},
    model::Purpose,
    party,
    precedence::{self, At, Candidate, Catalogue, Class, Figures, Head, Precedence, RequestId, Standing, Work},
    resources::Snapshot,
    store,
    watch::{Reply, Watches},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

// Old clients re-send acquisitions under a 30 s request deadline.
const ACQUIRE_HOLD: std::time::Duration = std::time::Duration::from_secs(20);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Identity {
    pid: u32,
    birth: u64,
}
impl Identity {
    fn new(pid: u32) -> Result<Self, String> {
        Ok(Self {
            pid,
            birth: crate::procs::started(pid).ok_or("host memory owner is gone")?,
        })
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
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Claim {
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
}
impl Claim {
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
#[derive(Default, Serialize, Deserialize)]
struct Claims {
    boot: String,
    sequence: u64,
    claims: BTreeMap<String, Claim>,
}
pub struct HostMemory {
    path: PathBuf,
    state: Claims,
    allocated: BTreeMap<String, u64>,
    parties: BTreeMap<String, String>,
    watches: Watches,
    /// Memory the last tick left for claims, after every grant it made.
    admissible: Option<u64>,
    /// What this tick's `open` found admissible.
    opened: Option<Opened>,
    /// The work counted in this tick's projection.
    live: BTreeSet<Work>,
    precedence: Precedence,
}
/// What a tick's `open` found: the budget the aged head is judged by, and S, what would be admissible once the
/// service's own work that ends has ended.
#[derive(Clone, Copy, Debug, Default)]
pub struct Opened {
    pub left: Budget,
    pub serviceable: u64,
}
impl HostMemory {
    pub async fn restore(directory: &std::path::Path, boot: &str) -> Result<Self, String> {
        let path = directory.join("host-memory.json");
        let mut state: Claims = if path.exists() {
            serde_json::from_value(store::read(&path).await?).map_err(|e| e.to_string())?
        } else {
            Claims::default()
        };
        if state.boot != boot {
            state = Claims {
                boot: boot.into(),
                ..Claims::default()
            };
        }
        Ok(Self {
            path,
            state,
            allocated: BTreeMap::new(),
            parties: BTreeMap::new(),
            watches: Watches::default(),
            admissible: None,
            opened: None,
            live: BTreeSet::new(),
            precedence: Precedence::default(),
        })
    }
    async fn save(&self) -> Result<(), String> {
        store::write(
            &self.path,
            &serde_json::to_value(&self.state).map_err(|e| e.to_string())?,
        )
        .await
    }
    fn owned(&self, id: &str, pid: u32) -> Result<&Claim, String> {
        let claim = self
            .state
            .claims
            .get(id)
            .ok_or("unknown or released host memory claim")?;
        if claim.owner.pid != pid || !claim.owner.alive() {
            return Err(match &claim.previous_owner {
                Some(previous) if previous.pid == pid => {
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
            "claims":self.state.claims.iter().map(|(id, claim)| {
                let mut status = Self::status(claim);
                status["party"] = json!(self.parties.get(id));
                if !claim.granted && claim.failure.is_none() {
                    status["pending_seconds"] = json!((claim.requested_at > 0.0)
                        .then(|| crate::protocol::now_unix() - claim.requested_at));
                    status["waiting"] = json!(self.admissible.map(|left| self.waiting(claim, left)));
                }
                status
            }).collect::<Vec<_>>()})
    }
    /// Why a pending claim waits.
    fn waiting(&self, claim: &Claim, left: u64) -> String {
        let id = RequestId::Claim { claim: claim.id.clone() };
        let serviceable = self.opened.unwrap_or_default().serviceable;
        let aged = precedence::aged(claim.requested_at, crate::protocol::now_unix());
        match (self.precedence.head().filter(|head| head.id == id), Class::of(claim.bytes).waiting()) {
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
        sample: Option<&Snapshot>,
        headroom: u64,
    ) -> Result<Value, String> {
        let pid = request["_peer_pid"]
            .as_u64()
            .and_then(|p| u32::try_from(p).ok())
            .ok_or("authenticated host memory owner missing")?;
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
                    self.owned(id, pid)?;
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
                    .any(|c| c.owner.pid == pid && c.owner.alive())
                {
                    return Err("one aggregate host memory claim per process; release it before requesting another".into());
                }
                if sample
                    .is_some_and(|s| s.capacity > 0 && bytes > s.capacity.saturating_sub(headroom))
                {
                    return Err("host memory requirement exceeds host/cgroup capacity after required headroom".into());
                }
                let owner = Identity::new(pid)?;
                self.state.sequence = self
                    .state
                    .sequence
                    .checked_add(1)
                    .ok_or("host memory sequence exhausted")?;
                let claim = Claim {
                    id: id.into(),
                    owner,
                    bytes,
                    label,
                    priority,
                    order: self.state.sequence,
                    granted: false,
                    baseline_anonymous: 0,
                    failure: None,
                    retiring: false,
                    workers: BTreeMap::new(),
                    requested_at: crate::protocol::now_unix(),
                    observed: Observed::default(),
                    purpose,
                    previous_owner: None,
                };
                let result = Self::status(&claim);
                self.state.claims.insert(id.into(), claim);
                self.save().await?;
                Ok(result)
            }
            "host_memory_status" => Ok(Self::status(self.owned(id, pid)?)),
            "host_memory_transfer" => self.transfer(id, pid, Identity::named(&request["to"])?).await,
            "host_memory_track" => {
                let claim = self.owned(id, pid)?;
                if !claim.granted {
                    return Err("host memory must be granted before registering workers".into());
                }
                let worker = request["worker_pid"]
                    .as_u64()
                    .and_then(|p| u32::try_from(p).ok())
                    .ok_or("invalid host memory worker")?;
                let identity = Identity::new(worker)?;
                if worker == pid || !Self::descends(worker, pid, crate::procs::parent) {
                    return Err(
                        "host memory worker must be a descendant of its authenticated owner".into(),
                    );
                }
                self.state
                    .claims
                    .get_mut(id)
                    .unwrap()
                    .workers
                    .insert(worker, identity);
                self.save().await?;
                Ok(Self::status(self.owned(id, pid)?))
            }
            "host_memory_abandon" => {
                if !self.state.claims.contains_key(id) {
                    return Ok(json!({"released":true}));
                }
                self.owned(id, pid)?;
                self.state.claims.get_mut(id).unwrap().retiring = true;
                self.save().await?;
                Ok(json!({"retiring":true}))
            }
            "host_memory_release" => {
                // Release is idempotent after an interrupted RPC response.
                if !self.state.claims.contains_key(id) {
                    return Ok(json!({"released":true}));
                }
                let claim = self.owned(id, pid)?;
                if claim.workers.values().any(Identity::alive) {
                    return Err(
                        "join all registered workers before releasing their host memory claim"
                            .into(),
                    );
                }
                let mut claim = self.state.claims.remove(id).expect("owned claim");
                // The releasing owner's own final high-water mark, read after its last allocation.
                if let Some(bytes) = request["high_water_bytes"].as_u64() {
                    claim
                        .observed
                        .high_water(claim.owner.pid, claim.owner.birth, bytes);
                }
                self.save().await?;
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
    async fn transfer(&mut self, id: &str, pid: u32, to: Identity) -> Result<Value, String> {
        let claim = self
            .state
            .claims
            .get(id)
            .ok_or("unknown or released host memory claim")?;
        let requester = Identity::new(pid)?;
        if claim.previous_owner.as_ref() == Some(&requester) && claim.owner == to {
            return Ok(Self::status(claim));
        }
        self.owned(id, pid)?;
        if to.pid == pid || !to.alive() || !Self::descends(to.pid, pid, crate::procs::parent) {
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
        let claim = self.state.claims.get_mut(id).expect("owned claim");
        claim.previous_owner = Some(std::mem::replace(&mut claim.owner, to));
        self.save().await?;
        Ok(Self::status(&self.state.claims[id]))
    }
    /// Registers the claim and answers once it is decided. Clients re-send
    /// host_memory_acquire until granted under a 30 s request deadline, so a
    /// pending answer is held at most ACQUIRE_HOLD instead of their polling.
    pub async fn wait(
        &mut self,
        request: &Value,
        sample: Option<&Snapshot>,
        headroom: u64,
        reply: Reply,
    ) {
        let answer = self.request(request, sample, headroom).await;
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
    /// The tick's first phase: ended claims retire, allocations are sampled, claims are attributed to parties,
    /// and the budget is computed once, signed. `fresh` is a reading of available memory taken now, beside the
    /// sample's; `compiles` are the running compiles the service's projection counts. Returns the budget the aged
    /// head is judged by and S, what the service's own work can free.
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        &mut self,
        sample: &Snapshot,
        fresh: u64,
        compiler_reserved: u64,
        headroom: u64,
        parties: &BTreeSet<String>,
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
        self.parties = self
            .state
            .claims
            .iter()
            .map(|(id, c)| (id.clone(), party::attribute(sample, c.owner.pid, parties)))
            .collect();
        record.available = fresh.min(sample.available);
        record.compiler_reserved = compiler_reserved;
        record.minimum_ram = headroom;
        record.claims_unused = self.reserved(sample);
        record.own_work = compiles;
        record.own_work.extend(self.state.claims.values().filter(|c| c.granted && c.purpose == Some(Purpose::HostOnly)).map(
            |claim| Counted {
                work: Work::Claim {
                    claim: claim.id.clone(),
                    order: claim.order,
                },
                bytes: claim.bytes,
            },
        ));
        self.live = record.own_work.iter().map(|counted| counted.work.clone()).collect();
        self.precedence.settle(&self.live);
        let left = record.budget();
        let own = record.own_work.iter().fold(0u64, |sum, counted| sum.saturating_add(counted.bytes));
        let opened = Opened {
            left,
            serviceable: decision::serviceable(left, own),
        };
        record.left = opened.left;
        record.serviceable = opened.serviceable;
        self.opened = Some(opened);
        self.settle(changed, ended, Vec::new()).await?;
        Ok(opened)
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
            let id = RequestId::Claim { claim: claim.id.clone() };
            (precedence::aged(claim.requested_at, now) && !self.precedence.gives_way(&id)).then(|| Candidate {
                id,
                party: self.parties[&claim.id].clone(),
                eligible_at: At(claim.requested_at),
                catalogue: Catalogue::Claim { order: claim.order },
                bytes: claim.bytes,
                capacity: None,
            })
        }));
        let standing = self.precedence.head().map(|head| match &head.id {
            RequestId::Claim { claim } => Standing {
                pending: self.state.claims.get(claim).is_some_and(|claim| waits(claim, u64::MAX)),
                bytes: self.state.claims.get(claim).map_or(head.bytes, |claim| claim.bytes),
                capacity: None,
            },
            RequestId::Compile { .. } => compile.clone().unwrap_or(Standing {
                pending: false,
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
            && Class::of(claim.bytes) == Class::Ordinary
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
            Some((RequestId::Claim { claim }, bytes, true)) => {
                record.admitted.push(Entry::Granted {
                    phase: Phase::Serve,
                    claim: claim.clone(),
                    class: Class::of(bytes),
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
        let (served, kept) = self.share_pass(sample, headroom, shares, &mut budget, Phase::Serve, record, &mut decided);
        record.admitted.push(Entry::Returned { bytes: kept, budget });
        budget = budget.plus(kept);
        self.settle(changed || served, Vec::new(), decided).await?;
        Ok(budget)
    }
    /// After the tick's ordinary launches, claims of `class` still pending are offered what is left, by party
    /// share: ordinary claims what the ordinary launches left, large claims what every smaller request left.
    /// `held` is each party's holding of running compiles. Returns what the pass leaves.
    pub async fn close(
        &mut self,
        sample: &Snapshot,
        headroom: u64,
        held: BTreeMap<String, u64>,
        class: Class,
        mut budget: Budget,
        record: &mut Record,
    ) -> Result<Budget, String> {
        let mut shares = Shares::new(held);
        let mut decided = Vec::new();
        self.wait_by_share(&mut shares, class);
        let (changed, _) =
            self.share_pass(sample, headroom, shares, &mut budget, Phase::closing(class), record, &mut decided);
        self.admissible = Some(budget.floor());
        self.settle(changed, Vec::new(), decided).await?;
        Ok(budget)
    }
    /// Granted claims count towards their party's holding; pending claims of `class` other than the aged head
    /// wait for their turn.
    fn wait_by_share(&self, shares: &mut Shares<Turn>, class: Class) {
        let head = self.precedence.head().map(|head| &head.id);
        for claim in self.state.claims.values() {
            let party = &self.parties[&claim.id];
            let is_head = head == Some(&RequestId::Claim { claim: claim.id.clone() });
            match (claim.granted, &claim.failure) {
                (true, _) => shares.grant(party, fairness::charge(claim.bytes, claim.priority)),
                (false, None) if Class::of(claim.bytes) == class && !is_head => {
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
        phase: Phase,
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
                class: Class::of(bytes),
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
                let id = RequestId::Claim { claim: claim.id.clone() };
                let decision = match head.filter(|head| head.id == id) {
                    Some(head) => Decision::head(head.fits),
                    None => Decision::waiting(
                        Class::of(claim.bytes),
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
        json!({"gpu":null,"aged":aged,"turn":self.precedence.turn()})
    }
    /// Decisions become durable, then their waiters learn them.
    async fn settle(
        &mut self,
        changed: bool,
        ended: Vec<String>,
        decided: Vec<String>,
    ) -> Result<(), String> {
        if changed {
            self.save().await?;
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
    use super::*;
    struct Owner(std::process::Child);
    impl Owner {
        fn new() -> Self {
            Self(
                std::process::Command::new("sleep")
                    .arg("60")
                    .spawn()
                    .unwrap(),
            )
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
    fn directory() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "host-memory-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
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
    /// as the engine's launches would. Its record must replay.
    async fn tick(
        m: &mut HostMemory,
        s: &Snapshot,
        compiler_reserved: u64,
        headroom: u64,
        parties: &BTreeSet<String>,
        turns: Shares<Turn>,
    ) -> Result<(), String> {
        let mut record = Record::default();
        m.open(s, u64::MAX, compiler_reserved, headroom, parties, Vec::new(), &mut record).await?;
        m.choose(s, headroom, Vec::new(), None, false, crate::protocol::now_unix(), &mut record);
        let mut budget = m.serve(s, headroom, turns, &mut record).await?;
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
                phase: Phase::Launch,
                group: party.clone(),
                job: party,
                class: Class::of(bytes),
                bytes,
                budget,
                reserved: 0,
                head: false,
                ahead: None,
            });
            budget = budget.less(bytes);
        }
        record.left_after_launch = budget.floor();
        let budget = m.close(s, headroom, BTreeMap::new(), Class::Ordinary, budget, &mut record).await?;
        m.close(s, headroom, BTreeMap::new(), Class::Large, budget, &mut record).await?;
        decision::replay(&record)
    }
    fn acquire(pid: u32, id: &str, bytes: u64) -> Value {
        json!({"op":"host_memory_acquire","_peer_pid":pid,"claim_id":id,"memory_bytes":bytes,"label":"fixture batch"})
    }
    fn operation(pid: u32, id: &str, op: &str) -> Value {
        json!({"op":op,"_peer_pid":pid,"claim_id":id})
    }
    #[tokio::test]
    async fn aggregate_claims_are_atomic_and_never_consume_cpu_slots() {
        let a = Owner::new();
        let b = Owner::new();
        let dir = directory();
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        let s = sample(&[a.pid(), b.pid()]);
        m.request(&acquire(a.pid(), "a", 600), Some(&s), 100)
            .await
            .unwrap();
        m.request(&acquire(b.pid(), "b", 400), Some(&s), 100)
            .await
            .unwrap();
        tick(&mut m, &s, 400, 100, &BTreeSet::new(), Shares::new(BTreeMap::new()))
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
        m.request(
            &operation(b.pid(), "b", "host_memory_release"),
            Some(&s),
            100,
        )
        .await
        .unwrap();
        tick(&mut m, &s, 0, 100, &BTreeSet::new(), Shares::new(BTreeMap::new()))
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
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        let s = sample(&[a.pid(), b.pid()]);
        let wait = |pid, id, bytes| {
            let mut request = acquire(pid, id, bytes);
            request["op"] = json!("host_memory_wait");
            request
        };
        let (reply, mut granted) = tokio::sync::oneshot::channel();
        m.wait(&wait(a.pid(), "a", 600), Some(&s), 0, reply).await;
        let (reply, mut released) = tokio::sync::oneshot::channel();
        m.wait(&wait(b.pid(), "b", 600), Some(&s), 0, reply).await;
        assert!(granted.try_recv().is_err() && released.try_recv().is_err());
        tick(&mut m, &s, 500, 0, &BTreeSet::new(), Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(
            granted.try_recv().is_err(),
            "a claim that does not fit stays parked"
        );
        tick(&mut m, &s, 0, 0, &BTreeSet::new(), Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert_eq!(granted.try_recv().unwrap().unwrap()["state"], "granted");
        let (reply, mut held) = tokio::sync::oneshot::channel();
        m.wait(&acquire(b.pid(), "b", 600), Some(&s), 0, reply)
            .await;
        assert!(
            held.try_recv().is_err(),
            "a re-sent acquisition is held, not polled"
        );
        m.answer_expired(std::time::Instant::now() + ACQUIRE_HOLD);
        assert_eq!(held.try_recv().unwrap().unwrap()["state"], "pending");
        let (reply, mut held) = tokio::sync::oneshot::channel();
        m.wait(&acquire(b.pid(), "b", 600), Some(&s), 0, reply)
            .await;
        m.request(&operation(b.pid(), "b", "host_memory_release"), Some(&s), 0)
            .await
            .unwrap();
        assert!(released
            .try_recv()
            .unwrap()
            .unwrap_err()
            .contains("released"));
        assert!(held.try_recv().unwrap().unwrap_err().contains("released"));
        let (reply, answered) = tokio::sync::oneshot::channel();
        m.wait(&wait(a.pid(), "a", 600), Some(&s), 0, reply).await;
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
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        m.request(&acquire(a.pid(), "large", 700), Some(&s), 0)
            .await
            .unwrap();
        m.request(&acquire(b.pid(), "small", 400), Some(&s), 0)
            .await
            .unwrap();

        // The large claim cannot fit after compiler reservations, but the
        // later claim can.  It must be granted without exceeding capacity.
        tick(&mut m, &s, 400, 0, &BTreeSet::new(), Shares::new(BTreeMap::new()))
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
        let parties = attributed(&mut s, &owners);
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        for (i, owner) in campaign.iter().enumerate() {
            m.request(&acquire(owner.pid(), &format!("c{i}"), 300), Some(&s), 0)
                .await
                .unwrap();
        }
        m.request(&acquire(small.pid(), "small", 300), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &s, 400, 0, &parties, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(m.state.claims["c0"].granted);
        assert!(
            m.state.claims["small"].granted,
            "the party holding no RAM goes before the campaign's second claim"
        );
        assert!(!m.state.claims["c1"].granted && !m.state.claims["c2"].granted);
        assert_eq!(m.health(Some(&s))["claims"][3]["party"], "session-200-200");
        std::fs::remove_dir_all(dir).unwrap();
    }
    /// Each claim owner leads its own session, spawned by its party's driver,
    /// which leads session `driver` born at tick `driver`.
    fn attributed(sample: &mut Snapshot, owners: &[(u32, u32)]) -> BTreeSet<String> {
        owners
            .iter()
            .map(|&(owner, driver)| {
                sample.processes.insert(owner, (driver, 0));
                sample.sessions.insert(owner, (owner, 0));
                sample.sessions.insert(driver, (driver, u64::from(driver)));
                format!("session-{driver}-{driver}")
            })
            .collect()
    }
    #[tokio::test]
    async fn a_priority_party_holds_a_weighted_share() {
        let owners = [Owner::new(), Owner::new(), Owner::new(), Owner::new()];
        let dir = directory();
        let mut s = sample(&owners.iter().map(Owner::pid).collect::<Vec<_>>());
        let drivers = [100, 200, 100, 200];
        let pairs: Vec<_> = owners.iter().map(Owner::pid).zip(drivers).collect();
        let parties = attributed(&mut s, &pairs);
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        let claim = |owner: &Owner, id: &str, bytes: u64, priority: i8| {
            let mut request = acquire(owner.pid(), id, bytes);
            request["priority"] = json!(priority);
            request
        };
        m.request(&claim(&owners[0], "ordinary-held", 100, 0), Some(&s), 0)
            .await
            .unwrap();
        m.request(&claim(&owners[1], "urgent-held", 500, 10), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &s, 200, 0, &parties, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        m.request(&claim(&owners[2], "ordinary-next", 200, 0), Some(&s), 0)
            .await
            .unwrap();
        m.request(&claim(&owners[3], "urgent-next", 200, 10), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &s, 200, 0, &parties, Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(
            m.state.claims["urgent-next"].granted,
            "500 bytes at priority 10 weigh less than 100 bytes at priority 0"
        );
        assert!(!m.state.claims["ordinary-next"].granted);
        assert!(m
            .request(&claim(&owners[3], "urgent-next", 200, 0), Some(&s), 0)
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
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        let none = || Shares::new(BTreeMap::new());
        m.request(&acquire(a.pid(), "held", 300), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &s, 400, 0, &BTreeSet::new(), none()).await.unwrap();
        m.request(&acquire(b.pid(), "next", 300), Some(&s), 0)
            .await
            .unwrap();
        let mut compilers = none();
        compilers.wait("compiles", Turn::Compiler(300, 0));
        tick(&mut m, &s, 400, 0, &BTreeSet::new(), compilers)
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
        tick(&mut m, &s, 100, 0, &BTreeSet::new(), compilers)
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
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        m.request(&acquire(a.pid(), "a", 600), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &s, 0, 0, &BTreeSet::new(), Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(m
            .request(&acquire(a.pid(), "b", 1), Some(&s), 0)
            .await
            .unwrap_err()
            .contains("aggregate"));
        assert!(m
            .request(&operation(b.pid(), "a", "host_memory_release"), Some(&s), 0)
            .await
            .is_err());
        drop(m);
        let m = HostMemory::restore(&dir, "boot").await.unwrap();
        assert!(m.state.claims["a"].granted);
        assert_eq!(m.reserved(&s), 600);
        let m = HostMemory::restore(&dir, "different-boot").await.unwrap();
        assert!(m.empty());
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test]
    async fn impossible_claims_fail_before_any_preparation() {
        let a = Owner::new();
        let dir = directory();
        let s = sample(&[a.pid()]);
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        assert!(m
            .request(&acquire(a.pid(), "a", 901), Some(&s), 100)
            .await
            .unwrap_err()
            .contains("capacity"));
        m.request(&acquire(a.pid(), "a", 901), None, 100)
            .await
            .unwrap();
        tick(&mut m, &s, 0, 100, &BTreeSet::new(), Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        let state = m
            .request(
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
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        m.request(&acquire(owner, "a", 600), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &s, 0, 0, &BTreeSet::new(), Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        let mut track = operation(owner, "a", "host_memory_track");
        track["worker_pid"] = json!(worker.pid());
        m.request(&track, Some(&s), 0).await.unwrap();
        assert!(m
            .request(&operation(owner, "a", "host_memory_release"), Some(&s), 0)
            .await
            .unwrap_err()
            .contains("join"));
        m.request(&operation(owner, "a", "host_memory_abandon"), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &s, 0, 0, &BTreeSet::new(), Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(!m.empty());
        worker.0.kill().unwrap();
        worker.0.wait().unwrap();
        tick(&mut m, &s, 0, 0, &BTreeSet::new(), Shares::new(BTreeMap::new()))
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
    async fn ticks(m: &mut HostMemory, s: &Snapshot, count: usize) {
        for _ in 0..count {
            tick(m, s, 0, 0, &BTreeSet::new(), Shares::new(BTreeMap::new()))
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
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        m.request(&acquire(owner.pid(), "a", 1000 * MIB), Some(&s), 0)
            .await
            .unwrap();
        ticks(&mut m, &s, 1).await;
        let pending = m
            .request(&operation(owner.pid(), "a", "host_memory_status"), Some(&s), 0)
            .await
            .unwrap();
        assert_eq!(pending["state"], "granted");
        owner.run();
        ticks(&mut m, &s, 4).await;
        let status = m
            .request(&operation(owner.pid(), "a", "host_memory_status"), Some(&s), 0)
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
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        m.request(&acquire(owner.pid(), "a", 400 * MIB), Some(&s), 0)
            .await
            .unwrap();
        ticks(&mut m, &s, 1).await;
        owner.run();
        ticks(&mut m, &s, 1).await;
        let reported: u64 = std::fs::read_to_string(dir.join("hwm"))
            .unwrap()
            .parse()
            .unwrap();
        let mut release = operation(owner.pid(), "a", "host_memory_release");
        release["high_water_bytes"] = json!(reported);
        let answer = m.request(&release, Some(&s), 0).await.unwrap();
        assert_eq!(answer["released"], true);
        let high_water = answer["observed_high_water_bytes"].as_u64().unwrap();
        assert!(high_water >= 600 * MIB, "{answer}");
        assert!(
            answer["observed_peak_bytes"].as_u64().is_some(),
            "a process that peaks between ticks can be missed, so the figures are labelled observed: {answer}"
        );
        assert!(m.empty());
        assert_eq!(
            m.request(&release, Some(&s), 0).await.unwrap(),
            json!({"released":true}),
            "a repeated release still answers released"
        );
        drop(owner);
        std::fs::remove_dir_all(dir).unwrap();
    }

    fn transfer(pid: u32, id: &str, to: u32) -> Value {
        let mut request = operation(pid, id, "host_memory_transfer");
        request["to"] = json!({"pid": to, "birth": crate::procs::started(to).unwrap()});
        request
    }

    #[tokio::test]
    async fn a_claim_is_transferred_only_to_a_live_descendant_that_owns_no_claim() {
        let owner = std::process::id();
        let child = Owner::new();
        let claimant = Owner::new();
        let dir = directory();
        let s = sample(&[child.pid(), claimant.pid()]);
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        m.request(&acquire(owner, "a", 300), Some(&s), 0).await.unwrap();
        m.request(&acquire(claimant.pid(), "b", 300), Some(&s), 0)
            .await
            .unwrap();
        ticks(&mut m, &s, 1).await;
        let sibling = m
            .request(&transfer(claimant.pid(), "b", child.pid()), Some(&s), 0)
            .await
            .unwrap_err();
        assert!(sibling.contains("live descendant"), "{sibling}");
        let owning = m
            .request(&transfer(owner, "a", claimant.pid()), Some(&s), 0)
            .await
            .unwrap_err();
        assert!(owning.contains("already owns"), "{owning}");
        let foreign = m
            .request(&transfer(child.pid(), "a", child.pid()), Some(&s), 0)
            .await
            .unwrap_err();
        assert!(foreign.contains("different process identity"), "{foreign}");
        let moved = m
            .request(&transfer(owner, "a", child.pid()), Some(&s), 0)
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
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        m.request(&acquire(owner, "a", 300), Some(&s), 0).await.unwrap();
        ticks(&mut m, &s, 1).await;
        m.request(&transfer(owner, "a", child.pid()), Some(&s), 0)
            .await
            .unwrap();
        // The connection closed before the answer; a successor restores the saved transfer.
        drop(m);
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        let resent = m
            .request(&transfer(owner, "a", child.pid()), Some(&s), 0)
            .await
            .unwrap();
        assert_eq!(resent["owner_pid"], child.pid(), "the resent transfer succeeds");
        for op in ["host_memory_status", "host_memory_release", "host_memory_track"] {
            let refused = m
                .request(&operation(owner, "a", op), Some(&s), 0)
                .await
                .unwrap_err();
            assert_eq!(refused, format!("host memory claim was transferred to {}", child.pid()), "{op}");
        }
        let again = m
            .request(&acquire(owner, "a", 300), Some(&s), 0)
            .await
            .unwrap_err();
        assert!(again.contains("transferred to"), "{again}");
        let released = m
            .request(&operation(child.pid(), "a", "host_memory_release"), Some(&s), 0)
            .await
            .unwrap();
        assert_eq!(released["released"], true);
        assert!(m.empty());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_claim_shows_its_declared_purpose_and_admission_never_reads_it() {
        let owners = [Owner::new(), Owner::new(), Owner::new(), Owner::new()];
        let dir = directory();
        let s = sample(&owners.iter().map(Owner::pid).collect::<Vec<_>>());
        let declared = |owner: &Owner, id: &str, bytes: u64, purpose: Value| {
            let mut request = acquire(owner.pid(), id, bytes);
            request["purpose"] = purpose;
            request
        };
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        let placed = m
            .request(&declared(&owners[0], "placed", 100, json!("gpu_place")), Some(&s), 0)
            .await
            .unwrap();
        assert_eq!(placed["purpose"], "gpu_place");
        let host = m
            .request(&declared(&owners[1], "host", 100, json!("host_only")), Some(&s), 0)
            .await
            .unwrap();
        assert_eq!(host["purpose"], "host_only");
        let unknown = m
            .request(&acquire(owners[2].pid(), "unknown", 100), Some(&s), 0)
            .await
            .unwrap();
        assert!(unknown["purpose"].is_null(), "gen-16 clients declare none");
        let conflicting = m
            .request(&declared(&owners[0], "placed", 100, json!("host_only")), Some(&s), 0)
            .await
            .unwrap_err();
        assert!(conflicting.contains("conflicting"), "{conflicting}");
        assert!(m
            .request(&declared(&owners[3], "bad", 100, json!("gpu")), Some(&s), 0)
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
            let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
            for ((owner, purpose), (id, bytes)) in owners
                .iter()
                .zip(purposes)
                .zip([("w", 300), ("x", 500), ("y", 200), ("z", 400)])
            {
                m.request(&declared(owner, id, bytes, purpose), Some(&s), 0)
                    .await
                    .unwrap();
            }
            let mut granted = Vec::new();
            for reserved in [600, 300, 0] {
                tick(&mut m, &s, reserved, 0, &BTreeSet::new(), Shares::new(BTreeMap::new()))
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
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        let s = sample(&[owner.pid()]);
        let status = m
            .request(&operation(owner.pid(), "a", "host_memory_status"), Some(&s), 0)
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
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        m.request(&acquire(owner.pid(), "a", 600), Some(&s), 0)
            .await
            .unwrap();
        tick(&mut m, &s, 0, 0, &BTreeSet::new(), Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        owner.0.kill().unwrap();
        owner.0.wait().unwrap();
        tick(&mut m, &s, 0, 0, &BTreeSet::new(), Shares::new(BTreeMap::new()))
            .await
            .unwrap();
        assert!(m.empty());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
