//! Atomic host RAM admission, independent of compiler slots and CPU affinity.
//! Claims take turns by compiler party: the party holding the least RAM goes next.
//! Only the channel actor mutates claims. Durable PID birth identities survive
//! handover and cannot be kept alive by recycled PIDs or forked token holders.
use super::{
    fairness::Shares,
    party,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
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
            return Err("host memory claim belongs to a different process identity".into());
        }
        Ok(claim)
    }
    fn status(claim: &Claim) -> Value {
        json!({"claim_id":claim.id,"state":if claim.failure.is_some() {"failed"} else if claim.granted {"granted"} else {"pending"},
            "error":claim.failure,"memory_bytes":claim.bytes,"owner_pid":claim.owner.pid,"label":claim.label,
            "worker_pids":claim.workers.keys().collect::<Vec<_>>()})
    }
    pub fn health(&self, sample: Option<&Snapshot>) -> Value {
        json!({"protocol":1,"granted":self.state.claims.values().filter(|c|c.granted).count(),
            "pending":self.state.claims.values().filter(|c|!c.granted && c.failure.is_none()).count(),
            "claimed_bytes":self.state.claims.values().filter(|c|c.granted).fold(0u64,|n,c|n.saturating_add(c.bytes)),"parked_waits":self.watches.len(),
            "unallocated_bytes":sample.map(|s|self.reserved(s)),
            "claims":self.state.claims.iter().map(|(id, claim)| {
                let mut status = Self::status(claim);
                status["party"] = json!(self.parties.get(id));
                status
            }).collect::<Vec<_>>()})
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
    fn anonymous(claim: &Claim, sample: &Snapshot) -> u64 {
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
        // PSS counts fork-shared anonymous pages once, unlike summed RSS.
        // Missing/inaccessible evidence receives no allocation credit.
        let mut total = 0u64;
        for pid in roots {
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
            "host_memory_acquire" | "host_memory_wait" => {
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
                if let Some(claim) = self.state.claims.get(id) {
                    self.owned(id, pid)?;
                    if claim.bytes != bytes || claim.label != label {
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
                    order: self.state.sequence,
                    granted: false,
                    baseline_anonymous: 0,
                    failure: None,
                    retiring: false,
                    workers: BTreeMap::new(),
                };
                let result = Self::status(&claim);
                self.state.claims.insert(id.into(), claim);
                self.save().await?;
                Ok(result)
            }
            "host_memory_status" => Ok(Self::status(self.owned(id, pid)?)),
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
                self.state.claims.remove(id);
                self.save().await?;
                self.watches
                    .resolve(id, Err("host memory claim was released".into()));
                Ok(json!({"released":true}))
            }
            _ => Err("unknown host memory operation".into()),
        }
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
    pub async fn tick(
        &mut self,
        sample: &Snapshot,
        compiler_reserved: u64,
        headroom: u64,
        parties: &BTreeSet<String>,
    ) -> Result<(), String> {
        let mut changed = false;
        let mut ended = Vec::new();
        let mut decided = Vec::new();
        self.state.claims.retain(|id, claim| {
            let keep = (claim.owner.alive() && !claim.retiring)
                || (claim.granted && claim.workers.values().any(Identity::alive));
            if !keep {
                ended.push(id.clone());
            }
            changed |= !keep;
            keep
        });
        self.allocated = self
            .state
            .claims
            .iter()
            .map(|(id, c)| (id.clone(), Self::anonymous(c, sample)))
            .collect();
        self.parties = self
            .state
            .claims
            .iter()
            .map(|(id, c)| (id.clone(), party::attribute(sample, c.owner.pid, parties)))
            .collect();
        let current_available = super::resources::memory()?.0.min(sample.available);
        let mut available = current_available
            .saturating_sub(compiler_reserved)
            .saturating_sub(headroom)
            .saturating_sub(self.reserved(sample));
        let mut held = BTreeMap::new();
        for claim in self.state.claims.values().filter(|c| c.granted) {
            let bytes: &mut u64 = held.entry(self.parties[&claim.id].clone()).or_default();
            *bytes = bytes.saturating_add(claim.bytes);
        }
        let mut shares = Shares::new(held);
        for claim in self
            .state
            .claims
            .values()
            .filter(|c| !c.granted && c.failure.is_none())
        {
            shares.wait(&self.parties[&claim.id], (claim.order, claim.id.clone()));
        }
        while let Some((party, (_, id))) = shares.next() {
            let owner = self.state.claims[&id].owner.pid;
            let overlaps = self.state.claims.values().any(|c| {
                c.granted
                    && (Self::descends(owner, c.owner.pid, |p| {
                        sample.processes.get(&p).map(|(parent, _)| *parent)
                    }) || Self::descends(c.owner.pid, owner, |p| {
                        sample.processes.get(&p).map(|(parent, _)| *parent)
                    }))
            });
            let claim = self.state.claims.get_mut(&id).unwrap();
            if overlaps {
                claim.failure=Some("overlapping process-tree host memory claims are forbidden; declare one aggregate claim".into());
                changed = true;
                decided.push(id);
                continue;
            }
            if claim.bytes > sample.capacity.saturating_sub(headroom) && sample.capacity > 0 {
                claim.failure = Some(
                    "host memory requirement exceeds host/cgroup capacity after required headroom"
                        .into(),
                );
                changed = true;
                decided.push(id);
                continue;
            }
            if claim.bytes > available {
                // A large claim next in turn must not block smaller claims
                // that can fit in the currently available RAM.  Keep the
                // pending claim ordered for a later tick, while continuing the
                // scan so admission makes progress without overcommitting RAM.
                continue;
            }
            claim.granted = true;
            claim.baseline_anonymous = self.allocated.get(&id).copied().unwrap_or(0);
            available = available.saturating_sub(claim.bytes);
            shares.grant(&party, claim.bytes);
            changed = true;
            decided.push(id);
        }
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
        m.tick(&s, 400, 100, &BTreeSet::new()).await.unwrap();
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
        m.tick(&s, 0, 100, &BTreeSet::new()).await.unwrap();
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
        m.tick(&s, 500, 0, &BTreeSet::new()).await.unwrap();
        assert!(
            granted.try_recv().is_err(),
            "a claim that does not fit stays parked"
        );
        m.tick(&s, 0, 0, &BTreeSet::new()).await.unwrap();
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
        m.tick(&s, 400, 0, &BTreeSet::new()).await.unwrap();
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
        // Each claim owner leads its own session, spawned by its party's driver.
        for (owner, driver) in campaign
            .iter()
            .map(|o| (o.pid(), 100))
            .chain([(small.pid(), 200)])
        {
            s.processes.insert(owner, (driver, 0));
            s.sessions.insert(owner, (owner, 0));
        }
        s.sessions.extend([(100, (100, 1)), (200, (200, 2))]);
        let parties = BTreeSet::from(["session-100-1".to_owned(), "session-200-2".to_owned()]);
        let mut m = HostMemory::restore(&dir, "boot").await.unwrap();
        for (i, owner) in campaign.iter().enumerate() {
            m.request(&acquire(owner.pid(), &format!("c{i}"), 300), Some(&s), 0)
                .await
                .unwrap();
        }
        m.request(&acquire(small.pid(), "small", 300), Some(&s), 0)
            .await
            .unwrap();
        m.tick(&s, 400, 0, &parties).await.unwrap();
        assert!(m.state.claims["c0"].granted);
        assert!(
            m.state.claims["small"].granted,
            "the party holding no RAM goes before the campaign's second claim"
        );
        assert!(!m.state.claims["c1"].granted && !m.state.claims["c2"].granted);
        assert_eq!(m.health(Some(&s))["claims"][3]["party"], "session-200-2");
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
        m.tick(&s, 0, 0, &BTreeSet::new()).await.unwrap();
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
        m.tick(&s, 0, 100, &BTreeSet::new()).await.unwrap();
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
        m.tick(&s, 0, 0, &BTreeSet::new()).await.unwrap();
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
        m.tick(&s, 0, 0, &BTreeSet::new()).await.unwrap();
        assert!(!m.empty());
        worker.0.kill().unwrap();
        worker.0.wait().unwrap();
        m.tick(&s, 0, 0, &BTreeSet::new()).await.unwrap();
        assert!(m.empty());
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
        m.tick(&s, 0, 0, &BTreeSet::new()).await.unwrap();
        owner.0.kill().unwrap();
        owner.0.wait().unwrap();
        m.tick(&s, 0, 0, &BTreeSet::new()).await.unwrap();
        assert!(m.empty());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
