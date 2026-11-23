//! One actor owns publication, fair admissions, process custody and results.
use crate::clock::Clock;
use crate::process_identity::{identity, Instance};
use super::{
    attempts::{Account, AttemptId, Cause, Launch, Move},
    budget::{Budget, Charge, Lending, Purse, Terms},
    containment::{Containment, Placement},
    contract::{self, Samples},
    decision::{Ahead, Counted, Decision, Decisions, Entry, Journal, Launching, Persisting, Record, Subject},
    fairness::{self, LastTurn, Shares},
    host_memory::{Compiles, Opened, Turn},
    memory_growth::Growth,
    model::{Cell, Group, Job, EXPECTED_MEMORY},
    party::{self, Attribution, Parties},
    precedence::{self, At, Attempt, Candidate, Capacity, Catalogue, Class, Head, Lacks, RequestId, Standing, Units, Work},
    progress::Progress,
    resources::Snapshot,
    store::{self, Operation, Optional, Store},
    watch::{Reply, Watches},
    Config,
};
use crate::procs::{Peer, Unread};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::Write;
use std::process::Stdio;
use tokio::process::{Child, Command};

// Waiters are answered at completion; the retention only serves late status
// queries and resubmissions of the same identities.
const RETENTION: f64 = 3600.0;

/// What clients may rely on beyond protocol 1, which gen-16 clients require exactly. A capability is never
/// removed: running campaigns may use it at any time.
const CAPABILITIES: &[&str] = &["peaks", "purpose", "transfer", "preparation_wait", "result_sink", EXPECTED_MEMORY];
/// A submission that names no party and whose sender is gone.
const NO_SUBMITTER: &str = "compiler submission needs a party or an authenticated submitter";

struct Running {
    record: Value,
    child: Option<Child>,
    exit: Option<i32>,
    awaiting_exit: bool,
    samples: Samples,
    /// The attempt's facts this instance saw, never persisted: only `record` is ever written.
    account: Account,
}
impl Running {
    /// Whether the service's contract can end this compile: it declares its runtime, and its start is known in
    /// this boot. One that is not is held outside the service: its projection, the shares and the aged head's
    /// capacity rule do not count it.
    fn contracted(&self, boot: &str) -> bool {
        self.finish(boot).is_some()
    }
    /// Its declared finish on the contract clock, its recorded start in this boot plus its declared runtime; none
    /// when the contract cannot end it.
    fn finish(&self, boot: &str) -> Option<f64> {
        let start = self.record["started_monotonic"].as_f64().filter(|_| self.record["boot_id"] == boot)?;
        Some(contract::finish(start, self.record["job"]["expected_runtime_secs"].as_u64()?))
    }
    /// The part of its cap the overcommit budget lends it: its record's `lending_bytes`, 0 when it names none.
    fn lending(&self) -> u64 {
        self.record["lending_bytes"].as_u64().unwrap_or(0)
    }
    /// Its ordinary reservation: its cap, `average` when its record names none, less its lending.
    fn ordinary(&self, average: u64) -> u64 {
        self.record["memory_bytes"].as_u64().unwrap_or(average) - self.lending()
    }
}
/// Writes an attempt's holding into its running record: its cap as `memory_bytes`, and its lending as `lending_bytes`
/// when it holds any. The one writer of a record's split.
fn hold(record: &mut Value, held: Charge) {
    record["memory_bytes"] = json!(held.cap());
    match held.lending {
        0 => record.as_object_mut().into_iter().for_each(|fields| drop(fields.remove("lending_bytes"))),
        lending => record["lending_bytes"] = json!(lending),
    }
}
/// What a job waiting for RAM needs: its whole cell, or, for a cell with a rest, its expected figure with the rest
/// from the overcommit budget, or its whole cell.
fn needs(cell: Cell, lendable: u64) -> String {
    match cell.rest() {
        0 => format!("needs {} bytes,", cell.limit),
        rest => format!(
            "needs {} bytes and {rest} bytes of the overcommit budget ({lendable} lendable), or {} bytes;",
            cell.expected, cell.limit
        ),
    }
}
/// The launch log's account of what the overcommit budget lent the attempt, when it lent any.
fn lent(charge: Charge) -> String {
    match charge.lending {
        0 => String::new(),
        lending => format!(
            "; admitted at {} MiB, the other {} MiB lent by the overcommit budget",
            charge.ordinary >> 20,
            lending >> 20
        ),
    }
}
/// A restored running record's lending: an absent key is 0; a present value, null included, that is not an integer or
/// exceeds its cap refuses restore.
fn restored_lending(id: &str, record: &Value) -> Result<u64, String> {
    record.get("lending_bytes").map_or(Ok(0), |lending| {
        lending
            .as_u64()
            .filter(|lending| *lending <= record["memory_bytes"].as_u64().unwrap_or(0))
            .ok_or_else(|| format!("bad running record {id}: lending_bytes"))
    })
}
struct Admission<'a> {
    sample: &'a Snapshot,
    used: BTreeSet<usize>,
    memory: u64,
    /// The aged head, as the last tick left it.
    head: Option<&'a Head>,
    /// The contract clock's reading now, and the run-ahead deadline: while the head does not fit, a job runs
    /// ahead only if it is declared to finish by the deadline.
    now: f64,
    deadline: Option<f64>,
    /// What the overcommit budget may still lend.
    lendable: u64,
}
/// One tick's compile slots, counted once before its first launch from the sample and the running compiles'
/// cores. Every attempted launch, ordinary or large, takes one; a search that finds no job takes none.
struct Slots {
    used: BTreeSet<usize>,
    left: usize,
}
impl Slots {
    fn of(sample: &Snapshot, used: BTreeSet<usize>) -> Self {
        let left = Engine::free_slots(sample, &used);
        Self { used, left }
    }
    /// Takes a free eligible core for one launch and charges its slot; none once the tick's slots are taken.
    fn take(&mut self, sample: &Snapshot) -> Option<usize> {
        let cpu = sample.cores.iter().find(|c| !self.used.contains(c)).copied().filter(|_| self.left > 0)?;
        self.used.insert(cpu);
        self.left -= 1;
        Some(cpu)
    }
}
pub struct Engine {
    config: Config,
    groups: BTreeMap<String, Group>,
    order: VecDeque<String>,
    progress: Progress,
    parties: Parties,
    // A pending job can only become impossible or dependency-blocked when its
    // group arrives, a result lands in it, or the host capacity changes.
    unchecked_capacity: BTreeSet<String>,
    unchecked_dependencies: BTreeSet<String>,
    group_watches: Watches,
    // Completion time of each complete group, until it is retired.
    completed: BTreeMap<String, f64>,
    submitted: BTreeMap<String, f64>,
    served: BTreeMap<String, f64>,
    running: BTreeMap<String, Running>,
    results: BTreeMap<String, Value>,
    sample: Option<Snapshot>,
    /// This engine's process in its boot: what health names, what reaper custody records, and what stamps its
    /// journal's records.
    instance: Instance,
    sequence: u64,
    draining: bool,
    host_memory: super::host_memory::HostMemory,
    containment: Containment,
    /// B, the overcommit budget this instance lends: fixed where restore establishes containment.
    overcommit: u64,
    growth: Growth,
    /// Each group's smallest and largest cell, so a tick's walk skips a group none of whose jobs waits for
    /// memory or is serviceable.
    bounds: BTreeMap<String, (u64, u64)>,
    /// What the last tick's ordinary launches left, which a non-head job could still have launched from; none
    /// after a restart.
    left_after_launch: Option<u64>,
    journal: Journal,
    /// Why a complete group's results could not be written to its result sink.
    sink_errors: BTreeMap<String, String>,
    /// Reads the memory available now, beside the sample's figure.
    read_memory: ReadMemory,
    /// The contract and wall clocks, shared with the host memory.
    clock: Clock,
    /// Every durable write, removal and directory sync goes through it, shared with the host memory.
    store: Store,
    /// Which unit a process belongs to: a claim's registrant, a submission's sender, a migrated claim's processes.
    attribution: Attribution,
}
/// A group as the engine saves it: the submitted group, and the party and priority it was given, published in one
/// rename.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    group: Group,
    party: String,
    priority: i8,
}
/// A saved descriptor: a record as the engine saves it, or a group saved in the form of generation 19 and earlier,
/// whose party is in `parties/<id>.json` or nowhere.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum Saved {
    Attributed(Descriptor),
    Legacy(Group),
}
impl Saved {
    /// The saved group, after its party and priority went to `parties`: a record's own, which win over any file in
    /// `parties/` with the group's ID; a group saved in generation 19's form keeps what its legacy file gave, or none.
    fn attributed(self, parties: &mut Parties) -> Group {
        match self {
            Saved::Attributed(Descriptor { group, party, priority }) => {
                parties.assign(&group.group_id, &party, priority);
                group
            }
            Saved::Legacy(group) => group,
        }
    }
}
/// Reads the memory available now and the capacity; one engine's own reader.
type ReadMemory = std::sync::Arc<dyn Fn() -> Result<(u64, u64), String> + Send + Sync>;
/// Seconds a running compile has run; a run from an earlier boot has no known start.
fn elapsed(record: &Value, boot: &str, now: f64) -> f64 {
    match record["boot_id"] == boot {
        true => (now - record["started_monotonic"].as_f64().unwrap_or(now)).max(0.0),
        false => 0.0,
    }
}
fn alive(record: &Value, boot: &str) -> bool {
    record["boot_id"] == boot
        && record["pid"].as_u64().is_some_and(|pid| {
            identity(pid as u32).is_ok_and(|(birth, state)| {
                Some(birth) == record["start_ticks"].as_u64() && !matches!(state, 'Z' | 'X')
            })
        })
}
fn signal(pid: u32, birth: u64, signal: i32) {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) as i32 };
    if fd >= 0 {
        if identity(pid).is_ok_and(|(current, _)| current == birth) {
            unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    fd,
                    signal,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                );
            }
        }
        unsafe {
            libc::close(fd);
        }
    }
}
/// An engine restored from its state that may not serve yet: it has no `serve`, `tick` or `request`.
pub struct Restored {
    engine: Engine,
}
impl Restored {
    pub fn await_reapers(&mut self, ids: &BTreeSet<String>) {
        for (id, running) in &mut self.engine.running {
            running.awaiting_exit = running.exit.is_none()
                && (ids.contains(id) || alive(&running.record["exit_reaper"], &self.engine.instance.boot_id));
        }
    }
    /// Restore's last phase, after authority and before serving: the parties restore migrated are saved. The only
    /// way to the engine.
    pub async fn activated(mut self) -> Result<Engine, String> {
        self.engine.host_memory.activated().await?;
        Ok(self.engine)
    }
}
impl Engine {
    pub async fn restore(
        config: Config,
        clock: Clock,
        store: Store,
        instance: Instance,
        attribution: Attribution,
    ) -> Result<Restored, String> {
        let manifest = config.state.join("format.json");
        let format = json!({"format":"rust-compiler-queue", "version":2});
        match store::optional(&manifest).await {
            Optional::Present(value) => {
                if value != format {
                    return Err("unsupported compiler state format".into());
                }
            }
            Optional::Absent => {
                let mut entries = tokio::fs::read_dir(&config.state)
                    .await
                    .map_err(|e| e.to_string())?;
                while let Some(entry) = entries.next_entry().await.map_err(|e| e.to_string())? {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    if name.starts_with("format.") && name.ends_with(".tmp") {
                        store.remove_file(Operation::new("format staging", name), &entry.path()).await?;
                    } else {
                        return Err("compiler state directory must be empty on first start".into());
                    }
                }
                store.write(Operation::new("format", "manifest"), &manifest, &format).await?;
            }
            Optional::Failed(error) => return Err(error),
        }
        for dir in [
            "groups",
            "running",
            "results",
            "logs",
            "cancellations",
            "reaper-exits",
            "parties",
        ] {
            store.create_directory(Operation::new("directory", dir), &config.state.join(dir)).await?;
            store.discard_abandoned(Operation::new("abandoned", dir), &config.state.join(dir)).await?;
        }
        store.discard_abandoned(Operation::new("abandoned", "state"), &config.state).await?;
        let host_memory =
            super::host_memory::HostMemory::restore(&config.state, &instance.boot_id, clock.clone(), store.clone(), &attribution)
                .await?;
        let containment = Containment::establish();
        let overcommit = config.lent(containment.caps());
        let mut engine = Self {
            config,
            groups: BTreeMap::new(),
            order: VecDeque::new(),
            progress: Progress::default(),
            parties: Parties::default(),
            unchecked_capacity: BTreeSet::new(),
            unchecked_dependencies: BTreeSet::new(),
            group_watches: Watches::default(),
            completed: BTreeMap::new(),
            submitted: BTreeMap::new(),
            served: BTreeMap::new(),
            running: BTreeMap::new(),
            results: BTreeMap::new(),
            sample: None,
            instance: instance.clone(),
            sequence: 0,
            draining: false,
            host_memory,
            containment,
            overcommit,
            growth: Growth::default(),
            bounds: BTreeMap::new(),
            left_after_launch: None,
            journal: Journal::new(instance),
            sink_errors: BTreeMap::new(),
            read_memory: std::sync::Arc::new(super::resources::memory),
            clock,
            store,
            attribution,
        };
        let growth = engine.config.state.join("growth.json");
        engine.growth = match store::optional(&growth).await {
            Optional::Present(value) => serde_json::from_value(value).map_err(|e| e.to_string())?,
            Optional::Absent => Growth::default(),
            Optional::Failed(error) => return Err(error),
        };
        for path in store::descriptors(&engine.config.state.join("results")).await? {
            let value = store::read(&path).await?;
            let id = value["job_id"]
                .as_str()
                .ok_or("missing result identity")?
                .to_owned();
            engine.results.insert(id, value);
        }
        // A cancellation is durable for all targeted jobs before applying it.
        for path in store::descriptors(&engine.config.state.join("cancellations")).await? {
            let transaction = store::read(&path).await?;
            let mut missing = Vec::new();
            for result in transaction["pending"]
                .as_array()
                .ok_or("bad cancellation journal")?
            {
                let id = result["job_id"]
                    .as_str()
                    .ok_or("bad cancellation identity")?;
                if !engine.results.contains_key(id) {
                    missing.push(result.clone());
                }
            }
            engine.finish_all(missing).await?;
        }
        for path in store::descriptors(&engine.config.state.join("parties")).await? {
            let record = store::read(&path).await?;
            let (Some(group), Some(party)) =
                (record["group_id"].as_str(), record["party"].as_str())
            else {
                return Err("bad compiler party record".into());
            };
            engine.parties.assign(
                group,
                party,
                party::priority(&record).map_err(|e| format!("bad compiler party record: {e}"))?,
            );
        }
        for path in store::descriptors(&engine.config.state.join("groups")).await? {
            let saved: Saved = serde_json::from_value(store::read(&path).await?).map_err(|e| e.to_string())?;
            let group = saved.attributed(&mut engine.parties);
            group.validate()?;
            engine.distinct(&group)?;
            engine.remember(group, store::modified(&path)?);
        }
        // Descriptors are read in group-ID order; a party's groups are served in submission order, so the
        // order is the descriptors' submission times, ties kept in group-ID order.
        let submitted = &engine.submitted;
        engine
            .order
            .make_contiguous()
            .sort_by(|a, b| submitted[a].total_cmp(&submitted[b]));
        let orphans: Vec<String> = engine
            .results
            .keys()
            .filter(|job| engine.progress.owner(job).is_none())
            .cloned()
            .collect();
        engine.forget_results(&orphans).await?;
        for path in store::descriptors(&engine.config.state.join("running")).await? {
            let record = store::read(&path).await?;
            let id = record["job_id"]
                .as_str()
                .ok_or("bad running identity")?
                .to_owned();
            if engine.results.contains_key(&id) {
                engine.store.remove_file(Operation::new("finished running record", &id), &path).await?;
            } else {
                restored_lending(&id, &record)?;
                engine.progress.start(&id);
                engine.running.insert(
                    id,
                    Running {
                        awaiting_exit: alive(&record["exit_reaper"], &engine.instance.boot_id),
                        record,
                        child: None,
                        exit: None,
                        samples: Samples::default(),
                        account: Account::restored(),
                    },
                );
            }
        }
        // Replay running cancellation requests only for the retained identities.
        // Once its marks are durable as well, a replayed journal is redundant.
        for path in store::descriptors(&engine.config.state.join("cancellations")).await? {
            let transaction = store::read(&path).await?;
            if let Some(targets) = transaction["running"].as_array() {
                for target in targets {
                    if let Some((id, running)) = target["job_id"]
                        .as_str()
                        .and_then(|id| engine.running.get_mut(id).map(|running| (id, running)))
                    {
                        if running.record["started_at"] == target["started_at"]
                            && !running.record["termination_reason"].is_string()
                        {
                            running.record["termination_reason"] = transaction["reason"].clone();
                            running.record["cancelled"] = json!(true);
                            engine.store.write(
                                Operation::new("cancellation mark", id),
                                &engine
                                    .config
                                    .state
                                    .join("running")
                                    .join(format!("{id}.json")),
                                &running.record,
                            )
                            .await?;
                        }
                    }
                }
            }
            engine.store.remove_file(Operation::new("replayed cancellation", path.display()), &path).await?;
        }
        engine
            .containment
            .prune(|job| engine.running.contains_key(job));
        engine.recontain();
        eprintln!("compile-queue: {}", engine.lending().describe());
        // A predecessor that ended between a group's durable results and their publication left its sink
        // unwritten; publishing again rewrites identical results.
        let complete: Vec<String> = engine.completed.keys().cloned().collect();
        for group in complete {
            engine.publish(&group).await;
        }
        for record in engine
            .results
            .values()
            .chain(engine.running.values().map(|r| &r.record))
        {
            if let (Some(group), Some(start)) =
                (record["group_id"].as_str(), record["started_at"].as_f64())
            {
                let last = engine
                    .served
                    .entry(engine.parties.of(group).to_owned())
                    .or_insert(start);
                *last = last.max(start);
            }
        }
        Ok(Restored { engine })
    }
    /// Refuses a group one of whose job IDs the engine holds: a submission's check before its publication, and
    /// restore's before `remember`.
    fn distinct(&self, group: &Group) -> Result<(), String> {
        if group
            .jobs
            .iter()
            .any(|job| self.progress.owner(&job.job_id).is_some())
        {
            return Err("duplicate compiler job identity".into());
        }
        Ok(())
    }
    /// Takes a group into the engine's state: the group in hand and its submission time, which a submission takes
    /// from its publication and restore from the record's modification time. It reads nothing and cannot fail.
    fn remember(&mut self, group: Group, timestamp: f64) {
        self.progress
            .admit(&group, |job| self.results.contains_key(job));
        if self.progress.complete(&group.group_id) {
            let finished = group
                .jobs
                .iter()
                .filter_map(|job| self.results.get(&job.job_id)?["finished_at"].as_f64())
                .fold(timestamp, f64::max);
            self.completed.insert(group.group_id.clone(), finished);
        }
        self.unchecked_capacity.insert(group.group_id.clone());
        self.unchecked_dependencies.insert(group.group_id.clone());
        self.bounds.insert(group.group_id.clone(), self.cells(&group));
        self.submitted.insert(group.group_id.clone(), timestamp);
        self.order.push_back(group.group_id.clone());
        self.groups.insert(group.group_id.clone(), group);
    }
    async fn finish(&mut self, result: Value) -> Result<(), String> {
        self.finish_all(vec![result]).await
    }
    /// A batch of results becomes durable together before any takes effect.
    async fn finish_all(&mut self, results: Vec<Value>) -> Result<(), String> {
        let mut fresh = BTreeMap::new();
        for result in results {
            let id = result["job_id"]
                .as_str()
                .ok_or("missing result ID")?
                .to_owned();
            match self.results.get(&id).or_else(|| fresh.get(&id)) {
                Some(existing) if *existing != result => {
                    return Err("terminal compiler result is immutable".into())
                }
                Some(_) => {}
                None => {
                    fresh.insert(id, result);
                }
            }
        }
        let paths: Vec<_> = fresh
            .keys()
            .map(|id| self.config.state.join("results").join(format!("{id}.json")))
            .collect();
        let entries: Vec<_> = paths
            .iter()
            .map(|path| path.as_path())
            .zip(fresh.values())
            .collect();
        let ids: Vec<&str> = fresh.keys().map(String::as_str).collect();
        self.store.write_all(Operation::new("results", ids.join(",")), &entries).await?;
        let mut complete = Vec::new();
        for (id, result) in fresh {
            complete.extend(self.record(id, result));
        }
        // A waiter learns of completion once the results are in the group's sink too.
        for group in complete {
            self.publish(&group).await;
            if self.group_watches.waits_for(&group) {
                let status = self.status(&group);
                self.group_watches.resolve(&group, status);
            }
        }
        Ok(())
    }
    /// Takes a durable result into effect; returns the group it completed.
    fn record(&mut self, id: String, result: Value) -> Option<String> {
        if let Some(group) = result["group_id"]
            .as_str()
            .filter(|_| result["outcome"] != "success")
        {
            self.unchecked_dependencies.insert(group.to_owned());
        }
        self.results.insert(id.clone(), result);
        let group = self.progress.settle(&id)?;
        self.completed.insert(group.clone(), self.clock.wall());
        Some(group)
    }
    /// A complete group's terminal results, as its status answers them, go to its result sink: a directory
    /// its submitter owns, so they outlive the group's retention. A sink that cannot be written fails only
    /// this publication, and the group's status records why.
    async fn publish(&mut self, group: &str) {
        let Some(sink) = self.groups.get(group).and_then(|g| g.result_sink.clone()) else {
            return;
        };
        let written = match (sink.is_dir(), self.status(group)) {
            (true, Ok(results)) => {
                let path = sink.join(format!("{group}.results.json"));
                self.store.write(Operation::new("result sink", group), &path, &results).await.map_err(String::from)
            }
            (false, _) => Err(format!("result sink {} is not a directory", sink.display())),
            (_, Err(error)) => Err(error),
        };
        match written {
            Ok(()) => self.sink_errors.remove(group),
            Err(error) => self.sink_errors.insert(group.to_owned(), error),
        };
    }
    async fn retire(&mut self, now: f64) -> Result<(), String> {
        let due: Vec<String> = self
            .completed
            .iter()
            .filter(|(_, completed)| now - **completed >= RETENTION)
            .map(|(id, _)| id.clone())
            .collect();
        if due.is_empty() {
            return Ok(());
        }
        // Descriptors go first and durably: a crash leaves orphaned results for
        // restore to remove, never a group whose results vanished.
        let descriptors = self.config.state.join("groups");
        let parties = self.config.state.join("parties");
        self.store.remove_all(
            Operation::new("retire", due.join(",")),
            due.iter()
                .flat_map(|id| {
                    [
                        descriptors.join(format!("{id}.json")),
                        parties.join(format!("{id}.json")),
                    ]
                })
                .collect(),
        )
        .await?;
        self.store.sync_directory(Operation::new("retire sync", due.join(",")), &descriptors).await?;
        let mut jobs = Vec::new();
        for id in &due {
            self.completed.remove(id);
            self.sink_errors.remove(id);
            self.submitted.remove(id);
            self.bounds.remove(id);
            self.parties.forget(id);
            self.unchecked_capacity.remove(id);
            self.unchecked_dependencies.remove(id);
            if let Some(group) = self.groups.remove(id) {
                self.progress.forget(&group);
                jobs.extend(group.jobs.into_iter().map(|job| job.job_id));
            }
        }
        self.order.retain(|id| self.groups.contains_key(id));
        let live = self.live_parties();
        self.served.retain(|party, _| live.contains(party));
        self.growth.forget(jobs.iter().map(String::as_str));
        self.store.write(
            Operation::new("growth", "retire"),
            &self.config.state.join("growth.json"),
            &serde_json::to_value(&self.growth).map_err(|e| e.to_string())?,
        )
        .await?;
        self.forget_results(&jobs).await
    }
    async fn forget_results(&mut self, jobs: &[String]) -> Result<(), String> {
        let mut paths = Vec::with_capacity(2 * jobs.len());
        for job in jobs {
            self.results.remove(job);
            paths.push(
                self.config
                    .state
                    .join("results")
                    .join(format!("{job}.json")),
            );
            paths.push(self.config.state.join("logs").join(format!("{job}.log")));
        }
        self.store.remove_all(Operation::new("forget results", jobs.join(",")), paths).await
    }
    fn result(&self, group: &str, job: &Job, outcome: &str, detail: &str) -> Value {
        let mut result = json!({"group_id":group,"job_id":job.job_id,"outcome":outcome,"detail":detail,"returncode":null,
            "queued_at":self.queued_at(group),"started_at":null,"finished_at":if outcome == "queued" {Value::Null}else{json!(self.clock.wall())},"execution_seconds":0.0,"log_path":""});
        job.expected_memory_bytes.into_iter().for_each(|expected| result["expected_memory_bytes"] = json!(expected));
        result
    }
    fn queued_at(&self, group: &str) -> Value {
        self.submitted
            .get(group)
            .map(|time| json!(time))
            .unwrap_or(Value::Null)
    }

    fn ready(&self, group: &Group, job: &Job) -> bool {
        !self.results.contains_key(&job.job_id)
            && !self.running.contains_key(&job.job_id)
            && self.has_capacity(group)
            && self.dependencies_met(job)
    }

    // A group keeps its declared limit; its party, however many groups it
    // splits into, never takes more than the two-thirds share.
    fn has_capacity(&self, group: &Group) -> bool {
        let party = self.parties.of(&group.group_id);
        let (in_group, in_party) = self
            .running
            .values()
            .filter_map(|r| r.record["group_id"].as_str())
            .filter(|id| self.parties.of(id) == party)
            .fold((0, 0), |(in_group, in_party), id| {
                (in_group + usize::from(id == group.group_id), in_party + 1)
            });
        in_group < self.group_limit(group) && in_party < self.core_cap()
    }
    fn live_parties(&self) -> BTreeSet<String> {
        self.groups
            .keys()
            .map(|group| self.parties.of(group).to_owned())
            .collect()
    }

    fn dependencies_met(&self, job: &Job) -> bool {
        job.dependencies.iter().all(|id| {
            self.results
                .get(id)
                .is_some_and(|r| r["outcome"] == "success")
        })
    }

    // Pending jobs are neither terminal nor running, so only the group capacity
    // and the job's prerequisites remain to decide readiness.
    fn first_ready<'a>(&'a self, group: &'a Group, fits: impl Fn(&Job) -> bool) -> Option<&'a Job> {
        if !self.has_capacity(group) {
            return None;
        }
        self.progress
            .pending(&group.group_id)
            .map(|index| &group.jobs[index])
            .find(|job| self.dependencies_met(job) && fits(job))
    }

    fn core_cap(&self) -> usize {
        let eligible = self
            .sample
            .as_ref()
            .map(|s| s.cores.len().saturating_sub(1))
            .unwrap_or(usize::MAX / 3);
        fairness::core_limit(eligible)
    }

    fn group_limit(&self, group: &Group) -> usize {
        if group.max_running == 0 {
            self.core_cap()
        } else {
            self.core_cap().min(group.max_running)
        }
    }

    /// What each party holds of its running compilers with a runtime contract, weighed by its priority.
    fn held(&self, amount: impl Fn(&Running) -> u64) -> BTreeMap<String, u64> {
        let mut held = BTreeMap::new();
        for running in self.running.values().filter(|running| running.contracted(&self.instance.boot_id)) {
            let group = running.record["group_id"].as_str().unwrap_or_default();
            let charged: &mut u64 = held.entry(self.parties.of(group).to_owned()).or_default();
            *charged = charged
                .saturating_add(fairness::charge(amount(running), self.parties.priority(group)));
        }
        held
    }

    /// When a party last took a slot; a party the service does not remember waits from the publication of its
    /// oldest unfinished group, `oldest`.
    fn last_turn(&self, party: &str, oldest: &BTreeMap<String, f64>) -> LastTurn {
        LastTurn(self.served.get(party).or_else(|| oldest.get(party)).copied().unwrap_or(0.0))
    }

    /// Each party's oldest unfinished group's publication.
    fn oldest_unfinished(&self) -> BTreeMap<String, f64> {
        let mut since: BTreeMap<String, f64> = BTreeMap::new();
        for group in self.progress.active() {
            let submitted = self.submitted[group];
            since
                .entry(self.parties.of(group).to_owned())
                .and_modify(|oldest| *oldest = oldest.min(submitted))
                .or_insert(submitted);
        }
        since
    }

    fn party_running(&self, party: &str) -> usize {
        self.running
            .values()
            .filter_map(|r| r.record["group_id"].as_str())
            .filter(|group| self.parties.of(group) == party)
            .count()
    }

    /// The running compiles the service's own projection counts: those its contract can end, by their cells.
    fn own_work(&self) -> Vec<Counted> {
        self.running
            .iter()
            .filter_map(|(id, running)| {
                let finish = running.finish(&self.instance.boot_id)?;
                Some(Counted::Compile {
                    attempt: Attempt {
                        job: id.clone(),
                        started_at: At(running.record["started_at"].as_f64().unwrap_or(0.0)),
                    },
                    bytes: running.ordinary(self.config.average_ram),
                    finish,
                })
            })
            .collect()
    }

    /// A job is eligible from its group's publication or its last prerequisite's result, whichever is later.
    fn eligible_at(&self, group: &str, job: &Job) -> f64 {
        job.dependencies
            .iter()
            .filter_map(|id| self.results.get(id)?["finished_at"].as_f64())
            .fold(self.submitted[group], f64::max)
    }

    /// What a job of `group` lacks besides memory, as the aged head's capacity rule reads it.
    fn capacity(&self, group: &Group) -> Capacity {
        let party = self.parties.of(&group.group_id);
        let contracted = |filter: &dyn Fn(&str) -> bool| {
            self.running
                .values()
                .filter(|running| filter(running.record["group_id"].as_str().unwrap_or_default()))
                .fold(Units::default(), |units, running| Units {
                    running: units.running + 1,
                    contracted: units.contracted + usize::from(running.contracted(&self.instance.boot_id)),
                })
        };
        let sample = self.sample.as_ref();
        Capacity {
            eligible: sample.map_or(0, |sample| sample.cores.len().saturating_sub(1)),
            held: sample.map_or(0, |sample| self.compiler_reserved(sample).0.len()),
            idle: sample.map_or(0, |sample| sample.idle),
            running: contracted(&|_| true),
            group: contracted(&|id| id == group.group_id),
            group_limit: self.group_limit(group),
            party: contracted(&|id| self.parties.of(id) == party),
            core_cap: self.core_cap(),
        }
    }

    /// Per group, the first pending ordinary job that waits for memory, is serviceable, aged and not giving way.
    /// It waits for memory when its cell exceeds the smaller of this tick's `left` and what the last tick's launches
    /// left; one that fits both waits for a slot, which the slot shares serve. No compile is a candidate while the
    /// engine drains, nor before a tick has launched since a restart.
    fn compile_candidates(&self, opened: Opened, now: f64) -> Vec<Candidate> {
        let Some(after) = self.left_after_launch.filter(|_| !self.draining) else {
            return Vec::new();
        };
        let waiting = opened.left.min(Budget::of(after));
        let mut candidates = Vec::new();
        for (position, id) in self.order.iter().enumerate() {
            let (smallest, largest) = self.bounds[id];
            if waiting.fits(largest) || smallest > opened.serviceable {
                continue;
            }
            let group = &self.groups[id];
            let found = self.progress.pending(id).map(|index| (index, &group.jobs[index])).find(|(_, job)| {
                let cell = self.cell(job).limit;
                Class::of(cell, None) == Class::Ordinary
                    && !waiting.fits(cell)
                    && cell <= opened.serviceable
                    && self.dependencies_met(job)
                    && precedence::aged(self.eligible_at(id, job), now)
                    && !self.host_memory.gives_way(&RequestId::Compile {
                        group: id.clone(),
                        job: job.job_id.clone(),
                    })
            });
            if let Some((index, job)) = found {
                candidates.push(Candidate {
                    id: RequestId::Compile {
                        group: id.clone(),
                        job: job.job_id.clone(),
                    },
                    party: self.parties.of(id).to_owned(),
                    eligible_at: At(self.eligible_at(id, job)),
                    catalogue: Catalogue::Compile { position, index },
                    bytes: self.cell(job).limit,
                    lacks: Lacks::Capacity(self.capacity(group)),
                });
            }
        }
        candidates
    }

    /// How a compile head of an earlier tick stands: still pending or not, its cell and its capacity.
    fn compile_standing(&self) -> Option<Standing> {
        let (group, job) = match self.host_memory.head().map(|head| &head.id) {
            Some(RequestId::Compile { group, job }) => (group, job),
            Some(RequestId::Claim(_)) | None => return None,
        };
        let group = self.groups.get(group)?;
        let job = group.jobs.iter().find(|j| &j.job_id == job)?;
        Some(Standing::of(
            !self.results.contains_key(&job.job_id) && !self.running.contains_key(&job.job_id),
            self.cell(job).limit,
            Lacks::Capacity(self.capacity(group)),
        ))
    }

    /// The smallest and largest cell among a group's jobs.
    fn cells(&self, group: &Group) -> (u64, u64) {
        group
            .jobs
            .iter()
            .map(|job| self.cell(job).limit)
            .fold((u64::MAX, 0), |(smallest, largest), cell| (smallest.min(cell), largest.max(cell)))
    }

    /// Every pending compile job's decision. The head and the jobs giving way are listed by themselves; for the
    /// rest of a group, one decision where its cell bounds leave its jobs one, otherwise how many have each.
    fn decisions(&self, serviceable: u64) -> BTreeMap<Subject, Decisions> {
        let head = self.host_memory.head();
        let decide = |cell: u64| Decision::waiting(Class::of(cell, None), cell, serviceable, false);
        let mut decisions = BTreeMap::new();
        for id in &self.order {
            let group = &self.groups[id];
            let lone: BTreeMap<&str, Decision> = head
                .and_then(|head| match &head.id {
                    RequestId::Compile { group, job } if group == id => Some((job.as_str(), Decision::head(head.fits))),
                    _ => None,
                })
                .into_iter()
                .chain(self.host_memory.giving_way().filter_map(|request| match request {
                    RequestId::Compile { group, job } if group == id => Some((job.as_str(), Decision::GivingWay)),
                    _ => None,
                }))
                .collect();
            let pending = |job: &str| !self.results.contains_key(job) && !self.running.contains_key(job);
            for (job, decision) in lone.iter().filter(|(job, _)| pending(job)) {
                let subject = Subject::Job {
                    group: id.clone(),
                    job: (*job).to_owned(),
                };
                decisions.insert(subject, Decisions::One(*decision));
            }
            let (smallest, largest) = self.bounds[id];
            let rest = match (decide(smallest) == decide(largest), self.progress.pending(id).next()) {
                (_, None) => None,
                (true, Some(_)) => Some(Decisions::One(decide(largest))),
                (false, Some(_)) => Some(Decisions::Jobs(
                    self.progress
                        .pending(id)
                        .map(|index| &group.jobs[index])
                        .filter(|job| !lone.contains_key(job.job_id.as_str()))
                        .fold(BTreeMap::new(), |mut counts, job| {
                            *counts.entry(decide(self.cell(job).limit)).or_insert(0) += 1;
                            counts
                        }),
                )),
            };
            if let Some(rest) = rest {
                decisions.insert(Subject::Group { group: id.clone() }, rest);
            }
        }
        decisions
    }

    /// The run-ahead deadline: the latest declared finish among the contracted running compiles, on the contract
    /// clock. A job declared to finish by it ends before the head's bytes could be freed anyway. None while no
    /// contracted compile runs: then nothing runs ahead.
    fn deadline(&self) -> Option<f64> {
        self.running.values().filter_map(|running| running.finish(&self.instance.boot_id)).reduce(f64::max)
    }

    /// Each running compile's usage is sampled; one past twice its declared
    /// runtime is ended.
    async fn observe(&mut self) -> Result<(), String> {
        let now = self.clock.monotonic_secs();
        let mut overran = Vec::new();
        for (id, running) in &mut self.running {
            if let Some((memory, cpu)) = self.containment.sample(id) {
                running.samples.observe(memory, cpu, now);
            }
            let declared = running.record["job"]["expected_runtime_secs"].as_u64();
            if contract::overran(declared, elapsed(&running.record, &self.instance.boot_id, now))
                && !running.record["termination_reason"].is_string()
            {
                running.record["termination_reason"] = json!(format!(
                    "compiler ran past twice its declared runtime of {} s",
                    declared.unwrap_or(0)
                ));
                running.record["overran"] = json!(true);
                overran.push(id.clone());
            }
        }
        for id in overran {
            self.store.write(
                Operation::new("overrun", &id),
                &self.config.state.join("running").join(format!("{id}.json")),
                &self.running[&id].record,
            )
            .await?;
        }
        Ok(())
    }

    /// The cores running compiles hold, and what the budget admits now from the sample's available memory.
    fn admissible(&self, sample: &Snapshot) -> (BTreeSet<usize>, u64) {
        let (used, compiler_reserved) = self.compiler_reserved(sample);
        let terms = Terms {
            available: sample.available,
            compiler_reserved,
            minimum_ram: self.config.minimum_ram,
            claims_unused: self.host_memory.reserved(sample),
            lending: self.lending(),
        };
        (used, terms.budget().floor())
    }

    // The reservation scans every remembered job; evaluate it once per request,
    // never once per queued job.
    fn admission(&self) -> Option<Admission<'_>> {
        let sample = self.sample.as_ref()?;
        let (used, memory) = self.admissible(sample);
        Some(Admission {
            sample,
            used,
            memory,
            head: self.host_memory.head(),
            now: self.clock.monotonic_secs(),
            deadline: self.deadline(),
            lendable: self.lending().lendable(),
        })
    }

    fn waiting_reason(&self, admission: Option<&Admission>, group: &Group, job: &Job) -> String {
        if !self.dependencies_met(job) {
            return "waiting for compile prerequisites".into();
        }
        if !self.ready(group, job) {
            return "waiting for subqueue concurrency capacity: at most two thirds of eligible compiler cores".into();
        }
        let Some(admission) = admission else {
            return "waiting for resource sample".into();
        };
        let holds = admission.head.is_some_and(|head| head.id.is_job(&job.job_id));
        if self.cell(job).limit > admission.memory && holds {
            return format!(
                "waiting for RAM, holding the aged compiler memory reservation: needs {} bytes, {} bytes admissible after headroom/reservations",
                self.cell(job).limit,
                admission.memory
            );
        }
        let large = self.class(job).waiting().filter(|_| self.cell(job).limit > admission.memory);
        if let Some(reason) = large {
            return reason.into();
        }
        if let Some(head) = admission.head.filter(|head| !head.fits) {
            let runs_ahead = self.cell(job).limit <= admission.memory
                && admission
                    .deadline
                    .is_some_and(|deadline| contract::finishes_by(admission.now, job.expected_runtime_secs, deadline));
            if !holds && !runs_ahead {
                let left = admission.deadline.map_or(0.0, |deadline| (deadline - admission.now).max(0.0));
                return format!(
                    "waiting for aged compiler memory reservation: {}; a job runs ahead only if its memory fits and it declares at most the {:.0} s the longest running compile has left",
                    head.id, left
                );
            }
        }
        if self.cell(job).limit > admission.memory {
            return format!(
                "waiting for RAM: {} {} bytes admissible after headroom/reservations",
                needs(self.cell(job), admission.lendable),
                admission.memory
            );
        }
        if admission.used.len() >= admission.sample.cores.len().saturating_sub(1)
            || admission.sample.idle == 0
        {
            return "waiting for CPU capacity; one core stays free of compilations".into();
        }
        "ready for fair admission".into()
    }

    pub fn handoff_config(&self) -> Config {
        Config {
            state: self.config.state.clone(),
            minimum_ram: self.config.minimum_ram,
            average_ram: self.config.average_ram,
            overcommit_percent: self.config.overcommit_percent,
            limits: self.config.limits,
        }
    }
    /// Health's `lending`: B, the lending held, what the budget adds back and what is charged as ordinary.
    fn lending_health(&self) -> Value {
        let lending = self.lending();
        json!({"overcommit_bytes":lending.lent,"lending_bytes":lending.held,"added_back_bytes":lending.added_back(),
            "charged_as_ordinary_bytes":lending.excess()})
    }
    /// B, and the lending the running attempts hold: the one computation restore, every tick and health read.
    fn lending(&self) -> Lending {
        Lending {
            lent: self.overcommit,
            held: self.running.values().fold(0u64, |held, running| held.saturating_add(running.lending())),
        }
    }
    pub async fn publish_reaper_custody(&mut self) -> Result<(), String> {
        let reaper = serde_json::to_value(&self.instance).map_err(|e| e.to_string())?;
        for (id, running) in &mut self.running {
            if running.child.is_some() {
                running.record["exit_reaper"] = reaper.clone();
                self.store.write(
                    Operation::new("reaper custody", id),
                    &self.config.state.join("running").join(format!("{id}.json")),
                    &running.record,
                )
                .await?;
            }
        }
        Ok(())
    }
    pub fn reaping_ids(&self) -> BTreeSet<String> {
        self.running
            .iter()
            .filter(|(_, r)| r.exit.is_none() && (r.child.is_some() || r.awaiting_exit))
            .map(|(id, _)| id.clone())
            .collect()
    }
    pub fn inherited_reaping_ids(&self) -> BTreeSet<String> {
        self.running
            .iter()
            .filter(|(_, r)| r.awaiting_exit && r.child.is_none())
            .map(|(id, _)| id.clone())
            .collect()
    }
    pub fn take_children(&mut self) -> Vec<(String, Child)> {
        self.running
            .iter_mut()
            .filter_map(|(id, r)| r.child.take().map(|c| (id.clone(), c)))
            .collect()
    }
    pub async fn external_exit(&mut self, id: &str, code: i32) -> Result<(), String> {
        if let Some(running) = self.running.get_mut(id) {
            if running.awaiting_exit {
                running.exit = Some(code);
                running.awaiting_exit = false;
                running.record["returncode"] = json!(code);
                self.store.write(
                    Operation::new("external exit", id),
                    &self.config.state.join("running").join(format!("{id}.json")),
                    &running.record,
                )
                .await?;
            }
        }
        Ok(())
    }
    pub fn reaper_gone(&mut self) {
        for running in self.running.values_mut() {
            running.awaiting_exit = false;
        }
    }
    pub fn drain(&mut self) {
        self.draining = true;
    }
    pub fn drained(&self) -> bool {
        self.draining && self.running.is_empty() && self.host_memory.empty()
    }
    /// Every request enters here. A wait is answered once its state is reached;
    /// every other operation is answered immediately. Once the store has stopped, every request is refused with its
    /// message.
    pub async fn serve(&mut self, request: Value, peer: Peer, reply: Reply) {
        if let Err(stopped) = self.store.serving() {
            let _ = reply.send(Err(stopped));
            return;
        }
        if request["protocol"] != 1 {
            let _ = reply.send(Err("unsupported compiler protocol".into()));
            return;
        }
        match request["op"].as_str() {
            Some("group_wait") => self.wait_group(&request, reply),
            Some("host_memory_acquire" | "host_memory_wait") => {
                self.host_memory
                    .wait(
                        &request,
                        &peer,
                        self.sample.as_ref(),
                        self.config.minimum_ram,
                        &self.attribution,
                        reply,
                    )
                    .await
            }
            _ => {
                let _ = reply.send(self.request(request, &peer).await);
            }
        }
    }
    fn wait_group(&mut self, request: &Value, reply: Reply) {
        match request["group_id"].as_str() {
            Some(id) if self.groups.contains_key(id) && !self.progress.complete(id) => {
                self.group_watches.park(id, reply, None)
            }
            Some(id) => {
                let _ = reply.send(self.status(id));
            }
            None => {
                let _ = reply.send(Err("missing group ID".into()));
            }
        }
    }
    /// A submission may name its party; otherwise it is the unit of its sender, read through the sender's
    /// descriptor.
    fn party(&self, request: &Value, peer: &Peer) -> Result<String, String> {
        match request["party"].as_str() {
            Some(party) => {
                super::model::identifier(party)?;
                Ok(party.to_owned())
            }
            None => peer
                .process()
                .and_then(|sender| self.attribution.party(sender.pid, sender.birth))
                .map_err(|unread| match unread {
                    Unread::Gone => NO_SUBMITTER.to_owned(),
                    Unread::Failed(error) => format!("compiler submission's submitter cannot be read: {error}"),
                }),
        }
    }
    fn status(&self, id: &str) -> Result<Value, String> {
        let group = self.groups.get(id).ok_or("unknown compiler group")?;
        // Only queued jobs need an admission view to explain their wait.
        let admission = self
            .progress
            .pending(id)
            .next()
            .and_then(|_| self.admission());
        let jobs: Vec<_> = group.jobs.iter().map(|job| {
            if let Some(result) = self.results.get(&job.job_id) { return result.clone(); }
            if let Some(running) = self.running.get(&job.job_id) {
                let r = &running.record;
                return json!({"group_id":id,"job_id":job.job_id,"outcome":"running","returncode":null,"detail":r["termination_reason"],
                    "queued_at":r["queued_at"],"started_at":r["started_at"],"finished_at":null,"execution_seconds":(self.clock.monotonic_secs()-r["started_monotonic"].as_f64().unwrap_or(self.clock.monotonic_secs())).max(0.0),"log_path":r["log_path"],"pid":r["pid"]});
            }
            self.result(id,job,"queued",&self.waiting_reason(admission.as_ref(),group,job))
        }).collect();
        Ok(json!({"group_id":id,"complete":self.progress.complete(id),"jobs":jobs,
            "result_sink_error":self.sink_errors.get(id)}))
    }
    async fn request(&mut self, request: Value, peer: &Peer) -> Result<Value, String> {
        let operation = request["op"].as_str().ok_or("missing compiler operation")?;
        if operation.starts_with("host_memory_") {
            return self
                .host_memory
                .request(&request, peer, self.sample.as_ref(), self.config.minimum_ram, &self.attribution)
                .await;
        }
        match operation {
            "health" => {
                let subqueues: Vec<_> = self.progress.active().map(|id| &self.groups[id])
                    .map(|g| json!({"group_id":g.group_id,"party":self.parties.of(&g.group_id),"priority":self.parties.priority(&g.group_id),
                        "party_running":self.party_running(self.parties.of(&g.group_id)),
                        "party_last_turn":self.served.get(self.parties.of(&g.group_id)),
                        "running":self.running.values().filter(|r|r.record["group_id"] == g.group_id).count(),
                        "concurrency_limit":self.group_limit(g)})).collect();
                let resources = self.sample.as_ref().map(|sample| {
                    let (used, reserved) = self.reserved(sample);
                    json!({"cores":sample.cores,"idle_slots":sample.idle,"assigned_cores":used,
                        "available_memory_bytes":sample.available,"memory_capacity_bytes":sample.capacity,"reserved_memory_bytes":reserved,
                        "minimum_memory_bytes":self.config.minimum_ram})
                });
                Ok(
                    json!({"protocol":1,"pid":self.instance.pid,"start_ticks":self.instance.start_ticks,
                    "boot_id":self.instance.boot_id,"running":self.running.len(),"draining":self.draining,"memory_containment":self.containment.describe(),"resources":resources,"subqueues":subqueues,
                    "parked_group_waits":self.group_watches.len(),
                    "host_memory":self.host_memory.health(self.sample.as_ref()),
                    "heads":self.host_memory.heads(),
                    "admission":self.journal.last(),
                    "lending":self.lending_health(),
                    "capabilities":CAPABILITIES,
                    "features":["host-memory-admission","channel-owned-state","direct-compilation","compile-dependencies","compile-group-limit","typed-results","bounded-memory-backfill","admission-reasons","slot-share-fairness","two-thirds-subqueue-cap","separate-process-handoff","push-waits"]}),
                )
            }
            "submit_group" => {
                let group: Group =
                    serde_json::from_value(request["group"].clone()).map_err(|e| e.to_string())?;
                group.validate()?;
                if let Some(job) = group.jobs.iter().find(|job| job.expected_runtime_secs.is_none()) {
                    return Err(format!("compile job {} {}", job.job_id, contract::UNDECLARED_RUNTIME));
                }
                let warnings: Vec<String> = group
                    .jobs
                    .iter()
                    .flat_map(|job| {
                        contract::queuing_warnings(job.memory_bytes)
                            .into_iter()
                            .map(move |warning| format!("{}: {warning}", job.job_id))
                    })
                    .collect();
                if let Some(existing) = self.groups.get(&group.group_id) {
                    if existing != &group {
                        return Err("conflicting existing compiler group".into());
                    }
                    return Ok(json!({"existing":true,"group_id":group.group_id}));
                }
                self.distinct(&group)?;
                let party = self.party(&request, peer)?;
                let priority = party::priority(&request)?;
                let id = group.group_id.clone();
                let descriptor = Descriptor { group, party, priority };
                let record = serde_json::to_value(&descriptor).map_err(|e| e.to_string())?;
                let path = self.config.state.join("groups").join(format!("{id}.json"));
                let published = self.store.publish(Operation::new("group", &id), &path, &record).await?;
                let Descriptor { group, party, priority } = descriptor;
                self.remember(group, published.modified);
                self.parties.assign(&id, &party, priority);
                Ok(json!({"existing":false,"group_id":id,"party":party,"priority":priority,"warnings":warnings}))
            }
            "group_status" => self.status(request["group_id"].as_str().ok_or("missing group ID")?),
            "cancel" => self.cancel(&request).await,
            _ => Err("unknown compiler operation".into()),
        }
    }
    async fn cancel(&mut self, request: &Value) -> Result<Value, String> {
        let group_id = request["group_id"]
            .as_str()
            .ok_or("missing cancellation group")?;
        let reason = request["reason"]
            .as_str()
            .filter(|s| !s.trim().is_empty())
            .ok_or("cancellation requires a reason")?;
        let group = self
            .groups
            .get(group_id)
            .ok_or("unknown cancellation group")?;
        let mut pending = Vec::new();
        let mut running = Vec::new();
        let mut members = Vec::new();
        for job in &group.jobs {
            if !request["job_id"].is_null() && request["job_id"] != job.job_id {
                continue;
            }
            let status = if let Some(result) = self.results.get(&job.job_id) {
                result["outcome"].as_str().unwrap_or("terminal")
            } else if let Some(active) = self.running.get(&job.job_id) {
                if request["include_running"] == true {
                    running.push(
                        json!({"job_id":job.job_id,"started_at":active.record["started_at"]}),
                    );
                    "cancelling"
                } else {
                    "running"
                }
            } else {
                pending.push(self.result(group_id, job, "cancelled", reason));
                "cancelled"
            };
            members.push(json!({"job_id":job.job_id,"status":status}));
        }
        if members.is_empty() {
            return Err("unknown cancellation job".into());
        }
        if !pending.is_empty() || !running.is_empty() {
            self.sequence += 1;
            let journal = self.config.state.join("cancellations").join(format!(
                "{}-{}-{}.json",
                std::process::id(),
                self.clock.wall().to_bits(),
                self.sequence
            ));
            self.store.write(
                Operation::new("cancellation", self.sequence),
                &journal,
                &json!({"pending":pending,"running":running,"reason":reason}),
            )
            .await?;
            self.finish_all(pending).await?;
            for target in running {
                let id = target["job_id"].as_str().unwrap();
                let active = self.running.get_mut(id).unwrap();
                if !active.record["termination_reason"].is_string() {
                    active.record["termination_reason"] = json!(reason);
                    active.record["cancelled"] = json!(true);
                }
                self.store.write(
                    Operation::new("cancellation mark", id),
                    &self.config.state.join("running").join(format!("{id}.json")),
                    &active.record,
                )
                .await?;
            }
            // Every result and mark the journal bridged is durable now.
            self.store.remove_file(Operation::new("cancellation removal", self.sequence), &journal).await?;
        }
        Ok(json!({"group_id":group_id,"members":members}))
    }
    /// The cells' memory controller came back: every running cell regains its limit, and every running attempt's
    /// peak counts afresh from now.
    fn controller_restored(&mut self) {
        eprintln!(
            "compile-queue: memory cells lost their controller; restored {} running cell(s)",
            self.recontain()
        );
        self.running.values_mut().for_each(|running| running.account.reset_peak());
    }
    /// Running attempts' cells regain their limits; returns how many did.
    fn recontain(&self) -> usize {
        self.running
            .iter()
            .filter(|(id, running)| {
                running.record["memory_bytes"]
                    .as_u64()
                    .is_some_and(|limit| self.containment.restore(id, limit).is_ok())
            })
            .count()
    }
    /// The memory cell of the job's next attempt: its declared cell, as its growth raised it, at most its ceiling.
    fn cell(&self, job: &Job) -> Cell {
        self.growth
            .cell(&job.job_id, job.cell(self.config.average_ram))
            .capped(job.error_memory_bytes)
    }
    /// The job's class, by its next attempt's cell: OOM growth can make a job large.
    fn class(&self, job: &Job) -> Class {
        Class::of(self.cell(job).limit, None)
    }
    fn compiler_reserved(&self, sample: &Snapshot) -> (BTreeSet<usize>, u64) {
        let mut used = BTreeSet::new();
        let mut memory: u64 = 0;
        for running in self.running.values() {
            let r = &running.record;
            if let Some(cpu) = r["cpu"].as_u64() {
                used.insert(cpu as usize);
            }
            let roots = r["pid"]
                .as_u64()
                .map(|p| BTreeSet::from([p as u32]))
                .unwrap_or_default();
            memory = memory.saturating_add(
                r["memory_bytes"]
                    .as_u64()
                    .unwrap_or(self.config.average_ram)
                    .saturating_sub(sample.rss(&roots)),
            );
        }
        (used, memory)
    }
    fn free_slots(sample: &Snapshot, used: &BTreeSet<usize>) -> usize {
        sample
            .cores
            .len()
            .saturating_sub(1)
            .saturating_sub(used.len())
            .min(sample.idle)
    }
    /// Compiler demand takes RAM turns with host claims by party share: a party
    /// holds its running compilers' declared memory and, while a compiler slot
    /// is free, asks for its next ready job.
    /// The aged head's bytes are held once: a party's turn names its first ready ordinary job other than the head.
    fn compiler_turns(&self, sample: &Snapshot) -> Shares<Turn> {
        let held = self.ram_held();
        let mut next = BTreeMap::new();
        if !self.draining && Self::free_slots(sample, &self.compiler_reserved(sample).0) > 0 {
            let head = self.host_memory.head().map(|head| &head.id);
            let starts: Vec<(&Group, &Job)> = self
                .order
                .iter()
                .filter_map(|id| {
                    let group = &self.groups[id];
                    self.first_ready(group, |job| {
                        self.class(job) == Class::Ordinary && !head.is_some_and(|head| head.is_job(&job.job_id))
                    })
                    .map(|job| (group, job))
                })
                .collect();
            for (group, job) in starts {
                let id = group.group_id.as_str();
                next.entry(self.parties.of(id))
                    .or_insert(Turn::Compiler(self.cell(job).limit, self.parties.priority(id)));
            }
        }
        let mut shares = Shares::new(held);
        for (party, turn) in next {
            shares.wait(party, turn);
        }
        shares
    }
    /// Each party's RAM holding of running compilers: their ordinary reservations, what they hold of the budget.
    fn ram_held(&self) -> BTreeMap<String, u64> {
        self.held(|running| running.ordinary(self.config.average_ram))
    }
    fn reserved(&self, sample: &Snapshot) -> (BTreeSet<usize>, u64) {
        let (cores, compiler) = self.compiler_reserved(sample);
        (
            cores,
            compiler.saturating_add(self.host_memory.reserved(sample)),
        )
    }
    pub async fn tick(&mut self, sample: Snapshot) -> Result<(), String> {
        if self.containment.reassert() {
            self.controller_restored();
        }
        self.group_watches.prune();
        if self.sample.as_ref().map(|s| s.capacity) != Some(sample.capacity) {
            let active: Vec<String> = self.progress.active().map(String::from).collect();
            self.unchecked_capacity.extend(active);
        }
        self.sample = Some(sample.clone());
        self.observe().await?;
        self.reap().await?;
        if !self.draining {
            self.fail_unrunnable(&sample).await?;
        }
        let now = self.clock.wall();
        let headroom = self.config.minimum_ram;
        let mut record = self.journal.begin(now);
        let charged = Compiles { reserved: self.compiler_reserved(&sample).1, lending: self.lending() };
        let fresh = (self.read_memory)()?.0;
        let own = self.own_work();
        let opened = self
            .host_memory
            .open(&sample, fresh, charged, headroom, own, &mut record)
            .await?;
        let candidates = self.compile_candidates(opened, now);
        let standing = self.compile_standing();
        self.host_memory
            .choose(&sample, headroom, candidates, standing, self.draining, now, &mut record);
        let turns = self.compiler_turns(&sample);
        let budget = self.host_memory.serve(&sample, headroom, turns, &mut record).await?;
        self.retire(now).await?;
        record.persisted(Persisting::Serve, self.clock.monotonic_secs(), budget);
        let mut slots = Slots::of(&sample, self.compiler_reserved(&sample).0);
        record.slots = slots.left;
        let mut purse = Purse {
            memory: budget,
            reserved: record.aged_reserved,
            lendable: record.lendable(),
        };
        let oldest = self.oldest_unfinished();
        if !self.draining {
            self.launch_ordinary(&sample, &mut slots, &mut purse, &oldest, &mut record).await?;
        }
        self.left_after_launch = Some(purse.memory.floor());
        record.left_after_launch = purse.memory.floor();
        purse.memory = self
            .host_memory
            .close(&sample, headroom, self.ram_held(), Persisting::Close, purse.memory, &mut record)
            .await?;
        record.persisted(Persisting::Close, self.clock.monotonic_secs(), purse.memory);
        if !self.draining {
            self.launch_large(&sample, &mut slots, &mut purse, &oldest, &mut record).await?;
        }
        let left = self
            .host_memory
            .close(&sample, headroom, self.ram_held(), Persisting::LargeClose, purse.memory, &mut record)
            .await?;
        record.persisted(Persisting::LargeClose, self.clock.monotonic_secs(), left);
        self.order.retain(|id| !self.progress.complete(id));
        let mut decisions = self.decisions(opened.serviceable);
        decisions.extend(self.host_memory.decisions());
        self.journal.finish(record, decisions);
        Ok(())
    }
    /// Pending jobs with impossible declarations, or whose prerequisites failed, fail before work is chosen.
    async fn fail_unrunnable(&mut self, sample: &Snapshot) -> Result<(), String> {
        // Impossible declarations must not establish a permanent reservation. Restored groups are checked by the
        // first tick, so only the generation that holds authority writes these refusals.
        while let Some(id) = self.unchecked_capacity.pop_first() {
            let Some(group) = self.groups.get(&id) else {
                continue;
            };
            let impossible: Vec<_> = self.progress.pending(&id).map(|index| &group.jobs[index])
                .filter_map(|job| self.refusal(sample, job).map(|detail| self.result(&id, job, "fail", detail))).collect();
            self.finish_all(impossible).await?;
        }
        // Propagate failed dependencies to a fixed point before choosing work.
        // A failure re-marks its group, so the loop ends when none remains.
        while let Some(id) = self.unchecked_dependencies.pop_first() {
            let Some(group) = self.groups.get(&id) else {
                continue;
            };
            let blocked: Vec<_> = self
                .progress
                .pending(&id)
                .map(|index| &group.jobs[index])
                .filter(|job| {
                    job.dependencies.iter().any(|dependency| {
                        self.results
                            .get(dependency)
                            .is_some_and(|r| r["outcome"] != "success")
                    })
                })
                .map(|job| self.result(&id, job, "fail", "compile prerequisite failed"))
                .collect();
            self.finish_all(blocked).await?;
        }
        Ok(())
    }
    /// Why a pending job can never run: its cell exceeds the host's capacity, or it declares no runtime, which
    /// every compile the service starts declares.
    fn refusal(&self, sample: &Snapshot, job: &Job) -> Option<&'static str> {
        let beyond = sample.capacity > 0 && self.cell(job).limit > sample.capacity.saturating_sub(self.config.minimum_ram);
        match (beyond, job.expected_runtime_secs) {
            (true, _) => Some("compiler memory requirement exceeds host/cgroup capacity after required headroom"),
            (false, None) => Some(contract::UNDECLARED_RUNTIME),
            (false, Some(_)) => None,
        }
    }
    /// Ordinary jobs take the free slots by slot shares, the aged head among them. A job's budget is the memory
    /// left plus what is reserved for it; while the head does not fit, a job declared to finish by the run-ahead
    /// deadline may run ahead into its bytes. The deadline is computed once, as the pass begins: its own launches
    /// cannot raise it, since each that runs ahead finishes by it. Each admission reads the contract clock once,
    /// after the last await before it, and that reading is the job's recorded start.
    async fn launch_ordinary(
        &mut self,
        sample: &Snapshot,
        slots: &mut Slots,
        purse: &mut Purse,
        oldest: &BTreeMap<String, f64>,
        record: &mut Record,
    ) -> Result<(), String> {
        let head = self.host_memory.head().map(|head| (head.id.clone(), head.fits));
        let waits = head.as_ref().is_some_and(|(_, fits)| !fits);
        let deadline = self.deadline().filter(|_| waits);
        while slots.left > 0 {
            let admitted_at = self.clock.monotonic_secs();
            let reserved_for = |job: &Job| {
                head.as_ref().is_some_and(|(id, _)| id.is_job(&job.job_id))
                    || deadline.is_some_and(|deadline| {
                        contract::finishes_by(admitted_at, job.expected_runtime_secs, deadline)
                    })
            };
            let Some(selected) = self.by_slot_share(oldest, |job| {
                self.class(job) == Class::Ordinary && purse.admits(self.cell(job), reserved_for(job)).is_some()
            }) else {
                break;
            };
            let Some(charge) = purse.admits(self.cell(&selected.1), reserved_for(&selected.1)) else {
                break;
            };
            let own = head.as_ref().is_some_and(|(id, _)| id.is_job(&selected.1.job_id));
            let admission = (admitted_at, deadline);
            if !self.start(sample, slots, purse, (selected, charge), Launching::Launch, own, admission, record).await? {
                break;
            }
        }
        Ok(())
    }
    /// Large jobs take the slots ordinary jobs left, from what every ordinary request of the tick left.
    async fn launch_large(
        &mut self,
        sample: &Snapshot,
        slots: &mut Slots,
        purse: &mut Purse,
        oldest: &BTreeMap<String, f64>,
        record: &mut Record,
    ) -> Result<(), String> {
        while slots.left > 0 {
            let admitted_at = self.clock.monotonic_secs();
            let Some(selected) = self.by_slot_share(oldest, |job| {
                self.class(job) == Class::Large && purse.admits(self.cell(job), false).is_some()
            }) else {
                break;
            };
            let Some(charge) = purse.admits(self.cell(&selected.1), false) else {
                break;
            };
            let admission = (admitted_at, None);
            if !self.start(sample, slots, purse, (selected, charge), Launching::LargeLaunch, false, admission, record).await? {
                break;
            }
        }
        Ok(())
    }
    fn by_slot_share(&self, oldest: &BTreeMap<String, f64>, fits: impl Fn(&Job) -> bool) -> Option<(String, Job)> {
        // The slot goes to the party holding the fewest; equal holdings go
        // to the party whose last turn is oldest, and within the party to
        // its earliest-submitted group with an eligible job.
        let mut slots = Shares::new(self.held(|_| 1));
        let mut candidates = BTreeMap::new();
        for (position, id) in self.order.iter().enumerate() {
            if let Some(job) = self.first_ready(&self.groups[id], &fits) {
                let party = self.parties.of(id);
                slots.wait(party, (self.last_turn(party, oldest), position));
                candidates.insert(position, job);
            }
        }
        slots
            .next()
            .map(|(_, (_, position))| (self.order[position].clone(), candidates[&position].clone()))
    }
    /// Launches the selected job on a free eligible core, charging its slot and the charge its selection admitted
    /// once, also when its spawn fails: the head from its reservation first, every other job from memory first and,
    /// running ahead, the rest from the reservation; its lending from the overcommit budget. `admission` is the
    /// contract-clock reading its selection used, which becomes its start, and the run-ahead deadline it was
    /// selected against. Returns false when no slot is left.
    #[allow(clippy::too_many_arguments)]
    async fn start(
        &mut self,
        sample: &Snapshot,
        slots: &mut Slots,
        purse: &mut Purse,
        ((group, job), charge): ((String, Job), Charge),
        phase: Launching,
        own: bool,
        (admitted_at, deadline): (f64, Option<f64>),
        record: &mut Record,
    ) -> Result<bool, String> {
        let Some(cpu) = slots.take(sample) else {
            return Ok(false);
        };
        let cell = self.cell(&job);
        let (budget, reserved) = (purse.memory, purse.reserved);
        let paid = purse.charge(charge, own);
        let (id, declared) = (job.job_id.clone(), job.expected_runtime_secs);
        self.launch(group.clone(), job, cpu, admitted_at, cell, charge).await?;
        let settled_at = self.clock.monotonic_secs();
        let started = self.running.get(&id).map(|running| running.record["started_at"].as_f64().unwrap_or(0.0));
        let ahead = (!own && paid > 0).then(|| Ahead {
            deadline: deadline.unwrap_or(0.0),
            work: Work::compile(&id, started.unwrap_or(0.0)),
        });
        if let Some(ahead) = ahead.as_ref().filter(|_| started.is_some()) {
            self.host_memory.ran_ahead(ahead.work.clone());
        }
        record.admitted.push(Entry::Launched {
            phase,
            group,
            job: id,
            class: Class::of(cell.limit, None),
            bytes: cell.limit,
            expected: cell.expected,
            lent: charge.lending,
            budget,
            reserved,
            head: own,
            ahead,
            admitted_at,
            declared,
            settled_at,
            running: started.is_some(),
        });
        Ok(true)
    }
    /// Launches `job` on `cpu` in a cell of `cell`'s limit, holding `charge`; `started` is the contract-clock reading
    /// recorded as its start.
    async fn launch(&mut self, group: String, job: Job, cpu: usize, started: f64, cell: Cell, charge: Charge) -> Result<(), String> {
        self.served.insert(
            self.parties.of(&group).to_owned(),
            self.clock.wall(),
        );
        let id = job.job_id.clone();
        let log_path = self.config.state.join("logs").join(format!("{id}.log"));
        let path = self.config.state.join("running").join(format!("{id}.json"));
        let limit = cell.limit;
        let launched = Launch { cell_bytes: cell.limit, expected_bytes: cell.expected, lending_bytes: charge.lending };
        let mut record = json!({"group_id":group,"job_id":id,"boot_id":self.instance.boot_id,"pid":null,"start_ticks":null,
            "cpu":cpu,"job":job,"log_path":log_path,
            "queued_at":self.queued_at(&group),"started_at":self.clock.wall(),"started_monotonic":started});
        hold(&mut record, charge);
        // The intent is durable before spawn. An interrupted publication is an
        // explicit unknown result after restart; only an attempt that outgrew
        // its memory cell runs again.
        self.store.write(Operation::new("intent", &id), &path, &record).await?;
        let mut log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| e.to_string())?;
        writeln!(log, "compile-queue: attempt in a {} MiB memory cell{}", limit >> 20, lent(charge))
            .map_err(|e| e.to_string())?;
        let placement = self.containment.enter(&id, limit);
        let cell = placement.as_ref().ok().and_then(Placement::descriptor);
        let mut command = Command::new(&job.argv[0]);
        command.args(&job.argv[1..]).current_dir(&job.cwd);
        if !job.inherit_environment {
            command.env_clear();
        }
        command.envs(&job.env);
        for name in [
            "NOTIFY_SOCKET",
            "WATCHDOG_PID",
            "WATCHDOG_USEC",
            "LISTEN_FDS",
            "LISTEN_PID",
            "LISTEN_FDNAMES",
        ] {
            command.env_remove(name);
        }
        command
            .env("CUDA_VISIBLE_DEVICES", "")
            .env("OMP_NUM_THREADS", "1")
            .env("MKL_NUM_THREADS", "1")
            .env("OPENBLAS_NUM_THREADS", "1")
            .env("COMPILE_QUEUE_JOB", &id)
            .env("COMPILE_QUEUE_GROUP", &group);
        command
            .env("COMPILE_QUEUE_STATE", &self.config.state)
            .stdin(Stdio::null())
            .stdout(log.try_clone().map_err(|e| e.to_string())?)
            .stderr(log)
            .kill_on_drop(false)
            .process_group(0);
        // Only child setup runs here. The next executable is the compiler itself.
        unsafe {
            command.pre_exec(move || {
                Placement::join(cell)?;
                let mut set: libc::cpu_set_t = std::mem::zeroed();
                libc::CPU_SET(cpu, &mut set);
                if libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                // Compilations share the service cgroup with the actor and any
                // successor it spawns; they must never outrank either.
                if libc::setpriority(libc::PRIO_PROCESS, 0, 19) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                let mut empty: libc::sigset_t = std::mem::zeroed();
                libc::sigemptyset(&mut empty);
                let rc = libc::pthread_sigmask(libc::SIG_SETMASK, &empty, std::ptr::null_mut());
                if rc != 0 {
                    return Err(std::io::Error::from_raw_os_error(rc));
                }
                Ok(())
            });
        }
        let spawned = placement.and_then(|placement| {
            let child = command.spawn().map_err(|e| e.to_string());
            drop(placement);
            child
        });
        match spawned {
            Ok(mut child) => {
                let pid = child.id().ok_or("missing compiler PID")?;
                // A fast compiler may exit before /proc is read. Its Child still
                // retains the exact exit status, and the actor remains its owner.
                let birth = identity(pid).ok().map(|v| v.0);
                record["pid"] = json!(pid);
                record["start_ticks"] = json!(birth);
                if let Err(error) = self.store.write(Operation::new("pid", &id), &path, &record).await {
                    let _ = child.kill().await;
                    return Err(error.into());
                }
                self.progress.start(&id);
                self.running.insert(
                    id,
                    Running {
                        record,
                        child: Some(child),
                        exit: None,
                        awaiting_exit: false,
                        samples: Samples::default(),
                        account: Account::launched(launched),
                    },
                );
            }
            Err(error) => {
                let mut result = self.result(
                    &group,
                    &job,
                    "fail",
                    &format!("cannot launch compiler: {error}"),
                );
                result["returncode"] = json!(127);
                result["log_path"] = json!(log_path);
                self.finish(result).await?;
                self.store.remove_file(Operation::new("intent removal", &id), &path).await?;
            }
        }
        Ok(())
    }
    async fn reap(&mut self) -> Result<(), String> {
        let mut active = std::mem::take(&mut self.running);
        let sample = self
            .sample
            .as_ref()
            .ok_or("missing resource sample")?
            .clone();
        let mut finished = BTreeSet::new();
        let mut outgrew = Vec::new();
        for (id, running) in &mut active {
            // An unreadable receipt leaves this pass undecided.
            let mut undecided = false;
            if running.exit.is_none() {
                if let Some(child) = running.child.as_mut() {
                    running.exit = child.try_wait().map_err(|e| e.to_string())?.map(|s| {
                        use std::os::unix::process::ExitStatusExt;
                        s.code().unwrap_or_else(|| -s.signal().unwrap_or(0))
                    });
                    if let Some(code) = running.exit {
                        running.record["returncode"] = json!(code);
                        self.store.write(
                            Operation::new("exit", id),
                            &self.config.state.join("running").join(format!("{id}.json")),
                            &running.record,
                        )
                        .await?;
                    }
                } else {
                    running.exit = running.record["returncode"].as_i64().map(|v| v as i32);
                    let receipt = self
                        .config
                        .state
                        .join("reaper-exits")
                        .join(format!("{id}.json"));
                    if running.exit.is_none() {
                        match store::optional(&receipt).await {
                            Optional::Present(value) => {
                                if value["pid"] == running.record["pid"] {
                                    running.exit = value["returncode"].as_i64().map(|v| v as i32);
                                    running.awaiting_exit = false;
                                }
                            }
                            Optional::Absent => {}
                            Optional::Failed(_) => undecided = true,
                        }
                    }
                }
            }
            if !undecided
                && running.awaiting_exit
                && !running.record["exit_reaper"].is_null()
                && !alive(&running.record["exit_reaper"], &self.instance.boot_id)
            {
                running.awaiting_exit = false;
            }
            let root_alive = alive(&running.record, &self.instance.boot_id);
            let pid = running.record["pid"].as_u64().unwrap_or(0) as u32;
            let mut members: Vec<_> = if running.record["boot_id"] == self.instance.boot_id {
                sample.group_members(pid)
            } else {
                Vec::new()
            };
            if root_alive {
                if let Some(birth) = running.record["start_ticks"].as_u64() {
                    if !members.iter().any(|(p, _)| *p == pid) {
                        members.push((pid, birth));
                    }
                }
            }
            // A sampled process group can be stale immediately after waitpid.
            members.retain(|(p, b)| {
                identity(*p).is_ok_and(|(birth, state)| birth == *b && !matches!(state, 'Z' | 'X'))
            });
            if !root_alive
                && !members.is_empty()
                && !running.record["termination_reason"].is_string()
            {
                running.record["termination_reason"] =
                    json!("compiler exited while descendants remained");
            }
            if running.record["termination_reason"].is_string() && !members.is_empty() {
                let now = self.clock.monotonic_secs();
                if !running.record["termination_started"].is_number() {
                    running.record["termination_started"] = json!(now);
                }
                let elapsed = now - running.record["termination_started"].as_f64().unwrap();
                let stage = if elapsed < 1.0 {
                    0
                } else if elapsed < 2.0 {
                    1
                } else {
                    2
                };
                if stage == 2 || running.record["termination_stage"].as_i64() != Some(stage) {
                    running.record["termination_stage"] = json!(stage);
                    self.store.write(
                        Operation::new("termination", id),
                        &self.config.state.join("running").join(format!("{id}.json")),
                        &running.record,
                    )
                    .await?;
                    for (p, birth) in members {
                        signal(
                            p,
                            birth,
                            match stage {
                                0 => libc::SIGINT,
                                1 => libc::SIGTERM,
                                _ => libc::SIGKILL,
                            },
                        );
                    }
                }
                continue;
            }
            // An exit can occur after try_wait but before the /proc read.
            // A live Child handle still owns the exact status: collect it on
            // the next tick instead of inventing an unknown/lost outcome.
            if running.exit.is_none()
                && (root_alive || running.child.is_some() || running.awaiting_exit || undecided)
            {
                continue;
            }
            let usage = self.containment.release(id);
            let record = &running.record;
            let job: Job =
                serde_json::from_value(record["job"].clone()).map_err(|e| e.to_string())?;
            let reason = record["termination_reason"].as_str();
            let limit = record["memory_bytes"].as_u64().unwrap_or(0);
            let ceiling = job.error_memory_bytes.unwrap_or(u64::MAX);
            // A cell's group kill can outpace the descendants check; the kill is the cause.
            let outgrown = usage.out_of_memory && record["cancelled"] != true;
            if outgrown {
                let group = record["group_id"].as_str().ok_or("missing group")?;
                let donor = AttemptId {
                    job_id: record["job_id"].as_str().ok_or("missing job")?.to_owned(),
                    started_at: record["started_at"].as_f64().unwrap_or(0.0),
                };
                outgrew.push((group.to_owned(), Charge::held(limit, running.lending()), donor));
                let mut log = std::fs::OpenOptions::new()
                    .append(true)
                    .open(self.config.state.join("logs").join(format!("{id}.log")))
                    .map_err(|e| e.to_string())?;
                writeln!(log, "compile-queue: out of memory in its {} MiB cell", limit >> 20)
                    .map_err(|e| e.to_string())?;
            }
            if outgrown && limit < ceiling {
                self.progress.requeue(id);
                let path = self.config.state.join("running").join(format!("{id}.json"));
                self.store.remove_file(Operation::new("requeued running record", id), &path).await?;
                finished.insert(id.clone());
                continue;
            }
            let exceeded = format!("compiler exceeded its error memory of {ceiling} bytes");
            let (outcome, detail) = if record["cancelled"] == true {
                ("cancelled", reason.unwrap_or("owner cancelled compilation"))
            } else if outgrown {
                ("fail", exceeded.as_str())
            } else if let Some(reason) = reason {
                ("fail", reason)
            } else {
                match running.exit {
                    Some(0) => ("success", "compiler exited successfully"),
                    Some(_) => ("fail", "compiler exited with non-zero status"),
                    None => (
                        "lost",
                        "daemon lost the compiler exit status; exact signal/cause unknown",
                    ),
                }
            };
            let mut result = self.result(
                record["group_id"].as_str().ok_or("missing group")?,
                &job,
                outcome,
                detail,
            );
            result["returncode"] = json!(running.exit);
            result["started_at"] = record["started_at"].clone();
            result["queued_at"] = record["queued_at"].clone();
            result["log_path"] = record["log_path"].clone();
            result["peak_memory_bytes"] = json!(usage.peak);
            result["memory_limit_bytes"] = json!(limit);
            record["lending_bytes"].as_u64().into_iter().for_each(|lending| result["lending_bytes"] = json!(lending));
            result["attempt"] = serde_json::to_value(running.account.concluded(Cause::of(outcome, outgrown))).map_err(|e| e.to_string())?;
            let seconds = elapsed(record, &self.instance.boot_id, self.clock.monotonic_secs());
            result["execution_seconds"] = json!(seconds);
            let report = contract::report(contract::Run {
                samples: &running.samples,
                peak: usage.peak,
                declared_memory: job.memory_bytes,
                declared_seconds: job.expected_runtime_secs.filter(|_| record["boot_id"] == self.instance.boot_id),
                seconds,
                succeeded: outcome == "success",
                overran: record["overran"] == true,
            });
            for (key, value) in report.as_object().into_iter().flatten() {
                result[key] = value.clone();
            }
            self.finish(result).await?;
            let path = self.config.state.join("running").join(format!("{id}.json"));
            self.store.remove_file(Operation::new("reaped running record", id), &path).await?;
            finished.insert(id.clone());
        }
        active.retain(|id, _| !finished.contains(id));
        self.running = active;
        for (group, released, donor) in outgrew {
            self.regrow(&group, released, donor).await?;
        }
        Ok(())
    }
    /// An attempt outgrew its memory cell: its subqueue grows, passed its whole cap as today. Each running sibling's
    /// applied raise is funded from the dead attempt's `released` holding, its ordinary reservation first and then
    /// its lending, which moves to the sibling as lending; what is not moved has left with the dead attempt's record.
    /// Each applied raise is a move into the sibling's account, naming `donor`.
    async fn regrow(&mut self, group: &str, mut released: Charge, donor: AttemptId) -> Result<(), String> {
        let running: Vec<(String, u64)> = self
            .running
            .values()
            .filter(|running| running.record["group_id"] == group)
            .filter_map(|running| {
                Some((
                    running.record["job_id"].as_str()?.to_owned(),
                    running.record["memory_bytes"].as_u64()?,
                ))
            })
            .collect();
        let waiting: Vec<(String, u64)> = self
            .progress
            .pending(group)
            .map(|index| &self.groups[group].jobs[index])
            .map(|pending| (pending.job_id.clone(), self.cell(pending).limit))
            .collect();
        for (id, limit) in self.growth.out_of_memory(released.cap(), &running, &waiting) {
            let Some(sibling) = self.running.get_mut(&id) else {
                continue;
            };
            let limit = limit.min(
                sibling.record["job"]["error_memory_bytes"]
                    .as_u64()
                    .unwrap_or(u64::MAX),
            );
            // A sibling whose cell refuses the raise keeps its declared limit.
            if self.containment.limit(&id, limit).is_ok() {
                let raise = limit.saturating_sub(sibling.record["memory_bytes"].as_u64().unwrap_or(limit));
                let moved = released.fund(raise);
                let held = Charge::held(limit, sibling.lending() + moved);
                hold(&mut sibling.record, held);
                self.store.write(
                    Operation::new("regrow", &id),
                    &self.config.state.join("running").join(format!("{id}.json")),
                    &sibling.record,
                )
                .await?;
                sibling.account.moved(Move { at: self.clock.wall(), donor: donor.clone(), lending_bytes: moved, cap_bytes: limit });
            }
        }
        self.store.write(
            Operation::new("growth", group),
            &self.config.state.join("growth.json"),
            &serde_json::to_value(&self.growth).map_err(|e| e.to_string())?,
        )
        .await?;
        self.unchecked_capacity.insert(group.to_owned());
        self.bounds.insert(group.to_owned(), self.cells(&self.groups[group]));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::clock::Manual;
    use super::super::decision;
    use super::super::precedence::{Registration, Step, Verdict};
    use super::super::store::{Hold, Mode, Unreadable};
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;

    /// The unit this process, and every raw child a test makes a claim's owner, are in.
    const SHARED: &str = "/user.slice/test.slice/engine-tests.scope";
    /// Where the tests' processes are, by pid and birth; this process is in `SHARED`.
    fn units() -> &'static party::Units {
        static UNITS: std::sync::OnceLock<party::Units> = std::sync::OnceLock::new();
        UNITS.get_or_init(|| {
            let units = party::Units::default();
            let own = std::process::id();
            units.place(own, crate::procs::started(own).unwrap(), SHARED);
            units
        })
    }
    fn attribution() -> Attribution {
        Attribution::reading(units().reader())
    }
    /// `child` is in `SHARED`.
    fn shared(child: &std::process::Child) {
        units().place(child.id(), crate::procs::started(child.id()).unwrap(), SHARED);
    }
    /// A request and its sender's descriptor, as a connection posts them.
    type Asked = (Value, Peer);
    /// What a test sends: a request with its sender, or a request alone, which the test process sends.
    trait Asking {
        fn split(self) -> Asked;
    }
    impl Asking for Asked {
        fn split(self) -> Asked {
            self
        }
    }
    impl Asking for Value {
        fn split(self) -> Asked {
            (self, Peer::opened(std::process::id()))
        }
    }
    /// Who submits a test's group: a named party, which the test process sends, or a claimant, whose unit is its
    /// party.
    trait Submitter {
        fn submitting(&self, submission: Value) -> Asked;
    }
    impl<Name: AsRef<str>> Submitter for Name {
        fn submitting(&self, mut submission: Value) -> Asked {
            submission["party"] = json!(self.as_ref());
            submission.split()
        }
    }
    impl Submitter for &Claimant {
        fn submitting(&self, submission: Value) -> Asked {
            (submission, Peer::opened(self.0.id()))
        }
    }
    impl Engine {
        /// `request` of what a test sends.
        async fn requested(&mut self, asking: impl Asking) -> Result<Value, String> {
            let (request, peer) = asking.split();
            self.request(request, &peer).await
        }
        /// Launches `job` in its cell, holding `lending` of it as lending: 0 is a launch at its whole cell.
        async fn launch_holding(&mut self, group: String, job: Job, cpu: usize, started: f64, lending: u64) -> Result<(), String> {
            let cell = self.cell(&job);
            self.launch(group, job, cpu, started, cell, Charge::held(cell.limit, lending)).await
        }
    }

    #[tokio::test]
    async fn eligible_work_fills_every_core_fairly_and_respects_group_limits() {
        let state = std::env::temp_dir().join(format!(
            "rust-compiler-capacity-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        std::fs::create_dir(&state).unwrap();
        let mut affinity: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        assert_eq!(
            unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&affinity), &mut affinity) },
            0
        );
        let cores: Vec<_> = (0..libc::CPU_SETSIZE as usize)
            .filter(|&c| unsafe { libc::CPU_ISSET(c, &affinity) })
            .take(4)
            .collect();
        let mut engine = Engine::restore(Config {
            state: state.clone(),
            minimum_ram: 0,
            average_ram: 1,
            overcommit_percent: super::super::OVERCOMMIT_PERCENT,
            limits: Default::default(),
        }, Clock::system(), Store::default(), Instance::current().unwrap(), attribution())
        .await
        .unwrap()
        .activated()
        .await
        .unwrap();
        let program: PathBuf = std::env::var_os("PATH")
            .unwrap()
            .to_string_lossy()
            .split(':')
            .map(|p| PathBuf::from(p).join("sleep"))
            .find(|p| p.is_file())
            .unwrap();
        let limit = cores.len().div_ceil(2);
        for group in ["left", "right"] {
            let jobs: Vec<_> = (0..cores.len())
                .map(
                    |i| json!({"job_id":format!("{group}-{i}"),"argv":[program,"0.1"],"cwd":state,"expected_runtime_secs":60}),
                )
                .collect();
            engine.requested(json!({"protocol":1,"op":"submit_group","party":group,"group":{"group_id":group,"jobs":jobs,"max_running":limit}})).await.unwrap();
        }
        let sample = Snapshot {
            available: 1 << 40,
            idle: cores.len(),
            cores: cores.clone(),
            ..Snapshot::default()
        };
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(engine.running.len(), cores.len().saturating_sub(1));
        let left = engine
            .running
            .values()
            .filter(|r| r.record["group_id"] == "left")
            .count();
        let right = engine.running.len() - left;
        assert!(left <= limit && right <= limit);
        assert!(left.abs_diff(right) <= 1);
        let assigned: BTreeSet<_> = engine
            .running
            .values()
            .map(|r| r.record["cpu"].as_u64().unwrap() as usize)
            .collect();
        assert_eq!(assigned, cores.into_iter().take(assigned.len()).collect());
        for active in engine.running.values() {
            assert_eq!(
                active.child.as_ref().unwrap().id().map(u64::from),
                active.record["pid"].as_u64()
            );
        }
        engine.drain();
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            engine.tick(sample.clone()).await.unwrap();
            if engine.drained() {
                break;
            }
        }
        assert!(engine.drained());
        assert_eq!(engine.results.len(), assigned.len());
        std::fs::remove_dir_all(state).unwrap();
    }
    async fn admission_fixture() -> (Engine, Snapshot) {
        admission_fixture_on(Clock::system(), Store::default()).await
    }
    /// The admission fixture reading time from `clock` and persisting through `store`.
    async fn admission_fixture_on(clock: Clock, store: Store) -> (Engine, Snapshot) {
        let state = std::env::temp_dir().join(format!(
            "compiler-memory-fairness-{}-{}-{}",
            std::process::id(),
            unsafe { libc::syscall(libc::SYS_gettid) },
            crate::protocol::now_unix().to_bits()
        ));
        std::fs::create_dir(&state).unwrap();
        let mut affinity: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        assert_eq!(
            unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&affinity), &mut affinity) },
            0
        );
        let cores = (0..libc::CPU_SETSIZE as usize)
            .filter(|&c| unsafe { libc::CPU_ISSET(c, &affinity) })
            .take(4)
            .collect::<Vec<_>>();
        let mut engine = Engine::restore(Config {
            state,
            minimum_ram: 10 << 30,
            average_ram: 1 << 30,
            overcommit_percent: super::super::OVERCOMMIT_PERCENT,
            limits: Default::default(),
        }, clock, store, Instance::current().unwrap(), attribution())
        .await
        .unwrap()
        .activated()
        .await
        .unwrap();
        // The fixture's sample alone says what memory is available.
        engine.read_memory = sample_alone();
        (
            engine,
            Snapshot {
                cores,
                idle: 4,
                available: 12 << 30,
                capacity: 128 << 30,
                ..Snapshot::default()
            },
        )
    }

    /// A fresh reading that never bounds the sample's figure.
    fn sample_alone() -> ReadMemory {
        Arc::new(|| Ok((u64::MAX, u64::MAX)))
    }

    async fn admit_group(engine: &mut Engine, name: &str, memory: u64, age: f64) {
        admit_jobs(engine, name, &[], memory, age).await;
    }

    // Groups are immutable once published, so every member is submitted at once.
    async fn admit_jobs(engine: &mut Engine, name: &str, extra: &[&str], memory: u64, age: f64) {
        admit_for(engine, name, name, extra, memory, age).await;
    }

    async fn admit_for(
        engine: &mut Engine,
        party: &str,
        name: &str,
        extra: &[&str],
        memory: u64,
        age: f64,
    ) {
        let program = std::env::var("PATH")
            .unwrap()
            .split(':')
            .map(|p| PathBuf::from(p).join("python3"))
            .find(|p| p.is_file())
            .unwrap();
        let jobs: Vec<_> = std::iter::once(name).chain(extra.iter().copied())
            .map(|id| json!({"job_id":id,"argv":[program,"-S","-c","import pathlib,time\nwhile not pathlib.Path('release').exists(): time.sleep(0.01)"],"cwd":engine.config.state,"memory_bytes":memory,"expected_runtime_secs":60}))
            .collect();
        engine
            .requested(
                json!({"protocol":1,"op":"submit_group","party":party,"group":{"group_id":name,"jobs":jobs}}),
            )
            .await
            .unwrap();
        engine
            .submitted
            .insert(name.into(), crate::protocol::now_unix() - age);
    }

    async fn finish_fixture(mut engine: Engine, sample: Snapshot) {
        std::fs::write(engine.config.state.join("release"), b"ready").unwrap();
        engine.drain();
        for _ in 0..1000 {
            engine.tick(sample.clone()).await.unwrap();
            if engine.drained() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(engine.drained());
        std::fs::remove_dir_all(engine.config.state).unwrap();
    }

    #[tokio::test]
    async fn aged_large_command_reserves_memory_against_repeated_small_backfill() {
        let (mut engine, mut sample) = admission_fixture().await;
        admit_group(&mut engine, "large", 4 << 30, 120.0).await;
        admit_group(&mut engine, "existing", 4 << 30, 0.0).await;
        let job = engine.groups["existing"].jobs[0].clone();
        engine
            .launch_holding("existing".into(), job, sample.cores[0], engine.clock.monotonic_secs(), 0)
            .await
            .unwrap();
        let pid = engine.running["existing"].record["pid"].as_u64().unwrap() as u32;
        sample.processes.insert(pid, (std::process::id(), 4 << 30));
        // A compile is a candidate only once a tick has launched since the restart.
        engine.tick(sample.clone()).await.unwrap();
        // Keep the already-running compiler alive while repeated arrivals are tested.
        for i in 0..5 {
            admit_group(&mut engine, &format!("small-{i}"), 1 << 30, 60.0).await;
            engine.tick(sample.clone()).await.unwrap();
            assert_eq!(engine.running.len(), 1);
        }
        let large = &engine.groups["large"];
        assert!(engine
            .waiting_reason(engine.admission().as_ref(), large, &large.jobs[0])
            .contains("waiting for RAM"));
        let small = &engine.groups["small-0"];
        assert!(engine
            .waiting_reason(engine.admission().as_ref(), small, &small.jobs[0])
            .contains("reservation: large"));
        sample.available = 15 << 30;
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("large"));
        assert_eq!(
            engine.running.len(),
            3,
            "the remaining GiB can still backfill"
        );
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn an_aged_compile_head_holds_back_younger_claims_and_compilers() {
        let (mut engine, mut sample) = admission_fixture().await;
        admit_group(&mut engine, "large", 4 << 30, 120.0).await;
        admit_group(&mut engine, "existing", 4 << 30, 0.0).await;
        let job = engine.groups["existing"].jobs[0].clone();
        engine
            .launch_holding("existing".into(), job, sample.cores[0], engine.clock.monotonic_secs(), 0)
            .await
            .unwrap();
        let pid = engine.running["existing"].record["pid"].as_u64().unwrap() as u32;
        sample.processes.insert(pid, (std::process::id(), 4 << 30));
        engine.tick(sample.clone()).await.unwrap();
        for i in 0..2 {
            admit_group(&mut engine, &format!("small-{i}"), 1 << 30, 60.0).await;
        }
        let mut owner = std::process::Command::new("sleep").arg("60").spawn().unwrap();
        shared(&owner);
        engine
            .requested((json!({"protocol":1,"op":"host_memory_acquire","claim_id":"preparation","memory_bytes":1u64 << 30,"label":"preparation","purpose":"host_only"}), Peer::opened(owner.id())))
            .await
            .unwrap();
        // Two GiB are left after headroom; the head needs four, which the running compiler frees.
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("large"));
        assert_eq!(engine.running.len(), 1, "the head holds every younger compiler back");
        assert_eq!(claim_status(&engine, "preparation")["state"], "pending", "and every younger claim");
        assert!(reason(&engine, "small-0", 0).contains("waiting for aged compiler memory reservation: large"));
        replays(&engine);
        owner.kill().unwrap();
        owner.wait().unwrap();
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_head_that_memory_held_elsewhere_makes_unserviceable_is_released_for_a_serviceable_candidate() {
        let (mut engine, mut sample) = admission_fixture().await;
        let sleeper = || {
            let child = std::process::Command::new("sleep").arg("60").spawn().unwrap();
            shared(&child);
            child
        };
        let claim = |owner: &std::process::Child, id: &str, bytes: u64| (json!({"protocol":1,"op":"host_memory_acquire","claim_id":id,"memory_bytes":bytes,"label":id}), Peer::opened(owner.id()));
        // Another campaign's granted, still unused claim of unknown purpose holds memory no compiler can drain.
        let mut owner = sleeper();
        let mut waiting = sleeper();
        sample.available = 35 * GIB / 2;
        engine.requested(claim(&owner, "elsewhere", 3 * GIB)).await.unwrap();
        publish(&mut engine, "x", "existing", &[("existing", 2 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "existing", 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "elsewhere")["state"], "granted");
        publish(&mut engine, "belief", "large", &[("large", 4 * GIB)], 120.0, 0).await;
        publish(&mut engine, "rwkv", "younger", &[("younger", 3 * GIB)], 60.0, 0).await;
        engine.requested(claim(&waiting, "oversized", 6 * GIB)).await.unwrap();
        // 2.5 GiB are left; the running compiler would free 2 more, so S is 4.5 GiB.
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("large"));
        // Memory held elsewhere grows: S is 3.5 GiB, too little for the head.
        sample.available = 33 * GIB / 2;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("younger"), "the serviceable candidate holds the head");
        assert!(reason(&engine, "large", 0).contains("waiting for aged compiler memory reservation: younger"));
        assert!(reason(&engine, "younger", 0).contains("holding the aged compiler memory reservation"));
        let oversized = claim_status(&engine, "oversized");
        assert_eq!(oversized["owner_pid"], waiting.id());
        assert!(oversized["pending_seconds"].as_f64().is_some_and(|age| age >= 0.0));
        assert!(oversized["waiting"].as_str().unwrap().starts_with("waiting for RAM: needs 6442450944 bytes"));
        // The other campaign ends: the head fits and starts.
        owner.kill().unwrap();
        owner.wait().unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("younger"));
        assert_eq!(claim_status(&engine, "oversized")["state"], "pending");
        replays(&engine);
        waiting.kill().unwrap();
        waiting.wait().unwrap();
        finish_fixture(engine, sample).await;
    }

    /// A compiler command that runs until the fixture writes its release file.
    fn waiting() -> Value {
        let program = std::env::var("PATH")
            .unwrap()
            .split(':')
            .map(|p| PathBuf::from(p).join("python3"))
            .find(|p| p.is_file())
            .unwrap();
        json!([program, "-S", "-c", "import pathlib,time\nwhile not pathlib.Path('release').exists(): time.sleep(0.01)"])
    }

    #[tokio::test]
    async fn every_compile_declares_its_runtime_and_large_requests_are_warned_at_queuing() {
        let (mut engine, sample) = admission_fixture().await;
        let state = engine.config.state.clone();
        let submit = |job: Value| json!({"protocol":1,"op":"submit_group","party":"p","group":{"group_id":job["job_id"],"jobs":[job]}});
        let undeclared = engine
            .requested(submit(json!({"job_id":"undeclared","argv":waiting(),"cwd":state})))
            .await
            .unwrap_err();
        assert!(undeclared.contains("declares no expected_runtime_secs"), "{undeclared}");
        let small = engine
            .requested(submit(json!({"job_id":"small","argv":waiting(),"cwd":state,"memory_bytes":1u64 << 30,"expected_runtime_secs":60})))
            .await
            .unwrap();
        assert_eq!(small["warnings"], json!([]));
        let large = engine
            .requested(submit(json!({"job_id":"large","argv":waiting(),"cwd":state,"memory_bytes":11_000_000_000u64,"expected_runtime_secs":60})))
            .await
            .unwrap();
        assert!(large["warnings"][0].as_str().unwrap().starts_with("large: declares 11000000000 bytes"));
        let mut owner = std::process::Command::new("sleep").arg("60").spawn().unwrap();
        shared(&owner);
        let claim = engine
            .requested((json!({"protocol":1,"op":"host_memory_register","claim_id":"large-claim","memory_bytes":11_000_000_000u64,"label":"large claim"}), Peer::opened(owner.id())))
            .await
            .unwrap();
        assert_eq!(claim["state"], "pending", "registering a claim does not wait for it");
        assert!(claim["warnings"][0].as_str().unwrap().contains("very low priority"));
        owner.kill().unwrap();
        owner.wait().unwrap();
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn while_the_aged_reservation_waits_only_jobs_ending_before_the_longest_running_one_run_ahead() {
        let (mut engine, mut sample) = admission_fixture().await;
        admit_group(&mut engine, "large", 4 << 30, 120.0).await;
        admit_group(&mut engine, "existing", 4 << 30, 0.0).await;
        let job = engine.groups["existing"].jobs[0].clone();
        engine
            .launch_holding("existing".into(), job, sample.cores[0], engine.clock.monotonic_secs(), 0)
            .await
            .unwrap();
        let pid = engine.running["existing"].record["pid"].as_u64().unwrap() as u32;
        sample.processes.insert(pid, (std::process::id(), 4 << 30));
        // A compile is a candidate only once a tick has launched since the restart.
        engine.tick(sample.clone()).await.unwrap();
        let state = engine.config.state.clone();
        for (name, seconds) in [("short", 30), ("long", 120)] {
            engine
                .requested(json!({"protocol":1,"op":"submit_group","party":name,"group":{"group_id":name,"jobs":[
                    {"job_id":name,"argv":waiting(),"cwd":state,"memory_bytes":1u64 << 30,"expected_runtime_secs":seconds}]}}))
                .await
                .unwrap();
            engine.submitted.insert(name.into(), crate::protocol::now_unix() - 60.0);
        }
        // The aged job waits for the running compiler, declared to run about 60 s more.
        engine.tick(sample.clone()).await.unwrap();
        assert!(!engine.running.contains_key("large"));
        assert!(engine.running.contains_key("short"), "a job declaring 30 s ends before the 60 s compiler");
        assert!(!engine.running.contains_key("long"));
        let admission = engine.admission();
        let long = &engine.groups["long"];
        let reason = engine.waiting_reason(admission.as_ref(), long, &long.jobs[0]);
        assert!(reason.contains("s the longest running compile has left"), "{reason}");
        let horizon = admission.as_ref().map(|admission| admission.deadline.unwrap() - admission.now).unwrap();
        assert!(horizon > 30.0 && horizon <= 60.0, "{horizon}");
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_compile_past_twice_its_declared_runtime_is_ended_and_its_result_says_so() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 64 << 30;
        let state = engine.config.state.clone();
        engine
            .requested(json!({"protocol":1,"op":"submit_group","party":"slow","group":{"group_id":"slow","jobs":[
                {"job_id":"slow","argv":waiting(),"cwd":state,"memory_bytes":1u64 << 30,"expected_runtime_secs":1}]}}))
            .await
            .unwrap();
        for _ in 0..800 {
            engine.tick(sample.clone()).await.unwrap();
            if engine.results.contains_key("slow") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let result = &engine.results["slow"];
        assert_eq!(result["outcome"], "fail");
        assert_eq!(result["detail"], "compiler ran past twice its declared runtime of 1 s");
        assert!(result["execution_seconds"].as_f64().unwrap() > 2.0);
        assert_eq!(result["expected_runtime_secs"], 1);
        assert!(result["warnings"][0].as_str().unwrap().contains("ended after running past twice its declared 1 s"));
        finish_fixture(engine, sample).await;
    }

    /// Needs a delegated cgroup of its own:
    /// `systemd-run --user --scope -p Delegate=memory <unittests> --ignored measured`
    #[tokio::test]
    #[ignore]
    async fn a_contained_compile_reports_its_measured_memory_and_cores_against_its_declaration() {
        let (mut engine, mut sample) = admission_fixture().await;
        engine.containment = Containment::delegated_scope();
        sample.available = 64 << 30;
        let program = waiting()[0].clone();
        let busy = "import time\nheld = bytearray(96 << 20)\nheld[::4096] = b'x' * len(held[::4096])\nend = time.monotonic() + 1.5\nwhile time.monotonic() < end: pass";
        let state = engine.config.state.clone();
        engine
            .requested(json!({"protocol":1,"op":"submit_group","party":"measured","group":{"group_id":"measured","jobs":[
                {"job_id":"measured","argv":[program,"-S","-c",busy],"cwd":state,"memory_bytes":1u64 << 30,"expected_runtime_secs":5}]}}))
            .await
            .unwrap();
        for _ in 0..1000 {
            engine.tick(sample.clone()).await.unwrap();
            if engine.results.contains_key("measured") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let result = &engine.results["measured"];
        assert_eq!(result["outcome"], "success", "{result}");
        let peak = result["peak_memory_bytes"].as_u64().unwrap();
        let median = result["median_memory_bytes"].as_u64().unwrap();
        let p90 = result["p90_memory_bytes"].as_u64().unwrap();
        assert!(peak >= 96 << 20 && p90 <= peak && median <= p90 && median > 0, "{result}");
        assert!(result["median_cores"].as_f64().unwrap() >= 1.0, "{result}");
        let p90_cores = result["p90_cores"].as_f64().unwrap();
        assert!(p90_cores > 0.5 && p90_cores < 2.0, "one busy core: {result}");
        let warnings = result["warnings"].to_string();
        assert!(warnings.contains("less than half its declared 5 s") && warnings.contains("more than 25% below"), "{result}");
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn compilations_take_ram_turns_with_host_claims_by_party_share() {
        let (mut engine, sample) = admission_fixture().await;
        let mut owners: Vec<_> = (0..2)
            .map(|_| {
                let child = std::process::Command::new("sleep").arg("60").spawn().unwrap();
                shared(&child);
                child
            })
            .collect();
        let claim = |owner: &std::process::Child, id: &str| (json!({"protocol":1,"op":"host_memory_acquire","claim_id":id,"memory_bytes":1u64 << 30,"label":"preparation"}), Peer::opened(owner.id()));
        engine.requested(claim(&owners[0], "held")).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        admit_group(&mut engine, "compiles", 1 << 30, 0.0).await;
        engine.requested(claim(&owners[1], "next")).await.unwrap();
        // One GiB is left after headroom and the granted claim.
        engine.tick(sample.clone()).await.unwrap();
        assert!(
            engine.running.contains_key("compiles"),
            "a party holding no RAM compiles before a party holding a claim gets another"
        );
        let states: BTreeMap<String, String> = engine.host_memory.health(None)["claims"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| {
                (
                    c["claim_id"].as_str().unwrap().into(),
                    c["state"].as_str().unwrap().into(),
                )
            })
            .collect();
        assert_eq!(states["held"], "granted");
        assert_eq!(states["next"], "pending");
        for owner in &mut owners {
            owner.kill().unwrap();
            owner.wait().unwrap();
        }
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn status_of_a_large_aged_backlog_stays_within_the_watchdog() {
        let (mut engine, sample) = admission_fixture().await;
        for g in 0..200 {
            let jobs: Vec<_> = (0..500)
                .map(|j| json!({"job_id":format!("g{g}-j{j}"),"argv":["true"],"cwd":engine.config.state,"memory_bytes":1u64 << 30,"expected_runtime_secs":60}))
                .collect();
            engine
                .requested(json!({"protocol":1,"op":"submit_group","party":format!("g{g}"),"group":{"group_id":format!("g{g}"),"jobs":jobs}}))
                .await
                .unwrap();
            engine
                .submitted
                .insert(format!("g{g}"), crate::protocol::now_unix() - 120.0);
        }
        engine.sample = Some(sample);
        let started = std::time::Instant::now();
        let status = engine
            .requested(json!({"protocol":1,"op":"group_status","group_id":"g7"}))
            .await
            .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(status["jobs"].as_array().unwrap().len(), 500);
        assert_eq!(status["jobs"][0]["detail"], "ready for fair admission");
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "one status request held the compiler actor for {elapsed:?}"
        );
        std::fs::remove_dir_all(engine.config.state).unwrap();
    }

    #[tokio::test]
    async fn a_group_wait_is_answered_once_when_the_group_completes() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 64 << 30;
        admit_group(&mut engine, "awaited", 1 << 30, 0.0).await;
        let wait = json!({"protocol":1,"op":"group_wait","group_id":"awaited"});
        let (reply, mut answer) = tokio::sync::oneshot::channel();
        engine.serve(wait.clone(), Peer::opened(std::process::id()), reply).await;
        let (reply, departed) = tokio::sync::oneshot::channel();
        engine.serve(wait.clone(), Peer::opened(std::process::id()), reply).await;
        drop(departed);
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("awaited"));
        assert!(
            answer.try_recv().is_err(),
            "an unfinished group stays parked"
        );
        assert_eq!(engine.group_watches.len(), 1, "a departed waiter is pruned");
        std::fs::write(engine.config.state.join("release"), b"ready").unwrap();
        for _ in 0..1000 {
            if engine.results.contains_key("awaited") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            engine.tick(sample.clone()).await.unwrap();
        }
        let status = answer.try_recv().unwrap().unwrap();
        assert_eq!(status["complete"], true);
        assert_eq!(status["jobs"][0]["outcome"], "success");
        assert_eq!(engine.group_watches.len(), 0);
        let (reply, complete) = tokio::sync::oneshot::channel();
        engine.serve(wait, Peer::opened(std::process::id()), reply).await;
        assert_eq!(complete.await.unwrap().unwrap()["complete"], true);
        let (reply, unknown) = tokio::sync::oneshot::channel();
        engine
            .serve(
                json!({"protocol":1,"op":"group_wait","group_id":"missing"}),
                Peer::opened(std::process::id()),
                reply,
            )
            .await;
        assert_eq!(
            unknown.await.unwrap().unwrap_err(),
            "unknown compiler group"
        );
        finish_fixture(engine, sample).await;
    }

    /// Needs a delegated cgroup of its own:
    /// `systemd-run --user --scope -p Delegate=memory <unittests> --ignored outgrows`
    #[tokio::test]
    #[ignore]
    async fn a_compilation_that_outgrows_its_cell_runs_again_and_its_subqueue_grows() {
        use super::super::memory_growth::STEP;
        const MIB: u64 = 1 << 20;
        let (mut engine, mut sample) = admission_fixture().await;
        engine.containment = Containment::delegated_scope();
        sample.available = 64 << 30;
        let allocate = |bytes: u64| json!(["/bin/sh", "-c", format!("dd if=/dev/zero of=/dev/null bs={bytes} count=1 status=none")]);
        let state = engine.config.state.clone();
        let jobs = json!([
            {"job_id":"hungry","argv":allocate(96 * MIB),"cwd":state,"memory_bytes":64 * MIB,"expected_runtime_secs":60},
            {"job_id":"steady","argv":["/bin/sh","-c","while [ ! -e release ]; do sleep 0.05; done"],"cwd":state,"memory_bytes":256 * MIB,"expected_runtime_secs":60},
            {"job_id":"later","argv":allocate(MIB),"cwd":state,"memory_bytes":128 * MIB,"dependencies":["steady"],"expected_runtime_secs":60},
        ]);
        engine
            .requested(json!({"protocol":1,"op":"submit_group","party":"grows","group":{"group_id":"grows","jobs":jobs}}))
            .await
            .unwrap();
        let capped = json!([{"job_id":"capped","argv":allocate(96 * MIB),"cwd":state,"memory_bytes":64 * MIB,"error_memory_bytes":64 * MIB,"expected_runtime_secs":60}]);
        engine
            .requested(json!({"protocol":1,"op":"submit_group","party":"ceiling","group":{"group_id":"ceiling","jobs":capped}}))
            .await
            .unwrap();
        for _ in 0..1000 {
            if engine.results.contains_key("hungry") && engine.results.contains_key("capped") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            engine.tick(sample.clone()).await.unwrap();
        }
        let hungry = &engine.results["hungry"];
        assert_eq!(hungry["outcome"], "success", "{hungry}");
        assert_eq!(hungry["memory_limit_bytes"], 64 * MIB + STEP, "the retry gains one step");
        assert!(hungry["peak_memory_bytes"].as_u64().unwrap() >= 96 * MIB);
        let log = std::fs::read_to_string(hungry["log_path"].as_str().unwrap()).unwrap();
        assert!(log.contains("out of memory in its 64 MiB cell\ncompile-queue: attempt in a 576 MiB memory cell"), "{log}");
        let capped = &engine.results["capped"];
        assert_eq!(capped["outcome"], "fail");
        assert!(capped["detail"].as_str().unwrap().contains("error memory"), "{capped}");
        let steady = engine.running["steady"].record["memory_bytes"].as_u64().unwrap();
        assert_eq!(steady, 256 * MIB + 64 * MIB, "the kill shares its cell with the running sibling");
        std::fs::write(engine.config.state.join("release"), b"ready").unwrap();
        for _ in 0..1000 {
            if engine.results.contains_key("later") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            engine.tick(sample.clone()).await.unwrap();
        }
        assert_eq!(engine.results["later"]["outcome"], "success");
        assert_eq!(engine.results["later"]["memory_limit_bytes"], 128 * MIB + STEP, "a waiting sibling gains a step");
        assert_eq!(engine.results["steady"]["memory_limit_bytes"], steady);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn young_memory_blocked_command_allows_backfill_until_it_ages() {
        let (mut engine, sample) = admission_fixture().await;
        admit_group(&mut engine, "large", 4 << 30, 1.0).await;
        admit_group(&mut engine, "small", 1 << 30, 0.0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert!(!engine.running.contains_key("large"));
        assert!(engine.running.contains_key("small"));
        // A job waiting for memory holds no head before AGED_SECS past its eligibility.
        engine.tick(sample.clone()).await.unwrap();
        assert!(head(&engine).is_none());
        assert!(reason(&engine, "large", 0).starts_with("waiting for RAM: needs"));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn cancelling_a_large_pending_group_records_every_result_durably() {
        let (mut engine, sample) = admission_fixture().await;
        let extra: Vec<String> = (1..1000).map(|i| format!("bulk-{i}")).collect();
        let extra: Vec<&str> = extra.iter().map(String::as_str).collect();
        admit_jobs(&mut engine, "bulk", &extra, 1 << 30, 0.0).await;
        engine
            .requested(json!({"protocol":1,"op":"cancel","group_id":"bulk","reason":"bulk fixture"}))
            .await
            .unwrap();
        assert!(engine.progress.complete("bulk"));
        assert_eq!(
            std::fs::read_dir(engine.config.state.join("cancellations"))
                .unwrap()
                .count(),
            0,
            "an applied journal is removed"
        );
        let written = std::fs::read_dir(engine.config.state.join("results"))
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|e| e == "json")
            })
            .count();
        assert_eq!(written, 1000);
        let config = Config {
            state: engine.config.state.clone(),
            minimum_ram: engine.config.minimum_ram,
            average_ram: engine.config.average_ram,
            overcommit_percent: engine.config.overcommit_percent,
            limits: engine.config.limits,
        };
        let restored = Engine::restore(config, Clock::system(), Store::default(), Instance::current().unwrap(), attribution()).await.unwrap().activated().await.unwrap();
        assert_eq!(
            restored
                .results
                .values()
                .filter(|r| r["outcome"] == "cancelled" && r["detail"] == "bulk fixture")
                .count(),
            1000
        );
        finish_fixture(restored, sample).await;
    }

    #[tokio::test]
    async fn restore_applies_and_removes_an_interrupted_cancellation_journal() {
        let (mut engine, sample) = admission_fixture().await;
        admit_jobs(&mut engine, "interrupted", &["interrupted-1"], 1 << 30, 0.0).await;
        let pending: Vec<_> = engine.groups["interrupted"]
            .jobs
            .iter()
            .map(|job| engine.result("interrupted", job, "cancelled", "interrupted fixture"))
            .collect();
        wrote(
            &engine
                .config
                .state
                .join("cancellations")
                .join("crashed.json"),
            &json!({"pending":pending,"running":[],"reason":"interrupted fixture"}),
        ).await;
        let config = Config {
            state: engine.config.state.clone(),
            minimum_ram: engine.config.minimum_ram,
            average_ram: engine.config.average_ram,
            overcommit_percent: engine.config.overcommit_percent,
            limits: engine.config.limits,
        };
        let restored = Engine::restore(config, Clock::system(), Store::default(), Instance::current().unwrap(), attribution()).await.unwrap().activated().await.unwrap();
        assert!(restored.progress.complete("interrupted"));
        assert!(restored
            .config
            .state
            .join("results")
            .join("interrupted-1.json")
            .exists());
        assert_eq!(
            std::fs::read_dir(restored.config.state.join("cancellations"))
                .unwrap()
                .count(),
            0
        );
        finish_fixture(restored, sample).await;
    }

    #[tokio::test]
    async fn compilations_run_below_the_daemon_priority() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 64 << 30;
        let program = std::env::var("PATH")
            .unwrap()
            .split(':')
            .map(|p| PathBuf::from(p).join("python3"))
            .find(|p| p.is_file())
            .unwrap();
        let probe = "import os,pathlib\npathlib.Path('nice').write_text(str(os.getpriority(os.PRIO_PROCESS, 0)))";
        engine
            .requested(
                json!({"protocol":1,"op":"submit_group","party":"nice","group":{"group_id":"nice","jobs":[
                {"job_id":"nice","argv":[program,"-S","-c",probe],"cwd":engine.config.state,"expected_runtime_secs":60}]}}),
            )
            .await
            .unwrap();
        for _ in 0..1000 {
            engine.tick(sample.clone()).await.unwrap();
            if engine.results.contains_key("nice") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(engine.results["nice"]["outcome"], "success");
        assert_eq!(
            std::fs::read_to_string(engine.config.state.join("nice")).unwrap(),
            "19"
        );
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_campaign_split_into_many_groups_receives_one_share() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 64 << 30;
        for i in 0..6 {
            admit_for(&mut engine, "campaign", &format!("c{i}"), &[], 1 << 30, 0.0).await;
        }
        admit_for(&mut engine, "validation", "v", &[], 1 << 30, 0.0).await;
        engine.tick(sample.clone()).await.unwrap();
        let campaign = engine
            .running
            .values()
            .filter(|r| engine.parties.of(r.record["group_id"].as_str().unwrap()) == "campaign")
            .count();
        assert!(
            engine.running.contains_key("v"),
            "a later small party is not parked behind every older group"
        );
        assert_eq!(campaign, 2, "three eligible cores cap one party at two");
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn legacy_groups_keep_their_party_file_or_are_unattributed() {
        let (mut engine, sample) = admission_fixture().await;
        let state = engine.config.state.clone();
        let group = |id: &str| json!({"group_id":id,"jobs":[]});
        let file = |directory: &str, id: &str| state.join(directory).join(format!("{id}.json"));
        wrote(&file("groups", "recorded"), &group("recorded")).await;
        wrote(&file("parties", "recorded"), &json!({"group_id":"recorded","party":"alpha","priority":3})).await;
        wrote(&file("groups", "legacy"), &group("legacy")).await;
        wrote(&file("groups", "new"), &json!({"group":group("new"),"party":"beta","priority":10})).await;
        wrote(&file("parties", "new"), &json!({"group_id":"new","party":"gamma","priority":0})).await;
        engine.attribution = Attribution::system();
        let answer = engine
            .requested(
                (json!({"protocol":1,"op":"submit_group",
                "group":{"group_id":"session","jobs":[]}}), Peer::opened(std::process::id())),
            )
            .await
            .unwrap();
        let own = crate::procs::Process::read(std::process::id()).unwrap();
        assert_eq!(answer["party"], json!(Attribution::system().party(own.pid, own.birth).unwrap()), "the test process's unit");
        let urgent = |id: &str, priority: i64| {
            json!({"protocol":1,"op":"submit_group","party":"urgent","priority":priority,
                "group":{"group_id":id,"jobs":[]}})
        };
        assert_eq!(
            engine.requested(urgent("urgent", 10)).await.unwrap()["priority"],
            10
        );
        assert!(engine.requested(urgent("beyond", 11)).await.is_err());
        let restored = restore_of(&engine).await;
        let held = |id: &str| (restored.parties.of(id).to_owned(), restored.parties.priority(id));
        assert_eq!(held("recorded"), ("alpha".to_owned(), 3), "a legacy group keeps its party file");
        assert_eq!(held("legacy"), (party::UNATTRIBUTED.to_owned(), 0), "a legacy group without one is unattributed");
        assert_eq!(held("new"), ("beta".to_owned(), 10), "a record's own party wins over an orphan party file");
        assert_eq!(restored.parties.priority("urgent"), 10);
        wrote(&file("groups", "unknown"), &json!({"group":group("unknown"),"party":"p","priority":0,"unknown":1})).await;
        assert!(
            Engine::restore(engine.handoff_config(), Clock::system(), Store::default(), Instance::current().unwrap(), attribution()).await.is_err(),
            "a record with an unknown field refuses the restore"
        );
        finish_fixture(restored, sample).await;
    }

    #[tokio::test]
    async fn a_completed_group_is_retired_after_its_retention() {
        let (mut engine, sample) = admission_fixture().await;
        admit_jobs(&mut engine, "done", &["done-1"], 1 << 30, 0.0).await;
        let state = engine.config.state.clone();
        std::fs::write(state.join("logs").join("done.log"), b"compiler output").unwrap();
        engine
            .requested(
                json!({"protocol":1,"op":"cancel","group_id":"done","reason":"retention fixture"}),
            )
            .await
            .unwrap();
        let completed = engine.completed["done"];
        engine.retire(completed + RETENTION - 1.0).await.unwrap();
        assert!(
            engine.groups.contains_key("done"),
            "kept until the retention passes"
        );
        engine.retire(completed + RETENTION).await.unwrap();
        assert!(!state.join("groups").join("done.json").exists());
        assert!(!state.join("results").join("done.json").exists());
        assert!(!state.join("results").join("done-1.json").exists());
        assert!(!state.join("logs").join("done.log").exists());
        assert!(!engine.results.contains_key("done"));
        assert_eq!(engine.status("done").unwrap_err(), "unknown compiler group");
        admit_jobs(&mut engine, "done", &["done-1"], 1 << 30, 0.0).await;
        assert!(
            !engine.progress.complete("done"),
            "retired identities are free again"
        );
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn restore_removes_results_whose_group_was_already_retired() {
        let (engine, sample) = admission_fixture().await;
        let state = engine.config.state.clone();
        wrote(
            &state.join("results").join("orphan.json"),
            &json!({"group_id":"gone","job_id":"orphan","outcome":"success","execution_seconds":1.0}),
        ).await;
        std::fs::write(state.join("logs").join("orphan.log"), b"output").unwrap();
        let restored = Engine::restore(engine.handoff_config(), Clock::system(), Store::default(), Instance::current().unwrap(), attribution()).await.unwrap().activated().await.unwrap();
        assert!(!restored.results.contains_key("orphan"));
        assert!(!state.join("results").join("orphan.json").exists());
        assert!(!state.join("logs").join("orphan.log").exists());
        finish_fixture(restored, sample).await;
    }

    #[tokio::test]
    async fn impossible_memory_request_fails_without_blocking_other_compilers() {
        let (mut engine, sample) = admission_fixture().await;
        admit_group(&mut engine, "impossible", 129 << 30, 120.0).await;
        admit_group(&mut engine, "small", 1 << 30, 0.0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(engine.results["impossible"]["outcome"], "fail");
        assert!(engine.results["impossible"]["detail"]
            .as_str()
            .unwrap()
            .contains("capacity"));
        assert!(engine.results["impossible"]["started_at"].is_null());
        assert!(engine.running.contains_key("small"));
        finish_fixture(engine, sample).await;
    }

    /// Writes a group as a generation before 15 saved it: its jobs may declare no runtime.
    async fn saved_before_runtimes(engine: &Engine, party: &str, group: &str, jobs: Value, max_running: usize) {
        let state = &engine.config.state;
        wrote(
            &state.join("groups").join(format!("{group}.json")),
            &json!({"group_id":group,"jobs":jobs,"max_running":max_running}),
        ).await;
        wrote(&state.join("parties").join(format!("{group}.json")), &json!({"group_id":group,"party":party,"priority":0})).await;
    }

    #[tokio::test]
    async fn the_first_tick_refuses_restored_pending_jobs_that_declare_no_runtime_and_restore_writes_nothing() {
        let (engine, sample) = admission_fixture().await;
        let program = waiting()[0].clone();
        let state = engine.config.state.clone();
        let job = |id: &str, memory: u64, runtime: Option<u64>, dependencies: &[&str]| {
            let mut job = json!({"job_id":id,"argv":[program,"-S","-c","import time"],"cwd":state,"memory_bytes":memory,"dependencies":dependencies});
            if let Some(runtime) = runtime {
                job["expected_runtime_secs"] = json!(runtime);
            }
            job
        };
        saved_before_runtimes(&engine, "p", "legacy", json!([job("u", GIB, None, &[]), job("h", 5 * GIB, None, &[]), job("dependant", GIB, Some(60), &["u"])]), 1).await;
        saved_before_runtimes(&engine, "q", "contracted", json!([job("k", GIB, Some(60), &[])]), 0).await;
        let config = engine.handoff_config();
        drop(engine);
        let mut predecessor = Engine::restore(config, Clock::system(), Store::default(), Instance::current().unwrap(), attribution()).await.unwrap().activated().await.unwrap();
        predecessor.read_memory = sample_alone();
        // A successor restores the same state and fails before activation: it writes no refusal.
        let successor = Engine::restore(predecessor.handoff_config(), Clock::system(), Store::default(), Instance::current().unwrap(), attribution()).await.unwrap().activated().await.unwrap();
        drop(successor);
        for id in ["u", "h", "dependant"] {
            assert!(!state.join("results").join(format!("{id}.json")).exists(), "restore writes no refusal");
            assert_eq!(predecessor.status("legacy").unwrap()["jobs"].as_array().unwrap().iter().find(|j| j["job_id"] == id).unwrap()["outcome"], "queued");
        }
        // The generation holding authority refuses them in its first tick, before any launch.
        let mut sample = sample;
        sample.available = 64 * GIB;
        predecessor.tick(sample.clone()).await.unwrap();
        for id in ["u", "h"] {
            let result = &predecessor.results[id];
            assert_eq!((result["outcome"].as_str(), result["detail"].as_str()), (Some("fail"), Some(contract::UNDECLARED_RUNTIME)));
            assert!(result["started_at"].is_null());
            assert!(state.join("results").join(format!("{id}.json")).exists(), "the refusal is durable");
        }
        assert_eq!(predecessor.results["dependant"]["detail"], "compile prerequisite failed");
        assert!(predecessor.running.contains_key("k"), "a contracted job of the same state launches as usual");
        for _ in 0..3 {
            predecessor.tick(sample.clone()).await.unwrap();
        }
        assert!(!predecessor.running.contains_key("u") && !predecessor.running.contains_key("h"));
        finish_fixture(predecessor, sample).await;
    }
    #[tokio::test]
    async fn serving_an_aged_group_gives_the_next_aged_group_its_turn() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.cores.truncate(2);
        // 2 GiB admissible; the running holder declares 2 GiB and uses them, so S is 4 GiB.
        publish(&mut engine, "holder", "holder", &[("holder", 2 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "holder", 2 * GIB).await;
        engine.tick(sample.clone()).await.unwrap();
        publish(&mut engine, "first", "first", &[("first", 3 * GIB), ("first-again", 3 * GIB)], 120.0, 0).await;
        publish(&mut engine, "second", "second", &[("second", 3 * GIB)], 60.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("first"), "both wait for memory; the oldest holds the head");
        assert!(reason(&engine, "first", 0).contains("holding the aged compiler memory reservation"));
        end(&mut engine, "holder");
        sample.available = 15 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("first"));
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(
            head(&engine).as_deref(),
            Some("second"),
            "the turn passed, so the second party's job holds the head although first-again is older"
        );
        assert!(reason(&engine, "second", 0).contains("holding the aged compiler memory reservation"));
        assert!(reason(&engine, "first", 1).contains("concurrency capacity"), "first-again's group is at its limit");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }
    #[tokio::test]
    async fn external_memory_pressure_cannot_turn_reservation_into_a_deadlock() {
        let (mut engine, mut sample) = admission_fixture().await;
        admit_group(&mut engine, "large", 4 << 30, 120.0).await;
        admit_group(&mut engine, "small", 1 << 30, 0.0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert!(!engine.running.contains_key("large"));
        assert!(engine.running.contains_key("small"));
        sample.available = 16 << 30;
        engine.tick(sample.clone()).await.unwrap();
        assert!(
            engine.running.contains_key("large"),
            "external memory recovery immediately restores the aged job's priority"
        );
        finish_fixture(engine, sample).await;
    }
    #[tokio::test]
    async fn one_subqueue_cannot_take_more_than_two_thirds_of_eligible_cores() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 64 << 30;
        let extra: Vec<String> = (0..6).map(|i| format!("alone-{i}")).collect();
        let extra: Vec<&str> = extra.iter().map(String::as_str).collect();
        admit_jobs(&mut engine, "alone", &extra, 1 << 30, 0.0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(
            engine.running.len(),
            2,
            "four affinity cores leave three eligible, capped at two per group"
        );
        admit_group(&mut engine, "other", 1 << 30, 0.0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(engine.running.len(), 3);
        assert!(engine.running.contains_key("other"));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_long_served_campaign_keeps_its_share_of_slots_beside_newer_parties() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 64 << 30;
        let backlog: Vec<String> = (0..6).map(|i| format!("campaign-{i}")).collect();
        let backlog: Vec<&str> = backlog.iter().map(String::as_str).collect();
        admit_for(&mut engine, "campaign", "served", &[], 1 << 30, 0.0).await;
        admit_jobs(&mut engine, "newer", &["newer-1", "newer-2"], 1 << 30, 0.0).await;
        admit_jobs(&mut engine, "newest", &["newest-1", "newest-2"], 1 << 30, 0.0).await;
        let job = &engine.groups["served"].jobs[0];
        let mut result = engine.result("served", job, "success", "a long history of service");
        result["execution_seconds"] = json!(1.0e6);
        engine.finish(result).await.unwrap();
        admit_for(&mut engine, "campaign", "backlog", &backlog, 1 << 30, 0.0).await;
        engine.tick(sample.clone()).await.unwrap();
        let held = engine.held(|_| 1);
        assert_eq!(
            [held.get("campaign"), held.get("newer"), held.get("newest")],
            [Some(&fairness::charge(1, 0)); 3],
            "three eligible slots go one to each party, whatever the campaign was served"
        );
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_group_with_a_result_sink_publishes_its_results_there_before_it_retires() {
        let (mut engine, sample) = admission_fixture().await;
        let sink = std::env::temp_dir().join(format!(
            "compile-result-sink-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        std::fs::create_dir(&sink).unwrap();
        let missing = sink.join("removed-campaign");
        let state = engine.config.state.clone();
        for (id, result_sink) in [("sunk", json!(sink)), ("plain", Value::Null), ("lost", json!(missing))] {
            engine
                .requested(json!({"protocol":1,"op":"submit_group","party":"p","group":{"group_id":id,
                    "jobs":[{"job_id":id,"argv":waiting(),"cwd":state,"expected_runtime_secs":60}],
                    "result_sink":result_sink}}))
                .await
                .unwrap();
            engine
                .requested(json!({"protocol":1,"op":"cancel","group_id":id,"reason":"sink fixture"}))
                .await
                .unwrap();
        }
        let published = store::read(&sink.join("sunk.results.json")).await.unwrap();
        let status = engine.status("sunk").unwrap();
        assert_eq!(published["jobs"], status["jobs"]);
        assert_eq!(published["complete"], true);
        assert_eq!(published["jobs"][0]["outcome"], "cancelled");
        assert!(status["result_sink_error"].is_null());
        assert_eq!(
            std::fs::read_dir(&sink).unwrap().count(),
            1,
            "a group without a sink writes nowhere"
        );
        assert!(engine.status("plain").unwrap()["result_sink_error"].is_null());
        assert!(engine.status("lost").unwrap()["result_sink_error"]
            .as_str()
            .unwrap()
            .contains("is not a directory"));
        let completed = engine.completed["sunk"];
        engine.retire(completed + RETENTION).await.unwrap();
        assert_eq!(engine.status("sunk").unwrap_err(), "unknown compiler group");
        assert_eq!(
            store::read(&sink.join("sunk.results.json")).await.unwrap(),
            published,
            "the sink outlives the group"
        );
        std::fs::remove_dir_all(sink).unwrap();
        finish_fixture(engine, sample).await;
    }

    /// One party's group, submitted now, whose jobs each declare `memory` and wait for the release file.
    async fn submit_party_group(engine: &mut Engine, submitter: impl Submitter, group: &str, jobs: &[(&str, u64)], max_running: usize) {
        let state = engine.config.state.clone();
        let jobs: Vec<_> = jobs
            .iter()
            .map(|(id, memory)| json!({"job_id":id,"argv":waiting(),"cwd":state,"memory_bytes":memory,"expected_runtime_secs":60}))
            .collect();
        engine
            .requested(submitter.submitting(json!({"protocol":1,"op":"submit_group",
                "group":{"group_id":group,"jobs":jobs,"max_running":max_running}})))
            .await
            .unwrap();
    }

    fn running_ids(engine: &Engine) -> BTreeSet<String> {
        engine.running.keys().cloned().collect()
    }

    #[tokio::test]
    async fn one_partys_groups_launch_in_submission_order_also_after_a_restore() {
        const GROUPS: [&str; 3] = ["z-first", "m-second", "a-third"];
        let (mut engine, sample) = admission_fixture().await;
        for group in GROUPS {
            let again = format!("{group}-again");
            submit_party_group(&mut engine, "p", group, &[(group, 1 << 30), (&again, 1 << 30)], 0).await;
        }
        // Two GiB are admissible: both slots go to the earliest-submitted group with eligible work.
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(running_ids(&engine), BTreeSet::from(["z-first".into(), "z-first-again".into()]));
        finish_fixture(engine, sample).await;

        let (mut engine, sample) = admission_fixture().await;
        let now = std::time::SystemTime::now();
        for (age, group) in [(30, "z-first"), (20, "m-second"), (10, "a-third")] {
            let again = format!("{group}-again");
            submit_party_group(&mut engine, "p", group, &[(group, 1 << 30), (&again, 1 << 30)], 0).await;
            std::fs::File::options()
                .write(true)
                .open(engine.config.state.join("groups").join(format!("{group}.json")))
                .unwrap()
                .set_modified(now - std::time::Duration::from_secs(age))
                .unwrap();
        }
        let config = engine.handoff_config();
        drop(engine);
        let mut restored = Engine::restore(config, Clock::system(), Store::default(), Instance::current().unwrap(), attribution()).await.unwrap().activated().await.unwrap();
        restored.read_memory = sample_alone();
        assert_eq!(restored.order, GROUPS, "a hand-over keeps submission order, not group-ID order");
        restored.tick(sample.clone()).await.unwrap();
        assert_eq!(running_ids(&restored), BTreeSet::from(["z-first".into(), "z-first-again".into()]));
        finish_fixture(restored, sample).await;
    }

    #[tokio::test]
    async fn the_aged_reservation_goes_to_the_partys_earliest_submitted_group() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.cores.truncate(2);
        submit_party_group(&mut engine, "other", "holding", &[("holding", 1 << 30)], 0).await;
        let job = engine.groups["holding"].jobs[0].clone();
        engine.launch_holding("holding".into(), job, sample.cores[0], engine.clock.monotonic_secs(), 0).await.unwrap();
        submit_party_group(&mut engine, "p", "z-first", &[("z-first", 1 << 30)], 0).await;
        submit_party_group(&mut engine, "p", "a-later", &[("a-later", 1 << 30)], 0).await;
        let now = crate::protocol::now_unix();
        for group in ["z-first", "a-later"] {
            engine.submitted.insert(group.into(), now - 60.0);
        }
        // The only slot is held; both groups are eligible and unserved for a minute, and both fit memory.
        engine.tick(sample.clone()).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert!(head(&engine).is_none(), "a job that waits only for a slot holds no head");
        sample.cores = admission_fixture_cores(3);
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("z-first"), "the earliest-submitted group takes the freed slot");
        assert!(!engine.running.contains_key("a-later"));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_reservation_already_held_keeps_its_job_when_an_earlier_group_becomes_ready() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 14 << 30;
        submit_party_group(&mut engine, "p", "z-first", &[("z-first", 3 << 30), ("z-first-again", 1 << 30)], 1).await;
        let job = engine.groups["z-first"].jobs[0].clone();
        engine.launch_holding("z-first".into(), job, sample.cores[0], engine.clock.monotonic_secs(), 0).await.unwrap();
        submit_party_group(&mut engine, "p", "a-later", &[("a-later", 2 << 30)], 0).await;
        let now = crate::protocol::now_unix();
        for group in ["z-first", "a-later"] {
            engine.submitted.insert(group.into(), now - 60.0);
        }
        engine.served.insert("p".into(), now - 60.0);
        // z-first is at its limit; a-later's job waits for the RAM z-first's running job declared.
        engine.tick(sample.clone()).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("a-later"));
        assert_eq!(running_ids(&engine), BTreeSet::from(["z-first".into()]));
        std::fs::write(engine.config.state.join("release"), b"ready").unwrap();
        for _ in 0..1000 {
            engine.tick(sample.clone()).await.unwrap();
            if engine.results.contains_key("z-first") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // In the tick that reaped z-first's job, the head kept its job and launched beside z-first's next job;
        // it takes no slot ahead of the shares, which within the party follow submission order.
        assert_eq!(engine.running["z-first-again"].record["cpu"], sample.cores[0], "{:?}", running_ids(&engine));
        assert_eq!(engine.running["a-later"].record["cpu"], sample.cores[1]);
        finish_fixture(engine, sample).await;
    }

    /// Publishes `party`'s group, published `age` seconds ago, whose jobs each run until the fixture's release
    /// file or their own, `release-<job>`, appears.
    async fn publish(engine: &mut Engine, submitter: impl Submitter, group: &str, jobs: &[(&str, u64)], age: f64, max_running: usize) {
        let jobs: Vec<(&str, u64, Option<u64>)> = jobs.iter().map(|&(id, memory)| (id, memory, None)).collect();
        publish_expecting(engine, submitter, group, &jobs, age, max_running).await;
    }
    /// Publishes as `publish` does, each job declaring its cell and, when given, its expected figure.
    async fn publish_expecting(engine: &mut Engine, submitter: impl Submitter, group: &str, jobs: &[(&str, u64, Option<u64>)], age: f64, max_running: usize) {
        let program = waiting()[0].clone();
        let code = "import pathlib,sys,time\nwhile not (pathlib.Path('release').exists() or pathlib.Path('release-' + sys.argv[1]).exists()): time.sleep(0.01)";
        let state = engine.config.state.clone();
        let jobs: Vec<_> = jobs
            .iter()
            .map(|(id, memory, expected)| {
                let mut job = json!({"job_id":id,"argv":[program,"-S","-c",code,id],"cwd":state,"memory_bytes":memory,"expected_runtime_secs":60});
                expected.iter().for_each(|expected| job["expected_memory_bytes"] = json!(expected));
                job
            })
            .collect();
        engine
            .requested(submitter.submitting(json!({"protocol":1,"op":"submit_group",
                "group":{"group_id":group,"jobs":jobs,"max_running":max_running}})))
            .await
            .unwrap();
        engine.submitted.insert(group.into(), crate::protocol::now_unix() - age);
    }
    /// Starts `job` at once on a free core; it uses `rss` bytes.
    async fn start(engine: &mut Engine, sample: &mut Snapshot, job: &str, rss: u64) {
        let now = engine.clock.monotonic_secs();
        start_at(engine, sample, job, rss, now).await;
    }
    /// Starts `job` at once on a free core, recorded as started at `started` on the contract clock; it uses `rss`
    /// bytes.
    async fn start_at(engine: &mut Engine, sample: &mut Snapshot, job: &str, rss: u64, started: f64) {
        start_holding(engine, sample, job, rss, started, 0).await;
    }
    /// Starts `job` at once on a free core, holding `lending` of its cell as lending, recorded as started at
    /// `started` on the contract clock; it uses `rss` bytes.
    async fn start_holding(engine: &mut Engine, sample: &mut Snapshot, job: &str, rss: u64, started: f64, lending: u64) {
        let group = engine.progress.owner(job).unwrap().to_owned();
        let spec = engine.groups[&group].jobs.iter().find(|j| j.job_id == job).unwrap().clone();
        let used: BTreeSet<u64> = engine.running.values().filter_map(|r| r.record["cpu"].as_u64()).collect();
        let cpu = *sample.cores.iter().find(|c| !used.contains(&(**c as u64))).unwrap();
        engine.launch_holding(group, spec, cpu, started, lending).await.unwrap();
        let pid = engine.running[job].record["pid"].as_u64().unwrap() as u32;
        sample.processes.insert(pid, (std::process::id(), rss));
    }
    /// Ends a running job; the next tick reaps it.
    fn end(engine: &mut Engine, job: &str) {
        ender(engine, job)();
    }
    /// What ends a running job, for use while the engine is busy.
    fn ender(engine: &Engine, job: &str) -> impl FnOnce() {
        let pid = engine.running[job].record["pid"].as_u64().unwrap() as u32;
        let release = engine.config.state.join(format!("release-{job}"));
        let job = job.to_owned();
        move || {
            std::fs::write(release, b"").unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while crate::procs::alive(pid) {
                assert!(std::time::Instant::now() < deadline, "{job} did not end");
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
    }
    fn head(engine: &Engine) -> Option<String> {
        engine.host_memory.head().map(|head| head.id.to_string())
    }
    /// Every request that held the head in a tick so far.
    fn heads_seen(engine: &Engine) -> BTreeSet<String> {
        engine
            .journal
            .history
            .iter()
            .filter_map(|(record, _)| record.aged.head.as_ref().map(|head| head.id.to_string()))
            .collect()
    }
    /// Every tick's record derives as one journal, and the written ones as the journal the service wrote.
    fn replays(engine: &Engine) {
        decision::replays(&engine.journal).unwrap_or_else(|error| panic!("{error}"));
    }
    /// The check of every tick's record with the record of `replacement`'s tick replaced by it, as a defective
    /// service would have written it.
    fn checked_with(engine: &Engine, replacement: Record) -> Result<(), String> {
        let records: Vec<Record> = engine
            .journal
            .history
            .iter()
            .map(|(record, _)| match record.tick == replacement.tick {
                true => replacement.clone(),
                false => record.clone(),
            })
            .collect();
        decision::check(&records)
    }

    /// Writes a fixture's file as an earlier service wrote it, through a store handle of its own.
    async fn wrote(path: &std::path::Path, value: &Value) {
        Store::default().write(Operation::new("fixture", path.display()), path, value).await.unwrap();
    }

    fn admission_fixture_cores(count: usize) -> Vec<usize> {
        let mut affinity: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        assert_eq!(
            unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&affinity), &mut affinity) },
            0
        );
        (0..libc::CPU_SETSIZE as usize)
            .filter(|&c| unsafe { libc::CPU_ISSET(c, &affinity) })
            .take(count)
            .collect()
    }

    #[tokio::test]
    async fn health_names_its_capabilities_and_keeps_protocol_one_for_gen_16_clients() {
        let (mut engine, sample) = admission_fixture().await;
        let health = engine
            .requested(json!({"protocol":1,"op":"health"}))
            .await
            .unwrap();
        assert_eq!(health["protocol"], 1);
        assert_eq!(health["host_memory"]["protocol"], 1);
        assert_eq!(
            health["capabilities"],
            json!(["peaks", "purpose", "transfer", "preparation_wait", "result_sink", "expected_memory"])
        );
        finish_fixture(engine, sample).await;
    }

    /// Q-T2: status reads the budget the tick's record holds, from the same terms.
    #[tokio::test]
    async fn status_and_the_record_read_one_budget() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 20 * GIB;
        let claimant = Claimant::new();
        engine.requested(claimant.claim("held", 2 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "held")["state"], "granted");
        admit_group(&mut engine, "running", 4 * GIB, 0.0).await;
        start(&mut engine, &mut sample, "running", GIB).await;
        engine.tick(sample.clone()).await.unwrap();
        let record = last_record(&engine);
        let memory = engine.admission().unwrap().memory;
        assert_eq!(memory, record.budget().floor(), "status and the record read one budget");
        assert_eq!(memory, 20 * GIB - 3 * GIB - 10 * GIB - 2 * GIB);
        replays(&engine);
        drop(claimant);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn equal_holdings_give_the_slot_to_the_party_whose_turn_is_oldest() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.cores.truncate(2);
        sample.available = 64 << 30;
        admit_group(&mut engine, "recent", 1 << 30, 0.0).await;
        admit_group(&mut engine, "waiting", 1 << 30, 0.0).await;
        let now = crate::protocol::now_unix();
        engine.served.insert("recent".into(), now - 10.0);
        engine.served.insert("waiting".into(), now - 100.0);
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("waiting"));
        assert!(!engine.running.contains_key("recent"));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_party_the_service_forgot_waits_from_its_oldest_unfinished_publication_so_a_fitting_head_launches() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.cores.truncate(2);
        let owner = Claimant::new();
        let held = Claimant::new();
        submit_party_group(&mut engine, &owner, "live", &[], 0).await;
        // B's compile runs; C's completed group is all C has left.
        publish(&mut engine, "c", "c0", &[("c0", GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "c0", 0).await;
        end(&mut engine, "c0");
        engine.tick(sample.clone()).await.unwrap();
        publish(&mut engine, "b", "b0", &[("b0", GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "b0", 0).await;
        engine.served.insert("a".into(), crate::protocol::now_unix() - 1000.0);
        // 8 GiB admissible beside the running compile, but a claim holds 4 of them while A's head forms.
        sample.available = 19 * GIB;
        engine.requested(held.host_only("held", 4 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        publish(&mut engine, "a", "head", &[("head", 5 * GIB)], 120.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("head"));
        engine.requested(held.release("held")).await.unwrap();
        engine.requested(owner.host_only("younger", 5 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.host_memory.head().is_some_and(|head| head.fits), "a fitting head, reserving");
        assert_eq!(claim_status(&engine, "younger")["state"], "pending");
        // While B's compile runs, C's completed group expires, and only then does C publish its next group.
        let expired = engine.completed["c0"] + RETENTION;
        engine.retire(expired).await.unwrap();
        assert!(!engine.served.contains_key("c"), "C is forgotten");
        publish(&mut engine, "c", "c1", &[("c1", GIB)], 0.0, 0).await;
        end(&mut engine, "b0");
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("head"), "C waits from its group's publication, later than A's last launch");
        assert!(!engine.running.contains_key("c1"));
        end(&mut engine, "head");
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "younger")["state"], "granted", "the claim is granted once the head's compile ends");
        replays(&engine);
        drop((owner, held));
        finish_fixture(engine, sample).await;
    }

    const GIB: u64 = 1 << 30;
    const LARGE_WAIT: &str =
        "declares more than 10 GB: served only from memory no smaller request needs; never reserves memory";

    /// A process that owns a claim, in the unit it names: its own, or another claimant's.
    struct Claimant(std::process::Child, String);
    impl Claimant {
        fn new() -> Self {
            let owner = std::process::Command::new("sleep").arg("60").spawn().unwrap();
            let unit = format!("/user.slice/test.slice/claimant-{}.scope", owner.id());
            Self::placed(owner, unit)
        }
        /// A claimant in `party`'s unit.
        fn beside(party: &Claimant) -> Self {
            Self::placed(std::process::Command::new("sleep").arg("60").spawn().unwrap(), party.1.clone())
        }
        fn placed(owner: std::process::Child, unit: String) -> Self {
            units().place(owner.id(), crate::procs::started(owner.id()).unwrap(), &unit);
            Self(owner, unit)
        }
        fn party(&self) -> String {
            self.1.clone()
        }
        fn claim(&self, id: &str, bytes: u64) -> Asked {
            (json!({"protocol":1,"op":"host_memory_acquire","claim_id":id,"memory_bytes":bytes,"label":id}), Peer::opened(self.0.id()))
        }
        fn host_only(&self, id: &str, bytes: u64) -> Asked {
            let mut claim = self.claim(id, bytes);
            claim.0["purpose"] = json!("host_only");
            claim
        }
        fn gpu_place(&self, id: &str, bytes: u64) -> Asked {
            let mut claim = self.claim(id, bytes);
            claim.0["purpose"] = json!("gpu_place");
            claim
        }
        fn release(&self, id: &str) -> Asked {
            (json!({"protocol":1,"op":"host_memory_release","claim_id":id}), Peer::opened(self.0.id()))
        }
    }
    impl Drop for Claimant {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    /// The request of `id`'s claim: its registration, its order as health shows it.
    fn request_of(engine: &Engine, id: &str) -> RequestId {
        RequestId::Claim(precedence::Registration { claim: id.into(), order: claim_status(engine, id)["order"].as_u64().unwrap() })
    }
    fn claim_status(engine: &Engine, id: &str) -> Value {
        engine.host_memory.health(None)["claims"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["claim_id"] == id)
            .unwrap()
            .clone()
    }
    fn reason(engine: &Engine, group: &str, job: usize) -> String {
        let group = &engine.groups[group];
        engine.waiting_reason(engine.admission().as_ref(), group, &group.jobs[job])
    }
    fn cores(sample: &mut Snapshot, count: usize) {
        sample.cores = admission_fixture_cores(count);
        sample.idle = count;
    }

    #[tokio::test]
    async fn an_aged_large_job_waits_while_small_jobs_start_and_never_holds_the_reservation() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 34 * GIB;
        admit_group(&mut engine, "large", 22 * GIB, 120.0).await;
        admit_group(&mut engine, "small-0", 2 * GIB, 60.0).await;
        admit_group(&mut engine, "small-1", 2 * GIB, 50.0).await;
        // 24 GiB admissible: the aged 22 GiB job would fit if it went first.
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("small-0") && engine.running.contains_key("small-1"));
        assert!(!engine.running.contains_key("large"));
        assert_eq!(reason(&engine, "large", 0), LARGE_WAIT);
        for _ in 0..3 {
            engine.tick(sample.clone()).await.unwrap();
            assert!(!engine.running.contains_key("large"), "20 GiB are left after the small jobs");
            assert!(head(&engine).is_none(), "a large job is never a candidate");
        }
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_small_claim_is_granted_before_a_large_one_of_a_party_holding_less() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 23 * GIB;
        let large = Claimant::new();
        let small = Claimant::new();
        // The small claim's party holds a running compile; the large claim's party holds nothing.
        submit_party_group(&mut engine, &large, "large-party", &[], 0).await;
        submit_party_group(&mut engine, &small, "small-party", &[("held", GIB)], 0).await;
        let job = engine.groups["small-party"].jobs[0].clone();
        engine.launch_holding("small-party".into(), job, sample.cores[0], engine.clock.monotonic_secs(), 0).await.unwrap();
        let pid = engine.running["held"].record["pid"].as_u64().unwrap() as u32;
        sample.processes.insert(pid, (std::process::id(), GIB));
        engine.requested(large.claim("large", 12 * GIB)).await.unwrap();
        engine.requested(small.claim("small", 2 * GIB)).await.unwrap();
        // 13 GiB admissible: each claim fits alone, not both.
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "small")["state"], "granted");
        let status = claim_status(&engine, "large");
        assert_eq!(status["state"], "pending");
        assert_eq!(status["waiting"], LARGE_WAIT);
        drop((large, small));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_pending_small_claim_is_granted_before_its_partys_large_compile_turn() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 32 * GIB;
        let owner = Claimant::new();
        submit_party_group(&mut engine, &owner, "large", &[("large", 22 * GIB)], 0).await;
        engine.requested(owner.claim("small", 2 * GIB)).await.unwrap();
        // 22 GiB admissible, one party: today its compile turn would take them all first.
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "small")["state"], "granted");
        assert!(!engine.running.contains_key("large"));
        assert_eq!(reason(&engine, "large", 0), LARGE_WAIT);
        drop(owner);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn an_ordinary_compile_takes_its_turn_before_a_large_claim_of_a_party_holding_less() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 23 * GIB;
        let owner = Claimant::new();
        submit_party_group(&mut engine, &owner, "claimant", &[], 0).await;
        submit_party_group(&mut engine, "compiles", "held", &[("held", GIB)], 0).await;
        let job = engine.groups["held"].jobs[0].clone();
        engine.launch_holding("held".into(), job, sample.cores[0], engine.clock.monotonic_secs(), 0).await.unwrap();
        let pid = engine.running["held"].record["pid"].as_u64().unwrap() as u32;
        sample.processes.insert(pid, (std::process::id(), GIB));
        submit_party_group(&mut engine, "compiles", "next", &[("next", 2 * GIB)], 0).await;
        engine.requested(owner.claim("large", 12 * GIB)).await.unwrap();
        // 13 GiB admissible; the claim's party holds nothing, the compiling party one compile.
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("next"));
        let large = claim_status(&engine, "large");
        assert_eq!(large["state"], "pending", "11 GiB are left after the compile");
        assert_eq!(large["waiting"], LARGE_WAIT);
        drop(owner);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_party_whose_first_ready_job_is_large_asks_an_ordinary_turn_for_its_next() {
        let (mut engine, sample) = admission_fixture().await;
        submit_party_group(&mut engine, "one-group", "both", &[("both-large", 22 * GIB), ("both-small", 2 * GIB)], 0).await;
        submit_party_group(&mut engine, "two-groups", "first", &[("first", 22 * GIB)], 0).await;
        submit_party_group(&mut engine, "two-groups", "second", &[("second", 2 * GIB)], 0).await;
        engine.sample = Some(sample.clone());
        let mut turns = engine.compiler_turns(&sample);
        let mut asked = BTreeMap::new();
        while let Some((party, turn)) = turns.next() {
            asked.insert(party, turn);
        }
        assert!(asked["one-group"] == Turn::Compiler(2 * GIB, 0), "the large job asks no turn");
        assert!(asked["two-groups"] == Turn::Compiler(2 * GIB, 0));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn ordinary_jobs_launched_in_a_tick_are_not_offered_to_a_large_claim() {
        let (mut engine, mut sample) = admission_fixture().await;
        cores(&mut sample, 6);
        sample.available = 32 * GIB;
        let owner = Claimant::new();
        let jobs = [("j0", 2 * GIB), ("j1", 2 * GIB), ("j2", 2 * GIB)];
        submit_party_group(&mut engine, &owner, "three", &jobs, 0).await;
        engine.requested(owner.claim("large", 20 * GIB)).await.unwrap();
        // 22 GiB admissible: one party's turn protects one job, yet all three launch first.
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(running_ids(&engine), BTreeSet::from(["j0".into(), "j1".into(), "j2".into()]));
        let large = claim_status(&engine, "large");
        assert_eq!(large["state"], "pending", "it is offered only the 16 GiB the launches left");
        assert_eq!(large["waiting"], LARGE_WAIT);
        drop(owner);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_large_compile_is_launched_only_from_what_ordinary_claims_left() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 30 * GIB;
        let owner = Claimant::new();
        submit_party_group(&mut engine, "large", "large", &[("large", 12 * GIB)], 0).await;
        engine.requested(owner.claim("ordinary", 9 * GIB)).await.unwrap();
        // 20 GiB admissible and free slots: the 9 GiB claim leaves 11 GiB, too little for the large job.
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "ordinary")["state"], "granted");
        assert!(!engine.running.contains_key("large"));
        assert_eq!(reason(&engine, "large", 0), LARGE_WAIT);
        drop(owner);
        finish_fixture(engine, sample).await;
    }

    /// Seven eligible cores, two of them idle, and memory for every job: the tick counts its slots once, so the
    /// ordinary launches leave the large phase only the slots they did not take.
    #[tokio::test]
    async fn the_large_launches_take_only_the_slots_the_ordinary_launches_left() {
        let (engine, sample) = slot_count_tick(3).await;
        assert_eq!(running_ids(&engine), ["o0", "o1"].map(String::from).into());
        for group in ["o2", "large"] {
            assert_eq!(reason(&engine, group, 0), "ready for fair admission", "only the tick's slots kept {group}");
        }
        assert_eq!(launches_against_slots(&engine), (2, 2), "the record's slot count and its launches");
        replays(&engine);
        finish_fixture(engine, sample).await;

        let (engine, sample) = slot_count_tick(1).await;
        assert_eq!(running_ids(&engine), ["o0", "large"].map(String::from).into(), "the large job takes the slot left");
        assert_eq!(launches_against_slots(&engine), (2, 2));
        replays(&engine);
        finish_fixture(engine, sample).await;
    }
    /// The last tick's launches and its recorded slot count.
    fn launches_against_slots(engine: &Engine) -> (usize, usize) {
        let record = &engine.journal.history.last().unwrap().0;
        let launches = record.admitted.iter().filter(|entry| matches!(entry, Entry::Launched { .. })).count();
        (launches, record.slots)
    }
    /// One tick with `ordinary` ready 1 GiB jobs and a ready 12 GiB job, each its own party's.
    async fn slot_count_tick(ordinary: usize) -> (Engine, Snapshot) {
        let (mut engine, mut sample) = admission_fixture().await;
        cores(&mut sample, 8);
        sample.idle = 2;
        sample.available = 40 * GIB;
        for index in 0..ordinary {
            let job = format!("o{index}");
            submit_party_group(&mut engine, &format!("party-{job}"), &job, &[(&job, GIB)], 0).await;
        }
        submit_party_group(&mut engine, "party-large", "large", &[("large", 12 * GIB)], 0).await;
        engine.tick(sample.clone()).await.unwrap();
        (engine, sample)
    }

    // Part B, B-2: the aged head.

    #[tokio::test]
    async fn a_claim_its_partys_compiles_pass_over_becomes_the_aged_head_and_is_granted_at_the_next_completion() {
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new();
        publish(&mut engine, &owner, "running", &[("r0", 3 * GIB), ("r1", 3 * GIB)], 0.0, 0).await;
        let queued: Vec<String> = (0..6).map(|i| format!("q{i}")).collect();
        let queue: Vec<(&str, u64)> = queued.iter().map(|id| (id.as_str(), 3 * GIB)).collect();
        publish(&mut engine, &owner, "queue", &queue, 0.0, 0).await;
        start(&mut engine, &mut sample, "r0", 0).await;
        start(&mut engine, &mut sample, "r1", 0).await;
        // 1 GiB admissible beside two running 3 GiB compiles; each completion frees 3 GiB.
        sample.available = 17 * GIB;
        engine.requested(owner.host_only("discovery", 5 * GIB / 4)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        let mut running = vec!["r0".to_owned(), "r1".to_owned()];
        let complete = |engine: &mut Engine, running: &mut Vec<String>| {
            end(engine, &running.remove(0));
        };
        for _ in 0..2 {
            complete(&mut engine, &mut running);
            engine.tick(sample.clone()).await.unwrap();
            running.extend(engine.running.keys().filter(|id| !running.contains(id)).cloned().collect::<Vec<_>>());
            assert_eq!(engine.running.len(), 2, "before AGED_SECS the party's compiles take every completion");
            let claim = claim_status(&engine, "discovery");
            assert_eq!(claim["state"], "pending");
            assert!(claim["waiting"].as_str().unwrap().starts_with("waiting for RAM: needs"), "{claim}");
        }
        engine.host_memory.backdate("discovery", 31.0);
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("discovery"));
        let claim = claim_status(&engine, "discovery");
        assert_eq!(claim["waiting"], format!("aged head, party {}'s turn: {} bytes reserved", owner.party(), 5 * GIB / 4));
        complete(&mut engine, &mut running);
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "discovery")["state"], "granted", "granted at the next completion");
        assert_eq!(engine.running.len(), 1, "the completion went to the claim");
        replays(&engine);
        drop(owner);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn an_aged_claim_holds_the_head_against_its_partys_younger_claims_and_is_granted() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 14 * GIB;
        let lead = Claimant::new();
        submit_party_group(&mut engine, &lead, "live", &[], 0).await;
        let mut small: Vec<Claimant> = (0..8).map(|_| Claimant::beside(&lead)).collect();
        for (i, owner) in small.iter().take(4).enumerate() {
            engine.requested(owner.host_only(&format!("s{i}"), GIB)).await.unwrap();
        }
        engine.tick(sample.clone()).await.unwrap();
        engine.requested(lead.host_only("big", 4 * GIB)).await.unwrap();
        // Each 1 GiB release goes to the next 1 GiB claim while the 4 GiB claim is young.
        for i in 0..2 {
            engine.requested(small[4 + i].host_only(&format!("s{}", 4 + i), GIB)).await.unwrap();
            engine.requested(small[i].release(&format!("s{i}"))).await.unwrap();
            engine.tick(sample.clone()).await.unwrap();
            assert_eq!(claim_status(&engine, &format!("s{}", 4 + i))["state"], "granted");
            assert_eq!(claim_status(&engine, "big")["state"], "pending");
        }
        engine.requested(small[6].host_only("s6", GIB)).await.unwrap();
        engine.host_memory.backdate("big", 31.0);
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("big"), "S is 4 GiB: the granted claims' declarations");
        for i in 2..6 {
            engine.requested(small[i].release(&format!("s{i}"))).await.unwrap();
            engine.tick(sample.clone()).await.unwrap();
            assert_eq!(claim_status(&engine, "s6")["state"], "pending", "no younger claim is granted from its bytes");
        }
        assert_eq!(claim_status(&engine, "big")["state"], "granted");
        replays(&engine);
        small.clear();
        drop(lead);
        finish_fixture(engine, sample).await;
    }

    /// A 5 GiB job waits for memory while its party's smaller jobs keep launching; `large` puts an earlier
    /// 22 GB job of that party before it, in another group or in its own.
    async fn an_aged_job_holds_the_head_against_its_partys_smaller_jobs(large: &str) {
        let (mut engine, mut sample) = admission_fixture().await;
        cores(&mut sample, 6);
        let first: Vec<(&str, u64)> = match large {
            "group" => {
                publish(&mut engine, "p", "huge", &[("huge", 22 * GIB)], 200.0, 0).await;
                vec![("big", 5 * GIB)]
            }
            "job" => vec![("huge", 22 * GIB), ("big", 5 * GIB)],
            _ => vec![("big", 5 * GIB)],
        };
        publish(&mut engine, "p", "first", &first, 0.0, 0).await;
        let small: Vec<String> = (0..10).map(|i| format!("s{i}")).collect();
        let jobs: Vec<(&str, u64)> = small.iter().map(|id| (id.as_str(), GIB)).collect();
        publish(&mut engine, "p", "small", &jobs, 0.0, 0).await;
        for id in &small[..3] {
            start(&mut engine, &mut sample, id, 0).await;
        }
        // 2 GiB admissible beside three running 1 GiB jobs: S is 5 GiB.
        sample.available = 15 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        end(&mut engine, "s0");
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("s3"), "while the 5 GiB job is young, a smaller one takes the completion");
        assert!(head(&engine).is_none(), "a young job holds no head");
        *engine.submitted.get_mut("first").unwrap() -= 121.0;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("big"));
        assert!(reason(&engine, "first", first.len() - 1).contains("concurrency capacity"), "its party is at its share");
        for id in ["s1", "s2", "s3"] {
            end(&mut engine, id);
            engine.tick(sample.clone()).await.unwrap();
            assert!(!engine.running.contains_key("s4"), "no smaller job launches from its bytes");
            if id == "s1" {
                assert!(reason(&engine, "first", first.len() - 1).contains("holding the aged compiler memory reservation"));
            }
        }
        assert!(engine.running.contains_key("big"));
        assert!(!heads_seen(&engine).contains("huge"), "the 22 GB job is never a candidate");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }
    #[tokio::test]
    async fn an_aged_job_holds_the_head_against_its_partys_smaller_jobs_and_starts() {
        an_aged_job_holds_the_head_against_its_partys_smaller_jobs("none").await;
    }
    #[tokio::test]
    async fn an_earlier_large_job_in_another_group_never_hides_the_aged_job() {
        an_aged_job_holds_the_head_against_its_partys_smaller_jobs("group").await;
    }
    #[tokio::test]
    async fn an_earlier_large_job_in_its_own_group_never_hides_the_aged_job() {
        an_aged_job_holds_the_head_against_its_partys_smaller_jobs("job").await;
    }

    #[tokio::test]
    async fn an_aged_compile_head_is_not_overtaken_by_a_stream_of_another_partys_claims() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 20 * GIB;
        let first = Claimant::new();
        let claims: Vec<Claimant> = (0..3).map(|_| Claimant::beside(&first)).collect();
        submit_party_group(&mut engine, &first, "claims", &[], 0).await;
        publish(&mut engine, "c", "running", &[("held", GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "held", 0).await;
        engine.requested(first.host_only("x0", 6 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "x0")["state"], "granted");
        publish(&mut engine, "c", "compile", &[("compile", 5 * GIB)], 120.0, 0).await;
        engine.requested(claims[0].host_only("x1", 6 * GIB)).await.unwrap();
        // 3 GiB left; S is 10 GiB. The compiling party holds more than the claiming one.
        engine.tick(sample.clone()).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("compile"));
        engine.requested(first.release("x0")).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("compile"), "it starts at the next release");
        assert_eq!(claim_status(&engine, "x1")["state"], "pending", "no claim is granted from its bytes");
        replays(&engine);
        drop((first, claims));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn an_unserviceable_head_is_released_and_its_party_keeps_the_turn_with_its_next_candidate() {
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new();
        let younger = Claimant::beside(&owner);
        submit_party_group(&mut engine, &owner, "live", &[], 0).await;
        publish(&mut engine, "q", "running", &[("w", 2 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        engine.requested(owner.host_only("older", 4 * GIB)).await.unwrap();
        engine.requested(younger.host_only("younger", 3 * GIB)).await.unwrap();
        engine.host_memory.backdate("older", 60.0);
        engine.host_memory.backdate("younger", 40.0);
        // 2.5 GiB left and S 4.5 GiB: both wait, the older one holds the head.
        sample.available = 29 * GIB / 2;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("older"));
        // Memory held elsewhere lowers S to 3.5 GiB: the older one cannot reserve; its party keeps the turn.
        sample.available = 27 * GIB / 2;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("younger"));
        assert!(claim_status(&engine, "older")["waiting"].as_str().unwrap().starts_with("waiting for RAM: needs"));
        sample.available = 16 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "younger")["state"], "granted");
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("older"), "the party's oldest candidate at its next turn");
        assert_eq!(claim_status(&engine, "older")["pending_seconds"].as_f64().unwrap() >= 60.0, true, "it kept its age");
        replays(&engine);
        drop((owner, younger));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn an_older_job_that_cannot_be_served_does_not_hide_a_younger_one_of_its_group() {
        let (mut engine, mut sample) = admission_fixture().await;
        publish(&mut engine, "q", "running", &[("w", 2 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        // 1.5 GiB left and S 3.5 GiB.
        sample.available = 27 * GIB / 2;
        engine.tick(sample.clone()).await.unwrap();
        publish(&mut engine, "p", "group", &[("older", 4 * GIB), ("younger", 2 * GIB)], 120.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("younger"));
        assert!(reason(&engine, "group", 1).contains("holding the aged compiler memory reservation"));
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn heads_rotate_over_parties_and_a_partys_younger_grants_keep_its_place() {
        let (mut engine, mut sample) = admission_fixture().await;
        let a = Claimant::new();
        let b = Claimant::new();
        let c = Claimant::new();
        let mut owners = vec![];
        let names: BTreeMap<String, &str> = [(a.party(), "a"), (b.party(), "b"), (c.party(), "c")].into_iter().collect();
        for (lead, name) in [(&a, "a"), (&b, "b"), (&c, "c")] {
            submit_party_group(&mut engine, lead, &format!("live-{name}"), &[], 0).await;
            for i in 0..2 {
                let owner = Claimant::beside(lead);
                let id = format!("{name}{i}");
                engine.requested(owner.host_only(&id, 3 * GIB)).await.unwrap();
                let age = match (name, i) { ("a", 0) => 100.0, (_, 0) => 90.0, _ => 80.0 };
                engine.host_memory.backdate(&id, age);
                owners.push(owner);
            }
        }
        publish(&mut engine, "q", "running", &[("w", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        let little = Claimant::beside(&a);
        let mut order = Vec::new();
        for step in 0..4 {
            // 1 GiB left: the head waits; then enough memory for it.
            let granted = engine.host_memory.reserved(&sample);
            sample.available = GIB + 10 * GIB + 4 * GIB + granted;
            engine.tick(sample.clone()).await.unwrap();
            let id = head(&engine).unwrap();
            order.push(names[&engine.host_memory.head().unwrap().party].to_owned());
            if step == 1 {
                engine.requested(little.host_only("little", GIB / 2)).await.unwrap();
            }
            sample.available += 3 * GIB + GIB / 2;
            engine.tick(sample.clone()).await.unwrap();
            assert_eq!(claim_status(&engine, &id)["state"], "granted");
        }
        assert_eq!(claim_status(&engine, "little")["state"], "granted", "a younger grant of party a in b's turn");
        assert_eq!(order, ["a", "b", "c", "a"]);
        replays(&engine);
        drop((a, b, c, little, owners));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn journals_merged_from_two_engine_instances_replay() {
        let (mut first, mut first_sample) = admission_fixture().await;
        let (mut second, mut second_sample) = admission_fixture().await;
        // Two engines of one test process: the second is given an instance of its own, as a second service process
        // has.
        second.instance = Instance { start_ticks: first.instance.start_ticks + 1, ..first.instance.clone() };
        second.journal = Journal::new(second.instance.clone());
        let a = Claimant::new();
        let b = Claimant::new();
        for (engine, owner, id) in [(&mut first, &a, "a"), (&mut second, &b, "b")] {
            engine.requested(owner.host_only(id, 3 * GIB)).await.unwrap();
            engine.host_memory.backdate(id, 60.0);
        }
        // Ticks 1 and 2: 1 GiB left, neither aged claim is serviceable.
        for (engine, sample) in [(&mut first, &mut first_sample), (&mut second, &mut second_sample)] {
            sample.available = 11 * GIB;
            engine.tick(sample.clone()).await.unwrap();
            engine.tick(sample.clone()).await.unwrap();
        }
        // Tick 3: the first engine has 4 GiB and grants a as its head at once, which passes a's party's turn.
        first_sample.available = 14 * GIB;
        first.tick(first_sample.clone()).await.unwrap();
        second.tick(second_sample.clone()).await.unwrap();
        assert_eq!(claim_status(&first, "a")["state"], "granted");
        // Tick 4: the second engine grants an unaged 1 GiB claim from 2 GiB, with no turn yet.
        let b2 = Claimant::beside(&b);
        second.requested(b2.host_only("b2", GIB)).await.unwrap();
        second_sample.available = 12 * GIB;
        second.tick(second_sample.clone()).await.unwrap();
        assert_eq!(claim_status(&second, "b2")["state"], "granted");
        replays(&first);
        replays(&second);
        let written = |engine: &Engine| -> Vec<Record> {
            engine.journal.history.iter().filter(|(_, written)| *written).map(|(record, _)| record.clone()).collect()
        };
        let (first_written, second_written) = (written(&first), written(&second));
        let ticks = |records: &[Record]| records.iter().map(|record| record.tick).collect::<Vec<_>>();
        assert_eq!((ticks(&first_written), ticks(&second_written)), (vec![1, 3], vec![1, 4]));
        // Merged in tick order, the first engine's tick 3 and the second's tick 4 are adjacent counters.
        let merged = vec![first_written[0].clone(), second_written[0].clone(), first_written[1].clone(), second_written[1].clone()];
        decision::check(&merged).unwrap();
        decision::check(&[first_written.clone(), second_written.clone()].concat()).unwrap();
        // Written by one instance, the second engine's tick 4 would start from the turn the first's tick 3 passed.
        let one: Vec<Record> = merged.into_iter().map(|record| Record { instance: first.instance.clone(), ..record }).collect();
        let error = decision::check(&one).unwrap_err();
        assert!(error.contains("tick 1 follows tick 1 of its instance"), "{error}");
        let adjacent = vec![first_written[0].clone(), first_written[1].clone(), Record { instance: first.instance.clone(), ..second_written[1].clone() }];
        let error = decision::check(&adjacent).unwrap_err();
        assert!(error.contains("tick 4: the record's /aged/after is null"), "{error}");
        drop((a, b, b2));
        finish_fixture(first, first_sample).await;
        finish_fixture(second, second_sample).await;
    }

    /// A job of `party`'s group that declares `runtime` seconds and runs until its release file appears.
    async fn publish_timed(engine: &mut Engine, submitter: impl Submitter, group: &str, jobs: &[(&str, u64, u64)], age: f64, max_running: usize) {
        let program = waiting()[0].clone();
        let code = "import pathlib,sys,time\nwhile not (pathlib.Path('release').exists() or pathlib.Path('release-' + sys.argv[1]).exists()): time.sleep(0.01)";
        let state = engine.config.state.clone();
        let jobs: Vec<_> = jobs
            .iter()
            .map(|(id, memory, runtime)| json!({"job_id":id,"argv":[program,"-S","-c",code,id],"cwd":state,"memory_bytes":memory,"expected_runtime_secs":runtime}))
            .collect();
        engine
            .requested(submitter.submitting(json!({"protocol":1,"op":"submit_group",
                "group":{"group_id":group,"jobs":jobs,"max_running":max_running}})))
            .await
            .unwrap();
        engine.submitted.insert(group.into(), crate::protocol::now_unix() - age);
    }
    fn running_of(engine: &Engine, party: &str) -> usize {
        engine.party_running(party)
    }

    #[tokio::test]
    async fn jobs_that_wait_only_for_a_slot_form_no_head_and_slots_go_by_share() {
        let (mut engine, mut sample) = admission_fixture().await;
        cores(&mut sample, 21);
        sample.available = 64 * GIB;
        for (party, running) in [("many", 10), ("few", 2)] {
            let ids: Vec<String> = (0..running + 10).map(|i| format!("{party}-{i}")).collect();
            let jobs: Vec<(&str, u64)> = ids.iter().map(|id| (id.as_str(), GIB)).collect();
            publish(&mut engine, party, party, &jobs, 120.0, 0).await;
            for id in &ids[..running] {
                start(&mut engine, &mut sample, id, 0).await;
            }
        }
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!((running_of(&engine, "many"), running_of(&engine, "few")), (10, 10), "the next 8 slots go to the party with 2");
        engine.tick(sample.clone()).await.unwrap();
        assert!(heads_seen(&engine).is_empty(), "backlogs that fit memory form no head");
        assert_eq!(reason(&engine, "many", 10), "waiting for CPU capacity; one core stays free of compilations");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// The head of a group with `max_running = 1`, or of a party at its core share (`share`), waits for memory
    /// while younger 1 GiB jobs of its party run ahead; it stays the head while they hold its limit.
    async fn the_head_stays_while_its_limit_is_held(share: bool) {
        let (mut engine, mut sample) = admission_fixture().await;
        publish_timed(&mut engine, "q", "running", &[("r", 4 * GIB, 60)], 0.0, 0).await;
        start(&mut engine, &mut sample, "r", 0).await;
        // 4 GiB left beside the running compile; S is 8 GiB.
        sample.available = 18 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        let younger = [("y1", GIB, 10), ("y2", GIB, 10)];
        match share {
            false => {
                let jobs = [("h", 5 * GIB, 60), younger[0], younger[1]];
                publish_timed(&mut engine, "p", "group", &jobs, 120.0, 1).await;
            }
            true => {
                publish_timed(&mut engine, "p", "group", &[("h", 5 * GIB, 60)], 120.0, 0).await;
                publish_timed(&mut engine, "p", "younger", &younger, 120.0, 0).await;
            }
        }
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("h"));
        let limit = match share {
            false => BTreeSet::from(["y1".to_owned()]),
            true => BTreeSet::from(["y1".to_owned(), "y2".to_owned()]),
        };
        assert_eq!(running_ids(&engine), &limit | &BTreeSet::from(["r".to_owned()]), "the younger jobs run ahead into its bytes");
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("h"), "it stays the head while its limit is held");
        // The running compile ends: the head fits, and a short job of another party may not run ahead.
        publish_timed(&mut engine, "z", "other", &[("z", 3 * GIB, 10)], 0.0, 0).await;
        end(&mut engine, "r");
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("h"));
        assert!(engine.host_memory.head().unwrap().fits);
        assert!(!engine.running.contains_key("z") && !engine.running.contains_key("h"));
        assert!(reason(&engine, "group", 0).contains("concurrency capacity"));
        end(&mut engine, "y1");
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("h"), "it launches once the unit frees");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }
    #[tokio::test]
    async fn the_head_stays_while_younger_jobs_of_its_group_hold_the_groups_limit() {
        the_head_stays_while_its_limit_is_held(false).await;
    }
    #[tokio::test]
    async fn the_head_stays_while_younger_jobs_of_its_party_hold_the_partys_core_share() {
        the_head_stays_while_its_limit_is_held(true).await;
    }

    #[tokio::test]
    async fn while_the_head_does_not_fit_only_jobs_finishing_by_the_deadline_run_ahead_and_once_it_fits_none() {
        let (mut engine, mut sample) = admission_fixture().await;
        cores(&mut sample, 6);
        publish_timed(&mut engine, "q", "running", &[("r", 4 * GIB, 60)], 0.0, 0).await;
        start(&mut engine, &mut sample, "r", 0).await;
        sample.available = 18 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        publish_timed(&mut engine, "p", "head", &[("h", 5 * GIB, 60)], 120.0, 1).await;
        publish_timed(&mut engine, "p", "head-again", &[("h2", 5 * GIB, 60)], 0.0, 0).await;
        publish_timed(&mut engine, "s", "short", &[("short", GIB, 10)], 0.0, 0).await;
        publish_timed(&mut engine, "l", "long", &[("long", GIB, 120)], 0.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("h"));
        assert!(engine.running.contains_key("short"), "10 s ends before the running compile's 60 s");
        assert!(!engine.running.contains_key("long"));
        assert!(reason(&engine, "long", 0).contains("waiting for aged compiler memory reservation: h"));
        let ahead = engine.journal.history.last().unwrap().0.admitted.iter().any(|entry| {
            matches!(entry, Entry::Launched { job, ahead: Some(_), .. } if job == "short")
        });
        assert!(ahead, "the short job ran ahead into the head's bytes");
        assert!(engine.host_memory.head().unwrap().recorded.iter().any(|work| matches!(work, Work::Compile(Attempt { job, .. }) if job == "short")));
        // The running compile ends: the head fits and launches.
        end(&mut engine, "r");
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("h"));
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn after_a_restart_the_first_head_is_the_oldest_serviceable_candidate_and_no_state_file_is_written() {
        let (mut engine, mut sample) = admission_fixture().await;
        let a = Claimant::new();
        let b = Claimant::new();
        let a2 = Claimant::beside(&a);
        submit_party_group(&mut engine, &a, "live-a", &[], 0).await;
        submit_party_group(&mut engine, &b, "live-b", &[], 0).await;
        publish(&mut engine, "q", "running", &[("w", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        for (owner, id, age) in [(&a, "a1", 100.0), (&a2, "a2", 97.0), (&b, "b1", 95.0)] {
            engine.requested(owner.host_only(id, 3 * GIB)).await.unwrap();
            engine.host_memory.backdate(id, age);
        }
        let files = |engine: &Engine| -> BTreeSet<String> {
            std::fs::read_dir(&engine.config.state).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect()
        };
        sample.available = 15 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        let before = files(&engine);
        assert_eq!(head(&engine).as_deref(), Some("a1"));
        sample.available = 18 * GIB + GIB / 2;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "a1")["state"], "granted");
        sample.available = 18 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("b1"), "the turn passed to b, though a2 is older");
        let config = engine.handoff_config();
        drop(engine);
        let mut restored = Engine::restore(config, Clock::system(), Store::default(), Instance::current().unwrap(), attribution()).await.unwrap().activated().await.unwrap();
        restored.read_memory = sample_alone();
        restored.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&restored).as_deref(), Some("a2"), "the oldest serviceable candidate after the restart");
        assert_eq!(files(&restored), before, "no state file is added");
        replays(&restored);
        drop((a, b, a2));
        finish_fixture(restored, sample).await;
    }

    #[tokio::test]
    async fn claims_of_unknown_purpose_are_never_recoverable_so_nothing_is_reserved_against_them() {
        let (mut engine, mut sample) = admission_fixture().await;
        let gen16 = Claimant::new();
        let small = Claimant::new();
        sample.available = 20 * GIB;
        engine.requested(gen16.claim("gen16", 8 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "gen16")["state"], "granted");
        publish(&mut engine, "p", "large", &[("large", 5 * GIB)], 120.0, 0).await;
        // 2 GiB left; the 8 GiB claim may wait for the GPU, so S is 2 GiB and the 5 GiB job cannot reserve.
        for round in 0..2 {
            engine.tick(sample.clone()).await.unwrap();
            publish(&mut engine, "s", &format!("small-{round}"), &[(&format!("small-{round}"), GIB / 2)], 0.0, 0).await;
            engine.requested(small.host_only(&format!("claim-{round}"), GIB / 2)).await.unwrap();
            engine.tick(sample.clone()).await.unwrap();
            assert!(engine.running.contains_key(&format!("small-{round}")));
            assert_eq!(claim_status(&engine, &format!("claim-{round}"))["state"], "granted");
            engine.requested(small.release(&format!("claim-{round}"))).await.unwrap();
            end(&mut engine, &format!("small-{round}"));
        }
        let record = &engine.journal.history.last().unwrap().0;
        assert!(record.own_work.iter().all(|counted| !matches!(counted.work(), Work::Claim(Registration { claim, .. }) if claim == "gen16")));
        assert!(heads_seen(&engine).is_empty());
        assert!(reason(&engine, "large", 0).starts_with("waiting for RAM: needs"));
        // The same across a hand-over: the claim restores without a purpose.
        let config = engine.handoff_config();
        drop(engine);
        let mut restored = Engine::restore(config, Clock::system(), Store::default(), Instance::current().unwrap(), attribution()).await.unwrap().activated().await.unwrap();
        restored.read_memory = sample_alone();
        for _ in 0..3 {
            restored.tick(sample.clone()).await.unwrap();
        }
        assert!(heads_seen(&restored).is_empty());
        replays(&restored);
        drop((gen16, small));
        finish_fixture(restored, sample).await;
    }

    #[tokio::test]
    async fn memory_held_outside_the_services_work_reserves_nothing_and_admission_goes_on() {
        let (mut engine, mut sample) = admission_fixture().await;
        let placed = Claimant::new();
        let allocating_dir = engine.config.state.join("allocating");
        std::fs::create_dir(&allocating_dir).unwrap();
        let program = waiting()[0].clone();
        let code = "import pathlib,time\nwhile not pathlib.Path('go').exists(): time.sleep(0.005)\nheld = bytearray(256 << 20)\nheld[::4096] = b'x' * len(held[::4096])\npathlib.Path('done').write_text('')\ntime.sleep(60)";
        let mut allocating = std::process::Command::new(program.as_str().unwrap()).args(["-S", "-c", code]).current_dir(&allocating_dir).spawn().unwrap();
        shared(&allocating);
        let small = Claimant::new();
        sample.available = 22 * GIB;
        let gpu = |owner: u32, id: &str, bytes: u64| (json!({"protocol":1,"op":"host_memory_acquire","claim_id":id,"memory_bytes":bytes,"label":id,"purpose":"gpu_place"}), Peer::opened(owner));
        engine.requested(gpu(placed.0.id(), "placed", 6 * GIB)).await.unwrap();
        engine.requested(gpu(allocating.id(), "allocating", 2 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        std::fs::write(allocating_dir.join("go"), b"").unwrap();
        while !allocating_dir.join("done").exists() {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        publish(&mut engine, "p", "large", &[("large", 5 * GIB)], 120.0, 0).await;
        // The GPU places' owners never get the GPU: their claims are not the service's own work.
        for round in 0..2 {
            engine.tick(sample.clone()).await.unwrap();
            publish(&mut engine, "s", &format!("small-{round}"), &[(&format!("small-{round}"), GIB / 2)], 0.0, 0).await;
            engine.requested(small.host_only(&format!("claim-{round}"), GIB / 2)).await.unwrap();
            engine.tick(sample.clone()).await.unwrap();
            assert!(engine.running.contains_key(&format!("small-{round}")));
            assert_eq!(claim_status(&engine, &format!("claim-{round}"))["state"], "granted");
            engine.requested(small.release(&format!("claim-{round}"))).await.unwrap();
            end(&mut engine, &format!("small-{round}"));
        }
        assert!(heads_seen(&engine).is_empty(), "nothing is reserved");
        assert!(engine.host_memory.reserved(&sample) < 8 * GIB, "one claim allocated part of its declaration");
        // A process outside the service holds the memory instead: the same.
        engine.requested((json!({"protocol":1,"op":"host_memory_release","claim_id":"placed"}), Peer::opened(placed.0.id()))).await.unwrap();
        sample.available -= 6 * GIB;
        for _ in 0..3 {
            engine.tick(sample.clone()).await.unwrap();
        }
        assert!(heads_seen(&engine).is_empty());
        replays(&engine);
        allocating.kill().unwrap();
        allocating.wait().unwrap();
        drop((placed, small));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_job_that_loses_its_memory_within_each_tick_becomes_the_head_and_launches() {
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new();
        publish(&mut engine, &owner, "jobs", &[("five", 5 * GIB), ("three", 3 * GIB)], 31.0, 0).await;
        sample.available = 14 * GIB;
        let mut claimants = Vec::new();
        for round in 0..3 {
            let claimant = Claimant::beside(&owner);
            let id = format!("claim-{round}");
            engine.requested(claimant.host_only(&id, 2 * GIB)).await.unwrap();
            engine.tick(sample.clone()).await.unwrap();
            let launched = engine.running.contains_key("three");
            match round {
                0 => {
                    assert_eq!(claim_status(&engine, &id)["state"], "granted", "a claim is granted from what is left");
                    assert!(!launched, "the 3 GiB job misses: the claim took its memory");
                }
                _ => {}
            }
            engine.requested(claimant.release(&id)).await.unwrap();
            claimants.push(claimant);
            if launched {
                break;
            }
        }
        assert!(engine.running.contains_key("three"), "it held the head and launched");
        assert!(heads_seen(&engine).contains("three"));
        assert!(!heads_seen(&engine).contains("five"), "the 5 GiB job is not serviceable");
        // Once the 3 GiB job has ended, the 5 GiB job is not serviceable again and claims are granted from what is left.
        end(&mut engine, "three");
        engine.tick(sample.clone()).await.unwrap();
        let claimant = Claimant::beside(&owner);
        engine.requested(claimant.host_only("after", 2 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "after")["state"], "granted", "the claims keep being granted");
        assert!(reason(&engine, "jobs", 0).starts_with("waiting for RAM: needs"));
        replays(&engine);
        drop((owner, claimants, claimant));
        finish_fixture(engine, sample).await;
    }

    /// 200 groups of 500 pending jobs of one party, all eligible and aged, every slot busy; `deps` makes every
    /// job but each group's first, a large one, wait for that first job.
    async fn one_tick_over_a_large_backlog(cell: u64, deps: bool) -> std::time::Duration {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.idle = 0;
        sample.available = 64 * GIB;
        publish(&mut engine, "q", "running", &[("w", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        sample.available = 15 * GIB;
        for g in 0..200 {
            let mut jobs: Vec<Value> = (0..500)
                .map(|j| json!({"job_id":format!("g{g}-j{j}"),"argv":["true"],"cwd":engine.config.state,"memory_bytes":cell,"expected_runtime_secs":60,
                    "dependencies": if deps && j > 0 { json!([format!("g{g}-j0")]) } else { json!([]) }}))
                .collect();
            if deps {
                jobs[0]["memory_bytes"] = json!(22 * GIB);
            }
            engine
                .requested(json!({"protocol":1,"op":"submit_group","party":"p","group":{"group_id":format!("g{g}"),"jobs":jobs}}))
                .await
                .unwrap();
            engine.submitted.insert(format!("g{g}"), crate::protocol::now_unix() - 120.0);
        }
        for _ in 0..3 {
            engine.tick(sample.clone()).await.unwrap();
        }
        let started = std::time::Instant::now();
        engine.tick(sample.clone()).await.unwrap();
        let elapsed = started.elapsed();
        assert!(head(&engine).is_none());
        std::fs::write(engine.config.state.join("release"), b"").unwrap();
        end(&mut engine, "w");
        std::fs::remove_dir_all(&engine.config.state).unwrap();
        elapsed
    }

    #[tokio::test]
    async fn one_tick_over_a_large_backlog_stays_within_fifty_milliseconds() {
        // Every job fits: no candidate, and the walk skips every group by its cell bounds.
        let fitting = one_tick_over_a_large_backlog(GIB, false).await;
        // Every job's cell lies between the waiting figure and S, but its prerequisite is pending: the walk scans
        // every pending job.
        let scanned = one_tick_over_a_large_backlog(7 * GIB / 2, true).await;
        assert!(fitting < std::time::Duration::from_millis(50), "{fitting:?}");
        assert!(scanned < std::time::Duration::from_millis(50), "{scanned:?}");
    }

    #[tokio::test]
    async fn heads_hold_their_bytes_but_take_no_slot_from_a_party_holding_fewer() {
        let (mut engine, mut sample) = admission_fixture().await;
        cores(&mut sample, 21);
        for party in ["a", "c"] {
            let ids: Vec<String> = (0..16).map(|i| format!("{party}-{i}")).collect();
            let jobs: Vec<(&str, u64)> = ids.iter().map(|id| (id.as_str(), 3 * GIB)).collect();
            publish(&mut engine, party, party, &jobs, if party == "a" { 130.0 } else { 120.0 }, 0).await;
            for id in &ids[..10] {
                start(&mut engine, &mut sample, id, 0).await;
            }
        }
        // 4 GiB at each tick's start beside the running compiles.
        let room = |engine: &Engine, sample: &mut Snapshot| {
            sample.available = 14 * GIB + 3 * GIB * engine.running.len() as u64;
        };
        room(&engine, &mut sample);
        engine.tick(sample.clone()).await.unwrap();
        let mut ended = 0;
        for party in ["a", "c"] {
            end(&mut engine, &format!("{party}-{ended}"));
            ended += 1;
            room(&engine, &mut sample);
            sample.available -= 3 * GIB;
            engine.tick(sample.clone()).await.unwrap();
        }
        assert!(engine.host_memory.head().is_some(), "the backlogs wait for memory and hold a head");
        publish(&mut engine, "b", "b", &[("b", GIB)], 0.0, 0).await;
        end(&mut engine, "a-2");
        room(&engine, &mut sample);
        sample.available -= 3 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("b"), "B's job launches at the first free slot");
        assert!(engine.journal.history.last().unwrap().0.aged_reserved > 0, "a head holds its bytes");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// A running job whose processes `rss` lists, children first named by fake PIDs.
    fn processes(sample: &mut Snapshot, engine: &Engine, job: &str, rss: &[u64]) {
        let pid = engine.running[job].record["pid"].as_u64().unwrap() as u32;
        sample.processes.insert(pid, (std::process::id(), rss[0]));
        for (i, bytes) in rss[1..].iter().enumerate() {
            sample.processes.insert(4_000_000 + pid * 8 + i as u32, (pid, *bytes));
        }
    }

    #[tokio::test]
    async fn the_projection_counts_cells_of_contracted_compiles_and_host_only_claims_never_measurements() {
        // A 4 GiB cell whose parent and forked child share 4 GiB: D is the cell, not 8 GiB of summed RSS.
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new();
        publish(&mut engine, "q", "forked", &[("forked", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "forked", 0).await;
        processes(&mut sample, &engine, "forked", &[4 * GIB, 4 * GIB]);
        engine.requested(owner.host_only("eight", 8 * GIB)).await.unwrap();
        engine.host_memory.backdate("eight", 60.0);
        engine.tick(sample.clone()).await.unwrap();
        let record = &engine.journal.history.last().unwrap().0;
        assert_eq!(record.own(), 4 * GIB);
        assert_eq!(record.serviceable, 6 * GIB);
        assert!(head(&engine).is_none(), "an 8 GiB claim is not serviceable");
        assert!(claim_status(&engine, "eight")["waiting"].as_str().unwrap().starts_with("waiting for RAM: needs"));
        drop(owner);
        finish_fixture(engine, sample).await;

        // Four 2 GiB cells holding their memory in a memfd: D is 8 GiB, and a 5 GiB claim is granted after two completions.
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new();
        let ids = ["m0", "m1", "m2", "m3"];
        publish(&mut engine, "q", "memfd", &ids.map(|id| (id, 2 * GIB)), 0.0, 0).await;
        cores(&mut sample, 6);
        for id in ids {
            start(&mut engine, &mut sample, id, 2 * GIB).await;
        }
        engine.requested(owner.host_only("five", 5 * GIB)).await.unwrap();
        engine.host_memory.backdate("five", 60.0);
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(engine.journal.history.last().unwrap().0.serviceable, 10 * GIB);
        assert_eq!(head(&engine).as_deref(), Some("five"));
        for id in ["m0", "m1"] {
            end(&mut engine, id);
            sample.available += 2 * GIB;
            engine.tick(sample.clone()).await.unwrap();
        }
        assert_eq!(claim_status(&engine, "five")["state"], "granted");
        replays(&engine);
        drop(owner);
        finish_fixture(engine, sample).await;

        // Three 1 GiB cells, each a parent and child sharing anonymous memory and mapping one file: D is 3 GiB.
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new();
        let ids = ["f0", "f1", "f2"];
        cores(&mut sample, 6);
        publish(&mut engine, "q", "files", &ids.map(|id| (id, GIB)), 0.0, 0).await;
        for id in ids {
            start(&mut engine, &mut sample, id, 0).await;
            processes(&mut sample, &engine, id, &[GIB, GIB]);
        }
        sample.available = 21 * GIB / 2;
        engine.requested(owner.host_only("two", 2 * GIB)).await.unwrap();
        engine.host_memory.backdate("two", 60.0);
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(engine.journal.history.last().unwrap().0.own(), 3 * GIB);
        assert_eq!(head(&engine).as_deref(), Some("two"));
        drop(owner);
        finish_fixture(engine, sample).await;

        // A compile restored without a runtime counts nothing and is never recorded; of the claims, only the
        // host-only one counts.
        let (mut engine, mut sample) = admission_fixture().await;
        let owners: Vec<Claimant> = (0..4).map(|_| Claimant::new()).collect();
        publish(&mut engine, "q", "legacy", &[("legacy", 2 * GIB)], 0.0, 0).await;
        let mut legacy = engine.groups["legacy"].jobs[0].clone();
        legacy.expected_runtime_secs = None;
        engine.launch_holding("legacy".into(), legacy, sample.cores[0], engine.clock.monotonic_secs(), 0).await.unwrap();
        sample.available = 30 * GIB;
        for (owner, (id, purpose)) in owners.iter().zip([("host", json!("host_only")), ("placed", json!("gpu_place")), ("unknown", Value::Null)]) {
            let mut claim = owner.claim(id, GIB);
            claim.0["purpose"] = purpose;
            engine.requested(claim).await.unwrap();
        }
        engine.tick(sample.clone()).await.unwrap();
        engine.requested(owners[3].host_only("waits", 15 * GIB / 2)).await.unwrap();
        engine.host_memory.backdate("waits", 60.0);
        engine.tick(sample.clone()).await.unwrap();
        let record = &engine.journal.history.last().unwrap().0;
        let counted: Vec<Work> = record.own_work.iter().map(Counted::work).collect();
        assert!(matches!(counted.as_slice(), [Work::Claim(Registration { claim, .. })] if claim == "host"), "{counted:?}");
        assert!(engine.host_memory.head().is_none_or(|head| head.recorded.iter().all(|work| !matches!(work, Work::Compile(Attempt { job, .. }) if job == "legacy"))));
        drop(owners);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn bytes_a_compiler_turn_kept_for_a_job_that_did_not_launch_go_to_ordinary_claims_before_large_ones() {
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new();
        let large = Claimant::beside(&owner);
        publish(&mut engine, "q", "running", &[("w", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        sample.available = 17 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        publish(&mut engine, &owner, "group", &[("head", 5 * GIB), ("nine", 9 * GIB)], 120.0, 1).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("head"));
        engine.requested(owner.host_only("eight", 8 * GIB)).await.unwrap();
        engine.requested(large.host_only("twelve", 12 * GIB)).await.unwrap();
        end(&mut engine, "w");
        // 20 GiB left.
        sample.available = 30 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        let record = &engine.journal.history.last().unwrap().0;
        let skipped = record.admitted.iter().position(|e| matches!(e, Entry::Turn { bytes, .. } if *bytes == 9 * GIB));
        assert!(skipped.is_some(), "the 9 GiB job's turn kept its bytes from the 8 GiB claim");
        assert!(engine.running.contains_key("head") && !engine.running.contains_key("nine"), "the head fills its group");
        assert!(record.admitted.iter().any(|e| matches!(e, Entry::Granted { phase: Persisting::Close, claim, budget, .. } if claim == "eight" && *budget == Budget::of(15 * GIB))));
        assert!(record.admitted.iter().all(|e| !matches!(e, Entry::Granted { claim, .. } if claim == "twelve")));
        assert_eq!(claim_status(&engine, "twelve")["waiting"], LARGE_WAIT, "offered only the 7 GiB after the second pass");
        replays(&engine);
        drop((owner, large));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_head_whose_named_work_ended_while_memory_held_elsewhere_took_its_bytes_gives_way() {
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new();
        let other = Claimant::new();
        submit_party_group(&mut engine, &owner, "live-p", &[], 0).await;
        submit_party_group(&mut engine, &other, "live-o", &[], 0).await;
        publish(&mut engine, "q", "named", &[("named", 4 * GIB)], 0.0, 0).await;
        publish(&mut engine, "q", "later", &[("later", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "named", 0).await;
        sample.available = 16 * GIB;
        engine.requested(owner.host_only("six", 6 * GIB)).await.unwrap();
        engine.host_memory.backdate("six", 60.0);
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("six"), "it reserves 6 GiB, naming the running compile");
        start(&mut engine, &mut sample, "later", 0).await;
        sample.available += 4 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("six"));
        // The named compile ends and a process outside the service takes what it freed.
        end(&mut engine, "named");
        sample.available -= 4 * GIB;
        engine.requested(other.host_only("younger", GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert!(head(&engine).is_none(), "it ended in vain");
        let six = request_of(&engine, "six");
        assert!(engine.host_memory.gives_way(&six));
        assert_eq!(claim_status(&engine, "younger")["state"], "granted", "younger work uses its bytes");
        engine.tick(sample.clone()).await.unwrap();
        assert!(head(&engine).is_none(), "no head until the work running then has ended");
        end(&mut engine, "later");
        sample.available -= 4 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        assert!(!engine.host_memory.gives_way(&six));
        replays(&engine);
        drop((owner, other));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn with_exact_accounting_a_head_fits_when_its_named_work_ends() {
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new();
        submit_party_group(&mut engine, &owner, "live", &[], 0).await;
        publish(&mut engine, "q", "named", &[("named", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "named", 0).await;
        sample.available = 16 * GIB;
        engine.requested(owner.host_only("six", 6 * GIB)).await.unwrap();
        engine.host_memory.backdate("six", 60.0);
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("six"));
        assert_eq!(claim_status(&engine, "six")["waiting"].as_str().unwrap(), format!("aged head, party {}'s turn: {} bytes reserved", owner.party(), 6 * GIB));
        end(&mut engine, "named");
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "six")["state"], "granted");
        replays(&engine);
        drop(owner);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_fitting_compile_head_waiting_for_its_partys_slot_is_kept_after_its_named_work_ended() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.cores.truncate(2);
        // The rival waits from an older publication than the head's party, so it takes the first tie.
        publish(&mut engine, "r", "rival", &[("rival", GIB), ("rival-2", GIB)], 200.0, 0).await;
        publish(&mut engine, "q", "named", &[("named", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "named", 0).await;
        sample.available = 16 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        publish(&mut engine, "p", "head", &[("head", 5 * GIB)], 120.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("head"));
        end(&mut engine, "named");
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("rival"), "the rival takes the tie for the freed slot");
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("head"), "it fits and is kept, its named work ended");
        assert!(engine.host_memory.head().unwrap().fits);
        end(&mut engine, "rival");
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("head"), "it launches at its party's next slot");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_decision_is_recorded_on_its_first_tick_and_when_it_changes_only() {
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new();
        publish(&mut engine, "q", "running", &[("w", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        // Nothing left and S is 4 GiB: the 6 GiB claim is not serviceable; then S is 6 GiB.
        sample.available = 14 * GIB;
        let mut claim = owner.claim("placed", 6 * GIB);
        claim.0["purpose"] = json!("gpu_place");
        engine.requested(claim).await.unwrap();
        let placed = Subject::from(&request_of(&engine, "placed"));
        let decided = |engine: &Engine| -> Vec<(bool, Option<Decisions>)> {
            engine.journal.history.iter().map(|(record, written)| (*written, record.decisions.iter()
                .find(|d| d.subject == placed).map(|d| d.decision.clone()))).collect()
        };
        for _ in 0..3 {
            engine.tick(sample.clone()).await.unwrap();
        }
        sample.available = 16 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        let one = |decision| Some(Decisions::One(decision));
        assert_eq!(
            decided(&engine),
            [(true, one(Decision::NotServiceable)), (false, None), (false, None), (true, one(Decision::WaitingForTurn))],
            "a record on its first tick and on its change, none between"
        );
        drop(owner);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn one_budget_runs_through_the_tick_and_its_compiler_turns_go_back_to_the_launches() {
        let fixture = |memory: u64| async move {
            let (mut engine, sample) = admission_fixture().await;
            engine.config.minimum_ram = 2 * GIB;
            engine.read_memory = match memory {
                20 => Arc::new(|| Ok((20 * GIB, 128 * GIB))),
                _ => sample_alone(),
            };
            (engine, sample)
        };
        // The sample says 30 GiB, a fresh reading 20 GiB, with 2 GiB headroom: 18 GiB for the whole tick.
        let (mut engine, mut sample) = fixture(20).await;
        sample.available = 30 * GIB;
        let owner = Claimant::new();
        publish(&mut engine, "p", "four", &[("four", 4 * GIB)], 0.0, 0).await;
        engine.requested(owner.host_only("twenty", 20 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        let record = engine.journal.history.last().unwrap().0.clone();
        assert_eq!(record.left, Budget::of(18 * GIB));
        assert!(engine.running.contains_key("four"));
        assert_eq!(record.left_after_launch, 14 * GIB);
        assert_eq!(claim_status(&engine, "twenty")["state"], "pending");
        drop(owner);
        finish_fixture(engine, sample).await;

        // A compile whose turn kept its bytes from a claim launches in the same tick.
        let (mut engine, mut sample) = fixture(0).await;
        sample.available = 15 * GIB;
        let owner = Claimant::new();
        publish(&mut engine, &owner, "four", &[("four", 4 * GIB)], 0.0, 0).await;
        engine.requested(owner.host_only("nine", 9 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "nine")["state"], "granted");
        assert!(engine.running.contains_key("four"), "the turn's 4 GiB went back to the launch");
        replays(&engine);
        drop(owner);
        finish_fixture(engine, sample).await;

        // The budget is signed: 6 GiB available, 2 GiB headroom, two compiles declaring 5 GiB and using 1 GiB.
        let (mut engine, mut sample) = fixture(0).await;
        let owner = Claimant::new();
        let small = Claimant::new();
        publish(&mut engine, "q", "running", &[("r0", 5 * GIB), ("r1", 5 * GIB)], 0.0, 0).await;
        for id in ["r0", "r1"] {
            start(&mut engine, &mut sample, id, GIB).await;
        }
        sample.available = 6 * GIB;
        engine.requested(owner.host_only("eight", 8 * GIB)).await.unwrap();
        engine.requested(small.host_only("one", GIB)).await.unwrap();
        engine.host_memory.backdate("eight", 60.0);
        engine.tick(sample.clone()).await.unwrap();
        let record = engine.journal.history.last().unwrap().0.clone();
        assert_eq!(record.left, Budget::of(0).less(4 * GIB));
        assert_eq!(record.serviceable, 6 * GIB, "flooring each term would promise 10 GiB");
        assert!(head(&engine).is_none(), "8 GiB is not serviceable");
        assert_eq!(claim_status(&engine, "one")["state"], "pending", "nothing is admitted from a negative budget");
        replays(&engine);
        let floored = Record { left: Budget::of(0), serviceable: 10 * GIB, ..record };
        assert!(checked_with(&engine, floored).is_err());
        drop((owner, small));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_reservation_is_deducted_once_and_the_head_launches_from_it() {
        let (mut engine, mut sample) = admission_fixture().await;
        publish(&mut engine, "q", "running", &[("w", 2 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        // 4 GiB left; S is 6 GiB.
        sample.available = 16 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        publish(&mut engine, "p", "head", &[("head", 5 * GIB)], 120.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("head"));
        end(&mut engine, "w");
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("head"));
        let record = &engine.journal.history.last().unwrap().0;
        assert!(record.admitted.iter().any(|e| matches!(e, Entry::Reserved { bytes, budget, .. } if *bytes == 5 * GIB && *budget == Budget::of(6 * GIB))));
        assert!(record.admitted.iter().any(|e| matches!(e, Entry::Launched { head: true, budget, reserved, .. } if *budget == Budget::of(GIB) && *reserved == 5 * GIB)));
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// Starts `job` at once as a compile restored from before runtimes were declared.
    async fn start_uncontracted(engine: &mut Engine, sample: &Snapshot, job: &str) {
        let group = engine.progress.owner(job).unwrap().to_owned();
        let mut spec = engine.groups[&group].jobs.iter().find(|j| j.job_id == job).unwrap().clone();
        spec.expected_runtime_secs = None;
        let used: BTreeSet<u64> = engine.running.values().filter_map(|r| r.record["cpu"].as_u64()).collect();
        let cpu = *sample.cores.iter().find(|c| !used.contains(&(**c as u64))).unwrap();
        engine.launch_holding(group, spec, cpu, engine.clock.monotonic_secs(), 0).await.unwrap();
    }

    #[tokio::test]
    async fn a_restored_compile_without_a_runtime_is_held_outside_the_service() {
        let (mut engine, mut sample) = admission_fixture().await;
        cores(&mut sample, 3);
        let owner = Claimant::new();
        publish(&mut engine, "a", "legacy", &[("legacy", GIB)], 0.0, 0).await;
        start_uncontracted(&mut engine, &sample, "legacy").await;
        // A record of a compile from an earlier boot, whose processes are gone.
        publish(&mut engine, "e", "earlier", &[("earlier", GIB)], 0.0, 0).await;
        let earlier = json!({"group_id":"earlier","job_id":"earlier","boot_id":"an-earlier-boot","pid":1,"start_ticks":1,"cpu":sample.cores[2],
            "memory_bytes":GIB,"job":engine.groups["earlier"].jobs[0],"log_path":engine.config.state.join("logs").join("earlier.log"),
            "queued_at":0.0,"started_at":1.0,"started_monotonic":1.0});
        wrote(&engine.config.state.join("running").join("earlier.json"), &earlier).await;
        publish(&mut engine, "a", "head", &[("head", 2 * GIB)], 120.0, 0).await;
        // Restore takes a group's publication from its descriptor.
        std::fs::File::options()
            .write(true)
            .open(engine.config.state.join("groups").join("head.json"))
            .unwrap()
            .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(120))
            .unwrap();
        submit_party_group(&mut engine, &owner, "live", &[], 0).await;
        let config = engine.handoff_config();
        drop(engine);
        let mut engine = Engine::restore(config, Clock::system(), Store::default(), Instance::current().unwrap(), attribution()).await.unwrap().activated().await.unwrap();
        engine.read_memory = sample_alone();
        // 5 GiB left beside the restored compile; a claim takes 4 GiB in the first tick, so the head job waits for
        // memory by what that tick's launches left.
        sample.available = 16 * GIB;
        engine.requested(owner.host_only("passing", 4 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert!(!engine.running.contains_key("earlier") && engine.results["earlier"]["outcome"] == "lost", "the first tick reaps a record of an earlier boot");
        assert_eq!(claim_status(&engine, "passing")["state"], "granted");
        let record = &engine.journal.history.last().unwrap().0;
        assert!(record.own_work.iter().all(|counted| !matches!(counted.work(), Work::Compile(Attempt { job, .. }) if job == "legacy")), "D does not count it");
        assert!(engine.held(|_| 1).get("a").is_none() && engine.ram_held().get("a").is_none(), "nor do the shares");
        engine.requested(owner.release("passing")).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        let record = &engine.journal.history.last().unwrap().0;
        let passed = record.aged.steps.iter().any(|step| matches!(step, Step::Chosen { chosen, verdict: Verdict::Passed, .. } if chosen.is_job("head")));
        assert!(passed, "the fitting head lacks its party's last core-share unit, which only the restored compile holds");
        assert!(head(&engine).is_none() && record.aged.after.as_ref().is_some_and(|turn| !turn.held), "its party's turn passed");
        assert!(reason(&engine, "head", 0).contains("concurrency capacity"));
        replays(&engine);
        drop(owner);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_compile_of_this_boot_without_a_known_start_is_held_outside_the_service_and_still_charged() {
        let (mut engine, mut sample) = admission_fixture().await;
        publish(&mut engine, "a", "unstarted", &[("unstarted", 2 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "unstarted", 0).await;
        // A running record of this boot that declares its runtime, without its monotonic start: the contract
        // cannot end it.
        let record = &mut engine.running.get_mut("unstarted").unwrap().record;
        assert!(record["boot_id"] == engine.instance.boot_id && record["job"]["expected_runtime_secs"] == 60);
        record.as_object_mut().unwrap().remove("started_monotonic");
        engine.tick(sample.clone()).await.unwrap();
        let record = &engine.journal.history.last().unwrap().0;
        assert!(record.own_work.is_empty(), "D does not count it");
        assert!(engine.held(|_| 1).get("a").is_none() && engine.ram_held().get("a").is_none(), "nor do the shares");
        let capacity = engine.capacity(&engine.groups["unstarted"]);
        assert_eq!(capacity.running, Units { running: 1, contracted: 0 }, "nor does the capacity rule's contracted count");
        assert_eq!((capacity.held, record.compiler_reserved), (1, 2 * GIB), "its core and its unused cell are charged");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_fitting_head_without_idle_cores_is_released_so_another_partys_claim_gets_the_head() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.idle = 0;
        sample.available = 20 * GIB;
        let staggered = Claimant::new();
        let owners: Vec<Claimant> = (0..4).map(|_| Claimant::beside(&staggered)).collect();
        let waiting = Claimant::new();
        submit_party_group(&mut engine, &staggered, "live-s", &[], 0).await;
        submit_party_group(&mut engine, &waiting, "live-t", &[], 0).await;
        publish(&mut engine, "p", "head", &[("head", 5 * GIB)], 120.0, 0).await;
        engine.requested(owners[0].host_only("s0", 2 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        // Two more 2 GiB claims in one tick leave 4 GiB after it.
        engine.requested(owners[1].host_only("s1", 2 * GIB)).await.unwrap();
        engine.requested(owners[2].host_only("s2", 2 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        engine.requested(waiting.host_only("nine", 9 * GIB)).await.unwrap();
        engine.host_memory.backdate("nine", 40.0);
        engine.requested(owners[2].release("s2")).await.unwrap();
        // 6 GiB left: the 5 GiB job fits, but no core is idle and no compile holds one.
        engine.tick(sample.clone()).await.unwrap();
        let record = &engine.journal.history.last().unwrap().0;
        assert!(record.aged.steps.iter().any(|step| matches!(step, Step::Chosen { chosen, verdict: Verdict::Passed, .. } if chosen.is_job("head"))));
        assert_eq!(head(&engine).as_deref(), Some("nine"), "the 9 GiB claim gets the aged head and reserves");
        engine.requested(owners[3].host_only("s3", 2 * GIB)).await.unwrap();
        engine.requested(owners[0].release("s0")).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "s3")["state"], "pending", "nothing is granted from its bytes");
        engine.requested(owners[1].release("s1")).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "nine")["state"], "granted");
        engine.requested(waiting.release("nine")).await.unwrap();
        sample.idle = 4;
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("head"), "with idle cores back, it launches, charged to memory");
        replays(&engine);
        drop((staggered, owners, waiting));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn every_launch_is_charged_its_cell_once_however_the_slot_shares_order_them() {
        let (mut engine, mut sample) = admission_fixture().await;
        cores(&mut sample, 6);
        publish(&mut engine, "a", "legacy", &[("legacy", GIB)], 0.0, 0).await;
        start_uncontracted(&mut engine, &sample, "legacy").await;
        publish(&mut engine, "a", "head", &[("head", 5 * GIB)], 120.0, 0).await;
        sample.available = 15 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        let small: Vec<String> = (0..4).map(|i| format!("b{i}")).collect();
        let jobs: Vec<(&str, u64)> = small.iter().map(|id| (id.as_str(), GIB)).collect();
        publish(&mut engine, "b", "b", &jobs, 0.0, 0).await;
        // A 6 GiB budget and four free slots.
        sample.available = 17 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        let record = &engine.journal.history.last().unwrap().0;
        let launched: u64 = record.admitted.iter().filter_map(|e| match e { Entry::Launched { bytes, .. } => Some(*bytes), _ => None }).sum();
        assert!(engine.running.contains_key("head"));
        assert_eq!(launched, 6 * GIB, "the tick admits at most its 6 GiB budget");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn while_the_engine_drains_a_compile_head_is_released_and_claims_are_served() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.cores.truncate(2);
        let owner = Claimant::new();
        submit_party_group(&mut engine, &owner, "live", &[], 0).await;
        publish(&mut engine, "q", "running", &[("w", 2 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        sample.available = 16 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        publish(&mut engine, "p", "head", &[("head", 5 * GIB)], 120.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        // 6 GiB admissible: it fits, and the only slot is held by a contracted compile.
        sample.available = 18 * GIB;
        engine.requested(owner.host_only("two", 2 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("head"));
        assert_eq!(claim_status(&engine, "two")["state"], "pending");
        engine.drain();
        engine.tick(sample.clone()).await.unwrap();
        assert!(head(&engine).is_none(), "a compile head is released while the engine drains");
        assert_eq!(claim_status(&engine, "two")["state"], "granted");
        engine.requested(owner.release("two")).await.unwrap();
        end(&mut engine, "w");
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.drained());
        replays(&engine);
        drop(owner);
        std::fs::remove_dir_all(engine.config.state).unwrap();
    }

    /// Party A holds a restored compile without a runtime and an aged 5 GiB head; party B's 1 GiB compiles end
    /// one at a time; party C's 6 GiB claims arrive one at a time. `long` adds four long compiles of party D,
    /// so a slot frees only when B's compile ends.
    async fn a_party_holding_only_an_uncontracted_compile_wins_slots_for_its_head(long: bool) {
        let (mut engine, mut sample) = admission_fixture().await;
        cores(&mut sample, 7);
        let c = Claimant::new();
        let claims: Vec<Claimant> = (0..2).map(|_| Claimant::beside(&c)).collect();
        submit_party_group(&mut engine, &c, "live-c", &[], 0).await;
        publish(&mut engine, "a", "legacy", &[("legacy", GIB)], 0.0, 0).await;
        start_uncontracted(&mut engine, &sample, "legacy").await;
        publish(&mut engine, "b", "b1", &[("b1", GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "b1", 0).await;
        let ids = ["d0", "d1", "d2", "d3"];
        match long {
            true => {
                publish(&mut engine, "d", "d", &ids.map(|id| (id, GIB / 4)), 0.0, 0).await;
                for id in ids {
                    start(&mut engine, &mut sample, id, 0).await;
                }
            }
            false => {}
        }
        // 8 GiB admissible beside the running compiles.
        let running: u64 = engine.running.values().map(|r| r.record["memory_bytes"].as_u64().unwrap()).sum();
        sample.available = 18 * GIB + running;
        engine.requested(claims[0].host_only("c1", 6 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "c1")["state"], "granted");
        assert_eq!(engine.held(|_| 1).get("a"), None, "the shares count nothing for the restored compile");
        assert_eq!(engine.held(|_| 1).get("b"), Some(&fairness::charge(1, 0)));
        publish(&mut engine, "a", "head", &[("head", 5 * GIB)], 120.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("head"), "it reserves while the first claim is granted");
        engine.requested(claims[1].host_only("c2", 6 * GIB)).await.unwrap();
        engine.requested(claims[0].release("c1")).await.unwrap();
        end(&mut engine, "b1");
        publish(&mut engine, "b", "b2", &[("b2", GIB)], 0.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("head"), "it launches in that tick");
        assert!(engine.running.contains_key("b2") || long, "B's job takes another free slot");
        assert_eq!(claim_status(&engine, "c2")["state"], "pending", "no claim is granted from its bytes");
        replays(&engine);
        drop((c, claims));
        finish_fixture(engine, sample).await;
    }
    #[tokio::test]
    async fn a_head_whose_party_holds_only_an_uncontracted_compile_launches_beside_spare_slots() {
        a_party_holding_only_an_uncontracted_compile_wins_slots_for_its_head(false).await;
    }
    #[tokio::test]
    async fn a_head_whose_party_holds_only_an_uncontracted_compile_launches_when_one_slot_frees() {
        a_party_holding_only_an_uncontracted_compile_wins_slots_for_its_head(true).await;
    }

    // Inputs across persistence boundaries (section 3.6, tests 40 and 42) and the store seam's removals.

    /// The admission fixture on a manual clock, with the store handle its tests hold operations through.
    async fn held_fixture() -> (Engine, Snapshot, Arc<Manual>, Store) {
        let (clock, handle) = Manual::new();
        let store = Store::default();
        let (engine, sample) = admission_fixture_on(handle, store.clone()).await;
        (engine, sample, clock, store)
    }
    /// Ticks until `id`'s attempt concludes, `clock` running at the rate the test waits, as the service's clock runs:
    /// a termination escalates past a signal its compiler lost, as it does in the service.
    async fn concluded(engine: &mut Engine, sample: &Snapshot, clock: &Manual, id: &str) {
        for _ in 0..1000 {
            if engine.results.contains_key(id) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            clock.advance(std::time::Duration::from_millis(10));
            engine.tick(sample.clone()).await.unwrap();
        }
        panic!("{id} did not conclude");
    }
    /// One tick that runs `during` while `hold`'s operation is held, once it has been entered.
    async fn tick_holding(engine: &mut Engine, sample: &Snapshot, mut hold: Hold, during: impl FnOnce()) -> Result<(), String> {
        let control = async move {
            entered(&mut hold).await;
            during();
            hold.release();
        };
        let (ticked, ()) = tokio::join!(engine.tick(sample.clone()), control);
        ticked
    }
    async fn entered(hold: &mut Hold) {
        tokio::time::timeout(std::time::Duration::from_secs(30), hold.entered()).await.expect("the held operation runs");
    }
    /// Party q's contracted compile r declares 60 s and started 55 s before; two slots are free beside it.
    async fn running_r(available: u64) -> (Engine, Snapshot, Arc<Manual>, Store) {
        let (mut engine, mut sample, clock, store) = held_fixture().await;
        cores(&mut sample, 6);
        sample.idle = 2;
        sample.available = available;
        publish_timed(&mut engine, "q", "running", &[("r", 4 * GIB, 60)], 0.0, 0).await;
        let started = engine.clock.monotonic_secs() - 55.0;
        start_at(&mut engine, &mut sample, "r", 4 * GIB, started).await;
        (engine, sample, clock, store)
    }
    /// Party p's aged 5 GiB job h, and party s's jobs a and b, 1 GiB each, declaring 4 s.
    async fn head_and_runners(engine: &mut Engine) {
        publish_timed(engine, "p", "head", &[("h", 5 * GIB, 60)], 120.0, 0).await;
        publish_timed(engine, "s", "s", &[("a", GIB, 4), ("b", GIB, 4)], 0.0, 0).await;
    }
    /// Test 40's case: beside r, h does not fit and reserves, and a and b fit only with its bytes. The next tick
    /// reads the clock at t, and r is declared to finish at t + 5.
    async fn run_ahead_case() -> (Engine, Snapshot, Arc<Manual>, Store) {
        let (mut engine, sample, clock, store) = running_r(12 * GIB).await;
        engine.tick(sample.clone()).await.unwrap();
        head_and_runners(&mut engine).await;
        (engine, sample, clock, store)
    }
    const RUN_AHEAD_WAIT: &str = "waiting for aged compiler memory reservation: h; a job runs ahead only if";
    fn last_record(engine: &Engine) -> Record {
        engine.journal.history.last().unwrap().0.clone()
    }
    /// `job`'s launch in `record`: its admission, its settlement, its run-ahead deadline, and whether it left a
    /// running compile.
    fn launch_of(record: &Record, job: &str) -> Option<(f64, f64, Option<f64>, bool)> {
        record.admitted.iter().find_map(|entry| match entry {
            Entry::Launched { job: launched, admitted_at, settled_at, ahead, running, .. } if launched == job => {
                Some((*admitted_at, *settled_at, ahead.as_ref().map(|ahead| ahead.deadline), *running))
            }
            _ => None,
        })
    }
    fn close_to(value: f64, expected: f64) -> bool {
        (value - expected).abs() < 1e-6
    }
    /// A 1 GiB launch of `job` running ahead into the head's bytes, as a service would record it.
    fn ahead_entry(job: &str, budget: Budget, reserved: u64, admitted_at: f64, deadline: f64) -> Entry {
        Entry::Launched {
            phase: Launching::Launch,
            group: "s".into(),
            job: job.into(),
            class: Class::Ordinary,
            bytes: GIB,
            expected: GIB,
            lent: 0,
            budget,
            reserved,
            head: false,
            ahead: Some(Ahead { deadline, work: Work::compile(job, 0.0) }),
            admitted_at,
            declared: Some(4),
            settled_at: admitted_at,
            running: true,
        }
    }
    /// `record` with b running ahead right after a, admitted at `admitted_at`, as a defective service would have.
    fn with_b_ahead(record: &Record, admitted_at: f64) -> Record {
        let mut record = record.clone();
        let index = record.admitted.iter().position(|entry| matches!(entry, Entry::Launched { job, .. } if job == "a")).unwrap();
        let b = match &record.admitted[index] {
            Entry::Launched { budget, reserved, ahead: Some(ahead), .. } => {
                ahead_entry("b", *budget, reserved - GIB, admitted_at, ahead.deadline)
            }
            other => panic!("a did not run ahead: {other:?}"),
        };
        record.admitted.insert(index + 1, b);
        record
    }
    /// `record` with a running ahead first after the pre-launch sequence, admitted at `admitted_at`.
    fn with_a_ahead_first(record: &Record, admitted_at: f64, deadline: f64) -> Record {
        let mut record = record.clone();
        let (index, budget) = record
            .admitted
            .iter()
            .enumerate()
            .find_map(|(index, entry)| match entry {
                Entry::Persisted { budget, .. } => Some((index, *budget)),
                _ => None,
            })
            .unwrap();
        record.admitted.insert(index + 1, ahead_entry("a", budget, record.aged_reserved, admitted_at, deadline));
        record
    }
    fn replay_error(engine: &Engine, record: &Record) -> String {
        checked_with(engine, record.clone()).unwrap_err()
    }

    #[tokio::test]
    async fn a_run_ahead_after_a_held_intent_write_is_admitted_with_the_clock_read_after_it() {
        let (mut engine, sample, clock, store) = run_ahead_case().await;
        let t = engine.clock.monotonic_secs();
        let hold = store.hold(Operation::new("intent", "a"));
        tick_holding(&mut engine, &sample, hold, || clock.advance(std::time::Duration::from_secs(4))).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("h"), "h keeps its bytes");
        assert!(engine.running.contains_key("a") && !engine.running.contains_key("b"));
        assert!(reason(&engine, "s", 1).starts_with(RUN_AHEAD_WAIT), "{}", reason(&engine, "s", 1));
        let record = last_record(&engine);
        let (admitted_at, settled_at, deadline, running) = launch_of(&record, "a").unwrap();
        assert_eq!(admitted_at, t, "a was admitted at t");
        assert!(settled_at >= engine.clock.monotonic_secs() && running, "a settled after its held write");
        assert!(deadline.is_some_and(|deadline| close_to(deadline, t + 5.0)), "{deadline:?}");
        let r = record.own_work.iter().find_map(|counted| match counted {
            Counted::Compile { attempt, finish, .. } if attempt.job == "r" => Some(*finish),
            _ => None,
        });
        assert!(r.is_some_and(|finish| close_to(finish, t + 5.0)), "D holds r's declared finish");
        assert_eq!(record.slots, 2);
        replays(&engine);
        // Admitted after the held write, b would finish after the deadline: it may not take from the reservation.
        let error = replay_error(&engine, &with_b_ahead(&record, t + 4.0));
        assert!(error.contains("b of 1073741824 bytes launched beyond its budget"), "{error}");
        assert!(replay_error(&engine, &with_b_ahead(&record, t)).contains("before the persistence boundary"));
        finish_fixture(engine, sample).await;

        // Without the hold both run ahead, each finishing by t + 5.
        let (mut engine, sample, _clock, _store) = run_ahead_case().await;
        let t = engine.clock.monotonic_secs();
        engine.tick(sample.clone()).await.unwrap();
        let record = last_record(&engine);
        for job in ["a", "b"] {
            let (admitted_at, _, deadline, _) = launch_of(&record, job).unwrap();
            assert!(admitted_at + 4.0 <= deadline.unwrap() && close_to(deadline.unwrap(), t + 5.0), "{job}");
        }
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_held_pid_write_bounds_the_next_admission_as_the_intent_write_does() {
        let (mut engine, sample, clock, store) = run_ahead_case().await;
        let hold = store.hold(Operation::new("pid", "a"));
        tick_holding(&mut engine, &sample, hold, || clock.advance(std::time::Duration::from_secs(4))).await.unwrap();
        assert!(engine.running.contains_key("a") && !engine.running.contains_key("b"));
        assert!(reason(&engine, "s", 1).starts_with(RUN_AHEAD_WAIT));
        let (_, settled_at, _, _) = launch_of(&last_record(&engine), "a").unwrap();
        assert!(settled_at >= engine.clock.monotonic_secs());
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_failed_spawn_settles_after_its_held_intent_removal_and_creates_no_work_for_the_kept_head() {
        let (mut engine, sample, clock, store) = run_ahead_case().await;
        engine.groups.get_mut("s").unwrap().jobs[0].argv[0] = "/nonexistent/compiler".into();
        let hold = store.hold(Operation::new("intent removal", "a"));
        tick_holding(&mut engine, &sample, hold, || clock.advance(std::time::Duration::from_secs(4))).await.unwrap();
        assert!(engine.results["a"]["detail"].as_str().unwrap().starts_with("cannot launch compiler"));
        assert!(!engine.running.contains_key("b") && reason(&engine, "s", 1).starts_with(RUN_AHEAD_WAIT));
        let failed = last_record(&engine);
        let (_, settled_at, deadline, running) = launch_of(&failed, "a").unwrap();
        assert!(settled_at >= engine.clock.monotonic_secs() && deadline.is_some() && !running, "a ran ahead and left no running compile");
        // Through the next tick r still runs and h is kept, waiting for r alone.
        engine.tick(sample.clone()).await.unwrap();
        let kept = last_record(&engine);
        assert!(matches!(kept.aged.steps.first(), Some(Step::Kept { head, .. }) if head.id.is_job("h")));
        let recorded = &engine.host_memory.head().unwrap().recorded;
        assert!(!recorded.iter().any(|work| matches!(work, Work::Compile(Attempt { job, .. }) if job == "a")), "{recorded:?}");
        replays(&engine);
        // The same journal with a's launch leaving a running compile is rejected: the kept head omits it.
        let mut ran = failed.clone();
        for entry in &mut ran.admitted {
            if let Entry::Launched { job, running, .. } = entry {
                *running |= job == "a";
            }
        }
        let error = checked_with(&engine, ran).unwrap_err();
        assert!(error.contains(&format!("tick {}: the record's /aged/head/recorded", kept.tick)), "{error}");
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_held_refusal_save_before_the_first_admission_bounds_it() {
        let (mut engine, mut sample, clock, store) = running_r(13 * GIB).await;
        let holder = Claimant::new();
        engine.requested(holder.claim("held", GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "held")["state"], "granted");
        head_and_runners(&mut engine).await;
        // A claim whose process descends from the holder's overlaps its tree, so serve refuses it and saves.
        let overlapping = Claimant::new();
        sample.processes.insert(overlapping.0.id(), (holder.0.id(), 0));
        engine.requested(overlapping.claim("overlapping", GIB)).await.unwrap();
        let t = engine.clock.monotonic_secs();
        let hold = store.hold(Operation::new("claims", "serve"));
        tick_holding(&mut engine, &sample, hold, || clock.advance(std::time::Duration::from_secs(2))).await.unwrap();
        assert_eq!(claim_status(&engine, "overlapping")["state"], "failed");
        persisted_before_the_first_admission(&engine, t);
        drop((holder, overlapping));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_held_retire_before_the_first_admission_bounds_it() {
        let (mut engine, sample, clock, store) = run_ahead_case().await;
        publish_timed(&mut engine, "z", "done", &[("d", GIB, 4)], 0.0, 0).await;
        engine.requested(json!({"protocol":1,"op":"cancel","group_id":"done","reason":"retired by the test"})).await.unwrap();
        engine.completed.insert("done".into(), engine.clock.wall() - RETENTION);
        let t = engine.clock.monotonic_secs();
        let hold = store.hold(Operation::new("retire", "done"));
        tick_holding(&mut engine, &sample, hold, || clock.advance(std::time::Duration::from_secs(2))).await.unwrap();
        assert!(!engine.groups.contains_key("done"), "the group was retired in this tick");
        persisted_before_the_first_admission(&engine, t);
        finish_fixture(engine, sample).await;
    }

    /// The tick's pre-launch sequence completed 2 s after t: a, admitted then, would finish after r's t + 5, so
    /// neither job runs ahead, and a record claiming a ran ahead from t is rejected.
    fn persisted_before_the_first_admission(engine: &Engine, t: f64) {
        assert_eq!(head(engine).as_deref(), Some("h"));
        assert!(!engine.running.contains_key("a") && !engine.running.contains_key("b"));
        assert!(reason(engine, "s", 0).starts_with(RUN_AHEAD_WAIT));
        let record = last_record(engine);
        let persisted = record.admitted.iter().find_map(|entry| match entry {
            Entry::Persisted { phase: Persisting::Serve, at, .. } => Some(*at),
            _ => None,
        });
        assert!(persisted.is_some_and(|at| at >= engine.clock.monotonic_secs()), "{persisted:?}");
        replays(engine);
        assert!(replay_error(&engine, &with_a_ahead_first(&record, t, t + 5.0)).contains("before the persistence boundary"));
    }

    #[tokio::test]
    async fn a_claim_that_ages_while_opens_save_is_held_is_judged_by_its_ticks_own_reading() {
        let (mut engine, mut sample, clock, store) = held_fixture().await;
        sample.available = 11 * GIB;
        publish_timed(&mut engine, "q", "running", &[("r", 4 * GIB, 60)], 0.0, 0).await;
        start(&mut engine, &mut sample, "r", 4 * GIB).await;
        let leaving = Claimant::new();
        engine.requested(leaving.claim("leaving", GIB / 2)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "leaving")["state"], "granted");
        let waiting = Claimant::new();
        engine.requested(waiting.claim("aging", 2 * GIB)).await.unwrap();
        clock.advance(std::time::Duration::from_secs_f64(precedence::AGED_SECS - 1.0));
        // The granted claim's owner exits, so open saves; while it is held, the claim passes its aging.
        drop(leaving);
        let hold = store.hold(Operation::new("claims", "open"));
        tick_holding(&mut engine, &sample, hold, || clock.advance(std::time::Duration::from_secs(2))).await.unwrap();
        assert_eq!(head(&engine), None, "by the tick's own reading the claim is younger than AGED_SECS");
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("aging"));
        assert_eq!(claim_status(&engine, "aging")["waiting"], format!("aged head, party {}'s turn: 2147483648 bytes reserved", waiting.party()));
        for (record, _) in &engine.journal.history {
            for step in &record.aged.steps {
                if let Step::Chosen { listed, .. } = step {
                    assert!(listed.iter().all(|entry| precedence::aged(entry.candidate.eligible_at.0, record.sampled_at)));
                }
            }
        }
        replays(&engine);
        drop(waiting);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn memory_freed_while_a_launch_write_is_held_is_admitted_only_from_the_next_tick() {
        let (mut engine, mut sample, _clock, store) = held_fixture().await;
        // The sample's ceiling is 16 GiB, and this engine's fresh reading 12 GiB until x has exited during the
        // tick: with 10 GiB headroom, the tick has 2 GiB.
        sample.available = 16 * GIB;
        let free = Arc::new(std::sync::atomic::AtomicU64::new(12 * GIB));
        let reading = free.clone();
        engine.read_memory = Arc::new(move || Ok((reading.load(std::sync::atomic::Ordering::SeqCst), 128 * GIB)));
        publish_timed(&mut engine, "x", "x", &[("x", 4 * GIB, 60)], 0.0, 0).await;
        start(&mut engine, &mut sample, "x", 4 * GIB).await;
        publish_timed(&mut engine, "j", "j", &[("j1", GIB, 60), ("j2", GIB, 60)], 0.0, 0).await;
        let owner = Claimant::new();
        engine.requested(owner.claim("waiting", 4 * GIB)).await.unwrap();
        let end_x = ender(&engine, "x");
        let hold = store.hold(Operation::new("pid", "j1"));
        let freed = move || {
            end_x();
            free.store(16 * GIB, std::sync::atomic::Ordering::SeqCst);
        };
        tick_holding(&mut engine, &sample, hold, freed).await.unwrap();
        assert_eq!((engine.read_memory)().unwrap().0, 16 * GIB, "a reading during the hold showed what x freed");
        assert!(engine.running.contains_key("j1") && engine.running.contains_key("j2"));
        let record = last_record(&engine);
        assert_eq!((record.available, record.left), (12 * GIB, Budget::of(2 * GIB)), "the tick's budget is its own reading's");
        assert_eq!(record.left_after_launch, 0, "j1 and j2 took the tick's 2 GiB");
        assert_eq!(claim_status(&engine, "waiting")["state"], "pending", "x's memory is not admitted in its tick");
        replays(&engine);
        // The next tick reaps x, and its fresh reading shows the 4 GiB x freed.
        engine.tick(sample.clone()).await.unwrap();
        assert!(!engine.running.contains_key("x"));
        assert_eq!(last_record(&engine).available, 16 * GIB);
        assert_eq!(claim_status(&engine, "waiting")["state"], "granted");
        replays(&engine);
        drop(owner);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_compile_that_exits_while_a_run_ahead_write_is_held_counts_until_the_next_reap_and_the_head_launches_later() {
        let (mut engine, mut sample, _clock, store) = run_ahead_case().await;
        let t = engine.clock.monotonic_secs();
        let end_r = ender(&engine, "r");
        let hold = store.hold(Operation::new("intent", "a"));
        tick_holding(&mut engine, &sample, hold, end_r).await.unwrap();
        // The deadline still counts r in its tick: b runs ahead within it, and h does not launch.
        assert!(!engine.running.contains_key("h") && head(&engine).as_deref() == Some("h"));
        let record = last_record(&engine);
        for job in ["a", "b"] {
            let (admitted_at, _, deadline, _) = launch_of(&record, job).unwrap();
            assert!(deadline.is_some_and(|deadline| admitted_at + 4.0 <= deadline && close_to(deadline, t + 5.0)), "{job}");
        }
        assert!(engine.deadline().is_some_and(|deadline| close_to(deadline, t + 5.0)));
        replays(&engine);
        // The next tick's reap removes r from the deadline and from D together.
        engine.tick(sample.clone()).await.unwrap();
        assert!(!engine.running.contains_key("r"));
        assert!(engine.deadline().is_some_and(|deadline| close_to(deadline, t + 4.0)), "a's and b's finishes are left");
        let counted = last_record(&engine).live();
        assert!(!counted.iter().any(|work| matches!(work, Work::Compile(Attempt { job, .. }) if job == "r")), "{counted:?}");
        // Once its named work has ended and the memory it held is free, h fits and launches at its party's slot.
        end(&mut engine, "a");
        end(&mut engine, "b");
        sample.available = 16 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("h"), "{:?}", reason(&engine, "head", 0));
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_failed_spawn_orders_its_party_by_its_launch_time_at_the_ticks_next_selection() {
        let (mut engine, mut sample, _clock, store) = held_fixture().await;
        cores(&mut sample, 6);
        sample.idle = 2;
        sample.available = 40 * GIB;
        publish(&mut engine, "p", "first", &[("p1", GIB)], 1000.0, 0).await;
        publish(&mut engine, "p", "second", &[("p2", GIB)], 900.0, 0).await;
        publish(&mut engine, "q", "other", &[("q1", GIB)], 0.0, 0).await;
        engine.groups.get_mut("first").unwrap().jobs[0].argv[0] = "/nonexistent/compiler".into();
        let served = engine.clock.wall() - 10.0;
        engine.served.insert("q".into(), served);
        let hold = store.hold(Operation::new("results", "p1"));
        tick_holding(&mut engine, &sample, hold, || ()).await.unwrap();
        assert_eq!(engine.results["p1"]["outcome"], "fail", "p's first launch failed to spawn");
        assert!(engine.running.contains_key("q1"), "q's turn of 10 s ago is older than p's failed launch");
        assert!(!engine.running.contains_key("p2"), "p is not ordered by its first group's publication");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// A shell in a unit of its own, and the pid of its `sleep` child: an owner that can transfer its claim.
    fn parent_of_a_child() -> (Claimant, u32) {
        use std::io::BufRead;
        let mut shell = std::process::Command::new("sh")
            .args(["-c", "sleep 60 & echo $!; wait"])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut line = String::new();
        std::io::BufReader::new(shell.stdout.take().unwrap()).read_line(&mut line).unwrap();
        let unit = format!("/user.slice/test.slice/parent-{}.scope", shell.id());
        (Claimant::placed(shell, unit), line.trim().parse().unwrap())
    }

    #[tokio::test]
    async fn a_claims_party_is_its_registrants_unit_and_never_changes() {
        let (mut engine, sample, _clock, store) = held_fixture().await;
        let (p, k) = parent_of_a_child();
        let unit = json!(p.party());
        publish(&mut engine, &p, "done", &[("d", GIB)], 0.0, 0).await;
        engine.requested(json!({"protocol":1,"op":"cancel","group_id":"done","reason":"completed for the test"})).await.unwrap();
        engine.completed.insert("done".into(), engine.clock.wall() - RETENTION);
        engine.requested(p.claim("p", GIB)).await.unwrap();
        let party = |engine: &Engine| claim_status(engine, "p")["party"].clone();
        assert_eq!(party(&engine), unit, "before any tick");
        tick_holding(&mut engine, &sample, store.hold(Operation::new("retire", "done")), || ()).await.unwrap();
        assert!(!engine.groups.contains_key("done"), "the tick's retire removed its party's last group");
        assert_eq!(party(&engine), unit, "in the tick whose retire removed the group");
        for _ in 0..2 {
            engine.tick(sample.clone()).await.unwrap();
        }
        assert_eq!(party(&engine), unit, "two ticks later");
        let moved = "/user.slice/test.slice/moved.scope";
        units().place(p.0.id(), crate::procs::started(p.0.id()).unwrap(), moved);
        for _ in 0..3 {
            engine.tick(sample.clone()).await.unwrap();
        }
        assert_eq!(party(&engine), unit, "three ticks after its owner moved to another unit");
        let birth = crate::procs::started(k).unwrap();
        units().place(k, birth, moved);
        let transfer = json!({"protocol":1,"op":"host_memory_transfer","claim_id":"p","to":{"pid":k,"birth":birth}});
        engine.requested((transfer, Peer::opened(p.0.id()))).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(party(&engine), unit, "a tick after its transfer to a process in another unit");
        replays(&engine);
        crate::procs::Process { pid: k, birth }.signal(libc::SIGKILL);
        drop(p);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_strict_removal_that_finds_no_running_record_fails_the_tick() {
        let (mut engine, mut sample, _clock, store) = held_fixture().await;
        publish_timed(&mut engine, "x", "x", &[("x", GIB, 60)], 0.0, 0).await;
        start(&mut engine, &mut sample, "x", GIB).await;
        end(&mut engine, "x");
        let path = engine.config.state.join("running").join("x.json");
        let hold = store.hold(Operation::new("reaped running record", "x"));
        let removed = path.clone();
        let error = tick_holding(&mut engine, &sample, hold, move || std::fs::remove_file(removed).unwrap()).await.unwrap_err();
        assert_eq!(error, std::fs::remove_file(&path).unwrap_err().to_string(), "NotFound propagates as today");
        std::fs::remove_dir_all(&engine.config.state).unwrap();
    }

    #[tokio::test]
    async fn a_strict_removal_that_finds_no_running_record_fails_restore() {
        let (mut engine, mut sample) = admission_fixture().await;
        publish_timed(&mut engine, "x", "x", &[("x", GIB, 60)], 0.0, 0).await;
        start(&mut engine, &mut sample, "x", GIB).await;
        let record = engine.running["x"].record.clone();
        end(&mut engine, "x");
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(engine.results["x"]["outcome"], "success");
        // A predecessor ended between x's durable result and its running record's removal.
        let path = engine.config.state.join("running").join("x.json");
        wrote(&path, &record).await;
        let (config, state) = (engine.handoff_config(), engine.config.state.clone());
        drop(engine);
        let store = Store::default();
        let mut hold = store.hold(Operation::new("finished running record", "x"));
        let removed = path.clone();
        let control = async move {
            entered(&mut hold).await;
            std::fs::remove_file(removed).unwrap();
            hold.release();
        };
        let (restored, ()) = tokio::join!(Engine::restore(config, Clock::system(), store, Instance::current().unwrap(), attribution()), control);
        let not_found = std::fs::remove_file(&path).unwrap_err().to_string();
        assert_eq!(restored.err(), Some(not_found), "restore fails before any tick");
        std::fs::remove_dir_all(state).unwrap();
    }

    #[tokio::test]
    async fn restore_creates_a_missing_state_directory_through_its_store_and_fails_with_the_creations_error() {
        let (engine, _) = admission_fixture().await;
        let (config, state) = (engine.handoff_config(), engine.config.state.clone());
        drop(engine);
        let running = state.join("running");
        std::fs::remove_dir(&running).unwrap();
        let store = Store::default();
        let mut hold = store.hold(Operation::new("directory", "running"));
        let control = async {
            entered(&mut hold).await;
            assert!(!running.exists(), "a held creation has not run");
            hold.release();
        };
        let (restored, ()) = tokio::join!(Engine::restore(config, Clock::system(), store, Instance::current().unwrap(), attribution()), control);
        let config = restored.unwrap().activated().await.unwrap().handoff_config();
        assert!(running.is_dir());
        // A file where the results directory belongs.
        let results = state.join("results");
        std::fs::remove_dir(&results).unwrap();
        std::fs::write(&results, b"").unwrap();
        let exists = std::fs::create_dir_all(&results).unwrap_err().to_string();
        let restored = Engine::restore(config, Clock::system(), Store::default(), Instance::current().unwrap(), attribution()).await;
        assert_eq!(restored.err(), Some(exists), "restore fails with the creation's error as it is formatted today");
        std::fs::remove_dir_all(state).unwrap();
    }

    // Part B, B-3's scenarios under generation 19: a GPU-place claim is ordinary (`Class::of`).

    const WAITING_FOR_RAM: &str = "waiting for RAM: needs";

    /// No tick so far put a claim in the GPU pass or reserved for one, and health shows no GPU head.
    fn no_gpu_turn(engine: &Engine) {
        assert!(engine.journal.history.iter().all(|(record, _)| record.gpu_reserved == 0 && record.gpu.is_empty()));
        assert!(engine.host_memory.heads()["gpu"].is_null());
    }

    /// 3 GiB left beside three running 2 GiB compiles, so 6 GiB the service's own work frees, or beside three
    /// granted 2 GiB host-only claims when `claims`.
    async fn three_gib_left_beside_six_of_own_work(engine: &mut Engine, sample: &mut Snapshot, claims: bool) -> Vec<Claimant> {
        cores(sample, 6);
        sample.available = 19 * GIB;
        match claims {
            false => {
                publish(engine, "q", "running", &[("r0", 2 * GIB), ("r1", 2 * GIB), ("r2", 2 * GIB)], 0.0, 0).await;
                for id in ["r0", "r1", "r2"] {
                    start(engine, sample, id, 0).await;
                }
                Vec::new()
            }
            true => {
                let owners: Vec<Claimant> = (0..3).map(|_| Claimant::new()).collect();
                for (i, owner) in owners.iter().enumerate() {
                    engine.requested(owner.host_only(&format!("h{i}"), 2 * GIB)).await.unwrap();
                }
                engine.tick(sample.clone()).await.unwrap();
                owners
            }
        }
    }

    #[tokio::test]
    async fn compiles_start_from_what_is_left_beside_a_waiting_gpu_place_claim() {
        let (mut engine, mut sample) = admission_fixture().await;
        three_gib_left_beside_six_of_own_work(&mut engine, &mut sample, false).await;
        let queue: Vec<String> = (0..4).map(|i| format!("q{i}")).collect();
        publish(&mut engine, "s", "small", &queue.iter().map(|id| (id.as_str(), GIB)).collect::<Vec<_>>(), 0.0, 0).await;
        let place = Claimant::new();
        engine.requested(place.gpu_place("place", 4 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert!(["q0", "q1"].iter().all(|job| engine.running.contains_key(*job)), "1 GiB compiles start from the 3 GiB left");
        let waits = reason(&engine, "small", 2);
        assert!(waits.starts_with("waiting for CPU capacity"), "q2 waits for a core, not for memory: {waits}");
        assert!(claim_status(&engine, "place")["waiting"].as_str().unwrap().starts_with(WAITING_FOR_RAM));
        no_gpu_turn(&engine);
        replays(&engine);
        drop(place);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn younger_host_claims_that_fit_are_granted_while_a_gpu_place_claim_waits() {
        let (mut engine, mut sample) = admission_fixture().await;
        let owners = three_gib_left_beside_six_of_own_work(&mut engine, &mut sample, true).await;
        let place = Claimant::new();
        engine.requested(place.gpu_place("place", 4 * GIB)).await.unwrap();
        let arriving: Vec<Claimant> = (0..2).map(|_| Claimant::new()).collect();
        for (i, owner) in arriving.iter().enumerate() {
            engine.requested(owner.host_only(&format!("a{i}"), GIB)).await.unwrap();
            engine.tick(sample.clone()).await.unwrap();
            assert_eq!(claim_status(&engine, &format!("a{i}"))["state"], "granted");
            assert_eq!(claim_status(&engine, "place")["state"], "pending");
        }
        no_gpu_turn(&engine);
        replays(&engine);
        drop((owners, place, arriving));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn later_gpu_place_claims_that_fit_are_granted_while_earlier_ones_wait() {
        let (mut engine, mut sample) = admission_fixture().await;
        three_gib_left_beside_six_of_own_work(&mut engine, &mut sample, false).await;
        let owners: Vec<Claimant> = (0..4).map(|_| Claimant::new()).collect();
        engine.requested(owners[0].gpu_place("first", 4 * GIB)).await.unwrap();
        engine.requested(owners[1].gpu_place("second", 4 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        for id in ["first", "second"] {
            assert!(claim_status(&engine, id)["waiting"].as_str().unwrap().starts_with(WAITING_FOR_RAM));
        }
        for (i, owner) in owners[2..4].iter().enumerate() {
            engine.requested(owner.gpu_place(&format!("small-{i}"), GIB)).await.unwrap();
            engine.tick(sample.clone()).await.unwrap();
            assert_eq!(claim_status(&engine, &format!("small-{i}"))["state"], "granted");
        }
        no_gpu_turn(&engine);
        replays(&engine);
        drop(owners);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_gpu_place_claim_waits_as_ordinary_behind_an_older_ordinary_head_and_is_not_served_first() {
        let (mut engine, mut sample) = admission_fixture().await;
        let older = Claimant::new();
        let place = Claimant::new();
        publish(&mut engine, "q", "running", &[("w", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        // 2 GiB left beside w's unused 4 GiB cell, so S is 6 GiB: the older 3 GiB claim holds the aged head and
        // reserves, and the 2 GiB GPU-place claim, which would fit the 2 GiB left, waits behind it.
        sample.available = 16 * GIB;
        engine.requested(older.claim("older", 3 * GIB)).await.unwrap();
        engine.host_memory.backdate("older", 60.0);
        engine.requested(place.gpu_place("place", 2 * GIB)).await.unwrap();
        for _ in 0..2 {
            engine.tick(sample.clone()).await.unwrap();
            assert_eq!(head(&engine).as_deref(), Some("older"));
            assert_eq!(claim_status(&engine, "place")["state"], "pending");
            assert!(claim_status(&engine, "place")["waiting"].as_str().unwrap().starts_with(WAITING_FOR_RAM));
            let decision = engine.host_memory.decisions().remove(&Subject::from(&request_of(&engine, "place")));
            assert_eq!(decision, Some(Decisions::One(Decision::WaitingForTurn)));
        }
        end(&mut engine, "w");
        engine.tick(sample.clone()).await.unwrap();
        let admitted: Vec<RequestId> = engine.journal.history.last().unwrap().0.admissions().collect();
        let claims = ["older", "place"].map(|claim| request_of(&engine, claim));
        assert_eq!(admitted, claims, "the head is granted first, then the GPU-place claim from what is left");
        no_gpu_turn(&engine);
        replays(&engine);
        drop((older, place));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_large_gpu_place_claim_gets_no_precedence_and_no_reservation() {
        let (mut engine, mut sample) = admission_fixture().await;
        three_gib_left_beside_six_of_own_work(&mut engine, &mut sample, false).await;
        let large = Claimant::new();
        engine.requested(large.gpu_place("large", 12 * GIB)).await.unwrap();
        for _ in 0..3 {
            engine.tick(sample.clone()).await.unwrap();
        }
        assert_eq!(claim_status(&engine, "large")["waiting"], LARGE_WAIT);
        no_gpu_turn(&engine);
        assert!(heads_seen(&engine).is_empty());
        replays(&engine);
        drop(large);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn an_unrecoverable_gpu_place_claim_reserves_nothing_and_leaves_the_aged_head_its_due() {
        let (mut engine, mut sample) = admission_fixture().await;
        let waiting = Claimant::new();
        let other = Claimant::new();
        let ninfer = Claimant::new();
        submit_party_group(&mut engine, &other, "live", &[], 0).await;
        publish(&mut engine, "q", "running", &[("w", 2 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        // A granted GPU-place claim waits on NInfer: left + D is 3 GiB.
        sample.available = 17 * GIB;
        engine.requested(ninfer.gpu_place("on-ninfer", 4 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "on-ninfer")["state"], "granted");
        engine.requested(waiting.gpu_place("place", 4 * GIB)).await.unwrap();
        engine.requested(other.host_only("aged", 2 * GIB)).await.unwrap();
        engine.host_memory.backdate("aged", 60.0);
        engine.tick(sample.clone()).await.unwrap();
        let record = &engine.journal.history.last().unwrap().0;
        assert_eq!((record.gpu_reserved, record.serviceable), (0, 3 * GIB));
        assert!(claim_status(&engine, "place")["waiting"].as_str().unwrap().starts_with(WAITING_FOR_RAM));
        assert_eq!(head(&engine).as_deref(), Some("aged"), "the aged head is judged against left + D alone");
        end(&mut engine, "w");
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "aged")["state"], "granted");
        no_gpu_turn(&engine);
        replays(&engine);
        drop((waiting, other, ninfer));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn only_the_aged_head_reserves_beside_a_gpu_place_claim_and_a_short_job_runs_ahead_into_its_bytes() {
        let (mut engine, mut sample) = admission_fixture().await;
        cores(&mut sample, 6);
        let place = Claimant::new();
        let younger = Claimant::new();
        submit_party_group(&mut engine, &younger, "live", &[], 0).await;
        publish_timed(&mut engine, "q", "running", &[("w", 6 * GIB, 60)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        sample.available = 18 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        publish_timed(&mut engine, "p", "head", &[("head", 3 * GIB, 60)], 120.0, 0).await;
        publish_timed(&mut engine, "s", "short", &[("short", GIB, 10)], 0.0, 0).await;
        engine.requested(place.gpu_place("place", 4 * GIB)).await.unwrap();
        engine.requested(younger.host_only("younger", GIB)).await.unwrap();
        // 2 GiB left and S 8 GiB: the 3 GiB job holds the aged head and reserves, which leaves nothing for the
        // GPU-place claim and the younger one, and the 1 GiB job declared to end before w runs ahead into its bytes.
        engine.tick(sample.clone()).await.unwrap();
        let record = engine.journal.history.last().unwrap().0.clone();
        assert_eq!((record.gpu_reserved, record.aged_reserved), (0, 3 * GIB));
        assert_eq!(head(&engine).as_deref(), Some("head"));
        for id in ["place", "younger"] {
            assert_eq!(claim_status(&engine, id)["state"], "pending");
        }
        assert!(engine.running.contains_key("short"), "the 2 GiB left hold the short job");
        end(&mut engine, "w");
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("head"));
        no_gpu_turn(&engine);
        replays(&engine);
        drop((place, younger));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn an_aged_gpu_place_claim_holds_the_aged_head_and_gives_way_once_its_recorded_work_ended_in_vain() {
        let (mut engine, mut sample) = admission_fixture().await;
        let place = Claimant::new();
        let small = Claimant::new();
        publish(&mut engine, "q", "named", &[("named", 4 * GIB)], 0.0, 0).await;
        publish(&mut engine, "q", "later", &[("later", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "named", 0).await;
        sample.available = 16 * GIB;
        engine.requested(place.gpu_place("place", 6 * GIB)).await.unwrap();
        engine.host_memory.backdate("place", 60.0);
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("place"), "an ordinary claim's protection, recording named");
        start(&mut engine, &mut sample, "later", 0).await;
        sample.available += 4 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        end(&mut engine, "named");
        sample.available -= 4 * GIB;
        engine.requested(small.gpu_place("small", GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        let record = &engine.journal.history.last().unwrap().0;
        assert!(matches!(record.aged.steps.first(), Some(Step::Kept { verdict: Verdict::EndedInVain, .. })));
        assert_eq!(record.aged_reserved, 0);
        assert_eq!(claim_status(&engine, "small")["state"], "granted", "younger work uses its bytes");
        engine.tick(sample.clone()).await.unwrap();
        let placed = request_of(&engine, "place");
        assert!(engine.host_memory.gives_way(&placed), "until the work then running ends");
        assert_eq!(engine.journal.history.last().unwrap().0.aged_reserved, 0);
        end(&mut engine, "later");
        sample.available -= 4 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        assert!(!engine.host_memory.gives_way(&placed));
        no_gpu_turn(&engine);
        replays(&engine);
        drop((place, small));
        finish_fixture(engine, sample).await;
    }

    /// A restarted service's restore of `state`.
    async fn restored(state: &std::path::Path) -> Result<Engine, String> {
        let config = Config { state: state.to_path_buf(), minimum_ram: 10 << 30, average_ram: 1 << 30, overcommit_percent: super::super::OVERCOMMIT_PERCENT, limits: Default::default() };
        Engine::restore(config, Clock::system(), Store::default(), Instance::current().unwrap(), attribution()).await?.activated().await
    }
    /// An empty state directory of its own.
    fn empty_state(name: &str) -> PathBuf {
        let state = std::env::temp_dir().join(format!(
            "compiler-{name}-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        std::fs::create_dir(&state).unwrap();
        state
    }

    #[tokio::test]
    async fn a_failed_growth_map_read_refuses_restore_and_keeps_every_cap() {
        use super::super::memory_growth::STEP;
        let (mut engine, _) = admission_fixture().await;
        publish(&mut engine, "a", "g", &[("j", GIB)], 0.0, 0).await;
        let state = engine.config.state.clone();
        let growth = state.join("growth.json");
        wrote(&growth, &json!({"j": GIB + STEP})).await;
        drop(engine);

        let fault = Unreadable::at(&growth);
        let refused = match restored(&state).await {
            Ok(engine) => panic!("the faulted restore answered Ok with limit {}", engine.growth.cell("j", Cell::whole(GIB)).limit),
            Err(refused) => refused,
        };
        assert_eq!(refused, format!("{}: Permission denied (os error 13)", growth.display()));
        fault.lift();
        let engine = restored(&state).await.unwrap();
        assert_eq!(engine.growth.cell("j", Cell::whole(GIB)).limit, GIB + STEP, "the healthy restore keeps the cap");
        drop(engine);

        std::fs::remove_file(&growth).unwrap();
        let engine = restored(&state).await.unwrap();
        assert_eq!(engine.growth.cell("j", Cell::whole(GIB)).limit, GIB, "an absent growth map restores no cap, as today");
        drop(engine);
        std::fs::remove_dir_all(state).unwrap();
    }

    #[tokio::test]
    async fn a_failed_manifest_read_refuses_restore_naming_its_error() {
        let (engine, _) = admission_fixture().await;
        let state = engine.config.state.clone();
        drop(engine);
        let manifest = state.join("format.json");
        let fault = Unreadable::at(&manifest);
        let refused = restored(&state).await.err();
        assert_eq!(refused, Some(format!("{}: Permission denied (os error 13)", manifest.display())));
        fault.lift();
        restored(&state).await.unwrap();
        std::fs::remove_dir_all(state).unwrap();

        let empty = empty_state("manifest-empty");
        restored(&empty).await.unwrap();
        let written = store::read(&empty.join("format.json")).await.unwrap();
        assert_eq!(written, json!({"format":"rust-compiler-queue","version":2}), "a first start writes the manifest");
        std::fs::remove_dir_all(empty).unwrap();
        let other = empty_state("manifest-other");
        std::fs::write(other.join("other.json"), b"{}\n").unwrap();
        let refused = restored(&other).await.err();
        assert_eq!(refused.as_deref(), Some("compiler state directory must be empty on first start"));
        std::fs::remove_dir_all(other).unwrap();
    }

    /// A reaped process of this boot.
    fn reaped() -> Instance {
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        let start_ticks = identity(pid).unwrap().0;
        child.wait().unwrap();
        Instance { boot_id: Instance::current().unwrap().boot_id, pid, start_ticks }
    }
    /// Restored `g`, its job `x` left to `reaper`.
    async fn left_to(reaper: &Instance) -> (Engine, Snapshot) {
        let (mut engine, sample) = admission_fixture().await;
        publish(&mut engine, "a", "g", &[("x", GIB)], 0.0, 0).await;
        let compiler = reaped();
        let record = json!({"group_id":"g","job_id":"x","boot_id":compiler.boot_id,"pid":compiler.pid,
            "start_ticks":compiler.start_ticks,"cpu":sample.cores[0],"memory_bytes":GIB,"job":engine.groups["g"].jobs[0],
            "log_path":engine.config.state.join("logs").join("x.log"),"queued_at":0.0,"started_at":engine.clock.wall(),
            "started_monotonic":engine.clock.monotonic_secs(),"exit_reaper":reaper});
        let state = engine.config.state.clone();
        wrote(&state.join("running").join("x.json"), &record).await;
        let receipt = json!({"job_id":"x","pid":compiler.pid,"returncode":3});
        wrote(&state.join("reaper-exits").join("x.json"), &receipt).await;
        drop(engine);
        let mut engine = restored(&state).await.unwrap();
        engine.read_memory = sample_alone();
        (engine, sample)
    }

    #[tokio::test]
    async fn a_failed_receipt_read_leaves_its_attempt_undecided_until_a_later_pass_records_its_exit_code() {
        let receipt = |engine: &Engine| engine.config.state.join("reaper-exits").join("x.json");
        let undecided = |engine: &Engine, case: &str| {
            let decided = engine.results.get("x").unwrap_or(&Value::Null);
            assert!(engine.running.contains_key("x") && decided.is_null(), "{case}: the faulted tick decided x: {decided}");
        };

        // (a) Reaper gone before restore: nothing awaited.
        let (mut engine, sample) = left_to(&reaped()).await;
        assert!(engine.inherited_reaping_ids().is_empty(), "(a) restore awaits no reaper");
        let fault = Unreadable::at(&receipt(&engine));
        engine.tick(sample.clone()).await.unwrap();
        undecided(&engine, "(a)");
        fault.lift();
        engine.tick(sample.clone()).await.unwrap();
        let result = &engine.results["x"];
        assert_eq!((&result["returncode"], &result["outcome"]), (&json!(3), &json!("fail")), "(a)");
        finish_fixture(engine, sample).await;

        // (b) Reaper alive at restore, gone before the tick.
        let mut sleeper = std::process::Command::new("sleep").arg("60").spawn().unwrap();
        let birth = identity(sleeper.id()).unwrap().0;
        let reaper = Instance { boot_id: Instance::current().unwrap().boot_id, pid: sleeper.id(), start_ticks: birth };
        let (mut engine, sample) = left_to(&reaper).await;
        assert!(engine.inherited_reaping_ids().contains("x"), "(b) restore awaits the live reaper");
        sleeper.kill().unwrap();
        sleeper.wait().unwrap();
        let fault = Unreadable::at(&receipt(&engine));
        engine.tick(sample.clone()).await.unwrap();
        undecided(&engine, "(b)");
        assert!(engine.inherited_reaping_ids().contains("x"), "(b) the reaper-gone transition ran");
        fault.lift();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(engine.results["x"]["returncode"], 3, "(b)");
        finish_fixture(engine, sample).await;

        // (c) No receipt, reaper gone: lost, as today.
        let (mut engine, sample) = left_to(&reaped()).await;
        std::fs::remove_file(receipt(&engine)).unwrap();
        engine.tick(sample.clone()).await.unwrap();
        let result = &engine.results["x"];
        assert_eq!((&result["outcome"], &result["returncode"]), (&json!("lost"), &Value::Null), "(c)");
        finish_fixture(engine, sample).await;
    }

    /// What `serve` answers `request`, as the connection would receive it.
    async fn served(engine: &mut Engine, asking: impl Asking) -> Result<Value, String> {
        let (request, peer) = asking.split();
        let (reply, answer) = tokio::sync::oneshot::channel();
        engine.serve(request, peer, reply).await;
        answer.await.expect("served requests are answered")
    }

    #[tokio::test]
    async fn a_stopped_store_refuses_every_request_at_the_engine() {
        let (mut engine, sample) = admission_fixture().await;
        let owner = Claimant::new();
        engine.requested(owner.host_only("c", GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "c")["state"], "granted");
        publish(&mut engine, "p", "g", &[("g-job", GIB)], 0.0, 1).await;
        let state = engine.config.state.clone();
        let unreadable = Mode::set(&state, 0o300);
        let stopped = served(&mut engine, owner.release("c")).await.unwrap_err();
        assert!(stopped.contains("could not be synced"), "{stopped}");
        for request in [json!({"protocol":1,"op":"health"}), json!({"protocol":1,"op":"group_status","group_id":"g"})] {
            assert_eq!(served(&mut engine, request.clone()).await, Err(stopped.clone()), "{request}");
        }
        drop(unreadable);
        std::fs::remove_dir_all(state).unwrap();
    }

    const WITHIN: std::time::Duration = std::time::Duration::from_secs(10);
    /// A successor restored from `engine`'s state, as a restart or a hand-over takes it.
    async fn restore_of(engine: &Engine) -> Engine {
        Engine::restore(engine.handoff_config(), Clock::system(), Store::default(), Instance::current().unwrap(), attribution()).await.unwrap().activated().await.unwrap()
    }
    /// The submission of group g, one job running coreutils `true` (16 MiB, a 10 s runtime), naming `party`.
    fn submission(state: &std::path::Path, party: &str) -> Value {
        json!({"protocol":1,"op":"submit_group","party":party,"group":{"group_id":"g","jobs":[
            {"job_id":"g-true","argv":["true"],"cwd":state,"memory_bytes":16u64 << 20,"expected_runtime_secs":10}]}})
    }
    /// g's record as `groups/g.json` holds it.
    fn record_of_g(engine: &Engine) -> Value {
        serde_json::from_slice(&std::fs::read(engine.config.state.join("groups").join("g.json")).unwrap()).unwrap()
    }
    /// The party health's subqueue shows for g.
    async fn subqueue_party_of_g(engine: &mut Engine) -> Value {
        let health = engine.requested(json!({"protocol":1,"op":"health"})).await.unwrap();
        health["subqueues"].as_array().unwrap().iter().find(|subqueue| subqueue["group_id"] == "g").unwrap()["party"].clone()
    }

    #[tokio::test]
    async fn a_group_whose_record_is_not_published_does_not_exist_and_its_retry_is_saved_whole() {
        let (mut engine, sample) = admission_fixture().await;
        let state = engine.config.state.clone();
        let groups = state.join("groups");
        let unwritable = Mode::set(&groups, 0o500);
        let refused = engine.requested(submission(&state, "p")).await;
        drop(unwritable);
        assert!(refused.is_err(), "the staging write fails before any rename: {refused:?}");
        let status = engine.requested(json!({"protocol":1,"op":"group_status","group_id":"g"})).await;
        assert_eq!(status, Err("unknown compiler group".into()));
        assert_eq!(std::fs::read_dir(&groups).unwrap().count(), 0, "groups/ holds no g.json and no staging file");
        assert!(!restore_of(&engine).await.groups.contains_key("g"), "a restore holds no g");
        let answer = engine.requested(submission(&state, "p")).await.unwrap();
        assert_eq!((&answer["existing"], &answer["party"]), (&json!(false), &json!("p")));
        let record = record_of_g(&engine);
        assert_eq!(
            (&record["group"]["group_id"], &record["party"], &record["priority"]),
            (&json!("g"), &json!("p"), &json!(0)),
            "groups/g.json decodes as the record: {record}"
        );
        assert!(!state.join("parties").join("g.json").exists(), "no party file is written");
        let restored = restore_of(&engine).await;
        assert_eq!((restored.parties.of("g"), restored.parties.priority("g")), ("p", 0));
        assert_eq!(restored.submitted["g"], engine.submitted["g"], "both are the record's modification time");
        finish_fixture(restored, sample).await;
    }

    #[tokio::test]
    async fn a_group_record_published_but_not_synced_is_published_again_and_kept() {
        let store = Store::default();
        let (mut engine, sample) = admission_fixture_on(Clock::system(), store.clone()).await;
        let state = engine.config.state.clone();
        let mut hold = store.hold(Operation::new("group", "resave g"));
        let unreadable = Mode::set(&state.join("groups"), 0o300);
        let (answer, ()) = tokio::join!(engine.requested(submission(&state, "p")), async {
            tokio::time::timeout(WITHIN, hold.entered()).await.expect("the re-publication is entered within 10 s");
            drop(unreadable);
            hold.release();
        });
        let answer = answer.unwrap();
        assert_eq!((&answer["existing"], &answer["party"]), (&json!(false), &json!("p")));
        assert_eq!(store.serving(), Ok(()));
        assert_eq!(engine.requested(submission(&state, "q")).await.unwrap()["existing"], true);
        assert_eq!(subqueue_party_of_g(&mut engine).await, "p", "health shows g's party");
        let restored = restore_of(&engine).await;
        assert_eq!(restored.parties.of("g"), "p", "a restore shows g's party");
        assert_eq!(restored.submitted["g"], engine.submitted["g"], "the modification time the re-publication gave");
        finish_fixture(restored, sample).await;
    }

    #[tokio::test]
    async fn a_group_record_that_cannot_be_made_durable_stops_the_store_and_restores_with_its_party() {
        let store = Store::default();
        let (mut engine, sample) = admission_fixture_on(Clock::system(), store.clone()).await;
        let state = engine.config.state.clone();
        let unreadable = Mode::set(&state.join("groups"), 0o300);
        let stopped = served(&mut engine, submission(&state, "p")).await.unwrap_err();
        assert!(stopped.contains("could not be synced"), "{stopped}");
        assert_eq!(store.serving(), Err(stopped.clone()));
        let retry = served(&mut engine, submission(&state, "q")).await;
        assert_eq!(retry, Err(stopped), "a failure after the publication is fatal: the engine serves nothing more");
        drop(unreadable);
        assert_eq!(record_of_g(&engine)["party"], "p", "the published record keeps its party");
        let mut restored = restore_of(&engine).await;
        assert_eq!(restored.parties.of("g"), "p", "a restore holds g with its party");
        assert_eq!(restored.requested(submission(&state, "q")).await.unwrap()["existing"], true);
        assert_eq!(subqueue_party_of_g(&mut restored).await, "p");
        finish_fixture(restored, sample).await;
    }

    /// The last line a test body printed in a run of the test binary, without the harness's framing: its `running`,
    /// `test <name> ...`, `ok` and `test result` lines.
    fn said_last(printed: &str) -> Option<&str> {
        let framing = ["running ", "test result:", "ok", "failures:", "successes:"];
        printed
            .lines()
            .map(|line| line.rsplit_once(" ... ").map_or(line, |(_, said)| said))
            .rfind(|line| !line.is_empty() && !framing.iter().any(|frame| line.starts_with(frame)))
    }

    /// The test's exact name, by which it runs its own child.
    #[cfg(target_arch = "x86_64")]
    const FAULT_TEST: &str = "compiler::engine::tests::a_fault_after_a_groups_publication_never_lets_a_retry_reassign_its_party";

    #[cfg(target_arch = "x86_64")]
    #[test]
    #[ignore = "installs a seccomp filter, which a build sandbox may refuse; run with --ignored"]
    fn a_fault_after_a_groups_publication_never_lets_a_retry_reassign_its_party() {
        match std::env::var_os("PA_FAULT_CHILD") {
            Some(_) => faulted_publication(),
            None => fault_outer_run(),
        }
    }

    /// The outer run: the child under the fault, then two restores without it.
    #[cfg(target_arch = "x86_64")]
    fn fault_outer_run() {
        let state = std::env::temp_dir().join(format!("compiler-publication-fault-{}-{}", std::process::id(), crate::protocol::now_unix().to_bits()));
        std::fs::create_dir(&state).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", FAULT_TEST, "--ignored", "--nocapture", "--test-threads=1"])
            .env("PA_FAULT_CHILD", "1")
            .env("PA_FAULT_STATE", &state)
            .output()
            .unwrap();
        let printed = String::from_utf8_lossy(&output.stdout);
        assert!(output.status.success(), "the child exited {}: {printed}{}", output.status, String::from_utf8_lossy(&output.stderr));
        assert_eq!(said_last(&printed), Some("published group kept"), "{printed}");
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let mut restored = fault_engine(&state).await;
            assert_eq!(restored.parties.of("g"), "p", "the restore holds g with party p");
            assert_eq!(record_of_g(&restored)["party"], "p");
            assert_eq!(restored.requested(submission(&state, "q")).await.unwrap()["existing"], true);
            let again = fault_engine(&state).await;
            assert_eq!(again.parties.of("g"), "p", "a second restore still holds g with party p");
        });
        std::fs::remove_dir_all(state).unwrap();
    }

    /// An engine restored on `state`, as the fixtures restore one.
    #[cfg(target_arch = "x86_64")]
    async fn fault_engine(state: &std::path::Path) -> Engine {
        let config = Config { state: state.to_path_buf(), minimum_ram: 10 << 30, average_ram: 1 << 30, overcommit_percent: super::super::OVERCOMMIT_PERCENT, limits: Default::default() };
        Engine::restore(config, Clock::system(), Store::default(), Instance::current().unwrap(), attribution()).await.unwrap().activated().await.unwrap()
    }

    /// Answers EIO to every metadata read by path on the calling thread alone: statx and newfstatat without
    /// AT_EMPTY_PATH, stat and lstat. Reads through a descriptor stay allowed.
    #[cfg(target_arch = "x86_64")]
    fn metadata_by_path_fails_on_this_thread() {
        const AUDIT_ARCH_X86_64: u32 = 0xC000_003E;
        const ARCH: u32 = 4;
        const NR: u32 = 0;
        const ARG: fn(u32) -> u32 = |index| 16 + 8 * index;
        let load = |offset: u32| libc::sock_filter { code: (libc::BPF_LD | libc::BPF_W | libc::BPF_ABS) as u16, jt: 0, jf: 0, k: offset };
        let equal = |value: u32, jt: u8, jf: u8| libc::sock_filter { code: (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16, jt, jf, k: value };
        let set = |bits: u32, jt: u8, jf: u8| libc::sock_filter { code: (libc::BPF_JMP | libc::BPF_JSET | libc::BPF_K) as u16, jt, jf, k: bits };
        let answer = |value: u32| libc::sock_filter { code: (libc::BPF_RET | libc::BPF_K) as u16, jt: 0, jf: 0, k: value };
        let empty_path = libc::AT_EMPTY_PATH as u32;
        let mut program = [
            load(ARCH),                                     // 0
            equal(AUDIT_ARCH_X86_64, 0, 10),                // 1: another architecture: allow
            load(NR),                                       // 2
            equal(libc::SYS_stat as u32, 7, 0),             // 3: EIO
            equal(libc::SYS_lstat as u32, 6, 0),            // 4: EIO
            equal(libc::SYS_statx as u32, 1, 0),            // 5: its flags, at 7
            equal(libc::SYS_newfstatat as u32, 2, 5),       // 6: its flags, at 9; anything else: allow
            load(ARG(2)),                                   // 7: statx's flags
            set(empty_path, 3, 2),                          // 8: through a descriptor: allow; by path: EIO
            load(ARG(3)),                                   // 9: newfstatat's flags
            set(empty_path, 1, 0),                          // 10: through a descriptor: allow; by path: EIO
            answer(libc::SECCOMP_RET_ERRNO | libc::EIO as u32), // 11
            answer(libc::SECCOMP_RET_ALLOW),                // 12
        ];
        let filter = libc::sock_fprog { len: program.len() as u16, filter: program.as_mut_ptr() };
        // SAFETY: prctl and seccomp with a filter program that outlives the calls.
        assert_eq!(unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) }, 0, "prctl(PR_SET_NO_NEW_PRIVS): {}", std::io::Error::last_os_error());
        let installed = unsafe { libc::syscall(libc::SYS_seccomp, libc::SECCOMP_SET_MODE_FILTER, 0, &filter as *const libc::sock_fprog) };
        assert_eq!(installed, 0, "seccomp(SECCOMP_SET_MODE_FILTER): {}", std::io::Error::last_os_error());
    }

    /// The child: g is published while every metadata read by path on the engine's thread fails.
    #[cfg(target_arch = "x86_64")]
    fn faulted_publication() {
        let state = PathBuf::from(std::env::var_os("PA_FAULT_STATE").unwrap());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .thread_keep_alive(std::time::Duration::from_secs(600))
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut engine = fault_engine(&state).await;
            let meeting = Arc::new(std::sync::Barrier::new(4));
            let idle: Vec<_> = (0..4).map(|_| { let meeting = meeting.clone(); tokio::task::spawn_blocking(move || { meeting.wait(); }) }).collect();
            for thread in idle {
                thread.await.unwrap();
            }
            let own = Peer::opened(std::process::id());
            metadata_by_path_fails_on_this_thread();
            let control = std::fs::metadata(&state).map(|_| ()).map_err(|e| e.to_string());
            assert_eq!(control, Err("Input/output error (os error 5)".into()), "the fault is in place on the engine's thread");
            let elsewhere = state.clone();
            assert!(tokio::task::spawn_blocking(move || std::fs::metadata(elsewhere)).await.unwrap().is_ok(), "and on no other");
            let answer = engine.request(submission(&state, "p"), &own).await.unwrap();
            assert_eq!((&answer["existing"], &answer["party"]), (&json!(false), &json!("p")), "nothing read the record again");
            let status = engine.request(json!({"protocol":1,"op":"group_status","group_id":"g"}), &own).await.unwrap();
            assert_eq!(status["jobs"][0]["job_id"], "g-true");
            assert_eq!(engine.request(submission(&state, "q"), &own).await.unwrap()["existing"], true);
            println!("published group kept");
        });
    }

    /// The submission of group g, as `submission` builds it, naming no party.
    fn unnamed(state: &std::path::Path) -> Value {
        let mut request = submission(state, "");
        request.as_object_mut().unwrap().remove("party");
        request
    }

    #[tokio::test]
    async fn a_request_without_a_readable_sender_is_refused_by_its_reason() {
        let (mut engine, sample) = admission_fixture().await;
        let state = engine.config.state.clone();
        let failed = || Peer::failed(Unread::Failed("x".into()));
        let gone = engine.requested((unnamed(&state), Peer::failed(Unread::Gone))).await;
        assert_eq!(gone, Err("compiler submission needs a party or an authenticated submitter".into()));
        let unreadable = engine.requested((unnamed(&state), failed())).await;
        assert_eq!(unreadable, Err("compiler submission's submitter cannot be read: x".into()));
        let named = engine.requested((submission(&state, "p"), failed())).await.unwrap();
        assert_eq!(named["party"], "p", "a submission that names its party needs no sender");
        assert!(engine.requested((json!({"protocol":1,"op":"health"}), failed())).await.is_ok(), "health needs no sender");
        finish_fixture(engine, sample).await;
    }

    const QUEUED_TEST: &str = "compiler::engine::tests::a_queued_request_whose_sender_ended_is_refused_and_never_its_pids_next_holder";

    #[test]
    #[ignore = "needs unprivileged user and pid namespaces, which a build sandbox may not offer; run with --ignored"]
    fn a_queued_request_whose_sender_ended_is_refused_and_never_its_pids_next_holder() {
        match std::env::var_os("PA_IN_NAMESPACE") {
            Some(_) => queued_in_a_namespace(),
            None => queued_outer_run(),
        }
    }

    /// The outer run: this test again, in user and pid namespaces of its own, where it may choose the next pid.
    fn queued_outer_run() {
        let output = std::process::Command::new("unshare")
            .args(["--user", "--map-root-user", "--pid", "--fork", "--mount", "--mount-proc"])
            .arg(std::env::current_exe().unwrap())
            .args(["--exact", QUEUED_TEST, "--ignored", "--nocapture"])
            .env("PA_IN_NAMESPACE", "1")
            .output()
            .unwrap();
        let printed = String::from_utf8_lossy(&output.stdout);
        assert!(output.status.success(), "the inner run exited {}: {printed}{}", output.status, String::from_utf8_lossy(&output.stderr));
        assert_eq!(said_last(&printed), Some("queued request refused"), "{printed}");
    }

    /// Inside the namespaces: two requests of p are queued, p is reaped, and q is given p's pid before they are served.
    fn queued_in_a_namespace() {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let (mut engine, _) = admission_fixture().await;
            let state = engine.config.state.clone();
            let units = party::Units::default();
            engine.attribution = Attribution::reading(units.reader());
            let mut p = std::process::Command::new("sleep").arg("60").spawn().unwrap();
            units.place(p.id(), crate::procs::started(p.id()).unwrap(), "/u.scope");
            let sender = Peer::opened(p.id());
            let registration = json!({"protocol":1,"op":"host_memory_acquire","claim_id":"c","memory_bytes":8u64 << 20,"label":"queued"});
            let submitted = unnamed(&state);
            let pid = p.id();
            p.kill().unwrap();
            p.wait().unwrap();
            std::fs::write("/proc/sys/kernel/ns_last_pid", (pid - 1).to_string()).unwrap();
            let mut q = std::process::Command::new("sleep").arg("60").spawn().unwrap();
            assert_eq!(q.id(), pid, "q was given p's pid");
            units.place(q.id(), crate::procs::started(q.id()).unwrap(), "/v.scope");
            assert_eq!(engine.request(registration, &sender).await, Err("host memory owner is gone".into()));
            let health = engine.requested(json!({"protocol":1,"op":"health"})).await.unwrap();
            assert_eq!(health["host_memory"]["claims"], json!([]), "health lists no claim");
            let saved = std::fs::read(state.join("host-memory.json")).map(|bytes| serde_json::from_slice::<Value>(&bytes).unwrap());
            assert!(saved.map_or(true, |saved| saved["claims"] == json!({})), "host-memory.json holds no claim");
            let refused = engine.request(submitted, &sender).await;
            assert_eq!(refused, Err("compiler submission needs a party or an authenticated submitter".into()));
            let status = engine.requested(json!({"protocol":1,"op":"group_status","group_id":"g"})).await;
            assert_eq!(status, Err("unknown compiler group".into()));
            assert!(!state.join("groups").join("g.json").exists());
            assert_eq!(units.reads(), Vec::new(), "no cgroup was read while the queued requests were served");
            let control = json!({"protocol":1,"op":"host_memory_acquire","claim_id":"control","memory_bytes":8u64 << 20,"label":"control"});
            assert_eq!(engine.request(control, &Peer::opened(q.id())).await.unwrap()["state"], "pending");
            let saved: Value = serde_json::from_slice(&std::fs::read(state.join("host-memory.json")).unwrap()).unwrap();
            let birth = crate::procs::started(q.id()).unwrap();
            assert_eq!(saved["claims"]["control"]["owner"], json!({"pid": q.id(), "birth": birth}), "the control claim is q's");
            assert_eq!(saved["claims"]["control"]["party"], "/v.scope", "and its party q's unit");
            q.kill().unwrap();
            q.wait().unwrap();
            std::fs::remove_dir_all(state).unwrap();
            println!("queued request refused");
        });
    }
    /// A claimant in `unit`.
    fn member(unit: &str) -> Claimant {
        Claimant::placed(std::process::Command::new("sleep").arg("60").spawn().unwrap(), unit.to_owned())
    }
    /// A tick that leaves 1 GiB beside a running compile of `beside` bytes, so no claim fits and every one is
    /// serviceable: the head it holds.
    async fn starved_head(engine: &mut Engine, sample: &mut Snapshot, beside: u64) -> String {
        sample.available = GIB + 10 * GIB + beside + engine.host_memory.reserved(sample);
        engine.tick(sample.clone()).await.unwrap();
        head(engine).unwrap()
    }
    /// A tick with the head's bytes freed, which grants it.
    async fn head_granted(engine: &mut Engine, sample: &mut Snapshot, id: &str) {
        sample.available += claim_status(engine, id)["memory_bytes"].as_u64().unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(engine, id)["state"], "granted", "{id}");
    }
    /// Every aged choice of the run, in order: the request chosen and the party its listing names.
    fn choices(engine: &Engine) -> Vec<(RequestId, String)> {
        let steps = engine.journal.history.iter().flat_map(|(record, _)| record.aged.steps.iter());
        steps
            .filter_map(|step| match step {
                Step::Chosen { listed, chosen: id, .. } => {
                    Some((id.clone(), listed.iter().find(|entry| &entry.candidate.id == id).unwrap().party.clone()))
                }
                Step::Kept { .. } => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn a_groupless_campaign_holds_its_own_units_turn() {
        let (mut engine, mut sample) = admission_fixture().await;
        let unit = |name: &str| format!("/user.slice/test.slice/{name}.scope");
        let (e, s, w) = (unit("e"), unit("s"), unit("w"));
        let campaign = member(&e);
        publish(&mut engine, &campaign, "live-e", &[("live-e-job", GIB)], 0.0, 0).await;
        engine.requested(json!({"protocol":1,"op":"cancel","group_id":"live-e","reason":"held, then completed"})).await.unwrap();
        let other = member(&s);
        submit_party_group(&mut engine, &other, "live-s", &[], 0).await;
        let owners: Vec<(Claimant, &str, u64, f64)> = vec![
            (member(&e), "e1", 4 * GIB, 100.0),
            (member(&e), "e2", 4 * GIB, 90.0),
            (member(&w), "w1", 6 * GIB, 80.0),
            (member(&s), "s1", 4 * GIB, 70.0),
        ];
        let mut seen_w1 = BTreeSet::new();
        for (owner, id, bytes, age) in &owners {
            engine.requested(owner.host_only(id, *bytes)).await.unwrap();
            engine.host_memory.backdate(id, *age);
            let shown = engine.host_memory.health(None)["claims"].as_array().unwrap().iter().find(|claim| claim["claim_id"] == "w1").cloned();
            seen_w1.extend(shown.map(|claim| claim["party"].to_string()));
        }
        publish(&mut engine, "q", "running", &[("run", 6 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "run", 0).await;
        for step in 0..4 {
            let id = starved_head(&mut engine, &mut sample, 6 * GIB).await;
            seen_w1.insert(claim_status(&engine, "w1")["party"].to_string());
            if step == 0 {
                engine.completed.insert("live-e".into(), engine.clock.wall() - RETENTION);
            }
            head_granted(&mut engine, &mut sample, &id).await;
            seen_w1.insert(claim_status(&engine, "w1")["party"].to_string());
        }
        assert!(!engine.groups.contains_key("live-e"), "E's group retired in the tick e1 was granted");
        let party_of = BTreeMap::from([("e1", &e), ("e2", &e), ("w1", &w), ("s1", &s)]);
        for (record, _) in &engine.journal.history {
            let mut seen: Vec<(String, String)> = Vec::new();
            for step in &record.aged.steps {
                match step {
                    Step::Kept { head, .. } => seen.push((head.id.to_string(), head.party.clone())),
                    Step::Chosen { listed, .. } => {
                        seen.extend(listed.iter().map(|entry| (entry.candidate.id.to_string(), entry.party.clone())));
                        seen.extend(listed.iter().map(|entry| (entry.candidate.id.to_string(), entry.candidate.party.clone())));
                    }
                }
            }
            seen.extend(record.aged.head.iter().map(|head| (head.id.to_string(), head.party.clone())));
            seen.extend(record.admitted.iter().filter_map(|entry| match entry {
                Entry::Granted { claim, party, .. } => Some((claim.clone(), party.clone())),
                _ => None,
            }));
            for (id, party) in seen {
                if let Some(expected) = party_of.get(id.as_str()) {
                    assert_eq!(&&party, expected, "tick {}: {id} names its registrant's unit", record.tick);
                }
            }
        }
        let chosen: Vec<(String, String)> = choices(&engine).into_iter().map(|(id, party)| (id.to_string(), party)).collect();
        let expected: Vec<(String, String)> = [("e1", &e), ("s1", &s), ("w1", &w), ("e2", &e)].iter().map(|(id, party)| (id.to_string(), party.to_string())).collect();
        assert_eq!(chosen, expected, "the turns follow cyclic name order e, s, w, e");
        assert_eq!(seen_w1, BTreeSet::from([json!(w).to_string()]), "w1's party in every health read from its registration on");
        replays(&engine);
        drop((campaign, other, owners));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_submissions_party_is_its_submitters_unit_unless_it_names_one() {
        let (mut engine, sample) = admission_fixture().await;
        let state = engine.config.state.clone();
        let submitter = Claimant::new();
        let group = |id: &str| {
            json!({"protocol":1,"op":"submit_group","group":{"group_id":id,"jobs":[
                {"job_id":format!("{id}-true"),"argv":["true"],"cwd":state,"memory_bytes":16u64 << 20,"expected_runtime_secs":10}]}})
        };
        let unnamed = engine.requested((group("unnamed"), Peer::opened(submitter.0.id()))).await.unwrap();
        assert_eq!(unnamed["party"], json!(submitter.party()), "a submission naming no party takes its sender's unit");
        let mut naming = group("named");
        naming["party"] = json!("p");
        let named = engine.requested((naming, Peer::opened(submitter.0.id()))).await.unwrap();
        assert_eq!(named["party"], "p");
        engine.requested(submitter.host_only("claim", GIB)).await.unwrap();
        assert_eq!(claim_status(&engine, "claim")["party"], json!(submitter.party()), "its claim shares the unit");
        let health = engine.requested(json!({"protocol":1,"op":"health"})).await.unwrap();
        let shown: BTreeMap<String, Value> =
            health["subqueues"].as_array().unwrap().iter().map(|subqueue| (subqueue["group_id"].as_str().unwrap().to_owned(), subqueue["party"].clone())).collect();
        assert_eq!(shown, BTreeMap::from([("named".to_owned(), json!("p")), ("unnamed".to_owned(), json!(submitter.party()))]));
        for (id, party) in [("named", json!("p")), ("unnamed", json!(submitter.party()))] {
            let record: Value = serde_json::from_slice(&std::fs::read(state.join("groups").join(format!("{id}.json"))).unwrap()).unwrap();
            assert_eq!(record["party"], party, "{id}'s record holds its party");
        }
        let (gone, unreadable) = (Claimant::new(), Claimant::new());
        units().refuse(gone.0.id(), crate::procs::started(gone.0.id()).unwrap(), Unread::Gone);
        units().refuse(unreadable.0.id(), crate::procs::started(unreadable.0.id()).unwrap(), Unread::Failed("x".into()));
        let refused = engine.requested((group("gone"), Peer::opened(gone.0.id()))).await;
        assert_eq!(refused, Err("compiler submission needs a party or an authenticated submitter".into()));
        let refused = engine.requested((group("unreadable"), Peer::opened(unreadable.0.id()))).await;
        assert_eq!(refused, Err("compiler submission's submitter cannot be read: x".into()));
        drop((submitter, gone, unreadable));
        finish_fixture(engine, sample).await;
    }

    /// A `sleep` leading a session of its own.
    fn session_leader() -> std::process::Child {
        use std::os::unix::process::CommandExt;
        let mut command = std::process::Command::new("sleep");
        command.arg("60");
        // SAFETY: setsid in the child before exec, with no allocation.
        unsafe {
            command.pre_exec(|| match libc::setsid() {
                -1 => Err(std::io::Error::last_os_error()),
                _ => Ok(()),
            });
        }
        command.spawn().unwrap()
    }

    #[tokio::test]
    async fn a_campaigns_claims_and_groups_share_its_unit_with_or_without_a_held_group() {
        let (mut engine, mut sample) = admission_fixture().await;
        let campaign = Claimant::new();
        let unit = campaign.party();
        let front_ends: Vec<Claimant> = (0..3).map(|_| Claimant::placed(session_leader(), unit.clone())).collect();
        let outside = Claimant::new();
        let claim = |index: usize| front_ends[index].host_only(&format!("f{}", index + 1), 4 * GIB);
        engine.requested(claim(0)).await.unwrap();
        publish(&mut engine, &campaign, "campaign", &[("campaign-job", GIB)], 0.0, 0).await;
        let health = engine.requested(json!({"protocol":1,"op":"health"})).await.unwrap();
        assert_eq!(health["subqueues"][0]["party"], json!(unit), "the campaign's group");
        engine.requested(claim(1)).await.unwrap();
        engine.requested(json!({"protocol":1,"op":"cancel","group_id":"campaign","reason":"retires"})).await.unwrap();
        engine.completed.insert("campaign".into(), engine.clock.wall() - RETENTION);
        engine.tick(sample.clone()).await.unwrap();
        assert!(!engine.groups.contains_key("campaign"), "the group retired");
        engine.requested(claim(2)).await.unwrap();
        engine.requested(outside.host_only("o1", 4 * GIB)).await.unwrap();
        for (index, id) in ["f1", "f2", "f3", "o1"].iter().enumerate() {
            engine.host_memory.backdate(id, 100.0 - index as f64);
        }
        for id in ["f1", "f2", "f3"] {
            assert_eq!(claim_status(&engine, id)["party"], json!(unit), "{id}: sessions play no part");
        }
        publish(&mut engine, "q", "running", &[("run", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "run", 0).await;
        let turns = [("f1", unit.clone()), ("o1", outside.party()), ("f2", unit.clone())];
        let expected: Vec<(RequestId, String)> = turns.iter().map(|(id, party)| (request_of(&engine, id), party.clone())).collect();
        for _ in &turns {
            let id = starved_head(&mut engine, &mut sample, 4 * GIB).await;
            head_granted(&mut engine, &mut sample, &id).await;
        }
        assert_eq!(choices(&engine), expected, "a served campaign turn, the outside party's turn, the campaign's next turn");
        let ours = ["f1", "f2", "f3"];
        for (record, _) in &engine.journal.history {
            for step in &record.aged.steps {
                if let Step::Chosen { listed, .. } = step {
                    let campaigns: Vec<&precedence::Listed> = listed.iter().filter(|entry| ours.contains(&entry.candidate.id.to_string().as_str())).collect();
                    assert_eq!(campaigns.len(), 1, "tick {}: the campaign's claims hold one turn: {campaigns:?}", record.tick);
                    assert!(campaigns.iter().all(|entry| entry.party == unit), "tick {}: listed under the campaign's unit", record.tick);
                }
            }
        }
        replays(&engine);
        drop((campaign, front_ends, outside));
        finish_fixture(engine, sample).await;
    }

    /// An engine state whose catalogue generation 19 saved: claim c of a process no longer running, at a pid above
    /// pid_max, whose unit `units` reads as /first.scope. Returns the state and the catalogue's bytes.
    async fn saved_by_generation_19(units: &party::Units) -> (Config, Vec<u8>, (u32, u64)) {
        let (engine, _) = admission_fixture().await;
        let config = engine.handoff_config();
        drop(engine);
        let maximum: u32 = std::fs::read_to_string("/proc/sys/kernel/pid_max").unwrap().trim().parse().unwrap();
        let owner = (maximum + 1000, 7);
        units.place(owner.0, owner.1, "/first.scope");
        let boot = Instance::current().unwrap().boot_id;
        let claim = json!({"id":"c","owner":{"pid":owner.0,"birth":owner.1},"bytes":GIB,"label":"generation 19","order":1,"granted":false,
            "baseline_anonymous":0,"failure":null,"retiring":false,"workers":{},"priority":0,"requested_at":0.0,"purpose":null,"previous_owner":null});
        let bytes = serde_json::to_vec(&json!({"boot":boot,"sequence":1,"claims":{"c":claim}})).unwrap();
        std::fs::write(config.state.join("host-memory.json"), &bytes).unwrap();
        (config, bytes, owner)
    }
    /// A restore of `config`'s state through `store`, its processes read in `units`.
    async fn restored_from(config: &Config, store: Store, units: &party::Units) -> Restored {
        let config = Config { state: config.state.clone(), ..*config };
        Engine::restore(config, Clock::system(), store, Instance::current().unwrap(), Attribution::reading(units.reader())).await.unwrap()
    }
    fn catalogue_of(config: &Config) -> Vec<u8> {
        std::fs::read(config.state.join("host-memory.json")).unwrap()
    }

    #[tokio::test]
    async fn a_migrated_party_is_durable_before_the_engine_serves() {
        // (a) Held: an activation interrupted at its save returns no engine and changes nothing.
        let units = party::Units::default();
        let (config, bytes, owner) = saved_by_generation_19(&units).await;
        let store = Store::default();
        let mut hold = store.hold(Operation::new("claims", "migrate"));
        let mut activation = Box::pin(restored_from(&config, store, &units).await.activated());
        tokio::select! {
            _ = &mut activation => panic!("(a) the activation went past its held save"),
            _ = hold.entered() => {}
        }
        drop(activation);
        assert_eq!(catalogue_of(&config), bytes, "(a) the file holds generation 19's bytes");
        units.place(owner.0, owner.1, "/second.scope");
        let engine = restored_from(&config, Store::default(), &units).await.activated().await.unwrap();
        assert_eq!(claim_status(&engine, "c")["party"], "/second.scope", "(a) a new restore migrates to the owner's unit then");
        drop(engine);
        std::fs::remove_dir_all(&config.state).unwrap();

        // (b) Not published: an activation whose save fails before its rename ends with an error.
        let units = party::Units::default();
        let (config, bytes, _) = saved_by_generation_19(&units).await;
        let restored = restored_from(&config, Store::default(), &units).await;
        let unwritable = Mode::set(&config.state, 0o500);
        let failed = restored.activated().await.err();
        drop(unwritable);
        assert!(failed.is_some_and(|error| error.contains("Permission denied")), "(b) the activation fails");
        assert_eq!(catalogue_of(&config), bytes, "(b) the file is unchanged");
        std::fs::remove_dir_all(&config.state).unwrap();

        // (c) Saved: a crash after the activation changes nothing, and the next activation writes nothing.
        let units = party::Units::default();
        let (config, _, owner) = saved_by_generation_19(&units).await;
        drop(restored_from(&config, Store::default(), &units).await.activated().await.unwrap());
        let saved = catalogue_of(&config);
        units.place(owner.0, owner.1, "/second.scope");
        let engine = restored_from(&config, Store::default(), &units).await.activated().await.unwrap();
        assert_eq!(claim_status(&engine, "c")["party"], "/first.scope", "(c) the saved party");
        assert_eq!(catalogue_of(&config), saved, "(c) the second activation writes nothing");
        drop(engine);
        std::fs::remove_dir_all(&config.state).unwrap();

        // (d) Published, not synced: the activation ends with an error and no engine; the file holds the party.
        let units = party::Units::default();
        let (config, _, owner) = saved_by_generation_19(&units).await;
        let restored = restored_from(&config, Store::default(), &units).await;
        let unreadable = Mode::set(&config.state, 0o300);
        let failed = restored.activated().await.err();
        drop(unreadable);
        assert!(failed.is_some(), "(d) an unsynced activation save ends the activation with an error");
        let file: Value = serde_json::from_slice(&catalogue_of(&config)).unwrap();
        assert_eq!(file["claims"]["c"]["party"], "/first.scope", "(d) the file holds the migrated party");
        let saved = catalogue_of(&config);
        units.place(owner.0, owner.1, "/second.scope");
        let engine = restored_from(&config, Store::default(), &units).await.activated().await.unwrap();
        assert_eq!(claim_status(&engine, "c")["party"], "/first.scope", "(d) a new restore keeps it and migrates nothing");
        assert_eq!(catalogue_of(&config), saved, "(d) its activation leaves the file's bytes unchanged");
        drop(engine);
        std::fs::remove_dir_all(&config.state).unwrap();
    }
    /// The kept head step and the grant of claim `claim` in the last record.
    fn kept_and_granted(engine: &Engine, claim: &str) -> (Step, Entry) {
        let record = last_record(engine);
        let kept = record.aged.steps.first().cloned().expect("a step judged the head the tick before left");
        let grant = record.admitted.iter().find(|entry| matches!(entry, Entry::Granted { claim: granted, .. } if granted == claim)).cloned();
        (kept, grant.expect("the claim was granted"))
    }

    /// T5.1's run: `c` of p1's party, the head, is released and registered again, 1 GiB, by a claimant beside p1 or in
    /// another unit.
    async fn registered_again(beside: bool) {
        let (mut engine, mut sample) = admission_fixture().await;
        let p1 = Claimant::new();
        let again = match beside {
            true => Claimant::beside(&p1),
            false => Claimant::new(),
        };
        publish(&mut engine, "w", "running", &[("w", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        // 4 GiB left beside the running 4 GiB compile, S 8 GiB.
        sample.available = 4 * GIB + 10 * GIB + 4 * GIB;
        engine.requested(p1.host_only("c", 5 * GIB)).await.unwrap();
        engine.host_memory.backdate("c", 60.0);
        engine.tick(sample.clone()).await.unwrap();
        let first = request_of(&engine, "c");
        assert_eq!(engine.host_memory.head().map(|head| head.id.clone()), Some(first.clone()), "c of registration 1 is the head");
        engine.requested(p1.release("c")).await.unwrap();
        engine.requested(again.host_only("c", GIB)).await.unwrap();
        let second = request_of(&engine, "c");
        sample.available = 6 * GIB + 10 * GIB + 4 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        let (kept, grant) = kept_and_granted(&engine, "c");
        let ended = matches!(&kept, Step::Kept { head, standing: Standing::Ended { .. }, verdict: Verdict::Ended } if head.id == first);
        assert!(ended, "the head of registration 1 stands ended: {kept:?}");
        let party = match beside {
            true => p1.party(),
            false => again.party(),
        };
        let RequestId::Claim(registration) = &second else { panic!("a claim request") };
        assert!(
            matches!(&grant, Entry::Granted { order, head: false, party: granted, .. } if *order == registration.order && *granted == party),
            "the grant names registration 2, not as the head: {grant:?}"
        );
        assert_eq!(last_record(&engine).aged.after, Some(precedence::Turn { party: p1.party(), held: false }), "the turn passed");
        replays(&engine);
        drop((p1, again));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_released_head_ends_and_its_id_registered_again_is_a_new_candidate() {
        registered_again(true).await;
        registered_again(false).await;
    }

    #[tokio::test]
    async fn a_registration_giving_way_does_not_hold_back_its_id_registered_again() {
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new();
        let other = Claimant::new();
        publish(&mut engine, "q", "named", &[("named", 4 * GIB)], 0.0, 0).await;
        publish(&mut engine, "q", "later", &[("later", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "named", 0).await;
        sample.available = 16 * GIB;
        engine.requested(owner.host_only("six", 6 * GIB)).await.unwrap();
        engine.host_memory.backdate("six", 60.0);
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("six"), "it reserves 6 GiB, naming the running compile");
        start(&mut engine, &mut sample, "later", 0).await;
        sample.available += 4 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        end(&mut engine, "named");
        sample.available -= 4 * GIB;
        engine.requested(other.host_only("younger", GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        let first = request_of(&engine, "six");
        assert!(head(&engine).is_none() && engine.host_memory.gives_way(&first), "registration 1 ended in vain and gives way");
        engine.requested(owner.release("six")).await.unwrap();
        engine.requested(owner.host_only("six", 6 * GIB)).await.unwrap();
        engine.host_memory.backdate("six", 60.0);
        let second = request_of(&engine, "six");
        assert_ne!(second, first, "a new registration");
        engine.tick(sample.clone()).await.unwrap();
        let listed: Vec<RequestId> = last_record(&engine)
            .aged
            .steps
            .iter()
            .flat_map(|step| match step {
                Step::Chosen { listed, .. } => listed.iter().map(|entry| entry.candidate.id.clone()).collect(),
                Step::Kept { .. } => Vec::new(),
            })
            .collect();
        assert!(listed.contains(&second), "the next choice lists registration 2: {listed:?}");
        assert!(!engine.host_memory.gives_way(&second), "registration 2 does not give way");
        assert!(engine.host_memory.gives_way(&first), "registration 1 gives way until its recorded work ends");
        end(&mut engine, "later");
        engine.tick(sample.clone()).await.unwrap();
        assert!(!engine.host_memory.gives_way(&first), "the recorded work ended");
        replays(&engine);
        drop((owner, other));
        finish_fixture(engine, sample).await;
    }
    /// Every listing of `request` in the written records, by tick, with the party it is listed under.
    fn listings(engine: &Engine, request: &RequestId) -> Vec<(u64, String)> {
        engine
            .journal
            .history
            .iter()
            .filter(|(_, written)| *written)
            .flat_map(|(record, _)| {
                record.aged.steps.iter().flat_map(move |step| match step {
                    Step::Chosen { listed, .. } => {
                        listed.iter().filter(|entry| &entry.candidate.id == request).map(|entry| (record.tick, entry.party.clone())).collect()
                    }
                    Step::Kept { .. } => Vec::new(),
                })
            })
            .collect()
    }

    #[tokio::test]
    async fn a_registration_withdrawn_unserved_and_registered_again_may_have_another_party() {
        let (mut engine, mut sample) = admission_fixture().await;
        let p1 = Claimant::new();
        let q1 = Claimant::new();
        let q2 = Claimant::beside(&q1);
        publish(&mut engine, "w", "running", &[("w", 6 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        engine.requested(q1.host_only("q1", 6 * GIB)).await.unwrap();
        engine.host_memory.backdate("q1", 90.0);
        engine.requested(p1.host_only("c", 6 * GIB)).await.unwrap();
        engine.host_memory.backdate("c", 60.0);
        let first = request_of(&engine, "c");
        // 1 GiB left beside the running 6 GiB compile, S 7 GiB: nothing fits, and both claims are serviceable.
        sample.available = GIB + 10 * GIB + 6 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("q1"), "Q's older claim is chosen; c is listed under P");
        sample.available += 6 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "q1")["state"], "granted");
        engine.requested(p1.release("c")).await.unwrap();
        engine.requested(q2.host_only("c", 6 * GIB)).await.unwrap();
        engine.host_memory.backdate("c", 60.0);
        let second = request_of(&engine, "c");
        sample.available = GIB + 10 * GIB + 6 * GIB + engine.host_memory.reserved(&sample);
        engine.tick(sample.clone()).await.unwrap();
        replays(&engine);
        assert!(listings(&engine, &first).iter().all(|(_, party)| *party == p1.party()) && !listings(&engine, &first).is_empty(), "registration 1 under P");
        assert!(listings(&engine, &second).iter().all(|(_, party)| *party == q1.party()) && !listings(&engine, &second).is_empty(), "registration 2 under Q");
        drop((p1, q1, q2));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_journal_that_regroups_a_claim_is_refused() {
        let (mut engine, mut sample) = admission_fixture().await;
        let p = Claimant::new();
        let q = Claimant::new();
        publish(&mut engine, "w", "running", &[("w", 6 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        engine.requested(q.host_only("q1", 6 * GIB)).await.unwrap();
        engine.host_memory.backdate("q1", 90.0);
        engine.requested(p.host_only("c", 6 * GIB)).await.unwrap();
        engine.host_memory.backdate("c", 60.0);
        let c = request_of(&engine, "c");
        sample.available = GIB + 10 * GIB + 6 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        sample.available += 6 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        sample.available = GIB + 10 * GIB + 6 * GIB + engine.host_memory.reserved(&sample);
        engine.tick(sample.clone()).await.unwrap();
        let listed = listings(&engine, &c);
        assert!(listed.len() >= 2, "c is listed in two records: {listed:?}");
        let regrouped_tick = listed[1].0;
        let mut written: Vec<Value> = engine.journal.history.iter().filter(|(_, written)| *written).map(|(record, _)| serde_json::to_value(record).unwrap()).collect();
        decision::derive_journal(&written).unwrap();
        let record = written.iter_mut().find(|record| record["tick"] == regrouped_tick).unwrap();
        for step in record["aged"]["steps"].as_array_mut().unwrap() {
            for entry in step["listed"].as_array_mut().into_iter().flatten() {
                if entry["candidate"]["id"] == json!(c) {
                    entry["party"] = json!("other");
                    entry["candidate"]["party"] = json!("other");
                }
            }
        }
        let refused = decision::derive_journal(&written).err().unwrap();
        let RequestId::Claim(registration) = &c else { panic!("a claim request") };
        let expected = format!(
            "tick {regrouped_tick}: claim c of registration {} is party \"other\", and party {:?} in an earlier record of its instance",
            registration.order,
            p.party()
        );
        assert!(refused.ends_with(&expected), "{refused}");
        drop((p, q));
        finish_fixture(engine, sample).await;
    }

    // GQ-3: the overcommit budget (PLAN-RA r27 §8.1).
    const MIB: u64 = 1 << 20;

    /// The records `engine` wrote, as journal lines (`decision::PREFIX` and the record), into
    /// `$ADMISSION_RECORDS/<name>.records` when the variable names a directory: what `compile-queue
    /// --check-admission-records` reads, captured in the test rather than from journald.
    fn wrote_records(engine: &Engine, name: &str) {
        let Some(directory) = std::env::var_os("ADMISSION_RECORDS") else {
            return;
        };
        let lines: String = engine
            .journal
            .history
            .iter()
            .filter(|(_, written)| *written)
            .map(|(record, _)| format!("{}{}\n", decision::PREFIX, serde_json::to_value(record).unwrap()))
            .collect();
        std::fs::write(PathBuf::from(directory).join(format!("{name}.records")), lines).unwrap();
    }
    /// `job`'s latest launch in the records so far.
    fn launched(engine: &Engine, job: &str) -> Option<Entry> {
        engine
            .journal
            .history
            .iter()
            .flat_map(|(record, _)| record.admitted.iter())
            .rfind(|entry| matches!(entry, Entry::Launched { job: launched, .. } if launched == job))
            .cloned()
    }
    fn lending_of(engine: &Engine, job: &str) -> Option<u64> {
        engine.running[job].record["lending_bytes"].as_u64()
    }
    /// The health of `engine`'s `lending` object.
    async fn lending_health(engine: &mut Engine) -> Value {
        engine.requested(json!({"protocol":1,"op":"health"})).await.unwrap()["lending"].clone()
    }
    fn lending_json(overcommit: u64, lending: u64, added_back: u64, charged_as_ordinary: u64) -> Value {
        json!({"overcommit_bytes":overcommit,"lending_bytes":lending,"added_back_bytes":added_back,"charged_as_ordinary_bytes":charged_as_ordinary})
    }
    /// A restore of `state` with `config`, as a new instance of another configuration starts.
    async fn restored_with(config: Config) -> Engine {
        let mut engine = Engine::restore(config, Clock::system(), Store::default(), Instance::current().unwrap(), attribution())
            .await
            .unwrap()
            .activated()
            .await
            .unwrap();
        engine.read_memory = sample_alone();
        engine
    }
    /// An engine of the admission fixture, B `overcommit`, whose job j {4, 1} GiB was launched by a tick, holding 3 GiB
    /// of lending when B allows it.
    async fn launched_j(overcommit: u64, cell: u64, expected: u64) -> (Engine, Snapshot) {
        let (mut engine, sample) = admission_fixture().await;
        engine.overcommit = overcommit;
        publish_expecting(&mut engine, "p", "j", &[("j", cell, Some(expected))], 0.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("j"), "j launched");
        (engine, sample)
    }

    /// Q-C4.
    #[tokio::test]
    async fn an_unconfined_service_lends_nothing() {
        let config = Config { state: PathBuf::new(), minimum_ram: 10 * GIB, average_ram: GIB, overcommit_percent: 50, limits: Default::default() };
        assert_eq!((config.lent(false), config.lent(true)), (0, 5 * GIB), "(a)");
        assert!(Containment::Cgroups(PathBuf::from("/nonexistent")).caps(), "(b) cgroups cap every cell");
        assert!(!Containment::Unconfined("not in the service".into()).caps(), "(b) unconfined caps nothing");
        let (mut engine, mut sample) = admission_fixture().await;
        assert_eq!(engine.overcommit, 0, "(c) restore under cargo test is unconfined and lends nothing");
        sample.available = 20 * GIB;
        publish_expecting(&mut engine, "p", "g", &[("a", 4 * GIB, Some(GIB))], 0.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert!(matches!(launched(&engine, "a"), Some(Entry::Launched { bytes, lent: 0, .. }) if bytes == 4 * GIB), "(c) at its whole cell");
        assert_eq!(lending_of(&engine, "a"), None, "(c) its record names no lending");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// Q-O1.
    #[tokio::test]
    async fn a_first_claim_job_is_admitted_at_its_expected_figure() {
        let (mut engine, sample) = admission_fixture().await;
        engine.overcommit = 3 * GIB;
        publish_expecting(&mut engine, "p", "a", &[("a", 4 * GIB, Some(GIB))], 0.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("a"), "(a) a runs after one tick");
        let record = &engine.running["a"].record;
        assert_eq!((record["memory_bytes"].as_u64(), record["lending_bytes"].as_u64()), (Some(4 * GIB), Some(3 * GIB)), "(b)");
        let (written, was_written) = &engine.journal.history[0];
        assert!(was_written, "(c) the launching tick wrote its record");
        assert_eq!((written.overcommit, written.lending), (3 * GIB, 0), "(c)");
        let launches: Vec<&Entry> = written.admitted.iter().filter(|entry| matches!(entry, Entry::Launched { .. })).collect();
        assert!(
            matches!(launches[..], [Entry::Launched { bytes, expected, lent, budget, .. }] if *bytes == 4 * GIB && *expected == GIB && *lent == 3 * GIB && *budget == Budget::of(2 * GIB)),
            "(c) {launches:?}"
        );
        engine.tick(sample.clone()).await.unwrap();
        let next = last_record(&engine);
        assert_eq!((next.lending, next.left), (3 * GIB, Budget::of(GIB)), "(d) 12 − 4 − 10 + 3");
        replays(&engine);
        wrote_records(&engine, "Q-O1");
        finish_fixture(engine, sample).await;
    }

    /// Q-O2.
    #[tokio::test]
    async fn the_whole_cell_once_b_is_spent() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 20 * GIB;
        engine.overcommit = 3 * GIB;
        publish_expecting(&mut engine, "p", "g", &[("a", 4 * GIB, Some(GIB)), ("b", 4 * GIB, Some(GIB))], 0.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(lending_of(&engine, "a"), Some(3 * GIB), "a lends the rest of its cell");
        assert_eq!(lending_of(&engine, "b"), None, "b at its whole cell, B being spent");
        engine.tick(sample.clone()).await.unwrap();
        let next = last_record(&engine);
        assert_eq!((next.lending, next.left), (3 * GIB, Budget::of(5 * GIB)), "20 − 8 − 10 + 3");
        replays(&engine);
        wrote_records(&engine, "Q-O2");
        finish_fixture(engine, sample).await;
    }

    /// The lent bytes of the launch of a job {4, 1} GiB from `budget` with B `overcommit`; none when it does not launch.
    async fn launch_of_one(budget: u64, overcommit: u64) -> Option<u64> {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 10 * GIB + budget;
        engine.overcommit = overcommit;
        publish_expecting(&mut engine, "p", "g", &[("j", 4 * GIB, Some(GIB))], 0.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        let lent = engine.running.contains_key("j").then(|| match launched(&engine, "j") {
            Some(Entry::Launched { lent, .. }) => lent,
            other => panic!("j runs without a launch: {other:?}"),
        });
        replays(&engine);
        finish_fixture(engine, sample).await;
        lent
    }

    /// Q-O3.
    #[tokio::test]
    async fn b_never_delays_the_attempt_it_charges() {
        let mut runs = Vec::new();
        for (budget, overcommit) in [(10 * GIB, 3 * GIB), (2 * GIB, 3 * GIB), (GIB / 2, 3 * GIB), (10 * GIB, 2 * GIB)] {
            runs.push((budget, overcommit, launch_of_one(budget, overcommit).await, launch_of_one(budget, 0).await));
        }
        for (budget, overcommit, with, without) in &runs {
            assert!(without.is_none() || with.is_some(), "(a) ({budget}, {overcommit}) launches without B, not with it");
        }
        assert!(matches!(runs[1], (_, _, Some(_), None)), "(b) (2 GiB, 3 GiB) launches only with B: {:?}", runs[1]);
        assert_eq!(runs[3].2, Some(0), "(c) (10 GiB, 2 GiB) launches with B at its whole cell");
    }

    /// Q-O4.
    #[tokio::test]
    async fn growth_to_the_cap_leaves_the_budget_unchanged() {
        let (mut engine, mut sample) = admission_fixture().await;
        engine.overcommit = 3 * GIB;
        publish_expecting(&mut engine, "p", "g", &[("a", 4 * GIB, Some(GIB))], 0.0, 0).await;
        let now = engine.clock.monotonic_secs();
        start_holding(&mut engine, &mut sample, "a", GIB / 2, now, 3 * GIB).await;
        sample.available = 11 * GIB + GIB / 2;
        engine.tick(sample.clone()).await.unwrap();
        let first = last_record(&engine).left;
        let pid = engine.running["a"].record["pid"].as_u64().unwrap() as u32;
        sample.processes.insert(pid, (std::process::id(), 4 * GIB));
        sample.available = 8 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        let second = last_record(&engine).left;
        assert_eq!((first, second), (Budget::of(GIB), Budget::of(GIB)), "left at RSS 0.5 GiB and at its 4 GiB cap");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// Q-O5.
    #[tokio::test]
    async fn the_lending_leaves_at_every_end() {
        let (mut engine, mut sample, clock, _store) = held_fixture().await;
        sample.available = 20 * GIB;
        engine.overcommit = 3 * GIB;
        let program = waiting()[0].clone();
        let state = engine.config.state.clone();
        let code = "import pathlib,sys,time\nwhile not (pathlib.Path('release').exists() or pathlib.Path('release-' + sys.argv[1]).exists()): time.sleep(0.01)\nsys.exit(int(sys.argv[2]))";
        let exiting = |id: &str, status: u8| json!([program, "-S", "-c", code, id, status.to_string()]);
        let submission = |id: &str, argv: Value| {
            json!({"protocol":1,"op":"submit_group","party":id,"group":{"group_id":id,"jobs":[
                {"job_id":id,"argv":argv,"cwd":state,"memory_bytes":4 * GIB,"expected_memory_bytes":GIB,"expected_runtime_secs":60}]}})
        };
        engine.requested(submission("x0", exiting("x0", 0))).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(lending_of(&engine, "x0"), Some(3 * GIB), "the first attempt holds 3 GiB of lending");
        // After each end, its running record is gone (a); the next tick's record holds no lending (b), and a second
        // job launches in that tick with 3 GiB of lending (c).
        let check = |engine: &Engine, end: &str, ended: &str, next: &str| {
            assert!(!engine.running.contains_key(ended) && !state.join("running").join(format!("{ended}.json")).exists(), "{end}: (a)");
            assert_eq!(last_record(engine).lending, 0, "{end}: (b)");
            assert_eq!(lending_of(engine, next), Some(3 * GIB), "{end}: (c)");
        };
        std::fs::write(state.join("release-x0"), b"").unwrap();
        concluded(&mut engine, &sample, &clock, "x0").await;
        engine.requested(submission("x1", exiting("x1", 1))).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        check(&engine, "exit 0", "x0", "x1");
        std::fs::write(state.join("release-x1"), b"").unwrap();
        concluded(&mut engine, &sample, &clock, "x1").await;
        assert_eq!(engine.results["x1"]["returncode"], 1);
        engine.requested(submission("x2", exiting("x2", 0))).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        check(&engine, "exit 1", "x1", "x2");
        engine
            .requested(json!({"protocol":1,"op":"cancel","group_id":"x2","job_id":null,"reason":"Q-O5","include_running":true}))
            .await
            .unwrap();
        concluded(&mut engine, &sample, &clock, "x2").await;
        assert_eq!(engine.results["x2"]["outcome"], "cancelled");
        engine.requested(submission("x3", exiting("x3", 0))).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        check(&engine, "a cancellation", "x2", "x3");
        clock.advance(std::time::Duration::from_secs(121));
        concluded(&mut engine, &sample, &clock, "x3").await;
        assert_eq!(engine.results["x3"]["detail"], "compiler ran past twice its declared runtime of 60 s");
        engine.requested(submission("y3", exiting("y3", 0))).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        check(&engine, "an overrun", "x3", "y3");
        std::fs::write(state.join("release-y3"), b"").unwrap();
        concluded(&mut engine, &sample, &clock, "y3").await;
        engine.requested(submission("x4", json!(["/nonexistent/compiler"]))).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert!(matches!(launched(&engine, "x4"), Some(Entry::Launched { lent, running: false, .. }) if lent == 3 * GIB), "the failed spawn was charged its lending");
        assert_eq!(engine.results["x4"]["returncode"], 127);
        engine.requested(submission("y4", exiting("y4", 0))).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        check(&engine, "a failed spawn", "x4", "y4");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// Q-O7.
    #[tokio::test]
    async fn shares_and_the_projection_count_the_ordinary_reservation() {
        let (mut engine, mut sample) = admission_fixture().await;
        engine.overcommit = 3 * GIB;
        publish_expecting(&mut engine, "p", "g", &[("a", 4 * GIB, Some(GIB))], 0.0, 0).await;
        let now = engine.clock.monotonic_secs();
        start_holding(&mut engine, &mut sample, "a", 0, now, 3 * GIB).await;
        let party = engine.parties.of("g").to_owned();
        assert_eq!(engine.ram_held()[&party], fairness::charge(GIB, engine.parties.priority("g")), "(a)");
        let own = engine.own_work();
        assert!(matches!(own[..], [Counted::Compile { bytes, .. }] if bytes == GIB), "(b) {own:?}");
        engine.tick(sample.clone()).await.unwrap();
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// Q-O8.
    #[tokio::test]
    async fn the_aged_head_reserves_its_cell_and_launches_on_the_lending_path() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 17 * GIB;
        publish(&mut engine, "r", "r", &[("r", 6 * GIB)], 0.0, 0).await;
        let now = engine.clock.monotonic_secs();
        start_holding(&mut engine, &mut sample, "r", 0, now, 0).await;
        publish_expecting(&mut engine, "j", "j", &[("j", 6 * GIB, Some(GIB))], 120.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        let second = last_record(&engine);
        let job = RequestId::Compile { group: "j".into(), job: "j".into() };
        let reserved = second.admitted.iter().any(|entry| matches!(entry, Entry::Reserved { request, bytes, .. } if *request == job && *bytes == 6 * GIB));
        assert!(reserved && !engine.running.contains_key("j"), "(a) j is the aged head, reserving its 6 GiB cell: {:?}", second.admitted);
        engine.overcommit = 5 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        assert!(
            matches!(launched(&engine, "j"), Some(Entry::Launched { head: true, reserved, lent, budget, .. }) if reserved == 6 * GIB && lent == 5 * GIB && budget == Budget::of(0).less(5 * GIB)),
            "(b) {:?}",
            launched(&engine, "j")
        );
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// Q-O9.
    #[tokio::test]
    async fn turns_read_the_cell() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 20 * GIB;
        engine.overcommit = 3 * GIB;
        let q = Claimant::new();
        engine.requested(q.claim("two", 2 * GIB)).await.unwrap();
        publish_expecting(&mut engine, "p", "g", &[("a", 4 * GIB, Some(GIB))], 0.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        let record = last_record(&engine);
        let turn = record.admitted.iter().find_map(|entry| match entry {
            Entry::Turn { party, bytes, .. } if party == "p" => Some(*bytes),
            _ => None,
        });
        assert_eq!(turn, Some(4 * GIB), "{:?}", record.admitted);
        replays(&engine);
        drop(q);
        finish_fixture(engine, sample).await;
    }

    /// Q-O10.
    #[tokio::test]
    async fn a_large_cell_is_large_whatever_its_expected_figure() {
        const GB: u64 = 1_000_000_000;
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 30 * GIB;
        engine.overcommit = 9 * GB;
        publish_expecting(&mut engine, "p", "g", &[("a", 11 * GB, Some(2 * GB))], 0.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert!(
            matches!(launched(&engine, "a"), Some(Entry::Launched { phase: Launching::LargeLaunch, class: Class::Large, lent, .. }) if lent == 9 * GB),
            "{:?}",
            launched(&engine, "a")
        );
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// Q-O11.
    #[tokio::test]
    async fn the_waiting_reason_names_both_charges() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 10 * GIB + GIB / 2;
        engine.overcommit = 3 * GIB;
        publish_expecting(&mut engine, "p", "g", &[("a", 4 * GIB, Some(GIB))], 0.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(
            reason(&engine, "g", 0),
            format!(
                "waiting for RAM: needs {} bytes and {} bytes of the overcommit budget ({} lendable), or {} bytes; {} bytes admissible after headroom/reservations",
                GIB,
                3 * GIB,
                3 * GIB,
                4 * GIB,
                GIB / 2
            )
        );
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// Q-O12.
    #[tokio::test]
    async fn health_shows_the_effective_bound() {
        let (mut engine, mut sample) = admission_fixture().await;
        engine.overcommit = 3 * GIB;
        publish_expecting(&mut engine, "p", "g", &[("a", 4 * GIB, Some(GIB))], 0.0, 0).await;
        let now = engine.clock.monotonic_secs();
        start_holding(&mut engine, &mut sample, "a", 0, now, 3 * GIB).await;
        assert_eq!(lending_health(&mut engine).await, lending_json(3 * GIB, 3 * GIB, 3 * GIB, 0));
        engine.tick(sample.clone()).await.unwrap();
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// Q-O13.
    #[tokio::test]
    async fn receipts_carry_the_expected_figure_and_the_lending() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 20 * GIB;
        engine.overcommit = 3 * GIB;
        for (job, age, expected) in [("lent", 3.0, Some(GIB)), ("fallback", 2.0, Some(GIB)), ("plain", 1.0, None)] {
            publish_expecting(&mut engine, job, job, &[(job, 4 * GIB, expected)], age, 0).await;
        }
        engine.tick(sample.clone()).await.unwrap();
        assert!(["lent", "fallback", "plain"].iter().all(|job| engine.running.contains_key(*job)), "all three run");
        std::fs::write(engine.config.state.join("release"), b"").unwrap();
        for _ in 0..1000 {
            if engine.running.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            engine.tick(sample.clone()).await.unwrap();
        }
        let state = engine.config.state.clone();
        engine
            .requested(json!({"protocol":1,"op":"submit_group","party":"spawn","group":{"group_id":"spawn","jobs":[
                {"job_id":"spawn","argv":["/nonexistent/compiler"],"cwd":state,"memory_bytes":4 * GIB,"expected_memory_bytes":GIB,"expected_runtime_secs":60}]}}))
            .await
            .unwrap();
        engine.tick(sample.clone()).await.unwrap();
        let receipt = |job: &str| {
            let result = &engine.results[job];
            (result["expected_memory_bytes"].as_u64(), result["lending_bytes"].as_u64(), result["memory_limit_bytes"].as_u64())
        };
        assert_eq!(receipt("lent"), (Some(GIB), Some(3 * GIB), Some(4 * GIB)), "(a)");
        assert_eq!(receipt("fallback"), (Some(GIB), None, Some(4 * GIB)), "(b)");
        let plain = engine.results["plain"].as_object().unwrap();
        assert!(!plain.contains_key("expected_memory_bytes") && !plain.contains_key("lending_bytes"), "(c) {plain:?}");
        let spawn = engine.results["spawn"].as_object().unwrap();
        assert_eq!((spawn.get("expected_memory_bytes"), spawn.get("returncode")), (Some(&json!(GIB)), Some(&json!(127))), "(d) {spawn:?}");
        assert!(!spawn.contains_key("memory_limit_bytes") && !spawn.contains_key("lending_bytes"), "(d) {spawn:?}");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// An engine with B `overcommit` whose group G runs `siblings` (job, cell, lending), after `regrow("G", released)`
    /// and one more tick.
    async fn regrown(overcommit: u64, siblings: &[(&str, u64, Option<u64>, u64)], released: Charge) -> (Engine, Snapshot) {
        let (mut engine, mut sample) = admission_fixture().await;
        engine.overcommit = overcommit;
        let jobs: Vec<(&str, u64, Option<u64>)> = siblings.iter().map(|&(job, cell, expected, _)| (job, cell, expected)).collect();
        publish_expecting(&mut engine, "p", "G", &jobs, 0.0, 0).await;
        let now = engine.clock.monotonic_secs();
        for &(job, _, _, lending) in siblings {
            start_holding(&mut engine, &mut sample, job, 0, now, lending).await;
        }
        engine.regrow("G", released, AttemptId { job_id: "dead".into(), started_at: 1.0 }).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        (engine, sample)
    }

    /// Q-O14.
    #[tokio::test]
    async fn a_siblings_raise_is_todays_and_is_funded_ordinary_first() {
        let siblings = [("S1", 256 * MIB, None, 0), ("S2", 256 * MIB, None, 0)];
        let (ra, ra_sample) = regrown(128 * MIB, &siblings, Charge::held(64 * MIB, 48 * MIB)).await;
        let (control, control_sample) = regrown(0, &siblings, Charge::whole(64 * MIB)).await;
        let caps = |engine: &Engine| ["S1", "S2"].map(|job| engine.running[job].record["memory_bytes"].as_u64());
        assert_eq!((caps(&ra), caps(&control)), ([Some(288 * MIB); 2], [Some(288 * MIB); 2]), "(a) today's raised caps in both runs");
        assert_eq!(["S1", "S2"].map(|job| lending_of(&ra, job)), [Some(16 * MIB), Some(32 * MIB)], "(b) RA, in job order");
        assert_eq!(["S1", "S2"].map(|job| lending_of(&control, job)), [None, None], "(b) the control");
        let (ra_record, control_record) = (last_record(&ra), last_record(&control));
        assert_eq!((ra_record.lending, ra_record.left), (48 * MIB, control_record.left.plus(48 * MIB)), "(c)");
        assert!(ra_record.lending <= ra.overcommit, "(d)");
        replays(&ra);
        replays(&control);
        finish_fixture(ra, ra_sample).await;
        finish_fixture(control, control_sample).await;
    }

    /// Q-O15.
    #[tokio::test]
    async fn a_transfer_keeps_the_lending_within_b() {
        let (lent, lent_sample) = regrown(3 * GIB, &[("S", 4 * GIB, Some(GIB), 3 * GIB)], Charge::whole(2 * GIB)).await;
        let record = &lent.running["S"].record;
        assert_eq!((record["memory_bytes"].as_u64(), record["lending_bytes"].as_u64()), (Some(4 * GIB + GIB / 2), Some(3 * GIB)), "(a)");
        let next = last_record(&lent);
        assert_eq!((next.lending, next.lendable()), (3 * GIB, 0), "(a)");
        let (plain, plain_sample) = regrown(3 * GIB, &[("S", 2 * GIB, None, 0)], Charge::held(GIB, 768 * MIB)).await;
        let record = &plain.running["S"].record;
        assert_eq!((record["memory_bytes"].as_u64(), record["lending_bytes"].as_u64()), (Some(2 * GIB + GIB / 2), Some(256 * MIB)), "(b)");
        assert_eq!(last_record(&plain).lending, 256 * MIB, "(b)");
        replays(&lent);
        replays(&plain);
        finish_fixture(lent, lent_sample).await;
        finish_fixture(plain, plain_sample).await;
    }

    /// The record of the second tick of a job A {8, 4} GiB with B `overcommit`, A's process and a child of it each at
    /// RSS 4 GiB, pages shared by the fork.
    async fn shared_pages(overcommit: u64) -> (Record, Engine, Snapshot) {
        let (mut engine, mut sample) = admission_fixture().await;
        engine.overcommit = overcommit;
        sample.available = 18 * GIB;
        publish_expecting(&mut engine, "p", "g", &[("a", 8 * GIB, Some(4 * GIB))], 0.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        let pid = engine.running["a"].record["pid"].as_u64().unwrap() as u32;
        sample.processes.insert(pid, (std::process::id(), 4 * GIB));
        sample.processes.insert(4_000_000_000, (pid, 4 * GIB));
        sample.available = 10 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        (last_record(&engine), engine, sample)
    }

    /// Q-O16: documents today's skew, not headroom.
    #[tokio::test]
    async fn shared_pages_skew_both_budgets_alike_and_ra_adds_at_most_b() {
        let (control, control_engine, control_sample) = shared_pages(0).await;
        let (ra, ra_engine, ra_sample) = shared_pages(5 * GIB).await;
        assert!(matches!(launched(&ra_engine, "a"), Some(Entry::Launched { lent, .. }) if lent == 4 * GIB), "RA lent 4 GiB");
        assert!(matches!(launched(&control_engine, "a"), Some(Entry::Launched { lent: 0, .. })), "the control at its whole cell");
        assert_eq!((control.compiler_reserved, ra.compiler_reserved), (0, 0), "(a) the summed RSS hides A's growth in both");
        assert_eq!(ra.left, control.left.plus(4 * GIB), "(b) RA adds min(lending, B)");
        replays(&control_engine);
        replays(&ra_engine);
        finish_fixture(control_engine, control_sample).await;
        finish_fixture(ra_engine, ra_sample).await;
    }

    /// Q-R1.
    #[tokio::test]
    async fn a_cold_restore_derives_each_lending_from_its_running_record() {
        let (e1, mut sample) = launched_j(3 * GIB, 4 * GIB, GIB).await;
        assert_eq!(lending_of(&e1, "j"), Some(3 * GIB));
        let mut e2 = restore_of(&e1).await;
        e2.read_memory = sample_alone();
        e2.overcommit = 3 * GIB;
        replays(&e1);
        drop(e1);
        sample.available = 20 * GIB;
        e2.tick(sample.clone()).await.unwrap();
        let first = last_record(&e2);
        assert_eq!((first.lending, first.left), (3 * GIB, Budget::of(20 * GIB - 4 * GIB - 10 * GIB + 3 * GIB)), "(a)");
        end(&mut e2, "j");
        e2.tick(sample.clone()).await.unwrap();
        assert!(!e2.running.contains_key("j"), "j was reaped");
        e2.tick(sample.clone()).await.unwrap();
        assert_eq!(last_record(&e2).lending, 0, "(b)");
        replays(&e2);
        finish_fixture(e2, sample).await;
    }

    /// Q-R2.
    #[tokio::test]
    async fn a_handover_carries_each_lending_once() {
        let (mut e1, sample) = launched_j(3 * GIB, 4 * GIB, GIB).await;
        e1.publish_reaper_custody().await.unwrap();
        let ids = e1.reaping_ids();
        let pid = e1.running["j"].record["pid"].clone();
        let state = e1.config.state.clone();
        let mut restored = Engine::restore(e1.handoff_config(), Clock::system(), Store::default(), Instance::current().unwrap(), attribution())
            .await
            .unwrap();
        restored.await_reapers(&ids);
        let mut e2 = restored.activated().await.unwrap();
        e2.read_memory = sample_alone();
        e2.overcommit = 3 * GIB;
        // The predecessor keeps its children and reaps them; its successor reads their exits from receipts.
        let children = e1.take_children();
        replays(&e1);
        drop(e1);
        e2.tick(sample.clone()).await.unwrap();
        assert_eq!(last_record(&e2).lending, 3 * GIB, "(a) exactly once");
        wrote(&state.join("reaper-exits").join("j.json"), &json!({"job_id":"j","pid":pid,"returncode":0})).await;
        e2.tick(sample.clone()).await.unwrap();
        assert_eq!(e2.results["j"]["lending_bytes"].as_u64(), Some(3 * GIB), "(b)");
        e2.tick(sample.clone()).await.unwrap();
        assert_eq!(last_record(&e2).lending, 0, "(c)");
        replays(&e2);
        std::fs::write(state.join("release"), b"").unwrap();
        for (_, mut child) in children {
            child.wait().await.unwrap();
        }
        finish_fixture(e2, sample).await;
    }

    /// Q-R3.
    #[tokio::test]
    async fn an_impossible_split_refuses_restore() {
        /// The state an engine left with one running record, j's, whose `memory_bytes` is 4 GiB and whose
        /// `lending_bytes` is `lending`, the key absent when `lending` is none.
        async fn state_holding(lending: Option<Value>) -> PathBuf {
            let (mut engine, sample) = admission_fixture().await;
            publish_expecting(&mut engine, "p", "g", &[("j", 4 * GIB, Some(GIB))], 0.0, 0).await;
            let state = engine.config.state.clone();
            let mut record = json!({"group_id":"g","job_id":"j","boot_id":engine.instance.boot_id,"pid":null,"start_ticks":null,
                "cpu":sample.cores[0],"memory_bytes":4 * GIB,"job":engine.groups["g"].jobs[0],
                "log_path":state.join("logs").join("j.log"),"queued_at":0.0,"started_at":engine.clock.wall(),
                "started_monotonic":engine.clock.monotonic_secs()});
            lending.into_iter().for_each(|lending| record["lending_bytes"] = lending);
            wrote(&state.join("running").join("j.json"), &record).await;
            state
        }
        for (case, lending) in [("(a)", json!(5 * GIB)), ("(b)", json!("x")), ("(c)", Value::Null)] {
            let state = state_holding(Some(lending)).await;
            let refused = restored(&state).await.err();
            assert!(refused.as_ref().is_some_and(|error| error.contains("lending_bytes") && error.contains('j')), "{case} {refused:?}");
            std::fs::remove_dir_all(state).unwrap();
        }
        let state = state_holding(None).await;
        let lending = restored(&state).await.map(|engine| engine.running["j"].lending());
        assert_eq!(lending, Ok(0), "(d) an absent lending_bytes is 0");
        std::fs::remove_dir_all(state).unwrap();
    }

    /// Q-R4.
    #[tokio::test]
    async fn an_attempt_concluded_at_restore_drops_its_lending_in_that_tick() {
        let (mut e1, sample) = launched_j(3 * GIB, 4 * GIB, GIB).await;
        end(&mut e1, "j");
        let state = e1.config.state.clone();
        replays(&e1);
        drop(e1);
        let mut e2 = restored(&state).await.unwrap();
        e2.read_memory = sample_alone();
        e2.overcommit = 3 * GIB;
        e2.tick(sample.clone()).await.unwrap();
        assert_eq!(e2.results["j"]["outcome"], "lost");
        assert_eq!(last_record(&e2).lending, 0);
        replays(&e2);
        finish_fixture(e2, sample).await;
    }

    /// Q-R5.
    #[tokio::test]
    async fn a_lower_floor_charges_inherited_lending_above_b_as_ordinary() {
        let (mut e1, mut sample) = launched_j(5 * GIB, 6 * GIB, GIB).await;
        assert_eq!(lending_of(&e1, "j"), Some(5 * GIB));
        publish(&mut e1, "k", "k", &[("k", 9 * GIB)], 0.0, 0).await;
        publish(&mut e1, "l", "l", &[("l", 4 * GIB)], 0.0, 0).await;
        let pid = e1.running["j"].record["pid"].as_u64().unwrap() as u32;
        let config = Config { state: e1.config.state.clone(), minimum_ram: GIB, average_ram: GIB, overcommit_percent: 50, limits: Default::default() };
        replays(&e1);
        drop(e1);
        let mut e2 = restored_with(config).await;
        e2.overcommit = e2.config.overcommit_bytes();
        sample.available = 10 * GIB;
        sample.processes.insert(pid, (std::process::id(), GIB));
        e2.tick(sample.clone()).await.unwrap();
        let first = last_record(&e2);
        assert_eq!((first.overcommit, first.lending, first.left), (GIB / 2, 5 * GIB, Budget::of(4 * GIB + GIB / 2)), "(a) 10 − 5 − 1 + 0.5");
        assert!(!e2.running.contains_key("k") && e2.running.contains_key("l"), "(b)");
        assert_eq!(lending_health(&mut e2).await, lending_json(GIB / 2, 5 * GIB, GIB / 2, 4 * GIB + GIB / 2), "(c)");
        let described = e2.lending().describe();
        assert!(described.contains(&format!("{} bytes charged as ordinary", 4 * GIB + GIB / 2)), "(d) {described}");
        replays(&e2);
        wrote_records(&e2, "Q-R5");
        finish_fixture(e2, sample).await;
    }

    /// Q-R6.
    #[tokio::test]
    async fn a_lower_percent_charges_inherited_lending_above_b_as_ordinary() {
        let (e1, mut sample) = launched_j(3 * GIB, 4 * GIB, GIB).await;
        let config = Config { state: e1.config.state.clone(), minimum_ram: 10 * GIB, average_ram: GIB, overcommit_percent: 10, limits: Default::default() };
        replays(&e1);
        drop(e1);
        let mut e2 = restored_with(config).await;
        e2.overcommit = e2.config.overcommit_bytes();
        sample.available = 20 * GIB;
        publish_expecting(&mut e2, "q", "m", &[("m", 2 * GIB, Some(3 * GIB / 2))], 0.0, 0).await;
        e2.tick(sample.clone()).await.unwrap();
        let first = last_record(&e2);
        assert_eq!((first.overcommit, first.lending, first.left), (GIB, 3 * GIB, Budget::of(7 * GIB)), "(a) 20 − 4 − 10 + 1");
        assert!(matches!(launched(&e2, "m"), Some(Entry::Launched { lent: 0, .. })), "(b) {:?}", launched(&e2, "m"));
        assert_eq!(lending_health(&mut e2).await, lending_json(GIB, 3 * GIB, GIB, 2 * GIB), "(c)");
        end(&mut e2, "j");
        e2.tick(sample.clone()).await.unwrap();
        assert!(!e2.running.contains_key("j"), "j was reaped");
        publish_expecting(&mut e2, "r", "n", &[("n", 2 * GIB, Some(3 * GIB / 2))], 0.0, 0).await;
        e2.tick(sample.clone()).await.unwrap();
        assert!(matches!(launched(&e2, "n"), Some(Entry::Launched { lent, .. }) if lent == GIB / 2), "(d) {:?}", launched(&e2, "n"));
        replays(&e2);
        finish_fixture(e2, sample).await;
    }

    /// Q-R7.
    #[tokio::test]
    async fn an_instance_that_cannot_contain_charges_all_inherited_lending_as_ordinary() {
        let (e1, mut sample) = launched_j(3 * GIB, 4 * GIB, GIB).await;
        let config = Config { state: e1.config.state.clone(), minimum_ram: 10 * GIB, average_ram: GIB, overcommit_percent: 50, limits: Default::default() };
        replays(&e1);
        drop(e1);
        let mut e2 = restored_with(config).await;
        assert_eq!(e2.overcommit, 0, "(a) restore under cargo test is unconfined");
        sample.available = 18 * GIB;
        publish_expecting(&mut e2, "q", "m", &[("m", 4 * GIB, Some(GIB))], 0.0, 0).await;
        e2.tick(sample.clone()).await.unwrap();
        let first = last_record(&e2);
        assert_eq!((first.overcommit, first.lending, first.left), (0, 3 * GIB, Budget::of(18 * GIB - 4 * GIB - 10 * GIB)), "(b)");
        assert_eq!(lending_health(&mut e2).await, lending_json(0, 3 * GIB, 0, 3 * GIB), "(c)");
        assert!(matches!(launched(&e2, "m"), Some(Entry::Launched { lent: 0, bytes, .. }) if bytes == 4 * GIB), "(d) {:?}", launched(&e2, "m"));
        replays(&e2);
        finish_fixture(e2, sample).await;
    }

    /// Q-O6's scenario, reused by GQ-4's Q-E2: B `overcommit`; group G with H {64, 16} MiB, which allocates 96 MiB and
    /// exits 0, and its sibling S, one-figure 256 MiB, a waiting process. Returns H's first `started_at`, read from
    /// `running/H.json` while that attempt runs, once the first attempt's out-of-memory kill has been reaped.
    async fn oom_beside_a_sibling(engine: &mut Engine, sample: &Snapshot, overcommit: u64) -> Value {
        engine.containment = Containment::delegated_scope();
        engine.overcommit = overcommit;
        let program = waiting()[0].clone();
        let state = engine.config.state.clone();
        let allocate = "held = bytearray(96 << 20)\nheld[::4096] = b'x' * len(held[::4096])";
        let jobs = json!([
            {"job_id":"H","argv":[program,"-S","-c",allocate],"cwd":state,"memory_bytes":64 * MIB,"expected_memory_bytes":16 * MIB,"expected_runtime_secs":60},
            {"job_id":"S","argv":waiting(),"cwd":state,"memory_bytes":256 * MIB,"expected_runtime_secs":60},
        ]);
        engine
            .requested(json!({"protocol":1,"op":"submit_group","party":"G","group":{"group_id":"G","jobs":jobs}}))
            .await
            .unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("H") && engine.running.contains_key("S"), "H and S launched");
        let first = store::read(&state.join("running").join("H.json")).await.unwrap()["started_at"].clone();
        for _ in 0..1000 {
            if !engine.running.get("H").is_some_and(|running| running.record["started_at"] == first) {
                return first;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            engine.tick(sample.clone()).await.unwrap();
        }
        panic!("H's first attempt was not reaped");
    }

    /// Q-O6. Needs a delegated cgroup of its own (G-Q-C).
    #[tokio::test]
    #[ignore]
    async fn an_oom_killed_first_claim_funds_todays_sibling_raise() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 64 * GIB;
        let first = oom_beside_a_sibling(&mut engine, &sample, 128 * MIB).await;
        assert!(!engine.running.get("H").is_some_and(|running| running.record["started_at"] == first), "(a) H's first record is removed");
        let Containment::Cgroups(root) = &engine.containment else { unreachable!() };
        let max = std::fs::read_to_string(root.join("job-S").join("memory.max")).unwrap();
        assert_eq!((engine.running["S"].record["memory_bytes"].as_u64(), max.trim()), (Some(320 * MIB), "335544320"), "(b) today's raise");
        assert_eq!(lending_of(&engine, "S"), Some(48 * MIB), "(c) 64 MiB raised: 16 MiB ordinary, then 48 MiB of lending");
        engine.tick(sample.clone()).await.unwrap();
        let next = last_record(&engine);
        assert_eq!(next.lending, 48 * MIB, "(d)");
        assert!(next.lending <= engine.overcommit, "(e)");
        assert!(
            matches!(launched(&engine, "H"), Some(Entry::Launched { bytes, expected, lent: 0, .. }) if bytes == 576 * MIB && expected == 576 * MIB),
            "(f) {:?}",
            launched(&engine, "H")
        );
        replays(&engine);
        finish_fixture(engine, sample).await;
        let (mut control, mut sample) = admission_fixture().await;
        sample.available = 64 * GIB;
        oom_beside_a_sibling(&mut control, &sample, 0).await;
        assert_eq!(control.running["S"].record["memory_bytes"].as_u64(), Some(320 * MIB), "(g) the control's raise is the same");
        assert_eq!(lending_of(&control, "S"), None, "(g) and names no lending");
        replays(&control);
        finish_fixture(control, sample).await;
    }

    /// Q-S1. Needs a delegated cgroup of its own (G-Q-C).
    #[tokio::test]
    #[ignore]
    async fn a_first_claim_cell_is_capped_at_its_cell_and_never_throttled() {
        let (mut engine, mut sample) = admission_fixture().await;
        engine.containment = Containment::delegated_scope();
        engine.overcommit = 256 * MIB;
        sample.available = 64 * GIB;
        let program = waiting()[0].clone();
        let state = engine.config.state.clone();
        let code = "import pathlib,time\nheld = bytearray(96 << 20)\nheld[::4096] = b'x' * len(held[::4096])\npathlib.Path('allocated').write_text('')\nwhile not pathlib.Path('release').exists(): time.sleep(0.01)";
        engine
            .requested(json!({"protocol":1,"op":"submit_group","party":"capped","group":{"group_id":"capped","jobs":[
                {"job_id":"capped","argv":[program,"-S","-c",code],"cwd":state,"memory_bytes":128 * MIB,"expected_memory_bytes":32 * MIB,"expected_runtime_secs":60}]}}))
            .await
            .unwrap();
        engine.tick(sample.clone()).await.unwrap();
        let Containment::Cgroups(root) = &engine.containment else { unreachable!() };
        let cell = root.join("job-capped");
        assert_eq!(std::fs::read_to_string(cell.join("memory.max")).unwrap().trim(), "134217728", "(a)");
        assert_eq!(std::fs::read_to_string(cell.join("memory.high")).unwrap().trim(), "max", "(b)");
        for _ in 0..1000 {
            if state.join("allocated").exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            engine.tick(sample.clone()).await.unwrap();
        }
        assert!(state.join("allocated").exists() && engine.running.contains_key("capped"), "(c) the allocation completed");
        std::fs::write(state.join("release"), b"").unwrap();
        for _ in 0..1000 {
            if engine.results.contains_key("capped") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            engine.tick(sample.clone()).await.unwrap();
        }
        let result = &engine.results["capped"];
        assert_eq!(result["outcome"], "success", "(c) no out-of-memory kill: {result}");
        assert_eq!(result["lending_bytes"].as_u64(), Some(96 * MIB), "(d)");
        assert!(result["peak_memory_bytes"].as_u64().is_some_and(|peak| peak > 32 * MIB), "(d) {result}");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    // GQ-4: a reaped result carries its attempt's facts (PLAN-RA r27 §8.1).
    use super::super::attempts;

    /// A result's `attempt`, decoded through its one decoder.
    fn attempt_of(result: &Value) -> attempts::Attempt {
        serde_json::from_value(result["attempt"].clone()).unwrap_or_else(|error| panic!("{error}: {result}"))
    }
    fn neither_count(result: &Value) -> bool {
        let keys = result.as_object().unwrap();
        !keys.contains_key("attempts") && !keys.contains_key("attempts_since")
    }

    /// Q-E1.
    #[tokio::test]
    async fn every_reaped_result_reports_its_attempt() {
        let (mut e1, mut sample, clock, _store) = held_fixture().await;
        sample.available = 20 * GIB;
        e1.overcommit = 3 * GIB;
        let program = waiting()[0].clone();
        let state = e1.config.state.clone();
        let code = "import pathlib,sys,time\nwhile not (pathlib.Path('release').exists() or pathlib.Path('release-' + sys.argv[1]).exists()): time.sleep(0.01)\nsys.exit(int(sys.argv[2]))";
        let submission = |id: &str, argv: Value| {
            json!({"protocol":1,"op":"submit_group","party":id,"group":{"group_id":id,"jobs":[
                {"job_id":id,"argv":argv,"cwd":state,"memory_bytes":4 * GIB,"expected_memory_bytes":GIB,"expected_runtime_secs":60}]}})
        };
        let exiting = |id: &str, status: u8| json!([program, "-S", "-c", code, id, status.to_string()]);
        for (id, status) in [("x0", 0), ("x1", 1), ("x2", 0), ("x3", 0)] {
            e1.requested(submission(id, exiting(id, status))).await.unwrap();
            e1.tick(sample.clone()).await.unwrap();
            assert_eq!(lending_of(&e1, id), Some(3 * GIB), "{id} launched with 3 GiB lent");
            match id {
                "x2" => drop(e1.requested(json!({"protocol":1,"op":"cancel","group_id":id,"job_id":null,"reason":"Q-E1","include_running":true})).await.unwrap()),
                "x3" => clock.advance(std::time::Duration::from_secs(121)),
                _ => std::fs::write(state.join(format!("release-{id}")), b"").unwrap(),
            }
            concluded(&mut e1, &sample, &clock, id).await;
        }
        e1.requested(submission("x4", exiting("x4", 0))).await.unwrap();
        e1.tick(sample.clone()).await.unwrap();
        assert_eq!(lending_of(&e1, "x4"), Some(3 * GIB), "the fifth attempt holds 3 GiB of lending");
        end(&mut e1, "x4");
        let reaped: Vec<Value> = ["x0", "x1", "x2", "x3"].iter().map(|id| e1.results[*id].clone()).collect();
        replays(&e1);
        drop(e1);
        let mut e2 = restored(&state).await.unwrap();
        e2.read_memory = sample_alone();
        e2.overcommit = 3 * GIB;
        e2.tick(sample.clone()).await.unwrap();
        e2.requested(submission("x5", json!(["/nonexistent/compiler"]))).await.unwrap();
        e2.tick(sample.clone()).await.unwrap();
        let launch = Launch { cell_bytes: 4 * GIB, expected_bytes: GIB, lending_bytes: 3 * GIB };
        for (result, id) in reaped.iter().zip(["x0", "x1", "x2", "x3"]) {
            assert!(result.get("attempt").is_some(), "(a) {id}'s result has no attempt: {result}");
            let attempt = attempt_of(result);
            assert_eq!((attempt.launch, attempt.moves.len()), (Some(launch), 0), "(a) {id}");
        }
        let causes: Vec<(Cause, bool)> = reaped.iter().map(|result| (attempt_of(result).cause, attempt_of(result).peak_reset)).collect();
        assert_eq!(causes, [(Cause::Success, false), (Cause::Failure, false), (Cause::Cancelled, false), (Cause::Failure, false)], "(b)");
        let lost = &e2.results["x4"];
        let attempt = attempt_of(lost);
        assert_eq!((attempt.launch, attempt.cause, lost["lending_bytes"].as_u64()), (None, Cause::Lost, Some(3 * GIB)), "(c) {lost}");
        assert!(e2.results["x5"].get("attempt").is_none() && e2.results["x5"]["returncode"] == 127, "(d) {}", e2.results["x5"]);
        assert!(reaped.iter().chain([lost, &e2.results["x5"]]).all(neither_count), "(e)");
        replays(&e2);
        finish_fixture(e2, sample).await;
    }

    /// Q-E2. Needs a delegated cgroup of its own (G-Q-C).
    #[tokio::test]
    #[ignore]
    async fn an_oom_retrys_result_is_its_own_and_its_sibling_records_the_move() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 64 * GIB;
        let first = oom_beside_a_sibling(&mut engine, &sample, 128 * MIB).await;
        for _ in 0..1000 {
            if engine.results.contains_key("H") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            engine.tick(sample.clone()).await.unwrap();
        }
        std::fs::write(engine.config.state.join("release"), b"").unwrap();
        for _ in 0..1000 {
            if engine.results.contains_key("S") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            engine.tick(sample.clone()).await.unwrap();
        }
        let (h, s) = (&engine.results["H"], &engine.results["S"]);
        let retry = attempt_of(h);
        assert_eq!(
            (retry.launch, retry.cause, retry.moves.len()),
            (Some(Launch { cell_bytes: 576 * MIB, expected_bytes: 576 * MIB, lending_bytes: 0 }), Cause::Success, 0),
            "(a) {h}"
        );
        assert!(h["started_at"] != first, "(a) H's result is its retry's, not its first attempt's: {h}");
        let sibling = attempt_of(s);
        let moves: Vec<(AttemptId, u64, u64)> = sibling.moves.iter().map(|raise| (raise.donor.clone(), raise.lending_bytes, raise.cap_bytes)).collect();
        let donor = AttemptId { job_id: "H".into(), started_at: first.as_f64().unwrap() };
        assert_eq!(moves, [(donor, 48 * MIB, 320 * MIB)], "(b) {s}");
        assert_eq!(
            (sibling.launch, s["lending_bytes"].as_u64(), s["memory_limit_bytes"].as_u64()),
            (Some(Launch { cell_bytes: 256 * MIB, expected_bytes: 256 * MIB, lending_bytes: 0 }), Some(48 * MIB), Some(320 * MIB)),
            "(c) {s}"
        );
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// Q-E3: a synthetic local test of the account's preservation across a move, not a reachable producer state: S
    /// already holds B of lending, so no admitted donor beside it holds 768 MiB more.
    #[tokio::test]
    async fn the_launch_figures_survive_a_move() {
        let (mut engine, mut sample) = admission_fixture().await;
        engine.overcommit = 3 * GIB;
        publish_expecting(&mut engine, "p", "G", &[("S", 4 * GIB, Some(GIB))], 0.0, 0).await;
        let now = engine.clock.monotonic_secs();
        start_holding(&mut engine, &mut sample, "S", 0, now, 3 * GIB).await;
        engine.regrow("G", Charge::held(GIB, 768 * MIB), AttemptId { job_id: "d".into(), started_at: 5.0 }).await.unwrap();
        end(&mut engine, "S");
        engine.tick(sample.clone()).await.unwrap();
        let result = &engine.results["S"];
        let attempt = attempt_of(result);
        assert_eq!(attempt.launch, Some(Launch { cell_bytes: 4 * GIB, expected_bytes: GIB, lending_bytes: 3 * GIB }), "(a) {result}");
        let moves: Vec<(AttemptId, u64, u64)> = attempt.moves.iter().map(|raise| (raise.donor.clone(), raise.lending_bytes, raise.cap_bytes)).collect();
        assert_eq!(moves, [(AttemptId { job_id: "d".into(), started_at: 5.0 }, 256 * MIB, 4 * GIB + GIB / 2)], "(b) {result}");
        assert_eq!((result["memory_limit_bytes"].as_u64(), result["lending_bytes"].as_u64()), (Some(4 * GIB + GIB / 2), Some(3 * GIB + 256 * MIB)), "(c)");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// Q-E4.
    #[tokio::test]
    async fn a_controller_restoration_marks_each_running_peak() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 20 * GIB;
        publish(&mut engine, "p", "r", &[("r", GIB)], 0.0, 0).await;
        publish(&mut engine, "q", "q", &[("q", GIB)], 0.0, 0).await;
        let now = engine.clock.monotonic_secs();
        start_holding(&mut engine, &mut sample, "r", 0, now, 0).await;
        engine.controller_restored();
        start_holding(&mut engine, &mut sample, "q", 0, now, 0).await;
        end(&mut engine, "r");
        end(&mut engine, "q");
        engine.tick(sample.clone()).await.unwrap();
        assert!(attempt_of(&engine.results["r"]).peak_reset, "(a) r ran through the restoration");
        assert!(!attempt_of(&engine.results["q"]).peak_reset, "(b) q launched after it");
        replays(&engine);
        finish_fixture(engine, sample).await;
    }

    /// Q-E6: DECISION-RA-37 fixture 1, the producer side.
    #[tokio::test]
    async fn an_intent_restored_before_spawn_concludes_lost_without_a_launch() {
        let (mut e1, sample) = admission_fixture().await;
        e1.overcommit = 3 * GIB;
        publish_expecting(&mut e1, "p", "g", &[("J", 4 * GIB, Some(GIB))], 0.0, 0).await;
        let state = e1.config.state.clone();
        let started_at = e1.clock.wall();
        // The durable state a crash between the intent's write and the spawn leaves (`launch`, with `hold`).
        let intent = json!({"group_id":"g","job_id":"J","boot_id":e1.instance.boot_id,"pid":null,"start_ticks":null,
            "cpu":sample.cores[0],"memory_bytes":4 * GIB,"lending_bytes":3 * GIB,"job":e1.groups["g"].jobs[0],
            "log_path":state.join("logs").join("J.log"),"queued_at":e1.queued_at("g"),"started_at":started_at,
            "started_monotonic":e1.clock.monotonic_secs()});
        wrote(&state.join("running").join("J.json"), &intent).await;
        drop(e1);
        let mut e2 = restored(&state).await.unwrap();
        e2.read_memory = sample_alone();
        e2.overcommit = 3 * GIB;
        e2.tick(sample.clone()).await.unwrap();
        let result = &e2.results["J"];
        assert_eq!((&result["outcome"], result["started_at"].as_f64(), &result["returncode"]), (&json!("lost"), Some(started_at), &Value::Null), "(a) {result}");
        assert_eq!(attempt_of(result), attempts::Attempt { launch: None, moves: Vec::new(), cause: Cause::Lost, peak_reset: false }, "(b) {result}");
        assert_eq!(result["lending_bytes"].as_u64(), Some(3 * GIB), "(c)");
        assert!(neither_count(result), "(d) {result}");
        replays(&e2);
        finish_fixture(e2, sample).await;
    }
}
