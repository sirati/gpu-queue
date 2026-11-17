//! One actor owns publication, fair admissions, process custody and results.
use super::{
    budget::{Budget, Purse},
    containment::{Containment, Placement},
    contract::{self, Samples},
    decision::{Ahead, Counted, Decision, Decisions, Entry, Journal, Phase, Record, Subject},
    fairness::{self, LastTurn, Shares},
    host_memory::{Opened, Turn},
    memory_growth::Growth,
    model::{Group, Job},
    party::{self, Parties},
    precedence::{self, At, Candidate, Capacity, Catalogue, Class, Head, RequestId, Standing, Units, Work},
    progress::Progress,
    resources::Snapshot,
    store,
    watch::{Reply, Watches},
    Config,
};
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
const CAPABILITIES: &[&str] = &["peaks", "purpose", "transfer", "preparation_wait", "result_sink"];

struct Running {
    record: Value,
    child: Option<Child>,
    exit: Option<i32>,
    awaiting_exit: bool,
    samples: Samples,
}
impl Running {
    /// Whether the service's contract can end this compile: it declares its runtime, and its start is known in
    /// this boot. One that is not is held outside the service: its projection, the shares and the aged head's
    /// capacity rule do not count it.
    fn contracted(&self, boot: &str) -> bool {
        self.record["job"]["expected_runtime_secs"].is_u64() && self.record["boot_id"] == boot
    }
}
struct Admission<'a> {
    sample: &'a Snapshot,
    used: BTreeSet<usize>,
    memory: u64,
    /// The aged head, as the last tick left it.
    head: Option<&'a Head>,
    /// While the head does not fit, a job runs ahead only if it declares no longer.
    horizon: f64,
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
    boot: String,
    sequence: u64,
    draining: bool,
    host_memory: super::host_memory::HostMemory,
    containment: Containment,
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
    read_memory: fn() -> Result<(u64, u64), String>,
}
fn identity(pid: u32) -> Result<(u64, char), String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).map_err(|e| e.to_string())?;
    let (_, tail) = stat.rsplit_once(')').ok_or("bad process stat")?;
    let fields: Vec<&str> = tail.split_whitespace().collect();
    Ok((
        fields
            .get(19)
            .ok_or("missing process birth")?
            .parse::<u64>()
            .map_err(|e| e.to_string())?,
        fields[0].chars().next().unwrap_or('X'),
    ))
}
/// Seconds a running compile has run; a run from an earlier boot has no known start.
fn elapsed(record: &Value, boot: &str, now: f64) -> f64 {
    match record["boot_id"] == boot {
        true => (now - record["started_monotonic"].as_f64().unwrap_or(now)).max(0.0),
        false => 0.0,
    }
}
fn monotonic() -> f64 {
    let mut t: libc::timespec = unsafe { std::mem::zeroed() };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut t) };
    t.tv_sec as f64 + t.tv_nsec as f64 / 1e9
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
impl Engine {
    pub async fn restore(config: Config) -> Result<Self, String> {
        let manifest = config.state.join("format.json");
        let format = json!({"format":"rust-compiler-queue", "version":2});
        if manifest.exists() {
            if store::read(&manifest).await? != format {
                return Err("unsupported compiler state format".into());
            }
        } else {
            let mut entries = tokio::fs::read_dir(&config.state)
                .await
                .map_err(|e| e.to_string())?;
            while let Some(entry) = entries.next_entry().await.map_err(|e| e.to_string())? {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("format.") && name.ends_with(".tmp") {
                    tokio::fs::remove_file(entry.path())
                        .await
                        .map_err(|e| e.to_string())?;
                } else {
                    return Err("compiler state directory must be empty on first start".into());
                }
            }
            store::write(&manifest, &format).await?;
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
            tokio::fs::create_dir_all(config.state.join(dir))
                .await
                .map_err(|e| e.to_string())?;
            store::discard_abandoned(&config.state.join(dir)).await?;
        }
        store::discard_abandoned(&config.state).await?;
        let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .map_err(|e| e.to_string())?
            .trim()
            .to_owned();
        let host_memory = super::host_memory::HostMemory::restore(&config.state, &boot).await?;
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
            boot,
            sequence: 0,
            draining: false,
            host_memory,
            containment: Containment::establish(),
            growth: Growth::default(),
            bounds: BTreeMap::new(),
            left_after_launch: None,
            journal: Journal::default(),
            sink_errors: BTreeMap::new(),
            read_memory: super::resources::memory,
        };
        let growth = engine.config.state.join("growth.json");
        if growth.exists() {
            engine.growth =
                serde_json::from_value(store::read(&growth).await?).map_err(|e| e.to_string())?;
        }
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
            let group: Group =
                serde_json::from_value(store::read(&path).await?).map_err(|e| e.to_string())?;
            group.validate()?;
            engine.remember(group)?;
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
                tokio::fs::remove_file(path)
                    .await
                    .map_err(|e| e.to_string())?;
            } else {
                engine.progress.start(&id);
                engine.running.insert(
                    id,
                    Running {
                        awaiting_exit: alive(&record["exit_reaper"], &engine.boot),
                        record,
                        child: None,
                        exit: None,
                        samples: Samples::default(),
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
                            store::write(
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
            tokio::fs::remove_file(&path)
                .await
                .map_err(|e| e.to_string())?;
        }
        engine
            .containment
            .prune(|job| engine.running.contains_key(job));
        engine.recontain();
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
        Ok(engine)
    }
    fn remember(&mut self, group: Group) -> Result<(), String> {
        if group
            .jobs
            .iter()
            .any(|job| self.progress.owner(&job.job_id).is_some())
        {
            return Err("duplicate compiler job identity across groups".into());
        }
        let timestamp = std::fs::metadata(
            self.config
                .state
                .join("groups")
                .join(format!("{}.json", group.group_id)),
        )
        .and_then(|m| m.modified())
        .map_err(|e| e.to_string())?
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_secs_f64();
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
        Ok(())
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
        store::write_all(&entries).await?;
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
        self.completed.insert(group.clone(), crate::protocol::now_unix());
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
                store::write(&sink.join(format!("{group}.results.json")), &results).await
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
        store::remove_all(
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
        store::sync_directory(&descriptors).await?;
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
        store::write(
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
        store::remove_all(paths).await
    }
    fn result(&self, group: &str, job: &Job, outcome: &str, detail: &str) -> Value {
        json!({"group_id":group,"job_id":job.job_id,"outcome":outcome,"detail":detail,"returncode":null,
            "queued_at":self.queued_at(group),"started_at":null,"finished_at":if outcome == "queued" {Value::Null}else{json!(crate::protocol::now_unix())},"execution_seconds":0.0,"log_path":""})
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
        for running in self.running.values().filter(|running| running.contracted(&self.boot)) {
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
            .filter(|(_, running)| running.contracted(&self.boot))
            .map(|(id, running)| Counted {
                work: Work::compile(id, running.record["started_at"].as_f64().unwrap_or(0.0)),
                bytes: running.record["memory_bytes"].as_u64().unwrap_or(self.config.average_ram),
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
                    contracted: units.contracted + usize::from(running.contracted(&self.boot)),
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
    /// It waits for memory when its cell exceeds both this tick's `left` and what the last tick's launches left;
    /// one that fits either waits for a slot, which the slot shares serve. No compile is a candidate while the
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
                let cell = self.memory(job);
                Class::of(cell) == Class::Ordinary
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
                    bytes: self.memory(job),
                    capacity: Some(self.capacity(group)),
                });
            }
        }
        candidates
    }

    /// How a compile head of an earlier tick stands: still pending or not, its cell and its capacity.
    fn compile_standing(&self) -> Option<Standing> {
        let (group, job) = match self.host_memory.head().map(|head| &head.id) {
            Some(RequestId::Compile { group, job }) => (group, job),
            Some(RequestId::Claim { .. }) | None => return None,
        };
        let group = self.groups.get(group)?;
        let job = group.jobs.iter().find(|j| &j.job_id == job)?;
        Some(Standing {
            pending: !self.results.contains_key(&job.job_id) && !self.running.contains_key(&job.job_id),
            bytes: self.memory(job),
            capacity: Some(self.capacity(group)),
        })
    }

    /// The smallest and largest cell among a group's jobs.
    fn cells(&self, group: &Group) -> (u64, u64) {
        group
            .jobs
            .iter()
            .map(|job| self.memory(job))
            .fold((u64::MAX, 0), |(smallest, largest), cell| (smallest.min(cell), largest.max(cell)))
    }

    /// Every pending compile job's decision. The head and the jobs giving way are listed by themselves; for the
    /// rest of a group, one decision where its cell bounds leave its jobs one, otherwise how many have each.
    fn decisions(&self, serviceable: u64) -> BTreeMap<Subject, Decisions> {
        let head = self.host_memory.head();
        let decide = |cell: u64| Decision::waiting(Class::of(cell), cell, serviceable, false);
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
                            *counts.entry(decide(self.memory(job))).or_insert(0) += 1;
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

    /// How much longer the longest admitted compile is declared to run: a job
    /// declaring no longer finishes before the reservation could start anyway.
    fn horizon(&self) -> f64 {
        self.running
            .values()
            .map(|running| {
                let job = &running.record["job"];
                contract::remaining(
                    job["expected_runtime_secs"].as_u64(),
                    elapsed(&running.record, &self.boot, monotonic()),
                )
            })
            .fold(0.0, f64::max)
    }

    /// Each running compile's usage is sampled; one past twice its declared
    /// runtime is ended.
    async fn observe(&mut self) -> Result<(), String> {
        let now = monotonic();
        let mut overran = Vec::new();
        for (id, running) in &mut self.running {
            if let Some((memory, cpu)) = self.containment.sample(id) {
                running.samples.observe(memory, cpu, now);
            }
            let declared = running.record["job"]["expected_runtime_secs"].as_u64();
            if contract::overran(declared, elapsed(&running.record, &self.boot, now))
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
            store::write(
                &self.config.state.join("running").join(format!("{id}.json")),
                &self.running[&id].record,
            )
            .await?;
        }
        Ok(())
    }

    fn admissible(&self, sample: &Snapshot) -> (BTreeSet<usize>, u64) {
        let (used, reserved) = self.reserved(sample);
        let memory = sample
            .available
            .saturating_sub(reserved)
            .saturating_sub(self.config.minimum_ram);
        (used, memory)
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
            horizon: self.horizon(),
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
        if self.memory(job) > admission.memory && holds {
            return format!(
                "waiting for RAM, holding the aged compiler memory reservation: needs {} bytes, {} bytes admissible after headroom/reservations",
                self.memory(job),
                admission.memory
            );
        }
        let large = self.class(job).waiting().filter(|_| self.memory(job) > admission.memory);
        if let Some(reason) = large {
            return reason.into();
        }
        if let Some(head) = admission.head.filter(|head| !head.fits) {
            let runs_ahead = self.memory(job) <= admission.memory
                && job
                    .expected_runtime_secs
                    .is_some_and(|seconds| seconds as f64 <= admission.horizon);
            if !holds && !runs_ahead {
                return format!(
                    "waiting for aged compiler memory reservation: {}; a job runs ahead only if its memory fits and it declares at most the {:.0} s the longest running compile has left",
                    head.id, admission.horizon
                );
            }
        }
        if self.memory(job) > admission.memory {
            return format!(
                "waiting for RAM: needs {} bytes, {} bytes admissible after headroom/reservations",
                self.memory(job),
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
        }
    }
    pub fn await_reapers(&mut self, ids: &BTreeSet<String>) {
        for (id, running) in &mut self.running {
            running.awaiting_exit = running.exit.is_none()
                && (ids.contains(id) || alive(&running.record["exit_reaper"], &self.boot));
        }
    }
    pub async fn publish_reaper_custody(&mut self) -> Result<(), String> {
        let pid = std::process::id();
        let reaper = json!({"pid":pid,"start_ticks":identity(pid)?.0,"boot_id":self.boot});
        for (id, running) in &mut self.running {
            if running.child.is_some() {
                running.record["exit_reaper"] = reaper.clone();
                store::write(
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
                store::write(
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
    /// every other operation is answered immediately.
    pub async fn serve(&mut self, request: Value, reply: Reply) {
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
                        self.sample.as_ref(),
                        self.config.minimum_ram,
                        reply,
                    )
                    .await
            }
            _ => {
                let _ = reply.send(self.request(request).await);
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
    /// A submission may name its party; otherwise it is the submitter's session.
    fn party(request: &Value) -> Result<String, String> {
        match request["party"].as_str() {
            Some(party) => {
                super::model::identifier(party)?;
                Ok(party.to_owned())
            }
            None => request["_peer_pid"]
                .as_u64()
                .and_then(|pid| u32::try_from(pid).ok())
                .and_then(party::session)
                .ok_or_else(|| {
                    "compiler submission needs a party or an authenticated submitter".into()
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
                    "queued_at":r["queued_at"],"started_at":r["started_at"],"finished_at":null,"execution_seconds":(monotonic()-r["started_monotonic"].as_f64().unwrap_or(monotonic())).max(0.0),"log_path":r["log_path"],"pid":r["pid"]});
            }
            self.result(id,job,"queued",&self.waiting_reason(admission.as_ref(),group,job))
        }).collect();
        Ok(json!({"group_id":id,"complete":self.progress.complete(id),"jobs":jobs,
            "result_sink_error":self.sink_errors.get(id)}))
    }
    async fn request(&mut self, request: Value) -> Result<Value, String> {
        let operation = request["op"].as_str().ok_or("missing compiler operation")?;
        if operation.starts_with("host_memory_") {
            return self
                .host_memory
                .request(&request, self.sample.as_ref(), self.config.minimum_ram)
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
                    json!({"protocol":1,"pid":std::process::id(),"start_ticks":identity(std::process::id())?.0,
                    "boot_id":self.boot,"running":self.running.len(),"draining":self.draining,"memory_containment":self.containment.describe(),"resources":resources,"subqueues":subqueues,
                    "parked_group_waits":self.group_watches.len(),
                    "host_memory":self.host_memory.health(self.sample.as_ref()),
                    "heads":self.host_memory.heads(),
                    "admission":self.journal.last(),
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
                if group
                    .jobs
                    .iter()
                    .any(|j| self.progress.owner(&j.job_id).is_some())
                {
                    return Err("duplicate compiler job identity".into());
                }
                let party = Self::party(&request)?;
                let priority = party::priority(&request)?;
                let id = group.group_id.clone();
                let state = &self.config.state;
                let descriptor = serde_json::to_value(&group).map_err(|e| e.to_string())?;
                let record = json!({"group_id":id,"party":party,"priority":priority});
                store::write_all(&[
                    (
                        &state.join("groups").join(format!("{id}.json")),
                        &descriptor,
                    ),
                    (&state.join("parties").join(format!("{id}.json")), &record),
                ])
                .await?;
                self.remember(group)?;
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
                crate::protocol::now_unix().to_bits(),
                self.sequence
            ));
            store::write(
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
                store::write(
                    &self.config.state.join("running").join(format!("{id}.json")),
                    &active.record,
                )
                .await?;
            }
            // Every result and mark the journal bridged is durable now.
            tokio::fs::remove_file(&journal)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(json!({"group_id":group_id,"members":members}))
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
    /// The memory cell of the job's next attempt.
    fn memory(&self, job: &Job) -> u64 {
        let declared = if job.memory_bytes == 0 {
            self.config.average_ram
        } else {
            job.memory_bytes
        };
        self.growth
            .limit(&job.job_id, declared)
            .min(job.error_memory_bytes.unwrap_or(u64::MAX))
    }
    /// The job's class, by its next attempt's cell: OOM growth can make a job large.
    fn class(&self, job: &Job) -> Class {
        Class::of(self.memory(job))
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
                    .or_insert(Turn::Compiler(self.memory(job), self.parties.priority(id)));
            }
        }
        let mut shares = Shares::new(held);
        for (party, turn) in next {
            shares.wait(party, turn);
        }
        shares
    }
    /// Each party's RAM holding of running compilers: their declared memory.
    fn ram_held(&self) -> BTreeMap<String, u64> {
        self.held(|running| {
            running.record["memory_bytes"]
                .as_u64()
                .unwrap_or(self.config.average_ram)
        })
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
            eprintln!(
                "compile-queue: memory cells lost their controller; restored {} running cell(s)",
                self.recontain()
            );
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
        let now = crate::protocol::now_unix();
        let headroom = self.config.minimum_ram;
        let mut record = self.journal.begin(now);
        let compiler_reserved = self.compiler_reserved(&sample).1;
        let live = self.live_parties();
        let fresh = (self.read_memory)()?.0;
        let own = self.own_work();
        let opened = self
            .host_memory
            .open(&sample, fresh, compiler_reserved, headroom, &live, own, &mut record)
            .await?;
        let candidates = self.compile_candidates(opened, now);
        let standing = self.compile_standing();
        self.host_memory
            .choose(&sample, headroom, candidates, standing, self.draining, now, &mut record);
        let turns = self.compiler_turns(&sample);
        let budget = self.host_memory.serve(&sample, headroom, turns, &mut record).await?;
        self.retire(now).await?;
        let mut used = self.compiler_reserved(&sample).0;
        let mut purse = Purse {
            memory: budget,
            reserved: record.aged_reserved,
        };
        let oldest = self.oldest_unfinished();
        if !self.draining {
            self.launch_ordinary(&sample, &mut used, &mut purse, &oldest, &mut record).await?;
        }
        self.left_after_launch = Some(purse.memory.floor());
        record.left_after_launch = purse.memory.floor();
        purse.memory = self
            .host_memory
            .close(&sample, headroom, self.ram_held(), Class::Ordinary, purse.memory, &mut record)
            .await?;
        if !self.draining {
            self.launch_large(&sample, &mut used, &mut purse, &oldest, &mut record).await?;
        }
        self.host_memory
            .close(&sample, headroom, self.ram_held(), Class::Large, purse.memory, &mut record)
            .await?;
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
        let beyond = sample.capacity > 0 && self.memory(job) > sample.capacity.saturating_sub(self.config.minimum_ram);
        match (beyond, job.expected_runtime_secs) {
            (true, _) => Some("compiler memory requirement exceeds host/cgroup capacity after required headroom"),
            (false, None) => Some(contract::UNDECLARED_RUNTIME),
            (false, Some(_)) => None,
        }
    }
    /// Ordinary jobs take the free slots by slot shares, the aged head among them. A job's budget is the memory
    /// left plus what is reserved for it; while the head does not fit, a job declaring at most the horizon may
    /// run ahead into its bytes.
    async fn launch_ordinary(
        &mut self,
        sample: &Snapshot,
        used: &mut BTreeSet<usize>,
        purse: &mut Purse,
        oldest: &BTreeMap<String, f64>,
        record: &mut Record,
    ) -> Result<(), String> {
        let head = self.host_memory.head().map(|head| (head.id.clone(), head.fits));
        let horizon = head.as_ref().filter(|(_, fits)| !fits).map(|_| self.horizon());
        for _ in 0..Self::free_slots(sample, used) {
            let reserved_for = |job: &Job| {
                head.as_ref().is_some_and(|(id, _)| id.is_job(&job.job_id))
                    || horizon.is_some_and(|horizon| {
                        job.expected_runtime_secs.is_some_and(|seconds| seconds as f64 <= horizon)
                    })
            };
            let Some(selected) = self.by_slot_share(oldest, |job| {
                self.class(job) == Class::Ordinary && purse.fits(self.memory(job), reserved_for(job))
            }) else {
                break;
            };
            let own = head.as_ref().is_some_and(|(id, _)| id.is_job(&selected.1.job_id));
            if !self.start(sample, used, purse, selected, Phase::Launch, own, horizon, record).await? {
                break;
            }
        }
        Ok(())
    }
    /// Large jobs take the slots ordinary jobs left, from what every ordinary request of the tick left.
    async fn launch_large(
        &mut self,
        sample: &Snapshot,
        used: &mut BTreeSet<usize>,
        purse: &mut Purse,
        oldest: &BTreeMap<String, f64>,
        record: &mut Record,
    ) -> Result<(), String> {
        for _ in 0..Self::free_slots(sample, used) {
            let Some(selected) = self
                .by_slot_share(oldest, |job| self.class(job) == Class::Large && purse.fits(self.memory(job), false))
            else {
                break;
            };
            if !self.start(sample, used, purse, selected, Phase::LargeLaunch, false, None, record).await? {
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
    /// Launches the selected job on a free eligible core, charging its cell once: the head from its reservation
    /// first, every other job from memory first and, running ahead, the rest from the reservation. Returns false
    /// when no core is free.
    #[allow(clippy::too_many_arguments)]
    async fn start(
        &mut self,
        sample: &Snapshot,
        used: &mut BTreeSet<usize>,
        purse: &mut Purse,
        (group, job): (String, Job),
        phase: Phase,
        own: bool,
        horizon: Option<f64>,
        record: &mut Record,
    ) -> Result<bool, String> {
        let Some(cpu) = sample.cores.iter().find(|c| !used.contains(c)).copied() else {
            return Ok(false);
        };
        let cell = self.memory(&job);
        let (budget, reserved) = (purse.memory, purse.reserved);
        let paid = purse.charge(cell, own);
        used.insert(cpu);
        let (id, declared) = (job.job_id.clone(), job.expected_runtime_secs);
        self.launch(group.clone(), job, cpu).await?;
        let started = self.running.get(&id).map(|running| running.record["started_at"].as_f64().unwrap_or(0.0));
        let ahead = (!own && paid > 0).then(|| Ahead {
            declared: declared.unwrap_or(0),
            horizon: horizon.unwrap_or(0.0),
            work: Work::compile(&id, started.unwrap_or(0.0)),
        });
        if let Some(ahead) = ahead.as_ref().filter(|_| started.is_some()) {
            self.host_memory.ran_ahead(ahead.work.clone());
        }
        record.admitted.push(Entry::Launched {
            phase,
            group,
            job: id,
            class: Class::of(cell),
            bytes: cell,
            budget,
            reserved,
            head: own,
            ahead,
        });
        Ok(true)
    }
    async fn launch(&mut self, group: String, job: Job, cpu: usize) -> Result<(), String> {
        self.served.insert(
            self.parties.of(&group).to_owned(),
            crate::protocol::now_unix(),
        );
        let id = job.job_id.clone();
        let log_path = self.config.state.join("logs").join(format!("{id}.log"));
        let path = self.config.state.join("running").join(format!("{id}.json"));
        let limit = self.memory(&job);
        let mut record = json!({"group_id":group,"job_id":id,"boot_id":self.boot,"pid":null,"start_ticks":null,
            "cpu":cpu,"memory_bytes":limit,"job":job,"log_path":log_path,
            "queued_at":self.queued_at(&group),"started_at":crate::protocol::now_unix(),"started_monotonic":monotonic()});
        // The intent is durable before spawn. An interrupted publication is an
        // explicit unknown result after restart; only an attempt that outgrew
        // its memory cell runs again.
        store::write(&path, &record).await?;
        let mut log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| e.to_string())?;
        writeln!(log, "compile-queue: attempt in a {} MiB memory cell", limit >> 20)
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
                if let Err(error) = store::write(&path, &record).await {
                    let _ = child.kill().await;
                    return Err(error);
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
                tokio::fs::remove_file(&path)
                    .await
                    .map_err(|e| e.to_string())?;
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
            if running.exit.is_none() {
                if let Some(child) = running.child.as_mut() {
                    running.exit = child.try_wait().map_err(|e| e.to_string())?.map(|s| {
                        use std::os::unix::process::ExitStatusExt;
                        s.code().unwrap_or_else(|| -s.signal().unwrap_or(0))
                    });
                    if let Some(code) = running.exit {
                        running.record["returncode"] = json!(code);
                        store::write(
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
                    if running.exit.is_none() && receipt.exists() {
                        let value = store::read(&receipt).await?;
                        if value["pid"] == running.record["pid"] {
                            running.exit = value["returncode"].as_i64().map(|v| v as i32);
                            running.awaiting_exit = false;
                        }
                    }
                }
            }
            if running.awaiting_exit
                && !running.record["exit_reaper"].is_null()
                && !alive(&running.record["exit_reaper"], &self.boot)
            {
                running.awaiting_exit = false;
            }
            let root_alive = alive(&running.record, &self.boot);
            let pid = running.record["pid"].as_u64().unwrap_or(0) as u32;
            let mut members: Vec<_> = if running.record["boot_id"] == self.boot {
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
                let now = monotonic();
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
                    store::write(
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
                && (root_alive || running.child.is_some() || running.awaiting_exit)
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
                outgrew.push((group.to_owned(), limit));
                let mut log = std::fs::OpenOptions::new()
                    .append(true)
                    .open(self.config.state.join("logs").join(format!("{id}.log")))
                    .map_err(|e| e.to_string())?;
                writeln!(log, "compile-queue: out of memory in its {} MiB cell", limit >> 20)
                    .map_err(|e| e.to_string())?;
            }
            if outgrown && limit < ceiling {
                self.progress.requeue(id);
                tokio::fs::remove_file(self.config.state.join("running").join(format!("{id}.json")))
                    .await
                    .map_err(|e| e.to_string())?;
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
            let seconds = elapsed(record, &self.boot, monotonic());
            result["execution_seconds"] = json!(seconds);
            let report = contract::report(contract::Run {
                samples: &running.samples,
                peak: usage.peak,
                declared_memory: job.memory_bytes,
                declared_seconds: job.expected_runtime_secs.filter(|_| record["boot_id"] == self.boot),
                seconds,
                succeeded: outcome == "success",
                overran: record["overran"] == true,
            });
            for (key, value) in report.as_object().into_iter().flatten() {
                result[key] = value.clone();
            }
            self.finish(result).await?;
            tokio::fs::remove_file(self.config.state.join("running").join(format!("{id}.json")))
                .await
                .map_err(|e| e.to_string())?;
            finished.insert(id.clone());
        }
        active.retain(|id, _| !finished.contains(id));
        self.running = active;
        for (group, limit) in outgrew {
            self.regrow(&group, limit).await?;
        }
        Ok(())
    }
    /// An attempt outgrew its memory cell: its subqueue grows.
    async fn regrow(&mut self, group: &str, freed: u64) -> Result<(), String> {
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
            .map(|pending| (pending.job_id.clone(), self.memory(pending)))
            .collect();
        for (id, limit) in self.growth.out_of_memory(freed, &running, &waiting) {
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
                sibling.record["memory_bytes"] = json!(limit);
                store::write(
                    &self.config.state.join("running").join(format!("{id}.json")),
                    &sibling.record,
                )
                .await?;
            }
        }
        store::write(
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
    use super::super::decision;
    use super::super::precedence::{Step, Verdict};
    use super::*;
    use std::path::PathBuf;

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
        })
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
            engine.request(json!({"protocol":1,"op":"submit_group","party":group,"group":{"group_id":group,"jobs":jobs,"max_running":limit}})).await.unwrap();
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
        })
        .await
        .unwrap();
        // The fixture's sample alone says what memory is available.
        engine.read_memory = || Ok((u64::MAX, u64::MAX));
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
            .request(
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
            .launch("existing".into(), job, sample.cores[0])
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
            .launch("existing".into(), job, sample.cores[0])
            .await
            .unwrap();
        let pid = engine.running["existing"].record["pid"].as_u64().unwrap() as u32;
        sample.processes.insert(pid, (std::process::id(), 4 << 30));
        engine.tick(sample.clone()).await.unwrap();
        for i in 0..2 {
            admit_group(&mut engine, &format!("small-{i}"), 1 << 30, 60.0).await;
        }
        let mut owner = std::process::Command::new("sleep").arg("60").spawn().unwrap();
        engine
            .request(json!({"protocol":1,"op":"host_memory_acquire","_peer_pid":owner.id(),"claim_id":"preparation","memory_bytes":1u64 << 30,"label":"preparation","purpose":"host_only"}))
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
        let sleeper = || std::process::Command::new("sleep").arg("60").spawn().unwrap();
        let claim = |owner: &std::process::Child, id: &str, bytes: u64| json!({"protocol":1,"op":"host_memory_acquire","_peer_pid":owner.id(),"claim_id":id,"memory_bytes":bytes,"label":id});
        // Another campaign's granted, still unused claim of unknown purpose holds memory no compiler can drain.
        let mut owner = sleeper();
        let mut waiting = sleeper();
        sample.available = 35 * GIB / 2;
        engine.request(claim(&owner, "elsewhere", 3 * GIB)).await.unwrap();
        publish(&mut engine, "x", "existing", &[("existing", 2 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "existing", 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "elsewhere")["state"], "granted");
        publish(&mut engine, "belief", "large", &[("large", 4 * GIB)], 120.0, 0).await;
        publish(&mut engine, "rwkv", "younger", &[("younger", 3 * GIB)], 60.0, 0).await;
        engine.request(claim(&waiting, "oversized", 6 * GIB)).await.unwrap();
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
            .request(submit(json!({"job_id":"undeclared","argv":waiting(),"cwd":state})))
            .await
            .unwrap_err();
        assert!(undeclared.contains("declares no expected_runtime_secs"), "{undeclared}");
        let small = engine
            .request(submit(json!({"job_id":"small","argv":waiting(),"cwd":state,"memory_bytes":1u64 << 30,"expected_runtime_secs":60})))
            .await
            .unwrap();
        assert_eq!(small["warnings"], json!([]));
        let large = engine
            .request(submit(json!({"job_id":"large","argv":waiting(),"cwd":state,"memory_bytes":11_000_000_000u64,"expected_runtime_secs":60})))
            .await
            .unwrap();
        assert!(large["warnings"][0].as_str().unwrap().starts_with("large: declares 11000000000 bytes"));
        let mut owner = std::process::Command::new("sleep").arg("60").spawn().unwrap();
        let claim = engine
            .request(json!({"protocol":1,"op":"host_memory_register","_peer_pid":owner.id(),"claim_id":"large-claim","memory_bytes":11_000_000_000u64,"label":"large claim"}))
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
            .launch("existing".into(), job, sample.cores[0])
            .await
            .unwrap();
        let pid = engine.running["existing"].record["pid"].as_u64().unwrap() as u32;
        sample.processes.insert(pid, (std::process::id(), 4 << 30));
        // A compile is a candidate only once a tick has launched since the restart.
        engine.tick(sample.clone()).await.unwrap();
        let state = engine.config.state.clone();
        for (name, seconds) in [("short", 30), ("long", 120)] {
            engine
                .request(json!({"protocol":1,"op":"submit_group","party":name,"group":{"group_id":name,"jobs":[
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
        let horizon = admission.as_ref().unwrap().horizon;
        assert!(horizon > 30.0 && horizon <= 60.0, "{horizon}");
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_compile_past_twice_its_declared_runtime_is_ended_and_its_result_says_so() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.available = 64 << 30;
        let state = engine.config.state.clone();
        engine
            .request(json!({"protocol":1,"op":"submit_group","party":"slow","group":{"group_id":"slow","jobs":[
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
            .request(json!({"protocol":1,"op":"submit_group","party":"measured","group":{"group_id":"measured","jobs":[
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
                std::process::Command::new("sleep")
                    .arg("60")
                    .spawn()
                    .unwrap()
            })
            .collect();
        let claim = |owner: &std::process::Child, id: &str| json!({"protocol":1,"op":"host_memory_acquire","_peer_pid":owner.id(),"claim_id":id,"memory_bytes":1u64 << 30,"label":"preparation"});
        engine.request(claim(&owners[0], "held")).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        admit_group(&mut engine, "compiles", 1 << 30, 0.0).await;
        engine.request(claim(&owners[1], "next")).await.unwrap();
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
                .request(json!({"protocol":1,"op":"submit_group","party":format!("g{g}"),"group":{"group_id":format!("g{g}"),"jobs":jobs}}))
                .await
                .unwrap();
            engine
                .submitted
                .insert(format!("g{g}"), crate::protocol::now_unix() - 120.0);
        }
        engine.sample = Some(sample);
        let started = std::time::Instant::now();
        let status = engine
            .request(json!({"protocol":1,"op":"group_status","group_id":"g7"}))
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
        engine.serve(wait.clone(), reply).await;
        let (reply, departed) = tokio::sync::oneshot::channel();
        engine.serve(wait.clone(), reply).await;
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
        engine.serve(wait, reply).await;
        assert_eq!(complete.await.unwrap().unwrap()["complete"], true);
        let (reply, unknown) = tokio::sync::oneshot::channel();
        engine
            .serve(
                json!({"protocol":1,"op":"group_wait","group_id":"missing"}),
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
            .request(json!({"protocol":1,"op":"submit_group","party":"grows","group":{"group_id":"grows","jobs":jobs}}))
            .await
            .unwrap();
        let capped = json!([{"job_id":"capped","argv":allocate(96 * MIB),"cwd":state,"memory_bytes":64 * MIB,"error_memory_bytes":64 * MIB,"expected_runtime_secs":60}]);
        engine
            .request(json!({"protocol":1,"op":"submit_group","party":"ceiling","group":{"group_id":"ceiling","jobs":capped}}))
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
            .request(json!({"protocol":1,"op":"cancel","group_id":"bulk","reason":"bulk fixture"}))
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
        };
        let restored = Engine::restore(config).await.unwrap();
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
        store::write(
            &engine
                .config
                .state
                .join("cancellations")
                .join("crashed.json"),
            &json!({"pending":pending,"running":[],"reason":"interrupted fixture"}),
        )
        .await
        .unwrap();
        let config = Config {
            state: engine.config.state.clone(),
            minimum_ram: engine.config.minimum_ram,
            average_ram: engine.config.average_ram,
        };
        let restored = Engine::restore(config).await.unwrap();
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
            .request(
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
    async fn parties_survive_restore_and_unrecorded_groups_share_one() {
        let (mut engine, sample) = admission_fixture().await;
        admit_for(&mut engine, "alpha", "recorded", &[], 1 << 30, 0.0).await;
        admit_for(&mut engine, "beta", "legacy", &[], 1 << 30, 0.0).await;
        let answer = engine
            .request(
                json!({"protocol":1,"op":"submit_group","_peer_pid":std::process::id(),
                "group":{"group_id":"session","jobs":[]}}),
            )
            .await
            .unwrap();
        assert_eq!(
            answer["party"].as_str(),
            party::session(std::process::id()).as_deref()
        );
        let urgent = |id: &str, priority: i64| {
            json!({"protocol":1,"op":"submit_group","party":"urgent","priority":priority,
                "group":{"group_id":id,"jobs":[]}})
        };
        assert_eq!(
            engine.request(urgent("urgent", 10)).await.unwrap()["priority"],
            10
        );
        assert!(engine.request(urgent("beyond", 11)).await.is_err());
        std::fs::remove_file(engine.config.state.join("parties").join("legacy.json")).unwrap();
        let restored = Engine::restore(engine.handoff_config()).await.unwrap();
        assert_eq!(
            (
                restored.parties.priority("urgent"),
                restored.parties.priority("recorded")
            ),
            (10, 0)
        );
        assert_eq!(restored.parties.of("recorded"), "alpha");
        assert_eq!(restored.parties.of("legacy"), party::UNATTRIBUTED);
        finish_fixture(restored, sample).await;
    }

    #[tokio::test]
    async fn a_completed_group_is_retired_after_its_retention() {
        let (mut engine, sample) = admission_fixture().await;
        admit_jobs(&mut engine, "done", &["done-1"], 1 << 30, 0.0).await;
        let state = engine.config.state.clone();
        std::fs::write(state.join("logs").join("done.log"), b"compiler output").unwrap();
        engine
            .request(
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
        store::write(
            &state.join("results").join("orphan.json"),
            &json!({"group_id":"gone","job_id":"orphan","outcome":"success","execution_seconds":1.0}),
        )
        .await
        .unwrap();
        std::fs::write(state.join("logs").join("orphan.log"), b"output").unwrap();
        let restored = Engine::restore(engine.handoff_config()).await.unwrap();
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
        store::write(
            &state.join("groups").join(format!("{group}.json")),
            &json!({"group_id":group,"jobs":jobs,"max_running":max_running}),
        )
        .await
        .unwrap();
        store::write(&state.join("parties").join(format!("{group}.json")), &json!({"group_id":group,"party":party,"priority":0}))
            .await
            .unwrap();
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
        let mut predecessor = Engine::restore(config).await.unwrap();
        predecessor.read_memory = || Ok((u64::MAX, u64::MAX));
        // A successor restores the same state and fails before activation: it writes no refusal.
        let successor = Engine::restore(predecessor.handoff_config()).await.unwrap();
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
                .request(json!({"protocol":1,"op":"submit_group","party":"p","group":{"group_id":id,
                    "jobs":[{"job_id":id,"argv":waiting(),"cwd":state,"expected_runtime_secs":60}],
                    "result_sink":result_sink}}))
                .await
                .unwrap();
            engine
                .request(json!({"protocol":1,"op":"cancel","group_id":id,"reason":"sink fixture"}))
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
    async fn submit_party_group(engine: &mut Engine, party: &str, group: &str, jobs: &[(&str, u64)], max_running: usize) {
        let state = engine.config.state.clone();
        let jobs: Vec<_> = jobs
            .iter()
            .map(|(id, memory)| json!({"job_id":id,"argv":waiting(),"cwd":state,"memory_bytes":memory,"expected_runtime_secs":60}))
            .collect();
        engine
            .request(json!({"protocol":1,"op":"submit_group","party":party,
                "group":{"group_id":group,"jobs":jobs,"max_running":max_running}}))
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
        let mut restored = Engine::restore(config).await.unwrap();
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
        engine.launch("holding".into(), job, sample.cores[0]).await.unwrap();
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
        engine.launch("z-first".into(), job, sample.cores[0]).await.unwrap();
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
    async fn publish(engine: &mut Engine, party: &str, group: &str, jobs: &[(&str, u64)], age: f64, max_running: usize) {
        let program = waiting()[0].clone();
        let code = "import pathlib,sys,time\nwhile not (pathlib.Path('release').exists() or pathlib.Path('release-' + sys.argv[1]).exists()): time.sleep(0.01)";
        let state = engine.config.state.clone();
        let jobs: Vec<_> = jobs
            .iter()
            .map(|(id, memory)| json!({"job_id":id,"argv":[program,"-S","-c",code,id],"cwd":state,"memory_bytes":memory,"expected_runtime_secs":60}))
            .collect();
        engine
            .request(json!({"protocol":1,"op":"submit_group","party":party,
                "group":{"group_id":group,"jobs":jobs,"max_running":max_running}}))
            .await
            .unwrap();
        engine.submitted.insert(group.into(), crate::protocol::now_unix() - age);
    }
    /// Starts `job` at once on a free core; it uses `rss` bytes.
    async fn start(engine: &mut Engine, sample: &mut Snapshot, job: &str, rss: u64) {
        let group = engine.progress.owner(job).unwrap().to_owned();
        let spec = engine.groups[&group].jobs.iter().find(|j| j.job_id == job).unwrap().clone();
        let used: BTreeSet<u64> = engine.running.values().filter_map(|r| r.record["cpu"].as_u64()).collect();
        let cpu = *sample.cores.iter().find(|c| !used.contains(&(**c as u64))).unwrap();
        engine.launch(group, spec, cpu).await.unwrap();
        let pid = engine.running[job].record["pid"].as_u64().unwrap() as u32;
        sample.processes.insert(pid, (std::process::id(), rss));
    }
    /// Ends a running job; the next tick reaps it.
    fn end(engine: &mut Engine, job: &str) {
        let pid = engine.running[job].record["pid"].as_u64().unwrap() as u32;
        std::fs::write(engine.config.state.join(format!("release-{job}")), b"").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while crate::procs::alive(pid) {
            assert!(std::time::Instant::now() < deadline, "{job} did not end");
            std::thread::sleep(std::time::Duration::from_millis(5));
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
    /// Every tick's record replays, and the written ones check in order.
    fn replays(engine: &Engine) {
        for (record, _) in &engine.journal.history {
            if let Err(error) = decision::replay(record) {
                panic!("{error}: {}", serde_json::to_string(record).unwrap());
            }
        }
        let written: Vec<_> = engine.journal.history.iter().filter(|(_, written)| *written).map(|(r, _)| r.clone()).collect();
        decision::check(&written).unwrap();
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
            .request(json!({"protocol":1,"op":"health"}))
            .await
            .unwrap();
        assert_eq!(health["protocol"], 1);
        assert_eq!(health["host_memory"]["protocol"], 1);
        assert_eq!(
            health["capabilities"],
            json!(["peaks", "purpose", "transfer", "preparation_wait", "result_sink"])
        );
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
        let owner = Claimant::new(&mut sample);
        let held = Claimant::new(&mut sample);
        submit_party_group(&mut engine, &owner.party(), "live", &[], 0).await;
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
        engine.request(held.host_only("held", 4 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        publish(&mut engine, "a", "head", &[("head", 5 * GIB)], 120.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("head"));
        engine.request(held.release("held")).await.unwrap();
        engine.request(owner.host_only("younger", 5 * GIB)).await.unwrap();
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

    /// A process that owns a claim, in the session that `party` names: its own, or another claimant's.
    struct Claimant(std::process::Child, u32);
    impl Claimant {
        fn new(sample: &mut Snapshot) -> Self {
            let owner = std::process::Command::new("sleep").arg("60").spawn().unwrap();
            sample.sessions.insert(owner.id(), (owner.id(), 0));
            let session = owner.id();
            Self(owner, session)
        }
        /// A claimant of `party`'s session.
        fn beside(sample: &mut Snapshot, party: &Claimant) -> Self {
            let owner = std::process::Command::new("sleep").arg("60").spawn().unwrap();
            sample.sessions.insert(owner.id(), (party.1, 0));
            Self(owner, party.1)
        }
        fn party(&self) -> String {
            format!("session-{}-0", self.1)
        }
        fn claim(&self, id: &str, bytes: u64) -> Value {
            json!({"protocol":1,"op":"host_memory_acquire","_peer_pid":self.0.id(),"claim_id":id,"memory_bytes":bytes,"label":id})
        }
        fn host_only(&self, id: &str, bytes: u64) -> Value {
            let mut claim = self.claim(id, bytes);
            claim["purpose"] = json!("host_only");
            claim
        }
        fn release(&self, id: &str) -> Value {
            json!({"protocol":1,"op":"host_memory_release","_peer_pid":self.0.id(),"claim_id":id})
        }
    }
    impl Drop for Claimant {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
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
        let large = Claimant::new(&mut sample);
        let small = Claimant::new(&mut sample);
        // The small claim's party holds a running compile; the large claim's party holds nothing.
        submit_party_group(&mut engine, &large.party(), "large-party", &[], 0).await;
        submit_party_group(&mut engine, &small.party(), "small-party", &[("held", GIB)], 0).await;
        let job = engine.groups["small-party"].jobs[0].clone();
        engine.launch("small-party".into(), job, sample.cores[0]).await.unwrap();
        let pid = engine.running["held"].record["pid"].as_u64().unwrap() as u32;
        sample.processes.insert(pid, (std::process::id(), GIB));
        engine.request(large.claim("large", 12 * GIB)).await.unwrap();
        engine.request(small.claim("small", 2 * GIB)).await.unwrap();
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
        let owner = Claimant::new(&mut sample);
        submit_party_group(&mut engine, &owner.party(), "large", &[("large", 22 * GIB)], 0).await;
        engine.request(owner.claim("small", 2 * GIB)).await.unwrap();
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
        let owner = Claimant::new(&mut sample);
        submit_party_group(&mut engine, &owner.party(), "claimant", &[], 0).await;
        submit_party_group(&mut engine, "compiles", "held", &[("held", GIB)], 0).await;
        let job = engine.groups["held"].jobs[0].clone();
        engine.launch("held".into(), job, sample.cores[0]).await.unwrap();
        let pid = engine.running["held"].record["pid"].as_u64().unwrap() as u32;
        sample.processes.insert(pid, (std::process::id(), GIB));
        submit_party_group(&mut engine, "compiles", "next", &[("next", 2 * GIB)], 0).await;
        engine.request(owner.claim("large", 12 * GIB)).await.unwrap();
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
        let owner = Claimant::new(&mut sample);
        let jobs = [("j0", 2 * GIB), ("j1", 2 * GIB), ("j2", 2 * GIB)];
        submit_party_group(&mut engine, &owner.party(), "three", &jobs, 0).await;
        engine.request(owner.claim("large", 20 * GIB)).await.unwrap();
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
        let owner = Claimant::new(&mut sample);
        submit_party_group(&mut engine, "large", "large", &[("large", 12 * GIB)], 0).await;
        engine.request(owner.claim("ordinary", 9 * GIB)).await.unwrap();
        // 20 GiB admissible and free slots: the 9 GiB claim leaves 11 GiB, too little for the large job.
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "ordinary")["state"], "granted");
        assert!(!engine.running.contains_key("large"));
        assert_eq!(reason(&engine, "large", 0), LARGE_WAIT);
        drop(owner);
        finish_fixture(engine, sample).await;
    }

    // Part B, B-2: the aged head.

    #[tokio::test]
    async fn a_claim_its_partys_compiles_pass_over_becomes_the_aged_head_and_is_granted_at_the_next_completion() {
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new(&mut sample);
        let party = owner.party();
        publish(&mut engine, &party, "running", &[("r0", 3 * GIB), ("r1", 3 * GIB)], 0.0, 0).await;
        let queued: Vec<String> = (0..6).map(|i| format!("q{i}")).collect();
        let queue: Vec<(&str, u64)> = queued.iter().map(|id| (id.as_str(), 3 * GIB)).collect();
        publish(&mut engine, &party, "queue", &queue, 0.0, 0).await;
        start(&mut engine, &mut sample, "r0", 0).await;
        start(&mut engine, &mut sample, "r1", 0).await;
        // 1 GiB admissible beside two running 3 GiB compiles; each completion frees 3 GiB.
        sample.available = 17 * GIB;
        engine.request(owner.host_only("discovery", 5 * GIB / 4)).await.unwrap();
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
        assert_eq!(claim["waiting"], format!("aged head, party {party}'s turn: {} bytes reserved", 5 * GIB / 4));
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
        let lead = Claimant::new(&mut sample);
        submit_party_group(&mut engine, &lead.party(), "live", &[], 0).await;
        let mut small: Vec<Claimant> = (0..8).map(|_| Claimant::beside(&mut sample, &lead)).collect();
        for (i, owner) in small.iter().take(4).enumerate() {
            engine.request(owner.host_only(&format!("s{i}"), GIB)).await.unwrap();
        }
        engine.tick(sample.clone()).await.unwrap();
        engine.request(lead.host_only("big", 4 * GIB)).await.unwrap();
        // Each 1 GiB release goes to the next 1 GiB claim while the 4 GiB claim is young.
        for i in 0..2 {
            engine.request(small[4 + i].host_only(&format!("s{}", 4 + i), GIB)).await.unwrap();
            engine.request(small[i].release(&format!("s{i}"))).await.unwrap();
            engine.tick(sample.clone()).await.unwrap();
            assert_eq!(claim_status(&engine, &format!("s{}", 4 + i))["state"], "granted");
            assert_eq!(claim_status(&engine, "big")["state"], "pending");
        }
        engine.request(small[6].host_only("s6", GIB)).await.unwrap();
        engine.host_memory.backdate("big", 31.0);
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("big"), "S is 4 GiB: the granted claims' declarations");
        for i in 2..6 {
            engine.request(small[i].release(&format!("s{i}"))).await.unwrap();
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
        let first = Claimant::new(&mut sample);
        let claims: Vec<Claimant> = (0..3).map(|_| Claimant::beside(&mut sample, &first)).collect();
        submit_party_group(&mut engine, &first.party(), "claims", &[], 0).await;
        publish(&mut engine, "c", "running", &[("held", GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "held", 0).await;
        engine.request(first.host_only("x0", 6 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "x0")["state"], "granted");
        publish(&mut engine, "c", "compile", &[("compile", 5 * GIB)], 120.0, 0).await;
        engine.request(claims[0].host_only("x1", 6 * GIB)).await.unwrap();
        // 3 GiB left; S is 10 GiB. The compiling party holds more than the claiming one.
        engine.tick(sample.clone()).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("compile"));
        engine.request(first.release("x0")).await.unwrap();
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
        let owner = Claimant::new(&mut sample);
        let younger = Claimant::beside(&mut sample, &owner);
        submit_party_group(&mut engine, &owner.party(), "live", &[], 0).await;
        publish(&mut engine, "q", "running", &[("w", 2 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        engine.request(owner.host_only("older", 4 * GIB)).await.unwrap();
        engine.request(younger.host_only("younger", 3 * GIB)).await.unwrap();
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
        let a = Claimant::new(&mut sample);
        let b = Claimant::new(&mut sample);
        let c = Claimant::new(&mut sample);
        let mut owners = vec![];
        let names: BTreeMap<String, &str> = [(a.party(), "a"), (b.party(), "b"), (c.party(), "c")].into_iter().collect();
        for (lead, name) in [(&a, "a"), (&b, "b"), (&c, "c")] {
            submit_party_group(&mut engine, &lead.party(), &format!("live-{name}"), &[], 0).await;
            for i in 0..2 {
                let owner = Claimant::beside(&mut sample, lead);
                let id = format!("{name}{i}");
                engine.request(owner.host_only(&id, 3 * GIB)).await.unwrap();
                let age = match (name, i) { ("a", 0) => 100.0, (_, 0) => 90.0, _ => 80.0 };
                engine.host_memory.backdate(&id, age);
                owners.push(owner);
            }
        }
        publish(&mut engine, "q", "running", &[("w", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        let little = Claimant::beside(&mut sample, &a);
        let mut order = Vec::new();
        for step in 0..4 {
            // 1 GiB left: the head waits; then enough memory for it.
            let granted = engine.host_memory.reserved(&sample);
            sample.available = GIB + 10 * GIB + 4 * GIB + granted;
            engine.tick(sample.clone()).await.unwrap();
            let id = head(&engine).unwrap();
            order.push(names[&engine.host_memory.head().unwrap().party].to_owned());
            if step == 1 {
                engine.request(little.host_only("little", GIB / 2)).await.unwrap();
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

    /// A job of `party`'s group that declares `runtime` seconds and runs until its release file appears.
    async fn publish_timed(engine: &mut Engine, party: &str, group: &str, jobs: &[(&str, u64, u64)], age: f64, max_running: usize) {
        let program = waiting()[0].clone();
        let code = "import pathlib,sys,time\nwhile not (pathlib.Path('release').exists() or pathlib.Path('release-' + sys.argv[1]).exists()): time.sleep(0.01)";
        let state = engine.config.state.clone();
        let jobs: Vec<_> = jobs
            .iter()
            .map(|(id, memory, runtime)| json!({"job_id":id,"argv":[program,"-S","-c",code,id],"cwd":state,"memory_bytes":memory,"expected_runtime_secs":runtime}))
            .collect();
        engine
            .request(json!({"protocol":1,"op":"submit_group","party":party,
                "group":{"group_id":group,"jobs":jobs,"max_running":max_running}}))
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
    async fn while_the_head_does_not_fit_only_jobs_declaring_at_most_the_horizon_run_ahead_and_once_it_fits_none() {
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
        assert!(engine.host_memory.head().unwrap().recorded.iter().any(|work| matches!(work, Work::Compile { job, .. } if job == "short")));
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
        let a = Claimant::new(&mut sample);
        let b = Claimant::new(&mut sample);
        let a2 = Claimant::beside(&mut sample, &a);
        submit_party_group(&mut engine, &a.party(), "live-a", &[], 0).await;
        submit_party_group(&mut engine, &b.party(), "live-b", &[], 0).await;
        publish(&mut engine, "q", "running", &[("w", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        for (owner, id, age) in [(&a, "a1", 100.0), (&a2, "a2", 97.0), (&b, "b1", 95.0)] {
            engine.request(owner.host_only(id, 3 * GIB)).await.unwrap();
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
        let mut restored = Engine::restore(config).await.unwrap();
        restored.read_memory = || Ok((u64::MAX, u64::MAX));
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
        let gen16 = Claimant::new(&mut sample);
        let small = Claimant::new(&mut sample);
        sample.available = 20 * GIB;
        engine.request(gen16.claim("gen16", 8 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "gen16")["state"], "granted");
        publish(&mut engine, "p", "large", &[("large", 5 * GIB)], 120.0, 0).await;
        // 2 GiB left; the 8 GiB claim may wait for the GPU, so S is 2 GiB and the 5 GiB job cannot reserve.
        for round in 0..2 {
            engine.tick(sample.clone()).await.unwrap();
            publish(&mut engine, "s", &format!("small-{round}"), &[(&format!("small-{round}"), GIB / 2)], 0.0, 0).await;
            engine.request(small.host_only(&format!("claim-{round}"), GIB / 2)).await.unwrap();
            engine.tick(sample.clone()).await.unwrap();
            assert!(engine.running.contains_key(&format!("small-{round}")));
            assert_eq!(claim_status(&engine, &format!("claim-{round}"))["state"], "granted");
            engine.request(small.release(&format!("claim-{round}"))).await.unwrap();
            end(&mut engine, &format!("small-{round}"));
        }
        let record = &engine.journal.history.last().unwrap().0;
        assert!(record.own_work.iter().all(|counted| !matches!(&counted.work, Work::Claim { claim, .. } if claim == "gen16")));
        assert!(heads_seen(&engine).is_empty());
        assert!(reason(&engine, "large", 0).starts_with("waiting for RAM: needs"));
        // The same across a hand-over: the claim restores without a purpose.
        let config = engine.handoff_config();
        drop(engine);
        let mut restored = Engine::restore(config).await.unwrap();
        restored.read_memory = || Ok((u64::MAX, u64::MAX));
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
        let placed = Claimant::new(&mut sample);
        let allocating_dir = engine.config.state.join("allocating");
        std::fs::create_dir(&allocating_dir).unwrap();
        let program = waiting()[0].clone();
        let code = "import pathlib,time\nwhile not pathlib.Path('go').exists(): time.sleep(0.005)\nheld = bytearray(256 << 20)\nheld[::4096] = b'x' * len(held[::4096])\npathlib.Path('done').write_text('')\ntime.sleep(60)";
        let mut allocating = std::process::Command::new(program.as_str().unwrap()).args(["-S", "-c", code]).current_dir(&allocating_dir).spawn().unwrap();
        sample.sessions.insert(allocating.id(), (allocating.id(), 0));
        let small = Claimant::new(&mut sample);
        sample.available = 22 * GIB;
        let gpu = |owner: u32, id: &str, bytes: u64| json!({"protocol":1,"op":"host_memory_acquire","_peer_pid":owner,"claim_id":id,"memory_bytes":bytes,"label":id,"purpose":"gpu_place"});
        engine.request(gpu(placed.0.id(), "placed", 6 * GIB)).await.unwrap();
        engine.request(gpu(allocating.id(), "allocating", 2 * GIB)).await.unwrap();
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
            engine.request(small.host_only(&format!("claim-{round}"), GIB / 2)).await.unwrap();
            engine.tick(sample.clone()).await.unwrap();
            assert!(engine.running.contains_key(&format!("small-{round}")));
            assert_eq!(claim_status(&engine, &format!("claim-{round}"))["state"], "granted");
            engine.request(small.release(&format!("claim-{round}"))).await.unwrap();
            end(&mut engine, &format!("small-{round}"));
        }
        assert!(heads_seen(&engine).is_empty(), "nothing is reserved");
        assert!(engine.host_memory.reserved(&sample) < 8 * GIB, "one claim allocated part of its declaration");
        // A process outside the service holds the memory instead: the same.
        engine.request(json!({"protocol":1,"op":"host_memory_release","_peer_pid":placed.0.id(),"claim_id":"placed"})).await.unwrap();
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
        let owner = Claimant::new(&mut sample);
        let party = owner.party();
        publish(&mut engine, &party, "jobs", &[("five", 5 * GIB), ("three", 3 * GIB)], 31.0, 0).await;
        sample.available = 14 * GIB;
        let mut claimants = Vec::new();
        for round in 0..3 {
            let claimant = Claimant::beside(&mut sample, &owner);
            let id = format!("claim-{round}");
            engine.request(claimant.host_only(&id, 2 * GIB)).await.unwrap();
            engine.tick(sample.clone()).await.unwrap();
            let launched = engine.running.contains_key("three");
            match round {
                0 => {
                    assert_eq!(claim_status(&engine, &id)["state"], "granted", "a claim is granted from what is left");
                    assert!(!launched, "the 3 GiB job misses: the claim took its memory");
                }
                _ => {}
            }
            engine.request(claimant.release(&id)).await.unwrap();
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
        let claimant = Claimant::beside(&mut sample, &owner);
        engine.request(claimant.host_only("after", 2 * GIB)).await.unwrap();
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
                .request(json!({"protocol":1,"op":"submit_group","party":"p","group":{"group_id":format!("g{g}"),"jobs":jobs}}))
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
        let owner = Claimant::new(&mut sample);
        publish(&mut engine, "q", "forked", &[("forked", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "forked", 0).await;
        processes(&mut sample, &engine, "forked", &[4 * GIB, 4 * GIB]);
        engine.request(owner.host_only("eight", 8 * GIB)).await.unwrap();
        engine.host_memory.backdate("eight", 60.0);
        engine.tick(sample.clone()).await.unwrap();
        let record = &engine.journal.history.last().unwrap().0;
        assert_eq!(record.own_work.iter().map(|c| c.bytes).sum::<u64>(), 4 * GIB);
        assert_eq!(record.serviceable, 6 * GIB);
        assert!(head(&engine).is_none(), "an 8 GiB claim is not serviceable");
        assert!(claim_status(&engine, "eight")["waiting"].as_str().unwrap().starts_with("waiting for RAM: needs"));
        drop(owner);
        finish_fixture(engine, sample).await;

        // Four 2 GiB cells holding their memory in a memfd: D is 8 GiB, and a 5 GiB claim is granted after two completions.
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new(&mut sample);
        let ids = ["m0", "m1", "m2", "m3"];
        publish(&mut engine, "q", "memfd", &ids.map(|id| (id, 2 * GIB)), 0.0, 0).await;
        cores(&mut sample, 6);
        for id in ids {
            start(&mut engine, &mut sample, id, 2 * GIB).await;
        }
        engine.request(owner.host_only("five", 5 * GIB)).await.unwrap();
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
        let owner = Claimant::new(&mut sample);
        let ids = ["f0", "f1", "f2"];
        cores(&mut sample, 6);
        publish(&mut engine, "q", "files", &ids.map(|id| (id, GIB)), 0.0, 0).await;
        for id in ids {
            start(&mut engine, &mut sample, id, 0).await;
            processes(&mut sample, &engine, id, &[GIB, GIB]);
        }
        sample.available = 21 * GIB / 2;
        engine.request(owner.host_only("two", 2 * GIB)).await.unwrap();
        engine.host_memory.backdate("two", 60.0);
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(engine.journal.history.last().unwrap().0.own_work.iter().map(|c| c.bytes).sum::<u64>(), 3 * GIB);
        assert_eq!(head(&engine).as_deref(), Some("two"));
        drop(owner);
        finish_fixture(engine, sample).await;

        // A compile restored without a runtime counts nothing and is never recorded; of the claims, only the
        // host-only one counts.
        let (mut engine, mut sample) = admission_fixture().await;
        let owners: Vec<Claimant> = (0..4).map(|_| Claimant::new(&mut sample)).collect();
        publish(&mut engine, "q", "legacy", &[("legacy", 2 * GIB)], 0.0, 0).await;
        let mut legacy = engine.groups["legacy"].jobs[0].clone();
        legacy.expected_runtime_secs = None;
        engine.launch("legacy".into(), legacy, sample.cores[0]).await.unwrap();
        sample.available = 30 * GIB;
        for (owner, (id, purpose)) in owners.iter().zip([("host", json!("host_only")), ("placed", json!("gpu_place")), ("unknown", Value::Null)]) {
            let mut claim = owner.claim(id, GIB);
            claim["purpose"] = purpose;
            engine.request(claim).await.unwrap();
        }
        engine.tick(sample.clone()).await.unwrap();
        engine.request(owners[3].host_only("waits", 15 * GIB / 2)).await.unwrap();
        engine.host_memory.backdate("waits", 60.0);
        engine.tick(sample.clone()).await.unwrap();
        let record = &engine.journal.history.last().unwrap().0;
        let counted: Vec<&Work> = record.own_work.iter().map(|c| &c.work).collect();
        assert!(matches!(counted.as_slice(), [Work::Claim { claim, .. }] if claim == "host"), "{counted:?}");
        assert!(engine.host_memory.head().is_none_or(|head| head.recorded.iter().all(|work| !matches!(work, Work::Compile { job, .. } if job == "legacy"))));
        drop(owners);
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn bytes_a_compiler_turn_kept_for_a_job_that_did_not_launch_go_to_ordinary_claims_before_large_ones() {
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new(&mut sample);
        let large = Claimant::beside(&mut sample, &owner);
        let party = owner.party();
        publish(&mut engine, "q", "running", &[("w", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        sample.available = 17 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        publish(&mut engine, &party, "group", &[("head", 5 * GIB), ("nine", 9 * GIB)], 120.0, 1).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("head"));
        engine.request(owner.host_only("eight", 8 * GIB)).await.unwrap();
        engine.request(large.host_only("twelve", 12 * GIB)).await.unwrap();
        end(&mut engine, "w");
        // 20 GiB left.
        sample.available = 30 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        let record = &engine.journal.history.last().unwrap().0;
        let skipped = record.admitted.iter().position(|e| matches!(e, Entry::Turn { bytes, .. } if *bytes == 9 * GIB));
        assert!(skipped.is_some(), "the 9 GiB job's turn kept its bytes from the 8 GiB claim");
        assert!(engine.running.contains_key("head") && !engine.running.contains_key("nine"), "the head fills its group");
        assert!(record.admitted.iter().any(|e| matches!(e, Entry::Granted { phase: Phase::Close, claim, budget, .. } if claim == "eight" && *budget == Budget::of(15 * GIB))));
        assert!(record.admitted.iter().all(|e| !matches!(e, Entry::Granted { claim, .. } if claim == "twelve")));
        assert_eq!(claim_status(&engine, "twelve")["waiting"], LARGE_WAIT, "offered only the 7 GiB after the second pass");
        replays(&engine);
        drop((owner, large));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn a_head_whose_named_work_ended_while_memory_held_elsewhere_took_its_bytes_gives_way() {
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new(&mut sample);
        let other = Claimant::new(&mut sample);
        submit_party_group(&mut engine, &owner.party(), "live-p", &[], 0).await;
        submit_party_group(&mut engine, &other.party(), "live-o", &[], 0).await;
        publish(&mut engine, "q", "named", &[("named", 4 * GIB)], 0.0, 0).await;
        publish(&mut engine, "q", "later", &[("later", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "named", 0).await;
        sample.available = 16 * GIB;
        engine.request(owner.host_only("six", 6 * GIB)).await.unwrap();
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
        engine.request(other.host_only("younger", GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert!(head(&engine).is_none(), "it ended in vain");
        assert!(engine.host_memory.gives_way(&RequestId::Claim { claim: "six".into() }));
        assert_eq!(claim_status(&engine, "younger")["state"], "granted", "younger work uses its bytes");
        engine.tick(sample.clone()).await.unwrap();
        assert!(head(&engine).is_none(), "no head until the work running then has ended");
        end(&mut engine, "later");
        sample.available -= 4 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        assert!(!engine.host_memory.gives_way(&RequestId::Claim { claim: "six".into() }));
        replays(&engine);
        drop((owner, other));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn with_exact_accounting_a_head_fits_when_its_named_work_ends() {
        let (mut engine, mut sample) = admission_fixture().await;
        let owner = Claimant::new(&mut sample);
        submit_party_group(&mut engine, &owner.party(), "live", &[], 0).await;
        publish(&mut engine, "q", "named", &[("named", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "named", 0).await;
        sample.available = 16 * GIB;
        engine.request(owner.host_only("six", 6 * GIB)).await.unwrap();
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
        let owner = Claimant::new(&mut sample);
        publish(&mut engine, "q", "running", &[("w", 4 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        // Nothing left and S is 4 GiB: the 6 GiB claim is not serviceable; then S is 6 GiB.
        sample.available = 14 * GIB;
        let mut claim = owner.claim("placed", 6 * GIB);
        claim["purpose"] = json!("gpu_place");
        engine.request(claim).await.unwrap();
        let decided = |engine: &Engine| -> Vec<(bool, Option<Decisions>)> {
            engine.journal.history.iter().map(|(record, written)| (*written, record.decisions.iter()
                .find(|d| d.subject == Subject::Claim { claim: "placed".into() }).map(|d| d.decision.clone()))).collect()
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
                20 => || Ok((20 * GIB, 128 * GIB)),
                _ => || Ok((u64::MAX, u64::MAX)),
            };
            (engine, sample)
        };
        // The sample says 30 GiB, a fresh reading 20 GiB, with 2 GiB headroom: 18 GiB for the whole tick.
        let (mut engine, mut sample) = fixture(20).await;
        sample.available = 30 * GIB;
        let owner = Claimant::new(&mut sample);
        publish(&mut engine, "p", "four", &[("four", 4 * GIB)], 0.0, 0).await;
        engine.request(owner.host_only("twenty", 20 * GIB)).await.unwrap();
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
        let owner = Claimant::new(&mut sample);
        publish(&mut engine, &owner.party(), "four", &[("four", 4 * GIB)], 0.0, 0).await;
        engine.request(owner.host_only("nine", 9 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "nine")["state"], "granted");
        assert!(engine.running.contains_key("four"), "the turn's 4 GiB went back to the launch");
        replays(&engine);
        drop(owner);
        finish_fixture(engine, sample).await;

        // The budget is signed: 6 GiB available, 2 GiB headroom, two compiles declaring 5 GiB and using 1 GiB.
        let (mut engine, mut sample) = fixture(0).await;
        let owner = Claimant::new(&mut sample);
        let small = Claimant::new(&mut sample);
        publish(&mut engine, "q", "running", &[("r0", 5 * GIB), ("r1", 5 * GIB)], 0.0, 0).await;
        for id in ["r0", "r1"] {
            start(&mut engine, &mut sample, id, GIB).await;
        }
        sample.available = 6 * GIB;
        engine.request(owner.host_only("eight", 8 * GIB)).await.unwrap();
        engine.request(small.host_only("one", GIB)).await.unwrap();
        engine.host_memory.backdate("eight", 60.0);
        engine.tick(sample.clone()).await.unwrap();
        let record = engine.journal.history.last().unwrap().0.clone();
        assert_eq!(record.left, Budget::of(0).less(4 * GIB));
        assert_eq!(record.serviceable, 6 * GIB, "flooring each term would promise 10 GiB");
        assert!(head(&engine).is_none(), "8 GiB is not serviceable");
        assert_eq!(claim_status(&engine, "one")["state"], "pending", "nothing is admitted from a negative budget");
        decision::replay(&record).unwrap();
        let floored = Record { left: Budget::of(0), serviceable: 10 * GIB, ..record };
        assert!(decision::replay(&floored).is_err());
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
        engine.launch(group, spec, cpu).await.unwrap();
    }

    #[tokio::test]
    async fn a_restored_compile_without_a_runtime_is_held_outside_the_service() {
        let (mut engine, mut sample) = admission_fixture().await;
        cores(&mut sample, 3);
        let owner = Claimant::new(&mut sample);
        publish(&mut engine, "a", "legacy", &[("legacy", GIB)], 0.0, 0).await;
        start_uncontracted(&mut engine, &sample, "legacy").await;
        // A record of a compile from an earlier boot, whose processes are gone.
        publish(&mut engine, "e", "earlier", &[("earlier", GIB)], 0.0, 0).await;
        let earlier = json!({"group_id":"earlier","job_id":"earlier","boot_id":"an-earlier-boot","pid":1,"start_ticks":1,"cpu":sample.cores[2],
            "memory_bytes":GIB,"job":engine.groups["earlier"].jobs[0],"log_path":engine.config.state.join("logs").join("earlier.log"),
            "queued_at":0.0,"started_at":1.0,"started_monotonic":1.0});
        store::write(&engine.config.state.join("running").join("earlier.json"), &earlier).await.unwrap();
        publish(&mut engine, "a", "head", &[("head", 2 * GIB)], 120.0, 0).await;
        // Restore takes a group's publication from its descriptor.
        std::fs::File::options()
            .write(true)
            .open(engine.config.state.join("groups").join("head.json"))
            .unwrap()
            .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(120))
            .unwrap();
        submit_party_group(&mut engine, &owner.party(), "live", &[], 0).await;
        let config = engine.handoff_config();
        drop(engine);
        let mut engine = Engine::restore(config).await.unwrap();
        engine.read_memory = || Ok((u64::MAX, u64::MAX));
        // 5 GiB left beside the restored compile; a claim takes 4 GiB in the first tick, so the head job waits for
        // memory by what that tick's launches left.
        sample.available = 16 * GIB;
        engine.request(owner.host_only("passing", 4 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert!(!engine.running.contains_key("earlier") && engine.results["earlier"]["outcome"] == "lost", "the first tick reaps a record of an earlier boot");
        assert_eq!(claim_status(&engine, "passing")["state"], "granted");
        let record = &engine.journal.history.last().unwrap().0;
        assert!(record.own_work.iter().all(|counted| !matches!(&counted.work, Work::Compile { job, .. } if job == "legacy")), "D does not count it");
        assert!(engine.held(|_| 1).get("a").is_none() && engine.ram_held().get("a").is_none(), "nor do the shares");
        engine.request(owner.release("passing")).await.unwrap();
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
    async fn a_fitting_head_without_idle_cores_is_released_so_another_partys_claim_gets_the_head() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.idle = 0;
        sample.available = 20 * GIB;
        let staggered = Claimant::new(&mut sample);
        let owners: Vec<Claimant> = (0..4).map(|_| Claimant::beside(&mut sample, &staggered)).collect();
        let waiting = Claimant::new(&mut sample);
        submit_party_group(&mut engine, &staggered.party(), "live-s", &[], 0).await;
        submit_party_group(&mut engine, &waiting.party(), "live-t", &[], 0).await;
        publish(&mut engine, "p", "head", &[("head", 5 * GIB)], 120.0, 0).await;
        engine.request(owners[0].host_only("s0", 2 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        // Two more 2 GiB claims in one tick leave 4 GiB after it.
        engine.request(owners[1].host_only("s1", 2 * GIB)).await.unwrap();
        engine.request(owners[2].host_only("s2", 2 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        engine.request(waiting.host_only("nine", 9 * GIB)).await.unwrap();
        engine.host_memory.backdate("nine", 40.0);
        engine.request(owners[2].release("s2")).await.unwrap();
        // 6 GiB left: the 5 GiB job fits, but no core is idle and no compile holds one.
        engine.tick(sample.clone()).await.unwrap();
        let record = &engine.journal.history.last().unwrap().0;
        assert!(record.aged.steps.iter().any(|step| matches!(step, Step::Chosen { chosen, verdict: Verdict::Passed, .. } if chosen.is_job("head"))));
        assert_eq!(head(&engine).as_deref(), Some("nine"), "the 9 GiB claim gets the aged head and reserves");
        engine.request(owners[3].host_only("s3", 2 * GIB)).await.unwrap();
        engine.request(owners[0].release("s0")).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "s3")["state"], "pending", "nothing is granted from its bytes");
        engine.request(owners[1].release("s1")).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "nine")["state"], "granted");
        engine.request(waiting.release("nine")).await.unwrap();
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
        let owner = Claimant::new(&mut sample);
        submit_party_group(&mut engine, &owner.party(), "live", &[], 0).await;
        publish(&mut engine, "q", "running", &[("w", 2 * GIB)], 0.0, 0).await;
        start(&mut engine, &mut sample, "w", 0).await;
        sample.available = 16 * GIB;
        engine.tick(sample.clone()).await.unwrap();
        publish(&mut engine, "p", "head", &[("head", 5 * GIB)], 120.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        // 6 GiB admissible: it fits, and the only slot is held by a contracted compile.
        sample.available = 18 * GIB;
        engine.request(owner.host_only("two", 2 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("head"));
        assert_eq!(claim_status(&engine, "two")["state"], "pending");
        engine.drain();
        engine.tick(sample.clone()).await.unwrap();
        assert!(head(&engine).is_none(), "a compile head is released while the engine drains");
        assert_eq!(claim_status(&engine, "two")["state"], "granted");
        engine.request(owner.release("two")).await.unwrap();
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
        let c = Claimant::new(&mut sample);
        let claims: Vec<Claimant> = (0..2).map(|_| Claimant::beside(&mut sample, &c)).collect();
        submit_party_group(&mut engine, &c.party(), "live-c", &[], 0).await;
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
        engine.request(claims[0].host_only("c1", 6 * GIB)).await.unwrap();
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(claim_status(&engine, "c1")["state"], "granted");
        assert_eq!(engine.held(|_| 1).get("a"), None, "the shares count nothing for the restored compile");
        assert_eq!(engine.held(|_| 1).get("b"), Some(&fairness::charge(1, 0)));
        publish(&mut engine, "a", "head", &[("head", 5 * GIB)], 120.0, 0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert_eq!(head(&engine).as_deref(), Some("head"), "it reserves while the first claim is granted");
        engine.request(claims[1].host_only("c2", 6 * GIB)).await.unwrap();
        engine.request(claims[0].release("c1")).await.unwrap();
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
}
