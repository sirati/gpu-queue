//! One actor owns publication, fair admissions, process custody and results.
use super::{
    fairness::{self, Fairness, Shares},
    host_memory::Turn,
    model::{Group, Job},
    party::{self, Parties},
    progress::Progress,
    resources::Snapshot,
    store,
    watch::{Reply, Watches},
    Config,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::process::Stdio;
use tokio::process::{Child, Command};

// Waiters are answered at completion; the retention only serves late status
// queries and resubmissions of the same identities.
const RETENTION: f64 = 3600.0;

struct Running {
    record: Value,
    child: Option<Child>,
    exit: Option<i32>,
    awaiting_exit: bool,
}
struct Admission<'a> {
    sample: &'a Snapshot,
    used: BTreeSet<usize>,
    memory: u64,
    reservation: Option<&'a Job>,
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
    fairness: Fairness,
    baselines: BTreeMap<String, f64>,
    running: BTreeMap<String, Running>,
    results: BTreeMap<String, Value>,
    sample: Option<Snapshot>,
    boot: String,
    sequence: u64,
    draining: bool,
    host_memory: super::host_memory::HostMemory,
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
            fairness: Fairness::default(),
            baselines: BTreeMap::new(),
            running: BTreeMap::new(),
            results: BTreeMap::new(),
            sample: None,
            boot,
            sequence: 0,
            draining: false,
            host_memory,
        };
        let baselines = engine.config.state.join("fairness.json");
        if baselines.exists() {
            engine.baselines = serde_json::from_value(store::read(&baselines).await?)
                .map_err(|e| e.to_string())?;
            if engine.baselines.values().any(|v| !v.is_finite()) {
                return Err("invalid compiler fairness baseline".into());
            }
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
            engine.parties.assign(group, party);
        }
        for path in store::descriptors(&engine.config.state.join("groups")).await? {
            let group: Group =
                serde_json::from_value(store::read(&path).await?).map_err(|e| e.to_string())?;
            group.validate()?;
            engine.remember(group)?;
        }
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
        // Service history is attributed once every retained group's party is known.
        engine.fairness = Fairness::default();
        for result in engine.results.values() {
            if let (Some(group), Some(seconds)) = (
                result["group_id"].as_str(),
                result["execution_seconds"].as_f64(),
            ) {
                engine.fairness.observe(engine.parties.of(group), seconds);
            }
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
        let live = engine.live_parties();
        engine.baselines.retain(|party, _| live.contains(party));
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
        for (id, result) in fresh {
            self.record(id, result);
        }
        Ok(())
    }
    fn record(&mut self, id: String, result: Value) {
        if let (Some(group), Some(seconds)) = (
            result["group_id"].as_str(),
            result["execution_seconds"].as_f64(),
        ) {
            self.fairness.observe(self.parties.of(group), seconds);
        }
        if let Some(group) = result["group_id"]
            .as_str()
            .filter(|_| result["outcome"] != "success")
        {
            self.unchecked_dependencies.insert(group.to_owned());
        }
        self.results.insert(id.clone(), result);
        if let Some(group) = self.progress.settle(&id) {
            if self.group_watches.waits_for(&group) {
                let status = self.status(&group);
                self.group_watches.resolve(&group, status);
            }
            self.completed.insert(group, crate::protocol::now_unix());
        }
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
            self.submitted.remove(id);
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
        self.baselines.retain(|party, _| live.contains(party));
        self.forget_results(&jobs).await?;
        store::write(
            &self.config.state.join("fairness.json"),
            &serde_json::to_value(&self.baselines).map_err(|e| e.to_string())?,
        )
        .await
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

    fn service_score(&self, party: &str) -> f64 {
        let now = monotonic();
        self.fairness.score(
            party,
            self.baselines.get(party).copied().unwrap_or(0.0),
            self.running
                .values()
                .filter(|r| {
                    r.record["group_id"]
                        .as_str()
                        .is_some_and(|group| self.parties.of(group) == party)
                })
                .map(|r| (now - r.record["started_monotonic"].as_f64().unwrap_or(now)).max(0.0)),
        )
    }

    // Backfill is bounded: once an eligible party has waited thirty seconds,
    // younger work cannot repeatedly consume RAM it needs. Running commands drain
    // naturally. Publication time survives replacement in the immutable descriptor.
    // A party waits from its oldest unfinished group or its last service.
    fn waiting_since(&self) -> BTreeMap<&str, f64> {
        let mut since: BTreeMap<&str, f64> = BTreeMap::new();
        for group in self.progress.active() {
            let submitted = self.submitted[group];
            since
                .entry(self.parties.of(group))
                .and_modify(|oldest| *oldest = oldest.min(submitted))
                .or_insert(submitted);
        }
        for (party, oldest) in &mut since {
            *oldest = oldest.max(self.served.get(*party).copied().unwrap_or(0.0));
        }
        since
    }

    fn reservation(&self, now: f64) -> Option<(&Group, &Job)> {
        // Reserve only memory that draining our own compilers can recover.
        // External pressure must not deadlock smaller otherwise fitting work.
        let recoverable = self
            .sample
            .as_ref()
            .map(|sample| {
                self.running
                    .values()
                    .fold(sample.available, |memory, running| {
                        let roots = running.record["pid"]
                            .as_u64()
                            .map(|pid| BTreeSet::from([pid as u32]))
                            .unwrap_or_default();
                        memory.saturating_add(sample.rss(&roots))
                    })
                    .saturating_sub(self.config.minimum_ram)
                    .saturating_sub(self.host_memory.reserved(sample))
            })
            .unwrap_or(u64::MAX);
        // The longest-waiting aged party wins; ties keep catalogue order.
        let waiting = self.waiting_since();
        let mut reserved: Option<(f64, &Group, &Job)> = None;
        for id in self.progress.active() {
            let group = &self.groups[id];
            let since = waiting[self.parties.of(id)];
            if now - since < 30.0
                || reserved
                    .as_ref()
                    .is_some_and(|(best, _, _)| !since.total_cmp(best).is_lt())
            {
                continue;
            }
            if let Some(job) = self.first_ready(group, |job| self.memory(job) <= recoverable) {
                reserved = Some((since, group, job));
            }
        }
        reserved.map(|(_, group, job)| (group, job))
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
            reservation: self
                .reservation(crate::protocol::now_unix())
                .map(|(_, job)| job),
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
        if let Some(first) = admission.reservation {
            if self.memory(first) > admission.memory && first.job_id != job.job_id {
                return format!(
                    "waiting for aged compiler memory reservation: {}",
                    first.job_id
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
        Ok(json!({"group_id":id,"complete":self.progress.complete(id),"jobs":jobs}))
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
                    .map(|g| json!({"group_id":g.group_id,"party":self.parties.of(&g.group_id),
                        "average_compilation_seconds":self.fairness.average(self.parties.of(&g.group_id)),
                        "accounted_service_seconds":self.service_score(self.parties.of(&g.group_id)),
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
                    "boot_id":self.boot,"running":self.running.len(),"draining":self.draining,"resources":resources,"subqueues":subqueues,
                    "parked_group_waits":self.group_watches.len(),
                    "host_memory":self.host_memory.health(self.sample.as_ref()),
                    "features":["host-memory-admission","channel-owned-state","direct-compilation","compile-dependencies","compile-group-limit","typed-results","bounded-memory-backfill","admission-reasons","service-time-fairness","two-thirds-subqueue-cap","separate-process-handoff","push-waits"]}),
                )
            }
            "submit_group" => {
                let group: Group =
                    serde_json::from_value(request["group"].clone()).map_err(|e| e.to_string())?;
                group.validate()?;
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
                // A party that (re)joins starts at the lowest active score, so it
                // neither catches up on its idle time nor starts behind.
                let joining = {
                    let active: BTreeSet<&str> = self
                        .progress
                        .active()
                        .map(|id| self.parties.of(id))
                        .collect();
                    (!active.contains(party.as_str())).then(|| {
                        active
                            .iter()
                            .map(|other| self.service_score(other))
                            .reduce(f64::min)
                            .unwrap_or(0.0)
                    })
                };
                if let Some(floor) = joining {
                    self.baselines
                        .insert(party.clone(), floor - self.fairness.served(&party));
                }
                let id = group.group_id.clone();
                let state = &self.config.state;
                let descriptor = serde_json::to_value(&group).map_err(|e| e.to_string())?;
                let record = json!({"group_id":id,"party":party});
                let baselines = serde_json::to_value(&self.baselines).map_err(|e| e.to_string())?;
                store::write_all(&[
                    (
                        &state.join("groups").join(format!("{id}.json")),
                        &descriptor,
                    ),
                    (&state.join("parties").join(format!("{id}.json")), &record),
                    (&state.join("fairness.json"), &baselines),
                ])
                .await?;
                self.remember(group)?;
                self.parties.assign(&id, &party);
                Ok(json!({"existing":false,"group_id":id,"party":party}))
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
    fn memory(&self, job: &Job) -> u64 {
        if job.memory_bytes == 0 {
            self.config.average_ram
        } else {
            job.memory_bytes
        }
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
    fn compiler_turns(&self, sample: &Snapshot) -> Shares<Turn> {
        let mut held = BTreeMap::new();
        for running in self.running.values() {
            let party = running.record["group_id"]
                .as_str()
                .map_or(party::UNATTRIBUTED, |id| self.parties.of(id));
            let bytes: &mut u64 = held.entry(party.to_owned()).or_default();
            *bytes = bytes.saturating_add(
                running.record["memory_bytes"]
                    .as_u64()
                    .unwrap_or(self.config.average_ram),
            );
        }
        let mut next = BTreeMap::new();
        if !self.draining && Self::free_slots(sample, &self.compiler_reserved(sample).0) > 0 {
            for id in &self.order {
                if let Some(job) = self.first_ready(&self.groups[id], |_| true) {
                    next.entry(self.parties.of(id)).or_insert(self.memory(job));
                }
            }
        }
        let mut shares = Shares::new(held);
        for (party, bytes) in next {
            shares.wait(party, Turn::Compiler(bytes));
        }
        shares
    }
    fn reserved(&self, sample: &Snapshot) -> (BTreeSet<usize>, u64) {
        let (cores, compiler) = self.compiler_reserved(sample);
        (
            cores,
            compiler.saturating_add(self.host_memory.reserved(sample)),
        )
    }
    pub async fn tick(&mut self, sample: Snapshot) -> Result<(), String> {
        self.group_watches.prune();
        if self.sample.as_ref().map(|s| s.capacity) != Some(sample.capacity) {
            let active: Vec<String> = self.progress.active().map(String::from).collect();
            self.unchecked_capacity.extend(active);
        }
        self.sample = Some(sample.clone());
        self.reap().await?;
        let compiler_reserved = self.compiler_reserved(&sample).1;
        let live = self.live_parties();
        let turns = self.compiler_turns(&sample);
        self.host_memory
            .tick(
                &sample,
                compiler_reserved,
                self.config.minimum_ram,
                &live,
                turns,
            )
            .await?;
        self.retire(crate::protocol::now_unix()).await?;
        if self.draining {
            return Ok(());
        }
        // Impossible declarations must not establish a permanent reservation.
        while let Some(id) = self.unchecked_capacity.pop_first() {
            let Some(group) = self.groups.get(&id) else {
                continue;
            };
            let impossible: Vec<_> = self.progress.pending(&id).map(|index| &group.jobs[index])
                .filter(|job| sample.capacity > 0 && self.memory(job) > sample.capacity.saturating_sub(self.config.minimum_ram))
                .map(|job| self.result(&id, job, "fail", "compiler memory requirement exceeds host/cgroup capacity after required headroom")).collect();
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
        let (mut used, mut memory) = self.admissible(&sample);
        for _ in 0..Self::free_slots(&sample, &used) {
            let reservation = self.reservation(crate::protocol::now_unix());
            let mut selected = reservation.map(|(g, j)| (g.group_id.clone(), j.clone()));
            if selected
                .as_ref()
                .is_some_and(|(_, job)| self.memory(job) > memory)
            {
                break;
            }
            if selected.is_none() {
                let mut best_score = f64::INFINITY;
                let mut scores: BTreeMap<&str, f64> = BTreeMap::new();
                for id in &self.order {
                    let group = &self.groups[id];
                    if let Some(job) = self.first_ready(group, |job| self.memory(job) <= memory) {
                        let party = self.parties.of(id);
                        let score = *scores
                            .entry(party)
                            .or_insert_with(|| self.service_score(party));
                        if score < best_score {
                            best_score = score;
                            selected = Some((id.clone(), job.clone()));
                        }
                    }
                }
            }
            let Some((group, job)) = selected else {
                break;
            };
            let Some(cpu) = sample.cores.iter().find(|c| !used.contains(c)).copied() else {
                break;
            };
            memory = memory.saturating_sub(self.memory(&job));
            used.insert(cpu);
            self.launch(group, job, cpu).await?;
        }
        self.order.retain(|id| !self.progress.complete(id));
        Ok(())
    }
    async fn launch(&mut self, group: String, job: Job, cpu: usize) -> Result<(), String> {
        if let Some(index) = self.order.iter().position(|id| id == &group) {
            self.order.remove(index);
            self.order.push_back(group.clone());
        }
        self.served.insert(
            self.parties.of(&group).to_owned(),
            crate::protocol::now_unix(),
        );
        let id = job.job_id.clone();
        let log_path = self.config.state.join("logs").join(format!("{id}.log"));
        let path = self.config.state.join("running").join(format!("{id}.json"));
        let mut record = json!({"group_id":group,"job_id":id,"boot_id":self.boot,"pid":null,"start_ticks":null,
            "cpu":cpu,"memory_bytes":self.memory(&job),"job":job,"log_path":log_path,
            "queued_at":self.queued_at(&group),"started_at":crate::protocol::now_unix(),"started_monotonic":monotonic()});
        // The intent is durable before spawn. An interrupted publication is an
        // explicit unknown result after restart, never a second execution.
        store::write(&path, &record).await?;
        let log = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&log_path)
            .map_err(|e| e.to_string())?;
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
        match command.spawn() {
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
            let record = &running.record;
            let job: Job =
                serde_json::from_value(record["job"].clone()).map_err(|e| e.to_string())?;
            let reason = record["termination_reason"].as_str();
            let (outcome, detail) = if record["cancelled"] == true {
                ("cancelled", reason.unwrap_or("owner cancelled compilation"))
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
            result["execution_seconds"] = json!(if record["boot_id"] == self.boot {
                (monotonic() - record["started_monotonic"].as_f64().unwrap_or(monotonic())).max(0.0)
            } else {
                0.0
            });
            self.finish(result).await?;
            tokio::fs::remove_file(self.config.state.join("running").join(format!("{id}.json")))
                .await
                .map_err(|e| e.to_string())?;
            finished.insert(id.clone());
        }
        active.retain(|id, _| !finished.contains(id));
        self.running = active;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
                    |i| json!({"job_id":format!("{group}-{i}"),"argv":[program,"0.1"],"cwd":state}),
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
        let engine = Engine::restore(Config {
            state,
            minimum_ram: 10 << 30,
            average_ram: 1 << 30,
        })
        .await
        .unwrap();
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
            .map(|id| json!({"job_id":id,"argv":[program,"-S","-c","import pathlib,time\nwhile not pathlib.Path('release').exists(): time.sleep(0.01)"],"cwd":engine.config.state,"memory_bytes":memory}))
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
        admit_group(&mut engine, "existing", 1 << 30, 0.0).await;
        let job = engine.groups["existing"].jobs[0].clone();
        engine
            .launch("existing".into(), job, sample.cores[0])
            .await
            .unwrap();
        let pid = engine.running["existing"].record["pid"].as_u64().unwrap() as u32;
        sample.processes.insert(pid, (std::process::id(), 4 << 30));
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
                .map(|j| json!({"job_id":format!("g{g}-j{j}"),"argv":["true"],"cwd":engine.config.state,"memory_bytes":1u64 << 30}))
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

    #[tokio::test]
    async fn young_memory_blocked_command_allows_backfill_until_it_ages() {
        let (mut engine, sample) = admission_fixture().await;
        admit_group(&mut engine, "large", 4 << 30, 1.0).await;
        admit_group(&mut engine, "small", 1 << 30, 0.0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert!(!engine.running.contains_key("large"));
        assert!(engine.running.contains_key("small"));
        // Service at the group boundary resets its age, so an old group cannot
        // turn its entire batch into a permanent priority over other groups.
        assert!(engine.reservation(crate::protocol::now_unix()).is_none());
        assert!(engine.waiting_since()["large"] > crate::protocol::now_unix() - 2.0);
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
                {"job_id":"nice","argv":[program,"-S","-c",probe],"cwd":engine.config.state}]}}),
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
        std::fs::remove_file(engine.config.state.join("parties").join("legacy.json")).unwrap();
        let restored = Engine::restore(engine.handoff_config()).await.unwrap();
        assert_eq!(restored.parties.of("recorded"), "alpha");
        assert_eq!(restored.parties.of("legacy"), party::UNATTRIBUTED);
        assert!(!restored.baselines.contains_key("beta"));
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
        assert!(!engine.baselines.contains_key("done"));
        assert!(store::read(&state.join("fairness.json")).await.unwrap()["done"].is_null());
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
    #[tokio::test]
    async fn serving_an_aged_group_gives_the_next_aged_group_its_turn() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.cores.truncate(2);
        admit_jobs(&mut engine, "first", &["first-again"], 1 << 30, 120.0).await;
        admit_group(&mut engine, "second", 1 << 30, 60.0).await;
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("first"));
        assert_eq!(
            engine
                .reservation(crate::protocol::now_unix())
                .unwrap()
                .0
                .group_id,
            "second"
        );
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
    async fn selection_corrects_equal_round_robin_turns_by_previous_duration() {
        let (mut engine, mut sample) = admission_fixture().await;
        sample.cores.truncate(2);
        sample.available = 64 << 30;
        admit_group(&mut engine, "long", 1 << 30, 0.0).await;
        admit_group(&mut engine, "short", 1 << 30, 0.0).await;
        engine.baselines.insert("short".into(), 0.0);
        engine.fairness.observe("long", 10.0);
        engine.fairness.observe("short", 1.0);
        engine.tick(sample.clone()).await.unwrap();
        assert!(engine.running.contains_key("short"));
        assert!(!engine.running.contains_key("long"));
        finish_fixture(engine, sample).await;
    }

    #[tokio::test]
    async fn service_history_and_late_arrival_baselines_survive_replacement() {
        let (mut engine, sample) = admission_fixture().await;
        admit_jobs(&mut engine, "first", &["first-next"], 1 << 30, 0.0).await;
        let job = &engine.groups["first"].jobs[0];
        let mut result = engine.result("first", job, "success", "recorded fixture exit");
        result["execution_seconds"] = json!(12.0);
        engine.finish(result).await.unwrap();
        admit_group(&mut engine, "new", 1 << 30, 0.0).await;
        assert_eq!(engine.baselines["new"], 12.0);
        let config = Config {
            state: engine.config.state.clone(),
            minimum_ram: engine.config.minimum_ram,
            average_ram: engine.config.average_ram,
        };
        let restored = Engine::restore(config).await.unwrap();
        assert_eq!(restored.fairness.average("first"), 12.0);
        assert_eq!(restored.baselines["new"], 12.0);
        finish_fixture(restored, sample).await;
    }
}
