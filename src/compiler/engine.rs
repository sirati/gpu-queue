//! One actor owns publication, fair admissions, process custody and results.
use super::{
    model::{Group, Job},
    resources::Snapshot,
    store, Config,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::process::Stdio;
use tokio::process::{Child, Command};

struct Running {
    record: Value,
    child: Option<Child>,
    exit: Option<i32>,
}
pub struct Engine {
    config: Config,
    groups: BTreeMap<String, Group>,
    order: VecDeque<String>,
    identities: BTreeMap<String, String>,
    running: BTreeMap<String, Running>,
    results: BTreeMap<String, Value>,
    sample: Option<Snapshot>,
    boot: String,
    sequence: u64,
    draining: bool,
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
        for dir in ["groups", "running", "results", "logs", "cancellations"] {
            tokio::fs::create_dir_all(config.state.join(dir))
                .await
                .map_err(|e| e.to_string())?;
        }
        let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .map_err(|e| e.to_string())?
            .trim()
            .to_owned();
        let mut engine = Self {
            config,
            groups: BTreeMap::new(),
            order: VecDeque::new(),
            identities: BTreeMap::new(),
            running: BTreeMap::new(),
            results: BTreeMap::new(),
            sample: None,
            boot,
            sequence: 0,
            draining: false,
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
            for result in transaction["pending"]
                .as_array()
                .ok_or("bad cancellation journal")?
            {
                let id = result["job_id"]
                    .as_str()
                    .ok_or("bad cancellation identity")?;
                if !engine.results.contains_key(id) {
                    engine.finish(result.clone()).await?;
                }
            }
        }
        for path in store::descriptors(&engine.config.state.join("groups")).await? {
            let group: Group =
                serde_json::from_value(store::read(&path).await?).map_err(|e| e.to_string())?;
            group.validate()?;
            engine.remember(group)?;
        }
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
                engine.running.insert(
                    id,
                    Running {
                        record,
                        child: None,
                        exit: None,
                    },
                );
            }
        }
        // Replay running cancellation requests only for the retained identities.
        for path in store::descriptors(&engine.config.state.join("cancellations")).await? {
            let transaction = store::read(&path).await?;
            if let Some(targets) = transaction["running"].as_array() {
                for target in targets {
                    if let Some(running) = target["job_id"]
                        .as_str()
                        .and_then(|id| engine.running.get_mut(id))
                    {
                        if running.record["started_at"] == target["started_at"]
                            && !running.record["termination_reason"].is_string()
                        {
                            running.record["termination_reason"] = transaction["reason"].clone();
                            running.record["cancelled"] = json!(true);
                        }
                    }
                }
            }
        }
        Ok(engine)
    }
    fn remember(&mut self, group: Group) -> Result<(), String> {
        for job in &group.jobs {
            if self.identities.contains_key(&job.job_id) {
                return Err("duplicate compiler job identity across groups".into());
            }
        }
        for job in &group.jobs {
            self.identities
                .insert(job.job_id.clone(), group.group_id.clone());
        }
        self.order.push_back(group.group_id.clone());
        self.groups.insert(group.group_id.clone(), group);
        Ok(())
    }
    async fn finish(&mut self, result: Value) -> Result<(), String> {
        let id = result["job_id"]
            .as_str()
            .ok_or("missing result ID")?
            .to_owned();
        if let Some(existing) = self.results.get(&id) {
            if *existing != result {
                return Err("terminal compiler result is immutable".into());
            }
            return Ok(());
        }
        store::write(
            &self.config.state.join("results").join(format!("{id}.json")),
            &result,
        )
        .await?;
        self.results.insert(id, result);
        Ok(())
    }
    fn result(&self, group: &str, job: &Job, outcome: &str, detail: &str) -> Value {
        json!({"group_id":group,"job_id":job.job_id,"outcome":outcome,"detail":detail,"returncode":null,
            "queued_at":self.queued_at(group),"started_at":null,"finished_at":if outcome == "queued" {Value::Null}else{json!(crate::protocol::now_unix())},"execution_seconds":0.0,"log_path":""})
    }
    fn queued_at(&self, group: &str) -> Value {
        // The publication timestamp is stored separately from the immutable declaration.
        self.groups
            .get(group)
            .map(|_| ())
            .and_then(|_| {
                std::fs::metadata(
                    self.config
                        .state
                        .join("groups")
                        .join(format!("{group}.json")),
                )
                .ok()
            })
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|t| json!(t.as_secs_f64()))
            .unwrap_or(Value::Null)
    }
    pub fn drain(&mut self) {
        self.draining = true;
    }
    pub fn drained(&self) -> bool {
        self.draining && self.running.is_empty()
    }
    pub async fn request(&mut self, request: Value) -> Result<Value, String> {
        if request["protocol"] != 1 {
            return Err("unsupported compiler protocol".into());
        }
        match request["op"].as_str().ok_or("missing compiler operation")? {
            "health" => {
                let resources = self.sample.as_ref().map(|sample| {
                    let (used, reserved) = self.reserved(sample);
                    json!({"cores":sample.cores,"idle_slots":sample.idle,"assigned_cores":used,
                        "available_memory_bytes":sample.available,"reserved_memory_bytes":reserved,
                        "minimum_memory_bytes":self.config.minimum_ram})
                });
                Ok(
                    json!({"protocol":1,"pid":std::process::id(),"start_ticks":identity(std::process::id())?.0,
                    "boot_id":self.boot,"running":self.running.len(),"draining":self.draining,"resources":resources,
                    "features":["channel-owned-state","direct-compilation","compile-dependencies","compile-group-limit","typed-results"]}),
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
                    .any(|j| self.identities.contains_key(&j.job_id))
                {
                    return Err("duplicate compiler job identity".into());
                }
                store::write(
                    &self
                        .config
                        .state
                        .join("groups")
                        .join(format!("{}.json", group.group_id)),
                    &serde_json::to_value(&group).map_err(|e| e.to_string())?,
                )
                .await?;
                let id = group.group_id.clone();
                self.remember(group)?;
                Ok(json!({"existing":false,"group_id":id}))
            }
            "group_status" => {
                let id = request["group_id"].as_str().ok_or("missing group ID")?;
                let group = self.groups.get(id).ok_or("unknown compiler group")?;
                let jobs: Vec<_> = group.jobs.iter().map(|job| {
                    if let Some(result) = self.results.get(&job.job_id) { return result.clone(); }
                    if let Some(running) = self.running.get(&job.job_id) {
                        let r = &running.record;
                        return json!({"group_id":id,"job_id":job.job_id,"outcome":"running","returncode":null,"detail":r["termination_reason"],
                            "queued_at":r["queued_at"],"started_at":r["started_at"],"finished_at":null,"execution_seconds":(monotonic()-r["started_monotonic"].as_f64().unwrap_or(monotonic())).max(0.0),"log_path":r["log_path"],"pid":r["pid"]});
                    }
                    self.result(id,job,"queued","")
                }).collect();
                Ok(
                    json!({"group_id":id,"complete":group.jobs.iter().all(|j|self.results.contains_key(&j.job_id)),"jobs":jobs}),
                )
            }
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
            store::write(
                &self.config.state.join("cancellations").join(format!(
                    "{}-{}-{}.json",
                    std::process::id(),
                    crate::protocol::now_unix().to_bits(),
                    self.sequence
                )),
                &json!({"pending":pending,"running":running,"reason":reason}),
            )
            .await?;
            for result in pending {
                self.finish(result).await?;
            }
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
    fn reserved(&self, sample: &Snapshot) -> (BTreeSet<usize>, u64) {
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
    pub async fn tick(&mut self, sample: Snapshot) -> Result<(), String> {
        self.sample = Some(sample.clone());
        self.reap().await?;
        if self.draining {
            return Ok(());
        }
        // Propagate failed dependencies to a fixed point before choosing work.
        loop {
            let mut blocked = Vec::new();
            for group in self.groups.values() {
                for job in &group.jobs {
                    if !self.results.contains_key(&job.job_id)
                        && !self.running.contains_key(&job.job_id)
                        && job.dependencies.iter().any(|id| {
                            self.results
                                .get(id)
                                .is_some_and(|r| r["outcome"] != "success")
                        })
                    {
                        blocked.push(self.result(
                            &group.group_id,
                            job,
                            "fail",
                            "compile prerequisite failed",
                        ));
                    }
                }
            }
            if blocked.is_empty() {
                break;
            }
            for result in blocked {
                self.finish(result).await?;
            }
        }
        let (mut used, reserved) = self.reserved(&sample);
        let mut memory = sample
            .available
            .saturating_sub(reserved)
            .saturating_sub(self.config.minimum_ram);
        let slots = sample
            .cores
            .len()
            .saturating_sub(1)
            .saturating_sub(used.len())
            .min(sample.idle);
        for _ in 0..slots {
            let mut selected = None;
            for _ in 0..self.order.len() {
                let id = self.order.pop_front().unwrap();
                self.order.push_back(id.clone());
                let group = &self.groups[&id];
                if group.max_running > 0
                    && self
                        .running
                        .values()
                        .filter(|r| r.record["group_id"] == id && r.record["cpu"].is_number())
                        .count()
                        >= group.max_running
                {
                    continue;
                }
                if let Some(job) = group.jobs.iter().find(|j| {
                    !self.results.contains_key(&j.job_id)
                        && !self.running.contains_key(&j.job_id)
                        && self.memory(j) <= memory
                        && j.dependencies.iter().all(|d| {
                            self.results
                                .get(d)
                                .is_some_and(|r| r["outcome"] == "success")
                        })
                }) {
                    selected = Some((id, job.clone()));
                    break;
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
        self.order.retain(|id| {
            self.groups[id]
                .jobs
                .iter()
                .any(|j| !self.results.contains_key(&j.job_id))
        });
        Ok(())
    }
    async fn launch(&mut self, group: String, job: Job, cpu: usize) -> Result<(), String> {
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
                self.running.insert(
                    id,
                    Running {
                        record,
                        child: Some(child),
                        exit: None,
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
                }
            }
            let root_alive = alive(&running.record, &self.boot);
            let pid = running.record["pid"].as_u64().unwrap_or(0) as u32;
            let mut members: Vec<_> = if running.record["boot_id"] == self.boot {
                sample
                    .groups
                    .iter()
                    .filter(|(_, (group, _))| *group == pid)
                    .map(|(&p, &(_, birth))| (p, birth))
                    .collect()
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
            let timed_out = running.record["boot_id"] == self.boot
                && monotonic()
                    - running.record["started_monotonic"]
                        .as_f64()
                        .unwrap_or(monotonic())
                    > running.record["job"]["timeout_seconds"]
                        .as_f64()
                        .unwrap_or(1200.0);
            if timed_out && !running.record["termination_reason"].is_string() {
                running.record["termination_reason"] = json!("compile timeout");
            }
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
            if root_alive && running.exit.is_none() {
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
            engine.request(json!({"protocol":1,"op":"submit_group","group":{"group_id":group,"jobs":jobs,"max_running":limit}})).await.unwrap();
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
}
