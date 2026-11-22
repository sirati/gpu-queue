//! Fairness accounts per party, not per group: a campaign that splits its work into many groups still receives one
//! share. A party is the systemd unit of the process that registered a claim or submitted a group, unless the
//! submission names one; it is read once and saved with the claim or the group.
use crate::procs::{self, Unread};
use std::collections::BTreeMap;
use std::sync::Arc;

/// The party of groups saved before parties were recorded without a party file. No claim has it.
pub const UNATTRIBUTED: &str = "unattributed";
/// The party of a claim saved without one, each of whose recorded processes is proven gone.
pub const UNIT_UNKNOWN: &str = "unit-unknown";

/// Reads the `0::` cgroup path of the process `pid` that started at `birth`.
pub type ReadCgroup = Arc<dyn Fn(u32, u64) -> Result<String, Unread> + Send + Sync>;

/// Which unit a process belongs to.
#[derive(Clone)]
pub struct Attribution {
    cgroup: ReadCgroup,
}
impl Attribution {
    /// Reads `/proc` for the identity that holds the pid.
    pub fn system() -> Self {
        Self {
            cgroup: Arc::new(|pid, birth| procs::Process { pid, birth }.cgroup()),
        }
    }
    /// Reads cgroup paths through `cgroup`.
    #[cfg(test)]
    pub fn reading(cgroup: ReadCgroup) -> Self {
        Self { cgroup }
    }
    /// The unit containing the process `pid` that started at `birth`.
    pub fn party(&self, pid: u32, birth: u64) -> Result<String, Unread> {
        (self.cgroup)(pid, birth).map(|path| unit(&path))
    }
}

/// The unit a cgroup path names, by systemd's naming: `/` and the path's components up to and including the deepest
/// one that ends in `.service` or `.scope`, compared whole; `/` when none does. A convention, not a lookup: a
/// delegated sub-cgroup named with such a suffix is a unit of its own.
fn unit(path: &str) -> String {
    let components: Vec<&str> = path.split('/').filter(|component| !component.is_empty()).collect();
    let names = |component: &&str| {
        [".service", ".scope"].iter().any(|suffix| component.len() > suffix.len() && component.ends_with(suffix))
    };
    let through = components.iter().rposition(names).map_or(0, |deepest| deepest + 1);
    format!("/{}", components[..through].join("/"))
}

/// Priorities share the GPU line's range.
pub fn bounded(priority: i64) -> Result<i8, String> {
    i8::try_from(priority)
        .ok()
        .filter(|p| (-10..=10).contains(p))
        .ok_or_else(|| "priority must be an integer between -10 and 10".into())
}
/// A submission's priority; unstated means 0.
pub fn priority(request: &serde_json::Value) -> Result<i8, String> {
    match &request["priority"] {
        serde_json::Value::Null => Ok(0),
        value => bounded(value.as_i64().ok_or("priority must be an integer")?),
    }
}

#[derive(Default)]
pub struct Parties {
    of: BTreeMap<String, (String, i8)>,
}
impl Parties {
    pub fn assign(&mut self, group: &str, party: &str, priority: i8) {
        self.of
            .insert(group.to_owned(), (party.to_owned(), priority));
    }
    pub fn of(&self, group: &str) -> &str {
        self.of.get(group).map_or(UNATTRIBUTED, |(party, _)| party)
    }
    pub fn priority(&self, group: &str) -> i8 {
        self.of.get(group).map_or(0, |&(_, priority)| priority)
    }
    pub fn forget(&mut self, group: &str) {
        self.of.remove(group);
    }
}

/// Each process's cgroup path, or why it cannot be read, by pid and birth.
#[cfg(test)]
type Placed = BTreeMap<(u32, u64), Result<String, Unread>>;
/// A test's stand-in for `/proc`: each process's cgroup path, or why it cannot be read, by pid and birth; every
/// read is recorded. A process it does not hold is gone.
#[cfg(test)]
#[derive(Clone, Default)]
pub struct Units {
    paths: Arc<std::sync::Mutex<Placed>>,
    reads: Arc<std::sync::Mutex<Vec<(u32, u64)>>>,
}
#[cfg(test)]
impl Units {
    /// The process `pid` that started at `birth` is in the cgroup `path`.
    pub fn place(&self, pid: u32, birth: u64, path: &str) {
        self.paths.lock().unwrap().insert((pid, birth), Ok(path.to_owned()));
    }
    /// Reading the process `pid` that started at `birth` fails with `unread`.
    pub fn refuse(&self, pid: u32, birth: u64, unread: Unread) {
        self.paths.lock().unwrap().insert((pid, birth), Err(unread));
    }
    pub fn reader(&self) -> ReadCgroup {
        let units = self.clone();
        Arc::new(move |pid, birth| {
            units.reads.lock().unwrap().push((pid, birth));
            units.paths.lock().unwrap().get(&(pid, birth)).cloned().unwrap_or(Err(Unread::Gone))
        })
    }
    /// Every read so far, in order.
    pub fn reads(&self) -> Vec<(u32, u64)> {
        self.reads.lock().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_groups_share_one_party_and_priorities_are_bounded() {
        let mut parties = Parties::default();
        parties.assign("g", "campaign", 10);
        assert_eq!(parties.of("g"), "campaign");
        assert_eq!((parties.priority("g"), parties.priority("legacy")), (10, 0));
        assert_eq!(priority(&serde_json::json!({})), Ok(0));
        assert_eq!(priority(&serde_json::json!({"priority": -10})), Ok(-10));
        for invalid in [
            serde_json::json!(11),
            serde_json::json!(2.5),
            serde_json::json!("10"),
        ] {
            assert!(priority(&serde_json::json!({ "priority": invalid })).is_err());
        }
        assert_eq!(parties.of("legacy"), UNATTRIBUTED);
        parties.forget("g");
        assert_eq!(parties.of("g"), UNATTRIBUTED);
    }

    const USER: &str = "/user.slice/user-1000.slice/user@1000.service";

    #[test]
    fn a_party_is_the_deepest_unit_of_its_cgroup_path() {
        let agent = format!("{USER}/app.slice/app-t3code.slice/app-t3code-agents.slice/t3code-claude-1bd874a8.scope");
        let service = format!("{USER}/app.slice/doggy-compile.service");
        let rows = [
            (agent.clone(), agent.clone()),
            (format!("{service}/worker.scope"), format!("{service}/worker.scope")),
            (format!("{service}/daemon"), service.clone()),
            (format!("{service}/job-obj-1a2b3c"), service.clone()),
            (format!("{service}/worker"), service.clone()),
            (format!("{USER}/app.slice/geist-perf-combined-rest.service"), format!("{USER}/app.slice/geist-perf-combined-rest.service")),
            (format!("{USER}/app.slice/foo.servicex/bar"), USER.to_owned()),
            (format!("{USER}/app.slice/foo.service.d/x"), USER.to_owned()),
            (
                "/machine.slice/container@eduvpn.service/payload/system.slice/dbus-broker.service".to_owned(),
                "/machine.slice/container@eduvpn.service/payload/system.slice/dbus-broker.service".to_owned(),
            ),
            ("/a//b.scope/".to_owned(), "/a/b.scope".to_owned()),
            ("/".to_owned(), "/".to_owned()),
        ];
        for (path, party) in rows {
            assert_eq!(unit(&path), party, "{path}");
        }
        assert_eq!(unit(&agent), unit(&agent), "an equal path twice is one party");
        let (rest, measured) = (format!("{USER}/app.slice/geist-perf-combined-rest.service"), format!("{USER}/app.slice/geist-perf-combined-measured.service"));
        assert_ne!(unit(&rest), unit(&measured), "siblings are two parties");
        let (foo, longer) = (format!("{USER}/app.slice/foo.service"), format!("{USER}/app.slice/foo.service2.scope"));
        assert_ne!(unit(&foo), unit(&longer), "a string prefix relates no two units");
    }

    /// The pids a shell's line names.
    fn pids(line: &str) -> Vec<u32> {
        line.split_whitespace().map(|pid| pid.parse().unwrap()).collect()
    }

    #[test]
    fn a_session_leaders_child_and_nested_sessions_share_their_units_party() {
        use std::io::BufRead;
        use std::os::unix::process::CommandExt;
        let mut command = std::process::Command::new("sh");
        command
            .args(["-c", "sleep 60 & a=$!; setsid sleep 60 & echo $a $!; wait"])
            .stdout(std::process::Stdio::piped());
        // SAFETY: setsid in the child before exec, with no allocation.
        unsafe {
            command.pre_exec(|| match libc::setsid() {
                -1 => Err(std::io::Error::last_os_error()),
                _ => Ok(()),
            });
        }
        let mut leader = command.spawn().unwrap();
        let mut line = String::new();
        std::io::BufReader::new(leader.stdout.take().unwrap()).read_line(&mut line).unwrap();
        let children = pids(&line);
        let system = Attribution::system();
        let party = |pid: u32| system.party(pid, procs::started(pid).unwrap());
        let own = party(std::process::id()).unwrap();
        let read: Vec<_> = std::iter::once(leader.id()).chain(children.iter().copied()).map(party).collect();
        for pid in children {
            procs::Process::read(pid).unwrap().signal(libc::SIGKILL);
        }
        leader.kill().unwrap();
        leader.wait().unwrap();
        assert_eq!(read, vec![Ok(own.clone()), Ok(own.clone()), Ok(own)], "the leader, its child and the nested session's leader");
    }

    /// A transient unit the test started: killed and stopped when dropped, whether the test passed or failed.
    struct Transient(String);
    impl Drop for Transient {
        fn drop(&mut self) {
            let systemctl = |arguments: &[&str]| std::process::Command::new("systemctl").arg("--user").args(arguments).output();
            let _ = systemctl(&["kill", "--signal=KILL", &self.0]);
            let _ = systemctl(&["stop", &self.0]);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while systemctl(&["is-active", "--quiet", &self.0]).is_ok_and(|output| output.status.success())
                && std::time::Instant::now() < deadline
            {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }

    /// Runs `script` in a transient scope `unit` with `properties`, and returns it with the first line it printed,
    /// read within 10 s. `systemd-run --scope` moves itself into the scope and execs the command in place, so the
    /// spawned pid is the script's.
    fn scoped(unit: &str, properties: &[&str], script: &str) -> (std::process::Child, Transient, std::sync::mpsc::Receiver<String>) {
        use std::io::BufRead;
        let mut child = std::process::Command::new("systemd-run")
            .args(["--user", "--scope", "--quiet"])
            .args(properties)
            .arg(format!("--unit={unit}"))
            .args(["--", "sh", "-c", script])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let transient = Transient(format!("{unit}.scope"));
        let (lines, received) = std::sync::mpsc::channel();
        let stdout = child.stdout.take().unwrap();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines().map_while(Result::ok) {
                let _ = lines.send(line);
            }
        });
        (child, transient, received)
    }
    fn line(received: &std::sync::mpsc::Receiver<String>) -> String {
        received.recv_timeout(std::time::Duration::from_secs(10)).expect("the scope's handshake within 10 s")
    }

    /// What `serve` answers `request` from `sender`.
    async fn served(engine: &mut super::super::engine::Engine, request: serde_json::Value, sender: u32) -> Result<serde_json::Value, String> {
        let (reply, answer) = tokio::sync::oneshot::channel();
        engine.serve(request, procs::Peer::opened(sender), reply).await;
        answer.await.unwrap()
    }

    #[tokio::test]
    #[ignore = "needs a systemd user manager; run with --ignored"]
    async fn real_scopes_and_delegated_children_take_the_party_their_names_give() {
        let own = std::process::id();
        let party = |pid: u32| Attribution::system().party(pid, procs::started(pid).unwrap()).unwrap();

        let (mut sibling, _sibling, said) = scoped(&format!("pa-sibling-{own}"), &[], "echo ready; exec sleep 60");
        assert_eq!(line(&said), "ready");
        let scope = party(sibling.id());
        assert!(scope.ends_with(&format!("/pa-sibling-{own}.scope")), "{scope}");
        assert_ne!(scope, party(own), "a sibling scope is a party of its own");
        let state = std::env::temp_dir().join(format!("party-scopes-{own}-{}", crate::protocol::now_unix().to_bits()));
        std::fs::create_dir(&state).unwrap();
        let config = super::super::Config { state: state.clone(), minimum_ram: 0, average_ram: 1 << 20, overcommit_percent: super::super::OVERCOMMIT_PERCENT };
        let instance = crate::process_identity::Instance::current().unwrap();
        let clock = super::super::clock::Clock::system();
        let restored = super::super::engine::Engine::restore(config, clock, super::super::store::Store::default(), instance, Attribution::system());
        let mut engine = restored.await.unwrap().activated().await.unwrap();
        let claim = |id: &str| serde_json::json!({"protocol":1,"op":"host_memory_register","claim_id":id,"memory_bytes":1u64 << 20,"label":id});
        let release = |id: &str| serde_json::json!({"protocol":1,"op":"host_memory_release","claim_id":id});
        let health = serde_json::json!({"protocol":1,"op":"health"});
        let claim_party = |health: serde_json::Value, id: &str| {
            health["host_memory"]["claims"].as_array().unwrap().iter().find(|claim| claim["claim_id"] == id).unwrap()["party"].clone()
        };
        served(&mut engine, claim("alone"), sibling.id()).await.unwrap();
        assert_eq!(claim_party(served(&mut engine, health.clone(), own).await.unwrap(), "alone"), scope, "with no group held");
        served(&mut engine, release("alone"), sibling.id()).await.unwrap();
        let group = serde_json::json!({"protocol":1,"op":"submit_group","group":{"group_id":"held","jobs":[
            {"job_id":"held-true","argv":["true"],"cwd":state,"memory_bytes":1u64 << 20,"expected_runtime_secs":10}]}});
        served(&mut engine, group, own).await.unwrap();
        served(&mut engine, claim("beside"), sibling.id()).await.unwrap();
        assert_eq!(claim_party(served(&mut engine, health, own).await.unwrap(), "beside"), scope, "with a group held");
        served(&mut engine, release("beside"), sibling.id()).await.unwrap();
        drop(engine);
        std::fs::remove_dir_all(&state).unwrap();
        let _ = sibling.kill();
        let _ = sibling.wait();

        let script = "c=$(sed -n 's/^0:://p' /proc/self/cgroup); d=/sys/fs/cgroup$c; mkdir $d/worker.scope $d/plain; \
                      sleep 60 & w=$!; sleep 60 & p=$!; sleep 60 & r=$!; \
                      echo $w > $d/worker.scope/cgroup.procs; echo $p > $d/plain/cgroup.procs; echo $w $p $r; echo ready; wait";
        let (mut delegate, _delegate, said) = scoped(&format!("pa-delegate-{own}"), &["-p", "Delegate=yes"], script);
        let placed = pids(&line(&said));
        assert_eq!(line(&said), "ready");
        let (w, p, r) = (placed[0], placed[1], placed[2]);
        let cgroup = |pid: u32| {
            let birth = procs::started(pid).unwrap();
            let path = procs::Process { pid, birth }.cgroup().unwrap();
            assert_eq!(procs::started(pid), Some(birth), "{pid} was read as itself");
            path
        };
        let unit = format!("/pa-delegate-{own}.scope");
        assert!(cgroup(r).ends_with(&unit) && cgroup(p).ends_with(&format!("{unit}/plain")) && cgroup(w).ends_with(&format!("{unit}/worker.scope")));
        let scope = party(r);
        assert!(scope.ends_with(&unit), "{scope}");
        assert_eq!(party(p), scope, "a delegated child named without a suffix maps to its unit");
        assert_eq!(party(w), format!("{scope}/worker.scope"), "a delegated child named with a suffix is a party of its own");
        let shown = std::process::Command::new("systemctl").args(["--user", "show", "worker.scope", "-p", "LoadState"]).output().unwrap();
        assert_eq!(String::from_utf8_lossy(&shown.stdout).trim(), "LoadState=not-found", "no manager knows it");
        let _ = delegate.kill();
        let _ = delegate.wait();
    }
}
