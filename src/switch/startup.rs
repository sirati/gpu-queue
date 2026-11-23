//! The grant-to-allocation deadline belongs to host enforcement. The daemon
//! supplies admitted jobs; this observer supplies process-tree VRAM evidence.
//! A grant with no allocation by its deadline breaks the GPU usage contract
//! (`super::contract`); one shutting down without an allocation gives up its
//! grant. This fast observer does not wait for the slower SM-activity sample.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::procs;
use crate::protocol::{JobState, Sig};
use crate::switch::card::{self, Heard};
use crate::switch::card_reader::Processes;
use crate::switch::compute_watchdog::Thresholds;
use crate::switch::config::secs;
use crate::switch::daemon::Daemon;
use crate::switch::state::{Job, State};

fn awaiting_allocation(job: &Job) -> bool {
    job.state == JobState::Stopping
        || (job.state == JobState::Running
            && job.retention.is_none()
            && (job.kill.is_none() || job.shutdown.is_some()))
}

fn observed(job: &Job) -> bool {
    matches!(job.state, JobState::Running | JobState::Stopping)
}

pub async fn run(d: Arc<Daemon>) {
    let interval = Duration::from_millis(50);
    let mut changed = d.subscribe();
    loop {
        changed.borrow_and_update();
        if !d.with(|s| s.jobs().any(observed)) {
            if changed.changed().await.is_err() {
                return;
            }
            continue;
        }
        let observed_at = d.clock.monotonic();
        let tick = d.clock.monotonic_ns();
        let observation = d.card.compute().await;
        // Every answer is published; only a read the owner applied is acted on, and a host without a card has
        // nothing to observe.
        let compute = match d.observe(|s| card::observe(&d, s, observation)) {
            Heard::Unconfigured => return,
            Heard::Read(compute) => Some(compute),
            Heard::Failed(_) | Heard::Panicked(_) | Heard::Superseded => None,
        };
        let walked = tokio::task::spawn_blocking(procs::parents).await;
        if let (Some(compute), Ok(parents)) = (compute, walked) {
            d.update(|s| pass(&d, s, &compute, &parents, observed_at, tick));
        }
        tokio::time::sleep(interval).await;
    }
}

/// One observation of the grants: `compute`, the card's processes, and `parents`, the host's process tree, as the
/// sample begun at `observed_at` (its tick `tick`) found them. Each observed job's allocation evidence is applied, a
/// stopping job's allocation is enforced, and each grant awaiting its allocation is judged at `observed_at`.
pub(crate) fn pass(
    d: &Daemon,
    s: &mut State,
    compute: &Processes,
    parents: &HashMap<u32, u32>,
    observed_at: Duration,
    tick: u64,
) {
    for side in s.sides.values_mut() {
        for job in side.jobs.values_mut().filter(|job| observed(job)) {
            // A sample begun before admission cannot prove anything about this grant.
            let Some(admitted) = job.admitted_at.filter(|at| *at <= observed_at) else {
                continue;
            };
            let allocated = job.pid_host.is_some_and(|root| {
                let owned = procs::job_tree(root, parents);
                compute.iter().any(|(pid, memory)| {
                    owned.contains(pid) && memory.is_none_or(|bytes| bytes > 0)
                })
            });
            let memory_use = job.pid_host.and_then(|root| {
                let owned = procs::job_tree(root, parents);
                let apps: Vec<_> = compute
                    .iter()
                    .filter(|(pid, _)| owned.contains(pid))
                    .collect();
                if apps.iter().any(|(_, bytes)| bytes.is_some_and(|n| n > 0)) {
                    Some(true)
                } else if apps.iter().any(|(_, bytes)| bytes.is_none()) {
                    None
                } else {
                    Some(false)
                }
            });
            if let Some(timing) = &mut job.timing {
                timing.memory(tick, memory_use);
            }
            if job.state == JobState::Stopping {
                let unauthorized = job.pid_host.is_some_and(|root| {
                    let owned = procs::job_tree(root, parents);
                    compute.iter().any(|(pid, memory)| {
                        owned.contains(pid) && memory.is_some_and(|bytes| bytes > 0)
                    })
                });
                if unauthorized {
                    job.end_reason = Some((
                        JobState::Killed,
                        "GPU allocation after the grant was revoked for shutdown"
                            .into(),
                    ));
                    super::enforcement::signal(d, job, Sig::Kill);
                }
                continue;
            }
            if awaiting_allocation(job) {
                let limit = secs(Thresholds::of(&d.config).vram);
                observe(d, job, allocated, observed_at, admitted, limit);
            }
        }
    }
}

fn observe(
    d: &Daemon,
    job: &mut Job,
    allocated: bool,
    now: Duration,
    admitted: Duration,
    limit: Duration,
) {
    if allocated {
        job.police.allocated = true;
        job.police.startup_since = None;
        job.police.startup_fired = false;
        return;
    }
    let since = *job.police.startup_since.get_or_insert_with(|| {
        if job.police.allocated || job.police.used_compute {
            now
        } else {
            admitted
        }
    });
    if now.saturating_sub(since) >= limit {
        job.police.startup_fired = true;
        if job.shutdown.is_some() {
            job.state = JobState::Stopping;
            if let Some(timing) = &mut job.timing {
                timing.stop(d.clock.monotonic_ns());
            }
            return;
        }
        super::contract::violated(d, job, crate::protocol::Rule::NoVram);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{Manual, Readings};
    use crate::protocol::{ClientMsg, JobSpec, Rule};
    use crate::switch::control::tests::{daemon_on, enter, spec, StateDir};
    use crate::switch::state::Side;
    use tokio::sync::mpsc::UnboundedReceiver;

    const SIDE: Side = Side::Host(1000);
    /// The startup limit these tests judge: the binary tests' 0.15 s.
    const LIMIT: Duration = Duration::from_millis(150);
    const NS: Duration = Duration::from_nanos(1);

    /// The observer's daemon on a manual source at 2^24 s, which only the test advances.
    struct Observer {
        d: Arc<Daemon>,
        _dir: StateDir,
        manual: Arc<Manual>,
        clients: Vec<UnboundedReceiver<ClientMsg>>,
    }

    impl Observer {
        fn new() -> Observer {
            let origin = Readings { monotonic: Duration::from_secs(16_777_216), wall: Duration::from_secs(1_800_000_000) };
            let (manual, clock) = Manual::starting_at(origin);
            let (d, dir) = daemon_on(clock, serde_json::json!({"startup_idle_secs": LIMIT.as_secs_f64()}));
            Observer { d, _dir: dir, manual, clients: Vec::new() }
        }

        /// A grant of `spec`, admitted through the real schedule at the source's reading, whose holder is `holder`.
        fn admitted(&mut self, spec: JobSpec, holder: u32) -> u64 {
            let (id, client) = enter(&self.d, SIDE, spec, false, None);
            self.clients.push(client);
            self.d.with(|s| crate::switch::daemon::schedule(&self.d, s));
            self.d.with(|s| {
                let job = s.job_mut(SIDE, id).unwrap();
                assert_eq!(job.state, JobState::Running, "the grant is admitted");
                job.pid_host = Some(holder);
            });
            id
        }

        /// A sample's facts, read on the source as the observer's run reads them.
        fn sample(&self) -> (Duration, u64) {
            (self.d.clock.monotonic(), self.d.clock.monotonic_ns())
        }

        /// One observer pass over `sampled`, with `compute` on the card and `parents` the host's tree.
        fn pass(&self, sampled: (Duration, u64), compute: Processes, parents: HashMap<u32, u32>) {
            self.d.update(|s| pass(&self.d, s, &compute, &parents, sampled.0, sampled.1));
        }

        fn job<R>(&self, id: u64, f: impl FnOnce(&Job) -> R) -> R {
            self.d.with(|s| f(s.job(SIDE, id).unwrap()))
        }

        fn rules(&self, id: u64) -> Vec<Rule> {
            self.job(id, |j| j.violations.iter().map(|v| v.rule).collect())
        }
    }

    /// Holders outside the test process's tree, so that what the card shows of them is theirs.
    const HOLDER: u32 = 4_000_001;
    const OTHER: u32 = 4_000_002;

    #[test]
    fn no_rule_is_recorded_one_nanosecond_before_the_startup_limit() {
        let mut o = Observer::new();
        let id = o.admitted(spec(None), HOLDER);
        o.manual.advance(LIMIT - NS);
        o.pass(o.sample(), Vec::new(), HashMap::new());
        assert_eq!(o.rules(id), [], "no rule before admitted + limit");
        assert_eq!(o.job(id, |j| j.state), JobState::Running);
    }

    #[test]
    fn a_grant_with_no_allocation_at_the_startup_limit_breaks_no_vram() {
        let mut o = Observer::new();
        let id = o.admitted(spec(None), HOLDER);
        o.manual.advance(LIMIT);
        o.pass(o.sample(), Vec::new(), HashMap::new());
        assert_eq!(o.rules(id), [Rule::NoVram], "the rule fires at admitted + limit");
    }

    #[test]
    fn an_allocation_resets_the_span_the_startup_limit_counts() {
        let mut o = Observer::new();
        let id = o.admitted(spec(None), HOLDER);
        o.manual.advance(LIMIT / 2);
        o.pass(o.sample(), Vec::new(), HashMap::new());
        assert_eq!(o.rules(id), [], "the span from the admission runs");
        o.manual.advance(LIMIT - LIMIT / 2 - NS);
        o.pass(o.sample(), vec![(HOLDER, Some(1 << 20))], HashMap::new());
        o.manual.advance(NS);
        o.pass(o.sample(), Vec::new(), HashMap::new());
        assert_eq!(o.rules(id), [], "an allocation before the limit resets its span");
        o.manual.advance(LIMIT - NS);
        o.pass(o.sample(), Vec::new(), HashMap::new());
        assert_eq!(o.rules(id), [], "the span counts from the first sample without the allocation");
        o.manual.advance(NS);
        o.pass(o.sample(), Vec::new(), HashMap::new());
        assert_eq!(o.rules(id), [Rule::NoVram]);
    }

    #[test]
    fn a_pending_shutdown_stops_the_grant_at_the_limit_and_freezes_its_gpu_time_at_the_stop() {
        let mut o = Observer::new();
        let id = o.admitted(spec(None), HOLDER);
        let begun = crate::clock::ns(o.job(id, |j| j.admitted_at.unwrap()));
        o.d.update(|s| {
            let job = s.job_mut(SIDE, id).unwrap();
            job.shutdown_grace_secs = Some(60.0);
            job.end_reason = Some((JobState::Cancelled, "cancelled while running".into()));
            crate::switch::enforcement::signal(&o.d, job, Sig::Term);
        });
        assert!(o.job(id, |j| j.shutdown.is_some() && j.state == JobState::Running), "a request, no stop yet");
        o.manual.advance(LIMIT);
        let sampled = o.sample();
        o.manual.advance(Duration::from_secs(1));
        o.pass(sampled, Vec::new(), HashMap::new());
        let stopped = o.d.clock.monotonic_ns();
        assert_eq!(o.job(id, |j| j.state), JobState::Stopping, "Stopping instead of a violation");
        assert_eq!(o.rules(id), []);
        let frozen = o.job(id, |j| j.timing.as_ref().unwrap().snapshot(stopped + 1_000_000_000).gpu_seconds);
        assert_eq!(frozen, (stopped - begun) as f64 / 1e9, "the stop reads the source at its own point");
    }

    #[test]
    fn another_holders_allocation_does_not_satisfy_a_grants_startup_limit() {
        let mut o = Observer::new();
        let busy = o.admitted(spec(None), HOLDER);
        o.manual.advance(NS);
        let empty = o.admitted(spec(None), OTHER);
        o.manual.advance(LIMIT);
        let child = HOLDER + 10;
        o.pass(o.sample(), vec![(child, Some(1 << 20))], HashMap::from([(child, HOLDER)]));
        assert_eq!(o.rules(busy), [], "the busy holder's tree allocated");
        assert_eq!(o.rules(empty), [Rule::NoVram], "the other grant's tree allocated nothing");
    }
}
