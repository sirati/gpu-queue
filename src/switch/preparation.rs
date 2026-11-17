//! Places: GPU-line entries that prepare only once told, at the front of the line, and are admissible once
//! prepared. A place is told while fewer than N counted entries stand ahead of it (the line window), or fewer
//! than K counted entries of its own share (the share floor). Counted are the waiting entries that arrived
//! ready and the told places that are not parked; a parked place waits for a large host claim in a process
//! that holds nothing. Telling is never taken back.
use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::protocol::{
    now_unix, Blocker, BlockerKind, ClientMsg, JobSpec, PreparationView, ProcessIdentity, ReadyFigures,
};
use crate::switch::daemon::Daemon;
use crate::switch::state::{Conn, Entry, Job, Side, State};

/// A place's preparation, kept with its job and saved with it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Place {
    pub told_at: Option<f64>,
    /// The figures of its `Ready`, once it is prepared.
    pub ready: Option<ReadyFigures>,
    pub ready_at: Option<f64>,
    /// Why its preparation waits, while it does.
    pub waiting: Option<String>,
    pub parked: bool,
}

impl Place {
    pub fn told(&self) -> bool {
        self.told_at.is_some()
    }
    pub fn view(&self) -> PreparationView {
        PreparationView {
            told_at: self.told_at,
            ready_at: self.ready_at,
            waiting: self.waiting.clone(),
            parked: self.parked,
        }
    }
    /// Why an unprepared place waits, at its 1-based line position.
    pub fn reason(&self, position: usize) -> String {
        match (self.told(), &self.waiting, self.parked) {
            (false, _, _) => format!("placed: position {position}"),
            (true, Some(waiting), true) => format!("parked: {waiting}"),
            (true, Some(waiting), false) => format!("preparing: waiting: {waiting}"),
            (true, None, _) => "preparing".into(),
        }
    }
}

/// One telling decision, as the journal records it.
pub struct Telling {
    pub counted_ahead: usize,
    pub counted_share_ahead: usize,
    pub places: usize,
    pub per_share: usize,
}

impl Telling {
    /// Whether a recorded decision obeyed the rule: the line window or the share floor told it.
    pub fn obeyed(record: &serde_json::Value) -> Result<(), String> {
        let field = |name: &str| {
            record[name]
                .as_u64()
                .ok_or_else(|| format!("a told record lacks {name}: {record}"))
        };
        let (ahead, share_ahead) = (field("counted_ahead")?, field("counted_share_ahead")?);
        let (places, per_share) = (field("preparation_places")?, field("preparation_places_per_share")?);
        match ahead < places || share_ahead < per_share {
            true => Ok(()),
            false => Err(format!(
                "told with {ahead} counted entries ahead (N = {places}) and {share_ahead} of its share (K = {per_share}): {record}"
            )),
        }
    }
}

/// Tells every untold place the line window or its share floor admits, in line order; returns, for each place
/// left untold, the shares of the counted entries ahead of it.
pub fn tell(d: &Daemon, s: &mut State) -> HashMap<(Side, u64), Blocker> {
    let (places, per_share) = (d.config.preparation_places, d.config.preparation_places_per_share);
    let mut counted = 0usize;
    let mut by_share: HashMap<String, usize> = HashMap::new();
    let mut ahead: BTreeSet<String> = BTreeSet::new();
    let mut told = Vec::new();
    let mut untold = HashMap::new();
    for (_, entry) in s.line() {
        let Entry::Job(side, id) = entry else {
            counted += 1;
            continue;
        };
        let Some(job) = s.job(side, id) else { continue };
        let share_ahead = by_share.get(&job.share).copied().unwrap_or(0);
        let waiting_untold = job.place.as_ref().is_some_and(|place| !place.told());
        let tells = waiting_untold && (counted < places || share_ahead < per_share);
        if tells {
            told.push((
                side,
                id,
                Telling {
                    counted_ahead: counted,
                    counted_share_ahead: share_ahead,
                    places,
                    per_share,
                },
            ));
        } else if waiting_untold {
            untold.insert(
                (side, id),
                Blocker {
                    kind: BlockerKind::PreparingAhead,
                    shares: ahead.iter().cloned().collect(),
                },
            );
        }
        if tells || job.counted() {
            counted += 1;
            *by_share.entry(job.share.clone()).or_default() += 1;
            ahead.insert(job.share.clone());
        }
    }
    let mut records = Vec::new();
    for (side, id, telling) in &told {
        let job = s.job_mut(*side, *id).expect("waiting job");
        job.place.as_mut().expect("a place").told_at = Some(now_unix());
        records.push(super::journal::Journal::told_entry(job, telling));
    }
    // The pass's decisions are durable before any place hears them.
    if let Err(error) = d.journal.told(records) {
        eprintln!("gpu-queue: {error}");
    }
    for (side, id, _) in told {
        let job = s.job(side, id).expect("waiting job");
        prepare(job);
        eprintln!("gpu-switch: place {} told", job.handle);
    }
    untold
}

/// Tells the place's client to prepare: at telling, and again to a client that reattached.
pub fn prepare(job: &Job) {
    if let Conn::Client(tx) = &job.conn {
        let _ = tx.send(ClientMsg::Prepare { prepare: true });
    }
}

/// The told place of `job`, or why there is none.
fn told(job: &mut Job) -> Result<&mut Place, String> {
    job.place
        .as_mut()
        .filter(|place| place.told())
        .ok_or_else(|| "this request is for a told place".into())
}

/// A told place is prepared, with the final figures of its run; a repeated identical `Ready` changes nothing.
pub fn ready(job: &mut Job, figures: ReadyFigures) -> Result<(), String> {
    let spec = JobSpec {
        vram: figures.vram,
        expected_runtime_secs: figures.expected_runtime_secs,
        max_runtime_secs: figures.max_runtime_secs,
        ..job.spec.clone()
    };
    let place = told(job)?;
    match &place.ready {
        Some(earlier) if *earlier == figures => return Ok(()),
        Some(_) => return Err("a place's Ready figures cannot change".into()),
        None => {}
    }
    spec.validate()?;
    if figures
        .shutdown_grace_secs
        .is_some_and(|s| !s.is_finite() || !(0.0..=60.0).contains(&s))
    {
        return Err("shutdown grace must be 0..60 seconds".into());
    }
    place.ready = Some(figures.clone());
    place.ready_at = Some(now_unix());
    place.waiting = None;
    place.parked = false;
    job.spec = spec;
    job.shutdown_grace_secs = figures.shutdown_grace_secs;
    Ok(())
}

/// The place now belongs to `to`, a live descendant of its owner: the identities `State::owned_lease`
/// authenticates become `to`'s. Only a told place before its `Ready` is transferred, by its owner, the
/// session's peer. A transfer its previous owner sends again, naming the current owner, changes nothing.
pub fn transfer(job: &mut Job, requester: u32, to: ProcessIdentity) -> Result<(), String> {
    let owner = ProcessIdentity {
        pid: job.runner_pid,
        birth: job.process_started.unwrap_or(0),
    };
    if owner == to && job.transferred_from.is_some_and(|from| from.pid == requester) {
        return Ok(());
    }
    if requester != owner.pid {
        return Err("a place is transferred by its owner".into());
    }
    let unready = job
        .place
        .as_ref()
        .is_some_and(|place| place.told() && place.ready.is_none());
    if !unready {
        return Err("only a told place before its Ready is transferred".into());
    }
    let live = crate::procs::started(to.pid) == Some(to.birth) && crate::procs::alive(to.pid);
    if !live || to.pid == owner.pid || !descends(to.pid, owner.pid) {
        return Err("a place is transferred only to a live descendant of its owner".into());
    }
    job.transferred_from = Some(owner);
    job.runner_pid = to.pid;
    job.pid_host = Some(to.pid);
    job.process_started = Some(to.birth);
    Ok(())
}

fn descends(mut pid: u32, root: u32) -> bool {
    let mut seen = BTreeSet::new();
    while seen.insert(pid) {
        if pid == root {
            return true;
        }
        let Some(parent) = crate::procs::parent(pid) else {
            return false;
        };
        pid = parent;
    }
    false
}

/// The place's new owner, to its previous owner when that one resumes: a resume attaches nothing then.
pub fn transferred_from(job: &Job, peer: u32) -> Option<ProcessIdentity> {
    job.transferred_from
        .filter(|from| from.pid == peer && crate::procs::started(peer) == Some(from.birth))
        .map(|_| ProcessIdentity {
            pid: job.runner_pid,
            birth: job.process_started.unwrap_or(0),
        })
}

/// Why a told place's preparation waits; an empty reason ends the wait.
pub fn wait(job: &mut Job, reason: String, parked: bool) -> Result<(), String> {
    let place = told(job)?;
    let reason: String = reason.lines().next().unwrap_or("").chars().take(200).collect();
    place.parked = parked && !reason.is_empty();
    place.waiting = (!reason.is_empty()).then_some(reason);
    Ok(())
}

/// What a lease reply says of a place, after a resume or reconnect too.
pub fn summary(job: &Job) -> Option<serde_json::Value> {
    job.place.as_ref().map(|place| {
        serde_json::json!({"told": place.told(), "ready": place.ready.is_some(), "parked": place.parked})
    })
}
