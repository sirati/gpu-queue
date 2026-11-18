//! The tick's request index: one fact of each request, however often its record names it. A request has one
//! declaration and one party in a tick, and is admitted at most once; each place the record names them is one
//! observation of that fact, which only this index reads. The capacities a record holds are one tick's facts too:
//! the index reads each as its raw counts and D give it (`Capacity::derived`), and checks only its raw counts' bounds
//! and their agreement within the tick.
use super::{
    super::precedence::{Capacity, Candidate, Head, Listed, RequestId, Standing, Step, Stood, Units},
    ensure, Entry, Projected, Record,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

/// One observation of a fact of one request, which only the tick's request index reads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Observation<T>(T);

/// The facts of the requests a record names.
pub struct Requests {
    tick: u64,
    bytes: BTreeMap<RequestId, u64>,
    party: BTreeMap<RequestId, String>,
    /// D's compiles, which every capacity's contracted running compiles are.
    compiles: usize,
    /// The kept head's standing, read as its kind requires.
    standing: Option<Standing>,
}

impl Requests {
    /// Reads the record's observations. `kept` is the head the tick before left, which the record judges first, and
    /// only it.
    pub fn read(record: &Record<Projected>, kept: Option<&Head>) -> Result<Self, String> {
        let tick = record.tick;
        let compiles = record.compiles();
        let mut index = Index { tick, ..Index::default() };
        let standing = match (kept, record.aged.steps.first()) {
            (Some(kept), Some(Step::Kept { standing, .. })) => {
                let stood = Stood {
                    pending: standing.pending,
                    bytes: standing.bytes.0,
                    capacity: standing.capacity.as_ref().map(|capacity| capacity.derived(compiles)),
                };
                let standing = Standing::read(&kept.id, stood).map_err(|e| format!("tick {tick}: {e}"))?;
                index.declares(&kept.id, standing.bytes())?;
                index.capacity(&kept.id, &kept.party, standing_capacity(&standing))?;
                Some(standing)
            }
            (Some(kept), _) => Err(format!("tick {tick}: the head the tick before left, {}, is not judged first", kept.id))?,
            (None, Some(Step::Kept { .. })) => {
                Err(format!("tick {tick}: the record judges a head the tick before left, and that tick left none"))?
            }
            (None, _) => None,
        };
        for step in &record.aged.steps {
            for entry in listing(step) {
                let candidate = &entry.candidate;
                index.declares(&candidate.id, candidate.bytes.0)?;
                index.belongs(&candidate.id, &candidate.party.0)?;
                let capacity = candidate.lacks.as_ref().map(|capacity| capacity.derived(compiles));
                index.capacity(&candidate.id, &candidate.party.0, capacity.as_ref())?;
            }
        }
        for id in record.admissions() {
            index.admits(&id)?;
        }
        for entry in &record.admitted {
            match entry {
                Entry::Granted { claim, party, bytes, .. } => {
                    let id = RequestId::Claim { claim: claim.clone() };
                    index.declares(&id, bytes.0)?;
                    index.belongs(&id, &party.0)?;
                }
                Entry::Launched { group, job, bytes, .. } => {
                    let id = RequestId::Compile {
                        group: group.clone(),
                        job: job.clone(),
                    };
                    index.declares(&id, bytes.0)?;
                }
                Entry::Reserved { .. } | Entry::Turn { .. } | Entry::Returned { .. } | Entry::Persisted { .. } => {}
            }
        }
        index.capacities.check(record)?;
        Ok(Self {
            tick,
            bytes: index.bytes,
            party: index.party,
            compiles,
            standing,
        })
    }
    /// A request's declared bytes this tick.
    pub fn bytes(&self, id: &RequestId) -> Result<u64, String> {
        self.bytes.get(id).copied().ok_or_else(|| format!("{id} has no declaration in its record"))
    }
    /// The party a request belongs to this tick.
    pub fn party(&self, id: &RequestId) -> Result<String, String> {
        self.party.get(id).cloned().ok_or_else(|| format!("{id} has no party in its record"))
    }
    /// A listed candidate, read as its kind requires, with its request's facts.
    pub fn listed(&self, entry: &Listed<Projected>) -> Result<Listed, String> {
        let candidate = &entry.candidate;
        let id = candidate.id.clone();
        Ok(Listed {
            party: entry.party.clone(),
            candidate: Candidate::read(
                id.clone(),
                (self.party(&id)?, self.bytes(&id)?),
                candidate.eligible_at,
                candidate.catalogue,
                candidate.lacks.as_ref().map(|capacity| capacity.derived(self.compiles)),
            )
            .map_err(|error| format!("tick {}: {error}", self.tick))?,
        })
    }
    /// The standing of the head the tick before left.
    pub fn standing(&self) -> Option<&Standing> {
        self.standing.as_ref()
    }
}

/// A step's listing; a kept head lists none.
fn listing(step: &Step<Projected>) -> &[Listed<Projected>] {
    match step {
        Step::Chosen { listed, .. } => listed,
        Step::Kept { .. } => &[],
    }
}

/// The capacity a kept head's standing holds: a pending compile's, or what an ended one's engine still knew.
fn standing_capacity(standing: &Standing) -> Option<&Capacity> {
    match standing {
        Standing::Pending { lacks, .. } => lacks.capacity(),
        Standing::Ended { capacity, .. } => capacity.as_ref(),
    }
}

/// The observations as they are read.
#[derive(Default)]
struct Index {
    tick: u64,
    bytes: BTreeMap<RequestId, u64>,
    party: BTreeMap<RequestId, String>,
    /// Each group's party: a compile job belongs to its group's.
    groups: BTreeMap<String, String>,
    admitted: BTreeSet<RequestId>,
    capacities: Capacities,
}

impl Index {
    fn declares(&mut self, id: &RequestId, bytes: u64) -> Result<(), String> {
        observe(self.tick, &mut self.bytes, id.clone(), bytes, &format!("{id}'s declaration"))
    }
    fn belongs(&mut self, id: &RequestId, party: &str) -> Result<(), String> {
        observe(self.tick, &mut self.party, id.clone(), party.to_owned(), &format!("{id}'s party"))?;
        match id {
            RequestId::Compile { group, .. } => {
                observe(self.tick, &mut self.groups, group.clone(), party.to_owned(), &format!("group {group}'s party"))
            }
            RequestId::Claim { .. } => Ok(()),
        }
    }
    fn admits(&mut self, id: &RequestId) -> Result<(), String> {
        let tick = self.tick;
        ensure(self.admitted.insert(id.clone()), || format!("tick {tick}: {id} is admitted twice in one tick"))
    }
    fn capacity(&mut self, id: &RequestId, party: &str, capacity: Option<&Capacity>) -> Result<(), String> {
        match (id, capacity) {
            (RequestId::Compile { group, .. }, Some(capacity)) => self.capacities.observe(self.tick, group, party, capacity),
            _ => Ok(()),
        }
    }
}

/// Records `value` as the one value of `key`'s fact, `what`, refusing another.
fn observe<K: Ord, V: PartialEq + Debug>(tick: u64, facts: &mut BTreeMap<K, V>, key: K, value: V, what: &str) -> Result<(), String> {
    match facts.get(&key) {
        Some(known) => ensure(*known == value, || format!("tick {tick}: {what} is {known:?} and {value:?} in one record")),
        None => {
            facts.insert(key, value);
            Ok(())
        }
    }
}

/// The capacities a record holds, which one tick's sample and running compiles give: the host's counts and all
/// running compiles in each, its group's units and limit in each of a group, and its party's units in each of a
/// party. Each is read as derived: its figures are the derivation's, and only its raw counts are the record's.
#[derive(Default)]
struct Capacities {
    host: Option<(usize, usize, usize, usize)>,
    groups: BTreeMap<String, (Units, usize)>,
    parties: BTreeMap<String, Units>,
}

impl Capacities {
    /// A derived capacity's raw counts: within their bounds, the figures among them, and the tick's other capacities'.
    fn observe(&mut self, tick: u64, group: &str, party: &str, capacity: &Capacity) -> Result<(), String> {
        let units = |units: &Units| units.contracted <= units.running;
        ensure(
            units(&capacity.running) && units(&capacity.group) && units(&capacity.party),
            || format!("tick {tick}: group {group}'s capacity counts more contracted compiles than running ones"),
        )?;
        ensure(
            capacity.group.running <= capacity.party.running
                && capacity.party.running <= capacity.running.running
                && capacity.group.contracted <= capacity.party.contracted
                && capacity.party.contracted <= capacity.running.contracted,
            || format!("tick {tick}: group {group}'s capacity counts more of its group's compiles than its party's, or its party's than all"),
        )?;
        ensure(capacity.held <= capacity.running.running, || {
            format!("tick {tick}: group {group}'s capacity holds more cores than compiles run")
        })?;
        ensure(capacity.group_limit <= capacity.core_cap, || {
            format!("tick {tick}: group {group}'s limit exceeds the core cap")
        })?;
        let host = (capacity.eligible, capacity.held, capacity.idle, capacity.running.running);
        self.host.get_or_insert(host);
        ensure(self.host == Some(host), || format!("tick {tick}: the capacities disagree on the host's figures"))?;
        observe(tick, &mut self.groups, group.to_owned(), (capacity.group, capacity.group_limit), &format!("group {group}'s units"))?;
        observe(tick, &mut self.parties, party.to_owned(), capacity.party, &format!("party {party}'s units"))
    }
    /// The host's counts against the record's slots, which the same tick's sample gives: the eligible cores no
    /// compile holds, at most the idle ones.
    fn check(&self, record: &Record<Projected>) -> Result<(), String> {
        let tick = record.tick;
        self.host.map_or(Ok(()), |(eligible, held, idle, _)| {
            let slots = eligible.saturating_sub(held).min(idle);
            ensure(record.slots == slots, || {
                format!("tick {tick}: the record counts {} slots, and its capacities give {slots}", record.slots)
            })
        })
    }
}
