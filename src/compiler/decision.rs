//! The decision record: what one admission tick decided, with every input it decided from. A tick writes one to
//! the journal when any pending request's decision changes, when it admits anything or changes the head or the
//! turn, and in every tick a head reserves. The service never reads a record back. The replay reads each record as
//! its raw inputs and decisions alone (`Projected`), derives every figure from them and from what the instance's
//! earlier ticks left (`Carried`), and `agree` checks the written record against that derivation.
mod agree;
mod carried;
mod decisions;
mod facts;
mod form;
#[cfg(test)]
mod mutations;
pub use form::{Form, Projected, Written};
#[cfg(test)]
pub use form::Variants;

use super::{
    budget::{Budget, Purse},
    contract,
    instance::Instance,
    precedence::{
        aged, Attempt, Choice, Class, Figures, Grant, Head, Listed, Precedence, RequestId, Step, Turn, Work,
    },
};
use agree::Unknown;
use carried::Carried;
use facts::Requests;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Where a record starts in the service's standard error.
pub const PREFIX: &str = "compile-queue: admission ";

/// The phases of a tick, in the order they admit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    /// The aged head, then claims and compiler turns by party share.
    Serve,
    /// Ordinary compile launches.
    Launch,
    /// Ordinary claims still pending, from what the launches left.
    Close,
    LargeLaunch,
    LargeClose,
}
impl Phase {
    fn class(self) -> Class {
        match self {
            Self::Serve | Self::Launch | Self::Close => Class::Ordinary,
            Self::LargeLaunch | Self::LargeClose => Class::Large,
        }
    }
}

/// A phase that grants claims and ends in a persistence sequence, which persists them: the pre-launch sequence,
/// the ordinary close and the large close. Only these phases record grants and completions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Persisting {
    Serve,
    Close,
    LargeClose,
}
impl Persisting {
    /// Every phase that persists, in the order a tick runs them. Each arm names the next, so a new phase does not
    /// compile until it has its place.
    pub fn all() -> impl Iterator<Item = Self> {
        std::iter::successors(Some(Self::Serve), |phase| match phase {
            Self::Serve => Some(Self::Close),
            Self::Close => Some(Self::LargeClose),
            Self::LargeClose => None,
        })
    }
    pub fn closing(class: Class) -> Self {
        match class {
            Class::Ordinary => Self::Close,
            Class::Large => Self::LargeClose,
        }
    }
}
impl From<Persisting> for Phase {
    fn from(phase: Persisting) -> Self {
        match phase {
            Persisting::Serve => Self::Serve,
            Persisting::Close => Self::Close,
            Persisting::LargeClose => Self::LargeClose,
        }
    }
}

/// A phase that launches compiles, each recording its own settlement. Only these phases record launches.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Launching {
    Launch,
    LargeLaunch,
}
impl From<Launching> for Phase {
    fn from(phase: Launching) -> Self {
        match phase {
            Launching::Launch => Self::Launch,
            Launching::LargeLaunch => Self::LargeLaunch,
        }
    }
}

/// A piece of the service's own work in its projection, with its declaration. Only a compile declares a finish.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Counted {
    /// A contracted running compile: its cell and its declared finish on the contract clock, its recorded start
    /// plus its declared runtime.
    Compile { attempt: Attempt, bytes: u64, finish: f64 },
    /// A granted claim held only for host work: its bytes.
    Claim { grant: Grant, bytes: u64 },
}
impl Counted {
    pub fn work(&self) -> Work {
        match self {
            Self::Compile { attempt, .. } => Work::Compile(attempt.clone()),
            Self::Claim { grant, .. } => Work::Claim(grant.clone()),
        }
    }
    pub fn bytes(&self) -> u64 {
        match self {
            Self::Compile { bytes, .. } | Self::Claim { bytes, .. } => *bytes,
        }
    }
    /// A compile's declared finish; a claim declares none.
    fn finish(&self) -> Option<f64> {
        match self {
            Self::Compile { finish, .. } => Some(*finish),
            Self::Claim { .. } => None,
        }
    }
}

/// A launch that ran ahead into the aged head's bytes: the run-ahead deadline it was checked against, and the
/// work the head waits for from then on, its own attempt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Ahead<F: Form = Written> {
    pub deadline: F::Figure<f64>,
    pub work: Work,
}

/// One step of the tick's running budget, in the order taken. `budget` is what it was admitted from.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case", bound = "")]
pub enum Entry<F: Form = Written> {
    /// The aged head's bytes are held from every later admission.
    Reserved {
        request: F::Figure<RequestId>,
        bytes: F::Figure<u64>,
        budget: F::Figure<Budget>,
    },
    /// A party's compiler turn kept its next job's bytes from claims.
    Turn { party: String, bytes: u64, budget: F::Figure<Budget> },
    /// The turns' bytes go back to the launches they were kept for.
    Returned { bytes: F::Figure<u64>, budget: F::Figure<Budget> },
    Granted {
        phase: Persisting,
        claim: String,
        /// The party the tick's attribution, made at its start, gave the claim.
        party: F::Observed<String>,
        class: F::Figure<Class>,
        bytes: F::Observed<u64>,
        budget: F::Figure<Budget>,
        head: F::Figure<bool>,
    },
    Launched {
        phase: Launching,
        group: String,
        job: String,
        class: F::Figure<Class>,
        bytes: F::Observed<u64>,
        budget: F::Figure<Budget>,
        /// What was reserved for the aged head before this launch.
        reserved: F::Figure<u64>,
        head: F::Figure<bool>,
        ahead: Option<Ahead<F>>,
        /// The contract-clock reading its selection used, which is also its recorded start.
        admitted_at: f64,
        declared: Option<u64>,
        /// The contract-clock reading once the whole launch operation returned: after its running record's
        /// write, or for a failed spawn after its result's publication and its intent's removal.
        settled_at: f64,
        /// Whether it left a running compile; a failed spawn leaves none.
        running: bool,
    },
    /// A persistence sequence that is no launch completed: the contract-clock reading after its last await.
    Persisted { phase: Persisting, at: f64, budget: F::Figure<Budget> },
}
impl<F: Form> Entry<F> {
    fn phase(&self) -> Phase {
        match self {
            Self::Reserved { .. } | Self::Turn { .. } | Self::Returned { .. } => Phase::Serve,
            Self::Granted { phase, .. } | Self::Persisted { phase, .. } => (*phase).into(),
            Self::Launched { phase, .. } => (*phase).into(),
        }
    }
    /// The request this entry admitted.
    fn admitted(&self) -> Option<RequestId> {
        match self {
            Self::Granted { claim, .. } => Some(RequestId::Claim { claim: claim.clone() }),
            Self::Launched { group, job, .. } => Some(RequestId::Compile {
                group: group.clone(),
                job: job.clone(),
            }),
            Self::Reserved { .. } | Self::Turn { .. } | Self::Returned { .. } | Self::Persisted { .. } => None,
        }
    }
}

/// Why a pending request stands where it does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Admitted,
    /// The aged head, reserving while it does not fit.
    AgedHead,
    /// The aged head, fitting: it waits for a slot or a unit of capacity.
    AgedHeadFitting,
    /// More than the service's own work can free.
    NotServiceable,
    /// Its head ended in vain; it waits for the work then counted to end.
    GivingWay,
    WaitingForTurn,
    /// Large: served only from what every smaller request left.
    LargeWaiting,
}
impl Decision {
    /// A pending request's decision, when it holds no head: by its class, its bytes and whether it gives way.
    pub fn waiting(class: Class, bytes: u64, serviceable: u64, giving_way: bool) -> Self {
        match (class, giving_way, bytes > serviceable) {
            (Class::Large, _, _) => Self::LargeWaiting,
            (Class::Ordinary, true, _) => Self::GivingWay,
            (Class::Ordinary, false, true) => Self::NotServiceable,
            (Class::Ordinary, false, false) => Self::WaitingForTurn,
        }
    }
    pub fn head(fits: bool) -> Self {
        match fits {
            true => Self::AgedHeadFitting,
            false => Self::AgedHead,
        }
    }
    /// Whether only a request the record shows can have this decision: one it admitted, or a head it holds.
    fn shown(self) -> bool {
        match self {
            Self::Admitted | Self::AgedHead | Self::AgedHeadFitting => true,
            Self::NotServiceable | Self::GivingWay | Self::WaitingForTurn | Self::LargeWaiting => false,
        }
    }
}

/// What a decision is about. `Group` stands for every pending job of a group not listed by itself.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Subject {
    Claim { claim: String },
    Job { group: String, job: String },
    Group { group: String },
}
impl From<&RequestId> for Subject {
    fn from(id: &RequestId) -> Self {
        match id {
            RequestId::Claim { claim } => Self::Claim { claim: claim.clone() },
            RequestId::Compile { group, job } => Self::Job {
                group: group.clone(),
                job: job.clone(),
            },
        }
    }
}

/// A subject's decision; for a group's jobs whose cells leave them more than one, how many have each, which
/// with the group's cells and the record's S says which job has which.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decisions {
    One(Decision),
    Jobs(BTreeMap<Decision, usize>),
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Decided {
    pub subject: Subject,
    pub decision: Decisions,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "", deserialize = "F::Lacks: Deserialize<'de>, F::Standing: Deserialize<'de>"))]
pub struct Record<F: Form = Written> {
    /// The engine instance that wrote it. Each instance starts from the empty admission state, and its ticks count
    /// from 1.
    pub instance: Instance,
    pub tick: u64,
    pub sampled_at: f64,
    /// The terms of the budget: the smaller of the sample's and a fresh reading, less running compiles' unused
    /// declarations, the headroom and granted claims' unused declarations.
    pub available: u64,
    pub compiler_reserved: u64,
    pub minimum_ram: u64,
    pub claims_unused: u64,
    /// D: the declarations of the service's own work that ends.
    pub own_work: Vec<Counted>,
    /// The budget the aged head and the share pass start from.
    pub left: F::Figure<Budget>,
    /// S: `left` plus D, floored at nothing.
    pub serviceable: F::Figure<u64>,
    pub draining: bool,
    pub aged: Choice<F>,
    pub aged_reserved: F::Figure<u64>,
    /// The tick's compile slots, counted once before its first launch.
    pub slots: usize,
    pub admitted: Vec<Entry<F>>,
    pub left_after_launch: F::Figure<u64>,
    /// The decisions that changed since the previous tick, a request's first included.
    pub decisions: Vec<Decided>,
}

impl Default for Record {
    fn default() -> Self {
        Self {
            instance: Instance::default(),
            tick: 0,
            sampled_at: 0.0,
            available: 0,
            compiler_reserved: 0,
            minimum_ram: 0,
            claims_unused: 0,
            own_work: Vec::new(),
            left: Budget::default(),
            serviceable: 0,
            draining: false,
            aged: Choice::default(),
            aged_reserved: 0,
            slots: 0,
            admitted: Vec::new(),
            left_after_launch: 0,
            decisions: Vec::new(),
        }
    }
}

impl Record {
    /// A persistence sequence that is no launch completed at `at` on the contract clock, `budget` left.
    pub fn persisted(&mut self, phase: Persisting, at: f64, budget: Budget) {
        self.admitted.push(Entry::Persisted { phase, at, budget });
    }
    /// Whether a head reserves in this tick: the instance's next tick then writes a record.
    pub fn reserves(&self) -> bool {
        self.aged_reserved > 0
    }
}

impl<F: Form> Record<F> {
    /// The budget's terms, kept signed.
    pub fn budget(&self) -> Budget {
        Budget::of(self.available)
            .less(self.compiler_reserved)
            .less(self.minimum_ram)
            .less(self.claims_unused)
    }
    /// D: what the service's own work that ends declares.
    pub fn own(&self) -> u64 {
        self.own_work.iter().fold(0u64, |sum, counted| sum.saturating_add(counted.bytes()))
    }
    /// The service's own work that ends.
    pub fn live(&self) -> BTreeSet<Work> {
        self.own_work.iter().map(Counted::work).collect()
    }
    /// D's compiles: the running compiles with a runtime contract.
    pub fn compiles(&self) -> usize {
        self.own_work.iter().filter_map(Counted::finish).count()
    }
    /// The requests the record admits: each it granted or launched. The one list of a record's admissions, which
    /// its journal and its replay both read.
    pub fn admissions(&self) -> impl Iterator<Item = RequestId> + '_ {
        self.admitted.iter().filter_map(Entry::admitted)
    }
}

/// What the service projects it can free: `left` plus the declarations of its own work that ends.
pub fn serviceable(left: Budget, own: u64) -> u64 {
    left.plus(own).floor()
}

fn ensure(holds: bool, rule: impl FnOnce() -> String) -> Result<(), String> {
    match holds {
        true => Ok(()),
        false => Err(rule()),
    }
}

/// One record's derivation: the record its facts and the carried facts give, the figures it could not know with
/// why, the precedence its tick left, and the decisions its tick is known to hold.
pub struct Derivation {
    pub record: Record,
    pub unknown: Vec<Unknown>,
    precedence: Precedence,
    held: BTreeMap<Subject, Decisions>,
}
impl Derivation {
    /// The derived record as a journal line holds it, each figure it could not know shown as such.
    pub fn line(&self) -> Result<Value, String> {
        let mut value = serde_json::to_value(&self.record).map_err(|e| e.to_string())?;
        for unknown in &self.unknown {
            unknown.mark(&mut value);
        }
        Ok(value)
    }
}

/// Derives one record from its facts, `facts`, and what its instance's earlier ticks left, `carried`. Every rule
/// reads only those, and the figures derived from them; the precedence the tick before left changes as the service
/// changes it. The written record, `written`, is then checked against the derivation, field for field, in one place.
fn derive(written: &Value, facts: &Record<Projected>, carried: &Carried) -> Result<Derivation, String> {
    let tick = facts.tick;
    let live = facts.live();
    ensure(live.len() == facts.own_work.len(), || format!("tick {tick}: D counts a piece of work twice"))?;
    let deadline = derive_deadline(facts)?;
    let mut precedence = carried.precedence();
    precedence.settle(&live);
    let requests = Requests::read(facts, precedence.head())?;
    let left = facts.budget();
    let figures = Figures {
        left,
        serviceable: serviceable(left, facts.own()),
        live: &live,
        draining: facts.draining,
    };
    let aged = derive_choice(facts, &requests, carried.waiting(left), &mut precedence, figures)?;
    derive_completions(facts)?;
    let admissions = derive_admissions(facts, &requests, &mut precedence, aged.head.as_ref(), figures, deadline)?;
    let record = Record {
        instance: facts.instance.clone(),
        tick,
        sampled_at: facts.sampled_at,
        available: facts.available,
        compiler_reserved: facts.compiler_reserved,
        minimum_ram: facts.minimum_ram,
        claims_unused: facts.claims_unused,
        own_work: facts.own_work.clone(),
        left,
        serviceable: figures.serviceable,
        draining: facts.draining,
        aged,
        aged_reserved: admissions.reserved,
        slots: facts.slots,
        admitted: admissions.entries,
        left_after_launch: admissions.after_launch,
        decisions: facts.decisions.clone(),
    };
    let held = decisions::check(&record, &precedence, carried)?;
    let unknown = carried.unknown();
    let derived = serde_json::to_value(&record).map_err(|e| e.to_string())?;
    agree::agree(written, &derived, &unknown).map_err(|difference| format!("tick {tick}: {difference}"))?;
    Ok(Derivation { record, unknown, precedence, held })
}

/// Every phase that ends in a persistence sequence records its completion once, as its last entry: the pre-launch
/// completion after every Serve entry, the ordinary close's before any large launch, and the large close's last of
/// all. A launch phase records none: each launch records its own settlement, and a completion names a phase that
/// persists.
fn derive_completions(record: &Record<Projected>) -> Result<(), String> {
    let tick = record.tick;
    let completion = |entry: &Entry<Projected>| matches!(entry, Entry::Persisted { .. });
    for phase in Persisting::all().map(Phase::from) {
        let entries: Vec<&Entry<Projected>> = record.admitted.iter().filter(|entry| entry.phase() == phase).collect();
        let completions = entries.iter().filter(|entry| completion(entry)).count();
        ensure(completions == 1, || format!("tick {tick}: the {phase:?} phase records {completions} completions, not one"))?;
        ensure(entries.last().is_some_and(|entry| completion(entry)), || {
            format!("tick {tick}: the {phase:?} phase's completion is not its last entry")
        })?;
    }
    Ok(())
}

/// The choice, from the precedence the tick before left: the head it left judged first, then, while no head is held,
/// each choice by the turn's walk over the listing the record holds, which the derivation checks. A choice drops the
/// request it judged unless it holds it; a choice that ends without a head ended because no other party was listed.
/// Each verdict and each choice the record shows must be the one derived.
fn derive_choice(
    facts: &Record<Projected>,
    requests: &Requests,
    waiting: Option<Budget>,
    precedence: &mut Precedence,
    figures: Figures,
) -> Result<Choice, String> {
    let tick = facts.tick;
    let before = precedence.turn().cloned();
    let mut dropped = BTreeSet::new();
    // The record judges a kept head first exactly when the tick before left one (`Requests::read`).
    let mut steps: Vec<Step> = precedence.judge_kept(requests.standing().cloned(), figures, &mut dropped).into_iter().collect();
    for (derived, written) in steps.iter().zip(&facts.aged.steps) {
        let (verdict, judged) = (written.verdict(), derived.verdict());
        ensure(judged == verdict, || {
            format!("tick {tick}: the head {} was {verdict:?}, its figures say {judged:?}", derived.judged())
        })?;
    }
    // The latest choice's listing and the party entry it chose.
    let mut last: Option<(Vec<Listed>, Listed)> = None;
    for step in &facts.aged.steps[steps.len()..] {
        let (listed, chosen, verdict) = match step {
            Step::Chosen { listed, chosen, verdict, .. } => Ok((listed, chosen, *verdict)),
            Step::Kept { .. } => Err(format!("tick {tick}: a head is judged again after the tick's first step")),
        }?;
        ensure(precedence.head().is_none(), || format!("tick {tick}: a choice while a head is held"))?;
        let listed = listed.iter().map(|entry| requests.listed(entry)).collect::<Result<Vec<Listed>, String>>()?;
        derive_listing(facts, figures, &listed, &dropped, precedence, waiting)?;
        last.as_ref().map(|(earlier, chose)| replay_relisting(tick, &listed, earlier, chose)).transpose()?;
        let derived = precedence
            .choose_from(listed.clone(), figures, &mut dropped)
            .ok_or_else(|| format!("tick {tick}: a choice with no party listed"))?;
        let (gives, judged) = (derived.judged().clone(), derived.verdict());
        ensure(gives == *chosen, || format!("tick {tick}: {chosen} was chosen, the turn's walk gives {gives}"))?;
        ensure(judged == verdict, || {
            format!("tick {tick}: the chosen {chosen} was {verdict:?}, its figures say {judged:?}")
        })?;
        let chose = listed.iter().find(|entry| entry.candidate.id == gives).cloned();
        last = chose.map(|chose| (listed, chose));
        steps.push(derived);
    }
    ensure(precedence.head().is_some() || last.as_ref().is_none_or(|(listed, _)| listed.len() == 1), || {
        format!("tick {tick}: the choice ended without a head while another party was listed")
    })?;
    Ok(Choice {
        before,
        steps,
        after: precedence.turn().cloned(),
        head: precedence.head().cloned(),
    })
}

/// A choice's listing, as the tick's figures show its candidates: each party once, in name order, with a candidate
/// of its own that is ordinary, at most S, aged by the tick's sample time, not giving way, a compile job only while
/// the engine does not drain and only while it waits for memory, and no request the tick judged and dropped. A
/// compile job waits for memory when its cell exceeds `waiting`, the smaller of `left` and what the last launches
/// left; with no `waiting`, no tick of the instance has launched yet, and no compile job is a candidate.
fn derive_listing(
    record: &Record<Projected>,
    figures: Figures,
    listed: &[Listed],
    dropped: &BTreeSet<RequestId>,
    precedence: &Precedence,
    waiting: Option<Budget>,
) -> Result<(), String> {
    let tick = record.tick;
    ensure(listed.windows(2).all(|pair| pair[0].party < pair[1].party), || {
        format!("tick {tick}: the listed parties are distinct and in name order")
    })?;
    for entry in listed {
        let candidate = &entry.candidate;
        ensure(
            candidate.party == entry.party
                && candidate.bytes <= figures.serviceable
                && Class::of(candidate.bytes) == Class::Ordinary,
            || format!("tick {tick}: {} is listed but is no serviceable ordinary candidate", candidate.id),
        )?;
        ensure(aged(candidate.eligible_at.0, record.sampled_at), || {
            format!("tick {tick}: {} is listed before it aged by the tick's sample at {}", candidate.id, record.sampled_at)
        })?;
        ensure(!figures.draining || matches!(candidate.id, RequestId::Claim { .. }), || {
            format!("tick {tick}: compile job {} is listed while the engine drains", candidate.id)
        })?;
        ensure(!dropped.contains(&candidate.id), || {
            format!("tick {tick}: {} is listed after the tick judged and dropped it", candidate.id)
        })?;
        ensure(!precedence.gives_way(&candidate.id), || {
            format!("tick {tick}: {} is listed while it gives way", candidate.id)
        })?;
        ensure(
            matches!(candidate.id, RequestId::Claim { .. }) || waiting.is_some_and(|waiting| !waiting.fits(candidate.bytes)),
            || format!("tick {tick}: compile job {} is listed, and it does not wait for memory", candidate.id),
        )?;
    }
    Ok(())
}

/// A listing after an earlier choice of the tick, which dropped `dropped`: every other party's candidate is the
/// one listed before, and `dropped`'s party lists only a younger one, or none.
fn replay_relisting(tick: u64, listed: &[Listed], earlier: &[Listed], dropped: &Listed) -> Result<(), String> {
    let others = |listing: &[Listed]| -> Vec<Listed> {
        listing.iter().filter(|entry| entry.party != dropped.party).cloned().collect()
    };
    ensure(others(listed) == others(earlier), || {
        format!("tick {tick}: after {} was dropped, another party's listing changed", dropped.candidate.id)
    })?;
    ensure(
        listed
            .iter()
            .filter(|entry| entry.party == dropped.party)
            .all(|entry| entry.candidate.age() > dropped.candidate.age()),
        || format!("tick {tick}: after {} was dropped, its party lists no younger candidate", dropped.candidate.id),
    )
}

/// How the held head is served, tick step 4's first action.
#[derive(Clone, Debug, PartialEq)]
enum Service {
    /// A fitting claim head is granted at once.
    AtOnce(String),
    /// Every other head reserves its bytes.
    Reserve,
}
impl Service {
    fn of(head: &Head) -> Self {
        at_once_id(head).map_or(Self::Reserve, |claim| Self::AtOnce(claim.clone()))
    }
}

/// What the tick's admissions derive: their entries with every figure, the reservation made, and what the ordinary
/// launches left.
struct Admissions {
    entries: Vec<Entry>,
    reserved: u64,
    after_launch: u64,
}

/// The tick's admissions, in their recorded order, from `left`: the held head's service first, every later admission
/// from the one running budget, each request charged its one declaration, and each launch's run-ahead derived from
/// what it took of the reservation. `precedence` changes as the service's does: a head granted at once is served, and
/// a compile that ran ahead into its bytes and runs is work the head waits for.
fn derive_admissions(
    facts: &Record<Projected>,
    requests: &Requests,
    precedence: &mut Precedence,
    head: Option<&Head>,
    figures: Figures,
    deadline: Option<f64>,
) -> Result<Admissions, String> {
    let tick = facts.tick;
    let service = head.map(Service::of);
    let at_once = head.and_then(at_once_id);
    let mut purse = Purse {
        memory: figures.left,
        reserved: 0,
    };
    let mut phase = Phase::Serve;
    let mut turns = 0u64;
    let mut after_launch = None;
    let mut reserved = 0u64;
    let mut launches = 0usize;
    // The latest persistence boundary so far: a launch's settlement or another sequence's completion.
    let mut boundary = f64::NEG_INFINITY;
    let mut entries = Vec::with_capacity(facts.admitted.len());
    for (index, entry) in facts.admitted.iter().enumerate() {
        ensure(entry.phase() >= phase, || format!("tick {tick}: entry {index} is out of phase order"))?;
        phase = entry.phase();
        after_launch = after_launch.or((phase > Phase::Launch).then_some(purse.memory));
        let serves = match entry {
            Entry::Reserved { .. } => Some(Service::Reserve),
            Entry::Granted { claim, .. } => (at_once == Some(claim)).then(|| Service::AtOnce(claim.clone())),
            Entry::Turn { .. } | Entry::Returned { .. } | Entry::Launched { .. } | Entry::Persisted { .. } => None,
        };
        ensure(serves == service.clone().filter(|_| index == 0), || {
            format!("tick {tick}: entry {index}: the held head's grant at once or its reservation is the first admission entry, and only it")
        })?;
        let budget = purse.memory;
        let derived = match entry {
            Entry::Reserved { .. } => {
                let head = head.ok_or_else(|| format!("tick {tick}: a reservation with no head held"))?;
                purse.memory = purse.memory.less(head.bytes);
                purse.reserved = head.bytes;
                reserved += head.bytes;
                Entry::Reserved {
                    request: head.id.clone(),
                    bytes: head.bytes,
                    budget,
                }
            }
            Entry::Turn { party, bytes, .. } => {
                ensure(purse.memory.fits(*bytes), || format!("tick {tick}: a compiler turn beyond the budget"))?;
                purse.memory = purse.memory.less(*bytes);
                turns += bytes;
                Entry::Turn {
                    party: party.clone(),
                    bytes: *bytes,
                    budget,
                }
            }
            Entry::Returned { .. } => {
                purse.memory = purse.memory.plus(turns);
                Entry::Returned {
                    bytes: std::mem::take(&mut turns),
                    budget,
                }
            }
            Entry::Granted { phase: granted, claim, .. } => {
                let id = RequestId::Claim { claim: claim.clone() };
                let bytes = requests.bytes(&id)?;
                let class = Class::of(bytes);
                ensure(class == phase.class(), || {
                    format!("tick {tick}: claim {claim} of class {class:?} granted in {phase:?}")
                })?;
                let at_once = at_once == Some(claim);
                ensure(at_once || head.is_none_or(|head| head.id != id), || {
                    format!("tick {tick}: claim {claim}, the held head, is granted only at once")
                })?;
                ensure(purse.memory.fits(bytes), || {
                    format!("tick {tick}: claim {claim} of {bytes} bytes granted beyond the budget")
                })?;
                purse.memory = purse.memory.less(bytes);
                match at_once {
                    true => precedence.served(),
                    false => {}
                }
                Entry::Granted {
                    phase: *granted,
                    claim: claim.clone(),
                    party: requests.party(&id)?,
                    class,
                    bytes,
                    budget,
                    head: at_once,
                }
            }
            Entry::Launched {
                phase: launching,
                group,
                job,
                ahead,
                admitted_at,
                declared,
                settled_at,
                running,
                ..
            } => {
                ensure(!facts.draining, || format!("tick {tick}: {job} launched while the engine drains"))?;
                let id = RequestId::Compile {
                    group: group.clone(),
                    job: job.clone(),
                };
                let bytes = requests.bytes(&id)?;
                let class = Class::of(bytes);
                ensure(class == phase.class(), || {
                    format!("tick {tick}: job {job} of class {class:?} launched in {phase:?}")
                })?;
                let own = head.is_some_and(|head| head.id.is_job(job));
                ensure(!own || head.is_some_and(|head| head.id == id), || {
                    format!("tick {tick}: {job}, the held head, launched in another group than its own, {group}")
                })?;
                ensure(admitted_at.is_finite() && settled_at.is_finite() && admitted_at <= settled_at, || {
                    format!("tick {tick}: {job} was admitted at {admitted_at} and settled at {settled_at}")
                })?;
                ensure(*admitted_at >= boundary, || {
                    format!("tick {tick}: {job} was admitted at {admitted_at}, before the persistence boundary at {boundary}")
                })?;
                // While the head does not fit, a launch of the ordinary pass declared to finish by the run-ahead
                // deadline may take from the reservation; the head takes its own.
                let ahead_deadline = deadline
                    .filter(|_| phase == Phase::Launch && head.is_some_and(|head| !head.fits))
                    .filter(|deadline| contract::finishes_by(*admitted_at, *declared, *deadline));
                ensure(purse.fits(bytes, own || ahead_deadline.is_some()), || {
                    format!("tick {tick}: {job} of {bytes} bytes launched beyond its budget")
                })?;
                let reserved = purse.reserved;
                let paid = purse.charge(bytes, own);
                let derived_ahead = ahead_deadline.filter(|_| !own && paid > 0).map(|deadline| Ahead {
                    deadline,
                    work: attempt(job, *running, ahead.as_ref().map(|ahead| &ahead.work)),
                });
                ensure(ahead.is_some() == derived_ahead.is_some(), || match derived_ahead {
                    Some(_) => format!("tick {tick}: {job} took {paid} bytes of the reservation, and its record shows no run-ahead"),
                    None => format!(
                        "tick {tick}: {job} records a run-ahead, and it took nothing of a reservation it could take: \
                         the deadline D supports is {deadline:?}, the head is {:?}, it took {paid}",
                        head.map(|head| (&head.id, head.fits))
                    ),
                })?;
                derived_ahead.iter().filter(|_| *running).for_each(|ahead| precedence.ran_ahead(ahead.work.clone()));
                launches += 1;
                boundary = *settled_at;
                Entry::Launched {
                    phase: *launching,
                    group: group.clone(),
                    job: job.clone(),
                    class,
                    bytes,
                    budget,
                    reserved,
                    head: own,
                    ahead: derived_ahead,
                    admitted_at: *admitted_at,
                    declared: *declared,
                    settled_at: *settled_at,
                    running: *running,
                }
            }
            Entry::Persisted { phase: persisted, at, .. } => {
                ensure(at.is_finite() && *at >= boundary, || {
                    format!("tick {tick}: a persistence sequence completed at {at}, before the boundary at {boundary}")
                })?;
                boundary = *at;
                Entry::Persisted {
                    phase: *persisted,
                    at: *at,
                    budget,
                }
            }
        };
        entries.push(derived);
    }
    ensure(launches <= facts.slots, || {
        format!("tick {tick}: {launches} launches against a slot count of {}", facts.slots)
    })?;
    Ok(Admissions {
        entries,
        reserved,
        after_launch: after_launch.unwrap_or(purse.memory).floor(),
    })
}

/// The attempt a launch started, which the head waits for when it ran ahead: its job, started at the wall reading its
/// running record holds, `observed`, or at 0 for a failed spawn, which leaves no running record.
fn attempt(job: &str, running: bool, observed: Option<&Work>) -> Work {
    let started = observed
        .and_then(|work| match work {
            Work::Compile(Attempt { started_at, .. }) => Some(started_at.0),
            Work::Claim(_) => None,
        })
        .filter(|_| running);
    Work::compile(job, started.unwrap_or(0.0))
}

/// The run-ahead deadline the record supports: the latest declared finish among D's compiles alone, each finite.
/// The tick's own launches cannot raise it: while the head does not fit, each runs ahead only with a finish by it.
/// None while D counts no compile.
fn derive_deadline(record: &Record<Projected>) -> Result<Option<f64>, String> {
    let tick = record.tick;
    let finishes: Vec<f64> = record.own_work.iter().filter_map(Counted::finish).collect();
    ensure(finishes.iter().all(|finish| finish.is_finite()), || {
        format!("tick {tick}: a declared finish in D is not finite")
    })?;
    Ok(finishes.into_iter().reduce(f64::max))
}

/// A fitting claim head is granted at once; every other head reserves.
fn at_once_id(head: &Head) -> Option<&String> {
    match (&head.id, head.fits) {
        (RequestId::Claim { claim }, true) => Some(claim),
        _ => None,
    }
}

/// The records in a service's standard error, each as written.
pub fn parse(journal: &str) -> Result<Vec<Value>, String> {
    journal
        .lines()
        .filter_map(|line| line.split_once(PREFIX).map(|(_, record)| record))
        .map(|record| serde_json::from_str(record).map_err(|e| format!("unreadable admission record: {e}")))
        .collect()
}

/// Derives a journal's records, in order. Each engine instance's records are derived on their own: its first record
/// starts from the empty admission state whatever its tick, its ticks rise, a tick in which a head reserves is
/// followed by the next one, and a merged journal carries nothing from one instance to another.
pub fn derive_journal(written: &[Value]) -> Result<Vec<Derivation>, String> {
    let mut instances: BTreeMap<Instance, Carried> = BTreeMap::new();
    written
        .iter()
        .map(|line| {
            let facts: Record<Projected> =
                serde_json::from_value(line.clone()).map_err(|e| format!("unreadable admission record: {e}"))?;
            let instance = facts.instance.clone();
            let derivation = instances
                .remove(&instance)
                .unwrap_or_else(Carried::initial)
                .next(facts.tick)
                .and_then(|carried| derive(line, &facts, &carried).map(|derivation| (carried, derivation)))
                .map_err(|error| format!("{instance}: {error}"))?;
            let (carried, derivation) = derivation;
            instances.insert(instance, carried.after(&derivation));
            Ok(derivation)
        })
        .collect()
}

/// Checks a journal's records, as `derive_journal` derives them.
#[cfg(test)]
pub fn check(records: &[Record]) -> Result<(), String> {
    let written = records
        .iter()
        .map(|record| serde_json::to_value(record).map_err(|e| e.to_string()))
        .collect::<Result<Vec<Value>, String>>()?;
    derive_journal(&written).map(|_| ())
}

/// Every tick's record of `journal` derives as one journal, with no tick silent, and the written ones as the journal
/// the service wrote.
#[cfg(test)]
pub fn replays(journal: &Journal) -> Result<(), String> {
    let every: Vec<Record> = journal.history.iter().map(|(record, _)| record.clone()).collect();
    check(&every).map_err(|error| format!("every tick: {error}"))?;
    let written: Vec<Record> =
        journal.history.iter().filter(|(_, written)| *written).map(|(record, _)| record.clone()).collect();
    check(&written).map_err(|error| format!("the written journal: {error}"))
}

/// The decisions a tick last wrote, and when the next tick writes.
pub struct Journal {
    /// The engine instance whose records these are.
    instance: Instance,
    tick: u64,
    decisions: BTreeMap<Subject, Decisions>,
    turn: Option<Turn>,
    head: Option<RequestId>,
    last: Option<Value>,
    /// Every tick's record, written or not, for the tests to replay.
    #[cfg(test)]
    pub history: Vec<(Record, bool)>,
}

impl Journal {
    /// The journal of `instance`, which has written nothing yet.
    pub fn new(instance: Instance) -> Self {
        Self {
            instance,
            tick: 0,
            decisions: BTreeMap::new(),
            turn: None,
            head: None,
            last: None,
            #[cfg(test)]
            history: Vec::new(),
        }
    }
    /// A new tick's record.
    pub fn begin(&mut self, sampled_at: f64) -> Record {
        self.tick += 1;
        Record {
            instance: self.instance.clone(),
            tick: self.tick,
            sampled_at,
            ..Record::default()
        }
    }
    /// Writes the record when a decision changed, the tick admitted anything, the head or the turn changed, or a
    /// head reserves. `pending` holds every pending request's decision.
    pub fn finish(&mut self, mut record: Record, mut pending: BTreeMap<Subject, Decisions>) {
        for id in record.admissions() {
            pending.insert(Subject::from(&id), Decisions::One(Decision::Admitted));
        }
        record.decisions = pending
            .iter()
            .filter(|(subject, decision)| self.decisions.get(*subject) != Some(*decision))
            .map(|(subject, decision)| Decided {
                subject: subject.clone(),
                decision: decision.clone(),
            })
            .collect();
        let head = record.aged.head.as_ref().map(|head| head.id.clone());
        let written = !record.decisions.is_empty()
            || record.admissions().next().is_some()
            || record.aged.after != self.turn
            || head != self.head
            || record.reserves();
        self.decisions = pending;
        self.turn = record.aged.after.clone();
        self.head = head;
        if written {
            let value = serde_json::to_value(&record).unwrap_or(Value::Null);
            eprintln!("{PREFIX}{value}");
            self.last = Some(value);
        }
        #[cfg(test)]
        self.history.push((record, written));
    }
    /// The last record written.
    pub fn last(&self) -> Option<&Value> {
        self.last.as_ref()
    }
}

// The record schema's enums declared here, each with a value of every variant (`form::Variants`). Each arm names the
// next variant, so a new variant does not compile until it has its place in the list.
#[cfg(test)]
impl form::Variants for Persisting {
    fn variants() -> Vec<Self> {
        Self::all().collect()
    }
}
#[cfg(test)]
impl form::Variants for Launching {
    fn variants() -> Vec<Self> {
        std::iter::successors(Some(Self::Launch), |phase| match phase {
            Self::Launch => Some(Self::LargeLaunch),
            Self::LargeLaunch => None,
        })
        .collect()
    }
}
#[cfg(test)]
impl form::Variants for Counted {
    fn variants() -> Vec<Self> {
        let attempt = Attempt { job: String::new(), started_at: super::precedence::At(0.0) };
        std::iter::successors(Some(Self::Compile { attempt, bytes: 0, finish: 0.0 }), |counted| match counted {
            Self::Compile { .. } => Some(Self::Claim { grant: Grant { claim: String::new(), order: 0 }, bytes: 0 }),
            Self::Claim { .. } => None,
        })
        .collect()
    }
}
#[cfg(test)]
impl form::Variants for Entry {
    fn variants() -> Vec<Self> {
        let budget = Budget::default();
        let reserved = Self::Reserved { request: RequestId::Claim { claim: String::new() }, bytes: 0, budget };
        std::iter::successors(Some(reserved), |entry| match entry {
            Self::Reserved { .. } => Some(Self::Turn { party: String::new(), bytes: 0, budget }),
            Self::Turn { .. } => Some(Self::Returned { bytes: 0, budget }),
            Self::Returned { .. } => Some(Self::Granted {
                phase: Persisting::Serve,
                claim: String::new(),
                party: String::new(),
                class: Class::Ordinary,
                bytes: 0,
                budget,
                head: false,
            }),
            Self::Granted { .. } => Some(Self::Launched {
                phase: Launching::Launch,
                group: String::new(),
                job: String::new(),
                class: Class::Ordinary,
                bytes: 0,
                budget,
                reserved: 0,
                head: false,
                ahead: None,
                admitted_at: 0.0,
                declared: None,
                settled_at: 0.0,
                running: false,
            }),
            Self::Launched { .. } => Some(Self::Persisted { phase: Persisting::Serve, at: 0.0, budget }),
            Self::Persisted { .. } => None,
        })
        .collect()
    }
}
#[cfg(test)]
impl form::Variants for Decision {
    fn variants() -> Vec<Self> {
        std::iter::successors(Some(Self::Admitted), |decision| match decision {
            Self::Admitted => Some(Self::AgedHead),
            Self::AgedHead => Some(Self::AgedHeadFitting),
            Self::AgedHeadFitting => Some(Self::NotServiceable),
            Self::NotServiceable => Some(Self::GivingWay),
            Self::GivingWay => Some(Self::WaitingForTurn),
            Self::WaitingForTurn => Some(Self::LargeWaiting),
            Self::LargeWaiting => None,
        })
        .collect()
    }
}
#[cfg(test)]
impl form::Variants for Subject {
    fn variants() -> Vec<Self> {
        std::iter::successors(Some(Self::Claim { claim: String::new() }), |subject| match subject {
            Self::Claim { .. } => Some(Self::Job { group: String::new(), job: String::new() }),
            Self::Job { .. } => Some(Self::Group { group: String::new() }),
            Self::Group { .. } => None,
        })
        .collect()
    }
}
#[cfg(test)]
impl form::Variants for Decisions {
    fn variants() -> Vec<Self> {
        std::iter::successors(Some(Self::One(Decision::Admitted)), |decisions| match decisions {
            Self::One(_) => Some(Self::Jobs(BTreeMap::new())),
            Self::Jobs(_) => None,
        })
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::super::precedence::{At, Candidate, Capacity, Catalogue, Lacks, Standing, Units, Verdict};
    use super::*;
    pub(super) const GIB: u64 = 1 << 30;

    pub(super) fn compile(job: &str) -> Work {
        Work::compile(job, 1.0)
    }
    pub(super) fn claim(id: &str) -> RequestId {
        RequestId::Claim { claim: id.into() }
    }
    /// `record` as its instance's journal writes it when every decision is a first one: `pending` holds the decisions
    /// of the requests still pending at its end, and every request it admits is admitted.
    pub(super) fn first(record: Record, pending: &[(RequestId, Decision)]) -> Record {
        decided(record, pending.iter().map(|(id, decision)| (Subject::from(id), Decisions::One(*decision))).collect())
    }
    /// `record` as its instance's journal writes it with every decision a first one, `pending` by subject.
    pub(super) fn decided(record: Record, pending: BTreeMap<Subject, Decisions>) -> Record {
        let mut journal = Journal::new(record.instance.clone());
        journal.finish(record, pending);
        journal.history.pop().unwrap().0
    }
    /// D's compile w: a 4 GiB cell, declared to finish at `finish`.
    pub(super) fn counted_w(finish: f64) -> Counted {
        Counted::Compile {
            attempt: Attempt { job: "w".into(), started_at: At(1.0) },
            bytes: 4 * GIB,
            finish,
        }
    }
    /// The head reserving its `bytes` from `left`, no turn kept, and the tick's three persistence sequences completed
    /// at 57.
    pub(super) fn served(request: RequestId, bytes: u64, left: Budget) -> Vec<Entry> {
        let mut entries = vec![
            Entry::Reserved { request, bytes, budget: left },
            Entry::Returned { bytes: 0, budget: left.less(bytes) },
        ];
        entries.extend(completions(57.0, left.less(bytes)));
        entries
    }
    /// The completions of the tick's persistence sequences, each at `at` with `budget` left.
    pub(super) fn completions(at: f64, budget: Budget) -> Vec<Entry> {
        Persisting::all().map(|phase| Entry::Persisted { phase, at, budget }).collect()
    }
    /// 2 GiB left beside a running 4 GiB compile, S 6 GiB, and a 6 GiB claim chosen as the head, reserving.
    pub(super) fn reserving(bytes: u64, available: u64) -> Record {
        let left = Budget::of(available).less(4 * GIB).less(10 * GIB);
        let turn = Turn { party: "p".into(), held: true };
        let head = Head {
            id: claim("head"),
            party: "p".into(),
            bytes,
            fits: false,
            recorded: BTreeSet::from([compile("w")]),
        };
        let record = Record {
            tick: 7,
            sampled_at: 60.0,
            available,
            compiler_reserved: 4 * GIB,
            minimum_ram: 10 * GIB,
            own_work: vec![counted_w(1.0 + 60.0)],
            left,
            serviceable: serviceable(left, 4 * GIB),
            aged: Choice {
                before: None,
                steps: vec![Step::Chosen {
                    turn: None,
                    listed: vec![Listed {
                        party: "p".into(),
                        candidate: Candidate {
                            id: claim("head"),
                            party: "p".into(),
                            eligible_at: At(0.0),
                            catalogue: Catalogue::Claim { order: 1 },
                            bytes,
                            lacks: Lacks::Nothing,
                        },
                    }],
                    chosen: claim("head"),
                    verdict: Verdict::Kept { fits: false },
                }],
                after: Some(turn),
                head: Some(head),
            },
            aged_reserved: bytes,
            admitted: served(claim("head"), bytes, left),
            left_after_launch: 0,
            ..Record::default()
        };
        first(record, &[(claim("head"), Decision::AgedHead)])
    }

    #[test]
    fn a_record_that_obeys_the_rules_replays() {
        check(&[reserving(6 * GIB, 16 * GIB)]).unwrap();
    }

    #[test]
    fn a_younger_grant_from_the_heads_bytes_is_rejected() {
        let mut record = reserving(5 * GIB, 18 * GIB);
        check(&[record.clone()]).unwrap();
        let younger = |budget| Entry::Granted {
            phase: Persisting::Serve,
            claim: "younger".into(),
            party: "q".into(),
            class: Class::Ordinary,
            bytes: 2 * GIB,
            budget,
            head: false,
        };
        record.admitted.insert(1, younger(Budget::of(4 * GIB)));
        record = first(record, &[(claim("head"), Decision::AgedHead)]);
        rejected(&record, "claim younger of 2147483648 bytes granted beyond the budget");
        record.admitted[1] = younger(Budget::of(4 * GIB).less(5 * GIB));
        rejected(&record, "beyond the budget");
    }

    /// Review round 3's tick: 4 GiB left beside D's 4 GiB compile w, S 8 GiB, and an aged 5 GiB claim head that
    /// does not fit and reserves, nothing launched.
    fn round_3() -> Record {
        reserving(5 * GIB, 18 * GIB)
    }
    const NOT_FIRST: &str = "entry 0: the held head's grant at once or its reservation is the first admission entry, and only it";

    #[test]
    fn the_heads_reservation_first_and_nothing_granted_replays() {
        let reserved_first = round_3();
        assert_eq!(reserved_first.left, Budget::of(4 * GIB));
        assert_eq!(reserved_first.serviceable, 8 * GIB);
        derive_journal(&parse(&journal_line(&reserved_first, |_| ())).unwrap()).unwrap();
    }

    #[test]
    fn a_younger_grant_before_the_heads_reservation_is_rejected() {
        // The counterexample: a younger 2 GiB claim granted from the 4 GiB, then the reservation from the 2 GiB it
        // left. Every budget follows the running budget; only the order breaks the rules.
        let mut granted_first = round_3();
        let left = granted_first.left;
        let younger = Entry::Granted {
            phase: Persisting::Serve,
            claim: "younger".into(),
            party: "q".into(),
            class: Class::Ordinary,
            bytes: 2 * GIB,
            budget: left,
            head: false,
        };
        granted_first.admitted = [younger].into_iter().chain(served(claim("head"), 5 * GIB, left.less(2 * GIB))).collect();
        rejected(&first(granted_first, &[(claim("head"), Decision::AgedHead)]), NOT_FIRST);
    }

    #[test]
    fn a_compiler_turn_before_the_heads_reservation_is_rejected() {
        // A 1 GiB compiler turn kept from the 4 GiB, the reservation from the 3 GiB it left, the turn's bytes back.
        let mut turn_first = round_3();
        let left = turn_first.left;
        let reserved = left.less(GIB).less(5 * GIB);
        turn_first.admitted = vec![
            Entry::Turn { party: "q".into(), bytes: GIB, budget: left },
            Entry::Reserved { request: claim("head"), bytes: 5 * GIB, budget: left.less(GIB) },
            Entry::Returned { bytes: GIB, budget: reserved },
        ];
        turn_first.admitted.extend(completions(57.0, reserved.plus(GIB)));
        rejected(&turn_first, NOT_FIRST);
    }

    #[test]
    fn a_head_kept_after_memory_held_elsewhere_lowered_s_below_its_bytes_is_rejected() {
        let earlier = reserving(5 * GIB, 18 * GIB);
        let head = earlier.aged.head.clone().unwrap();
        // An outside process lowered S to 4 GiB; the recorded compile still runs.
        let left = Budget::of(14 * GIB).less(4 * GIB).less(10 * GIB);
        let record = Record {
            tick: earlier.tick + 1,
            decisions: Vec::new(),
            available: 14 * GIB,
            left,
            serviceable: serviceable(left, 4 * GIB),
            aged: Choice {
                before: earlier.aged.after.clone(),
                steps: vec![Step::Kept {
                    head: head.clone(),
                    standing: Standing::of(true, 5 * GIB, Lacks::Nothing),
                    verdict: Verdict::Kept { fits: false },
                }],
                after: earlier.aged.after.clone(),
                head: Some(head),
            },
            admitted: served(claim("head"), 5 * GIB, left),
            ..earlier.clone()
        };
        assert_eq!(record.serviceable, 4 * GIB);
        let error = check(&[earlier, record]).unwrap_err();
        assert!(error.contains("its figures say Released"), "{error}");
    }

    /// Three eligible cores, a contracted compile running, none idle because work outside the service takes them;
    /// the group and the party have units free.
    pub(super) fn outside_idle() -> Capacity {
        Capacity {
            eligible: 3,
            held: 0,
            idle: 0,
            running: Units { running: 1, contracted: 1 },
            group: Units::default(),
            group_limit: 2,
            party: Units::default(),
            core_cap: 2,
        }
    }
    pub(super) fn compile_id(job: &str) -> RequestId {
        RequestId::Compile { group: "g".into(), job: job.into() }
    }
    /// The compile slots `capacity` gives: the eligible cores no compile holds, at most the idle ones.
    pub(super) fn slots(capacity: &Capacity) -> usize {
        capacity.eligible.saturating_sub(capacity.held).min(capacity.idle)
    }
    /// 8 GiB left beside D's 4 GiB compile w, S 12 GiB, and the fitting 5 GiB compile head g/head that
    /// `chosen_compile_head`'s tick chose, kept in the next tick with `capacity`, reserving.
    pub(super) fn kept_compile_head(capacity: Capacity) -> Record {
        let chosen = chosen_compile_head();
        let head = chosen.aged.head.clone().unwrap();
        let mut record = Record { tick: chosen.tick + 1, decisions: Vec::new(), slots: slots(&capacity), ..chosen };
        record.aged = Choice {
            before: Some(Turn { party: "p".into(), held: true }),
            steps: vec![Step::Kept {
                head: head.clone(),
                standing: Standing::of(true, 5 * GIB, Lacks::Capacity(capacity)),
                verdict: Verdict::Kept { fits: true },
            }],
            after: Some(Turn { party: "p".into(), held: true }),
            head: Some(head),
        };
        record
    }
    /// `kept_compile_head`'s journal: the tick that chose the head, then the tick that kept it.
    pub(super) fn kept_compile_journal(capacity: Capacity) -> Vec<Record> {
        vec![chosen_compile_head(), kept_compile_head(capacity)]
    }

    #[test]
    fn a_fitting_compile_head_kept_while_idle_cores_are_taken_from_outside_is_rejected() {
        let capacity = outside_idle();
        let error = check(&kept_compile_journal(capacity.clone())).unwrap_err();
        assert!(error.contains("its figures say Passed"), "{error}");
        check(&kept_compile_journal(Capacity { idle: 3, ..capacity })).unwrap();
    }

    /// The same tick with g/head newly chosen: eligible at 0, with an idle core, it is kept and reserves.
    pub(super) fn chosen_compile_head() -> Record {
        let mut record = reserving(5 * GIB, 22 * GIB);
        let id = compile_id("head");
        let capacity = Capacity { idle: 3, ..outside_idle() };
        if let Step::Chosen { listed, chosen, verdict, .. } = &mut record.aged.steps[0] {
            listed[0].candidate = Candidate {
                id: id.clone(),
                catalogue: Catalogue::Compile { position: 0, index: 0 },
                lacks: Lacks::Capacity(capacity.clone()),
                ..listed[0].candidate.clone()
            };
            *chosen = id.clone();
            *verdict = Verdict::Kept { fits: true };
        }
        record.aged.head = Some(Head { id: id.clone(), fits: true, ..record.aged.head.clone().unwrap() });
        record.admitted = served(id.clone(), 5 * GIB, record.left);
        record.left_after_launch = record.left.less(5 * GIB).floor();
        record.slots = slots(&capacity);
        first(record, &[(id, Decision::AgedHeadFitting)])
    }

    #[test]
    fn a_pending_compile_heads_capacity_is_required_as_its_record_is_read() {
        const NO_CAPACITY: &str = "pending compile job head carries no capacity";
        // Review round 4's finding 2: three eligible cores, one held by a contracted compile, none idle. The rule
        // rejects keeping the fitting compile head, which must pass its turn.
        let auditor = Capacity { held: 1, ..outside_idle() };
        let chosen = chosen_compile_head();
        let kept = kept_compile_head(auditor.clone());
        let after = |kept: &Record, edit: &dyn Fn(&mut Value)| journal_line(&chosen, |_| ()) + &journal_line(kept, edit);
        let error = refused(&after(&kept, &|_| ()));
        assert!(error.contains("its figures say Passed"), "{error}");
        // Its capacity null or omitted, the record is refused as it is read, by the kind of the head the tick before
        // left.
        fn standing(value: &mut Value) -> &mut serde_json::Map<String, Value> {
            value["aged"]["steps"][0]["standing"].as_object_mut().unwrap()
        }
        let nulled = after(&kept, &|value| {
            standing(value).insert("capacity".into(), Value::Null);
        });
        let omitted = after(&kept, &|value| {
            standing(value).remove("capacity");
        });
        for line in [nulled, omitted] {
            let error = refused(&line);
            assert!(error.contains(NO_CAPACITY), "{error}");
        }
        // With an idle core it is kept, and the journal checks.
        derive_journal(&parse(&after(&kept_compile_head(Capacity { idle: 3, ..auditor }), &|_| ())).unwrap()).unwrap();
        // A newly chosen fitting compile head: its candidate's capacity null is refused likewise.
        derive_journal(&parse(&journal_line(&chosen, |_| ())).unwrap()).unwrap();
        let nulled = journal_line(&chosen, |value| value["aged"]["steps"][0]["listed"][0]["candidate"]["capacity"] = Value::Null);
        let error = refused(&nulled);
        assert!(error.contains(NO_CAPACITY), "{error}");
    }

    /// Review round 4's finding 1: 4 GiB left beside D's 4 GiB compile w, S 8 GiB, and an aged 3 GiB claim head
    /// that fits and is granted at once, charged `charged`; a younger claim of `younger` follows from what is left.
    fn fitting_claim_head(charged: u64, younger: u64) -> Record {
        let mut record = reserving(3 * GIB, 18 * GIB);
        let left = record.left;
        if let Step::Chosen { verdict, .. } = &mut record.aged.steps[0] {
            *verdict = Verdict::Kept { fits: true };
        }
        record.aged.head.as_mut().unwrap().fits = true;
        record.aged_reserved = 0;
        let grant = |claim: &str, party: &str, bytes, budget, head| Entry::Granted {
            phase: Persisting::Serve,
            claim: claim.into(),
            party: party.into(),
            class: Class::Ordinary,
            bytes,
            budget,
            head,
        };
        let rest = left.less(charged).less(younger);
        record.admitted = vec![
            grant("head", "p", charged, left, true),
            grant("younger", "q", younger, left.less(charged), false),
            Entry::Returned { bytes: 0, budget: rest },
        ];
        record.admitted.extend(completions(57.0, rest));
        record.left_after_launch = rest.floor();
        first(record, &[])
    }

    #[test]
    fn a_fitting_claim_heads_grant_at_once_charges_its_declared_bytes_to_its_party() {
        let full = fitting_claim_head(3 * GIB, GIB);
        assert_eq!((full.left, full.serviceable), (Budget::of(4 * GIB), 8 * GIB));
        derive_journal(&parse(&journal_line(&full, |_| ())).unwrap()).unwrap();
        // The counterexample: the head charged 1 GiB, and the younger 3 GiB claim granted from the rest.
        rejected(&fitting_claim_head(GIB, 3 * GIB), "head's declaration is 3221225472 and 1073741824 in one record");
        let mut elsewhere = full.clone();
        if let Entry::Granted { party, .. } = &mut elsewhere.admitted[0] {
            *party = "q".into();
        }
        rejected(&elsewhere, "head's party is \"p\" and \"q\" in one record");
        // The head is granted once: no share-pass grant names it again, here from the 0 bytes left.
        let mut twice = full.clone();
        let again = Entry::Granted { phase: Persisting::Serve, claim: "head".into(), party: "p".into(), class: Class::Ordinary, bytes: 0, budget: Budget::of(0), head: false };
        twice.admitted.insert(2, again);
        rejected(&twice, "head is admitted twice in one tick");
    }

    /// `chosen_compile_head`'s tick, the head launched in it from its reservation, as `edit` changes the launch.
    fn head_launched(edit: impl FnOnce(&mut Entry)) -> Record {
        let mut record = chosen_compile_head();
        let memory = record.left.less(5 * GIB);
        record.admitted.truncate(completion(&record, Persisting::Close));
        let mut launched = Entry::Launched {
            phase: Launching::Launch,
            group: "g".into(),
            job: "head".into(),
            class: Class::Ordinary,
            bytes: 5 * GIB,
            budget: memory,
            reserved: 5 * GIB,
            head: true,
            ahead: None,
            admitted_at: 57.0,
            declared: Some(4),
            settled_at: 57.5,
            running: true,
        };
        edit(&mut launched);
        record.admitted.push(launched);
        let closes = [Persisting::Close, Persisting::LargeClose];
        record.admitted.extend(closes.map(|phase| Entry::Persisted { phase, at: 57.5, budget: memory }));
        first(record, &[])
    }

    #[test]
    fn the_heads_launch_is_the_held_heads_job_with_its_cell() {
        check(&[head_launched(|_| ())]).unwrap();
        let smaller = head_launched(|launched| {
            if let Entry::Launched { bytes, .. } = launched {
                *bytes = 4 * GIB;
            }
        });
        rejected(&smaller, "head's declaration is 5368709120 and 4294967296 in one record");
        let elsewhere = head_launched(|launched| {
            if let Entry::Launched { group, .. } = launched {
                *group = "other".into();
            }
        });
        rejected(&elsewhere, "head, the held head, launched in another group than its own, other");
    }

    #[test]
    fn nothing_launches_while_the_engine_drains() {
        let launched = launched_after_serve();
        check(&[launched.clone()]).unwrap();
        rejected(&Record { draining: true, ..launched }, "j launched while the engine drains");
    }

    #[test]
    fn the_head_held_after_the_choice_is_the_replayed_head_with_its_party_and_work() {
        let record = reserving(6 * GIB, 16 * GIB);
        const HELD: &str = "the record's /aged/head/";
        let mut other_party = record.clone();
        other_party.aged.head.as_mut().unwrap().party = "q".into();
        rejected(&other_party, HELD);
        let mut other_work = record;
        other_work.aged.head.as_mut().unwrap().recorded.insert(compile("x"));
        rejected(&other_work, HELD);
    }

    #[test]
    fn d_counts_each_piece_of_work_once() {
        // w counted twice, and S and everything after it as if D held 8 GiB.
        let mut twice = reserving(6 * GIB, 16 * GIB);
        twice.own_work.push(counted_w(61.0));
        twice.serviceable = serviceable(twice.left, 8 * GIB);
        rejected(&twice, "D counts a piece of work twice");
    }

    #[test]
    fn a_listing_holds_aged_candidates_no_compile_while_draining_and_nothing_the_tick_dropped() {
        let early = Record { sampled_at: 20.0, ..reserving(6 * GIB, 16 * GIB) };
        rejected(&early, "head is listed before it aged by the tick's sample at 20");
        // While the engine drains, a compile job listed and chosen would be released, its party keeping the turn.
        let mut draining = Record { draining: true, ..chosen_compile_head() };
        if let Step::Chosen { verdict, .. } = &mut draining.aged.steps[0] {
            *verdict = Verdict::Released;
        }
        draining.aged.head = None;
        draining.aged_reserved = 0;
        draining.admitted = [Entry::Returned { bytes: 0, budget: draining.left }].into_iter().chain(completions(57.0, draining.left)).collect();
        draining.left_after_launch = draining.left.floor();
        rejected(&draining, "compile job head is listed while the engine drains");
        // The claim head of an earlier tick ends in vain, its compile w having ended, and the same claim is chosen
        // again in the tick.
        let earlier = reserving(6 * GIB, 16 * GIB);
        let mut again = Record { tick: earlier.tick + 1, decisions: Vec::new(), ..earlier.clone() };
        again.own_work = vec![Counted::Compile { attempt: Attempt { job: "x".into(), started_at: At(1.0) }, bytes: 4 * GIB, finish: 61.0 }];
        let listed = match &again.aged.steps[0] {
            Step::Chosen { listed, .. } => listed.clone(),
            Step::Kept { .. } => unreachable!(),
        };
        let passed = Turn { party: "p".into(), held: false };
        again.aged.before = Some(Turn { party: "p".into(), held: true });
        again.aged.steps = vec![
            Step::Kept {
                head: earlier.aged.head.clone().unwrap(),
                standing: Standing::of(true, 6 * GIB, Lacks::Nothing),
                verdict: Verdict::EndedInVain,
            },
            Step::Chosen { turn: Some(passed), listed, chosen: claim("head"), verdict: Verdict::Kept { fits: false } },
        ];
        let error = check(&[earlier, again]).unwrap_err();
        assert!(error.contains("head is listed after the tick judged and dropped it"), "{error}");
    }

    pub(super) fn listed(party: &str, id: RequestId, eligible_at: f64, catalogue: Catalogue, lacks: Lacks) -> Listed {
        let candidate = Candidate { id, party: party.into(), eligible_at: At(eligible_at), catalogue, bytes: 5 * GIB, lacks };
        Listed { party: party.into(), candidate }
    }
    /// 8 GiB left beside D's 4 GiB compile w, S 12 GiB. With no turn yet the walk starts at the oldest candidate:
    /// party p's 5 GiB compile job g/c, eligible at 0, which fits but lacks an idle core no contracted compile holds,
    /// and passes. Then `relisted` is listed, from p's passed turn, and q's 5 GiB claim b, eligible at 10, is chosen,
    /// fits and is granted at once.
    pub(super) fn passed_then(relisted: Vec<Listed>) -> Record {
        let mut record = reserving(5 * GIB, 22 * GIB);
        let c = listed("p", compile_id("c"), 0.0, Catalogue::Compile { position: 0, index: 0 }, Lacks::Capacity(outside_idle()));
        record.aged = Choice {
            before: None,
            steps: vec![
                Step::Chosen { turn: None, listed: vec![c, b()], chosen: compile_id("c"), verdict: Verdict::Passed },
                Step::Chosen {
                    turn: Some(Turn { party: "p".into(), held: false }),
                    listed: relisted,
                    chosen: claim("b"),
                    verdict: Verdict::Kept { fits: true },
                },
            ],
            after: Some(Turn { party: "q".into(), held: true }),
            head: Some(Head { id: claim("b"), party: "q".into(), bytes: 5 * GIB, fits: true, recorded: record.live() }),
        };
        let rest = record.left.less(5 * GIB);
        let grant = Entry::Granted { phase: Persisting::Serve, claim: "b".into(), party: "q".into(), class: Class::Ordinary, bytes: 5 * GIB, budget: record.left, head: true };
        record.admitted = vec![grant, Entry::Returned { bytes: 0, budget: rest }];
        record.admitted.extend(completions(57.0, rest));
        record.aged_reserved = 0;
        record.left_after_launch = rest.floor();
        decided(record, BTreeMap::from([(Subject::Group { group: "g".into() }, Decisions::One(Decision::WaitingForTurn))]))
    }
    pub(super) fn b() -> Listed {
        listed("q", claim("b"), 10.0, Catalogue::Claim { order: 2 }, Lacks::Nothing)
    }

    #[test]
    fn a_listing_after_a_dropped_request_changes_only_that_partys_entry_and_a_choice_ends_once_none_is_left() {
        check(&[passed_then(vec![b()])]).unwrap();
        // q's entry changed: its claim's eligibility.
        let changed = Listed { candidate: Candidate { eligible_at: At(11.0), ..b().candidate }, ..b() };
        rejected(&passed_then(vec![changed]), "after c was dropped, another party's listing changed");
        // p lists a candidate older than the one it dropped.
        let older = listed("p", compile_id("o"), -5.0, Catalogue::Compile { position: 1, index: 0 }, Lacks::Capacity(outside_idle()));
        rejected(&passed_then(vec![older, b()]), "after c was dropped, its party lists no younger candidate");
        // The choice stops after c passed, while q was listed.
        let mut stopped = passed_then(vec![b()]);
        stopped.aged.steps.truncate(1);
        stopped.aged.after = Some(Turn { party: "p".into(), held: false });
        stopped.aged.head = None;
        stopped.admitted = [Entry::Returned { bytes: 0, budget: stopped.left }].into_iter().chain(completions(57.0, stopped.left)).collect();
        stopped.left_after_launch = stopped.left.floor();
        rejected(&stopped, "the choice ended without a head while another party was listed");
    }

    #[test]
    fn a_listed_decision_is_the_one_the_record_shows() {
        let decided = |decisions: &[(Subject, Decisions)]| Record {
            decisions: decisions.iter().map(|(subject, decision)| Decided { subject: subject.clone(), decision: decision.clone() }).collect(),
            ..reserving(6 * GIB, 16 * GIB)
        };
        let head = Subject::Claim { claim: "head".into() };
        let other = Subject::Claim { claim: "other".into() };
        let one = Decisions::One;
        check(&[decided(&[(head.clone(), one(Decision::AgedHead)), (other.clone(), one(Decision::WaitingForTurn))])]).unwrap();
        const DERIVED: &str = "and its derivation says Is(AgedHead)";
        const UNDESCRIBED: &str = "which a request the record does not describe cannot be";
        rejected(&decided(&[(head.clone(), one(Decision::AgedHeadFitting))]), DERIVED);
        rejected(&decided(&[(head.clone(), one(Decision::WaitingForTurn))]), DERIVED);
        rejected(&decided(&[(head.clone(), one(Decision::AgedHead)), (other.clone(), one(Decision::Admitted))]), UNDESCRIBED);
        let group = Subject::Group { group: "g".into() };
        rejected(&decided(&[(head.clone(), one(Decision::AgedHead)), (group, Decisions::Jobs(BTreeMap::from([(Decision::AgedHead, 1)])))]), UNDESCRIBED);
        rejected(&decided(&[(other.clone(), one(Decision::WaitingForTurn)), (other, one(Decision::NotServiceable))]), "is decided twice");
        // The head's first decision is not listed: a reserving head is certainly pending, and its decision changed.
        rejected(&decided(&[]), "Claim { claim: \"head\" }'s derivation says Is(AgedHead), its last listed decision is None");
    }

    #[test]
    fn a_run_ahead_names_its_own_launch_and_the_deadline_d_supports() {
        check(&[running_ahead(&[("a", 57.0, 3, 57.5, 61.0, true)])]).unwrap();
        rejected(&running_ahead(&[("a", 57.0, 3, 57.5, 60.0, true)]), "the record's /admitted/3/ahead/deadline is 60.0, its derivation 61.0");
        let mut other = running_ahead(&[("a", 57.0, 3, 57.5, 61.0, true)]);
        for entry in &mut other.admitted {
            if let Entry::Launched { ahead: Some(ahead), .. } = entry {
                ahead.work = compile("w");
            }
        }
        rejected(&other, "the record's /admitted/3/ahead/work/compile/job is \"w\", its derivation \"a\"");
    }

    /// A tick that admits nothing and holds no head: `bytes` available and left, D empty, and its persistence
    /// sequences completed at 50.
    pub(super) fn quiet(tick: u64, bytes: u64) -> Record {
        let left = Budget::of(bytes);
        Record {
            tick,
            available: bytes,
            left,
            serviceable: bytes,
            admitted: [Entry::Returned { bytes: 0, budget: left }].into_iter().chain(completions(50.0, left)).collect(),
            left_after_launch: bytes,
            ..Record::default()
        }
    }
    /// `record` with the turn `turn` before and after its choice, which judges nothing.
    pub(super) fn turned(record: Record, turn: Turn) -> Record {
        Record { aged: Choice { before: Some(turn.clone()), after: Some(turn), ..record.aged.clone() }, ..record }
    }

    #[test]
    fn a_tick_starts_from_the_turn_and_the_head_the_tick_before_it_left() {
        // A head granted at once is served: its party's turn passes, and the turn persists through silent ticks.
        let granted = fitting_claim_head(3 * GIB, GIB);
        let p = |held| Turn { party: "p".into(), held };
        check(&[granted.clone(), turned(quiet(8, GIB), p(false)), turned(quiet(10, GIB), p(false))]).unwrap();
        let error = check(&[granted.clone(), turned(quiet(8, GIB), p(true))]).unwrap_err();
        assert!(error.contains("tick 8: the record's /aged/after/held is true, its derivation false"), "{error}");
        let error = check(&[granted.clone(), turned(quiet(8, GIB), p(false)), quiet(10, GIB)]).unwrap_err();
        assert!(error.contains("tick 10: the record's /aged/after is null"), "{error}");
        // An instance's first record starts from no turn, whatever its tick.
        let error = check(&[turned(quiet(5, GIB), p(false))]).unwrap_err();
        assert!(error.contains("tick 5: the record's /aged/after is {"), "{error}");
        // A reserving head is judged first in the next tick, unchanged.
        let earlier = reserving(6 * GIB, 16 * GIB);
        check(&[earlier.clone(), kept_after(&earlier)]).unwrap();
        let mut grown = kept_after(&earlier);
        if let Step::Kept { head, .. } = &mut grown.aged.steps[0] {
            head.recorded.insert(compile("x"));
        }
        grown.aged.head.as_mut().unwrap().recorded.insert(compile("x"));
        let error = check(&[earlier.clone(), grown]).unwrap_err();
        assert!(error.contains("tick 8: the record's /aged/head/recorded holds 2 entries, its derivation 1"), "{error}");
        // Not judged at all: the next tick chooses afresh from the head's turn.
        let mut afresh = Record { tick: 8, ..turned(reserving(6 * GIB, 16 * GIB), p(true)) };
        if let Step::Chosen { turn, .. } = &mut afresh.aged.steps[0] {
            *turn = Some(p(true));
        }
        let error = check(&[earlier, afresh]).unwrap_err();
        assert!(error.contains("tick 8: the head the tick before left, head, is not judged first"), "{error}");
        // A head judged that no tick before held.
        let error = check(&[kept_after(&reserving(6 * GIB, 16 * GIB))]).unwrap_err();
        assert!(error.contains("tick 8: the record judges a head the tick before left, and that tick left none"), "{error}");
    }

    #[test]
    fn a_compile_job_is_listed_only_while_it_waits_for_what_the_last_launches_left() {
        // 8 GiB left; the 5 GiB job waits for memory only while the tick before left less than 5 GiB.
        check(&[quiet(6, 4 * GIB), chosen_compile_head()]).unwrap();
        const FITS: &str = "compile job head is listed, and it does not wait for memory";
        let error = check(&[quiet(6, 8 * GIB), chosen_compile_head()]).unwrap_err();
        assert!(error.contains(FITS), "{error}");
        // An instance's first tick has no launches before it: it lists no compile job, and starts with no turn.
        let error = check(&[Record { tick: 1, ..chosen_compile_head() }]).unwrap_err();
        assert!(error.contains(FITS), "{error}");
        check(&[Record { tick: 1, ..reserving(6 * GIB, 16 * GIB) }]).unwrap();
        let error = check(&[turned(quiet(1, GIB), Turn { party: "p".into(), held: true })]).unwrap_err();
        assert!(error.contains("tick 1: the record's /aged/after is {\"held\":true,\"party\":\"p\"}, its derivation null"), "{error}");
        // After silent ticks what the last launches left is unknown, at least nothing: a job waits beyond the
        // smaller of left and nothing.
        check(&[quiet(5, 8 * GIB), chosen_compile_head()]).unwrap();
    }

    #[test]
    fn a_floored_budget_and_a_wrong_turn_are_rejected() {
        let record = reserving(6 * GIB, 16 * GIB);
        let floored = Record { left: Budget::of(3 * GIB), ..record.clone() };
        rejected(&floored, "the record's /left is 3221225472, its derivation 2147483648");
        let mut wrong = record.clone();
        wrong.aged.after = Some(Turn { party: "p".into(), held: false });
        rejected(&wrong, "the record's /aged/after/held is false, its derivation true");
        let mut gap = record.clone();
        gap.tick = 9;
        assert!(check(&[record, gap]).unwrap_err().contains("next record is tick 9"), "a reserving head's ticks are consecutive");
    }

    /// One launch running ahead into the head's bytes: (job, admitted_at, declared, settled_at, deadline, running).
    type Launch<'a> = (&'a str, f64, u64, f64, f64, bool);

    /// `reserving`'s tick of a 6 GiB head, its pre-launch sequence completed at 57, then 1 GiB launches running
    /// ahead into the head's bytes, and its closes completed once the last launch settled. D's compile w is declared
    /// to finish at 61.
    fn running_ahead(launches: &[Launch]) -> Record {
        let mut record = reserving(6 * GIB, 16 * GIB);
        let memory = record.left.less(6 * GIB);
        record.admitted.truncate(completion(&record, Persisting::Close));
        for (index, (job, admitted_at, declared, settled_at, deadline, running)) in launches.iter().enumerate() {
            record.admitted.push(Entry::Launched {
                phase: Launching::Launch,
                group: job.to_string(),
                job: job.to_string(),
                class: Class::Ordinary,
                bytes: GIB,
                budget: memory,
                reserved: 6 * GIB - index as u64 * GIB,
                head: false,
                ahead: Some(Ahead { deadline: *deadline, work: Work::compile(job, f64::from(u8::from(*running))) }),
                admitted_at: *admitted_at,
                declared: Some(*declared),
                settled_at: *settled_at,
                running: *running,
            });
        }
        let closed = launches.iter().map(|launch| launch.3).fold(57.0, f64::max);
        let closes = [Persisting::Close, Persisting::LargeClose];
        record.admitted.extend(closes.map(|phase| Entry::Persisted { phase, at: closed, budget: memory }));
        record.slots = launches.len();
        first(record, &[(claim("head"), Decision::AgedHead)])
    }
    /// Where `record` holds `phase`'s persistence completion.
    fn completion(record: &Record, phase: Persisting) -> usize {
        record
            .admitted
            .iter()
            .position(|entry| matches!(entry, Entry::Persisted { phase: completed, .. } if *completed == phase))
            .unwrap()
    }
    /// Checks that `record`, its instance's only one, is rejected by `rule`.
    fn rejected(record: &Record, rule: &str) {
        let error = check(&[record.clone()]).unwrap_err();
        assert!(error.contains(rule), "expected {rule:?}, got {error}");
    }
    const BEYOND: &str = "a of 1073741824 bytes launched beyond its budget";
    /// A time that is not finite: JSON has no such number, so the service writes null, and the record is refused as
    /// it is read.
    const UNWRITABLE: &str = "unreadable admission record: invalid type: null, expected f64";

    #[test]
    fn a_run_ahead_is_checked_against_the_deadline_the_record_derives_at_a_fresh_admission() {
        check(&[running_ahead(&[("a", 57.0, 4, 57.5, 61.0, true)])]).unwrap();
        check(&[running_ahead(&[("a", 57.0, 4, 61.0, 61.0, true), ("b", 61.0, 0, 61.5, 61.0, true)])]).unwrap();
        // Admitted at 58 and declared for 4 s, it would finish at 62, after the deadline: it may not take from the
        // reservation, and nothing else is left.
        rejected(&running_ahead(&[("a", 58.0, 4, 58.5, 61.0, true)]), BEYOND);
        rejected(
            &running_ahead(&[("a", 57.0, 4, 59.0, 61.0, true), ("b", 58.0, 2, 58.5, 61.0, true)]),
            "before the persistence boundary at 59",
        );
        rejected(&running_ahead(&[("a", 57.0, 4, 57.5, 70.0, true)]), "the record's /admitted/3/ahead/deadline is 70.0, its derivation 61.0");
        let mut crowded = running_ahead(&[("a", 57.0, 1, 57.5, 61.0, true), ("b", 57.5, 1, 58.0, 61.0, true)]);
        crowded.slots = 1;
        rejected(&crowded, "2 launches against a slot count of 1");
    }

    #[test]
    fn a_stale_first_admission_a_backwards_settlement_and_a_run_ahead_without_a_contracted_compile_are_rejected() {
        rejected(&running_ahead(&[("a", 55.0, 4, 55.5, 61.0, true)]), "before the persistence boundary at 57");
        rejected(&running_ahead(&[("a", 57.0, 4, 56.5, 61.0, true)]), "was admitted at 57 and settled at 56.5");
        let mut unfinished = running_ahead(&[("a", 57.0, 4, 57.5, 61.0, true)]);
        let close = completion(&unfinished, Persisting::Close);
        unfinished.admitted[close] = Entry::Persisted { phase: Persisting::Close, at: 57.2, budget: unfinished.left.less(6 * GIB) };
        rejected(&unfinished, "completed at 57.2, before the boundary at 57.5");
        let mut claims_only = running_ahead(&[("a", 57.0, 4, 57.5, 61.0, true)]);
        claims_only.own_work = vec![Counted::Claim { grant: grant("held"), bytes: 4 * GIB }];
        recount(&mut claims_only);
        rejected(&claims_only, BEYOND);
        let mut infinite = running_ahead(&[("a", 57.0, 4, f64::INFINITY, 61.0, true)]);
        rejected(&infinite, UNWRITABLE);
        infinite.own_work = vec![counted_w(f64::NAN)];
        rejected(&infinite, UNWRITABLE);
    }

    fn grant(claim: &str) -> Grant {
        Grant { claim: claim.into(), order: 1 }
    }
    /// After a test changed D, the head chosen in `record` records the work D now counts, as a choice records it.
    fn recount(record: &mut Record) {
        let live = record.live();
        record.aged.head.iter_mut().for_each(|head| head.recorded = live.clone());
    }
    /// `record` as its journal line, with `edit` applied to its JSON.
    fn journal_line(record: &Record, edit: impl FnOnce(&mut Value)) -> String {
        let mut value = serde_json::to_value(record).unwrap();
        edit(&mut value);
        format!("{PREFIX}{value}\n")
    }
    /// What reading `line` and checking its records refused; a panic if both accept it.
    fn refused(line: &str) -> String {
        parse(line).and_then(|records| derive_journal(&records).map(|_| ())).unwrap_err()
    }

    #[test]
    fn only_a_counted_compile_declares_a_finish_and_the_deadline_comes_from_ds_compiles_alone() {
        // Test 25's claims-only record: a claim carrying a finish is refused as the journal is read.
        let mut claims_only = running_ahead(&[("a", 57.0, 4, 57.5, 61.0, true)]);
        claims_only.own_work = vec![Counted::Claim { grant: grant("held"), bytes: 4 * GIB }];
        parse(&journal_line(&claims_only, |_| ())).unwrap();
        let carrying = journal_line(&claims_only, |record| record["own_work"][0]["claim"]["finish"] = Value::from(61.0));
        assert!(refused(&carrying).contains("unknown field `finish`"), "{}", refused(&carrying));
        // A counted compile without a finish is refused likewise, a null one and an omitted one. Omitted from a
        // reserving record that launches nothing, no other rule would refuse a finish the reader supplied itself.
        let running = running_ahead(&[("a", 57.0, 4, 57.5, 61.0, true)]);
        let unfinished = journal_line(&running, |record| record["own_work"][0]["compile"]["finish"] = Value::Null);
        assert!(refused(&unfinished).contains("unreadable admission record"));
        let launching_nothing = reserving(6 * GIB, 16 * GIB);
        derive_journal(&parse(&journal_line(&launching_nothing, |_| ())).unwrap()).unwrap();
        let omitted = journal_line(&launching_nothing, |record| {
            record["own_work"][0]["compile"].as_object_mut().unwrap().remove("finish");
        });
        let error = refused(&omitted);
        assert!(error.contains("missing field `finish`"), "{error}");
        // A claim cannot extend a real compile's deadline: beside w, declared to finish at 61, a host-only claim adds
        // no term, so a run-ahead finishing at 66 against a recorded deadline of 70 is rejected, and a claim carrying
        // a finish of 70 is refused as read.
        let mut extended = running_ahead(&[("a", 57.0, 9, 57.5, 70.0, true)]);
        extended.own_work.push(Counted::Claim { grant: grant("held"), bytes: 0 });
        recount(&mut extended);
        rejected(&extended, BEYOND);
        let extending = journal_line(&extended, |record| record["own_work"][1]["claim"]["finish"] = Value::from(70.0));
        assert!(refused(&extending).contains("unknown field `finish`"), "{extending}");
    }

    #[test]
    fn a_record_missing_duplicating_or_misplacing_a_persistence_completion_is_rejected() {
        let launched = running_ahead(&[("a", 57.0, 4, 57.5, 61.0, true)]);
        check(&[launched.clone()]).unwrap();
        // Without the pre-launch completion at 57, nothing would bound an admission read at 55.
        let mut stale = running_ahead(&[("a", 55.0, 4, 55.5, 61.0, true)]);
        stale.admitted.remove(completion(&stale, Persisting::Serve));
        rejected(&stale, "the Serve phase records 0 completions, not one");
        for phase in Persisting::all() {
            let mut missing = launched.clone();
            missing.admitted.remove(completion(&missing, phase));
            rejected(&missing, &format!("the {phase:?} phase records 0 completions, not one"));
            let mut duplicated = launched.clone();
            let at = completion(&duplicated, phase);
            duplicated.admitted.insert(at, duplicated.admitted[at].clone());
            rejected(&duplicated, &format!("the {phase:?} phase records 2 completions, not one"));
        }
        let mut early = launched.clone();
        let serve = early.admitted.remove(completion(&early, Persisting::Serve));
        early.admitted.insert(0, serve);
        rejected(&early, "the Serve phase's completion is not its last entry");
        let mut late = launched.clone();
        let serve = late.admitted.remove(completion(&late, Persisting::Serve));
        late.admitted.insert(completion(&late, Persisting::Close), serve);
        rejected(&late, "out of phase order");
        let mut granted_after = launched.clone();
        let budget = launched.left.less(6 * GIB);
        let close = Entry::Granted { phase: Persisting::Close, claim: "c".into(), party: "q".into(), class: Class::Ordinary, bytes: 0, budget, head: false };
        granted_after.admitted.insert(completion(&granted_after, Persisting::Close) + 1, close);
        rejected(&granted_after, "the Close phase's completion is not its last entry");
    }

    #[test]
    fn a_persistence_sequence_completed_at_a_time_that_is_not_finite_is_rejected() {
        let launched = running_ahead(&[("a", 57.0, 4, 57.5, 61.0, true)]);
        let mut unending = launched.clone();
        let at = completion(&unending, Persisting::LargeClose);
        unending.admitted[at] = Entry::Persisted { phase: Persisting::LargeClose, at: f64::INFINITY, budget: launched.left.less(6 * GIB) };
        rejected(&unending, UNWRITABLE);
    }

    /// A headless tick: twice `bytes` available and no deductions, D empty, one slot, nothing reserved and no turn
    /// kept; its pre-launch sequence completes at 55, with nothing granted.
    fn headless(bytes: u64) -> Record {
        let left = Budget::of(2 * bytes);
        Record {
            tick: 1,
            available: 2 * bytes,
            left,
            serviceable: 2 * bytes,
            slots: 1,
            admitted: vec![
                Entry::Returned { bytes: 0, budget: left },
                Entry::Persisted { phase: Persisting::Serve, at: 55.0, budget: left },
            ],
            ..Record::default()
        }
    }
    /// A `bytes` compile launched in `phase` from twice `bytes`, declared for 4 s, admitted at 55 and settled at 55.5.
    fn launch(phase: Launching, bytes: u64) -> Entry {
        Entry::Launched {
            phase,
            group: "g".into(),
            job: "j".into(),
            class: Class::of(bytes),
            bytes,
            budget: Budget::of(2 * bytes),
            reserved: 0,
            head: false,
            ahead: None,
            admitted_at: 55.0,
            declared: Some(4),
            settled_at: 55.5,
            running: true,
        }
    }
    /// A `bytes` claim granted in `phase` from `budget`.
    fn granted(phase: Persisting, bytes: u64, budget: Budget) -> Entry {
        Entry::Granted { phase, claim: "c".into(), party: "q".into(), class: Class::of(bytes), bytes, budget, head: false }
    }
    /// Review round 2's tick, valid: `headless`'s for 1 GiB, a 1 GiB launch after the pre-launch completion, both
    /// closes completed at 57 with 1 GiB left.
    pub(super) fn launched_after_serve() -> Record {
        let mut record = headless(GIB);
        let closed = |phase| Entry::Persisted { phase, at: 57.0, budget: Budget::of(GIB) };
        record.admitted.extend([launch(Launching::Launch, GIB), closed(Persisting::Close), closed(Persisting::LargeClose)]);
        record.left_after_launch = GIB;
        first(record, &[])
    }
    /// A valid large tick: `headless`'s for 12 GiB, the ordinary close completed at 55 with all 24 GiB left, a
    /// 12 GiB large launch, an 11 GiB large claim granted from the 12 GiB it left, and the large close completed
    /// at 57 with 1 GiB left.
    fn launched_large() -> Record {
        let mut record = headless(12 * GIB);
        record.admitted.extend([
            Entry::Persisted { phase: Persisting::Close, at: 55.0, budget: Budget::of(24 * GIB) },
            launch(Launching::LargeLaunch, 12 * GIB),
            granted(Persisting::LargeClose, 11 * GIB, Budget::of(12 * GIB)),
            Entry::Persisted { phase: Persisting::LargeClose, at: 57.0, budget: Budget::of(GIB) },
        ]);
        record.left_after_launch = 24 * GIB;
        first(record, &[])
    }
    /// `record`'s journal line with its admitted entry `index` naming `phase`.
    fn naming(record: &Record, index: usize, phase: &str) -> String {
        journal_line(record, |value| value["admitted"][index]["phase"] = Value::from(phase))
    }
    /// Checks that `line` is refused as read, for naming `phase` where its event cannot occur, among `permitted`.
    fn refused_for_its_phase(line: &str, phase: &str, permitted: &str) {
        let error = refused(line);
        assert!(error.contains(&format!("unknown variant `{phase}`, expected {permitted}")), "{error}");
    }
    const LAUNCHING: &str = "`launch` or `large_launch`";
    const PERSISTING: &str = "one of `serve`, `close`, `large_close`";

    #[test]
    fn a_launch_names_a_launch_phase_alone() {
        for valid in [launched_after_serve(), launched_large()] {
            derive_journal(&parse(&journal_line(&valid, |_| ())).unwrap()).unwrap();
        }
        // Review round 2's record: the launch in Serve, ahead of the pre-launch completion, now at 57 with 1 GiB
        // left. Its class is ordinary, Serve's completion is still the phase's last entry, and the budget, slots
        // and boundaries agree: only its phase refuses it.
        let mut serving = launched_after_serve();
        let launched = serving.admitted.remove(2);
        serving.admitted[1] = Entry::Persisted { phase: Persisting::Serve, at: 57.0, budget: Budget::of(GIB) };
        serving.admitted.insert(1, launched);
        refused_for_its_phase(&naming(&serving, 1, "serve"), "serve", LAUNCHING);
        // In the ordinary close the closes' budget is left before the launch.
        let closing = Record { left_after_launch: 2 * GIB, ..launched_after_serve() };
        refused_for_its_phase(&naming(&closing, 2, "close"), "close", LAUNCHING);
        refused_for_its_phase(&naming(&launched_large(), 3, "large_close"), "large_close", LAUNCHING);
    }

    #[test]
    fn a_grant_names_a_phase_that_persists_alone() {
        let mut granting = launched_after_serve();
        granting.admitted.insert(3, granted(Persisting::Close, 0, Budget::of(GIB)));
        let granting = first(granting, &[]);
        derive_journal(&parse(&journal_line(&granting, |_| ())).unwrap()).unwrap();
        refused_for_its_phase(&naming(&granting, 3, "launch"), "launch", PERSISTING);
        refused_for_its_phase(&naming(&launched_large(), 4, "large_launch"), "large_launch", PERSISTING);
    }

    #[test]
    fn a_completion_names_a_phase_that_persists_alone() {
        // An extra completion after each launch: each persisting phase still records one, last.
        let mut ordinary = launched_after_serve();
        ordinary.admitted.insert(3, Entry::Persisted { phase: Persisting::Close, at: 55.5, budget: Budget::of(GIB) });
        refused_for_its_phase(&naming(&ordinary, 3, "launch"), "launch", PERSISTING);
        let mut large = launched_large();
        large.admitted.insert(4, Entry::Persisted { phase: Persisting::LargeClose, at: 55.5, budget: Budget::of(12 * GIB) });
        refused_for_its_phase(&naming(&large, 4, "large_launch"), "large_launch", PERSISTING);
    }

    /// The next tick of `earlier`, in which its head is kept, reserving, waiting for the work it recorded alone.
    fn kept_after(earlier: &Record) -> Record {
        let head = earlier.aged.head.clone().unwrap();
        Record {
            tick: earlier.tick + 1,
            decisions: Vec::new(),
            aged: Choice {
                before: earlier.aged.after.clone(),
                steps: vec![Step::Kept {
                    head: head.clone(),
                    standing: Standing::of(true, head.bytes, Lacks::Nothing),
                    verdict: Verdict::Kept { fits: false },
                }],
                after: earlier.aged.after.clone(),
                head: Some(head),
            },
            ..reserving(6 * GIB, 16 * GIB)
        }
    }

    #[test]
    fn a_failed_spawn_that_ran_ahead_creates_no_work_the_next_kept_head_waits_for() {
        let failed = running_ahead(&[("a", 57.0, 4, 57.5, 61.0, false)]);
        check(&[failed.clone(), kept_after(&failed)]).unwrap();
        let ran = running_ahead(&[("a", 57.0, 4, 57.5, 61.0, true)]);
        let error = check(&[ran.clone(), kept_after(&ran)]).unwrap_err();
        assert!(error.contains("tick 8: the record's /aged/head/recorded holds 1 entries, its derivation 2"), "{error}");
    }

    /// Another engine instance: process `pid` of boot b.
    pub(super) fn other(pid: u32) -> Instance {
        Instance { boot_id: "b".into(), pid, start_ticks: u64::from(pid) }
    }
    /// Review round 5's B8: instance `instance`'s tick 8 after seven silent ticks, 8 GiB available and left, no D,
    /// and an unaged 1 GiB claim b8 of party r granted from it in the share pass.
    pub(super) fn b8(instance: Instance) -> Record {
        let left = Budget::of(8 * GIB);
        let grant = Entry::Granted {
            phase: Persisting::Serve,
            claim: "b8".into(),
            party: "r".into(),
            class: Class::Ordinary,
            bytes: GIB,
            budget: left,
            head: false,
        };
        let rest = left.less(GIB);
        let record = Record {
            instance,
            tick: 8,
            available: 8 * GIB,
            left,
            serviceable: 8 * GIB,
            admitted: [grant, Entry::Returned { bytes: 0, budget: rest }].into_iter().chain(completions(57.0, rest)).collect(),
            left_after_launch: 7 * GIB,
            ..Record::default()
        };
        first(record, &[])
    }

    #[test]
    fn records_carry_state_only_within_their_engine_instance() {
        // Review round 5's finding 1: A's tick 7 grants its head and passes p's turn; B's first record is its tick 8.
        let a7 = fitting_claim_head(3 * GIB, GIB);
        check(&[b8(other(2))]).unwrap();
        check(&[a7.clone(), b8(other(2))]).unwrap();
        check(&[b8(other(2)), a7.clone()]).unwrap();
        // One instance's ticks 7 and 8 do carry the turn: B8 would start from p's passed turn.
        let error = check(&[a7.clone(), b8(a7.instance.clone())]).unwrap_err();
        assert!(error.contains("tick 8: the record's /aged/after is null, its derivation {\"held\":false,\"party\":\"p\"}"), "{error}");
        // A restart after a reserving tick: the successor's first record is its own tick 1, and starts empty.
        let reserving_7 = reserving(6 * GIB, 16 * GIB);
        let successor = Record { instance: other(3), tick: 1, ..quiet(1, GIB) };
        check(&[reserving_7.clone(), successor.clone()]).unwrap();
        let error = check(&[reserving_7, Record { instance: Instance::default(), ..successor }]).unwrap_err();
        assert!(error.contains("tick 1 follows tick 7 of its instance"), "{error}");
        // Within an instance a reserving tick is followed by the next, wherever another instance's records fall.
        let earlier = reserving(6 * GIB, 16 * GIB);
        check(&[earlier.clone(), b8(other(2)), kept_after(&earlier)]).unwrap();
    }

    #[test]
    fn the_turn_persists_through_silent_ticks() {
        // Review round 5's finding 2: A7 grants its head, A8 records p's passed turn, tick 9 is silent.
        let a7 = fitting_claim_head(3 * GIB, GIB);
        let turn = |party: &str| Turn { party: party.into(), held: false };
        let a8 = turned(quiet(8, GIB), turn("p"));
        check(&[a7.clone(), a8.clone(), turned(quiet(10, GIB), turn("p"))]).unwrap();
        let error = check(&[a7, a8, turned(quiet(10, GIB), turn("q"))]).unwrap_err();
        assert!(error.contains("tick 10: the record's /aged/after/party is \"q\", its derivation \"p\""), "{error}");
    }

    /// `fitting_claim_head`'s tick with q's younger claim listed beside p's head, declaring `declared` bytes; its
    /// grant of 1 GiB follows the head's.
    fn younger_listed(declared: u64) -> Record {
        let mut record = fitting_claim_head(3 * GIB, GIB);
        if let Step::Chosen { listed, .. } = &mut record.aged.steps[0] {
            let candidate = Candidate { id: claim("younger"), party: "q".into(), eligible_at: At(1.0), catalogue: Catalogue::Claim { order: 2 }, bytes: declared, lacks: Lacks::Nothing };
            listed.push(Listed { party: "q".into(), candidate });
        }
        record
    }

    #[test]
    fn a_request_is_charged_its_one_declaration_wherever_the_record_names_it() {
        // Review round 5's finding 3: the younger claim listed with its 3 GiB declaration and granted 1 GiB.
        check(&[younger_listed(GIB)]).unwrap();
        rejected(&younger_listed(3 * GIB), "younger's declaration is 3221225472 and 1073741824 in one record");
        // A listed compile job launched likewise: its launch's cell is its declaration.
        check(&[listed_launch(3 * GIB)]).unwrap();
        rejected(&listed_launch(2 * GIB), "c's declaration is 3221225472 and 2147483648 in one record");
    }

    /// 4 GiB left beside D's 4 GiB compile w, S 8 GiB: p's 3 GiB compile job g/c and q's older 5 GiB claim h are
    /// listed, h is chosen and reserves, and c runs ahead into its bytes with a launch of `launched` bytes.
    pub(super) fn listed_launch(launched: u64) -> Record {
        let capacity = Capacity { idle: 3, ..outside_idle() };
        let mut record = reserving(5 * GIB, 18 * GIB);
        let c = listed("p", compile_id("c"), 0.0, Catalogue::Compile { position: 0, index: 0 }, Lacks::Capacity(capacity.clone()));
        let h = Listed { party: "q".into(), candidate: Candidate { id: claim("h"), party: "q".into(), eligible_at: At(-10.0), catalogue: Catalogue::Claim { order: 1 }, bytes: 5 * GIB, lacks: Lacks::Nothing } };
        let c = Listed { candidate: Candidate { bytes: 3 * GIB, ..c.candidate }, ..c };
        record.aged = Choice {
            before: None,
            steps: vec![Step::Chosen { turn: None, listed: vec![c, h], chosen: claim("h"), verdict: Verdict::Kept { fits: false } }],
            after: Some(Turn { party: "q".into(), held: true }),
            head: Some(Head { id: claim("h"), party: "q".into(), bytes: 5 * GIB, fits: false, recorded: record.live() }),
        };
        let memory = record.left.less(5 * GIB);
        record.admitted = served(claim("h"), 5 * GIB, record.left);
        record.admitted.truncate(completion(&record, Persisting::Close));
        record.admitted.push(Entry::Launched {
            phase: Launching::Launch,
            group: "g".into(),
            job: "c".into(),
            class: Class::Ordinary,
            bytes: launched,
            budget: memory,
            reserved: 5 * GIB,
            head: false,
            ahead: Some(Ahead { deadline: 61.0, work: compile("c") }),
            admitted_at: 57.0,
            declared: Some(3),
            settled_at: 57.5,
            running: true,
        });
        let closes = [Persisting::Close, Persisting::LargeClose];
        record.admitted.extend(closes.map(|phase| Entry::Persisted { phase, at: 57.5, budget: memory }));
        record.slots = slots(&capacity);
        first(record, &[(claim("h"), Decision::AgedHead)])
    }

    #[test]
    fn the_capacitys_contracted_compiles_are_ds_compiles() {
        // Review round 5's finding 4: every core held by a running compile, none idle; the head is kept while the
        // capacity claims one of the three running compiles is contracted.
        let full = Capacity { held: 3, idle: 0, running: Units { running: 3, contracted: 1 }, ..outside_idle() };
        check(&kept_compile_journal(full.clone())).unwrap();
        // With D counting no compile, the capacity's contracted compiles are D's, none, whatever the record counts:
        // the head lacks a slot no contracted compile holds, and the verdict that keeps it is refused.
        let mut journal = kept_compile_journal(full.clone());
        journal[1].own_work.clear();
        journal[1].serviceable = journal[1].left.floor();
        let error = check(&journal).unwrap_err();
        assert!(error.contains("tick 8: the head head was Kept { fits: true }, its figures say Passed"), "{error}");
        // Counted as D counts them, none, the head lacks a slot no contracted compile holds: it must pass.
        let none = Capacity { running: Units { running: 3, contracted: 0 }, ..full };
        journal[1] = Record { aged: kept_compile_head(none).aged, ..journal[1].clone() };
        let error = check(&journal).unwrap_err();
        assert!(error.contains("its figures say Passed"), "{error}");
    }

    /// A 0-byte compile head g/zero, which an engine configured with no average cell gives a job declaring none:
    /// 1 GiB short beside D's w, S 3 GiB, chosen at tick 7. Not fitting, it reserves its 0 bytes.
    pub(super) fn zero_head() -> Record {
        let id = compile_id("zero");
        let capacity = Capacity { idle: 3, ..outside_idle() };
        let mut record = reserving(0, 13 * GIB);
        if let Step::Chosen { listed, chosen, .. } = &mut record.aged.steps[0] {
            listed[0].candidate = Candidate {
                id: id.clone(),
                catalogue: Catalogue::Compile { position: 0, index: 0 },
                lacks: Lacks::Capacity(capacity.clone()),
                ..listed[0].candidate.clone()
            };
            *chosen = id.clone();
        }
        record.aged.head = Some(Head { id: id.clone(), ..record.aged.head.clone().unwrap() });
        record.admitted = served(id.clone(), 0, record.left);
        record.slots = slots(&capacity);
        first(record, &[(id, Decision::AgedHead)])
    }
    /// `zero_head`'s head kept at `tick`, the tick before having left it fitting or not, `fits`.
    pub(super) fn zero_kept(earlier: &Record, tick: u64, fits: bool) -> Record {
        let head = earlier.aged.head.clone().unwrap();
        let mut kept = Record { tick, decisions: Vec::new(), ..earlier.clone() };
        let capacity = Capacity { idle: 3, ..outside_idle() };
        kept.aged = Choice {
            before: earlier.aged.after.clone(),
            steps: vec![Step::Kept {
                head: Head { fits, ..head.clone() },
                standing: Standing::of(true, 0, Lacks::Capacity(capacity)),
                verdict: Verdict::Kept { fits: false },
            }],
            after: earlier.aged.after.clone(),
            head: Some(head),
        };
        kept
    }

    #[test]
    fn a_held_heads_fit_is_unknown_after_silent_ticks_and_known_otherwise() {
        let chosen = zero_head();
        assert_eq!(chosen.aged_reserved, 0, "it reserves nothing, so the next tick need not write");
        // Kept in the next tick, it is the head the tick before left, fitting or not as it left it.
        check(&[chosen.clone(), zero_kept(&chosen, 8, false)]).unwrap();
        let error = check(&[chosen.clone(), zero_kept(&chosen, 8, true)]).unwrap_err();
        assert!(error.contains("tick 8: the record's /aged/steps/0/head/fits is true, its derivation false"), "{error}");
        // After the silent tick 8, whose verdict set it, either is the head the tick before left.
        for fits in [false, true] {
            check(&[chosen.clone(), zero_kept(&chosen, 9, fits)]).unwrap();
        }
        let written: Vec<Value> = [chosen.clone(), zero_kept(&chosen, 9, true)].iter().map(|record| serde_json::to_value(record).unwrap()).collect();
        let derived = derive_journal(&written).unwrap()[1].line().unwrap();
        assert_eq!(derived["aged"]["steps"][0]["head"]["fits"]["unknown"], "a head reserving 0 bytes is kept through silent ticks, whose verdicts set whether it fits");
        // Its other facts persist through the silent tick.
        let mut other = zero_kept(&chosen, 9, true);
        if let Step::Kept { head, .. } = &mut other.aged.steps[0] {
            head.party = "q".into();
        }
        let error = check(&[chosen, other]).unwrap_err();
        assert!(error.contains("tick 9: the record's /aged/steps/0/head/party is \"q\", its derivation \"p\""), "{error}");
    }

    #[test]
    fn an_admitted_decision_holds_for_the_next_tick_alone() {
        // A job admitted again in the next tick, as after its attempt outgrew its cell, keeps its Admitted unlisted.
        let first_launch = launched_after_serve();
        let again = Record { tick: 2, decisions: Vec::new(), ..first_launch.clone() };
        check(&[first_launch.clone(), again.clone()]).unwrap();
        // After a silent tick its subject left the pending set: its admission is a changed decision again.
        let error = check(&[first_launch, Record { tick: 3, ..again }]).unwrap_err();
        assert!(error.contains("tick 3: Job { group: \"g\", job: \"j\" }'s derivation says Is(Admitted), its last listed decision is None"), "{error}");
    }

    #[test]
    fn a_listed_requests_decision_is_the_one_its_listing_gives() {
        // Review round 5's finding 6: q's 1 GiB claim w listed behind p's chosen head, serviceable and not granted.
        let with_w = |decision: Decision| {
            let mut record = fitting_claim_head(3 * GIB, GIB);
            if let Step::Chosen { listed, .. } = &mut record.aged.steps[0] {
                let candidate = Candidate { id: claim("w"), party: "q".into(), eligible_at: At(1.0), catalogue: Catalogue::Claim { order: 3 }, bytes: GIB, lacks: Lacks::Nothing };
                listed.push(Listed { party: "q".into(), candidate });
            }
            first(record, &[(claim("w"), decision)])
        };
        check(&[with_w(Decision::WaitingForTurn)]).unwrap();
        for decision in [Decision::LargeWaiting, Decision::NotServiceable, Decision::GivingWay] {
            let error = check(&[with_w(decision)]).unwrap_err();
            assert!(error.contains(&format!("is One({decision:?}), and its derivation says Is(WaitingForTurn)")), "{error}");
        }
    }
}
