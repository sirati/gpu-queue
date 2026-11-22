//! Precedence between pending RAM requests. A request's class decides the pass that serves it: large requests,
//! above `LARGE_REQUEST`, only from what every smaller request of the tick left. Among ordinary requests the
//! share passes keep passing over, one holds the aged head: its bytes are reserved against every later
//! admission while the service's own work can free them. The turn to hold it rotates over the parties with a
//! candidate, in cyclic name order. Everything here lives in memory and is decided afresh each tick.
use super::{
    budget::Budget,
    contract::LARGE_REQUEST,
    decision::{Form, Gpu, Projected, Written},
    fairness,
    model::Purpose,
};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

/// How long a request waits, from its eligibility, before it may hold the aged head.
pub const AGED_SECS: f64 = 30.0;

/// Whether a request eligible since `eligible_at` has waited long enough to hold the head.
pub fn aged(eligible_at: f64, now: f64) -> bool {
    now - eligible_at >= AGED_SECS
}

/// The pass that serves a RAM request: claims and compile jobs alike.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    /// A claim held for work next on the GPU: served first, in request order, by the GPU pass. No claim is of this
    /// class while nothing ties a claim to a told GPU place (`Class::of`).
    Gpu,
    Ordinary,
    /// More than `LARGE_REQUEST`: served last, never reserved for.
    Large,
}

impl Class {
    /// The one place a request's class is decided, by its bytes and a claim's purpose. For a compile job, which
    /// has none, the bytes are its next attempt's cell. A `gpu_place` claim is ordinary: nothing ties it to a told
    /// GPU place, so it is not known to be for work next on the GPU.
    pub fn of(bytes: u64, purpose: Option<Purpose>) -> Self {
        match (bytes > LARGE_REQUEST, purpose) {
            (true, _) => Self::Large,
            (false, Some(Purpose::GpuPlace | Purpose::HostOnly) | None) => Self::Ordinary,
        }
    }

    /// Why a request of this class waits beyond its memory.
    pub fn waiting(self) -> Option<&'static str> {
        match self {
            Self::Gpu | Self::Ordinary => None,
            Self::Large => Some(
                "declares more than 10 GB: served only from memory no smaller request needs; never reserves memory",
            ),
        }
    }
}

/// A moment in Unix seconds, totally ordered.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct At(pub f64);
impl PartialEq for At {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for At {}
impl Ord for At {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}
impl PartialOrd for At {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A pending RAM request: a compile job or a claim.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestId {
    Compile { group: String, job: String },
    Claim(Registration),
}
impl RequestId {
    /// Whether this is the compile job `job`; job identities are unique across groups.
    pub fn is_job(&self, job: &str) -> bool {
        matches!(self, Self::Compile { job: own, .. } if own == job)
    }
}
impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Compile { job, .. } => f.write_str(job),
            Self::Claim(registration) => f.write_str(&registration.claim),
        }
    }
}

/// Work the service admitted that ends: a running compile with a runtime contract, or a granted claim held only
/// for host work.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Work {
    Compile(Attempt),
    Claim(Registration),
}
impl Work {
    /// A compile attempt, by its job and its start.
    pub fn compile(job: &str, started_at: f64) -> Self {
        Self::Compile(Attempt {
            job: job.into(),
            started_at: At(started_at),
        })
    }
}

/// A compile attempt, by its job and its start.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Attempt {
    pub job: String,
    pub started_at: At,
}

/// A claim's registration: its ID and its registration order. A claim ID registered again is a new registration.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Registration {
    pub claim: String,
    pub order: u64,
}

/// A request's place in the catalogue, which breaks ties between equal eligibility times: compile jobs by their
/// group's place in submission order and their place in the group, claims by registration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Catalogue {
    Compile { position: usize, index: usize },
    Claim { order: u64 },
}

/// Running compiles holding units of a limit, and how many of them have a runtime contract.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Units<T = usize> {
    pub running: usize,
    pub contracted: T,
}

/// What a compile job lacks to start besides memory, as the fitting-head rule reads it: cores, its group's limit
/// and its party's core share, each with how many of the running compiles holding it have a runtime contract. All
/// running compiles' contracted ones are D's compiles, and the core cap is the one the eligible cores give: both are
/// figures.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Capacity<F: Form = Written> {
    /// Cores a compile may use: all but one.
    pub eligible: usize,
    /// Cores running compiles hold.
    pub held: usize,
    /// The host's idle cores.
    pub idle: usize,
    pub running: Units<F::Figure<usize>>,
    pub group: Units,
    pub group_limit: usize,
    pub party: Units,
    pub core_cap: F::Figure<usize>,
}
impl Capacity<Projected> {
    /// The capacity its raw counts give, with `contracted` the running compiles that have a runtime contract, D's
    /// compiles: the core cap is the eligible cores' (`fairness::core_limit`).
    pub fn derived(&self, contracted: usize) -> Capacity {
        Capacity {
            eligible: self.eligible,
            held: self.held,
            idle: self.idle,
            running: Units {
                running: self.running.running,
                contracted,
            },
            group: self.group,
            group_limit: self.group_limit,
            party: self.party,
            core_cap: fairness::core_limit(self.eligible),
        }
    }
}
impl Capacity {
    /// A fitting compile head keeps its reservation only while compiles that end hold each thing it lacks: a
    /// free slot, a unit of its group's limit, a unit of its party's core share. Idle cores taken by work outside
    /// the service, or units held by a compile without a runtime contract, never keep it.
    pub fn held_by_contracted(&self) -> bool {
        let free = self.eligible.saturating_sub(self.held).min(self.idle);
        let slot = free > 0 || (self.held >= self.eligible && self.running.contracted > 0);
        let group = self.group.running < self.group_limit || self.group.contracted > 0;
        let party = self.party.running < self.core_cap || self.party.contracted > 0;
        slot && group && party
    }
}

/// What a pending request lacks besides memory, by its kind: a claim nothing, a compile job what its capacity
/// shows. A record holds it as the request's capacity, or null for a claim.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(into = "Option<Capacity>")]
pub enum Lacks {
    Nothing,
    Capacity(Capacity),
}
impl Lacks {
    /// What a pending request of `id`'s kind carries, as a record holds it. The one place a request's kind decides
    /// its evidence: a compile job must carry its capacity, a claim carries none.
    pub fn of(id: &RequestId, capacity: Option<Capacity>) -> Result<Self, String> {
        match (id, capacity) {
            (RequestId::Claim(_), None) => Ok(Self::Nothing),
            (RequestId::Compile { .. }, Some(capacity)) => Ok(Self::Capacity(capacity)),
            (RequestId::Claim(_), Some(_)) => {
                Err(format!("claim {id} carries a capacity; a claim waits for nothing but memory"))
            }
            (RequestId::Compile { .. }, None) => Err(format!("pending compile job {id} carries no capacity")),
        }
    }
    /// The capacity a compile job carries; a claim carries none.
    pub fn capacity(&self) -> Option<&Capacity> {
        match self {
            Self::Nothing => None,
            Self::Capacity(capacity) => Some(capacity),
        }
    }
    /// Whether compiles that end hold everything it lacks besides memory; a claim lacks nothing.
    fn held_by_contracted(&self) -> bool {
        match self {
            Self::Nothing => true,
            Self::Capacity(capacity) => capacity.held_by_contracted(),
        }
    }
}
impl From<Lacks> for Option<Capacity> {
    fn from(lacks: Lacks) -> Self {
        match lacks {
            Lacks::Nothing => None,
            Lacks::Capacity(capacity) => Some(capacity),
        }
    }
}

/// An ordinary pending request that may hold the head: aged, serviceable and waiting for memory. Its party and bytes
/// are its request's, which a record may name elsewhere too.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "", deserialize = "F::Lacks: Deserialize<'de>"))]
pub struct Candidate<F: Form = Written> {
    pub id: RequestId,
    pub party: F::Observed<String>,
    pub eligible_at: At,
    pub catalogue: Catalogue,
    pub bytes: F::Observed<u64>,
    /// A compile job's capacity; a claim waits for nothing but memory.
    #[serde(rename = "capacity")]
    pub lacks: F::Lacks,
}
impl Candidate {
    /// Its age: its eligibility time, then its catalogue place. The oldest is the least.
    pub fn age(&self) -> (At, Catalogue) {
        (self.eligible_at, self.catalogue)
    }
    /// How it stands as the chosen head: pending, with its bytes and what it lacks besides memory.
    pub fn standing(&self) -> Standing {
        Standing::Pending {
            bytes: self.bytes,
            lacks: self.lacks.clone(),
        }
    }
    /// A candidate as a record holds it, read as its kind requires: its kind is its id's, its catalogue place is of
    /// that kind, and it carries what that kind must.
    pub fn read(
        id: RequestId,
        (party, bytes): (String, u64),
        eligible_at: At,
        catalogue: Catalogue,
        capacity: Option<Capacity>,
    ) -> Result<Self, String> {
        match (&id, catalogue) {
            (RequestId::Compile { .. }, Catalogue::Compile { .. }) => Ok(()),
            (RequestId::Claim(registration), Catalogue::Claim { order }) => match order == registration.order {
                true => Ok(()),
                false => Err(format!("candidate {id}'s catalogue order {order} is not its registration's, {}", registration.order)),
            },
            _ => Err(format!("candidate {id} has a catalogue place of another kind")),
        }?;
        let lacks = Lacks::of(&id, capacity)?;
        Ok(Self { id, party, eligible_at, catalogue, bytes, lacks })
    }
}

/// How the current head stands this tick: pending, with its bytes and what it lacks besides memory, or ended. An
/// ended compile job's capacity, which the engine may no longer know, is kept as found and not judged.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(into = "Stood")]
pub enum Standing {
    Pending { bytes: u64, lacks: Lacks },
    Ended { bytes: u64, capacity: Option<Capacity> },
}
impl Standing {
    /// A head's request as it stands: pending or ended, its bytes, and what it lacks besides memory.
    pub fn of(pending: bool, bytes: u64, lacks: Lacks) -> Self {
        match pending {
            true => Self::Pending { bytes, lacks },
            false => Self::Ended {
                bytes,
                capacity: lacks.into(),
            },
        }
    }
    /// A kept head's standing as a record holds it, read as the head's kind requires.
    pub fn read(id: &RequestId, stood: Stood) -> Result<Self, String> {
        let Stood { pending, bytes, capacity } = stood;
        match pending {
            true => Ok(Self::Pending {
                bytes,
                lacks: Lacks::of(id, capacity)?,
            }),
            false => Ok(Self::Ended { bytes, capacity }),
        }
    }
    pub fn bytes(&self) -> u64 {
        match self {
            Self::Pending { bytes, .. } | Self::Ended { bytes, .. } => *bytes,
        }
    }
}

/// A standing as a record holds it, before its head's kind decides what it must carry. Its bytes are its request's
/// declaration, which a record may name elsewhere too.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Stood<F: Form = Written> {
    pub pending: bool,
    pub bytes: F::Observed<u64>,
    pub capacity: Option<Capacity<F>>,
}
impl From<Standing> for Stood {
    fn from(standing: Standing) -> Self {
        match standing {
            Standing::Pending { bytes, lacks } => Self {
                pending: true,
                bytes,
                capacity: lacks.into(),
            },
            Standing::Ended { bytes, capacity } => Self {
                pending: false,
                bytes,
                capacity,
            },
        }
    }
}

/// The party whose turn it is to hold the head, and whether it still holds it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Turn {
    pub party: String,
    pub held: bool,
}

/// The request holding the aged head, and the work it waits for while it does not fit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Head {
    pub id: RequestId,
    pub party: String,
    pub bytes: u64,
    pub fits: bool,
    pub recorded: BTreeSet<Work>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// It stays the head and reserves its bytes.
    Kept { fits: bool },
    /// Served, or ended unserved: the turn passes.
    Ended,
    /// No longer serviceable, large, or a compile while the engine drains: its party keeps the turn.
    Released,
    /// A fitting compile head lacking what no contracted compile holds: the turn passes.
    Passed,
    /// Its recorded work has ended and it still does not fit: the turn passes and it gives way.
    EndedInVain,
}

/// The figures a tick judges heads by.
#[derive(Clone, Copy)]
pub struct Figures<'a> {
    /// The budget left for the aged head to fit.
    pub left: Budget,
    /// What the service's own work can free: `left` plus the declarations of the work that ends.
    pub serviceable: u64,
    /// The work counted in that projection.
    pub live: &'a BTreeSet<Work>,
    pub draining: bool,
}

/// Whether the head keeps its place this tick.
pub fn judge(head: &Head, standing: &Standing, figures: Figures) -> Verdict {
    let compile = matches!(head.id, RequestId::Compile { .. });
    let (bytes, lacks) = match standing {
        Standing::Ended { .. } => return Verdict::Ended,
        Standing::Pending { bytes, lacks } => (*bytes, lacks),
    };
    if (figures.draining && compile)
        || Class::of(bytes, None) != Class::Ordinary
        || bytes > figures.serviceable
    {
        return Verdict::Released;
    }
    match figures.left.fits(bytes) {
        true if !lacks.held_by_contracted() => Verdict::Passed,
        true => Verdict::Kept { fits: true },
        false if head.recorded.is_disjoint(figures.live) => Verdict::EndedInVain,
        false => Verdict::Kept { fits: false },
    }
}

/// The turn after a verdict on its party's head.
pub fn turn_after(turn: Option<Turn>, verdict: Verdict) -> Option<Turn> {
    turn.map(|turn| match verdict {
        Verdict::Kept { .. } | Verdict::Released => turn,
        Verdict::Ended | Verdict::Passed | Verdict::EndedInVain => Turn { held: false, ..turn },
    })
}

/// A party with a serviceable candidate at a choice, and its oldest one.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "", deserialize = "F::Lacks: Deserialize<'de>"))]
pub struct Listed<F: Form = Written> {
    pub party: String,
    pub candidate: Candidate<F>,
}

/// The party whose candidate gets the head: in cyclic name order from the turn's party itself while it holds the
/// turn, from the next party once its head was served, and with no turn yet, the party whose candidate is oldest.
/// `listed` is in party name order.
pub fn next_party<'a>(listed: &'a [Listed], turn: Option<&Turn>) -> Option<&'a Listed> {
    match turn {
        None => listed.iter().min_by_key(|l| l.candidate.age()),
        Some(turn) => listed
            .iter()
            .find(|l| match turn.held {
                true => l.party >= turn.party,
                false => l.party > turn.party,
            })
            .or_else(|| listed.first()),
    }
}

/// One step of a tick's choice, with what it was decided from.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "step",
    rename_all = "snake_case",
    bound(serialize = "", deserialize = "F::Lacks: Deserialize<'de>, F::Standing: Deserialize<'de>")
)]
pub enum Step<F: Form = Written> {
    /// The head of an earlier tick, judged again.
    Kept { head: F::Figure<Head>, standing: F::Standing, verdict: Verdict },
    /// A choice among the listed parties, starting from `turn`.
    Chosen {
        turn: F::Figure<Option<Turn>>,
        listed: Vec<Listed<F>>,
        chosen: RequestId,
        verdict: Verdict,
    },
}

impl Step {
    /// The request the step judged.
    pub fn judged(&self) -> &RequestId {
        match self {
            Self::Kept { head, .. } => &head.id,
            Self::Chosen { chosen, .. } => chosen,
        }
    }
}
impl<F: Form> Step<F> {
    /// The verdict on the request the step judged.
    pub fn verdict(&self) -> Verdict {
        match self {
            Self::Kept { verdict, .. } | Self::Chosen { verdict, .. } => *verdict,
        }
    }
}

/// What a tick decided about the aged head.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(serialize = "", deserialize = "F::Lacks: Deserialize<'de>, F::Standing: Deserialize<'de>"))]
pub struct Choice<F: Form = Written> {
    pub before: F::Figure<Option<Turn>>,
    pub steps: Vec<Step<F>>,
    pub after: F::Figure<Option<Turn>>,
    pub head: F::Figure<Option<Head>>,
}
impl Default for Choice {
    fn default() -> Self {
        Self {
            before: None,
            steps: Vec::new(),
            after: None,
            head: None,
        }
    }
}

/// The aged head, the GPU head's recorded work, the turn and the requests giving way, held in memory only. The
/// service's ticks change them, and the replay of its records changes them the same way, from what an instance's
/// earlier records left.
#[derive(Clone, Debug, Default)]
pub struct Precedence {
    head: Option<Head>,
    /// The GPU pass's head and the work it waits for; the pass chooses it afresh each tick.
    gpu: Option<(RequestId, BTreeSet<Work>)>,
    turn: Option<Turn>,
    /// Requests whose head ended in vain, with the work counted when it did: none is a head again until all of it
    /// has ended.
    giving_way: BTreeMap<RequestId, BTreeSet<Work>>,
}

impl Precedence {
    pub fn head(&self) -> Option<&Head> {
        self.head.as_ref()
    }
    pub fn turn(&self) -> Option<&Turn> {
        self.turn.as_ref()
    }
    /// A request stops giving way once all the work it recorded has ended.
    pub fn settle(&mut self, live: &BTreeSet<Work>) {
        self.giving_way.retain(|_, work| !work.is_disjoint(live));
    }
    pub fn gives_way(&self, id: &RequestId) -> bool {
        self.giving_way.contains_key(id)
    }
    /// The requests giving way.
    pub fn giving_way(&self) -> impl Iterator<Item = &RequestId> {
        self.giving_way.keys()
    }
    /// Judges the head of an earlier tick, then, while there is none, gives it to the next party's oldest
    /// candidate. A request released this tick is not chosen again in it.
    pub fn choose(&mut self, figures: Figures, candidates: Vec<Candidate>, standing: Option<Standing>) -> Choice {
        let mut choice = Choice {
            before: self.turn.clone(),
            ..Choice::default()
        };
        let mut excluded = BTreeSet::new();
        choice.steps.extend(self.judge_kept(standing, figures, &mut excluded));
        while self.head.is_none() {
            let mut best: BTreeMap<&str, &Candidate> = BTreeMap::new();
            for candidate in candidates.iter().filter(|c| !excluded.contains(&c.id)) {
                best.entry(&candidate.party)
                    .and_modify(|held| {
                        if candidate.age() < held.age() {
                            *held = candidate;
                        }
                    })
                    .or_insert(candidate);
            }
            let listed: Vec<Listed> = best
                .into_iter()
                .map(|(party, candidate)| Listed { party: party.to_owned(), candidate: candidate.clone() })
                .collect();
            let Some(step) = self.choose_from(listed, figures, &mut excluded) else {
                break;
            };
            choice.steps.push(step);
        }
        choice.after = self.turn.clone();
        choice.head = self.head.clone();
        choice
    }
    /// Judges the head of an earlier tick as it stands now; a request it releases joins `excluded`.
    pub fn judge_kept(&mut self, standing: Option<Standing>, figures: Figures, excluded: &mut BTreeSet<RequestId>) -> Option<Step> {
        let (head, standing) = self.head.take().zip(standing)?;
        let verdict = judge(&head, &standing, figures);
        self.apply(&head, standing.bytes(), verdict, figures, excluded);
        Some(Step::Kept { head, standing, verdict })
    }
    /// Gives the head to the candidate of the party the turn's walk reaches in `listed`, each party's oldest, and
    /// judges it; one it releases joins `excluded`. None when no party is listed.
    pub fn choose_from(&mut self, listed: Vec<Listed>, figures: Figures, excluded: &mut BTreeSet<RequestId>) -> Option<Step> {
        let next = next_party(&listed, self.turn.as_ref())?.clone();
        let turn = self.turn.replace(Turn { party: next.party.clone(), held: true });
        let head = Head {
            id: next.candidate.id.clone(),
            party: next.party,
            bytes: next.candidate.bytes,
            fits: false,
            recorded: figures.live.clone(),
        };
        let standing = next.candidate.standing();
        let verdict = judge(&head, &standing, figures);
        self.apply(&head, head.bytes, verdict, figures, excluded);
        Some(Step::Chosen {
            turn,
            listed,
            chosen: head.id,
            verdict,
        })
    }
    fn apply(
        &mut self,
        head: &Head,
        bytes: u64,
        verdict: Verdict,
        figures: Figures,
        excluded: &mut BTreeSet<RequestId>,
    ) {
        self.turn = turn_after(self.turn.take(), verdict);
        match verdict {
            Verdict::Kept { fits } => {
                self.head = Some(Head {
                    bytes,
                    fits,
                    ..head.clone()
                })
            }
            Verdict::EndedInVain => {
                self.giving_way.insert(head.id.clone(), figures.live.clone());
                excluded.insert(head.id.clone());
            }
            Verdict::Ended | Verdict::Released | Verdict::Passed => {
                excluded.insert(head.id.clone());
            }
        }
    }
    /// Whether `id`, the first GPU-place claim of this tick's pass that does not fit but is recoverable, holds
    /// the GPU head: one new to it records the work counted now, one that held it keeps it while work it recorded
    /// is live, and otherwise ended in vain and gives way. Returns the work it waits for while it holds it.
    pub fn gpu(&mut self, id: &RequestId, live: &BTreeSet<Work>) -> Result<BTreeSet<Work>, BTreeSet<Work>> {
        match self.gpu.take() {
            Some((held, recorded)) if held == *id && recorded.is_disjoint(live) => {
                self.giving_way.insert(held, live.clone());
                Err(recorded)
            }
            Some((held, recorded)) if held == *id => {
                self.gpu = Some((held, recorded.clone()));
                Ok(recorded)
            }
            _ => {
                self.gpu = Some((id.clone(), live.clone()));
                Ok(live.clone())
            }
        }
    }
    /// The GPU pass's decision on the pending GPU-place claim `id` of `bytes` at its turn, from `budget`, beside the
    /// declarations `own` of the service's own work that ends, `held` whether an earlier claim of the pass holds the
    /// GPU head, and `live` the work counted now: granted if it fits; the first that does not but is recoverable gives
    /// way while it does, and otherwise holds the GPU head; one after the head is behind it, and one the service's
    /// own work cannot free is not serviceable.
    pub fn gpu_turn(&mut self, id: &RequestId, bytes: u64, budget: Budget, own: u64, held: bool, live: &BTreeSet<Work>) -> Gpu {
        match (budget.fits(bytes), budget.plus(own).fits(bytes) && !held) {
            (true, _) => Gpu::Granted,
            (false, true) if self.gives_way(id) => Gpu::GivingWay,
            (false, true) => match self.gpu(id, live) {
                Ok(recorded) => Gpu::Head { recorded },
                Err(recorded) => Gpu::EndedInVain { recorded },
            },
            (false, false) if held => Gpu::BehindGpuHead,
            (false, false) => Gpu::NotServiceable,
        }
    }
    /// The GPU pass found no head this tick.
    pub fn no_gpu(&mut self) {
        self.gpu = None;
    }
    /// The GPU head, as health shows it.
    pub fn gpu_head(&self) -> Option<&RequestId> {
        self.gpu.as_ref().map(|(id, _)| id)
    }
    /// The head was granted at once: the turn passes.
    pub fn served(&mut self) {
        self.head = None;
        self.turn = turn_after(self.turn.take(), Verdict::Ended);
    }
    /// A compile ran ahead into the head's bytes: the head waits for it too.
    pub fn ran_ahead(&mut self, work: Work) {
        if let Some(head) = self.head.as_mut() {
            head.recorded.insert(work);
        }
    }
}

/// A part of the precedence that an instance's ticks carry from one to the next, named for the replay's mutation
/// table.
#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub enum Part {
    Head,
    Gpu,
    Turn,
    GivingWay,
}
#[cfg(test)]
impl Precedence {
    /// Every part, by an exhaustive destructuring: a new part does not compile until it is named here, and the
    /// mutation table's match over the names until it has rows.
    pub fn parts(&self) -> Vec<Part> {
        let Self { head: _, gpu: _, turn: _, giving_way: _ } = self;
        vec![Part::Head, Part::Gpu, Part::Turn, Part::GivingWay]
    }
}

// The record schema's enums declared here, each with a value of every variant (`decision::Variants`). Each arm names
// the next variant, so a new variant does not compile until it has its place in the list.
#[cfg(test)]
impl super::decision::Variants for Class {
    fn variants() -> Vec<Self> {
        std::iter::successors(Some(Self::Gpu), |class| match class {
            Self::Gpu => Some(Self::Ordinary),
            Self::Ordinary => Some(Self::Large),
            Self::Large => None,
        })
        .collect()
    }
}
#[cfg(test)]
impl super::decision::Variants for RequestId {
    fn variants() -> Vec<Self> {
        std::iter::successors(Some(Self::Compile { group: String::new(), job: String::new() }), |id| match id {
            Self::Compile { .. } => Some(Self::Claim(Registration { claim: String::new(), order: 0 })),
            Self::Claim { .. } => None,
        })
        .collect()
    }
}
#[cfg(test)]
impl super::decision::Variants for Work {
    fn variants() -> Vec<Self> {
        std::iter::successors(Some(Self::compile("", 0.0)), |work| match work {
            Self::Compile(_) => Some(Self::Claim(Registration { claim: String::new(), order: 0 })),
            Self::Claim(_) => None,
        })
        .collect()
    }
}
#[cfg(test)]
impl super::decision::Variants for Catalogue {
    fn variants() -> Vec<Self> {
        std::iter::successors(Some(Self::Compile { position: 0, index: 0 }), |place| match place {
            Self::Compile { .. } => Some(Self::Claim { order: 0 }),
            Self::Claim { .. } => None,
        })
        .collect()
    }
}
#[cfg(test)]
impl super::decision::Variants for Verdict {
    fn variants() -> Vec<Self> {
        std::iter::successors(Some(Self::Kept { fits: false }), |verdict| match verdict {
            Self::Kept { .. } => Some(Self::Ended),
            Self::Ended => Some(Self::Released),
            Self::Released => Some(Self::Passed),
            Self::Passed => Some(Self::EndedInVain),
            Self::EndedInVain => None,
        })
        .collect()
    }
}
#[cfg(test)]
impl super::decision::Variants for Step {
    fn variants() -> Vec<Self> {
        let id = RequestId::Claim(Registration { claim: String::new(), order: 0 });
        let head = Head { id: id.clone(), party: String::new(), bytes: 0, fits: false, recorded: BTreeSet::new() };
        let kept = Self::Kept { head, standing: Standing::of(true, 0, Lacks::Nothing), verdict: Verdict::Ended };
        std::iter::successors(Some(kept), |step| match step {
            Self::Kept { .. } => Some(Self::Chosen { turn: None, listed: Vec::new(), chosen: id.clone(), verdict: Verdict::Ended }),
            Self::Chosen { .. } => None,
        })
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_request_is_large_only_above_the_large_request_bound() {
        assert_eq!(Class::of(LARGE_REQUEST, None), Class::Ordinary);
        assert_eq!(Class::of(LARGE_REQUEST + 1, None), Class::Large);
        assert_eq!(Class::of(0, None), Class::Ordinary);
        assert_eq!(Class::of(LARGE_REQUEST, Some(Purpose::GpuPlace)), Class::Ordinary, "nothing ties it to a told GPU place");
        assert_eq!(Class::of(LARGE_REQUEST + 1, Some(Purpose::GpuPlace)), Class::Large, "a large GPU place parks");
        assert_eq!(Class::of(1, Some(Purpose::HostOnly)), Class::Ordinary);
        assert!(Class::Gpu < Class::Ordinary && Class::Ordinary < Class::Large, "the order the passes serve them in");
    }

    const GIB: u64 = 1 << 30;

    fn claim(party: &str, id: &str, eligible_at: f64, order: u64, bytes: u64) -> Candidate {
        Candidate {
            id: RequestId::Claim(Registration { claim: id.into(), order }),
            party: party.into(),
            eligible_at: At(eligible_at),
            catalogue: Catalogue::Claim { order },
            bytes,
            lacks: Lacks::Nothing,
        }
    }
    fn work(job: &str) -> Work {
        Work::compile(job, 1.0)
    }
    fn figures(left: i64, serviceable: u64, live: &BTreeSet<Work>) -> Figures<'_> {
        Figures {
            left: Budget::of(0).plus(left.max(0) as u64).less((-left).max(0) as u64),
            serviceable,
            live,
            draining: false,
        }
    }
    fn chosen(choice: &Choice) -> Option<String> {
        choice.head.as_ref().map(|head| head.id.to_string())
    }

    #[test]
    fn the_turn_walks_the_parties_in_cyclic_name_order() {
        let listed: Vec<Listed> = [("a", 30.0), ("b", 10.0), ("c", 20.0)]
            .into_iter()
            .map(|(party, at)| Listed { party: party.into(), candidate: claim(party, party, at, 1, GIB) })
            .collect();
        let party = |turn: Option<Turn>| next_party(&listed, turn.as_ref()).unwrap().party.clone();
        assert_eq!(party(None), "b", "with no turn yet the oldest candidate's party goes first");
        assert_eq!(party(Some(Turn { party: "b".into(), held: true })), "b");
        assert_eq!(party(Some(Turn { party: "b".into(), held: false })), "c");
        assert_eq!(party(Some(Turn { party: "c".into(), held: false })), "a", "the walk wraps around");
        assert_eq!(party(Some(Turn { party: "bb".into(), held: true })), "c", "a party without a candidate is passed");
        assert!(next_party(&[], None).is_none());
    }

    #[test]
    fn heads_rotate_over_parties_and_a_partys_younger_grants_keep_its_place() {
        let live = BTreeSet::from([work("running")]);
        let mut precedence = Precedence::default();
        let candidates = |round: usize| -> Vec<Candidate> {
            ["a", "b", "c"]
                .into_iter()
                .map(|party| claim(party, &format!("{party}{round}"), 0.0, round as u64, 4 * GIB))
                .collect()
        };
        let mut order = Vec::new();
        for round in 0..4 {
            let choice = precedence.choose(figures(GIB as i64, 8 * GIB, &live), candidates(round), None);
            order.push(choice.head.unwrap().party);
            // The head is granted once memory frees; the turn then passes.
            let standing = Standing::of(true, 4 * GIB, Lacks::Nothing);
            let kept = precedence.choose(figures(5 * GIB as i64, 8 * GIB, &live), candidates(round), Some(standing));
            assert!(matches!(kept.steps[0], Step::Kept { verdict: Verdict::Kept { fits: true }, .. }));
            precedence.served();
        }
        assert_eq!(order, ["a", "b", "c", "a"]);
    }

    #[test]
    fn an_unserviceable_head_is_released_and_its_party_keeps_the_turn_with_its_next_candidate() {
        let live = BTreeSet::from([work("running")]);
        let mut precedence = Precedence::default();
        let candidates = vec![
            claim("a", "older", 0.0, 1, 4 * GIB),
            claim("a", "younger", 10.0, 2, 2 * GIB),
            claim("b", "other", 5.0, 3, 2 * GIB),
        ];
        let first = precedence.choose(figures(GIB as i64, 8 * GIB, &live), candidates.clone(), None);
        assert_eq!(chosen(&first).as_deref(), Some("older"));
        // Memory held elsewhere lowers what the service's own work can free below the older claim's bytes.
        let standing = Standing::of(true, 4 * GIB, Lacks::Nothing);
        let serviceable: Vec<_> = candidates.iter().filter(|c| c.bytes <= 3 * GIB).cloned().collect();
        let second = precedence.choose(figures(GIB as i64, 3 * GIB, &live), serviceable, Some(standing));
        assert!(matches!(second.steps[0], Step::Kept { verdict: Verdict::Released, .. }));
        assert_eq!(chosen(&second).as_deref(), Some("younger"), "the same party's next candidate, at once");
        assert_eq!(second.after, Some(Turn { party: "a".into(), held: true }));
    }

    #[test]
    fn a_head_whose_work_ended_in_vain_gives_way_until_the_work_then_counted_ends() {
        let live = BTreeSet::from([work("first")]);
        let mut precedence = Precedence::default();
        let candidates = vec![claim("a", "head", 0.0, 1, 6 * GIB), claim("b", "next", 5.0, 2, 2 * GIB)];
        precedence.choose(figures(GIB as i64, 8 * GIB, &live), candidates.clone(), None);
        // Its compile ended, but memory held elsewhere took what it freed.
        let now_live = BTreeSet::from([work("second")]);
        let standing = Standing::of(true, 6 * GIB, Lacks::Nothing);
        let choice = precedence.choose(figures(GIB as i64, 8 * GIB, &now_live), candidates.clone(), Some(standing));
        assert!(matches!(choice.steps[0], Step::Kept { verdict: Verdict::EndedInVain, .. }));
        assert_eq!(chosen(&choice).as_deref(), Some("next"), "the turn passed to the next party");
        let head = RequestId::Claim(Registration { claim: "head".into(), order: 1 });
        precedence.settle(&now_live);
        assert!(precedence.gives_way(&head), "it waits for the work running when it ended in vain");
        precedence.settle(&BTreeSet::from([work("third")]));
        assert!(!precedence.gives_way(&head));
    }

    #[test]
    fn a_fitting_compile_head_keeps_its_reservation_only_while_contracted_compiles_hold_what_it_lacks() {
        let capacity = Capacity {
            eligible: 3,
            held: 0,
            idle: 0,
            running: Units::default(),
            group: Units::default(),
            group_limit: 2,
            party: Units::default(),
            core_cap: 2,
        };
        let units = |running, contracted| Units { running, contracted };
        assert!(!capacity.held_by_contracted(), "idle cores taken by outside load never keep it");
        assert!(Capacity { idle: 3, ..capacity.clone() }.held_by_contracted(), "it lacks nothing");
        let full = Capacity { held: 3, running: units(3, 1), ..capacity.clone() };
        assert!(full.held_by_contracted());
        assert!(!Capacity { running: units(3, 0), ..full.clone() }.held_by_contracted());
        let group = Capacity { idle: 3, group: units(2, 0), ..capacity.clone() };
        assert!(!group.held_by_contracted(), "its group's units are held by uncontracted compiles");
        assert!(Capacity { group: units(2, 1), ..group }.held_by_contracted());
        let party = Capacity { idle: 3, party: units(2, 0), ..capacity };
        assert!(!party.held_by_contracted());
        let head = Head {
            id: RequestId::Compile { group: "g".into(), job: "j".into() },
            party: "p".into(),
            bytes: GIB,
            fits: true,
            recorded: BTreeSet::new(),
        };
        let live = BTreeSet::new();
        let standing = Standing::of(true, GIB, Lacks::Capacity(party.clone()));
        assert_eq!(judge(&head, &standing, figures(2 * GIB as i64, 2 * GIB, &live)), Verdict::Passed);
        let draining = Figures { draining: true, ..figures(2 * GIB as i64, 2 * GIB, &live) };
        assert_eq!(judge(&head, &standing, draining), Verdict::Released);
        let large = Standing::of(true, LARGE_REQUEST + 1, Lacks::Capacity(party));
        assert_eq!(judge(&head, &large, figures(i64::MAX, u64::MAX, &live)), Verdict::Released);
    }

    #[test]
    fn a_requests_kind_decides_the_capacity_its_record_carries() {
        use serde_json::{json, Value};
        let compile = RequestId::Compile { group: "g".into(), job: "j".into() };
        let head = Head { id: compile.clone(), party: "p".into(), bytes: GIB, fits: false, recorded: BTreeSet::new() };
        let free: Capacity = Capacity {
            eligible: 3,
            held: 0,
            idle: 3,
            running: Units::default(),
            group: Units::default(),
            group_limit: 2,
            party: Units::default(),
            core_cap: 2,
        };
        let capacity = serde_json::to_value(free).unwrap();
        // A kept head's standing is read as the head's kind requires; the head is the one the tick before left.
        let kept = |head: &Head, standing: Value| {
            Standing::read(&head.id, serde_json::from_value::<Stood>(standing).unwrap())
        };
        // A pending compile job carries its capacity, and its record is as before; one that ended may lack it, as
        // the engine's fallback for a job it no longer knows records it.
        let pending = kept(&head, json!({"pending": true, "bytes": GIB, "capacity": capacity})).unwrap();
        assert!(matches!(&pending, Standing::Pending { lacks: Lacks::Capacity(_), .. }));
        assert_eq!(serde_json::to_value(&pending).unwrap(), json!({"pending": true, "bytes": GIB, "capacity": capacity}));
        let missing = kept(&head, json!({"pending": true, "bytes": GIB, "capacity": null})).unwrap_err();
        assert!(missing.contains("pending compile job j carries no capacity"), "{missing}");
        let ended = kept(&head, json!({"pending": false, "bytes": GIB, "capacity": null})).unwrap();
        assert!(matches!(ended, Standing::Ended { capacity: None, .. }));
        // A claim waits for nothing but memory: it carries no capacity.
        let claim = Head { id: RequestId::Claim(Registration { claim: "c".into(), order: 1 }), ..head };
        let carrying = kept(&claim, json!({"pending": true, "bytes": GIB, "capacity": capacity})).unwrap_err();
        assert!(carrying.contains("claim c carries a capacity"), "{carrying}");
        // A candidate likewise, and its catalogue place is of its own kind.
        let offered = |id: &RequestId, catalogue: Catalogue, capacity: &Value| {
            let capacity = serde_json::from_value::<Option<Capacity>>(capacity.clone()).unwrap();
            Candidate::read(id.clone(), ("p".into(), GIB), At(0.0), catalogue, capacity)
        };
        let place = Catalogue::Compile { position: 0, index: 0 };
        let read = offered(&compile, place, &capacity).unwrap();
        assert_eq!(serde_json::to_value(&read).unwrap()["capacity"], capacity);
        assert!(offered(&compile, place, &Value::Null).unwrap_err().contains("pending compile job j carries no capacity"));
        let order = Catalogue::Claim { order: 1 };
        assert!(offered(&claim.id, order, &capacity).unwrap_err().contains("claim c carries a capacity"));
        assert!(offered(&compile, order, &capacity).unwrap_err().contains("candidate j has a catalogue place of another kind"));
        offered(&claim.id, order, &Value::Null).unwrap();
    }
    #[test]
    fn an_older_job_requeued_before_r_takes_its_partys_next_turn_and_r_the_one_after() {
        let live = BTreeSet::from([work("running")]);
        let mut precedence = Precedence::default();
        let job = Candidate {
            id: RequestId::Compile { group: "g".into(), job: "j".into() },
            party: "p".into(),
            eligible_at: At(10.0),
            catalogue: Catalogue::Compile { position: 0, index: 0 },
            bytes: 4 * GIB,
            lacks: Lacks::Nothing,
        };
        let r = claim("p", "r", 20.0, 2, 4 * GIB);
        let q = claim("q", "q", 15.0, 1, 4 * GIB);
        let turn = |party: &str| Some(Turn { party: party.into(), held: true });
        let mut chosen_and_served = |candidates: Vec<Candidate>| {
            let choice = precedence.choose(figures(GIB as i64, 8 * GIB, &live), candidates, None);
            let made = (chosen(&choice), choice.after.clone());
            precedence.served();
            made
        };
        assert_eq!(chosen_and_served(vec![job.clone(), r.clone(), q.clone()]), (Some("j".into()), turn("p")), "choice 1: P's turn chooses j");
        assert_eq!(chosen_and_served(vec![r.clone(), q]), (Some("q".into()), turn("q")), "choice 2: Q's turn chooses q");
        assert_eq!(chosen_and_served(vec![job, r.clone()]), (Some("j".into()), turn("p")), "choice 3: j, requeued with its eligibility, not r");
        assert_eq!(chosen_and_served(vec![r]), (Some("r".into()), turn("p")), "choice 4: Q has no candidate; P's turn chooses r");
    }
}
