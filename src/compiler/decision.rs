//! The decision record: what one admission tick decided, with every input it decided from. A tick writes one to
//! the journal when any pending request's decision changes, when it admits anything or changes the head or the
//! turn, and in every tick a head reserves. The service never reads a record back: `replay` checks one against
//! the admission rules from its own fields, and `check` checks a journal's records in order.
use super::{
    budget::{Budget, Purse},
    precedence::{judge, next_party, turn_after, Choice, Class, Figures, Head, RequestId, Standing, Step, Turn, Verdict, Work},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Where a record starts in the service's standard error.
pub const PREFIX: &str = "compile-queue: admission ";

/// The phases of a tick, in the order they admit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
    /// The class a phase admits.
    pub fn class(self) -> Class {
        match self {
            Self::Serve | Self::Launch | Self::Close => Class::Ordinary,
            Self::LargeLaunch | Self::LargeClose => Class::Large,
        }
    }
}

/// What the GPU pass decided for a pending GPU-place claim at its turn.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum Gpu {
    Granted,
    /// The GPU head: it reserves its bytes, and waits for the work recorded.
    Head { recorded: BTreeSet<Work> },
    /// It held the GPU head, and all the work it recorded has ended: it gives way.
    EndedInVain { recorded: BTreeSet<Work> },
    GivingWay,
    BehindGpuHead,
    /// More than the budget at its turn plus what the service's own work frees.
    NotServiceable,
}

/// A pending GPU-place claim's turn in the GPU pass: the budget at its turn and its decision.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GpuTurn {
    pub claim: String,
    pub bytes: u64,
    pub budget: Budget,
    #[serde(flatten)]
    pub decision: Gpu,
}

/// A piece of the service's own work in its projection, with its declaration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Counted {
    pub work: Work,
    pub bytes: u64,
}

/// A launch that ran ahead into the aged head's bytes: its declared runtime against the horizon, and the work
/// the head waits for from then on.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Ahead {
    pub declared: u64,
    pub horizon: f64,
    pub work: Work,
}

/// One step of the tick's running budget, in the order taken. `budget` is what it was admitted from.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Entry {
    /// The aged head's bytes are held from every later admission.
    Reserved { request: RequestId, bytes: u64, budget: Budget },
    /// A party's compiler turn kept its next job's bytes from claims.
    Turn { party: String, bytes: u64, budget: Budget },
    /// The turns' bytes go back to the launches they were kept for.
    Returned { bytes: u64, budget: Budget },
    Granted {
        phase: Phase,
        claim: String,
        class: Class,
        bytes: u64,
        budget: Budget,
        head: bool,
    },
    Launched {
        phase: Phase,
        group: String,
        job: String,
        class: Class,
        bytes: u64,
        budget: Budget,
        /// What was reserved for the aged head before this launch.
        reserved: u64,
        head: bool,
        ahead: Option<Ahead>,
    },
}
impl Entry {
    fn phase(&self) -> Phase {
        match self {
            Self::Reserved { .. } | Self::Turn { .. } | Self::Returned { .. } => Phase::Serve,
            Self::Granted { phase, .. } | Self::Launched { phase, .. } => *phase,
        }
    }
    fn budget(&self) -> Budget {
        match self {
            Self::Reserved { budget, .. }
            | Self::Turn { budget, .. }
            | Self::Returned { budget, .. }
            | Self::Granted { budget, .. }
            | Self::Launched { budget, .. } => *budget,
        }
    }
    /// The request this entry admitted.
    fn admits(&self) -> Option<Subject> {
        match self {
            Self::Granted { claim, .. } => Some(Subject::Claim { claim: claim.clone() }),
            Self::Launched { group, job, .. } => Some(Subject::Job {
                group: group.clone(),
                job: job.clone(),
            }),
            Self::Reserved { .. } | Self::Turn { .. } | Self::Returned { .. } => None,
        }
    }
}

/// Why a pending request stands where it does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Admitted,
    /// The GPU head, reserving until the service's own work frees its bytes.
    GpuHead,
    /// A GPU-place claim behind the GPU head.
    BehindGpuHead,
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
            (Class::Gpu | Class::Ordinary, true, _) => Self::GivingWay,
            (Class::Gpu | Class::Ordinary, false, true) => Self::NotServiceable,
            (Class::Gpu | Class::Ordinary, false, false) => Self::WaitingForTurn,
        }
    }
    pub fn head(fits: bool) -> Self {
        match fits {
            true => Self::AgedHeadFitting,
            false => Self::AgedHead,
        }
    }
    /// A GPU-place claim's decision, from the GPU pass.
    pub fn gpu(decision: &Gpu) -> Self {
        match decision {
            Gpu::Granted => Self::Admitted,
            Gpu::Head { .. } => Self::GpuHead,
            Gpu::EndedInVain { .. } | Gpu::GivingWay => Self::GivingWay,
            Gpu::BehindGpuHead => Self::BehindGpuHead,
            Gpu::NotServiceable => Self::NotServiceable,
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

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Record {
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
    /// The GPU pass, in request order, from the budget's terms.
    pub gpu: Vec<GpuTurn>,
    /// b1, the GPU head's reservation.
    pub gpu_reserved: u64,
    /// The budget the aged head and the share pass start from: what the GPU pass left.
    pub left: Budget,
    /// S: `left` plus D, floored at nothing.
    pub serviceable: u64,
    pub draining: bool,
    pub aged: Choice,
    pub aged_reserved: u64,
    pub admitted: Vec<Entry>,
    pub left_after_launch: u64,
    /// The decisions that changed since the previous tick, a request's first included.
    pub decisions: Vec<Decided>,
}

impl Record {
    /// The budget's terms, kept signed.
    pub fn budget(&self) -> Budget {
        Budget::of(self.available)
            .less(self.compiler_reserved)
            .less(self.minimum_ram)
            .less(self.claims_unused)
    }
    fn own(&self) -> u64 {
        self.own_work.iter().fold(0u64, |sum, counted| sum.saturating_add(counted.bytes))
    }
    fn live(&self) -> BTreeSet<Work> {
        self.own_work.iter().map(|counted| counted.work.clone()).collect()
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

/// Checks one record against the admission rules, from its own fields.
pub fn replay(record: &Record) -> Result<(), String> {
    let tick = record.tick;
    let live = record.live();
    let left = replay_gpu(record, &live)?;
    ensure(record.left == left, || {
        format!("tick {tick}: left {:?} is not what the GPU pass left of the budget's terms, {left:?}", record.left)
    })?;
    ensure(record.serviceable == serviceable(record.left, record.own()), || {
        format!("tick {tick}: S {} is not left plus D, floored", record.serviceable)
    })?;
    let figures = Figures {
        left: record.left,
        serviceable: record.serviceable,
        live: &live,
        draining: record.draining,
    };
    replay_choice(tick, &record.aged, figures)?;
    replay_admissions(record)
}

/// The GPU pass from the budget's terms: each claim is granted if it fits the budget at its turn; the first
/// that does not but is recoverable, its bytes at most that budget plus D, and is not giving way is the GPU head,
/// which waits for live recorded work and reserves its bytes; claims after it are behind it unless they fit.
/// Returns what the pass left.
fn replay_gpu(record: &Record, live: &BTreeSet<Work>) -> Result<Budget, String> {
    let tick = record.tick;
    let own = record.own();
    let mut budget = record.budget();
    let mut head = None;
    for turn in &record.gpu {
        let claim = &turn.claim;
        ensure(turn.budget == budget, || format!("tick {tick}: GPU claim {claim}'s turn had {:?}, the pass {budget:?}", turn.budget))?;
        let fits = budget.fits(turn.bytes);
        let recoverable = budget.plus(own).fits(turn.bytes);
        let holds = match &turn.decision {
            Gpu::Granted => fits,
            Gpu::Head { recorded } => !fits && recoverable && head.is_none() && !recorded.is_disjoint(live),
            Gpu::EndedInVain { recorded } => !fits && recoverable && head.is_none() && recorded.is_disjoint(live),
            Gpu::GivingWay => !fits,
            Gpu::BehindGpuHead => !fits && head.is_some(),
            Gpu::NotServiceable => !fits && !recoverable && head.is_none(),
        };
        ensure(holds, || format!("tick {tick}: GPU claim {claim} of {} bytes was {:?} at {budget:?}", turn.bytes, turn.decision))?;
        match &turn.decision {
            Gpu::Granted => budget = budget.less(turn.bytes),
            Gpu::Head { .. } => {
                budget = budget.less(turn.bytes);
                head = Some(turn.bytes);
            }
            Gpu::EndedInVain { .. } | Gpu::GivingWay | Gpu::BehindGpuHead | Gpu::NotServiceable => {}
        }
    }
    ensure(record.gpu_reserved == head.unwrap_or(0), || format!("tick {tick}: b1 is not the GPU head's bytes"))?;
    Ok(budget)
}

fn replay_choice(tick: u64, choice: &Choice, figures: Figures) -> Result<(), String> {
    let mut turn = choice.before.clone();
    let mut head: Option<Head> = None;
    for (index, step) in choice.steps.iter().enumerate() {
        match step {
            Step::Kept { head: kept, standing, verdict } => {
                ensure(index == 0, || format!("tick {tick}: an earlier head is judged first"))?;
                ensure(
                    turn == Some(Turn { party: kept.party.clone(), held: true }),
                    || format!("tick {tick}: a kept head's party holds the turn"),
                )?;
                let judged = judge(kept, standing, figures);
                ensure(judged == *verdict, || {
                    format!("tick {tick}: the head {} was {verdict:?}, its figures say {judged:?}", kept.id)
                })?;
                head = held(kept, standing.bytes, *verdict);
                turn = turn_after(turn, *verdict);
            }
            Step::Chosen { turn: from, listed, chosen, verdict } => {
                ensure(head.is_none() && *from == turn, || {
                    format!("tick {tick}: a choice starts from the turn and only while no head is held")
                })?;
                ensure(listed.windows(2).all(|pair| pair[0].party < pair[1].party), || {
                    format!("tick {tick}: the listed parties are distinct and in name order")
                })?;
                for entry in listed {
                    let candidate = &entry.candidate;
                    ensure(
                        candidate.party == entry.party
                            && candidate.bytes <= figures.serviceable
                            && Class::of(candidate.bytes, None) == Class::Ordinary,
                        || format!("tick {tick}: {} is listed but is no serviceable ordinary candidate", candidate.id),
                    )?;
                }
                let next = next_party(listed, from.as_ref())
                    .ok_or_else(|| format!("tick {tick}: a choice with no party listed"))?;
                ensure(next.candidate.id == *chosen, || {
                    format!("tick {tick}: {chosen} was chosen, the turn's walk gives {}", next.candidate.id)
                })?;
                let candidate = Head {
                    id: chosen.clone(),
                    party: next.party.clone(),
                    bytes: next.candidate.bytes,
                    fits: false,
                    recorded: figures.live.clone(),
                };
                let standing = Standing {
                    pending: true,
                    bytes: candidate.bytes,
                    capacity: next.candidate.capacity.clone(),
                };
                let judged = judge(&candidate, &standing, figures);
                ensure(judged == *verdict, || {
                    format!("tick {tick}: the chosen {chosen} was {verdict:?}, its figures say {judged:?}")
                })?;
                head = held(&candidate, candidate.bytes, *verdict);
                turn = turn_after(Some(Turn { party: next.party.clone(), held: true }), *verdict);
            }
        }
    }
    ensure(choice.after == turn, || format!("tick {tick}: the turn after the choice is {turn:?}"))?;
    let same = |a: &Head, b: &Head| a.id == b.id && a.bytes == b.bytes && a.fits == b.fits;
    ensure(
        match (&choice.head, &head) {
            (Some(recorded), Some(replayed)) => same(recorded, replayed),
            (None, None) => true,
            _ => false,
        },
        || format!("tick {tick}: the head held after the choice is {:?}", head.map(|h| h.id)),
    )
}

fn held(head: &Head, bytes: u64, verdict: Verdict) -> Option<Head> {
    match verdict {
        Verdict::Kept { fits } => Some(Head {
            bytes,
            fits,
            ..head.clone()
        }),
        Verdict::Ended | Verdict::Released | Verdict::Passed | Verdict::EndedInVain => None,
    }
}

fn replay_admissions(record: &Record) -> Result<(), String> {
    let tick = record.tick;
    let head = record.aged.head.as_ref();
    let mut purse = Purse {
        memory: record.left,
        reserved: 0,
    };
    let mut phase = Phase::Serve;
    let mut turns = 0u64;
    let mut after_launch = None;
    let mut reservations = 0;
    let mut grants_at_once = 0;
    for (index, entry) in record.admitted.iter().enumerate() {
        ensure(entry.phase() >= phase, || format!("tick {tick}: entry {index} is out of phase order"))?;
        if phase <= Phase::Launch && entry.phase() > Phase::Launch {
            after_launch = Some(purse.memory);
        }
        phase = entry.phase();
        ensure(entry.budget() == purse.memory, || {
            format!(
                "tick {tick}: entry {index} was admitted from {:?}, the running budget is {:?}",
                entry.budget(),
                purse.memory
            )
        })?;
        match entry {
            Entry::Reserved { request, bytes, .. } => {
                reservations += 1;
                ensure(
                    reservations == 1
                        && head.is_some_and(|h| h.id == *request && h.bytes == *bytes)
                        && *bytes <= record.serviceable,
                    || format!("tick {tick}: only the held head reserves its bytes, once and at most S"),
                )?;
                purse.memory = purse.memory.less(*bytes);
                purse.reserved = *bytes;
            }
            Entry::Turn { bytes, .. } => {
                ensure(purse.memory.fits(*bytes), || format!("tick {tick}: a compiler turn beyond the budget"))?;
                purse.memory = purse.memory.less(*bytes);
                turns += bytes;
            }
            Entry::Returned { bytes, .. } => {
                ensure(*bytes == turns, || format!("tick {tick}: the turns kept {turns}, {bytes} went back"))?;
                purse.memory = purse.memory.plus(*bytes);
                turns = 0;
            }
            Entry::Granted {
                phase, claim, class, bytes, head: at_once, ..
            } => {
                ensure(*class == phase.class() && Class::of(*bytes, None) == *class, || {
                    format!("tick {tick}: claim {claim} of class {class:?} granted in {phase:?}")
                })?;
                ensure(!at_once || (index == 0 && head.is_some_and(|h| at_once_id(h) == Some(claim))), || {
                    format!("tick {tick}: only a fitting claim head is granted at once, first")
                })?;
                grants_at_once += usize::from(*at_once);
                ensure(purse.memory.fits(*bytes), || {
                    format!("tick {tick}: claim {claim} of {bytes} bytes granted beyond the budget")
                })?;
                purse.memory = purse.memory.less(*bytes);
            }
            Entry::Launched {
                phase, job, class, bytes, reserved, head: own, ahead, ..
            } => {
                ensure(*class == phase.class() && Class::of(*bytes, None) == *class, || {
                    format!("tick {tick}: job {job} of class {class:?} launched in {phase:?}")
                })?;
                ensure(*reserved == purse.reserved, || {
                    format!("tick {tick}: {reserved} reserved before {job}, the replay holds {}", purse.reserved)
                })?;
                let is_head = head.is_some_and(|h| matches!(&h.id, RequestId::Compile { job: held, .. } if held == job));
                ensure(*own == is_head, || format!("tick {tick}: {job} launched as the head: {own}"))?;
                let runs_ahead = ahead.as_ref().is_some_and(|ahead| {
                    head.is_some_and(|h| !h.fits) && ahead.declared as f64 <= ahead.horizon && *phase == Phase::Launch
                });
                ensure(ahead.is_none() || runs_ahead, || {
                    format!("tick {tick}: {job} ran ahead while the head fits or beyond the horizon")
                })?;
                ensure(purse.fits(*bytes, *own || runs_ahead), || {
                    format!("tick {tick}: {job} of {bytes} bytes launched beyond its budget")
                })?;
                let paid = purse.charge(*bytes, *own);
                ensure(*own || (paid > 0) == ahead.is_some(), || {
                    format!("tick {tick}: {job} took {paid} bytes of the reservation")
                })?;
            }
        }
    }
    ensure(record.aged_reserved == record.admitted.iter().filter_map(reserved_bytes).sum::<u64>(), || {
        format!("tick {tick}: aged_reserved is not the reservation made")
    })?;
    let at_once = usize::from(head.and_then(at_once_id).is_some());
    let reserving = usize::from(head.is_some()) - at_once;
    ensure(grants_at_once == at_once && reservations == reserving, || {
        format!("tick {tick}: the held head is granted at once if it is a fitting claim, and reserves otherwise")
    })?;
    let after_launch = after_launch.unwrap_or(purse.memory);
    ensure(record.left_after_launch == after_launch.floor(), || {
        format!("tick {tick}: left_after_launch is {}, the launches left {:?}", record.left_after_launch, after_launch)
    })
}

fn reserved_bytes(entry: &Entry) -> Option<u64> {
    match entry {
        Entry::Reserved { bytes, .. } => Some(*bytes),
        _ => None,
    }
}

/// A fitting claim head is granted at once; every other head reserves.
fn at_once_id(head: &Head) -> Option<&String> {
    match (&head.id, head.fits) {
        (RequestId::Claim { claim }, true) => Some(claim),
        _ => None,
    }
}

/// Checks a journal's records in order: each against the rules, the ticks of a reserving head consecutive, and
/// every compile that ran ahead into the head's bytes recorded as work the head waits for.
pub fn check(records: &[Record]) -> Result<(), String> {
    for record in records {
        replay(record)?;
    }
    for pair in records.windows(2) {
        let (earlier, later) = (&pair[0], &pair[1]);
        let reserving = earlier.aged_reserved > 0 || earlier.gpu_reserved > 0;
        ensure(!reserving || later.tick == earlier.tick + 1, || {
            format!("tick {}: a head reserved, and the next record is tick {}", earlier.tick, later.tick)
        })?;
        let ahead: Vec<&Work> = earlier
            .admitted
            .iter()
            .filter_map(|entry| match entry {
                Entry::Launched { ahead: Some(ahead), .. } => Some(&ahead.work),
                _ => None,
            })
            .collect();
        let waits = |work: &Work| match later.aged.steps.first() {
            Some(Step::Kept { head, .. }) => head.recorded.contains(work),
            _ => true,
        };
        ensure(ahead.into_iter().all(waits), || {
            format!("tick {}: the head does not wait for a compile that ran ahead into its bytes", later.tick)
        })?;
    }
    Ok(())
}

/// The records in a service's standard error.
pub fn parse(journal: &str) -> Result<Vec<Record>, String> {
    journal
        .lines()
        .filter_map(|line| line.split_once(PREFIX).map(|(_, record)| record))
        .map(|record| serde_json::from_str(record).map_err(|e| format!("unreadable admission record: {e}")))
        .collect()
}

/// The decisions a tick last wrote, and when the next tick writes.
#[derive(Default)]
pub struct Journal {
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
    /// A new tick's record.
    pub fn begin(&mut self, sampled_at: f64) -> Record {
        self.tick += 1;
        Record {
            tick: self.tick,
            sampled_at,
            ..Record::default()
        }
    }
    /// Writes the record when a decision changed, the tick admitted anything, the head or the turn changed, or a
    /// head reserves. `pending` holds every pending request's decision.
    pub fn finish(&mut self, mut record: Record, mut pending: BTreeMap<Subject, Decisions>) {
        let granted = record.gpu.iter().filter(|turn| turn.decision == Gpu::Granted);
        let gpu = granted.map(|turn| Subject::Claim { claim: turn.claim.clone() });
        for subject in record.admitted.iter().filter_map(Entry::admits).chain(gpu) {
            pending.insert(subject, Decisions::One(Decision::Admitted));
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
        let admitted = record.admitted.iter().any(|entry| entry.admits().is_some());
        let written = !record.decisions.is_empty()
            || admitted
            || record.aged.after != self.turn
            || head != self.head
            || record.aged_reserved > 0
            || record.gpu_reserved > 0
            || record.gpu.iter().any(|turn| turn.decision == Gpu::Granted);
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

#[cfg(test)]
mod tests {
    use super::super::precedence::{At, Candidate, Capacity, Catalogue, Listed, Units};
    use super::*;
    const GIB: u64 = 1 << 30;

    fn compile(job: &str) -> Work {
        Work::compile(job, 1.0)
    }
    fn claim(id: &str) -> RequestId {
        RequestId::Claim { claim: id.into() }
    }
    /// 2 GiB left beside a running 4 GiB compile, S 6 GiB, and a 6 GiB claim chosen as the head, reserving.
    fn reserving(bytes: u64, available: u64) -> Record {
        let left = Budget::of(available).less(4 * GIB).less(10 * GIB);
        let turn = Turn { party: "p".into(), held: true };
        let head = Head {
            id: claim("head"),
            party: "p".into(),
            bytes,
            fits: false,
            recorded: BTreeSet::from([compile("w")]),
        };
        Record {
            tick: 7,
            available,
            compiler_reserved: 4 * GIB,
            minimum_ram: 10 * GIB,
            own_work: vec![Counted { work: compile("w"), bytes: 4 * GIB }],
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
                            capacity: None,
                        },
                    }],
                    chosen: claim("head"),
                    verdict: Verdict::Kept { fits: false },
                }],
                after: Some(turn),
                head: Some(head),
            },
            aged_reserved: bytes,
            admitted: vec![
                Entry::Reserved { request: claim("head"), bytes, budget: left },
                Entry::Returned { bytes: 0, budget: left.less(bytes) },
            ],
            left_after_launch: 0,
            ..Record::default()
        }
    }

    #[test]
    fn a_record_that_obeys_the_rules_replays() {
        replay(&reserving(6 * GIB, 16 * GIB)).unwrap();
        check(&[reserving(6 * GIB, 16 * GIB)]).unwrap();
    }

    #[test]
    fn a_younger_grant_from_the_heads_bytes_is_rejected() {
        let mut record = reserving(5 * GIB, 18 * GIB);
        replay(&record).unwrap();
        let younger = |budget| Entry::Granted {
            phase: Phase::Serve,
            claim: "younger".into(),
            class: Class::Ordinary,
            bytes: 2 * GIB,
            budget,
            head: false,
        };
        record.admitted.insert(1, younger(Budget::of(4 * GIB)));
        assert!(replay(&record).unwrap_err().contains("running budget"), "granted from before the reservation");
        record.admitted[1] = younger(Budget::of(4 * GIB).less(5 * GIB));
        assert!(replay(&record).unwrap_err().contains("beyond the budget"), "granted beyond what is left");
    }

    #[test]
    fn a_head_kept_after_memory_held_elsewhere_lowered_s_below_its_bytes_is_rejected() {
        let earlier = reserving(5 * GIB, 18 * GIB);
        let head = earlier.aged.head.clone().unwrap();
        // An outside process lowered S to 4 GiB; the recorded compile still runs.
        let left = Budget::of(14 * GIB).less(4 * GIB).less(10 * GIB);
        let record = Record {
            available: 14 * GIB,
            left,
            serviceable: serviceable(left, 4 * GIB),
            aged: Choice {
                before: earlier.aged.after.clone(),
                steps: vec![Step::Kept {
                    head: head.clone(),
                    standing: Standing { pending: true, bytes: 5 * GIB, capacity: None },
                    verdict: Verdict::Kept { fits: false },
                }],
                after: earlier.aged.after.clone(),
                head: Some(head),
            },
            admitted: vec![Entry::Reserved { request: claim("head"), bytes: 5 * GIB, budget: left }, Entry::Returned { bytes: 0, budget: left.less(5 * GIB) }],
            ..earlier
        };
        assert_eq!(record.serviceable, 4 * GIB);
        let error = replay(&record).unwrap_err();
        assert!(error.contains("its figures say Released"), "{error}");
    }

    #[test]
    fn a_fitting_compile_head_kept_while_idle_cores_are_taken_from_outside_is_rejected() {
        let mut record = reserving(5 * GIB, 22 * GIB);
        let id = RequestId::Compile { group: "g".into(), job: "head".into() };
        let capacity = Capacity {
            eligible: 3,
            held: 0,
            idle: 0,
            running: Units { running: 1, contracted: 1 },
            group: Units::default(),
            group_limit: 2,
            party: Units::default(),
            core_cap: 2,
        };
        let head = Head { id: id.clone(), fits: true, ..record.aged.head.clone().unwrap() };
        record.aged = Choice {
            before: Some(Turn { party: "p".into(), held: true }),
            steps: vec![Step::Kept {
                head: Head { fits: false, ..head.clone() },
                standing: Standing { pending: true, bytes: 5 * GIB, capacity: Some(capacity.clone()) },
                verdict: Verdict::Kept { fits: true },
            }],
            after: Some(Turn { party: "p".into(), held: true }),
            head: Some(head),
        };
        record.admitted[0] = Entry::Reserved { request: id, bytes: 5 * GIB, budget: record.left };
        record.admitted[1] = Entry::Returned { bytes: 0, budget: record.left.less(5 * GIB) };
        record.left_after_launch = record.left.less(5 * GIB).floor();
        let error = replay(&record).unwrap_err();
        assert!(error.contains("its figures say Passed"), "{error}");
        if let Step::Kept { standing, .. } = &mut record.aged.steps[0] {
            standing.capacity = Some(Capacity { idle: 3, ..capacity });
        }
        replay(&record).unwrap();
    }

    #[test]
    fn a_floored_budget_and_a_wrong_turn_are_rejected() {
        let record = reserving(6 * GIB, 16 * GIB);
        let floored = Record { left: Budget::of(3 * GIB), ..record.clone() };
        assert!(replay(&floored).unwrap_err().contains("budget's terms"));
        let mut wrong = record.clone();
        wrong.aged.after = Some(Turn { party: "p".into(), held: false });
        assert!(replay(&wrong).unwrap_err().contains("turn after"));
        let mut gap = record.clone();
        gap.tick = 9;
        assert!(check(&[record, gap]).unwrap_err().contains("next record is tick 9"), "a reserving head's ticks are consecutive");
    }

    #[test]
    fn the_gpu_pass_is_replayed_from_each_claims_budget_at_its_turn() {
        // 4.5 GiB and three pending 2 GiB GPU-place claims beside a 4 GiB running compile.
        let available = 10 * GIB + 4 * GIB + 9 * GIB / 2;
        let terms = Budget::of(available).less(4 * GIB).less(10 * GIB);
        let turn = |claim: &str, budget: Budget, decision: Gpu| GpuTurn { claim: claim.into(), bytes: 2 * GIB, budget, decision };
        let recorded = BTreeSet::from([compile("w")]);
        let left = terms.less(6 * GIB);
        let record = Record {
            available,
            compiler_reserved: 4 * GIB,
            minimum_ram: 10 * GIB,
            own_work: vec![Counted { work: compile("w"), bytes: 4 * GIB }],
            gpu: vec![
                turn("a", terms, Gpu::Granted),
                turn("b", terms.less(2 * GIB), Gpu::Granted),
                turn("c", terms.less(4 * GIB), Gpu::Head { recorded: recorded.clone() }),
            ],
            gpu_reserved: 2 * GIB,
            left,
            serviceable: serviceable(left, 4 * GIB),
            admitted: vec![Entry::Returned { bytes: 0, budget: left }],
            ..Record::default()
        };
        assert_eq!(record.gpu[2].budget, Budget::of(GIB / 2));
        replay(&record).unwrap();
        let mut judged_after = record.clone();
        judged_after.gpu[2].budget = left;
        assert!(replay(&judged_after).unwrap_err().contains("turn had"), "each claim is judged by the budget at its turn");
        let mut granted_from_head = record.clone();
        granted_from_head.gpu.push(turn("d", left, Gpu::Granted));
        assert!(replay(&granted_from_head).is_err(), "no later claim is granted from the head's bytes");
        let mut unrecoverable = record;
        unrecoverable.own_work.clear();
        unrecoverable.serviceable = serviceable(unrecoverable.left, 0);
        assert!(replay(&unrecoverable).is_err(), "the GPU head's bytes are at most its turn's budget plus D");
    }
}
