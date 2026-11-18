//! The decisions a record lists, against its derivation and what its instance's earlier ticks left. A request the
//! record describes has the decision the service gives it, listed when it changed and carried when it did not; one
//! the record does not describe has a decision only such a request can have.
use super::{
    super::precedence::{Class, Precedence, RequestId, Step, Verdict},
    carried::Carried,
    ensure, Decided, Decision, Decisions, Record, Subject,
};
use std::collections::{BTreeMap, BTreeSet};

/// What the derivation says of a subject's decision at its tick's end.
#[derive(Clone, Debug, PartialEq)]
enum Expected {
    /// This decision.
    Is(Decision),
    /// A group's decisions include this one: one of its pending jobs has it.
    Includes(Decision),
}
impl Expected {
    fn holds(&self, decisions: &Decisions) -> bool {
        match (self, decisions) {
            (Self::Is(expected) | Self::Includes(expected), Decisions::One(decision)) => expected == decision,
            (Self::Is(_), Decisions::Jobs(_)) => false,
            (Self::Includes(expected), Decisions::Jobs(counts)) => counts.contains_key(expected),
        }
    }
}

/// A request's part in its tick, by the last thing the derivation shows of it.
#[derive(Clone, Copy, Debug)]
enum Fate {
    Admitted,
    /// Still the aged head after the choice, fitting or not.
    Head(bool),
    EndedInVain,
    /// Listed, passed or released, with its bytes: it waits.
    Waiting(u64),
    /// No longer pending.
    Ended,
    /// Decided by the GPU pass; a granted claim's fate is its admission's.
    Gpu(Decision),
}

/// Each request's fate in the derived tick.
fn fates(record: &Record) -> BTreeMap<RequestId, Fate> {
    let judged = |verdict: Verdict, bytes: u64| match verdict {
        Verdict::Kept { fits } => Fate::Head(fits),
        Verdict::Ended => Fate::Ended,
        Verdict::Released | Verdict::Passed => Fate::Waiting(bytes),
        Verdict::EndedInVain => Fate::EndedInVain,
    };
    let mut fates = BTreeMap::new();
    for step in &record.aged.steps {
        match step {
            Step::Kept { head, standing, verdict } => {
                fates.insert(head.id.clone(), judged(*verdict, standing.bytes()));
            }
            Step::Chosen { listed, chosen, verdict, .. } => {
                for entry in listed {
                    fates.entry(entry.candidate.id.clone()).or_insert(Fate::Waiting(entry.candidate.bytes));
                }
                let bytes = listed.iter().find(|entry| entry.candidate.id == *chosen).map_or(0, |entry| entry.candidate.bytes);
                fates.insert(chosen.clone(), judged(*verdict, bytes));
            }
        }
    }
    fates.extend(record.gpu.iter().map(|turn| (RequestId::Claim { claim: turn.claim.clone() }, Fate::Gpu(Decision::gpu(&turn.decision)))));
    fates.extend(record.admissions().map(|id| (id, Fate::Admitted)));
    fates
}

/// What a fate says of its request's decision, the subject that holds it, and whether the request is certainly
/// pending at the tick's end. A claim that waits may be refused later in its tick, and then has none.
fn expected(id: &RequestId, fate: Fate, serviceable: u64) -> Option<(Subject, Expected, bool)> {
    let waiting = |bytes: u64| Decision::waiting(Class::of(bytes, None), bytes, serviceable, false);
    match (fate, id) {
        (Fate::Admitted, _) => Some((Subject::from(id), Expected::Is(Decision::Admitted), true)),
        (Fate::Head(fits), _) => Some((Subject::from(id), Expected::Is(Decision::head(fits)), true)),
        (Fate::Gpu(decision), _) => Some((Subject::from(id), Expected::Is(decision), true)),
        (Fate::EndedInVain, RequestId::Claim { .. }) => Some((Subject::from(id), Expected::Is(Decision::GivingWay), false)),
        (Fate::EndedInVain, RequestId::Compile { .. }) => Some((Subject::from(id), Expected::Is(Decision::GivingWay), true)),
        (Fate::Waiting(bytes), RequestId::Claim { .. }) => Some((Subject::from(id), Expected::Is(waiting(bytes)), false)),
        (Fate::Waiting(bytes), RequestId::Compile { group, .. }) => {
            Some((Subject::Group { group: group.clone() }, Expected::Includes(waiting(bytes)), true))
        }
        (Fate::Ended, _) => None,
    }
}

/// A decision only a request waiting by its bytes has: a group's jobs have no other.
fn waits(decision: &Decision) -> bool {
    matches!(decision, Decision::WaitingForTurn | Decision::NotServiceable | Decision::LargeWaiting)
}

/// The request a subject names; a group names none.
fn request(subject: &Subject) -> Option<RequestId> {
    match subject {
        Subject::Claim { claim } => Some(RequestId::Claim { claim: claim.clone() }),
        Subject::Job { group, job } => Some(RequestId::Compile {
            group: group.clone(),
            job: job.clone(),
        }),
        Subject::Group { .. } => None,
    }
}

/// Whether a subject's decisions are of its kind's form: a group's jobs wait by their bytes, each decision counted at
/// least once; a single request has one decision.
fn of_its_form(subject: &Subject, decisions: &Decisions) -> bool {
    match (subject, decisions) {
        (Subject::Group { .. }, Decisions::One(decision)) => waits(decision),
        (Subject::Group { .. }, Decisions::Jobs(counts)) => counts.iter().all(|(decision, count)| waits(decision) && *count > 0),
        (Subject::Claim { .. } | Subject::Job { .. }, Decisions::One(_)) => true,
        (Subject::Claim { .. } | Subject::Job { .. }, Decisions::Jobs(_)) => false,
    }
}

/// The decision of a subject the record does not describe: a job is listed by itself only while it gives way, the
/// head and admitted jobs being described; and a claim gives way exactly while it does, and has no decision only a
/// described request has.
fn undescribed(subject: &Subject, decisions: &Decisions, precedence: &Precedence) -> bool {
    let gives_way = request(subject).is_some_and(|id| precedence.gives_way(&id));
    match (subject, decisions) {
        (Subject::Group { .. }, _) => true,
        (Subject::Job { .. }, decisions) => gives_way && *decisions == Decisions::One(Decision::GivingWay),
        (Subject::Claim { .. }, Decisions::One(decision)) => !decision.shown() && (*decision == Decision::GivingWay) == gives_way,
        (Subject::Claim { .. }, Decisions::Jobs(_)) => false,
    }
}

/// Checks the record's listed decisions: each subject once, and each changed from the decision the tick before held
/// for it; one the record describes has the derived decision; one it does not describe has one it can have; and a
/// described request certainly pending that the record does not list kept its last listed decision, which is then
/// the derived one. Returns the decisions this tick is known to hold: those it lists, and those of the requests
/// certainly pending that it does not list.
pub fn check(record: &Record, precedence: &Precedence, carried: &Carried) -> Result<BTreeMap<Subject, Decisions>, String> {
    let tick = record.tick;
    let mut expectations: BTreeMap<Subject, Vec<(Expected, bool)>> = BTreeMap::new();
    let fates = fates(record);
    for (subject, expected, certain) in fates.iter().filter_map(|(id, fate)| expected(id, *fate, record.serviceable)) {
        expectations.entry(subject).or_default().push((expected, certain));
    }
    let mut listed = BTreeSet::new();
    for Decided { subject, decision } in &record.decisions {
        ensure(listed.insert(subject), || format!("tick {tick}: {subject:?} is decided twice"))?;
        ensure(!carried.held(subject, decision), || {
            format!("tick {tick}: {subject:?} is listed with {decision:?}, the decision the tick before held for it")
        })?;
        let described = expectations.get(subject);
        ensure(of_its_form(subject, decision) && (described.is_some() || undescribed(subject, decision, precedence)), || {
            format!("tick {tick}: {subject:?} is {decision:?}, which a request the record does not describe cannot be")
        })?;
        for (expected, _) in described.into_iter().flatten() {
            ensure(expected.holds(decision), || {
                format!("tick {tick}: {subject:?} is {decision:?}, and its derivation says {expected:?}")
            })?;
        }
    }
    let mut held: BTreeMap<Subject, Decisions> =
        record.decisions.iter().map(|decided| (decided.subject.clone(), decided.decision.clone())).collect();
    for (subject, expected) in &expectations {
        for (expected, _) in expected.iter().filter(|(_, certain)| *certain).filter(|_| !listed.contains(subject)) {
            let last = carried.decision(subject);
            ensure(last.is_some_and(|last| expected.holds(last)), || {
                format!("tick {tick}: {subject:?}'s derivation says {expected:?}, its last listed decision is {last:?}, and the record does not list a change")
            })?;
            held.extend(last.map(|last| (subject.clone(), last.clone())));
        }
    }
    Ok(held)
}

