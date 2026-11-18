//! What an engine instance's earlier ticks leave its next record's derivation. A tick writes a record whenever the
//! turn, the head or a pending request's decision changes, so a silent tick, which writes none, cannot change those:
//! they persist across silent ticks. What a silent tick can change is unknown after one.
use super::{
    super::{budget::Budget, precedence::Precedence},
    agree::Unknown,
    ensure, Decision, Decisions, Derivation, Subject,
};
#[cfg(test)]
use super::super::precedence::Part;
use std::collections::BTreeMap;

/// What the last launches of the tick before left (`left_after_launch`).
#[derive(Clone, Copy, Debug)]
enum Remainder {
    /// No tick of the instance has launched yet.
    None,
    Known(u64),
    /// Every tick sets it, and silent ticks have since the last record.
    Unknown,
}

/// What the tick before the next record left.
pub struct Carried {
    /// The last record's tick; 0 before the instance's first.
    tick: u64,
    /// Whether a head reserved in that tick: the next tick then writes a record.
    reserving: bool,
    /// The turn, the aged head with the work it waits for, and the requests giving way, as that tick left them.
    precedence: Precedence,
    /// Whether the held head's `fits` is the one the last record left. A head reserving 0 bytes is kept through
    /// silent ticks, whose verdicts set it.
    fits_known: bool,
    remainder: Remainder,
    /// Each subject's last listed decision. `Admitted` holds only for the next tick: its subject is no longer
    /// pending after it.
    decisions: BTreeMap<Subject, Decisions>,
    /// The decisions the tick before is known to hold, which its journal compares the next tick's with: those it
    /// listed, those of the requests certainly pending, and its admissions. Silent ticks hold others.
    held: BTreeMap<Subject, Decisions>,
}

impl Carried {
    /// §3.7's state, which every instance starts from: no turn, no head, nothing giving way and no decision.
    pub fn initial() -> Self {
        Self {
            tick: 0,
            reserving: false,
            precedence: Precedence::default(),
            fits_known: true,
            remainder: Remainder::None,
            decisions: BTreeMap::new(),
            held: BTreeMap::new(),
        }
    }
    /// What the instance's record of `tick` starts from: its ticks rise, a tick in which a head reserved is followed
    /// by the next, and the ticks between were silent.
    pub fn next(self, tick: u64) -> Result<Self, String> {
        ensure(tick > self.tick, || format!("tick {tick} follows tick {} of its instance", self.tick))?;
        ensure(!self.reserving || tick == self.tick + 1, || {
            format!("tick {}: a head reserved, and the instance's next record is tick {tick}", self.tick)
        })?;
        let gap = tick - self.tick - 1;
        Ok(self.across(gap))
    }
    /// What survives `gap` silent ticks: everything whose change writes a record, the precedence among them. What
    /// the last launches left is unknown, a held head's `fits` is unknown, and an `Admitted` decision has lapsed.
    fn across(self, gap: u64) -> Self {
        match gap {
            0 => self,
            _ => Self {
                remainder: Remainder::Unknown,
                fits_known: false,
                decisions: lapsed(self.decisions),
                held: BTreeMap::new(),
                ..self
            },
        }
    }
    /// The precedence the tick before left, which the next tick changes as the service does.
    pub fn precedence(&self) -> Precedence {
        self.precedence.clone()
    }
    /// What a compile job's cell must exceed to wait for memory: the smaller of `left` and what the last launches
    /// left. Unknown after silent ticks, that is any figure of at least nothing, so the smaller of `left` and 0. None
    /// before the instance's first launches: then no compile job waits for memory.
    pub fn waiting(&self, left: Budget) -> Option<Budget> {
        match self.remainder {
            Remainder::None => None,
            Remainder::Known(after) => Some(left.min(Budget::of(after))),
            Remainder::Unknown => Some(left.min(Budget::of(0))),
        }
    }
    /// A subject's last listed decision.
    pub fn decision(&self, subject: &Subject) -> Option<&Decisions> {
        self.decisions.get(subject)
    }
    /// Whether the tick before is known to hold `decisions` for `subject`: then its journal does not list it again.
    pub fn held(&self, subject: &Subject, decisions: &Decisions) -> bool {
        self.held.get(subject) == Some(decisions)
    }
    /// The figures the next record's derivation cannot know.
    pub fn unknown(&self) -> Vec<Unknown> {
        self.precedence
            .head()
            .iter()
            .filter(|_| !self.fits_known)
            .map(|_| Unknown {
                path: "/aged/steps/0/head/fits",
                reason: "a head reserving 0 bytes is kept through silent ticks, whose verdicts set whether it fits",
            })
            .collect()
    }
    /// What `derivation`'s tick leaves the next: its precedence as the tick changed it, and its decisions.
    pub fn after(self, derivation: &Derivation) -> Self {
        let record = &derivation.record;
        let mut decisions = lapsed(self.decisions);
        decisions.extend(record.decisions.iter().map(|decided| (decided.subject.clone(), decided.decision.clone())));
        let admitted: Vec<(Subject, Decisions)> =
            record.admissions().map(|id| (Subject::from(&id), Decisions::One(Decision::Admitted))).collect();
        decisions.extend(admitted.iter().cloned());
        let mut held = derivation.held.clone();
        held.extend(admitted);
        Self {
            tick: record.tick,
            reserving: record.reserves(),
            precedence: derivation.precedence.clone(),
            fits_known: true,
            remainder: Remainder::Known(record.left_after_launch),
            decisions,
            held,
        }
    }
}

/// A fact a tick leaves the next, named for the replay's mutation table.
#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub enum Fact {
    Tick,
    Reserving,
    Precedence(Part),
    Fits,
    Remainder,
    Decisions,
    Held,
}
#[cfg(test)]
impl Carried {
    /// Every fact, by an exhaustive destructuring: a new fact does not compile until it is named here, and the
    /// mutation table's match over the names until it has rows.
    pub fn facts(&self) -> Vec<Fact> {
        let Self { tick: _, reserving: _, precedence, fits_known: _, remainder: _, decisions: _, held: _ } = self;
        let parts = precedence.parts().into_iter().map(Fact::Precedence);
        [Fact::Tick, Fact::Reserving].into_iter().chain(parts).chain([Fact::Fits, Fact::Remainder, Fact::Decisions, Fact::Held]).collect()
    }
}

/// The decisions that still hold a tick later: every one but an admission, whose subject has left the pending set.
fn lapsed(decisions: BTreeMap<Subject, Decisions>) -> BTreeMap<Subject, Decisions> {
    decisions.into_iter().filter(|(_, decisions)| *decisions != Decisions::One(Decision::Admitted)).collect()
}
