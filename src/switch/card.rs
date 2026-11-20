//! What the switch knows of the card, in which order it learned it, and what a grant may use of it (PLAN-L §3.3).
//! Every observation reaches the state through [`observe`], under the state lock, one at a time in publication
//! order; a grant reaches a sample only through [`usable`], asked at the moment of granting.

use std::time::{Duration, Instant};

use crate::gpu::{Failure, Sample};
use crate::protocol::{now_unix, DeviceLoss, Telemetry};
use crate::switch::card_reader::{Answer, Observation, Pending, Processes};
use crate::switch::config::secs;
use crate::switch::daemon::Daemon;
use crate::switch::state::{Association, State};

/// What a restart keeps of the card (`persist`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SavedCard {
    pub loss: Option<DeviceLoss>,
}

/// The four things the switch can know of the card.
#[derive(Debug, Clone)]
enum Known {
    /// No NVML library is configured: a host without a card.
    Unconfigured,
    /// Configured, with no usable sample: before the first read, or after an unanswered one.
    Unread { error: Option<String> },
    /// The usable sample, dated by its read's start.
    Readable { sample: Sample, began: Instant },
    Lost { loss: DeviceLoss, latest: String },
}

/// The card's owner, in `State.view`. Beside what it knows, never read by a grant decision: the start of the
/// last read that became usable, the failure floor, the retired generation and the next loss's number.
#[derive(Debug, Clone)]
pub struct CardState {
    known: Known,
    /// The read start of the last sample that became usable, with its wall-clock twin; kept through Unread and
    /// Lost.
    last_good: Option<(Instant, f64)>,
    /// The highest return of any applied failure; it never decreases.
    floor: Option<Instant>,
    /// The highest generation whose loss was applied; 0 at start, so generation 0's "not opened yet" is
    /// discarded too.
    retired: u64,
    /// The number the next loss of this incarnation takes, from 1.
    next_loss: u64,
}

/// What an observation told the owner.
#[derive(Debug, Clone, PartialEq)]
pub enum Heard<T> {
    Unconfigured,
    /// An applied success.
    Read(T),
    /// An applied failure, or the loss that discarded the observation.
    Failed(Failure),
    /// An applied panic.
    Panicked(String),
    /// Discarded while the card is not lost: a failure applied before it overtook it.
    Superseded,
}

/// Whether the card is usable for a new grant, now.
#[derive(Debug)]
pub enum Usable<'a> {
    /// The usable sample; none on a host without a card.
    Card(Option<&'a Sample>),
    Unanswered(String),
    Lost(String),
}

/// What keeps admission closed whatever the state holds: a failed read not yet applied, or a read outstanding
/// past the deadline.
enum Closure {
    Failed,
    Outstanding(Duration),
}

impl Closure {
    fn of(pending: Pending, now: Instant, deadline: Duration) -> Option<Closure> {
        let age = pending.outstanding.map(|since| now.saturating_duration_since(since));
        match (pending.failed, age) {
            (true, _) => Some(Closure::Failed),
            (false, Some(age)) if age > deadline => Some(Closure::Outstanding(age)),
            (false, _) => None,
        }
    }

    fn reason(&self) -> String {
        match self {
            Closure::Failed => "waiting for gpu-switch to apply a failed GPU read".into(),
            Closure::Outstanding(age) => format!("waiting for a GPU read outstanding {} s", age.as_secs()),
        }
    }
}

/// What a successful read gives the owner: a sample becomes the usable one; a process list changes nothing.
pub trait Reading: Clone {
    fn applied(&self, d: &Daemon, card: &mut CardState, began: Instant, began_unix: f64);
}

impl Reading for Sample {
    fn applied(&self, d: &Daemon, card: &mut CardState, began: Instant, began_unix: f64) {
        card.readable(d, self.clone(), began, began_unix);
    }
}

impl Reading for Processes {
    fn applied(&self, _: &Daemon, _: &mut CardState, _: Instant, _: f64) {}
}

impl CardState {
    pub fn new(configured: bool) -> CardState {
        CardState {
            known: match configured {
                true => Known::Unread { error: None },
                false => Known::Unconfigured,
            },
            last_good: None,
            floor: None,
            retired: 0,
            next_loss: 1,
        }
    }

    /// Departure freshness: a usable sample begun after `left`, or no card to read.
    pub fn fresh_after(&self, left: Option<Instant>) -> bool {
        match &self.known {
            Known::Unconfigured => true,
            Known::Readable { began, .. } => left.is_none_or(|left| *began > left),
            Known::Unread { .. } | Known::Lost { .. } => false,
        }
    }

    /// What a restart keeps of the card: on a configured host, the loss it is in.
    pub fn saved(&self) -> Option<SavedCard> {
        match &self.known {
            Known::Unconfigured => None,
            Known::Unread { .. } | Known::Readable { .. } | Known::Lost { .. } => Some(SavedCard {
                loss: self.loss().cloned(),
            }),
        }
    }

    /// Restores what a predecessor saved, under the current configuration: an unconfigured host restores nothing,
    /// and a loss saved in another boot says nothing about the card in this one. A restored loss is the one its
    /// predecessor published, with its identity and times.
    pub fn restore(&mut self, saved: Option<SavedCard>, same_boot: bool) {
        let restored = saved.and_then(|saved| saved.loss).filter(|_| same_boot);
        match (&self.known, restored) {
            (Known::Unread { .. }, Some(loss)) => {
                let latest = loss.cause.clone();
                self.known = Known::Lost { loss, latest };
            }
            (Known::Unconfigured | Known::Unread { .. } | Known::Readable { .. } | Known::Lost { .. }, _) => {}
        }
    }

    pub fn loss(&self) -> Option<&DeviceLoss> {
        match &self.known {
            Known::Lost { loss, .. } => Some(loss),
            Known::Unconfigured | Known::Unread { .. } | Known::Readable { .. } => None,
        }
    }

    /// The status summary: `rest`, today's summary, behind the loss while the card is lost.
    pub fn summary(&self, rest: String) -> String {
        match &self.known {
            Known::Lost { loss, latest } => format!(
                "GPU LOST, found at {}: {latest}; no lease is granted and NInfer is not started until gpu-switch reads \
                 the card again. {rest}",
                crate::local_time::text(loss.observed)
            ),
            Known::Unconfigured | Known::Unread { .. } | Known::Readable { .. } => rest,
        }
    }

    /// The compute processes of the usable sample.
    pub fn processes(&self) -> usize {
        match &self.known {
            Known::Readable { sample, .. } => sample.compute.len(),
            Known::Unconfigured | Known::Unread { .. } | Known::Lost { .. } => 0,
        }
    }

    fn usable_at(&self, pending: Pending, now: Instant, deadline: Duration) -> Usable<'_> {
        match (&self.known, Closure::of(pending, now, deadline)) {
            (Known::Unconfigured, _) => Usable::Card(None),
            (Known::Lost { loss, .. }, _) => Usable::Lost(loss.reason()),
            (_, Some(closure)) => Usable::Unanswered(closure.reason()),
            (Known::Readable { sample, .. }, None) => Usable::Card(Some(sample)),
            (Known::Unread { error }, None) => Usable::Unanswered(match error {
                Some(error) => format!("waiting for a GPU sample: the last read failed: {error}"),
                None => "waiting for a GPU sample".into(),
            }),
        }
    }

    fn telemetry_at(&self, pending: Pending, now: Instant, deadline: Duration) -> Telemetry {
        let unavailable = |error: String| Telemetry::Unavailable { error };
        match (&self.known, Closure::of(pending, now, deadline)) {
            (Known::Unconfigured, _) => Telemetry::default(),
            (Known::Lost { loss, latest }, _) => unavailable(format!(
                "found lost at {}: {latest}",
                crate::local_time::text(loss.observed)
            )),
            (_, Some(Closure::Failed)) => unavailable(Closure::Failed.reason()),
            (_, Some(closure)) => unavailable(match self.last_good {
                Some((_, began)) => format!(
                    "{}; the last usable sample began at {}",
                    closure.reason(),
                    crate::local_time::text(began)
                ),
                None => closure.reason(),
            }),
            (Known::Readable { sample, .. }, None) => Telemetry::Read(sample.card.clone()),
            (Known::Unread { error: Some(error) }, None) => unavailable(error.clone()),
            (Known::Unread { error: None }, None) => Telemetry::default(),
        }
    }

    /// Rule 4: a discarded observation is answered from the current state.
    fn discarded<T>(&self) -> Heard<T> {
        match &self.known {
            Known::Lost { loss, .. } => Heard::Failed(Failure::Lost(loss.cause.clone())),
            Known::Unconfigured | Known::Unread { .. } | Known::Readable { .. } => Heard::Superseded,
        }
    }

    /// An applied failure raises the floor; it never lowers it.
    fn failed_at(&mut self, returned: Instant) {
        self.floor = Some(self.floor.map_or(returned, |floor| floor.max(returned)));
    }

    /// While lost, a new latest error is written once; consecutive duplicates are not.
    fn still_lost(&mut self, error: &str) {
        if let Known::Lost { latest, .. } = &mut self.known {
            if latest != error {
                eprintln!("gpu-switch: the GPU is still lost: {error}");
                *latest = error.to_string();
            }
        }
    }

    /// An unanswered failure or a panic: no usable sample, and no loss.
    fn unanswered(&mut self, returned: Instant, error: &str) {
        self.failed_at(returned);
        match &self.known {
            Known::Readable { .. } => eprintln!("gpu-switch: GPU read failed: {error}"),
            Known::Unread { .. } | Known::Lost { .. } | Known::Unconfigured => {}
        }
        match &self.known {
            Known::Lost { .. } => self.still_lost(error),
            Known::Unread { .. } | Known::Readable { .. } | Known::Unconfigured => {
                self.known = Known::Unread {
                    error: Some(error.to_string()),
                }
            }
        }
    }

    /// An applied sample is the usable one; from `Lost` it is the recovery (PLAN-L §3.5).
    fn readable(&mut self, d: &Daemon, sample: Sample, began: Instant, began_unix: f64) {
        match &self.known {
            Known::Lost { loss, .. } => {
                eprintln!(
                    "gpu-switch: the GPU is read again after {:.1} s lost (found at {}); the same card {}",
                    began_unix - loss.observed,
                    crate::local_time::text(loss.observed),
                    sample.identity
                );
                let detail = serde_json::json!({"observed": loss.observed, "lost_secs": began_unix - loss.observed,
                    "uuids": sample.identity.uuids()});
                if let Err(error) = d.journal.card("card_read_again", loss, detail) {
                    eprintln!("gpu-queue: {error}");
                }
                d.keep();
            }
            Known::Unread { error: Some(_) } => eprintln!("gpu-switch: GPU read again"),
            Known::Unread { error: None } | Known::Readable { .. } | Known::Unconfigured => {}
        }
        self.last_good = Some((began, began_unix));
        self.known = Known::Readable { sample, began };
    }
}

/// Whether the card is usable for a new grant, at this call: the reader's pending reads and the clock are read
/// here, under the state lock its caller holds.
pub fn usable<'s>(d: &Daemon, s: &'s State) -> Usable<'s> {
    s.view
        .card
        .usable_at(d.card.pending(), Instant::now(), secs(d.config.gpu_read_deadline_secs))
}

/// The card as a viewer is shown it, from the same state and pending reads as [`usable`].
pub fn telemetry(d: &Daemon, s: &State) -> Telemetry {
    s.view
        .card
        .telemetry_at(d.card.pending(), Instant::now(), secs(d.config.gpu_read_deadline_secs))
}

/// The one way an observation reaches the state (PLAN-L §3.3, rules 1 to 6).
pub fn observe<T: Reading>(d: &Daemon, s: &mut State, observation: Observation<T>) -> Heard<T> {
    let card = &s.view.card;
    let succeeded = matches!(observation.answer, Answer::Read(_));
    let discarded = !matches!(observation.answer, Answer::Unconfigured)
        && (observation.generation <= card.retired
            || (succeeded && card.floor.is_some_and(|floor| observation.began <= floor)));
    let heard = match discarded {
        true => card.discarded(),
        false => apply(d, s, &observation),
    };
    match observation.answer {
        Answer::Failed(_) | Answer::Panicked(_) => d.card.applied(&observation),
        Answer::Unconfigured | Answer::Read(_) => {}
    }
    heard
}

/// Rule 3: the transition table.
fn apply<T: Reading>(d: &Daemon, s: &mut State, observation: &Observation<T>) -> Heard<T> {
    match &observation.answer {
        Answer::Unconfigured => Heard::Unconfigured,
        Answer::Read(value) => {
            value.applied(d, &mut s.view.card, observation.began, observation.began_unix);
            Heard::Read(value.clone())
        }
        Answer::Failed(Failure::Unanswered(error)) => {
            s.view.card.unanswered(observation.returned, error);
            Heard::Failed(Failure::Unanswered(error.clone()))
        }
        Answer::Panicked(payload) => {
            s.view.card.unanswered(observation.returned, payload);
            Heard::Panicked(payload.clone())
        }
        Answer::Failed(Failure::Lost(cause)) => {
            lost(d, s, observation, cause);
            Heard::Failed(Failure::Lost(cause.clone()))
        }
    }
}

/// An applied loss retires its generation; from `Unread` or `Readable` it enters `Lost` (PLAN-L §3.4.1).
fn lost<T>(d: &Daemon, s: &mut State, observation: &Observation<T>, cause: &str) {
    let card = &mut s.view.card;
    card.failed_at(observation.returned);
    card.retired = card.retired.max(observation.generation);
    if let Known::Lost { .. } = card.known {
        card.still_lost(cause);
        return;
    }
    let loss = DeviceLoss {
        incarnation: d.incarnation.clone(),
        number: card.next_loss,
        last_good_began: card.last_good.map(|(_, began)| began),
        read_began: observation.began_unix,
        observed: observation.returned_unix,
        published: now_unix(),
        cause: cause.to_string(),
    };
    card.next_loss += 1;
    let marked = mark(d, s, &loss);
    eprintln!(
        "gpu-switch: the GPU was found lost at {} ({}): {cause}; marked: {}",
        crate::local_time::text(loss.observed),
        loss.last_good_began.map_or_else(
            || "no read since gpu-switch started was usable".to_string(),
            |began| format!("last good read began {}", crate::local_time::text(began))
        ),
        match marked.is_empty() {
            true => "none".to_string(),
            false => marked.join(", "),
        }
    );
    s.view.card.known = Known::Lost {
        loss,
        latest: cause.to_string(),
    };
    d.keep();
}

/// Links `loss` to every open grant and journals it: one `card_lost` record with the handles by association, and
/// one `device_lost` record per affected run. Answers the marked runs.
fn mark(d: &Daemon, s: &mut State, loss: &DeviceLoss) -> Vec<String> {
    let mut by = serde_json::json!({"marked": [], "already_marked": [], "released": []});
    let mut affected = Vec::new();
    let mut marked = Vec::new();
    for job in s.sides.values_mut().flat_map(|side| side.jobs.values_mut()) {
        let association = job.device_lost(loss);
        let key = match association {
            Association::Marked => "marked",
            Association::AlreadyMarked => "already_marked",
            Association::Ended => "released",
            Association::NotHeld => continue,
        };
        by[key].as_array_mut().expect("a list").push(serde_json::json!(job.handle));
        match association {
            Association::Marked => {
                affected.push((job.side, job.id));
                marked.push(format!("{} run {}", job.handle, job.runs));
            }
            Association::AlreadyMarked => affected.push((job.side, job.id)),
            Association::Ended | Association::NotHeld => {}
        }
    }
    if let Err(error) = d.journal.card("card_lost", loss, by) {
        eprintln!("gpu-queue: {error}");
    }
    for (side, id) in affected {
        let job = s.job(side, id).expect("an affected job");
        if let Err(error) = d.journal.device_lost(job, job.runs, loss) {
            eprintln!("gpu-queue: {error}");
        }
    }
    marked
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::switch::card_reader::tests::{lost, timeout, Act, Call, Script};
    use crate::switch::control::tests::{incarnation, reading, StateDir};
    use std::sync::Arc;

    /// A successful sample of generation 1, begun at `began`, as the poller would publish it.
    pub(crate) fn sampled(sample: Sample, began: Instant) -> Observation<Sample> {
        let unix = now_unix();
        Observation {
            read: 0,
            generation: 1,
            began,
            returned: began,
            began_unix: unix,
            returned_unix: unix,
            answer: Answer::Read(sample),
        }
    }

    /// The start of the usable sample's read.
    pub(crate) fn readable_began(s: &State) -> Option<Instant> {
        match &s.view.card.known {
            Known::Readable { began, .. } => Some(*began),
            Known::Unconfigured | Known::Unread { .. } | Known::Lost { .. } => None,
        }
    }

    /// The latest error while lost.
    pub(crate) fn latest(s: &State) -> String {
        match &s.view.card.known {
            Known::Lost { latest, .. } => latest.clone(),
            other => panic!("not lost: {other:?}"),
        }
    }

    /// The number the next loss would take.
    pub(crate) fn next_loss(s: &State) -> u64 {
        s.view.card.next_loss
    }

    /// The kind of what the state knows, by name.
    pub(crate) fn known(s: &State) -> &'static str {
        match s.view.card.known {
            Known::Unconfigured => "unconfigured",
            Known::Unread { .. } => "unread",
            Known::Readable { .. } => "readable",
            Known::Lost { .. } => "lost",
        }
    }

    /// Observations at constructed instants: milliseconds after one base, with wall-clock twins to match.
    struct Clock {
        base: Instant,
        unix: f64,
        read: u64,
    }

    impl Clock {
        fn new() -> Clock {
            Clock { base: Instant::now(), unix: now_unix(), read: 0 }
        }

        fn at(&self, ms: u64) -> Instant {
            self.base + Duration::from_millis(ms)
        }

        fn seen<T>(&mut self, generation: u64, began: u64, returned: u64, answer: Answer<T>) -> Observation<T> {
            self.read += 1;
            Observation {
                read: self.read,
                generation,
                began: self.at(began),
                returned: self.at(returned),
                began_unix: self.unix + began as f64 / 1000.0,
                returned_unix: self.unix + returned as f64 / 1000.0,
                answer,
            }
        }

        fn sample<T>(&mut self, generation: u64, began: u64) -> Observation<Sample> {
            self.seen(generation, began, began, Answer::Read(Sample::default()))
        }

        fn failed(&mut self, generation: u64, began: u64, returned: u64, failure: Failure) -> Observation<Sample> {
            self.seen(generation, began, returned, Answer::Failed(failure))
        }
    }

    fn daemon() -> (Arc<Daemon>, StateDir) {
        reading(serde_json::json!({}), Some(Script::new().driver()), incarnation(1))
    }

    fn observe<T: Reading>(d: &Daemon, observation: Observation<T>) -> Heard<T> {
        d.with(|s| super::observe(d, s, observation))
    }

    fn state(d: &Daemon) -> &'static str {
        d.with(|s| known(s))
    }

    fn floor(d: &Daemon) -> Option<Instant> {
        d.with(|s| s.view.card.floor)
    }

    /// Leaves the state `from` at generation 1, and answers the next instant and generation to use.
    fn enter(d: &Daemon, clock: &mut Clock, from: &str) -> (u64, u64) {
        match from {
            "unread" => (10, 1),
            "readable" => {
                observe(d, clock.sample::<()>(1, 10));
                (20, 1)
            }
            _ => {
                observe(d, clock.failed(1, 10, 11, lost()));
                (20, 2)
            }
        }
    }

    #[test]
    fn every_cell_of_the_transition_table() {
        let answers: [(&str, fn() -> Answer<Sample>); 4] = [
            ("read", || Answer::Read(Sample::default())),
            ("unanswered", || Answer::Failed(timeout())),
            ("panicked", || Answer::Panicked("a scripted panic".into())),
            ("lost", || Answer::Failed(lost())),
        ];
        let expected = [
            ("unread", ["readable", "unread", "unread", "lost"]),
            ("readable", ["readable", "unread", "unread", "lost"]),
            ("lost", ["readable", "lost", "lost", "lost"]),
        ];
        for (from, to) in expected {
            for ((name, answer), to) in answers.iter().zip(to) {
                let (d, _dir) = daemon();
                let mut clock = Clock::new();
                let (at, generation) = enter(&d, &mut clock, from);
                let heard = observe(&d, clock.seen(generation, at, at + 1, answer()));
                assert_eq!(state(&d), to, "{from} on {name}: {heard:?}");
                let applied = match (name, &heard) {
                    (&"read", Heard::Read(_)) => true,
                    (&"unanswered", Heard::Failed(Failure::Unanswered(_))) => true,
                    (&"panicked", Heard::Panicked(_)) => true,
                    (&"lost", Heard::Failed(Failure::Lost(_))) => true,
                    _ => false,
                };
                assert!(applied, "{from} on {name}: {heard:?}");
                let latest = d.with(|s| match &s.view.card.known {
                    Known::Lost { latest, .. } => Some(latest.clone()),
                    _ => None,
                });
                match (from, *name) {
                    ("lost", "panicked") => assert_eq!(latest.as_deref(), Some("a scripted panic")),
                    ("lost", "unanswered") => assert_eq!(latest, Some(timeout().text().to_string())),
                    _ => {}
                }
            }
        }
    }

    #[test]
    fn a_success_and_a_failure_that_returned_after_it_began_in_each_publication_order() {
        for first in ["success", "failure", "loss"] {
            let (d, _dir) = daemon();
            let mut clock = Clock::new();
            let success = clock.sample::<()>(1, 1);
            let failure = match first {
                "loss" => clock.failed(1, 0, 2, lost()),
                _ => clock.failed(1, 0, 2, timeout()),
            };
            match first {
                "success" => {
                    assert!(matches!(observe(&d, success), Heard::Read(_)));
                    assert_eq!(state(&d), "readable");
                    assert!(matches!(observe(&d, failure), Heard::Failed(Failure::Unanswered(_))));
                    assert_eq!(state(&d), "unread", "the failure closes the card");
                }
                "failure" => {
                    observe(&d, failure);
                    assert_eq!(observe(&d, success).map_value(), Heard::Superseded);
                    assert_eq!(state(&d), "unread");
                }
                _ => {
                    observe(&d, failure);
                    assert_eq!(observe(&d, success).map_value(), Heard::Failed(lost()));
                    assert_eq!(state(&d), "lost");
                }
            }
        }
        let (d, _dir) = daemon();
        let mut clock = Clock::new();
        observe(&d, clock.failed(1, 0, 5, timeout()));
        assert_eq!(observe(&d, clock.sample::<()>(1, 5)).map_value(), Heard::Superseded, "equal instants are rejected");
    }

    #[test]
    fn the_floor_is_a_high_water_mark_in_all_six_publication_orders() {
        let orders = [["a", "b", "s"], ["a", "s", "b"], ["b", "a", "s"], ["b", "s", "a"], ["s", "a", "b"], ["s", "b", "a"]];
        for order in orders {
            let (d, _dir) = daemon();
            let mut clock = Clock::new();
            let mut a = Some(clock.failed(1, 0, 2, timeout()));
            let mut b = Some(clock.failed(1, 0, 5, timeout()));
            let mut s = Some(clock.sample::<()>(1, 3));
            let mut applied = false;
            for name in order {
                match name {
                    "a" => drop(observe(&d, a.take().unwrap())),
                    "b" => drop(observe(&d, b.take().unwrap())),
                    _ => applied = matches!(observe(&d, s.take().unwrap()), Heard::Read(_)),
                }
            }
            let before_b = order.iter().position(|n| *n == "s") < order.iter().position(|n| *n == "b");
            assert_eq!(applied, before_b, "{order:?}");
            assert_eq!((state(&d), floor(&d)), ("unread", Some(clock.at(5))), "{order:?}");
        }
        let (d, _dir) = daemon();
        let mut clock = Clock::new();
        observe(&d, clock.failed(1, 0, 2, lost()));
        assert_eq!(observe(&d, clock.failed(1, 0, 5, timeout())).map_value(), Heard::Failed(lost()));
        assert_eq!(floor(&d), Some(clock.at(2)), "a lost A retires the generation and discards B");
    }

    #[test]
    fn a_retired_generation_never_closes_a_recovered_card_and_leaving_lost_takes_a_higher_one() {
        let (d, _dir) = daemon();
        let mut clock = Clock::new();
        observe(&d, clock.failed(1, 0, 1, lost()));
        assert_eq!(observe(&d, clock.sample::<()>(1, 5)).map_value(), Heard::Failed(lost()), "the loss answers");
        assert_eq!(state(&d), "lost");
        assert!(matches!(observe(&d, clock.sample::<()>(2, 6)), Heard::Read(_)));
        assert_eq!(state(&d), "readable");
        for late in [timeout(), lost()] {
            assert_eq!(observe(&d, clock.failed(1, 0, 9, late)).map_value(), Heard::Superseded);
            assert_eq!(state(&d), "readable");
        }
        assert!(matches!(observe(&d, clock.seen(1, 7, 7, Answer::<Sample>::Panicked("p".into()))).map_value(), Heard::Superseded));
        let processes: Processes = vec![(9, Some(1))];
        assert_eq!(observe(&d, clock.seen(2, 8, 8, Answer::Read(processes.clone()))), Heard::Read(processes));
        assert_eq!(state(&d), "readable", "a process list changes nothing");
    }

    #[test]
    fn usable_and_telemetry_answer_each_state_with_the_pending_reads() {
        let (d, _dir) = daemon();
        let mut clock = Clock::new();
        let deadline = Duration::from_millis(200);
        let none = Pending { outstanding: None, failed: false };
        let failed = Pending { outstanding: None, failed: true };
        let overdue = Pending { outstanding: Some(clock.at(0)), failed: false };
        let young = Pending { outstanding: Some(clock.at(3900)), failed: false };
        let now = clock.at(4000);
        let answer = |d: &Daemon, pending: Pending| {
            d.with(|s| match s.view.card.usable_at(pending, now, deadline) {
                Usable::Card(card) => format!("card {}", card.is_some()),
                Usable::Unanswered(text) => text,
                Usable::Lost(text) => text,
            })
        };
        assert_eq!(answer(&d, none), "waiting for a GPU sample");
        observe(&d, clock.sample::<()>(1, 10));
        assert_eq!(answer(&d, none), "card true");
        assert_eq!(answer(&d, young), "card true", "a read inside the deadline closes nothing");
        assert_eq!(answer(&d, overdue), "waiting for a GPU read outstanding 4 s");
        assert_eq!(answer(&d, failed), "waiting for gpu-switch to apply a failed GPU read", "regardless of age");
        let telemetry = d.with(|s| s.view.card.telemetry_at(overdue, now, deadline));
        let Telemetry::Unavailable { error } = telemetry else { panic!("{telemetry:?}") };
        assert!(error.starts_with("waiting for a GPU read outstanding 4 s; the last usable sample began at "), "{error}");
        observe(&d, clock.failed(1, 20, 30, timeout()));
        let unread = format!("waiting for a GPU sample: the last read failed: {}", timeout().text());
        assert_eq!(answer(&d, none), unread, "the last good sample is never returned");
        assert_eq!(answer(&d, overdue), "waiting for a GPU read outstanding 4 s");
        assert_eq!(answer(&d, failed), "waiting for gpu-switch to apply a failed GPU read");
        assert_eq!(d.with(|s| s.view.card.last_good), Some((clock.at(10), clock.unix + 0.01)), "kept through unread");
        observe(&d, clock.failed(1, 40, 50, lost()));
        let reason = d.with(|s| s.view.card.loss().unwrap().reason());
        for pending in [none, failed, overdue] {
            assert_eq!(answer(&d, pending), reason);
        }
        assert_eq!(d.with(|s| s.view.card.last_good), Some((clock.at(10), clock.unix + 0.01)), "kept through lost");
        let (unconfigured, _dir) = reading(serde_json::json!({}), None, incarnation(1));
        assert_eq!(answer(&unconfigured, failed), "card false");
        assert_eq!(unconfigured.with(|s| s.view.card.telemetry_at(none, now, deadline)), Telemetry::default());
    }

    #[test]
    fn freshness_counts_a_sample_begun_strictly_after_the_departure() {
        let (d, _dir) = daemon();
        let mut clock = Clock::new();
        assert!(!d.with(|s| s.view.card.fresh_after(None)), "unread is never fresh");
        observe(&d, clock.sample::<()>(1, 10));
        for (left, fresh) in [(None, true), (Some(9), true), (Some(10), false), (Some(11), false)] {
            let left = left.map(|ms| clock.at(ms));
            assert_eq!(d.with(|s| s.view.card.fresh_after(left)), fresh, "{left:?}");
        }
        let (unconfigured, _dir) = reading(serde_json::json!({}), None, incarnation(1));
        assert!(unconfigured.with(|s| s.view.card.fresh_after(Some(Instant::now()))));
    }

    #[test]
    fn the_lost_text_stays_the_same_while_the_latest_error_changes() {
        let (d, _dir) = daemon();
        let mut clock = Clock::new();
        observe(&d, clock.failed(1, 0, 1, lost()));
        let text = || d.with(|s| match usable(&d, s) {
            Usable::Lost(text) => text,
            other => panic!("{other:?}"),
        });
        let first = text();
        for n in 0..50 {
            observe(&d, clock.failed(2 + n, 2 + n, 3 + n, Failure::Lost(format!("error {n}"))));
            assert_eq!(text(), first);
        }
        assert!(first.contains(lost().text()) && first.contains(&crate::local_time::text(clock.unix + 0.001)));
    }

    #[test]
    fn a_panic_of_a_retired_generation_answers_the_loss_and_a_panic_raises_the_floor() {
        let (d, _dir) = daemon();
        let mut clock = Clock::new();
        observe(&d, clock.seen(1, 0, 5, Answer::<Sample>::Panicked("p".into())));
        assert_eq!(floor(&d), Some(clock.at(5)));
        assert_eq!(observe(&d, clock.sample::<()>(1, 4)).map_value(), Heard::Superseded);
        let usable = d.with(|s| match usable(&d, s) {
            Usable::Unanswered(text) => text,
            other => panic!("{other:?}"),
        });
        assert_eq!(usable, "waiting for a GPU sample: the last read failed: p");
        observe(&d, clock.failed(1, 6, 7, lost()));
        assert_eq!(observe(&d, clock.seen(1, 8, 9, Answer::<Sample>::Panicked("q".into()))).map_value(), Heard::Failed(lost()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_slow_failing_read_is_dated_by_its_start_its_return_and_its_publication_and_rule_six_acknowledges() {
        let script = Script::new();
        let (d, _dir) = reading(serde_json::json!({}), Some(script.driver()), incarnation(1));
        observe(&d, d.card.sample().await);
        for (act, kind) in [(Act::Panic("a slow panic"), "unread"), (Act::Fail(lost()), "lost")] {
            script.then(Call::Compute, Act::Hold);
            script.pause("compute");
            let read = tokio::spawn({
                let d = d.clone();
                async move { d.card.compute().await }
            });
            let held = script.held(Call::Compute).await;
            std::thread::sleep(Duration::from_millis(5));
            script.release(held, act);
            let paused = script.paused().await;
            std::thread::sleep(Duration::from_millis(5));
            paused.release();
            let observation = read.await.unwrap();
            assert!(d.card.pending().failed);
            observe(&d, observation);
            assert_eq!(state(&d), kind);
            assert!(!d.card.pending().failed, "rule 6: the owner acknowledged it");
        }
        let loss = d.with(|s| s.view.card.loss().cloned()).unwrap();
        assert!(loss.read_began < loss.observed && loss.observed < loss.published, "{loss:?}");
        assert!(loss.last_good_began.is_some_and(|began| began < loss.read_began));
        assert_eq!((loss.number, &loss.incarnation), (1, &incarnation(1)));
    }
}

#[cfg(test)]
impl<T> Heard<T> {
    /// The answer without its value, so answers of any read compare.
    pub(crate) fn map_value(self) -> Heard<()> {
        match self {
            Heard::Unconfigured => Heard::Unconfigured,
            Heard::Read(_) => Heard::Read(()),
            Heard::Failed(failure) => Heard::Failed(failure),
            Heard::Panicked(payload) => Heard::Panicked(payload),
            Heard::Superseded => Heard::Superseded,
        }
    }
}
