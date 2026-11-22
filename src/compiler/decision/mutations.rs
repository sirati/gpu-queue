//! The mutation table (DECISION-B-IMPL-R5 ruling 3, DECISION-B-IMPL-R6 ruling 3): every serialized leaf of the
//! admission record mutated alone in a valid journal, and each carried fact and each kind of admission across a
//! consecutive tick, a silent gap and an instance boundary. The checker rejects each mutation, or its row says why
//! the mutation cannot change any admission.
//!
//! The inventory is derived, not typed: the test computes the leaf paths of its base journals (`Normalizer`), and fails
//! on a leaf path without a row or a row whose path does not occur. A figure, which the projection does not hold, has
//! its row from its path: changed, `agree` refuses it there. Every variant of every record-schema enum
//! (`form::enums`, review round 7), by the discriminator serde writes for it, must occur in a base journal, or be a
//! variant no record holds, with why and a row writing it that the checker refuses (`unwritten`); a unit variant is a
//! leaf path of its own, `path=variant`. The carried rows are keyed by an exhaustive destructuring of what
//! a tick leaves the next (`Carried::facts`), and the admissions' by an exhaustive match over what admits a request
//! (`admission`). With `R5_MUTATION_TABLE` set, the test prints each row and what the checker said.
use super::super::precedence::{At, Attempt, Candidate, Capacity, Catalogue, Choice, Registration, Lacks, Listed, Part, Standing, Step, Turn, Units, Verdict};
use super::carried::{Carried, Fact};
use super::form::Typed;
use super::tests::*;
use super::{derive_journal, numbered, serviceable, Counted, Decision, Decisions, Entry, Journal, Launching, Persisting, Projected, Record, Subject};
use super::super::{budget::Budget, instance::Instance, precedence::Class};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

/// The base journal's records, by their place in it.
const J1: usize = 0;
const J2: usize = 1;
const B1: usize = 2;
const J3: usize = 3;
const J4: usize = 4;

/// J1, instance A's tick 7, after six silent ticks: 4 GiB left beside D's 4 GiB compile w and 1 GiB host-only claim
/// k, S 9 GiB. p's 3 GiB compile job g/c, q's older 5 GiB claim h and s's 1 GiB claim w2 are listed; h is chosen
/// and reserves, and c runs ahead into its bytes.
fn j1() -> Record {
    let mut record = listed_launch(3 * GIB);
    record.own_work.push(Counted::Claim { grant: Registration { claim: "k".into(), order: 5 }, bytes: GIB });
    record.serviceable = serviceable(record.left, record.own());
    let live = record.live();
    record.aged.head.as_mut().unwrap().recorded = live;
    if let Step::Chosen { listed, .. } = &mut record.aged.steps[0] {
        let candidate = Candidate { id: claim("w2"), party: "s".into(), eligible_at: At(5.0), catalogue: super::super::precedence::Catalogue::Claim { order: 4 }, bytes: GIB, lacks: Lacks::Nothing };
        listed.push(Listed { party: "s".into(), candidate });
    }
    first(record, &[(claim("h"), Decision::AgedHead), (claim("w2"), Decision::WaitingForTurn)])
}

/// J2, tick 8: c runs, so D counts w, k and c; h is kept, waiting for c too, and reserves again.
fn j2(j1: &Record) -> Record {
    let left = Budget::of(4 * GIB);
    let mut own_work = j1.own_work.clone();
    own_work.push(Counted::Compile { attempt: Attempt { job: "c".into(), started_at: At(1.0) }, bytes: 3 * GIB, finish: 60.0 });
    let mut head = j1.aged.head.clone().unwrap();
    head.recorded.insert(compile("c"));
    let held = Turn { party: "q".into(), held: true };
    Record {
        tick: 8,
        sampled_at: 61.0,
        serviceable: serviceable(left, 8 * GIB),
        own_work,
        aged: Choice {
            before: Some(held.clone()),
            steps: vec![Step::Kept { head: head.clone(), standing: Standing::of(true, 5 * GIB, Lacks::Nothing), verdict: Verdict::Kept { fits: false } }],
            after: Some(held),
            head: Some(head),
        },
        admitted: served(claim("h"), 5 * GIB, left),
        slots: 0,
        left_after_launch: 0,
        decisions: Vec::new(),
        ..j1.clone()
    }
}

/// J3, tick 9: 11 GiB left, so h fits and is granted at once; party r's compiler turn keeps 2 GiB, and s's 1 GiB
/// claim y is granted from what is left.
fn j3(j2: &Record) -> Record {
    let left = Budget::of(11 * GIB);
    let head = j2.aged.head.clone().unwrap();
    let held = Turn { party: "q".into(), held: true };
    let grant = |claim: &str, party: &str, bytes: u64, budget: Budget, head: bool| Entry::Granted {
        phase: Persisting::Serve,
        claim: claim.into(),
        order: order(claim),
        party: party.into(),
        class: Class::Ordinary,
        bytes,
        budget,
        head,
    };
    let mut admitted = vec![
        grant("h", "q", 5 * GIB, left, true),
        Entry::Turn { party: "r".into(), bytes: 2 * GIB, budget: Budget::of(6 * GIB) },
        grant("y", "s", GIB, Budget::of(4 * GIB), false),
        Entry::Returned { bytes: 2 * GIB, budget: Budget::of(3 * GIB) },
    ];
    admitted.extend(completions(63.0, Budget::of(5 * GIB)));
    let record = Record {
        tick: 9,
        sampled_at: 62.0,
        available: 25 * GIB,
        serviceable: serviceable(left, 8 * GIB),
        left,
        aged: Choice {
            before: Some(held.clone()),
            steps: vec![Step::Kept { head: head.clone(), standing: Standing::of(true, 5 * GIB, Lacks::Nothing), verdict: Verdict::Kept { fits: true } }],
            after: Some(held),
            head: Some(super::super::precedence::Head { fits: true, ..head }),
        },
        aged_reserved: 0,
        admitted,
        left_after_launch: 5 * GIB,
        ..j2.clone()
    };
    first(record, &[])
}

/// J4, tick 11, after the silent tick 10: q's turn passed with its head; 36 GiB left beside D's w, no candidate; a
/// 12 GiB large compile g/big launches, and t's 11 GiB large claim L is granted from what it left. Four subjects the
/// record does not describe are decided: group g, claim z, a large claim M, and group m, whose pending jobs wait for
/// three reasons.
fn j4(j3: &Record) -> Record {
    let left = Budget::of(36 * GIB);
    let passed = Turn { party: "q".into(), held: false };
    let record = Record {
        tick: 11,
        sampled_at: 70.0,
        available: 50 * GIB,
        own_work: vec![counted_w(61.0)],
        left,
        serviceable: serviceable(left, 4 * GIB),
        aged: Choice { before: Some(passed.clone()), steps: Vec::new(), after: Some(passed), head: None },
        admitted: vec![
            Entry::Returned { bytes: 0, budget: left },
            Entry::Persisted { phase: Persisting::Serve, at: 70.0, budget: left },
            Entry::Persisted { phase: Persisting::Close, at: 70.0, budget: left },
            Entry::Launched {
                phase: Launching::LargeLaunch,
                group: "g".into(),
                job: "big".into(),
                class: Class::Large,
                bytes: 12 * GIB,
                budget: left,
                reserved: 0,
                head: false,
                ahead: None,
                admitted_at: 70.0,
                declared: Some(30),
                settled_at: 70.5,
                running: true,
            },
            Entry::Granted { phase: Persisting::LargeClose, claim: "L".into(), order: order("L"), party: "t".into(), class: Class::Large, bytes: 11 * GIB, budget: Budget::of(24 * GIB), head: false },
            Entry::Persisted { phase: Persisting::LargeClose, at: 70.5, budget: Budget::of(13 * GIB) },
        ],
        slots: 1,
        left_after_launch: 36 * GIB,
        ..j3.clone()
    };
    let pending = BTreeMap::from([
        (Subject::Group { group: "g".into() }, Decisions::One(Decision::NotServiceable)),
        (Subject::Claim(registration("z")), Decisions::One(Decision::WaitingForTurn)),
        (Subject::Claim(registration("M")), Decisions::One(Decision::LargeWaiting)),
        (
            Subject::Group { group: "m".into() },
            Decisions::Jobs(BTreeMap::from([(Decision::WaitingForTurn, 1), (Decision::NotServiceable, 2), (Decision::LargeWaiting, 1)])),
        ),
    ]);
    decided(record, pending)
}

/// The base journal: instance A's J1 to J4, with instance B's tick 3 between J2 and J3.
pub(super) fn base() -> Vec<Value> {
    let j1 = j1();
    let j2 = j2(&j1);
    let j3 = j3(&j2);
    let j4 = j4(&j3);
    let b1 = Record { tick: 3, ..b8(other(2)) };
    numbered(&[j1, j2, b1, j3, j4])
}

enum Expect<S = &'static str> {
    /// The checker refuses it with this.
    Rejected(S),
    /// It is accepted: why it cannot change any admission.
    Irrelevant(S),
}
use Expect::{Irrelevant, Rejected};
impl Expect {
    fn owned(&self) -> Expect<String> {
        match self {
            Rejected(rule) => Rejected((*rule).to_owned()),
            Irrelevant(reason) => Irrelevant((*reason).to_owned()),
        }
    }
}

struct Row {
    /// The normalized serialized path it mutates (`nodes`); the empty path is the journal's records themselves.
    path: &'static str,
    mutation: &'static str,
    edit: fn(&mut Vec<Value>),
    expect: Expect,
}
const fn row(path: &'static str, mutation: &'static str, edit: fn(&mut Vec<Value>), expect: Expect) -> Row {
    Row { path, mutation, edit, expect }
}

/// The capacity of J1's listed compile job c.
fn c_capacity(journal: &mut [Value]) -> &mut Value {
    &mut journal[J1]["aged"]["steps"][0]["listed"][0]["candidate"]["capacity"]
}
/// J1's listed entry `index`: c, h, w2.
fn j1_listed(journal: &mut [Value], index: usize) -> &mut Value {
    &mut journal[J1]["aged"]["steps"][0]["listed"][index]
}
/// J1's launch of c.
fn c_launch(journal: &mut [Value]) -> &mut Value {
    &mut journal[J1]["admitted"][3]
}
const G: u64 = GIB;

/// The rows for the fields B-2′ records.
fn rows() -> Vec<Row> {
    vec![
        // The instance and the tick: what scopes and orders carried facts.
        row("instance.boot_id", "J2's boot id another", |j| j[J2]["instance"]["boot_id"] = json!("x"), Rejected("tick 8: the record judges a head the tick before left, and that tick left none")),
        row("instance.pid", "J2's pid another", |j| j[J2]["instance"]["pid"] = json!(9), Rejected("tick 8: the record judges a head the tick before left, and that tick left none")),
        row("instance.start_ticks", "J2's start ticks another", |j| j[J2]["instance"]["start_ticks"] = json!(9), Rejected("tick 8: the record judges a head the tick before left, and that tick left none")),
        row("aged.steps[][step=chosen].listed[].candidate.id.claim.order", "J1's w2 of registration 4 → 6", |j| j1_listed(j, 2)["candidate"]["id"]["claim"]["order"] = json!(6), Rejected("tick 7: candidate w2's catalogue order 4 is not its registration's, 6")),
        row("aged.steps[][step=chosen].listed[].candidate.catalogue.claim.order", "J1's w2 catalogue order 4 → 6", |j| j1_listed(j, 2)["candidate"]["catalogue"]["claim"]["order"] = json!(6), Rejected("tick 7: candidate w2's catalogue order 6 is not its registration's, 4")),
        row("aged.steps[][step=chosen].chosen.claim.order", "J1's chosen h of registration 1 → 2", |j| j[J1]["aged"]["steps"][0]["chosen"]["claim"]["order"] = json!(2), Rejected("tick 7: h was chosen, the turn's walk gives h")),
        row("decisions[].subject.claim.order", "J1's w2 decision for registration 4 → 5", |j| { for decided in j[J1]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"claim": {"claim": "w2", "order": 4}}) { decided["subject"]["claim"]["order"] = json!(5); } } }, Irrelevant("w2 of registration 4 is listed, so its decision is possible but not certain to be listed (a listed claim can be refused later in its tick), and registration 5 of w2 is described nowhere: WaitingForTurn is a decision such a claim can have")),
        row("admitted[][event=granted].order", "J3's y of registration 6 → 7", |j| j[J3]["admitted"][2]["order"] = json!(7), Rejected("tick 9: Claim(Registration { claim: \"y\", order: 6 }) is One(Admitted), which a request the record does not describe cannot be")),
        row("number", "J2's number 2 → 3", |j| j[J2]["number"] = json!(3), Irrelevant("a record's number is its place among its instance's written records, a diagnostic; no rule reads it (DECISION-PA-5 ruling 3)")),
        // The budget's terms.
        row("sampled_at", "J1's 60 → 20: c not aged", |j| j[J1]["sampled_at"] = json!(20.0), Rejected("tick 7: c is listed before it aged by the tick's sample at 20")),
        row("available", "J1's +1 byte", |j| j[J1]["available"] = json!(18 * G + 1), Rejected("tick 7: the record's /admitted/0/budget")),
        row("compiler_reserved", "J1's +1 byte", |j| j[J1]["compiler_reserved"] = json!(4 * G + 1), Rejected("tick 7: the record's /admitted/0/budget")),
        row("minimum_ram", "J1's −1 byte", |j| j[J1]["minimum_ram"] = json!(10 * G - 1), Rejected("tick 7: the record's /admitted/0/budget")),
        row("claims_unused", "J1's 0 → 1 GiB", |j| j[J1]["claims_unused"] = json!(G), Rejected("tick 7: the record's /admitted/0/budget")),
        // D.
        row("own_work", "J1's w counted twice", |j| { let w = j[J1]["own_work"][0].clone(); j[J1]["own_work"].as_array_mut().unwrap().push(w) }, Rejected("tick 7: D counts a piece of work twice")),
        row("own_work", "J1's w removed", |j| { j[J1]["own_work"].as_array_mut().unwrap().remove(0); }, Rejected("tick 7: c of 3221225472 bytes launched beyond its budget")),
        row("own_work[].compile.attempt.job", "J1's w → v", |j| j[J1]["own_work"][0]["compile"]["attempt"]["job"] = json!("v"), Rejected("tick 7: the record's /aged/head/recorded")),
        row("own_work[].compile.attempt.started_at", "J1's w started 1 → 2", |j| j[J1]["own_work"][0]["compile"]["attempt"]["started_at"] = json!(2.0), Rejected("tick 7: the record's /aged/head/recorded")),
        row("own_work[].compile.bytes", "J1's w 4 → 5 GiB", |j| j[J1]["own_work"][0]["compile"]["bytes"] = json!(5 * G), Rejected("tick 7: the record's /serviceable is 9663676416, its derivation 10737418240")),
        row("own_work[].compile.finish", "J1's w 61 → 59: c would finish after the deadline", |j| j[J1]["own_work"][0]["compile"]["finish"] = json!(59.0), Rejected("tick 7: c of 3221225472 bytes launched beyond its budget")),
        row("own_work[].compile.finish", "J1's w 61 → 62: a later deadline", |j| j[J1]["own_work"][0]["compile"]["finish"] = json!(62.0), Rejected("tick 7: the record's /admitted/3/ahead/deadline is 61.0, its derivation 62.0")),
        row("own_work[].compile.finish", "J1's w's finish removed", |j| { j[J1]["own_work"][0]["compile"].as_object_mut().unwrap().remove("finish"); }, Rejected("missing field `finish`")),
        row("own_work[].claim.grant.claim", "J1's k → k2", |j| j[J1]["own_work"][1]["claim"]["grant"]["claim"] = json!("k2"), Rejected("tick 7: the record's /aged/head/recorded")),
        row("own_work[].claim.grant.order", "J1's k order 5 → 6", |j| j[J1]["own_work"][1]["claim"]["grant"]["order"] = json!(6), Rejected("tick 7: the record's /aged/head/recorded")),
        row("own_work[].claim.bytes", "J1's k 1 → 2 GiB", |j| j[J1]["own_work"][1]["claim"]["bytes"] = json!(2 * G), Rejected("tick 7: the record's /serviceable")),
        row("own_work[].claim", "J1's k carrying a finish", |j| j[J1]["own_work"][1]["claim"]["finish"] = json!(70.0), Rejected("unknown field `finish`")),
        // Figures of the budget.
        row("draining", "J1 draining", |j| j[J1]["draining"] = json!(true), Rejected("tick 7: compile job c is listed while the engine drains")),
        row("draining", "J4 draining", |j| j[J4]["draining"] = json!(true), Rejected("tick 11: big launched while the engine drains")),
        // The choice.
        row("aged.steps", "J1's choice removed", |j| { j[J1]["aged"]["steps"].as_array_mut().unwrap().clear(); }, Rejected("tick 7: entry 0: the held head's grant at once or its reservation is the first admission entry, and only it")),
        row("aged.steps", "J1's choice taken twice", |j| { let step = j[J1]["aged"]["steps"][0].clone(); j[J1]["aged"]["steps"].as_array_mut().unwrap().push(step) }, Rejected("tick 7: a choice while a head is held")),
        row("aged.steps[][step=chosen]", "J1's chosen step named kept", |j| j[J1]["aged"]["steps"][0]["step"] = json!("kept"), Rejected("unreadable admission record")),
        row("aged.steps[][step=kept].standing.pending", "J2's h ended", |j| j[J2]["aged"]["steps"][0]["standing"]["pending"] = json!(false), Rejected("tick 8: the head h was Kept { fits: false }, its figures say Ended")),
        row("aged.steps[][step=kept].standing.bytes", "J2's h 5 → 6 GiB", |j| j[J2]["aged"]["steps"][0]["standing"]["bytes"] = json!(6 * G), Rejected("tick 8: the record's /admitted/0/bytes")),
        row("aged.steps[][step=kept].standing.capacity", "J2's claim h carrying a capacity", |j| j[J2]["aged"]["steps"][0]["standing"]["capacity"] = j[J1]["aged"]["steps"][0]["listed"][0]["candidate"]["capacity"].clone(), Rejected("tick 8: claim h carries a capacity")),
        row("aged.steps[][step=kept].verdict.kept.fits", "J2's h fits", |j| j[J2]["aged"]["steps"][0]["verdict"] = json!({"kept": {"fits": true}}), Rejected("tick 8: the head h was Kept { fits: true }, its figures say Kept { fits: false }")),
        row("aged.steps[][step=chosen].listed", "J1 without h", |j| { j[J1]["aged"]["steps"][0]["listed"].as_array_mut().unwrap().remove(1); }, Rejected("tick 7: h was chosen, the turn's walk gives c")),
        row("aged.steps[][step=chosen].listed", "J1 without p's c", |j| { j[J1]["aged"]["steps"][0]["listed"].as_array_mut().unwrap().remove(0); }, Irrelevant("the record holds c's eligibility and its wait for memory only in its listing entry; without it c's candidacy is unknown (its launch needs none), and the walk with no turn still gives the oldest, h")),
        row("aged.steps[][step=chosen].listed", "J1's p and q swapped", |j| { j[J1]["aged"]["steps"][0]["listed"].as_array_mut().unwrap().swap(0, 1); }, Rejected("tick 7: the listed parties are distinct and in name order")),
        row("aged.steps[][step=chosen].listed[].party", "J1's c listed for party pp", |j| j1_listed(j, 0)["party"] = json!("pp"), Rejected("tick 7: c is listed but is no serviceable ordinary candidate")),
        row("aged.steps[][step=chosen].listed[].candidate.id.compile.job", "J1's c → g/c2: its group's decision is not listed", |j| j1_listed(j, 0)["candidate"]["id"]["compile"]["job"] = json!("c2"), Rejected("Group { group: \"g\" }'s derivation says Includes(WaitingForTurn), its last listed decision is None")),
        row("aged.steps[][step=chosen].listed[].candidate.id", "J1's c a claim c", |j| j1_listed(j, 0)["candidate"]["id"] = json!({"claim": {"claim": "c", "order": 9}}), Rejected("candidate c has a catalogue place of another kind")),
        row("aged.steps[][step=chosen].listed[].candidate.id.compile.group", "J1's c listed in group h2: that group's decision is not listed", |j| j1_listed(j, 0)["candidate"]["id"]["compile"]["group"] = json!("h2"), Rejected("Group { group: \"h2\" }'s derivation says Includes(WaitingForTurn), its last listed decision is None")),
        row("aged.steps[][step=chosen].listed[].candidate.id.claim.claim", "J1's h listed as claim h9, the oldest", |j| j1_listed(j, 1)["candidate"]["id"]["claim"]["claim"] = json!("h9"), Rejected("tick 7: h was chosen, the turn's walk gives h9")),
        row("aged.steps[][step=chosen].listed[].candidate.party", "J1's c of party q", |j| j1_listed(j, 0)["candidate"]["party"] = json!("q"), Rejected("tick 7: c is listed but is no serviceable ordinary candidate")),
        row("aged.steps[][step=chosen].listed[].candidate.eligible_at", "J1's c eligible at 40: not aged", |j| j1_listed(j, 0)["candidate"]["eligible_at"] = json!(40.0), Rejected("tick 7: c is listed before it aged by the tick's sample at 60")),
        row("aged.steps[][step=chosen].listed[].candidate.eligible_at", "J1's h eligible at 0, tied with c, whose catalogue place comes first", |j| j1_listed(j, 1)["candidate"]["eligible_at"] = json!(0.0), Rejected("tick 7: h was chosen, the turn's walk gives c")),
        row("aged.steps[][step=chosen].listed[].candidate.catalogue", "J1's c a claim's place", |j| j1_listed(j, 0)["candidate"]["catalogue"] = json!({"claim": {"order": 9}}), Rejected("candidate c has a catalogue place of another kind")),
        row("aged.steps[][step=chosen].listed[].candidate.catalogue.compile.position", "J1's c at position 1", |j| j1_listed(j, 0)["candidate"]["catalogue"]["compile"]["position"] = json!(1), Irrelevant("the catalogue place breaks ties of equal eligibility only (`Candidate::age`); h, eligible at −10, is older than c whatever c's place")),
        row("aged.steps[][step=chosen].listed[].candidate.bytes", "J1's c 3 → 2 GiB; its launch says 3", |j| j1_listed(j, 0)["candidate"]["bytes"] = json!(2 * G), Rejected("tick 7: c's declaration is 2147483648 and 3221225472 in one record")),
        row("aged.steps[][step=chosen].listed[].candidate.bytes", "J1's h 5 → 6 GiB", |j| j1_listed(j, 1)["candidate"]["bytes"] = json!(6 * G), Rejected("tick 7: the record's /admitted/0/bytes")),
        row("aged.steps[][step=chosen].listed[].candidate.capacity", "J1's c without capacity", |j| *c_capacity(j) = Value::Null, Rejected("pending compile job c carries no capacity")),
        row("aged.steps[][step=chosen].listed[].candidate.capacity", "J1's claim h with a capacity", |j| j1_listed(j, 1)["candidate"]["capacity"] = c_capacity(j).clone(), Rejected("claim h carries a capacity")),
        row("aged.steps[][step=chosen].listed[].candidate.capacity.eligible", "J1's c 3 → 1: a core cap of 1, below its group's limit", |j| c_capacity(j)["eligible"] = json!(1), Rejected("tick 7: group g's limit exceeds the core cap")),
        row("aged.steps[][step=chosen].listed[].candidate.capacity.held", "J1's c 0 → 1", |j| c_capacity(j)["held"] = json!(1), Rejected("tick 7: the record counts 3 slots, and its capacities give 2")),
        row("aged.steps[][step=chosen].listed[].candidate.capacity.idle", "J1's c 3 → 0", |j| c_capacity(j)["idle"] = json!(0), Rejected("tick 7: the record counts 3 slots, and its capacities give 0")),
        row("aged.steps[][step=chosen].listed[].candidate.capacity.running.running", "J1's c 1 → 0", |j| c_capacity(j)["running"]["running"] = json!(0), Rejected("tick 7: group g's capacity counts more contracted compiles than running ones")),
        row("aged.steps[][step=chosen].listed[].candidate.capacity.group.running", "J1's c 0 → 1, more than its party's", |j| c_capacity(j)["group"]["running"] = json!(1), Rejected("tick 7: group g's capacity counts more of its group's compiles than its party's, or its party's than all")),
        row("aged.steps[][step=chosen].listed[].candidate.capacity.group.contracted", "J1's c 0 → 1", |j| c_capacity(j)["group"]["contracted"] = json!(1), Rejected("tick 7: group g's capacity counts more contracted compiles than running ones")),
        row("aged.steps[][step=chosen].listed[].candidate.capacity.group_limit", "J1's c 2 → 3, above the core cap", |j| c_capacity(j)["group_limit"] = json!(3), Rejected("tick 7: group g's limit exceeds the core cap")),
        row("aged.steps[][step=chosen].listed[].candidate.capacity.party.running", "J1's c 0 → 2, above all running", |j| c_capacity(j)["party"]["running"] = json!(2), Rejected("tick 7: group g's capacity counts more of its group's compiles than its party's, or its party's than all")),
        row("aged.steps[][step=chosen].listed[].candidate.capacity.party.contracted", "J1's c 0 → 1", |j| c_capacity(j)["party"]["contracted"] = json!(1), Rejected("tick 7: group g's capacity counts more contracted compiles than running ones")),
        row("aged.steps[][step=chosen].chosen.claim.claim", "J1 chose c", |j| j[J1]["aged"]["steps"][0]["chosen"] = json!({"compile": {"group": "g", "job": "c"}}), Rejected("tick 7: c was chosen, the turn's walk gives h")),
        row("aged.steps[][step=chosen].verdict.kept.fits", "J1's h fits", |j| j[J1]["aged"]["steps"][0]["verdict"] = json!({"kept": {"fits": true}}), Rejected("tick 7: the chosen h was Kept { fits: true }, its figures say Kept { fits: false }")),
        row("slots", "J1's 3 → 2, its capacity giving 3", |j| j[J1]["slots"] = json!(2), Rejected("tick 7: the record counts 2 slots, and its capacities give 3")),
        row("slots", "J4's 1 → 0", |j| j[J4]["slots"] = json!(0), Rejected("tick 11: 1 launches against a slot count of 0")),
        row("slots", "J4's 1 → 5", |j| j[J4]["slots"] = json!(5), Irrelevant("J4 holds no capacity, and the slots only bound its launches (`derive_admissions`): more slots than launches is a tick whose pending set, which the record does not hold, had no other job ready")),
        // The admissions.
        row("admitted", "J1's reservation removed", |j| { j[J1]["admitted"].as_array_mut().unwrap().remove(0); }, Rejected("tick 7: entry 0: the held head's grant at once or its reservation is the first admission entry, and only it")),
        row("admitted", "J3's turn and y's grant swapped", |j| { j[J3]["admitted"].as_array_mut().unwrap().swap(1, 2); }, Rejected("tick 9: the record's /admitted/1/budget")),
        row("admitted", "J3's y granted twice", |j| { let y = j[J3]["admitted"][2].clone(); j[J3]["admitted"].as_array_mut().unwrap().insert(3, y) }, Rejected("tick 9: y is admitted twice in one tick")),
        row("admitted[][event=turn].party", "J3's r → x", |j| j[J3]["admitted"][1]["party"] = json!("x"), Irrelevant("a compiler turn names no job and the record holds no pending set; the running budget falls by its bytes alone (`derive_admissions`), whichever party's turn it was")),
        row("admitted[][event=turn].bytes", "J3's 2 → 3 GiB", |j| j[J3]["admitted"][1]["bytes"] = json!(3 * G), Rejected("tick 9: the record's /admitted/2/budget")),
        row("admitted[][event=granted].phase=serve", "J3's y in the close", |j| j[J3]["admitted"][2]["phase"] = json!("close"), Rejected("tick 9: entry 3 is out of phase order")),
        row("admitted[][event=granted].phase=serve", "J3's y in the large close", |j| j[J3]["admitted"][2]["phase"] = json!("large_close"), Rejected("tick 9: claim y of class Ordinary granted in LargeClose")),
        row("admitted[][event=granted].claim", "J3's y → y2", |j| j[J3]["admitted"][2]["claim"] = json!("y2"), Rejected("tick 9: Claim(Registration { claim: \"y\", order: 6 }) is One(Admitted), which a request the record does not describe cannot be")),
        row("admitted[][event=granted].party", "J3's y of party s → t", |j| j[J3]["admitted"][2]["party"] = json!("t"), Irrelevant("y's party is observed once in its instance, in this grant; J2 compares observations of one registration, and y has one")),
        row("admitted[][event=granted].party", "J3's head grant of h to party x", |j| j[J3]["admitted"][0]["party"] = json!("x"), Rejected("tick 9: h's party is \"q\" and \"x\" in one record")),
        row("admitted[][event=granted].party", "J1's w2 granted to party x, listed for s", |j| { let grant = json!({"event": "granted", "phase": "close", "claim": "w2", "order": 4, "party": "x", "class": "ordinary", "bytes": 1073741824, "budget": 0, "head": false}); j[J1]["admitted"].as_array_mut().unwrap().insert(5, grant) }, Rejected("tick 7: w2's party is \"s\" and \"x\" in one record")),
        row("admitted[][event=granted].bytes", "J3's y 1 → 2 GiB", |j| j[J3]["admitted"][2]["bytes"] = json!(2 * G), Rejected("tick 9: the record's /admitted/3/budget")),
        row("admitted[][event=granted].bytes", "J3's head grant 5 → 4 GiB; its standing says 5", |j| j[J3]["admitted"][0]["bytes"] = json!(4 * G), Rejected("tick 9: h's declaration is 5368709120 and 4294967296 in one record")),
        row("admitted[][event=granted].bytes", "J4's L 11 → 25 GiB, beyond what the large launch left", |j| j[J4]["admitted"][4]["bytes"] = json!(25 * G), Rejected("tick 11: claim L of 26843545600 bytes granted beyond the budget")),
        row("admitted[][event=launched].phase=launch", "J1's c a large launch", |j| c_launch(j)["phase"] = json!("large_launch"), Rejected("tick 7: job c of class Ordinary launched in LargeLaunch")),
        row("admitted[][event=launched].phase=large_launch", "J4's big an ordinary launch", |j| j[J4]["admitted"][3]["phase"] = json!("launch"), Rejected("tick 11: entry 3 is out of phase order")),
        row("admitted[][event=launched].group", "J1's c in group h2", |j| c_launch(j)["group"] = json!("h2"), Rejected("Job { group: \"g\", job: \"c\" } is One(Admitted), which a request the record does not describe cannot be")),
        row("admitted[][event=launched].job", "J1's c → d", |j| c_launch(j)["job"] = json!("d"), Rejected("Job { group: \"g\", job: \"c\" } is One(Admitted), which a request the record does not describe cannot be")),
        row("admitted[][event=launched].bytes", "J1's c 3 → 2 GiB; its listing says 3", |j| c_launch(j)["bytes"] = json!(2 * G), Rejected("tick 7: c's declaration is 3221225472 and 2147483648 in one record")),
        row("admitted[][event=launched].ahead", "J1's c without its run-ahead", |j| c_launch(j)["ahead"] = Value::Null, Rejected("tick 7: c took 3221225472 bytes of the reservation, and its record shows no run-ahead")),
        row("admitted[][event=launched].ahead", "J4's big with a run-ahead", |j| j[J4]["admitted"][3]["ahead"] = json!({"deadline": 61.0, "work": {"compile": {"job": "big", "started_at": 0.0}}}), Rejected("tick 11: big records a run-ahead, and it took nothing of a reservation it could take")),
        row("admitted[][event=launched].ahead.work.compile.job", "J1's c → w", |j| c_launch(j)["ahead"]["work"]["compile"]["job"] = json!("w"), Rejected("tick 7: the record's /admitted/3/ahead/work/compile/job")),
        row("admitted[][event=launched].ahead.work.compile.started_at", "J1's c started 1 → 2: the head then waits for an attempt D does not count", |j| c_launch(j)["ahead"]["work"]["compile"]["started_at"] = json!(2.0), Rejected("tick 8: the record's /aged/head/recorded")),
        row("admitted[][event=launched].admitted_at", "J1's c 57 → 55, before the pre-launch completion", |j| c_launch(j)["admitted_at"] = json!(55.0), Rejected("tick 7: c was admitted at 55, before the persistence boundary at 57")),
        row("admitted[][event=launched].admitted_at", "J1's c 57 → 58.1, after its settlement and finishing after the deadline", |j| c_launch(j)["admitted_at"] = json!(58.1), Rejected("tick 7: c was admitted at 58.1 and settled at 57.5")),
        row("admitted[][event=launched].declared", "J1's c 3 → 5 s: it would finish at 62, after the deadline", |j| c_launch(j)["declared"] = json!(5), Rejected("tick 7: c of 3221225472 bytes launched beyond its budget")),
        row("admitted[][event=launched].declared", "J1's c declaring nothing", |j| c_launch(j)["declared"] = Value::Null, Rejected("tick 7: c of 3221225472 bytes launched beyond its budget")),
        row("admitted[][event=launched].settled_at", "J1's c 57.5 → 56, before its admission", |j| c_launch(j)["settled_at"] = json!(56.0), Rejected("tick 7: c was admitted at 57 and settled at 56")),
        row("admitted[][event=launched].settled_at", "J1's c 57.5 → 58, after the closes' completion", |j| c_launch(j)["settled_at"] = json!(58.0), Rejected("tick 7: a persistence sequence completed at 57.5, before the boundary at 58")),
        row("admitted[][event=launched].running", "J1's c a failed spawn", |j| c_launch(j)["running"] = json!(false), Rejected("tick 7: the record's /admitted/3/ahead/work/compile/started_at")),
        row("admitted[][event=persisted].phase=serve", "J1's pre-launch completion named close", |j| j[J1]["admitted"][2]["phase"] = json!("close"), Rejected("tick 7: the Serve phase records 0 completions, not one")),
        row("admitted[][event=persisted].at", "J1's pre-launch completion 57 → 58, after c's admission", |j| j[J1]["admitted"][2]["at"] = json!(58.0), Rejected("tick 7: c was admitted at 57, before the persistence boundary at 58")),
        row("admitted[][event=granted].phase=large_close", "J4's L in the ordinary close", |j| j[J4]["admitted"][4]["phase"] = json!("close"), Rejected("tick 11: the Close phase's completion is not its last entry")),
        row("admitted[][event=persisted].phase=close", "J4's ordinary close named serve", |j| j[J4]["admitted"][2]["phase"] = json!("serve"), Rejected("tick 11: the Serve phase records 2 completions, not one")),
        row("admitted[][event=persisted].phase=large_close", "J4's large close named close", |j| j[J4]["admitted"][5]["phase"] = json!("close"), Rejected("tick 11: the Close phase records 2 completions, not one")),
        row("admitted[][event=persisted].at", "J4's large close 70.5 → 71", |j| j[J4]["admitted"][5]["at"] = json!(71.0), Irrelevant("a completion later than every boundary before it bounds only admissions after it (`derive_admissions`), and the large close is J4's last entry")),
        // The decisions.
        row("decisions", "J1 without h's first decision", |j| { j[J1]["decisions"].as_array_mut().unwrap().retain(|decided| decided["subject"] != json!({"claim": {"claim": "h", "order": 1}})); }, Rejected("Claim(Registration { claim: \"h\", order: 1 })'s derivation says Is(AgedHead), its last listed decision is None")),
        row("decisions", "J1 deciding w2 twice", |j| { let w2 = j[J1]["decisions"].as_array().unwrap().iter().find(|decided| decided["subject"] == json!({"claim": {"claim": "w2", "order": 4}})).unwrap().clone(); j[J1]["decisions"].as_array_mut().unwrap().push(w2) }, Rejected("is decided twice")),
        row("decisions", "J4 without z's decision", |j| { j[J4]["decisions"].as_array_mut().unwrap().retain(|decided| decided["subject"] != json!({"claim": {"claim": "z", "order": 8}})); }, Irrelevant("z is described nowhere in the record: whether its decision changed is unknown, and no admission reads a waiting reason")),
        row("decisions[].subject.claim.claim", "J1's w2 → w3", |j| { for decided in j[J1]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"claim": {"claim": "w2", "order": 4}}) { decided["subject"]["claim"]["claim"] = json!("w3"); } } }, Irrelevant("w2 is listed, so its decision is possible but not certain to be listed (a listed claim can be refused later in its tick), and w3 is described nowhere: WaitingForTurn is a decision such a claim can have")),
        row("decisions[].decision.one=admitted", "J3's y Admitted → WaitingForTurn", |j| { for decided in j[J3]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"claim": {"claim": "y", "order": 6}}) { decided["decision"] = json!({"one": "waiting_for_turn"}); } } }, Rejected("tick 9: Claim(Registration { claim: \"y\", order: 6 }) is One(WaitingForTurn), and its derivation says Is(Admitted)")),
        row("decisions[].decision.one=not_serviceable", "J4's group g NotServiceable → AgedHead", |j| { for decided in j[J4]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"group": {"group": "g"}}) { decided["decision"] = json!({"one": "aged_head"}); } } }, Rejected("tick 11: Group { group: \"g\" } is One(AgedHead), which a request the record does not describe cannot be")),
        row("decisions[].decision.one=large_waiting", "J4's large claim M LargeWaiting → AgedHead", |j| { for decided in j[J4]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"claim": {"claim": "M", "order": 9}}) { decided["decision"] = json!({"one": "aged_head"}); } } }, Rejected("tick 11: Claim(Registration { claim: \"M\", order: 9 }) is One(AgedHead), which a request the record does not describe cannot be")),
        row("decisions[].decision.one=aged_head", "J1's h AgedHead → WaitingForTurn", |j| { for decided in j[J1]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"claim": {"claim": "h", "order": 1}}) { decided["decision"] = json!({"one": "waiting_for_turn"}); } } }, Rejected("is One(WaitingForTurn), and its derivation says Is(AgedHead)")),
        row("decisions[].decision.one=waiting_for_turn", "J1's listed w2 WaitingForTurn → LargeWaiting", |j| { for decided in j[J1]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"claim": {"claim": "w2", "order": 4}}) { decided["decision"] = json!({"one": "large_waiting"}); } } }, Rejected("is One(LargeWaiting), and its derivation says Is(WaitingForTurn)")),
        row("decisions[].decision.one=waiting_for_turn", "J4's z WaitingForTurn → NotServiceable", |j| { for decided in j[J4]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"claim": {"claim": "z", "order": 8}}) { decided["decision"] = json!({"one": "not_serviceable"}); } } }, Irrelevant("z is described nowhere in the record: its bytes and state are unknown, and no admission reads a waiting reason")),
        row("decisions[].decision.one=waiting_for_turn", "J4's z WaitingForTurn → GivingWay", |j| { for decided in j[J4]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"claim": {"claim": "z", "order": 8}}) { decided["decision"] = json!({"one": "giving_way"}); } } }, Rejected("Claim(Registration { claim: \"z\", order: 8 }) is One(GivingWay), which a request the record does not describe cannot be")),
        row("decisions[].decision.one=waiting_for_turn", "J4's z → AgedHead", |j| { for decided in j[J4]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"claim": {"claim": "z", "order": 8}}) { decided["decision"] = json!({"one": "aged_head"}); } } }, Rejected("Claim(Registration { claim: \"z\", order: 8 }) is One(AgedHead), which a request the record does not describe cannot be")),
        row("decisions[].decision", "J4's group g counting 0 not serviceable", |j| { for decided in j[J4]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"group": {"group": "g"}}) { decided["decision"] = json!({"jobs": {"not_serviceable": 0}}); } } }, Rejected("Group { group: \"g\" } is Jobs({NotServiceable: 0}), which a request the record does not describe cannot be")),
        row("decisions[].decision", "J4's group g counting 2 aged heads", |j| { for decided in j[J4]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"group": {"group": "g"}}) { decided["decision"] = json!({"jobs": {"aged_head": 2}}); } } }, Rejected("Group { group: \"g\" } is Jobs({AgedHead: 2}), which a request the record does not describe cannot be")),
        row("decisions[].decision", "J4's group g counting 2 waiting, 1 not serviceable", |j| { for decided in j[J4]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"group": {"group": "g"}}) { decided["decision"] = json!({"jobs": {"waiting_for_turn": 2, "not_serviceable": 1}}); } } }, Irrelevant("group g's pending jobs are described nowhere in J4: their cells are unknown, and no admission reads a waiting reason")),
        row("decisions[].decision.jobs.waiting_for_turn", "J4's group m counting no job waiting for its turn", |j| { for decided in j[J4]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"group": {"group": "m"}}) { decided["decision"]["jobs"]["waiting_for_turn"] = json!(0); } } }, Rejected("tick 11: Group { group: \"m\" } is Jobs({NotServiceable: 2, WaitingForTurn: 0, LargeWaiting: 1}), which a request the record does not describe cannot be")),
        row("decisions[].decision.jobs.not_serviceable", "J4's group m counting no job not serviceable", |j| { for decided in j[J4]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"group": {"group": "m"}}) { decided["decision"]["jobs"]["not_serviceable"] = json!(0); } } }, Rejected("tick 11: Group { group: \"m\" } is Jobs({NotServiceable: 0, WaitingForTurn: 1, LargeWaiting: 1}), which a request the record does not describe cannot be")),
        row("decisions[].decision.jobs.large_waiting", "J4's group m counting no large job waiting", |j| { for decided in j[J4]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"group": {"group": "m"}}) { decided["decision"]["jobs"]["large_waiting"] = json!(0); } } }, Rejected("tick 11: Group { group: \"m\" } is Jobs({NotServiceable: 2, WaitingForTurn: 1, LargeWaiting: 0}), which a request the record does not describe cannot be")),
    ]
}

/// What a row is the row of: a leaf (`input`), or a case of a carried fact (`carried`) or of a kind of admission
/// (`admission`), named.
struct Key {
    kind: &'static str,
    case: String,
}
/// A base journal and the rows mutated on it.
struct Table {
    name: &'static str,
    base: Vec<Value>,
    rows: Vec<(Key, Row)>,
}
fn table(name: &'static str, base: Vec<Value>, rows: Vec<Row>) -> Table {
    Table { name, base, rows: rows.into_iter().map(|row| (Key { kind: "input", case: String::new() }, row)).collect() }
}

/// One case of a carried fact or a kind of admission: rows on named tables, or why it cannot arise.
enum Case {
    Rows(Vec<(&'static str, Row)>),
    Cannot(&'static str),
}
fn on(table: &'static str, rows: Vec<Row>) -> Case {
    Case::Rows(rows.into_iter().map(|row| (table, row)).collect())
}
/// A carried fact's or a kind of admission's three cases: across a consecutive tick, a silent gap and an instance
/// boundary.
struct Cases {
    consecutive: Case,
    gap: Case,
    boundary: Case,
}

/// What serde writes to tell a variant from the others of its enum: a unit variant's string, an externally tagged
/// variant's key, or an internal tag and its value. Displayed as it shows in a normalized path.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Discriminator {
    Unit(String),
    External(String),
    Internal { tag: String, variant: String },
}
impl std::fmt::Display for Discriminator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unit(variant) => write!(formatter, "={variant}"),
            Self::External(key) => write!(formatter, ".{key}"),
            Self::Internal { tag, variant } => write!(formatter, "[{tag}={variant}]"),
        }
    }
}

/// The key serde wrote first in a serialized object, or none for a string: serde writes an internal tag before the
/// variant's fields.
fn first_key(text: &str) -> Option<String> {
    struct First;
    impl<'de> serde::de::Visitor<'de> for First {
        type Value = Option<String>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a serialized variant")
        }
        fn visit_str<E>(self, _: &str) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let first = map.next_key::<String>()?;
            first.iter().try_for_each(|_| map.next_value::<serde::de::IgnoredAny>().map(drop))?;
            while map.next_entry::<serde::de::IgnoredAny, serde::de::IgnoredAny>()?.is_some() {}
            Ok(first)
        }
    }
    serde::Deserializer::deserialize_any(&mut serde_json::Deserializer::from_str(text), First).unwrap()
}

/// An enum of the record schema: its name, a value of every variant as serde writes it, and their discriminators.
struct Enum {
    name: &'static str,
    values: Vec<Value>,
    discriminators: Vec<Discriminator>,
}
impl Enum {
    /// An enum's discriminators, from serde's output alone. A string is a unit variant. An object is named by the key
    /// serde wrote first: an internal tag when every object variant wrote that key first, holding a string, and a
    /// second object variant or a field beside it shows it is no one variant's key; otherwise the externally tagged
    /// variant's key, the object's only one.
    fn of((name, written): (&'static str, Vec<String>)) -> Self {
        let values: Vec<Value> = written.iter().map(|text| serde_json::from_str(text).unwrap()).collect();
        let objects: Vec<(String, &serde_json::Map<String, Value>)> =
            written.iter().zip(&values).filter_map(|(text, value)| Some((first_key(text)?, value.as_object()?))).collect();
        let lone = objects.len() == 1 && objects[0].1.len() == 1 && objects[0].1[&objects[0].0].is_string();
        assert!(!lone, "{name}: serde's output cannot tell whether {} is an internal tag or a variant's key", objects[0].0);
        let tag = objects.first().map(|(first, _)| first.clone()).filter(|first| {
            let shared = objects.iter().all(|(key, map)| key == first && map[first].is_string());
            shared && (objects.len() > 1 || objects.iter().any(|(_, map)| map.len() > 1))
        });
        let discriminators = values
            .iter()
            .map(|value| match (value, &tag) {
                (Value::String(variant), _) => Discriminator::Unit(variant.clone()),
                (Value::Object(map), Some(tag)) => {
                    Discriminator::Internal { tag: tag.clone(), variant: map[tag].as_str().unwrap().to_owned() }
                }
                (Value::Object(map), None) if map.len() == 1 => Discriminator::External(map.keys().next().unwrap().clone()),
                (other, _) => panic!("{name}'s variant {other} carries no discriminator serde writes"),
            })
            .collect();
        Self { name, values, discriminators }
    }
}

/// The record schema's enums (`form::enums`), with the internal tags serde writes, each naming one enum.
struct Schema {
    enums: Vec<Enum>,
    tagged: BTreeMap<String, &'static str>,
}
impl Schema {
    fn of(enums: Vec<(&'static str, Vec<String>)>) -> Self {
        let enums: Vec<Enum> = enums.into_iter().map(Enum::of).collect();
        let tags: BTreeSet<(String, &'static str)> = enums
            .iter()
            .flat_map(|of| of.discriminators.iter().filter_map(move |discriminator| match discriminator {
                Discriminator::Internal { tag, .. } => Some((tag.clone(), of.name)),
                Discriminator::Unit(_) | Discriminator::External(_) => None,
            }))
            .collect();
        let tagged: BTreeMap<String, &'static str> = tags.iter().cloned().collect();
        assert_eq!(tagged.len(), tags.len(), "an internal tag names one enum: {tags:?}");
        Self { enums, tagged }
    }
    /// Every variant of every enum: the enum's name and the variant's discriminator.
    fn variants(&self) -> BTreeSet<(&'static str, Discriminator)> {
        self.enums.iter().flat_map(|of| of.discriminators.iter().map(|discriminator| (of.name, discriminator.clone()))).collect()
    }
}

/// A node of a record: its normalized path, its JSON pointer in the record, whether it is a leaf, and the enum and
/// variant it is, when it is a variant of a record-schema enum.
struct Node {
    path: String,
    pointer: String,
    leaf: bool,
    variant: Option<(&'static str, Discriminator)>,
}

/// One normalized path syntax for every record, read from the record schema's types. An object's key is a segment, so
/// an externally tagged variant is one; an array's elements are `[]`; an object holding an internal tag is
/// `[tag=variant]` on its path, and the tag is no node of its own; a unit variant is `path=variant`. A leaf is a
/// scalar, null, an empty array or object, or a tagged object with nothing beside its tag.
struct Normalizer {
    schema: Schema,
    /// What the probes found, by the normalized path they probed, which the record's types decide: whether an enum
    /// sits at a string leaf, and the enums with a discriminator that sit at a node.
    units: std::cell::RefCell<BTreeMap<String, bool>>,
    sitting: std::cell::RefCell<BTreeMap<(String, Discriminator), Vec<&'static str>>>,
}
impl Normalizer {
    fn new(schema: Schema) -> Self {
        Self { schema, units: Default::default(), sitting: Default::default() }
    }
    /// The normalizer of the record schema's own enums.
    fn record() -> Self {
        Self::new(Schema::of(super::form::enums()))
    }

    /// The nodes of `value` at `path` and `pointer`: its structure, which needs the internal tags alone.
    fn walk(&self, value: &Value, path: String, pointer: String, found: &mut Vec<Node>) {
        match value {
            Value::Object(map) if !map.is_empty() => {
                let tag = map.iter().find_map(|(key, value)| Some((key.as_str(), *self.schema.tagged.get(key)?, value.as_str()?)));
                let path = tag.map_or(path.clone(), |(tag, _, variant)| format!("{path}[{tag}={variant}]"));
                let fields: Vec<(&String, &Value)> = map.iter().filter(|(key, _)| tag.is_none_or(|(tag, _, _)| tag != key.as_str())).collect();
                for (key, value) in &fields {
                    self.walk(value, [path.as_str(), key.as_str()].into_iter().filter(|part| !part.is_empty()).collect::<Vec<_>>().join("."), format!("{pointer}/{key}"), found);
                }
                let variant = tag.map(|(tag, name, variant)| (name, Discriminator::Internal { tag: tag.into(), variant: variant.into() }));
                found.push(Node { path, pointer, leaf: fields.is_empty(), variant });
            }
            Value::Array(items) if !items.is_empty() => {
                for (index, item) in items.iter().enumerate() {
                    self.walk(item, format!("{path}[]"), format!("{pointer}/{index}"), found);
                }
                found.push(Node { path, pointer, leaf: false, variant: None });
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) | Value::Object(_) => {
                found.push(Node { path, pointer, leaf: true, variant: None })
            }
        }
    }

    /// The nodes of a whole record: its structure (`walk`), with each variant of a record-schema enum named and given
    /// its enum by the record's types (`typed`).
    fn nodes(&self, record: &Value) -> Vec<Node> {
        serde_json::from_value::<Record<Typed>>(record.clone()).unwrap_or_else(|error| panic!("a record that does not read as its types: {error}: {record}"));
        let mut found = Vec::new();
        self.walk(record, String::new(), String::new(), &mut found);
        found.into_iter().map(|node| self.typed(record, node)).collect()
    }

    /// `node` as the record's types read it: a string where an enum sits is that enum's unit variant, `path=variant`,
    /// and an object of one key is the externally tagged variant of the enum that sits there with that key.
    fn typed(&self, record: &Value, node: Node) -> Node {
        match record.pointer(&node.pointer) {
            Some(Value::String(variant)) if self.unit(record, &node) => {
                let discriminator = Discriminator::Unit(variant.clone());
                let sitting = self.sitting(record, &node, &discriminator);
                assert_eq!(sitting.len(), 1, "{}: {sitting:?} sit there with the unit variant {variant}, and one registered enum must (`form::enums`)", node.path);
                Node { path: format!("{}{discriminator}", node.path), variant: Some((sitting[0], discriminator)), ..node }
            }
            Some(Value::Object(map)) if map.len() == 1 && node.variant.is_none() => {
                let discriminator = Discriminator::External(map.keys().next().unwrap().clone());
                let sitting = self.sitting(record, &node, &discriminator);
                assert!(sitting.len() < 2, "{}: {sitting:?} sit there with the variant {discriminator}", node.path);
                Node { variant: sitting.first().map(|name| (*name, discriminator)), ..node }
            }
            _ => node,
        }
    }

    /// Whether an enum sits at a string leaf: set to a string that is no variant, the record no longer reads.
    fn unit(&self, record: &Value, node: &Node) -> bool {
        *self.units.borrow_mut().entry(node.path.clone()).or_insert_with(|| {
            let mut probe = record.clone();
            *probe.pointer_mut(&node.pointer).unwrap() = json!("\u{0}no variant");
            serde_json::from_value::<Record<Typed>>(probe).is_err()
        })
    }

    /// The registered enums with `discriminator` that sit at `node`: each of their variants, put there, the record
    /// still reads.
    fn sitting(&self, record: &Value, node: &Node, discriminator: &Discriminator) -> Vec<&'static str> {
        let reads = |value: &Value| {
            let mut probe = record.clone();
            *probe.pointer_mut(&node.pointer).unwrap() = value.clone();
            serde_json::from_value::<Record<Typed>>(probe).is_ok()
        };
        let key = (node.path.clone(), discriminator.clone());
        self.sitting
            .borrow_mut()
            .entry(key)
            .or_insert_with(|| {
                let having = self.schema.enums.iter().filter(|of| of.discriminators.contains(discriminator));
                having.filter(|of| of.values.iter().all(reads)).map(|of| of.name).collect()
            })
            .clone()
    }

    /// What a journal's records hold: each normalized path at its first occurrence, and each variant of a
    /// record-schema enum. An externally tagged variant's node shares its path with its enum's other variants, so the
    /// variants are read from every node.
    fn occurrences(&self, journal: &[Value]) -> Occurrences {
        let mut held = Occurrences { first: BTreeMap::new(), variants: BTreeMap::new() };
        for (place, record) in journal.iter().enumerate() {
            for node in self.nodes(record) {
                if let Some(variant) = &node.variant {
                    held.variants.entry(variant.clone()).or_insert_with(|| node.path.clone());
                }
                held.first.entry(node.path.clone()).or_insert((place, node));
            }
        }
        held
    }
}

/// What a journal's records hold (`Normalizer::occurrences`).
struct Occurrences {
    /// Each normalized path at its first occurrence: the record's place and the node.
    first: BTreeMap<String, (usize, Node)>,
    /// Each variant of a record-schema enum, with the path of its first occurrence.
    variants: BTreeMap<(&'static str, Discriminator), String>,
}

/// Whether the leaf at `pointer` of `record` is a figure: the projection holds nothing of it (`Unread`), so the record
/// still reads as a projection whatever the leaf holds.
fn figure(record: &Value, pointer: &str) -> bool {
    let mut probe = record.clone();
    probe.pointer_mut(pointer).into_iter().for_each(|leaf| *leaf = json!({ "probe": [] }));
    serde_json::from_value::<Record<Projected>>(probe).is_ok()
}

/// A figure's value changed: a number by one, a flag flipped, a name lengthened, and null or an empty list or object
/// replaced.
fn changed(value: &Value) -> Value {
    match value {
        Value::Bool(flag) => json!(!flag),
        Value::Number(number) => number.as_u64().map_or_else(|| json!(number.as_f64().unwrap_or_default() + 0.5), |number| json!(number + 1)),
        Value::String(name) => json!(format!("{name}x")),
        Value::Null | Value::Array(_) | Value::Object(_) => json!("changed"),
    }
}

/// One row's run: its journal derived, against what it expects.
struct Trial {
    table: &'static str,
    kind: &'static str,
    case: String,
    path: String,
    mutation: String,
    verdict: &'static str,
    rule: String,
    said: String,
    holds: bool,
}
/// Derives `journal`, a row of `table` with `key` mutating `path` by `mutation`, against `expect`.
fn trial(table: &'static str, key: &Key, path: String, mutation: String, journal: &[Value], expect: Expect<String>) -> Trial {
    let outcome = derive_journal(journal).map(|_| ());
    let (verdict, holds, said, rule) = match (expect, &outcome) {
        (Rejected(rule), Err(error)) => ("rejected", error.contains(&rule), error.clone(), rule),
        (Rejected(rule), Ok(())) => ("rejected", false, "accepted".to_owned(), rule),
        (Irrelevant(reason), Ok(())) => ("irrelevant", true, "accepted".to_owned(), reason),
        (Irrelevant(reason), Err(error)) => ("irrelevant", false, error.clone(), reason),
    };
    Trial { table, kind: key.kind, case: key.case.clone(), path, mutation, verdict, rule, said, holds }
}

/// What a table's run found: its trials, its leaf paths and every path of its records.
struct Ran {
    trials: Vec<Trial>,
    leaves: BTreeSet<String>,
    paths: BTreeSet<String>,
    /// Each variant of a record-schema enum its records hold, with the path of its first occurrence.
    variants: BTreeMap<(&'static str, Discriminator), String>,
}

/// Runs a table: its base must replay; each of its rows, and a row for each figure its records hold, changed at its
/// first occurrence, must be refused by its rule or accepted.
fn run(table: &Table, normalizer: &Normalizer) -> Ran {
    derive_journal(&table.base).unwrap_or_else(|error| panic!("table {}'s base journal: {error}", table.name));
    let Occurrences { first, variants } = normalizer.occurrences(&table.base);
    let leaves: BTreeSet<String> = first.iter().filter(|(_, (_, node))| node.leaf).map(|(path, _)| path.clone()).collect();
    let paths: BTreeSet<String> = first.keys().cloned().collect();
    let mut trials: Vec<Trial> = table
        .rows
        .iter()
        .map(|(key, row)| {
            let mut journal = table.base.clone();
            (row.edit)(&mut journal);
            trial(table.name, key, row.path.to_owned(), row.mutation.to_owned(), &journal, row.expect.owned())
        })
        .collect();
    let figure_key = Key { kind: "figure", case: String::new() };
    for (path, (place, node)) in first.iter().filter(|(_, (place, node))| node.leaf && figure(&table.base[*place], &node.pointer)) {
        let mut journal = table.base.clone();
        let value = journal[*place].pointer(&node.pointer).cloned().unwrap_or_default();
        journal[*place].pointer_mut(&node.pointer).into_iter().for_each(|leaf| *leaf = changed(&value));
        let tick = &table.base[*place]["tick"];
        let mutation = format!("record {place}'s {}: {value} → {}", node.pointer, changed(&value));
        let rule = format!("tick {tick}: the record's {}", node.pointer);
        trials.push(trial(table.name, &figure_key, path.clone(), mutation, &journal, Rejected(rule)));
    }
    Ran { trials, leaves, paths, variants }
}

/// K: `kept_compile_journal`'s ticks 7 and 8, the fitting compile head g/head chosen and then kept with an idle core.
fn kept() -> Vec<Value> {
    let capacity = Capacity { idle: 3, ..outside_idle() };
    numbered(&kept_compile_journal(capacity))
}
/// K's kept standing's capacity.
fn k_capacity(journal: &mut [Value]) -> &mut Value {
    &mut journal[1]["aged"]["steps"][0]["standing"]["capacity"]
}

/// The rows of a pending compile head's kept standing, on K.
fn kept_rows() -> Vec<Row> {
    vec![
        row("aged.steps[][step=kept].standing.capacity", "K's pending compile head without capacity", |j| *k_capacity(j) = Value::Null, Rejected("tick 8: pending compile job head carries no capacity")),
        row("aged.steps[][step=kept].standing.capacity.idle", "K's 3 → 0", |j| k_capacity(j)["idle"] = json!(0), Rejected("tick 8: the record counts 3 slots, and its capacities give 0")),
        row("aged.steps[][step=kept].standing.capacity.held", "K's 0 → 3, one compile running", |j| k_capacity(j)["held"] = json!(3), Rejected("tick 8: group g's capacity holds more cores than compiles run")),
        row("aged.steps[][step=kept].standing.capacity.group_limit", "K's 2 → 0: no unit of its group's limit is free", |j| k_capacity(j)["group_limit"] = json!(0), Rejected("tick 8: the head head was Kept { fits: true }, its figures say Passed")),
        row("aged.steps[][step=kept].standing.pending", "K's head ended, its capacity as found", |j| j[1]["aged"]["steps"][0]["standing"]["pending"] = json!(false), Rejected("tick 8: the head head was Kept { fits: true }, its figures say Ended")),
        row("aged.steps[][step=kept].standing.capacity.eligible", "K's 3 → 1: a core cap of 1, below its group's limit", |j| k_capacity(j)["eligible"] = json!(1), Rejected("tick 8: group g's limit exceeds the core cap")),
        row("aged.steps[][step=kept].standing.capacity.running.running", "K's 1 → 0, below D's one compile", |j| k_capacity(j)["running"]["running"] = json!(0), Rejected("tick 8: group g's capacity counts more contracted compiles than running ones")),
        row("aged.steps[][step=kept].standing.capacity.group.running", "K's 0 → 1, more than its party's", |j| k_capacity(j)["group"]["running"] = json!(1), Rejected("tick 8: group g's capacity counts more of its group's compiles than its party's, or its party's than all")),
        row("aged.steps[][step=kept].standing.capacity.group.contracted", "K's 0 → 1", |j| k_capacity(j)["group"]["contracted"] = json!(1), Rejected("tick 8: group g's capacity counts more contracted compiles than running ones")),
        row("aged.steps[][step=kept].standing.capacity.party.running", "K's 0 → 2, above all running", |j| k_capacity(j)["party"]["running"] = json!(2), Rejected("tick 8: group g's capacity counts more of its group's compiles than its party's, or its party's than all")),
        row("aged.steps[][step=kept].standing.capacity.party.contracted", "K's 0 → 1", |j| k_capacity(j)["party"]["contracted"] = json!(1), Rejected("tick 8: group g's capacity counts more contracted compiles than running ones")),
        row("decisions[].decision.one=aged_head_fitting", "K's tick 7: the fitting head AgedHeadFitting → AgedHead", |j| j[0]["decisions"][0]["decision"] = json!({"one": "aged_head"}), Rejected("tick 7: Job { group: \"g\", job: \"head\" } is One(AgedHead), and its derivation says Is(AgedHeadFitting)")),
        row("aged.steps[][step=chosen].chosen.compile.job", "K's tick 7 chose g/other", |j| j[0]["aged"]["steps"][0]["chosen"]["compile"]["job"] = json!("other"), Rejected("tick 7: other was chosen, the turn's walk gives head")),
        row("aged.steps[][step=chosen].chosen.compile.group", "K's tick 7 chose h2/head", |j| j[0]["aged"]["steps"][0]["chosen"]["compile"]["group"] = json!("h2"), Rejected("tick 7: head was chosen, the turn's walk gives head")),
    ]
}

/// G: `reserving`'s tick 7, its 6 GiB claim head reserving for w; tick 8, w has ended and x runs: the head ended in
/// vain and gives way until x ends; tick 10, after a silent tick, x has ended and y runs: it gives way no longer, and
/// its decision is a waiting one.
fn giving() -> Vec<Value> {
    numbered(&giving_records())
}
/// G's records.
fn giving_records() -> [Record; 3] {
    let earlier = reserving(6 * GIB, 16 * GIB);
    let attempt = |job: &str| Counted::Compile { attempt: Attempt { job: job.into(), started_at: At(1.0) }, bytes: 4 * GIB, finish: 61.0 };
    let left = earlier.left;
    let passed = Turn { party: "p".into(), held: false };
    let ended = Record {
        tick: 8,
        own_work: vec![attempt("x")],
        aged: Choice {
            before: earlier.aged.after.clone(),
            steps: vec![Step::Kept { head: earlier.aged.head.clone().unwrap(), standing: Standing::of(true, 6 * GIB, Lacks::Nothing), verdict: Verdict::EndedInVain }],
            after: Some(passed.clone()),
            head: None,
        },
        aged_reserved: 0,
        admitted: [Entry::Returned { bytes: 0, budget: left }].into_iter().chain(completions(57.0, left)).collect(),
        left_after_launch: left.floor(),
        ..earlier.clone()
    };
    let ended = first(ended, &[(claim("head"), Decision::GivingWay)]);
    let settled = Record {
        tick: 10,
        own_work: vec![attempt("y")],
        aged: Choice { before: Some(passed.clone()), steps: Vec::new(), after: Some(passed), head: None },
        ..ended.clone()
    };
    let settled = first(settled, &[(claim("head"), Decision::WaitingForTurn)]);
    [earlier, ended, settled]
}

/// The rows of the giving-way set, a carried fact, on G.
fn giving_rows() -> Vec<Row> {
    vec![
        row("decisions[].decision.one=giving_way", "G's tick 8: the head GivingWay → WaitingForTurn", |j| j[1]["decisions"][0]["decision"] = json!({"one": "waiting_for_turn"}), Rejected("tick 8: Claim(Registration { claim: \"head\", order: 1 }) is One(WaitingForTurn), and its derivation says Is(GivingWay)")),
        row("aged.steps[][step=kept].verdict=ended_in_vain", "G's tick 8: the head kept, though w, all it recorded, ended", |j| j[1]["aged"]["steps"][0]["verdict"] = json!({"kept": {"fits": false}}), Rejected("tick 8: the head head was Kept { fits: false }, its figures say EndedInVain")),
    ]
}

/// X: `reserving`'s tick 7, its 6 GiB claim head reserving for w; tick 8, the claim has ended (its holder withdrew it
/// before its grant): the kept head's verdict is Ended, and the turn passes.
fn ended() -> Vec<Value> {
    let earlier = reserving(6 * GIB, 16 * GIB);
    let left = earlier.left;
    let ended = Record {
        tick: 8,
        aged: Choice {
            before: earlier.aged.after.clone(),
            steps: vec![Step::Kept { head: earlier.aged.head.clone().unwrap(), standing: Standing::of(false, 6 * GIB, Lacks::Nothing), verdict: Verdict::Ended }],
            after: Some(Turn { party: "p".into(), held: false }),
            head: None,
        },
        aged_reserved: 0,
        admitted: [Entry::Returned { bytes: 0, budget: left }].into_iter().chain(completions(57.0, left)).collect(),
        left_after_launch: left.floor(),
        ..earlier.clone()
    };
    numbered(&[earlier, first(ended, &[])])
}
fn ended_rows() -> Vec<Row> {
    vec![
        row("aged.steps[][step=kept].verdict=ended", "X's tick 8: the ended head kept", |j| j[1]["aged"]["steps"][0]["verdict"] = json!({"kept": {"fits": false}}), Rejected("tick 8: the head head was Kept { fits: false }, its figures say Ended")),
        row("aged.steps[][step=kept].standing.pending", "X's tick 8: the head still pending", |j| j[1]["aged"]["steps"][0]["standing"]["pending"] = json!(true), Rejected("tick 8: the head head was Ended, its figures say Kept { fits: false }")),
    ]
}

/// V: `reserving`'s tick 7, its 5 GiB claim head reserving for w; tick 8, memory held outside the service lowered S to
/// 4 GiB while w still runs: the kept head is no longer serviceable, its verdict is Released, and its party keeps the
/// turn.
fn released() -> Vec<Value> {
    let earlier = reserving(5 * GIB, 18 * GIB);
    let left = Budget::of(14 * GIB).less(4 * GIB).less(10 * GIB);
    let released = Record {
        tick: 8,
        available: 14 * GIB,
        left,
        serviceable: serviceable(left, 4 * GIB),
        aged: Choice {
            before: earlier.aged.after.clone(),
            steps: vec![Step::Kept { head: earlier.aged.head.clone().unwrap(), standing: Standing::of(true, 5 * GIB, Lacks::Nothing), verdict: Verdict::Released }],
            after: earlier.aged.after.clone(),
            head: None,
        },
        aged_reserved: 0,
        admitted: [Entry::Returned { bytes: 0, budget: left }].into_iter().chain(completions(57.0, left)).collect(),
        left_after_launch: left.floor(),
        ..earlier.clone()
    };
    let released = first(released, &[(claim("head"), Decision::NotServiceable)]);
    numbered(&[earlier, released])
}
fn released_rows() -> Vec<Row> {
    vec![
        row("aged.steps[][step=kept].verdict=released", "V's tick 8: the head no longer serviceable kept", |j| j[1]["aged"]["steps"][0]["verdict"] = json!({"kept": {"fits": false}}), Rejected("tick 8: the head head was Kept { fits: false }, its figures say Released")),
        row("available", "V's tick 8: 14 → 15 GiB, S 5 GiB: the head serviceable again", |j| j[1]["available"] = json!(15 * G), Rejected("tick 8: the head head was Released, its figures say Kept { fits: false }")),
    ]
}

/// Y: `launched_after_serve`'s tick, whose 1 GiB launch leaves 1 GiB, with an ordinary 512 MiB claim c2 granted in
/// the ordinary close from what the launch left.
fn closing() -> Vec<Value> {
    let mut record = launched_after_serve();
    let half = Budget::of(GIB / 2);
    let grant = Entry::Granted { phase: Persisting::Close, claim: "c2".into(), order: order("c2"), party: "q".into(), class: Class::Ordinary, bytes: GIB / 2, budget: Budget::of(GIB), head: false };
    record.admitted.insert(3, grant);
    for entry in &mut record.admitted[4..] {
        if let Entry::Persisted { budget, .. } = entry {
            *budget = half;
        }
    }
    numbered(&[first(record, &[])])
}
fn closing_rows() -> Vec<Row> {
    vec![
        row("admitted[][event=granted].phase=close", "Y's c2 granted in the serve phase, after the launch", |j| j[0]["admitted"][3]["phase"] = json!("serve"), Rejected("tick 1: the Serve phase's completion is not its last entry")),
    ]
}

/// The records of `ticks` that `instance`'s journal writes, each tick with the decisions of the requests pending at
/// its end: the journal lists the decisions that changed and writes a record when the service's does.
fn written(instance: Instance, ticks: Vec<(Record, BTreeMap<Subject, Decisions>)>) -> Vec<Value> {
    let mut journal = Journal::new(instance);
    for (record, pending) in ticks {
        journal.finish(record, pending);
    }
    journal.history.iter().filter(|(_, written)| *written).map(|(record, _)| serde_json::to_value(record).unwrap()).collect()
}
/// `claims` waiting for their turn, each a decision of a request the record does not describe.
fn waiting(claims: &[&str]) -> BTreeMap<Subject, Decisions> {
    claims.iter().map(|claim| (Subject::Claim(registration(claim)), Decisions::One(Decision::WaitingForTurn))).collect()
}
/// `record` without `claim`'s listed decision.
fn unlisted(record: &mut Value, claim: &str) {
    record["decisions"].as_array_mut().unwrap().retain(|decided| decided["subject"] != json!({"claim": {"claim": claim, "order": order(claim)}}));
}

/// W: G's ticks 7 and 8, then tick 9: x, which the head recorded when it ended in vain, still runs, so the head gives
/// way; the first decision of a claim v the record does not describe writes it.
fn giving_on() -> Vec<Value> {
    let [earlier, ended, _] = giving_records();
    let passed = ended.aged.after.clone();
    let on = Record { tick: 9, aged: Choice { before: passed.clone(), steps: Vec::new(), after: passed, head: None }, ..ended.clone() };
    numbered(&[earlier, ended, decided(on, waiting(&["v"]))])
}

/// Z: `zero_head`'s 0-byte head chosen at tick 7, kept at tick 8 as tick 7 left it, not fitting, and kept again at
/// tick 10, after the silent tick 9, fitting.
fn zero() -> Vec<Value> {
    let chosen = zero_head();
    let journal = [zero_kept(&chosen, 8, false), zero_kept(&chosen, 10, true)];
    numbered(&[[chosen].as_slice(), &journal].concat())
}

/// E: K's fitting compile head with no compile running: chosen at tick 7 and kept at tick 8, it records no work.
fn unrecorded() -> Vec<Value> {
    let capacity = Capacity { idle: 3, running: Units::default(), ..outside_idle() };
    let idle = |mut record: Record| {
        record.own_work.clear();
        record.serviceable = record.left.floor();
        record.aged.head.iter_mut().for_each(|head| head.recorded.clear());
        for step in &mut record.aged.steps {
            match step {
                Step::Kept { head, .. } => head.recorded.clear(),
                Step::Chosen { listed, .. } => listed.iter_mut().for_each(|entry| entry.candidate.lacks = Lacks::Capacity(capacity.clone())),
            }
        }
        record
    };
    numbered(&kept_compile_journal(capacity.clone()).into_iter().map(idle).collect::<Vec<_>>())
}

/// Q: what the last launches left, a tick apart and across a silent tick. Instance A's quiet tick 6 leaves 4 GiB, and
/// its tick 7 lists the 5 GiB compile head; instance B's quiet tick 5 leaves 8 GiB, and its tick 7, after the silent
/// tick 6, lists the same head.
fn remainders() -> Vec<Value> {
    let b = other(2);
    let journal = [
        quiet(6, 4 * GIB),
        chosen_compile_head(),
        Record { instance: b.clone(), ..quiet(5, 8 * GIB) },
        Record { instance: b, ..chosen_compile_head() },
    ];
    numbered(&journal)
}

/// O: `reserving`'s tick 7, its 6 GiB claim head of party p listed beside party q's 1 GiB claim younger, eligible at
/// the same time: with no turn, the head's earlier catalogue place gets it the head.
fn tied() -> Vec<Value> {
    let mut record = reserving(6 * GIB, 16 * GIB);
    if let Step::Chosen { listed, .. } = &mut record.aged.steps[0] {
        let younger = Candidate { id: claim("younger"), party: "q".into(), eligible_at: At(0.0), catalogue: Catalogue::Claim { order: 2 }, bytes: GIB, lacks: Lacks::Nothing };
        listed.push(Listed { party: "q".into(), candidate: younger });
    }
    numbered(&[first(record, &[(claim("head"), Decision::AgedHead), (claim("younger"), Decision::WaitingForTurn)])])
}
fn tied_rows() -> Vec<Row> {
    vec![
        row("aged.steps[][step=chosen].listed[].candidate.catalogue.claim.order", "O's head at order 3, after younger's 2 at the same eligibility", |j| { let head = &mut j[0]["aged"]["steps"][0]["listed"][0]["candidate"]; head["catalogue"]["claim"]["order"] = json!(3); head["id"]["claim"]["order"] = json!(3); }, Rejected("tick 7: head was chosen, the turn's walk gives younger")),
    ]
}

/// C: `passed_then`'s tick 7: party p's compile g/c passes, and p relists g/c2, eligible at the same time in the same
/// group, at the next index: younger by its catalogue place alone.
fn relisted() -> Vec<Value> {
    let c2 = listed("p", compile_id("c2"), 0.0, Catalogue::Compile { position: 0, index: 1 }, Lacks::Capacity(outside_idle()));
    numbered(&[passed_then(vec![c2, b()])])
}
fn relisted_rows() -> Vec<Row> {
    vec![
        row("aged.steps[][step=chosen].listed[].candidate.catalogue.compile.index", "C's relisted c2 at index 0, no younger than the dropped c", |j| j[0]["aged"]["steps"][1]["listed"][0]["candidate"]["catalogue"]["compile"]["index"] = json!(0), Rejected("tick 7: after c was dropped, its party lists no younger candidate")),
        row("aged.steps[][step=chosen].verdict=passed", "C's c released, not passed", |j| j[0]["aged"]["steps"][0]["verdict"] = json!("released"), Rejected("tick 7: the chosen c was Released, its figures say Passed")),
        row("decisions[].subject.group.group", "C's group g's decision listed for g2", |j| { for decided in j[0]["decisions"].as_array_mut().unwrap() { if decided["subject"] == json!({"group": {"group": "g"}}) { decided["subject"]["group"]["group"] = json!("g2"); } } }, Rejected("tick 7: Group { group: \"g\" }'s derivation says Includes(WaitingForTurn), its last listed decision is None")),
    ]
}

/// R: a claim granted, then released and registered again under its ID, granted again. Instance A's tick 1 decides
/// claims x and v waiting; ticks 2, 3 and 4 grant v; tick 5 is silent; tick 6 grants x and v. Instance B's tick 1
/// grants v and x. Each tick's decisions are the ones A's and B's journals list.
fn regranted() -> (Table, Cases) {
    let a = Instance::default();
    let b = other(2);
    let mut journal = written(a.clone(), vec![
        (quiet(1, 8 * GIB), waiting(&["x", "v"])),
        (granting(a.clone(), 2, &["v"]), waiting(&["x"])),
        (granting(a.clone(), 3, &["v"]), waiting(&["x"])),
        (granting(a.clone(), 4, &["v"]), waiting(&["x"])),
        (quiet(5, 8 * GIB), waiting(&["x"])),
        (granting(a, 6, &["x", "v"]), BTreeMap::new()),
    ]);
    journal.extend(written(b.clone(), vec![(granting(b, 1, &["v", "x"]), BTreeMap::new())]));
    let cases = Cases {
        consecutive: on("R", vec![row("decisions", "R's A tick 3 listing v's unchanged Admitted", |j| j[2]["decisions"] = json!([{"subject": {"claim": {"claim": "v", "order": 13}}, "decision": {"one": "admitted"}}]), Rejected("tick 3: Claim(Registration { claim: \"v\", order: 13 }) is listed with One(Admitted), the decision the tick before held for it"))]),
        gap: on("R", vec![row("decisions", "R's A tick 6 without v's Admitted: an admission lapses through the silent tick 5", |j| unlisted(&mut j[4], "v"), Rejected("tick 6: Claim(Registration { claim: \"v\", order: 13 })'s derivation says Is(Admitted), its last listed decision is None"))]),
        boundary: on("R", vec![row("decisions", "R's B tick 1 without v's Admitted, which A held", |j| unlisted(&mut j[5], "v"), Rejected("tick 1: Claim(Registration { claim: \"v\", order: 13 })'s derivation says Is(Admitted), its last listed decision is None"))]),
    };
    (table("R", journal, Vec::new()), cases)
}
/// `instance`'s tick `tick`: 8 GiB available and left, no D, and each of `claims`, 1 GiB of party r, granted in turn
/// from it in the share pass.
fn granting(instance: Instance, tick: u64, claims: &[&str]) -> Record {
    let left = Budget::of(8 * GIB);
    let mut budget = left;
    let mut admitted = Vec::new();
    for claim in claims {
        admitted.push(Entry::Granted { phase: Persisting::Serve, claim: (*claim).into(), order: order(claim), party: "r".into(), class: Class::Ordinary, bytes: GIB, budget, head: false });
        budget = budget.less(GIB);
    }
    admitted.push(Entry::Returned { bytes: 0, budget });
    admitted.extend(completions(57.0, budget));
    Record { instance, tick, available: 8 * GIB, left, serviceable: 8 * GIB, admitted, left_after_launch: budget.floor(), ..Record::default() }
}

/// L: a compile job launched, then, its attempt gone, launched again. Instance A launches g/j at ticks 1, 2 and 3, and
/// after the silent tick 4 at tick 5; instance B launches it at its tick 1.
fn relaunched() -> (Table, Cases) {
    let a = Instance::default();
    let b = other(2);
    let launched = |instance: &Instance, tick: u64| (Record { instance: instance.clone(), tick, decisions: Vec::new(), ..launched_after_serve() }, BTreeMap::new());
    let silent = (quiet(4, 2 * GIB), BTreeMap::new());
    let mut journal = written(a.clone(), vec![launched(&a, 1), launched(&a, 2), launched(&a, 3), silent, launched(&a, 5)]);
    journal.extend(written(b.clone(), vec![launched(&b, 1)]));
    let cases = Cases {
        consecutive: on("L", vec![row("decisions", "L's A tick 2 listing j's unchanged Admitted", |j| j[1]["decisions"] = json!([{"subject": {"job": {"group": "g", "job": "j"}}, "decision": {"one": "admitted"}}]), Rejected("tick 2: Job { group: \"g\", job: \"j\" } is listed with One(Admitted), the decision the tick before held for it"))]),
        gap: on("L", vec![row("decisions", "L's A tick 5 without j's Admitted: an admission lapses through the silent tick 4", |j| j[3]["decisions"] = json!([]), Rejected("tick 5: Job { group: \"g\", job: \"j\" }'s derivation says Is(Admitted), its last listed decision is None"))]),
        boundary: on("L", vec![row("decisions", "L's B tick 1 without j's Admitted, which A held", |j| j[4]["decisions"] = json!([]), Rejected("tick 1: Job { group: \"g\", job: \"j\" }'s derivation says Is(Admitted), its last listed decision is None"))]),
    };
    let rows = vec![
        row("decisions[].subject.job.group", "L's A tick 1: j's Admitted listed for group h", |j| j[0]["decisions"][0]["subject"]["job"]["group"] = json!("h"), Rejected("tick 1: Job { group: \"h\", job: \"j\" } is One(Admitted), which a request the record does not describe cannot be")),
        row("decisions[].subject.job.job", "L's A tick 1: j's Admitted listed for j2", |j| j[0]["decisions"][0]["subject"]["job"]["job"] = json!("j2"), Rejected("tick 1: Job { group: \"g\", job: \"j2\" } is One(Admitted), which a request the record does not describe cannot be")),
    ];
    (table("L", journal, rows), cases)
}

/// A kind of admission's table and cases.
type Kind = fn() -> (Table, Cases);

/// Each kind of admission (`Record::admissions`), with its table and its cases: an exhaustive match over what admits
/// a request, so a new kind of entry does not compile until it has them, or is shown to admit nothing.
fn admission(entry: &Entry) -> Option<(&'static str, Kind)> {
    match entry {
        Entry::Granted { .. } => Some(("granted", regranted as Kind)),
        Entry::Launched { .. } => Some(("launched", relaunched as Kind)),
        Entry::Reserved { .. } | Entry::Turn { .. } | Entry::Returned { .. } | Entry::Persisted { .. } => None,
    }
}
/// Each carried fact's cases, by an exhaustive match over the facts `Carried::facts` names.
/// `record` with its share grant of `claim` renamed w2 of registration 4 and given `party`, the subject of its
/// decision renamed too, so its budgets, decisions and intra-tick index stay as they were.
pub(super) fn regrouped(record: &mut Value, claim: &str, party: &str) {
    let w2 = json!({"claim": {"claim": "w2", "order": 4}});
    for entry in record["admitted"].as_array_mut().unwrap() {
        if entry["event"] == "granted" && entry["claim"] == claim {
            entry["claim"] = json!("w2");
            entry["order"] = json!(4);
            entry["party"] = json!(party);
        }
    }
    for decided in record["decisions"].as_array_mut().unwrap() {
        if decided["subject"]["claim"]["claim"] == claim {
            decided["subject"] = w2.clone();
        }
    }
}
fn fact_cases(fact: Fact) -> Cases {
    match fact {
        Fact::Tick => Cases {
            consecutive: on("J", vec![row("tick", "J4's tick 11 → 9, J3's", |j| j[J4]["tick"] = json!(9), Rejected("tick 9 follows tick 9 of its instance"))]),
            gap: on("J", vec![row("tick", "J4's tick 11 → 10: no silent tick", |j| j[J4]["tick"] = json!(10), Irrelevant("whether tick 10 was silent changes no fact J4 reads: it lists no compile job, holds no head, and decides no request J3 admitted (`Carried::across`)"))]),
            boundary: on("J", vec![row("tick", "B1's tick 3 → 8, the counter J2 holds", |j| j[B1]["tick"] = json!(8), Irrelevant("each instance's records are derived on their own (`derive_journal`): A's tick 8 carries nothing to B's"))]),
        },
        Fact::Reserving => Cases {
            consecutive: Case::Cannot("J1 reserves and J2 is its next tick: a record at the next tick is what a reservation obliges, so only a silent tick or the instance's end can break it, the next two cases"),
            gap: Case::Rows(vec![
                ("J", row("tick", "J2's tick 8 → 9: a silent tick after J1 reserved", |j| j[J2]["tick"] = json!(9), Rejected("tick 7: a head reserved, and the instance's next record is tick 9"))),
            ]),
            boundary: on("J", vec![row("", "instance A ending at its reserving J1, instance B's record after it", |j| { j.truncate(B1 + 1); j.remove(J2); }, Irrelevant("an instance can end at any tick, a reserving one too: its successor starts from §3.7's state (`derive_journal`), and a reservation obliges only its own instance's next record"))]),
        },
        Fact::Precedence(Part::Head) => Cases {
            consecutive: on("J", vec![row("aged.steps", "J2's kept head not judged", |j| { j[J2]["aged"]["steps"].as_array_mut().unwrap().clear(); }, Rejected("tick 8: the head the tick before left, h, is not judged first"))]),
            gap: on("Z", vec![row("aged.steps", "Z's tick 10 without judging the head tick 8 left", |j| { j[2]["aged"]["steps"].as_array_mut().unwrap().clear(); }, Rejected("tick 10: the head the tick before left, zero, is not judged first"))]),
            boundary: on("J", vec![row("instance.pid", "J3 written by B's instance: B's tick 3 left no head", |j| j[J3]["instance"] = j[B1]["instance"].clone(), Rejected("tick 9: the record judges a head the tick before left, and that tick left none"))]),
        },
        Fact::Precedence(Part::Gpu) => Cases {
            consecutive: Case::Cannot(NO_GPU_TURN),
            gap: Case::Cannot(NO_GPU_TURN),
            boundary: Case::Cannot(NO_GPU_TURN),
        },
        Fact::Precedence(Part::Turn) => Cases {
            consecutive: on("J", vec![row("aged.before", "J2's figure: no turn", |j| j[J2]["aged"]["before"] = Value::Null, Rejected("tick 8: the record's /aged/before is null"))]),
            gap: on("J", vec![row("aged.before.party", "J4's turn after silent tick 10: r's", |j| { j[J4]["aged"]["before"]["party"] = json!("r"); j[J4]["aged"]["after"]["party"] = json!("r"); }, Rejected("tick 11: the record's /aged/after/party is \"r\", its derivation \"q\""))]),
            boundary: on("J", vec![row("aged.before", "B1 starting from and keeping J2's turn, q's", |j| { j[B1]["aged"]["before"] = json!({"party": "q", "held": true}); j[B1]["aged"]["after"] = json!({"party": "q", "held": true}); }, Rejected("tick 3: the record's /aged/after is {\"held\":true,\"party\":\"q\"}, its derivation null"))]),
        },
        Fact::Precedence(Part::GivingWay) => Cases {
            consecutive: on("W", vec![row("decisions[].decision.one=waiting_for_turn", "W's tick 9 decides the head WaitingForTurn while x, which it recorded, runs", |j| j[2]["decisions"] = json!([{"subject": {"claim": {"claim": "head", "order": 1}}, "decision": {"one": "waiting_for_turn"}}]), Rejected("tick 9: Claim(Registration { claim: \"head\", order: 1 }) is One(WaitingForTurn), which a request the record does not describe cannot be"))]),
            gap: Case::Rows(vec![
                ("G", row("own_work[].compile.attempt.job", "G's tick 10: x, which the head recorded when it ended in vain, still runs", |j| j[2]["own_work"][0]["compile"]["attempt"]["job"] = json!("x"), Rejected("tick 10: Claim(Registration { claim: \"head\", order: 1 }) is One(WaitingForTurn), which a request the record does not describe cannot be"))),
                ("G", row("decisions[].decision.one=waiting_for_turn", "G's tick 10 decides the head GivingWay after x ended", |j| j[2]["decisions"][0]["decision"] = json!({"one": "giving_way"}), Rejected("tick 10: Claim(Registration { claim: \"head\", order: 1 }) is One(GivingWay), which a request the record does not describe cannot be"))),
            ]),
            boundary: on("G", vec![row("instance.pid", "G's tick 10 written by another instance, deciding the head GivingWay", |j| { j[2]["instance"]["pid"] = json!(7); j[2]["decisions"][0]["decision"] = json!({"one": "giving_way"}); }, Rejected("tick 10: Claim(Registration { claim: \"head\", order: 1 }) is One(GivingWay), which a request the record does not describe cannot be"))]),
        },
        Fact::Fits => Cases {
            consecutive: on("Z", vec![row("aged.steps[][step=kept].head.fits", "Z's tick 8: the kept head fitting, as tick 7 did not leave it", |j| j[1]["aged"]["steps"][0]["head"]["fits"] = json!(true), Rejected("tick 8: the record's /aged/steps/0/head/fits is true, its derivation false"))]),
            gap: on("Z", vec![row("aged.steps[][step=kept].head.fits", "Z's tick 10: the kept head not fitting", |j| j[2]["aged"]["steps"][0]["head"]["fits"] = json!(false), Irrelevant("a head reserving 0 bytes is kept through silent ticks, whose verdicts set whether it fits: the derivation marks it unknown (`Carried::unknown`), and no rule reads it"))]),
            boundary: Case::Cannot("an instance's first record judges no kept head (`Requests::read`), so no fit is carried into it"),
        },
        Fact::Remainder => Cases {
            consecutive: on("Q", vec![row("aged.steps[][step=chosen].listed[].candidate.bytes", "Q's A tick 7: the head's cell 5 → 4 GiB, which fits what tick 6's launches left", |j| j[1]["aged"]["steps"][0]["listed"][0]["candidate"]["bytes"] = json!(4 * G), Rejected("tick 7: compile job head is listed, and it does not wait for memory"))]),
            gap: on("Q", vec![row("aged.steps[][step=chosen].listed[].candidate.bytes", "Q's B tick 7, after a silent tick: the head's cell 5 → 4 GiB", |j| j[3]["aged"]["steps"][0]["listed"][0]["candidate"]["bytes"] = json!(4 * G), Rejected("tick 7: the record's /admitted/0/bytes is 5368709120, its derivation 4294967296"))]),
            boundary: on("J", vec![row("tick", "J1's tick 7 → 1: the instance's first tick, before any launch", |j| j[J1]["tick"] = json!(1), Rejected("tick 1: compile job c is listed, and it does not wait for memory"))]),
        },
        Fact::Decisions => Cases {
            consecutive: on("R", vec![row("decisions", "R's A tick 2 without v's Admitted, after tick 1's WaitingForTurn", |j| unlisted(&mut j[1], "v"), Rejected("tick 2: Claim(Registration { claim: \"v\", order: 13 })'s derivation says Is(Admitted), its last listed decision is Some(One(WaitingForTurn))"))]),
            gap: on("R", vec![row("decisions", "R's A tick 6 without x's Admitted: tick 1's WaitingForTurn persists through the silent tick 5", |j| unlisted(&mut j[4], "x"), Rejected("tick 6: Claim(Registration { claim: \"x\", order: 14 })'s derivation says Is(Admitted), its last listed decision is Some(One(WaitingForTurn))"))]),
            boundary: on("R", vec![row("decisions", "R's B tick 1 without x's Admitted: A's WaitingForTurn is not B's", |j| unlisted(&mut j[5], "x"), Rejected("tick 1: Claim(Registration { claim: \"x\", order: 14 })'s derivation says Is(Admitted), its last listed decision is None"))]),
        },
        Fact::Parties => Cases {
            consecutive: on("J", vec![row("admitted[][event=granted].claim", "J3's grant of y renamed w2 of registration 4, party t", |j| regrouped(&mut j[J3], "y", "t"), Rejected("tick 9: claim w2 of registration 4 is party \"t\", and party \"s\" in an earlier record of its instance"))]),
            gap: on("J", vec![row("admitted[][event=granted].claim", "J4's grant of L renamed w2 of registration 4, party t, after the silent tick 10", |j| regrouped(&mut j[J4], "L", "t"), Rejected("tick 11: claim w2 of registration 4 is party \"t\", and party \"s\" in an earlier record of its instance"))]),
            boundary: on("J", vec![row("admitted[][event=granted].claim", "B1's grant of b8 renamed w2 of registration 4, party t", |j| regrouped(&mut j[B1], "b8", "t"), Irrelevant("each instance's records are derived on their own (derive_journal); a hand-over keeps the saved party, which T4.13 and X-PA test"))]),
        },
        Fact::Held => Cases {
            consecutive: on("J", vec![row("decisions", "J2 listing h's unchanged AgedHead", |j| j[J2]["decisions"] = json!([{"subject": {"claim": {"claim": "h", "order": 1}}, "decision": {"one": "aged_head"}}]), Rejected("tick 8: Claim(Registration { claim: \"h\", order: 1 }) is listed with One(AgedHead), the decision the tick before held for it"))]),
            gap: on("Z", vec![row("decisions", "Z's tick 10 listing the head's AgedHead", |j| j[2]["decisions"] = json!([{"subject": {"job": {"group": "g", "job": "zero"}}, "decision": {"one": "aged_head"}}]), Irrelevant("what the tick before held is unknown after a silent tick, which may have held another decision (`Carried::across` empties it); the derived decision still binds the one listed"))]),
            boundary: Case::Cannot("an instance's first record has no tick before it in its instance (`Carried::initial` holds nothing): R's B tick 1 lists v's Admitted, which A's tick 6 held, and replays"),
        },
    }
}

/// Every table with its rows: the leaves' (J, K, G, O, C, E), the carried facts' (W, Z, Q), and each kind of
/// admission's, found by `admission` in every table's records. Returns them, and the cases that cannot arise.
fn tables() -> (Vec<Table>, Vec<(String, &'static str)>) {
    let mut tables = vec![
        table("J", base(), rows()),
        table("K", kept(), kept_rows()),
        table("G", giving(), giving_rows()),
        table("X", ended(), ended_rows()),
        table("V", released(), released_rows()),
        table("Y", closing(), closing_rows()),
        table("O", tied(), tied_rows()),
        table("C", relisted(), relisted_rows()),
        table("W", giving_on(), Vec::new()),
        table("Z", zero(), Vec::new()),
        table("E", unrecorded(), Vec::new()),
        table("Q", remainders(), Vec::new()),
    ];
    let entries = |table: &Table| -> Vec<Entry> {
        table.base.iter().flat_map(|record| serde_json::from_value::<Vec<Entry>>(record["admitted"].clone()).unwrap()).collect()
    };
    let kinds: BTreeMap<&'static str, Kind> = tables.iter().flat_map(entries).filter_map(|entry| admission(&entry)).collect();
    let mut keyed: Vec<(Key, Cases)> = Carried::initial()
        .facts()
        .into_iter()
        .map(|fact| (Key { kind: "carried", case: format!("{fact:?}") }, fact_cases(fact)))
        .collect();
    for (name, kind) in kinds {
        let (table, cases) = kind();
        tables.push(table);
        keyed.push((Key { kind: "admission", case: name.to_owned() }, cases));
    }
    let mut cannot = Vec::new();
    for (key, cases) in keyed {
        for (case, across) in [(cases.consecutive, "consecutive"), (cases.gap, "gap"), (cases.boundary, "boundary")] {
            let named = format!("{} {across}", key.case);
            match case {
                Case::Rows(rows) => {
                    for (name, row) in rows {
                        let table = tables.iter_mut().find(|table| table.name == name).unwrap_or_else(|| panic!("no table {name}"));
                        table.rows.push((Key { kind: key.kind, case: named.clone() }, row));
                    }
                }
                Case::Cannot(reason) => cannot.push((format!("{} {named}", key.kind), reason)),
            }
        }
    }
    for Unwritten { variant: (name, discriminator), table, row, .. } in unwritten() {
        let table = tables.iter_mut().find(|named| named.name == table).unwrap_or_else(|| panic!("no table {table}"));
        table.rows.push((Key { kind: "unwritten", case: format!("{name}{discriminator}") }, row));
    }
    (tables, cannot)
}

/// A variant of a record-schema enum that no record holds: why, and a row on a base journal that writes it there,
/// which the checker must refuse.
struct Unwritten {
    variant: (&'static str, Discriminator),
    reason: &'static str,
    table: &'static str,
    row: Row,
}
/// Why no record holds a GPU turn or a GPU decision, and nothing of the GPU head is carried.
const NO_GPU_TURN: &str = "no claim is of the GPU class (`Class::of` makes a GPU-place claim ordinary), so the GPU pass holds no claim: a GPU turn is not of the GPU pass (`derive_gpu`), and no GPU head is held, carried or shown";
/// J3's GPU pass holding the 1 GiB claim gpu, which J3 names nowhere else, with `decision` and its fields.
fn gpu_turn(journal: &mut [Value], decision: Value) {
    let mut turn = json!({"claim": "gpu", "order": 10, "bytes": G, "budget": 0});
    turn.as_object_mut().unwrap().extend(decision.as_object().unwrap().clone());
    journal[J3]["gpu"] = json!([turn]);
}
/// J3 listing the claim gpu, which it does not describe, with `decision`.
fn gpu_listed(journal: &mut [Value], decision: &str) {
    journal[J3]["decisions"].as_array_mut().unwrap().push(json!({"subject": {"claim": {"claim": "gpu", "order": 10}}, "decision": {"one": decision}}));
}
/// The variants no record holds.
fn unwritten() -> Vec<Unwritten> {
    const ORDINARY: &str = "tick 9: GPU claim gpu of 1073741824 bytes is of class Ordinary, not of the GPU pass";
    const HEAD: &str = "tick 9: Claim(Registration { claim: \"gpu\", order: 10 }) is One(GpuHead), which a request the record does not describe cannot be";
    const BEHIND: &str = "tick 9: Claim(Registration { claim: \"gpu\", order: 10 }) is One(BehindGpuHead), which a request the record does not describe cannot be";
    let gpu = |variant: &str, row: Row| Unwritten {
        variant: ("Gpu", Discriminator::Internal { tag: "decision".into(), variant: variant.into() }),
        reason: NO_GPU_TURN,
        table: "J",
        row,
    };
    let decision = |variant: &str, row: Row| Unwritten { variant: ("Decision", Discriminator::Unit(variant.into())), reason: NO_GPU_TURN, table: "J", row };
    vec![
        Unwritten {
            variant: ("Class", Discriminator::Unit("gpu".into())),
            reason: "every grant the share passes record and every launch is of the ordinary or the large class, which the replay derives from its bytes (`Class::of`); no claim is of the GPU class, and the GPU pass's turns name no class (`Precedence::gpu_turn`)",
            table: "J",
            row: row("admitted[][event=granted].class=ordinary", "J3's y granted by the share pass as of the GPU class", |j| j[J3]["admitted"][2]["class"] = json!("gpu"), Rejected("tick 9: the record's /admitted/2/class is \"gpu\", its derivation \"ordinary\"")),
        },
        gpu("granted", row("gpu", "J3's GPU pass granting gpu", |j| gpu_turn(j, json!({"decision": "granted"})), Rejected(ORDINARY))),
        gpu("head", row("gpu", "J3's GPU pass holding gpu as its head", |j| gpu_turn(j, json!({"decision": "head", "recorded": []})), Rejected(ORDINARY))),
        gpu("ended_in_vain", row("gpu", "J3's GPU head gpu ending in vain", |j| gpu_turn(j, json!({"decision": "ended_in_vain", "recorded": []})), Rejected(ORDINARY))),
        gpu("giving_way", row("gpu", "J3's GPU pass with gpu giving way", |j| gpu_turn(j, json!({"decision": "giving_way"})), Rejected(ORDINARY))),
        gpu("behind_gpu_head", row("gpu", "J3's GPU pass with gpu behind a GPU head", |j| gpu_turn(j, json!({"decision": "behind_gpu_head"})), Rejected(ORDINARY))),
        gpu("not_serviceable", row("gpu", "J3's GPU pass with gpu not serviceable", |j| gpu_turn(j, json!({"decision": "not_serviceable"})), Rejected(ORDINARY))),
        decision("gpu_head", row("decisions", "J3 listing gpu as the GPU head", |j| gpu_listed(j, "gpu_head"), Rejected(HEAD))),
        decision("behind_gpu_head", row("decisions", "J3 listing gpu behind the GPU head", |j| gpu_listed(j, "behind_gpu_head"), Rejected(BEHIND))),
    ]
}

#[test]
fn every_leaf_carried_fact_and_admission_mutated_alone_is_rejected_or_cannot_change_an_admission() {
    let print = std::env::var_os("R5_MUTATION_TABLE").is_some();
    let (tables, cannot) = tables();
    let mut failures = Vec::new();
    let mut leaves = BTreeSet::new();
    let mut covered = BTreeSet::new();
    let mut variants = BTreeMap::new();
    let normalizer = Normalizer::record();
    for table in &tables {
        let ran = run(table, &normalizer);
        for trial in &ran.trials {
            print.then(|| println!("R6ROW\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}", trial.table, trial.kind, trial.case, trial.path, trial.mutation, trial.verdict, trial.rule, trial.said));
            failures.extend((!trial.holds).then(|| format!("{} {} ({}): expected {} by {:?}, got {}", trial.table, trial.path, trial.mutation, trial.verdict, trial.rule, trial.said)));
            failures.extend((!ran.paths.contains(&trial.path)).then(|| format!("{} ({}): its path {} occurs in no record of table {}", trial.path, trial.mutation, trial.path, trial.table)));
            covered.insert(trial.path.clone());
        }
        for (variant, path) in ran.variants {
            variants.entry(variant).or_insert((table.name, path));
        }
        leaves.extend(ran.leaves);
    }
    for (case, reason) in &cannot {
        print.then(|| println!("R6CANNOT\t{case}\t{reason}"));
    }
    for leaf in &leaves {
        print.then(|| println!("R6LEAF\t{leaf}"));
    }
    for ((name, discriminator), (table, path)) in &variants {
        print.then(|| println!("R7VARIANT\t{name}\t{discriminator}\t{table}\t{path}"));
    }
    failures.extend(leaves.difference(&covered).map(|leaf| format!("the leaf {leaf} has no row")));
    let held: BTreeSet<(&'static str, Discriminator)> = variants.into_keys().collect();
    let obliged = normalizer.schema.variants();
    let mut accounted = held.clone();
    for Unwritten { variant, reason, table, row } in unwritten() {
        let (name, discriminator) = &variant;
        print.then(|| println!("R7UNWRITTEN\t{name}\t{discriminator}\t{table}\t{reason}"));
        let mut journal = tables.iter().find(|named| named.name == table).unwrap().base.clone();
        (row.edit)(&mut journal);
        let written = normalizer.occurrences(&journal).variants.contains_key(&variant);
        failures.extend((!written).then(|| format!("{} ({}) does not write the variant {discriminator} of {name}", row.path, row.mutation)));
        failures.extend(held.contains(&variant).then(|| format!("the variant {discriminator} of {name} occurs in a base journal, which its reason denies")));
        failures.extend((!obliged.contains(&variant)).then(|| format!("{discriminator} is no variant of the record-schema enum {name}")));
        accounted.insert(variant);
    }
    failures.extend(obliged.difference(&accounted).map(|(name, discriminator)| format!("the variant {discriminator} of {name} occurs in no base journal")));
    assert!(failures.is_empty(), "{} rows do not hold:\n{}", failures.len(), failures.join("\n"));
}

/// The leaf paths of every record the service wrote in a test run (each `*.records` file of the directory
/// `ADMISSION_CORPUS` names, one record a line: the library diagnostic's ticks, and the records of the integration
/// and Python tests' services) are leaf paths of the table's base journals.
#[test]
#[ignore = "reads a test run's records: ADMISSION_CORPUS names their directory"]
fn every_leaf_the_service_writes_is_a_leaf_of_the_base_journals() {
    let directory = std::env::var("ADMISSION_CORPUS").expect("ADMISSION_CORPUS names the test run's records");
    let normalizer = Normalizer::record();
    let mut corpus = BTreeSet::new();
    let mut records = 0usize;
    for file in std::fs::read_dir(&directory).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.extension().is_some_and(|extension| extension == "records")) {
        for line in std::fs::read_to_string(&file).unwrap().lines() {
            let record: Value = serde_json::from_str(line).unwrap_or_else(|error| panic!("{}: {error}", file.display()));
            corpus.extend(normalizer.occurrences(&[record]).first.into_iter().filter(|(_, (_, node))| node.leaf).map(|(path, _)| path));
            records += 1;
        }
    }
    let (tables, _) = tables();
    let base: BTreeSet<String> = tables.iter().flat_map(|table| normalizer.occurrences(&table.base).first.into_iter().filter(|(_, (_, node))| node.leaf).map(|(path, _)| path)).collect();
    let missing: Vec<&String> = corpus.difference(&base).collect();
    println!("R7CORPUS\trecords {records}\tleaf paths {}\tbase leaf paths {}\tmissing {}", corpus.len(), base.len(), missing.len());
    assert!(missing.is_empty(), "leaf paths the service writes that no base journal holds:\n{}", missing.iter().map(|path| path.as_str()).collect::<Vec<_>>().join("\n"));
}

/// Review round 7's normalizer probe: a registered internally tagged enum's variants, the unit verdicts of a kept step
/// and a tagged object with nothing beside its tag each have paths of their own, and a known tag still tells its
/// variants apart.
#[test]
fn the_normalizer_names_each_variant_of_a_record_schema_enum() {
    #[derive(serde::Serialize)]
    #[serde(tag = "mode", rename_all = "snake_case")]
    enum NewType {
        Admit { bytes: u64 },
        Wait { bytes: u64 },
    }
    let (admit, wait) = (NewType::Admit { bytes: 1 }, NewType::Wait { bytes: 1 });
    let mut enums = super::form::enums();
    enums.push(("NewType", [&admit, &wait].iter().map(|variant| serde_json::to_string(variant).unwrap()).collect()));
    let normalizer = Normalizer::new(Schema::of(enums));
    let leaves = |value: Value| -> BTreeSet<String> {
        let mut found = Vec::new();
        normalizer.walk(&value, String::new(), String::new(), &mut found);
        found.into_iter().filter(|node| node.leaf).map(|node| node.path).collect()
    };
    let admitting = leaves(json!({ "new_type": admit }));
    assert_ne!(admitting, leaves(json!({ "new_type": wait })), "a registered internally tagged enum's variants share {admitting:?}");
    // A unit variant on a whole record: a fragment has no types to say that a string is an enum's variant.
    let kept = kept()[1].clone();
    let verdicts: BTreeSet<BTreeSet<String>> = ["passed", "released", "ended", "ended_in_vain"]
        .into_iter()
        .map(|verdict| {
            let mut record = kept.clone();
            record["aged"]["steps"][0]["verdict"] = json!(verdict);
            normalizer.occurrences(&[record]).first.into_iter().filter(|(_, (_, node))| node.leaf).map(|(path, _)| path).collect()
        })
        .collect();
    assert_eq!(verdicts.len(), 4, "the four unit verdicts of a kept step share their paths");
    let payload_free = leaves(json!({"future": {"event": "new_unit_variant"}}));
    assert_eq!(payload_free, BTreeSet::from(["future[event=new_unit_variant]".to_owned()]), "a tagged object with nothing beside its tag");
    let granted = leaves(json!({"admitted": [{"event": "granted", "bytes": 1}]}));
    assert_ne!(granted, leaves(json!({"admitted": [{"event": "turn", "bytes": 1}]})), "the known tag event no longer tells its variants apart");
}
