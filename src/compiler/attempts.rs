//! A reaped result's `attempt`: the facts of the attempt it concludes, as the instance that concluded it saw them.
//! The engine keeps an account beside each running attempt, in memory only, and writes its facts into the attempt's
//! result once. A fact the instance did not see is stated as unknown, never inferred: a restored attempt has no launch.
//! These types are the result key's one encoder and its one decoder; none refuses a field a later service adds.
use serde::{Deserialize, Serialize};

/// An attempt's identity: its job and the wall-clock reading its intent recorded as `started_at`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AttemptId {
    pub job_id: String,
    pub started_at: f64,
}

/// What the launch admitted: its cell and what of it the overcommit budget lent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Launch {
    /// C: the cell's limit, the charge's cap.
    pub cell_bytes: u64,
    /// F: the cell's expected part; C for a job without an expected figure or one that grew.
    pub expected_bytes: u64,
    /// The charge's lending.
    pub lending_bytes: u64,
}

/// One raise this attempt received from an out-of-memory-killed sibling's holding.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Move {
    /// The wall clock when the raise was applied.
    pub at: f64,
    /// The killed attempt that funded it.
    pub donor: AttemptId,
    /// The lending that moved with it; 0 when the raise was funded by the donor's ordinary reservation alone.
    pub lending_bytes: u64,
    /// This attempt's cap after the raise.
    pub cap_bytes: u64,
}

/// How the attempt ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cause {
    Success,
    Failure,
    OutOfMemory,
    Cancelled,
    Lost,
}

impl Cause {
    /// The cause of a settled `outcome`; `outgrown` is whether its cell killed it for outgrowing it.
    pub fn of(outcome: &str, outgrown: bool) -> Self {
        match outcome {
            "success" => Self::Success,
            "cancelled" => Self::Cancelled,
            "lost" => Self::Lost,
            _ => match outgrown {
                true => Self::OutOfMemory,
                false => Self::Failure,
            },
        }
    }
}

/// The result's `attempt`: the facts of the attempt the result concludes, as the concluding instance saw them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Attempt {
    /// None: the attempt was restored, so this instance saw no launch, nor whether one spawned.
    pub launch: Option<Launch>,
    /// The raises this instance applied to it, in order.
    pub moves: Vec<Move>,
    pub cause: Cause,
    /// The cell's memory controller was restored while this instance ran the attempt, so its peak counts afresh.
    pub peak_reset: bool,
}

/// A running attempt's facts, kept beside its running record in memory and written only into its result.
#[derive(Debug, Default)]
pub struct Account {
    launch: Option<Launch>,
    moves: Vec<Move>,
    peak_reset: bool,
}

impl Account {
    /// The account of an attempt this instance launched and spawned.
    pub fn launched(launch: Launch) -> Self {
        Self { launch: Some(launch), ..Self::default() }
    }
    /// The account of an attempt this instance restored: no launch, no move.
    pub fn restored() -> Self {
        Self::default()
    }
    pub fn moved(&mut self, raise: Move) {
        self.moves.push(raise);
    }
    pub fn reset_peak(&mut self) {
        self.peak_reset = true;
    }
    pub fn concluded(&self, cause: Cause) -> Attempt {
        Attempt { launch: self.launch, moves: self.moves.clone(), cause, peak_reset: self.peak_reset }
    }
}
