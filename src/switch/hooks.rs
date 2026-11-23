//! Test-only: the daemon's pause points at the boundaries whose order a test forces (PLAN-L §5). Each belongs to
//! one daemon. An unarmed point returns at once, polling no Tokio resource; an armed one acknowledges each pass and,
//! where the pass can wait, holds it until the test releases it.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, PoisonError};

use crate::switch::daemon::Daemon;
use crate::switch::departure::{Context, Teardown, Verdict};
use crate::switch::lifecycle::Life;

/// A held settlement point's future.
pub(crate) type Hold = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// One pass's choice, as settlement made it: its fact, the life and job shutdown it read, its age and its verdict.
#[derive(Debug, Clone)]
pub(crate) struct Choice {
    pub teardown: Teardown,
    pub life: Life,
    pub job_shutdown: bool,
    pub since_ms: u128,
    pub verdict: Verdict,
}

/// Settlement's points, installed only by the stopping fixture's daemon role in its own process, which runs one role
/// and one daemon.
pub(crate) trait Settlement: Send + Sync {
    /// Before each pass's dispatch: settlement's start, in nanoseconds of the daemon's monotonic clock, and the pass.
    fn dispatch(&self, id: u64, started_ns: u64, pass: u32) -> Hold;
    /// Each pass's choice, before its action.
    fn choice(&self, id: u64, choice: Choice) -> Hold;
    /// Settlement's end: it returned, or its future was dropped unfinished.
    fn end(&self, id: u64, returned: bool);
}

pub(crate) async fn dispatch(d: &Daemon, id: u64, started_ns: u64, pass: u32) {
    match d.hooks.settlement.get() {
        Some(hooks) => hooks.dispatch(id, started_ns, pass).await,
        None => {}
    }
}

pub(crate) async fn choice(d: &Daemon, id: u64, teardown: &Teardown, context: &Context, verdict: &Verdict) {
    match d.hooks.settlement.get() {
        Some(hooks) => {
            let choice = Choice {
                teardown: teardown.clone(),
                life: context.life,
                job_shutdown: context.job_shutdown,
                since_ms: context.since.as_millis(),
                verdict: verdict.clone(),
            };
            hooks.choice(id, choice).await
        }
        None => {}
    }
}

/// A guard local to `settle`: its drop reports the end, `dropped` unless settle returned.
pub struct Ended<'a> {
    d: &'a Daemon,
    id: u64,
    returned: bool,
}

impl<'a> Ended<'a> {
    pub fn new(d: &'a Daemon, id: u64) -> Ended<'a> {
        Ended { d, id, returned: false }
    }

    pub fn returned(mut self) {
        self.returned = true;
    }
}

impl Drop for Ended<'_> {
    fn drop(&mut self) {
        if let Some(hooks) = self.d.hooks.settlement.get() {
            hooks.end(self.id, self.returned);
        }
    }
}

/// The daemon's points.
#[derive(Default)]
pub struct Hooks {
    /// Settlement's points; none outside the stopping fixture's daemon role.
    pub(crate) settlement: std::sync::OnceLock<Box<dyn Settlement>>,
    /// The poller, after its read returned and before it dispatches its process walk.
    pub before_walk: Point,
    /// A scheduling pass, under the state lock, before preparation's journal write.
    pub preparation: Point,
    /// A lease session in its release arm, before the final report's snapshot.
    pub release_arm: Point,
}

/// A pass the test saw reach a point.
pub struct Reached {
    release: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Reached {
    /// Lets a held pass go on.
    pub fn release(self) {
        let _ = self.release.map(|release| release.send(()));
    }
}

#[derive(Default)]
struct State {
    armed: usize,
    reached: VecDeque<Reached>,
}

#[derive(Default)]
pub struct Point {
    state: Mutex<State>,
    changed: tokio::sync::Notify,
}

impl Point {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The next pass reaches the point armed.
    pub fn arm(&self) {
        self.lock().armed += 1;
    }

    /// Takes one arming, if any, and records the pass with its release.
    fn reach(&self, release: Option<tokio::sync::oneshot::Sender<()>>) -> bool {
        let mut state = self.lock();
        let armed = state.armed > 0;
        state.armed -= usize::from(armed);
        match armed {
            true => state.reached.push_back(Reached { release }),
            false => {}
        }
        drop(state);
        self.changed.notify_waiters();
        armed
    }

    /// A pass that can wait: armed, it holds here until released.
    pub async fn pass(&self) {
        let (release, released) = tokio::sync::oneshot::channel();
        match self.reach(Some(release)) {
            true => {
                let _ = released.await;
            }
            false => {}
        }
    }

    /// A pass in a section that cannot wait, such as one under the state lock: armed, it only acknowledges.
    pub fn acknowledge(&self) {
        self.reach(None);
    }

    /// Until an armed pass reached the point.
    pub async fn reached(&self) -> Reached {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(reached) = self.lock().reached.pop_front() {
                return reached;
            }
            notified.await;
        }
    }
}
