//! Test-only: the daemon's pause points at the boundaries whose order a test forces (PLAN-L §5). Each belongs to
//! one daemon. An unarmed point returns at once, polling no Tokio resource; an armed one acknowledges each pass and,
//! where the pass can wait, holds it until the test releases it.

use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard, PoisonError};

/// The daemon's points.
#[derive(Default)]
pub struct Hooks {
    /// The poller, after its read returned and before it dispatches its process walk.
    pub before_walk: Point,
    /// A scheduling pass, under the state lock, before preparation's journal write.
    pub preparation: Point,
    /// A lease session, before its `select!`.
    pub lease_select: Point,
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
