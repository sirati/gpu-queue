//! The daemon's own life: serving, then stopping, one way (PLAN-L §3.10.1). The service holds the one [`Serving`]
//! token and stops it before it saves its state; the token's drop publishes too, also while unwinding, so every route
//! out of the service publishes before the runtime drops. Only departure reads the life in production.

use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Life {
    Serving,
    Stopping,
}

#[derive(Default)]
pub struct Lifecycle {
    stopping: AtomicBool,
}

impl Lifecycle {
    pub fn new() -> Lifecycle {
        Lifecycle::default()
    }

    pub fn life(&self) -> Life {
        match self.stopping.load(Ordering::Acquire) {
            true => Life::Stopping,
            false => Life::Serving,
        }
    }

    /// The token whose stop, or drop, publishes that the daemon stops.
    pub fn serving(&self) -> Serving<'_> {
        Serving { lifecycle: self }
    }

    fn publish(&self) {
        self.stopping.store(true, Ordering::Release);
    }
}

/// While it lives, the daemon serves.
pub struct Serving<'a> {
    lifecycle: &'a Lifecycle,
}

impl Serving<'_> {
    /// Publishes that the daemon stops.
    pub fn stop(self) {
        self.lifecycle.publish();
    }
}

impl Drop for Serving<'_> {
    fn drop(&mut self) {
        self.lifecycle.publish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn life_starts_serving_and_a_stop_or_a_drop_while_unwinding_publishes_stopping() {
        let lifecycle = Lifecycle::new();
        assert_eq!(lifecycle.life(), Life::Serving);
        lifecycle.serving().stop();
        assert_eq!(lifecycle.life(), Life::Stopping);
        let unwound = Lifecycle::new();
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _serving = unwound.serving();
            panic!("a service that unwinds after taking its token");
        }));
        assert!(caught.is_err());
        assert_eq!(unwound.life(), Life::Stopping);
    }
}
