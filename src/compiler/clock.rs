//! The service's two clocks: the monotonic contract clock, which compile starts, declared finishes and the
//! run-ahead deadline are read on, and the wall clock records and request ages are read on. The engine and its
//! host memory hold one `Clock` between them; production reads the system clocks, and a test advances its own.
use std::sync::Arc;

/// Where a clock's readings come from.
pub trait Source: Send + Sync {
    fn monotonic(&self) -> f64;
    fn wall(&self) -> f64;
}

struct System;
impl Source for System {
    fn monotonic(&self) -> f64 {
        crate::posix_clock::monotonic().as_secs_f64()
    }
    fn wall(&self) -> f64 {
        crate::protocol::now_unix()
    }
}

/// One handle on a clock, shared by every reader of one engine.
#[derive(Clone)]
pub struct Clock(Arc<dyn Source>);

impl Clock {
    pub fn system() -> Self {
        Self(Arc::new(System))
    }
    /// Seconds on the contract clock.
    pub fn monotonic(&self) -> f64 {
        self.0.monotonic()
    }
    /// Seconds since the Unix epoch.
    pub fn wall(&self) -> f64 {
        self.0.wall()
    }
}

/// A clock that stands still until its test advances it, starting at the system clocks' readings.
#[cfg(test)]
pub struct Manual(std::sync::Mutex<(f64, f64)>);

#[cfg(test)]
impl Manual {
    /// A new manual clock and the engine's handle on it.
    pub fn new() -> (Arc<Self>, Clock) {
        let manual = Arc::new(Self(std::sync::Mutex::new((System.monotonic(), System.wall()))));
        (manual.clone(), Clock(manual))
    }
    /// Moves both clocks `seconds` ahead.
    pub fn advance(&self, seconds: f64) {
        let mut readings = self.0.lock().unwrap();
        *readings = (readings.0 + seconds, readings.1 + seconds);
    }
}

#[cfg(test)]
impl Source for Manual {
    fn monotonic(&self) -> f64 {
        self.0.lock().unwrap().0
    }
    fn wall(&self) -> f64 {
        self.0.lock().unwrap().1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_manual_clock_moves_only_when_advanced_and_both_readings_move_together() {
        let (manual, clock) = Manual::new();
        let (monotonic, wall) = (clock.monotonic(), clock.wall());
        assert_eq!((clock.monotonic(), clock.wall()), (monotonic, wall));
        manual.advance(4.0);
        assert_eq!((clock.monotonic(), clock.wall()), (monotonic + 4.0, wall + 4.0));
        let system = Clock::system();
        assert!(system.monotonic() <= system.monotonic());
    }
}
