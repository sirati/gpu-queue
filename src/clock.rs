//! The crate's time readings: the monotonic contract clock, which compile starts, declared finishes, the run-ahead
//! deadline and every instant of the switch daemon are read on, and the wall clock records and request ages are read
//! on. A reading is an integer `Duration` since its clock's origin, so it keeps every nanosecond at any origin. The
//! compile engine with its host memory, and the switch daemon, each hold one `Clock`; production reads the system
//! clocks, and a test advances its own. Each unit a reader keeps is derived here, once.
use std::sync::Arc;
use std::time::Duration;

/// Where a clock's readings come from.
pub trait Source: Send + Sync {
    /// CLOCK_MONOTONIC's time since its origin.
    fn monotonic(&self) -> Duration;
    /// The time since the Unix epoch.
    fn wall(&self) -> Duration;
}

struct System;
impl Source for System {
    fn monotonic(&self) -> Duration {
        crate::posix_clock::monotonic()
    }
    fn wall(&self) -> Duration {
        crate::protocol::unix_time()
    }
}

/// A reading in the integer nanoseconds that the switch's timings, placement keys and saved snapshots keep.
pub fn ns(reading: Duration) -> u64 {
    reading.as_nanos() as u64
}

/// One handle on a clock, shared by every reader of one engine or one daemon.
#[derive(Clone)]
pub struct Clock(Arc<dyn Source>);

impl Clock {
    /// The handle on `source`.
    pub fn new(source: Arc<dyn Source>) -> Self {
        Self(source)
    }
    pub fn system() -> Self {
        Self::new(Arc::new(System))
    }
    /// The monotonic reading.
    pub fn monotonic(&self) -> Duration {
        self.0.monotonic()
    }
    /// The monotonic reading in integer nanoseconds.
    pub fn monotonic_ns(&self) -> u64 {
        ns(self.monotonic())
    }
    /// Seconds on the contract clock, as the compile engine keeps them.
    pub fn monotonic_secs(&self) -> f64 {
        self.monotonic().as_secs_f64()
    }
    /// Seconds since the Unix epoch.
    pub fn wall(&self) -> f64 {
        self.0.wall().as_secs_f64()
    }
}

/// Both readings of a manual clock.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Readings {
    pub monotonic: Duration,
    pub wall: Duration,
}

/// A clock that stands still until its test advances it.
#[cfg(test)]
pub struct Manual(std::sync::Mutex<Readings>);

#[cfg(test)]
impl Manual {
    /// A manual clock that starts at `readings`, and the handle on it.
    pub fn starting_at(readings: Readings) -> (Arc<Self>, Clock) {
        let manual = Arc::new(Self(std::sync::Mutex::new(readings)));
        (manual.clone(), Clock::new(manual))
    }
    /// A manual clock that starts at the system clocks' readings, and the handle on it.
    pub fn new() -> (Arc<Self>, Clock) {
        Self::starting_at(Readings { monotonic: System.monotonic(), wall: System.wall() })
    }
    /// Moves both readings `step` ahead.
    pub fn advance(&self, step: Duration) {
        let mut readings = self.0.lock().unwrap();
        *readings = Readings { monotonic: readings.monotonic + step, wall: readings.wall + step };
    }
}

#[cfg(test)]
impl Source for Manual {
    fn monotonic(&self) -> Duration {
        self.0.lock().unwrap().monotonic
    }
    fn wall(&self) -> Duration {
        self.0.lock().unwrap().wall
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A monotonic origin of 2^24 s, where one step of f64 seconds is 3.73 ns.
    const ORIGIN: Duration = Duration::from_secs(16_777_216);

    /// A manual clock whose monotonic reading starts at `monotonic`.
    fn at(monotonic: Duration) -> (Arc<Manual>, Clock) {
        Manual::starting_at(Readings { monotonic, wall: Duration::from_secs(1_800_000_000) })
    }

    #[test]
    fn one_nanosecond_moves_a_large_reading_by_exactly_one_nanosecond() {
        let (manual, clock) = at(ORIGIN);
        let (reading, nanoseconds) = (clock.monotonic(), clock.monotonic_ns());
        manual.advance(Duration::from_nanos(1));
        assert_eq!(clock.monotonic() - reading, Duration::from_nanos(1));
        assert_eq!(clock.monotonic_ns() - nanoseconds, 1);
    }

    #[test]
    fn a_reading_one_nanosecond_short_of_a_deadline_is_before_it_and_the_next_equals_it() {
        let (manual, clock) = at(ORIGIN);
        let deadline = ORIGIN + Duration::from_secs(60);
        manual.advance(Duration::from_secs(60) - Duration::from_nanos(1));
        assert!(clock.monotonic() < deadline, "{:?} is not before {deadline:?}", clock.monotonic());
        manual.advance(Duration::from_nanos(1));
        assert_eq!(clock.monotonic(), deadline);
    }

    #[test]
    fn readings_one_nanosecond_apart_past_two_to_the_fifty_third_stay_apart_and_convert_exactly() {
        let (low, high) = (Duration::from_nanos(9_007_199_254_740_992), Duration::from_nanos(9_007_199_254_740_993));
        let ((_, first), (_, second)) = (at(low), at(high));
        assert_ne!(first.monotonic(), second.monotonic());
        assert_ne!(first.monotonic_ns(), second.monotonic_ns());
        assert_eq!((ns(low), ns(high)), (9_007_199_254_740_992, 9_007_199_254_740_993));
        assert_eq!((Duration::from_nanos(ns(low)), Duration::from_nanos(ns(high))), (low, high));
    }

    #[test]
    fn a_manual_clock_moves_only_when_advanced_and_both_readings_move_together() {
        let (manual, clock) = Manual::new();
        let readings = *manual.0.lock().unwrap();
        assert_eq!((clock.monotonic_secs(), clock.wall()), (readings.monotonic.as_secs_f64(), readings.wall.as_secs_f64()));
        let (monotonic, wall) = (clock.monotonic(), clock.wall());
        assert_eq!((clock.monotonic(), clock.wall()), (monotonic, wall));
        manual.advance(Duration::from_secs(4));
        let step = Duration::from_secs(4);
        assert_eq!(
            (clock.monotonic(), clock.wall()),
            (readings.monotonic + step, (readings.wall + step).as_secs_f64())
        );
        let system = Clock::system();
        assert!(system.monotonic() <= system.monotonic());
    }
}
