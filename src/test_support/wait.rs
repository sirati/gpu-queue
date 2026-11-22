//! The one waiter of the tests: it waits, with no deadline, until a condition holds. The run's containment is the only
//! hang guard; a wait that never completes hangs the run, and its line on standard error names it.

/// Returns once `condition` holds. It checks once; if the condition does not hold, it writes `waiting for {what}` to
/// standard error once and then checks again every 20 ms, which paces the checks and bounds nothing.
pub fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    if condition() {
        return;
    }
    eprintln!("waiting for {what}");
    while !condition() {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}
