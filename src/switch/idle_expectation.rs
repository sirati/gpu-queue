//! A lease owner's single, bounded declaration that its grant is knowingly
//! idle for a while: the watchdog neither counts that time nor treats it as
//! evidence. A forked GPU child reaches it through its lease owner.
use super::state::{monotonic_ns, Side, State};
use crate::protocol::{now_unix, Reply, Resume};

/// Long enough for a continuation exit, a first-call trace or host preparation.
pub const MAX_SECONDS: f64 = 60.0;

pub fn request(s: &mut State, side: Side, peer_pid: u32, resume: &Resume, seconds: f64) -> Reply {
    match begin(s, side, peer_pid, resume, seconds) {
        Ok(deadline) => Reply::message(deadline.to_string()),
        Err(error) => Reply::error(error),
    }
}

fn begin(
    s: &mut State,
    side: Side,
    peer_pid: u32,
    resume: &Resume,
    seconds: f64,
) -> Result<f64, String> {
    if seconds > MAX_SECONDS {
        return Err(format!("an idle expectation lasts at most {MAX_SECONDS} s"));
    }
    let job = s.owned_lease(side, peer_pid, resume, "an idle expectation")?;
    if job.retention.is_some() {
        return Err("a VRAM-retaining grant is already outside the compute watchdog".into());
    }
    job.police
        .compute_watchdog
        .excuse(monotonic_ns(), now_unix(), seconds)
}
