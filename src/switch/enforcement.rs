//! Single owner of shutdown policy. Callers supply a reason plus an enforcement
//! signal. Cooperative clients receive a fixed deadline; acknowledgements only
//! record progress. The same daemon engine is used in every operating mode.
use super::config::secs;
use super::daemon::{schedule_kill, Daemon};
use super::state::Job;
use crate::protocol::{now_unix, Reply, ShutdownRequest, Sig};
use std::time::Duration;

fn record(d: &Daemon, event: &str, job: &Job) {
    if let Err(e) = d.journal.record(event, job) {
        // A failed disk must not leave a noncompliant process holding the GPU.
        eprintln!(
            "gpu-queue: enforcement journal failed: {e}; {} {:?}",
            job.handle, job.end_reason
        );
    }
}

pub fn signal(d: &Daemon, job: &mut Job, sig: Sig) {
    if job.shutdown.is_none() && job.end_reason.is_some() {
        if let Some(client_grace) = job.shutdown_grace_secs {
            let (grace, tail) = job
                .terminal_status
                .map(|status| {
                    watchdog_delays(
                        status,
                        secs(d.config.sigint_to_sigterm_secs),
                        secs(d.config.sigterm_to_sigkill_secs),
                    )
                })
                .unwrap_or((secs(client_grace), secs(d.config.sigterm_to_sigkill_secs)));
            let reason = job.end_reason.as_ref().expect("checked").1.clone();
            let shutdown = ShutdownRequest {
                request_id: format!("{}-{}-{}", job.handle, job.runs, now_unix()),
                reason,
                deadline: now_unix() + grace.as_secs_f64(),
                permanent: matches!(
                    job.end_reason.as_ref().expect("checked").0,
                    crate::protocol::JobState::Failed | crate::protocol::JobState::Killed
                ),
                handler_started: false,
            };
            job.shutdown = Some(shutdown.clone());
            // With no allocation left, CPU checkpoint cleanup does not own the GPU.
            if job.police.startup_fired {
                job.state = crate::protocol::JobState::Stopping;
                if let Some(timing) = &mut job.timing {
                    timing.stop(super::state::monotonic_ns());
                }
            }
            record(d, "shutdown_requested", job);
            schedule_kill(job, &[(grace, Sig::Term), (tail, Sig::Kill)]);
            job.shutdown_notice(shutdown);
            return;
        }
    }
    if matches!(sig, Sig::Term | Sig::Kill) {
        if let Some(request) = &mut job.shutdown {
            if !request.permanent {
                request.permanent = true;
                job.end_reason = Some((
                    crate::protocol::JobState::Killed,
                    format!("shutdown deadline exceeded: {}", request.reason),
                ));
            }
        }
    }
    record(d, "signal", job);
    job.signal(sig);
    // Notifications alone cannot enforce policy against an unresponsive lease
    // client. Runner sessions already deliver signals to their own child group.
    if matches!(sig, Sig::Term | Sig::Kill) {
        job.signal = Some(sig.number());
        if let (Some(pid), Some(birth)) =
            (job.pid_host.or(Some(job.runner_pid)), job.process_started)
        {
            if let Err(error) = crate::procs::terminate_tree(pid, birth, sig.number()) {
                eprintln!("gpu-queue: signal {}: {error}", job.handle);
                schedule_kill(job, &[(Duration::from_secs(1), sig)]);
            }
        }
    }
}

pub fn acknowledge(d: &Daemon, job: &mut Job, request_id: &str) -> Reply {
    match &mut job.shutdown {
        Some(request) if request.request_id == request_id => {
            request.handler_started = true;
            record(d, "shutdown_handler_started", job);
            Reply::ok()
        }
        _ => Reply::error("stale or unknown shutdown request"),
    }
}

/// Fixed watchdog deadlines also govern cooperative shutdown handlers.
pub fn watchdog(
    job: &mut Job,
    status: crate::protocol::TerminalStatus,
    long: Duration,
    tail: Duration,
) {
    job.terminal_status = Some(status);
    let (first, second) = watchdog_delays(status, long, tail);
    schedule_kill(
        job,
        &[
            (Duration::ZERO, Sig::Int),
            (first, Sig::Term),
            (second, Sig::Kill),
        ],
    );
}

fn watchdog_delays(
    status: crate::protocol::TerminalStatus,
    long: Duration,
    tail: Duration,
) -> (Duration, Duration) {
    if status == crate::protocol::TerminalStatus::IdleCompute90 {
        (long, tail)
    } else {
        (Duration::from_secs(1), Duration::from_secs(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::TerminalStatus::*;
    #[test]
    fn only_ninety_second_rule_gets_the_long_signal_grace() {
        let long = Duration::from_secs(60);
        let tail = Duration::from_secs(10);
        assert_eq!(watchdog_delays(IdleCompute90, long, tail), (long, tail));
        for reason in [NoVram, NoInitialCompute, LowComputeDuty, IdleCompute30] {
            assert_eq!(
                watchdog_delays(reason, long, tail),
                (Duration::from_secs(1), Duration::from_secs(1))
            );
        }
    }
}
