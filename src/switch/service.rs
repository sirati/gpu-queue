//! The service's start and stop: `run` registers the signals, binds the listeners and builds the daemon; `serve`
//! runs it until its stop future resolves; `stop` publishes that the daemon stops, then finishes the saver, which
//! saves last.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use crate::switch::config::SwitchConfig;
use crate::switch::control::{self, Role};
use crate::switch::daemon::{self, Daemon};
use crate::switch::lifecycle::Serving;
use crate::switch::{listen_fds, persist, poller, startup};
use tokio::net::UnixListener;
use tokio::signal::unix::{signal, SignalKind};

pub async fn run(config: SwitchConfig, config_path: PathBuf) -> Result<(), String> {
    let mut fds = listen_fds();
    let mut listeners = Vec::new();
    for l in &config.listen {
        let role = Role::of(&l.name).ok_or_else(|| format!("listen name {:?}", l.name))?;
        let std_listener = match fds.iter().position(|(n, _)| *n == l.name) {
            Some(i) => std::os::unix::net::UnixListener::from(fds.remove(i).1),
            None => match &l.path {
                Some(p) => crate::switch::bind(p)?,
                None => return Err(format!("no socket for listen {:?}: no fd, no path", l.name)),
            },
        };
        std_listener
            .set_nonblocking(true)
            .map_err(|e| format!("socket {}: {e}", l.name))?;
        let listener = tokio::net::UnixListener::from_std(std_listener)
            .map_err(|e| format!("socket {}: {e}", l.name))?;
        listeners.push((listener, role, l.name.clone()));
    }
    let mut term = signal(SignalKind::terminate()).map_err(|e| e.to_string())?;
    let mut int = signal(SignalKind::interrupt()).map_err(|e| e.to_string())?;
    let d = Daemon::new(config, config_path)?;
    serve(d, listeners, async move {
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
    })
    .await;
    Ok(())
}

/// The service of `d` on `listeners` until `stop` resolves. It takes the daemon's one serving token first, so
/// every route out of here publishes that the daemon stops before the runtime drops.
pub async fn serve(
    d: Arc<Daemon>,
    listeners: Vec<(UnixListener, Role, String)>,
    stop: impl std::future::Future<Output = ()>,
) {
    let serving = d.lifecycle.serving();
    persist::restore(&d);
    for (listener, role, name) in listeners {
        eprintln!("gpu-switch: listening ({name})");
        tokio::spawn(control::serve(d.clone(), listener, role));
    }
    tokio::spawn(poller::run(d.clone()));
    tokio::spawn(startup::run(d.clone()));
    tokio::spawn(daemon::run(d.clone()));
    let saver = persist::Saver::spawn(d.clone());
    stop.await;
    self::stop(serving, saver, &d).await;
}

/// Publishes that the daemon stops, then finishes the saver: its join, the last save and everything after them fall
/// inside the interval in which settlement's choices read `Stopping`.
async fn stop(serving: Serving<'_>, saver: persist::Saver, d: &Daemon) {
    #[cfg(test)]
    let _scope = shutdown::ShutdownScope::open(d);
    serving.stop();
    let _ = writeln!(std::io::stderr(), "gpu-switch: stopping");
    // Saved before any connection closes: the clients resume what it holds.
    saver.finish(d).await;
    eprintln!("gpu-switch: shutting down");
    // Leases end with their helpers; the arbiter stops NInfer when its link drops.
    for l in &d.config.listen {
        if let Some(p) = &l.path {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// Test-only: the save entry of one daemon's shutdown, observed on the thread its stop runs on (DECISION-L6-5 ruling
/// 3, DECISION-L6-6). The arm is this thread's, bound to one daemon; `ShutdownScope` opens an invocation on it only
/// for that daemon. A save outside a scope, on an unarmed thread, or of another daemon passes at once.
#[cfg(test)]
pub(crate) mod shutdown {
    use std::cell::RefCell;
    use std::sync::{Arc, Weak};

    use crate::switch::daemon::Daemon;
    use crate::switch::lifecycle::Life;

    /// What the arm acknowledges at the save's entry: the invocation and the life the save read.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct Entry {
        pub invocation: u32,
        pub life: Life,
    }

    struct Arm {
        daemon: Weak<Daemon>,
        invocations: u32,
        open: Option<u32>,
        report: Box<dyn FnMut(Entry)>,
        hold: Option<std::sync::mpsc::Receiver<()>>,
    }

    thread_local! {
        static ARM: RefCell<Option<Arm>> = const { RefCell::new(None) };
    }

    /// The arm of this thread; it disarms the thread when it drops.
    pub(crate) struct Armed(());

    impl Drop for Armed {
        fn drop(&mut self) {
            ARM.with(|arm| arm.borrow_mut().take());
        }
    }

    /// Arms this thread for `daemon`'s stops: each save entry inside one reports its invocation, then, with a
    /// `hold`, waits for a release on it.
    pub(crate) fn arm(
        daemon: &Arc<Daemon>,
        report: impl FnMut(Entry) + 'static,
        hold: Option<std::sync::mpsc::Receiver<()>>,
    ) -> Armed {
        ARM.with(|arm| {
            *arm.borrow_mut() = Some(Arm {
                daemon: Arc::downgrade(daemon),
                invocations: 0,
                open: None,
                report: Box::new(report),
                hold,
            })
        });
        Armed(())
    }

    /// The first statement of `service::stop`: an invocation of this thread's arm, for its own daemon only.
    pub(crate) struct ShutdownScope(());

    impl ShutdownScope {
        pub(crate) fn open(d: &Daemon) -> ShutdownScope {
            ARM.with(|arm| {
                if let Some(arm) = arm.borrow_mut().as_mut().filter(|arm| std::ptr::eq(arm.daemon.as_ptr(), d)) {
                    arm.invocations += 1;
                    arm.open = Some(arm.invocations);
                }
            });
            ShutdownScope(())
        }
    }

    impl Drop for ShutdownScope {
        fn drop(&mut self) {
            ARM.with(|arm| arm.borrow_mut().as_mut().map(|arm| arm.open = None));
        }
    }

    /// The entry of `persist::save`, before its snapshot.
    pub(crate) fn save_entry(d: &Daemon) {
        let hold = ARM.with(|arm| {
            let mut arm = arm.borrow_mut();
            let arm = arm.as_mut().filter(|arm| std::ptr::eq(arm.daemon.as_ptr(), d))?;
            let invocation = arm.open?;
            (arm.report)(Entry {
                invocation,
                life: d.lifecycle.life(),
            });
            arm.hold.take()
        });
        if let Some(hold) = hold {
            let _ = hold.recv();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::switch::card_reader::tests::Script;
    use crate::switch::control::tests::{incarnation, reading};
    use crate::switch::lifecycle::Life;

    /// `d`'s stop with a saver of its own, on this thread: a current-thread runtime runs the stop, its last save and the
    /// saver here, and the saver, first polled after the stop was sent, returns without saving.
    fn stopped(d: &Arc<Daemon>) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async { stop(d.lifecycle.serving(), persist::Saver::spawn(d.clone()), d).await });
    }

    #[test]
    fn the_shutdown_save_reads_stopping_at_its_entry_on_the_arm_of_its_own_daemon_only() {
        let (p, _p) = reading(serde_json::json!({}), Some(Script::new().driver()), incarnation(1));
        let (q, _q) = reading(serde_json::json!({}), Some(Script::new().driver()), incarnation(2));
        let (heard, entries) = std::sync::mpsc::channel();
        let (release, hold) = std::sync::mpsc::channel();
        release.send(()).unwrap();
        let _armed = shutdown::arm(&p, move |entry| heard.send(entry).unwrap(), Some(hold));
        persist::save(&p);
        assert!(entries.try_recv().is_err(), "a save outside any scope passes");
        std::thread::spawn({
            let q = q.clone();
            move || stopped(&q)
        })
        .join()
        .unwrap();
        assert!(entries.try_recv().is_err(), "another thread's stop cannot reach this arm");
        stopped(&q);
        assert!(entries.try_recv().is_err(), "another daemon's stop on the armed thread neither acknowledges nor holds");
        stopped(&p);
        assert_eq!(
            entries.try_recv().unwrap(),
            shutdown::Entry { invocation: 1, life: Life::Stopping },
            "observed at the save's entry"
        );
        assert!(entries.try_recv().is_err(), "exactly one invocation");
    }

    #[test]
    fn a_service_that_unwinds_has_published_stopping_before_its_runtime_drops() {
        let (d, _dir) = reading(serde_json::json!({}), Some(Script::new().driver()), incarnation(1));
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.block_on(serve(d.clone(), Vec::new(), async { panic!("the stop future panics") }))
        }));
        assert!(caught.is_err());
        assert_eq!(d.lifecycle.life(), Life::Stopping);
        drop(runtime);
    }
}
