//! What the proxy shares with the NInfer supervisor: whether requests may go
//! upstream, how many are in flight, and the abort epoch for drain timeouts.

use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::arbiter::config::ArbiterConfig;

pub struct CachedResponse {
    pub status: hyper::StatusCode,
    pub headers: hyper::HeaderMap,
    pub body: bytes::Bytes,
}

#[derive(Default)]
pub struct Inner {
    /// Proxy may forward upstream: NInfer is serving under a live grant.
    pub ready: bool,
    pub in_flight: usize,
    pub held: usize,
    pub abort_epoch: u64,
    pub ninfer_pid: Option<u32>,
}

pub struct Shared {
    pub config: ArbiterConfig,
    inner: Mutex<Inner>,
    bump: watch::Sender<u64>,
    abort: watch::Sender<u64>,
    pub models_cache: Mutex<Option<CachedResponse>>,
}

impl Shared {
    pub fn new(config: ArbiterConfig) -> Arc<Shared> {
        Arc::new(Shared {
            config,
            inner: Mutex::new(Inner::default()),
            bump: watch::channel(0).0,
            abort: watch::channel(0).0,
            models_cache: Mutex::new(None),
        })
    }

    pub fn with<R>(&self, f: impl FnOnce(&mut Inner) -> R) -> R {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut g)
    }

    pub fn update<R>(&self, f: impl FnOnce(&mut Inner) -> R) -> R {
        let r = self.with(f);
        self.bump.send_modify(|v| *v = v.wrapping_add(1));
        r
    }

    pub fn set_ready(&self, ready: bool) {
        self.update(|i| i.ready = ready);
    }

    pub async fn wait_idle(&self) {
        let mut rx = self.bump.subscribe();
        loop {
            rx.borrow_and_update();
            if self.with(|i| i.in_flight == 0) {
                return;
            }
            let _ = rx.changed().await;
        }
    }

    pub fn try_enter(self: &Arc<Self>) -> Option<InFlight> {
        let epoch = self.with(|i| {
            i.ready.then(|| {
                i.in_flight += 1;
                i.abort_epoch
            })
        })?;
        Some(InFlight {
            shared: self.clone(),
            epoch,
        })
    }

    /// Wait until NInfer is serving, then count the caller as in flight.
    pub async fn enter(self: &Arc<Self>) -> InFlight {
        if let Some(g) = self.try_enter() {
            return g;
        }
        self.update(|i| i.held += 1);
        let _held = Held(self.clone());
        let mut rx = self.bump.subscribe();
        loop {
            rx.borrow_and_update();
            if let Some(g) = self.try_enter() {
                return g;
            }
            let _ = rx.changed().await;
        }
    }

    /// Make every request currently in flight give up its upstream exchange.
    pub fn abort_in_flight(&self) {
        // Under the lock, so an entry never pairs a new epoch with the old watch value.
        self.with(|i| {
            i.abort_epoch += 1;
            self.abort.send_replace(i.abort_epoch);
        });
    }
}

pub struct InFlight {
    shared: Arc<Shared>,
    epoch: u64,
}

impl InFlight {
    /// Resolves once [`Shared::abort_in_flight`] ran after this request entered.
    pub async fn aborted(&self) {
        let mut rx = self.shared.abort.subscribe();
        if rx.wait_for(|e| *e != self.epoch).await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.shared.update(|i| i.in_flight -= 1);
    }
}

struct Held(Arc<Shared>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0.update(|i| i.held -= 1);
    }
}
