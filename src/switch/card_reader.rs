//! Reaching the card: opening it, its handle generations, closing a lost handle, re-opening, the identity it
//! adopted, and the reads in progress (PLAN-L §3.2). Every read is one blocking call on the handle of one
//! generation, and every outcome of that call, a panic and a cancellation included, is an [`Answer`]. No caller
//! holds an NVML handle, counts generations, dispatches a blocking read, tracks a read's completion, sees a
//! `JoinError` or knows the re-open and close policy.

use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::{Duration, Instant};

use crate::gpu::{Driver, Failure, Identity, Reader, Sample};
use crate::protocol::now_unix;

/// Every device's compute processes with their memory in bytes.
pub type Processes = Vec<(u32, Option<u64>)>;

/// What one read answered.
#[derive(Debug, Clone, PartialEq)]
pub enum Answer<T> {
    /// No NVML library is configured.
    Unconfigured,
    Read(T),
    Failed(Failure),
    /// The driver call, or the reader around it, panicked: it says nothing about the device.
    Panicked(String),
}

/// One answer with its read: the read's number, the generation of the handle it used, and when it began (its
/// dispatch) and returned, each with its wall-clock twin for records. Decisions use the monotonic instants.
#[derive(Debug, Clone)]
pub struct Observation<T> {
    pub read: u64,
    pub generation: u64,
    pub began: Instant,
    pub returned: Instant,
    pub began_unix: f64,
    pub returned_unix: f64,
    pub answer: Answer<T>,
}

/// The reads not yet taken by the card's owner: the oldest outstanding read's dispatch, and whether a failed
/// read waits for the owner to apply it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pending {
    pub outstanding: Option<Instant>,
    pub failed: bool,
}

/// How a read that failed ended.
#[derive(Debug, Clone)]
enum Ending {
    Failed(Failure),
    Panicked(String),
}

impl Ending {
    fn answer<T>(self) -> Answer<T> {
        match self {
            Ending::Failed(failure) => Answer::Failed(failure),
            Ending::Panicked(payload) => Answer::Panicked(payload),
        }
    }
}

/// A failed observation kept whole, answered again unchanged while no handle is open (a replay).
#[derive(Debug, Clone)]
struct Stored {
    key: Key,
    returned: Instant,
    returned_unix: f64,
    ending: Ending,
}

impl Stored {
    fn replay<T>(&self) -> Observation<T> {
        self.key.observation(self.returned, self.returned_unix, self.ending.clone().answer())
    }
}

/// What identifies a read from its dispatch on.
#[derive(Debug, Clone, Copy)]
struct Key {
    read: u64,
    generation: u64,
    began: Instant,
    began_unix: f64,
}

impl Key {
    fn observation<T>(self, returned: Instant, returned_unix: f64, answer: Answer<T>) -> Observation<T> {
        Observation {
            read: self.read,
            generation: self.generation,
            began: self.began,
            returned,
            began_unix: self.began_unix,
            returned_unix,
            answer,
        }
    }
}

/// An open NVML handle of one generation. A read holds its lock shared for its call; the close holds it
/// exclusively and leaves it empty.
struct Handle {
    generation: u64,
    reader: RwLock<Option<Box<dyn Reader>>>,
}

/// One dispatched call in the register.
#[derive(Debug, Clone, Copy)]
enum Entry {
    /// From the dispatch until the blocking call returns; `on` is the generation it reads, once it has one.
    Outstanding { began: Instant, on: Option<u64> },
    /// A failure the owner has not taken yet: a pending invalidation.
    Failed { returned: Instant, returned_unix: f64 },
}

#[derive(Default)]
struct Register {
    current: Option<Arc<Handle>>,
    /// Handles whose generation answered lost and whose close has not returned.
    retired: Vec<Arc<Handle>>,
    /// The highest generation whose loss the owner has applied.
    applied: u64,
    /// Adopted from the first sample this process reads.
    identity: Option<Identity>,
    /// The failure that retired the current generation's handle, or that failed its open.
    last: Option<Stored>,
    /// When the next open may run.
    open_after: Option<Instant>,
    reads: BTreeMap<u64, Entry>,
    next_read: u64,
    generations: u64,
}

impl Register {
    /// A read of generation `generation` is in flight.
    fn reading(&self, generation: u64) -> bool {
        self.reads
            .values()
            .any(|entry| matches!(entry, Entry::Outstanding { on: Some(on), .. } if *on == generation))
    }

    /// `generation`'s loss: its handle, if current, retires, and this failure is the one replays answer.
    fn retire(&mut self, generation: u64, stored: Stored, reopen: Duration) {
        let Some(handle) = self.current.take_if(|handle| handle.generation == generation) else {
            return;
        };
        self.retired.push(handle);
        self.open_after = Some(stored.returned + reopen);
        self.last = Some(stored);
    }

    /// A sample of an identity other than the adopted one is the loss of the card this process adopted.
    fn adopt(&mut self, answer: Answer<Sample>) -> Answer<Sample> {
        let Answer::Read(sample) = answer else {
            return answer;
        };
        let adopted = self.identity.get_or_insert_with(|| sample.identity.clone());
        match *adopted == sample.identity {
            true => Answer::Read(sample),
            false => Answer::Failed(Failure::Lost(format!(
                "the GPU answers as another device: adopted {adopted}, found {}; gpu-switch adopts a new card when \
                 it restarts",
                sample.identity
            ))),
        }
    }
}

/// What a `sample()` does after its closes.
enum Next {
    Read(Arc<Handle>),
    Open(u64),
    Replay,
}

/// A dispatched call's register entry. Moved into the blocking call, it turns the entry into a pending
/// invalidation if the call is destroyed before its bookkeeping completes: unrun at a runtime's shutdown, or
/// unwinding from the reader's own bookkeeping.
struct Dispatch {
    reader: Arc<CardReader>,
    key: Key,
    armed: bool,
}

impl Drop for Dispatch {
    fn drop(&mut self) {
        if self.armed {
            self.reader.invalidate(self.key.read);
        }
    }
}

pub struct CardReader {
    driver: Option<Box<dyn Driver>>,
    reopen: Duration,
    register: Mutex<Register>,
}

impl CardReader {
    /// No driver: a host without a card, every call answering `Unconfigured`.
    pub fn new(driver: Option<Box<dyn Driver>>, reopen: Duration) -> CardReader {
        CardReader {
            driver,
            reopen,
            register: Mutex::new(Register::default()),
        }
    }

    pub fn configured(&self) -> bool {
        self.driver.is_some()
    }

    fn lock(&self) -> MutexGuard<'_, Register> {
        self.register.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn unconfigured<T>() -> Observation<T> {
        let (now, unix) = (Instant::now(), now_unix());
        Key {
            read: 0,
            generation: 0,
            began: now,
            began_unix: unix,
        }
        .observation(now, unix, Answer::Unconfigured)
    }

    /// The answer of a read while no handle is open: the stored failure again, or, before any open has returned,
    /// that NVML is not opened yet, as generation 0.
    fn replay<T>(register: &Register) -> Observation<T> {
        register.last.as_ref().map(Stored::replay).unwrap_or_else(|| {
            let (now, unix) = (Instant::now(), now_unix());
            Key {
                read: 0,
                generation: 0,
                began: now,
                began_unix: unix,
            }
            .observation(
                now,
                unix,
                Answer::Failed(Failure::Unanswered("gpu-switch has not opened NVML yet".into())),
            )
        })
    }

    fn dispatch(self: &Arc<Self>, register: &mut Register, on: Option<u64>) -> Dispatch {
        let (began, began_unix) = (Instant::now(), now_unix());
        register.next_read += 1;
        let read = register.next_read;
        register.reads.insert(read, Entry::Outstanding { began, on });
        Dispatch {
            reader: self.clone(),
            key: Key {
                read,
                generation: on.unwrap_or(0),
                began,
                began_unix,
            },
            armed: true,
        }
    }

    /// The register entry of a call that ended before its bookkeeping: a pending invalidation, dated now.
    fn invalidate(&self, read: u64) -> (Instant, f64) {
        let mut register = self.lock();
        let ended = (Instant::now(), now_unix());
        match register.reads.get(&read).copied() {
            Some(Entry::Failed { returned, returned_unix }) => (returned, returned_unix),
            _ => {
                register.reads.insert(read, Entry::Failed { returned: ended.0, returned_unix: ended.1 });
                ended
            }
        }
    }

    /// The poller's read: closes what may close, opens when due, and samples (PLAN-L §3.2, steps 1 to 4).
    pub async fn sample(self: &Arc<Self>) -> Observation<Sample> {
        if !self.configured() {
            return Self::unconfigured();
        }
        let dispatch = {
            let mut register = self.lock();
            let on = register.current.as_ref().map(|handle| handle.generation);
            self.dispatch(&mut register, on)
        };
        let key = dispatch.key;
        let reader = self.clone();
        let joined = tokio::task::spawn_blocking(move || reader.sampled(dispatch)).await;
        let observation = self.joined(key, joined);
        #[cfg(test)]
        self.paused("sample", &observation).await;
        observation
    }

    /// Departure's and the allocation observer's read, on the current handle; it never opens or closes one.
    pub async fn compute(self: &Arc<Self>) -> Observation<Processes> {
        if !self.configured() {
            return Self::unconfigured();
        }
        let dispatched = {
            let mut register = self.lock();
            match register.current.clone() {
                Some(handle) => Ok((self.dispatch(&mut register, Some(handle.generation)), handle)),
                None => Err(Self::replay(&register)),
            }
        };
        let observation = match dispatched {
            Ok((dispatch, handle)) => {
                let key = dispatch.key;
                let reader = self.clone();
                let joined =
                    tokio::task::spawn_blocking(move || reader.read_on(dispatch, &handle, |reader| reader.compute()))
                        .await;
                self.joined(key, joined)
            }
            Err(replay) => replay,
        };
        #[cfg(test)]
        self.paused("compute", &observation).await;
        observation
    }

    #[cfg(test)]
    async fn paused<T>(&self, call: &'static str, observation: &Observation<T>) {
        let failed = !matches!(observation.answer, Answer::Read(_) | Answer::Unconfigured);
        self.driver
            .as_ref()
            .expect("configured")
            .completed(call, observation.read, failed)
            .await;
    }

    /// The awaiting side: the only code that sees a `JoinError`.
    fn joined<T>(&self, key: Key, joined: Result<Observation<T>, tokio::task::JoinError>) -> Observation<T> {
        joined.unwrap_or_else(|error| {
            let (returned, returned_unix) = self.invalidate(key.read);
            key.observation(returned, returned_unix, converted(error))
        })
    }

    /// `sample()`'s blocking call.
    fn sampled(self: &Arc<Self>, mut dispatch: Dispatch) -> Observation<Sample> {
        self.close_retired();
        let next = {
            let mut register = self.lock();
            let due = register.open_after.is_none_or(|at| Instant::now() >= at);
            let next = match register.current.clone() {
                Some(handle) => Next::Read(handle),
                None if register.retired.is_empty() && due => {
                    register.generations += 1;
                    Next::Open(register.generations)
                }
                None => Next::Replay,
            };
            let on = match &next {
                Next::Read(handle) => Some(handle.generation),
                Next::Open(generation) => Some(*generation),
                Next::Replay => None,
            };
            if let Some(Entry::Outstanding { on: entry, .. }) = register.reads.get_mut(&dispatch.key.read) {
                *entry = on;
            }
            next
        };
        match next {
            Next::Read(handle) => self.read_on(dispatch, &handle, |reader| reader.sample()),
            Next::Open(generation) => {
                dispatch.key.generation = generation;
                self.open(dispatch, generation)
            }
            Next::Replay => self.replayed(dispatch),
        }
    }

    /// Opens generation `generation`, then samples on it; a failed open is stored and answered.
    fn open(self: &Arc<Self>, mut dispatch: Dispatch, generation: u64) -> Observation<Sample> {
        let driver = self.driver.as_ref().expect("configured");
        let opened = match catch_unwind(AssertUnwindSafe(|| driver.open())) {
            Ok(Ok(reader)) => Ok(reader),
            Ok(Err(failure)) => Err(Ending::Failed(failure)),
            Err(panic) => Err(Ending::Panicked(text(panic.as_ref()))),
        };
        match opened {
            Ok(reader) => {
                let handle = Arc::new(Handle {
                    generation,
                    reader: RwLock::new(Some(reader)),
                });
                self.lock().current = Some(handle.clone());
                self.read_on(dispatch, &handle, |reader| reader.sample())
            }
            Err(ending) => {
                let (returned, returned_unix) = (Instant::now(), now_unix());
                let mut register = self.lock();
                #[cfg(test)]
                driver.fault();
                dispatch.armed = false;
                register.reads.insert(dispatch.key.read, Entry::Failed { returned, returned_unix });
                let stored = Stored {
                    key: dispatch.key,
                    returned,
                    returned_unix,
                    ending,
                };
                register.open_after = Some(returned + self.reopen);
                register.last = Some(stored.clone());
                stored.replay()
            }
        }
    }

    /// A registered call whose answer is a replay: its own entry leaves when it returns.
    fn replayed<T>(&self, mut dispatch: Dispatch) -> Observation<T> {
        let mut register = self.lock();
        #[cfg(test)]
        self.driver.as_ref().expect("configured").fault();
        dispatch.armed = false;
        register.reads.remove(&dispatch.key.read);
        Self::replay(&register)
    }

    /// One driver call on `handle`, then its bookkeeping in one critical section with the guard's disarming.
    fn read_on<T: Screened>(
        &self,
        mut dispatch: Dispatch,
        handle: &Handle,
        call: impl FnOnce(&dyn Reader) -> Result<T, Failure>,
    ) -> Observation<T> {
        let answered = {
            let reader = handle.reader.read().unwrap_or_else(PoisonError::into_inner);
            reader.as_deref().map(|reader| match catch_unwind(AssertUnwindSafe(|| call(reader))) {
                Ok(Ok(value)) => Answer::Read(value),
                Ok(Err(failure)) => Answer::Failed(failure),
                Err(panic) => Answer::Panicked(text(panic.as_ref())),
            })
        };
        let Some(answer) = answered else {
            return self.replayed(dispatch);
        };
        let (returned, returned_unix) = (Instant::now(), now_unix());
        let mut register = self.lock();
        #[cfg(test)]
        self.driver.as_ref().expect("configured").fault();
        dispatch.armed = false;
        let answer = T::screened(&mut register, answer);
        let key = Key {
            generation: handle.generation,
            ..dispatch.key
        };
        let ending = match &answer {
            Answer::Read(_) | Answer::Unconfigured => None,
            Answer::Failed(failure) => Some(Ending::Failed(failure.clone())),
            Answer::Panicked(payload) => Some(Ending::Panicked(payload.clone())),
        };
        match ending {
            None => {
                register.reads.remove(&key.read);
            }
            Some(ending) => {
                register.reads.insert(key.read, Entry::Failed { returned, returned_unix });
                if let Ending::Failed(Failure::Lost(_)) = ending {
                    let stored = Stored {
                        key,
                        returned,
                        returned_unix,
                        ending,
                    };
                    register.retire(handle.generation, stored, self.reopen);
                }
            }
        }
        key.observation(returned, returned_unix, answer)
    }

    /// Step 1: every retired handle whose loss was applied and whose reads have all returned is closed, with no
    /// register lock held; a handle leaves `retired` once its close has returned, whatever it answered.
    fn close_retired(&self) {
        let closable: Vec<Arc<Handle>> = {
            let register = self.lock();
            register
                .retired
                .iter()
                .filter(|handle| handle.generation <= register.applied && !register.reading(handle.generation))
                .cloned()
                .collect()
        };
        for handle in closable {
            eprintln!("gpu-switch: closing NVML handle {}", handle.generation);
            let began = Instant::now();
            let closed = handle
                .reader
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
                .map(|reader| catch_unwind(AssertUnwindSafe(|| reader.close())));
            match closed {
                Some(Ok(Ok(()))) | None => eprintln!(
                    "gpu-switch: NVML handle {} closed after {:?}",
                    handle.generation,
                    began.elapsed()
                ),
                Some(Ok(Err(failure))) => eprintln!(
                    "gpu-switch: NVML handle {} close failed: {}",
                    handle.generation,
                    failure.text()
                ),
                Some(Err(panic)) => eprintln!(
                    "gpu-switch: NVML handle {} close failed: panicked: {}",
                    handle.generation,
                    text(panic.as_ref())
                ),
            }
            self.lock().retired.retain(|retired| !Arc::ptr_eq(retired, &handle));
        }
    }

    /// The reads the owner has not taken: from one look at the register.
    pub fn pending(&self) -> Pending {
        let register = self.lock();
        Pending {
            outstanding: register
                .reads
                .values()
                .filter_map(|entry| match entry {
                    Entry::Outstanding { began, .. } => Some(*began),
                    Entry::Failed { .. } => None,
                })
                .min(),
            failed: register.reads.values().any(|entry| matches!(entry, Entry::Failed { .. })),
        }
    }

    /// The owner took `observation`: its pending invalidation leaves, and a loss lets its generation's handle
    /// close. Idempotent: a later copy of the same read finds no entry.
    pub fn applied<T>(&self, observation: &Observation<T>) {
        let mut register = self.lock();
        if let Some(Entry::Failed { .. }) = register.reads.get(&observation.read) {
            register.reads.remove(&observation.read);
        }
        if let Answer::Failed(Failure::Lost(_)) = observation.answer {
            register.applied = register.applied.max(observation.generation);
        }
    }

    #[cfg(test)]
    pub(crate) fn open_after(&self) -> Option<Instant> {
        self.lock().open_after
    }

    /// Re-opening becomes due now, in place of waiting out the interval.
    #[cfg(test)]
    pub(crate) fn reopen_due(&self) {
        self.lock().open_after = None;
    }

    /// The instant read `read`'s pending invalidation is dated by.
    #[cfg(test)]
    pub(crate) fn invalidated(&self, read: u64) -> Option<Instant> {
        match self.lock().reads.get(&read) {
            Some(Entry::Failed { returned, .. }) => Some(*returned),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn registered(&self) -> usize {
        self.lock().reads.len()
    }
}

/// What a read's success must pass before it counts: a sample, the adopted identity; a process list, nothing.
trait Screened: Sized {
    fn screened(register: &mut Register, answer: Answer<Self>) -> Answer<Self>;
}

impl Screened for Sample {
    fn screened(register: &mut Register, answer: Answer<Sample>) -> Answer<Sample> {
        register.adopt(answer)
    }
}

impl Screened for Processes {
    fn screened(_: &mut Register, answer: Answer<Processes>) -> Answer<Processes> {
        answer
    }
}

/// A join that failed: a panic of the reader's bookkeeping, or a call the runtime cancelled before it ran.
fn converted<T>(error: tokio::task::JoinError) -> Answer<T> {
    match error.try_into_panic() {
        Ok(panic) => Answer::Panicked(format!("gpu-switch's card reader panicked: {}", text(panic.as_ref()))),
        Err(_) => Answer::Failed(Failure::Unanswered(
            "gpu-switch's runtime cancelled the read before it ran".into(),
        )),
    }
}

/// A panic's text payload.
fn text(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|text| text.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic without a text payload".into())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::gpu::classify;
    use nvml_wrapper::error::NvmlError;
    use std::collections::{HashMap, VecDeque};
    use std::sync::Condvar;

    /// A driver call the script answers.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub(crate) enum Call {
        Open,
        Sample,
        Compute,
        Close,
    }

    /// What a scripted call does.
    #[derive(Debug, Clone)]
    pub(crate) enum Act {
        Answer,
        Fail(Failure),
        Panic(&'static str),
        /// Blocks the calling thread until the test releases it with another act.
        Hold,
    }

    /// A call the script holds in flight.
    pub(crate) struct Held {
        ticket: u64,
    }

    /// A complete read held at the completion pause, before its caller applies it.
    pub(crate) struct Paused {
        pub call: &'static str,
        pub read: u64,
        pub failed: bool,
        release: tokio::sync::oneshot::Sender<()>,
    }

    impl Paused {
        pub(crate) fn release(self) {
            let _ = self.release.send(());
        }
    }

    struct State {
        queued: HashMap<Call, VecDeque<Act>>,
        defaults: HashMap<Call, Act>,
        held: VecDeque<(Call, u64)>,
        released: HashMap<u64, Act>,
        tickets: u64,
        counts: HashMap<Call, usize>,
        order: Vec<Call>,
        identity: Vec<String>,
        sample: Sample,
        fault: bool,
        pauses: HashMap<&'static str, usize>,
        paused: VecDeque<Paused>,
    }

    struct Shared {
        state: Mutex<State>,
        wake: Condvar,
        changed: tokio::sync::Notify,
    }

    /// The scripted driver of the card reader's, the card owner's and the daemon's tests: each call does what the
    /// test queued for it, or its default, and fails as NVML does, through the same classifier.
    #[derive(Clone)]
    pub(crate) struct Script(Arc<Shared>);

    pub(crate) fn lost() -> Failure {
        classify("nvmlDeviceGetComputeRunningProcesses_v3", &NvmlError::GpuLost)
    }

    pub(crate) fn timeout() -> Failure {
        classify("nvmlDeviceGetComputeRunningProcesses_v3", &NvmlError::Timeout)
    }

    pub(crate) fn unknown() -> Failure {
        classify("nvmlDeviceGetHandleByIndex_v2", &NvmlError::Unknown)
    }

    impl Script {
        pub(crate) fn new() -> Script {
            Script(Arc::new(Shared {
                state: Mutex::new(State {
                    queued: HashMap::new(),
                    defaults: HashMap::new(),
                    held: VecDeque::new(),
                    released: HashMap::new(),
                    tickets: 0,
                    counts: HashMap::new(),
                    order: Vec::new(),
                    identity: vec!["GPU-a".into()],
                    sample: Sample {
                        memory: Some((24 << 30, 20 << 30)),
                        ..Sample::default()
                    },
                    fault: false,
                    pauses: HashMap::new(),
                    paused: VecDeque::new(),
                }),
                wake: Condvar::new(),
                changed: tokio::sync::Notify::new(),
            }))
        }

        pub(crate) fn driver(&self) -> Box<dyn Driver> {
            Box::new(self.clone())
        }

        pub(crate) fn reader(&self, reopen: Duration) -> Arc<CardReader> {
            Arc::new(CardReader::new(Some(self.driver()), reopen))
        }

        fn lock(&self) -> MutexGuard<'_, State> {
            self.0.state.lock().unwrap_or_else(PoisonError::into_inner)
        }

        fn changed(&self) {
            self.0.changed.notify_waiters();
            self.0.wake.notify_all();
        }

        /// The next `call` does `act`.
        pub(crate) fn then(&self, call: Call, act: Act) {
            self.lock().queued.entry(call).or_default().push_back(act);
        }

        /// Every `call` with nothing queued does `act`.
        pub(crate) fn always(&self, call: Call, act: Act) {
            self.lock().defaults.insert(call, act);
        }

        pub(crate) fn identity(&self, uuids: &[&str]) {
            self.lock().identity = uuids.iter().map(|uuid| uuid.to_string()).collect();
        }

        /// What a successful read answers: its processes and memory.
        pub(crate) fn answers(&self, compute: Processes, memory: Option<(u64, u64)>) {
            let mut state = self.lock();
            state.sample.compute = compute;
            state.sample.memory = memory;
        }

        pub(crate) fn fault(&self, on: bool) {
            self.lock().fault = on;
        }

        pub(crate) fn count(&self, call: Call) -> usize {
            self.lock().counts.get(&call).copied().unwrap_or(0)
        }

        pub(crate) fn order(&self) -> Vec<Call> {
            self.lock().order.clone()
        }

        /// The next completion of `call` ("sample" or "compute") waits at the completion pause.
        pub(crate) fn pause(&self, call: &'static str) {
            *self.lock().pauses.entry(call).or_default() += 1;
        }

        async fn until<R>(&self, mut found: impl FnMut(&mut State) -> Option<R>) -> R {
            loop {
                let notified = self.0.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Some(found) = found(&mut self.lock()) {
                    return found;
                }
                notified.await;
            }
        }

        /// The first call of `call` held in flight.
        pub(crate) async fn held(&self, call: Call) -> Held {
            self.until(|state| {
                let index = state.held.iter().position(|(held, _)| *held == call)?;
                state.held.remove(index).map(|(_, ticket)| Held { ticket })
            })
            .await
        }

        /// Every held and every later `call` does `act`.
        pub(crate) fn answer_held(&self, call: Call, act: Act) {
            let mut state = self.lock();
            state.defaults.insert(call, act.clone());
            let tickets: Vec<u64> = state.held.iter().filter(|(held, _)| *held == call).map(|(_, ticket)| *ticket).collect();
            state.held.retain(|(held, _)| *held != call);
            for ticket in tickets {
                state.released.insert(ticket, act.clone());
            }
            drop(state);
            self.changed();
        }

        pub(crate) fn release(&self, held: Held, act: Act) {
            self.lock().released.insert(held.ticket, act);
            self.changed();
        }

        /// The next read held at the completion pause.
        pub(crate) async fn paused(&self) -> Paused {
            self.until(|state| state.paused.pop_front()).await
        }

        fn perform(&self, call: Call) -> Act {
            let mut state = self.lock();
            *state.counts.entry(call).or_default() += 1;
            state.order.push(call);
            let act = state
                .queued
                .get_mut(&call)
                .and_then(VecDeque::pop_front)
                .or_else(|| state.defaults.get(&call).cloned())
                .unwrap_or(Act::Answer);
            let Act::Hold = act else {
                return act;
            };
            state.tickets += 1;
            let ticket = state.tickets;
            state.held.push_back((call, ticket));
            self.changed();
            loop {
                if let Some(act) = state.released.remove(&ticket) {
                    return act;
                }
                state = self.0.wake.wait(state).unwrap_or_else(PoisonError::into_inner);
            }
        }

        fn act(&self, call: Call) -> Result<(), Failure> {
            match self.perform(call) {
                Act::Answer => Ok(()),
                Act::Fail(failure) => Err(failure),
                Act::Panic(payload) => panic!("{payload}"),
                Act::Hold => unreachable!("a released call does another act"),
            }
        }

        fn sample(&self) -> Sample {
            let state = self.lock();
            Sample {
                identity: Identity::of(state.identity.clone()),
                ..state.sample.clone()
            }
        }
    }

    impl Driver for Script {
        fn open(&self) -> Result<Box<dyn Reader>, Failure> {
            self.act(Call::Open).map(|()| Box::new(Scripted(self.clone())) as Box<dyn Reader>)
        }

        fn fault(&self) {
            let armed = self.lock().fault;
            assert!(!armed, "the scripted fault");
        }

        fn completed(
            &self,
            call: &'static str,
            read: u64,
            failed: bool,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>> {
            let mut state = self.lock();
            let Some(armed) = state.pauses.get_mut(call).filter(|armed| **armed > 0) else {
                return Box::pin(std::future::ready(()));
            };
            *armed -= 1;
            let (release, released) = tokio::sync::oneshot::channel();
            state.paused.push_back(Paused {
                call,
                read,
                failed,
                release,
            });
            drop(state);
            self.changed();
            Box::pin(async move {
                let _ = released.await;
            })
        }
    }

    struct Scripted(Script);

    impl Reader for Scripted {
        fn sample(&self) -> Result<Sample, Failure> {
            self.0.act(Call::Sample).map(|()| self.0.sample())
        }

        fn compute(&self) -> Result<Processes, Failure> {
            self.0.act(Call::Compute).map(|()| self.0.sample().compute)
        }

        fn close(self: Box<Self>) -> Result<(), Failure> {
            self.0.act(Call::Close)
        }
    }

    const FOREVER: Duration = Duration::from_secs(3600);

    fn same<A, B>(a: &Observation<A>, b: &Observation<B>) -> bool {
        (a.read, a.generation, a.began, a.returned) == (b.read, b.generation, b.began, b.returned)
            && (a.began_unix, a.returned_unix) == (b.began_unix, b.returned_unix)
    }

    /// An observation without its value, so reads of both kinds compare.
    pub(crate) fn unit<T>(observation: Observation<T>) -> Observation<()> {
        let answer = match observation.answer {
            Answer::Unconfigured => Answer::Unconfigured,
            Answer::Read(_) => Answer::Read(()),
            Answer::Failed(failure) => Answer::Failed(failure),
            Answer::Panicked(payload) => Answer::Panicked(payload),
        };
        Observation {
            read: observation.read,
            generation: observation.generation,
            began: observation.began,
            returned: observation.returned,
            began_unix: observation.began_unix,
            returned_unix: observation.returned_unix,
            answer,
        }
    }

    fn lost_answer<T>(observation: &Observation<T>) -> bool {
        matches!(observation.answer, Answer::Failed(Failure::Lost(_)))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_open_is_stored_whole_and_each_replay_carries_it_and_registers_nothing() {
        for original_first in [true, false] {
            let script = Script::new();
            script.then(Call::Open, Act::Fail(Failure::Lost("nvmlInit_v2: NVML from x: no".into())));
            let reader = script.reader(FOREVER);
            let failed = unit(reader.sample().await);
            assert!(lost_answer(&failed) && failed.generation == 1, "{failed:?}");
            assert_eq!(reader.open_after(), Some(failed.returned + FOREVER), "counted from the failure's return");
            assert_eq!((reader.pending().failed, reader.registered()), (true, 1));
            let replay = unit(reader.compute().await);
            let again = unit(reader.sample().await);
            for copy in [&replay, &again] {
                assert!(same(copy, &failed) && lost_answer(copy), "{copy:?}");
            }
            assert_eq!((reader.registered(), script.count(Call::Open)), (1, 1), "replays register nothing");
            let (first, second) = match original_first {
                true => (&failed, &replay),
                false => (&replay, &failed),
            };
            reader.applied(first);
            assert_eq!(reader.registered(), 0, "either copy acknowledges the one entry");
            reader.applied(second);
            assert_eq!(reader.pending(), Pending { outstanding: None, failed: false });
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lost_read_retires_its_generation_and_closes_once_after_applied_before_the_next_open() {
        for from_sample in [false, true] {
            let script = Script::new();
            let reader = script.reader(Duration::ZERO);
            assert!(matches!(reader.sample().await.answer, Answer::Read(_)));
            let loss = match from_sample {
                true => {
                    script.then(Call::Sample, Act::Fail(lost()));
                    unit(reader.sample().await)
                }
                false => {
                    script.then(Call::Compute, Act::Fail(lost()));
                    unit(reader.compute().await)
                }
            };
            assert_eq!(loss.generation, 1);
            for _ in 0..3 {
                let withheld = reader.sample().await;
                assert!(lost_answer(&withheld) && withheld.read == loss.read, "{withheld:?}");
            }
            assert!(matches!(reader.compute().await.answer, Answer::Failed(Failure::Lost(_))), "compute never opens");
            assert_eq!((script.count(Call::Close), script.count(Call::Open)), (0, 1), "no close before applied");
            reader.applied(&loss);
            let recovered = reader.sample().await;
            assert!(matches!(recovered.answer, Answer::Read(_)) && recovered.generation == 2, "{recovered:?}");
            let order = script.order();
            assert_eq!(order[order.len() - 3..], [Call::Close, Call::Open, Call::Sample], "{order:?}");
            assert_eq!((script.count(Call::Close), script.count(Call::Open)), (1, 2));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retired_handle_with_a_read_in_flight_is_not_closed_and_nothing_opens_until_it_returns() {
        let script = Script::new();
        let reader = script.reader(Duration::ZERO);
        reader.sample().await;
        script.then(Call::Compute, Act::Hold);
        let inflight = tokio::spawn({
            let reader = reader.clone();
            async move { reader.compute().await }
        });
        let held = script.held(Call::Compute).await;
        script.then(Call::Compute, Act::Fail(lost()));
        let loss = reader.compute().await;
        reader.applied(&loss);
        for _ in 0..3 {
            assert!(same(&reader.sample().await, &loss));
        }
        assert_eq!((script.count(Call::Close), script.count(Call::Open)), (0, 1));
        script.answers(vec![(7, Some(1 << 20))], None);
        script.release(held, Act::Answer);
        let late = inflight.await.unwrap();
        assert_eq!((late.generation, late.answer), (1, Answer::Read(vec![(7, Some(1 << 20))])));
        let recovered = reader.sample().await;
        assert_eq!(recovered.generation, 2);
        let order = script.order();
        assert_eq!(order[order.len() - 3..], [Call::Close, Call::Open, Call::Sample], "{order:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_driver_call_runs_before_the_reopen_interval_counted_from_the_failures_return() {
        let script = Script::new();
        let reader = script.reader(FOREVER);
        reader.sample().await;
        script.then(Call::Compute, Act::Fail(lost()));
        let loss = reader.compute().await;
        assert_eq!(reader.open_after(), Some(loss.returned + FOREVER));
        reader.applied(&loss);
        for _ in 0..3 {
            assert!(same(&reader.sample().await, &loss));
        }
        assert_eq!((script.count(Call::Close), script.count(Call::Open)), (1, 1), "closed, not reopened");
        let calls = script.order().len();
        reader.sample().await;
        assert_eq!(script.order().len(), calls, "no driver call while re-opening is not due");
        reader.reopen_due();
        assert_eq!(reader.sample().await.generation, 2);
        assert_eq!(script.count(Call::Open), 2, "exactly one open once it is due");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_first_sample_adopts_its_identity_and_another_identity_is_a_loss_until_the_same_card_answers() {
        let script = Script::new();
        let reader = script.reader(Duration::ZERO);
        assert!(matches!(reader.sample().await.answer, Answer::Read(_)));
        script.identity(&["GPU-b"]);
        let other = reader.sample().await;
        let Answer::Failed(Failure::Lost(text)) = &other.answer else {
            panic!("{other:?}")
        };
        assert_eq!(
            text,
            "the GPU answers as another device: adopted GPU-a, found GPU-b; gpu-switch adopts a new card when it restarts"
        );
        assert!(same(&reader.compute().await, &other), "the other identity retired its generation");
        reader.applied(&other);
        assert!(lost_answer(&reader.sample().await), "a re-open answering another identity stays lost");
        script.identity(&["GPU-a"]);
        let failed = reader.sample().await;
        reader.applied(&failed);
        let again = reader.sample().await;
        assert!(matches!(again.answer, Answer::Read(_)) && again.generation == 3, "{again:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compute_never_opens_and_answers_not_opened_yet_before_and_during_the_first_open() {
        let script = Script::new();
        let reader = script.reader(Duration::ZERO);
        let not_yet = Answer::Failed(Failure::Unanswered("gpu-switch has not opened NVML yet".into()));
        let before = reader.compute().await;
        assert_eq!((before.generation, &before.answer), (0, &not_yet));
        assert_eq!((script.count(Call::Open), reader.registered()), (0, 0));
        script.then(Call::Open, Act::Hold);
        let opening = tokio::spawn({
            let reader = reader.clone();
            async move { reader.sample().await }
        });
        let held = script.held(Call::Open).await;
        let during = reader.compute().await;
        assert_eq!((during.generation, &during.answer), (0, &not_yet));
        script.release(held, Act::Answer);
        assert_eq!(opening.await.unwrap().generation, 1);
        assert_eq!(script.count(Call::Open), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unanswered_failure_keeps_the_handle() {
        let script = Script::new();
        let reader = script.reader(Duration::ZERO);
        reader.sample().await;
        script.always(Call::Compute, Act::Fail(timeout()));
        let failed = reader.compute().await;
        assert!(matches!(failed.answer, Answer::Failed(Failure::Unanswered(_))) && failed.generation == 1);
        script.always(Call::Compute, Act::Answer);
        assert!(reader.pending().failed);
        reader.applied(&failed);
        let next = reader.compute().await;
        assert!(matches!(next.answer, Answer::Read(_)) && next.generation == 1);
        assert_eq!((script.count(Call::Close), script.count(Call::Open)), (0, 1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unconfigured_reader_answers_unconfigured_and_registers_nothing() {
        let reader = Arc::new(CardReader::new(None, Duration::ZERO));
        assert!(matches!(reader.sample().await.answer, Answer::Unconfigured));
        assert_eq!(reader.compute().await.answer, Answer::Unconfigured);
        assert_eq!(reader.pending(), Pending { outstanding: None, failed: false });
    }

    /// Reads `pending()` continuously until stopped and counts the looks at which a read was neither outstanding nor
    /// a pending invalidation.
    struct Watcher {
        stop: Arc<std::sync::atomic::AtomicBool>,
        thread: std::thread::JoinHandle<usize>,
    }

    impl Watcher {
        fn start(reader: &Arc<CardReader>) -> Watcher {
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let thread = std::thread::spawn({
                let (reader, stop) = (reader.clone(), stop.clone());
                move || {
                    let mut neither = 0;
                    while !stop.load(std::sync::atomic::Ordering::Acquire) {
                        let pending = reader.pending();
                        neither += usize::from(pending.outstanding.is_none() && !pending.failed);
                    }
                    neither
                }
            });
            Watcher { stop, thread }
        }

        fn neither(self) -> usize {
            self.stop.store(true, std::sync::atomic::Ordering::Release);
            self.thread.join().unwrap()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_read_is_outstanding_until_it_returns_and_a_failure_is_pending_from_inside_its_call_until_applied() {
        for act in [Act::Answer, Act::Fail(timeout()), Act::Panic("a scripted panic")] {
            let script = Script::new();
            let reader = script.reader(Duration::ZERO);
            reader.sample().await;
            script.then(Call::Compute, Act::Hold);
            script.pause("compute");
            let dispatched = Instant::now();
            let read = tokio::spawn({
                let reader = reader.clone();
                async move { reader.compute().await }
            });
            let held = script.held(Call::Compute).await;
            let outstanding = reader.pending().outstanding.expect("outstanding from its dispatch");
            assert!(outstanding >= dispatched && outstanding <= Instant::now());
            let watcher = Watcher::start(&reader);
            let failed = !matches!(act, Act::Answer);
            script.release(held, act.clone());
            let paused = script.paused().await;
            assert_eq!((paused.call, paused.failed), ("compute", failed));
            assert!(paused.read > 1, "the paused read is the held compute");
            assert_eq!(
                reader.pending(),
                Pending { outstanding: None, failed },
                "{act:?}: the blocking call made the change before it returned"
            );
            paused.release();
            let observation = read.await.unwrap();
            match failed {
                true => assert_eq!(watcher.neither(), 0, "{act:?}: a failed read was always one or the other"),
                false => drop(watcher.neither()),
            }
            assert_eq!(reader.pending().failed, failed, "{observation:?}");
            reader.applied(&observation);
            assert_eq!(reader.pending(), Pending { outstanding: None, failed: false });
            if let Act::Panic(_) = act {
                assert_eq!(observation.answer, Answer::Panicked("a scripted panic".into()));
                assert_eq!(reader.compute().await.generation, 1, "a panic keeps the handle");
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_held_close_keeps_its_sample_outstanding_and_the_close_only_branch_leaves_nothing_registered() {
        for close in [Act::Answer, Act::Fail(unknown()), Act::Panic("a scripted close panic")] {
            for reopen in [Duration::ZERO, FOREVER] {
                let script = Script::new();
                let reader = script.reader(reopen);
                reader.sample().await;
                script.then(Call::Compute, Act::Fail(lost()));
                let loss = reader.compute().await;
                reader.applied(&loss);
                script.then(Call::Close, Act::Hold);
                let closing = tokio::spawn({
                    let reader = reader.clone();
                    async move { reader.sample().await }
                });
                let held = script.held(Call::Close).await;
                assert!(reader.pending().outstanding.is_some(), "the closing sample() stays outstanding");
                assert_eq!(script.count(Call::Open), 1, "no open while the close is held");
                script.release(held, close.clone());
                let answered = closing.await.unwrap();
                match reopen.is_zero() {
                    true => {
                        assert_eq!(answered.generation, 2, "{close:?}: one close, then one open");
                        let order = script.order();
                        assert_eq!(order[order.len() - 3..], [Call::Close, Call::Open, Call::Sample]);
                    }
                    false => {
                        assert!(same(&answered, &loss), "{close:?}: the close-only call answers the replay");
                        reader.applied(&answered);
                        assert_eq!(reader.registered(), 0, "{close:?}: its own entry left when it returned");
                        reader.reopen_due();
                        assert!(matches!(reader.sample().await.answer, Answer::Read(_)));
                    }
                }
                assert_eq!((script.count(Call::Close), script.count(Call::Open)), (1, 2), "{close:?}");
            }
        }
    }

    #[test]
    fn a_cancelled_dispatch_is_an_unanswered_failure_dated_by_its_guard_and_pending_until_applied() {
        let script = Script::new();
        let reader = script.reader(Duration::ZERO);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        assert_eq!(runtime.block_on(reader.sample()).generation, 1);
        let (holding, held) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let (answer, answered) = std::sync::mpsc::channel();
        runtime.spawn({
            let reader = reader.clone();
            async move {
                holding.send(tokio::task::coop::has_budget_remaining()).unwrap();
                released.recv().unwrap();
                answer.send(reader.compute().await).unwrap();
            }
        });
        assert!(held.recv().unwrap(), "the forced entry holds with a fresh budget");
        runtime.shutdown_background();
        release.send(()).unwrap();
        let cancelled = answered.recv().unwrap();
        assert_eq!(
            cancelled.answer,
            Answer::Failed(Failure::Unanswered("gpu-switch's runtime cancelled the read before it ran".into()))
        );
        assert_eq!((cancelled.read, cancelled.generation), (2, 1));
        assert!(cancelled.began <= cancelled.returned);
        assert_eq!(reader.invalidated(cancelled.read), Some(cancelled.returned), "dated by the guard");
        assert_eq!(script.count(Call::Compute), 0, "the closure never ran");
        assert!(reader.pending().failed, "pending after the converted observation returned");
        reader.applied(&cancelled);
        assert_eq!(reader.pending(), Pending { outstanding: None, failed: false });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unrun_guard_and_a_panic_in_the_completion_bookkeeping_leave_a_pending_invalidation() {
        let script = Script::new();
        let reader = script.reader(Duration::ZERO);
        reader.sample().await;
        let unrun = {
            let mut register = reader.lock();
            reader.dispatch(&mut register, Some(1))
        };
        let read = unrun.key.read;
        drop(unrun);
        assert!(reader.invalidated(read).is_some() && reader.pending().failed, "an unrun guard invalidates");
        reader.lock().reads.remove(&read);
        script.fault(true);
        let faulted = reader.compute().await;
        assert_eq!(
            faulted.answer,
            Answer::Panicked("gpu-switch's card reader panicked: the scripted fault".into())
        );
        assert!(reader.register.is_poisoned(), "the fault panicked while the register lock was held");
        assert!(reader.pending().failed, "the guard invalidated while unwinding");
        script.fault(false);
        reader.applied(&faulted);
        assert_eq!(reader.pending(), Pending { outstanding: None, failed: false });
        assert!(matches!(reader.compute().await.answer, Answer::Read(_)), "the poisoned lock is recovered");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_panicking_close_is_a_failed_close_and_the_next_due_open_runs_once() {
        let script = Script::new();
        let reader = script.reader(Duration::ZERO);
        reader.sample().await;
        script.then(Call::Compute, Act::Fail(lost()));
        let loss = reader.compute().await;
        reader.applied(&loss);
        script.then(Call::Close, Act::Panic("a scripted close panic"));
        let recovered = reader.sample().await;
        assert!(matches!(recovered.answer, Answer::Read(_)) && recovered.generation == 2);
        assert_eq!((script.count(Call::Close), script.count(Call::Open)), (1, 2));
        assert!(reader.lock().retired.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_join_errors_convert_to_a_panic_and_a_cancellation() {
        let panicked = tokio::spawn(async { panic!("a task's panic") }).await.unwrap_err();
        assert_eq!(
            converted::<()>(panicked),
            Answer::Panicked("gpu-switch's card reader panicked: a task's panic".into())
        );
        let task = tokio::spawn(std::future::pending::<()>());
        task.abort();
        let aborted = task.await.unwrap_err();
        assert_eq!(
            converted::<()>(aborted),
            Answer::Failed(Failure::Unanswered("gpu-switch's runtime cancelled the read before it ran".into()))
        );
    }
}
