//! Python declarations and results only. Rust owns transport and all policy.
use super::{
    attempts::{Attempt, Launch, Move},
    client::{CancelToken, Client, Deadline, Failure},
    model::{Group, Job},
};
use crate::python::AuthorityUnavailable;
use pyo3::exceptions::{asyncio::CancelledError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyType;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;

pyo3::create_exception!(_native, CompilationFailed, PyRuntimeError);

/// The daemon's warnings about a request reach the caller as Python warnings.
pub(crate) fn warn_each(py: Python<'_>, warnings: &Value) -> PyResult<()> {
    for warning in warnings.as_array().into_iter().flatten().filter_map(Value::as_str) {
        let message = std::ffi::CString::new(warning).map_err(|e| PyValueError::new_err(e.to_string()))?;
        PyErr::warn(py, &py.get_type::<pyo3::exceptions::PyUserWarning>(), &message, 2)?;
    }
    Ok(())
}
pub(crate) fn runtime() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}
fn seconds(value: f64) -> PyResult<Duration> {
    if !value.is_finite() || value <= 0.0 || value > 315360000.0 {
        return Err(PyValueError::new_err(
            "timeout must be positive finite seconds, at most ten years",
        ));
    }
    Ok(Duration::from_secs_f64(value))
}
/// The one conversion of the transport's failures, for every compiler and claim entry point. An outage is
/// AuthorityUnavailable, naming its source and operation, never a verdict; a refusal or an invalid operation
/// stays RuntimeError; a local cancellation is asyncio's CancelledError. The binding decides no availability.
pub(crate) fn raised(py: Python<'_>, failure: Failure) -> PyErr {
    match failure {
        Failure::Unavailable(outage) => {
            let error = AuthorityUnavailable::new_err(outage.reason);
            let named = error
                .value(py)
                .setattr("source", outage.source)
                .and_then(|()| error.value(py).setattr("operation", outage.operation));
            named.map_or_else(|failed| failed, |()| error)
        }
        Failure::Refused(reason) | Failure::Invalid(reason) => PyRuntimeError::new_err(reason),
        Failure::Cancelled => CancelledError::new_err(Failure::Cancelled.to_string()),
    }
}
/// Awaits one native answer with no deadline of its own, its failure raised by `raised`. Signals are checked
/// locally, so Ctrl-C still interrupts a wait that sends nothing until the daemon replies.
pub(crate) async fn answered<T>(
    answer: impl std::future::Future<Output = Result<T, Failure>>,
) -> PyResult<T> {
    interruptible(async {
        answer
            .await
            .map_err(|failure| Python::attach(|py| raised(py, failure)))
    })
    .await
}
/// Awaits an answer whose failure is already the Python exception to raise, checking signals meanwhile.
pub(crate) async fn interruptible<T>(
    answer: impl std::future::Future<Output = PyResult<T>>,
) -> PyResult<T> {
    tokio::pin!(answer);
    let mut signals = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            value = &mut answer => return value,
            _ = signals.tick() => Python::attach(|py| py.check_signals())?,
        }
    }
}
pub(crate) fn json_object(py: Python<'_>, value: Value) -> PyResult<Py<PyAny>> {
    Ok(py
        .import("json")?
        .call_method1("loads", (value.to_string(),))?
        .unbind())
}
fn json_value(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Value> {
    let encoded: String = py
        .import("json")?
        .call_method1("dumps", (value,))?
        .extract()?;
    serde_json::from_str(&encoded).map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyclass(name = "CompileJob", module = "gpu_queue._native", frozen)]
#[derive(Clone)]
pub struct PythonJob {
    job: Job,
}
#[pymethods]
impl PythonJob {
    #[new]
    #[pyo3(signature=(job_id, argv, cwd, *, expected_runtime_secs, env=None, memory_bytes=0, expected_memory_bytes=None, error_memory_bytes=None, dependencies=None, metadata=None, inherit_environment=true))]
    fn new(
        job_id: String,
        argv: Vec<String>,
        cwd: String,
        expected_runtime_secs: u64,
        env: Option<BTreeMap<String, String>>,
        memory_bytes: u64,
        expected_memory_bytes: Option<u64>,
        error_memory_bytes: Option<u64>,
        dependencies: Option<Vec<String>>,
        metadata: Option<BTreeMap<String, String>>,
        inherit_environment: bool,
    ) -> PyResult<Self> {
        Self::validated(Job {
            job_id,
            argv,
            cwd,
            env: env.unwrap_or_default().into(),
            timeout_seconds: None,
            memory_bytes,
            expected_memory_bytes,
            expected_runtime_secs: Some(expected_runtime_secs),
            error_memory_bytes,
            dependencies: dependencies.unwrap_or_default(),
            metadata: metadata.unwrap_or_default(),
            inherit_environment,
        })
    }

    #[getter]
    fn job_id(&self) -> &str {
        &self.job.job_id
    }
    #[getter]
    fn argv(&self) -> Vec<String> {
        self.job.argv.clone()
    }
    #[getter]
    fn cwd(&self) -> &str {
        &self.job.cwd
    }
    #[getter]
    fn dependencies(&self) -> Vec<String> {
        self.job.dependencies.clone()
    }
    #[getter]
    fn memory_bytes(&self) -> u64 {
        self.job.memory_bytes
    }
    #[getter]
    fn expected_memory_bytes(&self) -> Option<u64> {
        self.job.expected_memory_bytes
    }
    #[getter]
    fn expected_runtime_secs(&self) -> Option<u64> {
        self.job.expected_runtime_secs
    }
    #[getter]
    fn error_memory_bytes(&self) -> Option<u64> {
        self.job.error_memory_bytes
    }
    #[getter]
    fn env(&self) -> BTreeMap<String, String> {
        self.job.env.to_map()
    }
    #[getter]
    fn metadata(&self) -> BTreeMap<String, String> {
        self.job.metadata.clone()
    }
    #[getter]
    fn inherit_environment(&self) -> bool {
        self.job.inherit_environment
    }
    fn with_env(&self, env: BTreeMap<String, String>) -> Self {
        let mut job = self.job.clone();
        job.env = env.into();
        Self { job }
    }
    fn to_json(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_object(
            py,
            serde_json::to_value(&self.job).map_err(|e| PyValueError::new_err(e.to_string()))?,
        )
    }
    #[classmethod]
    fn from_json(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let job: Job = serde_json::from_value(json_value(py, value)?)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Self::validated(job)
    }
}
impl PythonJob {
    /// Persisted declarations keep their exact fields so journals round-trip.
    fn validated(job: Job) -> PyResult<Self> {
        super::model::identifier(&job.job_id).map_err(PyValueError::new_err)?;
        // Validate the command immediately; dependencies are checked by its group.
        let mut command = job.clone();
        command.dependencies.clear();
        Group {
            group_id: "validation".into(),
            jobs: vec![command],
            metadata: BTreeMap::new(),
            max_running: 0,
            result_sink: None,
        }
        .validate()
        .map_err(PyValueError::new_err)?;
        Ok(Self { job })
    }
}
#[pyclass(name = "CompileGroup", module = "gpu_queue._native", frozen)]
#[derive(Clone)]
pub struct PythonGroup {
    group: Group,
}
#[pymethods]
impl PythonGroup {
    #[new]
    #[pyo3(signature=(group_id,jobs,*,max_running=0,metadata=None,result_sink=None))]
    fn new(
        group_id: String,
        jobs: Vec<PyRef<'_, PythonJob>>,
        max_running: usize,
        metadata: Option<BTreeMap<String, String>>,
        result_sink: Option<std::path::PathBuf>,
    ) -> PyResult<Self> {
        let group = Group {
            group_id,
            jobs: jobs.iter().map(|j| j.job.clone()).collect(),
            max_running,
            metadata: metadata.unwrap_or_default(),
            result_sink,
        };
        group.validate().map_err(PyValueError::new_err)?;
        Ok(Self { group })
    }
    #[getter]
    fn group_id(&self) -> &str {
        &self.group.group_id
    }
    #[getter]
    fn jobs(&self) -> Vec<PythonJob> {
        self.group
            .jobs
            .iter()
            .map(|j| PythonJob { job: j.clone() })
            .collect()
    }
    #[getter]
    fn max_running(&self) -> usize {
        self.group.max_running
    }
    #[getter]
    fn metadata(&self) -> BTreeMap<String, String> {
        self.group.metadata.clone()
    }
    #[getter]
    fn result_sink(&self) -> Option<std::path::PathBuf> {
        self.group.result_sink.clone()
    }
    fn to_json(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_object(
            py,
            serde_json::to_value(&self.group).map_err(|e| PyValueError::new_err(e.to_string()))?,
        )
    }
    #[classmethod]
    fn from_json(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let group: Group = serde_json::from_value(json_value(py, value)?)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        group.validate().map_err(PyValueError::new_err)?;
        Ok(Self { group })
    }
}
#[pyclass(name = "CompileResult", module = "gpu_queue._native", frozen, get_all)]
#[derive(Clone)]
pub struct PythonResult {
    group_id: String,
    job_id: String,
    outcome: String,
    returncode: Option<i32>,
    detail: String,
    queued_at: Option<f64>,
    started_at: Option<f64>,
    finished_at: Option<f64>,
    execution_seconds: f64,
    log_path: String,
    pid: Option<u32>,
    peak_memory_bytes: Option<u64>,
    memory_limit_bytes: Option<u64>,
    median_memory_bytes: Option<u64>,
    p90_memory_bytes: Option<u64>,
    median_cores: Option<f64>,
    p90_cores: Option<f64>,
    expected_runtime_secs: Option<u64>,
    warnings: Vec<String>,
    /// The job's expected figure, when it declares one.
    expected_memory_bytes: Option<u64>,
    /// What the overcommit budget lent the attempt at its end, when it lent any.
    lending_bytes: Option<u64>,
    /// The facts of the attempt a reaped result concludes; none for any other result.
    attempt: Option<PythonAttempt>,
}
impl PythonResult {
    fn decode(value: &Value) -> PyResult<Self> {
        Ok(Self {
            group_id: value["group_id"]
                .as_str()
                .ok_or_else(|| PyRuntimeError::new_err("missing compiler result group"))?
                .into(),
            job_id: value["job_id"]
                .as_str()
                .ok_or_else(|| PyRuntimeError::new_err("missing compiler result ID"))?
                .into(),
            outcome: value["outcome"]
                .as_str()
                .ok_or_else(|| PyRuntimeError::new_err("missing compiler result outcome"))?
                .into(),
            returncode: value["returncode"].as_i64().map(|n| n as i32),
            detail: value["detail"].as_str().unwrap_or("").into(),
            queued_at: value["queued_at"].as_f64(),
            started_at: value["started_at"].as_f64(),
            finished_at: value["finished_at"].as_f64(),
            execution_seconds: value["execution_seconds"].as_f64().unwrap_or(0.0),
            log_path: value["log_path"].as_str().unwrap_or("").into(),
            pid: value["pid"].as_u64().map(|p| p as u32),
            peak_memory_bytes: value["peak_memory_bytes"].as_u64(),
            memory_limit_bytes: value["memory_limit_bytes"].as_u64(),
            median_memory_bytes: value["median_memory_bytes"].as_u64(),
            p90_memory_bytes: value["p90_memory_bytes"].as_u64(),
            median_cores: value["median_cores"].as_f64(),
            p90_cores: value["p90_cores"].as_f64(),
            expected_runtime_secs: value["expected_runtime_secs"].as_u64(),
            warnings: value["warnings"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|w| w.as_str().map(String::from))
                .collect(),
            expected_memory_bytes: value["expected_memory_bytes"].as_u64(),
            lending_bytes: value["lending_bytes"].as_u64(),
            attempt: match &value["attempt"] {
                Value::Null => None,
                attempt => Some(PythonAttempt(
                    serde_json::from_value(attempt.clone())
                        .map_err(|e| PyValueError::new_err(format!("invalid compiler result attempt: {e}")))?,
                )),
            },
        })
    }
}
#[pymethods]
impl PythonResult {
    fn __deepcopy__(&self, _memo: &Bound<'_, PyAny>) -> Self {
        self.clone()
    }
    fn to_json(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut value = json!({"group_id":self.group_id,"job_id":self.job_id,"outcome":self.outcome,"returncode":self.returncode,
            "detail":self.detail,"queued_at":self.queued_at,"started_at":self.started_at,"finished_at":self.finished_at,
            "execution_seconds":self.execution_seconds,"log_path":self.log_path,"pid":self.pid,
            "peak_memory_bytes":self.peak_memory_bytes,"memory_limit_bytes":self.memory_limit_bytes,
            "median_memory_bytes":self.median_memory_bytes,"p90_memory_bytes":self.p90_memory_bytes,
            "median_cores":self.median_cores,"p90_cores":self.p90_cores,
            "expected_runtime_secs":self.expected_runtime_secs,"warnings":self.warnings});
        // The keys GQ-3 added are written only when present, so an older receipt writes back as it was.
        [("expected_memory_bytes", self.expected_memory_bytes), ("lending_bytes", self.lending_bytes)]
            .into_iter()
            .filter_map(|(key, bytes)| Some((key, bytes?)))
            .for_each(|(key, bytes)| value[key] = json!(bytes));
        // `attempt` is written through its one encoder, and only when the result holds one.
        let attempt = self.attempt.as_ref().map(|attempt| serde_json::to_value(&attempt.0)).transpose();
        attempt
            .map_err(|e| PyValueError::new_err(e.to_string()))?
            .into_iter()
            .for_each(|attempt| value["attempt"] = attempt);
        json_object(py, value)
    }
    #[classmethod]
    fn from_json(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let value = Self::decode(&json_value(py, value)?)?;
        super::model::identifier(&value.group_id).map_err(PyValueError::new_err)?;
        super::model::identifier(&value.job_id).map_err(PyValueError::new_err)?;
        if !["queued", "running", "success", "fail", "cancelled", "lost"]
            .contains(&value.outcome.as_str())
            || (value.outcome == "success" && value.returncode != Some(0))
        {
            return Err(PyValueError::new_err("invalid compiler result outcome"));
        }
        Ok(value)
    }
}
/// A reaped result's `attempt` (`attempts::Attempt`), read through its own getters.
#[pyclass(name = "CompileAttempt", module = "gpu_queue._native", frozen)]
#[derive(Clone)]
pub struct PythonAttempt(Attempt);
#[pymethods]
impl PythonAttempt {
    /// None when the concluding instance restored the attempt and saw no launch.
    #[getter]
    fn launch(&self) -> Option<PythonLaunch> {
        self.0.launch.map(PythonLaunch)
    }
    #[getter]
    fn moves(&self) -> Vec<PythonMove> {
        self.0.moves.iter().cloned().map(PythonMove).collect()
    }
    #[getter]
    fn cause(&self) -> PyResult<String> {
        serde_json::to_value(self.0.cause)
            .ok()
            .and_then(|cause| cause.as_str().map(String::from))
            .ok_or_else(|| PyValueError::new_err("invalid compiler result cause"))
    }
    #[getter]
    fn peak_reset(&self) -> bool {
        self.0.peak_reset
    }
}
/// An attempt's launch: its cell, its expected figure and what the overcommit budget lent it.
#[pyclass(name = "CompileLaunch", module = "gpu_queue._native", frozen)]
#[derive(Clone)]
pub struct PythonLaunch(Launch);
#[pymethods]
impl PythonLaunch {
    #[getter]
    fn cell_bytes(&self) -> u64 {
        self.0.cell_bytes
    }
    #[getter]
    fn expected_bytes(&self) -> u64 {
        self.0.expected_bytes
    }
    #[getter]
    fn lending_bytes(&self) -> u64 {
        self.0.lending_bytes
    }
}
/// One raise an attempt received from an out-of-memory-killed sibling's holding.
#[pyclass(name = "CompileMove", module = "gpu_queue._native", frozen)]
#[derive(Clone)]
pub struct PythonMove(Move);
#[pymethods]
impl PythonMove {
    #[getter]
    fn at(&self) -> f64 {
        self.0.at
    }
    #[getter]
    fn donor_job_id(&self) -> String {
        self.0.donor.job_id.clone()
    }
    #[getter]
    fn donor_started_at(&self) -> f64 {
        self.0.donor.started_at
    }
    #[getter]
    fn lending_bytes(&self) -> u64 {
        self.0.lending_bytes
    }
    #[getter]
    fn cap_bytes(&self) -> u64 {
        self.0.cap_bytes
    }
}
#[pyclass(name = "CompileStatus", module = "gpu_queue._native", frozen, get_all)]
#[derive(Clone)]
pub struct PythonStatus {
    group_id: String,
    complete: bool,
    jobs: Vec<PythonResult>,
    /// Why the group's results could not be written to its result sink.
    result_sink_error: Option<String>,
}
impl PythonStatus {
    fn decode(value: Value) -> PyResult<Self> {
        Ok(Self {
            group_id: value["group_id"]
                .as_str()
                .ok_or_else(|| PyRuntimeError::new_err("missing compiler group"))?
                .into(),
            complete: value["complete"]
                .as_bool()
                .ok_or_else(|| PyRuntimeError::new_err("missing compiler completion state"))?,
            jobs: value["jobs"]
                .as_array()
                .ok_or_else(|| PyRuntimeError::new_err("missing compiler results"))?
                .iter()
                .map(PythonResult::decode)
                .collect::<PyResult<_>>()?,
            result_sink_error: value["result_sink_error"].as_str().map(String::from),
        })
    }
}
#[pymethods]
impl PythonStatus {
    #[getter]
    fn successful(&self) -> bool {
        self.complete && self.jobs.iter().all(|j| j.outcome == "success")
    }
    fn check(&self, py: Python<'_>) -> PyResult<()> {
        if !self.complete {
            return Err(PyRuntimeError::new_err("compilation is not complete"));
        }
        if self.successful() {
            return Ok(());
        }
        let failures: Vec<_> = self
            .jobs
            .iter()
            .filter(|j| j.outcome != "success")
            .map(|j| format!("{}: {} ({})", j.job_id, j.outcome, j.detail))
            .collect();
        let error = CompilationFailed::new_err(failures.join("; "));
        error
            .value(py)
            .setattr("status", Py::new(py, self.clone())?)?;
        Err(error)
    }
}
/// A local cancellation of `CompilerQueue.wait` and `HostMemoryClaim.acquire`. Cancel it from any thread,
/// before or during the wait, without borrowing the queue or the claim: it is latched, so a cancellation before
/// the wait starts is never lost. The wait raises CancelledError and the service keeps its work.
#[pyclass(name = "CancelToken", module = "gpu_queue._native", frozen)]
pub struct PythonCancel {
    latch: CancelToken,
}
#[pymethods]
impl PythonCancel {
    #[new]
    fn new() -> Self {
        Self {
            latch: CancelToken::new(),
        }
    }
    fn cancel(&self) {
        self.latch.cancel();
    }
    #[getter]
    fn cancelled(&self) -> bool {
        self.latch.cancelled()
    }
}
impl PythonCancel {
    /// The native latch, cloned while attached so the wait holds no Python object.
    pub(crate) fn latch(&self) -> CancelToken {
        self.latch.clone()
    }
}
#[pyclass(name = "CompilerQueue", module = "gpu_queue._native", frozen)]
#[derive(Clone)]
pub struct PythonQueue {
    client: Client,
    /// A probe's explicit deadline for each snapshot exchange; scheduling clients have none.
    probe: Option<Duration>,
}
impl PythonQueue {
    /// Each snapshot exchange's deadline: the probe's own, or a scheduling snapshot's.
    fn deadline(&self) -> Deadline {
        self.probe.map_or_else(Deadline::snapshot, Deadline::after)
    }
}
#[pymethods]
impl PythonQueue {
    /// `request_timeout` exists only for service probes. Scheduling clients omit it: each snapshot exchange
    /// then has the scheduling patience, and a busy daemon delays a parked wait instead of failing it.
    #[new]
    #[pyo3(signature=(*,state_dir=None,request_timeout=None))]
    fn new(
        py: Python<'_>,
        state_dir: Option<String>,
        request_timeout: Option<f64>,
    ) -> PyResult<Self> {
        let state = state_dir.map_or_else(super::default_state, std::path::PathBuf::from);
        Ok(Self {
            probe: request_timeout.map(seconds).transpose()?,
            client: Client::new(&state).map_err(|failure| raised(py, failure))?,
        })
    }
    #[getter]
    fn state_dir(&self) -> String {
        self.client.state().to_string_lossy().into_owned()
    }
    fn health(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let rt = runtime()?;
        let value = py.detach(|| rt.block_on(answered(self.client.health(self.deadline()))))?;
        json_object(py, value)
    }
    #[pyo3(signature=(group,*,priority=0))]
    fn submit(
        &self,
        py: Python<'_>,
        group: PyRef<'_, PythonGroup>,
        priority: i64,
    ) -> PyResult<String> {
        let priority = super::party::bounded(priority).map_err(PyValueError::new_err)?;
        let group = group.group.clone();
        let rt = runtime()?;
        let reply = py.detach(|| {
            rt.block_on(answered(self.client.submit(&group, priority, self.deadline())))
        })?;
        warn_each(py, &reply["warnings"])?;
        Ok(group.group_id)
    }
    fn status(&self, py: Python<'_>, group_id: &str) -> PyResult<PythonStatus> {
        let rt = runtime()?;
        PythonStatus::decode(
            py.detach(|| rt.block_on(answered(self.client.status(group_id, self.deadline()))))?,
        )
    }
    /// Pending until every job is terminal; only an explicit cancel ends it early. Cancelling `token` ends
    /// this wait locally with CancelledError, and the service keeps the group.
    #[pyo3(signature=(group_id,token,*,check=true))]
    fn wait(
        &self,
        py: Python<'_>,
        group_id: &str,
        token: &Bound<'_, PythonCancel>,
        check: bool,
    ) -> PyResult<PythonStatus> {
        let cancel = token.get().latch();
        let rt = runtime()?;
        let value = py.detach(|| rt.block_on(answered(self.client.wait_group(group_id, &cancel))))?;
        let status = PythonStatus::decode(value)?;
        if check {
            status.check(py)?;
        }
        Ok(status)
    }
    #[pyo3(signature=(group_id,reason,*,job_id=None,include_running=false))]
    fn cancel(
        &self,
        py: Python<'_>,
        group_id: &str,
        reason: &str,
        job_id: Option<&str>,
        include_running: bool,
    ) -> PyResult<Py<PyAny>> {
        let rt = runtime()?;
        let value = py.detach(|| {
            rt.block_on(answered(self.client.cancel(
                group_id,
                job_id,
                reason,
                include_running,
                self.deadline(),
            )))
        })?;
        json_object(py, value)
    }
}
pub fn register(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PythonJob>()?;
    module.add_class::<PythonGroup>()?;
    module.add_class::<PythonResult>()?;
    module.add_class::<PythonAttempt>()?;
    module.add_class::<PythonLaunch>()?;
    module.add_class::<PythonMove>()?;
    module.add_class::<PythonStatus>()?;
    module.add_class::<PythonQueue>()?;
    module.add_class::<PythonCancel>()?;
    super::memory_python::register(module)?;
    module.add("CompilationFailed", py.get_type::<CompilationFailed>())?;
    Ok(())
}
