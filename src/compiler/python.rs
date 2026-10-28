//! Python declarations and results only. Rust owns transport and all policy.
use super::{
    client::Client,
    model::{Group, Job, Pipeline},
};
use pyo3::exceptions::{PyRuntimeError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyType;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

pyo3::create_exception!(_native, CompilationFailed, PyRuntimeError);

fn runtime() -> PyResult<tokio::runtime::Runtime> {
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
fn json_object(py: Python<'_>, value: Value) -> PyResult<Py<PyAny>> {
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
    #[pyo3(signature=(job_id, argv, cwd, *, env=None, timeout_seconds=1200.0, memory_bytes=0, dependencies=None, metadata=None, inherit_environment=true))]
    fn new(
        job_id: String,
        argv: Vec<String>,
        cwd: String,
        env: Option<BTreeMap<String, String>>,
        timeout_seconds: f64,
        memory_bytes: u64,
        dependencies: Option<Vec<String>>,
        metadata: Option<BTreeMap<String, String>>,
        inherit_environment: bool,
    ) -> PyResult<Self> {
        super::model::identifier(&job_id).map_err(PyValueError::new_err)?;
        seconds(timeout_seconds)?;
        let job = Job {
            job_id,
            argv,
            cwd,
            env: env.unwrap_or_default(),
            timeout_seconds,
            memory_bytes,
            dependencies: dependencies.unwrap_or_default(),
            metadata: metadata.unwrap_or_default(),
            inherit_environment,
        };
        // Validate the command immediately; dependencies are checked by its group.
        let mut command = job.clone();
        command.dependencies.clear();
        Group {
            group_id: "validation".into(),
            jobs: vec![command],
            metadata: BTreeMap::new(),
            max_running: 0,
        }
        .validate()
        .map_err(PyValueError::new_err)?;
        Ok(Self { job })
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
    fn timeout_seconds(&self) -> f64 {
        self.job.timeout_seconds
    }
    #[getter]
    fn env(&self) -> BTreeMap<String, String> {
        self.job.env.clone()
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
        job.env = env;
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
        Self::new(
            job.job_id,
            job.argv,
            job.cwd,
            Some(job.env),
            job.timeout_seconds,
            job.memory_bytes,
            Some(job.dependencies),
            Some(job.metadata),
            job.inherit_environment,
        )
    }
}
#[pyclass(name = "Pipeline", module = "gpu_queue._native", frozen)]
#[derive(Clone)]
pub struct PythonPipeline {
    plan: Pipeline,
}
#[pymethods]
impl PythonPipeline {
    #[new]
    #[pyo3(signature=(compile_group,gpu_jobs,*,max_running=0))]
    fn new(
        compile_group: PyRef<'_, PythonGroup>,
        gpu_jobs: Vec<PyRef<'_, PythonJob>>,
        max_running: usize,
    ) -> PyResult<Self> {
        let plan = Pipeline {
            compile_group: compile_group.group.clone(),
            gpu_jobs: gpu_jobs.iter().map(|j| j.job.clone()).collect(),
            max_running,
        };
        plan.validate().map_err(PyValueError::new_err)?;
        Ok(Self { plan })
    }
    #[getter]
    fn compile_group(&self) -> PythonGroup {
        PythonGroup {
            group: self.plan.compile_group.clone(),
        }
    }
    #[getter]
    fn gpu_jobs(&self) -> Vec<PythonJob> {
        self.plan
            .gpu_jobs
            .iter()
            .map(|j| PythonJob { job: j.clone() })
            .collect()
    }
    #[getter]
    fn max_running(&self) -> usize {
        self.plan.max_running
    }
    fn to_json(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_object(
            py,
            serde_json::to_value(&self.plan).map_err(|e| PyValueError::new_err(e.to_string()))?,
        )
    }
    #[classmethod]
    fn from_json(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let plan: Pipeline = serde_json::from_value(json_value(py, value)?)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        plan.validate().map_err(PyValueError::new_err)?;
        Ok(Self { plan })
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
    #[pyo3(signature=(group_id,jobs,*,max_running=0,metadata=None))]
    fn new(
        group_id: String,
        jobs: Vec<PyRef<'_, PythonJob>>,
        max_running: usize,
        metadata: Option<BTreeMap<String, String>>,
    ) -> PyResult<Self> {
        let group = Group {
            group_id,
            jobs: jobs.iter().map(|j| j.job.clone()).collect(),
            max_running,
            metadata: metadata.unwrap_or_default(),
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
        })
    }
}
#[pymethods]
impl PythonResult {
    fn __deepcopy__(&self, _memo: &Bound<'_, PyAny>) -> Self {
        self.clone()
    }
    fn to_json(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_object(
            py,
            json!({"group_id":self.group_id,"job_id":self.job_id,"outcome":self.outcome,"returncode":self.returncode,
            "detail":self.detail,"queued_at":self.queued_at,"started_at":self.started_at,"finished_at":self.finished_at,
            "execution_seconds":self.execution_seconds,"log_path":self.log_path,"pid":self.pid}),
        )
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
#[pyclass(name = "CompileStatus", module = "gpu_queue._native", frozen, get_all)]
#[derive(Clone)]
pub struct PythonStatus {
    group_id: String,
    complete: bool,
    jobs: Vec<PythonResult>,
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
#[pyclass(name = "CompilerQueue", module = "gpu_queue._native", frozen)]
#[derive(Clone)]
pub struct PythonQueue {
    client: Client,
}
#[pymethods]
impl PythonQueue {
    #[new]
    #[pyo3(signature=(*,state_dir=None,request_timeout=30.0))]
    fn new(state_dir: Option<String>, request_timeout: f64) -> PyResult<Self> {
        let state = state_dir
            .unwrap_or_else(|| format!("/var/tmp/rust-compile-{}", unsafe { libc::getuid() }));
        Ok(Self {
            client: Client::new(Path::new(&state), seconds(request_timeout)?)
                .map_err(PyRuntimeError::new_err)?,
        })
    }
    #[getter]
    fn state_dir(&self) -> String {
        self.client.state().to_string_lossy().into_owned()
    }
    fn health(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let rt = runtime()?;
        let value = py
            .detach(|| rt.block_on(self.client.health()))
            .map_err(PyRuntimeError::new_err)?;
        json_object(py, value)
    }
    fn submit(&self, py: Python<'_>, group: PyRef<'_, PythonGroup>) -> PyResult<String> {
        let group = group.group.clone();
        let rt = runtime()?;
        py.detach(|| rt.block_on(self.client.submit(&group)))
            .map_err(PyRuntimeError::new_err)?;
        Ok(group.group_id)
    }
    fn submit_pipeline(&self, py: Python<'_>, plan: PyRef<'_, PythonPipeline>) -> PyResult<String> {
        let plan = plan.plan.clone();
        let rt = runtime()?;
        py.detach(|| {
            rt.block_on(
                self.client
                    .request(json!({"op":"submit_pipeline","pipeline":plan})),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
        Ok(plan.compile_group.group_id)
    }
    fn pipeline_status(&self, py: Python<'_>, group_id: &str) -> PyResult<PythonStatus> {
        let rt = runtime()?;
        PythonStatus::decode(
            py.detach(|| {
                rt.block_on(
                    self.client
                        .request(json!({"op":"pipeline_status","group_id":group_id})),
                )
            })
            .map_err(PyRuntimeError::new_err)?,
        )
    }
    fn status(&self, py: Python<'_>, group_id: &str) -> PyResult<PythonStatus> {
        let rt = runtime()?;
        PythonStatus::decode(
            py.detach(|| rt.block_on(self.client.status(group_id)))
                .map_err(PyRuntimeError::new_err)?,
        )
    }
    #[pyo3(signature=(group_id,*,timeout=None,check=true))]
    fn wait(
        &self,
        py: Python<'_>,
        group_id: &str,
        timeout: Option<f64>,
        check: bool,
    ) -> PyResult<PythonStatus> {
        self.wait_operation(py, group_id, timeout, check, "group_status")
    }
    #[pyo3(signature=(group_id,*,timeout=None,check=true))]
    fn wait_pipeline(
        &self,
        py: Python<'_>,
        group_id: &str,
        timeout: Option<f64>,
        check: bool,
    ) -> PyResult<PythonStatus> {
        self.wait_operation(py, group_id, timeout, check, "pipeline_status")
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
        let value = py
            .detach(|| {
                rt.block_on(
                    self.client
                        .cancel(group_id, job_id, reason, include_running),
                )
            })
            .map_err(PyRuntimeError::new_err)?;
        json_object(py, value)
    }
    #[pyo3(signature=(*,memory_bytes,cpus=1,token=None,timeout=None))]
    fn host(
        &self,
        memory_bytes: u64,
        cpus: usize,
        token: Option<String>,
        timeout: Option<f64>,
    ) -> PyResult<PythonReservation> {
        if memory_bytes == 0 || cpus == 0 {
            return Err(PyValueError::new_err(
                "positive host memory and CPU count required",
            ));
        }
        let token = token.unwrap_or_else(|| {
            format!(
                "host-{}-{}",
                std::process::id(),
                crate::protocol::now_unix().to_bits()
            )
        });
        super::model::identifier(&token).map_err(PyValueError::new_err)?;
        Ok(PythonReservation {
            client: self.client.clone(),
            token,
            memory_bytes,
            count: cpus,
            timeout: timeout.map(seconds).transpose()?,
            cpus: Vec::new(),
            original: Vec::new(),
            owner: std::process::id(),
            active: false,
        })
    }
}
impl PythonQueue {
    fn wait_operation(
        &self,
        py: Python<'_>,
        group_id: &str,
        timeout: Option<f64>,
        check: bool,
        operation: &str,
    ) -> PyResult<PythonStatus> {
        let timeout = timeout.map(seconds).transpose()?;
        let rt = runtime()?;
        let value = py.detach(|| {
            rt.block_on(async {
                let start = Instant::now();
                loop {
                    Python::attach(|py| py.check_signals())?;
                    let request = self
                        .client
                        .request(json!({"op":operation,"group_id":group_id}));
                    let status = if let Some(limit) = timeout {
                        tokio::time::timeout(limit.saturating_sub(start.elapsed()), request)
                            .await
                            .map_err(|_| {
                                PyTimeoutError::new_err(
                                    "compilation wait timed out; jobs are still in custody",
                                )
                            })?
                    } else {
                        request.await
                    }
                    .map_err(PyRuntimeError::new_err)?;
                    if status["complete"] == true {
                        return Ok(status);
                    }
                    if timeout.is_some_and(|limit| start.elapsed() >= limit) {
                        return Err(PyTimeoutError::new_err(
                            "compilation wait timed out; jobs are still in custody",
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        })?;
        let status = PythonStatus::decode(value)?;
        if check {
            status.check(py)?;
        }
        Ok(status)
    }
}

fn affinity() -> PyResult<Vec<usize>> {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    if unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&set), &mut set) } != 0 {
        return Err(PyRuntimeError::new_err(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    Ok((0..libc::CPU_SETSIZE as usize)
        .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &set) })
        .collect())
}
fn set_affinity(cpus: &[usize]) -> PyResult<()> {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    for &cpu in cpus {
        if cpu >= libc::CPU_SETSIZE as usize {
            return Err(PyRuntimeError::new_err("invalid core in compiler response"));
        }
        unsafe {
            libc::CPU_SET(cpu, &mut set);
        }
    }
    if unsafe { libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set) } != 0 {
        return Err(PyRuntimeError::new_err(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    Ok(())
}
#[pyclass(name = "HostReservation", module = "gpu_queue._native")]
pub struct PythonReservation {
    client: Client,
    token: String,
    memory_bytes: u64,
    count: usize,
    timeout: Option<Duration>,
    cpus: Vec<usize>,
    original: Vec<usize>,
    owner: u32,
    active: bool,
}
#[pymethods]
impl PythonReservation {
    #[getter]
    fn cpus(&self) -> Vec<usize> {
        self.cpus.clone()
    }
    #[getter]
    fn token(&self) -> &str {
        &self.token
    }
    fn __enter__<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        if slf.active || slf.owner != std::process::id() {
            return Err(PyRuntimeError::new_err(
                "host reservation already entered or inherited",
            ));
        }
        slf.original = affinity()?;
        let request = json!({"op":"reserve_host","token":slf.token,"memory_bytes":slf.memory_bytes,"cpus":slf.count,"affinity":slf.original});
        let client = slf.client.clone();
        let timeout = slf.timeout;
        let rt = runtime()?;
        let value = py.detach(|| {
            rt.block_on(async {
                let start = Instant::now();
                loop {
                    Python::attach(|py| py.check_signals())?;
                    let request = client.request(request.clone());
                    let value = if let Some(limit) = timeout {
                        tokio::time::timeout(limit.saturating_sub(start.elapsed()), request)
                            .await
                            .map_err(|_| PyTimeoutError::new_err("host reservation timed out"))?
                    } else {
                        request.await
                    }
                    .map_err(PyRuntimeError::new_err)?;
                    if value["admitted"] == true {
                        return Ok(value);
                    }
                    if timeout.is_some_and(|limit| start.elapsed() >= limit) {
                        return Err(PyTimeoutError::new_err("host reservation timed out"));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        });
        let value = match value {
            Ok(value) => value,
            Err(error) => {
                // A timed-out response may already have committed a reservation.
                let token = slf.token.clone();
                let _ = py.detach(|| {
                    rt.block_on(client.request(json!({"op":"release_host","token":token})))
                });
                return Err(error);
            }
        };
        slf.cpus = value["cpus"]
            .as_array()
            .ok_or_else(|| PyRuntimeError::new_err("missing host cores"))?
            .iter()
            .map(|c| {
                c.as_u64()
                    .map(|v| v as usize)
                    .ok_or_else(|| PyRuntimeError::new_err("invalid host core"))
            })
            .collect::<PyResult<_>>()?;
        slf.active = true;
        if let Err(error) = set_affinity(&slf.cpus) {
            let _ = slf.release(py);
            return Err(error);
        }
        Ok(slf)
    }
    fn release(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.owner != std::process::id() {
            return Err(PyRuntimeError::new_err(
                "host reservation belongs to its original process",
            ));
        }
        if !self.active {
            return Ok(());
        }
        let restored = set_affinity(&self.original);
        let rt = runtime()?;
        let result = py
            .detach(|| {
                rt.block_on(
                    self.client
                        .request(json!({"op":"release_host","token":self.token})),
                )
            })
            .map_err(PyRuntimeError::new_err);
        if result.is_ok() {
            self.active = false;
        }
        restored?;
        result?;
        Ok(())
    }
    fn __exit__(
        &mut self,
        py: Python<'_>,
        _kind: Py<PyAny>,
        _value: Py<PyAny>,
        _traceback: Py<PyAny>,
    ) -> PyResult<bool> {
        self.release(py)?;
        Ok(false)
    }
}
impl Drop for PythonReservation {
    fn drop(&mut self) {
        if self.active && self.owner == std::process::id() {
            Python::try_attach(|py| {
                let _ = self.release(py);
            });
        }
    }
}
pub fn register(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PythonPipeline>()?;
    module.add_class::<PythonJob>()?;
    module.add_class::<PythonGroup>()?;
    module.add_class::<PythonResult>()?;
    module.add_class::<PythonStatus>()?;
    module.add_class::<PythonQueue>()?;
    module.add_class::<PythonReservation>()?;
    module.add("CompilationFailed", py.get_type::<CompilationFailed>())?;
    Ok(())
}
