//! PyO3 owns a native RAM claim handle, never the admission policy.
use super::{
    client::{CancelToken, Client},
    memory_client::{Handover, HostMemoryClaim, LARGE_REQUEST},
    party,
    python::{answered, raised, runtime, PythonCancel},
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// A pending claim stays queued until granted or its owner is interrupted.
#[pyclass(name = "HostMemoryClaim", module = "gpu_queue._native")]
pub struct PythonMemory {
    claim: HostMemoryClaim,
}
#[pymethods]
impl PythonMemory {
    #[new]
    #[pyo3(signature=(*,memory_bytes,state_dir=None,label="",priority=0))]
    fn new(
        py: Python<'_>,
        memory_bytes: u64,
        state_dir: Option<String>,
        label: &str,
        priority: i64,
    ) -> PyResult<Self> {
        let priority = party::bounded(priority).map_err(PyValueError::new_err)?;
        let state = state_dir.map_or_else(super::default_state, std::path::PathBuf::from);
        let client = Client::new(&state).map_err(|failure| raised(py, failure))?;
        let claim = HostMemoryClaim::new(client, memory_bytes, label.into(), priority, None)
            .map_err(PyValueError::new_err)?;
        Ok(Self { claim })
    }
    /// The granted claim a `host-claim` front-end handed to this process through `HOST_MEMORY_CLAIM`.
    #[staticmethod]
    fn inherited(py: Python<'_>) -> PyResult<Self> {
        let handover = Handover::of_environment().map_err(PyValueError::new_err)?;
        let runtime = runtime()?;
        let claim = py.detach(|| runtime.block_on(answered(HostMemoryClaim::inherited(&handover))))?;
        Ok(Self { claim })
    }
    /// Gives the claim to `pid`, a live descendant that owns no claim; this process holds it no longer.
    fn transfer(&mut self, py: Python<'_>, pid: u32) -> PyResult<()> {
        let runtime = runtime()?;
        let claim = &mut self.claim;
        py.detach(|| runtime.block_on(answered(claim.transfer(pid))))
    }
    #[getter]
    fn memory_bytes(&self) -> u64 {
        self.claim.memory_bytes()
    }
    #[getter]
    fn claim_id(&self) -> &str {
        self.claim.id()
    }
    /// Registers the claim and waits for its grant. Cancelling `token` ends the registration or the wait
    /// locally with CancelledError.
    fn acquire(&mut self, py: Python<'_>, token: &Bound<'_, PythonCancel>) -> PyResult<()> {
        self.acquired(py, &token.get().latch())
    }
    fn track_worker(&self, py: Python<'_>, pid: u32) -> PyResult<()> {
        let runtime = runtime()?;
        py.detach(|| runtime.block_on(self.claim.track_worker(pid)))
            .map_err(|failure| raised(py, failure))
    }
    /// Returns what admission observed of the claim, or None for a claim never registered.
    fn release(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let runtime = runtime()?;
        py.detach(|| runtime.block_on(self.claim.release()))
            .map_err(|failure| raised(py, failure))?
            .map(|answer| super::python::json_object(py, answer))
            .transpose()
    }
    /// Acquires with a token of its own that nothing cancels.
    fn __enter__<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.acquired(py, &CancelToken::new())?;
        Ok(slf)
    }
    fn __exit__(
        &mut self,
        py: Python<'_>,
        _kind: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _trace: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        self.release(py)?;
        Ok(false)
    }
}
impl PythonMemory {
    /// Registration and the wait for the grant, both ended by `cancel`. Any failure releases the claim within
    /// its cleanup deadline and raises the original error.
    fn acquired(&mut self, py: Python<'_>, cancel: &CancelToken) -> PyResult<()> {
        let runtime = runtime()?;
        let claim = &mut self.claim;
        let registered = py.detach(|| runtime.block_on(answered(cancel.or_cancelled(claim.register()))));
        let result = registered
            .and_then(|status| super::python::warn_each(py, &status["warnings"]))
            .and_then(|()| py.detach(|| runtime.block_on(answered(claim.acquire(cancel)))));
        if result.is_err() {
            let _ = py.detach(|| runtime.block_on(claim.release()));
        }
        result
    }
}
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("LARGE_REQUEST", LARGE_REQUEST)?;
    module.add_class::<PythonMemory>()
}
