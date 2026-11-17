//! PyO3 owns a native RAM claim handle, never the admission policy.
use super::{
    client::{Client, Failure},
    memory_client::{Handover, HostMemoryClaim, LARGE_REQUEST},
    party,
    python::{interruptible, runtime},
};
use crate::python::AuthorityUnavailable;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

/// An outage of the compile service is AuthorityUnavailable, never a verdict; its refusals and invalid
/// operations are RuntimeError.
fn raised(failure: Failure) -> PyErr {
    match failure {
        Failure::Unavailable(outage) => AuthorityUnavailable::new_err(outage.reason),
        Failure::Refused(reason) | Failure::Invalid(reason) => PyRuntimeError::new_err(reason),
    }
}

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
        memory_bytes: u64,
        state_dir: Option<String>,
        label: &str,
        priority: i64,
    ) -> PyResult<Self> {
        let priority = party::bounded(priority).map_err(PyValueError::new_err)?;
        let state = state_dir.map_or_else(super::default_state, std::path::PathBuf::from);
        let client = Client::new(&state)
            .map_err(|failure| PyRuntimeError::new_err(failure.to_string()))?;
        let claim = HostMemoryClaim::new(client, memory_bytes, label.into(), priority, None)
            .map_err(PyValueError::new_err)?;
        Ok(Self { claim })
    }
    /// The granted claim a `host-claim` front-end handed to this process through `HOST_MEMORY_CLAIM`.
    #[staticmethod]
    fn inherited(py: Python<'_>) -> PyResult<Self> {
        let handover = Handover::of_environment().map_err(PyValueError::new_err)?;
        let runtime = runtime()?;
        let claim = py.detach(|| {
            runtime.block_on(interruptible(async {
                HostMemoryClaim::inherited(&handover).await.map_err(raised)
            }))
        })?;
        Ok(Self { claim })
    }
    /// Gives the claim to `pid`, a live descendant that owns no claim; this process holds it no longer.
    fn transfer(&mut self, py: Python<'_>, pid: u32) -> PyResult<()> {
        let runtime = runtime()?;
        let claim = &mut self.claim;
        py.detach(|| {
            runtime.block_on(interruptible(async {
                claim.transfer(pid).await.map_err(raised)
            }))
        })
    }
    #[getter]
    fn memory_bytes(&self) -> u64 {
        self.claim.memory_bytes()
    }
    #[getter]
    fn claim_id(&self) -> &str {
        self.claim.id()
    }
    fn acquire(&mut self, py: Python<'_>) -> PyResult<()> {
        let runtime = runtime()?;
        let registered = py.detach(|| {
            runtime.block_on(interruptible(async {
                self.claim.register().await.map_err(raised)
            }))
        });
        let result = registered
            .and_then(|status| super::python::warn_each(py, &status["warnings"]))
            .and_then(|()| {
                py.detach(|| {
                    runtime.block_on(interruptible(async {
                        self.claim.acquire().await.map_err(raised)
                    }))
                })
            });
        if result.is_err() {
            let _ = py.detach(|| runtime.block_on(self.claim.release()));
        }
        result
    }
    fn track_worker(&self, py: Python<'_>, pid: u32) -> PyResult<()> {
        let runtime = runtime()?;
        py.detach(|| runtime.block_on(self.claim.track_worker(pid)))
            .map_err(raised)
    }
    /// Returns what admission observed of the claim, or None for a claim never registered.
    fn release(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let runtime = runtime()?;
        py.detach(|| runtime.block_on(self.claim.release()))
            .map_err(raised)?
            .map(|answer| super::python::json_object(py, answer))
            .transpose()
    }
    fn __enter__<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.acquire(py)?;
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
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("LARGE_REQUEST", LARGE_REQUEST)?;
    module.add_class::<PythonMemory>()
}
