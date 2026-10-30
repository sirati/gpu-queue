//! PyO3 owns a native RAM claim handle, never the admission policy.
use super::{client::Client, memory_client::HostMemoryClaim, party, python::awaited};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::path::Path;

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
        let state = state_dir
            .unwrap_or_else(|| format!("/var/tmp/rust-compile-{}", unsafe { libc::getuid() }));
        let client = Client::new(Path::new(&state), None).map_err(PyRuntimeError::new_err)?;
        let claim = HostMemoryClaim::new(client, memory_bytes, label.into(), priority)
            .map_err(PyValueError::new_err)?;
        Ok(Self { claim })
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
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let result = py.detach(|| runtime.block_on(awaited(self.claim.acquire())));
        if result.is_err() {
            let _ = py.detach(|| runtime.block_on(self.claim.release()));
        }
        result
    }
    fn track_worker(&self, py: Python<'_>, pid: u32) -> PyResult<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        py.detach(|| runtime.block_on(self.claim.track_worker(pid)))
            .map_err(PyRuntimeError::new_err)
    }
    fn release(&mut self, py: Python<'_>) -> PyResult<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        py.detach(|| runtime.block_on(self.claim.release()))
            .map_err(PyRuntimeError::new_err)
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
    module.add_class::<PythonMemory>()
}
