//! PyO3 owns Python delivery and cleanup callbacks only. Rust owns admission,
//! failure classification, shutdown deadlines, acknowledgement and escalation.
//! SIGUSR1 schedules the typed interruption on Python's main thread. The context
//! manager runs cleanup as the interruption unwinds; a catching caller can call
//! run_shutdown_handler or shutdown_started explicitly without renewing a grant.
use crate::protocol::{LeaseRequest, Mode, ShutdownRequest};
use pyo3::class::gc::{PyTraverseError, PyVisit};
use pyo3::exceptions::{PyKeyboardInterrupt, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pyo3::create_exception!(_native, GpuLeaseInterrupted, PyKeyboardInterrupt);
type Pending = Arc<Mutex<Option<ShutdownRequest>>>;
static ACTIVE: Mutex<Option<Pending>> = Mutex::new(None);

fn interrupted(py: Python<'_>, request: &ShutdownRequest) -> PyResult<PyErr> {
    let error = GpuLeaseInterrupted::new_err(request.reason.clone());
    let value = error.value(py);
    value.setattr("reason", &request.reason)?;
    value.setattr("request_id", &request.request_id)?;
    value.setattr("deadline", request.deadline)?;
    value.setattr("permanent", request.permanent)?;
    Ok(error)
}

#[pyfunction]
fn _interrupt(py: Python<'_>, _signum: i32, _frame: Py<PyAny>) -> PyResult<()> {
    let active = ACTIVE.lock().unwrap_or_else(|e| e.into_inner()).clone();
    if let Some(active) = active {
        let request = active.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(request) = request {
            return Err(interrupted(py, &request)?);
        }
    }
    Ok(())
}

#[pyclass(name = "Lease", module = "gpu_queue._native", weakref)]
struct PythonLease {
    socket: PathBuf,
    request: LeaseRequest,
    native: Option<crate::lease::Lease>,
    pending: Pending,
    handler: Option<Py<PyAny>>,
    old_signal: Option<Py<PyAny>>,
    cleanup_ran: bool,
    independent_directory: Option<PathBuf>,
    daemon_executable: PathBuf,
    nvidia_smi: PathBuf,
}

#[pymethods]
impl PythonLease {
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.handler)?;
        visit.call(&self.old_signal)
    }

    fn __clear__(&mut self) {
        self.handler = None;
    }

    #[new]
    #[pyo3(signature = (*, mode="exclusive", vram=0, expected_runtime_secs=None, socket="/run/gpu-switch/host.sock", label="", priority=0, group=None, cooperative=false, shutdown_grace_secs=30.0, shutdown_handler=None, independent=false, runtime_dir="", daemon_executable="gpu-switch", nvidia_smi="nvidia-smi"))]
    fn new(
        py: Python<'_>,
        mode: &str,
        vram: u64,
        expected_runtime_secs: Option<i64>,
        socket: &str,
        label: &str,
        priority: i8,
        group: Option<String>,
        cooperative: bool,
        shutdown_grace_secs: f64,
        shutdown_handler: Option<Py<PyAny>>,
        independent: bool,
        runtime_dir: &str,
        daemon_executable: &str,
        nvidia_smi: &str,
    ) -> PyResult<Py<Self>> {
        let mode = match mode {
            "exclusive" => Mode::Exclusive,
            "shared" => Mode::Shared,
            _ => return Err(PyValueError::new_err("mode must be exclusive or shared")),
        };
        crate::estimate::validate(expected_runtime_secs).map_err(PyValueError::new_err)?;
        if (mode == Mode::Shared) != (vram > 0) {
            return Err(PyValueError::new_err(
                "shared leases require vram > 0; exclusive leases require vram = 0",
            ));
        }
        if !shutdown_grace_secs.is_finite() || !(0.0..=60.0).contains(&shutdown_grace_secs) {
            return Err(PyValueError::new_err(
                "shutdown grace must be 0..60 seconds",
            ));
        }
        let lease = Py::new(
            py,
            Self {
                socket: socket.into(),
                request: LeaseRequest {
                    expected_runtime_secs,
                    mode,
                    vram: Some(vram),
                    label: Some(label.into()),
                    priority,
                    group,
                    requeued: false,
                    cooperative,
                    resume: None,
                    shutdown_grace_secs: Some(shutdown_grace_secs),
                },
                native: None,
                pending: Arc::new(Mutex::new(None)),
                handler: shutdown_handler,
                old_signal: None,
                cleanup_ran: false,
                independent_directory: independent.then(|| {
                    if runtime_dir.is_empty() {
                        std::env::temp_dir()
                            .join(format!("gpu-queue-{}", unsafe { libc::getuid() }))
                    } else {
                        PathBuf::from(runtime_dir)
                    }
                }),
                daemon_executable: option_env!("GPU_QUEUE_DAEMON")
                    .filter(|_| daemon_executable == "gpu-switch")
                    .unwrap_or(daemon_executable)
                    .into(),
                nvidia_smi: nvidia_smi.into(),
            },
        )?;
        py.import("gpu_queue._native")?
            .getattr("_leases")?
            .call_method1("add", (&lease,))?;
        Ok(lease)
    }

    fn acquire(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.native.is_some() {
            return Err(PyRuntimeError::new_err("lease already acquired"));
        }
        *self.pending.lock().unwrap_or_else(|e| e.into_inner()) = None;
        {
            let mut active = ACTIVE.lock().unwrap_or_else(|e| e.into_inner());
            if active.is_some() {
                return Err(PyRuntimeError::new_err("one Python GPU lease per process"));
            }
            *active = Some(self.pending.clone());
        }
        let signal = py.import("signal")?;
        let callback = py.import("gpu_queue._native")?.getattr("_interrupt")?;
        match signal.call_method1("signal", (libc::SIGUSR1, callback)) {
            Ok(old) => self.old_signal = Some(old.unbind()),
            Err(e) => {
                *ACTIVE.lock().unwrap_or_else(|e| e.into_inner()) = None;
                return Err(e);
            }
        }
        let pending = self.pending.clone();
        let request = self.request.clone();
        let socket = self.socket.clone();
        let independent = self.independent_directory.clone();
        let executable = self.daemon_executable.clone();
        let nvidia_smi = self.nvidia_smi.clone();
        let interruption = Arc::new(Mutex::new(None));
        let caught = interruption.clone();
        let result = py.detach(move || {
            let socket = match independent {
                Some(directory) => {
                    crate::independent::socket(&directory, &executable, &nvidia_smi)?
                }
                None => socket,
            };
            crate::lease::Lease::acquire_checked(
                &socket,
                request,
                move |request| {
                    *pending.lock().unwrap_or_else(|e| e.into_inner()) = Some(request);
                    unsafe {
                        libc::kill(std::process::id() as i32, libc::SIGUSR1);
                    }
                },
                move || {
                    Python::attach(|py| match py.check_signals() {
                        Ok(()) => Ok(()),
                        Err(error) => {
                            *caught.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
                            Err("Python interrupted while queued".into())
                        }
                    })
                },
            )
        });
        match result {
            Ok(native) => {
                self.native = Some(native);
                self.cleanup_ran = false;
                Ok(())
            }
            Err(error) => {
                self.restore_signal(py)?;
                Err(interruption
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                    .unwrap_or_else(|| PyRuntimeError::new_err(error)))
            }
        }
    }

    fn __enter__<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.acquire(py)?;
        Ok(slf)
    }

    fn shutdown_started(&self, request_id: &str) -> PyResult<()> {
        self.native
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("no active lease"))?
            .shutdown_started(request_id)
            .map_err(PyRuntimeError::new_err)
    }

    fn inherited_close(&mut self, py: Python<'_>) -> PyResult<()> {
        if self
            .native
            .as_ref()
            .is_some_and(|native| native.check_owner().is_ok())
        {
            return Err(PyRuntimeError::new_err(
                "inherited_close is only valid in a forked child",
            ));
        }
        if let Some(native) = self.native.take() {
            native.inherited_close().map_err(PyRuntimeError::new_err)?;
            self.restore_signal(py)?;
            self.pending = Arc::new(Mutex::new(None));
        }
        Ok(())
    }

    fn run_shutdown_handler(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let cleanup = {
            let mut this = slf.borrow_mut();
            let request = this
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            match request.filter(|_| !this.cleanup_ran) {
                Some(request) => {
                    this.shutdown_started(&request.request_id)?;
                    this.cleanup_ran = true;
                    this.handler
                        .as_ref()
                        .map(|handler| (handler.clone_ref(py), request))
                }
                None => None,
            }
        };
        if let Some((handler, request)) = cleanup {
            let error = interrupted(py, &request)?;
            handler.call1(py, (error.value(py),))?;
        }
        Ok(())
    }

    fn release(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let cleanup = Self::run_shutdown_handler(slf, py);
        let native = slf.borrow_mut().native.take();
        if let Some(mut native) = native {
            py.detach(move || native.release());
        }
        slf.borrow_mut().restore_signal(py)?;
        cleanup
    }

    fn __exit__(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        _kind: Py<PyAny>,
        _value: Py<PyAny>,
        _traceback: Py<PyAny>,
    ) -> PyResult<bool> {
        if !_value.bind(py).is_none() && !_value.bind(py).is_instance_of::<GpuLeaseInterrupted>() {
            if let Some(native) = &slf.borrow().native {
                native
                    .fail(_value.bind(py).str()?.to_string())
                    .map_err(PyRuntimeError::new_err)?;
            }
        }
        Self::release(slf, py)?;
        Ok(false)
    }
}

impl PythonLease {
    fn restore_signal(&mut self, py: Python<'_>) -> PyResult<()> {
        {
            let mut active = ACTIVE.lock().unwrap_or_else(|e| e.into_inner());
            if active
                .as_ref()
                .is_some_and(|pending| Arc::ptr_eq(pending, &self.pending))
            {
                *active = None;
            }
        }
        if let Some(old) = self.old_signal.take() {
            py.import("signal")?
                .call_method1("signal", (libc::SIGUSR1, old))?;
        }
        Ok(())
    }
}

impl Drop for PythonLease {
    fn drop(&mut self) {
        if let Some(mut native) = self.native.take() {
            native.release();
        }
        if self.old_signal.is_some() {
            Python::try_attach(|py| {
                let _ = self.restore_signal(py);
            });
        }
    }
}

#[pymodule]
fn _native(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PythonLease>()?;
    module.add("GpuLeaseInterrupted", py.get_type::<GpuLeaseInterrupted>())?;
    module.add_function(wrap_pyfunction!(_interrupt, module)?)?;
    module.add(
        "_leases",
        py.import("weakref")?.getattr("WeakSet")?.call0()?,
    )?;
    module.add_function(wrap_pyfunction!(_after_fork, module)?)?;
    let kwargs = pyo3::types::PyDict::new(py);
    kwargs.set_item("after_in_child", module.getattr("_after_fork")?)?;
    py.import("os")?
        .call_method("register_at_fork", (), Some(&kwargs))?;
    Ok(())
}

#[pyfunction]
fn _after_fork(py: Python<'_>) -> PyResult<()> {
    let leases = py.import("gpu_queue._native")?.getattr("_leases")?;
    for lease in leases.try_iter()? {
        lease?.call_method0("inherited_close")?;
    }
    Ok(())
}
