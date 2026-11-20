//! The card, read through NVML in this process: its processes and their memory, which of them compute now, its
//! memory and the figures a viewer is shown. NVML keeps one utilisation sample per process, its last one with any
//! use, dated in microseconds since the epoch; while a process computes, the driver renews it every 100 to 300 ms.
//! Every failed NVML call answers a typed [`Failure`], classified from nvml-wrapper's error enum and naming the
//! call; no caller sees an `NvmlError`.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nvml_wrapper::enum_wrappers::device::TemperatureSensor;
use nvml_wrapper::enums::device::UsedGpuMemory;
use nvml_wrapper::error::NvmlError;
use nvml_wrapper::struct_wrappers::device::ProcessInfo;
use nvml_wrapper::Device;

use crate::protocol::Card;

/// How recent a process's utilisation sample must be for the process to compute now: a second spans several of
/// the driver's renewals of a computing process's sample.
const RECENT: Duration = Duration::from_secs(1);

const MIB: u64 = 1 << 20;

/// `NVML_ERROR_GPU_NOT_FOUND` (nvml.h 13.4 :1543 range, code 28), which nvml-wrapper 0.13 maps to
/// `UnexpectedVariant`.
const GPU_NOT_FOUND: u32 = 28;

/// What a failed read says about the card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    /// The device, or the driver stack every grant runs on, is gone or unusable.
    Lost(String),
    /// This read got no answer; it says nothing about the device.
    Unanswered(String),
}

impl Failure {
    pub fn text(&self) -> &str {
        match self {
            Failure::Lost(text) | Failure::Unanswered(text) => text,
        }
    }
}

/// The class of `error`, returned by NVML's `call`, whose name the text carries. The match is exhaustive and
/// without a wildcard: nvml-wrapper's enum is not `#[non_exhaustive]`, so a new variant fails the build.
#[allow(deprecated)]
pub(crate) fn classify(call: &str, error: &NvmlError) -> Failure {
    let text = format!("{call}: NVML: {error}");
    match error {
        NvmlError::GpuLost
        | NvmlError::ResetRequired
        | NvmlError::UnexpectedVariant(GPU_NOT_FOUND)
        | NvmlError::DriverNotLoaded
        | NvmlError::LibRmVersionMismatch
        | NvmlError::IrqIssue
        | NvmlError::InsufficientPower
        | NvmlError::LibloadingError(_)
        | NvmlError::FailedToLoadSymbol(_)
        | NvmlError::LibraryNotFound
        | NvmlError::FunctionNotFound => Failure::Lost(text),
        NvmlError::Unknown
        | NvmlError::Timeout
        | NvmlError::InUse
        | NvmlError::InsufficientMemory
        | NvmlError::NoData
        | NvmlError::UnexpectedVariant(_)
        | NvmlError::Uninitialized
        | NvmlError::InvalidArg
        | NvmlError::NotSupported
        | NvmlError::NoPermission
        | NvmlError::NotFound
        | NvmlError::InsufficientSize(_)
        | NvmlError::CorruptedInfoROM
        | NvmlError::OperatingSystem
        | NvmlError::VgpuEccNotSupported
        | NvmlError::AlreadyInitialized
        | NvmlError::Utf8Error(_)
        | NvmlError::NulError(_)
        | NvmlError::StringTooLong { .. }
        | NvmlError::IncorrectBits(_)
        | NvmlError::SetReleaseFailed
        | NvmlError::GetPciInfoFailed
        | NvmlError::PciInfoToCFailed => Failure::Unanswered(text),
    }
}

/// The classifier of NVML's `call`, for `map_err`.
fn failed(call: &str) -> impl Fn(NvmlError) -> Failure + '_ {
    move |error| classify(call, &error)
}

/// What opens the card: NVML from the driver's library, or a test's script.
pub trait Driver: Send + Sync {
    fn open(&self) -> Result<Box<dyn Reader>, Failure>;
    /// A test's fault point inside the card reader's completion bookkeeping; nothing outside a test's script.
    #[cfg(test)]
    fn fault(&self) {}
    /// A test's completion pause: the card reader's `call` ("sample" or "compute") has its complete answer for
    /// read `read`, `failed` or not, and its caller has not applied it. Nothing outside a test's script.
    #[cfg(test)]
    fn completed(
        &self,
        _call: &'static str,
        _read: u64,
        _failed: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>> {
        Box::pin(std::future::ready(()))
    }
}

/// One open handle on the card.
pub trait Reader: Send + Sync {
    fn sample(&self) -> Result<Sample, Failure>;
    /// Every device's compute processes with their memory.
    fn compute(&self) -> Result<Vec<(u32, Option<u64>)>, Failure>;
    /// Closes the handle and answers whether NVML's shutdown succeeded, which a drop would ignore.
    fn close(self: Box<Self>) -> Result<(), Failure>;
}

/// NVML from the driver's library (`libnvidia-ml.so.1`).
pub struct Nvml {
    library: PathBuf,
}

impl Nvml {
    pub fn new(library: PathBuf) -> Nvml {
        Nvml { library }
    }
}

impl Driver for Nvml {
    /// Every failure of NVML's initialization is a loss: a configured NVML that cannot open can never answer.
    fn open(&self) -> Result<Box<dyn Reader>, Failure> {
        nvml_wrapper::Nvml::builder()
            .lib_path(self.library.as_os_str())
            .init()
            .map(|nvml| Box::new(Handle { nvml }) as Box<dyn Reader>)
            .map_err(|error| {
                Failure::Lost(format!(
                    "nvmlInit_v2: NVML from {}: {error}",
                    self.library.display()
                ))
            })
    }
}

/// An initialized NVML.
struct Handle {
    nvml: nvml_wrapper::Nvml,
}

/// The devices of one read: never empty.
struct Devices<'nvml>(Vec<Device<'nvml>>);

impl<'nvml> Devices<'nvml> {
    fn first(&self) -> &Device<'nvml> {
        &self.0[0]
    }
}

/// The devices a sample read: the sorted set of their NVML UUIDs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Identity(Vec<String>);

impl Identity {
    pub fn of(mut uuids: Vec<String>) -> Identity {
        uuids.sort();
        uuids.dedup();
        Identity(uuids)
    }

    pub fn uuids(&self) -> &[String] {
        &self.0
    }
}

impl std::fmt::Display for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.join(", "))
    }
}

/// Every device at one moment.
#[derive(Debug, Clone, Default)]
pub struct Sample {
    /// Compute processes with their memory in bytes; `None` where the driver does not report it.
    pub compute: Vec<(u32, Option<u64>)>,
    /// Graphics processes with their memory in bytes, as for compute ones.
    pub graphics: Vec<(u32, Option<u64>)>,
    /// The processes whose utilisation sample is recent and shows SM use.
    pub computing: HashSet<u32>,
    /// `(total, free)` bytes of the device with least free memory.
    pub memory: Option<(u64, u64)>,
    /// The devices read.
    pub identity: Identity,
    /// The first device, as a viewer is shown it.
    pub card: Card,
}

impl Reader for Handle {
    fn compute(&self) -> Result<Vec<(u32, Option<u64>)>, Failure> {
        let devices = self.devices()?;
        processes(
            &devices,
            "nvmlDeviceGetComputeRunningProcesses_v3",
            Device::running_compute_processes,
        )
    }

    fn sample(&self) -> Result<Sample, Failure> {
        let devices = self.devices()?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let since = now.saturating_sub(RECENT).as_micros() as u64;
        let mut computing = HashSet::new();
        for device in &devices.0 {
            computing.extend(recently_computing(device, since)?);
        }
        let memory = devices
            .0
            .iter()
            .map(|device| device.memory_info().map_err(failed("nvmlDeviceGetMemoryInfo_v2")))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .min_by_key(|memory| memory.free)
            .map(|memory| (memory.total, memory.free));
        let identity = Identity::of(
            devices
                .0
                .iter()
                .map(|device| device.uuid().map_err(failed("nvmlDeviceGetUUID")))
                .collect::<Result<_, _>>()?,
        );
        Ok(Sample {
            compute: processes(
                &devices,
                "nvmlDeviceGetComputeRunningProcesses_v3",
                Device::running_compute_processes,
            )?,
            graphics: processes(
                &devices,
                "nvmlDeviceGetGraphicsRunningProcesses_v3",
                Device::running_graphics_processes,
            )?,
            computing,
            memory,
            identity,
            card: card(devices.first())?,
        })
    }

    fn close(self: Box<Self>) -> Result<(), Failure> {
        self.nvml.shutdown().map_err(failed("nvmlShutdown"))
    }
}

impl Handle {
    /// The enumeration both reads use. No device on a configured host is a lost card, never an empty sample.
    fn devices(&self) -> Result<Devices<'_>, Failure> {
        let count = self
            .nvml
            .device_count()
            .map_err(failed("nvmlDeviceGetCount_v2"))?;
        if count == 0 {
            return Err(Failure::Lost("NVML reports no GPU".into()));
        }
        (0..count)
            .map(|index| {
                self.nvml
                    .device_by_index(index)
                    .map_err(failed("nvmlDeviceGetHandleByIndex_v2"))
            })
            .collect::<Result<_, _>>()
            .map(Devices)
    }
}

impl Sample {
    /// Every process on the card with its memory in bytes: the compute processes, then the graphics-only ones.
    pub fn processes(&self) -> Vec<(u32, Option<u64>)> {
        let graphics = self
            .graphics
            .iter()
            .filter(|(pid, _)| self.compute.iter().all(|(compute, _)| compute != pid))
            .copied();
        self.compute.iter().copied().chain(graphics).collect()
    }

    /// Whether any of `owned` computes now; another process's compute never counts for them.
    pub fn computes(&self, owned: &HashSet<u32>) -> bool {
        owned.iter().any(|pid| self.computing.contains(pid))
    }
}

/// The processes `list`, NVML's `call`, reports on each device, with their memory.
fn processes<'nvml>(
    devices: &Devices<'nvml>,
    call: &str,
    list: impl Fn(&Device<'nvml>) -> Result<Vec<ProcessInfo>, NvmlError>,
) -> Result<Vec<(u32, Option<u64>)>, Failure> {
    let mut found = Vec::new();
    for device in &devices.0 {
        found.extend(list(device).map_err(failed(call))?.into_iter().map(|process| {
            let memory = match process.used_gpu_memory {
                UsedGpuMemory::Used(bytes) => Some(bytes),
                UsedGpuMemory::Unavailable => None,
            };
            (process.pid, memory)
        }));
    }
    Ok(found)
}

/// The processes on `device` whose utilisation sample is dated after `since` and shows SM use; NVML answers
/// "not found" when no process has one.
fn recently_computing(device: &Device<'_>, since: u64) -> Result<Vec<u32>, Failure> {
    match device.process_utilization_stats(since) {
        Ok(samples) => Ok(samples
            .into_iter()
            .filter(|sample| sample.sm_util > 0)
            .map(|sample| sample.pid)
            .collect()),
        Err(NvmlError::NotFound) => Ok(Vec::new()),
        Err(error) => Err(classify("nvmlDeviceGetProcessUtilization", &error)),
    }
}

/// A figure the panel only shows, never decides on: one the card does not support holds its type's default and
/// is listed by `field` in `unsupported`; any other failure fails the read with its class.
fn shown<T: Default>(
    unsupported: &mut Vec<String>,
    field: &str,
    call: &str,
    answer: Result<T, NvmlError>,
) -> Result<T, Failure> {
    match answer {
        Ok(value) => Ok(value),
        Err(NvmlError::NotSupported) => {
            unsupported.push(field.to_string());
            Ok(T::default())
        }
        Err(error) => Err(classify(call, &error)),
    }
}

fn card(device: &Device<'_>) -> Result<Card, Failure> {
    let memory = device
        .memory_info()
        .map_err(failed("nvmlDeviceGetMemoryInfo_v2"))?;
    let mut unsupported = Vec::new();
    let name = shown(&mut unsupported, "name", "nvmlDeviceGetName", device.name())?;
    let power_draw = shown(
        &mut unsupported,
        "power_draw",
        "nvmlDeviceGetPowerUsage",
        device.power_usage(),
    )?;
    let power_limit = shown(
        &mut unsupported,
        "power_limit",
        "nvmlDeviceGetEnforcedPowerLimit",
        device.enforced_power_limit(),
    )?;
    let temperature = shown(
        &mut unsupported,
        "temperature",
        "nvmlDeviceGetTemperature",
        device.temperature(TemperatureSensor::Gpu),
    )?;
    let utilization = shown(
        &mut unsupported,
        "utilization",
        "nvmlDeviceGetUtilizationRates",
        device.utilization_rates().map(|rates| rates.gpu),
    )?;
    Ok(Card {
        name,
        power_draw: f64::from(power_draw) / 1000.0,
        power_limit: f64::from(power_limit) / 1000.0,
        temperature,
        utilization,
        memory_used_mib: memory.used / MIB,
        memory_total_mib: memory.total / MIB,
        unsupported,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graphics_only_processes_follow_the_compute_ones_each_once() {
        let sample = Sample {
            compute: vec![(100, Some(40 * MIB)), (300, None)],
            graphics: vec![(100, Some(55 * MIB)), (200, Some(8 * MIB)), (400, None)],
            ..Sample::default()
        };
        assert_eq!(
            sample.processes(),
            [
                (100, Some(40 * MIB)),
                (300, None),
                (200, Some(8 * MIB)),
                (400, None)
            ]
        );
    }

    #[test]
    fn another_processs_compute_never_counts_for_an_idle_owner() {
        let sample = Sample {
            computing: HashSet::from([2]),
            ..Sample::default()
        };
        assert!(!sample.computes(&HashSet::from([1])));
        assert!(sample.computes(&HashSet::from([1, 2])));
        assert!(!sample.computes(&HashSet::new()));
    }

    /// Every nvml-wrapper variant with its class (PLAN-L §2.3): lost, else unanswered.
    #[allow(deprecated)]
    fn every_variant() -> Vec<(NvmlError, bool)> {
        let library = nvml_wrapper::Nvml::builder()
            .lib_path(std::ffi::OsStr::new("/nonexistent/libnvidia-ml.so.1"))
            .init()
            .err()
            .expect("a missing library fails to load");
        assert!(matches!(library, NvmlError::LibloadingError(_)), "{library:?}");
        let invalid = vec![0xffu8];
        let utf8 = std::str::from_utf8(&invalid).unwrap_err();
        let nul = std::ffi::CString::new(vec![0u8]).unwrap_err();
        vec![
            (NvmlError::GpuLost, true),
            (NvmlError::ResetRequired, true),
            (NvmlError::DriverNotLoaded, true),
            (NvmlError::LibRmVersionMismatch, true),
            (NvmlError::IrqIssue, true),
            (NvmlError::InsufficientPower, true),
            (library, true),
            (NvmlError::FailedToLoadSymbol("nvmlDeviceGetUUID".into()), true),
            (NvmlError::LibraryNotFound, true),
            (NvmlError::FunctionNotFound, true),
            (NvmlError::Unknown, false),
            (NvmlError::Timeout, false),
            (NvmlError::InUse, false),
            (NvmlError::InsufficientMemory, false),
            (NvmlError::NoData, false),
            (NvmlError::Uninitialized, false),
            (NvmlError::InvalidArg, false),
            (NvmlError::NotSupported, false),
            (NvmlError::NoPermission, false),
            (NvmlError::NotFound, false),
            (NvmlError::InsufficientSize(Some(5)), false),
            (NvmlError::InsufficientSize(None), false),
            (NvmlError::CorruptedInfoROM, false),
            (NvmlError::OperatingSystem, false),
            (NvmlError::VgpuEccNotSupported, false),
            (NvmlError::AlreadyInitialized, false),
            (NvmlError::Utf8Error(utf8), false),
            (NvmlError::NulError(nul), false),
            (NvmlError::StringTooLong { max_len: 1, actual_len: 2 }, false),
            (NvmlError::IncorrectBits(nvml_wrapper::error::Bits::U32(0)), false),
            (NvmlError::SetReleaseFailed, false),
            (NvmlError::GetPciInfoFailed, false),
            (NvmlError::PciInfoToCFailed, false),
        ]
    }

    #[test]
    fn every_nvml_error_has_its_class_and_its_text_names_the_call() {
        let mut cases = every_variant();
        cases.extend((23..=30).map(|code| (NvmlError::UnexpectedVariant(code), code == GPU_NOT_FOUND)));
        cases.extend([(NvmlError::UnexpectedVariant(31), false), (NvmlError::UnexpectedVariant(12345), false)]);
        for (error, lost) in cases {
            let failure = classify("nvmlDeviceGetHandleByIndex_v2", &error);
            assert_eq!(matches!(failure, Failure::Lost(_)), lost, "{error:?}");
            assert_eq!(failure.text(), format!("nvmlDeviceGetHandleByIndex_v2: NVML: {error}"));
        }
        assert_eq!(
            classify("nvmlDeviceGetComputeRunningProcesses_v3", &NvmlError::GpuLost),
            Failure::Lost(
                "nvmlDeviceGetComputeRunningProcesses_v3: NVML: device fell off the bus or has otherwise become \
                 inacessible"
                    .into()
            ),
            "the text observed tonight"
        );
    }

    #[test]
    fn the_real_reader_answers_the_mock_cards_failures_with_their_class() {
        let dir = crate::switch::control::tests::StateDir::new();
        let role = crate::roles::Role::start(
            "gpu::tests::the_real_reader_on_the_mock",
            &[("NINFER_MOCK_GPU_DIR", dir.path().as_os_str())],
        );
        assert!(role.wait().success());
    }

    /// The role of the test above, in its own process: the mock's control directory is process-wide.
    #[test]
    #[ignore]
    fn the_real_reader_on_the_mock() {
        let Some(dir) = std::env::var_os("NINFER_MOCK_GPU_DIR").map(PathBuf::from) else {
            return;
        };
        let control = |file: &str, text: &str| std::fs::write(dir.join(file), text).unwrap();
        let clear = |file: &str| std::fs::remove_file(dir.join(file)).unwrap();
        let nvml = Nvml::new(crate::roles::nvml_mock());
        let reader = nvml.open().unwrap();
        let healthy = reader.sample().unwrap();
        assert_eq!(healthy.identity.uuids(), ["GPU-00000000-0000-0000-0000-00000000c0de"]);
        assert_eq!((healthy.card.name.as_str(), healthy.card.unsupported.len()), ("Mock GPU", 0));
        for call in [
            "nvmlDeviceGetName",
            "nvmlDeviceGetPowerUsage",
            "nvmlDeviceGetEnforcedPowerLimit",
            "nvmlDeviceGetTemperature",
            "nvmlDeviceGetUtilizationRates",
        ] {
            control("fail", call);
            let failure = reader.sample().unwrap_err();
            assert!(matches!(&failure, Failure::Lost(text) if text.starts_with(call)), "{call}: {failure:?}");
        }
        clear("fail");
        control("absent", "");
        assert_eq!(reader.sample().unwrap_err(), Failure::Lost("NVML reports no GPU".into()));
        assert_eq!(reader.compute().unwrap_err(), Failure::Lost("NVML reports no GPU".into()));
        clear("absent");
        control("unsupported", "nvmlDeviceGetPowerUsage");
        let sample = reader.sample().unwrap();
        assert_eq!((sample.card.unsupported, sample.card.power_draw), (vec!["power_draw".to_string()], 0.0));
        control("unsupported", "nvmlDeviceGetMemoryInfo_v2");
        let failure = reader.sample().unwrap_err();
        assert!(
            matches!(&failure, Failure::Unanswered(text) if text.starts_with("nvmlDeviceGetMemoryInfo_v2")),
            "{failure:?}"
        );
        clear("unsupported");
        control("lost", "");
        let failure = reader.compute().unwrap_err();
        assert!(matches!(&failure, Failure::Lost(text) if text.contains("device fell off the bus")), "{failure:?}");
        clear("lost");
        assert_eq!(reader.close(), Ok(()));
        let reader = nvml.open().unwrap();
        control("fail", "nvmlShutdown");
        let failure = reader.close().unwrap_err();
        assert!(matches!(&failure, Failure::Lost(text) if text.starts_with("nvmlShutdown")), "{failure:?}");
        clear("fail");
        let failure = Nvml::new(dir.join("missing.so")).open().err().expect("no library");
        assert!(matches!(&failure, Failure::Lost(text) if text.starts_with("nvmlInit_v2")), "{failure:?}");
    }
}
