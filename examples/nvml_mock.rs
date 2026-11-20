//! A stand-in for the NVIDIA driver's NVML library in the tests: the calls gpu-switch makes, answered for one
//! device from the fake card `ninfer-mock` keeps in `$NINFER_MOCK_GPU_DIR`. A computing card draws near its cap
//! and an idle one idles; each computing process has a utilisation sample dated now, as the driver renews it.
//! A test makes the card fail through control files in the same directory: `lost`, under which every device call
//! answers GPU_IS_LOST (15); `absent`, under which the count is 0; `fail`, naming one NVML function that answers
//! GPU_IS_LOST; and `unsupported`, naming one that answers NOT_SUPPORTED (3).
#![allow(non_snake_case)]

use std::ffi::{c_char, c_uint, c_ulonglong};
use std::time::{SystemTime, UNIX_EPOCH};

use nvml_wrapper_sys::bindings::{
    nvmlDevice_t, nvmlMemory_v2_t, nvmlProcessInfo_t, nvmlProcessUtilizationSample_t,
    nvmlReturn_enum_NVML_ERROR_INSUFFICIENT_SIZE as INSUFFICIENT_SIZE,
    nvmlReturn_enum_NVML_ERROR_INVALID_ARGUMENT as INVALID_ARGUMENT,
    nvmlReturn_enum_NVML_ERROR_GPU_IS_LOST as GPU_IS_LOST,
    nvmlReturn_enum_NVML_ERROR_NOT_FOUND as NOT_FOUND,
    nvmlReturn_enum_NVML_ERROR_NOT_SUPPORTED as NOT_SUPPORTED, nvmlReturn_enum_NVML_SUCCESS as SUCCESS,
    nvmlReturn_t, nvmlTemperatureSensors_t, nvmlUtilization_t,
};

#[path = "../src/bin/mock/gpu_claims.rs"]
mod gpu_claims;
use gpu_claims::{gpu_state, GpuProc};

const MIB: u64 = 1 << 20;
const NAME: &[u8] = b"Mock GPU\0";
const UUID: &[u8] = b"GPU-00000000-0000-0000-0000-00000000c0de\0";

/// The code a test's control file forces on NVML's `function`, if any.
fn forced(function: &str) -> Option<nvmlReturn_t> {
    let dir = gpu_claims::gpu_dir()?;
    let names = |file: &str| std::fs::read_to_string(dir.join(file)).is_ok_and(|text| text.trim() == function);
    let device_call = function.starts_with("nvmlDevice") && function != "nvmlDeviceGetCount_v2";
    [
        (device_call && dir.join("lost").exists(), GPU_IS_LOST),
        (names("fail"), GPU_IS_LOST),
        (names("unsupported"), NOT_SUPPORTED),
    ]
    .into_iter()
    .find_map(|(applies, code)| applies.then_some(code))
}

/// Answers what a control file forces on the enclosing function, before its own answer.
macro_rules! forced {
    ($function:literal) => {
        if let Some(code) = forced($function) {
            return code;
        }
    };
}

/// The one device's handle: not null, never dereferenced.
fn device() -> nvmlDevice_t {
    std::ptr::NonNull::dangling().as_ptr()
}

fn computing(processes: &[(u32, GpuProc)]) -> bool {
    processes.iter().any(|(_, process)| process.sm > 0)
}

/// The processes of `kind` (`C` or `G`) with their memory.
fn running(kind: char) -> Vec<nvmlProcessInfo_t> {
    gpu_state()
        .0
        .into_iter()
        .filter(|(_, process)| process.kind.contains(kind))
        .map(|(pid, process)| nvmlProcessInfo_t {
            pid,
            usedGpuMemory: process.mem * MIB,
            gpuInstanceId: u32::MAX,
            computeInstanceId: u32::MAX,
        })
        .collect()
}

/// `entries` into the caller's buffer of `*count` entries as NVML answers: `empty` when there are none, the count
/// alone when the buffer is missing or too short.
///
/// # Safety
/// `count` points to a count; `buffer` is null or holds `*count` entries.
unsafe fn answer<T>(
    entries: Vec<T>,
    count: *mut c_uint,
    buffer: *mut T,
    empty: nvmlReturn_t,
) -> nvmlReturn_t {
    let room = *count as usize;
    *count = entries.len() as c_uint;
    match (entries.is_empty(), buffer.is_null() || room < entries.len()) {
        (true, _) => empty,
        (false, true) => INSUFFICIENT_SIZE,
        (false, false) => {
            std::ptr::copy_nonoverlapping(entries.as_ptr(), buffer, entries.len());
            SUCCESS
        }
    }
}

#[no_mangle]
pub extern "C" fn nvmlInit_v2() -> nvmlReturn_t {
    forced!("nvmlInit_v2");
    SUCCESS
}

#[no_mangle]
pub extern "C" fn nvmlShutdown() -> nvmlReturn_t {
    forced!("nvmlShutdown");
    SUCCESS
}

/// # Safety
/// `count` points to a count.
#[no_mangle]
pub unsafe extern "C" fn nvmlDeviceGetCount_v2(count: *mut c_uint) -> nvmlReturn_t {
    forced!("nvmlDeviceGetCount_v2");
    *count = match gpu_claims::gpu_dir().is_some_and(|dir| dir.join("absent").exists()) {
        true => 0,
        false => 1,
    };
    SUCCESS
}

/// # Safety
/// `handle` points to a handle.
#[no_mangle]
pub unsafe extern "C" fn nvmlDeviceGetHandleByIndex_v2(
    index: c_uint,
    handle: *mut nvmlDevice_t,
) -> nvmlReturn_t {
    forced!("nvmlDeviceGetHandleByIndex_v2");
    match index {
        0 => {
            *handle = device();
            SUCCESS
        }
        _ => INVALID_ARGUMENT,
    }
}

/// # Safety
/// As `answer`.
#[no_mangle]
pub unsafe extern "C" fn nvmlDeviceGetComputeRunningProcesses_v3(
    _: nvmlDevice_t,
    count: *mut c_uint,
    infos: *mut nvmlProcessInfo_t,
) -> nvmlReturn_t {
    forced!("nvmlDeviceGetComputeRunningProcesses_v3");
    answer(running('C'), count, infos, SUCCESS)
}

/// # Safety
/// As `answer`.
#[no_mangle]
pub unsafe extern "C" fn nvmlDeviceGetGraphicsRunningProcesses_v3(
    _: nvmlDevice_t,
    count: *mut c_uint,
    infos: *mut nvmlProcessInfo_t,
) -> nvmlReturn_t {
    forced!("nvmlDeviceGetGraphicsRunningProcesses_v3");
    answer(running('G'), count, infos, SUCCESS)
}

/// # Safety
/// As `answer`.
#[no_mangle]
pub unsafe extern "C" fn nvmlDeviceGetProcessUtilization(
    _: nvmlDevice_t,
    samples: *mut nvmlProcessUtilizationSample_t,
    count: *mut c_uint,
    last_seen: c_ulonglong,
) -> nvmlReturn_t {
    forced!("nvmlDeviceGetProcessUtilization");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as c_ulonglong;
    let recent = gpu_state()
        .0
        .into_iter()
        .filter(|(_, process)| process.sm > 0 && now > last_seen)
        .map(|(pid, process)| nvmlProcessUtilizationSample_t {
            pid,
            timeStamp: now,
            smUtil: process.sm,
            memUtil: 0,
            encUtil: 0,
            decUtil: 0,
        })
        .collect();
    answer(recent, count, samples, NOT_FOUND)
}

/// # Safety
/// `utilization` points to a utilisation.
#[no_mangle]
pub unsafe extern "C" fn nvmlDeviceGetUtilizationRates(
    _: nvmlDevice_t,
    utilization: *mut nvmlUtilization_t,
) -> nvmlReturn_t {
    forced!("nvmlDeviceGetUtilizationRates");
    (*utilization).gpu = match computing(&gpu_state().0) {
        true => 100,
        false => 0,
    };
    (*utilization).memory = 0;
    SUCCESS
}

/// # Safety
/// `memory` points to a memory record.
#[no_mangle]
pub unsafe extern "C" fn nvmlDeviceGetMemoryInfo_v2(
    _: nvmlDevice_t,
    memory: *mut nvmlMemory_v2_t,
) -> nvmlReturn_t {
    forced!("nvmlDeviceGetMemoryInfo_v2");
    let (processes, total) = gpu_state();
    let used: u64 = processes.iter().map(|(_, process)| process.mem).sum();
    (*memory).total = total * MIB;
    (*memory).reserved = 0;
    (*memory).used = used * MIB;
    (*memory).free = total.saturating_sub(used) * MIB;
    SUCCESS
}

/// # Safety
/// `power` points to a power in milliwatts.
#[no_mangle]
pub unsafe extern "C" fn nvmlDeviceGetPowerUsage(
    _: nvmlDevice_t,
    power: *mut c_uint,
) -> nvmlReturn_t {
    forced!("nvmlDeviceGetPowerUsage");
    *power = match computing(&gpu_state().0) {
        true => 580_250,
        false => 35_500,
    };
    SUCCESS
}

/// # Safety
/// `limit` points to a power in milliwatts.
#[no_mangle]
pub unsafe extern "C" fn nvmlDeviceGetEnforcedPowerLimit(
    _: nvmlDevice_t,
    limit: *mut c_uint,
) -> nvmlReturn_t {
    forced!("nvmlDeviceGetEnforcedPowerLimit");
    *limit = 600_000;
    SUCCESS
}

/// # Safety
/// `temperature` points to degrees Celsius.
#[no_mangle]
pub unsafe extern "C" fn nvmlDeviceGetTemperature(
    _: nvmlDevice_t,
    _: nvmlTemperatureSensors_t,
    temperature: *mut c_uint,
) -> nvmlReturn_t {
    forced!("nvmlDeviceGetTemperature");
    *temperature = match computing(&gpu_state().0) {
        true => 75,
        false => 40,
    };
    SUCCESS
}

/// # Safety
/// `name` holds `length` bytes.
#[no_mangle]
pub unsafe extern "C" fn nvmlDeviceGetName(
    _: nvmlDevice_t,
    name: *mut c_char,
    length: c_uint,
) -> nvmlReturn_t {
    forced!("nvmlDeviceGetName");
    match (length as usize) < NAME.len() {
        true => INSUFFICIENT_SIZE,
        false => {
            std::ptr::copy_nonoverlapping(NAME.as_ptr().cast(), name, NAME.len());
            SUCCESS
        }
    }
}

/// # Safety
/// `uuid` holds `length` bytes.
#[no_mangle]
pub unsafe extern "C" fn nvmlDeviceGetUUID(
    _: nvmlDevice_t,
    uuid: *mut c_char,
    length: c_uint,
) -> nvmlReturn_t {
    forced!("nvmlDeviceGetUUID");
    match (length as usize) < UUID.len() {
        true => INSUFFICIENT_SIZE,
        false => {
            std::ptr::copy_nonoverlapping(UUID.as_ptr().cast(), uuid, UUID.len());
            SUCCESS
        }
    }
}
