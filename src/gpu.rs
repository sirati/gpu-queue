//! The card, read through NVML in this process: its processes and their memory, which of them compute now, its
//! memory and the figures a viewer is shown. NVML keeps one utilisation sample per process, its last one with any
//! use, dated in microseconds since the epoch; while a process computes, the driver renews it every 100 to 300 ms.

use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nvml_wrapper::enum_wrappers::device::TemperatureSensor;
use nvml_wrapper::enums::device::UsedGpuMemory;
use nvml_wrapper::error::NvmlError;
use nvml_wrapper::struct_wrappers::device::ProcessInfo;
use nvml_wrapper::{Device, Nvml};

use crate::protocol::{Card, Telemetry};

/// How recent a process's utilisation sample must be for the process to compute now: a second spans several of
/// the driver's renewals of a computing process's sample.
const RECENT: Duration = Duration::from_secs(1);

const MIB: u64 = 1 << 20;

/// NVML, from the driver's library.
pub struct Gpu {
    nvml: Nvml,
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
    /// The first device, as a viewer is shown it.
    pub card: Telemetry,
}

impl Gpu {
    /// NVML from the driver library at `library` (`libnvidia-ml.so.1`).
    pub fn open(library: &Path) -> Result<Gpu, String> {
        Nvml::builder()
            .lib_path(library.as_os_str())
            .init()
            .map(|nvml| Gpu { nvml })
            .map_err(|error| format!("NVML from {}: {error}", library.display()))
    }

    /// Every device's compute processes with their memory.
    pub fn compute(&self) -> Result<Vec<(u32, Option<u64>)>, String> {
        self.devices()
            .and_then(|devices| processes(&devices, Device::running_compute_processes))
            .map_err(failed)
    }

    pub fn sample(&self) -> Result<Sample, String> {
        self.read().map_err(failed)
    }

    fn read(&self) -> Result<Sample, NvmlError> {
        let devices = self.devices()?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let since = now.saturating_sub(RECENT).as_micros() as u64;
        let mut computing = HashSet::new();
        for device in &devices {
            computing.extend(recently_computing(device, since)?);
        }
        let memory = devices
            .iter()
            .map(|device| device.memory_info())
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .min_by_key(|memory| memory.free)
            .map(|memory| (memory.total, memory.free));
        Ok(Sample {
            compute: processes(&devices, Device::running_compute_processes)?,
            graphics: processes(&devices, Device::running_graphics_processes)?,
            computing,
            memory,
            card: devices.first().map(card).unwrap_or(Telemetry::Unavailable {
                error: "NVML reports no GPU".into(),
            }),
        })
    }

    fn devices(&self) -> Result<Vec<Device<'_>>, NvmlError> {
        (0..self.nvml.device_count()?)
            .map(|index| self.nvml.device_by_index(index))
            .collect()
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

fn failed(error: NvmlError) -> String {
    format!("NVML: {error}")
}

/// The processes `list` reports on each device, with their memory.
fn processes<'nvml>(
    devices: &[Device<'nvml>],
    list: impl Fn(&Device<'nvml>) -> Result<Vec<ProcessInfo>, NvmlError>,
) -> Result<Vec<(u32, Option<u64>)>, NvmlError> {
    let mut found = Vec::new();
    for device in devices {
        found.extend(list(device)?.into_iter().map(|process| {
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
fn recently_computing(device: &Device<'_>, since: u64) -> Result<Vec<u32>, NvmlError> {
    match device.process_utilization_stats(since) {
        Ok(samples) => Ok(samples
            .into_iter()
            .filter(|sample| sample.sm_util > 0)
            .map(|sample| sample.pid)
            .collect()),
        Err(NvmlError::NotFound) => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}

fn card(device: &Device<'_>) -> Telemetry {
    let read = || -> Result<Card, NvmlError> {
        let memory = device.memory_info()?;
        Ok(Card {
            name: device.name()?,
            power_draw: f64::from(device.power_usage()?) / 1000.0,
            power_limit: f64::from(device.enforced_power_limit()?) / 1000.0,
            temperature: device.temperature(TemperatureSensor::Gpu)?,
            utilization: device.utilization_rates()?.gpu,
            memory_used_mib: memory.used / MIB,
            memory_total_mib: memory.total / MIB,
        })
    };
    read().map_or_else(
        |error| Telemetry::Unavailable {
            error: failed(error),
        },
        Telemetry::Read,
    )
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
}
