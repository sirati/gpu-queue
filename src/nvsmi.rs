//! Host GPU facts from the `nvidia-smi` CLI. Stands in for the NVML calls of
//! tools/_gpu_nvml.py: compute and graphics processes, per-pid memory, SM
//! activity, free memory.

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const MIB: u64 = 1 << 20;

/// How long a viewer waits for the card's telemetry before showing it as unavailable.
const CARD_DEADLINE: Duration = Duration::from_secs(5);

fn run(nvidia_smi: &Path, args: &[&str]) -> Result<String, String> {
    let out = Command::new(nvidia_smi)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("running {}: {e}", nvidia_smi.display()))?;
    answer(nvidia_smi, args, out)
}

/// Like `run`, but the query runs in a process group of its own, and one that has not exited and closed its
/// output by `deadline` (a process it started may hold the output) is killed with its group and reported as
/// unanswered.
fn run_within(nvidia_smi: &Path, args: &[&str], deadline: Duration) -> Result<String, String> {
    let end = Instant::now() + deadline;
    let mut child = Command::new(nvidia_smi)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .spawn()
        .map_err(|e| format!("running {}: {e}", nvidia_smi.display()))?;
    let (sink, drained) = mpsc::channel();
    drain(
        child.stdout.take().expect("stdout is piped"),
        0,
        sink.clone(),
    );
    drain(child.stderr.take().expect("stderr is piped"), 1, sink);
    let unanswered = |child: &mut Child| {
        // SAFETY: signals the query's own process group: the query and what it started.
        unsafe { libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL) };
        let _ = child.wait();
        format!(
            "{} {}: no answer within {} s",
            nvidia_smi.display(),
            args.join(" "),
            deadline.as_secs_f64()
        )
    };
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < end => thread::sleep(Duration::from_millis(10)),
            Ok(None) => return Err(unanswered(&mut child)),
            Err(e) => return Err(format!("waiting for {}: {e}", nvidia_smi.display())),
        }
    };
    let mut outputs = [Vec::new(), Vec::new()];
    for _ in 0..outputs.len() {
        match drained.recv_timeout(end.saturating_duration_since(Instant::now())) {
            Ok((pipe, bytes)) => outputs[pipe] = bytes,
            Err(_) => return Err(unanswered(&mut child)),
        }
    }
    let [stdout, stderr] = outputs;
    answer(
        nvidia_smi,
        args,
        Output {
            status,
            stdout,
            stderr,
        },
    )
}

/// A pipe read to its end on its own thread, so a full pipe never stalls the child; its bytes go to `sink`
/// as pipe `which`.
fn drain(mut pipe: impl Read + Send + 'static, which: usize, sink: mpsc::Sender<(usize, Vec<u8>)>) {
    thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = pipe.read_to_end(&mut bytes);
        let _ = sink.send((which, bytes));
    });
}

/// A finished query's output, or its failure with nvidia-smi's own words.
fn answer(nvidia_smi: &Path, args: &[&str], out: Output) -> Result<String, String> {
    if !out.status.success() {
        return Err(format!(
            "{} {}: {}",
            nvidia_smi.display(),
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Compute processes with used memory in bytes; `None` where the driver
/// reports it unavailable.
pub fn compute_apps(nvidia_smi: &Path) -> Result<Vec<(u32, Option<u64>)>, String> {
    let text = run(
        nvidia_smi,
        &[
            "--query-compute-apps=pid,used_memory",
            "--format=csv,noheader,nounits",
        ],
    )?;
    let mut out = Vec::new();
    for line in text.lines().map(str::trim).filter(|l| !l.is_empty()) {
        let mut f = line.split(',').map(str::trim);
        let pid = f
            .next()
            .and_then(|p| p.parse().ok())
            .ok_or_else(|| format!("unexpected nvidia-smi line {line:?}"))?;
        let mem = f
            .next()
            .and_then(|m| m.parse::<u64>().ok())
            .map(|m| m * MIB);
        out.push((pid, mem));
    }
    Ok(out)
}

/// The first card's live telemetry, as nvidia-smi's header shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct Card {
    pub name: String,
    /// Watts drawn now (the driver refreshes it about every half second) and the enforced cap.
    pub power_draw: f64,
    pub power_limit: f64,
    /// Degrees Celsius and percent.
    pub temperature: u32,
    pub utilization: u32,
    pub memory_used_mib: u64,
    pub memory_total_mib: u64,
}

pub fn card(nvidia_smi: &Path) -> Result<Card, String> {
    let text = run_within(
        nvidia_smi,
        &[
            "--query-gpu=name,power.draw.instant,power.limit,temperature.gpu,utilization.gpu,memory.used,memory.total",
            "--format=csv,noheader,nounits",
        ],
        CARD_DEADLINE,
    )?;
    parse_card(&text)
}

pub fn parse_card(text: &str) -> Result<Card, String> {
    let line = text
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .ok_or("nvidia-smi reported no GPU")?;
    let fields: Vec<&str> = line.split(',').map(str::trim).collect();
    let [name, draw, limit, temperature, utilization, used, total] = fields.as_slice() else {
        return Err(format!("unexpected nvidia-smi line {line:?}"));
    };
    let number = |value: &str| {
        value
            .parse::<f64>()
            .map_err(|_| format!("unexpected nvidia-smi value {value:?} in {line:?}"))
    };
    Ok(Card {
        name: name.to_string(),
        power_draw: number(draw)?,
        power_limit: number(limit)?,
        temperature: number(temperature)? as u32,
        utilization: number(utilization)? as u32,
        memory_used_mib: number(used)? as u64,
        memory_total_mib: number(total)? as u64,
    })
}

/// `(total, free)` bytes of the device with least free memory
/// (tools/_gpu_nvml.py:114-124).
pub fn memory(nvidia_smi: &Path) -> Result<(u64, u64), String> {
    let text = run(
        nvidia_smi,
        &[
            "--query-gpu=memory.total,memory.free",
            "--format=csv,noheader,nounits",
        ],
    )?;
    text.lines()
        .filter_map(|l| {
            let mut f = l.split(',').map(|v| v.trim().parse::<u64>().ok());
            Some((f.next()??, f.next()??))
        })
        .min_by_key(|(_, free)| *free)
        .map(|(t, f)| (t * MIB, f * MIB))
        .ok_or_else(|| format!("no memory.total,memory.free in {text:?}"))
}

/// Zero on every device proves inactivity even if per-process SM is unavailable.
fn device_activity(nvidia_smi: &Path) -> Result<bool, String> {
    let text = run(
        nvidia_smi,
        &[
            "--query-gpu=utilization.gpu",
            "--format=csv,noheader,nounits",
        ],
    )?;
    let values: Result<Vec<u32>, _> = text.lines().map(|line| line.trim().parse()).collect();
    let values = values.map_err(|_| "GPU utilisation unavailable".to_string())?;
    if values.is_empty() {
        return Err("no GPU utilisation samples".into());
    }
    Ok(values.iter().any(|value| *value > 0))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pmon {
    pub pid: u32,
    /// `C`, `G` or `C+G`.
    pub kind: String,
    /// SM utilisation in percent; `None` when not sampled (`-`).
    pub sm: Option<u32>,
    /// Frame buffer memory in bytes; `None` when not sampled.
    pub fb: Option<u64>,
}

impl Pmon {
    pub fn graphics(&self) -> bool {
        self.kind.contains('G')
    }
    pub fn computing(&self) -> bool {
        self.sm.is_some_and(|s| s > 0)
    }
}

/// One `nvidia-smi pmon -c 1 -s um` sample: utilisation and memory of every process.
pub fn pmon(nvidia_smi: &Path) -> Result<Vec<Pmon>, String> {
    let text = run(nvidia_smi, &["pmon", "-c", "1", "-s", "um"])?;
    Ok(parse_pmon(&text))
}

/// The rows; SM utilisation is the fourth column whatever is selected, the memory column is found by its
/// header, which names one column per field (`# gpu pid type sm ... fb ccpm command`).
pub fn parse_pmon(text: &str) -> Vec<Pmon> {
    let fb = text
        .lines()
        .find(|l| l.trim_start().starts_with('#'))
        .and_then(|header| {
            header
                .trim_start()
                .trim_start_matches('#')
                .split_whitespace()
                .position(|name| name == "fb")
        });
    text.lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .filter_map(|l| {
            let f: Vec<&str> = l.split_whitespace().collect();
            Some(Pmon {
                pid: f.get(1)?.parse().ok()?,
                kind: f.get(2)?.to_string(),
                sm: f.get(3).and_then(|s| s.parse().ok()),
                fb: fb
                    .and_then(|at| f.get(at))
                    .and_then(|m| m.parse::<u64>().ok())
                    .map(|m| m * MIB),
            })
        })
        .collect()
}

#[derive(Debug, Clone, Default)]
pub struct Sample {
    pub compute: Vec<(u32, Option<u64>)>,
    pub pmon: Vec<Pmon>,
    /// Any device computing, or None when the device query is unavailable.
    pub device_active: Option<bool>,
    /// `(total, free)` bytes.
    pub memory: Option<(u64, u64)>,
}

impl Sample {
    pub fn take(nvidia_smi: &Path) -> Result<Sample, String> {
        Ok(Sample {
            compute: compute_apps(nvidia_smi)?,
            pmon: pmon(nvidia_smi)?,
            memory: Some(memory(nvidia_smi)?),
            device_active: device_activity(nvidia_smi).ok(),
        })
    }

    /// Every process on the card with its memory in bytes: the compute processes as the compute query
    /// reports them, then the graphics-only ones with their frame buffer.
    pub fn processes(&self) -> Vec<(u32, Option<u64>)> {
        let graphics = self
            .pmon
            .iter()
            .filter(|p| p.graphics() && self.compute.iter().all(|(pid, _)| *pid != p.pid))
            .map(|p| (p.pid, p.fb));
        self.compute.iter().copied().chain(graphics).collect()
    }

    /// Shared by accounting and enforcement so unavailable pmon rows cannot
    /// freeze idle accounting while another path declares the same job idle.
    pub fn activity(&self, owned: &HashSet<u32>) -> Option<bool> {
        let rows: Vec<_> = self
            .pmon
            .iter()
            .filter(|p| owned.contains(&p.pid))
            .collect();
        if rows.iter().any(|p| p.computing()) {
            return Some(true);
        }
        if self.device_active == Some(false) {
            return Some(false);
        }
        let unknown = rows.iter().any(|p| p.sm.is_none())
            || self.compute.iter().any(|(pid, _)| {
                owned.contains(pid) && !rows.iter().any(|p| p.pid == *pid && p.sm.is_some())
            });
        if unknown {
            None
        } else {
            Some(false)
        }
    }

    pub fn computing(&self) -> HashMap<u32, bool> {
        self.pmon.iter().map(|p| (p.pid, p.computing())).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pmon_rows() {
        let text = "\
# gpu         pid   type     sm    mem    enc    dec    jpg    ofa    command
# Idx           #    C/G      %      %      %      %      %      %    name
    0       1234     C     45     10      -      -      -      -    python
    0       5678     G      -      -      -      -      -      -    Xorg
    0       9012   C+G      0      0      -      -      -      -    firefox
    0          -     -      -      -      -      -      -      -    -
";
        let rows = parse_pmon(text);
        assert_eq!(rows.len(), 3);
        assert!(rows[0].computing() && !rows[0].graphics());
        assert!(rows[1].graphics() && rows[1].sm.is_none());
        assert!(rows[2].graphics() && !rows[2].computing());
        assert!(
            rows.iter().all(|row| row.fb.is_none()),
            "no memory column selected"
        );
    }
    #[test]
    fn graphics_only_processes_join_the_compute_ones_with_their_frame_buffer() {
        let text = "\
# gpu         pid   type     sm    mem    enc    dec    jpg    ofa     fb   ccpm    command
# Idx           #    C/G      %      %      %      %      %      %     MB     MB    name
    0        100   C+G      -      -      -      -      -      -     55      0    kwin_wayland
    0        200     G      -      -      -      -      -      -      8      0    plasmashell
    0        300     C     90      -      -      -      -      -  12934      0    python
    0        400     G      -      -      -      -      -      -      -      -    helper
";
        let rows = parse_pmon(text);
        assert_eq!(rows[2].sm, Some(90));
        assert_eq!(rows[0].fb, Some(55 * MIB));
        let sample = Sample {
            compute: vec![(100, Some(40 * MIB)), (300, None)],
            pmon: rows,
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
    fn unavailable_process_sm_uses_only_proven_device_inactivity() {
        let owned = HashSet::from([1234]);
        let mut sample = Sample {
            compute: vec![(1234, Some(100))],
            pmon: vec![Pmon {
                pid: 1234,
                kind: "C".into(),
                sm: None,
                fb: None,
            }],
            ..Sample::default()
        };
        assert_eq!(sample.activity(&owned), None);
        sample.device_active = Some(true);
        assert_eq!(sample.activity(&owned), None);
        sample.device_active = Some(false);
        assert_eq!(sample.activity(&owned), Some(false));
        sample.pmon[0].sm = Some(20);
        assert_eq!(sample.activity(&owned), Some(true));
    }
    #[test]
    fn another_shared_job_cannot_supply_compute_for_an_idle_owner() {
        let sample = Sample {
            device_active: Some(true),
            pmon: vec![
                Pmon {
                    pid: 1,
                    kind: "C".into(),
                    sm: Some(0),
                    fb: None,
                },
                Pmon {
                    pid: 2,
                    kind: "C".into(),
                    sm: Some(90),
                    fb: None,
                },
            ],
            ..Sample::default()
        };
        assert_eq!(sample.activity(&HashSet::from([1])), Some(false));
        assert_eq!(sample.activity(&HashSet::from([2])), Some(true));
    }
    #[test]
    fn the_card_line_is_read_and_an_unreadable_one_is_an_error() {
        assert_eq!(
            parse_card("NVIDIA GeForce RTX 5090, 598.40, 600.00, 75, 100, 18289, 32607\n"),
            Ok(Card {
                name: "NVIDIA GeForce RTX 5090".into(),
                power_draw: 598.4,
                power_limit: 600.0,
                temperature: 75,
                utilization: 100,
                memory_used_mib: 18289,
                memory_total_mib: 32607,
            })
        );
        assert!(parse_card("NVIDIA, [N/A], 600, 75, 100, 1, 2\n").is_err());
        assert!(parse_card("").is_err());
    }
    #[test]
    fn a_query_past_its_deadline_is_killed_and_unanswered() {
        let started = Instant::now();
        let error =
            run_within(Path::new("sleep"), &["30"], Duration::from_millis(200)).unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{:?}",
            started.elapsed()
        );
        assert!(
            error.ends_with("sleep 30: no answer within 0.2 s"),
            "{error}"
        );
        assert_eq!(
            run_within(Path::new("echo"), &["card"], Duration::from_secs(10)),
            Ok("card\n".into())
        );
    }
    #[test]
    fn a_process_the_query_left_holding_its_output_is_killed_by_the_deadline() {
        let dir = std::env::temp_dir().join(format!("nvsmi-held-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let held = dir.join("pid");
        let script = format!("echo card; sleep 30 & echo $! > {}; exit 0", held.display());
        let started = Instant::now();
        let error = run_within(
            Path::new("sh"),
            &["-c", &script],
            Duration::from_millis(300),
        )
        .unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{:?}",
            started.elapsed()
        );
        assert!(error.ends_with("no answer within 0.3 s"), "{error}");
        let pid = std::fs::read_to_string(&held).unwrap();
        let ended = || {
            std::fs::read_to_string(format!("/proc/{}/stat", pid.trim()))
                .map(|stat| stat[stat.rfind(')').unwrap() + 2..].starts_with('Z'))
                .unwrap_or(true)
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while !ended() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        assert!(
            ended(),
            "the query's background process {pid} outlived its deadline"
        );
        std::fs::remove_file(&held).unwrap();
        std::fs::remove_dir(&dir).unwrap();
    }
}
