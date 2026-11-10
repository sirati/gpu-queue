//! A status snapshot as lines of text: the serving line, the GPU panel and one table of every job. Cells the
//! daemon gives no data for stay empty. `gpu-run status` prints the snapshot, `status --live` redraws it.
use crate::gpu_view::GpuView;
use crate::protocol::{JobState, LineKind, Mode, OwnJob, Status};

pub fn profile_line(current: Option<&str>, pending: Option<&str>) -> String {
    let current = current.unwrap_or("-");
    match pending {
        Some(p) => format!("profile: {current} current, {p} pending (next NInfer start)"),
        None => format!("profile: {current}"),
    }
}

pub fn exit_of(j: &OwnJob) -> Option<String> {
    match (j.exit_code, j.signal) {
        (Some(c), _) => Some(c.to_string()),
        (None, Some(s)) => Some(format!("sig {s}")),
        _ => None,
    }
}

pub fn duration(seconds: Option<f64>) -> Option<String> {
    seconds.map(|s| format!("{s:.1}s"))
}

pub fn size(bytes: u64) -> Option<String> {
    if bytes == 0 {
        None
    } else if bytes >= 1 << 30 {
        Some(format!("{:.1}G", bytes as f64 / (1u64 << 30) as f64))
    } else {
        Some(format!("{}M", bytes >> 20))
    }
}

pub fn what(j: &OwnJob) -> String {
    j.label
        .clone()
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| format!("execution-{}", j.handle))
}

pub fn display_state(j: &OwnJob) -> &str {
    j.terminal_status
        .map(|s| s.as_str())
        .unwrap_or(j.state.as_str())
}

pub fn command_line(cmd: &[String], max: usize) -> String {
    let s = cmd.join(" ");
    if s.chars().count() > max {
        let cut: String = s.chars().take(max.saturating_sub(3)).collect();
        format!("{cut}...")
    } else {
        s
    }
}

pub fn queued_time(unix: f64) -> Option<String> {
    if !unix.is_finite() {
        return None;
    }
    let seconds = unix as libc::time_t;
    let mut local = std::mem::MaybeUninit::<libc::tm>::uninit();
    // SAFETY: localtime_r initializes the output on success; neither pointer escapes.
    let result = unsafe { libc::localtime_r(&seconds, local.as_mut_ptr()) };
    if result.is_null() {
        return None;
    }
    // SAFETY: localtime_r succeeded above.
    let local = unsafe { local.assume_init() };
    Some(format!(
        "{:02}:{:02}:{:02}",
        local.tm_hour, local.tm_min, local.tm_sec
    ))
}

/// Columns sized to their contents: numeric ones right-aligned, the last one unpadded, so a long last cell
/// (a job's name) never pushes the others apart.
pub struct Table {
    columns: Vec<(&'static str, Align)>,
    rows: Vec<Vec<String>>,
}

#[derive(Clone, Copy, PartialEq)]
pub enum Align {
    Left,
    Right,
}

impl Table {
    pub fn new(columns: &[(&'static str, Align)]) -> Table {
        Table {
            columns: columns.to_vec(),
            rows: Vec::new(),
        }
    }

    /// A row; a cell the daemon gave no data for is `None` and stays empty.
    pub fn row(&mut self, cells: Vec<Option<String>>) {
        self.rows
            .push(cells.into_iter().map(Option::unwrap_or_default).collect());
    }

    pub fn lines(&self) -> Vec<String> {
        let header: Vec<String> = self
            .columns
            .iter()
            .map(|(name, _)| name.to_string())
            .collect();
        let widths: Vec<usize> = (0..self.columns.len())
            .map(|column| {
                std::iter::once(&header)
                    .chain(&self.rows)
                    .map(|row| row[column].chars().count())
                    .max()
                    .unwrap_or(0)
            })
            .collect();
        let last = self.columns.len() - 1;
        std::iter::once(&header)
            .chain(&self.rows)
            .map(|row| {
                row.iter()
                    .zip(&self.columns)
                    .zip(&widths)
                    .enumerate()
                    .map(
                        |(column, ((cell, (_, align)), width))| match (*align, column == last) {
                            (_, true) => cell.clone(),
                            (Align::Left, false) => format!("{cell:<width$}"),
                            (Align::Right, false) => format!("{cell:>width$}"),
                        },
                    )
                    .collect::<Vec<_>>()
                    .join("  ")
                    .trim_end()
                    .to_string()
            })
            .collect()
    }
}

/// The serving line; profile and host cache only while serving, the only time they matter.
pub fn serving(s: &Status) -> String {
    let serving = format!(
        "serving: {}, GPU owner: {}",
        if s.serve_wanted { "on" } else { "off" },
        s.owner
    );
    let mib = |v: Option<u64>| {
        v.map(|m| format!("{m} MiB"))
            .unwrap_or_else(|| "unset".into())
    };
    let cache = match s.host_cache_pending_mib {
        Some(pending) => format!(
            "host cache: {} current, {} pending (next NInfer start)",
            mib(s.host_cache_current_mib),
            mib(Some(pending))
        ),
        None => format!("host cache: {}", mib(s.host_cache_current_mib)),
    };
    let profile = profile_line(
        s.serve.profile_current.as_deref(),
        s.serve.profile_pending.as_deref(),
    );
    match s.serve_wanted {
        true => format!("{serving}, {profile}, {cache}"),
        false => serving,
    }
}

/// Where a job's row goes: finished, then running, then waiting in line order.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Place {
    Finished(u64),
    Running(u64),
    Waiting(usize),
}

fn place(state: JobState, position: Option<usize>, order: u64) -> Place {
    match state {
        JobState::Queued => Place::Waiting(position.unwrap_or(usize::MAX)),
        JobState::Running | JobState::Stopping => Place::Running(order),
        _ => Place::Finished(order),
    }
}

fn mode(mode: Mode) -> Option<String> {
    Some(mode.as_str().to_string())
}

fn yes(flag: bool) -> Option<String> {
    flag.then(|| "yes".to_string())
}

/// The viewer's own jobs worth listing: the last ten finished and every unfinished one.
fn listed(s: &Status) -> Vec<&OwnJob> {
    let finished: Vec<&OwnJob> = s.jobs.iter().filter(|j| j.state.is_terminal()).collect();
    finished[finished.len().saturating_sub(10)..]
        .iter()
        .copied()
        .chain(s.jobs.iter().filter(|j| !j.state.is_terminal()))
        .collect()
}

/// One table of every job the viewer may see: its own listed ones, the other side's, and NInfer's place in
/// the line.
pub fn jobs(s: &Status, show_command: bool) -> Vec<String> {
    use Align::{Left, Right};
    let mut table = Table::new(&[
        ("ID", Right),
        ("QUEUED", Right),
        ("STATE", Left),
        ("POS", Right),
        ("MODE", Left),
        ("VRAM", Right),
        ("PRIO", Right),
        ("CATEGORY", Left),
        ("COOP", Left),
        ("EXIT", Right),
        ("EXPECT", Right),
        ("PREP", Right),
        ("GPU", Right),
        ("NO-VRAM", Right),
        ("NO-UTIL", Right),
        (
            if show_command {
                "NAME / COMMAND"
            } else {
                "NAME"
            },
            Left,
        ),
    ]);
    let mut rows: Vec<(Place, Vec<Option<String>>)> = listed(s)
        .into_iter()
        .map(|j| {
            let timing = j.timing.as_ref();
            let name = match show_command {
                true => format!("{} / {}", what(j), command_line(&j.cmd, 120)),
                false => what(j),
            };
            let state = match j.behind_ninfer {
                true => "waiting".to_string(),
                false => display_state(j).to_string(),
            };
            (
                place(j.state, j.position, j.id),
                vec![
                    Some(j.id.to_string()),
                    queued_time(j.submitted_at),
                    Some(state),
                    j.position.map(|p| p.to_string()),
                    mode(j.mode),
                    size(j.vram),
                    Some(j.priority.to_string()),
                    Some(j.category.as_str().to_string()),
                    yes(j.cooperative),
                    exit_of(j),
                    duration(j.expected_runtime_secs.map(|n| n as f64)),
                    duration(timing.map(|t| t.preparation_seconds)),
                    duration(timing.map(|t| t.gpu_seconds)),
                    duration(timing.map(|t| t.no_vram_seconds)),
                    duration(timing.map(|t| t.no_utilisation_seconds)),
                    Some(name),
                ],
            )
        })
        .collect();
    // The daemon tells another side's job's handle, state, place, mode, VRAM, priority and cooperation only.
    rows.extend(s.other_jobs.iter().enumerate().map(|(order, j)| {
        (
            place(j.state, j.position, order as u64),
            vec![
                Some(j.handle.clone()),
                None,
                Some(j.state.as_str().to_string()),
                j.position.map(|p| p.to_string()),
                mode(j.mode),
                size(j.vram),
                Some(j.priority.to_string()),
                None,
                yes(j.cooperative),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
        )
    }));
    rows.extend(
        s.line
            .iter()
            .filter(|e| e.kind == LineKind::Ninfer)
            .map(|e| {
                (
                    Place::Waiting(e.position),
                    vec![
                        None,
                        None,
                        Some("waiting".to_string()),
                        Some(e.position.to_string()),
                        None,
                        None,
                        Some(e.priority.to_string()),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        Some("NInfer (local model)".to_string()),
                    ],
                )
            }),
    );
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    for (_, cells) in rows {
        table.row(cells);
    }
    table.lines()
}

/// The card like nvidia-smi's header, and each GPU process of the viewer's jobs with its job's GPU seconds
/// against the requested ones and the job's name last; every other process is one `other` row.
pub fn panel(s: &Status, gpu: &GpuView) -> Vec<String> {
    let card = match &gpu.card {
        Ok(card) => card,
        Err(error) => return vec![format!("GPU: telemetry unavailable: {error}")],
    };
    let mut lines = vec![format!(
        "GPU: {}  power {:.0} / {:.0} W  {} C  utilisation {} %  VRAM {} / {} MiB",
        card.name,
        card.power_draw,
        card.power_limit,
        card.temperature,
        card.utilization,
        card.memory_used_mib,
        card.memory_total_mib
    )];
    use Align::{Left, Right};
    let mut table = Table::new(&[
        ("PID", Right),
        ("VRAM", Right),
        ("GPU / REQUESTED", Right),
        ("NAME", Left),
    ]);
    for process in &gpu.processes {
        let job = s.jobs.iter().find(|j| j.id == process.job);
        let used = job
            .and_then(|j| j.timing.as_ref())
            .map(|t| format!("{:.0}", t.gpu_seconds));
        let requested = job
            .and_then(|j| j.expected_runtime_secs)
            .map(|n| n.to_string());
        table.row(vec![
            Some(process.pid.to_string()),
            Some(format!("{} MiB", process.memory_mib)),
            Some(format!(
                "{}/{} s",
                used.unwrap_or_default(),
                requested.unwrap_or_default()
            )),
            job.map(|j| format!("{} ({})", what(j), j.id)),
        ]);
    }
    table.row(vec![
        Some("other".to_string()),
        Some(format!("{} MiB", gpu.other_mib)),
        None,
        None,
    ]);
    lines.extend(table.lines().into_iter().map(|line| format!("  {line}")));
    lines
}

/// Each GPU usage contract warning of the viewer's listed jobs.
fn warnings(s: &Status) -> Vec<String> {
    listed(s)
        .into_iter()
        .flat_map(|j| j.warnings(&format!("{} ({})", what(j), j.id)))
        .collect()
}

/// `gpu-run status`: the summary, the serving line, NInfer's last error, the GPU panel, the job table and the
/// jobs' contract warnings.
pub fn snapshot(s: &Status, gpu: &GpuView, show_command: bool) -> Vec<String> {
    let mut lines = vec![s.summary.clone(), serving(s)];
    lines.extend(
        s.ninfer_error
            .iter()
            .map(|e| format!("NInfer last error: {e}")),
    );
    lines.extend(panel(s, gpu));
    lines.extend(jobs(s, show_command));
    lines.extend(warnings(s));
    lines
}

/// `gpu-run status --live`: the serving line only while serving (the line then waits for NInfer), the GPU
/// panel, the job table and the jobs' contract warnings.
pub fn live(s: &Status, gpu: &GpuView) -> Vec<String> {
    let mut lines: Vec<String> = s.serve_wanted.then(|| serving(s)).into_iter().collect();
    lines.extend(panel(s, gpu));
    lines.extend(jobs(s, false));
    lines.extend(warnings(s));
    lines
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{own, status};
    use crate::gpu_view::JobProcess;
    use crate::nvsmi::Card;
    use crate::protocol::{JobTiming, LineEntry, OtherJob};

    fn card() -> Card {
        Card {
            name: "NVIDIA GeForce RTX 5090".into(),
            power_draw: 598.4,
            power_limit: 600.0,
            temperature: 75,
            utilization: 100,
            memory_used_mib: 18289,
            memory_total_mib: 32607,
        }
    }

    #[test]
    fn a_table_sizes_each_column_to_its_contents_and_leaves_missing_cells_empty() {
        let mut table = Table::new(&[
            ("ID", Align::Right),
            ("STATE", Align::Left),
            ("NAME", Align::Left),
        ]);
        table.row(vec![
            Some("7".into()),
            Some("running".into()),
            Some("a very long job name".into()),
        ]);
        table.row(vec![Some("12345".into()), None, None]);
        assert_eq!(
            table.lines(),
            [
                "   ID  STATE    NAME",
                "    7  running  a very long job name",
                "12345"
            ]
        );
    }

    #[test]
    fn every_job_is_one_row_finished_running_then_waiting_in_line_order() {
        let mut done = own(1, JobState::Done);
        done.exit_code = Some(0);
        let mut running = own(2, JobState::Running);
        running.label = Some("training".into());
        let mut queued = own(3, JobState::Queued);
        queued.position = Some(2);
        let other = OtherJob {
            handle: "n7".into(),
            state: JobState::Queued,
            position: Some(1),
            mode: Mode::Shared,
            vram: 4 << 30,
            cooperative: true,
            priority: 0,
        };
        let mut s = status(vec![queued, done, running], vec![other]);
        s.line = vec![LineEntry {
            position: 3,
            kind: LineKind::Ninfer,
            job: None,
            priority: 0,
        }];
        let lines = jobs(&s, false);
        let ids: Vec<&str> = lines[1..]
            .iter()
            .map(|line| line.split_whitespace().next().unwrap_or(""))
            .collect();
        assert_eq!(ids, ["1", "2", "n7", "3", "waiting"]);
        assert!(lines.iter().all(|line| !line.contains(" - ")), "{lines:#?}");
        assert!(lines[2].ends_with("training"), "{}", lines[2]);
        assert!(lines[5].ends_with("NInfer (local model)"), "{}", lines[5]);
        assert!(
            lines[3].trim_end().ends_with("yes"),
            "the other side's row ends at its last known cell: {}",
            lines[3]
        );
    }

    #[test]
    fn the_serving_details_show_only_while_serving() {
        let mut s = status(Vec::new(), Vec::new());
        s.host_cache_current_mib = Some(16384);
        s.serve.profile_current = Some("mtp".into());
        assert_eq!(serving(&s), "serving: off, GPU owner: queue");
        s.serve_wanted = true;
        s.owner = "ninfer".into();
        assert_eq!(
            serving(&s),
            "serving: on, GPU owner: ninfer, profile: mtp, host cache: 16384 MiB"
        );
    }

    #[test]
    fn the_panel_lists_the_viewers_gpu_processes_and_one_other_row() {
        let mut job = own(5, JobState::Running);
        job.label = Some("bench".into());
        job.expected_runtime_secs = Some(600);
        job.timing = Some(JobTiming {
            gpu_seconds: 412.4,
            ..JobTiming::default()
        });
        let s = status(vec![job], Vec::new());
        let gpu = GpuView {
            card: Ok(card()),
            processes: vec![JobProcess {
                pid: 633362,
                memory_mib: 17958,
                job: 5,
            }],
            other_mib: 331,
        };
        let lines = panel(&s, &gpu);
        assert_eq!(
            lines[0],
            "GPU: NVIDIA GeForce RTX 5090  power 598 / 600 W  75 C  utilisation 100 %  VRAM 18289 / 32607 MiB"
        );
        assert_eq!(lines[1], "     PID       VRAM  GPU / REQUESTED  NAME");
        assert_eq!(lines[2], "  633362  17958 MiB        412/600 s  bench (5)");
        assert_eq!(lines[3], "   other    331 MiB");
        let failed = GpuView {
            card: Err("nvidia-smi: not found".into()),
            processes: Vec::new(),
            other_mib: 0,
        };
        assert_eq!(
            panel(&s, &failed),
            ["GPU: telemetry unavailable: nvidia-smi: not found"]
        );
    }

    #[test]
    fn the_live_view_has_no_summary_and_the_serving_line_only_while_serving() {
        let mut s = status(vec![own(1, JobState::Running)], Vec::new());
        s.summary = "OFF: serving is off".into();
        let gpu = GpuView {
            card: Ok(card()),
            processes: Vec::new(),
            other_mib: 0,
        };
        let view = live(&s, &gpu);
        assert!(view[0].starts_with("GPU: "), "{view:#?}");
        assert!(!view.iter().any(|line| line.contains("OFF: serving is off")));
        s.serve_wanted = true;
        assert!(live(&s, &gpu)[0].starts_with("serving: on"));
        assert_eq!(snapshot(&s, &gpu, false)[0], "OFF: serving is off");
    }
}
