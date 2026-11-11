//! A status snapshot as lines of text: the serving line, the GPU panel and one table of every job. Cells the
//! daemon gives no data for stay empty. `gpu-run status` prints the snapshot, `status --live` redraws it.
use crate::cells::{printable, width};
use crate::protocol::{Card, JobState, LineKind, Mode, OwnJob, Status, Telemetry};

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
        self.rows.push(
            cells
                .into_iter()
                .map(|cell| printable(&cell.unwrap_or_default()))
                .collect(),
        );
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
                    .map(|row| width(&row[column]))
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
                    .map(|(column, ((cell, (_, align)), column_width))| {
                        let padding = " ".repeat(column_width - width(cell));
                        match (*align, column == last) {
                            (_, true) => cell.clone(),
                            (Align::Left, false) => format!("{cell}{padding}"),
                            (Align::Right, false) => format!("{padding}{cell}"),
                        }
                    })
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

fn cooperation(flag: bool) -> Option<String> {
    Some(if flag { "yes" } else { "no" }.to_string())
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
                    cooperation(j.cooperative),
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
                cooperation(j.cooperative),
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

/// The card as one row under its header, each figure as wide as its largest value, so the columns keep still
/// while the figures change.
fn card_table(card: &Card) -> Vec<String> {
    use Align::{Left, Right};
    let limit = format!("{:.0}", card.power_limit);
    let total = card.memory_total_mib.to_string();
    let mut table = Table::new(&[
        ("GPU", Left),
        ("POWER", Right),
        ("TEMP", Right),
        ("UTIL", Right),
        ("VRAM", Left),
    ]);
    table.row(vec![
        Some(card.name.clone()),
        Some(format!(
            "{:>width$.0} / {limit} W",
            card.power_draw,
            width = limit.len()
        )),
        Some(format!("{:>3} C", card.temperature)),
        Some(format!("{:>3} %", card.utilization)),
        Some(format!(
            "{:>width$} / {total} MiB",
            card.memory_used_mib,
            width = total.len()
        )),
    ]);
    table.lines()
}

/// The card as gpu-switch read it, and each GPU process of the viewer's running jobs (as the daemon attributes
/// them, by pids of the viewer's side) with its job's GPU seconds against the requested ones and the job's name
/// last; every other process is one `other` row.
pub fn panel(s: &Status) -> Vec<String> {
    let card = match &s.card {
        Telemetry::Read(card) => card,
        Telemetry::Unavailable { error } => {
            return vec![printable(&format!("GPU: telemetry unavailable: {error}"))]
        }
    };
    let mut lines = card_table(card);
    use Align::{Left, Right};
    let mut table = Table::new(&[
        ("PID", Right),
        ("VRAM", Right),
        ("GPU / REQUESTED", Right),
        ("NAME", Left),
    ]);
    let mut known = 0;
    for j in &s.jobs {
        for process in &j.gpu_processes {
            known += process.memory_mib.unwrap_or(0);
            let used = j.timing.as_ref().map(|t| format!("{:.0}", t.gpu_seconds));
            let requested = j.expected_runtime_secs.map(|n| n.to_string());
            table.row(vec![
                Some(process.pid.to_string()),
                process.memory_mib.map(|mib| format!("{mib} MiB")),
                Some(format!(
                    "{}/{} s",
                    used.unwrap_or_default(),
                    requested.unwrap_or_default()
                )),
                Some(format!("{} ({})", what(j), j.id)),
            ]);
        }
    }
    table.row(vec![
        Some("other".to_string()),
        Some(format!(
            "{} MiB",
            card.memory_used_mib.saturating_sub(known)
        )),
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
        .map(|warning| printable(&warning))
        .collect()
}

/// `gpu-run status`: the summary, the serving line, NInfer's last error, the GPU panel, the job table and the
/// jobs' contract warnings.
pub fn snapshot(s: &Status, show_command: bool) -> Vec<String> {
    let mut lines = vec![printable(&s.summary), printable(&serving(s))];
    lines.extend(
        s.ninfer_error
            .iter()
            .map(|e| printable(&format!("NInfer last error: {e}"))),
    );
    lines.extend(panel(s));
    lines.extend(jobs(s, show_command));
    lines.extend(warnings(s));
    lines
}

/// `gpu-run status --live`: the summary and the serving line only while serving (the line then waits for
/// NInfer), the GPU panel, the job table and the jobs' contract warnings.
pub fn live(s: &Status, show_command: bool) -> Vec<String> {
    let mut lines: Vec<String> = match s.serve_wanted {
        true => vec![printable(&s.summary), printable(&serving(s))],
        false => Vec::new(),
    };
    lines.extend(panel(s));
    lines.extend(jobs(s, show_command));
    lines.extend(warnings(s));
    lines
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{own, status};
    use crate::protocol::{GpuProcess, JobTiming, LineEntry, OtherJob};

    fn card() -> Telemetry {
        Telemetry::Read(Card {
            name: "NVIDIA GeForce RTX 5090".into(),
            power_draw: 598.4,
            power_limit: 600.0,
            temperature: 75,
            utilization: 100,
            memory_used_mib: 18289,
            memory_total_mib: 32607,
        })
    }

    fn with_card(mut s: Status) -> Status {
        s.card = card();
        s
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
        job.gpu_processes = vec![
            GpuProcess {
                pid: 633362,
                memory_mib: Some(17958),
            },
            GpuProcess {
                pid: 633400,
                memory_mib: None,
            },
        ];
        let mut s = with_card(status(vec![job], Vec::new()));
        let lines = panel(&s);
        assert_eq!(
            lines[..2],
            [
                "GPU                            POWER   TEMP   UTIL  VRAM",
                "NVIDIA GeForce RTX 5090  598 / 600 W   75 C  100 %  18289 / 32607 MiB"
            ]
        );
        assert_eq!(lines[2], "     PID       VRAM  GPU / REQUESTED  NAME");
        assert_eq!(lines[3], "  633362  17958 MiB        412/600 s  bench (5)");
        assert_eq!(lines[4], "  633400                   412/600 s  bench (5)");
        assert_eq!(lines[5], "   other    331 MiB");
        s.card = Telemetry::Unavailable {
            error: "NVML: driver not loaded".into(),
        };
        assert_eq!(
            panel(&s),
            ["GPU: telemetry unavailable: NVML: driver not loaded"]
        );
    }

    #[test]
    fn the_card_columns_keep_still_while_the_figures_change() {
        let Telemetry::Read(busy) = card() else {
            unreachable!()
        };
        let idle = Card {
            power_draw: 35.5,
            temperature: 7,
            utilization: 0,
            memory_used_mib: 5,
            ..busy.clone()
        };
        let rows = [busy, idle].map(|card| card_table(&card));
        assert_eq!(rows[0][0], rows[1][0]);
        assert_eq!(
            rows[1][1],
            "NVIDIA GeForce RTX 5090   36 / 600 W    7 C    0 %      5 / 32607 MiB"
        );
        assert_eq!(width(&rows[0][1]), width(&rows[1][1]));
    }

    #[test]
    fn the_live_view_has_no_summary_and_the_serving_line_only_while_serving() {
        let mut s = with_card(status(vec![own(1, JobState::Running)], Vec::new()));
        s.summary = "OFF: serving is off".into();
        let view = live(&s, false);
        assert!(view[0].starts_with("GPU "), "{view:#?}");
        assert!(!view.iter().any(|line| line.contains("OFF: serving is off")));
        assert_eq!(snapshot(&s, false)[0], "OFF: serving is off");
        s.serve_wanted = true;
        s.summary = "SERVING: jobs wait for 'switch-gpu'".into();
        assert_eq!(
            live(&s, false)[..2],
            [
                "SERVING: jobs wait for 'switch-gpu'",
                "serving: on, GPU owner: queue, profile: -, host cache: unset"
            ]
        );
    }

    #[test]
    fn a_name_stays_one_line_and_wide_characters_keep_the_columns_aligned() {
        let mut job = own(9, JobState::Running);
        job.label = Some("two\nlines\x1b[2J".into());
        let lines = jobs(&status(vec![job], Vec::new()), false);
        assert!(lines[1].ends_with("two?lines?[2J"), "{}", lines[1]);
        let mut table = Table::new(&[("NAME", Align::Left), ("ID", Align::Right)]);
        table.row(vec![Some("显卡".into()), Some("1".into())]);
        table.row(vec![Some("card".into()), Some("2".into())]);
        assert_eq!(
            table.lines(),
            ["NAME  ID", "显卡  1", "card  2"],
            "显卡 is four columns wide, as wide as NAME"
        );
    }

    #[test]
    fn known_non_cooperation_reads_no() {
        let lines = jobs(&status(vec![own(1, JobState::Running)], Vec::new()), false);
        let coop = lines[0].find("COOP").unwrap();
        assert_eq!(&lines[1][coop..coop + 2], "no", "{lines:#?}");
    }
}
