//! gpu-run's command and flag tables, shared by its parser and its bash
//! completion so the two cannot drift.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::protocol::{Category, Reply, Request, Status};

pub const SOCKET: &str = "--socket";

pub const SUBMIT: &str = "submit";
pub const STATUS: &str = "status";
pub const SWITCH_GPU: &str = "switch-gpu";
pub const SWITCH_NINFER: &str = "switch-ninfer";
pub const WAIT: &str = "wait";
pub const LOGS: &str = "logs";
pub const CANCEL: &str = "cancel";
pub const KILL: &str = "kill";
pub const SET_HOST_CACHE: &str = "set-host-cache";
pub const SET_PROFILE: &str = "set-profile";
pub const SERVE: &str = "serve";
pub const COMPLETIONS: &str = "completions";

pub const SUBCOMMANDS: &[&str] = &[
    SUBMIT,
    STATUS,
    SWITCH_GPU,
    SWITCH_NINFER,
    WAIT,
    LOGS,
    CANCEL,
    KILL,
    SET_HOST_CACHE,
    SET_PROFILE,
    SERVE,
    COMPLETIONS,
];

pub const EXCLUSIVE: &str = "--exclusive";
pub const SHARED: &str = "--shared";
pub const VRAM: &str = "--vram";
pub const CATEGORY: &str = "--category";
pub const COOPERATIVE: &str = "--cooperative";
pub const PRIORITY: &str = "--priority";
pub const TIMEOUT: &str = "--timeout";
pub const END: &str = "--";
pub const MODES: &[&str] = &[EXCLUSIVE, SHARED];
pub const SUBMIT_FLAGS: &[&str] = &[
    EXCLUSIVE,
    SHARED,
    VRAM,
    CATEGORY,
    COOPERATIVE,
    PRIORITY,
    TIMEOUT,
    END,
];
/// Flags followed by a value.
pub const VALUED: &[&str] = &[VRAM, CATEGORY, PRIORITY, TIMEOUT];

pub const APPLY_NOW: &str = "--apply-now";
pub const JSON: &str = "--json";
pub const FOLLOW: &str = "-f";

pub const SERVE_ON: &str = "on";
pub const SERVE_OFF: &str = "off";
pub const SERVE_STATUS: &str = "status";
pub const SERVE_ACTIONS: &[&str] = &[SERVE_ON, SERVE_OFF, SERVE_STATUS];

pub const SHELLS: &[&str] = &["bash"];

/// Offered for `set-profile` when the daemon cannot be asked.
pub const FALLBACK_PROFILES: &[&str] = &["mtp", "dflash2"];

pub const BASH: &str = r#"_gpu_run() {
    local IFS=$'\n'
    COMPREPLY=($(gpu-run __complete "$COMP_CWORD" "${COMP_WORDS[@]}" 2>/dev/null))
}
complete -o default -F _gpu_run gpu-run
"#;

const LIVE_TIMEOUT: Duration = Duration::from_millis(300);

/// `status` from the daemon, or nothing within the timeout.
pub fn live_status(socket: &Path) -> Option<Status> {
    let mut s = UnixStream::connect(socket).ok()?;
    s.set_read_timeout(Some(LIVE_TIMEOUT)).ok()?;
    s.set_write_timeout(Some(LIVE_TIMEOUT)).ok()?;
    let mut line = serde_json::to_vec(&Request::Status).ok()?;
    line.push(b'\n');
    s.write_all(&line).ok()?;
    let mut reply = String::new();
    BufReader::new(s).read_line(&mut reply).ok()?;
    serde_json::from_str::<Reply>(&reply).ok()?.status
}

/// Candidates for `words[cword]`; `words[0]` is the program. `live` is
/// asked at most once, with the socket the command line would use.
pub fn complete(
    words: &[String],
    cword: usize,
    env_socket: Option<PathBuf>,
    live: &dyn Fn(&Path) -> Option<Status>,
) -> Vec<String> {
    let cur = words.get(cword).map(String::as_str).unwrap_or("");
    let mut i = 1;
    let mut socket = env_socket;
    if words.get(1).map(String::as_str) == Some(SOCKET) {
        if cword == 2 {
            return Vec::new();
        }
        socket = words.get(2).map(PathBuf::from);
        i = 3;
    }
    let matching = |all: Vec<String>| -> Vec<String> {
        all.into_iter().filter(|c| c.starts_with(cur)).collect()
    };
    let strs = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    if cword == i {
        let mut all = strs(SUBCOMMANDS);
        if i == 1 {
            all.push(SOCKET.into());
        }
        return matching(all);
    }
    if cword < i {
        return Vec::new();
    }
    let sub = words[i].as_str();
    let before = &words[i + 1..cword];
    let prev = words.get(cword - 1).map(String::as_str).unwrap_or("");
    let status = || socket.as_deref().and_then(live);
    let own_ids = |active_only: bool| -> Vec<String> {
        status()
            .map(|s| {
                s.jobs
                    .iter()
                    .filter(|j| !active_only || !j.state.is_terminal())
                    .map(|j| j.id.to_string())
                    .collect()
            })
            .unwrap_or_default()
    };
    let all = match sub {
        SUBMIT => {
            if before.iter().any(|w| w == END) {
                return Vec::new();
            }
            match prev {
                CATEGORY => Category::ALL
                    .iter()
                    .map(|c| c.as_str().to_string())
                    .collect(),
                VRAM | PRIORITY | TIMEOUT => Vec::new(),
                _ => {
                    let has_mode = before.iter().any(|w| MODES.contains(&w.as_str()));
                    SUBMIT_FLAGS
                        .iter()
                        .filter(|f| !(has_mode && MODES.contains(f)))
                        .filter(|f| **f == END || !before.iter().any(|w| w == *f))
                        .map(|f| f.to_string())
                        .collect()
                }
            }
        }
        STATUS => {
            let mut v = if before.iter().any(|w| w != JSON) {
                Vec::new()
            } else {
                own_ids(false)
            };
            if !before.iter().any(|w| w == JSON) {
                v.push(JSON.into());
            }
            v
        }
        WAIT | CANCEL if before.is_empty() => own_ids(sub == CANCEL),
        LOGS => {
            let mut v = if before.iter().any(|w| w != FOLLOW) {
                Vec::new()
            } else {
                own_ids(false)
            };
            if !before.iter().any(|w| w == FOLLOW) {
                v.push(FOLLOW.into());
            }
            v
        }
        KILL if before.is_empty() => status()
            .map(|s| s.other_jobs.iter().map(|j| j.handle.clone()).collect())
            .unwrap_or_default(),
        SET_PROFILE if before.is_empty() => {
            let mut v: Vec<String> = status()
                .map(|s| s.profiles)
                .filter(|p| !p.is_empty())
                .unwrap_or_else(|| strs(FALLBACK_PROFILES));
            v.push(APPLY_NOW.into());
            v
        }
        SET_PROFILE | SET_HOST_CACHE if !before.iter().any(|w| w == APPLY_NOW) => {
            vec![APPLY_NOW.into()]
        }
        SERVE if before.is_empty() => strs(SERVE_ACTIONS),
        COMPLETIONS if before.is_empty() => strs(SHELLS),
        _ => Vec::new(),
    };
    matching(all)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Category as Cat, Counts, JobState, Mode, OtherJob, OwnJob, ServeStatus};
    use std::cell::Cell;

    fn w(line: &str) -> Vec<String> {
        let mut v: Vec<String> = line.split(' ').map(str::to_string).collect();
        if line.ends_with(' ') {
            v.pop();
            v.push(String::new());
        }
        v
    }

    fn at_end(line: &str, live: &dyn Fn(&Path) -> Option<Status>) -> Vec<String> {
        let words = w(line);
        complete(&words, words.len() - 1, Some("/sock".into()), live)
    }

    fn down(_: &Path) -> Option<Status> {
        None
    }

    fn own(id: u64, state: JobState) -> OwnJob {
        OwnJob {
            id,
            handle: format!("s{id}"),
            state,
            mode: Mode::Exclusive,
            vram: 0,
            category: Cat::Other,
            cooperative: false,
            priority: 0,
            position: None,
            behind_ninfer: false,
            cmd: vec!["true".into()],
            label: None,
            lease: false,
            cwd: "/".into(),
            log: "/l".into(),
            exit_code: None,
            signal: None,
            error: None,
            runner_pid: 1,
            pid: None,
            submitted_at: 0.0,
            started_at: None,
            finished_at: None,
            runs: 0,
        }
    }

    fn up(_: &Path) -> Option<Status> {
        Some(Status {
            summary: String::new(),
            serve: ServeStatus {
                phase: "OFF".into(),
                profile_current: None,
                profile_pending: None,
            },
            serve_wanted: false,
            owner: "queue".into(),
            profiles: vec!["alpha".into(), "beta".into()],
            host_cache_current_mib: None,
            host_cache_pending_mib: None,
            gpu_processes: 0,
            own: Counts::default(),
            others: Counts::default(),
            line: Vec::new(),
            jobs: vec![own(3, JobState::Queued), own(4, JobState::Done)],
            other_jobs: vec![OtherJob {
                handle: "n7".into(),
                state: JobState::Running,
                position: None,
                mode: Mode::Shared,
                vram: 1,
                cooperative: false,
                priority: 0,
            }],
            ninfer_error: None,
        })
    }

    #[test]
    fn subcommands_and_socket() {
        assert_eq!(
            at_end("gpu-run se", &down),
            ["set-host-cache", "set-profile", "serve"]
        );
        assert!(at_end("gpu-run ", &down).contains(&"--socket".to_string()));
        assert_eq!(
            at_end("gpu-run --socket /x sw", &down),
            ["switch-gpu", "switch-ninfer"]
        );
        assert!(at_end("gpu-run --socket ", &down).is_empty());
    }

    #[test]
    fn submit_flags() {
        assert_eq!(at_end("gpu-run submit --e", &down), ["--exclusive"]);
        let after_mode = at_end("gpu-run submit --shared ", &down);
        assert!(
            !after_mode.contains(&"--exclusive".into()) && !after_mode.contains(&"--shared".into())
        );
        assert!(after_mode.contains(&"--vram".into()));
        assert_eq!(
            at_end("gpu-run submit --exclusive --category t", &down),
            ["train", "test"]
        );
        assert!(at_end("gpu-run submit --shared --vram ", &down).is_empty());
        assert!(
            at_end("gpu-run submit --exclusive -- ", &down).is_empty(),
            "after -- bash's default"
        );
        assert!(at_end("gpu-run submit --exclusive -- python --", &down).is_empty());
    }

    #[test]
    fn static_values() {
        assert_eq!(at_end("gpu-run serve ", &down), ["on", "off", "status"]);
        assert_eq!(
            at_end("gpu-run set-profile ", &down),
            ["mtp", "dflash2", "--apply-now"]
        );
        assert_eq!(
            at_end("gpu-run set-host-cache 4096 ", &down),
            ["--apply-now"]
        );
        assert_eq!(at_end("gpu-run completions ", &down), ["bash"]);
    }

    #[test]
    fn daemon_down_offers_no_live_values() {
        assert!(at_end("gpu-run cancel ", &down).is_empty());
        assert!(at_end("gpu-run kill ", &down).is_empty());
        assert_eq!(at_end("gpu-run status ", &down), ["--json"]);
        assert_eq!(at_end("gpu-run logs ", &down), ["-f"]);
    }

    #[test]
    fn live_values_and_one_query() {
        assert_eq!(
            at_end("gpu-run cancel ", &up),
            ["3"],
            "only active jobs can be cancelled"
        );
        assert_eq!(at_end("gpu-run wait ", &up), ["3", "4"]);
        assert_eq!(at_end("gpu-run status ", &up), ["3", "4", "--json"]);
        assert_eq!(at_end("gpu-run kill ", &up), ["n7"]);
        assert_eq!(
            at_end("gpu-run set-profile ", &up),
            ["alpha", "beta", "--apply-now"]
        );
        let asked = Cell::new(None::<PathBuf>);
        let spy = |p: &Path| {
            asked.set(Some(p.to_path_buf()));
            None
        };
        let words = w("gpu-run --socket /given cancel ");
        complete(&words, words.len() - 1, Some("/env".into()), &spy);
        assert_eq!(
            asked.take(),
            Some(PathBuf::from("/given")),
            "--socket wins over the env"
        );
    }

    #[test]
    fn unreachable_socket_is_quiet_and_quick() {
        let t = std::time::Instant::now();
        assert!(live_status(Path::new("/nonexistent/gpu-switch.sock")).is_none());
        assert!(t.elapsed() < Duration::from_millis(400));
    }
}
