//! The harness's own guarantee: it signals a process only while the process is the one it recorded.

mod common;

use std::os::unix::process::ExitStatusExt;
use std::process::Command;
use std::time::Duration;

use common::gpu_claims::{registration, GpuProc};
use common::*;
use gpu_arbiter::procs::Process;

/// A fake GPU file naming `pid`, as the process that started at `birth` registers itself.
fn register(h: &Harness, pid: u32, birth: u64) {
    let usage = GpuProc::parse("mem=1 sm=0 type=C");
    std::fs::write(h.gpu_dir.join(pid.to_string()), registration(&usage, birth)).unwrap();
}

#[test]
fn the_drop_spares_a_process_given_a_recorded_pid_and_ends_the_recorded_one() {
    let h = Harness::start(
        "identity-claims",
        Opts {
            arbiter: false,
            fake_gpu: false,
            ..Opts::default()
        },
    );
    let mut ended = Command::new("true").spawn().unwrap();
    let recorded = spawned(&ended);
    ended.wait().unwrap();
    // A pid is given again only after its process has ended, so its next process starts at a later clock tick
    // (start times count ticks of 10 ms): two ticks pass before the unrelated sleeper starts.
    std::thread::sleep(Duration::from_millis(30));
    let mut unrelated = Command::new("sleep").arg("30").spawn().unwrap();
    assert!(spawned(&unrelated).birth > recorded.birth);
    // The record of a registrant that has ended names a pid an unrelated sleeper holds now: the pid reused.
    register(&h, unrelated.id(), recorded.birth);
    let mut registrant = Command::new("sleep").arg("30").spawn().unwrap();
    register(&h, registrant.id(), spawned(&registrant).birth);
    drop(h);
    assert_eq!(registrant.wait().unwrap().signal(), Some(libc::SIGKILL));
    assert!(
        unrelated.try_wait().unwrap().is_none(),
        "the drop signalled a process it never recorded"
    );
    unrelated.kill().unwrap();
    unrelated.wait().unwrap();
}

#[test]
fn the_drop_ends_the_process_group_a_running_job_leads() {
    let h = Harness::start(
        "identity-group",
        Opts {
            arbiter: false,
            ..Opts::default()
        },
    );
    let member = h.dir.join("member");
    let script = format!(
        "sleep 30 & echo $! > {}.tmp; mv {0}.tmp {0}; exec sleep 30",
        member.display()
    );
    let id = h.submit(Who::Host, EXCL, &["sh", "-c", &script]);
    wait_until("the job's member", || {
        member.exists()
    });
    let member = Process::now(
        std::fs::read_to_string(&member)
            .unwrap()
            .trim()
            .parse()
            .unwrap(),
    )
    .unwrap();
    wait_until("the job's pid", || {
        !h.job(Who::Host, id)["pid"].is_null()
    });
    let job = Process::now(h.job(Who::Host, id)["pid"].as_u64().unwrap() as u32).unwrap();
    drop(h);
    // Its runner would keep it for minutes without gpu-switch: only the drop ends it this soon.
    wait_until("the job ends", || !job.running());
    wait_until(
        "the member of its group ends",
        || !member.running(),
    );
}

/// The state letter and the start time of process `pid`, from `/proc/<pid>/stat`.
fn state_and_birth(pid: u32) -> Option<(char, u64)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    Some((fields.first()?.chars().next()?, fields.get(19)?.parse().ok()?))
}

#[test]
fn a_runner_waiting_to_reattach_and_its_job_group_end_with_the_harness() {
    let mut h = Harness::start(
        "reattaching-runner",
        Opts {
            arbiter: false,
            ..Opts::default()
        },
    );
    let command = h.held("--sm 50");
    let argv: Vec<&str> = command.argv().iter().map(String::as_str).collect();
    let id = h.submit(Who::Host, EXCL, &argv);
    h.wait_job_state(Who::Host, id, "running");
    // Admission sets `running` before the runner reports `Started`: the job's pid is the job value that names it.
    let mut job = serde_json::Value::Null;
    wait_until("the job's published pid", || {
        job = h.job(Who::Host, id);
        !job["pid"].is_null()
    });
    let named = |pid: &serde_json::Value| Process::now(pid.as_u64().unwrap() as u32).unwrap();
    let (runner, leader) = (named(&job["runner_pid"]), named(&job["pid"]));
    let (runner_fd, group_fd) = (runner.open().unwrap(), leader.open().unwrap());
    // A stopped runner reads nothing, so it never sees gpu-switch leave and never starts its reattachment patience.
    assert!(runner_fd.signal(libc::SIGSTOP));
    wait_until("the runner stopped", || state_and_birth(runner.pid) == Some(('T', runner.birth)));
    h.stop_switch();
    drop(h);
    assert!(runner_fd.exited(), "the runner waiting to reattach survived the harness");
    assert!(group_fd.exited(), "the job's group survived the harness");
}

/// One read of a held record: bytes it yields, or the point where it reports that it is held and blocks until its
/// test releases it.
enum Step {
    Yield(Vec<u8>),
    Hold,
}

/// A record's reader that its test holds part-way: it yields its steps in order.
struct HeldRecord {
    steps: std::collections::VecDeque<Step>,
    report: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

impl std::io::Read for HeldRecord {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.steps.pop_front() {
                Some(Step::Yield(bytes)) => {
                    buf[..bytes.len()].copy_from_slice(&bytes);
                    return Ok(bytes.len());
                }
                Some(Step::Hold) => {
                    self.report.send(()).unwrap();
                    self.release.recv().unwrap();
                }
                None => return Ok(0),
            }
        }
    }
}

#[test]
fn a_start_record_being_written_is_not_read() {
    let h = Harness::start(
        "start-record-held",
        Opts {
            arbiter: false,
            ..Opts::default()
        },
    );
    let argv: Vec<String> = ["ninfer-mock", "--spec", "mtp"].map(String::from).to_vec();
    let record = serde_json::to_vec(&argv).unwrap();
    let (first, rest) = record.split_at(record.len() / 2);
    let (report, held) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let reader = HeldRecord {
        steps: [Step::Yield(first.to_vec()), Step::Hold, Step::Yield(rest.to_vec())].into(),
        report,
        release: released,
    };
    let directory = h.start_records.clone();
    let writer = std::thread::spawn(move || start_records::publish(&directory, reader));
    held.recv().expect("the writer reports that it is held");
    assert!(h.starts().is_empty(), "a record being written was read");
    let entries: Vec<std::fs::DirEntry> =
        std::fs::read_dir(&h.start_records).unwrap().map(Result::unwrap).collect();
    assert_eq!(entries.len(), 1, "the directory holds the record being written alone");
    let name = entries[0].file_name().into_string().unwrap();
    assert!(name.parse::<u64>().is_err(), "the record being written is under a sequence number: {name}");
    assert_eq!(std::fs::read(entries[0].path()).unwrap(), first, "the observation was made mid-write");
    release.send(()).unwrap();
    writer.join().unwrap().expect("the held record is published once released");
    assert_eq!(h.starts(), [argv]);
}

#[test]
fn start_records_are_read_in_their_sequence() {
    let h = Harness::start(
        "start-record-sequence",
        Opts {
            arbiter: false,
            ..Opts::default()
        },
    );
    let argvs: Vec<Vec<String>> = (0..12).map(|n| vec!["ninfer-mock".to_string(), format!("--start-{n}")]).collect();
    for argv in &argvs {
        start_records::record(&h.start_records, argv).unwrap();
    }
    assert_eq!(h.starts(), argvs, "the records come back in the order they were made");
}
