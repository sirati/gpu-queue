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
