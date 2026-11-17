# Native compiler and GPU APIs

`gpu_queue` is a Rust PyO3 extension. The compiler daemon is `compile-queue`.
It executes the submitted compiler executable directly. There is no compiler
worker executable, Python scheduler, Python socket transport, or old-state import.

## Run the compiler service

ML-project starts the packaged `doggy-compile` user service when entering its
Nix environment. The Python API only connects to its socket. To apply/start the
packaged service directly, run `compile-queue --activate-user-service`. It needs
no sudo. Changed package declarations request a graceful replacement; unchanged
declarations only start the existing service. Compilation subprocesses cannot
activate their own service.

Run one `compile-queue` per user under a service manager. Its default state is
`/var/tmp/rust-compile-<uid>`. The service binds a Linux abstract Unix socket
whose name includes the canonical state directory. No socket file or lock file
can survive a crash and obstruct startup. A second daemon for the same state is
refused by the kernel before it can change queue metadata.

```
compile-queue --state-dir /var/tmp/rust-compile-1000 \
  --minimum-ram 10737418240 --average-ram 1073741824
```

`--minimum-ram` preserves host memory headroom. `--average-ram` is the default
reservation for a command that omits `memory_bytes`. Explicit per-command memory
estimates let smaller compilations fit alongside larger ones. The daemon also
respects its CPU affinity, measured idle CPU capacity, existing command RSS,
its cgroup's memory ceilings. It leaves one CPU core free and fills the remaining
eligible compilation slots on each scheduling pass. A group limit restricts that
group's concurrent commands, not the whole daemon.

Slots are shared between parties (the submitter's session unless a submission names
a `party`), not between groups: each free slot goes to the party with ready work
that holds the fewest running commands, and equal holdings go to the party whose
last turn is oldest. Within a party its groups are served in submission order, so
earlier rows finish first; a hand-over keeps that order. The aged memory reservation
walks the same order, and a reservation already held keeps its job. What a party was served
before never counts, so a long-running large campaign keeps its share of slots while
a newly arrived small one takes its turn beside it. Priority weighs the share: a
party at priority p holds 1+p times the slots of a priority-0 party (1/(1-p) below
zero). One party never takes more than two thirds of the eligible slots. Health
lists each group's `party_running` and `party_last_turn`.

The state format is versioned and belongs exclusively to this implementation.
The daemon rejects unrelated state instead of interpreting old queue files.

## Submit compilations together

```python
from pathlib import Path
from gpu_queue import CompileJob, CompileGroup, CompilerQueue

cwd = str(Path.cwd())
queue = CompilerQueue()
objects = [
    CompileJob(
        f"feature-object-{index}",
        ["cc", "-c", source, "-o", f"object-{index}.o"],
        cwd,
        expected_runtime_secs=30,
        memory_bytes=512 << 20,
    )
    for index, source in enumerate(["first.c", "second.c", "third.c"])
]
link = CompileJob(
    "feature-link",
    ["cc", "object-0.o", "object-1.o", "object-2.o", "-shared", "-o", "feature.so"],
    cwd,
    expected_runtime_secs=10,
    dependencies=[job.job_id for job in objects],
    memory_bytes=1 << 30,
)
group = CompileGroup("feature-build-source-commit", [*objects, link])
queue.submit(group)
# Submit other independent groups here before waiting.
status = queue.wait(group.group_id)
```

Submit all independent compiler commands in one group, or publish several groups
before waiting. Each command receives one CPU and has CUDA devices hidden.
OpenMP, MKL and OpenBLAS thread counts are one. A command must represent one
compilation, not a controller which submits another group and waits for it.
Do not invoke a parallel build driver inside one command; expose its independent
compiler invocations as jobs so the daemon can schedule them across cores.

A dependency starts only after every prerequisite succeeds. A failed or cancelled
prerequisite produces a terminal dependency failure without running the dependent
command. Cycles and unknown dependencies are rejected before publication.
`max_running=0` means no additional group limit. Set a positive limit only when
that group needs a specific resource bound.

Every command declares `expected_runtime_secs`, the seconds it runs. There is no
default: a group with an undeclared command is refused at submission. A command
declaring more than 10 GB of memory is accepted with a warning, because admission
serves requests that large last and may delay them indefinitely; verify the work
needs that much and declare less where it can. While an aged command holds the
memory reservation, a younger command runs ahead only if its memory fits and its
declared runtime is at most what the longest running command has left by its
own declaration. `HostMemoryClaim` requests above 10 GB receive the same warning
when queued.

`CompileJob` accepts `env`, `inherit_environment`, and string `metadata` as well
as the arguments above. With `inherit_environment=False`, provide the complete
environment required by the executable, including its `PATH` where necessary.
IDs are durable and globally unique within this compiler state. Repeating an
identical group publication is idempotent; changing a published declaration under
the same identity is an error. Use a new identity for intentionally new work.

## Results, errors and cancellation

`queue.status(group_id)` returns a `CompileStatus`. Its `jobs` are typed
`CompileResult` values with `job_id`, `group_id`, `outcome`, `returncode`, `detail`,
`queued_at`, `started_at`, `finished_at`, `execution_seconds`, and `log_path`.
Each finished result also reports its contract: `expected_runtime_secs`,
`peak_memory_bytes`, `p90_memory_bytes`, `median_memory_bytes`, `median_cores`
(at least 1) and `p90_cores`, sampled from the command's memory cell, and
`warnings` naming each violation: a runtime more than 50% over the declaration,
under half of it, or a peak more than 25% below the declared memory.
Timestamps are Unix seconds. `complete` means all commands are terminal;
`successful` means they all succeeded. A running result also exposes `pid`.

`queue.wait()` releases the Python interpreter while Rust waits and stays pending
until every command is terminal; a busy or restarting daemon delays it, never fails
it. Python signals still interrupt the wait without withdrawing work. Scheduling
clients use no request deadline; `CompilerQueue(request_timeout=...)` exists only
for service probes. A command running past twice its `expected_runtime_secs` is ended and fails with
that reason; queue waiting time never ends a compilation. By default, unsuccessful terminal results raise
`CompilationFailed`, with the full status attached as `exception.status`.
Use `check=False` to inspect results without raising. An actual non-zero exit is
recorded with its exact exit code or negative signal number, plus its log.

```python
queue.cancel(group.group_id, "owner no longer needs pending compilation")
queue.cancel(group.group_id, "owner cancelled this command",
             job_id="feature-link", include_running=True)
```

Cancellation preserves running commands unless explicitly requested otherwise.
An explicit running cancellation sends SIGINT, then SIGTERM after
one second, then SIGKILL after another second. Process birth identities and pidfds
fence signal delivery. Terminal receipts retain their original reason and cannot
be overwritten by a later cancellation. Cancellation is never success.

On graceful service shutdown, the daemon stops admitting new commands and drains
its direct children, preserving their exact exit statuses. Queued commands remain
for the replacement daemon. After a crash, identified commands retain custody
until they exit and are never duplicated. If the old parent died before recording
the exit status, the result is `lost`, with `returncode=None` and an explicit
unknown-cause explanation. It does not invent SIGKILL or claim success. A durable
dispatch intent also prevents an ambiguous crash during spawn from causing an
automatic second execution.

## CPU preparation and GPU admission

The compiler queue owns compilation, and the GPU daemon owns GPU leases. Finish
compilation and CPU preparation before acquiring a GPU lease. Never wait for a
compiler result, host reservation, or another queue while holding a GPU lease.

GPU preparation executes outside the compiler queue without reserving compiler
CPU slots or changing CPU affinity. Preparation finishes before requesting the
GPU lease. Only actual compilation commands consume compiler slots.

Acquire the GPU through the native API only after all prerequisites are ready:

```python
from gpu_queue import Lease

with Lease(
    mode="shared",
    vram=4 << 30,
    expected_runtime_secs=20,
    category="test",
    label="kernel feature correctness source-commit",
    priority=0,
) as lease:
    run_gpu_check(prepared)
    release_all_owned_gpu_allocations()
```

Shared jobs declare positive VRAM bytes. Exclusive jobs use `mode="exclusive"`
and `vram=0`. Both modes declare an expected duration in seconds. Omitting it
warns; `expected_runtime_secs=-1` explicitly declares no estimate and suppresses
that warning. Positive estimates enable bounded shared backfilling: a shared job
may pass a blocked waiter only if its VRAM fits and its estimate fits the longest
remaining declared runtime among active jobs.

Use `category="benchmark"` for timing, `"accuracy"` for numerical accuracy,
`"test"` for correctness or resource checks, and `"other"` for other work.
Priority ranges from -10 through 10, with zero as the default. Positive priority
may advance work according to the GPU queue's fairness rules; it does not bypass
capacity or exclusivity. Names should identify the kernel, feature, operation and
actual source commit.

Allocate VRAM within one second of admission and start GPU computation within
three seconds. Prepare imports, fixtures and CPU reference data before admission.
Every owned VRAM allocation must be gone before normal release. A context with
zero externally measured VRAM is allowed. The watchdog rules, the GPU usage
contract, apply to shared and exclusive jobs alike. A grant that breaks one keeps
running; the holder sees `GPU USAGE CONTRACT VIOLATED` on stderr and the broken
rules in `lease.violations`, and must fix its use of the GPU.

If an admitted operation discovers missing compiled code, propagate the exception
through the `Lease` context so the attempt is recorded as failed. Alternatively,
a caller deliberately withdrawing an incomplete attempt calls
`lease.cancel("missing compiled object; continuation required")`. Cancellation
releases the lease and records the reason instead of marking the attempt done.
Only after leaving the lease should the caller publish and wait for the missing
compilation. A future GPU attempt acquires a new lease. `__exit__` preserves the
explicit cancellation instead of replacing it with success.

For graceful cleanup when the queue ends a grant (a kill, cancellation, runtime
limit or cooperative yield), pass `shutdown_handler=save_checkpoint`.
`GpuLeaseInterrupted` includes the reason, request identity and deadline. The
context manager acknowledges and runs the handler while unwinding. A caller that
catches the exception inside the context calls `lease.run_shutdown_handler()`;
a caller managing cleanup itself acknowledges with
`lease.shutdown_started(request_id)`. Cleanup does not renew the GPU grant.

## Python packaging

The Nix Python package is built with the interpreter supplied by its package set.
Use the flake overlay's `pythonPackagesExtensions`, or call `python-package.nix`
from your selected Python package set. There is no fixed Python minor version.
The transport, scheduling and GPU client are Rust; the Python package initializer
only exports PyO3 classes.

## Claim host RAM before fixture preparation

`HostMemoryClaim` reserves one aggregate byte budget for a native preparation
owner. It does not submit a command, acquire a GPU lease, reserve compiler slots,
or change CPU affinity. The Rust authority serializes RAM claims against compiler
RAM reservations while maintaining the service's configured memory headroom.

```python
from gpu_queue import HostMemoryClaim

with HostMemoryClaim(memory_bytes=batch_peak_bytes, label="both-source fixtures") as memory:
    # Prepare only after __enter__ grants the entire batch budget.
    fixtures = prepare_batch()
    workers = start_workers_waiting_for_inputs()
    for worker in workers:
        memory.track_worker(worker.pid)
    try:
        supply_prepared_inputs(workers, fixtures)
        collect_results(workers)
    finally:
        stop_and_join_owned_workers(workers)
        del fixtures
```

Include every source, seed, retained fixture, worker-private copy, and transient
preparation allocation in `batch_peak_bytes`. Partition a campaign into batches
that fit the host. A request exceeding usable host/cgroup capacity fails before
preparation. Only one aggregate claim may be outstanding per process; overlapping
process-tree claims are rejected. Incremental partial acquisitions cannot be used
to hold some RAM while waiting for the rest. A pending claim stays queued until
granted; interrupting its owner withdraws it.

Register each child immediately after spawning, before it starts preparation.
The authenticated original owner alone can update or release its claim. Release
refuses living registered workers; join them first. A crashed owner automatically
releases its claim after registered workers exit. Living orphan workers remain
charged, so an owner crash cannot allow another preparation to reuse their RAM.
A dropped handle retires its claim using the same worker-lifetime rule.

Claims use process birth identities and survive separate-process service handover
and daemon restart. No filesystem ownership locks or Python scheduling state are
used. Anonymous proportional resident memory accounts for fork-shared pages once;
unavailable measurement receives no credit against the declared reservation.
`CompilerQueue().health()['host_memory']` reports protocol 1, claimed bytes,
unallocated bytes, pending/granted counts, owner PIDs, and registered workers.

### What a claim used

Admission observes every granted claim at its 250 ms ticks and reports two
figures in the claim's status, in health and in the answer to its release:

- `observed_peak_bytes`: the largest anonymous memory admission charged the
  claim above its grant-time baseline. This is the quantity admission counts.
- `observed_high_water_bytes`: the sum, over the processes seen in the claim's
  tree, of each one's largest resident set (`VmHWM`). A release carries the
  releasing process's own final `VmHWM`, which the service takes as its value;
  `release()` returns the service's answer.

Neither figure is a bound: a child that peaks and exits between two ticks, or
lives less than one, can be missed. Only a memory cgroup per claim would give a
complete lifetime peak, and claims run in their campaign's own unit.

The claim's `warnings` name a use beyond the declaration ("used more than its
declared X bytes", definite, since a sample never exceeds the truth) and, as
information, an observed high-water whose 1.25x is below 0.75x the
declaration. The declaration stays its owner's to change.

Health lists `capabilities` a client may rely on beyond protocol 1, which
gen-16 clients require exactly: `peaks` reports these figures. Capabilities
are never removed.

### Declared purpose

An acquisition may declare `purpose`: `gpu_place` (held for work next on the
GPU) or `host_only` (held only for host work, which ends). Absent means
unknown, which is what every generation-16 client sends and what state saved
by generation 16 restores as. Status and health show it, and a repeated
acquisition with a different purpose is refused as conflicting. No admission
rule reads it. Python acquisition declares none.

### The `host-claim` front-end, and handing a claim on

```
host-claim --bytes N [--label L] [--priority P] [--state-dir D] (--gpu-place | --host-only) \
           [--cancel-file PATH] -- ARGV
```

`host-claim` waits for the claim in a process that holds nothing else, then
executes ARGV in the same process with
`HOST_MEMORY_CLAIM={"claim_id","memory_bytes","state_dir"}`. It needs the
`transfer` and `purpose` capabilities. It exits 125 when the cancel file
appears while it waits (within a second), 75 when the compile service does
not answer within 60 s or lacks a capability (an outage, never a verdict),
and 2 on a usage error, including `GPU_QUEUE_PLACE` set without `--gpu-place`.

The executed program adopts the claim with `HostMemoryClaim.inherited()`,
which checks that it is granted, owned by this process and of the stated
bytes, and may give it to a live descendant that owns no claim with
`claim.transfer(pid)`. The service saves a transfer before it answers, and
answers a resent transfer from the previous owner naming the current one
again; anything else the previous owner asks is refused with "transferred
to <pid>". An unreachable service or a missing capability raises
`gpu_queue.AuthorityUnavailable`; a refusal raises `RuntimeError`.
`gpu_queue.LARGE_REQUEST` is the 10 GB large class.

### Result sinks

`CompileGroup(..., result_sink=DIR)` names a directory the submitter owns.
When the group completes, the service writes its terminal results, the
status answer, atomically to `DIR/<group_id>.results.json` before any waiter
is answered and before the group can retire, so a campaign that resumes
after the retention still finds them. A sink that cannot be written (a
directory that does not exist) fails only that publication; the group's
status gives `result_sink_error`.

These are service generation 17; `--handoff-capabilities` is unchanged and
a successor of a lower generation is refused.
