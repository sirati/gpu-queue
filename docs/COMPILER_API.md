# Native compiler and GPU APIs

`gpu_queue` is a Rust PyO3 extension. The compiler daemon is `compile-queue`.
It executes the submitted compiler executable directly. There is no compiler
worker executable, Python scheduler, Python socket transport, or old-state import.

## Run the compiler service

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
host preparation reservations, and its cgroup's memory ceilings. It fills all
eligible CPU slots on each scheduling pass. Groups rotate fairly; a group limit
restricts that group's concurrent commands, not the whole daemon.

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
        memory_bytes=512 << 20,
        timeout_seconds=300,
    )
    for index, source in enumerate(["first.c", "second.c", "third.c"])
]
link = CompileJob(
    "feature-link",
    ["cc", "object-0.o", "object-1.o", "object-2.o", "-shared", "-o", "feature.so"],
    cwd,
    dependencies=[job.job_id for job in objects],
    memory_bytes=1 << 30,
)
group = CompileGroup("feature-build-source-commit", [*objects, link])
queue.submit(group)
# Submit other independent groups here before waiting.
status = queue.wait(group.group_id, timeout=600)
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
Timestamps are Unix seconds. `complete` means all commands are terminal;
`successful` means they all succeeded. A running result also exposes `pid`.

`queue.wait()` releases the Python interpreter while Rust waits. Its timeout does
not withdraw or resubmit work. By default, unsuccessful terminal results raise
`CompilationFailed`, with the full status attached as `exception.status`.
Use `check=False` to inspect results without raising. An actual non-zero exit is
recorded with its exact exit code or negative signal number, plus its log.

```python
queue.cancel(group.group_id, "owner no longer needs pending compilation")
queue.cancel(group.group_id, "owner cancelled this command",
             job_id="feature-link", include_running=True)
```

Cancellation preserves running commands unless explicitly requested otherwise.
Running cancellation and compilation timeouts send SIGINT, then SIGTERM after
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

For CPU preparation that needs a bounded host reservation:

```python
with queue.host(memory_bytes=2 << 30, cpus=1, timeout=60) as reservation:
    prepared = prepare_inputs_on_cpu()
# The host reservation is released before waiting for compiler or GPU admission.
```

This context reserves memory and CPU cores through the Rust actor and applies the
calling thread's CPU affinity. It restores affinity on exit, including an
exception. Tokens identify one reservation and cannot be reused after release.
Owner death also releases capacity. Do not wait for compilations while reserving
the cores those compilations need.

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
zero externally measured VRAM is allowed. Idle watchdog rules apply to both
shared and exclusive jobs.

If an admitted operation discovers missing compiled code, propagate the exception
through the `Lease` context so the attempt is recorded as failed. Alternatively,
a caller deliberately withdrawing an incomplete attempt calls
`lease.cancel("missing compiled object; continuation required")`. Cancellation
releases the lease and records the reason instead of marking the attempt done.
Only after leaving the lease should the caller publish and wait for the missing
compilation. A future GPU attempt acquires a new lease. `__exit__` preserves the
explicit cancellation instead of replacing it with success.

For graceful watchdog cleanup, pass `shutdown_handler=save_checkpoint`.
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

## Native dependency plans

The native `Pipeline(compile_group, gpu_jobs, max_running=...)` declaration lets a
runner publish its dependency graph once. Each continuation is a `CompileJob`
command declaration whose `dependencies` name compiler jobs in that group.
`queue.submit_pipeline(plan)`, `queue.pipeline_status(group_id)` and
`queue.wait_pipeline(group_id, check=False)` use the same Rust socket client.
The continuation executable runs directly, after its own prerequisites succeed.
It owns its native `Lease` acquisition after CPU preparation; the compiler actor
never obtains a GPU lease. `max_running` bounds concurrent continuation processes
independently of the compile group's limit. Their startup memory reservations
also remain independent of compilation CPU slots. The unified kernel runner
supplies the exact source interpreter, environment and native lease declaration.
No legacy GPU wrapper or alternate admission path is supported.
