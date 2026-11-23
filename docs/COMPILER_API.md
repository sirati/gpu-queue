# Native compiler and GPU APIs

`gpu_queue` is a Rust PyO3 extension. The compiler daemon is `compile-queue`.
It executes the submitted compiler executable directly. There is no compiler
worker executable, Python scheduler, Python socket transport, or old-state import.

## Run the compiler service

Entering ML-project's Nix environment performs no service operation. Its runner
owner deploys a pin explicitly and verifies it (ML-project's
`tools/kernel_testing/README.md`, section 9, "The compile service"). The Python
API only connects to its socket. To apply/start the packaged service directly,
run `compile-queue --activate-user-service`. It needs
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
reservation for a command that omits `memory_bytes`. `--overcommit-percent` sets
the overcommit budget (below, Memory admission). `--preflight-patience-secs N`
(whole seconds, default 2) bounds an upgrade's capability preflight of its
successor; a successor that does not answer within it is refused. It is general
configuration, not a test switch, and a hand-over passes it to the successor.
`--request-deadline-secs N` (whole seconds, default 30) bounds how long a
connection may take to send its request line, and then to accept its reply; it
never bounds the awaited work. It is general configuration as well, passed on
at a hand-over. Explicit per-command memory estimates let smaller compilations fit alongside
larger ones. The daemon also
respects its CPU affinity, measured idle CPU capacity, existing command RSS,
its cgroup's memory ceilings. It leaves one CPU core free and fills the remaining
eligible compilation slots on each scheduling pass. A group limit restricts that
group's concurrent commands, not the whole daemon.

Slots are shared between parties (the systemd unit of the submitting process unless a submission names
a `party`), not between groups: each free slot goes to the party with ready work
that holds the fewest running commands, and equal holdings go to the party whose
last turn is oldest. Within a party its groups are served in submission order, so
earlier rows finish first; a hand-over keeps that order. What a party was served
before never counts, so a long-running large campaign keeps its share of slots while
a newly arrived small one takes its turn beside it. Priority weighs the share: a
party at priority p holds 1+p times the slots of a priority-0 party (1/(1-p) below
zero). One party never takes more than two thirds of the eligible slots. Health
lists each group's `party_running` and `party_last_turn`. The shares count running
commands that declare a runtime and started in this boot; a command restored from before
runtimes were declared is held outside the service, like memory any other process holds.

A host claim's party is the systemd unit of the process that registered it: the deepest component of its
cgroup path that ends in `.service` or `.scope`. This is a naming convention, not a lookup: a delegated
sub-cgroup named with such a suffix is a party of its own. The party is read once, at registration, saved with
the claim, and never changes, also when the process moves. A claim saved by generation 19 or earlier takes the
unit of its current owner when a successor first restores it, saved before that successor serves; an
unreadable recorded process refuses that restore, and a claim each of whose processes is gone is
`unit-unknown` and ends at the first pass. Groups submitted before generation 20 keep the party they were
given.

### Memory admission

Each scheduling pass computes once what may be admitted (the smaller of the sample's and
a fresh reading of available memory, less the headroom, running commands' unused cells and
granted claims' unused declarations, kept signed) and serves the classes in order, each
from what the one before left:

1. The aged head (below), then ordinary claims and each party's next ordinary command by
   party share, then the ordinary launches by slot shares, then ordinary claims still
   pending.
2. Requests above 10 GB, compiles before claims, from what every smaller request left.
   They are never reserved for.

A `gpu_place` claim is ordinary: nothing ties it to a told GPU place, so it is not known
to be for work next on the GPU. The GPU pass, which would serve such claims first and
reserve for the first one waiting, therefore admits none: each record's `gpu` is empty
and health's `heads.gpu` is null.

What the service's own work can free, S, is what is left plus the declarations of its
work that ends: the cells of running commands with a runtime, and granted `host_only`
claims. Memory held by `gpu_place` claims, by claims of unknown purpose or outside the
service is not counted.

An ordinary request that the share passes keep passing over gets the aged head: a pending
claim, or a command waiting for memory (its cell exceeds what is left and what the last
pass's launches left), 30 s past its eligibility and at most S. The turn to hold it
rotates over the parties with such a request in name order, and a party's oldest request
goes first. The head's bytes are reserved against every younger admission; a claim head
that fits is granted at once, and a command head launches by the slot shares like any
command, with its reservation. While the head does not fit, a younger command runs ahead
into its bytes only if, from its own admission, it is declared to finish by the latest
declared finish of the running commands with a runtime contract, and the head then waits
for it too. A head waits only for work the service admitted and that
ends: once that work has ended and the head still does not fit, it is released and gives
way until the work then running has ended; a fitting command head is released when what it
lacks (a slot, its group's or party's limit) is held by nothing that ends. Nothing of this
is saved: after a restart the oldest request gets the first head.

Each pass that changes a request's decision, admits anything, changes a head or the turn,
or in which a head reserves writes one `compile-queue: admission {...}` record to standard
error with every input of its decisions and the service instance that wrote it (`instance`:
its boot id, pid and start ticks); health's `admission` repeats the last one and
`heads` shows the GPU head, the aged head and the turn. `compile-queue
--check-admission-records < journal` checks a journal's records against these rules, each
instance's records from that instance's first, and `--derive-admission-records < journal`
prints each record as the check derives it from its inputs and decisions. Each generation checks
the records its own rules wrote: after a hand-over across generations, check each generation's
instances with that generation's executable (a generation-19 check refuses a generation-18 GPU turn,
whose class generation 19 no longer has). Each record carries `number`, its place among the records its
instance wrote, from 1. It is a diagnostic: health's `admission` shows the last one. The check reads no
rule from it, and a journal missing records is not proved complete or incomplete by it. A claim request is named by its ID and its registration
order; health lists each claim's `order`. A generation-20 check refuses records in which a registration's party
changes or a head's grant names another party.

### The overcommit budget

From generation 21, `--overcommit-percent P` (default 25, at most 50) sets the overcommit
budget B to P percent of `--minimum-ram`: the part of the floor admission may lend. A larger
value refuses to start, with exit status 2, before the state directory, the endpoint or any
state is touched; a hand-over passes the option to its successor. B is 0 while the service
runs unconfined, since nothing then caps a cell.

A command whose `expected_memory_bytes` F is below its cell C is admitted with F from the
budget and C − F lent, when what B can still lend (B less the lending running attempts hold)
holds C − F and the budget holds F; otherwise it is admitted at its whole cell, as every
command without an expected figure is, or waits. One rule decides every launch. The cell is C
either way: `memory.max` is C, no `memory.high` is written, and out-of-memory growth raises it
exactly as before. A command that grew is admitted at its whole grown cell.

Each running attempt holds its cap (`memory_bytes`) as an ordinary reservation plus its lending
(`lending_bytes` in its running record, absent when 0). The budget adds back min(Σ lending, B),
so an attempt costs the budget exactly its ordinary reservation, which is also what the party
RAM shares and S count. When an attempt is killed for outgrowing its cell, each sibling's raise
is the same as without B; it is funded from the dead attempt's ordinary reservation first and
then from its lending, which moves to the sibling as lending. Lending inherited above the
current B (after a restart with a lower floor or percent, an unconfined restart, or the
rollback build) is charged as ordinary until its attempts end, and nothing new is lent while
Σ lending ≥ B. Restore refuses a running record whose `lending_bytes` is not an integer or
exceeds its `memory_bytes`, and states the bound in one line, `compile-queue: the overcommit
budget is B bytes; running attempts hold L bytes of lending, A bytes added back, E bytes
charged as ordinary until their attempts end`.

Health's `lending` reports `overcommit_bytes` (B), `lending_bytes` (Σ lending),
`added_back_bytes` and `charged_as_ordinary_bytes`, with or without a resource sample. Each
admission record carries `overcommit` and `lending` as facts, and each launch its `expected`
figure (its cell when it declares none) and `lent`, which the check derives from them. A result
carries `expected_memory_bytes` when its job declares one, and `lending_bytes` when its attempt
held lending at its end. Health's capabilities name `expected_memory`; a client refuses locally
to submit a group with an expected figure to a service that does not name it. The rollback
build lends nothing whatever its configuration, charges inherited lending as ordinary until its
attempts end, and keeps every field above.

The state format is versioned and belongs exclusively to this implementation.
The daemon rejects unrelated state instead of interpreting old queue files.

## Submit compilations together

```python
from pathlib import Path
from gpu_queue import CancelToken, CompileJob, CompileGroup, CompilerQueue

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
status = queue.wait(group.group_id, CancelToken())
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
needs that much and declare less where it can. `HostMemoryClaim` requests above
10 GB receive the same warning when queued.

`CompileJob` accepts `env`, `inherit_environment`, and string `metadata` as well
as the arguments above. With `inherit_environment=False`, provide the complete
environment required by the executable, including its `PATH` where necessary.
IDs are durable and globally unique within this compiler state. Repeating an
identical group publication is idempotent; changing a published declaration under
the same identity is an error. Use a new identity for intentionally new work.

`CompileJob(…, expected_memory_bytes=None)` may declare the memory a command is
expected to use, at most its cell. A command that declares it declares its
`memory_bytes` too: an expected figure of 0, above `memory_bytes`, or beside no
`memory_bytes` is refused before publication. The cell stays `memory_bytes`, which
caps the command as before, and a command that grew after an out-of-memory kill is
admitted at its whole grown cell. Admission reads the expected figure from compile
service generation 21 on; before it, the figure changes nothing.

## Results, errors and cancellation

`queue.status(group_id)` returns a `CompileStatus`. Its `jobs` are typed
`CompileResult` values with `job_id`, `group_id`, `outcome`, `returncode`, `detail`,
`queued_at`, `started_at`, `finished_at`, `execution_seconds`, and `log_path`.
Each finished result also reports its contract: `expected_runtime_secs`,
`peak_memory_bytes`, `p90_memory_bytes`, `median_memory_bytes`, `median_cores`
(at least 1) and `p90_cores`, sampled from the command's memory cell, and
`warnings` naming each violation: a runtime more than 50% over the declaration,
under half of it, or a peak more than 25% below the declared memory.
A result the service reaped also carries `attempt` (`CompileResult.attempt`, a
`CompileAttempt`): the facts of the attempt it concludes, as the instance that
concluded it saw them. `launch` (`CompileLaunch`) holds its cell (`cell_bytes`),
its expected figure (`expected_bytes`, the cell when the command declares none or
grew) and what the overcommit budget lent it (`lending_bytes`); `moves`
(`CompileMove`) lists each raise it received from an out-of-memory-killed sibling,
in order, with when (`at`), the sibling's `donor_job_id` and `donor_started_at`, the
lending that moved and its cap after the raise; `cause` is `success`, `failure`,
`out_of_memory`, `cancelled` or `lost`; `peak_reset` says that its cell's memory
controller was restored while it ran, so `peak_memory_bytes` counts from then. The
attempt's identity is its `job_id` and `started_at`, its cap at the end is
`memory_limit_bytes` and its lending at the end `lending_bytes`. What a result
cannot state, it states as unknown: an attempt the instance restored (across a
restart or a hand-over, or an intent restored before its process spawned) has
`launch` null and lists only the moves after the restore, and says nothing about
whether a process spawned; earlier attempts of the job, such as one killed for
outgrowing its cell and run again, appear in no result, and nothing counts them.
A failed spawn's, a pending job's and a running job's results carry no `attempt`.
Nothing in admission reads it.

Timestamps are Unix seconds. `complete` means all commands are terminal;
`successful` means they all succeeded. A running result also exposes `pid`.

`queue.wait(group_id, token)` releases the Python interpreter while Rust waits and
stays pending until every command is terminal; a busy queue never fails it, and a
restart or hand-over reconnects and asks again. Beside the parked wait Rust probes
the service's health every 5 s. When the service stays unanswered for 60 s (the
outage's start counted, a pending probe included), or the wait cannot be written to
it within 60 s, the wait raises `gpu_queue.AuthorityUnavailable`: an outage to stop
on and resume after, never a verdict. Detecting an outage that only a probe sees
adds at most one probe interval. Every other request (`submit`, `status`, `cancel`,
`health`) is answered within 60 s or raises `AuthorityUnavailable`;
`CompilerQueue(request_timeout=...)` gives a service probe a deadline of its own.
`AuthorityUnavailable` is a `RuntimeError` and names its `source` and `operation`;
a refusal or an invalid request raises `RuntimeError`. A missing state directory
raises `AuthorityUnavailable` when the client is constructed.

`token` is a `gpu_queue.CancelToken`. Cancelling it, from any thread and before or
during the wait, ends the wait locally with `asyncio.CancelledError`; the service
keeps the group, and a later wait reattaches. A caller that nothing cancels passes
a fresh token. Python signals still interrupt the wait without withdrawing work. A command running past twice its `expected_runtime_secs` is ended and fails with
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

# The context manager acquires with a token of its own; `memory.acquire(token)` takes a CancelToken.
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
granted; interrupting its owner, or cancelling the token given to `acquire`
(`asyncio.CancelledError`), withdraws it. The wait for the grant has the same
outage rule as `queue.wait`, and every reissue after a reconnect carries the whole
declaration. A claim's final cleanup (`release`, the release after a failed
acquisition, and the abandon of a dropped handle) shares one 10 s deadline from
its first step. No answer renews it, a refused release included: a later release
and the abandon spend what remains, after which nothing more is sent and process
lifetime is the fallback.

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
acquisition with a different purpose is refused as conflicting. Admission counts
`host_only` claims as work that ends (Memory admission, above); `gpu_place` and unknown
claims are ordinary and never counted. Python acquisition declares none.

### The `host-claim` front-end, and handing a claim on

```
host-claim --bytes N [--label L] [--priority P] [--state-dir D] (--gpu-place | --host-only) \
           [--cancel-file PATH] [--outage-file PATH] -- ARGV
```

`host-claim` waits for the claim in a process that holds nothing else, then
executes ARGV in the same process with
`HOST_MEMORY_CLAIM={"claim_id","memory_bytes","state_dir"}`. It needs the
`transfer` and `purpose` capabilities. It exits 125 when the cancel file
appears while it waits (within a second), 75 when the compile service does
not answer within 60 s, stays unanswered for 60 s during the wait, or lacks a
capability (an outage, never a verdict), and 2 on a usage error, including
`GPU_QUEUE_PLACE` set without `--gpu-place`. Before every exit 75, of the compile
service or of a handed-over place's gpu-switch, it records the outage at
`--outage-file` by one durable write, if its launcher gave one; stderr keeps the
reason too. `gpu_queue.FrontEndOutage.read(path)` reads that record, with its
`source`, `operation` and `reason`, and returns None when there is none; the
format has this one codec.

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

These are service generation 17. Memory admission as described above is
generation 19, which serves `gpu_place` claims as ordinary where generation 18 served
them first. Parties as described above are generation 20: a claim's or group's party is the
systemd unit of its registrant or submitter, read once and saved; `--handoff-capabilities` is
unchanged and a successor of a lower generation is refused.
