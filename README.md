# gpu-arbiter

One GPU, one line. The line is shared by:

- the host user's and the container user's jobs (`gpu-run`),
- processes that ask for a grant themselves (lease clients, e.g.
  ml-project's `tools/_gpu_switch.py`),
- NInfer, which runs in a container of its own behind a holding proxy and
  serves the host and the nointernet container.

gpu-switch stands alone. It never opens, lists or locks anything of
ml-project's Python lock. Python uses gpu-switch through
`tools/_gpu_switch.py` when it is reachable, and leases as before when it
is not. The design is in `DESIGN.md`; this file describes what is built.

- `gpu-switch --config FILE [--check]`: host daemon. It owns the line, the
  GPU owner state and the policing.
- `gpu-arbiter --config FILE [--check]`: in NInfer's container. It runs NInfer on
  gpu-switch's orders, plus the proxy.
- `gpu-run`: the CLI on both sides, and the detached job runner.
  `gpu-run mcp` and `gpu-run hook` serve Claude Code in the container (below).
- `ninfer-relay --listen 127.0.0.1:PORT --socket PATH [--hold-secs N]`: a
  loopback port in front of one of the proxy's unix sockets, for clients
  that speak only TCP. While the socket is away (the container restarting)
  each connection is held, its request read ahead, and handed over when the
  socket answers; default hold 3600 s.
- `ninfer-mock`: an ninfer-serve test double. It is also a fake
  `nvidia-smi`, fake GPU jobs and a lease client.

Nothing is configured at build time.

## gpu-switch config (JSON, unknown fields rejected)

| field | default | meaning |
|---|---|---|
| `listen` | required | `[{name, path?}]`. `name` is `host`, `container` or `arbiter` (the arbiter's link, nothing else). Uses the `LISTEN_FDNAMES` fd of that name, otherwise binds `path` (mode 0660). |
| `nvidia_smi` | null | host nvidia-smi. Null disables card checks, room accounting, policing and the NInfer-off-card wait. |
| `linger_secs` | 60 | how long the line stays empty before NInfer queues again |
| `gpu_poll_interval_secs` | 2 | card and process-tree poll |
| `job_kill_grace_secs` | 10 | `cancel`: SIGTERM, then SIGKILL after this |
| `profiles` | `[]` | profile names the arbiter knows |
| `profile` | null | default profile |
| `host_context_mib` | null | default `--host-context-mib` |
| `drain_timeout_secs` | null | sent to the arbiter with `stop`. Null drains forever. |
| `restart_backoff_initial_secs` | 2 | NInfer retry after a failure, doubled each time |
| `restart_backoff_max_secs` | 60 | backoff cap |
| `startup_idle_secs` | 1 | zero-VRAM grant limit |
| `idle_secs` | 90 | idle rule |
| `sigint_to_sigterm_secs` | 60 | idle rule and `kill`: SIGINT, then SIGTERM after this |
| `sigterm_to_sigkill_secs` | 10 | every escalation: SIGTERM, then SIGKILL after this |
| `coop_min_hold_secs` | 180 | a cooperative job yields only after this |

## gpu-arbiter config

| field | default | meaning |
|---|---|---|
| `proxy.listen` | null | a TCP address serving the whole API (tests, manual runs) |
| `proxy.listeners` | `[]` | unix sockets: `[{name, path?, mode?, messages_only?}]`. The `LISTEN_FDNAMES` fd of `name`, otherwise `path`, bound with `mode` (octal string, default 0660). `messages_only`: `POST /v1/messages`, `POST /v1/messages/count_tokens` and `GET /v1/models`; anything else is 404. One of `listen` and `listeners` is required. |
| `proxy.keepalive_interval_secs` | 5 | `: keep-alive` period for held streams |
| `ninfer.executable`, `.model`, `.port` | required | the model is the first argument; the arbiter adds `--host 127.0.0.1 --port N` |
| `ninfer.args` | `[]` | common args |
| `ninfer.profiles` | `{}` | name -> extra args. `start` names the profile. Profile args come last, and ninfer-serve keeps the last value of a repeated flag. |
| `ninfer.health_poll_interval_secs` | 1 | |
| `ninfer.startup_timeout_secs` | 1800 | |
| `ninfer.stop_timeout_secs` | 120 | SIGKILL after this |
| `switch_socket` | required | gpu-switch's arbiter socket |
| `reconnect_initial_secs` | 1 | link reconnect backoff |
| `reconnect_max_secs` | 30 | link reconnect backoff cap |

The arbiter starts NInfer only on `start` and reports `serving` after
`/health` answers 200. On `stop` it drains (honouring the drain timeout),
sends SIGTERM, waits, and reports `stopped`. If the link drops, it stops
NInfer at once, aborting in-flight requests, then reconnects. The proxy
holds requests whenever NInfer is not serving.

## The line

Every job is an entry: a gpu-run job or a lease client, plus NInfer when
it waits for the GPU. They are ordered by arrival.

Admission scans the waiting line in priority order:

- An exclusive job needs no job on the card and no foreign busy compute.
  Compute processes outside every job's process tree and outside NInfer's
  count as foreign; idle graphics/compute clients are tolerated.
- A shared head needs no exclusive job on the card, and room (below).
  Several shared jobs may be admitted in one pass.
- A shared job may pass blocked shared or exclusive waiters if its VRAM fits
  and its estimated duration fits in the longest remaining declared runtime of
  the currently running jobs. Estimates are per admission; elapsed grant time
  is deducted. Unknown or expired estimates do not authorise a queue jump.
- Running exclusives and NInfer still prevent shared admission. A queued
  NInfer switch retains its explicit barrier.
- Decisions use a card sample taken after the last time anything left the
  GPU, so a fast successor is never admitted on a stale view.

**Room.** Every running job occupies max(declared, actual), with actual
summed over its process tree:

```
available = capacity − Σ_jobs max(declared_i, actual_i)
                     − max(0, used_total − Σ_jobs actual_i)
```

The last term is card usage outside every job (stray processes). A shared
head enters when `available ≥ request`, or when the card is otherwise
empty. Samples are polled, so actual usage lags by up to
`gpu_poll_interval_secs`; a just-admitted job counts at its declaration.

**Priority** (`--priority p`, −10..10, default 0). From the back of the
line, a new entry passes waiting entries of lower priority q, at most
p − q of each level, never one of equal or higher priority. This includes
negative q: priority 0 passes a −1 job once.

**Re-queue.** A cooperative job that yields (exit 76) re-enters right
behind the head: after entries re-queued before it, before every fresh
waiter. This mirrors ml-project's `_gpu_line.py`.

## Owner states

`serve_wanted` starts off at every daemon start. `gpu-run serve status`
prints one state word:

| word | meaning |
|---|---|
| OFF | serving off; the line runs |
| WAITING_GPU | NInfer waits in the line |
| SWITCHING_TO_NINFER | `switch-ninfer`: resident jobs finish, then NInfer starts |
| STARTING | NInfer admitted and loading |
| SERVING | NInfer holds the GPU; every job waits for `switch-gpu` |
| DRAINING / STOPPING | NInfer handing the GPU back |
| RUNNING_JOBS | the line owns the GPU |
| LINGERING | line empty; NInfer queues after `linger_secs` |
| FAILED | NInfer failed. The GPU is released; it retries with backoff while serve is on. |
| WAITING_ARBITER | serve on, no arbiter connected |

- **`switch-gpu`:** when NInfer is starting or serving and a job waits,
  stop NInfer and wait until the host nvidia-smi no longer lists its pid;
  the line then runs. With nothing queued it is refused. With serve off it
  is a no-op and says so.
- **Empty line:** once no job has been queued or running for
  `linger_secs`, NInfer queues like any entry. `serve on` with an already
  empty line skips the linger.
- **`switch-ninfer`** (either side): implies serve on. NInfer's entry goes
  to the front. Resident jobs finish, then NInfer starts. Every waiting job
  keeps its place and runs after the next `switch-gpu`. **Guarantee:**
  nothing waiting starts before NInfer.
- **`set-profile` / `set-host-cache`:** in memory, for the next `start`.
  `--apply-now` restarts a serving NInfer without letting the line in.

## Lease clients

On either socket, a process sends one line:

```
{"op":"lease","mode":"exclusive"|"shared","vram":<bytes, shared only, >0>,"label":"<text>","priority":<-10..10, default 0>}
```

Every line the daemon sends on this connection carries
`"lease_protocol": 2`. A client that misses it on the first reply is
talking to a gpu-switch without leases.

1. `{"ok":true,"job":{"id":..,"handle":..},"lease_protocol":2}` or
   `{"ok":false,"error":..,"lease_protocol":2}`.
2. While it waits: `{"waiting":"<reason>","lease_protocol":2}` whenever the
   reason changes, e.g. `NInfer holds the GPU; NOT running until 'gpu-run
   switch-gpu'`, `position 3 in line`, or `waiting for 4001 MiB of VRAM
   beside 1 job(s): 4000 MiB available`.
3. `{"granted":true,"lease_protocol":2}`.
4. After the grant: `{"signal":"INT"|"TERM"|"KILL","lease_protocol":2}` for
   idle policing and the other side's kill. The client delivers it to
   itself.

EOF or `{"op":"release"}` requests release. A live allocation keeps its grant
until verified teardown, with enforcement of retained processes. A queued client
that is cancelled or killed sees EOF. Cooperative clients additionally negotiate
`shutdown_protocol: 1` and receive typed shutdown notices.

A lease client is a job like any other: same line, room, NInfer rules,
policing and cross-side view. Its process tree is the peer's: the
SO_PEERCRED pid, which the kernel reports in gpu-switch's (host) pid
namespace, plus its descendants. That tree is used for room accounting,
idle policing and the foreign-compute check. The label is shown to its
own side only.

## Jobs

```
gpu-run submit (--exclusive | --shared --vram SIZE) [--category C] [--cooperative] [--priority N] [--timeout SECS] [--] CMD...
gpu-run status [ID] [--json]     gpu-run wait ID [--report]   gpu-run logs ID [-f]
gpu-run cancel ID                gpu-run kill HANDLE    gpu-run switch-gpu
gpu-run switch-ninfer            gpu-run completions bash
gpu-run set-host-cache MIB [--apply-now]                gpu-run set-profile NAME [--apply-now]
gpu-run serve on|off|status
```

- Socket: `--socket PATH` or `$GPU_SWITCH_SOCKET`.
- SIZE parses as Python's does: `12G`, `1536M`, `2.5GiB`, or plain bytes.
- `--timeout SECS`: a wall-clock limit on the job's running time, counted by
  the runner (across the runs of a cooperative job). At the limit the job's
  process group gets SIGTERM, 10 s later SIGKILL; both are noted in its log.

The runner is the CLI re-executed in a new session (setsid), with the
caller's exact environment, cwd and umask. It writes the log as the
submitter, to `${XDG_STATE_HOME:-$HOME/.local/state}/gpu-run/<id>.log`.
It also delivers every signal: the daemon cannot signal other users.

The job's environment gets these additions:

- `GPU_SWITCH_GRANTED=<id>`. A lease client inside the job must not ask
  for a grant of its own: it would wait behind this very job, a deadlock.
  `tools/_gpu_switch.py` and `ninfer-mock lease-client` skip it.
- `ML_GPU_LOCK=<XDG_RUNTIME_DIR or state dir>/gpu-run/<id>/ml-gpu.lock`.
  This is a private lock, removed after the job. Python inside the job is
  alone on it and passes at once. On the caller's lock it could wait for a
  process that holds that lock while itself waiting in gpu-switch behind
  this job: a deadlock. ml-project's client asks gpu-switch only for
  leases on the default lock, so the private lock alone already keeps it
  away.
- `ML_GPU_LEASE_MODE`, `ML_GPU_VRAM`: the submit's mode and declaration.
- `--cooperative` only: `ML_GPU_YIELD`, a path that is created when the job
  should checkpoint and exit 76.

The yield is requested when the job has held the GPU for
`coop_min_hold_secs`, an exclusive entry waits, and every running job is
cooperative. Exit 76 re-queues the job right behind the head.

A wrapper may set `GPU_SWITCH_SOCKET`, which the job then also sees; use
`--set-default`, not `--prefix PATH`.

If a runner's connection drops, its job fails. The job's processes may
outlive it; exclusive admission then sees them as foreign compute.

## Claude Code in the container: `gpu-run mcp` and `gpu-run hook`

The agent's model is NInfer. While queued jobs run NInfer is down, and while
NInfer serves the jobs wait behind it, so anything that waits for the GPU
from inside a tool call waits for the agent's own next request: a deadlock.

`gpu-run mcp [--max-timeout-secs N]` is an MCP server on stdio (JSON-RPC, one
message per line; the socket as for the CLI). None of its tools waits for the
GPU; only `gpu_run_queue` waits, for jobs that run while the agent cannot read
anyway:

| tool | does |
|---|---|
| `gpu_submit` | `submit`: `command` (argv array), `mode`, `vram`, `category`, `priority`, `cwd`, and a required `timeout_secs` (at most N, default 86400; the runner enforces it). Returns the id, whether the job waits behind NInfer, and the monitor to start: `gpu-run wait ID --report` as a background Bash task, which ends with the job and prints how it went. That wakes every agent whose job ran, not only the one that switched. |
| `gpu_status` | the line, the serving state and the agent's jobs; with `id`, one job |
| `gpu_logs` | the last lines (`tail`, default 100, at most 2000) of a job's log; never follows |
| `gpu_cancel` | `cancel` |
| `gpu_run_queue` | `switch-gpu` for the jobs this session (this server process) queued. Returns once they have ended, with how each went (state, exit, run time, the end of the log of any that failed), or early when NInfer takes the GPU back with some still queued. With none of the session's jobs waiting it does not switch: another agent probably ran the queue already, and it says how the session's jobs went. `force: true` switches anyway. |

Not offered, because each can leave the agent without its model: `wait`,
`logs -f`, `switch-ninfer`, `serve on|off`, `set-profile`, `set-host-cache`
(`--apply-now` restarts the model under it) and `kill`.

`gpu-run hook` is a Claude Code PreToolUse hook for Bash. It reads the hook
JSON on stdin and, when the command line runs `gpu-run` (shell-aware: quotes,
`$(...)`, separators, `env`, `sudo`, `timeout`, `sh -c`, `nix run`, ...) and
`run_in_background` is not true, answers with a deny whose reason is `Please
use the MCP tool, otherwise you can deadlock yourself.` A background Bash
task may run `gpu-run wait`. It reads the command text only, so a script that
runs `gpu-run` itself gets past it: a guardrail, not a boundary.

llm.nix registers both: `/etc/claude-code/managed-mcp.json` (Claude Code then
ignores MCP servers the user adds) and a `hooks.PreToolUse` entry in
`managed-settings.json`. `cargo test --test mcp` drives the real server and
hook against the real daemons.

## Idle policing (gpu-run jobs and lease clients)

Compute means SM utilisation of the job's process tree, from
`nvidia-smi pmon` each poll. The status shows the reason.

- **Startup rule:** every grant must allocate VRAM within `startup_idle_secs`
  (default 1 second), or receive SIGKILL. This includes lease clients and
  shared jobs beside other holders. Queue time is excluded.
- **Idle rule:** after allocating VRAM or computing, `idle_secs` without compute: SIGINT;
  still idle, SIGTERM; then SIGKILL. Compute in between cancels it.
- **`kill HANDLE`** (host side only, container jobs): SIGINT, SIGTERM,
  SIGKILL.

## Privacy

- Your own jobs: everything.
- Another side's jobs: handle (`n3`, `s2`), state, position, mode, VRAM,
  cooperative, priority.
- Never across sides: ids, commands, labels, cwd, env, categories, log
  paths, pids, timing, error and kill texts.
- NInfer's error text goes to the container side only.
- `serve on|off|status` prints phase words and profiles only.
- A host uid is its own side, so another host user is "other" too.

## Control protocol

One JSON object per line.

Requests (`op`): `status`, `serve_status`, `serve {on}`, `switch_gpu`,
`switch_ninfer`, `cancel {id}`, `wait {id}`, `kill {handle}`,
`set_host_cache {mib, apply_now}`, `set_profile {profile, apply_now}`,
`submit {cmd, cwd, spec, log_dir}`, `lease {mode, vram, label, priority}`
(above). `arbiter_attach` is taken on the `arbiter` socket only, which takes
nothing else.

Reply: `{ok, error?, message?, status?, job?, serve?}`.

After `submit` (runner session):

- daemon → runner `event`: `go`, `cancel`, `signal {sig: INT|TERM|KILL}`,
  `yield`, `done`
- runner → daemon `op`: `started {pid}`, `spawn_failed {error}`,
  `finished {exit_code, signal}`

After `arbiter_attach`:

- daemon → arbiter `cmd`: `start {profile, host_context_mib}`,
  `stop {drain_timeout_secs}`
- arbiter → daemon `event`: `starting {pid}`, `serving {pid}`, `draining`,
  `stopped {pid}`, `failed {error}`

Runner and arbiter pids are in the sender's namespace. The daemon maps
them through NSpid: the child of the peer whose innermost pid matches.

## ninfer-mock

The fake card: `$NINFER_MOCK_GPU_DIR/<pid>` holds `mem=<MiB> sm=<pct>
type=<C|G|C+G>`; `total` holds the card size in MiB.

- Symlinked as `nvidia-smi`: answers the queries gpu-switch makes.
- `gpu-hold [--ms N] [--mem MiB] [--sm PCT] [--sm-ms N] [--type T]
  [--ignore-int] [--ignore-term] [--busy-on-int] [--exit-on-yield]`: a GPU
  job.
- `lease-client (--exclusive | --shared --vram BYTES) [--label L]
  [--priority N] [--hold-ms N] [--mem MiB] [--sm PCT] [--ignore-int]`: a
  lease client over `$GPU_SWITCH_SOCKET`. It prints every daemon line and
  shows itself on the fake card once granted.
- A serving mock registers itself (`--gpu-mem`).

## Tests

`cargo test`: unit tests, `tests/proxy.rs`, `tests/queue.rs`,
`tests/lease.rs`, `tests/mcp.rs`. `GA_TEST_LOG=1` with `--nocapture` prints the daemons'
logs.

`tests/interop.rs` drives ml-project's real client, `_gpu_switch.acquire()`,
through `tests/interop/switch_client.py`, against the real daemon:

```
git -C ml-project archive gpu-lease-room-fix tools | tar -x -C $T
GA_ML_TOOLS=$T GA_PYTHON=python3 cargo test --test interop
```

It takes no Python lock: `acquire()` only compares its lock argument with
the default path. Without these variables the tests pass as skipped.

## Native queue modes

This repository starts from `sirati-nixos/nointernet/gpu-arbiter`, preserving the
existing joint queue, admission rules, runner, NInfer isolation, proxy, resume
protocol and tests. The internal Rust crate remains named `gpu-arbiter`.

Running grants resume only from retained daemon state. After state loss, signed
queue places can be restored, but clients cannot declare themselves admitted.
Runners terminate their commands when the authority refuses reattachment.

`gpu-queue daemon --config FILE` runs that same engine as `gpu-switch`.
`gpu-queue client --socket PATH --exclusive -- COMMAND` connects to it.
`gpu-queue independent --exclusive -- COMMAND` starts the same engine in a local
helper, shared through the user's runtime directory. Independent mode refuses
to start alongside a configured system queue; client mode never falls back to
an independent queue after a connection failure. For shared commands use
`--shared 12G` in place of `--exclusive`.

Every grant must allocate VRAM within one second by default. Returning to zero
VRAM for the same interval also triggers enforcement. Exclusive and shared
clients are both subject to post-use compute idleness. A revoked zero-VRAM job
has state `stopping`, without a GPU grant, while bounded CPU cleanup runs.
Reallocating after revocation causes immediate termination.

Enforcement appends and fsyncs `failures.jsonl` before notification or signalling.
Permanent failures remain there after the bounded status history is pruned.
The host service needs `AmbientCapabilities=CAP_KILL` to enforce deadlines
against other users' clients; leases it cannot enforce are refused. Started
runner PIDs are verified as actual children before becoming signal targets.

## Python shutdown

Build the extension with `maturin develop --features python`, or use
`nix build .#python`. Queue policy lives in Rust; Python supplies its checkpoint
callback and acknowledges when cleanup starts.

```python
from gpu_queue import Lease, GpuLeaseInterrupted

with Lease(mode="shared", vram=12 << 30, shutdown_handler=save_checkpoint,
           expected_runtime_secs=120, shutdown_grace_secs=30) as lease:
    train()
```

`save_checkpoint(interruption)` receives `reason`, `request_id`, `deadline` and
`permanent`. `GpuLeaseInterrupted` subclasses `KeyboardInterrupt`. It is raised
on Python's main thread; normal context-manager unwinding runs the callback.
A caller catching it inside the context can call `lease.run_shutdown_handler()`.
A caller managing cleanup itself calls `lease.shutdown_started(request_id)`.
Stale acknowledgements are rejected. Acknowledging or catching the exception
does not extend the fixed deadline. Blocked native calls may delay Python's
signal handling; the daemon still escalates to process termination.

The context must release its CUDA resources before an ordinary release.
During an already requested shutdown, remaining VRAM stays attributed until
normal process teardown or the existing shutdown deadline. CPU cleanup after
zero-VRAM revocation cannot allocate on the GPU again.

Set `cooperative=True` for the existing cooperative minimum-hold policy. A yield
has `permanent=False`; exceeding its shutdown deadline becomes a permanent
failure. Priority and group membership use the existing daemon protocol.

Cooperative shared work also has a 600-second weighted budget, a 300-second
sole-holder turn when an exclusive waits, and a 60-second idle yield under the
same contention. Each other admitted party reduces the budget's charging rate
by a factor of 1.25. Group members count once. These policies are daemon-owned
and configured with `shared_budget_secs`, `shared_turn_secs`, and
`shared_idle_yield_secs`. Cleanup grace is reserved before budget exhaustion.

Python fork hooks close inherited native sessions in the child without
releasing the parent's grant. The child must acquire its own lease before GPU
use. Fork only before initializing CUDA, as required by CUDA itself.

The Nix expression is interpreter-independent. Call it from the desired Python
package set, or apply `overlays.default` and use
`python.withPackages (ps: [ ps.gpu-queue ])` with your chosen `python` attribute.
The flake's `python` package follows nixpkgs' default interpreter. PyO3 0.30.1
supports the current Python API; no interpreter version is pinned. The Nix
adapter also references the matching daemon executable for independent mode.

Declare expected GPU duration for either mode with `gpu-run submit --expected-secs
120`, `gpu-queue client --expected-secs 120`, Python's `expected_runtime_secs=120`,
or the MCP/protocol field `expected_runtime_secs`. It is an estimate, separate
from the enforced `--timeout`. Older clients without estimates can retain their
place in the line but cannot backfill or provide a time horizon for backfill.
Omitting the estimate emits a submission warning. Use `--expected-secs -1`, or
`expected_runtime_secs=-1` in an API, to explicitly declare no estimate and
suppress the warning. Both omission and `-1` remain ineligible for backfill and
provide no runtime horizon. Zero and values below `-1` are rejected. CLI/native
client warnings go to stderr; MCP returns them in its submission response.

Status replaces job age with `EXPECT`, `PREP`, `GPU`, `NO-VRAM`, and `NO-UTIL`,
all in seconds. `PREP` is grant to first observed positive VRAM allocation.
`GPU` is total time holding the GPU grant, including preparation. `NO-VRAM`
and `NO-UTIL` independently sum sampled zero-memory and zero-SM intervals while
the grant is held. Queue wait and CPU cleanup after revocation are excluded.
Counters accumulate across admissions and survive daemon restarts. Unknown
telemetry and long sampling gaps are not charged as idle. Historical jobs without
these measurements display `-`; no measurements are invented for them.

Run `cargo test`, then build the extension and run
`python -m unittest discover -s tests -p test_python_shutdown.py` for process-level
Python interruption, checkpoint, acknowledgement, timeout and independent-mode
tests. These use the existing fake GPU, not the real device.

To check compatibility against an actual older installed package, run:

```sh
GPU_QUEUE_TEST_BIN_DIR="$PWD/result/bin" \
GPU_QUEUE_LEGACY_BIN_DIR=/path/to/older/package/bin \
GPU_QUEUE_LEGACY_TOOLS_ROOT=/path/to/existing/ml-project \
python -m unittest discover -s tests -p test_backward_compatibility.py -v
```

These tests exercise older CLI submissions without estimates, status decoding,
and running and queued runners reconnecting after an upgrade with retained
state. The optional tools root also tests the existing Python `tools._gpu_switch`
client reconnecting. They use disposable sockets and fake GPU allocations.
They do not test restart recovery for native PyO3 lease bindings, which terminate
on daemon disconnection.
