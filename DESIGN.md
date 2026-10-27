# Joint GPU queue: host daemon, container arbiter, ml-project interop

The native migration in `MIGRATION.md` supersedes the historical Python fallback,
lease-exemption and lost-state running-grant provisions below. All lease types
now share Rust enforcement. A live grant resumes only from retained daemon
state; a signed queue position alone cannot prove GPU admission.
Shared backfill now supersedes the historical head-of-line barrier as well:
later shared jobs may pass blocked waiters within the memory and declared-time
limits documented in README.md.

Decided with sirati on 2026-10-14. This document replaces the GPU-queue part of
/home/sirati/ninfer-gpu-arbiter-spec.md. NInfer, the proxy, the Claude Code
setup and the arbiter's `serve`/profile/host-cache behaviour stay as they are,
except where this says otherwise.

## Course correction (sirati, 2026-10-15): gpu-switch stands alone

This supersedes "Taking part in the Python protocol" below, and every mention
of the Python lock elsewhere in this document.

- **Standalone.** gpu-switch keeps its own line and admission (head-of-line,
  exclusive/shared, room by max(requested, observed), priority, re-queue).
  It never opens, lists or flocks anything of the Python lock, and it never
  reads `/proc/locks`. There are no lease helpers, and no `lock`,
  `lease_socket` or `hog_wait_secs` config.
- **Python is a client.** ml-project's `tools/_gpu_switch.py` (branch
  `gpu-lease-room-fix`) asks gpu-switch for a grant after its own lock
  admission, when gpu-switch is reachable and the lease is on the default
  lock. Otherwise it leases as before.
- **Lease clients.** Any process may hold a grant for itself:
  - It sends `{"op":"lease","mode","vram","label","priority"}` on either
    socket.
  - The daemon answers `ok`, then `waiting` lines whenever the reason
    changes, then `granted`. Every line carries `"lease_protocol": 1`.
  - Afterwards the daemon may send `signal` lines, which the client delivers
    to itself.
  - The grant lasts as long as the connection.
  - It is a job like any other. Its process tree is the SO_PEERCRED pid's
    (host namespace) plus descendants.
- **Nesting.**
  - gpu-run jobs get `GPU_SWITCH_GRANTED=<id>`; a lease client that sees it
    takes no grant.
  - They also keep a private per-job `ML_GPU_LOCK`, so Python inside a job
    never waits on the caller's lock, held by a process that may be waiting
    in gpu-switch behind this job.
- **Decisions made without sirati** (asleep):
  - The "never pass the line's first ticket" restriction is gone; it only
    existed because of Python's head gate. Priority may pass any waiting
    entry within its budget.
  - `switch-ninfer` now guarantees that nothing waiting starts before
    NInfer.
  - Admissions wait for a card sample taken after the last departure from
    the GPU. A sample is dated when it starts, and is taken at once while
    an admission waits on one.
  - ~~Lease clients are policed like gpu-run jobs, including the 15 s
    startup rule.~~ Reverted 2026-10-15: it contradicted the addendum
    ("never Python leases") and killed ml-project processes in CPU phases.
  - Python leases no longer appear as separate counts: Python processes
    that use gpu-switch are lease-client jobs. Those that don't are
    invisible to it, apart from their compute, which counts as foreign.


## Who is involved

- **sirati** on the host and **nointernet** inside the container both run GPU
  work through `gpu-run`. They share one queue.
- **ml-project Python leases** (doggy-master, `tools/gpu_lock.py`,
  `tools/_gpu_*.py`, newest lease change `94d1b97951`) are the second kind of
  participant. They keep their own Python implementation. Our daemon takes
  part in their protocol, so both form one line under one set of rules.
- **NInfer** is not a job and is not in the queue.

## Processes

1. **`gpu-switch`** is a new host daemon. It is a system service running as
   its own user, `gpu-switch`. It owns:
   - the joint queue
   - the GPU owner state
   - its participation in the Python protocol
   - host-side GPU checks with the host's `nvidia-smi`

   It has two listening sockets, which systemd creates and hands over by name
   through `LISTEN_FDS`/`LISTEN_FDNAMES`. In tests, the config may give a path
   to bind instead.
   - `host`, `/run/gpu-switch/host.sock`: mode 0660, group `gpu-switch-users`
     (sirati is a member). The peer is identified by SO_PEERCRED uid.
   - `container`, `/run/gpu-switch-container/sock`: mode 0660, group
     `nointernet`. The directory is bind-mounted into the container at
     `/run/gpu-switch`, so the container sees `/run/gpu-switch/sock`. Every
     connection on this socket belongs to the "nointernet" side.
     SO_PEERCRED gives the peer's pid in the daemon's own (host) pid
     namespace; use it to learn container processes' host pids.
2. **`gpu-arbiter`** in the container now only supervises NInfer and runs the
   proxy. It keeps one long-lived control connection to the daemon over the
   container socket (op `arbiter_attach`) and obeys:
   - `start {profile, host_context_mib}`: start NInfer and report `serving`
     once `/health` returns 200
   - `stop`: drain the proxy's in-flight requests (respecting
     `drain_timeout_secs`), SIGTERM, wait for exit, report `stopped {pid}`.
     Report NInfer's host pid as well; the daemon resolves it from the child's
     NSpid, or the arbiter reports the container pid and the daemon maps it
     through `/proc/*/status` NSpid.

   **Fail-safe:** if the control connection drops, the arbiter stops NInfer.
   NInfer never runs without a live grant from the daemon. It reconnects with
   backoff, and the proxy keeps holding requests throughout (unchanged).
3. **`gpu-run`** is the same CLI on both sides. The socket comes from
   `--socket` or `GPU_SWITCH_SOCKET`; a Nix wrapper sets the default. The
   runner model is unchanged: a detached runner keeps the caller's exact
   env/cwd/umask, registers, waits for `go`, runs, and reports.
   - **Logs:** the runner writes the job's log itself, to
     `${XDG_STATE_HOME:-$HOME/.local/state}/gpu-run/<id>.log`, as the caller.
     The daemon only stores that path for the submitting side, so
     `gpu-run logs` reads it locally.
   - **Mode:** every submit names its mode, as in Python: `--exclusive` or
     `--shared --vram SIZE`, with SIZE like 12G, 1536M, 2.5GiB or bytes, using
     the same parsing as Python. There is no default.
   - **Time limit:** `--timeout SECS` is kept by the runner, which sends
     SIGTERM to the job's process group at the limit and SIGKILL 10 s later.
     The daemon does not know about it.
   - **The agent in the container** does not use the CLI but `gpu-run mcp`
     (README): tools that never wait for the GPU, because the agent's model is
     NInfer and anything that did would wait for the agent's own next
     request. `gpu-run hook` refuses the CLI in a foreground Bash
     call.

## Ownership states

The daemon keeps two values:

- `serve_wanted`: set by `serve on|off` from either side. It defaults to off
  at every daemon start and is not persisted.
- `owner`: either NINFER or QUEUE.

The rules:

- **Serve on, owner NINFER:** the daemon holds an exclusive Python lease for
  NInfer (registry job `ninfer`) and has told the arbiter to `start`.
  - Our submits and Python leases wait in the line behind that lease. Nothing
    runs.
  - Every new submit joins the Python line at once, publishing its ticket at
    submit time, so the order after a switch is true arrival order across both
    kinds of participant.
  - `switch-gpu` from either side, if NInfer is serving or starting: `stop`
    the arbiter, wait for `stopped` and for the host `nvidia-smi` to no longer
    list NInfer's pid, release the NInfer lease, set owner QUEUE.
- **Owner QUEUE:** the line runs under Python's rules; our jobs run as Python
  participants.
  - The queue counts as empty when we have no queued or running job, the
    Python line is empty, and no live Python lease is held.
  - Once it has been empty for `linger_secs` and serve is still wanted, the
    daemon queues an exclusive NInfer lease like any participant. Anything
    that arrived first runs first.
  - Once admitted, it tells the arbiter `start`, and owner becomes NINFER.
- **Serve off:**
  - If NInfer runs: stop it as above, release its lease, owner QUEUE, and
    never start it again.
  - With serve off, jobs never wait for a switch. They just take part in the
    line. `switch-gpu` does nothing then and says so.
- **Serve on while owner is QUEUE:** NInfer starts after the queue empties and
  lingers. If the queue is already empty, it queues its lease immediately,
  without the linger.
- **`set-profile` and `set-host-cache`:** in memory, applied at the next
  `start`. `--apply-now` does a stop and a start, without letting the queue
  in, and only when NInfer is serving.

## Taking part in the Python protocol (superseded, see "Course correction")

Read the real code on doggy-master before writing any of this; do not guess
the formats. The parts that matter are `_gpu_lease.py`, `_gpu_line.py`,
`_gpu_registry.py`, `_gpu_room.py`, `_gpu_card.py`, `_gpu_flocks.py`,
`_gpu_nvml.py`, `_gpu_lock_holders.py` and `GPU_LOCK_RECIPE.md`.

- **What the daemon does per job:** everything a Python process does for its
  own lease, done on the job's behalf with the daemon's own file descriptors.
  - publish a line ticket and wait in the flock of the ticket ahead
  - take `<lock>.head` as head
  - run the admission
  - take the intent and main flocks in the right mode with the right order
  - write a registry record
  - release in the right order
- **Exclusive admission:** a clear card, with the same tolerance for idle
  graphics/compute clients as `_gpu_card.py`, using the host `nvidia-smi`.
- **Shared admission:** the same room rule as `_gpu_room.py`, based on the
  declared VRAM and the other records' `reserved_bytes`.
- **Registry records:** must parse in Python. Use anonymised job names
  (`gpu-run-<n>`, `ninfer`) and never put commands or paths into them. A
  record's pids are the job's host pids:
  - for the container side, from SO_PEERCRED and the process tree
  - for the host side, the runner's pid and its children

  Fill them in however the registry expects, so Python's liveness pruning and
  per-job VRAM accounting work.
- **The ninfer lease:** an exclusive record whose pids are the arbiter's and
  NInfer's host pids.
- **Python features we don't need to implement:** held-time budgets and yield,
  OOM requeue, worker sessions, delegation and compile windows. Our jobs simply
  hold the lease for their whole run. Python's own processes keep doing all
  of that among themselves.
- **Lock path:** from the config, `lock`. papa uses Python's default,
  `/tmp/ml-gpu.lock`. All writes there come from the lease helpers, which run
  as the Python tool's user; gpu-switch only reads. A moved lock split the line
  between old and new sessions on 2026-10-14, so the location stays the
  default. Tests use a temp directory.

## Restarts and lease protocol 2 (2026-10-15)

- **Python leases queue here.** Where gpu-switch is reachable, ml-project's
  lease tool waits in gpu-switch's line from the moment it wants the GPU
  (`tools/_gpu_queue.py`), so `gpu-run status` shows every waiter on both
  sides. Without gpu-switch it uses its own lock line, as before.
- **Lease request** (`"lease_protocol": 2`): `mode`, `vram`, `label`,
  `priority`, plus `group` (one job's processes: a shared member of a running
  group never queues, and the group's VRAM counts once), `requeued` (behind
  the head, before fresh waiters), `cooperative`, `resume`.
- **Census** `{"op":"census","group"}`: `exclusive_waiting`, `others`,
  `non_cooperative_others` — counts only, for the client's yield decisions.
- **State survives a restart.** It is written to `state_dir` (default
  `$STATE_DIRECTORY`) after changes and at SIGTERM, before any connection
  closes. Restored queued/running jobs are detached until their clients
  resume them, for `reattach_grace_secs`; a detached head holds the line
  meanwhile; unclaimed after the grace, they end.
- **Resume.** Every job carries `{id, token, key, running}` from its first
  reply. A client whose connection drops resends its original request with
  it: re-attached if gpu-switch kept the job; otherwise registered again at
  `key` when the token (HMAC-SHA256 under a secret kept apart from the state
  file) proves it, at a fresh place when not; a running job is accepted as
  running. gpu-run runners and ml-project lease clients both resume.
- **NInfer through a restart.** The arbiter keeps NInfer serving for
  `link_grace_secs` after its link drops and reports what runs when it
  attaches again; gpu-switch adopts a running NInfer it had not kept.

## Privacy

What either user may see of the other is counts only:

- **sirati sees** of nointernet: queued, running and waiting job counts, the
  owner, serve state, profile, and the GPU process count.
- **nointernet sees** of sirati: the same.
- **Python leases** appear as counts too: held and waiting.
- **Your own jobs** are listed in full: id, mode, VRAM, state, exit code,
  command.
- **Never across sides:** no ids, commands, cwd, env, log paths, pids or
  error texts. `cancel`, `wait` and `logs` only accept your own side's job
  ids.

`serve status` stays state-only (as now). `status` is the full view, limited
by the rules above.

## Config

**`gpu-switch`** (JSON, unknown fields rejected):

| field | meaning |
|---|---|
| `listen` | `[{name, path?}]`; uses the `LISTEN_FDNAMES` fd of that name, otherwise binds `path` |
| `lock` | Python lock path |
| `nvidia_smi` | path or null; null disables card checks (tests) |
| `linger_secs` | |
| `gpu_poll_interval_secs` | card sample interval while no admission waits on one (policing) |
| `gpu_wanted_poll_interval_secs` | minimum gap between samples while an admission waits on one (0.05) |
| `state_dir` | where state and the resume secret live (default `$STATE_DIRECTORY`) |
| `reattach_grace_secs` | how long clients, and the arbiter, have to come back (60) |
| `job_kill_grace_secs` | |
| `profiles` | list of profile names |
| `profile` | default profile |
| `host_context_mib` | default host cache size |
| `drain_timeout_secs` | sent to the arbiter with `stop` |

**`gpu-arbiter`** (container): `link_grace_secs` (30) keeps NInfer through a
dropped link; the proxy and ninfer sections as now, plus
`switch_socket`. The job queue, `nvidia_smi`, `linger_secs`, `autostart` and
`job_log_dir` fields all move to the daemon or go away.

## Host-side commands for sirati

- `gpu-run`: wrapped with `GPU_SWITCH_SOCKET=/run/gpu-switch/host.sock`.
- `ninfer`: keeps `fetch-model`. `ninfer on|off|status` become `gpu-run serve
  on|off|status` against the host socket. The polkit oneshot units go away.

## Tests

- Rust integration tests with a temp lock directory and the fake
  `nvidia-smi`.
- Interop tests that drive the real Python lease code from doggy-master
  against the same temp lock:
  - Python exclusive and shared leases interleaved with our jobs: FIFO across
    both, shared/shared concurrency, exclusive exclusion, room admission by
    declared VRAM
  - Python leases waiting behind the NInfer lease until `switch-gpu`
  - NInfer coming back after the queue empties
- The VM test exercises sirati and nointernet sharing the queue. Tell the
  Nix side what the VM test needs.

## Addendum (sirati, same day): idle policing, kills, visibility, categories

### Idle GPU policing (gpu-run jobs and Python lease clients)

Updated 2026-10-26: the Python lease exemption is removed. Both kinds of
grant are subject to these checks, in exclusive and shared mode.

**"Compute"** means SM utilisation of the job's processes, measured on the
host from per-process utilisation samples, e.g. `nvidia-smi pmon -s u`. VRAM
alone doesn't count. The fake nvidia-smi must grow a matching `pmon` mode for
tests.

**Startup rule.** Every grant must allocate VRAM in its process tree within
one second of admission, or receive SIGKILL. A separate fast observer checks
allocations without waiting for SM samples. Queue time is excluded; another
job's allocation does not satisfy the deadline.

**Idle rule.** It applies to any job that has allocated VRAM or used compute and shows none
for 90 s in a row. Escalate:
1. SIGINT
2. if still idle 60 s later, SIGTERM
3. if still idle 10 s after that, SIGKILL

Any compute in between cancels the escalation, and the 90 s count starts
again.

**Status.** The job's status names the reason: "killed: no GPU VRAM allocation within
1 s of grant", "killed: idle 90 s (SIGINT, then SIGTERM/SIGKILL)", or "killed
by sirati".

**Signals.** The daemon can't signal other users' processes, so the job's
runner (running as the submitter) delivers every signal to the job's process
group on the daemon's order. The runner dying still frees the slot.

### Kill across sides

sirati may kill any nointernet job by its anonymous handle (below):
`gpu-run kill <handle>`. Sequence: SIGINT, then SIGTERM after 60 s, then
SIGKILL 10 s later. nointernet cannot kill sirati's jobs. Either side can kill
its own jobs, which stays `cancel`, unchanged.

### What each side sees of the other

Per job, only:
- a short anonymous handle (e.g. `n3` for nointernet, `s2` for sirati)
- state (queued / running)
- mode (exclusive / shared)
- declared VRAM
- whether it is cooperative

Never ids, commands, cwd, env, categories, log paths, pids, timing or kill
reasons. The daemon's state, cache and any files of its own are private to the
`gpu-switch` user (0700). Python leases are still shown as counts only.

### Submit options

- `--category train|benchmark|accuracy|test|eval|inference|other`: optional,
  default `other`. Shown to the submitter only.
- `--cooperative`: the job declares it checkpoints and resumes on yield. The
  daemon registers it in Python's `CooperativeRegistry`
  (`_gpu_lock_holders.py`, `<lock>.coop/`) for the job's lifetime, so Python's
  yield logic counts it as cooperative. Also tell the job when to yield: set
  `ML_GPU_YIELD` semantics as in `GPU_LOCK_RECIPE.md`, or whatever the Python
  side uses. If there's a clean way, deliver it the same way (e.g. a file the
  job can watch, or SIGUSR1). Document what you choose.

## Addendum 2: positions and priority

- **Positions.** Both sides may query the line position of every waiting job.
  Their own jobs show it by id; the other side's by handle; Python leases as
  entries in the line. Add the position and the priority to the cross-side
  view: handle, state, position, mode, VRAM, cooperative, priority.
- **Priority.** Set at submit with `--priority N`, N from -10 to 10, default
  0. Python leases count as 0.
- **The skip rule.** A job of priority p may pass waiting jobs of a lower
  priority q, at most p − q of them for each lower level q, counted separately
  per level. Example: a priority-2 job passes up to two priority-0 jobs (not a
  third) and, separately, up to one priority-1 job. Equal or higher
  priorities are never passed.
- **Placement.** A job takes its place when it is queued, and it never moves
  back. Later arrivals may pass it within their own budgets. Passing only
  applies to waiting jobs, never to one that is admitted or running.
- **Interop.** Python orders the line by ticket name (`_gpu_line.py`); our
  daemon places a job by choosing where its ticket sorts. Verify against the
  real code that a Python waiter that is already asleep in the flock of the
  ticket ahead of it notices a ticket inserted ahead of it (it re-reads
  `ahead()` after waking, or must be woken). If it cannot be done safely
  without changing Python, say so, and implement priority among our own jobs
  only: never pass Python waiters, and keep FIFO relative to them.

## NInfer in a container of its own, for both sides (2026-10-24)

This supersedes "gpu-arbiter in the container" above.

- **One NInfer** serves the host (sirati, CLIProxyAPI) and the
  nointernet container. It and its gpu-arbiter run in a container of
  their own (`nointernet/ninfer-container.nix`): rootless podman of the
  host account `ninfer`, no network, the model's directory read-only, no
  homes, the GPU only, the service user inside being that account's uid.
  Neither sirati nor the container user can ptrace it or read its memory or
  files.
- **gpu-switch** gets a third socket, `arbiter`, for the arbiter's link and
  nothing else; the container socket no longer takes `arbiter_attach`. The
  line, the owner states and `serve`/`switch-gpu` from either side are as
  before: NInfer is still one participant.
- **The proxy** listens on unix sockets instead of a port: one for the host
  (the whole API), one for the container (Anthropic Messages only). Each side
  reaches its socket through `ninfer-relay` on a loopback port: sirati's user
  service on the host, a service in nointernet. The relays hold
  connections while the socket is away; the arbiter holds requests while
  NInfer is.
- **Prefix cache** stays shared between the sides (sirati, same day).
