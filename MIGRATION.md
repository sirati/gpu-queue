# Native migration boundary

The existing gpu-arbiter implementation was copied from
`sirati-nixos/nointernet/gpu-arbiter` (the actual checkout is
`/home/sirati/sirati-nixos/nointernet/gpu-arbiter`). The crate and existing
executables retain their names. `gpu-queue` adds operating modes over the same
service implementation.

The source checkout's last commit affecting that daemon was
`23c54b4eb6306095909235886bb072898a1fff2f`. The import includes the enforcement
work present in its working tree, followed by the native migration here.

Queue admission, allocation deadlines, idle enforcement, shared fairness,
shutdown escalation and failure storage belong to Rust. Python supplies CUDA
entry hooks and application checkpoint callbacks. It must not choose another
queue after a connection error.

`switch::fairness` owns shared turn/budget accounting. Its boundary is the
daemon's admitted parties plus monotonic time; callers receive shutdown through
the existing enforcement API. Group members count as one party.

`lease` owns fork fencing of native socket sessions. A forked child must close
inherited descriptors without sending release or shutting down the parent's
socket. Python fork hooks call that API before the child can acquire a new lease.

Consumers must install matching binaries and extension before switching their
entry hooks. Old Python admission and budget code is still present in ml-project;
this repository alone does not change running consumers or the host daemon.

Admission scans waiting jobs in priority order. A blocked job is not a barrier
to a later shared job that fits. An admitted exclusive or NInfer remains a
barrier. `switch::timing` owns per-job monotonic accounting. Admission starts its
clock; VRAM and SM observers report independent evidence; grant revocation or
completion freezes it. Preparation means grant to first observed VRAM allocation,
excluding queue wait. GPU time means total time holding the grant, including
preparation. Missing historical telemetry is displayed as unavailable.

`estimate` owns validation, warning text, and conversion to usable duration.
Omission and explicit `-1` both give no duration; only omission warns. Admission
receives positive durations through that API. Submission replies carry warnings
to each client transport, without putting diagnostics on machine-readable stdout.
