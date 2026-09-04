
# Sidecars: processes coupled to concurrent stage execution

> STATUS: **DECIDED — aio adopted as the base.** The sidecar bolt-on below was never built;
> it survives as the record of what the async model replaced. dagrunner's Experiment/Stage
> API is now an AOT plan layer compiled onto aio.Ctx.submit (thread scheduler deleted;
> ordering/skip/force-cascade/failure-propagation/MAX_PARALLEL all live in aio). Final sizes:
> aio core 346 lines, dagrunner 432 (from 571; ~130 of it is the declarative surface +
> validation, ~110 the plan lowering). Lifecycle coupling is example code
> (toy_aio_sidecar.py), including the freshness-without-wait coupling a re-run needs:
> ``force=None if train.skipped else "new"``.

## Context

The driver-rendered report exposed a pattern the DAG can't express: "run B *while* A runs" (vs
"after A"). Coleman's canonical case: a validation process consuming checkpoints a training
stage writes, on its own GPU/LSF job, starting when training starts and stopping when training
stops. Nothing in spearmint can say this today — and the scheduler holds no kill handle (the
`Popen` never escapes `_run_stage`, `bkill` doesn't exist in the package, and killing a
`bsub -K` client does NOT kill the LSF job).

Decided with Coleman — all three semantics **configurable per-sidecar**: `on_fail` =
`"ignore"` (default; failed row + loud print, DAG untouched) | `"abandon"` (dependents abandon
like a stage failure) | `"restart"` (relaunch extend-mode while the watch still runs, cap 3);
`stop` = `"all"` (default) | `"any"`; `ledger=True` (default: full stage-like identity —
managed row, own outdir, requirable, visible in status/browse) | `False` (no row/outdir,
`SPEARMINT_WATCH` env only, not requirable).

Key facts from exploration: `SPEARMINT_INPUTS` carries *done* savedirs, so sidecars get a new
live-dir handshake (`SPEARMINT_WATCH`); `reconcile_wip` already handles rows carrying an
`lsf_jobid`; mia-muvit's trainer writes `checkpoints/epoch*.ckpt` but its eval scripts consume
`model.pth` folders — that converter is mia-muvit work, out of scope (the toy demo uses the
streaming toy worker).

## Design (settled)

**API** — `Sidecar(Stage)` dataclass (+`watch`, `stop`, `on_fail`, `ledger`);
`e.Sidecar(name, cmd, watch, req=None, cmd_prefix=None, outdir_args=None, stop="all",
on_fail="ignore", ledger=True)` mirrors `e.Stage`, registers the job_key, appends to
`e.stages` (so `closure()`, `e.savedir()`, `main()` name-resolution keep working);
`run_experiment` splits by isinstance. Build-time asserts: watch non-empty and all plain local
Stages; vocab checks; `ledger=False` forbids `outdir_args` and being required; no
force flag may name a sidecar (they never skip-if-done, so forcing is meaningless); deadlock
check via `_transitive_dependents` (a watch target must not transitively require its sidecar).

**Rules of the model** (keep exactly one failure-propagation rule):
- `requires` = data dependency: gates start, propagates failure/abandon, carries `r.inputs`.
- `watch` = lifecycle coupling: gates start ("all watched launched-or-finalized, ≥1 running"),
  triggers stop, carries `SPEARMINT_WATCH` (live outdirs) → `r.watch`, and joins the
  **forcing** cascade (`--new train` re-runs the watcher and its dependents — bug caught in
  review: `_reverse_edges` map gets watch edges appended; the abandon check reads `s.requires`
  directly so failure never propagates through watch).
- A watched stage failing does NOT fail the sidecar: deliberate stop always finalizes `done`
  (its data is valid as far as it goes). "Don't run if train failed" is spelled explicitly:
  `req=[watcher, train]`.

**Scheduler integration** (`run_experiment`, ~110 lines):
- New state: `side_pending`, `side_running: dict[Future, Sidecar]`, per-sidecar `handle`
  (`{"proc": None, "jobid": None}`, filled by `_run_stage`), `stopping: dict[Sidecar, time]`,
  `restarts`, and `live: dict[Stage, Run]` (every stage launch stashes its Run — the sidecar
  launcher reads current outdirs/run_ids; a skipped watch target resolves via its done row).
- `_scan_sidecars()` after the pending scan and each finalize batch:
  - never-can-start (all watch finalized before launch): prior done row + clean watch →
    finalize `"skipped"` (re-run chains mirror stage skip logic, `[stale]` via recorded
    inputs); else `"abandoned"` + loud print.
  - start: launch via `start_managed(job_key, "new", argv, inputs=[req done ids + watch
    CURRENT run_ids])` (restarts use `"extend"`; never `"replace"` — it would clear a prior
    done outdir), env = standard `SPEARMINT_*` block + `SPEARMINT_WATCH`; `ledger=False` gets
    ONLY `SPEARMINT_WATCH` (no identity — document that such workers must not call rundb).
  - stop trigger (`all`/`any` over finalized watches) → `_signal_stop` + record in `stopping`.
  - escalation: `stopping` entries older than ~15s → `_escalate`; re-read `handle["jobid"]`
    (covers stop-before-bsub-ack).
- **Two-phase stop**: `_signal_stop(handle)` = `bkill <jobid>` if known (bkill escalates
  INT→TERM→KILL itself; the -K client then exits on its own) else `proc.terminate()`;
  `_escalate` = `proc.kill()` / re-bkill + terminate the client. Never block: the sidecar's
  future completes through the shared `futures.wait({**running, **side_running}, timeout=...)`
  (short ~5s timeout while `stopping` is non-empty so escalation ticks; existing
  REPORT_TICK timeout logic composes).
- Completion branch: in `stopping` OR clean self-exit → `finish_managed(ok=True)`, finalize
  `"done"` (early self-exit prints `[sidecar] exited early`; dependents unlock). Nonzero
  self-exit → `on_fail`: restart (if stop condition not now satisfied and under cap;
  `_assert_not_running` passes since the row was closed) / ignore vs abandon: both record
  `failed`; "ignore" lets dependents proceed against a PRIOR done row (loud print) and only
  abandons them when no done row exists (nothing to read); "abandon" is stage-failure
  semantics.
- Loop condition: `while pending or side_pending or running or side_running`; stuck-assert
  covers both. Pool sizing: `min(MAX_PARALLEL, len(stages)) + len(sidecars)` — a sidecar
  thread must never queue behind stage backlog (a queued future has no proc to kill).
  Sidecars don't count against `MAX_PARALLEL` (launch check counts `running` only).
- Loop end needs no epilogue: last stage finalize triggers stops; the loop spins on
  `side_running` until they join.

**`_run_stage(cmd, run_id, handle=None)`**: fills `handle["proc"]`/`["jobid"]`; guards the
`set_lsf_jobid/state` calls with `run_id is not None` (ledger=False).

**`rundb.py`** (~6 lines): `Run.watch: list[str]` from `$SPEARMINT_WATCH`, exactly like
`inputs`/`SPEARMINT_INPUTS`. Sidecar rows are ordinary rows — status/browse/gc/stale_inputs
work unchanged; "done regardless of exit code" is purely driver-side.

## Files

> Historical note: references below to `e.report` predate the split between declarative live
> dashboards and independent retrospective analysis scripts. `Experiment.report` no longer
> exists.

- `spearmint/dagrunner.py` — ~90% of the diff (Sidecar, Experiment.Sidecar, helpers
  `_signal_stop`/`_escalate`, `SIDECAR_RESTARTS = 3`, run_experiment integration)
- `spearmint/rundb.py` — `Run.watch` + `SPEARMINT_WATCH` read
- `spearmint/examples/watcher.py` — NEW (~35 lines): `with rundb.run() as r:` poll
  `Path(r.watch[0])/"metrics.jsonl"` every 0.5s, write cumulative `val.jsonl` rows to
  `r.outdir`, loop until killed; `--fail-after N` exits 1 after N rows
- `spearmint/examples/toy_sidecar_demo.py` — NEW (~60 lines): `train` (script.py streams
  20 rows/1s) + `watcher = e.Sidecar(..., watch=[train])` + `summary` stage `req=[watcher]`
  reading `r.inputs` + `e.report` overlaying train + val curves
- `README.md` — sidecar section under the report section

## Verification

1. Fresh demo run → `{train: done, watcher: done, summary: done}`; watcher row `done` in
   `spearmint status`; `val.jsonl` non-empty; summary launched only after watcher finalized;
   report shows both curve families.
2. Re-run → all three skip (watcher `skipped` via prior done row).
3. `--new train` → watcher re-runs (fresh dir) and summary re-runs (watch-edge forcing).
4. Train forced to `--fail` → watcher stopped `done`, summary still runs; a
   `req=[watcher, train]` variant abandons.
5. Watcher `--fail-after` under each `on_fail`: ignore (failed row, train untouched, loud);
   abandon (summary abandons); restart (extend relaunch into the same outdir, ≤3 attempts
   visible as ledger history).
6. `stop="any"` with two watched trains of different lengths → stops at the first finalize.
7. Ctrl-C the driver mid-run → next pass `reconcile_wip` marks the orphan wip rows failed.
8. Kill-machinery unit check locally (SIGTERM path); the bkill path is exercised by
   `cluster_smoke`-style run on the real cluster later (out of scope here, noted in demo docstring).

Commit with jj, advance main. Out of scope: the mia-muvit ckpt→`model.pth`-folder converter
that a real e00 validation sidecar needs (that lands in mia-muvit when we port e00).

---

# Is the DAG the right substrate? Four concurrency models compared

The sidecar plan above bolts lifecycle coupling onto the DAG with new vocabulary
(`watch=`/`stop=`/`on_fail=`). That's the same shape of mistake the JSON view-specs made —
rebuilding, inside a data structure, a worse version of things a language already says — and
the per-sidecar-configurability requirement is the tell: when every semantic needs a knob,
you're designing a language. Before committing, the alternatives:

## A. Full async: the experiment file is an async program

`job = ctx.submit("pretrain_mae", cmd, gpu(...))` returns an awaitable; `await job` is a
dependency edge; every sidecar policy becomes ordinary code:

```python
val = ctx.submit("val_a", cmd, watch_dir=train.outdir)   # just another job
result = await train
val.cancel()                                             # "stop when train stops"
# stop="any"   -> asyncio.wait(FIRST_COMPLETED)
# on_fail="restart" -> a while-loop
# "run B while A and C run" -> whatever Python can say. No vocabulary to design, ever.
```

The load-bearing realization: **everything the DAG uniquely provides actually lives in the
LEDGER, not in the DAG structure.**

- Skip-if-done: `ctx.submit(job_key)` consults rundb and returns an already-done future.
- Crash-resume of orchestration: re-run the program; memoized submits fast-forward. Cleaner
  than today, and the same mechanism as skip.
- Provenance/inputs: recorded at submit exactly as `start_managed` does now.
- Forced re-runs + cascade: a forced set + the recorded-inputs staleness machinery
  (`stale_inputs`) that already exists.

Genuine losses: the *static plan* — a status page can only show jobs the program has reached;
`--replace 'w*'` name-resolution wants jobs declared before submission. Mitigable
(pre-declared names), but real. And cancellation semantics are subtle (CancelledError
handling, shielding ledger writes, the same bkill machinery the sidecar plan needs anyway).

## B. Event-based CSP (channels + select)

Splits in two, and neither is a distinct system for spearmint:

- **In-driver CSP** = asyncio tasks + queues. Channels are a discipline you'd adopt *inside*
  option A where they help, not a competing architecture.
- **Cross-process CSP** already exists here in disguise: **the shared filesystem is the
  channel.** The trainer writing `epoch007.ckpt` is the send; the validator's poll is the
  receive — NFS-native, zero infrastructure, crash-durable. Socket CSP between compute nodes
  would buy sub-second reactivity for events that occur every few minutes (checkpoints):
  the wrong trade everywhere it matters in this domain.

## C. Persistent TCP orchestrator (every job connected to the driver)

Buys: push-based liveness (no bjobs polling), live metric streaming, remote control ("drop
the LR now"). Costs: every worker must speak a protocol (killing the untouched-hydra-worker
property); the driver becomes a stateful server whose crash orphans connections (today a dead
driver loses nothing that `reconcile_wip` can't repair); network-partition-vs-process-death
ambiguity imports the distributed-systems bug class spearmint has structurally avoided.
And this system already exists: it's Ray. If spearmint ever needs this, the move is ADOPT,
not rebuild — spearmint's identity (post-launch/remote deletion) is being the thing that
isn't this. **Rejected.**

## D. Message queues

A broker (Redis/Rabbit) is a long-lived cluster service someone must operate — new
infrastructure, new failure domain. But note what already exists: **the ledger is a durable
event log** (rows are events, the dashboard is a consumer), and **a directory of files is a
fine queue** on a shared FS (a future tile-processing work-queue would be exactly this).
Brokers earn their complexity when producers/consumers are decoupled across teams/time;
an experiment's stages are the opposite. **Rejected as infrastructure; file-dirs-as-queues
when the need arrives.**

## Synthesis

C and D reject cleanly. B collapses into A (in-driver) or into the filesystem (cross-process,
already have). The live fork:

| | DAG + sidecar bolt-on (above) | async core |
|---|---|---|
| sidecar/validator | new vocabulary, ~150 scheduler lines | ~10 lines of user code |
| skip/force/resume/provenance | already works | same ledger mechanics through `ctx.submit` (~150-200 lines, REPLACING the ~350-line scheduler) |
| static plan visibility, `--replace` UX | free | degraded unless jobs pre-declared |
| kill machinery (bkill etc.) | needed | needed (identical work) |
| readability | declarative `build()` reads well for simple DAGs | imperative main(); simple pipelines read worse |
| future lifecycle patterns | a knob each time | just code, forever |

**Candidate synthesis: async core + DAG sugar.** Rebuild the scheduler on asyncio;
`run_experiment` becomes ~40 lines of gather-loops over `ctx.submit`, so
`e.Stage`/`e.run()`/`main()` keep working verbatim for simple pipelines; experiments needing
lifecycle coupling write an async function; ship a 10-line `sidecar()` helper as a PATTERN
(example code), not vocabulary. One failure-propagation rule (exceptions through awaits),
one identity/provenance layer (the ledger), no second language.

Open decision: async core + sugar vs. shipping the sidecar bolt-on above vs. prototyping the
async core as a sibling commit and comparing side by side.

---

# Prototype results (spearmint/aio.py, this commit)

Built and verified: `Ctx.submit` (ledger-memoized, same skip/force/cascade semantics as the
scheduler — the cascade decision must defer to launch time, since `.ran` isn't knowable at
submit), awaitable `Job` with `.outdir` live at submit for dep-free jobs (the validator
handshake needs NO new env var — the experiment code passes `train.outdir` as plain argv),
`.cancel()` = deliberate stop (signal, 15s escalation, row closes `done`, the await still
resolves), `JobFailed` as the whole failure-propagation story, `aio.main` with the standard
force flags + `--submit`.

**Numbers:**

| | DAG scheduler | aio core |
|---|---|---|
| core lines | 308 (graph helpers + run_experiment + _run_stage) **+ ~150 planned for sidecars** | **303, sidecars included** (they're not a feature) |
| fanout example | toy_fanout.py, 30 lines | toy_aio_fanout.py, 23 lines — reads near-identically |
| sidecar expression | 4 knobs (`watch/stop/on_fail/ledger`), ~150 scheduler lines, 13 edge-case rulings | **8 lines of user code** (toy_aio_sidecar.py); every knob is a visible Python construct |

**Verified:** fanout fresh/skip/`--new 'w*'` force+cascade; sidecar demo end-to-end (watcher
validates all 20 steps DURING training, stopped → `done`, summary consumes `r.inputs`,
re-run all-skips); failure paths (`--fail-after` → catchable `JobFailed`, train unharmed,
dep cascade through `gather`). Ledger rows indistinguishable from scheduler rows in
status/browse.

**Honest costs observed:** no static plan (force patterns warn-at-end instead of failing up
front; status shows only reached jobs); `job.outdir` is only submit-time-available for
dep-free jobs (asserted with a pointed message otherwise); cancellation correctness took care
(deliberate-stop vs loop-teardown are distinct paths); bkill path untested off-cluster.

**Not yet built** (would come with adoption, not the bakeoff): DAG sugar reimplementing
`Experiment.run()` over `ctx.submit` (~40 lines, keeps e.Stage files working verbatim),
`e.report`-equivalent (an asyncio periodic task — cleaner than the wait-timeout hack),
`[stale]` prints on skip, `closure()`/shared-stage equivalents.
