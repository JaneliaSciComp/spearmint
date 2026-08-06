"""Minimal DAG scheduler over rundb.py -- a from-scratch prototype of "phase 2" (job_key-tagged
stages, skip-if-done via a Stage's own resolved `.savedir`, dependency paths read straight off
an upstream Stage's `.savedir` instead of fixed at build time), kept deliberately separate from
orchestration.py so it can be freely reshaped without touching anything that already works.

Stages run as blocking subprocesses (plain local commands, or LSF jobs via a bsub -K prefix --
see lsf.py), independent ones concurrently, and the ONLY state this scheduler tracks itself is
which stages to abandon after a failure. All completion/identity bookkeeping lives in rundb.db,
with the rows of scheduler-launched stages MANAGED by this driver (inserted at launch, closed
from the stage's exit code -- compute nodes must never write the shared-filesystem sqlite db;
see rundb.start_managed/finish_managed and rundb.run's managed branch).
"""

import re
import subprocess
from concurrent import futures
from dataclasses import dataclass, field
from typing import Callable

from . import rundb
from .config import CONFIG

# Max concurrently-running stage subprocesses. A ready stage beyond this stays pending until a
# slot frees, so [run] lines are only ever printed for stages actually executing.
MAX_PARALLEL = CONFIG.max_parallel

# job_key -> the one Stage object allowed to have claimed it, this process's lifetime. Catches
# an accidental prefix/name collision (two *different* Stage objects landing on the same
# job_key) loudly at definition time, rather than silently letting one experiment's scheduler
# skip its own work because of an unrelated stage that happens to share a string.
_registered_job_keys: "dict[str, Stage]" = {}


# eq=False: identity-based hashing, so a Stage works as a dict/set key (mirrors
# orchestration.Stage's same reasoning -- the reverse-edge map keys on Stage objects).
@dataclass(eq=False)
class Stage:
    name: str
    job_key: str
    # Builder resolved at submit time (after `requires` are confirmed done), taking this run's
    # mode ("new"/"extend"/"replace") so it can append a bare --extend/--replace flag (nothing
    # for the default "new") -- rundb.py parses that flag straight out of its own sys.argv and
    # resolves the actual outdir from job_key+mode itself, so this scheduler never has to look
    # one up or special-case any mode's command. Same "lazy command" shape as orchestration.py's
    # _eval_cmd reading a threshold file written by an earlier stage. By the time THIS builder
    # runs, every stage in `requires` already has an up-to-date `.savedir` (run_experiment sets
    # it right before calling this) -- reference ``upstream_stage.savedir`` directly instead of
    # querying rundb.latest_outdir(upstream_stage.job_key) yourself.
    command: Callable[[str], "list[str]"]
    requires: "list[Stage]" = field(default_factory=list)
    # PLAIN-command stage (hydra/argparse workers that would choke on spearmint's argv): when
    # set, run_experiment appends ONLY these templates formatted with the run's outdir (e.g.
    # "+run_dir={}") -- no --job-key/--run-row/mode flags. Legit because the driver fully
    # manages the row; the child needn't know spearmint exists.
    outdir_args: "list[str] | None" = None
    # Set by run_experiment once this job_key is confirmed done (freshly run or skipped) --
    # never passed in at construction time, so it's excluded from __init__.
    savedir: "str | None" = field(default=None, init=False)


class Experiment:
    """Builder for a group of Stages sharing a job_key prefix and a command prefix (e.g. the
    interpreter invocation every stage's command starts with) -- matches
    orchestration.ExperimentConfig.add_stage: ``.Stage(...)`` creates, registers, and returns
    each Stage so it can be referenced in a later stage's ``req=``.

    job_key is always ``f"{prefix}/{name}"`` -- never hand-typed at each call site, so a
    stage's own definition and whatever references it (a downstream ``req=``, or
    ``stage.savedir`` in a sibling's ``cmd=``) can never drift out of sync. ``--job-key
    <job_key>`` is appended to every stage's command automatically for the same reason (see
    ``script.py``, which reads it via argparse).

    A stage can override the experiment-wide command prefix with its own ``cmd_prefix`` -- a
    CALLABLE receiving the stage's job_key, resolved at submit time -- so per-stage launchers
    that need the job identity (an LSF ``bsub -K`` prefix wanting a -J job name and -oo log
    path; see lsf.gpu()/lsf.cpu()) still never hand-type it."""

    def __init__(self, prefix: str, cmd_prefix: "list[str] | None" = None):
        self.prefix = prefix
        self.cmd_prefix = list(cmd_prefix or [])
        self.stages: "list[Stage]" = []

    def Stage(
        self,
        name: str,
        cmd: "Callable[[], list[str]]",
        req: "list[Stage] | None" = None,
        cmd_prefix: "Callable[[str], list[str]] | None" = None,
        outdir_args: "list[str] | None" = None,
    ) -> Stage:
        job_key = f"{self.prefix}/{name}"
        assert job_key not in _registered_job_keys, (
            f"job_key {job_key!r} is already registered (by stage {_registered_job_keys[job_key].name!r}). "
            f"To share a stage across experiments, reuse that Stage object (import it) -- don't "
            f"build a second one with a matching prefix/name; two independently-built Stage "
            f"objects landing on the same job_key by coincidence is almost always a bug, not "
            f"intentional sharing. See shared_preprocess.py for the intended pattern."
        )
        # A plain-command stage (outdir_args set) gets no spearmint argv at all -- its worker
        # (hydra, argparse) wouldn't survive it; run_experiment appends the formatted outdir
        # templates instead.
        native = outdir_args is None
        full_cmd = lambda mode="replace": [
            *(self.cmd_prefix if cmd_prefix is None else cmd_prefix(job_key)),
            *cmd(),
            *(["--job-key", job_key] if native else []),
            *([f"--{mode}"] if native and mode != "new" else []),
        ]
        s = Stage(
            name=name, job_key=job_key, command=full_cmd, requires=req or [], outdir_args=outdir_args
        )
        _registered_job_keys[job_key] = s
        self.stages.append(s)
        return s

    def run(
        self,
        new: "list[Stage] | None" = None,
        extend: "list[Stage] | None" = None,
        replace: "list[Stage] | None" = None,
    ) -> "dict[str, str]":
        return run_experiment(self.stages, new=new, extend=extend, replace=replace)


def _reverse_edges(stages: "list[Stage]") -> "dict[Stage, list[Stage]]":
    """dependents[s] = local stages that directly require s."""
    dependents: "dict[Stage, list[Stage]]" = {s: [] for s in stages}
    for s in stages:
        for d in s.requires:
            if d in dependents:
                dependents[d].append(s)
    return dependents


def _topo_order(stages: "list[Stage]") -> "list[Stage]":
    """Deterministic topological order (Kahn's), ties broken by input order.

    A stage's ``requires`` may name a Stage that ISN'T in ``stages`` at all -- e.g. a shared
    stage owned by a different experiment file's Experiment (see shared_preprocess.py). Such an
    "external" require plays no part in this DAG's own ordering (it's not going to be run as
    part of this pass either way); run_experiment resolves its ``.savedir`` (confirming it's
    actually done in the process) instead, right before the stage that needs it would run."""
    local = set(stages)
    index = {s: i for i, s in enumerate(stages)}
    indeg = {s: sum(1 for d in s.requires if d in local) for s in stages}
    dependents = _reverse_edges(stages)
    ready = sorted((s for s in stages if indeg[s] == 0), key=lambda s: index[s])
    order: "list[Stage]" = []
    while ready:
        s = ready.pop(0)
        order.append(s)
        freed = []
        for t in dependents[s]:
            indeg[t] -= 1
            if indeg[t] == 0:
                freed.append(t)
        ready = sorted(ready + freed, key=lambda s: index[s])
    assert len(order) == len(stages), "cycle in stage graph"
    return order


def _transitive_dependents(seeds: "list[Stage]", dependents: "dict[Stage, list[Stage]]") -> "set[Stage]":
    """Reverse-edge closure, including the seeds themselves -- forcing a stage to redo its work
    means everything that reads its (possibly now different) output needs to redo theirs too."""
    out: "set[Stage]" = set()
    stack = list(seeds)
    while stack:
        s = stack.pop()
        if s in out:
            continue
        out.add(s)
        stack.extend(dependents.get(s, []))
    return out


def _stale_deps(s: "Stage") -> "list[str]":
    """job_keys s's most recent success was built against an outdated version of. Prefers exact
    recorded provenance (rundb.stale_inputs -- the input run_ids this scheduler stamped onto the
    run; robust to a dep re-run via "extend" reusing its directory, and to same-second ties).
    Falls back to a timestamp heuristic when the run has no recorded inputs (launched outside
    dagrunner): a dep is stale if its most recent success FINISHED after s's most recent success
    STARTED -- only "dep fully finished before s started reading" counts as fresh, which also
    catches a dep whose re-run was still in flight when s began. The fallback infers what s read
    from when it ran, not from a record of what it read."""
    exact = rundb.stale_inputs(s.job_key)
    if exact is not None:
        return exact
    mine = rundb.started_at(s.job_key, status="done")
    if mine is None:
        return []
    stale = []
    for d in s.requires:
        dep_ended = rundb.ended_at(d.job_key, status="done")
        if dep_ended is not None and dep_ended > mine:
            stale.append(d.job_key)
    return stale


def closure(stages: "list[Stage]") -> "list[Stage]":
    """``stages`` plus every Stage they require, transitively -- for handing a consumer
    experiment's stages to run_experiment such that its shared/external requires get BUILT as
    part of the same pass (topo-ordered, abandon-on-failure spanning the boundary) instead of
    merely verified already-done. ``run_experiment(closure(e.stages))`` is the explicit spelling
    of "run this experiment and whatever upstream it's missing"; a bare ``e.run()`` keeps the
    stricter consumers-verify-but-don't-build default. Deduped; input order preserved with
    discovered requires appended (run_experiment topo-sorts regardless)."""
    out = list(stages)
    seen = set(stages)
    i = 0
    while i < len(out):
        for d in out[i].requires:
            if d not in seen:
                seen.add(d)
                out.append(d)
        i += 1
    return out


def _run_stage(cmd: "list[str]", run_id: int) -> int:
    """Launch a stage process, streaming its stdout through ours while watching bsub -K's
    chatter (never printed by a plain local command): the 'Job <id> is submitted' ack upgrades
    the managed row's liveness handle from the driver's identity to the stage's own LSF job id
    (and marks it PEND -- freshly submitted means queued), and '<<Starting on <host>>>' records
    the queued->running transition, host included. No bjobs polling -- the information streams
    past us anyway. Returns the process's exit code."""
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    assert proc.stdout is not None
    for line in proc.stdout:
        print(line, end="", flush=True)
        m = re.match(r"Job <(\d+)> is submitted", line)
        if m is not None:
            rundb.set_lsf_jobid(run_id, m.group(1))
        m = re.match(r"<<Starting on (\S+?)>>", line)
        if m is not None:
            rundb.set_lsf_state(run_id, f"RUN {m.group(1)}")
    return proc.wait()


def run_experiment(
    stages: "list[Stage]",
    new: "list[Stage] | None" = None,
    extend: "list[Stage] | None" = None,
    replace: "list[Stage] | None" = None,
) -> "dict[str, str]":
    """Run every stage, independent stages CONCURRENTLY: a stage launches (as a local
    subprocess) as soon as every local require of it is finalized, and the scheduler then blocks
    in concurrent.futures.wait -- event-driven, no polling -- until some child exits, finalizes
    it, and launches whatever that unlocked. Skip stages rundb.py already has a 'done' row for.
    Abandon a stage's transitive dependents if it (or any of its own requires) failed/was
    abandoned -- orchestration._run_dag's same rule, applied as stages finalize.

    A ``requires`` target outside ``stages`` (an external/shared stage -- see _topo_order and
    closure()) is never run in this pass, so its state can't change during it: every external is
    resolved (its ``.savedir`` populated) and asserted already-done UP FRONT, before anything
    launches. This scheduler never waits on an external -- one that isn't done is a hard error
    before any work starts, not something to queue behind.

    ``new``/``extend``/``replace`` each force a stage to run despite already being done, plus
    its transitive dependents (their own prior output was computed against the old version of
    this stage's output, so they need to see the new one too) -- those dependents always get a
    plain ``new`` treatment regardless of which bucket the seed stage came from. All three just
    become a bare --extend/--replace flag appended onto the stage's own command
    (Experiment.Stage's ``full_cmd``, above; nothing is appended for the default "new");
    rundb.py parses that flag out of its own sys.argv and resolves what it actually means for
    the outdir (fresh / resume job_key's last one / clear-then-reuse job_key's last one) once
    it's running inside the subprocess -- this scheduler never looks up or touches a path
    itself:
      new:     a normal from-scratch re-attempt, fresh directory.
      extend:  resume into the SAME directory the stage last wrote.
      replace: clear job_key's most recent outdir and start over in that SAME directory --
               older outdirs from earlier "new" attempts are left untouched (this supersedes
               only the one attempt being replaced, not job_key's whole history).

    A stage that isn't done yet and ISN'T in any of new=/extend=/replace= (the common case: it
    simply hasn't succeeded before) defaults to "replace", not "new" -- this branch is only ever
    reachable when job_key has no 'done' row at all (a done stage is permanently skipped instead,
    see below), so "replace" here can never destroy a successful result, only clear away a
    wip/failed attempt before retrying in the same directory. For a stage's true first-ever
    attempt this degenerates to exactly "new" (nothing to clear yet). This keeps a debug
    run-fails-fix-rerun loop from leaving one orphaned directory behind per failed attempt.
    One nuance: a "wip" row (a subprocess killed hard enough -- e.g. SIGKILL -- to bypass
    rundb.run()'s exception handling) is cleared the same way; don't rely on this default while a
    same-job_key process might still be concurrently writing into that directory.

    A stage that's about to be skipped (already done, not forced) whose dependencies completed
    MORE RECENTLY than it last did prints a [stale] warning first -- it still skips (this
    scheduler never auto-forces just because something upstream changed), but it tells you."""
    local = set(stages)
    seeds_all = [s for group in (replace, extend, new) if group for s in group]
    assert len(seeds_all) == len(set(seeds_all)), (
        "a stage was passed to more than one of replace=/extend=/new="
    )
    seed_mode: "dict[Stage, str]" = {}
    for s in replace or []:
        seed_mode[s] = "replace"
    for s in extend or []:
        seed_mode[s] = "extend"
    for s in new or []:
        seed_mode[s] = "new"
    dependents = _reverse_edges(stages)
    forced = _transitive_dependents(list(seed_mode), dependents) if seed_mode else set()
    mode = {s: seed_mode.get(s, "new") for s in forced}

    order = _topo_order(stages)

    # Externals never run in this pass, so their state is fixed: resolve every one's savedir
    # (confirming it's actually done, via savedir being non-None instead of a separate
    # rundb.is_done query) before anything launches.
    for s in order:
        external_not_done = []
        for d in s.requires:
            if d not in local:
                d.savedir = rundb.latest_outdir(d.job_key, status="done")
                if d.savedir is None:
                    external_not_done.append(d.job_key)
        assert not external_not_done, (
            f"{s.name} requires external stage(s) {external_not_done} that aren't done yet -- "
            f"run whatever builds them first, or schedule them too via closure()"
        )

    # Keyed by job_key, not stage name -- names are only unique within one Experiment, and
    # run_experiment can be handed stages from several (see toy_dag_a.py running a shared stage).
    # A stage is finalized once its job_key is present here; a stage is READY to launch once
    # every local require is finalized.
    status: "dict[str, str]" = {}
    pending = list(order)
    # Future -> (stage, its managed row's run_id) for each currently-running child.
    running: "dict[futures.Future, tuple[Stage, int]]" = {}

    def finalize(s: Stage, st: str) -> None:
        status[s.job_key] = st
        if st == "done":
            s.savedir = rundb.latest_outdir(s.job_key, status="done")

    with futures.ThreadPoolExecutor(max_workers=max(1, min(MAX_PARALLEL, len(stages)))) as pool:
        while pending or running:
            # Launch pass: pending is in topo order, so a stage finalized here (abandoned or
            # skipped) unlocks its own dependents later in the SAME scan -- after one scan,
            # every stage is finalized, running, or genuinely waiting on a running child.
            still_pending: "list[Stage]" = []
            for s in pending:
                deps = [d for d in s.requires if d in local]
                if any(status.get(d.job_key) in ("failed", "abandoned") for d in deps):
                    finalize(s, "abandoned")
                    print(f"[abandon] {s.name}: upstream failed/abandoned", flush=True)
                    continue
                if not all(d.job_key in status for d in deps):
                    still_pending.append(s)
                    continue
                if s not in mode:
                    s.savedir = rundb.latest_outdir(s.job_key, status="done")
                    if s.savedir is not None:
                        stale = _stale_deps(s)
                        if stale:
                            print(
                                f"[stale] {s.name}: {stale} have newer results than {s.job_key}'s last "
                                f"success was built from -- skipping anyway (pass new=/extend=/replace= to force)",
                                flush=True,
                            )
                        finalize(s, "skipped")
                        print(f"[skip] {s.name}: already done ({s.job_key})", flush=True)
                        continue
                if len(running) >= MAX_PARALLEL:
                    still_pending.append(s)  # ready, but all slots busy -- retry next scan
                    continue
                stage_mode = mode.get(s, "replace")
                base_cmd = s.command(stage_mode)
                # Provenance, known exactly at launch: the dep runs whose outputs this stage is
                # about to read (all confirmed done by this point) -- stamped at row insert.
                input_ids: "list[int]" = []
                for d in s.requires:
                    rid = rundb.latest_run_id(d.job_key, status="done")
                    assert rid is not None, f"{s.name}: require {d.job_key} has no done run"
                    input_ids.append(rid)
                # This DRIVER inserts the stage's row (managed run) and closes it from the exit
                # code below -- the child never touches the db (compute nodes writing one sqlite
                # file over the shared filesystem is what broke). --run-row/--run-outdir hand
                # the child its identity; rundb.run() sees them and skips all db work.
                row = rundb.start_managed(s.job_key, stage_mode, argv=base_cmd, inputs=input_ids)
                if s.outdir_args is None:
                    cmd = [*base_cmd, "--run-row", str(row.run_id), "--run-outdir", row.outdir]
                else:
                    cmd = [*base_cmd, *(a.format(row.outdir) for a in s.outdir_args)]
                print(f"[run] {s.name}: {' '.join(cmd)}", flush=True)
                running[pool.submit(_run_stage, cmd, row.run_id)] = (s, row.run_id)
            pending = still_pending
            if not running:
                assert not pending, f"scheduler stuck with pending stages {[s.name for s in pending]}"
                break
            # Block -- no polling -- until at least one child exits, then finalize it.
            done_futs, _ = futures.wait(running, return_when=futures.FIRST_COMPLETED)
            for fut in done_futs:
                s, run_id = running.pop(fut)
                ok = fut.result() == 0
                rundb.finish_managed(run_id, ok)
                finalize(s, "done" if ok else "failed")
                print(f"  [{s.name}] {'ok' if ok else 'FAILED'}", flush=True)
    return status
