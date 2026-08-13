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

import os
import re
import subprocess
import sys
from concurrent import futures
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from . import rundb

# Max concurrently-running stage subprocesses. A ready stage beyond this stays pending until a
# slot frees, so [run] lines are only ever printed for stages actually executing.
MAX_PARALLEL = 32

# How often the scheduler re-renders an Experiment's report while stages are RUNNING (see
# Experiment.report) -- frequent enough that mid-stage curves (a growing metrics.jsonl) stay
# fresh, rare enough that rendering cost is noise. Finalizes re-render immediately regardless.
REPORT_TICK_SECONDS = 120

# job_key -> the one Stage object allowed to have claimed it, this process's lifetime. Catches
# an accidental prefix/name collision (two *different* Stage objects landing on the same
# job_key) loudly at definition time, rather than silently letting one experiment's scheduler
# skip its own work because of an unrelated stage that happens to share a string.
_registered_job_keys: "dict[str, Stage]" = {}


@dataclass
class Config:
    """Where an Experiment's ledger + run outputs live -- spearmint's whole per-project context,
    as a value you pass in (no config files, no env vars, nothing resolved at import). Both
    fields default from the experiment file's own location, so most projects never construct
    one; to relocate outputs (e.g. onto scratch), define a single ``CFG = spearmint.Config(
    root=...)`` in one shared module and pass it to every Experiment. The first Experiment
    anchors the process's ledger (rundb.anchor); a second one with a conflicting root asserts."""

    root: "str | None" = None  # ledger + run outputs; None -> <repo>/output_rundb
    repo: "str | None" = None  # checkout provenance is read from; None -> the experiment file's


# eq=False: identity-based hashing, so a Stage works as a dict/set key (mirrors
# orchestration.Stage's same reasoning -- the reverse-edge map keys on Stage objects).
@dataclass(eq=False)
class Stage:
    name: str
    job_key: str
    # Builder resolved at submit time (after `requires` are confirmed done) -- same "lazy
    # command" shape as orchestration.py's _eval_cmd reading a threshold file written by an
    # earlier stage. Identity (job_key, mode, managed row) is NOT the builder's concern:
    # run_experiment prepends it as an ``env SPEARMINT_*=...`` prefix, invisible to the
    # worker's own argv parsing. By the time THIS builder runs, every stage in `requires`
    # already has an up-to-date `.savedir` (run_experiment sets it right before calling this)
    # -- reference ``upstream_stage.savedir`` directly instead of querying
    # rundb.latest_outdir(upstream_stage.job_key) yourself.
    command: Callable[[], "list[str]"]
    requires: "list[Stage]" = field(default_factory=list)
    # PLAIN-command stage (a worker that never calls rundb itself): when set, run_experiment
    # appends these templates formatted with the run's outdir (e.g. "+run_dir={}") -- the
    # worker's own output override -- so the child needn't know spearmint exists. The
    # SPEARMINT_* env prefix rides along regardless (harmless to a worker that doesn't look;
    # keeps a worker that DOES call rundb.run() managed instead of silently writing the db
    # from a compute node, and enables hydra's ${oc.env:SPEARMINT_RUN_OUTDIR,...}).
    outdir_args: "list[str] | None" = None
    # Set by run_experiment once this job_key is confirmed done (freshly run or skipped) --
    # never passed in at construction time, so it's excluded from __init__. Internal backing
    # for the `savedir` property; scheduler code reads/writes this directly (it owns the
    # None-means-unresolved state), everyone else goes through the property.
    _savedir: "str | None" = field(default=None, init=False)

    @property
    def savedir(self) -> str:
        """The stage's resolved output dir -- a plain ``str``, so referencing it in a
        downstream stage's command type-checks. Only available once run_experiment has
        confirmed this stage done: reference it lazily (inside a ``cmd=lambda``, resolved at
        submit time after requires complete), never at DAG-build time."""
        assert self._savedir is not None, (
            f"stage {self.name!r} ({self.job_key}) has no resolved savedir yet -- it's set once "
            f"run_experiment confirms the stage done; reference it inside a cmd=lambda (resolved "
            f"at submit time), not at DAG-build time"
        )
        return self._savedir


class Experiment:
    """Builder for a group of Stages sharing a job_key prefix and a command prefix (e.g. the
    interpreter invocation every stage's command starts with) -- matches
    orchestration.ExperimentConfig.add_stage: ``.Stage(...)`` creates, registers, and returns
    each Stage so it can be referenced in a later stage's ``req=``.

    job_key is always ``f"{prefix}/{name}"`` -- never hand-typed at each call site, so a
    stage's own definition and whatever references it (a downstream ``req=``, or
    ``stage.savedir`` in a sibling's ``cmd=``) can never drift out of sync. run_experiment
    hands it to every native stage automatically for the same reason, as $SPEARMINT_JOB_KEY
    via an ``env`` prefix (see ``script.py``, whose rundb.run() reads it from the environment).

    A stage can override the experiment-wide command prefix with its own ``cmd_prefix`` -- a
    CALLABLE receiving the stage's job_key, resolved at submit time -- so per-stage launchers
    that need the job identity (an LSF ``bsub -K`` prefix wanting a -J job name and -oo log
    path; see lsf.gpu()/lsf.cpu()) still never hand-type it.

    Constructing an Experiment anchors the process's ledger from ``config`` (see Config):
    unset fields default from THIS experiment file's location -- the caller's ``__file__`` ->
    enclosing git repo -> <repo>/output_rundb -- so the ledger follows the code that defines
    the experiment, wherever it's launched from."""

    def __init__(self, prefix: str, cmd_prefix: "list[str] | None" = None, config: "Config | None" = None):
        if config is None:
            config = Config()
        repo = config.repo
        if repo is None:
            caller = sys._getframe(1).f_globals.get("__file__")
            assert caller, (
                "can't locate the experiment file (caller has no __file__, e.g. a REPL) -- "
                "pass Config(repo=...) explicitly"
            )
            repo = rundb._git_root(str(Path(caller).resolve().parent))
        rundb.anchor(config.root or f"{repo}/output_rundb", repo=repo)
        self.config = config
        self.prefix = prefix
        self.cmd_prefix = list(cmd_prefix or [])
        # A shell string pasted as one element (["uv run python"]) execs a program literally
        # named "uv run python" -- caught here at build time, not as GNU env's cryptic ENOENT.
        spaced = [c for c in self.cmd_prefix if any(ch.isspace() for ch in c)]
        assert not spaced, (
            f"cmd_prefix elements contain whitespace {spaced!r} -- pass one argv element per "
            f"list item, e.g. ['uv', 'run', 'python']"
        )
        self.stages: "list[Stage]" = []
        # Optional report renderer: a plain function ``fn(savedir) -> html`` (build it with
        # spearmint.viz; ``savedir`` is self.savedir below, so partial runs render too). The
        # scheduler re-runs it after every stage finalize, every REPORT_TICK_SECONDS while
        # anything is running, and once at DAG end, writing ROOT/_reports/<prefix>/report.html
        # (linked from the status table). A raising report renders as a printed error -- it
        # can never fail or delay a stage.
        self.report: "Callable[[Callable], str] | None" = None

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
        # Identity is run_experiment's job (env prefix / outdir_args); this builder only
        # composes launcher prefix + the stage's own command.
        full_cmd = lambda: [
            *(self.cmd_prefix if cmd_prefix is None else cmd_prefix(job_key)),
            *cmd(),
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
        return run_experiment(self.stages, new=new, extend=extend, replace=replace,
                              on_update=self._render_report if self.report else None)

    def savedir(self, stage: "Stage | str") -> "str | None":
        """A stage's latest DONE outdir (by object or name), or None -- what a report fn keys
        on, so a partially-complete run still renders its finished parts (contrast
        Stage.savedir, which asserts when unresolved: right for commands, wrong for reports).
        Reads the ledger, so it also sees runs from before this pass."""
        s = stage if isinstance(stage, Stage) else next((t for t in self.stages if t.name == stage), None)
        assert s is not None, f"no stage named {stage!r} in experiment {self.prefix!r}"
        return rundb.latest_outdir(s.job_key, status="done")

    def _render_report(self) -> None:
        assert self.report is not None
        out = Path(rundb.root()) / rundb.REPORTS_DIR / self.prefix
        out.mkdir(parents=True, exist_ok=True)
        (out / "report.html").write_text(self.report(self.savedir))

    def main(self, argv: "list[str] | None" = None) -> "dict[str, str] | None":
        """The standard experiment-file entrypoint -- spearmint's own flags, parsed explicitly
        (never sniffed from a library call): ``--new/--extend/--replace STAGE`` (repeatable)
        force stages by name (see run_experiment for what each mode means), and ``--submit``
        submits this same invocation as the long-lived LSF driver job instead of running
        in-process -- the driver re-runs sys.argv minus --submit, so the tier and force flags
        ride along (and the DAG was already built by the time we submit, so definition errors
        fail here on your terminal, not minutes later in a driver log). Returns run()'s status
        dict, or None when it only submitted.

        ``argv`` defaults to sys.argv[1:]; an experiment file with its own args (a tier, a
        worker dispatch) parses them first and hands over the remainder:

            args, rest = parser.parse_known_args()
            build(TIERS[args.tier]).main(rest)

        Anything unrecognized here is a usage error -- worker-only flags belong before the
        parse_known_args split, not in ``argv``."""
        import argparse

        p = argparse.ArgumentParser(prog=f"{self.prefix} (spearmint stage flags)")
        # nargs="+" + extend: several names per flag (--new w1 w2), still repeatable. Each name
        # may be a glob over stage names (--new 'w*'), for loop-generated stages.
        p.add_argument("--new", action="extend", nargs="+", default=[], metavar="STAGE",
                       help="force STAGE(s) (+ dependents) to re-run in a fresh dir; globs ok")
        p.add_argument("--extend", action="extend", nargs="+", default=[], metavar="STAGE",
                       help="force STAGE(s) (+ dependents) to resume the existing dir; globs ok")
        p.add_argument("--replace", action="extend", nargs="+", default=[], metavar="STAGE",
                       help="force STAGE(s) (+ dependents), clearing the existing dir first; globs ok")
        p.add_argument("--submit", action="store_true",
                       help="submit this invocation as the LSF driver job (login node)")
        a = p.parse_args(sys.argv[1:] if argv is None else argv)
        if a.submit:
            from . import lsf  # local-only experiments never need the LSF module

            lsf.submit_driver(sys.argv[0], *(x for x in sys.argv[1:] if x != "--submit"))
            return None
        by_name = {s.name: s for s in self.stages}

        def resolve(names: "list[str]") -> "list[Stage]":
            """Names -> Stages: exact match, else a glob over stage names (its expansion is
            printed -- forcing is destructive enough to deserve an echo). Deduped, order kept;
            a stage landing in two different mode buckets still trips run_experiment's assert."""
            from fnmatch import fnmatch

            out: "list[Stage]" = []
            for n in names:
                if n in by_name:
                    hits = [by_name[n]]
                else:
                    hits = [by_name[k] for k in sorted(by_name) if fnmatch(k, n)]
                    assert hits, f"no stage matches {n!r}; choices: {', '.join(sorted(by_name))}"
                    print(f"[force] {n!r} -> {[s.name for s in hits]}", flush=True)
                out += [s for s in hits if s not in out]
            return out

        status = self.run(new=resolve(a.new), extend=resolve(a.extend), replace=resolve(a.replace))
        print(status)
        return status


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
    on_update: "Callable[[], None] | None" = None,
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
    plain ``new`` treatment regardless of which bucket the seed stage came from. The mode goes
    to rundb.start_managed, which resolves what it actually means for the outdir (fresh /
    resume job_key's last one / clear-then-reuse job_key's last one):
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
                d._savedir = rundb.latest_outdir(d.job_key, status="done")
                if d._savedir is None:
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
            s._savedir = rundb.latest_outdir(s.job_key, status="done")

    def update() -> None:
        """Re-render the experiment's report (Experiment.report -> on_update). Presentation
        must never endanger the run: a raising report prints and the DAG carries on."""
        if on_update is None:
            return
        try:
            on_update()
        except Exception as e:  # noqa: BLE001 -- see docstring
            print(f"[report] render failed: {e!r}", flush=True)

    update()  # initial render, so the report exists (all-waiting) from the first second
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
                    s._savedir = rundb.latest_outdir(s.job_key, status="done")
                    if s._savedir is not None:
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
                base_cmd = s.command()
                # Provenance, known exactly at launch: the dep runs whose outputs this stage is
                # about to read (all confirmed done by this point) -- stamped at row insert.
                input_ids: "list[int]" = []
                for d in s.requires:
                    rid = rundb.latest_run_id(d.job_key, status="done")
                    assert rid is not None, f"{s.name}: require {d.job_key} has no done run"
                    input_ids.append(rid)
                # This DRIVER inserts the stage's row (managed run) and closes it from the exit
                # code below -- the child never touches the db (compute nodes writing one sqlite
                # file over the shared filesystem is what broke). EVERY stage gets the
                # ``env SPEARMINT_*=...`` identity prefix, invisible to its own argv parsing
                # (identity itself is recorded on the row, not in argv): a native child's
                # rundb.run() sees RUN_ROW/RUN_OUTDIR and skips all db work -- and so does an
                # outdir_args worker that happens to call rundb.run() anyway (without the env it
                # would silently become an UNMANAGED compute-node db writer). bsub ships the
                # submission environment to the compute node, so the prefix may wrap a bsub
                # launcher. outdir_args templates are recorded on the row VERBATIM ("output={}")
                # -- the formatted values aren't knowable before the row exists (run_id names
                # the outdir), and the row's own outdir column completes them unambiguously.
                argv = base_cmd if s.outdir_args is None else [*base_cmd, *s.outdir_args]
                row = rundb.start_managed(s.job_key, stage_mode, argv=argv, inputs=input_ids)
                # $SPEARMINT_INPUTS: the deps' resolved outdirs (requires order), so a fan-in
                # worker reads r.inputs instead of taking N paths via its own argv.
                inputs_env = (
                    [f"SPEARMINT_INPUTS={os.pathsep.join(d.savedir for d in s.requires)}"]
                    if s.requires else []
                )
                cmd = [
                    "env",
                    f"SPEARMINT_JOB_KEY={s.job_key}",
                    f"SPEARMINT_MODE={stage_mode}",
                    f"SPEARMINT_RUN_ROW={row.run_id}",
                    f"SPEARMINT_RUN_OUTDIR={row.outdir}",
                    *inputs_env,
                    *base_cmd,
                    *(a.format(row.outdir) for a in s.outdir_args or []),
                ]
                print(f"[run] {s.name}: {' '.join(cmd)}", flush=True)
                running[pool.submit(_run_stage, cmd, row.run_id)] = (s, row.run_id)
            pending = still_pending
            if not running:
                assert not pending, f"scheduler stuck with pending stages {[s.name for s in pending]}"
                break
            # Block until at least one child exits, then finalize it. With a report to keep
            # fresh, wake on a timer too (mid-stage data -- a growing metrics.jsonl -- changes
            # with no finalize to piggyback on); an empty done_futs is just a tick.
            done_futs, _ = futures.wait(running, return_when=futures.FIRST_COMPLETED,
                                        timeout=REPORT_TICK_SECONDS if on_update else None)
            for fut in done_futs:
                s, run_id = running.pop(fut)
                ok = fut.result() == 0
                rundb.finish_managed(run_id, ok)
                finalize(s, "done" if ok else "failed")
                print(f"  [{s.name}] {'ok' if ok else 'FAILED'}", flush=True)
            update()  # finalize batch, or a bare tick -- either way the report re-renders
    update()  # final render: every stage finalized (skips/abandons included)
    return status
