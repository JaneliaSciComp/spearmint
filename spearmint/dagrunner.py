"""AOT-declared DAG plans, compiled onto the aio execution core. An experiment file builds
``Experiment``/``Stage``s -- a STATIC plan, so force flags validate against real stage names
up front, cycles fail at run start, and the whole pipeline is visible before anything launches
-- and ``run_experiment`` lowers it to ``aio.Ctx.submit`` calls: each stage becomes a
ledger-memoized awaitable Job whose deps ARE its requires. Ordering, skip-if-done, force
cascades, failure/abandon propagation, and MAX_PARALLEL all come from the aio layer; this
module contributes only the declarative surface and its up-front validation.

Anything the static plan can't say (validators running WHILE training runs, retry loops,
dynamic fan-out) is written directly against spearmint.aio -- same ledger rows, same identity,
freely mixable per experiment file. See examples/toy_aio_sidecar.py.
"""

import asyncio
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from . import aio
from . import rundb

if TYPE_CHECKING:
    from .dashspec import Dashboard

# Max concurrently-running stage subprocesses. A ready stage beyond this stays pending until a
# slot frees, so [run] lines are only ever printed for stages actually executing.
MAX_PARALLEL = 32

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
    # Exactly one action: a lazy argv builder, or an in-experiment Python function. Functions
    # are self-dispatched through a fresh invocation of the experiment file, preserving the
    # same process/managed-run boundary as command stages.
    command: "Callable[[], list[str]] | None"
    function: "Callable[[rundb.Run], object] | None" = None
    _experiment_file: str = ""
    _experiment_argv: "list[str]" = field(default_factory=list)
    requires: "list[Stage]" = field(default_factory=list)
    # Launcher prefix (e.g. lsf's bsub -K flags), kept SEPARATE from the command: aio formats
    # any "{}" in prefix elements with the run's minted outdir at launch (-oo {}/log.txt).
    prefix: "list[str]" = field(default_factory=list)
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
        caller = sys._getframe(1).f_globals.get("__file__")
        repo = config.repo
        if repo is None:
            assert caller, (
                "can't locate the experiment file (caller has no __file__, e.g. a REPL) -- "
                "pass Config(repo=...) explicitly"
            )
            repo = rundb._git_root(str(Path(caller).resolve().parent))
        rundb.anchor(config.root or f"{repo}/output_rundb", repo=repo)
        self.config = config
        self.prefix = prefix
        self.experiment_file = str(Path(caller or sys.argv[0]).resolve())
        self.experiment_argv = list(sys.argv[1:])
        self.cmd_prefix = list(cmd_prefix or [])
        # A shell string pasted as one element (["uv run python"]) execs a program literally
        # named "uv run python" -- caught here at build time, not as GNU env's cryptic ENOENT.
        spaced = [c for c in self.cmd_prefix if any(ch.isspace() for ch in c)]
        assert not spaced, (
            f"cmd_prefix elements contain whitespace {spaced!r} -- pass one argv element per "
            f"list item, e.g. ['uv', 'run', 'python']"
        )
        self.stages: "list[Stage]" = []
        self.dashboard: "Dashboard | None" = None  # serialized for the generic viewer
        # A Stage assigned here is an ordinary retrospective stage. At run time every other
        # stage becomes an implicit dependency, so it produces one immutable final artifact.
        self.report: "Stage | None" = None

    def Stage(
        self,
        name: str,
        cmd: "Callable[[], list[str]] | None" = None,
        fn: "Callable[[rundb.Run], object] | None" = None,
        req: "list[Stage] | None" = None,
        cmd_prefix: "Callable[[str], list[str]] | None" = None,
        outdir_args: "list[str] | None" = None,
    ) -> Stage:
        assert (cmd is None) != (fn is None), "a stage needs exactly one of cmd= or fn="
        job_key = f"{self.prefix}/{name}"
        assert job_key not in _registered_job_keys, (
            f"job_key {job_key!r} is already registered (by stage {_registered_job_keys[job_key].name!r}). "
            f"To share a stage across experiments, reuse that Stage object (import it) -- don't "
            f"build a second one with a matching prefix/name; two independently-built Stage "
            f"objects landing on the same job_key by coincidence is almost always a bug, not "
            f"intentional sharing. See shared_preprocess.py for the intended pattern."
        )
        # Identity is run_experiment's job (env prefix / outdir_args). The launcher prefix
        # stays SEPARATE from the stage's own command (resolved here -- job_key is known) so
        # aio can format its "{}" templates with the minted outdir (lsf's -oo {}/log.txt);
        # baking it into one merged lambda hid the prefix from that machinery.
        prefix = list(self.cmd_prefix) if cmd_prefix is None else cmd_prefix(job_key)
        if fn is not None and not prefix:
            prefix = [sys.executable]
        s = Stage(
            name=name, job_key=job_key, command=cmd, function=fn, requires=req or [], outdir_args=outdir_args,
            prefix=prefix,
            _experiment_file=self.experiment_file, _experiment_argv=self.experiment_argv,
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
        if self.report is not None:
            assert self.report in self.stages, "e.report must be a Stage belonging to this Experiment"
            assert self.report.function is not None, "the report stage must use fn="
            self.report.requires = list(dict.fromkeys([
                *self.report.requires, *(s for s in self.stages if s is not self.report)
            ]))
        return run_experiment(self.stages, new=new, extend=extend, replace=replace)

    def _write_dashboard(self) -> "Path | None":
        out = Path(rundb.root()) / self.prefix / ".dashboard.json"
        if self.dashboard is None:
            out.unlink(missing_ok=True)  # removing e.dashboard removes its rebuildable metadata
            return None
        spec = self.dashboard.as_dict()
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(".json.tmp")
        import json
        tmp.write_text(json.dumps(spec, indent=2) + "\n")
        tmp.replace(out)
        return out

    def savedir(self, stage: "Stage | str") -> "str | None":
        """A stage's latest DONE outdir (by object or name), or None -- what a report fn keys
        on, so a partially-complete run still renders its finished parts (contrast
        Stage.savedir, which asserts when unresolved: right for commands, wrong for reports).
        Reads the ledger, so it also sees runs from before this pass."""
        s = stage if isinstance(stage, Stage) else next((t for t in self.stages if t.name == stage), None)
        assert s is not None, f"no stage named {stage!r} in experiment {self.prefix!r}"
        return rundb.latest_outdir(s.job_key, status="done")

    def main(self, argv: "list[str] | None" = None) -> "dict[str, str] | None":
        """The standard experiment-file entrypoint -- spearmint's own flags, parsed explicitly
        (never sniffed from a library call): ``--new/--extend/--replace STAGE`` (repeatable)
        force stages by name (see run_experiment for what each mode means), ``--submit``
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

        import os
        dispatch = os.environ.get("SPEARMINT_EXEC_STAGE")
        if dispatch:
            stage = next((s for s in self.stages if s.job_key == dispatch), None)
            assert stage is not None and stage.function is not None, f"no function stage {dispatch!r}"
            with rundb.run() as run:
                stage.function(run)
            return None

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
        p.add_argument("-d", "--dashboard", action="store_true",
                       help="publish the dashboard configuration and exit; run/submit nothing")
        a = p.parse_args(sys.argv[1:] if argv is None else argv)
        if a.dashboard:
            assert self.dashboard is not None, f"{self.prefix}: no dashboard configured"
            path = self._write_dashboard()
            print(f"dashboard published: {path}")
            return None
        self._write_dashboard()
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




def run_experiment(
    stages: "list[Stage]",
    new: "list[Stage] | None" = None,
    extend: "list[Stage] | None" = None,
    replace: "list[Stage] | None" = None,
) -> "dict[str, str]":
    """Lower the static plan onto the aio core and run it to completion. Every stage becomes
    one ``aio.Ctx.submit`` whose deps are its local requires -- ordering, skip-if-done, the
    force cascade (a re-run dep forces dependents to "new"), failure/abandon propagation
    (JobFailed through the awaits), MAX_PARALLEL, and the [run]/[skip]/[stale] prints all come
    from aio. This function contributes the up-front validation the static plan enables:
    duplicate force buckets, cycles (via _topo_order's assert), and externals -- a ``requires``
    target outside ``stages`` is never run in this pass, so it's resolved and asserted
    already-done BEFORE anything launches (schedule it too via closure() to build it instead).

    ``new``/``extend``/``replace`` force the named stages despite being done (see rundb._start
    for what each mode means for the outdir); their dependents re-run as "new" automatically.
    A stage whose LATEST attempt isn't done and that isn't forced launches as "new" -- a fresh
    dir every attempt, so a failed run's dir (and one day its log) survives as evidence, and a
    failure after an older success reruns instead of hiding behind it. Returns
    {job_key: done|skipped|failed|abandoned}.

    A retrospective report is just another function stage; Experiment.run adds every other
    stage as its dependencies when it has been assigned to ``e.report``."""
    local = set(stages)
    seeds_all = [s for group in (replace, extend, new) if group for s in group]
    assert len(seeds_all) == len(set(seeds_all)), (
        "a stage was passed to more than one of replace=/extend=/new="
    )
    seed_mode: "dict[Stage, str]" = {}
    for mode_name, group in (("replace", replace), ("extend", extend), ("new", new)):
        for s in group or []:
            seed_mode[s] = mode_name
    order = _topo_order(stages)  # deterministic submit order + the cycle assert

    # Externals never run in this pass, so their state is fixed: resolve every one's savedir
    # (confirming it's actually done) before anything launches.
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

    assert rundb._ANCHOR is not None  # Experiment() anchored when the stages were declared
    return asyncio.run(_run_plan(order, local, seed_mode))


async def _run_plan(order, local, seed_mode) -> "dict[str, str]":
    anchor = rundb._ANCHOR
    assert anchor is not None
    ctx = aio.Ctx(prefix="", cmd_prefix=[], repo=anchor.repo or anchor.root, root=anchor.root,
                  max_parallel=MAX_PARALLEL)

    jobs: "dict[Stage, aio.Job]" = {}

    def _stash_savedir(s: Stage, j: "aio.Job"):
        # Resolve Stage.savedir the moment its job finishes -- callbacks attach here,
        # synchronously at submit, so they run BEFORE any dependent's gather resumes and
        # evaluates a cmd lambda that reads upstream.savedir (call_soon is FIFO).
        def cb(_fut) -> None:
            if j._row is not None and not _fut.cancelled() and _fut.exception() is None:
                s._savedir = j._row.outdir
        return cb

    for s in order:
        cmd = s.command
        env = None
        if s.function is not None:
            cmd = lambda s=s: [s._experiment_file, *s._experiment_argv]
            env = {"SPEARMINT_EXEC_STAGE": s.job_key}
        jobs[s] = ctx.submit(
            s.name, cmd=cmd, deps=[jobs[d] for d in s.requires if d in local],
            cmd_prefix=s.prefix, key=s.job_key, force=seed_mode.get(s),
            outdir_args=s.outdir_args, env=env,
        )
        jobs[s]._task.add_done_callback(_stash_savedir(s, jobs[s]))
    tasks = [jobs[s]._task for s in order]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    status: "dict[str, str]" = {}
    keyed = [(s.job_key, jobs[s]) for s in order]
    for (job_key, job), res in zip(keyed, results):
        if isinstance(res, aio.JobFailed):
            st = "failed" if res.job_key == job_key else "abandoned"
            if st == "abandoned":
                print(f"[abandon] {job.name}: upstream failed/abandoned", flush=True)
        elif isinstance(res, BaseException):
            raise res  # ctrl-c / unexpected errors propagate, never swallowed into a status
        else:
            st = "done" if job.ran else "skipped"
        status[job_key] = st
    return status
