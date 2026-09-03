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
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from . import aio
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
        # Optional report renderer, re-run by the scheduler after every stage finalize, every
        # REPORT_TICK_SECONDS while anything is running, and once at DAG end. THE pattern is a
        # STANDALONE SCRIPT path ("my_report.py"): the driver shells out to it (under this
        # experiment's cmd_prefix, so the project env), and the same file runs by hand --
        # during a run, after it, or a week later with new report code; every render is a
        # fresh process, so editing it mid-run just works. Build it with spearmint.load +
        # spearmint.viz and write report.html into load.report_dir(<name>). The script is
        # ALSO a real stage ("<prefix>/report", depending on every other stage): when results
        # change it re-runs into a fresh, VERSIONED run dir -- old reports are never lost --
        # while tick/hand renders keep the live _reports/<name>/report.html current (what the
        # status table links). A plain function ``fn(savedir) -> html`` also works for
        # one-file demos (live view only, no versioning, fixed for the run's lifetime).
        # Either way a failing render prints and the DAG carries on.
        self.report: "Callable[[Callable], str] | str | None" = None

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
        report_cmd = report_force = None
        report_key = f"{self.prefix}/report"
        if isinstance(self.report, str):
            assert not any(s.name == "report" for s in self.stages), (
                f"experiment {self.prefix!r} has a stage named 'report' AND e.report set -- "
                f"the report stage claims that job_key; rename the stage"
            )
            report_cmd = [*self.cmd_prefix, self.report]
            # aio's skip rule is done + no-dep-ran-THIS-session: a dep rerun by an earlier or
            # concurrent driver leaves the report stale-but-skippable, so staleness forces a
            # fresh version here.
            report_force = "new" if rundb.stale_inputs(report_key) else None
        return run_experiment(self.stages, new=new, extend=extend, replace=replace,
                              on_update=self._render_report if self.report else None,
                              report_cmd=report_cmd, report_key=report_key,
                              report_force=report_force)

    def savedir(self, stage: "Stage | str") -> "str | None":
        """A stage's latest DONE outdir (by object or name), or None -- what a report fn keys
        on, so a partially-complete run still renders its finished parts (contrast
        Stage.savedir, which asserts when unresolved: right for commands, wrong for reports).
        Reads the ledger, so it also sees runs from before this pass."""
        s = stage if isinstance(stage, Stage) else next((t for t in self.stages if t.name == stage), None)
        assert s is not None, f"no stage named {stage!r} in experiment {self.prefix!r}"
        return rundb.latest_outdir(s.job_key, status="done")

    def _render_report(self) -> "Path | None":
        assert self.report is not None, \
            f"{self.prefix}: no report renderer registered (set e.report = ...)"
        if isinstance(self.report, str):
            # Fresh process per render: the script picks up its own edits, its stderr flows
            # through the driver's log, and a hang can't wedge the scheduler (generous cap --
            # a report is file reads + HTML). check=True routes failure into run_experiment's
            # [report] guard.
            subprocess.run([*self.cmd_prefix, self.report], check=True, timeout=600)
            return None
        out = Path(rundb.root()) / rundb.REPORTS_DIR / self.prefix
        out.mkdir(parents=True, exist_ok=True)
        path = out / "report.html"
        path.write_text(self.report(self.savedir))
        return path

    def main(self, argv: "list[str] | None" = None) -> "dict[str, str] | None":
        """The standard experiment-file entrypoint -- spearmint's own flags, parsed explicitly
        (never sniffed from a library call): ``--new/--extend/--replace STAGE`` (repeatable)
        force stages by name (see run_experiment for what each mode means), ``--submit``
        submits this same invocation as the long-lived LSF driver job instead of running
        in-process -- the driver re-runs sys.argv minus --submit, so the tier and force flags
        ride along (and the DAG was already built by the time we submit, so definition errors
        fail here on your terminal, not minutes later in a driver log) -- and ``-r``/``--report``
        re-renders report.html from the CURRENT ledger state and exits, reading only: no stage
        is ever submitted, even a never-run one (bare invocation defaults every not-done stage
        to mode "replace" and launches it for real -- ``--report`` is the safe alternative when
        you just want to see the current report). Returns run()'s status dict, or None when it
        only submitted or only rendered the report.

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
        p.add_argument("-r", "--report", action="store_true",
                       help="render the report from the current ledger state and exit -- "
                            "reads only, submits/runs nothing (takes priority over --submit; "
                            "--new/--extend/--replace are ignored)")
        a = p.parse_args(sys.argv[1:] if argv is None else argv)
        if a.report:
            path = self._render_report()
            if path:
                print(f"report rendered: {path}")
            return None
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
    on_update: "Callable[[], object] | None" = None,
    report_cmd: "list[str] | None" = None,
    report_key: str = "",
    report_force: "str | None" = None,
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
    A stage that isn't done and isn't forced defaults to "replace" (only reachable with no
    done row, so it can only clear a failed/wip attempt -- keeps the debug loop from littering
    one orphan dir per attempt). Returns {job_key: done|skipped|failed|abandoned}.

    ``report_cmd``/``report_key``/``report_force`` (set by Experiment.run when e.report is a
    script): the report is submitted as a REAL stage depending on every other job -- a rerun
    dep cascades it to mode "new", so each version lands in its own run dir (old reports are
    never lost), its ledger inputs make staleness exact, and it skips when nothing changed."""
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
    return asyncio.run(_run_plan(order, local, seed_mode, on_update,
                                 report_cmd, report_key, report_force))


async def _run_plan(order, local, seed_mode, on_update,
                    report_cmd=None, report_key="", report_force=None) -> "dict[str, str]":
    anchor = rundb._ANCHOR
    assert anchor is not None
    ctx = aio.Ctx(prefix="", cmd_prefix=[], repo=anchor.repo or anchor.root, root=anchor.root,
                  max_parallel=MAX_PARALLEL)

    def update() -> None:
        """Presentation must never endanger the run: a raising report prints and we carry on."""
        if on_update is None:
            return
        try:
            on_update()
        except Exception as e:  # noqa: BLE001 -- see docstring
            print(f"[report] render failed: {e!r}", flush=True)

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
        jobs[s] = ctx.submit(
            s.name, cmd=s.command, deps=[jobs[d] for d in s.requires if d in local],
            key=s.job_key, force=seed_mode.get(s), outdir_args=s.outdir_args,
        )
        jobs[s]._task.add_done_callback(_stash_savedir(s, jobs[s]))
    ticker = None
    if on_update is not None:
        for j in jobs.values():  # re-render the report the moment anything finalizes
            j._task.add_done_callback(lambda _fut: update())

        async def _tick() -> None:
            while True:  # module var read each lap, so demos can retune it pre-run
                await asyncio.sleep(REPORT_TICK_SECONDS)
                update()

        ticker = asyncio.create_task(_tick())
    update()  # initial render: the report exists (all-waiting) from the first second

    # The report as a REAL stage, depending on every job: a rerun dep cascades it to "new"
    # (fresh run dir per version -- old reports survive), inputs record the exact dep run_ids,
    # skip-if-fresh applies. Submitted after the finalize-callback wiring above on purpose:
    # its own completion needn't trigger another live render (the final update() below runs
    # regardless). Its subprocess gets SPEARMINT_RUN_OUTDIR, steering load.report_dir into
    # the versioned dir; tick renders (no identity) keep writing the live _reports file.
    rjob = None
    if report_cmd is not None:
        rjob = ctx.submit("report", cmd=report_cmd, deps=list(jobs.values()),
                          key=report_key, force=report_force)

    tasks = [jobs[s]._task for s in order] + ([rjob._task] if rjob is not None else [])
    results = await asyncio.gather(*tasks, return_exceptions=True)
    if ticker is not None:
        ticker.cancel()
    update()  # final render: every stage finalized

    status: "dict[str, str]" = {}
    keyed = [(s.job_key, jobs[s]) for s in order] + ([(report_key, rjob)] if rjob is not None else [])
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
