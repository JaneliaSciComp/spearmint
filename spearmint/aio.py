"""Async execution core -- PROTOTYPE for the bakeoff against the sidecar DAG bolt-on (see
sidecar.md; neither is the winner yet, dagrunner is untouched and both coexist).

An experiment file is an async program: ``ctx.submit(name, cmd)`` returns an awaitable Job,
``await job`` is a dependency edge, and lifecycle coupling (validators, sidecars, retry loops)
is ordinary Python instead of scheduler vocabulary. The thesis under test: everything the DAG
scheduler uniquely provides lives in the LEDGER, not the DAG structure -- submit is
ledger-memoized (a done row and no force -> an already-completed Job, printed as [skip]), so
forcing and crash-resume work by REPLAYING the program against the ledger, exactly like
re-running an experiment file does today.

    async def main(ctx):
        train = ctx.submit("train", ["scripts/train.py"], cmd_prefix=lsf.gpu())
        val = ctx.submit("val", ["scripts/watch_val.py", "--watch", train.outdir])
        try:
            await train
        finally:
            val.cancel()                       # "stop when train stops" -- just code
        await ctx.submit("plot", ["scripts/plot.py"], deps=(val,))

    if __name__ == "__main__":
        aio.main(main, prefix="e07", cmd_prefix=["uv", "run", "python"])

Same ledger contract as dagrunner: this driver is the only db writer; children get the
``env SPEARMINT_*`` identity prefix and never touch the db. Known prototype limitation vs the
DAG: there is no static plan -- force flags resolve against names as they're submitted (a
pattern matching nothing warns at the end instead of failing up front), and the status table
only shows jobs the program has reached."""

import asyncio
import os
import re
import subprocess
import sys
from fnmatch import fnmatch
from pathlib import Path

from . import rundb

STOP_GRACE_SECONDS = 15  # signal -> escalate gap for Job.cancel()
WAIT_POLL_SECONDS = 5  # ledger re-check cadence while waiting on another process's live run

_JOBID_RE = re.compile(r"Job <(\d+)> is submitted")
_STARTED_RE = re.compile(r"<<Starting on (\S+?)>>")


class JobFailed(RuntimeError):
    """Awaiting a Job whose process exited nonzero (or whose dependency did) raises this --
    failure propagation is ordinary exception propagation. ``job_key`` names the ORIGIN of
    the failure, so a catcher (or the DAG layer) can tell "I failed" from "my dep failed"."""

    def __init__(self, job_key: str, detail: str):
        super().__init__(f"{job_key}: {detail}")
        self.job_key = job_key


def _signal(proc, jobid: "str | None") -> None:
    """Deliberate-stop signal: bkill the LSF job when we know its id (killing only the
    bsub -K client would orphan the job; bkill escalates INT->TERM->KILL itself and the -K
    client then exits on its own), else SIGTERM the local process."""
    if jobid:
        subprocess.run(["bkill", jobid], capture_output=True)
    else:
        proc.terminate()


async def _wait_not_running(name: str, job_key: str) -> None:
    """Block this job while ANOTHER process holds a live wip row for job_key -- a second
    driver waits for the first, per stage, instead of dying on rundb's guard. Each lap
    reconciles first, so a crash-killed holder (SIGKILL, node death) flips to failed and
    frees us within one poll; the guard in rundb._start stays as the TOCTOU backstop."""
    waited = False
    while True:
        rundb.reconcile_wip(job_key)
        live = rundb.latest_outdir(job_key, status="wip")
        if live is None:
            return
        if not waited:
            print(f"[wait] {name}: {job_key} live in another process ({live}) -- waiting", flush=True)
            waited = True
        await asyncio.sleep(WAIT_POLL_SECONDS)


class Job:
    """An awaitable handle on one submitted job. ``await job`` -> savedir (str), raising
    JobFailed on nonzero exit; ``job.outdir`` is the LIVE run dir (known once the row exists
    -- immediately for dep-free submits, so a validator can be handed a trainer's live dir at
    submit time); ``job.cancel()`` is a DELIBERATE stop: signal, escalate after
    STOP_GRACE_SECONDS, row closed done (its data is valid as far as it goes) and the await
    still resolves normally -- matching the sidecar-design semantics, but as code."""

    def __init__(self, ctx: "Ctx", name: str, job_key: str):
        self.name = name
        self.job_key = job_key
        self._ctx = ctx
        self.ran = False           # actually launched this session (skips stay False)
        self.skipped = False       # memoized against the ledger; for a dep-free job this is
        #                            already knowable at submit -- how a lifecycle-coupled
        #                            companion decides its own freshness (see toy_aio_sidecar)
        self._mode = "new"         # resolved at submit (dep-free) or launch (dep-carrying)
        # The command spec, resolved lazily: _cmd may be a callable (evaluated only after deps
        # are done, so it can reference their savedirs -- the Stage-lambda pattern).
        self._prefix: "list[str]" = []
        self._cmd = None
        self._outdir_args: "list[str] | None" = None
        self._env: "dict[str, str]" = {}
        self._base_cmd: "list[str]" = []
        self._argv: "list[str]" = []  # what the ledger records
        self._row: "rundb.Run | None" = None
        self._proc = None
        self._jobid: "str | None" = None
        self._stopping = False
        self._closed = False       # row closed exactly once
        self._task: "asyncio.Task | None" = None  # set by Ctx.submit

    def __await__(self):
        assert self._task is not None
        return self._task.__await__()

    @property
    def outdir(self) -> str:
        """The live run dir. For a memoized (skipped) job this is its done outdir; for a
        launched job it's known as soon as the row exists -- which is at submit time unless
        the job has pending deps (then reference it only after the deps resolved)."""
        assert self._row is not None, (
            f"job {self.name!r} has no outdir yet -- its row is inserted after its deps "
            f"resolve; submit it dep-free or take the outdir after awaiting"
        )
        return self._row.outdir

    @property
    def run_id(self) -> "int | None":
        return self._row.run_id if self._row is not None else None

    def cancel(self) -> None:
        """Deliberately stop this job. Launched -> signal + escalation, row closes done, the
        await resolves normally. Not launched yet (deps still pending) -> it never starts and
        the await raises JobFailed (there is nothing to read). Already finished -> no-op."""
        self._stopping = True
        if self._proc is not None:
            asyncio.get_running_loop().create_task(self._escalate())

    async def _escalate(self) -> None:
        _signal(self._proc, self._jobid)
        try:
            await asyncio.wait_for(self._proc.wait(), STOP_GRACE_SECONDS)
        except (asyncio.TimeoutError, TimeoutError):
            if self._jobid:
                subprocess.run(["bkill", "-s", "KILL", self._jobid], capture_output=True)
                self._proc.terminate()  # and free the -K client regardless
            else:
                self._proc.kill()

    def _close(self, ok: bool) -> None:
        if self._row is not None and not self._closed:
            self._closed = True
            rundb.finish_managed(self._row.run_id, ok)

    def _resolve_spec(self) -> None:
        """Evaluate the (possibly callable) command now -- deps are done, their savedirs
        referenceable -- and fix the ledger argv (outdir_args recorded verbatim; the row's
        outdir column completes them)."""
        c = self._cmd() if callable(self._cmd) else self._cmd
        self._base_cmd = [*self._prefix, *list(c)]
        self._argv = self._base_cmd if self._outdir_args is None else [*self._base_cmd, *self._outdir_args]

    def _final_cmd(self, deps: "tuple[Job, ...]") -> "list[str]":
        row = self._row
        assert row is not None
        inputs_env = (
            [f"SPEARMINT_INPUTS={os.pathsep.join(d._row.outdir for d in deps)}"] if deps else []
        )
        # Launcher-prefix elements holding a "{}" are formatted with the minted outdir (the
        # row exists by now) -- how lsf's "-oo {}/log.txt" lands each attempt's log in its own
        # run dir. Only the prefix: worker argv may contain literal braces (hydra overrides).
        npre = len(self._prefix)
        base = [a.format(row.outdir) if i < npre and "{}" in a else a
                for i, a in enumerate(self._base_cmd)]
        return [
            "env",
            f"SPEARMINT_JOB_KEY={self.job_key}",
            f"SPEARMINT_MODE={self._mode}",
            f"SPEARMINT_RUN_ROW={row.run_id}",
            f"SPEARMINT_RUN_OUTDIR={row.outdir}",
            *(f"{k}={v}" for k, v in self._env.items()),
            *inputs_env,
            *base,
            *(a.format(row.outdir) for a in self._outdir_args or []),
        ]

    async def _run(self, deps: "tuple[Job, ...]", forced: "str | None") -> str:
        if deps:
            await asyncio.gather(*deps)  # a failed dep raises JobFailed here -> we cascade
        if self._row is None:  # not minted in submit(): dep-carrying, or dep-free deferred
            # behind another process's live run. The skip/mode decision happens HERE, not at
            # submit -- whether a dep actually re-ran (.ran) is only knowable once the deps
            # resolved, and a live job_key must close before its ledger state means anything.
            await _wait_not_running(self.name, self.job_key)
            dep_ran = any(d.ran for d in deps)
            if forced is None and not dep_ran and rundb.is_done(self.job_key):
                self._row = _done_row(self.job_key)
                self.skipped = True
                _skip_print(self.name, self.job_key)
                return self._row.outdir
            # Unforced launches default to "new": a failed last attempt keeps its dir (and
            # one day its log) as evidence -- clearing it was worse than the orphan dirs.
            self._mode = forced or "new"
            if self._stopping:
                raise JobFailed(self.job_key, "cancelled before start")
            self._resolve_spec()
            try:
                self._ctx._insert_row(self, deps)
            except AssertionError as e:  # lost the cross-process TOCTOU race: fail THIS stage
                raise JobFailed(self.job_key, f"launch refused: {e}")
        cmd = self._final_cmd(deps)
        async with self._ctx._sem:  # cap concurrent child processes (Ctx max_parallel)
            print(f"[run] {self.name}: {' '.join(cmd)}", flush=True)
            self.ran = True
            # A LOCAL stage's pipe carries the worker's own stdout/stderr -- worker output, so
            # tee it to the attempt's run dir like any other product. An LSF stage's pipe is
            # only bsub -K chatter (the worker's stdout goes to -oo log.txt on the compute
            # node), so no tee -- same filename either way.
            logf = None if "bsub" in cmd else (Path(self._row.outdir) / "log.txt").open("w")
            try:
                self._proc = await asyncio.create_subprocess_exec(
                    *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
                )
                assert self._proc.stdout is not None
                async for raw in self._proc.stdout:
                    line = raw.decode(errors="replace")
                    print(line, end="", flush=True)
                    if logf is not None:
                        logf.write(line)
                        logf.flush()  # tail -f-able while the stage runs
                    if (m := _JOBID_RE.match(line)) is not None:
                        self._jobid = m.group(1)
                        rundb.set_lsf_jobid(self._row.run_id, self._jobid)
                    if (m := _STARTED_RE.match(line)) is not None:
                        rundb.set_lsf_state(self._row.run_id, f"RUN {m.group(1)}")
                rc = await self._proc.wait()
            except asyncio.CancelledError:  # loop teardown (e.g. ctrl-c), NOT job.cancel()
                if self._proc is not None and self._proc.returncode is None:
                    _signal(self._proc, self._jobid)
                self._close(ok=False)
                raise
            finally:
                if logf is not None:
                    logf.close()
        if self._stopping:  # deliberate stop: whatever the exit code, the data stands
            self._close(ok=True)
            print(f"  [{self.name}] stopped", flush=True)
            return self._row.outdir
        self._close(ok=rc == 0)
        print(f"  [{self.name}] {'ok' if rc == 0 else 'FAILED'}", flush=True)
        if rc != 0:
            raise JobFailed(self.job_key, f"exited {rc}")
        return self._row.outdir


class Ctx:
    """The submission context: anchors the ledger like dagrunner.Experiment (repo/root from
    the experiment file's own location), carries the experiment-wide cmd_prefix and the CLI
    force patterns, and memoizes submits against the ledger."""

    def __init__(self, prefix: str, cmd_prefix: "list[str] | None" = None,
                 repo: "str | None" = None, root: "str | None" = None,
                 forced: "dict[str, list[str]] | None" = None,
                 caller_file: "str | None" = None, max_parallel: int = 32):
        if repo is None:
            caller = caller_file or sys._getframe(1).f_globals.get("__file__")
            assert caller, "can't locate the experiment file -- pass repo= explicitly"
            repo = rundb._git_root(str(Path(caller).resolve().parent))
        rundb.anchor(root or f"{repo}/output_rundb", repo=repo)
        if not Path(rundb._db_path()).is_file():
            rundb.initialize()
        self.prefix = prefix
        self.cmd_prefix = list(cmd_prefix or [])
        self.forced = forced or {"new": [], "extend": [], "replace": []}
        self._matched: "set[str]" = set()
        self._jobs: "dict[str, Job]" = {}
        self._sem = asyncio.Semaphore(max_parallel)

    def _forced_mode(self, name: str) -> "str | None":
        for mode in ("replace", "extend", "new"):
            for pat in self.forced[mode]:
                if fnmatch(name, pat):
                    self._matched.add(pat)
                    return mode
        return None

    def _insert_row(self, job: Job, deps: "tuple[Job, ...]") -> None:
        input_ids = [d.run_id for d in deps if d.run_id is not None]
        job._row = rundb.start_managed(job.job_key, job._mode, argv=job._argv, inputs=input_ids)

    def submit(self, name: str, cmd, deps: "tuple[Job, ...] | list[Job]" = (),
               cmd_prefix=None, outdir_args: "list[str] | None" = None,
               key: "str | None" = None, force: "str | None" = None,
               env: "dict[str, str] | None" = None) -> Job:
        """Submit a job (ledger-memoized). ``cmd`` is the worker's own argv -- a list, or a
        CALLABLE returning one, evaluated only after ``deps`` are done (so it may reference
        their savedirs: the Stage-lambda pattern). ``deps`` are awaited before launch,
        recorded as input provenance, and handed to the child as $SPEARMINT_INPUTS;
        ``cmd_prefix`` overrides the ctx-wide one (a list, or a callable receiving the job_key
        -- lsf.gpu()/cpu() work unchanged). ``key``/``force`` are the AOT layer's hooks: a full
        job_key override, and an explicit mode bypassing the CLI pattern matching. Skip rule
        mirrors the DAG scheduler: LATEST attempt done + not forced + no dep re-ran this
        session -> skip (a failed last attempt reruns even over an older success); every
        unforced launch defaults to mode "new" -- a fresh dir, the failed attempt kept as
        evidence -- and a force/cascade uses its named mode."""
        deps = tuple(deps)
        job_key = key or f"{self.prefix}/{name}"
        assert job_key not in self._jobs, f"job {job_key!r} submitted twice"
        job = Job(self, name, job_key)
        self._jobs[job_key] = job

        forced = force if force is not None else self._forced_mode(name)
        job._prefix = self.cmd_prefix if cmd_prefix is None else (
            cmd_prefix(job_key) if callable(cmd_prefix) else list(cmd_prefix)
        )
        job._cmd = cmd
        job._outdir_args = outdir_args
        job._env = dict(env or {})
        if not deps:
            # Dep-free jobs decide NOW when they can: a done row skips immediately (even if a
            # NEWER attempt is live under another driver -- a completed result exists), and a
            # launching job with no wip row is minted synchronously so job.outdir is
            # immediately usable (validators). Only a would-launch job behind a visible wip
            # row -- live under another driver, or stale from a crash -- defers into the task
            # (Job._run), which waits for the row to close (reconciling each lap, so a stale
            # one frees up in one lap) and then re-decides; such a job's .outdir is not
            # available at submit time.
            if forced is None and rundb.is_done(job_key):
                job._row = _done_row(job_key)
                job.skipped = True
                _skip_print(name, job_key)
                job._task = asyncio.get_running_loop().create_task(_done(job._row.outdir))
                return job
            if rundb.latest_outdir(job_key, status="wip") is None:
                job._mode = forced or "new"  # fresh dir; a failed attempt's dir stays as evidence
                job._resolve_spec()
                self._insert_row(job, deps)
        job._task = asyncio.get_running_loop().create_task(job._run(deps, forced))
        return job

    def warn_unmatched(self) -> None:
        for mode, pats in self.forced.items():
            for pat in pats:
                if pat not in self._matched:
                    print(f"[force] --{mode} {pat!r} matched no submitted job", flush=True)


async def _done(savedir: str) -> str:
    return savedir


def _skip_print(name: str, job_key: str) -> None:
    """[skip] plus a ledger-based [stale] warning: recorded-input provenance says which deps
    have newer results than this job's last success consumed (rundb.stale_inputs -- works for
    any skip, no DAG needed)."""
    stale = rundb.stale_inputs(job_key)
    if stale:
        print(f"[stale] {name}: {stale} have newer results than {job_key}'s last success "
              f"was built from -- skipping anyway (--new/--extend/--replace to force)", flush=True)
    print(f"[skip] {name}: already done ({job_key})", flush=True)


def _done_row(job_key: str) -> rundb.Run:
    """A Run view of a job_key's latest done row, for skipped jobs."""
    run_id = rundb.latest_run_id(job_key, status="done")
    outdir = rundb.latest_outdir(job_key, status="done")
    assert run_id is not None and outdir is not None
    return rundb.Run(run_id=run_id, outdir=outdir, job_key=job_key)


def main(async_fn, prefix: str, cmd_prefix: "list[str] | None" = None) -> None:
    """The experiment-file entrypoint: parses spearmint's standard flags
    (--new/--extend/--replace NAME..., globs resolved against names AS SUBMITTED;
    --submit replays this invocation as the LSF driver job), builds the Ctx, and
    ``asyncio.run``s your ``async def main(ctx)``."""
    import argparse

    p = argparse.ArgumentParser(prog=f"{prefix} (spearmint aio flags)")
    for mode in ("new", "extend", "replace"):
        p.add_argument(f"--{mode}", action="extend", nargs="+", default=[], metavar="JOB")
    p.add_argument("--submit", action="store_true")
    a = p.parse_args()
    if a.submit:
        from . import lsf

        lsf.submit_driver(sys.argv[0], *(x for x in sys.argv[1:] if x != "--submit"))
        return
    caller = sys._getframe(1).f_globals.get("__file__")

    async def runner():
        ctx = Ctx(prefix, cmd_prefix, caller_file=caller,
                  forced={"new": a.new, "extend": a.extend, "replace": a.replace})
        try:
            await async_fn(ctx)
        finally:
            ctx.warn_unmatched()

    asyncio.run(runner())
