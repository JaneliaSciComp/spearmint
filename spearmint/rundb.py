"""Run-tracking helpers: every run gets a fresh, uniquely-named output directory and a row in
a local sqlite db (ROOT/rundb.db) recording sys.argv, the git commit, the git diff, and
-- for dagrunner-launched runs -- the exact input run_ids consumed: full reproducibility with
almost no caller-side state. Everything lives under ROOT ($SPEARMINT_ROOT if set, else
<git repo root>/output_rundb -- see the constant below), deliberately separate from output/
where real (orchestration.py-era) experiment results live -- nothing rundb writes or deletes
can touch that tree. `run()` additionally tags a row with a `job_key` (e.g.
"e00/short/pretrain_mae") and tracks wip/done/failed status, so a DAG-shaped caller
(dagrunner.py) can ask "is this done" / "where did it last write" as a query instead of
assuming a fixed filesystem path or trusting file sentinels.

`job_key` and the new/extend/replace mode are both optional -- left unset, they're parsed out of
this process's own sys.argv, so a worker script needs none of its own --job-key/--extend/--replace
argparse plumbing (see script.py).

WHO writes the db depends on how a run was launched. A bare run (no dagrunner) writes its own
rows. A dagrunner-launched stage does NOT: sqlite's fcntl locking is unreliable when several
compute nodes hit one db on a shared filesystem ("disk I/O error" under concurrency), so the
DRIVER -- one process on one node, writes additionally serialized by _DB_LOCK -- inserts the
wip row before launching (start_managed) and marks done/failed from the stage's exit code
(finish_managed), threading the row identity to the child via --run-row/--run-outdir argv, at
which point the child's rundb.run() skips all db access (see run()).
"""

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path

from .config import CONFIG, _repo_root  # _repo_root re-exported: remote.push() calls rundb._repo_root

# Serializes THIS process's db writes across threads (dagrunner's pool threads + main loop).
# Cross-process/cross-node safety comes from the managed-run design above, not from locking.
_DB_LOCK = threading.Lock()

# Everything rundb ever writes or deletes lives under ROOT -- never output/. ROOT is resolved in
# config._load ($SPEARMINT_ROOT / spearmint.toml / <repo root>/output_rundb); DB_PATH derives from
# it once, at import -- control ROOT via config (env/toml), not by assigning rundb.ROOT after import
# (DB_PATH would keep pointing at the old ledger).
ROOT = CONFIG.root
DB_PATH = f"{ROOT}/rundb.db"


@dataclass
class Run:
    run_id: int
    outdir: str
    job_key: str


def _connect() -> sqlite3.Connection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS git_diffs (
            hash TEXT PRIMARY KEY,
            diff TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_key TEXT,
            argv0 TEXT NOT NULL,
            argv_rest TEXT NOT NULL,
            outdir TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'wip' CHECK (status IN ('wip', 'done', 'failed')),
            pid INTEGER NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            commit_id TEXT NOT NULL,
            diff_hash TEXT NOT NULL REFERENCES git_diffs (hash),
            inputs TEXT,
            lsf_jobid TEXT,
            lsf_state TEXT
        )
    """)
    # Migrations for older dbs (cluster ledgers are long-lived; can't just delete them like
    # local test dbs). CREATE TABLE IF NOT EXISTS won't retrofit an existing table.
    columns = [row[1] for row in conn.execute("PRAGMA table_info(runs)")]
    for missing in {"lsf_jobid", "lsf_state"} - set(columns):
        conn.execute(f"ALTER TABLE runs ADD COLUMN {missing} TEXT")
    return conn


def _git(*args: str) -> str:
    result = subprocess.run(["git", *args], capture_output=True, text=True)
    assert result.returncode == 0, f"git {' '.join(args)} failed: {result.stderr}"
    return result.stdout.strip()


def _provenance() -> "tuple[str, str]":
    """(commit_id, diff text) for the code this run is executing. Normally read from git in the
    cwd. But a run launched via spearmint.launch arrived by rsync -- the cluster tree is not this
    machine's git checkout, and its git HEAD is meaningless -- so the LAPTOP's values are threaded
    in via env instead: SPEARMINT_COMMIT (the base @- commit, which launch pushed to origin, so
    it's durably recoverable) and SPEARMINT_DIFF_FILE (path to the working-copy diff text captured
    on the laptop). See launch.py. Only the driver process needs these; it inserts the managed
    rows (start_managed), so the env is set on the driver, not on each stage's child."""
    commit = os.environ.get("SPEARMINT_COMMIT")
    if commit:
        diff_file = os.environ.get("SPEARMINT_DIFF_FILE")  # may be ~-relative (cluster home)
        path = Path(os.path.expanduser(diff_file)) if diff_file else None
        diff = path.read_text() if path and path.exists() else ""
        return commit, diff
    return _git("rev-parse", "HEAD"), _git("diff", "HEAD")


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d-%H-%M-%S")


def _abs(outdir: str) -> str:
    """Stored outdirs are ROOT-relative (so a db pulled from the cluster resolves against the
    local ROOT with no path mapping); public APIs return/accept absolute paths. Tolerates a
    legacy absolute row by passing it through untouched."""
    return outdir if os.path.isabs(outdir) else f"{ROOT}/{outdir}"


def _rel(outdir: str) -> str:
    """Inverse of _abs for querying: strip the local ROOT prefix if present, so callers can
    hand back the absolute paths our public API gave them."""
    prefix = f"{ROOT}/"
    return outdir[len(prefix):] if outdir.startswith(prefix) else outdir


def _job_key_from_argv() -> str:
    """Default job_key when the caller doesn't pass one explicitly: whatever follows --job-key
    in this process's own sys.argv (how dagrunner.py's stages tag their subprocess -- see
    Experiment.Stage), or else just the script's own name (Path(sys.argv[0]).stem), so a worker
    run directly with no dagrunner involved still gets a reasonable, queryable identity instead
    of none at all. Deliberately ignores the rest of argv -- baking other args in (e.g. a
    resolved --upstream path) would make the SAME script's identity drift between invocations
    whose args happen to differ, so extend/replace/gc/is_done could never relate them even when
    you consider them the same job; if you want per-argument distinctness for repeated direct
    runs, pass an explicit --job-key yourself, same as dagrunner does for its own stages. Lets
    every worker script skip its own --job-key argparse plumbing entirely."""
    argv = sys.argv[1:]
    if "--job-key" in argv:
        return argv[argv.index("--job-key") + 1]
    return Path(sys.argv[0]).stem


def _mode_from_argv() -> str:
    """Default mode when the caller doesn't pass one explicitly: whichever of --replace/--extend
    is a bare flag in sys.argv (how dagrunner.py's forced stages signal it -- see
    Experiment.Stage's full_cmd), else "new". Lets every worker script skip its own
    --replace/--extend/--new argparse plumbing entirely."""
    if "--replace" in sys.argv:
        return "replace"
    if "--extend" in sys.argv:
        return "extend"
    return "new"


def _managed_from_argv() -> "tuple[int, str] | None":
    """(run_id, absolute outdir) when this process is a dagrunner-MANAGED stage -- the driver
    already inserted our row and appended ``--run-row <id> --run-outdir <path>`` to our argv --
    else None. In the managed case run() must do no db work at all: several compute nodes
    hitting one sqlite db over the shared filesystem is exactly what broke (see module
    docstring); the driver owns our row's whole lifecycle."""
    argv = sys.argv[1:]
    if "--run-row" not in argv:
        return None
    run_id = int(argv[argv.index("--run-row") + 1])
    outdir = argv[argv.index("--run-outdir") + 1]
    return run_id, outdir


def _lsf_alive(jobid: str) -> bool:
    """Liveness of an LSF job by id, via ``bjobs -o stat -noheader <id>``. Alive iff the state
    is pending/running/suspended-ish. An UNKNOWN id counts as dead: bjobs forgets finished jobs
    after its clean period (~1h), so a still-'wip' row whose jobid bjobs can't find means the
    job ended long ago without _finish running (hard kill / node death) -- exactly the case
    reconcile_wip exists to correct. A false-dead flip self-heals: _finish updates the row
    unconditionally when the process actually exits cleanly."""
    try:
        result = subprocess.run(
            ["bjobs", "-o", "stat", "-noheader", jobid], capture_output=True, text=True
        )
    except FileNotFoundError:
        raise AssertionError(
            "bjobs not on PATH -- reconcile LSF-launched rows on the cluster, not locally"
        ) from None
    state = result.stdout.strip().split()[0] if result.stdout.strip() else ""
    return state in ("PEND", "RUN", "PSUSP", "USUSP", "SSUSP", "PROV", "WAIT")


def _pid_alive(pid: int) -> bool:
    """Best-effort local liveness check (POSIX) -- os.kill(pid, 0) sends no actual signal, it
    just checks whether this process could signal `pid` at all. Only meaningful for a pid
    recorded by a process on THIS machine -- spearmint only ever runs local subprocesses today, so
    that's the only case that comes up; a future remote/cluster-submitted stage would need its
    own platform-specific check (e.g. `bjobs`) instead of this. Known limitation: pids can be
    reused by the OS after a process exits, so a stale pid could in rare cases collide with an
    unrelated live process and be (wrongly) reported alive -- acceptable for a local dev-loop
    tool, not something this checks further."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, just not ours to signal
    return True


def reconcile_wip(job_key: str) -> None:
    """For every row still marked 'wip' under job_key, check whether the process that started it
    is actually still alive -- via bjobs when the row recorded an LSF job id (the pid lives on
    some compute node and means nothing here), else via its pid (same-machine assumption). If
    dead, it crashed hard enough (e.g. SIGKILL, node death) to bypass rundb.run()'s own
    except/finally cleanup, so mark it 'failed' now instead of leaving it 'wip' forever. A row
    whose process IS still alive is left untouched -- it's still genuinely running, not stale;
    "wip" is assumed to mean what it says unless disproven. Called automatically by _start() and
    wipe() before deciding what to reuse/clear/delete, and by gc() before deciding what's safe
    to remove -- never something a caller needs to remember to call itself, but also callable
    directly."""
    conn = _connect()
    rows = conn.execute(
        "SELECT run_id, pid, lsf_jobid FROM runs WHERE job_key = ? AND status = 'wip'", (job_key,)
    ).fetchall()
    conn.close()
    for run_id, pid, lsf_jobid in rows:
        alive = _lsf_alive(lsf_jobid) if lsf_jobid else _pid_alive(pid)
        if not alive:
            _finish(run_id, "failed")


def _assert_not_running(job_key: str) -> None:
    """Raise if job_key has a genuinely still-alive 'wip' row (call reconcile_wip(job_key) first
    so only a truly-live one remains) -- refuses to start/replace/wipe while another process is
    actively working on the same job_key, rather than silently racing it."""
    still_running = latest_outdir(job_key, status="wip")
    assert still_running is None, (
        f"job_key {job_key!r} already has a run in progress (outdir {still_running!r}) -- "
        f"refusing to proceed while it's live"
    )


def _start(
    job_key: "str | None",
    mode: "str | None",
    argv: "list[str] | None" = None,
    inputs: "list[int] | None" = None,
) -> Run:
    """Insert a `wip` row (+ its diff into git_diffs, deduped by hash) and return a Run.

    ``argv`` overrides what's recorded as the row's command (start_managed passes the STAGE
    command; by default this process's own sys.argv is recorded). ``inputs`` -- the exact dep
    run_ids this run consumes -- is stamped at insert when the caller (the driver) already
    knows it.

    ``mode`` decides whether this run is fresh at all -- "new" (default) always mints a new
    directory; "extend" resumes into job_key's most recent outdir (looked up here, not passed
    in -- the caller only needs to say *what* it wants, not resolve a path itself); "replace"
    reuses that SAME most-recent outdir too, but clears its old contents first, so the script
    starts from empty in-place rather than resuming -- older outdirs from earlier "new" attempts
    are left alone; this only clears the one attempt immediately being superseded, not the whole
    history (for that, see ``wipe()``). If job_key has no prior outdir at all, "replace" just
    behaves like "new" (nothing to clear). ``job_key``/``mode`` fall back to
    ``_job_key_from_argv()``/``_mode_from_argv()`` when not given explicitly.

    Before any of that, ``reconcile_wip(job_key)`` runs (see its docstring) so a "wip" row left
    behind by a hard-killed process (SIGKILL etc.) gets corrected to "failed" instead of being
    assumed dead outright, then ``_assert_not_running(job_key)`` refuses to proceed at all if a
    "wip" row is confirmed genuinely still alive -- "extend"/"replace" reusing/clearing a
    directory a live process is still writing into, or two concurrent attempts racing each
    other under "new", are both worse than a loud, early failure here.

    Directory naming for a fresh mint: {ROOT}/{job_key}/run{run_id:05d} -- job_key's "/"s
    become real path nesting (experiment prefix -> stage name -> attempt mirrors the model, and
    "/" is reserved in names so the mapping is unambiguous; flattening to "_" wasn't, since
    prefixes and stage names contain "_" themselves). The worker script's name isn't a path
    segment -- it's already recorded in the row's argv0, and stages of one experiment that use
    different worker scripts should still land under one experiment directory. A bare
    no---job-key run gets {ROOT}/{script stem}/run{run_id:05d} for free, since the script stem
    is exactly its fallback job_key. run_id is the already-unique-per-row global counter rather
    than a separately tracked per-job_key one, since it's sitting right here with nothing extra
    to compute. The db row stores the ROOT-RELATIVE form ({job_key}/run{run_id:05d}, see _abs/
    _rel) so a ledger pulled from another machine resolves against the local ROOT unmapped;
    everything public (Run.outdir, latest_outdir, ...) speaks absolute paths.

    The returned outdir always exists (mkdir'd here) -- callers never need their own
    ``Path(r.outdir).mkdir(...)`` boilerplate, and can never forget it.
    """
    if job_key is None:
        job_key = _job_key_from_argv()
    if mode is None:
        mode = _mode_from_argv()
    assert mode in ("new", "extend", "replace"), f"bad mode {mode!r}"
    reconcile_wip(job_key)
    _assert_not_running(job_key)
    if argv is None:
        argv = sys.argv
    argv0, argv_rest = argv[0], argv[1:]
    pid = os.getpid()
    lsf_jobid = os.environ.get("LSB_JOBID")  # set by LSF inside a job; None for local runs
    started_at = _now()
    commit_id, diff = _provenance()
    diff_hash = sha256(diff.encode()).hexdigest()
    # Raw stored (relative) value, not latest_outdir's absolute form -- what we re-store must
    # stay relative, and _abs tolerates a legacy absolute row either way.
    reuse = _latest("outdir", job_key, None) if mode in ("extend", "replace") else None
    if mode == "replace" and reuse is not None:
        shutil.rmtree(_abs(reuse), ignore_errors=True)
    with _DB_LOCK:
        conn = _connect()
        with conn:
            conn.execute(
                "INSERT OR IGNORE INTO git_diffs (hash, diff) VALUES (?, ?)", (diff_hash, diff)
            )
            cur = conn.execute(
                "INSERT INTO runs (job_key, argv0, argv_rest, outdir, pid, lsf_jobid, started_at, commit_id, diff_hash, inputs) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    job_key, argv0, json.dumps(argv_rest), "", pid, lsf_jobid,
                    started_at, commit_id, diff_hash,
                    json.dumps(inputs) if inputs is not None else None,
                ),
            )
            run_id = cur.lastrowid
            assert run_id is not None, "INSERT INTO runs did not produce a rowid"
            out = reuse if reuse is not None else f"{job_key}/run{run_id:05d}"
            Path(_abs(out)).mkdir(parents=True, exist_ok=True)
            conn.execute("UPDATE runs SET outdir = ? WHERE run_id = ?", (out, run_id))
        conn.close()
    return Run(run_id=run_id, outdir=_abs(out), job_key=job_key)


def _finish(run_id: int, status: str) -> None:
    assert status in ("done", "failed"), f"bad status {status!r}"
    with _DB_LOCK:
        conn = _connect()
        with conn:
            conn.execute(
                "UPDATE runs SET status = ?, ended_at = ? WHERE run_id = ?", (status, _now(), run_id)
            )
        conn.close()


def start_managed(job_key: str, mode: str, argv: "list[str]", inputs: "list[int]") -> Run:
    """The DRIVER-side half of a managed run: insert the wip row for a stage it is about to
    launch, recording the stage's command and its exact input run_ids at insert. The driver
    then threads the identity to the child via --run-row/--run-outdir (see run()) and closes
    the lifecycle with finish_managed() when the stage's process exits. The row inherits the
    driver's own pid/LSB_JOBID for liveness until set_lsf_jobid() records the stage's real job
    id from bsub's submission ack -- either way reconcile_wip has something true to check."""
    return _start(job_key, mode, argv=argv, inputs=inputs)


def finish_managed(run_id: int, ok: bool) -> None:
    """The driver-side close of a managed run, from the stage process's exit status."""
    _finish(run_id, "done" if ok else "failed")


def set_lsf_jobid(run_id: int, jobid: str) -> None:
    """Upgrade a managed row's liveness handle from the driver's identity to the stage's own
    LSF job id (parsed from bsub's 'Job <id> is submitted' ack) -- after this, reconcile_wip
    tracks the stage job itself, which keeps running even if the driver dies. A freshly
    submitted job is by definition queued, so lsf_state starts at PEND here."""
    with _DB_LOCK:
        conn = _connect()
        with conn:
            conn.execute(
                "UPDATE runs SET lsf_jobid = ?, lsf_state = 'PEND' WHERE run_id = ?", (jobid, run_id)
            )
        conn.close()


def set_lsf_state(run_id: int, state: str) -> None:
    """LSF dispatch detail for a still-wip managed row -- 'PEND' (queued) vs 'RUN <host>',
    parsed from bsub -K's own '<<Starting on ...>>' chatter as it streams past the driver (no
    bjobs polling). Pure display metadata: the wip/done/failed lifecycle doesn't depend on it,
    and it's only meaningful while status is wip."""
    with _DB_LOCK:
        conn = _connect()
        with conn:
            conn.execute("UPDATE runs SET lsf_state = ? WHERE run_id = ?", (state, run_id))
        conn.close()


@contextmanager
def run(job_key: "str | None" = None, mode: "str | None" = None):
    """``with rundb.run() as r: ... use r.outdir ...`` -- wip on entry, done on clean exit,
    failed (+ reraise) on any other exception. ``sys.exit(0)``/a bare ``SystemExit()`` counts as
    a clean exit too -- only a truthy exit code marks it failed.

    MANAGED runs (launched by dagrunner, detected via --run-row/--run-outdir in our argv --
    see _managed_from_argv) touch the db not at all: the driver already inserted our row and
    will mark done/failed from our exit code, which the try/except below still shapes
    faithfully (exceptions propagate -> nonzero exit -> failed). This is what makes cluster
    runs safe -- compute nodes never write the shared-filesystem sqlite file.

    Both args are optional: left unset, ``job_key`` and ``mode`` ("new"/"extend"/"replace", see
    ``_start``) are parsed straight out of this invocation's own sys.argv
    (``_job_key_from_argv``/``_mode_from_argv``) -- a caller wired up through dagrunner.py never
    needs its own --job-key/--extend/--replace argparse plumbing, it just needs to forward its
    unrecognized args (see script.py).

    "extend" only hands the script back the SAME directory it wrote before -- whether the
    script actually reads existing state out of that directory and picks up where it left off,
    versus just starting over and overwriting what's there, is entirely up to the script's own
    logic. rundb can preserve the *option* to resume; it has no way to inspect or enforce
    whether a script actually takes it.

    This is the sentinel-file lifecycle (.start/.end/.done) from orchestration.py's bash
    payload, as one atomic DB row instead of three separate files that can get out of sync (a
    stray .log with no .done, etc.)."""
    managed = _managed_from_argv()
    if managed is not None:
        run_id, outdir = managed
        yield Run(run_id=run_id, outdir=outdir, job_key=job_key or _job_key_from_argv())
        return
    r = _start(job_key, mode)
    try:
        yield r
    except SystemExit as e:
        _finish(r.run_id, "done" if e.code in (None, 0) else "failed")
        raise
    except BaseException:
        _finish(r.run_id, "failed")
        raise
    else:
        _finish(r.run_id, "done")


def _latest(column: str, job_key: str, status: "str | None"):
    """``column``'s value (whatever type that column holds) on the most recent run tagged
    ``job_key`` (optionally filtered by status), or None if no such run exists (or the column is
    NULL there). ``column`` is interpolated into the SQL -- callers pass a literal column name,
    never user input."""
    sql = f"SELECT {column} FROM runs WHERE job_key = ?"
    params: "tuple[str, ...]" = (job_key,)
    if status is not None:
        sql += " AND status = ?"
        params = (job_key, status)
    conn = _connect()
    row = conn.execute(sql + " ORDER BY run_id DESC LIMIT 1", params).fetchone()
    conn.close()
    return row[0] if row else None


def latest_outdir(job_key: str, status: "str | None" = None) -> "str | None":
    """The outdir (absolute, resolved against this machine's ROOT) of the most recent run
    tagged ``job_key`` (optionally filtered by status -- e.g. "done"), or None if no such run
    exists. Lets a caller resolve a dependency's output path via a query instead of assuming a
    fixed filesystem path."""
    raw = _latest("outdir", job_key, status)
    return _abs(raw) if raw is not None else None


def is_done(job_key: str) -> bool:
    """True iff the most recent run tagged ``job_key`` completed successfully."""
    return latest_outdir(job_key, status="done") is not None


def started_at(job_key: str, status: "str | None" = None) -> "str | None":
    """The started_at of the most recent run tagged ``job_key`` (optionally filtered by status),
    or None if no such run exists. Sortable lexicographically (YYYY-MM-DD-HH-MM-SS) -- lets a
    caller compare "when did this last succeed" against a dependency's own ended_at to detect
    staleness, without needing a fuller row type than latest_outdir already returns."""
    return _latest("started_at", job_key, status)


def ended_at(job_key: str, status: "str | None" = None) -> "str | None":
    """started_at's counterpart -- the ended_at of the most recent run tagged ``job_key``
    (optionally filtered by status). None if no such run exists, or if it's still wip."""
    return _latest("ended_at", job_key, status)


def latest_run_id(job_key: str, status: "str | None" = None) -> "int | None":
    """run_id of the most recent run tagged ``job_key`` (optionally filtered by status) -- the
    exact-provenance handle that start_managed's inputs/stale_inputs work in."""
    return _latest("run_id", job_key, status)


def _job_key_of(run_id: int) -> str:
    conn = _connect()
    row = conn.execute("SELECT job_key FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    conn.close()
    assert row is not None, f"no run with run_id {run_id}"
    return row[0]


def stale_inputs(job_key: str) -> "list[str] | None":
    """Exact staleness for job_key's most recent successful run, from recorded provenance: the
    job_keys among its recorded inputs whose own latest done run is no longer the very run it
    consumed. [] = provenance says fresh. None = that run recorded no inputs (it wasn't launched
    by dagrunner, so its dependencies are unknown -- which is not the same as fresh). Exact
    where timestamps aren't: a dep re-run via "extend" reuses its directory but gets a new
    run_id, and same-second ties don't confuse run_id comparison. Judged against the inputs
    recorded when the run happened, not the current DAG definition."""
    raw = _latest("inputs", job_key, "done")
    if raw is None:
        return None
    stale = []
    for rid in json.loads(raw):
        dep_key = _job_key_of(rid)
        if latest_run_id(dep_key, status="done") != rid:
            stale.append(dep_key)
    return stale


def _duration(started: str, ended: "str | None") -> timedelta:
    """ended - started -- a still-``wip`` row (ended is None, e.g. a training run this is
    called on while it's still going) counts up to now instead of being skipped, so a live
    total keeps growing during an active run rather than looking stuck."""
    start = datetime.strptime(started, "%Y-%m-%d-%H-%M-%S")
    end = datetime.strptime(ended, "%Y-%m-%d-%H-%M-%S") if ended else datetime.now()
    return end - start


def _durations_by_status(column: str, value: str) -> "dict[str, timedelta]":
    """Wall-clock time summed per status ("wip"/"done"/"failed") over every row where ``column``
    (a literal column name -- "outdir" or "job_key", never user input) equals ``value``. Only
    statuses with at least one row are present."""
    conn = _connect()
    rows = conn.execute(
        f"SELECT started_at, ended_at, status FROM runs WHERE {column} = ?", (value,)
    ).fetchall()
    conn.close()
    by_status: "dict[str, timedelta]" = {}
    for started, ended, status in rows:
        by_status[status] = by_status.get(status, timedelta()) + _duration(started, ended)
    return by_status


def time_in_outdir_by_status(outdir: str) -> "dict[str, timedelta]":
    """Wall-clock time actually spent writing to ``outdir`` (absolute, as returned by
    latest_outdir), per status. A single outdir can span multiple rows -- "extend" mode resumes
    the same outdir under a brand new row/run_id each time -- so a stage's true invested time is
    this sum, not just its most recent session's duration."""
    return _durations_by_status("outdir", _rel(outdir))


def time_in_outdir(outdir: str) -> timedelta:
    """time_in_outdir_by_status's total, combined into one number."""
    return sum(time_in_outdir_by_status(outdir).values(), timedelta())


def time_for_job_by_status(job_key: str) -> "dict[str, timedelta]":
    """Wall-clock time spent across EVERY outdir ``job_key`` has ever used, per status -- covers
    new/replace too, where a stage's history spans multiple distinct outdirs rather than just its
    current one. E.g. shows how much time was burned on failed attempts versus the run(s) that
    actually finished."""
    return _durations_by_status("job_key", job_key)


def time_for_job(job_key: str) -> timedelta:
    """time_for_job_by_status's total, combined into one number."""
    return sum(time_for_job_by_status(job_key).values(), timedelta())


def gc(
    job_key: str, keep_last_n_failed: int = 1, keep_done: bool = True, dry_run: bool = False
) -> "list[str]":
    """Delete on-disk outdirs for job_key's past attempts, keeping the most recent 'done' outdir
    (if keep_done) and the keep_last_n_failed most recent FAILED outdirs -- everything else for
    job_key is rmtree'd. A "wip" outdir is never removed: reconcile_wip(job_key) runs first, so
    any "wip" row still standing afterward is confirmed genuinely alive, not stale. Never touches
    runs rows -- only files, like wipe(). Groups by outdir (not run_id), since "extend" can have
    multiple rows share one outdir -- an outdir's status/recency for ranking purposes comes from
    its most recent row. Returns the outdirs removed (or that would be removed, if dry_run).
    Never called automatically -- an explicit, on-demand cleanup action, same spirit as wipe()."""
    reconcile_wip(job_key)
    conn = _connect()
    rows = conn.execute(
        "SELECT run_id, outdir, status FROM runs WHERE job_key = ? ORDER BY run_id", (job_key,)
    ).fetchall()
    conn.close()
    latest_status: "dict[str, str]" = {}
    latest_run_id: "dict[str, int]" = {}
    for run_id, out, status in rows:
        latest_status[out] = status
        latest_run_id[out] = run_id
    by_recency = sorted(latest_status, key=lambda out: latest_run_id[out], reverse=True)

    keep: "set[str]" = set()
    kept_done = False
    kept_failed = 0
    for out in by_recency:
        status = latest_status[out]
        if status == "wip":
            keep.add(out)  # confirmed genuinely alive by reconcile_wip above -- never remove
        elif status == "done":
            if keep_done and not kept_done:
                keep.add(out)
                kept_done = True
        elif kept_failed < keep_last_n_failed:
            keep.add(out)
            kept_failed += 1

    to_remove = [_abs(out) for out in by_recency if out not in keep]
    if not dry_run:
        for out in to_remove:
            shutil.rmtree(out, ignore_errors=True)
    return to_remove


def gc_all(
    keep_done: bool = True, keep_last_n_failed: int = 1, dry_run: bool = False
) -> "dict[str, list[str]]":
    """gc() for every job_key that has ever recorded a run -- see gc()."""
    conn = _connect()
    job_keys = [
        row[0]
        for row in conn.execute("SELECT DISTINCT job_key FROM runs WHERE job_key IS NOT NULL").fetchall()
    ]
    conn.close()
    return {
        job_key: gc(job_key, keep_last_n_failed=keep_last_n_failed, keep_done=keep_done, dry_run=dry_run)
        for job_key in job_keys
    }


def wipe(job_key: str) -> "list[str]":
    """Delete EVERY directory ever recorded for ``job_key`` from disk -- job_key's entire
    history, not just its most recent outdir (``_start``'s "replace" mode clears just that one,
    inline, since it already has the path in hand). Exactly ``gc()`` told to keep nothing, plus a
    refusal (via _assert_not_running, after reconcile_wip) if job_key has a genuinely still-alive
    "wip" attempt -- wiping out from under a live process is worse than a loud failure here. No
    mode calls this automatically; it's a standalone, explicit "start completely over" action.
    Doesn't touch rundb.db's rows -- the ledger keeps recording that those runs happened, only
    the files are gone."""
    reconcile_wip(job_key)
    _assert_not_running(job_key)
    return gc(job_key, keep_last_n_failed=0, keep_done=False)


if __name__ == "__main__":
    from . import _cli

    _cli.help_if_asked(__doc__)  # otherwise: a no-arg smoke self-test of the run lifecycle
    with run(job_key="smoke-test") as r:
        print(r.run_id, r.outdir, is_done("smoke-test"))
    print("is_done:", is_done("smoke-test"))
    print("latest_outdir:", latest_outdir("smoke-test"))
