"""Read-side loaders for dashboards and independent analysis programs.

``history()`` exposes every recorded attempt, including its code provenance and dependency
run IDs, so retrospective reports can compare runs from different commits without being part
of an experiment DAG. ``runs()`` selects only the latest attempt per job key. The artifact
helpers turn run-dir files into the plain Python shapes consumed by :mod:`spearmint.viz`.

``runs()`` maps job_keys to outdirs; ``rows``/``json_file`` read one artifact each. Together
they make cross-run reports one comprehension:

    rr = load.runs("e05_report/*")
    viz.lines({k: load.rows(f"{d}/metrics.jsonl") for k, d in rr.items()}, x="step")
    viz.table({k: load.json_file(f"{d}/summary.json") for k, d in rr.items()})

The artifact helpers absorb live-render races ON PURPOSE: dashboards read while runs write,
replace clears dirs mid-read, summary.json may be half-written. Missing or torn data comes
back as an EMPTY collection (never None, never a raise) -- the next refresh heals it."""

import csv
import io
import json as _json
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path

from . import rundb


@dataclass(frozen=True)
class RunRecord:
    """One immutable ledger attempt, with enough provenance for retrospective selection."""

    run_id: int
    job_key: "str | None"
    outdir: str
    status: str
    commit_id: str
    diff: str
    argv: "list[str]"
    inputs: "list[int] | None"
    started_at: str
    ended_at: "str | None"
    lsf_jobid: "str | None"
    lsf_state: "str | None"


def history(
    pattern: str = "*",
    status: "str | None" = None,
    commit: "str | None" = None,
) -> "list[RunRecord]":
    """Return all matching attempts, oldest first.

    ``pattern`` is a fnmatch over job keys. ``status`` selects an exact ledger status;
    ``commit`` accepts a full commit ID or prefix. Unlike :func:`runs`, attempts are never
    collapsed, so superseded runs remain available for cross-version analysis.
    """
    clauses = []
    params = []
    if status is not None:
        clauses.append("r.status = ?")
        params.append(status)
    if commit is not None:
        clauses.append("r.commit_id LIKE ?")
        params.append(f"{commit}%")
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    query = (
        "SELECT r.run_id, r.job_key, r.outdir, r.status, r.commit_id, d.diff, "
        "r.argv0, r.argv_rest, r.inputs, r.started_at, r.ended_at, "
        "r.lsf_jobid, r.lsf_state FROM runs r "
        "JOIN git_diffs d ON d.hash = r.diff_hash" + where + " ORDER BY r.run_id"
    )
    with rundb._connect(readonly=True) as conn:
        rows = conn.execute(query, params).fetchall()
    return [
        RunRecord(
            run_id=rid,
            job_key=job_key,
            outdir=rundb._abs(outdir),
            status=run_status,
            commit_id=commit_id,
            diff=diff,
            argv=[argv0, *_json.loads(argv_rest)],
            inputs=None if inputs is None else _json.loads(inputs),
            started_at=started_at,
            ended_at=ended_at,
            lsf_jobid=lsf_jobid,
            lsf_state=lsf_state,
        )
        for (rid, job_key, outdir, run_status, commit_id, diff, argv0, argv_rest, inputs,
             started_at, ended_at, lsf_jobid, lsf_state) in rows
        if fnmatch(job_key or "", pattern)
    ]


def runs(pattern: str = "*", status: "str | None" = "done") -> "dict[str, str]":
    """{job_key: absolute outdir of its latest run} for ledger job_keys matching the fnmatch
    ``pattern``. ``status="done"`` (default) sees only completed runs -- right for summaries;
    ``status=None`` sees the newest run of ANY status -- right for live curves."""
    q = "SELECT job_key, outdir FROM runs" + ("" if status is None else " WHERE status = ?")
    with rundb._connect(readonly=True) as conn:
        got = conn.execute(q + " ORDER BY run_id", () if status is None else (status,)).fetchall()
    # run_id order + dict assignment: last write per key wins = latest run per key.
    return {k: rundb._abs(d) for k, d in got if fnmatch(k, pattern)}


def _read(path) -> "str | None":
    """File text, or None if it's missing/vanishing (see module docstring)."""
    try:
        return Path(path).read_text()
    except OSError:
        return None


def rows(path) -> "list[dict]":
    """metrics.jsonl or .csv -> list of row dicts (what viz.lines eats). A torn trailing jsonl
    line (writer mid-append) is skipped; csv cells stay strings (viz coerces numerics)."""
    text = _read(path)
    if text is None:
        return []
    if str(path).endswith(".csv"):
        return list(csv.DictReader(io.StringIO(text)))
    out = []
    for ln in text.splitlines():
        try:
            out.append(_json.loads(ln))
        except ValueError:
            pass  # torn write -- complete on the next rebuild
    return out


def json_file(path) -> dict:
    """A .json file -> dict (what viz.table eats per column). Missing/torn -> {}."""
    text = _read(path)
    try:
        got = _json.loads(text) if text is not None else {}
    except ValueError:
        got = {}
    return got if isinstance(got, dict) else {}


def filenames(dirs, pattern: str = "*.png") -> "list[str]":
    """Sorted union of matching filenames across ``dirs`` -- the column axis for a cross-run
    image grid: viz.images renders '-' where a run lacks one, so alignment is by name."""
    return sorted({p.name for d in dirs for p in Path(d).glob(pattern)})
