"""Read-side loaders for report functions: ledger + run-dir files -> the shapes viz eats.
``runs()`` maps job_keys to outdirs; ``rows``/``json_file`` read one artifact each. Together
they make cross-run reports one comprehension:

    rr = load.runs("e05_report/*")
    viz.lines({k: load.rows(f"{d}/metrics.jsonl") for k, d in rr.items()}, x="step")
    viz.table({k: load.json_file(f"{d}/summary.json") for k, d in rr.items()})

Everything here absorbs the live-render races ON PURPOSE: reports rebuild while runs write,
replace clears dirs mid-read, summary.json may be half-written. Missing or torn data comes
back as an EMPTY collection (never None, never a raise) -- the next rebuild heals it. This is
the one corner of spearmint where errors are data, because a report must render the 90% that
exists rather than die on the 10% that doesn't yet."""

import csv
import io
import json as _json
from fnmatch import fnmatch
from pathlib import Path

from . import rundb


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
