"""Design scratchpad: the QUERY INTERFACE for report scripts -- what belongs in
spearmint.load, what stays a comprehension, what waits. Companion to rundirs.md
(folders-vs-db, report-as-script sections); runnable against this repo's toy ledger:

    uv run python queries.py

Prototypes here use load/rundb/viz internals freely; anything we adopt graduates into
load.py with a real name. Nothing in this file is API."""

import time
from pathlib import Path

from spearmint import load, rundb, viz

# ---------------------------------------------------------------------------------------
# The premise (settled once already, by the viz-spec rejection): the query LANGUAGE is
# Python. We tried a JSON view-spec for reports and it was janky within a day -- filters,
# renames, joins all wanted to be code. Same trap here: the moment load grows
# where=/group_by=/order_by= parameters it's a bad SQL. So the design target is:
#
#   load gets data ACROSS THE LEDGER BOUNDARY into flat Python shapes (dicts, lists of
#   dicts) -- and stops. Everything after is comprehensions, sorted(), fnmatch.
#
# What load has today:  runs(pattern, status) -> {job_key: outdir}
#                       rows(path) -> [row dicts]   json_file(path) -> dict
#                       filenames(dirs, glob) -> [names]
# The open question: which RECURRING comprehensions deserve promotion to primitives?
# ---------------------------------------------------------------------------------------


# --- Use case 1: one experiment's report (today's API, no gaps) --------------------------
# Curves per stage, summary table per stage. Two comprehensions; this is toy_report.py.
# Verdict: nothing to add.

def usecase_one_experiment() -> str:
    rr = load.runs("e05_report/*")
    return viz.page(
        viz.lines({k: load.rows(f"{d}/metrics.jsonl") for k, d in rr.items()}, x="step"),
        viz.table({k: load.json_file(f"{d}/summary.json") for k, d in rr.items()}),
    )


# --- Use case 2: hyperparam sweep -- the "one row per run" table -------------------------
# 200 trials, job_keys like "e07_sweep/lr1e-3_wd1e-4". Every real question -- best-k,
# filter by param, sort by metric, param-vs-metric plot -- wants the SAME shape first:
# one flat dict per run, params and metrics as columns. That reshape is the recurring
# comprehension, and it's just barely annoying enough (nested-summary flattening, keeping
# the outdir joinable) to deserve promotion:

def sweep(pattern: str) -> "list[dict]":
    """One row per (matching job_key's latest done run): job_key + outdir + flattened
    summary.json. THE sweep primitive candidate."""
    return [
        {"job_key": k, "outdir": d, **viz._flat(load.json_file(f"{d}/summary.json"))}
        for k, d in load.runs(pattern).items()
    ]

# Everything downstream is stdlib on that shape -- no query params needed, QED:
#
#   rows = sweep("e07_sweep/*")
#   best = sorted(rows, key=lambda r: r.get("best_val_loss", 9e9))[:8]      # ORDER BY/LIMIT
#   small = [r for r in rows if r.get("lr", 0) < 1e-3]                      # WHERE
#   viz.table({r["job_key"]: r for r in best})                              # (extra cols fine:
#                                                                           #   _flat drops none;
#                                                                           #   metrics= filters)
#   viz.lines({r["job_key"]: load.rows(f"{r['outdir']}/metrics.jsonl") for r in best},
#             x="step", y="val_*")                                          # top-k curves: the
#                                                                           # outdir column IS
#                                                                           # the join key
#
# Param-vs-metric ("does lr matter?"): sweep rows are already viz-shaped rows, so
# viz.lines(rows, x="lr", y="best_val_loss") nearly works -- except lines draws LINES and
# trials are unordered points. A viz.scatter (same row-dict contract, mode="markers") is
# the missing viz half of this use case. Worth building only alongside a real sweep.
#
# Design wobble: should sweep() also parse params out of the job_key name ("lr1e-3" ->
# lr=1e-3)? NO -- that's guessing at user encoding, the config-language trap again. The
# contract stays: params the user wants queryable, the user WRITES to summary.json (or a
# config.json we read the same way). Data the user recorded, not names we parse.


# --- Use case 3: cross-experiment comparison ---------------------------------------------
# A-vs-B across prefixes is just two patterns -- dict union, rekeyed however reads best.
# The whole-language principle paying rent; nothing to promote:
#
#   rr = {"mae": load.runs("e00_mae/train"), "lejepa": load.runs("e01_lejepa/train")}
#   viz.lines({name: load.rows(f"{d}/metrics.jsonl")
#              for name, m in rr.items() for d in m.values()}, x="step")


# --- Use case 4: attempts of ONE job_key over time ---------------------------------------
# "Did today's attempt beat yesterday's?" load.runs collapses to latest-per-key, so
# history needs a second primitive. Note the coupling to rundirs.md's rerun-in-place
# question: extend/replace REUSE outdirs, so run rows outnumber distinct dirs and history
# through the filesystem is already lossy -- dedupe is load-bearing here, and under
# always-fresh-dirs it would evaporate. (The /diff page dances this same dance.)

def attempts(job_key: str) -> "list[str]":
    """Distinct done outdirs of job_key, oldest -> newest. THE history primitive candidate."""
    with rundb._connect(readonly=True) as conn:
        got = conn.execute(
            "SELECT outdir FROM runs WHERE job_key = ? AND status = 'done' ORDER BY run_id",
            (job_key,)).fetchall()
    return list(dict.fromkeys(rundb._abs(d) for (d,) in got))

#   viz.lines({Path(d).name: load.rows(f"{d}/metrics.jsonl") for d in attempts(k)}, x="step")


# --- The scale question (rundirs.md's pressure point, measured) ---------------------------
# sweep() opens one summary.json per trial. On local disk that's microseconds; on GPFS
# metadata ops are the slow part, so 500 trials might cost single-digit seconds. For a
# REPORT SCRIPT (renders on demand / per driver tick) that's acceptable; for the DASHBOARD
# home page (auto-refreshing, spearmint-core, no project env) it isn't -- which is exactly
# why rundirs.md puts the fix in an INDEX (driver copies summary key-values into a ledger
# table at finalize), not in making load cleverer. load stays dumb; the index feeds the
# dashboard; both read the same files-as-truth.
#
# If a project outgrows even that, the escape hatch is duckdb IN THE REPORT SCRIPT --
# project-side dependency, zero spearmint involvement, folder truth queried in place:
#
#   import duckdb
#   duckdb.query("SELECT max(step), min(loss) FROM read_json_auto('output_rundb/e07_sweep/*/run*/metrics.jsonl')")
#
# (2.0's async I/O + VARIANT type make this niche genuinely good; see rundirs.md for why
# duckdb-as-central-db stays rejected.)


# --- Current lean -------------------------------------------------------------------------
# Promote NOTHING yet. sweep() and attempts() are the two candidates, each ~5 lines a user
# could write in their report script (this file is proof). The e00 mia-muvit report
# conversion is the forcing function: if it writes either of these verbatim, that's the
# signal to move it into load.py. Premature promotion costs API surface forever; a 5-line
# wait costs nothing.


if __name__ == "__main__":
    rundb.anchor("output_rundb")
    t0 = time.perf_counter()
    rows = sweep("e05_report/*")
    dt = time.perf_counter() - t0
    print(f"sweep('e05_report/*'): {len(rows)} rows in {dt * 1e3:.1f} ms")
    for r in rows:
        print(" ", {k: v for k, v in r.items() if k != "outdir"})
    best = min(rows, key=lambda r: r.get("best_val_loss", 9e9))
    print("best:", best["job_key"])
    print("attempts(train_a):", [Path(d).name for d in attempts("e05_report/train_a")])
