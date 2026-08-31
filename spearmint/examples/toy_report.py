#!/usr/bin/env python
"""Standalone report script for toy_report_demo: ledger + run dirs -> static
ROOT/_reports/e05_report/report.html. The driver shells out to this on every stage finalize
and tick, and the SAME file runs by hand -- mid-run, after the run, or a week later with new
report code (every render is a fresh process, so edits just take effect):

    uv run python spearmint/examples/toy_report.py

spearmint.load absorbs the live races (missing/torn files -> empty collections), so there is
nothing to guard here -- render what exists, the next rebuild fills in the rest."""

from pathlib import Path

from spearmint import load, rundb, viz

PREFIX = "e05_report"


def main() -> None:
    done = load.runs(f"{PREFIX}/*")                # completed runs -> summaries
    live = load.runs(f"{PREFIX}/*", status=None)   # newest run of ANY status -> growing curves
    curves = {k.removeprefix(f"{PREFIX}/"): load.rows(f"{d}/metrics.jsonl") for k, d in live.items()}
    finals = {k.removeprefix(f"{PREFIX}/"): load.json_file(f"{d}/summary.json") for k, d in done.items()}
    missing = [k for k in curves if not finals.get(k)]
    html = viz.page(
        viz.note(f"still running: {', '.join(missing)}") if missing else "",
        viz.lines(curves, x="step", y=["loss", "val_*"], dash={"val_*": "dash"}, logy=True,
                  title="loss, A vs B (val dashed)"),
        viz.table(finals, title="final metrics"),
        title=PREFIX,
        refresh=1 if missing else None,
    )
    out = Path(rundb.root()) / rundb.REPORTS_DIR / PREFIX
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.html").write_text(html)


if __name__ == "__main__":
    main()
