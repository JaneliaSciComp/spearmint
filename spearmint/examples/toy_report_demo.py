#!/usr/bin/env python
"""Report demo: a normal function Stage scheduled as a live sidecar. Its one versioned run
directory is updated while training runs and becomes immutable when the experiment settles.
Each refresh self-dispatches this file in a fresh process, so edits take effect mid-run.

    uv run python spearmint/examples/toy_report_demo.py --replace 'train_*'
    # while it runs: edit render() below and watch the report change
"""

from pathlib import Path

import spearmint as O
from spearmint import load, viz

SCRIPT = "spearmint/examples/script.py"

# Toy stages stream metrics for only ~20s; tick fast enough that edits + curves show up
# mid-stage. Real experiments keep the 120s default.
O.dagrunner.REPORT_TICK_SECONDS = 0.5

e = O.Experiment(prefix="e05_report", cmd_prefix=["uv", "run", "python"])
train_a = e.Stage("train_a", cmd=lambda: [SCRIPT, "--stage=a"])
train_b = e.Stage("train_b", cmd=lambda: [SCRIPT, "--stage=b"])


def render(run: O.rundb.Run) -> None:
    done = load.runs("e05_report/train_*")
    live = load.runs("e05_report/train_*", status=None)
    curves = {k.rsplit("/", 1)[-1]: load.rows(f"{d}/metrics.jsonl") for k, d in live.items()}
    finals = {k.rsplit("/", 1)[-1]: load.json_file(f"{d}/summary.json") for k, d in done.items()}
    missing = [k for k in curves if not finals.get(k)]
    html = viz.page(
        viz.note(f"still running: {', '.join(missing)}") if missing else "",
        viz.lines(curves, x="step", y=["loss", "val_*"], dash={"val_*": "dash"}, logy=True,
                  title="loss, A vs B (val dashed)"),
        viz.table(finals, title="final metrics"),
        title="e05_report", refresh=1 if missing else None,
    )
    Path(run.outdir, "report.html").write_text(html)


e.report = e.Stage("report", fn=render)

if __name__ == "__main__":
    e.main()
