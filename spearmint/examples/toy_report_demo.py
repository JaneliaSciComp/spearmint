#!/usr/bin/env python
"""Live declarative dashboard plus a normal retrospective function report stage.

    uv run python spearmint/examples/toy_report_demo.py --replace 'train_*'
    # browse shows the growing dashboard; the report runs once after both trainers settle
"""

from pathlib import Path

import spearmint as O
from spearmint import load, viz

SCRIPT = "spearmint/examples/script.py"

e = O.Experiment(prefix="e05_report", cmd_prefix=["uv", "run", "python"])
train_a = e.Stage("train_a", cmd=lambda: [SCRIPT, "--stage=a"])
train_b = e.Stage("train_b", cmd=lambda: [SCRIPT, "--stage=b"])


def render_report(run: O.rundb.Run) -> None:
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


e.dashboard = O.Dashboard(
    O.Lines(
        [train_a, train_b], file="metrics.jsonl", x="step", y=["loss", "val_*"],
        dash={"val_*": "dash"}, colors={"train_a": "#58a6ff", "train_b": "#3fb950"},
        logy=True, title="live loss",
    ),
    title="e05 live", refresh=1,
)
e.report = e.Stage("report", fn=render_report)

if __name__ == "__main__":
    e.main()
