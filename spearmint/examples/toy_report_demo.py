#!/usr/bin/env python
"""Build a retrospective report independently of an experiment run.

    uv run python spearmint/examples/toy_report_demo.py
"""

from pathlib import Path

from spearmint import load, rundb, viz

rundb.anchor("output_rundb")
runs = load.history("dashboard_demo/train_*", status="done")
curves = {
    f"{run.job_key}@{run.run_id}": load.rows(Path(run.outdir) / "metrics.jsonl")
    for run in runs
}
out = Path("_reports/toy_report.html")
out.parent.mkdir(exist_ok=True)
out.write_text(viz.page(viz.lines(curves, x="step", y=["loss", "val_*"]),
                             title="dashboard history"))
print(out)
