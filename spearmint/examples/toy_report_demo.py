#!/usr/bin/env python
"""Report demo: a plain Python function, composed with spearmint.viz, assigned to
``e.report``. The DRIVER re-renders it after every stage finalize, every couple of minutes
while stages run, and once at the end -- writing ROOT/_reports/e05_report/report.html, linked
from the status table. It's just code: load whatever files you like, munge with the whole
language, and tolerate not-yet-done stages (``savedir(...)`` returns None) so the report is
useful DURING the run, not only after it.

    uv run python spearmint/examples/toy_report_demo.py   # then: spearmint browse
"""

import json
from pathlib import Path

import spearmint as O
from spearmint import viz

SCRIPT = "spearmint/examples/script.py"

# Toy stages stream metrics for only ~10s; tick fast enough that the report visibly grows
# mid-stage. Real experiments keep the 120s default.
O.dagrunner.REPORT_TICK_SECONDS = 0.5

e = O.Experiment(prefix="e05_report", cmd_prefix=["uv", "run", "python"])
train_a = e.Stage("train_a", cmd=lambda: [SCRIPT, "--stage=a"])
train_b = e.Stage("train_b", cmd=lambda: [SCRIPT, "--stage=b"])


def make_report(savedir) -> str:
    curves: "dict[str, list[dict]]" = {}
    finals: "dict[str, dict]" = {}
    missing = []
    for stage in (train_a, train_b):
        # Curves from the LIVE dir (wip included -- python-native means one rundb call away),
        # so mid-run renders show them growing; summaries only from a completed run.
        live = O.rundb.latest_outdir(stage.job_key)
        metrics = Path(live or "") / "metrics.jsonl"
        if live is not None and metrics.exists():
            curves[stage.name] = [json.loads(ln) for ln in metrics.read_text().splitlines()]
        # exists()-guard even though savedir() says done: a REPLACE re-run clears the last
        # done outdir in place, so 'latest done' files can vanish mid-run. Reports render
        # whatever half-state exists right now -- guard every read.
        done = savedir(stage)
        summary = Path(done or "") / "summary.json"
        if done is not None and summary.exists():
            finals[stage.name] = json.loads(summary.read_text())
        else:
            missing.append(stage.name)
    return viz.page(
        viz.note(f"still running: {', '.join(missing)}") if missing else "",
        viz.lines(curves, x="step", y=["loss", "val_*"], dash={"val_*": "dash"}, logy=True,
                  title="loss, A vs B (val dashed)"),
        viz.table(finals, title="final metrics"),
        title="e05_report",
        # Refresh only while incomplete: the driver's FINAL render emits a refresh-free page,
        # so an open tab's last tick loads it and stops reloading (reload by hand to re-arm
        # after kicking a new run).
        refresh=1 if missing else None,
    )


e.report = make_report

if __name__ == "__main__":
    e.main()
