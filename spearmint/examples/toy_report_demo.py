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

e = O.Experiment(prefix="e05_report", cmd_prefix=["uv", "run", "python"])
train_a = e.Stage("train_a", cmd=lambda: [SCRIPT, "--stage=a"])
train_b = e.Stage("train_b", cmd=lambda: [SCRIPT, "--stage=b"])


def make_report(savedir) -> str:
    curves: "dict[str, list[dict]]" = {}
    finals: "dict[str, dict]" = {}
    missing = []
    for stage in (train_a, train_b):
        d = savedir(stage)
        if d is None:
            missing.append(stage.name)
            continue
        curves[stage.name] = [json.loads(ln) for ln in (Path(d) / "metrics.jsonl").read_text().splitlines()]
        finals[stage.name] = json.loads((Path(d) / "summary.json").read_text())
    return viz.page(
        viz.note(f"waiting on: {', '.join(missing)}") if missing else "",
        viz.lines(curves, x="step", y=["loss", "val_*"], dash={"val_*": "dash"}, logy=True,
                  title="loss, A vs B (val dashed)"),
        viz.table(finals, title="final metrics"),
        title="e05_report", refresh=60,
    )


e.report = make_report

if __name__ == "__main__":
    e.main()
