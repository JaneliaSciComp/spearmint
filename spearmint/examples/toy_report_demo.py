#!/usr/bin/env python
"""Report demo: the report function lives in its OWN module (toy_report_fn.py) referenced by
path, so the driver hot-reloads it -- edit the report code WHILE the experiment runs and the
next tick (or stage finalize) renders with your changes. The driver re-renders at start,
after every finalize, every REPORT_TICK_SECONDS while stages run, and once at the end,
writing ROOT/_reports/e05_report/report.html (linked from the status table).

    uv run python spearmint/examples/toy_report_demo.py --replace 'train_*'
    # while it runs: edit toy_report_fn.py (a title, a new panel) and watch the report change

A plain function assigned to ``e.report`` still works (static for the run's lifetime); the
path spec is what buys live editing.
"""

import spearmint as O

SCRIPT = "spearmint/examples/script.py"

# Toy stages stream metrics for only ~20s; tick fast enough that edits + curves show up
# mid-stage. Real experiments keep the 120s default.
O.dagrunner.REPORT_TICK_SECONDS = 0.5

e = O.Experiment(prefix="e05_report", cmd_prefix=["uv", "run", "python"])
train_a = e.Stage("train_a", cmd=lambda: [SCRIPT, "--stage=a"])
train_b = e.Stage("train_b", cmd=lambda: [SCRIPT, "--stage=b"])

e.report = "spearmint/examples/toy_report_fn.py:make_report"  # hot-reloaded on file change

if __name__ == "__main__":
    e.main()
