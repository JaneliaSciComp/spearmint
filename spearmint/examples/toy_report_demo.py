#!/usr/bin/env python
"""Report demo: the report is a STANDALONE SCRIPT (toy_report.py) that builds static HTML
from the ledger. Assigning its path to ``e.report`` makes the driver shell out to it -- at
start, after every stage finalize, every REPORT_TICK_SECONDS while stages run, and once at
the end -- and the same script runs by hand whenever you like. Every render is a fresh
process, so editing the report (structure and all) DURING a run just works.

    uv run python spearmint/examples/toy_report_demo.py --replace 'train_*'
    # while it runs: edit toy_report.py (a title, a new panel) and watch the report change
    # anytime at all: uv run python spearmint/examples/toy_report.py

A plain function ``fn(savedir) -> html`` assigned to ``e.report`` also works for one-file
demos, but is fixed for the run's lifetime; the script is the real pattern.
"""

import spearmint as O

SCRIPT = "spearmint/examples/script.py"

# Toy stages stream metrics for only ~20s; tick fast enough that edits + curves show up
# mid-stage. Real experiments keep the 120s default.
O.dagrunner.REPORT_TICK_SECONDS = 0.5

e = O.Experiment(prefix="e05_report", cmd_prefix=["uv", "run", "python"])
train_a = e.Stage("train_a", cmd=lambda: [SCRIPT, "--stage=a"])
train_b = e.Stage("train_b", cmd=lambda: [SCRIPT, "--stage=b"])

e.report = "spearmint/examples/toy_report.py"

if __name__ == "__main__":
    e.main()
