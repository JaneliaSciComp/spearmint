#!/usr/bin/env python
"""Cluster smoke test -- the gating first real-LSF run for spearmint.

Eight independent tiny CPU stages (blocking ``bsub -K`` on the `local` queue) all write the
shared-filesystem sqlite ledger concurrently -- exactly the risk this smoke exists to probe --
plus a join stage whose provenance should record all eight input run_ids. The driver prints a
db integrity check, the join's recorded inputs, and the status report when the DAG finishes, so
the driver log is the whole verdict.

Run on the login node, from the repo root (a real checkout with spearmint installed, so
provenance and ``import spearmint`` both resolve):

    python -c "from spearmint import lsf; lsf.submit_driver('spearmint/examples/cluster_smoke.py')"
    tail -f output_rundb/_lsf_logs/cluster_smoke_driver.log
"""

import sqlite3

import spearmint as O
from spearmint import lsf, report, rundb

e = O.Experiment(prefix="smoke", cmd_prefix=["uv", "run", "python"])

workers = [
    e.Stage(f"w{i}", cmd=lambda: ["spearmint/examples/script.py"], cmd_prefix=lsf.cpu())
    for i in range(8)
]
join = e.Stage(
    "join",
    cmd=lambda: ["spearmint/examples/script.py", "--upstream", workers[0].savedir],
    req=list(workers),
    cmd_prefix=lsf.cpu(),
)

if __name__ == "__main__":
    print(e.run())
    conn = sqlite3.connect(rundb._db_path())
    print("integrity_check:", conn.execute("PRAGMA integrity_check").fetchone()[0])
    print("join inputs:", conn.execute(
        "SELECT inputs FROM runs WHERE job_key = 'smoke/join' ORDER BY run_id DESC LIMIT 1"
    ).fetchone()[0])
    conn.close()
    print(report.render(report.collect()))
