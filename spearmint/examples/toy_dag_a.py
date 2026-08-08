#!/usr/bin/env python
"""Demo A: builds the shared preprocess stage (shared_preprocess.py) as part of its own pass,
then its own stage depending on it. Run this FIRST, then toy_dag_b.py -- b imports the exact
same preprocess Stage object and sees (via rundb.db, not via anything a and b share at runtime)
that it's already done, so it never re-runs it.

    uv run python spearmint/examples/toy_dag_a.py
"""

import spearmint as O
from shared_preprocess import preprocess

e = O.Experiment(prefix="e_a", cmd_prefix=["uv run python"])
own = e.Stage(
    "own_stage",
    cmd=lambda: ["spearmint/examples/script.py", "--upstream", preprocess.savedir],
    req=[preprocess],
)

if __name__ == "__main__":
    # preprocess isn't in e.stages (it belongs to shared_preprocess's own Experiment), so a bare
    # e.run() would only VERIFY it's done. closure() pulls such external requires into the same
    # scheduling pass, so it's built here (or skipped if already done) with topo-ordering and
    # abandon-on-failure spanning the boundary -- one unified DAG, not two disconnected passes.
    print(O.run_experiment(O.closure(e.stages)))
