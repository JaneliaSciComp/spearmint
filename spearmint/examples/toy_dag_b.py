#!/usr/bin/env python
"""Demo B: same shared preprocess stage as toy_dag_a.py, own separate downstream stage. Run
toy_dag_a.py FIRST -- this file deliberately does NOT run preprocess itself (it only requires
it), so if preprocess hasn't already completed, run_experiment's external-dependency check
fails loudly instead of silently doing the wrong thing.

    uv run python spearmint/examples/toy_dag_a.py   # first
    uv run python spearmint/examples/toy_dag_b.py   # then this -- preprocess is NOT re-run
"""

import spearmint as O
from shared_preprocess import preprocess

e = O.Experiment(prefix="e_b", cmd_prefix=["uv", "run", "python"])
own = e.Stage(
    "own_stage",
    cmd=lambda: ["spearmint/examples/script.py", "--upstream", preprocess.savedir],
    req=[preprocess],
)

if __name__ == "__main__":
    print(e.run())
