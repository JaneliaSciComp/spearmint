#!/usr/bin/env python
"""e04 -- minimal DAG demo for dagrunner.py + rundb.py.

Three toy stages (root -> mid -> leaf), each just script.py writing one file -- no LSF/cluster,
no GPU, runs entirely locally. Demonstrates the two things phase 2 changes: job_key-tagged
skip-if-done (run this twice; the second time every stage skips), and downstream dependency-path
resolution via an upstream Stage's own ``.savedir`` instead of a path fixed at build() time.

    uv run python spearmint/examples/toy_dag_demo.py   # run it
    uv run python spearmint/examples/toy_dag_demo.py   # run again -- everything skips
"""

import spearmint as O

e = O.Experiment(prefix="e04_toy_dag", cmd_prefix=["uv", "run", "python"])

root = e.Stage("root", cmd=lambda: ["spearmint/examples/script.py", "--stage=root"])
mid = e.Stage(
    "mid",
    cmd=lambda: ["spearmint/examples/script.py", "--upstream", root.savedir, "--stage=mid"],
    req=[root],
)
leaf = e.Stage(
    "leaf",
    cmd=lambda: ["spearmint/examples/script.py", "--upstream", mid.savedir, "--stage=leaf"],
    req=[mid],
)


if __name__ == "__main__":
    print(e.run())
