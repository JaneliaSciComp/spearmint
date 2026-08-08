#!/usr/bin/env python
"""Fan-out/fan-in demo: N independent stages generated in a loop, one join stage requiring all
of them. The workers run CONCURRENTLY (up to dagrunner.MAX_PARALLEL -- watch the [run] lines
all appear before the first ok); the join launches only once every worker is done. Its command
carries NO upstream paths: a managed run's dependencies' resolved outdirs arrive as
``r.inputs`` ($SPEARMINT_INPUTS, requires order), and its ledger row records all N worker
run_ids as exact provenance. (A worker wanting explicit paths can still take them via its own
flags built from ``w.savedir`` -- see toy_dag_demo.py.)

    uv run python spearmint/examples/toy_fanout.py   # run it
    uv run python spearmint/examples/toy_fanout.py   # run again -- everything skips
"""

import spearmint as O

SCRIPT = "spearmint/examples/script.py"
N = 4

e = O.Experiment(prefix="toy_fanout", cmd_prefix=["uv", "run", "python"])

# Loop-generated independent stages, named by index. NB the ``i=i`` default-arg capture: a bare
# ``lambda: [SCRIPT, f"--stage=w{i}"]`` would late-bind i and every worker would see i=N-1.
workers = [e.Stage(f"w{i}", cmd=lambda i=i: [SCRIPT, f"--stage=w{i}"]) for i in range(N)]

# Fan-in: req= all workers; the worker reads their outdirs from r.inputs, so the command
# stays this short no matter how large N gets.
join = e.Stage("join", cmd=lambda: [SCRIPT, "--stage=join"], req=list(workers))

if __name__ == "__main__":
    print(e.run())
