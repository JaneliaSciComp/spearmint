# spearmint

Reproducible, queryable experiment runs on an LSF cluster — with no framework buy-in.

Spearmint gives every run a fresh output dir and a row in a local sqlite ledger recording its
argv, git commit, git diff, and (for DAG stages) the exact upstream run_ids it consumed. On top
of the ledger sits a small DAG scheduler that runs stages as blocking `bsub -K` LSF jobs
(independent ones concurrently), plus terminal/browser status UIs and a generic results-dir
browser. The core is **stdlib-only** — the only externals are processes it shells out to
(`git`/`bsub`/`bjobs`/`uv`).

Spearmint runs where your code and data live — for cluster work, on the cluster, from a real
git checkout (provenance is read from its HEAD + diff). There is no push/pull/sync machinery:
the browser UIs serve from the machine that owns the ledger, and you connect through an ssh
tunnel.

## Install

Add it as a git dependency to your project:

```toml
# pyproject.toml
dependencies = ["spearmint @ git+ssh://git@github.com/JaneliaSciComp/spearmint.git@main"]
```

For local development, clone it and install editable:

```bash
git clone git@github.com:AI-HHMI/spearmint.git ~/proj/spearmint
uv pip install -e ~/proj/spearmint
```

## Configure

There are no config files and no env vars — configuration is plain Python values, and importing
spearmint has no side effects. Two knobs exist:

- **Where runs live**: by default, `<repo>/output_rundb`, where `<repo>` is the git root of the
  experiment file itself. To relocate (e.g. onto scratch), define one shared
  `CFG = spearmint.Config(root=...)` in a project module and pass it to every
  `Experiment(..., config=CFG)`.
- **LSF constants** (`lsf.LSF_PROJECT`, `lsf.GPU_QUEUE`, `lsf.GPU_SLOTS`, `lsf.CPU_QUEUE`):
  sensible Janelia defaults; override per stage (`lsf.gpu(queue=...)`) or once in your shared
  module (`lsf.LSF_PROJECT = "..."`).

## Run experiments (library API)

An experiment file builds a DAG of stages, each a plain command wrapped in an LSF prefix:

```python
import spearmint as sp
from spearmint import lsf

e = sp.Experiment(prefix="my_exp", cmd_prefix=["uv", "run", "python"])
train = e.Stage("train", cmd=lambda: ["train.py"], cmd_prefix=lsf.gpu(walltime="8:00"))
plot  = e.Stage("plot",  cmd=lambda: ["plot.py", "--in", train.savedir], req=[train], cmd_prefix=lsf.cpu())
e.run()
```

Running the file *is* running the experiment — locally it's just `python my_exp.py`. On the
cluster, don't run it on the login node directly (long processes are forbidden there); submit it
as the driver job, which then submits the per-stage `bsub -K` jobs from inside its own job:

```bash
python -c "from spearmint import lsf; lsf.submit_driver('experiments/my_exp.py', 'smoke')"
tail -f output_rundb/_lsf_logs/my_exp_smoke_driver.log
```

Worker scripts need no spearmint plumbing beyond `with spearmint.run() as r:` (or none at all —
a hydra/argparse worker gets its run dir injected via the stage's `outdir_args`). See
`spearmint/examples/` for runnable toy DAGs (no cluster needed — `python -m
spearmint.examples.toy_dag_demo`) and a real-LSF smoke test (`examples/cluster_smoke.py`).

One constraint to know: only the driver process writes the ledger — stages it launches never
touch the db (sqlite over a shared filesystem breaks under multi-node writes). Don't wrap your
own independently-`bsub`bed jobs in `spearmint.run()` from many nodes at once; go through the
scheduler, or keep bare runs on a single machine.

## Watch runs + browse results (CLI)

```bash
spearmint status [dir]          # terminal status table over a run ledger
spearmint browse [dir]          # browser UI: if dir holds a rundb.db it's the live dashboard
                                # (status table + per-run pages); otherwise a results-dir
                                # browser (tables+plots, JSON trees, zoomable images)
```

`dir` defaults to `<git root of cwd>/output_rundb`.

The servers bind 127.0.0.1 on the machine they run on. Running them on the cluster, next to the
live ledger, is the intended mode — each prints the exact tunnel command at startup, e.g.:

```bash
# on the laptop:
ssh -N -L 8766:localhost:8766 login1.int.janelia.org   # then open http://127.0.0.1:8766/
```

`spearmint browse` works anywhere — it needs no ledger, no config, not even a git repo.

## Adopting spearmint in a new project

1. Add the git dependency and run `uv sync` (in your cluster checkout too).
2. Write experiment files that `import spearmint` and build `Experiment`/`Stage`s. If the
   defaults don't fit (output location, LSF project/queues), set them once in a shared module.
