# spearmint

Reproducible, queryable experiment runs on an LSF cluster — with no framework buy-in.

Spearmint gives every run a fresh output dir and a row in a local sqlite ledger recording its
argv, git commit, git diff, and (for DAG stages) the exact upstream run_ids it consumed. On top of
the ledger sits a small DAG scheduler that runs stages as blocking `bsub -K` LSF jobs (independent
ones concurrently), a one-command laptop→cluster launcher, and terminal/browser status UIs. The
core is **stdlib-only** — the only externals are processes it shells out to (`git`/`jj`/`ssh`/
`bsub`/`bjobs`/`uv`/`oc-rsync`).

## Install

Add it as a git dependency to your project (a jj-colocated repo works out of the box):

```toml
# pyproject.toml
dependencies = ["spearmint @ git+ssh://git@github.com/AI-HHMI/spearmint.git@main"]
```

For local development, clone it and install editable:

```bash
git clone git@github.com:AI-HHMI/spearmint.git ~/proj/spearmint
uv pip install -e ~/proj/spearmint
```

## Configure

Drop a `spearmint.toml` at your repo root (or a `[tool.spearmint]` table in your `pyproject.toml`)
naming your cluster dir, bookmark, and LSF project. See `spearmint.toml.example` for every key and
its default; any key is also overridable by an env var `SPEARMINT_<KEY>`. Config is discovered at
the git repo root, so the CLI works without importing your project code.

## Use

```bash
spearmint launch experiments/my_exp.py smoke   # laptop -> push code + submit a driver on the cluster
spearmint report                               # terminal status table (--remote pulls first)
spearmint dashboard                            # browser status UI
spearmint pull                                 # mirror the cluster ledger + artifacts down
```

An experiment file builds a DAG of stages, each a plain command wrapped in an LSF prefix:

```python
import spearmint as sp
from spearmint import lsf

e = sp.Experiment(prefix="my_exp", cmd_prefix=["uv", "run", "python"])
train = e.Stage("train", cmd=lambda: ["train.py"], cmd_prefix=lsf.gpu(walltime="8:00"))
plot  = e.Stage("plot",  cmd=lambda: ["plot.py", "--in", train.savedir], req=[train], cmd_prefix=lsf.cpu())
e.run()
```

See `spearmint/examples/` for runnable toy DAGs (no cluster needed — `python -m
spearmint.examples.toy_dag_demo`) and a fake-`bsub` cluster smoke test.

## Adopting spearmint in a new project

1. Add the git dependency and run `uv sync`.
2. Write a `spearmint.toml` with your `remote_repo` (your own `proj/<project>-spearmint` cluster
   dir), `bookmark`, and `lsf_project`.
3. One-time cluster setup: create that cluster dir as a checkout/copy of your project whose
   `uv sync` pulls spearmint from git, so `import spearmint` resolves there.
4. Write experiment files that `import spearmint` and build `Experiment`/`Stage`s.
