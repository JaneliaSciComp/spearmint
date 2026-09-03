# Repository Guidelines

## Project Layout

`spearmint/` contains the Python package. Scheduling and run bookkeeping live in `aio.py`, `dagrunner.py`, and `rundb.py`; LSF support is in `lsf.py`; browser and report code is in `dashboard.py`, `explorer.py`, `report.py`, and `viz.py`. Use `spearmint/examples/` for runnable smoke tests. `sidecar.md` and `rundirs.md` record design decisions.

This is a sole-user, experimental project. APIs and architecture may change freely; favor clear, direct changes over compatibility layers or premature abstractions.

## Development Checks

- `uv pip install -e .` installs the package and `spearmint` command in editable mode.
- `uv run python -m compileall spearmint` checks syntax.
- `uv run python spearmint/examples/toy_dag_demo.py` exercises a local DAG; run it twice to verify skipping.
- `uv run python spearmint/examples/toy_aio_fanout.py` exercises asynchronous fan-out.
- `uv run python -m spearmint --help` smoke-tests the CLI.

Run the smallest relevant example after a change. Cluster examples require real `bsub`/`bjobs` access and are not routine checks.

## Code Style

Target Python 3.11+ and keep the core standard-library-only. Use four spaces, `snake_case` for functions, `CapWords` for classes, and `UPPER_CASE` for constants. Match nearby type annotations and docstrings. Comment invariants and operational constraints, not obvious mechanics. Commands must be argv lists such as `['uv', 'run', 'python']`, not shell strings. No formatter or linter is configured.

## Safety and Generated Data

There is no formal test suite or coverage target. For scheduler or ledger changes, exercise a fresh run, a repeated run, and the relevant force mode (`--new`, `--extend`, or `--replace`). Preserve the key invariants: only the driver writes the SQLite ledger, run directories remain versioned, and failures propagate predictably.

Do not edit or commit `output_rundb/`, `*.egg-info/`, or `__pycache__/`. These are generated. Never delete run data unless explicitly asked. Preserve unrelated working-tree changes.

## Commits

This repository uses Jujutsu (`jj`) for version-control operations. Do not mutate the working copy with Git commands. Use concise, behavior-focused descriptions, optionally scoped: `dashboard: show per-stage attempts` or `lsf: fix resource syntax`. Record non-obvious architectural decisions in a short Markdown note or an explanatory change description.
