"""Spearmint: reproducible, queryable runs (rundb) plus a minimal DAG scheduler over them
(dagrunner) -- for organizing spearmints.

The flattened names below are the public API most callers need: a worker script wraps its work
in ``with spearmint.run() as r:``, an experiment file builds ``spearmint.Experiment(...)`` /
``.Stage(...)`` (optionally with a shared ``spearmint.Config``) and calls ``.run()``.
Everything else (query helpers, gc, staleness) lives on the ``spearmint.rundb`` /
``spearmint.dagrunner`` submodules. See examples/.

Importing spearmint has no side effects -- no config files, no env vars, no git: the ledger
location is anchored at first use (see rundb.anchor), so these are plain imports."""

from . import dagrunner
from . import rundb
from .dagrunner import Config, Experiment, Stage, closure, run_experiment
from .rundb import run
