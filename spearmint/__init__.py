"""Spearmint: reproducible, queryable runs (rundb) plus a minimal DAG scheduler over them
(dagrunner) -- for organizing spearmints.

The flattened names below are the public API most callers need: a worker script wraps its work
in ``with spearmint.run() as r:``, an experiment file builds ``spearmint.Experiment(...)`` /
``.Stage(...)`` and calls ``.run()``. Everything else (query helpers, gc, staleness) lives on
the ``spearmint.rundb`` / ``spearmint.dagrunner`` submodules. See examples/.
"""

from . import rundb
from . import dagrunner
from .rundb import run
from .dagrunner import Experiment, Stage, closure, run_experiment
