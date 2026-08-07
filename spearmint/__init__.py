"""Spearmint: reproducible, queryable runs (rundb) plus a minimal DAG scheduler over them
(dagrunner) -- for organizing spearmints.

The flattened names below are the public API most callers need: a worker script wraps its work
in ``with spearmint.run() as r:``, an experiment file builds ``spearmint.Experiment(...)`` /
``.Stage(...)`` and calls ``.run()``. Everything else (query helpers, gc, staleness) lives on
the ``spearmint.rundb`` / ``spearmint.dagrunner`` submodules. See examples/.

Exports resolve lazily (PEP 562): rundb/config anchor ROOT at import and assert outside a git
repo, but ledger-free entrypoints -- spearmint.explorer (``spearmint browse``) serves arbitrary
directories anywhere -- must load without that. Deferring the submodule import to first
attribute access keeps ``spearmint.run`` / ``spearmint.Experiment`` working unchanged."""

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # static view of the lazy exports below, so type checkers resolve them
    from . import dagrunner as dagrunner
    from . import rundb as rundb
    from .dagrunner import Experiment as Experiment, Stage as Stage
    from .dagrunner import closure as closure, run_experiment as run_experiment
    from .rundb import run as run

_LAZY = {  # public name -> submodule defining it (None = the name IS the submodule)
    "rundb": None,
    "dagrunner": None,
    "run": "rundb",
    "Experiment": "dagrunner",
    "Stage": "dagrunner",
    "closure": "dagrunner",
    "run_experiment": "dagrunner",
}


def __getattr__(name: str):
    if name not in _LAZY:  # AttributeError (not an assert): hasattr/dir probes rely on it
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = importlib.import_module(f".{_LAZY[name] or name}", __name__)
    return module if _LAZY[name] is None else getattr(module, name)
