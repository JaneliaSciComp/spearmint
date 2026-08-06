"""A single shared Stage, importable by multiple experiment files that all depend on the same
upstream work (e.g. a common preprocessing step) -- this is the correct way to share a stage
across dagrunner.py DAGs: import and reuse this exact object in your own ``req=[...]``. Don't
build a second Stage with a matching prefix/name -- dagrunner.Experiment.Stage now asserts
loudly if two different Stage objects ever land on the same job_key (see toy_dag_a.py /
toy_dag_b.py for the intended pattern: both list ``preprocess`` in their own ``req=``, but it's
the same object, not two coincidentally-matching ones)."""

import spearmint as O

_shared = O.Experiment(prefix="shared", cmd_prefix=["uv", "run", "python"])
preprocess = _shared.Stage("preprocess", cmd=lambda: ["spearmint/examples/script.py"])
