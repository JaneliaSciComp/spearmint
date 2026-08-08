#!/usr/bin/env python
"""Minimal toy stage worker for dagrunner.py/toy_dag_demo.py.

Records itself in rundb.db via `rundb.run()`, optionally reads an upstream stage's
already-resolved output dir (passed in explicitly by the caller -- see toy_dag_demo.py, which
resolves it via rundb.latest_outdir() before building this command), writes one small file into
its own outdir, and exits 0 -- or raises with --fail, to exercise the abort-on-failure /
abandon-dependents path in dagrunner.run_experiment.

No job-key/mode plumbing of its own -- rundb.run() reads $SPEARMINT_JOB_KEY/$SPEARMINT_MODE
(set by dagrunner's ``env`` prefix on the stage command; see rundb._job_key_from_env/
_mode_from_env), so this parser only declares its own actual flags.

    SPEARMINT_JOB_KEY=demo/root uv run python spearmint/examples/script.py
    SPEARMINT_JOB_KEY=demo/mid uv run python spearmint/examples/script.py --upstream output_rundb/demo/root/run00001
    SPEARMINT_JOB_KEY=demo/root uv run python spearmint/examples/script.py --fail
    SPEARMINT_JOB_KEY=demo/root SPEARMINT_MODE=extend uv run python spearmint/examples/script.py
"""

import argparse
from pathlib import Path

from spearmint import rundb


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--upstream", default=None, help="an upstream stage's already-resolved outdir")
    p.add_argument("--fail", action="store_true", help="raise after writing, to test abandon-on-failure")
    p.add_argument("--stage", help="stage name")
    args = p.parse_args()

    with rundb.run() as r:
        print(f"[{r.job_key}] run_id={r.run_id} writing to {r.outdir}")
        import time

        time.sleep(3)  # long enough to watch the DAG progress in the report/dashboard
        text = f"upstream={args.upstream}\n" if args.upstream else "no upstream\n"
        (Path(r.outdir) / "result.txt").write_text(text)
        if args.fail:
            raise RuntimeError("--fail was passed")


if __name__ == "__main__":
    main()
