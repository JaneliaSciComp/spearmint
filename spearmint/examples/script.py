#!/usr/bin/env python
"""Minimal toy stage worker for dagrunner.py/toy_dag_demo.py.

Records itself in rundb.db via `rundb.run()`, optionally reads an upstream stage's
already-resolved output dir (passed in explicitly by the caller -- see toy_dag_demo.py, which
resolves it via rundb.latest_outdir() before building this command), writes one small file into
its own outdir, and exits 0 -- or raises with --fail, to exercise the abort-on-failure /
abandon-dependents path in dagrunner.run_experiment.

No --job-key/--extend/--replace of its own -- rundb.run() parses those straight out of
sys.argv (see rundb._job_key_from_argv/_mode_from_argv), so this parser only needs to declare
its own actual flags and pass through whatever else it doesn't recognize.

    uv run python spearmint/examples/script.py --job-key demo/root
    uv run python spearmint/examples/script.py --job-key demo/mid --upstream output_rundb/demo/root/run00001
    uv run python spearmint/examples/script.py --job-key demo/root --fail
    uv run python spearmint/examples/script.py --job-key demo/root --extend
"""

import argparse
from pathlib import Path

from spearmint import rundb


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--upstream", default=None, help="an upstream stage's already-resolved outdir")
    p.add_argument("--fail", action="store_true", help="raise after writing, to test abandon-on-failure")
    p.add_argument("--stage", help="stage name")
    args, _ = p.parse_known_args()

    with rundb.run() as r:
        print(f"[{r.job_key}] run_id={r.run_id} writing to {r.outdir}")
        import time

        # time.sleep(3)
        # if args.stage == "root":
        text = f"upstream={args.upstream}\n" if args.upstream else "no upstream\n"

        p = Path(r.outdir) / "result.txt"
        if p.exists():
            p.write_text("we've been here before")
        else:
            time.sleep(3)
            p.write_text(text)
            
        if args.fail:
            raise RuntimeError("--fail was passed")


if __name__ == "__main__":
    main()
