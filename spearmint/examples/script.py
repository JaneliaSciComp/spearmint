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
import json
import random
from pathlib import Path

from spearmint import rundb


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--upstream", nargs="*", default=None, help="upstream stages' already-resolved outdirs")
    p.add_argument("--fail", action="store_true", help="raise after writing, to test abandon-on-failure")
    p.add_argument("--stage", help="stage name")
    args = p.parse_args()

    with rundb.run() as r:
        print(f"[{r.job_key}] run_id={r.run_id} writing to {r.outdir}", flush=True)  # stdout is a pipe -> block-buffered
        import time

        # Explicit --upstream wins; else r.inputs -- the deps' outdirs a managed run gets for
        # free ($SPEARMINT_INPUTS), so a fan-in stage needs no argv plumbing (see toy_fanout.py).
        upstreams = args.upstream if args.upstream is not None else r.inputs
        text = "".join(f"upstream={u}\n" for u in upstreams) or "no upstream\n"
        (Path(r.outdir) / "result.txt").write_text(text)
        # Fake training curves, deterministic per job_key, appended at 4 Hz (one complete,
        # flushed JSONL row every 0.25s). Eighty points keep the worker alive for ~20s so the
        # live dashboard's refresh behavior is easy to inspect.
        rng = random.Random(r.job_key)
        base = rng.uniform(1.0, 2.0)
        rows = []
        with open(Path(r.outdir) / "metrics.jsonl", "w") as f:
            for i in range(80):
                rows.append({"step": i, "loss": base * 0.90**i + rng.uniform(0, 0.02),
                             "val_loss": base * 0.92**i + rng.uniform(0, 0.05)})
                f.write(json.dumps(rows[-1]) + "\n")
                f.flush()
                time.sleep(0.25)
        (Path(r.outdir) / "summary.json").write_text(json.dumps(
            {"final_loss": rows[-1]["loss"],
             "best_val_loss": min(row["val_loss"] for row in rows), "n_steps": len(rows)}
        ))
        if args.fail:
            raise RuntimeError("--fail was passed")


if __name__ == "__main__":
    main()
