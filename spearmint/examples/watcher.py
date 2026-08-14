#!/usr/bin/env python
"""Toy validation sidecar worker: polls a training run's growing metrics.jsonl and writes a
derived val.jsonl into its OWN run dir, forever -- it never exits on its own; whoever launched
it stops it when the watched work is done (see toy_aio_sidecar.py). ``--fail-after N`` exits 1
after N rows, for exercising failure handling. A plain argv contract (--watch DIR): the live
dir is handed in by the experiment code, no discovery.

    SPEARMINT_JOB_KEY=demo/val uv run python spearmint/examples/watcher.py --watch <dir>
"""

import argparse
import json
import time
from pathlib import Path

from spearmint import rundb


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--watch", required=True, help="the watched run's LIVE outdir")
    p.add_argument("--fail-after", type=int, default=None, metavar="N")
    args = p.parse_args()

    with rundb.run() as r:
        print(f"[{r.job_key}] watching {args.watch}", flush=True)
        seen = 0
        out = Path(r.outdir) / "val.jsonl"
        while True:  # stopped by whoever launched us (SIGTERM/bkill)
            metrics = Path(args.watch) / "metrics.jsonl"
            rows = [json.loads(ln) for ln in metrics.read_text().splitlines()] if metrics.exists() else []
            if len(rows) > seen:
                with out.open("a") as f:
                    for row in rows[seen:]:  # a fake "expensive validation" of each new step
                        f.write(json.dumps({"step": row["step"], "val_metric": row["loss"] * 1.1}) + "\n")
                seen = len(rows)
                print(f"[{r.job_key}] validated through step {seen - 1}", flush=True)
            if args.fail_after is not None and seen >= args.fail_after:
                raise RuntimeError(f"--fail-after {args.fail_after}")
            time.sleep(0.5)


if __name__ == "__main__":
    main()
