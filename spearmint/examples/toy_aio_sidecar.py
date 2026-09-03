#!/usr/bin/env python
"""aio bakeoff: the cases the DAG can't express, as ordinary code. Two lifecycle patterns:

1. A validation SIDECAR running WHILE training runs: the trainer's live dir is handed to the
   watcher at submit time (rows for dep-free jobs are minted synchronously, so
   ``train.outdir`` is usable immediately), "stop when train stops" is try/finally +
   cancel(), and the cancelled watcher still resolves done (its validations stand), so
   summary can depend on it. Freshness WITHOUT wait-coupling: ``force=None if train.skipped
   else "new"`` -- deps= would serialize them, which is exactly wrong.
2. A continuously re-rendered REPORT: an in-driver task looping render+sleep -- the same
   sidecar shape minus the subprocess. The raw pattern owns its own isolation (the try/except
   in the loop); the DAG layer's ``e.report`` is sugar bundling exactly that plus the output
   convention and triggers.

    uv run python spearmint/examples/toy_aio_sidecar.py
    uv run python spearmint/examples/toy_aio_sidecar.py --new train   # rerun; val+summary cascade
    spearmint browse   # aio_sidecar 📈 -> the live report

Every policy the sidecar design needed a knob for is a line of code here:
  stop="any"        -> asyncio.wait([...], return_when=FIRST_COMPLETED) then cancel()
  on_fail="restart" -> while True: try: await val; break
                       except aio.JobFailed: val = ctx.submit(f"val{i}", ...)
  on_fail="abandon" -> just let the JobFailed propagate through the awaits
"""

import asyncio
import json
from pathlib import Path

from spearmint import aio, rundb, viz

SCRIPT = "spearmint/examples/script.py"
WATCHER = "spearmint/examples/watcher.py"


async def main(ctx: aio.Ctx) -> None:
    train = ctx.submit("train", [SCRIPT, "--stage=a"])
    val = ctx.submit("val", [WATCHER, "--watch", train.outdir],
                     force=None if train.skipped else "new")

    ## `live=True` makes an open tab poll the html file and update its plots in place
    ## (zoom/pan survive); the final render drops the live marker and the tab goes quiet
    def render(live: bool) -> None:
        curves = {}  # every jsonl in both LIVE dirs, re-read fresh each render
        for job in (train, val):
            for f in Path(job.outdir).glob("*.jsonl"):
                curves[f"{job.name}:{f.stem}"] = \
                    [json.loads(ln) for ln in f.read_text().splitlines()]
        # This raw aio demo has no managed report Stage; keep its ad-hoc human-only view with
        # the training attempt it describes. Experiment.report provides versioned sidecar
        # lifecycle when that identity/history matters.
        out = Path(train.outdir)
        (out / "report.html").write_text(viz.page(
            viz.lines(curves, x="step", logy=True, title="train + val, live"),
            title=ctx.prefix, refresh=2 if live else None,
        ))

    ## loop so that the html file rerenders to reflect new data
    async def report_loop() -> None:
        while True:
            try:
                render(live=True)
            except Exception as e:  # raw pattern: isolation is YOUR job (e.report bundles it)
                print(f"[report] render failed: {e!r}", flush=True)
            await asyncio.sleep(2)

    rep = asyncio.create_task(report_loop())
    try:
        await train
    finally:
        val.cancel()  # stop when train stops (success OR failure -- finally is the semantics)
    await val         # resolves 'done': the validations it made stand
    await ctx.submit("summary", [SCRIPT, "--stage=summary"], deps=(val,))
    rep.cancel()
    render(live=False)  # final render drops the live marker; the open tab stops polling, zoom intact


if __name__ == "__main__":
    aio.main(main, prefix="aio_sidecar", cmd_prefix=["uv", "run", "python"])
