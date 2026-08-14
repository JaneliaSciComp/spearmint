#!/usr/bin/env python
"""aio bakeoff: the case the DAG can't express -- a validation sidecar that runs WHILE
training runs. No watch=/stop=/on_fail= vocabulary: the trainer's live dir is handed to the
watcher at submit time (row minted synchronously for dep-free jobs, so ``train.outdir`` is
usable immediately), "stop when train stops" is try/finally + cancel(), and the cancelled
watcher still resolves as done (its validations stand), so summary can depend on it.

    uv run python spearmint/examples/toy_aio_sidecar.py
    uv run python spearmint/examples/toy_aio_sidecar.py --new train   # rerun; watcher+summary cascade

Every policy the sidecar design needed a knob for is a line of code here:
  stop="any"        -> asyncio.wait([...], return_when=FIRST_COMPLETED) then cancel()
  on_fail="restart" -> while True: try: await val; break
                       except aio.JobFailed: val = ctx.submit(f"val{i}", ...)
  on_fail="abandon" -> just let the JobFailed propagate through the awaits
"""

from spearmint import aio

SCRIPT = "spearmint/examples/script.py"
WATCHER = "spearmint/examples/watcher.py"


async def main(ctx: aio.Ctx) -> None:
    train = ctx.submit("train", [SCRIPT, "--stage=a"])
    val = ctx.submit("val", [WATCHER, "--watch", train.outdir])
    try:
        await train
    finally:
        val.cancel()  # stop when train stops (success OR failure -- finally is the semantics)
    await val         # resolves 'done': the validations it made stand
    await ctx.submit("summary", [SCRIPT, "--stage=summary"], deps=(val,))


if __name__ == "__main__":
    aio.main(main, prefix="aio_sidecar", cmd_prefix=["uv", "run", "python"])
