#!/usr/bin/env python
"""aio bakeoff mirror of toy_fanout.py: N loop-submitted independent workers, a join awaiting
all of them (their outdirs arrive via r.inputs), skip-if-done on re-run, force flags with
globs -- same ledger rows, same status/browse rendering, expressed as an async program.

    uv run python spearmint/examples/toy_aio_fanout.py                # run it
    uv run python spearmint/examples/toy_aio_fanout.py                # run again -- all skip
    uv run python spearmint/examples/toy_aio_fanout.py --new 'w*'     # force workers (+ join, via cascade)
"""

from spearmint import aio

SCRIPT = "spearmint/examples/script.py"
N = 4


async def main(ctx: aio.Ctx) -> None:
    workers = [ctx.submit(f"w{i}", [SCRIPT, f"--stage=w{i}"]) for i in range(N)]
    await ctx.submit("join", [SCRIPT, "--stage=join"], deps=workers)


if __name__ == "__main__":
    aio.main(main, prefix="aio_fanout", cmd_prefix=["uv", "run", "python"])
