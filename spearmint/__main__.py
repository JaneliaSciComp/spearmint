"""Unified spearmint CLI: ``spearmint <command> [args]`` (installed console script) or the
equivalent ``python -m spearmint <command> [args]``.

The CLI is the viewing surface only -- a status table and a browser over run ledgers / results
directories. Running experiments is a library affair: an experiment file imports spearmint,
builds an Experiment/Stages, and ends in ``e.main()`` -- run it directly (locally) or with
--submit to become an LSF driver job (see README + examples/).

A thin dispatcher -- each command just re-runs the corresponding ``python -m spearmint.<module>``
entrypoint (with argv rewritten), so every command keeps its OWN -h/--help and usage handling
(see spearmint._cli).
"""

import runpy
import sys

# friendly command -> the module whose __main__ implements it
COMMANDS = {
    "status": "spearmint.report",        # one-shot terminal status table over a run ledger
    "browse": "spearmint.dashboard",     # browser UI: ledger dashboard, or any results dir
}

_USAGE = (
    "usage: spearmint <command> [args]    (`spearmint <command> -h` for per-command help)\n\n"
    "commands:\n"
    "  status [dir]                              terminal status table over a run ledger\n"
    "  browse [dir] [--port N] [--no-browser] [--takeover]    browser UI: the ledger dashboard when dir\n"
    "                                            holds a rundb.db, else a results-dir browser\n\n"
    "dir defaults to <git root of cwd>/output_rundb. Running experiments is library API, not\n"
    "a CLI verb -- see the README and spearmint/examples/.\n"
)


def main() -> None:
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        print(_USAGE)
        raise SystemExit(0 if args else 2)  # bare `spearmint` is a usage error; -h is not
    cmd, rest = args[0], args[1:]
    if cmd not in COMMANDS:
        print(f"error: unknown command {cmd!r}\n\n{_USAGE}", file=sys.stderr)
        raise SystemExit(2)
    # Re-dispatch to the module's own __main__ with argv rewritten -- it does its own arg/-h
    # handling on this argv (help_if_asked reads sys.argv[1:] = rest).
    sys.argv = [f"spearmint {cmd}", *rest]
    runpy.run_module(COMMANDS[cmd], run_name="__main__", alter_sys=True)


if __name__ == "__main__":
    main()
