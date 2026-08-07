"""Unified spearmint CLI: ``spearmint <command> [args]`` (installed console script) or the
equivalent ``python -m spearmint <command> [args]``.

The CLI is the viewing surface only -- the dashboard over the run ledger and the file/results
browser. Running experiments is a library affair: an experiment file imports spearmint, builds
an Experiment/Stages, and is executed directly (locally) or submitted as an LSF driver job via
lsf.submit_driver (see README + examples/).

A thin dispatcher -- each command just re-runs the corresponding ``python -m spearmint.<module>``
entrypoint (with argv rewritten), so every command keeps its OWN -h/--help and usage handling
(see spearmint._cli).
"""

import runpy
import sys

# friendly command -> the module whose __main__ implements it
COMMANDS = {
    "status": "spearmint.report",        # one-shot terminal status table over the run ledger
    "dashboard": "spearmint.dashboard",  # browser status UI over the run ledger (--no-browser)
    "browse": "spearmint.explorer",      # serve any results directory in the browser
}

_USAGE = (
    "usage: spearmint <command> [args]    (`spearmint <command> -h` for per-command help)\n\n"
    "commands:\n"
    "  status                                    terminal status table over the run ledger\n"
    "  dashboard [--no-browser]                  browser status UI over the run ledger\n"
    "  browse <dir> [--port N] [--no-browser]    serve a results dir in the browser\n\n"
    "running experiments is library API, not a CLI verb -- see the README and\n"
    "spearmint/examples/ (`python my_exp.py`, or lsf.submit_driver on the cluster).\n"
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
