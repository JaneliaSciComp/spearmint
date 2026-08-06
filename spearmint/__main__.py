"""Unified spearmint CLI: ``spearmint <command> [args]`` (installed console script) or the
equivalent ``python -m spearmint <command> [args]``.

A thin dispatcher -- each command just re-runs the corresponding ``python -m spearmint.<module>``
entrypoint (with argv rewritten), so every command keeps its OWN -h/--help and usage handling
(see spearmint._cli): ``spearmint report -h``, ``spearmint pull db``, ``spearmint launch e00.py
smoke --replace slices`` all behave exactly as the module forms do. The per-module ``python -m
spearmint.<module>`` forms keep working too; this is just the friendly front door.
"""

import runpy
import sys

# friendly command -> the module whose __main__ implements it
COMMANDS = {
    "launch": "spearmint.launch",        # from the laptop: push code + submit a driver on the cluster
    "pull": "spearmint.remote",          # mirror the cluster ledger down ('pull db' = db-only, fast)
    "report": "spearmint.report",        # terminal status table ('report --remote' pulls first)
    "dashboard": "spearmint.dashboard",  # browser status UI (--remote / --no-browser)
    "lsf": "spearmint.lsf",              # on the login node: submit the driver job (usually via launch)
}

_USAGE = (
    "usage: spearmint <command> [args]    (`spearmint <command> -h` for per-command help)\n\n"
    "commands:\n"
    "  launch <exp.py> <tier> [--new/--extend/--replace STAGE]   push code + run on the cluster\n"
    "  pull [db]                                                 mirror the cluster ledger down\n"
    "  report [--remote]                                         terminal status table\n"
    "  dashboard [--remote] [--no-browser]                       browser status UI\n"
    "  lsf <exp.py> <tier> [...]                                 (login node) submit the driver\n"
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
