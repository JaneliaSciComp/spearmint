"""Tiny CLI helpers shared by spearmint's ``python -m spearmint.<mod>`` entrypoints. Each
module's own docstring doubles as its help text; these honor -h/--help and turn bad usage into
clean help output + the right exit code, instead of pulling in argparse for every trivial
entrypoint or dumping an assertion traceback."""

import sys


def help_if_asked(doc: "str | None") -> None:
    """If -h/--help appears anywhere in argv, print the module docstring and exit 0."""
    if any(a in ("-h", "--help") for a in sys.argv[1:]):
        print((doc or "").strip())
        raise SystemExit(0)


def usage_error(doc: "str | None", msg: str) -> None:
    """Print ``msg`` + the module docstring to stderr and exit 2 (bad usage)."""
    print(f"error: {msg}\n", file=sys.stderr)
    print((doc or "").strip(), file=sys.stderr)
    raise SystemExit(2)
