"""Pull the cluster's spearmint ledger + run artifacts down to this machine.

Delta sync via oc-rsync (rsync-compatible, installed user-level on BOTH ends -- the cluster has
no system rsync): repeat pulls transfer only changed bytes and propagate remote deletions
(--delete), keeping the local rundb.ROOT an in-place authoritative mirror of the cluster's. Do
local-only spearmint work under a different $SPEARMINT_ROOT if you want to keep it; an
interrupted pull is just rerun.

The live sqlite db is EXCLUDED from the sync (copying a db mid-write can produce a torn copy);
the remote side first writes a consistent snapshot via sqlite's backup API, which syncs along
with everything else and is then copied over the local rundb.db. Outdirs are stored
ROOT-relative in the db, so the pulled ledger resolves against the local ROOT with no path
mapping.

    uv run python -m spearmint.remote       # full mirror (ledger + artifacts)
    uv run python -m spearmint.remote db    # ledger only -- the ~1s status path (see report)
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

from . import rundb
from .config import CONFIG

REMOTE = CONFIG.remote_host
REMOTE_REPO = CONFIG.remote_repo  # spearmint's OWN cluster dir, relative to $HOME -- deliberately
# separate from a project's live run tree so a spearmint push (rsync --delete) can never touch it.
# One-time setup: rsync the code across, then uv sync + editable install (see spearmint.launch).
REMOTE_ROOT = CONFIG.remote_root  # relative to REMOTE_REPO (rundb anchors at the repo root)
SSH_OPTS = [
    "-o", "ConnectTimeout=30",
    "-o", "ControlMaster=auto",
    "-o", f"ControlPath={CONFIG.ssh_control_path}",
    "-o", "ControlPersist=120s",
]
# Local client binary: config override, else PATH, else the mise install location (.zshrc PATH
# additions aren't seen by non-interactive shells). Remote side is resolved by --rsync-path and
# must be on the remote's non-interactive PATH.
RSYNC = CONFIG.rsync_bin or shutil.which("oc-rsync") or os.path.expanduser(
    "~/.local/share/mise/installs/ubi-oferchen-rsync/latest/oc-rsync"
)
REMOTE_RSYNC = CONFIG.remote_rsync
# Big/regenerable files stay on the cluster; the live db + journals are excluded in favor of
# the consistent snapshot. Excluded files are also protected from --delete on the local side.
EXCLUDES = ("*.pth", "*.ckpt", "*.zarr", "wandb", "rundb.db", "rundb.db-journal", "rundb.db-wal")
# push (code up) excludes: VCS metadata (the cluster tree is NOT a checkout -- launch threads
# provenance via env instead), the platform-specific venv (the cluster keeps its own), and all
# generated output. Protected from --delete too, so a push never touches the cluster's venv or
# ledger. .spearmint_diff is the launch-shipped diff file (delivered separately, not clobbered).
CODE_EXCLUDES = (
    ".git", ".jj", ".venv", "*.egg-info", "output", "output_rundb", "outputs", "logs",
    "__pycache__", "*.pyc", "wandb", ".spearmint_diff", ".DS_Store",
)

_SNAPSHOT = "rundb.snapshot.db"
_BACKUP_PY = (
    "import sqlite3; "
    f"s = sqlite3.connect('{REMOTE_ROOT}/rundb.db'); "
    f"d = sqlite3.connect('{REMOTE_ROOT}/{_SNAPSHOT}'); "
    "s.backup(d); d.close(); s.close()"
)


def _require_repo() -> None:
    """Every remote op targets REMOTE_REPO (this project's OWN cluster dir); it has no safe
    default, so refuse loudly rather than let an unconfigured project touch another's dir."""
    assert REMOTE_REPO, (
        "spearmint: remote_repo is unset -- set it in spearmint.toml (or SPEARMINT_REMOTE_REPO) to "
        "your project's cluster dir before any pull/push/launch"
    )


def _ssh_argv(remote_cmd: str) -> "list[str]":
    return ["ssh", *SSH_OPTS, REMOTE, remote_cmd]


def _src(path: str) -> str:
    """rsync source spec for a path relative to the remote repo root."""
    return f"{REMOTE}:{REMOTE_REPO}/{path}"


def _dst(path: str) -> str:
    """rsync destination spec for a path relative to the remote repo root."""
    return f"{REMOTE}:{REMOTE_REPO}/{path}"


def _snapshot_remote() -> None:
    """Write a consistent ledger snapshot on the cluster via sqlite's backup API."""
    _require_repo()
    result = subprocess.run(_ssh_argv(f'cd {REMOTE_REPO} && {CONFIG.remote_venv} -c "{_BACKUP_PY}"'))
    assert result.returncode == 0, f"remote db snapshot failed (rc={result.returncode})"


def _rsync(args: "list[str]") -> None:
    cmd = [RSYNC, f"--rsync-path={REMOTE_RSYNC}", "-e", "ssh " + " ".join(SSH_OPTS), *args]
    result = subprocess.run(cmd)
    assert result.returncode == 0, f"rsync failed (rc={result.returncode}): {' '.join(cmd)}"


def _install_snapshot() -> None:
    snapshot = Path(rundb.ROOT) / _SNAPSHOT
    assert snapshot.exists(), f"pull produced no db snapshot at {snapshot}"
    shutil.copyfile(snapshot, Path(rundb.ROOT) / "rundb.db")


def pull() -> str:
    """Mirror the cluster's {REMOTE_ROOT} into the local rundb.ROOT (in place, deletions
    propagated -- see module docstring) and return the local ROOT path."""
    _snapshot_remote()
    Path(rundb.ROOT).mkdir(parents=True, exist_ok=True)
    excludes = [f"--exclude={e}" for e in EXCLUDES]
    _rsync(["-a", "--delete", *excludes, _src(f"{REMOTE_ROOT}/"), rundb.ROOT])
    _install_snapshot()
    print(f"pulled {REMOTE}:{REMOTE_REPO}/{REMOTE_ROOT} -> {rundb.ROOT}", flush=True)
    return rundb.ROOT


def pull_db() -> str:
    """Ledger-only fast path: fresh status in ~a second (the snapshot is tiny and repeat pulls
    reuse the ControlMaster connection) without touching the artifact tree. What
    ``spearmint.report --remote`` uses."""
    _snapshot_remote()
    Path(rundb.ROOT).mkdir(parents=True, exist_ok=True)
    _rsync([_src(f"{REMOTE_ROOT}/{_SNAPSHOT}"), f"{rundb.ROOT}/"])
    _install_snapshot()
    return rundb.ROOT


_DIFF_HOME_NAME = "spearmint_launch.diff"  # lives in the cluster HOME (repo root may be read-only)


def push() -> None:
    """rsync the local working tree UP to the cluster code dir (in place, --delete). The cluster
    tree becomes a plain copy of your working files -- not a git/jj checkout -- so there's no
    on-cluster VCS to snapshot generated files into (the brittleness the git-branch flow had).
    Never touches the cluster's venv, ledger, or outputs (CODE_EXCLUDES protects them from copy
    AND delete)."""
    repo_root = rundb._repo_root()
    excludes = [f"--exclude={e}" for e in CODE_EXCLUDES]
    _rsync(["-a", "--delete", *excludes, f"{repo_root}/", _dst("")])


def ship_diff(local_diff: str) -> str:
    """Ship launch's captured working-copy diff to the cluster HOME (the repo root often isn't
    writable for new files there) and return the ~-relative path the driver's provenance env
    should point at (rundb._provenance expanduser's it)."""
    _rsync([local_diff, f"{REMOTE}:{_DIFF_HOME_NAME}"])
    return f"~/{_DIFF_HOME_NAME}"


if __name__ == "__main__":
    from . import _cli

    _cli.help_if_asked(__doc__)
    if not (len(sys.argv) == 1 or sys.argv[1:] == ["db"]):
        _cli.usage_error(__doc__, f"unexpected args {sys.argv[1:]} (expected nothing, or 'db')")
    pull_db() if sys.argv[1:] == ["db"] else pull()
