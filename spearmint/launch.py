"""One-command experiment launch: from the laptop, straight to a running driver on the cluster.

    uv run python -m spearmint.launch spearmint/experiments/e00_flyem_mae_vs_lejepa.py smoke

Replaces the old commit -> jj git push -> ssh -> jj git fetch -> jj new -> run dance (and its
.gitignore/auto-snapshot brittleness, which came from running jj on the cluster at all). Here
the cluster code directory is a plain rsync target -- no VCS runs there, so nothing snapshots
generated files.

Steps, in order:
  1. Capture provenance: the base commit (git HEAD == jj @-, the last described commit) and the
     working-copy diff (git diff HEAD == everything in jj @ on top of @-).
  2. Push that base commit to origin (move the BOOKMARK to @- and `jj git push`) so a recorded
     run is always durably reconstructable -- base on origin + the stored diff. @- is described,
     so this never hits jj's "won't push an undescribed commit" rule (@ itself might be empty).
  3. rsync the working tree up (remote.push) + ship the diff file alongside.
  4. ssh-run submit_driver on the login node, threading (commit, diff-file path) as the driver's
     provenance env so its managed-row inserts record the laptop's exact code state -- NOT the
     cluster tree's meaningless git HEAD.

Provenance is therefore identical to the git-branch flow: every run records (base commit on
origin, full diff), reproducible later regardless of transport.
"""

import subprocess
import sys
import tempfile

from . import remote
from . import rundb
from .config import CONFIG

BOOKMARK = CONFIG.bookmark  # the origin bookmark the base commit is pushed to


def _push_base() -> None:
    """Point BOOKMARK at the base commit (@-) and push it to origin -- durable provenance."""
    subprocess.run(["jj", "bookmark", "set", BOOKMARK, "-r", "@-"], check=True)
    subprocess.run(["jj", "git", "push", "--bookmark", BOOKMARK], check=True)


def launch(experiment_file: str, *args: str) -> str:
    """Push code + provenance to the cluster and submit the driver; return its LSF job id."""
    commit = rundb._git("rev-parse", "HEAD")  # jj @- (base, described)
    diff = rundb._git("diff", "HEAD")  # jj @ on top of @- (the working-copy changes)

    _push_base()
    remote.push()

    # Only ship a diff when there actually are uncommitted changes (jj @ on top of @-); the
    # common "committed, then launched" case has an empty diff and needs no file. rundb records
    # an empty diff when SPEARMINT_DIFF_FILE is unset.
    env = f"SPEARMINT_COMMIT={commit}"
    if diff.strip():
        with tempfile.NamedTemporaryFile("w", suffix=".diff", delete=False) as f:
            f.write(diff)
            diff_local = f.name
        env += f" SPEARMINT_DIFF_FILE={remote.ship_diff(diff_local)}"

    # Kick the existing login-node entrypoint (spearmint.lsf, which bsubs the driver) over ssh,
    # with provenance set in the remote shell env -- lsf.__main__ reads it and threads it onto
    # the driver, so managed-row inserts record the laptop's git state, not the cluster tree's.
    forwarded = " ".join([experiment_file, *args])
    remote_cmd = f"cd {remote.REMOTE_REPO} && {env} uv run python -m spearmint.lsf {forwarded}"
    result = subprocess.run(remote._ssh_argv(remote_cmd), capture_output=True, text=True)
    print(result.stdout, end="", flush=True)
    assert result.returncode == 0, f"remote submit failed: {result.stderr.strip()}"
    return result.stdout.strip()


if __name__ == "__main__":
    from . import _cli

    _cli.help_if_asked(__doc__)
    if len(sys.argv) < 2:
        _cli.usage_error(__doc__, "need an experiment file")
    launch(sys.argv[1], *sys.argv[2:])
