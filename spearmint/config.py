"""Per-project spearmint configuration.

Every project/cluster constant spearmint used to hardcode (LSF project + queues, the remote host
+ its repo/root/venv, the ssh control path, the origin bookmark, dashboard port) lives here as a
``SpearmintConfig`` field. Values resolve, highest precedence first, from: an env var
``SPEARMINT_<FIELD>``; a ``[tool.spearmint]`` table in a project's ``spearmint.toml`` (or, failing
that, its ``pyproject.toml``) at the git repo root; the built-in default. Loaded once at import
into the module-level ``CONFIG`` -- every other spearmint module reads ``CONFIG.*`` rather than a
literal, so a new project only has to drop in its own ``spearmint.toml``.

This module depends on nothing else in spearmint (rundb and friends import it, not the reverse),
so it can compute ROOT and discover the config file with no import cycle.
"""

import os
import subprocess
import tomllib
from dataclasses import dataclass, fields
from pathlib import Path


def _repo_root() -> "str | None":
    """Absolute path of the enclosing git repo, or None if not in one. Config + ROOT anchor here
    rather than the cwd, so a script run from any subdir hits the same ledger + run tree instead
    of minting a fresh empty one wherever it happened to launch from."""
    result = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else None


@dataclass
class SpearmintConfig:
    # LSF cluster (Janelia; GPU queue preference b300 > h200 > h100 > a100 > l4)
    lsf_project: str = "miaai"
    gpu_queue: str = "gpu_b300"
    gpu_slots: int = 12
    cpu_queue: str = "local"
    # remote (cluster) side -- repo/root/venv are relative to $HOME on remote_host
    remote_host: str = "login1.int.janelia.org"
    remote_repo: "str | None" = None  # per-project cluster dir -- REQUIRED (spearmint.toml/env) to
    #                                   run any remote op; asserted at use so a project can't
    #                                   silently push to another's dir (see remote._require_repo)
    remote_root: str = "output_rundb"
    remote_venv: str = ".venv/bin/python"
    remote_rsync: str = "oc-rsync"
    ssh_control_path: str = "~/.ssh/cm-spearmint-%r@%h:%p"
    # local side
    rsync_bin: "str | None" = None  # None -> shutil.which('oc-rsync') or the mise install path
    root: "str | None" = None       # None -> $SPEARMINT_ROOT (env), else <repo root>/output_rundb
    # launch / VCS (jj)
    bookmark: "str | None" = None  # origin bookmark launch pushes to -- REQUIRED (asserted in launch)
    # dashboard
    port: int = 8766
    refresh_seconds: int = 10
    # dagrunner
    max_parallel: int = 32


def _toml_overrides(repo_root: str) -> dict:
    """[tool.spearmint] overrides discovered at the repo root: a standalone spearmint.toml wins
    over a [tool.spearmint] table in pyproject.toml; {} if neither is present."""
    sm = Path(repo_root) / "spearmint.toml"
    if sm.exists():
        with open(sm, "rb") as f:
            return tomllib.load(f)
    pp = Path(repo_root) / "pyproject.toml"
    if pp.exists():
        with open(pp, "rb") as f:
            return tomllib.load(f).get("tool", {}).get("spearmint", {})
    return {}


def _coerce(raw: str, default) -> object:
    """Env vars arrive as strings; coerce to the field's declared type (int fields only)."""
    return int(raw) if isinstance(default, int) and not isinstance(default, bool) else raw


def _load() -> SpearmintConfig:
    """Build CONFIG from env > [tool.spearmint] > defaults, then resolve root (None ->
    <repo root>/output_rundb, the env case having already been applied via SPEARMINT_ROOT)."""
    repo_root = _repo_root()
    overrides = _toml_overrides(repo_root) if repo_root else {}
    values: dict = {}
    for fld in fields(SpearmintConfig):
        env = os.environ.get(f"SPEARMINT_{fld.name.upper()}")
        if env is not None:
            values[fld.name] = _coerce(env, fld.default)
        elif fld.name in overrides:
            values[fld.name] = overrides[fld.name]
    cfg = SpearmintConfig(**values)
    if cfg.root is None:
        assert repo_root is not None, (
            "not inside a git repo and no SPEARMINT_ROOT set -- nowhere to put run outputs"
        )
        cfg.root = f"{repo_root}/output_rundb"
    return cfg


CONFIG = _load()
