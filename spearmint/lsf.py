"""bsub -K command prefixes for running spearmint stages as LSF jobs, plus the driver kickoff.

A stage becomes an LSF job purely through its command: ``bsub -K <flags> uv run python ...``
blocks until the job finishes and exits with the job's own exit code, so dagrunner's scheduler
needs no changes at all -- MAX_PARALLEL simply caps in-flight LSF jobs. bsub treats everything
after its flags as the command argv, so dagrunner appending --job-key/--extend/--replace at the
END still works. Constraint: stage argv must stay free of shell metacharacters (LSF re-joins the
argv through a shell on the compute node) -- spearmint payloads are plain
``uv run python script.py ...``, which is fine. Jobs inherit the submission cwd and environment,
so submit from the repo root.

Usage in an experiment file:

    e = spearmint.Experiment(prefix="e05", cmd_prefix=["uv", "run", "python"])
    train = e.Stage("train", cmd=..., cmd_prefix=lsf.gpu(walltime="8:00"))
    plot = e.Stage("plot", cmd=..., req=[train], cmd_prefix=lsf.cpu())

Kick off from the login node, from your repo root (a real checkout -- rundb reads provenance
from its git HEAD). Long processes are forbidden on login nodes, so don't run the experiment
file there directly -- submit it as the driver job (a 7-day CPU job on the `local` queue that
submits the per-stage jobs from inside its own job) via submit_driver:

    python -c "from spearmint import lsf; lsf.submit_driver('experiments/my_exp.py', 'smoke')"
"""

import subprocess
from pathlib import Path
from typing import Callable

from . import rundb
from .config import CONFIG

# Stage outdirs don't exist at submit time (the child mints its own via rundb.run), so bsub -oo
# logs live here instead, keyed by job name and overwritten per attempt.
LOG_DIR = f"{rundb.ROOT}/_lsf_logs"


def _prefix(job_key: str, queue: str, walltime: str, slots: int, gpu: bool) -> "list[str]":
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    name = job_key.replace("/", "_")
    return [
        "bsub", "-K",
        "-J", name,
        "-P", CONFIG.lsf_project,
        "-q", queue,
        "-W", walltime,
        "-n", str(slots),
        "-R", "span[hosts=1]",
        *(["-gpu", "num=1:mode=exclusive_process"] if gpu else []),
        "-oo", f"{LOG_DIR}/{name}.log",
        "uv", "run", "python",
    ]


def gpu(queue: "str | None" = None, walltime: str = "4:00", slots: "int | None" = None) -> "Callable[[str], list[str]]":
    """Stage cmd_prefix for a single-GPU LSF job. queue/slots default to CONFIG.gpu_queue/gpu_slots
    (cluster queue preference: gpu_b300 > gpu_h200 > gpu_h100 > gpu_a100 > gpu_l4)."""
    queue = queue or CONFIG.gpu_queue
    slots = CONFIG.gpu_slots if slots is None else slots
    return lambda job_key: _prefix(job_key, queue, walltime, slots, gpu=True)


def cpu(queue: "str | None" = None, walltime: str = "1:00", slots: int = 1) -> "Callable[[str], list[str]]":
    """Stage cmd_prefix for a small CPU-only LSF job (queue defaults to CONFIG.cpu_queue)."""
    queue = queue or CONFIG.cpu_queue
    return lambda job_key: _prefix(job_key, queue, walltime, slots, gpu=False)


def submit_driver(experiment_file: str, *args: str) -> str:
    """Submit ``experiment_file`` itself as the long-lived driver job (non-blocking bsub onto
    the 7-day `local` queue) and return the LSF job id. Run this on the login node, from the
    repo root -- the driver job inherits that cwd/env and submits the per-stage bsub -K jobs
    from inside its own job. Extra ``args`` are forwarded to the experiment file verbatim (e.g.
    a tier) and become part of the driver's job name + log path, so different tiers coexist."""
    # Args (tier, and any --new/--replace/--extend force flags) go into the driver's job name +
    # log path so runs coexist; strip flag punctuation so the name stays a clean identifier.
    stem = "_".join([Path(experiment_file).stem, *args]).replace("--", "").replace("/", "_")
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    log = f"{LOG_DIR}/{stem}_driver.log"
    cmd = [
        "bsub",
        "-J", f"{stem}_driver",
        "-P", CONFIG.lsf_project,
        "-q", "local",
        "-W", "168:00",
        "-n", "1",
        "-oo", log,
        "uv", "run", "python", experiment_file, *args,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    ack = result.stdout.strip()
    assert result.returncode == 0 and "Job <" in ack, (
        f"driver submission failed (rc={result.returncode}): {ack or result.stderr.strip()}"
    )
    jobid = ack.split("<", 1)[1].split(">", 1)[0]
    print(f"driver submitted: job {jobid} -- follow along with: tail -f {log}", flush=True)
    return jobid
