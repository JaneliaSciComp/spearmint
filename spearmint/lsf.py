"""bsub -K command prefixes for running spearmint stages as LSF jobs, plus the driver kickoff.

A stage becomes an LSF job purely through its command: ``bsub -K <flags> uv run python ...``
blocks until the job finishes and exits with the job's own exit code, so dagrunner's scheduler
needs no changes at all -- MAX_PARALLEL simply caps in-flight LSF jobs. Jobs inherit the
submission cwd and environment -- so submit from the repo root, and the ``env SPEARMINT_*=...``
identity prefix run_experiment wraps around this whole command reaches the worker on the
compute node (the vars ride the submission environment through bsub). Constraint: stage argv
must stay free of shell metacharacters (LSF re-joins the argv through a shell on the compute
node) -- spearmint payloads are plain ``uv run python script.py ...``, which is fine.

Usage in an experiment file:

    e = spearmint.Experiment(prefix="e05", cmd_prefix=["uv", "run", "python"])
    train = e.Stage("train", cmd=..., cmd_prefix=lsf.gpu(walltime="8:00"))
    plot = e.Stage("plot", cmd=..., req=[train], cmd_prefix=lsf.cpu())

Kick off from the login node, from your repo root (a real checkout -- rundb reads provenance
from its git HEAD). Long processes are forbidden on login nodes, so don't run the experiment
file there directly -- submit it as the driver job (a 7-day CPU job on the `local` queue that
submits the per-stage jobs from inside its own job). An experiment file ending in ``e.main()``
does this itself via its --submit flag (Experiment.main calls submit_driver on sys.argv):

    uv run python experiments/my_exp.py smoke --submit

submit_driver is also plain-callable, and is nothing bsub can't do by hand -- it just encodes
the flags worth not retyping (-W 168:00 so the queue's default runtime limit doesn't kill the
driver mid-experiment, the -oo log under _lsf_logs, a job name carrying the args).
"""

import subprocess
from pathlib import Path
from typing import Callable

from . import rundb

# Cluster constants (Janelia defaults). Module-level on purpose: a project needing different
# values sets them once in its own shared module (``lsf.LSF_PROJECT = "..."``) or passes
# queue=/slots= per stage -- no config files.
LSF_PROJECT = "miaai"
GPU_QUEUE = "gpu_b300"  # Janelia GPU queue preference: b300 > h200 > h100 > a100 > l4
GPU_SLOTS = 12
CPU_QUEUE = "local"


def _log_dir() -> str:
    """Stage outdirs don't exist at submit time (the child mints its own via rundb.run), so
    bsub -oo logs live here instead, keyed by job name and overwritten per attempt. Under the
    anchored ledger root, so it resolves lazily -- never at import."""
    return f"{rundb.root()}/_lsf_logs"


def _prefix(job_key: str, queue: str, walltime: str, slots: int, gpus: int,
           exclude_hosts: "list[str] | None" = None) -> "list[str]":
    log_dir = _log_dir()
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    name = job_key.replace("/", "_")
    select = " && ".join(f"hname!='{h}'" for h in (exclude_hosts or []))
    resource_req = f"select[{select}] && span[hosts=1]" if select else "span[hosts=1]"
    return [
        "bsub", "-K",
        "-J", name,
        "-P", LSF_PROJECT,
        "-q", queue,
        "-W", walltime,
        "-n", str(slots),
        "-R", resource_req,
        *(["-gpu", f"num={gpus}:mode=exclusive_process"] if gpus else []),
        "-oo", f"{log_dir}/{name}.log",
        "uv", "run", "python",
    ]


def gpu(queue: "str | None" = None, walltime: str = "4:00", slots: "int | None" = None, gpus: int = 1,
       exclude_hosts: "list[str] | None" = None) -> "Callable[[str], list[str]]":
    """Stage cmd_prefix for a GPU LSF job (queue defaults to GPU_QUEUE, slots to GPU_SLOTS per
    GPU). ``gpus>1`` puts them all on one host (span[hosts=1]) -- single-node DDP territory;
    the worker still has to opt into using them (e.g. Lightning strategy=ddp). ``exclude_hosts``:
    hostnames to steer LSF away from (e.g. a node whose GPUs keep throwing
    cudaErrorDevicesUnavailable across unrelated jobs -- LSF itself doesn't know it's
    unhealthy, so it keeps getting scheduled there on retry without this)."""
    queue = queue or GPU_QUEUE
    slots = GPU_SLOTS * gpus if slots is None else slots
    return lambda job_key: _prefix(job_key, queue, walltime, slots, gpus=gpus, exclude_hosts=exclude_hosts)


def cpu(queue: "str | None" = None, walltime: str = "1:00", slots: int = 1) -> "Callable[[str], list[str]]":
    """Stage cmd_prefix for a small CPU-only LSF job (queue defaults to CPU_QUEUE)."""
    queue = queue or CPU_QUEUE
    return lambda job_key: _prefix(job_key, queue, walltime, slots, gpus=0)


def submit_driver(experiment_file: str, *args: str) -> str:
    """Submit ``experiment_file`` itself as the long-lived driver job (non-blocking bsub onto
    the 7-day `local` queue) and return the LSF job id. Run this on the login node, from the
    repo root -- the driver job inherits that cwd/env and submits the per-stage bsub -K jobs
    from inside its own job. Extra ``args`` are forwarded to the experiment file verbatim (e.g.
    a tier) and become part of the driver's job name + log path, so different tiers coexist."""
    if rundb._ANCHOR is None:  # e.g. a bare python -c submit; a built Experiment already anchored
        rundb.anchor_for_script(experiment_file)
    # Args (tier, and any --new/--replace/--extend force flags) go into the driver's job name +
    # log path so runs coexist; strip flag punctuation so the name stays a clean identifier.
    stem = "_".join([Path(experiment_file).stem, *args]).replace("--", "").replace("/", "_")
    log_dir = _log_dir()
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log = f"{log_dir}/{stem}_driver.log"
    cmd = [
        "bsub",
        "-J", f"{stem}_driver",
        "-P", LSF_PROJECT,
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
