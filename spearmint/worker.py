"""Execute a managed payload and publish its exit status without touching SQLite.

LSF runs this wrapper on the compute node, so the receipt survives loss of the submitting
driver. A scheduler hard kill may also kill the wrapper: absence of a receipt is not success.
"""

import json
import os
import signal
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def main() -> int:
    args = sys.argv[1:]
    python_payload = args[:1] == ["--python"]
    if python_payload:
        args = args[1:]
    assert args[:1] == ["--"] and len(args) > 1, "worker expects -- COMMAND [ARGS]"
    command = ([sys.executable] if python_payload else []) + args[1:]
    run_id = int(os.environ["SPEARMINT_RUN_ROW"])
    outdir = Path(os.environ["SPEARMINT_RUN_OUTDIR"])
    started_at = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    process = None
    pending = []

    def forward(signum, _frame):
        if process is None:
            pending.append(signum)
        else:
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                pass

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGUSR2):
        signal.signal(signum, forward)
    error = None
    try:
        process = subprocess.Popen(command, start_new_session=True)
        for signum in pending:
            forward(signum, None)
        code = process.wait()
    except OSError as exc:
        code = 127
        error = str(exc)
        print(f"[worker] {error}", file=sys.stderr, flush=True)
    receipt = {
        "run_id": run_id, "exit_code": code,
        "started_at": started_at,
        "ended_at": datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),
        "error": error,
    }
    path = outdir / f"exit.{run_id}.json"
    tmp = path.with_suffix(f".json.{os.getpid()}.tmp")
    with tmp.open("w") as stream:
        json.dump(receipt, stream)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    tmp.replace(path)
    return code if code >= 0 else 128 - code


if __name__ == "__main__":
    sys.exit(main())
