"""toy_report_demo's report function, in its own module so the driver HOT-RELOADS it: edit
this file while the experiment runs and the next tick renders with your changes (a broken
save prints [report] render failed and heals on the next save). Import-side-effect-free by
contract -- no Experiment construction; stages are resolved BY NAME through savedir()/the
ledger, never by closing over Stage objects."""

import json
from pathlib import Path

from spearmint import rundb, viz

PREFIX = "e05_report"
STAGES = ("train_a", "train_b")


def make_report(savedir) -> str:
    curves: "dict[str, list[dict]]" = {}
    finals: "dict[str, dict]" = {}
    missing = []
    for name in STAGES:
        # Curves from the LIVE dir (wip included) so mid-run renders show them growing;
        # summaries only from a completed run. exists()-guard everything: a replace re-run
        # clears the "latest done" dir in place, so files can vanish mid-render.
        live = rundb.latest_outdir(f"{PREFIX}/{name}")
        metrics = Path(live or "") / "metrics.jsonl"
        if live is not None and metrics.exists():
            curves[name] = [json.loads(ln) for ln in metrics.read_text().splitlines()]
        done = savedir(name)
        summary = Path(done or "") / "summary.json"
        if done is not None and summary.exists():
            finals[name] = json.loads(summary.read_text())
        else:
            missing.append(name)
    return viz.page(
        viz.note(f"still running: {', '.join(missing)}") if missing else "",
        viz.lines(curves, x="step", y=["loss", "val_*"], dash={"val_*": "dash"}, logy=True,
                  title="loss, A vs B (val dashed)"),
        viz.table(finals, title="final metrics"),
        title=PREFIX,
        refresh=1 if missing else None,
    )
