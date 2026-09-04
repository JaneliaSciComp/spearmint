#!/usr/bin/env python
"""Declarative live dashboard: multiple plots, facets, aligned PNGs, and overlays.

    uv run python spearmint/examples/toy_dashboard_demo.py --new 'model_*'
    spearmint browse

Open the 📊 link for ``dashboard_demo`` while the workers run. Metrics append at 4 Hz and a
new aligned image pair appears every two seconds.
"""

import spearmint as sp

WORKER = "spearmint/examples/dashboard_worker.py"

e = sp.Experiment(prefix="dashboard_demo", cmd_prefix=["uv", "run", "python"])
model_a = e.Stage("model_a", cmd=lambda: [WORKER, "--model", "a"])
model_b = e.Stage("model_b", cmd=lambda: [WORKER, "--model", "b"])

e.dashboard = sp.Dashboard(
    sp.Lines(
        [model_a, model_b], x="step", y=["loss", "val_loss"],
        dash={"val_loss": "dash"},
        colors={"model_a": "#58a6ff", "model_b": "#f0883e"},
        logy=True, title="Loss curves",
    ),
    sp.Lines(
        [model_a, model_b], x="step", y=["accuracy", "iou"],
        colors={"model_a": "#58a6ff", "model_b": "#f0883e"},
        title="Quality metrics",
    ),
    sp.Lines(
        [model_a, model_b], x="step", y="loss", facet="view",
        colors={"model_a": "#58a6ff", "model_b": "#f0883e"},
        title="Loss faceted by view", height=320,
    ),
    sp.Table(
        [model_a, model_b], path="summary.json", rows="metric", columns="stage",
        metrics=["final_*", "best_*"], title="Final metrics",
    ),
    sp.Images(
        [model_a, model_b], path="frames/{row}_sharedbase_{col}_{overlay}.png",
        stage_mode="rows", overlays=["raw", "prediction"],
        title="Stages and views as rows", width=150,
    ),
    sp.Images(
        [model_a, model_b], path="frames/{row}_sharedbase_{col}_{overlay}.png",
        stage_mode="overlay", overlays=["raw", "prediction"],
        title="Models and modalities overlaid", width=150,
    ),
    title="Live dashboard demo",
    refresh=1,
)

if __name__ == "__main__":
    e.main()
