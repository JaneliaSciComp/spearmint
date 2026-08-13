"""Python-native report building: compose an HTML report from data YOUR code already loaded
and shaped -- ``viz.page(viz.lines(...), viz.table(...), viz.images(...))`` -- and hand it to
the driver via ``e.report = fn`` (see dagrunner: the fn re-renders as stages finish and on a
timer, so the report stays fresh through a long run). Helpers take PYTHON DATA (lists of row
dicts, dicts of scalars), never file paths or spec dicts: loading, filtering, renaming, math
happen in your experiment file with the whole language. Stdlib-only; plots are Plotly via CDN.

Ported in spirit from mia-muvit's experiments/report.py (metric_table / image_panel / the
loss-chart panels / the zoom-pan-arrow lightbox); the ssh/status half of that file is rundb +
dashboard territory now and is deliberately absent here."""

import base64
import html as _html
import itertools
import json
from fnmatch import fnmatch
from pathlib import Path

_PLOT_CDN = '<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>'
# Trace palette (dark-theme friendly), cycled per trace.
_PALETTE = ["#58a6ff", "#3fb950", "#f85149", "#d29922", "#a371f7", "#ffa657", "#79c0ff", "#7ee787"]
_ids = itertools.count()  # unique plot-div ids across one render


def _num(v) -> "float | None":
    """``v`` as a float for plotting, else None (Plotly renders None as a gap)."""
    if v is None or isinstance(v, bool):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _partition(rows: "list[dict]", col: str) -> "dict":
    """Rows grouped by their value in ``col``, insertion-ordered."""
    out: "dict" = {}
    for r in rows:
        out.setdefault(r.get(col), []).append(r)
    return out


def _flat(obj, prefix: str = "") -> "dict[str, object]":
    """Nested dict -> {dotted.key: scalar}; non-scalar leaves dropped."""
    if isinstance(obj, dict):
        out: "dict[str, object]" = {}
        for k, v in obj.items():
            out.update(_flat(v, f"{prefix}{k}."))
        return out
    return {prefix[:-1]: obj} if isinstance(obj, (int, float, str, bool)) else {}


def lines(
    series,
    x: "str | None" = None,
    y=None,
    color: "str | None" = None,
    facet: "str | None" = None,
    dash: "dict[str, str] | None" = None,
    logy: bool = False,
    title: str = "",
) -> str:
    """Multi-trace Plotly line chart -> HTML fragment. ``series``: {label: rows} (or one bare
    rows list), each row a dict. One trace per (series, y column, color-column value), faceted
    into grid subplots by the facet column. ``y``: column name(s), fnmatch globs over numeric
    columns (default: every numeric column except x). ``dash``: {y-glob: plotly dash style}
    (e.g. {"val_*": "dash"}). ``x`` None -> row index."""
    if isinstance(series, list):
        series = {"": series}
    y_pats = [y] if isinstance(y, str) else y
    traces: "list[dict]" = []
    facet_vals: "list" = []  # first-seen order, shared across series so facets align
    for label, rows in series.items():
        cols = list(rows[0]) if rows else []
        numeric = [c for c in cols if c != x and any(_num(r.get(c)) is not None for r in rows[:50])]
        ys = numeric if y_pats is None else \
            list(dict.fromkeys(c for p in y_pats for c in numeric if fnmatch(c, p)))
        for fval, frows in ({None: rows} if facet is None else _partition(rows, facet)).items():
            if facet is not None and fval not in facet_vals:
                facet_vals.append(fval)
            for gval, grows in ({None: frows} if color is None else _partition(frows, color)).items():
                for yc in ys:
                    d = next((v for p, v in (dash or {}).items() if fnmatch(yc, p)), "solid")
                    fi = facet_vals.index(fval) if facet is not None else 0
                    traces.append({
                        "x": [r.get(x) for r in grows] if x else list(range(len(grows))),
                        "y": [_num(r.get(yc)) for r in grows],
                        "name": " · ".join(str(v) for v in (label, yc, gval) if v not in (None, "")),
                        "mode": "lines",
                        "line": {"dash": d, "color": _PALETTE[len(traces) % len(_PALETTE)]},
                        **({"xaxis": f"x{fi + 1}", "yaxis": f"y{fi + 1}"} if fi else {}),
                    })
    n = len(facet_vals) or 1
    ncols = min(n, 3)
    nrows = -(-n // ncols)
    layout: "dict" = {
        "margin": {"t": 24, "r": 10}, "paper_bgcolor": "#0d1117", "plot_bgcolor": "#161b22",
        "font": {"color": "#c9d1d9"}, "showlegend": True, "legend": {"orientation": "h"},
        "height": 380 * nrows,
    }
    if n > 1:
        layout["grid"] = {"rows": nrows, "columns": ncols, "pattern": "independent"}
    for i in range(n):
        s = "" if i == 0 else str(i + 1)
        layout[f"yaxis{s}"] = {"type": "log"} if logy else {}
        layout[f"xaxis{s}"] = {"title": f"{facet}={facet_vals[i]}" if facet_vals else (x or "index")}
    pid = f"viz{next(_ids)}"
    head = f"<h2>{_html.escape(title)}</h2>" if title else ""
    return (f'{head}<div id="{pid}" style="width:100%"></div>'
            f'<script>Plotly.newPlot("{pid}", {json.dumps(traces)}, {json.dumps(layout)}, '
            f"{{displayModeBar: false}});</script>")


def table(columns: "dict[str, dict]", metrics: "list[str] | None" = None, title: str = "") -> str:
    """Pivoted metric table -> HTML fragment: {column label: {metric: value}} renders metric
    rows x label columns (mia-muvit's metric_table, generalized past two models). Nested dicts
    flatten to dotted keys; ``metrics``: key globs filtering AND ordering the rows (default:
    all keys, sorted). Numbers %.4g; missing cells an en dash."""
    flat = {label: _flat(d) for label, d in columns.items()}
    all_keys = sorted({k for d in flat.values() for k in d})
    keys = all_keys if metrics is None else \
        list(dict.fromkeys(k for p in metrics for k in all_keys if fnmatch(k, p)))

    def fmt(v) -> str:
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return _html.escape(str(v))
        return f"{v:.4g}"

    head = "<tr><th>metric</th>" + "".join(f"<th>{_html.escape(str(c))}</th>" for c in flat) + "</tr>"
    trs = "".join(
        f"<tr><td>{_html.escape(k)}</td>"
        + "".join(f"<td>{fmt(d[k]) if k in d else '–'}</td>" for d in flat.values())
        + "</tr>"
        for k in keys
    )
    t = f"<h2>{_html.escape(title)}</h2>" if title else ""
    return f'{t}<table class="metrics">{head}{trs}</table>'


def images(root, rows: "list[str]", cols: "list", name: str = "{row}_z{col}.png",
           width: int = 220, title: str = "") -> str:
    """Grid of base64-embedded PNGs -> HTML fragment (mia-muvit's image_panel): one table row
    per ``rows`` value, one column per ``cols`` value, filenames from the ``name`` template
    over {row}/{col}, resolved under ``root``. Missing files render '-'. Click any image for
    page()'s lightbox: scroll = zoom (nearest-neighbor), drag = pan, arrows = walk the grid."""
    root = Path(root)
    out = ['<table class="imggrid"><tr><th></th>'
           + "".join(f"<th>{_html.escape(str(c))}</th>" for c in cols) + "</tr>"]
    for row in rows:
        out.append(f"<tr><td><b>{_html.escape(str(row))}</b></td>")
        for col in cols:
            p = root / name.format(row=row, col=col)
            if p.exists():
                b64 = base64.b64encode(p.read_bytes()).decode()
                out.append(f'<td><img class="zoom" src="data:image/png;base64,{b64}" width="{width}"/></td>')
            else:
                out.append("<td>-</td>")
        out.append("</tr>")
    out.append("</table>")
    t = f"<h2>{_html.escape(title)}</h2>" if title else ""
    return t + "".join(out)


def note(text: str) -> str:
    """A muted one-liner (e.g. 'pretrain still running -- curves below are partial')."""
    return f"<p class='note'>{_html.escape(text)}</p>"


_STYLE = """
  body { font: 14px ui-monospace, monospace; background: #0d1117; color: #c9d1d9;
         margin: 24px; max-width: 1100px; }
  h1 { font-size: 17px; } h2 { font-size: 14px; border-bottom: 1px solid #30363d;
       padding-bottom: 3px; margin-top: 1.6rem; }
  .note { color: #8b949e; }
  table { border-collapse: collapse; } td, th { text-align: left; padding: 4px 12px 4px 0; }
  table.metrics th { color: #8b949e; border-bottom: 1px solid #30363d; }
  table.metrics td { border-bottom: 1px solid #21262d; }
  img.zoom { cursor: zoom-in; border: 1px solid #30363d; }
  #viewer { position: fixed; inset: 0; background: rgba(0,0,0,.93); display: none;
            z-index: 1000; cursor: grab; overflow: hidden; }
  #viewer.open { display: block; }
  #viewer img { position: absolute; left: 0; top: 0; image-rendering: pixelated; }
  #viewer .hint { position: fixed; top: 8px; left: 12px; color: #8b949e; font-size: 12px; }
"""

# The lightbox, ported from mia-muvit's report.py: click a .zoom image to open it full-screen
# with nearest-neighbor zoom toward the cursor, drag-pan, and arrow-key navigation across the
# clicked image's own <table> grid (skipping gaps). Sized via width/height, NOT a CSS
# transform -- the GPU bilinear-filters transforms at high zoom, wrecking pixel inspection.
_LIGHTBOX_JS = """
(function() {
  var V = document.getElementById("viewer"), IMG = document.getElementById("viewer_img");
  var scale = 1, tx = 0, ty = 0, drag = false, lx = 0, ly = 0;
  var grid = [], row = 0, col = 0, curW = 0, curH = 0;
  function apply() {
    IMG.style.left = tx + "px"; IMG.style.top = ty + "px";
    IMG.style.width = (IMG.naturalWidth * scale) + "px";
    IMG.style.height = (IMG.naturalHeight * scale) + "px";
  }
  function fit() {
    var vw = V.clientWidth, vh = V.clientHeight, nw = IMG.naturalWidth || 1, nh = IMG.naturalHeight || 1;
    scale = Math.min(vw / nw, vh / nh) * 0.95; tx = (vw - nw * scale) / 2; ty = (vh - nh * scale) / 2; apply();
  }
  function shown() {  // same-size tile -> keep zoom/pan (arrow scrubbing stays put); else re-fit
    if (IMG.naturalWidth === curW && IMG.naturalHeight === curH) apply(); else fit();
    curW = IMG.naturalWidth; curH = IMG.naturalHeight;
  }
  function show() { IMG.src = grid[row][col].src; if (IMG.complete) shown(); else IMG.onload = shown; }
  function open(el) {
    var trs = Array.prototype.slice.call(el.closest("table").querySelectorAll("tr"));
    grid = trs.filter(function(tr) { return tr.querySelector("td"); }).map(function(tr) {
      var tds = Array.prototype.slice.call(tr.children).filter(function(n) { return n.tagName === "TD"; });
      return tds.map(function(td) { return td.querySelector("img.zoom"); });
    });
    row = 0; col = 0;
    for (var i = 0; i < grid.length; i++) {
      var j = grid[i].indexOf(el);
      if (j >= 0) { row = i; col = j; break; }
    }
    curW = 0; curH = 0;
    V.classList.add("open"); show();
  }
  function move(dr, dc) {  // step in a direction, skipping gaps, stopping at edges
    var nr = row, nc = col;
    while (true) {
      nr += dr; nc += dc;
      if (nr < 0 || nr >= grid.length || nc < 0 || nc >= grid[nr].length) return;
      if (grid[nr][nc]) { row = nr; col = nc; show(); return; }
    }
  }
  function close() { V.classList.remove("open"); }
  document.addEventListener("click", function(e) {
    if (e.target.tagName === "IMG" && e.target.classList.contains("zoom")) open(e.target);
  });
  V.addEventListener("click", function(e) { if (e.target === V) close(); });
  document.addEventListener("keydown", function(e) {
    if (!V.classList.contains("open")) return;
    if (e.key === "Escape") { close(); return; }
    var d = {ArrowRight: [0, 1], ArrowLeft: [0, -1], ArrowDown: [1, 0], ArrowUp: [-1, 0]}[e.key];
    if (d) { e.preventDefault(); move(d[0], d[1]); }
  });
  V.addEventListener("wheel", function(e) {  // zoom toward the cursor, ~8%/notch
    e.preventDefault();
    var r = V.getBoundingClientRect(), px = e.clientX - r.left, py = e.clientY - r.top;
    var f = Math.exp(-e.deltaY * 0.0008);
    tx = px - (px - tx) * f; ty = py - (py - ty) * f; scale *= f; apply();
  }, {passive: false});
  V.addEventListener("mousedown", function(e) {
    if (e.target !== IMG) return;
    drag = true; lx = e.clientX; ly = e.clientY; V.style.cursor = "grabbing"; e.preventDefault();
  });
  window.addEventListener("mousemove", function(e) {
    if (!drag) return; tx += e.clientX - lx; ty += e.clientY - ly; lx = e.clientX; ly = e.clientY; apply();
  });
  window.addEventListener("mouseup", function() { drag = false; V.style.cursor = "grab"; });
})();
"""


def page(*sections: str, title: str = "report", refresh: "int | None" = None) -> str:
    """A self-contained HTML document wrapping ``sections`` (fragments from lines/table/images
    or any HTML string of your own): dark shell, Plotly CDN, the image lightbox. ``refresh``
    adds a meta-refresh (seconds) -- with the driver re-rendering as stages finish, an open
    tab then tracks the run with zero server machinery. Pass it CONDITIONALLY
    (``refresh=5 if missing else None``): the run's final render then emits a refresh-free
    page and the tab stops reloading once everything is done."""
    meta = f'<meta http-equiv="refresh" content="{refresh}">' if refresh else ""
    body = "".join(sections)
    return (f'<!doctype html><html><head><meta charset="utf-8">{meta}'
            f"<title>{_html.escape(title)}</title>{_PLOT_CDN}<style>{_STYLE}</style></head>"
            f"<body><h1>{_html.escape(title)}</h1>{body}"
            f'<div id="viewer"><div class="hint">scroll = zoom · drag = pan · arrows = walk the '
            f'grid · Esc / click background = close</div><img id="viewer_img"/></div>'
            f"<script>{_LIGHTBOX_JS}</script></body></html>")
