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
_PALETTE = ["#58a6ff", "#3fb950", "#f85149", "#d29922", "#a371f7", "#ffa657", "#79c0ff", "#7ee787",
            "#ff7b72", "#d2a8ff", "#56d364", "#e3b341", "#f778ba", "#76e3ea", "#ffbedd", "#aff5b4"]
_ids = itertools.count()  # unique plot-div ids across one render
# Default plot px per facet row -- deliberately SHORT (~2/5 the old 380; 76 was too cramped
# once margins ate their share) so a many-panel report scans densely; any plot grows by
# dragging its bottom edge, or via lines(height=...).
ROW_HEIGHT = 152


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
    height: "int | None" = None,
) -> str:
    """Multi-trace Plotly line chart -> HTML fragment. ``series``: {label: rows} (or one bare
    rows list), each row a dict. One trace per (series, y column, color-column value), faceted
    into grid subplots by the facet column. ``y``: column name(s), fnmatch globs over numeric
    columns (default: every numeric column except x). ``dash``: {y-glob: plotly dash style}
    (e.g. {"val_*": "dash"}). ``x`` None -> row index. ``height``: total plot px (None ->
    ROW_HEIGHT per facet row -- deliberately short); every plot is also drag-resizable by its
    bottom edge in the browser."""
    if isinstance(series, list):
        series = {"": series}
    y_pats = [y] if isinstance(y, str) else y
    traces: "list[dict]" = []
    facet_vals: "list" = []  # first-seen order, shared across series so facets align
    for li, (label, rows) in enumerate(series.items()):
        # Union over all rows, first-seen order: interleaved logs (train rows + sparse val
        # rows) mean rows[0]'s keyset alone misses late-appearing series like val_loss.
        cols = list(dict.fromkeys(c for r in rows for c in r))
        numeric = [c for c in cols if c != x and any(_num(r.get(c)) is not None for r in rows)]
        ys = numeric if y_pats is None else \
            list(dict.fromkeys(c for p in y_pats for c in numeric if fnmatch(c, p)))
        for fval, frows in ({None: rows} if facet is None else _partition(rows, facet)).items():
            if facet is not None and fval not in facet_vals:
                facet_vals.append(fval)
            for gval, grows in ({None: frows} if color is None else _partition(frows, color)).items():
                for yc in ys:
                    d = next((v for p, v in (dash or {}).items() if fnmatch(yc, p)), "solid")
                    fi = facet_vals.index(fval) if facet is not None else 0
                    # Drop rows where this series is absent: a sparse series (val_* logged
                    # every N epochs) draws as one connected line, not null-broken fragments.
                    xs = [r.get(x) for r in grows] if x else list(range(len(grows)))
                    pts = [(xv, yv) for xv, yv in zip(xs, (_num(r.get(yc)) for r in grows))
                           if yv is not None]
                    # Multi-series overlays (e.g. ablation arms) get ONE color per series
                    # label -- its train/val traces share it, distinguished by dash. Single-
                    # series plots keep the old per-trace coloring (one color per y column).
                    ci = li if len(series) > 1 else len(traces)
                    traces.append({
                        "x": [xv for xv, _ in pts],
                        "y": [yv for _, yv in pts],
                        "name": " · ".join(str(v) for v in (label, yc, gval) if v not in (None, "")),
                        "mode": "lines",
                        "line": {"dash": d, "color": _PALETTE[ci % len(_PALETTE)]},
                        **({"xaxis": f"x{fi + 1}", "yaxis": f"y{fi + 1}"} if fi else {}),
                    })
    n = len(facet_vals) or 1
    ncols = min(n, 3)
    nrows = -(-n // ncols)
    layout: "dict" = {
        # Legend rides ABOVE the plot area (anchored to its own bottom at y=1, growing upward
        # into the top margin) -- Plotly's default bottom placement for horizontal legends
        # overlaps the x-axis title. Margins are tight to match the short default height;
        # the top fits one legend row.
        "margin": {"t": 24, "b": 34, "l": 56, "r": 10},
        "paper_bgcolor": "#0d1117", "plot_bgcolor": "#161b22",
        "font": {"color": "#c9d1d9"}, "showlegend": True,
        "legend": {"orientation": "h", "x": 0, "xanchor": "left", "y": 1.0, "yanchor": "bottom"},
        "height": height or ROW_HEIGHT * nrows,
        # Constant across re-renders, so Plotly.react (page()'s live poller) keeps the USER's
        # zoom/pan/legend state while the data underneath updates.
        "uirevision": "keep",
    }
    if n > 1:
        layout["grid"] = {"rows": nrows, "columns": ncols, "pattern": "independent"}
    # y-axis label from the y patterns the caller asked for (a plot with several y columns
    # names them in the legend; the axis still says what family it shows).
    ytitle = ", ".join(y_pats) if y_pats else ""
    for i in range(n):
        s = "" if i == 0 else str(i + 1)
        layout[f"yaxis{s}"] = {"title": ytitle, **({"type": "log"} if logy else {})}
        layout[f"xaxis{s}"] = {"title": f"{facet}={facet_vals[i]}" if facet_vals else (x or "index")}
    pid = f"viz{next(_ids)}"
    head = f"<h2>{_html.escape(title)}</h2>" if title else ""
    # Data rides in a JSON island next to an empty div; page()'s shared runtime draws every
    # island (initial load AND live updates go through the same Plotly.react). "</" escaped so
    # payload strings can never terminate the script element.
    payload = json.dumps({"traces": traces, "layout": layout}).replace("</", "<\\/")
    return (f'{head}<div id="{pid}" class="vizplot" style="width:100%"></div>'
            f'<script type="application/json" id="{pid}-data">{payload}</script>')


def table(columns: "dict[str, dict]", metrics: "list[str] | None" = None, title: str = "",
          corner: str = "metric") -> str:
    """Pivoted metric table -> HTML fragment: {column label: {metric: value}} renders metric
    rows x label columns (mia-muvit's metric_table, generalized past two models). Nested dicts
    flatten to dotted keys; ``metrics``: key globs filtering AND ordering the rows (default:
    all keys, sorted). ``corner``: the row-label column header -- set it when transposing
    (rows = arms/models rather than metrics). Numbers %.4g; missing cells an en dash."""
    flat = {label: _flat(d) for label, d in columns.items()}
    all_keys = sorted({k for d in flat.values() for k in d})
    keys = all_keys if metrics is None else \
        list(dict.fromkeys(k for p in metrics for k in all_keys if fnmatch(k, p)))

    def fmt(v) -> str:
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return _html.escape(str(v))
        return f"{v:.4g}"

    head = f"<tr><th>{_html.escape(corner)}</th>" + "".join(f"<th>{_html.escape(str(c))}</th>" for c in flat) + "</tr>"
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
  .logy { float: right; color: #8b949e; font-size: 12px; cursor: pointer; user-select: none; }
  a.home { float: right; color: #8b949e; font-size: 12px; text-decoration: none; }
  a.home:hover { color: #c9d1d9; }
  /* native drag handle (bottom edge): the runtime's ResizeObserver replots at the new height.
     min-height sits under ROW_HEIGHT so the short default isn't padded with dead space. */
  div.vizplot { resize: vertical; overflow: hidden; min-height: 40px; }
  table { border-collapse: collapse; } td, th { text-align: left; padding: 4px 12px 4px 0; }
  table.metrics th { color: #8b949e; border-bottom: 1px solid #30363d;
                     cursor: pointer; user-select: none; }
  table.metrics th.asc::after { content: " \\25B4"; } table.metrics th.desc::after { content: " \\25BE"; }
  table.metrics td { border-bottom: 1px solid #21262d; }
  img.zoom { cursor: zoom-in; border: 1px solid #30363d; }
  pre { background: #161b22; border: 1px solid #30363d; padding: 10px; overflow-x: auto;
        border-radius: 6px; }
  .da { color: #3fb950; } .dr { color: #f85149; }   /* diff added / removed */
  tr.hl td { background: #20261c; }                 /* changed metric row */
  input { background: #161b22; color: #c9d1d9; border: 1px solid #30363d;
          border-radius: 4px; padding: 2px 6px; }
  summary { cursor: pointer; color: #8b949e; }
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


# The page runtime: draws every plot island (initial load and updates alike go through
# Plotly.react -- with layout.uirevision constant, the user's zoom/pan SURVIVES data updates),
# and, when __INTERVAL__ > 0, polls this page's own URL: plot islands whose data changed are
# react'ed in place; a section whose NON-plot structure changed is hot-swapped (its plots
# redraw fresh); a fetched page without data-live (the run's final render) stops the polling
# -- the tab goes quiet WITH the zoom state intact. Sections are matched by position, so keep
# a live report's section count stable ("" placeholders are fine). Built with placeholder
# replace (not an f-string) so the JS braces don't need doubling.
_RUNTIME_JS = """
(function(){
  // Legend single-clicks commit only after plotly's doubleClickDelay window (it must wait to
  // distinguish them from a double-click's isolate gesture). We tried 150ms -- snappier
  // toggles, but the double-click window felt too tight to hit; plotly's 300ms default is
  // the better trade.
  // No modebar: scroll = zoom, drag = pan-select, double-click = reset axes -- the button
  // strip added chrome above every plot without adding a gesture that isn't already there.
  var CFG = {displayModeBar: false, scrollZoom: true, responsive: true};
  function strip(el){  // a section's structural html: islands blanked (data is not structure)
    var c = el.cloneNode(true);
    c.querySelectorAll("script[type='application/json']").forEach(function(n){ n.textContent = ""; });
    return c.innerHTML;
  }
  // Per-plot y-scale override (click the "y: log/linear" toggle above a plot). Keyed by plot
  // id in JS -- like the table sorts -- so it survives both live island updates and whole
  // section swaps; the server's logy= is only the default.
  var LOGY = {};
  function draw(){
    document.querySelectorAll("div.vizplot").forEach(function(div){
      var isl = document.getElementById(div.id + "-data");
      if (!isl) return;
      var d = JSON.parse(isl.textContent);
      if (div.dataset.userh) d.layout.height = +div.dataset.userh;  // drag-resized: keep it across live re-renders
      var log = (div.id in LOGY) ? LOGY[div.id]
                                 : !!(d.layout.yaxis && d.layout.yaxis.type === "log");
      Object.keys(d.layout).forEach(function(k){  // every facet's axis flips together
        if (/^yaxis\\d*$/.test(k)) d.layout[k].type = log ? "log" : "linear";
      });
      Plotly.react(div.id, d.traces, d.layout, CFG);
      var tog = document.getElementById(div.id + "-logy");
      if (!tog) {  // (re)created after section swaps wipe it
        tog = document.createElement("span");
        tog.id = div.id + "-logy"; tog.className = "logy";
        div.parentNode.insertBefore(tog, div);
        tog.onclick = function(){ LOGY[div.id] = tog.dataset.log !== "1"; draw(); };
      }
      tog.dataset.log = log ? "1" : "";
      tog.textContent = log ? "y: log" : "y: linear";
      if (!div.dataset.ro) {  // CSS resize drags the div's bottom edge; replot at the new height
        div.dataset.ro = "1";
        new ResizeObserver(function(){
          var h = div.clientHeight, cur = +div.dataset.userh || d.layout.height;
          if (h > 100 && Math.abs(h - cur) > 4) {
            div.dataset.userh = h;
            Plotly.relayout(div.id, {height: h});
          }
        }).observe(div);
      }
    });
  }
  // Click-to-sort on every metrics table: numeric-aware, toggling desc/asc. State lives here
  // (keyed by table order) rather than in the DOM, so live section swaps keep the user's sort.
  var SORT = {};
  function tables(){ return document.querySelectorAll("table.metrics"); }
  function applySort(t){
    var k = Array.prototype.indexOf.call(tables(), t), st = SORT[k];
    if (!st) return;
    var rows = Array.prototype.slice.call(t.querySelectorAll("tr"), 1);  // [0] is the header
    rows.sort(function(a, b){
      var av = a.cells[st[0]] ? a.cells[st[0]].textContent : "";
      var bv = b.cells[st[0]] ? b.cells[st[0]].textContent : "";
      var an = parseFloat(av), bn = parseFloat(bv);
      var c = (!isNaN(an) && !isNaN(bn)) ? an - bn
                                         : av.localeCompare(bv, undefined, {numeric: true});
      return st[1] * c;
    });
    if (rows.length) rows.forEach(function(r){ rows[0].parentNode.appendChild(r); });
    t.querySelectorAll("th").forEach(function(h, i){
      h.classList.toggle("asc", i === st[0] && st[1] === 1);
      h.classList.toggle("desc", i === st[0] && st[1] === -1);
    });
  }
  function applySorts(){ tables().forEach(applySort); }
  document.addEventListener("click", function(ev){
    var th = ev.target.closest && ev.target.closest("table.metrics th");
    if (!th) return;
    var t = th.closest("table.metrics"), k = Array.prototype.indexOf.call(tables(), t);
    var st = SORT[k];
    SORT[k] = (st && st[0] === th.cellIndex) ? [th.cellIndex, -st[1]] : [th.cellIndex, -1];
    applySort(t);
  });
  var pristine = {};  // section -> structural html as SERVED (the live DOM mutates once drawn)
  document.querySelectorAll("div.sect").forEach(function(s){ pristine[s.id] = strip(s); });
  draw();
  if (__INTERVAL__ <= 0) return;
  var iv = setInterval(function(){
    fetch(location.href, {cache: "no-store"}).then(function(r){ return r.text(); }).then(function(text){
      var doc = new DOMParser().parseFromString(text, "text/html");
      doc.querySelectorAll("div.sect").forEach(function(fresh){
        var cur = document.getElementById(fresh.id);
        if (!cur) return;
        if (strip(fresh) !== pristine[fresh.id]) {         // structure changed -> swap section
          cur.innerHTML = fresh.innerHTML;
          pristine[fresh.id] = strip(fresh);
        } else {                                           // data-only change -> update islands
          fresh.querySelectorAll("script[type='application/json']").forEach(function(nisl){
            var isl = document.getElementById(nisl.id);
            if (isl && isl.textContent !== nisl.textContent) isl.textContent = nisl.textContent;
          });
        }
      });
      draw();
      applySorts();  // freshly swapped table DOM: re-impose the user's sort
      if (!doc.body.hasAttribute("data-live")) clearInterval(iv);  // final render: stop polling
    }).catch(function(){});  // server briefly gone: keep the page, try again next tick
  }, __INTERVAL__ * 1000);
})();
"""


def page(*sections: str, title: str = "report", short_title: "str | None" = None,
         description: str = "", refresh: "int | None" = None) -> str:
    """A self-contained HTML document wrapping ``sections`` (fragments from lines/table/images
    or any HTML string of your own): dark shell, Plotly CDN, the image lightbox, and the plot
    runtime (zoom/pan toolbar + scroll-zoom on every chart). ``refresh`` (seconds) makes an
    open tab POLL this page's own URL and update IN PLACE -- growing curves react into the
    existing plots, so your zoom/pan survives updates (see _RUNTIME_JS). Pass it CONDITIONALLY
    (``refresh=5 if missing else None``): the run's final render omits the live marker and the
    tab stops polling, zoom intact. Keep a live report's section count/order stable across
    renders ("" placeholders are fine) -- sections are matched by position.

    ``description`` (a sentence or two on what the report shows/tests) renders as a muted line
    under the heading. ``short_title`` (a few words, e.g. "token dropout") is a dashboard-sized
    label for this experiment -- dashboard.py scrapes it off the saved report.html (see the
    ``data-short-title`` body attribute below) to label the group row; defaults to ``title``."""
    global _ids
    body = "".join(f'<div class="sect" id="sect{i}">{s}</div>' for i, s in enumerate(sections))
    runtime = _RUNTIME_JS.replace("__INTERVAL__", str(refresh or 0))
    _ids = itertools.count()  # plot ids restart per page, so re-renders line up island-for-island
    live = ' data-live="1"' if refresh else ""  # backslash-free: 3.11 f-strings reject \ in expressions
    short = _html.escape(short_title or title, quote=True)
    desc = note(description) if description else ""
    # "⌂ dashboard" -> the browse server's home when served through it (the normal path);
    # a report opened straight off disk just has a dead muted link. Beats browser-back, which
    # re-fetches the whole home page (this page's timers keep it out of bfcache).
    home = '<a class="home" href="/">&#8962; dashboard</a>'
    return (f'<!doctype html><html><head><meta charset="utf-8">'
            f"<title>{_html.escape(title)}</title>{_PLOT_CDN}<style>{_STYLE}</style></head>"
            f'<body{live} data-short-title="{short}">{home}<h1>{_html.escape(title)}</h1>{desc}{body}'
            f'<div id="viewer"><div class="hint">scroll = zoom · drag = pan · arrows = walk the '
            f'grid · Esc / click background = close</div><img id="viewer_img"/></div>'
            f"<script>{_LIGHTBOX_JS}</script><script>{runtime}</script></body></html>")
