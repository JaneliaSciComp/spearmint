"""Browser status dashboard for the spearmint ledger -- the http sibling of `python -m
spearmint.report`, served locally on the laptop.

    uv run python -m spearmint.dashboard              # serve the local ledger
    uv run python -m spearmint.dashboard --remote     # pull a fresh ledger snapshot each refresh

Reads rundb.db (via report.collect) and serves an auto-refreshing status table -- experiments x
stages, wip/PEND/RUN/done/failed, live durations, staleness. With --remote each refresh first
runs remote.pull_db (the ~1s db-only pull), so the page tracks the cluster live; without it the
page shows whatever the last `spearmint.remote` pull left locally. --no-browser skips opening a
tab (matches the old dashboard, so a future "refresh" command can relaunch headless). This is
distinct from the orchestration.py-era experiments/dashboard.py (different port, different data
source -- ssh/bjobs scraping vs one sqlite read); both can run at once.
"""

import html
import http.server
import json
import mimetypes
import os
import re
import sys
import webbrowser
from pathlib import Path
from urllib.parse import quote, unquote

from . import report
from . import rundb
from .config import CONFIG

PORT = CONFIG.port  # spearmint's own dashboard port (config); distinct from any other UI's
REFRESH_SECONDS = CONFIG.refresh_seconds
_REMOTE = False  # set from argv in main(); each /table refresh pulls a fresh snapshot when True


class _Server(http.server.ThreadingHTTPServer):
    # allow_reuse_address=False so a still-LIVE dashboard on this port is a hard bind failure
    # rather than a silent coexist (the stale-server trap). A subclass, NOT a mutation of the
    # shared http.server.ThreadingHTTPServer class -- the old dashboard uses that same class, so
    # setting the flag on it globally would leak spearmint's choice into anything co-imported.
    allow_reuse_address = False

_STYLE = """
  body { font: 13px ui-monospace, monospace; background: #0d1117; color: #c9d1d9; margin: 24px; }
  h1 { font-size: 15px; font-weight: 600; }
  #meta { color: #8b949e; margin-bottom: 12px; }
  table { border-collapse: collapse; width: 100%; }
  th, td { text-align: left; padding: 3px 12px 3px 0; }
  th { color: #8b949e; font-weight: 600; border-bottom: 1px solid #30363d; }
  tr.group td { padding-top: 16px; color: #58a6ff; font-weight: 600; }
  td.key { color: #c9d1d9; } td.stale { color: #8b949e; }
  .mark { color: #8b949e; }  /* trailing content-kind glyphs (see report.CONTENT_SYMBOLS) */
  .ctrl { margin: 6px 0; color: #8b949e; }
  select { background: #161b22; color: #c9d1d9; border: 1px solid #30363d; border-radius: 4px; }
  .plot { width: 680px; height: 380px; }
  .datatable { font-size: 11px; margin: 8px 0 16px; }
  .datatable td, .datatable th { padding: 2px 10px 2px 0; border-bottom: 1px solid #21262d; }
  .jtree { margin: 6px 0 16px; }
  .jtree summary { cursor: pointer; } .jtree .jbody { margin-left: 14px; }
  .jrow { margin-left: 14px; } .jkey { color: #79c0ff; } .jmuted { color: #6e7681; }
  .jstr { color: #a5d6ff; } .jnum { color: #ffa657; } .jbool, .jnull { color: #d2a8ff; }
  .badge { padding: 1px 7px; border-radius: 10px; font-size: 11px; color: #0d1117; }
  .done { background: #3fb950; } .failed { background: #f85149; color: #fff; }
  .run { background: #58a6ff; } .pend { background: #d29922; }
  .skipped { background: #6e7681; } .abandoned { background: #30363d; color: #c9d1d9; }
  a { color: #58a6ff; text-decoration: none; } a:hover { text-decoration: underline; }
  .note { color: #8b949e; } code { color: #c9d1d9; }
  pre { background: #161b22; border: 1px solid #30363d; padding: 10px; overflow-x: auto; border-radius: 6px; }
  pre.err { color: #ffa198; }
  figure { display: inline-block; margin: 8px 12px 8px 0; vertical-align: top; }
  figure img { max-width: 260px; border: 1px solid #30363d; display: block; image-rendering: pixelated; }
  figure.clik { cursor: zoom-in; } figcaption { color: #8b949e; font-size: 11px; margin-top: 3px; }
  .sp-ov { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.93); z-index: 1000;
           overflow: hidden; cursor: grab; }
  .sp-img { position: absolute; left: 0; top: 0; image-rendering: pixelated; }  /* sized via w/h, not transform */
  .sp-hud { position: fixed; top: 12px; left: 12px; z-index: 1001; color: #c9d1d9;
            background: rgba(13,17,23,.85); padding: 8px 11px; border-radius: 6px; line-height: 1.5; }
  .sp-hud .sp-on { color: #58a6ff; font-weight: 600; } .sp-hud .sp-hint { color: #6e7681; margin-top: 6px; }
"""

_REFRESH_SCRIPT = f"""<script>
async function pull() {{
  const r = await fetch('/table');
  document.getElementById('table').innerHTML = await r.text();
  document.getElementById('meta').textContent =
    'updated ' + new Date().toLocaleTimeString() + ' ({REFRESH_SECONDS}s refresh)';
}}
setInterval(pull, {REFRESH_SECONDS} * 1000);
</script>"""


def _page(body: str, live: bool) -> str:
    """Wrap page ``body`` in the shared shell. ``live`` (only the home status table) adds the
    auto-refresh script that re-fetches /table into #table every REFRESH_SECONDS -- a per-run
    page passes live=False so its content is NOT clobbered by the status table on the next tick."""
    remote = "[remote]" if _REMOTE else ""
    meta = '<div id="meta">loading…</div>' if live else ""
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>spearmint</title>
<style>{_STYLE}</style></head><body>
<h1>spearmint status <span style="color:#58a6ff">{remote}</span></h1>
{meta}
<div id="table">{body}</div>
{_REFRESH_SCRIPT if live else ""}</body></html>"""


# file extension -> content kind (see report.CONTENT_SYMBOLS). .jsonl/.csv are "table"
# (row-oriented -> table + plot); a .json is "json" (folding tree) unless it's an array of
# records (peeked below), which renders as a table too.
_EXT_KIND = {".png": "png", ".jsonl": "table", ".csv": "table", ".json": "json"}


def _peek_char(path: Path) -> str:
    """First non-whitespace character of a file (''), for cheaply telling a JSON array ('[')
    from an object ('{') without parsing."""
    try:
        with path.open() as f:
            while (ch := f.read(1)):
                if not ch.isspace():
                    return ch
    except OSError:
        pass
    return ""


def _content_kinds(job_key: str, status: str) -> "set[str]":
    """The content kinds this stage's /run page would show -- subset of {png, table, json, log}.
    'log' = a failed stage with an err log. Cheap: prunes *.zarr trees (huge, never rendered) so
    it doesn't walk chunk files. Reads the LOCAL mirror, so a stage only lights up once a full
    `spearmint.remote` pull has brought its artifacts down (a db-only refresh won't)."""
    kinds: "set[str]" = set()
    if status == "failed" and (Path(rundb.ROOT) / report.lsf_log_relpath(job_key)).exists():
        kinds.add("log")
    outdir = rundb.latest_outdir(job_key)
    if outdir and Path(outdir).is_dir():
        for dirpath, dirnames, filenames in os.walk(outdir):
            dirnames[:] = [d for d in dirnames if not d.endswith(".zarr")]
            for f in filenames:
                kind = _EXT_KIND.get(Path(f).suffix)
                # a .json starting with "[" is an array-of-records -> renders as a table; peek a
                # few bytes rather than parse (this runs for every stage on every refresh).
                if kind == "json" and _peek_char(Path(dirpath) / f) == "[":
                    kind = "table"
                if kind:
                    kinds.add(kind)
    return kinds


def _table() -> str:
    if _REMOTE:
        from . import remote

        remote.pull_db()
    groups = report.collect()
    kinds = {r.job_key: _content_kinds(r.job_key, r.status) for rows in groups.values() for r in rows}
    kinds = {k: v for k, v in kinds.items() if v}  # only stages that actually have something
    return report.render_html(groups, kinds=kinds)


def _safe(relpath: str) -> Path:
    """Absolute path for a ROOT-relative request path, refusing to escape ROOT (a served-file
    server must never hand out arbitrary filesystem paths). Raises if the resolved path isn't
    ROOT or under it."""
    base = Path(rundb.ROOT).resolve()
    p = (base / relpath).resolve()
    assert p == base or base in p.parents, f"path {relpath!r} escapes ROOT"
    return p


_PLOT_CDN = '<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>'
_TABLE_CAP = 200   # rows shown in the HTML table
_PLOT_CAP = 5000   # rows shipped to the browser for the plot

# Built with placeholder replace (not an f-string) so the JS braces don't need doubling.
_PLOT_JS = """
(function(){
  const data = __DATA__, cols = __COLS__;
  const xs = document.getElementById("__PID__x"), ys = document.getElementById("__PID__y");
  const isnum = c => data.some(r => r[c] !== "" && r[c] != null && !isNaN(+r[c]));
  ys.value = cols.find(isnum) || cols[cols.length - 1];  // default y to first numeric column
  function draw(){
    const x = xs.value, y = ys.value;
    Plotly.newPlot("__PID__", [{x: data.map(r => r[x]), y: data.map(r => +r[y]),
        mode: "lines+markers", type: "scatter"}],
      {margin: {t: 10, r: 10}, xaxis: {title: x}, yaxis: {title: y},
       paper_bgcolor: "#0d1117", plot_bgcolor: "#161b22", font: {color: "#c9d1d9"}},
      {displayModeBar: false});
  }
  xs.onchange = draw; ys.onchange = draw; draw();
})();
"""


def _parse_tabular(path: Path) -> "tuple[list[str], list[dict]]":
    """(columns, rows) from a .csv or .jsonl file. jsonl columns = first-seen union of keys."""
    if path.suffix == ".csv":
        import csv

        with path.open(newline="") as f:
            rows = [dict(r) for r in csv.DictReader(f)]
        return (list(rows[0].keys()) if rows else []), rows
    rows: "list[dict]" = []
    cols: "list[str]" = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        rows.append(obj)
        cols += [k for k in obj if k not in cols]
    return cols, rows


def _records(obj) -> "tuple[list[str], list[dict]] | None":
    """(columns, rows) if ``obj`` is a table schema -- a non-empty list of dicts -- else None.
    Lets a .json that's actually an array of records render as a table+plot like jsonl/csv;
    anything else (a config dict, nested structure) falls through to the folding JSON tree."""
    if isinstance(obj, list) and obj and all(isinstance(r, dict) for r in obj):
        cols: "list[str]" = []
        for r in obj:
            cols += [k for k in r if k not in cols]
        return cols, obj
    return None


def _scalar_html(v) -> str:
    if v is None:
        return '<span class="jnull">null</span>'
    if isinstance(v, bool):
        return f'<span class="jbool">{"true" if v else "false"}</span>'
    if isinstance(v, (int, float)):
        return f'<span class="jnum">{html.escape(str(v))}</span>'
    return f'<span class="jstr">{html.escape(json.dumps(v))}</span>'  # quoted, escaped string


def _json_node(key, val) -> str:
    """One key:value entry of the folding JSON tree. dicts/lists become native <details> (no JS,
    works offline); scalars render inline. dicts open by default, lists collapsed."""
    label = "" if key is None else f'<span class="jkey">{html.escape(str(key))}</span>: '
    if isinstance(val, dict):
        if not val:
            return f'<div class="jrow">{label}<span class="jmuted">{{}}</span></div>'
        body = "".join(_json_node(k, v) for k, v in val.items())
        return (f'<details open><summary>{label}<span class="jmuted">{{{len(val)}}}</span>'
                f'</summary><div class="jbody">{body}</div></details>')
    if isinstance(val, list):
        if not val:
            return f'<div class="jrow">{label}<span class="jmuted">[]</span></div>'
        body = "".join(_json_node(i, v) for i, v in enumerate(val))
        return (f'<details><summary>{label}<span class="jmuted">[{len(val)}]</span>'
                f'</summary><div class="jbody">{body}</div></details>')
    return f'<div class="jrow">{label}{_scalar_html(val)}</div>'


def _json_tree(obj) -> str:
    """Folding browser for a general JSON value (see _json_node)."""
    if isinstance(obj, (dict, list)):
        return f'<div class="jtree">{_json_node(None, obj)}</div>'
    return f'<div class="jtree jrow">{_scalar_html(obj)}</div>'


def _tabular_render(label: str, cols: "list[str]", rows: "list[dict]", idx: int) -> str:
    """A values table + a Plotly scatter with x/y column dropdowns, from already-parsed
    columns/rows (shared by .jsonl/.csv files and array-of-records .json)."""
    if not rows:
        return f"<h3>{html.escape(label)}</h3><p class='note'>empty</p>"
    rel = label
    pid = f"plot{idx}"
    opts = "".join(f"<option>{html.escape(str(c))}</option>" for c in cols)
    head = "".join(f"<th>{html.escape(str(c))}</th>" for c in cols)
    shown = rows[:_TABLE_CAP]
    trs = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(r.get(c, '')))}</td>" for c in cols) + "</tr>"
        for r in shown
    )
    note = f"<p class='note'>table showing {len(shown)} of {len(rows)} rows</p>" if len(rows) > len(shown) else ""
    js = (
        _PLOT_JS.replace("__DATA__", json.dumps(rows[:_PLOT_CAP]))
        .replace("__COLS__", json.dumps([str(c) for c in cols]))
        .replace("__PID__", pid)
    )
    return (
        f"<h3>{html.escape(str(rel))}</h3>"
        f'<div class="ctrl">x <select id="{pid}x">{opts}</select> '
        f'y <select id="{pid}y">{opts}</select></div>'
        f'<div id="{pid}" class="plot"></div>'
        f'<table class="datatable"><tr>{head}</tr>{trs}</table>{note}'
        f"<script>{js}</script>"
    )


_NUM_LABEL = re.compile(r"([A-Za-z]+)(\d+)")  # e.g. "z10500" -> ("z", 10500)

# Built with placeholder replace (not an f-string) so the JS braces don't need doubling.
_IMAGE_JS = """
(function(){
  const IMAGES = __IMAGES__, DIMS = __DIMS__;
  const ov = document.getElementById("sp-ov"), img = document.getElementById("sp-img"),
        hud = document.getElementById("sp-hud");
  const ckey = c => DIMS.map(d => d.name + "=" + c[d.name]).join("|");
  const LUT = {}; IMAGES.forEach(im => { LUT[ckey(im.coord)] = im.src; });
  let cur = {}, active = 0, scale = 1, tx = 0, ty = 0, curW = 0, curH = 0;
  let drag = false, lx = 0, ly = 0;
  function place(){  // size the raster via width/height (keeps image-rendering:pixelated crisp,
    // unlike a CSS transform which the GPU bilinear-filters at high zoom) + left/top for pan.
    img.style.left = tx + "px"; img.style.top = ty + "px";
    img.style.width = (img.naturalWidth * scale) + "px";
    img.style.height = (img.naturalHeight * scale) + "px";
  }
  function fit(){  // center + scale-to-fit
    const vw = ov.clientWidth, vh = ov.clientHeight, nw = img.naturalWidth || 1, nh = img.naturalHeight || 1;
    scale = Math.min(vw / nw, vh / nh) * 0.95; tx = (vw - nw * scale) / 2; ty = (vh - nh * scale) / 2; place();
  }
  function shown(){  // same-size image -> keep the current zoom/pan (scrub stays put); else re-fit
    if (img.naturalWidth === curW && img.naturalHeight === curH) place(); else fit();
    curW = img.naturalWidth; curH = img.naturalHeight;
  }
  function drawHud(){
    const dims = DIMS.map((d, i) => {
      const v = cur[d.name], idx = d.values.indexOf(v);
      return "<div class='" + (i === active ? "sp-on" : "") + "'>" +
             d.name + ": " + v + " (" + (idx + 1) + "/" + d.values.length + ")</div>";
    }).join("") || "<div>(one image)</div>";
    hud.innerHTML = dims + "<div class='sp-hint'>↑↓ dim · ←→ move · scroll zoom · 0 fit · esc</div>";
  }
  function show(){
    drawHud();
    const src = LUT[ckey(cur)];
    if (!src) { img.style.visibility = "hidden"; return; }  // a combo that doesn't exist -> blank
    img.style.visibility = "visible"; img.src = "/file/" + src;
    if (img.complete) shown(); else img.onload = shown;
  }
  window.spOpen = function(i){
    cur = Object.assign({}, IMAGES[i].coord); active = 0; curW = 0; curH = 0;  // curW=0 -> first fits
    ov.style.display = "block"; show();
  };
  const close = () => { ov.style.display = "none"; };
  function step(delta){
    if (!DIMS.length) return;
    const d = DIMS[active], vals = d.values;
    let idx = vals.indexOf(cur[d.name]);
    idx = Math.max(0, Math.min(vals.length - 1, idx + delta));
    cur[d.name] = vals[idx]; show();
  }
  document.addEventListener("keydown", e => {
    if (ov.style.display === "none") return;
    if (e.key === "Escape") close();
    else if (e.key === "ArrowUp" && DIMS.length) { active = (active - 1 + DIMS.length) % DIMS.length; drawHud(); }
    else if (e.key === "ArrowDown" && DIMS.length) { active = (active + 1) % DIMS.length; drawHud(); }
    else if (e.key === "ArrowLeft") step(-1);
    else if (e.key === "ArrowRight") step(1);
    else if (e.key === "0") fit();
    else return;
    e.preventDefault();
  });
  ov.addEventListener("wheel", e => {  // zoom toward the cursor; gentle, proportional to scroll
    e.preventDefault();
    const r = ov.getBoundingClientRect(), px = e.clientX - r.left, py = e.clientY - r.top;
    const f = Math.exp(-e.deltaY * 0.0008);
    tx = px - (px - tx) * f; ty = py - (py - ty) * f; scale *= f; place();
  }, { passive: false });
  ov.addEventListener("mousedown", e => {  // preventDefault stops the browser's native image drag
    if (e.target !== img) return;
    drag = true; lx = e.clientX; ly = e.clientY; ov.style.cursor = "grabbing"; e.preventDefault();
  });
  window.addEventListener("mousemove", e => {
    if (!drag) return; tx += e.clientX - lx; ty += e.clientY - ly; lx = e.clientX; ly = e.clientY; place();
  });
  window.addEventListener("mouseup", () => { drag = false; ov.style.cursor = "grab"; });
  ov.addEventListener("click", e => { if (e.target === ov) close(); });  // backdrop click closes
})();
"""


def _infer_image_dims(pngs: "list[Path]", base: Path) -> "tuple[list[dict], list[dict]]":
    """Factor a set of PNG filenames into navigable dimensions. Each numeric-labelled token
    (``z10500`` -> dim "z" = 10500) becomes a numeric dimension; the digit-stripped stem
    (``mae_probability_z``) is the categorical "series" dimension. Returns (dims, images) where
    dims is [{name, values(sorted)}] keeping only dimensions that actually vary, and images is
    [{src, name, coord}] with coord placing each image in that space -- the client looks an
    image up by coordinate as you scrub (see _IMAGE_JS)."""
    images: "list[dict]" = []
    label_vals: "dict[str, set[int]]" = {}
    series_vals: "set[str]" = set()
    for p in pngs:
        stem = p.stem
        coord: "dict[str, object]" = {}
        for label, num in _NUM_LABEL.findall(stem):
            coord[label] = int(num)
            label_vals.setdefault(label, set()).add(int(num))
        series = re.sub(r"\d+", "", stem)  # digit-stripped -> the categorical series key
        coord["series"] = series
        series_vals.add(series)
        images.append({"src": quote(str(p.resolve().relative_to(base))), "name": p.name, "coord": coord})
    dims: "list[dict]" = []
    if len(series_vals) > 1:
        dims.append({"name": "series", "values": sorted(series_vals)})
    for label in sorted(label_vals):
        if len(label_vals[label]) > 1:
            dims.append({"name": label, "values": sorted(label_vals[label])})
    return dims, images


def _images_html(pngs: "list[Path]", base: Path) -> str:
    """Clickable thumbnail grid + a single zoomable overlay viewer navigable along the
    dimensions inferred from the filenames (see _infer_image_dims / _IMAGE_JS)."""
    dims, images = _infer_image_dims(pngs, base)
    thumbs = "".join(
        f'<figure class="clik" onclick="spOpen({i})">'
        f'<img src="/file/{im["src"]}" loading="lazy">'
        f'<figcaption>{html.escape(im["name"])}</figcaption></figure>'
        for i, im in enumerate(images)
    )
    js = _IMAGE_JS.replace("__IMAGES__", json.dumps(images)).replace("__DIMS__", json.dumps(dims))
    overlay = ('<div id="sp-ov" class="sp-ov"><div id="sp-hud" class="sp-hud"></div>'
               '<img id="sp-img" class="sp-img" draggable="false"></div>')
    return thumbs + overlay + f"<script>{js}</script>"


def _artifacts_html(outdir: str) -> str:
    """Component-B: render a run dir's artifacts -- .jsonl/.csv (and array-of-records .json) as a
    table + interactive Plotly plot (axes selectable from columns), general JSON as a folding
    tree, PNGs inline, zarr trees noted (never rendered). Reads only the LOCAL mirror, so it
    shows what a `spearmint.remote` full pull brought down (a db-only refresh won't)."""
    root = Path(outdir)
    if not root.exists():
        return "<p class='note'>artifacts not on this machine — run <code>uv run python -m spearmint.remote</code> for a full pull</p>"
    base = Path(rundb.ROOT).resolve()

    def walk(pattern: str) -> "list[Path]":
        return [p for p in sorted(root.rglob(pattern)) if ".zarr" not in str(p)]

    # First split content: tables (jsonl/csv + array-of-records json) vs general JSON trees.
    tables: "list[tuple[str, list[str], list[dict]]]" = []  # (label, cols, rows)
    trees: "list[tuple[str, object]]" = []  # (label, parsed json)
    for f in walk("*.jsonl") + walk("*.csv"):
        try:
            cols, rows = _parse_tabular(f)
            tables.append((str(f.resolve().relative_to(base)), cols, rows))
        except (ValueError, OSError):
            trees.append((str(f.resolve().relative_to(base)), "(unreadable)"))
    for j in walk("*.json")[:20]:
        label = str(j.resolve().relative_to(base))
        try:
            obj = json.loads(j.read_text())
        except (ValueError, OSError):
            trees.append((label, "(unreadable)"))
            continue
        rec = _records(obj)
        (tables.append((label, *rec)) if rec else trees.append((label, obj)))

    parts: "list[str]" = []
    if tables:
        parts.append(_PLOT_CDN)  # load plotly once, only when there's a table to plot
        for i, (label, cols, rows) in enumerate(tables[:10]):
            parts.append(_tabular_render(label, cols, rows, i))
    for label, obj in trees:
        parts.append(f"<h3>{html.escape(label)}</h3>{_json_tree(obj)}")
    pngs = walk("*.png")[:300]
    if pngs:
        parts.append(_images_html(pngs, base))
    zarrs = sorted({str(p.resolve().relative_to(base)) for p in root.rglob("*.zarr")})
    if zarrs:
        parts.append("<p class='note'>zarr volumes (not rendered): " + ", ".join(html.escape(z) for z in zarrs) + "</p>")
    return "".join(parts) or "<p class='note'>no renderable artifacts in this run dir</p>"


def _run_page(job_key: str) -> str:
    """Per-run component-B report: metadata, the err log (link + tail) when failed, and the
    artifact panels. Linked from every stage in the status table."""
    status = rundb._latest("status", job_key, None)
    if status is None:
        return _page(f"<p>no runs for {html.escape(job_key)}</p>", live=False)
    outdir = rundb.latest_outdir(job_key) or ""
    commit = (rundb._latest("commit_id", job_key, None) or "")[:12]
    started = rundb._latest("started_at", job_key, None) or ""
    meta = (
        f"<p><a href='/'>&larr; all stages</a></p>"
        f"<h2>{html.escape(job_key)} "
        f'<span class="badge {report._status_class_for(status)}">{html.escape(status)}</span></h2>'
        f"<p class='note'>commit {html.escape(commit)} · started {html.escape(started)} · "
        f"<code>{html.escape(outdir)}</code></p>"
    )
    err = ""
    if status == "failed":
        log_rel = report.lsf_log_relpath(job_key)
        log_abs = Path(rundb.ROOT) / log_rel
        tail = ""
        if log_abs.exists():
            tail = "".join(log_abs.read_text(errors="replace").splitlines(keepends=True)[-50:])
        err = (
            f"<h3>err log <a href='/file/{quote(log_rel)}'>({html.escape(log_rel)})</a></h3>"
            f"<pre class='err'>{html.escape(tail) or '(log not pulled)'}</pre>"
        )
    body = meta + err + _artifacts_html(outdir)
    return _page(body, live=False)


class _Handler(http.server.BaseHTTPRequestHandler):
    def _send(self, body: str, content_type: str = "text/html; charset=utf-8") -> None:
        self._send_bytes(body.encode(), content_type)

    def _send_bytes(self, payload: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _serve_file(self, relpath: str) -> None:
        try:
            path = _safe(relpath)
        except AssertionError:
            self.send_error(403)
            return
        if not path.is_file():
            self.send_error(404)
            return
        ctype = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        # logs/text: view inline in the browser rather than download
        if ctype == "application/octet-stream" and path.suffix in (".log", ".out", ".err", ".txt"):
            ctype = "text/plain; charset=utf-8"
        self._send_bytes(path.read_bytes(), ctype)

    def do_GET(self) -> None:
        if self.path == "/table":
            self._send(_table())
        elif self.path == "/":
            self._send(_page(_table(), live=True))
        elif self.path.startswith("/file/"):
            self._serve_file(unquote(self.path[len("/file/"):]))
        elif self.path.startswith("/run/"):
            self._send(_run_page(unquote(self.path[len("/run/"):])))
        else:
            self.send_error(404)

    def log_message(self, format: str, *args) -> None:  # quiet -- no per-request terminal logging
        pass


def main() -> None:
    from . import _cli

    _cli.help_if_asked(__doc__)
    extra = [a for a in sys.argv[1:] if a not in ("--remote", "--no-browser")]
    if extra:
        _cli.usage_error(__doc__, f"unexpected args {extra} (only --remote / --no-browser)")
    global _REMOTE
    _REMOTE = "--remote" in sys.argv[1:]
    # _Server (allow_reuse_address=False) makes a still-LIVE dashboard on this port a hard, loud
    # bind failure rather than a silent coexist (the stale-server trap). The server always runs
    # in the FOREGROUND (serve_forever below, ctrl-c to stop), so the process holding the port is
    # always visible and killable.
    try:
        server = _Server(("127.0.0.1", PORT), _Handler)
    except OSError as e:
        raise SystemExit(
            f"port {PORT} is already in use ({e.strerror}) -- a dashboard is likely already "
            f"running. Free it with:  lsof -ti tcp:{PORT} | xargs kill"
        )
    url = f"http://127.0.0.1:{PORT}/"
    print(f"spearmint dashboard: {url}  (ctrl-c to stop)", flush=True)
    if "--no-browser" not in sys.argv[1:]:
        webbrowser.open(url)
    server.serve_forever()


if __name__ == "__main__":
    main()
