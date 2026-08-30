"""Rendering + serving library for results directories -- what ``spearmint browse``
(dashboard.py) is built from. .jsonl/.csv (and array-of-records .json) render as a table +
interactive Plotly plot (axes selectable from columns), general JSON as a folding tree, PNGs as
a clickable thumbnail grid with a zoomable overlay navigable along dimensions inferred from the
filenames (raw_z10500.png -> series "raw_z", z=10500). A directory renders FLATTENED
(recursive, with visible caps) as one page -- built for run-dir-sized result trees; listing()
provides the one-hop index over a bigger tree (content-bearing dirs -> their flattened pages).
Knows nothing of spearmint's ledger, config, or git -- pure stdlib over a directory."""

import html
import http.server
import json
import mimetypes
import os
import re
import socket
import webbrowser
from pathlib import Path
from urllib.parse import quote

# file extension -> content kind. .jsonl/.csv are "table" (row-oriented -> table + plot); a
# .json is "json" (folding tree) unless it's an array of records (peeked below), which renders
# as a table too; .html is a ready-made page (e.g. a viz-built report) -- linked, not inlined.
_EXT_KIND = {".png": "png", ".jsonl": "table", ".csv": "table", ".json": "json", ".html": "html"}

# content kind -> the marker glyph shown after a dir/stage name; an entry may have several.
# "log" only occurs via the dashboard (a failed stage's LSF err log).
CONTENT_SYMBOLS = {"html": "📈", "png": "🖼", "table": "📊", "json": "{}", "log": "⚠"}

_LISTING_CAP = 500  # rows on a listing page

_PLOT_CDN = '<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>'
# Render caps (a directory is rendered whole, so a huge tree is capped, loudly -- render_dir
# appends a "showing N of M" note whenever one bites).
_TABLE_CAP = 200    # rows shown in the HTML table
_PLOT_CAP = 5000    # rows shipped to the browser for the plot
_TABLES_CAP = 10    # .jsonl/.csv/records-.json panels per page
_JSON_CAP = 20      # .json files considered per page
_PNG_CAP = 300      # images per page

# Base theme + every artifact panel's styling. The dashboard prepends this to its own additions
# (status badges, group rows) so both UIs render identically.
STYLE = """
  body { font: 13px ui-monospace, monospace; background: #0d1117; color: #c9d1d9; margin: 24px; }
  h1 { font-size: 15px; font-weight: 600; }
  table { border-collapse: collapse; width: 100%; }
  th, td { text-align: left; padding: 3px 12px 3px 0; }
  th { color: #8b949e; font-weight: 600; border-bottom: 1px solid #30363d; }
  .ctrl { margin: 6px 0; color: #8b949e; }
  select { background: #161b22; color: #c9d1d9; border: 1px solid #30363d; border-radius: 4px; }
  .plot { width: 680px; height: 380px; }
  .datatable { font-size: 11px; margin: 8px 0 16px; }
  .datatable td, .datatable th { padding: 2px 10px 2px 0; border-bottom: 1px solid #21262d; }
  .jtree { margin: 6px 0 16px; }
  .jtree summary { cursor: pointer; } .jtree .jbody { margin-left: 14px; }
  .jrow { margin-left: 14px; } .jkey { color: #79c0ff; } .jmuted { color: #6e7681; }
  .jstr { color: #a5d6ff; } .jnum { color: #ffa657; } .jbool, .jnull { color: #d2a8ff; }
  a { color: #58a6ff; text-decoration: none; } a:hover { text-decoration: underline; }
  .note { color: #8b949e; } code { color: #c9d1d9; }
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

# Built with placeholder replace (not an f-string) so the JS braces don't need doubling.
_PLOT_JS = """
(function(){
  const data = __DATA__, cols = __COLS__;
  const xs = document.getElementById("__PID__x"), ys = document.getElementById("__PID__y");
  const isnum = c => data.some(r => r[c] !== "" && r[c] != null && !isNaN(+r[c]));
  // Defaults must differ: x prefers a step/epoch-ish column (else the first), y the first
  // NUMERIC column that isn't x -- otherwise metrics.jsonl opened as step-vs-step.
  xs.value = cols.find(c => ["step", "epoch", "iter", "iteration", "t"].includes(c)) || cols[0];
  ys.value = cols.find(c => c !== xs.value && isnum(c))
          || cols.find(c => c !== xs.value) || cols[cols.length - 1];
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

def _safe(base: str, relpath: str) -> Path:
    """Absolute path for a ``base``-relative request path, refusing to escape base (a
    served-file server must never hand out arbitrary filesystem paths). Raises if the resolved
    path isn't base or under it."""
    b = Path(base).resolve()
    p = (b / relpath).resolve()
    assert p == b or b in p.parents, f"path {relpath!r} escapes {base!r}"
    return p


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


def kinds_in(outdir: str) -> "set[str]":
    """The renderable content kinds under ``outdir`` -- subset of {png, table, json}: what
    render_dir would show. Empty for a missing/non-directory path. Cheap: prunes *.zarr trees
    (huge, never rendered) so it doesn't walk chunk files; a .json that's an array of records
    counts as a table (peeked, not parsed -- this can run for every stage on every dashboard
    refresh)."""
    kinds: "set[str]" = set()
    if not Path(outdir).is_dir():
        return kinds
    for dirpath, dirnames, filenames in os.walk(outdir):
        dirnames[:] = [d for d in dirnames if not d.endswith(".zarr")]
        for f in filenames:
            kind = _EXT_KIND.get(Path(f).suffix)
            if kind == "json" and _peek_char(Path(dirpath) / f) == "[":
                kind = "table"
            if kind:
                kinds.add(kind)
    return kinds


def listing(base: str) -> "list[tuple[str, set[str]]]":
    """(relpath, kinds) for every dir under ``base`` that directly holds renderable files --
    the one-hop index a no-ledger browse home shows (each entry links to that dir's flattened
    render_dir page). One pruned walk (.zarr and dot-dirs skipped); sorted by relpath, so the
    path hierarchy reads as grouping. '.' appears when base itself holds renderable files."""
    found: "dict[str, set[str]]" = {}
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = [d for d in dirnames if not d.endswith(".zarr") and not d.startswith(".")]
        kinds: "set[str]" = set()
        for f in filenames:
            kind = _EXT_KIND.get(Path(f).suffix)
            if kind == "json" and _peek_char(Path(dirpath) / f) == "[":
                kind = "table"
            if kind:
                kinds.add(kind)
        if kinds:
            found[os.path.relpath(dirpath, base)] = kinds
    return sorted(found.items())


def listing_html(base: str) -> str:
    """Home-page fragment for a plain (no-ledger) directory: the listing() table, each dir
    linking to its /dir/<relpath> page with content-kind glyphs -- the ledger-less sibling of
    the dashboard's status table."""
    rows = listing(base)
    if not rows:
        return "<p class='note'>no renderable files under this directory</p>"
    trs = []
    for rel, kinds in rows[:_LISTING_CAP]:
        marks = "".join(CONTENT_SYMBOLS[k] for k in CONTENT_SYMBOLS if k in kinds)
        trs.append(
            f'<tr><td class="key"><a href="/dir/{quote(rel)}">{html.escape(rel)}</a> '
            f'<span class="mark">{marks}</span></td></tr>'
        )
    note = (
        f"<p class='note'>showing {_LISTING_CAP} of {len(rows)} directories</p>"
        if len(rows) > _LISTING_CAP else ""
    )
    return f"<table><tr><th>directory</th></tr>{''.join(trs)}</table>{note}"


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
        f"<h3>{html.escape(str(label))}</h3>"
        f'<div class="ctrl">x <select id="{pid}x">{opts}</select> '
        f'y <select id="{pid}y">{opts}</select></div>'
        f'<div id="{pid}" class="plot"></div>'
        f'<table class="datatable"><tr>{head}</tr>{trs}</table>{note}'
        f"<script>{js}</script>"
    )


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


def render_dir(outdir: str, base: str) -> str:
    """Render a directory's contents as one HTML fragment -- .jsonl/.csv (and array-of-records
    .json) as a table + interactive Plotly plot, general JSON as a folding tree, PNGs inline,
    zarr trees noted (never rendered). Recursive with visible caps (see the cap constants).
    ``base`` anchors the /file/ links and panel labels -- file paths are served base-relative,
    so it must be (an ancestor of) ``outdir`` and the enclosing server's file root."""
    root = Path(outdir)
    if not root.exists():
        return f"<p class='note'>directory does not exist: <code>{html.escape(str(outdir))}</code></p>"
    base_path = Path(base).resolve()

    def walk(pattern: str) -> "list[Path]":
        return [p for p in sorted(root.rglob(pattern)) if ".zarr" not in str(p)]

    def rel(p: Path) -> str:
        return str(p.resolve().relative_to(base_path))

    parts_top: "list[str]" = []
    # Ready-made pages first (a viz-built report.html deserves top billing) -- linked, not
    # inlined: they're self-contained documents, best opened as their own tab.
    htmls = walk("*.html")
    if htmls:
        parts_top.append("<p>" + " · ".join(
            f'📈 <a href="/file/{quote(rel(p))}">{html.escape(rel(p))}</a>' for p in htmls[:20]
        ) + "</p>")

    # First split content: tables (jsonl/csv + array-of-records json) vs general JSON trees.
    tables: "list[tuple[str, list[str], list[dict]]]" = []  # (label, cols, rows)
    trees: "list[tuple[str, object]]" = []  # (label, parsed json)
    for f in walk("*.jsonl") + walk("*.csv"):
        try:
            cols, rows = _parse_tabular(f)
            tables.append((rel(f), cols, rows))
        except (ValueError, OSError):
            trees.append((rel(f), "(unreadable)"))
    jsons = walk("*.json")
    for j in jsons[:_JSON_CAP]:
        try:
            obj = json.loads(j.read_text())
        except (ValueError, OSError):
            trees.append((rel(j), "(unreadable)"))
            continue
        rec = _records(obj)
        (tables.append((rel(j), *rec)) if rec else trees.append((rel(j), obj)))

    parts: "list[str]" = []
    if tables:
        parts.append(_PLOT_CDN)  # load plotly once, only when there's a table to plot
        for i, (label, cols, rows) in enumerate(tables[:_TABLES_CAP]):
            parts.append(_tabular_render(label, cols, rows, i))
        if len(tables) > _TABLES_CAP:
            parts.append(f"<p class='note'>showing {_TABLES_CAP} of {len(tables)} tables</p>")
    for label, obj in trees:
        parts.append(f"<h3>{html.escape(label)}</h3>{_json_tree(obj)}")
    if len(jsons) > _JSON_CAP:
        parts.append(f"<p class='note'>showing {_JSON_CAP} of {len(jsons)} .json files</p>")
    pngs = walk("*.png")
    if pngs:
        parts.append(_images_html(pngs[:_PNG_CAP], base_path))
        if len(pngs) > _PNG_CAP:
            parts.append(f"<p class='note'>showing {_PNG_CAP} of {len(pngs)} images</p>")
    zarrs = sorted({rel(p) for p in root.rglob("*.zarr")})
    if zarrs:
        parts.append("<p class='note'>zarr volumes (not rendered): " + ", ".join(html.escape(z) for z in zarrs) + "</p>")
    return "".join(parts_top + parts) or "<p class='note'>no renderable files in this directory</p>"


# --- run diff -----------------------------------------------------------------------------
# Generic dir-vs-dir comparison (diff_dirs); the dashboard layers ledger sections (argv,
# commits, stored working diffs) on top and serves the whole thing as a viz.page.

_DIFF_PAIRS_CAP = 40          # differing file pairs rendered per diff page
_DIFF_TEXT_CAP = 256 * 1024   # bytes; larger text files -> "differs (too large)"
_UDIFF_LINES_CAP = 400        # unified-diff lines shown per file
_X_CANDIDATES = ("step", "epoch", "iter", "iteration", "t")  # overlay x column, if shared


def _labels(rel_a: str, rel_b: str) -> "tuple[str, str]":
    """Short distinguishing side labels: the relpaths minus their common leading segments --
    different job_keys read 'train_a/run00001' vs 'train_b/run00003'; same job_key reads
    'run00001' vs 'run00003'; identical paths fall back to an a:/b: prefix."""
    pa, pb = rel_a.split("/"), rel_b.split("/")
    i = 0
    while i < min(len(pa), len(pb)) - 1 and pa[i] == pb[i]:
        i += 1
    la, lb = "/".join(pa[i:]), "/".join(pb[i:])
    return (la, lb) if la != lb else (f"a:{la}", f"b:{lb}")


def _udiff(rel: str, text_a: str, text_b: str, label_a: str, label_b: str) -> str:
    """Colored unified diff of two small texts (+/- lines via viz's .da/.dr classes)."""
    import difflib

    lines = list(difflib.unified_diff(
        text_a.splitlines(), text_b.splitlines(), label_a, label_b, lineterm=""))
    shown = lines[:_UDIFF_LINES_CAP]

    def cls(l: str) -> str:
        return "da" if l.startswith("+") else ("dr" if l.startswith("-") else "")

    body = "\n".join(f'<span class="{cls(l)}">{html.escape(l)}</span>' for l in shown)
    note = (f"<p class='note'>showing {_UDIFF_LINES_CAP} of {len(lines)} diff lines</p>"
            if len(lines) > len(shown) else "")
    return f"<h3>{html.escape(rel)}</h3><pre>{body}</pre>{note}"


def _delta_table(rel: str, flat_a: "dict", flat_b: "dict", label_a: str, label_b: str) -> str:
    """Scalar JSONs side by side: metric | A | B | Δ (Δ only when both numeric); changed rows
    highlighted."""

    def fmt(v) -> str:
        if v is None:
            return "–"
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return html.escape(str(v))
        return f"{v:.4g}"

    rows = []
    for k in sorted(set(flat_a) | set(flat_b)):
        va, vb = flat_a.get(k), flat_b.get(k)
        numeric = all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in (va, vb))
        delta = f"{vb - va:+.4g}" if numeric else ""
        rows.append(f'<tr class="{"hl" if va != vb else ""}"><td>{html.escape(k)}</td>'
                    f"<td>{fmt(va)}</td><td>{fmt(vb)}</td><td>{delta}</td></tr>")
    head = (f"<tr><th>metric</th><th>{html.escape(label_a)}</th>"
            f"<th>{html.escape(label_b)}</th><th>Δ</th></tr>")
    return f'<h3>{html.escape(rel)}</h3><table class="metrics">{head}{"".join(rows)}</table>'


def diff_dirs(a: str, b: str, base: str, label_a: str, label_b: str) -> str:
    """Two directories, one comparison fragment (embed in a viz.page -- it needs viz's plot
    runtime and lightbox): files matched by IDENTICAL relpath; byte-identical ones collapse
    into a single grouped note; tabular files (.jsonl/.csv) overlay both sides' curves on ONE
    plot with side-labelled traces; scalar JSONs become a metric|A|B|Δ table; small texts
    render as colored unified diffs; same-named PNGs land in one table where the lightbox's
    ←/→ toggles the sides and ↑/↓ walks the files. One-side-only files are listed.
    Ledger-free: any two dirs, both browse modes."""
    from filecmp import cmp as _filecmp

    from . import viz

    base_path = Path(base).resolve()

    def files(root: str) -> "dict[str, Path]":
        r = Path(root)
        return {str(p.relative_to(r)): p for p in sorted(r.rglob("*"))
                if p.is_file() and ".zarr" not in str(p)}

    fa, fb = files(a), files(b)
    parts: "list[str]" = []
    for label, only in ((label_a, sorted(set(fa) - set(fb))), (label_b, sorted(set(fb) - set(fa)))):
        if only:
            parts.append(f"<p class='note'>only in {html.escape(label)}: "
                         f"{html.escape(', '.join(only[:20]))}</p>")
    common = sorted(set(fa) & set(fb))
    identical = {rel for rel in common if _filecmp(fa[rel], fb[rel], shallow=False)}
    differing = [rel for rel in common if rel not in identical]
    if identical:
        parts.append(f"<p class='note'>identical: {html.escape(', '.join(sorted(identical)))}</p>")
    if len(differing) > _DIFF_PAIRS_CAP:
        parts.append(f"<p class='note'>comparing {_DIFF_PAIRS_CAP} of "
                     f"{len(differing)} differing files</p>")

    pngs: "list[str]" = []
    for rel in differing[:_DIFF_PAIRS_CAP]:
        pa, pb = fa[rel], fb[rel]
        try:
            if pa.suffix in (".jsonl", ".csv"):
                cols_a, rows_a = _parse_tabular(pa)
                cols_b, rows_b = _parse_tabular(pb)
                if max(len(rows_a), len(rows_b)) > _PLOT_CAP:
                    parts.append(f"<p class='note'>{html.escape(rel)}: plotting first "
                                 f"{_PLOT_CAP} rows per side</p>")
                x = next((c for c in _X_CANDIDATES if c in cols_a and c in cols_b), None)
                parts.append(viz.lines({label_a: rows_a[:_PLOT_CAP], label_b: rows_b[:_PLOT_CAP]},
                                       x=x, title=rel))
            elif pa.suffix == ".json":
                obj_a, obj_b = json.loads(pa.read_text()), json.loads(pb.read_text())
                rec_a, rec_b = _records(obj_a), _records(obj_b)
                if rec_a and rec_b:  # arrays of records -> overlay like jsonl
                    parts.append(viz.lines({label_a: rec_a[1], label_b: rec_b[1]}, title=rel))
                    continue
                flat_a, flat_b = viz._flat(obj_a), viz._flat(obj_b)
                if flat_a or flat_b:
                    parts.append(_delta_table(rel, flat_a, flat_b, label_a, label_b))
                else:
                    parts.append(_udiff(rel, json.dumps(obj_a, indent=1, sort_keys=True),
                                        json.dumps(obj_b, indent=1, sort_keys=True),
                                        label_a, label_b))
            elif pa.suffix == ".png":
                pngs.append(rel)
            elif pa.suffix in (".txt", ".yaml", ".yml", ".cfg", ".toml", ".log"):
                if max(pa.stat().st_size, pb.stat().st_size) > _DIFF_TEXT_CAP:
                    parts.append(f"<p class='note'>{html.escape(rel)}: differs "
                                 f"(too large to diff)</p>")
                else:
                    parts.append(_udiff(rel, pa.read_text(errors="replace"),
                                        pb.read_text(errors="replace"), label_a, label_b))
            else:
                parts.append(f"<p class='note'>{html.escape(rel)}: differs (binary/unhandled)</p>")
        except Exception as e:  # noqa: BLE001 -- served page: one bad file degrades to a note
            parts.append(f"<p class='note'>{html.escape(rel)}: diff failed "
                         f"({html.escape(repr(e))})</p>")

    if pngs:
        head = (f"<tr><th></th><th>{html.escape(label_a)}</th>"
                f"<th>{html.escape(label_b)}</th></tr>")
        trs = "".join(
            f"<tr><td>{html.escape(rel)}</td>" + "".join(
                f'<td><img class="zoom" width="260" '
                f'src="/file/{quote(str(p.resolve().relative_to(base_path)))}"/></td>'
                for p in (fa[rel], fb[rel])
            ) + "</tr>"
            for rel in pngs
        )
        parts.append("<h3>images <span class='note'>(click one: ←/→ toggle sides, "
                     f"↑/↓ walk files)</span></h3><table>{head}{trs}</table>")
    return "".join(parts) or "<p class='note'>nothing to compare</p>"


class Server(http.server.ThreadingHTTPServer):
    # allow_reuse_address (SO_REUSEADDR) permits rebinding over TIME_WAIT remnants -- without
    # it, ctrl-c'ing a server whose tabs were recently talking to it (keep-alives, the live
    # report poller) leaves the port unbindable for ~30-60s. It does NOT weaken the
    # still-live-server protection: binding over an ACTIVE listener still fails loudly with
    # EADDRINUSE on Linux/macOS regardless (that laxity would be SO_REUSEPORT, a different
    # flag we don't set).
    allow_reuse_address = True
    # daemon_threads: request threads must never outlive the server. A browser keep-alive
    # connection (idle tab) parks its handler thread in recv; with non-daemon threads (the
    # ThreadingHTTPServer default) a ctrl-c'd/crashed-out process then HANGS on thread join --
    # invisibly alive and still holding the port. Daemon threads die with the process.
    daemon_threads = True


class Handler(http.server.BaseHTTPRequestHandler):
    """Shared HTTP plumbing for the explorer and the dashboard: response helpers, contained
    file serving, quiet logging. Subclasses define do_GET and pass their own file root to
    _serve_file."""

    def _send(self, body: str, content_type: str = "text/html; charset=utf-8") -> None:
        self._send_bytes(body.encode(), content_type)

    def _send_bytes(self, payload: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _serve_file(self, relpath: str, base: str) -> None:
        try:
            path = _safe(base, relpath)
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

    def log_message(self, format: str, *args) -> None:  # quiet -- no per-request terminal logging
        pass


def serve(handler: "type[Handler]", port: int, open_browser: bool) -> None:
    """Bind on loopback (a taken port is a hard, loud failure -- see Server) and serve forever
    in the foreground (ctrl-c to stop). Prints the URL plus the ssh tunnel command that reaches
    this server from another machine -- the sanctioned remote path; the bind itself is always
    127.0.0.1."""
    try:
        server = Server(("127.0.0.1", port), handler)
    except OSError as e:
        raise SystemExit(
            f"port {port} is already in use ({e.strerror}) -- a server is likely already "
            f"running. Free it with:  lsof -ti tcp:{port} | xargs kill"
        )
    url = f"http://127.0.0.1:{port}/"
    print(f"serving {url}  (ctrl-c to stop)", flush=True)
    print(f"from another machine: ssh -L {port}:localhost:{port} {socket.gethostname()}", flush=True)
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped", flush=True)
    finally:
        server.server_close()  # release the listener even when serve_forever raised
