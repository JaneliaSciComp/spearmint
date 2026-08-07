"""Browser viewer for a results directory: .jsonl/.csv (and array-of-records .json) rendered
as a table + interactive Plotly plot (axes selectable from columns), general JSON as a folding
tree, PNGs as a clickable thumbnail grid with a zoomable overlay navigable along dimensions
inferred from the filenames (raw_z10500.png -> series "raw_z", z=10500). The directory renders
FLATTENED (recursive, with visible caps) as one page -- built for run-dir-sized result trees,
not for exploring large nested hierarchies. Knows nothing of spearmint's ledger -- dashboard.py
embeds render_dir() on each run's page, and standalone this serves ANY directory:

    spearmint browse <dir> [--port N] [--no-browser]   # or: python -m spearmint.explorer ...

Binds 127.0.0.1 only -- when the server runs on another machine (e.g. the cluster, next to the
data), connect through the ssh tunnel command printed at startup.
"""

import html
import http.server
import json
import mimetypes
import os
import re
import socket
import sys
import webbrowser
from pathlib import Path
from urllib.parse import quote

PORT = 8767  # standalone default; distinct from the dashboard's so both can run at once

# file extension -> content kind. .jsonl/.csv are "table" (row-oriented -> table + plot); a
# .json is "json" (folding tree) unless it's an array of records (peeked below), which renders
# as a table too.
_EXT_KIND = {".png": "png", ".jsonl": "table", ".csv": "table", ".json": "json"}

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

# Directory served by the standalone entrypoint (set in main); the dashboard never touches this,
# it passes its own base explicitly.
_DIR: "str | None" = None


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
    return "".join(parts) or "<p class='note'>no renderable files in this directory</p>"


class Server(http.server.ThreadingHTTPServer):
    # allow_reuse_address=False so a still-LIVE server on this port is a hard bind failure
    # rather than a silent coexist (the stale-server trap). A subclass, NOT a mutation of the
    # shared http.server.ThreadingHTTPServer class -- other UIs use that same class, so setting
    # the flag on it globally would leak this choice into anything co-imported.
    allow_reuse_address = False


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
    server.serve_forever()


def _page(title: str, body: str) -> str:
    """Minimal page shell for the standalone explorer (the dashboard has its own shell, with
    auto-refresh and status styling)."""
    return (f'<!doctype html><html><head><meta charset="utf-8"><title>{html.escape(title)}</title>'
            f"<style>{STYLE}</style></head><body><h1>{html.escape(title)}</h1>{body}</body></html>")


class _Handler(Handler):
    def do_GET(self) -> None:
        assert _DIR is not None  # set in main before serve()
        if self.path == "/":
            self._send(_page(_DIR, render_dir(_DIR, _DIR)))
        elif self.path.startswith("/file/"):
            from urllib.parse import unquote

            self._serve_file(unquote(self.path[len("/file/"):]), _DIR)
        else:
            self.send_error(404)


def main() -> None:
    from . import _cli  # stdlib-only helpers; explorer deliberately never imports config/rundb

    _cli.help_if_asked(__doc__)
    argv = sys.argv[1:]
    port = PORT
    if "--port" in argv:
        i = argv.index("--port")
        assert i + 1 < len(argv), "--port needs a value"
        port = int(argv[i + 1])
        del argv[i:i + 2]
    open_browser = "--no-browser" not in argv
    argv = [a for a in argv if a != "--no-browser"]
    if len(argv) != 1:
        _cli.usage_error(__doc__, "need exactly one directory to serve")
    d = Path(argv[0]).resolve()
    assert d.is_dir(), f"{argv[0]} is not a directory"
    global _DIR
    _DIR = str(d)
    serve(_Handler, port, open_browser)


if __name__ == "__main__":
    main()
