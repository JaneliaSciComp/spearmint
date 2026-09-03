"""``spearmint browse``: one browser UI for run ledgers AND plain results directories.

    spearmint browse [dir] [--port N] [-n/--no-browser]   # or: python -m spearmint.dashboard ...

``dir`` defaults to <git root of cwd>/output_rundb. If it holds a ``rundb.db``, it's a spearmint
ledger: the home page is the auto-refreshing status table (experiments x stages,
wip/PEND/RUN/done/failed, live durations, staleness), each stage linking to a per-run page --
ledger metadata, the err log when failed, the run dir's artifacts. Without a ledger it's a plain
results tree: the home page lists every content-bearing directory (same one-hop overview, minus
the run metadata), each linking to that directory's rendered page. Artifact rendering is
explorer.py either way (tables + plots, JSON trees, zoomable images).

Binds 127.0.0.1 only -- run it where the data lives (e.g. on the cluster, next to the live
ledger) and connect through the ssh tunnel command printed at startup.
"""

import html
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlsplit

from . import explorer
from . import report
from . import rundb
from . import viz

PORT = 8766
REFRESH_SECONDS = 10  # status-table auto-refresh (ledger mode's home page only)

# Status-table styling on top of explorer.STYLE (which carries the theme + artifact panels).
_STYLE = """
  #meta { color: #8b949e; margin-bottom: 12px; }
  /* experiments table: blue shortname/name/title carry the structure; row click selects */
  td.short { color: #58a6ff; font-weight: 600; }
  td.full { color: #58a6ff; }
  td.rep { color: #8b949e; } td.rep a { color: inherit; }
  tr.exp { cursor: pointer; }
  tr.exp.sel td, #stages tr.sel td { background: #161b22; }
  #stages tr { cursor: pointer; }
  #stages, #runs { margin-top: 22px; }
  /* stage links muted -- the blue experiment rows carry the visual structure */
  td.key a { color: #8b949e; } td.key a:hover { color: #c9d1d9; }
  td.key { color: #8b949e; } td.stale { color: #8b949e; }
  .mark { color: #8b949e; }  /* trailing content-kind glyphs (explorer.CONTENT_SYMBOLS) */
  .badge { padding: 1px 7px; border-radius: 10px; font-size: 11px; color: #0d1117; }
  .done { background: #3fb950; } .failed { background: #f85149; color: #fff; }
  .run { background: #58a6ff; } .pend { background: #d29922; }
  .skipped { background: #6e7681; } .abandoned { background: #30363d; color: #c9d1d9; }
  pre { background: #161b22; border: 1px solid #30363d; padding: 10px; overflow-x: auto; border-radius: 6px; }
  pre.err { color: #ffa198; }
  tr.subbreak td { padding-top: 18px; }  /* vertical break between sub-experiments (e00/short | e00/smoke) */
  th { cursor: pointer; user-select: none; }
"""

# Click a column header -> sort that table by it (toggle direction, ▲/▼ sigil). The
# experiments table sorts as one flat block; the stages table sorts WITHIN blocks delimited
# by experiment (data-grp change) and sub-experiment tier (subbreak), so tiers never
# intermix and the subbreak spacing re-pins to each block's new first row. Sort state and
# the selected experiment live in JS globals and re-apply after every /table swap (pull()
# calls spSort + spSel). Built as a plain string (no f-string brace doubling).
_SORT_JS = """
(function(){
  var SORT = {};  // table id -> [col, dir]
  function key(td){
    var t = td.textContent.trim();
    var m = t.match(/^(?:(\\d+) days?, )?(\\d+):(\\d\\d):(\\d\\d)$/);  // timedelta strings
    if (m) return ((+m[1] || 0) * 86400) + (+m[2]) * 3600 + (+m[3]) * 60 + (+m[4]);
    if (t !== "" && !isNaN(+t)) return +t;
    return t.toLowerCase();
  }
  function apply(table){
    var st = SORT[table.id];
    table.querySelectorAll("th").forEach(function(th, i){
      th.textContent = th.textContent.replace(/ [▲▼]$/, "");
      if (st && i === st[0]) th.textContent += st[1] > 0 ? " ▲" : " ▼";
      th.onclick = function(){
        SORT[table.id] = (st && st[0] === i) ? [i, -st[1]] : [i, 1];
        apply(table);
      };
    });
    if (!st) return;
    var rows = Array.prototype.filter.call(table.querySelectorAll("tr"),
                                           function(r){ return !r.querySelector("th"); });
    var blocks = [], prev = null;
    rows.forEach(function(r){
      // Sort segments: #exps one flat block; #stages per experiment + subbreak tier;
      // #runs per stage (rows of different job_keys never intermix).
      var pk = table.id === "runs" ? r.dataset.key : r.dataset.grp;
      var brk = table.id !== "exps" && (pk !== prev || r.classList.contains("subbreak"));
      if (!blocks.length || brk) blocks.push([]);
      blocks[blocks.length - 1].push(r);
      prev = pk;
    });
    var body = table.tBodies[0] || table, prevGrp = null;
    blocks.forEach(function(block){
      block.sort(function(a, b){
        var ka = key(a.cells[st[0]]), kb = key(b.cells[st[0]]);
        return (ka < kb ? -1 : ka > kb ? 1 : 0) * st[1];
      });
      block.forEach(function(r, ri){
        // subbreak = later tier of the SAME experiment; a new experiment's first block isn't one
        if (table.id === "stages") r.classList.toggle("subbreak", ri === 0 && r.dataset.grp === prevGrp);
        body.appendChild(r);
      });
      prevGrp = block[block.length - 1].dataset.grp;
    });
  }
  function applyAll(){ document.querySelectorAll("#table table").forEach(apply); }
  window.spSort = applyAll;

  // Selecting a row in the experiments table shows that experiment's stages in the stages
  // table; selecting a stage row shows its ledger attempts in the runs table (click again to
  // hide; switching experiment clears it). First paint selects the server-suggested
  // experiment (data-open = most recent activity); after that both choices are sticky across
  // /table swaps.
  var sel = null, selInit = false, selKey = null;
  function select(){
    var exps = document.getElementById("exps"), stages = document.getElementById("stages"),
        runs = document.getElementById("runs");
    if (!exps || !stages) return;
    if (!selInit) {
      var d = exps.querySelector("tr[data-open]");
      sel = d ? d.dataset.grp : null;
      selInit = true;
    }
    exps.querySelectorAll("tr.exp").forEach(function(r){
      r.classList.toggle("sel", r.dataset.grp === sel);
      r.onclick = function(e){
        if (e.target.closest("a")) return;  // report links still navigate
        sel = r.dataset.grp;
        selKey = null;
        select();
      };
    });
    stages.querySelectorAll("tr").forEach(function(r){
      if (!r.dataset.grp) return;  // the <th> header row
      r.style.display = r.dataset.grp === sel ? "" : "none";
      r.classList.toggle("sel", r.dataset.key === selKey);
      r.onclick = function(e){
        if (e.target.closest("a")) return;  // stage/badge links still navigate
        selKey = (selKey === r.dataset.key) ? null : r.dataset.key;
        select();
      };
    });
    if (runs) {
      runs.style.display = selKey ? "" : "none";
      runs.querySelectorAll("tr").forEach(function(r){
        if (!r.dataset.key) return;  // the <th> header row
        r.style.display = r.dataset.key === selKey ? "" : "none";
      });
    }
  }
  window.spSel = select;

  // Arrow keys walk the selection through the experiments table in its CURRENT (sorted)
  // row order; edges clamp rather than wrap.
  document.addEventListener("keydown", function(e){
    if (e.key !== "ArrowUp" && e.key !== "ArrowDown") return;
    var exps = document.getElementById("exps");
    if (!exps) return;
    var rows = Array.prototype.slice.call(exps.querySelectorAll("tr.exp"));
    if (!rows.length) return;
    e.preventDefault();  // the page must not scroll under the moving selection
    var i = rows.findIndex(function(r){ return r.dataset.grp === sel; });
    var j = i < 0 ? 0 : Math.min(rows.length - 1, Math.max(0, i + (e.key === "ArrowDown" ? 1 : -1)));
    sel = rows[j].dataset.grp;
    selKey = null;  // stage selection belongs to the experiment it was made in
    select();
  });
  applyAll();
  select();
})();
"""

_REFRESH_SCRIPT = f"""<script>
async function pull() {{
  const r = await fetch('/table');
  document.getElementById('table').innerHTML = await r.text();
  document.getElementById('meta').textContent =
    'updated ' + new Date().toLocaleTimeString() + ' ({REFRESH_SECONDS}s refresh)';
  window.spSort && window.spSort();  // re-apply each table's active column sort + sigil
  window.spSel && window.spSel();    // re-apply which experiment is selected
}}
setInterval(pull, {REFRESH_SECONDS} * 1000);
</script>"""


def _page(body: str, live: bool, title: str = "spearmint") -> str:
    """Wrap page ``body`` in the shared shell. ``live`` (only the ledger-mode home) adds the
    auto-refresh script that re-fetches /table into #table every REFRESH_SECONDS -- every other
    page passes live=False so its content is NOT clobbered on the next tick."""
    meta = '<div id="meta">loading…</div>' if live else ""
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>{html.escape(title)}</title>
<style>{explorer.STYLE}{_STYLE}</style></head><body>
<h1>{html.escape(title)}</h1>
{meta}
<div id="table">{body}</div>
<script>{_SORT_JS}</script>
{_REFRESH_SCRIPT if live else ""}</body></html>"""


# --- ledger mode (dir has a rundb.db; rundb is anchored there) ---------------------------------

# kinds_in walks a stage's outdir -- even capped it's the home page's only per-stage fs cost,
# so cache per outdir for a minute: glyphs are hints, and each home load / refresh tick was
# re-walking EVERY stage's dir over GPFS (seconds per load, paid again on browser-back).
_KINDS_CACHE: "dict[str, tuple[float, set[str]]]" = {}


def _content_kinds(r: "report.JobRow") -> "set[str]":
    """The content kinds this stage's /run page would show -- explorer.kinds_in over the row's
    outdir (cached, see _KINDS_CACHE), plus 'log' for a failed stage whose LSF err log exists.
    Takes the JobRow so the outdir comes from collect()'s single scan -- a per-stage
    latest_outdir() here meant a fresh sqlite connection per stage over GPFS."""
    kinds: "set[str]" = set()
    if r.status == "failed" and (Path(rundb.root()) / report.lsf_log_relpath(r.job_key)).exists():
        kinds.add("log")
    now = time.monotonic()
    hit = _KINDS_CACHE.get(r.outdir)
    if hit is None or hit[0] < now:
        hit = (now + 60, explorer.kinds_in(r.outdir))
        _KINDS_CACHE[r.outdir] = hit
    return kinds | hit[1]


def _reports() -> "set[str]":
    """Experiment prefixes with a driver-rendered ROOT/_reports/<prefix>/report.html
    (prefixes may contain '/'), for linking from the status table's group rows."""
    rdir = Path(rundb.root()) / rundb.REPORTS_DIR
    if not rdir.is_dir():
        return set()
    return {str(p.parent.relative_to(rdir)) for p in rdir.rglob("report.html")}


# Dashboard-sized label per report prefix, scraped from its own report.html (viz.page's
# ``data-short-title`` body attribute) rather than duplicated state -- cached like
# _KINDS_CACHE, same GPFS-cost rationale (every home load / refresh tick would otherwise
# reread every report.html).
_TITLE_CACHE: "dict[str, tuple[float, str]]" = {}


def _report_title(prefix: str) -> str:
    now = time.monotonic()
    hit = _TITLE_CACHE.get(prefix)
    if hit is not None and hit[0] > now:
        return hit[1]
    path = Path(rundb.root()) / rundb.REPORTS_DIR / prefix / "report.html"
    title = ""
    if path.exists():
        m = re.search(r'data-short-title="([^"]*)"', path.read_text(errors="replace"))
        if m:
            title = html.unescape(m.group(1))
    _TITLE_CACHE[prefix] = (now + 60, title)
    return title


def _table() -> str:
    groups = report.collect()
    kinds = {r.job_key: _content_kinds(r) for rows in groups.values() for r in rows}
    kinds = {k: v for k, v in kinds.items() if v}  # only stages that actually have something
    reports = _reports()
    titles = {prefix: _report_title(prefix) for prefix in reports}
    return report.render_html(groups, kinds=kinds, reports=reports, report_titles=titles,
                              runs=report.collect_runs())


# --- run diff ------------------------------------------------------------------------------

def _row_by_outdir(rel: str):
    """The most recent ledger row whose outdir is ``rel`` (ROOT-relative), as a dict -- or
    None (a foreign dir, or a purged row): the diff page then degrades to file sections."""
    conn = rundb._connect(readonly=True)
    row = conn.execute(
        "SELECT run_id, job_key, status, argv0, argv_rest, commit_id, diff_hash, started_at, "
        "ended_at FROM runs WHERE outdir = ? ORDER BY run_id DESC LIMIT 1", (rel,)
    ).fetchone()
    conn.close()
    if row is None:
        return None
    keys = ("run_id", "job_key", "status", "argv0", "argv_rest", "commit_id", "diff_hash",
            "started_at", "ended_at")
    return dict(zip(keys, row))


def _stored_diff(diff_hash: str) -> str:
    conn = rundb._connect(readonly=True)
    row = conn.execute("SELECT diff FROM git_diffs WHERE hash = ?", (diff_hash,)).fetchone()
    conn.close()
    return row[0] if row else ""


def _resolve_side(param: str) -> "tuple[str, str] | None":
    """?a=/?b= param -> (abs dir, ROOT-relative rel). A job_key wins (its latest done run,
    else latest any-status); otherwise a ROOT-contained directory path. None = unresolvable
    (including containment escapes -- no path disclosure, just the usage page)."""
    d = rundb.latest_outdir(param, status="done") or rundb.latest_outdir(param)
    if d is not None and Path(d).is_dir():
        return d, str(Path(d).resolve().relative_to(Path(rundb.root()).resolve()))
    try:
        p = explorer._safe(rundb.root(), param)
    except AssertionError:
        return None
    if p.is_dir():
        return str(p), str(p.relative_to(Path(rundb.root()).resolve()))
    return None


def _argv_diff_html(row_a, row_b, label_a: str, label_b: str) -> str:
    """Token-level argv diff from the two ledger rows: equal tokens muted, a-side deletions
    red, b-side insertions green."""
    import difflib

    ta = [row_a["argv0"], *json.loads(row_a["argv_rest"])]
    tb = [row_b["argv0"], *json.loads(row_b["argv_rest"])]
    la: "list[str]" = []
    lb: "list[str]" = []
    for op, i1, i2, j1, j2 in difflib.SequenceMatcher(a=ta, b=tb, autojunk=False).get_opcodes():
        cls_a, cls_b = ("note", "note") if op == "equal" else ("dr", "da")
        la += [f'<span class="{cls_a}">{html.escape(t)}</span>' for t in ta[i1:i2]]
        lb += [f'<span class="{cls_b}">{html.escape(t)}</span>' for t in tb[j1:j2]]
    return (f"<h3>argv</h3><pre>{html.escape(label_a)}: {' '.join(la)}\n"
            f"{html.escape(label_b)}: {' '.join(lb)}</pre>")


def _code_section(row_a, row_b) -> str:
    """Commit ids (+ git log summary when the checkout is reachable) and the stored
    working-copy diffs -- 'was this the same code?' at a glance."""
    ca, cb = row_a["commit_id"], row_b["commit_id"]
    parts = ["<h3>code</h3>"]
    if ca == cb:
        parts.append(f"<p class='note'>same commit ✓ {html.escape(ca[:12])}</p>")
    else:
        parts.append(f"<p>commits differ: <code>{html.escape(ca[:12])}</code> → "
                     f"<code>{html.escape(cb[:12])}</code></p>")
        anchor = rundb._ANCHOR
        repo = (anchor.repo or anchor.root) if anchor else "."
        got = subprocess.run(["git", "-C", repo, "log", "--oneline", "--no-decorate",
                              f"{ca}..{cb}"], capture_output=True, text=True)
        if got.returncode == 0 and got.stdout.strip():
            parts.append(f"<pre>{html.escape(got.stdout.strip())}</pre>")
        else:
            parts.append("<p class='note'>(git log unavailable here)</p>")
    if row_a["diff_hash"] == row_b["diff_hash"]:
        clean = _stored_diff(row_a["diff_hash"]).strip() == ""
        parts.append(f"<p class='note'>working-copy diff: "
                     f"{'both clean ✓' if clean else 'identical on both sides'}</p>")
    else:
        parts.append("<p class='note'>working-copy diffs differ:</p>")
        for label, row in (("a", row_a), ("b", row_b)):
            text = _stored_diff(row["diff_hash"]).strip() or "(clean working copy)"
            parts.append(f"<details><summary>{label} diff</summary>"
                         f"<pre>{html.escape(text)}</pre></details>")
    return "".join(parts)


def _diff_form(a_rel: str = "") -> str:
    return (f'<form style="display:inline" action="/diff">'
            f'<input type="hidden" name="a" value="{html.escape(a_rel)}"/>'
            f'diff vs <input name="b" size="30" placeholder="job_key or run dir"/></form>')


def _diff_page(query: str, ledger: bool, base: str) -> str:
    """The /diff page (a standalone viz.page -- plot runtime + lightbox included). Sides from
    ?a=&b=; ledger mode resolves job_keys and adds meta/argv/code sections; both modes get
    explorer.diff_dirs. Unresolvable input degrades to the usage form, never an error page."""
    qs = parse_qs(query)
    a_param = (qs.get("a") or [""])[0]
    b_param = (qs.get("b") or [""])[0]
    usage = f"<p class='note'>compare two runs: {_diff_form(a_param)}</p>"
    if not a_param or not b_param:
        return viz.page(usage, title="diff")
    if ledger:
        ra, rb = _resolve_side(a_param), _resolve_side(b_param)
    else:
        def _plain(p: str):
            try:
                d = explorer._safe(base, p)
            except AssertionError:
                return None
            return (str(d), str(d.relative_to(Path(base).resolve()))) if d.is_dir() else None
        ra, rb = _plain(a_param), _plain(b_param)
    if ra is None or rb is None:
        bad = a_param if ra is None else b_param
        return viz.page(f"<p class='note'>can't resolve {html.escape(bad)!s} "
                        f"(job_key or run dir?)</p>" + usage, title="diff")
    (dir_a, rel_a), (dir_b, rel_b) = ra, rb
    label_a, label_b = explorer._labels(rel_a, rel_b)
    parts = []  # viz.page's shell carries the "⌂ dashboard" home link now
    if ledger:
        row_a, row_b = _row_by_outdir(rel_a), _row_by_outdir(rel_b)
        if row_a and row_b:
            trs = "".join(
                f"<tr><td class='note'>{k}</td><td>{html.escape(str(f(row_a)))}</td>"
                f"<td>{html.escape(str(f(row_b)))}</td></tr>"
                for k, f in (
                    ("job_key", lambda r: r["job_key"]), ("run", lambda r: r["run_id"]),
                    ("status", lambda r: r["status"]),
                    ("started", lambda r: report.fmt_ts(r["started_at"])),
                    ("wall", lambda r: rundb._duration(r["started_at"], r["ended_at"])),
                )
            )
            parts.append(f'<table class="metrics"><tr><th></th><th>{html.escape(label_a)}</th>'
                         f"<th>{html.escape(label_b)}</th></tr>{trs}</table>")
            parts.append(_argv_diff_html(row_a, row_b, label_a, label_b))
            parts.append(_code_section(row_a, row_b))
        else:
            gone = rel_a if not row_a else rel_b
            parts.append(f"<p class='note'>no ledger row for {html.escape(gone)} -- "
                         f"file comparison only</p>")
    parts.append(explorer.diff_dirs(dir_a, dir_b, base, label_a, label_b))
    return viz.page(*parts, title=f"{label_a} vs {label_b}")


def _previous_outdir(job_key: str, latest_rel: str) -> "str | None":
    """The job_key's most recent DONE outdir before ``latest_rel`` (distinct dirs only --
    extend-mode rows share outdirs), for the run page's 'diff vs previous run' link."""
    conn = rundb._connect(readonly=True)
    rows = conn.execute("SELECT outdir FROM runs WHERE job_key = ? AND status = 'done' "
                        "ORDER BY run_id DESC", (job_key,)).fetchall()
    conn.close()
    seen: "list[str]" = []
    for (out,) in rows:
        if out not in seen:
            seen.append(out)
    return next((o for o in seen if o != latest_rel), None)


def _run_page(job_key: str) -> str:
    """Per-run page: ledger metadata, the err log (link + tail) when failed, and the run dir's
    artifact panels (explorer.render_dir). Linked from every stage in the status table."""
    status = rundb._latest("status", job_key, None)
    if status is None:
        return _page(f"<p>no runs for {html.escape(job_key)}</p>", live=False)
    outdir = rundb.latest_outdir(job_key) or ""
    commit = (rundb._latest("commit_id", job_key, None) or "")[:12]
    started = report.fmt_ts(rundb._latest("started_at", job_key, None))
    # No h2 repeating the job_key -- the page shell's h1 already carries it (title= below);
    # the badge rides the meta line instead.
    meta = (
        f"<p><a href='/'>&larr; all stages</a></p>"
        f'<p class="note"><span class="badge {report._status_class_for(status)}">'
        f"{html.escape(status)}</span> · commit {html.escape(commit)} · "
        f"started {html.escape(started)} · <code>{html.escape(outdir)}</code></p>"
    )
    rel = str(Path(outdir).resolve().relative_to(Path(rundb.root()).resolve())) if outdir else ""
    prev = _previous_outdir(job_key, rel) if rel else None
    prev_link = (f'<a href="/diff?a={quote(prev)}&b={quote(rel)}">diff vs previous run</a> · '
                 if prev else "")
    compare = f"<p class='note'>{prev_link}{_diff_form(rel)}</p>"
    err = ""
    if status == "failed":
        log_rel = report.lsf_log_relpath(job_key)
        log_abs = Path(rundb.root()) / log_rel
        if log_abs.exists():
            tail = "".join(log_abs.read_text(errors="replace").splitlines(keepends=True)[-50:])
            err = (
                f"<h3>err log <a href='/file/{quote(log_rel)}'>({html.escape(log_rel)})</a></h3>"
                f"<pre class='err'>{html.escape(tail)}</pre>"
            )
        else:  # locally-run stage: its output went to the driver's stdout, there is no file
            err = "<p class='note'>no err log file (stage wasn't LSF-launched; see the driver's stdout)</p>"
    body = meta + compare + err + explorer.render_dir(outdir, rundb.root())
    return _page(body, live=False, title=job_key)


class _LedgerHandler(explorer.Handler):
    def do_GET(self) -> None:
        if self.path == "/table":
            self._send(_table())
        elif self.path == "/":
            self._send(_page(_table(), live=True, title="spearmint status"))
        elif self.path.startswith("/file/"):
            self._serve_file(unquote(self.path[len("/file/"):]), rundb.root())
        elif self.path.startswith("/run/"):
            self._send(_run_page(unquote(self.path[len("/run/"):])))
        elif self.path.startswith("/dir/"):
            # A SPECIFIC run's directory (the runs table links here) -- /run/<job_key> only
            # ever shows the latest attempt's dir.
            rel = unquote(self.path[len("/dir/"):])
            try:
                d = explorer._safe(rundb.root(), rel)
            except AssertionError:
                self.send_error(403)
                return
            if not d.is_dir():
                self.send_error(404)
                return
            body = (f"<p><a href='/'>&larr; all stages</a></p>"
                    + explorer.render_dir(str(d), rundb.root()))
            self._send(_page(body, live=False, title=rel))
        elif self.path.startswith("/diff"):
            self._send(_diff_page(urlsplit(self.path).query, ledger=True, base=rundb.root()))
        else:
            self.send_error(404)


# --- plain mode (no rundb.db; just a results tree) ---------------------------------------------

_BASE = ""  # the served directory; set in main before serve()


class _PlainHandler(explorer.Handler):
    def do_GET(self) -> None:
        if self.path == "/":
            self._send(_page(explorer.listing_html(_BASE), live=False, title=_BASE))
        elif self.path.startswith("/dir/"):
            rel = unquote(self.path[len("/dir/"):])
            try:
                d = explorer._safe(_BASE, rel)
            except AssertionError:
                self.send_error(403)
                return
            if not d.is_dir():
                self.send_error(404)
                return
            body = (f"<p><a href='/'>&larr; all directories</a></p><h2>{html.escape(rel)}</h2>"
                    + explorer.render_dir(str(d), _BASE))
            self._send(_page(body, live=False, title=rel))
        elif self.path.startswith("/file/"):
            self._serve_file(unquote(self.path[len("/file/"):]), _BASE)
        elif self.path.startswith("/diff"):
            self._send(_diff_page(urlsplit(self.path).query, ledger=False, base=_BASE))
        else:
            self.send_error(404)


def main() -> None:
    from . import _cli

    _cli.help_if_asked(__doc__)
    argv = sys.argv[1:]
    port = PORT
    if "--port" in argv:
        i = argv.index("--port")
        assert i + 1 < len(argv), "--port needs a value"
        port = int(argv[i + 1])
        del argv[i:i + 2]
    open_browser = not ("--no-browser" in argv or "-n" in argv)
    # --takeover is now the default; accepted silently for muscle memory.
    argv = [a for a in argv if a not in ("--no-browser", "-n", "--takeover")]
    if len(argv) > 1:
        _cli.usage_error(__doc__, f"unexpected args {argv[1:]} (at most one directory)")
    d = Path(argv[0]).resolve() if argv else Path(rundb.default_root())
    assert d.is_dir(), f"{d} is not a directory -- pass the ledger or results dir to browse"
    if (d / "rundb.db").exists():
        rundb.anchor(str(d))
        handler: "type[explorer.Handler]" = _LedgerHandler
    else:
        global _BASE
        _BASE = str(d)
        handler = _PlainHandler
    explorer.serve(handler, port, open_browser)


if __name__ == "__main__":
    main()
