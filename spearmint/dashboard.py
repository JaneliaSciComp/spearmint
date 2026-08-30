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
import subprocess
import sys
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
  tr.group td { padding-top: 16px; color: #58a6ff; font-weight: 600; }
  /* stage links muted -- the blue bold group rows carry the visual structure */
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

# Click a column header -> sort by it (toggle direction, ▲/▼ sigil on the active header).
# Rows sort WITHIN their blocks -- group rows and subbreak rows delimit segments, so
# experiments and sub-experiment tiers never intermix; the subbreak spacing is re-pinned to
# each block's new first row. State lives in JS globals and re-applies after every /table
# swap (pull() calls spSort). Built as a plain string (no f-string brace doubling).
_SORT_JS = """
(function(){
  var sortCol = null, sortDir = 1;
  function key(td){
    var t = td.textContent.trim();
    var m = t.match(/^(?:(\\d+) days?, )?(\\d+):(\\d\\d):(\\d\\d)$/);  // timedelta strings
    if (m) return ((+m[1] || 0) * 86400) + (+m[2]) * 3600 + (+m[3]) * 60 + (+m[4]);
    if (t !== "" && !isNaN(+t)) return +t;
    return t.toLowerCase();
  }
  function apply(){
    var table = document.querySelector("#table table");
    if (!table) return;
    table.querySelectorAll("th").forEach(function(th, i){
      th.textContent = th.textContent.replace(/ [▲▼]$/, "");
      if (i === sortCol) th.textContent += sortDir > 0 ? " ▲" : " ▼";
      th.onclick = function(){
        sortDir = (sortCol === i) ? -sortDir : 1;
        sortCol = i;
        apply();
      };
    });
    if (sortCol === null) return;
    var rows = Array.prototype.filter.call(table.querySelectorAll("tr"),
                                           function(r){ return !r.querySelector("th"); });
    var groups = [];
    rows.forEach(function(r){
      if (r.classList.contains("group")) { groups.push({header: r, blocks: [[]]}); return; }
      if (!groups.length) groups.push({header: null, blocks: [[]]});
      var g = groups[groups.length - 1];
      if (r.classList.contains("subbreak") && g.blocks[g.blocks.length - 1].length) g.blocks.push([]);
      g.blocks[g.blocks.length - 1].push(r);
    });
    var body = table.tBodies[0] || table;
    groups.forEach(function(g){
      if (g.header) body.appendChild(g.header);
      g.blocks.forEach(function(block, bi){
        block.sort(function(a, b){
          var ka = key(a.cells[sortCol]), kb = key(b.cells[sortCol]);
          return (ka < kb ? -1 : ka > kb ? 1 : 0) * sortDir;
        });
        block.forEach(function(r, ri){
          r.classList.toggle("subbreak", bi > 0 && ri === 0);
          body.appendChild(r);
        });
      });
    });
  }
  window.spSort = apply;
  apply();
})();
"""

_REFRESH_SCRIPT = f"""<script>
async function pull() {{
  const r = await fetch('/table');
  document.getElementById('table').innerHTML = await r.text();
  document.getElementById('meta').textContent =
    'updated ' + new Date().toLocaleTimeString() + ' ({REFRESH_SECONDS}s refresh)';
  window.spSort && window.spSort();  // re-apply the active column sort + sigil after the swap
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

def _content_kinds(job_key: str, status: str) -> "set[str]":
    """The content kinds this stage's /run page would show -- explorer.kinds_in over its latest
    outdir, plus 'log' for a failed stage whose LSF err log exists."""
    kinds: "set[str]" = set()
    if status == "failed" and (Path(rundb.root()) / report.lsf_log_relpath(job_key)).exists():
        kinds.add("log")
    outdir = rundb.latest_outdir(job_key)
    if outdir:
        kinds |= explorer.kinds_in(outdir)
    return kinds


def _reports() -> "set[str]":
    """Experiment prefixes with a driver-rendered ROOT/_reports/<prefix>/report.html
    (prefixes may contain '/'), for linking from the status table's group rows."""
    rdir = Path(rundb.root()) / rundb.REPORTS_DIR
    if not rdir.is_dir():
        return set()
    return {str(p.parent.relative_to(rdir)) for p in rdir.rglob("report.html")}


def _table() -> str:
    groups = report.collect()
    kinds = {r.job_key: _content_kinds(r.job_key, r.status) for rows in groups.values() for r in rows}
    kinds = {k: v for k, v in kinds.items() if v}  # only stages that actually have something
    return report.render_html(groups, kinds=kinds, reports=_reports())


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
    back = "<p><a href='/'>&larr; back</a></p>"
    usage = f"<p class='note'>compare two runs: {_diff_form(a_param)}</p>"
    if not a_param or not b_param:
        return viz.page(back + usage, title="diff")
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
        return viz.page(back + f"<p class='note'>can't resolve {html.escape(bad)!s} "
                        f"(job_key or run dir?)</p>" + usage, title="diff")
    (dir_a, rel_a), (dir_b, rel_b) = ra, rb
    label_a, label_b = explorer._labels(rel_a, rel_b)
    parts = [back]
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
    meta = (
        f"<p><a href='/'>&larr; all stages</a></p>"
        f"<h2>{html.escape(job_key)} "
        f'<span class="badge {report._status_class_for(status)}">{html.escape(status)}</span></h2>'
        f"<p class='note'>commit {html.escape(commit)} · started {html.escape(started)} · "
        f"<code>{html.escape(outdir)}</code></p>"
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
        tail = ""
        if log_abs.exists():
            tail = "".join(log_abs.read_text(errors="replace").splitlines(keepends=True)[-50:])
        err = (
            f"<h3>err log <a href='/file/{quote(log_rel)}'>({html.escape(log_rel)})</a></h3>"
            f"<pre class='err'>{html.escape(tail) or '(log not on this machine)'}</pre>"
        )
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
