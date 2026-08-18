"""``spearmint browse``: one browser UI for run ledgers AND plain results directories.

    spearmint browse [dir] [--port N] [--no-browser]   # or: python -m spearmint.dashboard ...

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
import sys
from pathlib import Path
from urllib.parse import quote, unquote

from . import explorer
from . import report
from . import rundb

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


def _run_page(job_key: str) -> str:
    """Per-run page: ledger metadata, the err log (link + tail) when failed, and the run dir's
    artifact panels (explorer.render_dir). Linked from every stage in the status table."""
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
        log_abs = Path(rundb.root()) / log_rel
        tail = ""
        if log_abs.exists():
            tail = "".join(log_abs.read_text(errors="replace").splitlines(keepends=True)[-50:])
        err = (
            f"<h3>err log <a href='/file/{quote(log_rel)}'>({html.escape(log_rel)})</a></h3>"
            f"<pre class='err'>{html.escape(tail) or '(log not on this machine)'}</pre>"
        )
    body = meta + err + explorer.render_dir(outdir, rundb.root())
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
    open_browser = "--no-browser" not in argv
    argv = [a for a in argv if a != "--no-browser"]
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
