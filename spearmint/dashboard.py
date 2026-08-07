"""Live browser dashboard for the spearmint ledger -- the browser sibling of
``spearmint report``, served where you run it.

    spearmint dashboard [--no-browser]   # or: python -m spearmint.dashboard ...

Reads rundb.db (via report.collect) and serves an auto-refreshing status table -- experiments x
stages, wip/PEND/RUN/done/failed, live durations, staleness. Each stage links to a per-run page:
ledger metadata, the err log when failed, and the run dir's artifacts rendered by
explorer.render_dir (tables + plots, JSON trees, zoomable images). Binds 127.0.0.1 only -- run
it where the ledger lives (on the cluster, next to the live db, connect through the ssh tunnel
command printed at startup). --no-browser skips opening a tab.
"""

import html
import sys
from pathlib import Path
from urllib.parse import quote, unquote

from . import explorer
from . import report
from . import rundb
from .config import CONFIG

PORT = CONFIG.port  # spearmint's own dashboard port (config); distinct from any other UI's
REFRESH_SECONDS = CONFIG.refresh_seconds

# Status-table styling on top of explorer.STYLE (which carries the theme + artifact panels).
_STYLE = """
  #meta { color: #8b949e; margin-bottom: 12px; }
  tr.group td { padding-top: 16px; color: #58a6ff; font-weight: 600; }
  td.key { color: #c9d1d9; } td.stale { color: #8b949e; }
  .mark { color: #8b949e; }  /* trailing content-kind glyphs (see report.CONTENT_SYMBOLS) */
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


def _page(body: str, live: bool) -> str:
    """Wrap page ``body`` in the shared shell. ``live`` (only the home status table) adds the
    auto-refresh script that re-fetches /table into #table every REFRESH_SECONDS -- a per-run
    page passes live=False so its content is NOT clobbered by the status table on the next tick."""
    meta = '<div id="meta">loading…</div>' if live else ""
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>spearmint</title>
<style>{explorer.STYLE}{_STYLE}</style></head><body>
<h1>spearmint status</h1>
{meta}
<div id="table">{body}</div>
{_REFRESH_SCRIPT if live else ""}</body></html>"""


def _content_kinds(job_key: str, status: str) -> "set[str]":
    """The content kinds this stage's /run page would show -- explorer.kinds_in over its latest
    outdir, plus 'log' for a failed stage whose LSF err log exists locally."""
    kinds: "set[str]" = set()
    if status == "failed" and (Path(rundb.ROOT) / report.lsf_log_relpath(job_key)).exists():
        kinds.add("log")
    outdir = rundb.latest_outdir(job_key)
    if outdir:
        kinds |= explorer.kinds_in(outdir)
    return kinds


def _table() -> str:
    groups = report.collect()
    kinds = {r.job_key: _content_kinds(r.job_key, r.status) for rows in groups.values() for r in rows}
    kinds = {k: v for k, v in kinds.items() if v}  # only stages that actually have something
    return report.render_html(groups, kinds=kinds)


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
        log_abs = Path(rundb.ROOT) / log_rel
        tail = ""
        if log_abs.exists():
            tail = "".join(log_abs.read_text(errors="replace").splitlines(keepends=True)[-50:])
        err = (
            f"<h3>err log <a href='/file/{quote(log_rel)}'>({html.escape(log_rel)})</a></h3>"
            f"<pre class='err'>{html.escape(tail) or '(log not on this machine)'}</pre>"
        )
    body = meta + err + explorer.render_dir(outdir, rundb.ROOT)
    return _page(body, live=False)


class _Handler(explorer.Handler):
    def do_GET(self) -> None:
        if self.path == "/table":
            self._send(_table())
        elif self.path == "/":
            self._send(_page(_table(), live=True))
        elif self.path.startswith("/file/"):
            self._serve_file(unquote(self.path[len("/file/"):]), rundb.ROOT)
        elif self.path.startswith("/run/"):
            self._send(_run_page(unquote(self.path[len("/run/"):])))
        else:
            self.send_error(404)


def main() -> None:
    from . import _cli

    _cli.help_if_asked(__doc__)
    extra = [a for a in sys.argv[1:] if a != "--no-browser"]
    if extra:
        _cli.usage_error(__doc__, f"unexpected args {extra} (only --no-browser is accepted)")
    explorer.serve(_Handler, PORT, open_browser="--no-browser" not in sys.argv[1:])


if __name__ == "__main__":
    main()
