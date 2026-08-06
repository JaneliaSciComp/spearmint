"""Status report over the rundb ledger (component A: run history / job status).

Read-only and reconciliation-free: on a laptop reading a pulled snapshot there's no bjobs to
ask, so a wip row renders honestly as wip with its recency rather than being second-guessed.
``collect()`` is pure data and ``render()`` a pure formatter -- the later component B (per-run
artifact panels: plots/tables read from run dirs) adds a new renderer beside these without
reworking either.

    uv run python -m spearmint.report            # render the local ledger
    uv run python -m spearmint.report --remote   # pull a fresh ledger snapshot first (~1s)
"""

import sys
from dataclasses import dataclass
from datetime import timedelta

from . import rundb


@dataclass
class JobRow:
    job_key: str
    status: str
    lsf_state: "str | None"  # 'PEND' / 'RUN <host>' dispatch detail; meaningful while wip
    n_runs: int
    started_at: str
    ended_at: "str | None"
    total: timedelta
    stale: "list[str] | None"  # None = provenance unknown (run not launched by dagrunner)


def collect() -> "dict[str, list[JobRow]]":
    """One ledger scan -> {experiment group: [JobRow, ...]}. Group = first job_key segment
    (the Experiment prefix). The latest row per job_key wins for status/recency; total sums
    every attempt's wall clock (a still-wip attempt counts up to now, so totals stay live)."""
    conn = rundb._connect()
    rows = conn.execute(
        "SELECT job_key, status, lsf_state, started_at, ended_at FROM runs ORDER BY run_id"
    ).fetchall()
    conn.close()
    latest: "dict[str, tuple[str, str | None, str, str | None]]" = {}
    counts: "dict[str, int]" = {}
    totals: "dict[str, timedelta]" = {}
    for job_key, status, lsf_state, started, ended in rows:
        latest[job_key] = (status, lsf_state, started, ended)
        counts[job_key] = counts.get(job_key, 0) + 1
        totals[job_key] = totals.get(job_key, timedelta()) + rundb._duration(started, ended)
    groups: "dict[str, list[JobRow]]" = {}
    for job_key, (status, lsf_state, started, ended) in latest.items():
        row = JobRow(
            job_key=job_key,
            status=status,
            lsf_state=lsf_state,
            n_runs=counts[job_key],
            started_at=started,
            ended_at=ended,
            total=totals[job_key],
            stale=rundb.stale_inputs(job_key),
        )
        groups.setdefault(job_key.split("/")[0], []).append(row)
    return groups


def render(groups: "dict[str, list[JobRow]]") -> str:
    """Fixed-width terminal table, one section per experiment group. A wip row with LSF
    dispatch detail shows it inline -- wip(PEND) queued vs wip(RUN <host>) actually running.
    stale column: '-' fresh, '?' provenance unknown, else the dep job_keys with newer
    results."""
    lines: "list[str]" = []
    for group in sorted(groups):
        lines.append(group)
        for r in sorted(groups[group], key=lambda r: r.job_key):
            status = f"wip({r.lsf_state})" if r.status == "wip" and r.lsf_state else r.status
            stale = "?" if r.stale is None else (",".join(r.stale) or "-")
            total = str(r.total).split(".")[0]  # a live (wip) total carries microseconds; drop them
            lines.append(
                f"  {r.job_key:<40} {status:<16} runs={r.n_runs:<3} "
                f"total={total:<10} last={r.started_at}  stale={stale}"
            )
        lines.append("")
    return "\n".join(lines).rstrip()


def _status_label(r: JobRow) -> str:
    """The status as shown -- wip carries its LSF dispatch detail when known."""
    return f"wip({r.lsf_state})" if r.status == "wip" and r.lsf_state else r.status


def _status_class_for(status: str, lsf_state: "str | None" = None) -> str:
    """CSS class keying the status badge colour (see dashboard.py's stylesheet), from a bare
    status string (+ optional LSF dispatch state to split wip into pend/run)."""
    if status == "wip":
        return "run" if (lsf_state or "").startswith("RUN") else "pend"
    return status  # done / failed / skipped / abandoned


def _status_class(r: JobRow) -> str:
    return _status_class_for(r.status, r.lsf_state)


def lsf_log_relpath(job_key: str) -> str:
    """ROOT-relative path of a stage's LSF -oo log -- lsf.py writes it to
    _lsf_logs/{job_key with / -> _}.log. Where the driver captured the child's stdout/stderr, so
    it's the err log to link on failure."""
    return f"_lsf_logs/{job_key.replace('/', '_')}.log"


# content kind -> the marker glyph shown (trailing) after a stage's name; a stage may have
# several. dashboard._content_kinds resolves which kinds each stage's run dir actually holds.
CONTENT_SYMBOLS = {"png": "🖼", "table": "📊", "json": "{}", "log": "⚠"}


def render_html(groups: "dict[str, list[JobRow]]", kinds: "dict[str, set[str]] | None" = None) -> str:
    """The status table as an HTML fragment (no <html>/<head> shell -- dashboard.py wraps it,
    and swaps just this fragment in on each refresh). Pure formatter over collect()'s data, the
    HTML sibling of render(). Each stage links to its component-B run report (/run/<job_key>);
    a failed stage's badge links to its err log (/file/<lsf log>) -- both dashboard.py routes.
    ``kinds`` maps job_key -> the set of content kinds its run page holds (png/table/json/log,
    resolved from the local mirror by dashboard._content_kinds); each gets a trailing glyph
    (CONTENT_SYMBOLS) so you can see at a glance what's there."""
    import html
    from urllib.parse import quote

    kinds = kinds or {}
    rows = []
    for group in sorted(groups):
        rows.append(f'<tr class="group"><td colspan="6">{html.escape(group)}</td></tr>')
        for r in sorted(groups[group], key=lambda r: r.job_key):
            stale = "?" if r.stale is None else (", ".join(r.stale) or "–")
            total = str(r.total).split(".")[0]
            key_link = f'/run/{quote(r.job_key)}'  # job_key slashes are real path structure
            have = kinds.get(r.job_key, set())
            marks = "".join(CONTENT_SYMBOLS[k] for k in CONTENT_SYMBOLS if k in have)
            mark = f' <span class="mark" title="{html.escape(str(sorted(have)))}">{marks}</span>' if marks else ""
            badge = f'<span class="badge {_status_class(r)}">{html.escape(_status_label(r))}</span>'
            if r.status == "failed":  # click the red badge -> the err log
                badge = f'<a href="/file/{quote(lsf_log_relpath(r.job_key))}">{badge}</a>'
            rows.append(
                "<tr>"
                f'<td class="key"><a href="{key_link}">{html.escape(r.job_key)}</a>{mark}</td>'
                f"<td>{badge}</td>"
                f"<td>{r.n_runs}</td>"
                f"<td>{total}</td>"
                f"<td>{r.started_at}</td>"
                f'<td class="stale">{html.escape(stale)}</td>'
                "</tr>"
            )
    header = (
        "<tr><th>stage</th><th>status</th><th>runs</th><th>total</th>"
        "<th>last</th><th>stale</th></tr>"
    )
    return f"<table>{header}{''.join(rows)}</table>"


if __name__ == "__main__":
    from . import _cli

    _cli.help_if_asked(__doc__)
    extra = [a for a in sys.argv[1:] if a != "--remote"]
    if extra:
        _cli.usage_error(__doc__, f"unexpected args {extra} (only --remote is accepted)")
    if "--remote" in sys.argv:
        from . import remote

        remote.pull_db()
    print(render(collect()))
