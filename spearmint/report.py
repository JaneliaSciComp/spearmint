"""Terminal status report over the rundb ledger (run history / job status).

Read-only and reconciliation-free: a wip row renders honestly as wip with its recency rather
than being second-guessed (bjobs-based reconciliation is rundb.reconcile_wip's job, and only
works where bjobs exists). ``collect()`` is pure data and ``render()``/``render_html()`` pure
formatters -- dashboard.py builds its status page from the same collect().

    spearmint status [dir]   # or: python -m spearmint.report [dir]

``dir`` is the ledger/run-output dir; default <git root of cwd>/output_rundb.
"""

import re
import sys
from dataclasses import dataclass
from datetime import timedelta

from . import rundb


def fmt_ts(ts: "str | None") -> str:
    """rundb's filename-safe timestamp (2026-06-30-14-25-52) rendered for humans
    (2026-06-30 14:25:52); anything unexpected passes through untouched."""
    if not ts:
        return ""
    p = ts.split("-")
    return f"{p[0]}-{p[1]}-{p[2]} {p[3]}:{p[4]}:{p[5]}" if len(p) == 6 else ts


@dataclass
class JobRow:
    job_key: str
    status: str
    lsf_state: "str | None"  # 'PEND' / 'RUN <host>' dispatch detail; meaningful while wip
    n_runs: int
    started_at: str
    ended_at: "str | None"
    total: timedelta
    avg: timedelta  # total / n_runs -- typical wall clock of ONE attempt
    stale: "list[str] | None"  # None = provenance unknown (run not launched by dagrunner)


def collect() -> "dict[str, list[JobRow]]":
    """One ledger scan -> {experiment group: [JobRow, ...]}. Group = first job_key segment
    (the Experiment prefix). The latest row per job_key wins for status/recency; total sums
    every attempt's wall clock (a still-wip attempt counts up to now, so totals stay live)."""
    conn = rundb._connect(readonly=True)
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
            avg=totals[job_key] / counts[job_key],
            stale=rundb.stale_inputs(job_key),
        )
        groups.setdefault(job_key.split("/")[0], []).append(row)
    return groups


def render(groups: "dict[str, list[JobRow]]") -> str:
    """Fixed-width terminal table, one section per experiment group. A wip row with LSF
    dispatch detail shows it inline -- wip(PEND) queued vs wip(RUN <host>) actually running.
    stale column: 'no' fresh, 'n/a' provenance unknown (no recorded inputs -- the run wasn't
    scheduler-launched), else 'yes:' + the dep job_keys with newer
    results."""
    lines: "list[str]" = []
    for group in sorted(groups):
        lines.append(group)
        prev_sub = None
        for r in sorted(groups[group], key=lambda r: r.job_key):
            sub = "/".join(r.job_key.split("/")[:-1])
            if prev_sub is not None and sub != prev_sub:
                lines.append("")  # blank line between sub-experiments (e00/short vs e00/smoke)
            prev_sub = sub
            status = f"wip({r.lsf_state})" if r.status == "wip" and r.lsf_state else r.status
            stale = "n/a" if r.stale is None else (f"yes: {','.join(r.stale)}" if r.stale else "no")
            total = str(r.total).split(".")[0]  # a live (wip) total carries microseconds; drop them
            avg = str(r.avg).split(".")[0]
            lines.append(
                f"  {r.job_key:<40} {status:<16} runs={r.n_runs:<3} "
                f"total={total:<10} avg={avg:<10} last={fmt_ts(r.started_at)}  stale={stale}"
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


def _spark(vals: "list[float]") -> str:
    """Unicode histogram (8 bins over the value range, bar height = bin count) -- the
    collapsed group row's at-a-glance distribution. No spread -> no distribution -> ''."""
    bars = "▁▂▃▄▅▆▇█"
    lo, hi = min(vals), max(vals)
    if hi == lo:
        return ""
    bins = [0] * 8
    for v in vals:
        bins[min(7, int((v - lo) / (hi - lo) * 8))] += 1
    m = max(bins)
    return "".join(bars[round(b * 7 / m)] for b in bins)


def _fmt_td(td: timedelta) -> str:
    return str(td).split(".")[0]  # live (wip) durations carry microseconds; drop them


def _exp_names(group: str) -> "tuple[str, str]":
    """'e00_flyem_mae_vs_lejepa' -> ('e00', 'flyem_mae_vs_lejepa'); a group without the eNN
    prefix keeps its whole name as the short name (empty long half)."""
    m = re.match(r"(e\d+)[_-](.+)", group)
    return (m.group(1), m.group(2)) if m else (group, "")


def _exp_row_html(group: str, rs: "list[JobRow]", title: str, rep_html: str, opn: str) -> str:
    """One experiments-table row: shortname | name | report title | report links | the
    aggregates over the group's stages (stage count, status-count badges, summed runs/total,
    mean attempt duration with a per-stage-avg histogram; min/max ride the tooltips)."""
    import html
    from collections import Counter

    counts = Counter(_status_class(r) for r in rs)
    badges = " ".join(
        f'<span class="badge {c}" title="{c}">{counts[c]}</span>'
        for c in ("done", "run", "pend", "failed", "skipped", "abandoned") if counts[c]
    )
    total = sum((r.total for r in rs), timedelta())
    avg = total / sum(r.n_runs for r in rs)  # mean over ATTEMPTS -- same semantics as the avg column
    avgs = sorted(r.avg.total_seconds() for r in rs)
    spark = f' <span class="mark">{_spark(avgs)}</span>' if len(rs) >= 4 else ""
    totals = sorted(r.total for r in rs)
    n_stale = sum(1 for r in rs if r.stale)
    stale = "n/a" if all(r.stale is None for r in rs) else (f"{n_stale} stale" if n_stale else "no")
    short, _ = _exp_names(group)
    return (
        f'<tr class="exp" data-grp="{html.escape(group, quote=True)}"{opn}>'
        # Full group name rides the tooltip -- the column shows just the short handle.
        f'<td class="short" title="{html.escape(group, quote=True)}">{html.escape(short)}</td>'
        f'<td class="full">{html.escape(title)}</td>'
        f'<td class="rep">{rep_html}</td>'
        f"<td>{len(rs)}</td>"
        f"<td>{badges}</td>"
        f"<td>{sum(r.n_runs for r in rs)}</td>"
        f'<td title="per stage: min {_fmt_td(totals[0])} · max {_fmt_td(totals[-1])}">{_fmt_td(total)}</td>'
        f'<td title="per stage: min {_fmt_td(timedelta(seconds=avgs[0]))} · max '
        f'{_fmt_td(timedelta(seconds=avgs[-1]))}">{_fmt_td(avg)}{spark}</td>'
        f"<td>{fmt_ts(max(r.started_at for r in rs))}</td>"
        f'<td class="stale">{stale}</td>'
        "</tr>"
    )


def lsf_log_relpath(job_key: str) -> str:
    """ROOT-relative path of a stage's LSF -oo log -- lsf.py writes it to
    _lsf_logs/{job_key with / -> _}.log. Where the driver captured the child's stdout/stderr, so
    it's the err log to link on failure."""
    return f"_lsf_logs/{job_key.replace('/', '_')}.log"


def render_html(
    groups: "dict[str, list[JobRow]]",
    kinds: "dict[str, set[str]] | None" = None,
    reports: "set[str] | None" = None,
    report_titles: "dict[str, str] | None" = None,
) -> str:
    """The status view as an HTML fragment (no <html>/<head> shell -- dashboard.py wraps it,
    and swaps just this fragment in on each refresh): TWO tables. #exps has one row per
    experiment group -- shortname | name | report title | report links | aggregates -- and
    #stages holds every group's stage rows; the dashboard's select script shows only the
    clicked experiment's stages (most recently active one on first paint, via ``data-open``).
    Each stage links to its run page (/run/<job_key>); a failed stage's badge links to its err
    log (/file/<lsf log>) -- both dashboard.py routes. ``kinds`` maps job_key -> the set of
    content kinds its run page holds (png/table/json/log, resolved by
    dashboard._content_kinds); each gets a trailing glyph (explorer.CONTENT_SYMBOLS).
    ``reports`` holds experiment prefixes with a driver-rendered report.html under
    ROOT/_reports (may be multi-segment, e.g. "e00/smoke"): own + nested links land in the
    reports column. ``report_titles`` maps those same prefixes -> the short label scraped off
    report.html (viz.page's ``short_title``, see dashboard._report_title); the title column
    prefers the group's own title, else the first nested one that has one."""
    import html
    from urllib.parse import quote

    from .explorer import CONTENT_SYMBOLS

    kinds = kinds or {}
    reports = reports or set()
    report_titles = report_titles or {}
    exp_rows = []
    rows = []

    def report_link(prefix: str, text: str) -> str:
        return f'<a href="/file/{quote(f"_reports/{prefix}/report.html")}">{html.escape(text)}</a>'

    newest = max(groups, key=lambda g: max(r.started_at for r in groups[g]), default=None)
    for group in sorted(groups):
        own_report = group in reports
        nested = sorted(r for r in reports if r.startswith(f"{group}/"))
        title = report_titles.get(group) if own_report else None
        if not title:
            title = next((report_titles[r] for r in nested if report_titles.get(r)), None)
        links = ([report_link(group, "📈")] if own_report else []) + \
            [report_link(r, "📈" + r.split("/", 1)[1]) for r in nested]
        opn = " data-open" if group == newest else ""
        exp_rows.append(_exp_row_html(group, groups[group], title or "", " ".join(links), opn))
        prev_sub = None
        for r in sorted(groups[group], key=lambda r: r.job_key):
            # A vertical break where the SUB-experiment prefix changes (e00/short -> e00/smoke)
            # -- adjacent tiers otherwise read as one experiment. The class also delimits the
            # client-side sort segments (see dashboard's sort script).
            sub = "/".join(r.job_key.split("/")[:-1])
            brk = ' class="subbreak"' if prev_sub is not None and sub != prev_sub else ""
            prev_sub = sub
            stale = "n/a" if r.stale is None else (f"yes: {', '.join(r.stale)}" if r.stale else "no")
            total = str(r.total).split(".")[0]
            avg = str(r.avg).split(".")[0]
            key_link = f'/run/{quote(r.job_key)}'  # job_key slashes are real path structure
            have = kinds.get(r.job_key, set())
            marks = "".join(CONTENT_SYMBOLS[k] for k in CONTENT_SYMBOLS if k in have)
            mark = f' <span class="mark" title="{html.escape(str(sorted(have)))}">{marks}</span>' if marks else ""
            badge = f'<span class="badge {_status_class(r)}">{html.escape(_status_label(r))}</span>'
            if r.status == "failed":  # click the red badge -> the err log
                badge = f'<a href="/file/{quote(lsf_log_relpath(r.job_key))}">{badge}</a>'
            rows.append(
                f'<tr{brk} data-grp="{html.escape(group, quote=True)}">'
                f'<td class="key"><a href="{key_link}">{html.escape(r.job_key)}</a>{mark}</td>'
                f"<td>{badge}</td>"
                f"<td>{r.n_runs}</td>"
                f"<td>{total}</td>"
                f"<td>{avg}</td>"
                f"<td>{fmt_ts(r.started_at)}</td>"
                f'<td class="stale">{html.escape(stale)}</td>'
                "</tr>"
            )
    exp_header = (
        "<tr><th>exp</th><th>title</th><th>reports</th><th>stages</th>"
        "<th>status</th><th>runs</th><th>total</th><th>avg</th><th>last</th><th>stale</th></tr>"
    )
    header = (
        "<tr><th>stage</th><th>status</th><th>runs</th><th>total</th><th>avg</th>"
        "<th>last</th><th>stale</th></tr>"
    )
    return (f'<table id="exps">{exp_header}{"".join(exp_rows)}</table>'
            f'<table id="stages">{header}{"".join(rows)}</table>')


if __name__ == "__main__":
    from . import _cli

    _cli.help_if_asked(__doc__)
    if len(sys.argv[1:]) > 1:
        _cli.usage_error(__doc__, f"unexpected args {sys.argv[2:]} (at most one ledger dir)")
    rundb.anchor(sys.argv[1] if sys.argv[1:] else rundb.default_root())
    print(render(collect()))
