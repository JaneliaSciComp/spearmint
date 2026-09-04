"""Declarative, JSON-serializable live dashboard specifications.

Panels only select and present artifacts already emitted by stages. Computation and custom
data munging belong in stages or retrospective report functions.
"""

from dataclasses import dataclass, field


def _keys(stages) -> "list[str]":
    if not isinstance(stages, (list, tuple)):
        stages = [stages]
    return [s.job_key if hasattr(s, "job_key") else str(s) for s in stages]


@dataclass
class Lines:
    """Plot rows loaded from ``path`` under each stage; ``stage`` is a virtual row field."""

    stages: object
    path: str = "metrics.jsonl"
    x: "str | None" = None
    y: object = None
    color: "str | None" = None
    facet: "str | None" = None
    dash: "dict[str, str]" = field(default_factory=dict)
    colors: "dict[str, str]" = field(default_factory=dict)
    logy: bool = False
    title: str = ""
    height: "int | None" = None

    def as_dict(self) -> dict:
        return {"kind": "lines", **self.__dict__, "stages": _keys(self.stages)}


@dataclass
class Table:
    """Compare flattened JSON metrics across stages, in either metric/stage orientation."""

    stages: object
    path: str = "summary.json"
    metrics: "list[str] | None" = None
    title: str = ""
    corner: str = "metric"
    rows: str = "metric"
    columns: str = "stage"

    def as_dict(self) -> dict:
        assert {self.rows, self.columns} == {"metric", "stage"}, \
            "table rows/columns must be 'metric' and 'stage'"
        return {"kind": "table", **self.__dict__, "stages": _keys(self.stages)}


@dataclass
class Images:
    """Image grid whose row, column, and layer values are captured from ``path``."""

    stages: object
    path: str = "{row}_{col}_{overlay}.png"
    rows: "list[str] | None" = None
    columns: "list[str] | None" = None
    overlays: "list[str] | None" = None
    stage_mode: str = "rows"
    width: int = 220
    title: str = ""

    def as_dict(self) -> dict:
        import string

        fields = {name for _, name, _, _ in string.Formatter().parse(self.path) if name}
        assert fields <= {"row", "col", "overlay"}, \
            "image path placeholders must be {row}, {col}, or {overlay}"
        assert self.stage_mode in {"rows", "columns", "overlay"}, \
            "image stage_mode must be 'rows', 'columns', or 'overlay'"
        return {"kind": "images", **self.__dict__, "stages": _keys(self.stages)}


class Dashboard:
    """A live-view layout serialized by the driver and interpreted by ``spearmint browse``."""

    def __init__(self, *panels, title: str = "", refresh: int = 10):
        self.panels = list(panels)
        self.title = title
        self.refresh = refresh

    def as_dict(self) -> dict:
        return {
            "version": 1,
            "title": self.title,
            "refresh": self.refresh,
            "panels": [panel.as_dict() for panel in self.panels],
        }
