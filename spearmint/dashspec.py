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
    stages: object
    file: str = "metrics.jsonl"
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
class Images:
    stages: object
    glob: str = "*.png"
    align: str = "filename"
    overlay: bool = False
    width: int = 220
    title: str = ""

    def as_dict(self) -> dict:
        assert self.align == "filename", "only filename image alignment is currently supported"
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
