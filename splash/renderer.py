from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader

_TEMPLATE_DIR = Path(__file__).parent / "templates"


def _fmt_number(value: int | float) -> str:
    if isinstance(value, float):
        return f"{value:,.2f}"
    return f"{value:,}"


def _fmt_bytes(value: int | float) -> str:
    v = float(value)
    for unit in ("B", "KB", "MB", "GB"):
        if abs(v) < 1024:
            return f"{v:,.1f} {unit}"
        v /= 1024
    return f"{v:,.1f} TB"


def _fmt_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}m"
    hours = minutes / 60
    return f"{hours:.1f}h"


def _fmt_pct(value: float) -> str:
    return f"{value:.1f}%"


def render_dashboard(data) -> str:
    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATE_DIR)),
        autoescape=True,
    )
    env.filters["fmt_number"] = _fmt_number
    env.filters["fmt_bytes"] = _fmt_bytes
    env.filters["fmt_duration"] = _fmt_duration
    env.filters["fmt_pct"] = _fmt_pct

    template = env.get_template("dashboard.html.j2")
    return template.render(data=data)
