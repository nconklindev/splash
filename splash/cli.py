from __future__ import annotations

import webbrowser
from datetime import date
from pathlib import Path

import click

from . import __version__
from .analyzers import run_all_analyses
from .loader import filter_by_date, load_csvs
from .renderer import render_dashboard


def _parse_date(
    ctx: click.Context, param: click.Parameter, value: str | None
) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise click.BadParameter("Use YYYY-MM-DD format (e.g. 2025-01-15)", param=param)


@click.command()
@click.argument(
    "csv_files", nargs=-1, required=True, type=click.Path(exists=True, path_type=Path)
)
@click.option(
    "-o",
    "--output",
    default="splash_report.html",
    type=click.Path(path_type=Path),
    help="Output HTML path.",
)
@click.option("--title", default="Splash Report", help="Dashboard title.")
@click.option(
    "--open", "open_browser", is_flag=True, help="Open in browser after generation."
)
@click.option("-q", "--quiet", is_flag=True, help="Suppress info output.")
@click.option(
    "--start-date",
    metavar="YYYY-MM-DD",
    default=None,
    callback=_parse_date,
    is_eager=False,
    expose_value=True,
    help="Only include rows on or after this date (based on start_datetime).",
)
@click.option(
    "--end-date",
    metavar="YYYY-MM-DD",
    default=None,
    callback=_parse_date,
    is_eager=False,
    expose_value=True,
    help="Only include rows on or before this date (based on start_datetime).",
)
@click.version_option(version=__version__)
def main(
    csv_files: tuple[Path, ...],
    output: Path,
    title: str,
    open_browser: bool,
    quiet: bool,
    start_date: date | None,
    end_date: date | None,
) -> None:
    """Generate an HTML dashboard from Splunk CSV exports."""
    if not quiet:
        click.echo(f"Loading {len(csv_files)} CSV file(s)...")

    dataset = load_csvs(list(csv_files))

    if start_date or end_date:
        original_count = len(dataset.rows)
        dataset = filter_by_date(dataset, start=start_date, end=end_date)
        if not quiet:
            excluded = original_count - len(dataset.rows)
            date_range = " – ".join(
                filter(
                    None,
                    [
                        str(start_date) if start_date else None,
                        str(end_date) if end_date else None,
                    ],
                )
            )
            click.echo(
                f"  Date filter [{date_range}]: kept {len(dataset.rows)} rows, excluded {excluded}"
            )

    if not quiet:
        click.echo(
            f"  {len(dataset.rows)} rows, {len(dataset.available_columns)} known columns detected"
        )

    data = run_all_analyses(
        dataset,
        title=title,
        csv_files=[p.name for p in csv_files],
    )

    html = render_dashboard(data)
    output.write_text(html, encoding="utf-8")

    if not quiet:
        click.echo(f"Dashboard written to {output}")

    if open_browser:
        webbrowser.open(output.resolve().as_uri())
