from __future__ import annotations

import webbrowser
from pathlib import Path

import click

from . import __version__
from .analyzers import run_all_analyses
from .loader import load_csvs
from .renderer import render_dashboard


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
@click.version_option(version=__version__)
def main(
    csv_files: tuple[Path, ...],
    output: Path,
    title: str,
    open_browser: bool,
    quiet: bool,
) -> None:
    """Generate an HTML dashboard from Splunk CSV exports."""
    if not quiet:
        click.echo(f"Loading {len(csv_files)} CSV file(s)...")

    dataset = load_csvs(list(csv_files))

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
