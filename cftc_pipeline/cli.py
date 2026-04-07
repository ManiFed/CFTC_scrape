"""CLI entry point: cftc <command> [options]"""
from __future__ import annotations

import sys
import logging
import os
from urllib.parse import urlparse

import click
from rich.console import Console
from rich.table import Table
from sqlalchemy.exc import OperationalError

from cftc_pipeline.db.session import get_db
from cftc_pipeline.pipeline.runner import STAGE_ORDER, get_pipeline_status, run_pipeline, run_stage

console = Console()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def _is_likely_railway_private_host(database_url: str) -> bool:
    """Return True when DATABASE_URL points at Railway's private-only hostname."""
    hostname = (urlparse(database_url).hostname or "").lower()
    return hostname.endswith(".railway.internal")


def _running_on_railway() -> bool:
    """Best-effort detection for Railway runtime."""
    return bool(
        os.getenv("RAILWAY_ENVIRONMENT")
        or os.getenv("RAILWAY_PROJECT_ID")
        or os.getenv("RAILWAY_SERVICE_ID")
    )


def _format_empty_export_guidance(docket: str, pipeline_has_run: bool) -> str:
    """Return a helpful message when export finds zero submissions."""
    if pipeline_has_run:
        return (
            f"Warning: 0 submissions found for docket '{docket}'. "
            "This usually means the docket URL has no public comments yet, "
            "or the registered docket URL is incorrect. "
            f"Check stage results with: cftc status --docket {docket}. "
            f"If needed, rerun crawl with: cftc run --docket {docket} --stages crawl_docket --force"
        )
    return (
        f"Warning: 0 submissions found for docket '{docket}'. "
        f"Run the pipeline first: cftc run --docket {docket}"
    )


@click.group()
def cli():
    """CFTC public comment pipeline."""
    pass


@cli.command()
@click.option("--docket", required=True, help="CFTC docket ID, e.g. '3116'")
@click.option("--url", required=True, help="Full URL to the comment list page")
@click.option("--title", default="", help="Human-readable docket title")
def init_docket(docket: str, url: str, title: str):
    """Register a new docket in the database."""
    from cftc_pipeline.db.models import Docket

    with get_db() as db:
        existing = db.query(Docket).filter(Docket.docket_id == docket).first()
        if existing:
            console.print(f"[yellow]Docket '{docket}' already exists (id={existing.id})[/yellow]")
            return
        d = Docket(docket_id=docket, url=url, title=title)
        db.add(d)
        db.flush()
        console.print(f"[green]Created docket '{docket}' (id={d.id})[/green]")


@cli.command()
@click.option("--docket", required=True, help="CFTC docket ID")
@click.option("--stages", default=None, help="Comma-separated stage names to run (default: all)")
@click.option("--stop-after", default=None, help="Stop after this stage")
@click.option("--force", is_flag=True, help="Re-run stages even if already completed")
def run(docket: str, stages: str, stop_after: str, force: bool):
    """Run the full pipeline (or specific stages) for a docket."""
    from cftc_pipeline.db.models import Docket

    with get_db() as db:
        d = db.query(Docket).filter(Docket.docket_id == docket).first()
        if not d:
            console.print(f"[red]Docket '{docket}' not found. Run 'cftc init-docket' first.[/red]")
            sys.exit(1)

        stages_list = [s.strip() for s in stages.split(",")] if stages else None

        try:
            results = run_pipeline(
                db=db,
                docket_id=d.id,
                stages=stages_list,
                force=force,
                stop_after=stop_after,
            )
            console.print("[green]Pipeline complete:[/green]")
            for stage, result in results.items():
                console.print(f"  [bold]{stage}[/bold]: {result}")
        except Exception as exc:
            console.print(f"[red]Pipeline failed: {exc}[/red]")
            sys.exit(1)


@cli.command()
@click.option("--docket", required=True, help="CFTC docket ID")
@click.option("--stage", required=True, help="Stage name to run")
@click.option("--force", is_flag=True)
def run_stage_cmd(docket: str, stage: str, force: bool):
    """Run a single pipeline stage."""
    from cftc_pipeline.db.models import Docket

    with get_db() as db:
        d = db.query(Docket).filter(Docket.docket_id == docket).first()
        if not d:
            console.print(f"[red]Docket '{docket}' not found.[/red]")
            sys.exit(1)
        try:
            result = run_stage(db, d.id, stage, force=force)
            console.print(f"[green]Stage '{stage}' complete:[/green] {result}")
        except Exception as exc:
            console.print(f"[red]Stage '{stage}' failed: {exc}[/red]")
            sys.exit(1)


@cli.command()
@click.option("--docket", required=True, help="CFTC docket ID")
def status(docket: str):
    """Show pipeline status for a docket."""
    from cftc_pipeline.db.models import Docket

    with get_db() as db:
        d = db.query(Docket).filter(Docket.docket_id == docket).first()
        if not d:
            console.print(f"[red]Docket '{docket}' not found.[/red]")
            sys.exit(1)

        stage_status = get_pipeline_status(db, d.id)

    table = Table(title=f"Pipeline status: {docket}")
    table.add_column("Stage", style="cyan")
    table.add_column("Status")
    table.add_column("Completed")
    table.add_column("Artifacts")

    status_colors = {
        "completed": "green",
        "running": "yellow",
        "failed": "red",
        "pending": "dim",
        "skipped": "blue",
    }

    for row in stage_status:
        s = row["status"]
        color = status_colors.get(s, "white")
        table.add_row(
            row["stage"],
            f"[{color}]{s}[/{color}]",
            row.get("completed_at", "") or "",
            str(row.get("artifacts", "") or ""),
        )

    console.print(table)


@cli.command()
def create_tables():
    """Create all database tables (idempotent)."""
    from cftc_pipeline.db.models import Base
    from cftc_pipeline.db.session import engine
    from cftc_pipeline.config import settings

    try:
        Base.metadata.create_all(engine)
    except OperationalError as exc:
        if _is_likely_railway_private_host(settings.database_url) and not _running_on_railway():
            console.print(
                "[red]Database connection failed.[/red] "
                "Your DATABASE_URL uses a Railway private host (.railway.internal), "
                "which is only reachable from inside Railway's network."
            )
            console.print(
                "Use Railway's [bold]public/external[/bold] Postgres URL for local development, "
                "or run this command inside a Railway shell."
            )
            sys.exit(1)
        raise exc
    console.print("[green]Tables created (or already exist).[/green]")


@cli.command()
@click.option("--docket", required=True, help="CFTC docket ID")
def export_csv(docket: str):
    """Export submissions and analyses to CSV/JSONL."""
    from cftc_pipeline.db.models import Docket, JobStatus, PipelineJob
    from cftc_pipeline.pipeline.runner import _build_exports

    with get_db() as db:
        d = db.query(Docket).filter(Docket.docket_id == docket).first()
        if not d:
            console.print(f"[red]Docket '{docket}' not found.[/red]")
            sys.exit(1)
        pipeline_has_run = (
            db.query(PipelineJob.id)
            .filter(
                PipelineJob.docket_id == d.id,
                PipelineJob.status == JobStatus.completed,
            )
            .first()
            is not None
        )
        result = _build_exports(db, d.id, {})
    sub_count = result.get("submission_count", 0)
    analysis_count = result.get("analysis_count", 0)
    if sub_count == 0:
        guidance = _format_empty_export_guidance(docket, pipeline_has_run=pipeline_has_run)
        console.print(f"[yellow]{guidance}[/yellow]")
    else:
        console.print(
            f"[green]Exports complete: {sub_count} submissions, "
            f"{analysis_count} analyses → {result.get('path', '')}[/green]"
        )
