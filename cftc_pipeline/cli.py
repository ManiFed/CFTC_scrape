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


def _print_db_connection_help() -> None:
    """Print actionable guidance for known local DB connection issues."""
    from cftc_pipeline.config import settings

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
        return

    console.print(
        "[red]Database connection failed.[/red] "
        "Check that DATABASE_URL is set correctly and the Postgres server is reachable."
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

    try:
        with get_db() as db:
            d = db.query(Docket).filter(Docket.docket_id == docket).first()
            if not d:
                console.print(f"[red]Docket '{docket}' not found.[/red]")
                sys.exit(1)

            stage_status = get_pipeline_status(db, d.id)
    except OperationalError:
        _print_db_connection_help()
        sys.exit(1)

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
@click.option("--docket", required=True, help="CFTC docket ID")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt")
def delete_docket(docket: str, yes: bool):
    """Delete a docket and all its data so it can be re-initialised and re-run."""
    from cftc_pipeline.db.models import (
        ClusterMembership,
        DedupeGroup,
        Docket,
        LLMAnalysis,
        Attachment,
        ExtractionResult,
        PipelineJob,
        ReportClaimSource,
        ReportRun,
        Submission,
        SubmissionDedupe,
        ThemeCluster,
    )

    with get_db() as db:
        d = db.query(Docket).filter(Docket.docket_id == docket).first()
        if not d:
            console.print(f"[red]Docket '{docket}' not found.[/red]")
            sys.exit(1)

        if not yes:
            console.print(
                f"[yellow]This will permanently delete docket '{docket}' "
                f"(id={d.id}) and ALL associated data.[/yellow]"
            )
            click.confirm("Are you sure?", abort=True)

        docket_db_id = d.id

        # Delete in dependency order to satisfy FK constraints.
        sub_ids = [r for (r,) in db.query(Submission.id).filter(Submission.docket_id == docket_db_id)]
        run_ids = [r for (r,) in db.query(ReportRun.id).filter(ReportRun.docket_id == docket_db_id)]

        if sub_ids:
            db.query(ClusterMembership).filter(ClusterMembership.submission_id.in_(sub_ids)).delete(synchronize_session=False)
            db.query(SubmissionDedupe).filter(SubmissionDedupe.submission_id.in_(sub_ids)).delete(synchronize_session=False)
            db.query(LLMAnalysis).filter(LLMAnalysis.submission_id.in_(sub_ids)).delete(synchronize_session=False)
            db.query(ExtractionResult).filter(ExtractionResult.submission_id.in_(sub_ids)).delete(synchronize_session=False)
            db.query(Attachment).filter(Attachment.submission_id.in_(sub_ids)).delete(synchronize_session=False)

        if run_ids:
            db.query(ReportClaimSource).filter(ReportClaimSource.report_run_id.in_(run_ids)).delete(synchronize_session=False)

        db.query(ReportRun).filter(ReportRun.docket_id == docket_db_id).delete(synchronize_session=False)
        db.query(ThemeCluster).filter(ThemeCluster.docket_id == docket_db_id).delete(synchronize_session=False)
        db.query(DedupeGroup).filter(DedupeGroup.docket_id == docket_db_id).delete(synchronize_session=False)
        db.query(PipelineJob).filter(PipelineJob.docket_id == docket_db_id).delete(synchronize_session=False)
        db.query(Submission).filter(Submission.docket_id == docket_db_id).delete(synchronize_session=False)
        db.query(Docket).filter(Docket.id == docket_db_id).delete(synchronize_session=False)

    console.print(f"[green]Docket '{docket}' and all its data have been deleted.[/green]")
    console.print(f"[dim]Re-register it with: cftc init-docket --docket {docket} --url <URL> --title <TITLE>[/dim]")


@cli.command()
def create_tables():
    """Create all database tables (idempotent)."""
    from cftc_pipeline.db.models import Base
    from cftc_pipeline.db.session import engine
    try:
        Base.metadata.create_all(engine)
    except OperationalError:
        _print_db_connection_help()
        sys.exit(1)
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
