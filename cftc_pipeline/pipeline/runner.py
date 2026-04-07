"""Pipeline runner with job tracking, retries, and logging."""
from __future__ import annotations

import logging
import traceback
from datetime import datetime
from typing import Callable, Optional

from sqlalchemy.orm import Session

from cftc_pipeline.db.models import Docket, JobStatus, PipelineJob

logger = logging.getLogger(__name__)

STAGE_ORDER = [
    "crawl_docket",
    "fetch_comment_pages",
    "fetch_attachments",
    "extract_text",
    "normalize_text",
    "dedupe_submissions",
    "analyze_submission_llm",
    "cluster_themes",
    "summarize_clusters",
    "rank_high_signal_submissions",
    "generate_report",
    "build_exports",
]


def get_stage_fn(stage: str) -> Callable:
    from cftc_pipeline.pipeline import stages

    stage_map = {
        "crawl_docket": stages.crawl_docket,
        "fetch_comment_pages": stages.fetch_comment_pages,
        "fetch_attachments": stages.fetch_attachments,
        "extract_text": stages.extract_text,
        "normalize_text": stages.normalize_text,
        "dedupe_submissions": stages.dedupe_submissions,
        "analyze_submission_llm": stages.analyze_submission_llm,
        "cluster_themes": stages.cluster_themes,
        "summarize_clusters": stages.summarize_clusters,
        "rank_high_signal_submissions": stages.rank_high_signal_submissions,
        "generate_report": stages.generate_report_stage,
        "build_exports": _build_exports_stub,
    }
    fn = stage_map.get(stage)
    if fn is None:
        raise ValueError(f"Unknown stage: {stage}")
    return fn


def _build_exports_stub(db: Session, docket_id: int, config: dict) -> dict:
    """CSV/JSONL export generation."""
    return _build_exports(db, docket_id, config)


def _build_exports(db: Session, docket_id: int, config: dict) -> dict:
    """Export key tables to CSV/JSONL for downstream use.

    Returns a dict with export stats (submission_count, analysis_count, path).
    """
    import csv
    import json
    from pathlib import Path

    from cftc_pipeline.config import settings
    from cftc_pipeline.db.models import LLMAnalysis, Submission, SubmissionDedupe

    docket = db.get(Docket, docket_id)
    if docket is None:
        logger.error("_build_exports: docket_id=%d not found in database", docket_id)
        return {"submission_count": 0, "analysis_count": 0, "error": "docket not found"}

    out_dir = Path(settings.storage_base_path) / "exports" / docket.docket_id
    out_dir.mkdir(parents=True, exist_ok=True)

    submissions = db.query(Submission).filter(Submission.docket_id == docket_id).all()

    if not submissions:
        logger.warning(
            "_build_exports: 0 submissions found for docket %d (%s). "
            "The export files will be empty. Make sure the pipeline has been "
            "run first (cftc run --docket %s).",
            docket_id,
            docket.docket_id,
            docket.docket_id,
        )

    analyses = {
        a.submission_id: a
        for a in db.query(LLMAnalysis)
        .join(Submission)
        .filter(Submission.docket_id == docket_id)
        .all()
    }
    dedupe_map = {
        dm.submission_id: dm
        for dm in db.query(SubmissionDedupe)
        .join(Submission, SubmissionDedupe.submission_id == Submission.id)
        .filter(Submission.docket_id == docket_id)
        .all()
    }

    # CSV export
    with open(out_dir / "submissions.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "id", "external_id", "commenter_name", "organization",
                "submission_date", "stance", "commenter_type",
                "substantive_score", "template_likelihood",
                "is_canonical", "dedupe_group_id", "issues",
            ],
        )
        writer.writeheader()
        for sub in submissions:
            a = analyses.get(sub.id)
            dm = dedupe_map.get(sub.id)
            writer.writerow(
                {
                    "id": sub.id,
                    "external_id": sub.external_id,
                    "commenter_name": sub.commenter_name,
                    "organization": sub.organization,
                    "submission_date": str(sub.submission_date or ""),
                    "stance": a.stance.value if a and a.stance else "",
                    "commenter_type": a.commenter_type.value if a and a.commenter_type else "",
                    "substantive_score": a.substantive_score if a else "",
                    "template_likelihood": a.template_likelihood if a else "",
                    "is_canonical": dm.is_canonical if dm else True,
                    "dedupe_group_id": dm.dedupe_group_id if dm else "",
                    "issues": "|".join(a.issues or []) if a else "",
                }
            )

    # JSONL export (full analysis)
    with open(out_dir / "analyses.jsonl", "w", encoding="utf-8") as f:
        for sub in submissions:
            a = analyses.get(sub.id)
            record = {
                "submission_id": sub.id,
                "external_id": sub.external_id,
                "commenter_name": sub.commenter_name,
                "analysis": a.analysis if a else None,
            }
            f.write(json.dumps(record) + "\n")

    logger.info(
        "Exports written to %s (%d submissions, %d analyses)",
        out_dir,
        len(submissions),
        len(analyses),
    )
    return {
        "submission_count": len(submissions),
        "analysis_count": len(analyses),
        "path": str(out_dir),
    }


def run_stage(
    db: Session,
    docket_id: int,
    stage: str,
    config: dict = None,
    force: bool = False,
) -> dict:
    """
    Run a single pipeline stage with job tracking.

    If force=False and the stage already completed successfully, skip it.
    """
    config = config or {}

    # Check for existing successful job
    if not force:
        existing = (
            db.query(PipelineJob)
            .filter(
                PipelineJob.docket_id == docket_id,
                PipelineJob.stage == stage,
                PipelineJob.status == JobStatus.completed,
            )
            .first()
        )
        if existing:
            logger.info("Stage '%s' already completed — skipping (use force=True to rerun)", stage)
            return existing.artifacts or {}

    # Create job record
    job = PipelineJob(
        docket_id=docket_id,
        stage=stage,
        status=JobStatus.running,
        started_at=datetime.utcnow(),
        config_snapshot=config,
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    stage_fn = get_stage_fn(stage)

    try:
        logger.info("Starting stage: %s (job_id=%d)", stage, job.id)
        artifacts = stage_fn(db, docket_id, config)
        job.status = JobStatus.completed
        job.completed_at = datetime.utcnow()
        job.artifacts = artifacts or {}
        # Update items_processed/items_failed from common artifact keys
        a = artifacts or {}
        job.items_processed = (
            a.get("new", 0) + a.get("ok", 0) + a.get("processed", 0)
            + a.get("ranked", 0) + a.get("submission_count", 0)
            + a.get("groups", 0) + a.get("clusters", 0)
            + a.get("clusters_summarized", 0)
        )
        job.items_failed = a.get("failed", 0)
        db.commit()
        logger.info("Stage '%s' completed: %s", stage, artifacts)
        return artifacts or {}

    except Exception as exc:
        tb = traceback.format_exc()
        logger.error("Stage '%s' failed: %s\n%s", stage, exc, tb)
        job.status = JobStatus.failed
        job.completed_at = datetime.utcnow()
        job.error = f"{exc}\n{tb}"
        db.commit()
        raise


def run_pipeline(
    db: Session,
    docket_id: int,
    stages: Optional[list[str]] = None,
    config: dict = None,
    force: bool = False,
    stop_after: Optional[str] = None,
) -> dict:
    """
    Run the full pipeline (or a subset of stages) for a docket.

    stages: if provided, only run these stages (in order).
    stop_after: if provided, stop after this stage.
    """
    stages_to_run = stages or STAGE_ORDER
    config = config or {}
    results = {}

    for stage in stages_to_run:
        result = run_stage(db, docket_id, stage, config=config, force=force)
        results[stage] = result
        if stop_after and stage == stop_after:
            break

    return results


def get_pipeline_status(db: Session, docket_id: int) -> list[dict]:
    """Return status of all pipeline stages for a docket."""
    jobs = (
        db.query(PipelineJob)
        .filter(PipelineJob.docket_id == docket_id)
        .order_by(PipelineJob.created_at.desc())
        .all()
    )

    # One entry per stage (latest job)
    seen_stages = set()
    status_list = []
    for job in jobs:
        if job.stage not in seen_stages:
            seen_stages.add(job.stage)
            status_list.append(
                {
                    "stage": job.stage,
                    "status": job.status.value,
                    "started_at": str(job.started_at or ""),
                    "completed_at": str(job.completed_at or ""),
                    "items_processed": job.items_processed,
                    "error": job.error,
                    "artifacts": job.artifacts,
                }
            )

    # Add pending stages not yet started
    for stage in STAGE_ORDER:
        if stage not in seen_stages:
            status_list.append({"stage": stage, "status": "pending"})

    return status_list
