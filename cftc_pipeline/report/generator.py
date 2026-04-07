"""Report generation: assemble data and render Jinja2 template."""
from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy.orm import Session

from cftc_pipeline.config import settings
from cftc_pipeline.db.models import (
    ClusterMembership,
    DedupeGroup,
    Docket,
    LLMAnalysis,
    ReportClaimSource,
    ReportRun,
    Submission,
    SubmissionDedupe,
    ThemeCluster,
)

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent / "templates"


def generate_report(db: Session, docket_id: int, report_run_id: int) -> str:
    """Assemble all data and render the report. Returns rendered markdown."""

    docket = db.get(Docket, docket_id)
    report_run = db.get(ReportRun, report_run_id)

    # --- Stats ---
    submissions = db.query(Submission).filter(Submission.docket_id == docket_id).all()
    total = len(submissions)

    analyses = (
        db.query(LLMAnalysis)
        .join(Submission)
        .filter(Submission.docket_id == docket_id)
        .all()
    )
    analysis_by_sub = {a.submission_id: a for a in analyses}

    # Dedup stats
    dedupe_memberships = (
        db.query(SubmissionDedupe)
        .join(Submission, SubmissionDedupe.submission_id == Submission.id)
        .filter(Submission.docket_id == docket_id)
        .all()
    )
    dedupe_by_sub = {dm.submission_id: dm for dm in dedupe_memberships}

    unique_count = sum(1 for dm in dedupe_memberships if dm.is_canonical)
    exact_dups = sum(1 for dm in dedupe_memberships if dm.dedup_method == "exact" and not dm.is_canonical)
    near_dups = sum(1 for dm in dedupe_memberships if dm.dedup_method == "near_duplicate" and not dm.is_canonical)

    # Campaign families
    groups = db.query(DedupeGroup).filter(DedupeGroup.docket_id == docket_id).all()
    campaign_families = sum(1 for g in groups if g.dedup_method == "campaign")

    # Commenter mix
    type_counter = Counter()
    stance_raw = Counter()
    stance_unique = Counter()

    for sub in submissions:
        a = analysis_by_sub.get(sub.id)
        if a:
            ctype = a.commenter_type.value if a.commenter_type else "other"
            type_counter[ctype] += 1
            stance = a.stance.value if a.stance else "unclear"
            stance_raw[stance] += 1
            if dedupe_by_sub.get(sub.id) and dedupe_by_sub[sub.id].is_canonical:
                stance_unique[stance] += 1

    commenter_mix = [
        {"type": t, "count": c, "pct": round(100 * c / max(total, 1), 1)}
        for t, c in type_counter.most_common()
    ]
    stance_breakdown = [
        {"stance": s, "raw": stance_raw[s], "unique": stance_unique[s]}
        for s in ["support", "oppose", "mixed", "neutral", "unclear"]
        if stance_raw[s] > 0
    ]

    has_attachments = sum(1 for s in submissions if s.has_attachments)
    llm_analyzed = len(analyses)
    extraction_failed = sum(
        1 for a in analyses if a.analysis_status == "failed"
    )

    stats = {
        "total_submissions": total,
        "unique_submissions": unique_count or total,
        "exact_duplicates": exact_dups,
        "near_duplicates": near_dups,
        "campaign_families": campaign_families,
        "has_attachments": has_attachments,
        "attachment_only": sum(
            1 for s in submissions if s.has_attachments and not (s.raw_comment_text or "").strip()
        ),
        "llm_analyzed": llm_analyzed,
        "extraction_failed": extraction_failed,
        "commenter_mix": commenter_mix,
        "stance_breakdown": stance_breakdown,
        "attachment_downloads_ok": 0,  # TODO: pull from attachments table
        "attachment_downloads_failed": 0,
        "noise_submissions": 0,
    }

    # --- Top clusters ---
    clusters = (
        db.query(ThemeCluster)
        .filter(ThemeCluster.docket_id == docket_id)
        .order_by(ThemeCluster.total_count.desc())
        .limit(10)
        .all()
    )

    top_clusters = []
    for cluster in clusters:
        rep_memberships = (
            db.query(ClusterMembership)
            .filter(
                ClusterMembership.cluster_id == cluster.id,
                ClusterMembership.is_representative == True,
            )
            .limit(3)
            .all()
        )
        rep_excerpts = []
        for cm in rep_memberships:
            sub = db.get(Submission, cm.submission_id)
            a = analysis_by_sub.get(cm.submission_id)
            excerpt_text = ""
            if a and a.notable_quotes:
                quotes = a.notable_quotes
                if quotes and isinstance(quotes, list):
                    excerpt_text = quotes[0].get("quote", "")[:300]
            rep_excerpts.append(
                {
                    "submission_id": cm.submission_id,
                    "commenter": sub.commenter_name if sub else "Unknown",
                    "text": excerpt_text,
                    "url": f"/submissions/{cm.submission_id}",
                }
            )

        top_clusters.append(
            {
                "label": cluster.analyst_label or cluster.auto_label or f"Cluster {cluster.id}",
                "total_count": cluster.total_count or 0,
                "unique_count": cluster.unique_count or 0,
                "keywords": cluster.keywords or [],
                "description": cluster.description or cluster.cluster_summary or "",
                "rep_arguments_for": cluster.rep_arguments_for or [],
                "rep_arguments_against": cluster.rep_arguments_against or [],
                "rep_excerpts": rep_excerpts,
            }
        )

    # --- High-signal submissions ---
    top_analyses = sorted(
        analyses,
        key=lambda a: float(a.substantive_score or 0),
        reverse=True,
    )

    def _sub_dict(a: LLMAnalysis) -> dict:
        sub = db.get(Submission, a.submission_id)
        return {
            "id": a.submission_id,
            "commenter_name": sub.commenter_name if sub else "",
            "organization": a.organization_extracted or (sub.organization if sub else ""),
            "stance": a.stance.value if a.stance else "unclear",
            "substantive_score": a.substantive_score or 0,
            "summary_short": a.summary_short or "",
            "legal_arguments": a.legal_arguments or [],
            "economic_arguments": a.economic_arguments or [],
            "operational_arguments": a.operational_arguments or [],
            "cited_authorities": a.cited_authorities or [],
        }

    high_legal = [
        _sub_dict(a)
        for a in sorted(top_analyses, key=lambda a: len(a.legal_arguments or []), reverse=True)[:5]
    ]
    high_economic = [
        _sub_dict(a)
        for a in sorted(top_analyses, key=lambda a: len(a.economic_arguments or []), reverse=True)[:5]
    ]
    high_operational = [
        _sub_dict(a)
        for a in sorted(top_analyses, key=lambda a: len(a.operational_arguments or []), reverse=True)[:5]
    ]

    # --- Notable organizations ---
    notable_orgs = []
    for a in top_analyses[:20]:
        if a.commenter_type and a.commenter_type.value in ("trade_association", "company", "academic", "government"):
            sub = db.get(Submission, a.submission_id)
            notable_orgs.append(
                {
                    "name": a.organization_extracted or (sub.organization if sub else "Unknown"),
                    "commenter_type": a.commenter_type.value,
                    "stance": a.stance.value if a.stance else "unclear",
                }
            )

    # --- Outliers ---
    # Submissions not in any cluster (noise) with high substantive score
    all_memberships = set(
        cm.submission_id
        for cm in db.query(ClusterMembership)
        .join(Submission, ClusterMembership.submission_id == Submission.id)
        .filter(Submission.docket_id == docket_id)
        .all()
    )
    outlier_analyses = [
        a
        for a in sorted(top_analyses, key=lambda a: float(a.substantive_score or 0), reverse=True)
        if a.submission_id not in all_memberships
    ][:8]
    outliers = [_sub_dict(a) for a in outlier_analyses]

    # --- Claim sources ---
    claim_sources = (
        db.query(ReportClaimSource)
        .filter(ReportClaimSource.report_run_id == report_run_id)
        .all()
    )
    claim_source_dicts = [
        {
            "claim_text": c.claim_text or "",
            "submission_id": c.submission_id,
            "source_excerpt": (c.source_excerpt or "")[:120],
        }
        for c in claim_sources
    ]

    # --- Render ---
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(disabled_extensions=("md.j2",)),
    )
    template = env.get_template("report.md.j2")

    rendered = template.render(
        docket_id=docket.docket_id if docket else str(docket_id),
        docket_title=docket.title if docket else "",
        generated_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        report_run_id=report_run_id,
        stats=stats,
        top_clusters=top_clusters,
        high_signal={
            "legal": high_legal,
            "economic": high_economic,
            "operational": high_operational,
        },
        notable_organizations=notable_orgs[:10],
        outliers=outliers,
        claim_sources=claim_source_dicts,
        llm_model=settings.llm_model,
        prompt_version=settings.prompt_version,
        dedup_threshold=settings.minhash_threshold,
        minhash_perms=settings.minhash_num_perm,
        campaign_min=settings.campaign_min_size,
        hdbscan_min_cluster=5,
    )

    return rendered
