"""Pipeline stage implementations.

Each stage function takes (db, docket_id, job_id, config) and mutates DB state.
Stages are idempotent — they skip already-completed work.
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from cftc_pipeline.config import settings
from cftc_pipeline.db.models import (
    Attachment,
    ClusterMembership,
    DedupeGroup,
    DedupeMethod,
    Docket,
    ExtractionResult,
    LLMAnalysis,
    PipelineJob,
    ReportRun,
    Submission,
    SubmissionDedupe,
    ThemeCluster,
)
from cftc_pipeline.storage import detail_html_key, sha256, storage

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stage: crawl_docket
# ---------------------------------------------------------------------------


def crawl_docket(db: Session, docket_id: int, config: dict) -> dict:
    """Crawl the comment list and upsert Submission records."""
    from cftc_pipeline.scraper.cftc_scraper import crawl_comment_list

    docket = db.get(Docket, docket_id)
    url = config.get("url") or docket.url

    processed = 0
    skipped = 0

    for entry in crawl_comment_list(url):
        existing = (
            db.query(Submission)
            .filter(
                Submission.docket_id == docket_id,
                Submission.external_id == entry.external_id,
            )
            .first()
        )
        if existing:
            skipped += 1
            continue

        sub = Submission(
            docket_id=docket_id,
            external_id=entry.external_id,
            comment_url=entry.detail_url,
            commenter_name=entry.commenter_name,
            organization=entry.organization,
            submission_date=entry.submission_date,
            crawl_status="listed",
        )
        db.add(sub)
        db.flush()
        processed += 1

    db.commit()
    if processed == 0 and skipped == 0:
        logger.warning(
            "crawl_docket: found 0 submissions for docket %d (url=%s). "
            "Check the URL is correct and the docket has public comments.",
            docket_id,
            url,
        )
    else:
        logger.info("crawl_docket: %d new, %d skipped", processed, skipped)
    return {"new": processed, "skipped": skipped}


# ---------------------------------------------------------------------------
# Stage: fetch_comment_pages
# ---------------------------------------------------------------------------


def fetch_comment_pages(db: Session, docket_id: int, config: dict) -> dict:
    """Visit each comment detail page, extract body and attachment links."""
    from cftc_pipeline.scraper.cftc_scraper import CommentListEntry, fetch_comment_detail
    from cftc_pipeline.storage import detail_html_key

    docket = db.get(Docket, docket_id)

    submissions = (
        db.query(Submission)
        .filter(
            Submission.docket_id == docket_id,
            Submission.crawl_status.in_(["listed", "failed"]),
        )
        .all()
    )

    ok = failed = 0
    for sub in submissions:
        try:
            entry = CommentListEntry(
                external_id=sub.external_id,
                commenter_name=sub.commenter_name or "",
                organization=sub.organization,
                submission_date=sub.submission_date,
                detail_url=sub.comment_url,
            )
            detail = fetch_comment_detail(entry)

            # Store raw HTML
            key = detail_html_key(docket.docket_id, sub.external_id)
            file_path = storage.write(key, detail.raw_html)

            sub.detail_html_path = file_path
            sub.raw_comment_text = detail.body_text
            sub.received_date = detail.received_date or sub.submission_date
            sub.crawl_status = "crawled"
            sub.has_attachments = len(detail.attachment_urls) > 0
            sub.updated_at = datetime.utcnow()

            # Create Attachment records
            for att_info in detail.attachment_urls:
                existing_att = (
                    db.query(Attachment)
                    .filter(
                        Attachment.submission_id == sub.id,
                        Attachment.original_url == att_info["url"],
                    )
                    .first()
                )
                if not existing_att:
                    att = Attachment(
                        submission_id=sub.id,
                        filename=att_info.get("filename"),
                        original_url=att_info["url"],
                        file_type=Path(att_info.get("filename", "")).suffix.lstrip(".").lower() or "unknown",
                        download_status="pending",
                    )
                    db.add(att)

            db.flush()
            ok += 1

        except Exception as exc:
            logger.error("fetch_comment_pages: submission %d failed: %s", sub.id, exc)
            sub.crawl_status = "failed"
            db.flush()
            failed += 1

    db.commit()
    return {"ok": ok, "failed": failed}


# ---------------------------------------------------------------------------
# Stage: fetch_attachments
# ---------------------------------------------------------------------------


def fetch_attachments(db: Session, docket_id: int, config: dict) -> dict:
    """Download all pending attachments."""
    from cftc_pipeline.scraper.attachment_downloader import download_attachment

    docket = db.get(Docket, docket_id)

    attachments = (
        db.query(Attachment)
        .join(Submission, Attachment.submission_id == Submission.id)
        .filter(
            Submission.docket_id == docket_id,
            Attachment.download_status == "pending",
        )
        .all()
    )

    ok = failed = 0
    for att in attachments:
        sub = db.get(Submission, att.submission_id)
        result = download_attachment(
            url=att.original_url,
            docket_id=docket.docket_id,
            external_id=sub.external_id,
            suggested_filename=att.filename,
        )
        att.file_path = result["file_path"]
        att.file_type = result["file_type"] or att.file_type
        att.file_size = result["file_size"]
        att.content_hash = result["content_hash"]
        att.download_status = result["status"]
        att.filename = result["filename"] or att.filename
        db.flush()

        if result["status"] == "downloaded":
            ok += 1
        else:
            failed += 1

    db.commit()
    return {"ok": ok, "failed": failed}


# ---------------------------------------------------------------------------
# Stage: extract_text
# ---------------------------------------------------------------------------


def extract_text(db: Session, docket_id: int, config: dict) -> dict:
    """Extract text from HTML bodies and attachments."""
    from cftc_pipeline.extraction.docx_extractor import extract_docx
    from cftc_pipeline.extraction.html_extractor import extract_html
    from cftc_pipeline.extraction.pdf_extractor import extract_pdf

    submissions = (
        db.query(Submission)
        .filter(Submission.docket_id == docket_id, Submission.crawl_status == "crawled")
        .all()
    )

    ok = failed = 0
    for sub in submissions:
        # HTML body
        if sub.raw_comment_text:
            existing = (
                db.query(ExtractionResult)
                .filter(
                    ExtractionResult.submission_id == sub.id,
                    ExtractionResult.source_type == "html",
                    ExtractionResult.attachment_id == None,
                )
                .first()
            )
            if not existing:
                result = extract_html(sub.raw_comment_text)
                er = ExtractionResult(
                    submission_id=sub.id,
                    attachment_id=None,
                    source_type="html",
                    extraction_method=result.method,
                    raw_text=result.text,
                    cleaned_text=result.text,
                    char_count=len(result.text),
                    page_count=result.page_count,
                    extraction_status=result.status,
                    error_message=result.error,
                )
                db.add(er)
                ok += 1

        # Attachments
        for att in sub.attachments:
            if att.download_status != "downloaded" or not att.file_path:
                continue
            existing = (
                db.query(ExtractionResult)
                .filter(ExtractionResult.attachment_id == att.id)
                .first()
            )
            if existing:
                continue

            try:
                data = storage.read(att.file_path) if not Path(att.file_path).is_absolute() else Path(att.file_path).read_bytes()
            except Exception as exc:
                # Try reading as absolute path
                try:
                    data = Path(att.file_path).read_bytes()
                except Exception:
                    logger.warning("Cannot read attachment %d: %s", att.id, exc)
                    continue

            ftype = (att.file_type or "").lower()
            if ftype == "pdf":
                result = extract_pdf(data)
            elif ftype in ("docx", "doc"):
                result = extract_docx(data)
            elif ftype in ("txt", "html", "htm"):
                result = extract_html(data.decode("utf-8", errors="replace"))
            else:
                result = extract_html(data.decode("utf-8", errors="replace"))

            er = ExtractionResult(
                submission_id=sub.id,
                attachment_id=att.id,
                source_type=ftype or "unknown",
                extraction_method=result.method,
                raw_text=result.text,
                cleaned_text=result.text,
                char_count=len(result.text),
                page_count=result.page_count,
                extraction_status=result.status,
                error_message=result.error,
            )
            db.add(er)
            ok += 1

        db.flush()

    db.commit()
    return {"ok": ok, "failed": failed}


# ---------------------------------------------------------------------------
# Stage: normalize_text
# ---------------------------------------------------------------------------


def normalize_text(db: Session, docket_id: int, config: dict) -> dict:
    """Build canonical combined text for each submission."""
    from cftc_pipeline.extraction.text_cleaner import build_canonical_text

    submissions = (
        db.query(Submission)
        .filter(Submission.docket_id == docket_id)
        .all()
    )

    processed = 0
    for sub in submissions:
        extractions = (
            db.query(ExtractionResult)
            .filter(ExtractionResult.submission_id == sub.id)
            .all()
        )
        html_results = [e for e in extractions if e.source_type == "html" and not e.attachment_id]
        att_results = [e for e in extractions if e.attachment_id is not None]

        html_text = html_results[0].cleaned_text if html_results else sub.raw_comment_text or ""

        att_texts = []
        for er in att_results:
            att = db.get(Attachment, er.attachment_id)
            filename = att.filename if att else f"attachment_{er.attachment_id}"
            att_texts.append((filename, er.cleaned_text or ""))

        canonical = build_canonical_text(html_text, att_texts)

        # Store as a special ExtractionResult with no attachment_id and source_type="canonical"
        existing = (
            db.query(ExtractionResult)
            .filter(
                ExtractionResult.submission_id == sub.id,
                ExtractionResult.source_type == "canonical",
            )
            .first()
        )
        if existing:
            existing.cleaned_text = canonical
            existing.char_count = len(canonical)
        else:
            er_canonical = ExtractionResult(
                submission_id=sub.id,
                attachment_id=None,
                source_type="canonical",
                extraction_method="combined",
                raw_text=canonical,
                cleaned_text=canonical,
                char_count=len(canonical),
                page_count=1,
                extraction_status="ok" if canonical.strip() else "empty",
            )
            db.add(er_canonical)

        db.flush()
        processed += 1

    db.commit()
    return {"processed": processed}


# ---------------------------------------------------------------------------
# Stage: dedupe_submissions
# ---------------------------------------------------------------------------


def dedupe_submissions(db: Session, docket_id: int, config: dict) -> dict:
    """Run deduplication across all submissions."""
    from cftc_pipeline.dedup.deduplicator import run_deduplication

    submissions = db.query(Submission).filter(Submission.docket_id == docket_id).all()

    # Build text lookup
    sub_texts = []
    for sub in submissions:
        canonical_er = (
            db.query(ExtractionResult)
            .filter(
                ExtractionResult.submission_id == sub.id,
                ExtractionResult.source_type == "canonical",
            )
            .first()
        )
        text = (canonical_er.cleaned_text if canonical_er else sub.raw_comment_text) or ""
        sub_texts.append({"id": sub.id, "text": text})

    results = run_deduplication(sub_texts)

    # Build group records
    group_map: dict[int, int] = {}  # local_group_id -> DB dedupe_group.id

    for result in results:
        if result.group_id not in group_map:
            # Create group
            group = DedupeGroup(
                docket_id=docket_id,
                group_size=0,
                dedup_method=DedupeMethod(result.dedup_method)
                if result.dedup_method in ("exact", "near_duplicate", "campaign")
                else DedupeMethod.exact,
                content_hash=result.content_hash,
            )
            db.add(group)
            db.flush()
            group_map[result.group_id] = group.id

        db_group_id = group_map[result.group_id]

        # Upsert membership
        existing = (
            db.query(SubmissionDedupe)
            .filter(SubmissionDedupe.submission_id == result.submission_id)
            .first()
        )
        if existing:
            existing.dedupe_group_id = db_group_id
            existing.is_canonical = result.is_canonical
            existing.similarity_score = result.similarity_score
            existing.dedup_method = DedupeMethod(result.dedup_method) if result.dedup_method in ("exact", "near_duplicate", "campaign") else DedupeMethod.exact
        else:
            dm = SubmissionDedupe(
                submission_id=result.submission_id,
                dedupe_group_id=db_group_id,
                is_canonical=result.is_canonical,
                similarity_score=result.similarity_score,
                dedup_method=DedupeMethod(result.dedup_method) if result.dedup_method in ("exact", "near_duplicate", "campaign") else DedupeMethod.exact,
            )
            db.add(dm)

        db.flush()

    # Update group sizes and canonical
    for local_gid, db_gid in group_map.items():
        group = db.get(DedupeGroup, db_gid)
        members = db.query(SubmissionDedupe).filter(SubmissionDedupe.dedupe_group_id == db_gid).all()
        group.group_size = len(members)
        canonical_members = [m for m in members if m.is_canonical]
        if canonical_members:
            group.canonical_submission_id = canonical_members[0].submission_id
        db.flush()

    db.commit()
    return {"groups": len(group_map), "submissions": len(results)}


# ---------------------------------------------------------------------------
# Stage: analyze_submission_llm
# ---------------------------------------------------------------------------


def analyze_submission_llm(db: Session, docket_id: int, config: dict) -> dict:
    """Run LLM structured analysis on all unanalyzed submissions."""
    from cftc_pipeline.analysis.llm_analyzer import analyze_submission
    from cftc_pipeline.db.models import CommenterTypeEnum, StanceEnum

    submissions = db.query(Submission).filter(Submission.docket_id == docket_id).all()

    # Filter to unanalyzed
    analyzed_ids = {
        a.submission_id
        for a in db.query(LLMAnalysis)
        .join(Submission)
        .filter(Submission.docket_id == docket_id)
        .all()
    }
    to_analyze = [s for s in submissions if s.id not in analyzed_ids]

    ok = failed = 0
    for sub in to_analyze:
        canonical_er = (
            db.query(ExtractionResult)
            .filter(
                ExtractionResult.submission_id == sub.id,
                ExtractionResult.source_type == "canonical",
            )
            .first()
        )
        text = (canonical_er.cleaned_text if canonical_er else sub.raw_comment_text) or ""

        try:
            analysis, model_id, version = analyze_submission(
                submission_id=sub.id,
                commenter_name=sub.commenter_name or "",
                organization=sub.organization or "",
                submission_date=str(sub.submission_date or ""),
                text=text,
            )

            llm = LLMAnalysis(
                submission_id=sub.id,
                model_id=model_id,
                prompt_version=version,
                analysis=analysis.model_dump(),
                stance=StanceEnum(analysis.stance),
                commenter_type=CommenterTypeEnum(analysis.commenter_type),
                commenter_name_extracted=analysis.commenter_name,
                organization_extracted=analysis.organization,
                issues=analysis.issues,
                requested_changes=analysis.requested_changes,
                legal_arguments=analysis.legal_arguments,
                economic_arguments=analysis.economic_arguments,
                operational_arguments=analysis.operational_arguments,
                policy_arguments=analysis.policy_arguments,
                cited_authorities=analysis.cited_authorities,
                notable_quotes=[q.model_dump() for q in analysis.notable_quotes],
                summary_short=analysis.summary_short,
                summary_detailed=analysis.summary_detailed,
                template_likelihood=analysis.template_likelihood,
                substantive_score=analysis.substantive_score,
                confidence=analysis.confidence,
                source_spans=[s.model_dump() for s in analysis.source_spans],
                analysis_status="ok",
            )
            db.add(llm)
            db.flush()
            ok += 1

        except Exception as exc:
            logger.error("LLM analysis failed for submission %d: %s", sub.id, exc)
            llm = LLMAnalysis(
                submission_id=sub.id,
                model_id=settings.llm_model,
                prompt_version=settings.prompt_version,
                analysis={},
                analysis_status="failed",
                error_message=str(exc),
            )
            db.add(llm)
            db.flush()
            failed += 1

    db.commit()
    return {"ok": ok, "failed": failed, "skipped": len(analyzed_ids)}


# ---------------------------------------------------------------------------
# Stage: cluster_themes
# ---------------------------------------------------------------------------


def cluster_themes(db: Session, docket_id: int, config: dict) -> dict:
    """Generate embeddings and cluster submissions into themes."""
    from cftc_pipeline.clustering.theme_clusterer import run_clustering

    submissions = db.query(Submission).filter(Submission.docket_id == docket_id).all()
    analyses = {
        a.submission_id: a
        for a in db.query(LLMAnalysis)
        .join(Submission)
        .filter(Submission.docket_id == docket_id)
        .all()
    }

    sub_data = []
    for sub in submissions:
        canonical_er = (
            db.query(ExtractionResult)
            .filter(
                ExtractionResult.submission_id == sub.id,
                ExtractionResult.source_type == "canonical",
            )
            .first()
        )
        text = (canonical_er.cleaned_text if canonical_er else sub.raw_comment_text) or ""
        issues = analyses[sub.id].issues if sub.id in analyses else []
        sub_data.append({"id": sub.id, "text": text, "issues": issues or []})

    cluster_results, cluster_summaries = run_clustering(
        sub_data,
        min_cluster_size=config.get("min_cluster_size", 5),
        min_samples=config.get("min_samples", 3),
    )

    # Persist clusters
    cluster_id_map: dict[int, int] = {}  # local_id -> DB id
    for summary in cluster_summaries:
        tc = ThemeCluster(
            docket_id=docket_id,
            auto_label=summary.auto_label,
            keywords=summary.keywords,
            total_count=summary.total_count,
            unique_count=summary.total_count,  # refined in summarize_clusters
        )
        db.add(tc)
        db.flush()
        cluster_id_map[summary.cluster_id] = tc.id

    # Persist memberships
    for result in cluster_results:
        if result.cluster_id == -1:
            continue
        db_cluster_id = cluster_id_map.get(result.cluster_id)
        if not db_cluster_id:
            continue
        cm = ClusterMembership(
            submission_id=result.submission_id,
            cluster_id=db_cluster_id,
            relevance_score=result.relevance_score,
            is_representative=result.is_representative,
        )
        db.add(cm)

    db.commit()
    noise = sum(1 for r in cluster_results if r.cluster_id == -1)
    return {"clusters": len(cluster_summaries), "noise": noise}


# ---------------------------------------------------------------------------
# Stage: summarize_clusters
# ---------------------------------------------------------------------------


def summarize_clusters(db: Session, docket_id: int, config: dict) -> dict:
    """Use LLM to generate cluster descriptions and rep arguments."""
    from openai import OpenAI

    client = OpenAI(api_key=settings.openai_auth_token())
    clusters = (
        db.query(ThemeCluster).filter(ThemeCluster.docket_id == docket_id).all()
    )
    docket = db.get(Docket, docket_id)

    for cluster in clusters:
        # Get representative submissions' summaries
        rep_memberships = (
            db.query(ClusterMembership)
            .filter(
                ClusterMembership.cluster_id == cluster.id,
                ClusterMembership.is_representative == True,
            )
            .limit(5)
            .all()
        )
        rep_summaries = []
        for cm in rep_memberships:
            a = (
                db.query(LLMAnalysis)
                .filter(LLMAnalysis.submission_id == cm.submission_id)
                .first()
            )
            if a:
                rep_summaries.append(
                    f"- {a.summary_short or ''} (stance: {a.stance.value if a.stance else 'unclear'})"
                )

        if not rep_summaries:
            continue

        prompt = f"""You are summarizing a cluster of related public comments submitted to the CFTC about "{docket.title or 'a proposed rule'}".

The cluster is labeled: "{cluster.auto_label}"
Keywords: {', '.join(cluster.keywords or [])}

Representative comment summaries:
{chr(10).join(rep_summaries)}

Write:
1. A 2-3 sentence description of this theme cluster
2. 2-3 representative arguments in SUPPORT of the rule (if any)
3. 2-3 representative arguments OPPOSING the rule (if any)

Return JSON: {{"description": "...", "rep_arguments_for": [...], "rep_arguments_against": [...]}}"""

        try:
            resp = client.responses.create(
                model=settings.llm_model,
                max_output_tokens=1000,
                input=prompt,
            )
            import json

            text = resp.output_text.strip()
            if text.startswith("```"):
                text = "\n".join(l for l in text.split("\n") if not l.startswith("```"))
            data = json.loads(text)
            cluster.description = data.get("description", "")
            cluster.rep_arguments_for = data.get("rep_arguments_for", [])
            cluster.rep_arguments_against = data.get("rep_arguments_against", [])
            cluster.cluster_summary = data.get("description", "")
            db.flush()
        except Exception as exc:
            logger.warning("Cluster summarization failed for cluster %d: %s", cluster.id, exc)

    db.commit()
    return {"clusters_summarized": len(clusters)}


# ---------------------------------------------------------------------------
# Stage: rank_high_signal_submissions
# ---------------------------------------------------------------------------


def rank_high_signal_submissions(db: Session, docket_id: int, config: dict) -> dict:
    """Score and rank all submissions; store scores back to LLMAnalysis."""
    from cftc_pipeline.ranking.ranker import rank_submissions

    submissions = db.query(Submission).filter(Submission.docket_id == docket_id).all()
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

    sub_data = []
    for sub in submissions:
        a = analyses.get(sub.id)
        dm = dedupe_map.get(sub.id)
        canonical_er = (
            db.query(ExtractionResult)
            .filter(
                ExtractionResult.submission_id == sub.id,
                ExtractionResult.source_type == "canonical",
            )
            .first()
        )
        text_length = canonical_er.char_count if canonical_er else len(sub.raw_comment_text or "")

        sub_data.append(
            {
                "id": sub.id,
                "analysis": a.analysis if a else {},
                "text_length": text_length,
                "is_canonical": dm.is_canonical if dm else True,
            }
        )

    ranked = rank_submissions(sub_data)
    # Store scores back — reuse substantive_score field for total ranking score
    for r in ranked:
        a = analyses.get(r.submission_id)
        if a:
            # Update with computed score (override LLM's self-reported score)
            a.substantive_score = r.total_score
            db.flush()

    db.commit()
    return {"ranked": len(ranked)}


# ---------------------------------------------------------------------------
# Stage: generate_report
# ---------------------------------------------------------------------------


def generate_report_stage(db: Session, docket_id: int, config: dict) -> dict:
    """Generate the final Markdown report."""
    from cftc_pipeline.report.generator import generate_report

    docket = db.get(Docket, docket_id)
    report_run = ReportRun(
        docket_id=docket_id,
        config=config,
        status="running",
    )
    db.add(report_run)
    db.flush()

    try:
        markdown = generate_report(db, docket_id, report_run.id)
        output_dir = Path(settings.storage_base_path) / "exports" / docket.docket_id
        output_dir.mkdir(parents=True, exist_ok=True)
        report_path = output_dir / f"report_{report_run.id}.md"
        report_path.write_text(markdown, encoding="utf-8")

        report_run.report_path = str(report_path)
        report_run.status = "completed"
        db.commit()
        return {"report_path": str(report_path), "report_run_id": report_run.id}
    except Exception as exc:
        report_run.status = "failed"
        db.commit()
        raise
