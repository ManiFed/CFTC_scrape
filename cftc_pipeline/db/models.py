"""SQLAlchemy ORM models for the CFTC pipeline."""
from __future__ import annotations

import enum
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class StanceEnum(str, enum.Enum):
    support = "support"
    oppose = "oppose"
    mixed = "mixed"
    neutral = "neutral"
    unclear = "unclear"


class CommenterTypeEnum(str, enum.Enum):
    individual = "individual"
    company = "company"
    trade_association = "trade_association"
    nonprofit = "nonprofit"
    academic = "academic"
    government = "government"
    other = "other"


class DedupeMethod(str, enum.Enum):
    exact = "exact"
    near_duplicate = "near_duplicate"
    campaign = "campaign"


class JobStatus(str, enum.Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"
    skipped = "skipped"


# ---------------------------------------------------------------------------
# Dockets
# ---------------------------------------------------------------------------


class Docket(Base):
    __tablename__ = "dockets"

    id = Column(Integer, primary_key=True)
    docket_id = Column(String(100), unique=True, nullable=False, index=True)
    title = Column(Text)
    url = Column(Text)
    description = Column(Text)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    meta = Column("metadata", JSONB, default={})

    submissions = relationship("Submission", back_populates="docket")
    theme_clusters = relationship("ThemeCluster", back_populates="docket")
    pipeline_jobs = relationship("PipelineJob", back_populates="docket")
    report_runs = relationship("ReportRun", back_populates="docket")
    dedupe_groups = relationship("DedupeGroup", back_populates="docket")


# ---------------------------------------------------------------------------
# Submissions
# ---------------------------------------------------------------------------


class Submission(Base):
    __tablename__ = "submissions"

    id = Column(Integer, primary_key=True)
    docket_id = Column(Integer, ForeignKey("dockets.id"), nullable=False)
    external_id = Column(String(200))  # CFTC's own comment ID
    comment_url = Column(Text)
    detail_html_path = Column(Text)  # path in file store
    commenter_name = Column(Text)
    organization = Column(Text)
    submission_date = Column(DateTime)
    received_date = Column(DateTime)
    raw_comment_text = Column(Text)  # text from the detail page body
    has_attachments = Column(Boolean, default=False)
    crawl_status = Column(String(50), default="pending")  # pending/crawled/failed
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    page_meta = Column(JSONB, default={})

    docket = relationship("Docket", back_populates="submissions")
    attachments = relationship("Attachment", back_populates="submission")
    extraction_results = relationship("ExtractionResult", back_populates="submission")
    dedupe_memberships = relationship("SubmissionDedupe", back_populates="submission")
    llm_analyses = relationship("LLMAnalysis", back_populates="submission")
    cluster_memberships = relationship("ClusterMembership", back_populates="submission")

    __table_args__ = (
        UniqueConstraint("docket_id", "external_id", name="uq_submission_docket_external"),
        Index("ix_submissions_docket_crawl", "docket_id", "crawl_status"),
    )


# ---------------------------------------------------------------------------
# Attachments
# ---------------------------------------------------------------------------


class Attachment(Base):
    __tablename__ = "attachments"

    id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey("submissions.id"), nullable=False)
    filename = Column(Text)
    original_url = Column(Text)
    file_path = Column(Text)  # path in file store
    file_type = Column(String(20))  # pdf / docx / txt / html / other
    file_size = Column(Integer)
    content_hash = Column(String(64))
    download_status = Column(String(50), default="pending")  # pending/downloaded/failed
    created_at = Column(DateTime, default=func.now())

    submission = relationship("Submission", back_populates="attachments")
    extraction_results = relationship("ExtractionResult", back_populates="attachment")

    __table_args__ = (Index("ix_attachments_submission", "submission_id"),)


# ---------------------------------------------------------------------------
# Extraction results
# ---------------------------------------------------------------------------


class ExtractionResult(Base):
    __tablename__ = "extraction_results"

    id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey("submissions.id"), nullable=False)
    attachment_id = Column(Integer, ForeignKey("attachments.id"), nullable=True)
    source_type = Column(String(20))  # html / pdf / docx / txt
    extraction_method = Column(String(100))  # e.g. "pymupdf", "pdfplumber", "python-docx"
    raw_text = Column(Text)
    cleaned_text = Column(Text)
    char_count = Column(Integer)
    page_count = Column(Integer)
    extraction_status = Column(String(50))  # ok / partial / failed / empty
    error_message = Column(Text)
    created_at = Column(DateTime, default=func.now())

    submission = relationship("Submission", back_populates="extraction_results")
    attachment = relationship("Attachment", back_populates="extraction_results")

    __table_args__ = (Index("ix_extraction_submission", "submission_id"),)


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


class DedupeGroup(Base):
    __tablename__ = "dedupe_groups"

    id = Column(Integer, primary_key=True)
    docket_id = Column(Integer, ForeignKey("dockets.id"), nullable=False)
    canonical_submission_id = Column(Integer, ForeignKey("submissions.id"), nullable=True)
    group_size = Column(Integer, default=1)
    dedup_method = Column(Enum(DedupeMethod))
    content_hash = Column(String(64))  # for exact matches
    template_fingerprint = Column(Text)  # normalized template text
    created_at = Column(DateTime, default=func.now())

    docket = relationship("Docket", back_populates="dedupe_groups")
    memberships = relationship("SubmissionDedupe", back_populates="dedupe_group")


class SubmissionDedupe(Base):
    __tablename__ = "submission_dedupe"

    submission_id = Column(Integer, ForeignKey("submissions.id"), primary_key=True)
    dedupe_group_id = Column(Integer, ForeignKey("dedupe_groups.id"), primary_key=True)
    is_canonical = Column(Boolean, default=False)
    similarity_score = Column(Float)
    dedup_method = Column(Enum(DedupeMethod))

    submission = relationship("Submission", back_populates="dedupe_memberships")
    dedupe_group = relationship("DedupeGroup", back_populates="memberships")


# ---------------------------------------------------------------------------
# LLM analysis
# ---------------------------------------------------------------------------


class LLMAnalysis(Base):
    __tablename__ = "llm_analyses"

    id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey("submissions.id"), nullable=False, unique=True)
    model_id = Column(String(100))
    prompt_version = Column(String(50))
    analysis = Column(JSONB)  # full structured output
    stance = Column(Enum(StanceEnum))
    commenter_type = Column(Enum(CommenterTypeEnum))
    commenter_name_extracted = Column(Text)
    organization_extracted = Column(Text)
    issues = Column(ARRAY(Text), default=[])
    requested_changes = Column(ARRAY(Text), default=[])
    legal_arguments = Column(ARRAY(Text), default=[])
    economic_arguments = Column(ARRAY(Text), default=[])
    operational_arguments = Column(ARRAY(Text), default=[])
    policy_arguments = Column(ARRAY(Text), default=[])
    cited_authorities = Column(ARRAY(Text), default=[])
    notable_quotes = Column(JSONB, default=[])
    summary_short = Column(Text)
    summary_detailed = Column(Text)
    template_likelihood = Column(Float)
    substantive_score = Column(Float)
    confidence = Column(Float)
    source_spans = Column(JSONB, default=[])
    analysis_status = Column(String(50), default="pending")
    error_message = Column(Text)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    submission = relationship("Submission", back_populates="llm_analyses")

    __table_args__ = (Index("ix_llm_analyses_submission", "submission_id"),)


# ---------------------------------------------------------------------------
# Theme clusters
# ---------------------------------------------------------------------------


class ThemeCluster(Base):
    __tablename__ = "theme_clusters"

    id = Column(Integer, primary_key=True)
    docket_id = Column(Integer, ForeignKey("dockets.id"), nullable=False)
    auto_label = Column(Text)
    analyst_label = Column(Text)
    description = Column(Text)
    keywords = Column(ARRAY(Text), default=[])
    total_count = Column(Integer, default=0)
    unique_count = Column(Integer, default=0)
    support_count = Column(Integer, default=0)
    oppose_count = Column(Integer, default=0)
    rep_arguments_for = Column(ARRAY(Text), default=[])
    rep_arguments_against = Column(ARRAY(Text), default=[])
    rep_excerpts = Column(JSONB, default=[])
    cluster_summary = Column(Text)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    docket = relationship("Docket", back_populates="theme_clusters")
    memberships = relationship("ClusterMembership", back_populates="cluster")


class ClusterMembership(Base):
    __tablename__ = "cluster_memberships"

    submission_id = Column(Integer, ForeignKey("submissions.id"), primary_key=True)
    cluster_id = Column(Integer, ForeignKey("theme_clusters.id"), primary_key=True)
    relevance_score = Column(Float)
    is_representative = Column(Boolean, default=False)

    submission = relationship("Submission", back_populates="cluster_memberships")
    cluster = relationship("ThemeCluster", back_populates="memberships")


# ---------------------------------------------------------------------------
# Report runs
# ---------------------------------------------------------------------------


class ReportRun(Base):
    __tablename__ = "report_runs"

    id = Column(Integer, primary_key=True)
    docket_id = Column(Integer, ForeignKey("dockets.id"), nullable=False)
    report_path = Column(Text)
    config = Column(JSONB, default={})
    status = Column(String(50), default="pending")
    created_at = Column(DateTime, default=func.now())

    docket = relationship("Docket", back_populates="report_runs")
    claim_sources = relationship("ReportClaimSource", back_populates="report_run")


class ReportClaimSource(Base):
    __tablename__ = "report_claim_sources"

    id = Column(Integer, primary_key=True)
    report_run_id = Column(Integer, ForeignKey("report_runs.id"), nullable=False)
    claim_text = Column(Text)
    submission_id = Column(Integer, ForeignKey("submissions.id"))
    source_excerpt = Column(Text)
    span_start = Column(Integer)
    span_end = Column(Integer)
    created_at = Column(DateTime, default=func.now())

    report_run = relationship("ReportRun", back_populates="claim_sources")


# ---------------------------------------------------------------------------
# Pipeline jobs
# ---------------------------------------------------------------------------


class PipelineJob(Base):
    __tablename__ = "pipeline_jobs"

    id = Column(Integer, primary_key=True)
    docket_id = Column(Integer, ForeignKey("dockets.id"), nullable=False)
    stage = Column(String(100), nullable=False)
    status = Column(Enum(JobStatus), default=JobStatus.pending)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    items_processed = Column(Integer, default=0)
    items_failed = Column(Integer, default=0)
    error = Column(Text)
    artifacts = Column(JSONB, default={})
    config_snapshot = Column(JSONB, default={})
    created_at = Column(DateTime, default=func.now())

    docket = relationship("Docket", back_populates="pipeline_jobs")

    __table_args__ = (Index("ix_pipeline_jobs_docket_stage", "docket_id", "stage"),)
