"""Pydantic schemas for LLM extraction output validation."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


class SourceSpan(BaseModel):
    claim: str
    excerpt: str


class NotableQuote(BaseModel):
    quote: str
    span_hint: str = ""


class CommentAnalysis(BaseModel):
    summary_short: str = Field(..., description="Summary in ≤100 words")
    summary_detailed: str = Field(..., description="Detailed summary in ≤400 words")
    stance: Literal["support", "oppose", "mixed", "neutral", "unclear"]
    commenter_type: Literal[
        "individual",
        "company",
        "trade_association",
        "nonprofit",
        "academic",
        "government",
        "other",
    ]
    commenter_name: str = ""
    organization: Optional[str] = None
    issues: list[str] = Field(default_factory=list)
    requested_changes: list[str] = Field(default_factory=list)
    legal_arguments: list[str] = Field(default_factory=list)
    economic_arguments: list[str] = Field(default_factory=list)
    operational_arguments: list[str] = Field(default_factory=list)
    policy_arguments: list[str] = Field(default_factory=list)
    cited_authorities: list[str] = Field(default_factory=list)
    notable_quotes: list[NotableQuote] = Field(default_factory=list)
    template_likelihood: float = Field(
        0.0, ge=0.0, le=1.0, description="0=original, 1=pure form letter"
    )
    substantive_score: float = Field(
        0.0, ge=0.0, le=1.0, description="0=trivial, 1=highly substantive"
    )
    confidence: float = Field(0.0, ge=0.0, le=1.0)
    source_spans: list[SourceSpan] = Field(default_factory=list)

    @field_validator("issues", "requested_changes", "legal_arguments",
                     "economic_arguments", "operational_arguments",
                     "policy_arguments", "cited_authorities", mode="before")
    @classmethod
    def ensure_list(cls, v):
        if v is None:
            return []
        if isinstance(v, str):
            return [v] if v.strip() else []
        return v
