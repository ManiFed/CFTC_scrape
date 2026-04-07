"""Multi-factor ranking of submissions by signal value."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class RankedSubmission:
    submission_id: int
    total_score: float
    substantive_score: float
    legal_density: float
    institutional_score: float
    novelty_score: float
    rank: int


# Commenter type weights
INSTITUTIONAL_TYPES = {
    "trade_association": 0.9,
    "company": 0.7,
    "government": 0.8,
    "nonprofit": 0.6,
    "academic": 0.8,
    "individual": 0.2,
    "other": 0.3,
}


def score_submission(analysis: dict, text_length: int, is_canonical: bool) -> dict:
    """
    Compute multi-factor signal score for a submission.

    analysis: dict from LLMAnalysis (or CommentAnalysis.model_dump())
    """
    # Base substantive score from LLM
    substantive = float(analysis.get("substantive_score") or 0.0)

    # Legal density: count of legal_arguments + cited_authorities
    legal_args = analysis.get("legal_arguments") or []
    cited = analysis.get("cited_authorities") or []
    legal_density = min(1.0, (len(legal_args) + len(cited)) / 10.0)

    # Institutional weight
    ctype = analysis.get("commenter_type") or "other"
    institutional = INSTITUTIONAL_TYPES.get(ctype, 0.3)

    # Penalize form letters and non-canonical (duplicate) submissions
    template_penalty = float(analysis.get("template_likelihood") or 0.0)
    canonical_bonus = 1.0 if is_canonical else 0.3

    # Breadth of issues raised
    issues = analysis.get("issues") or []
    issue_breadth = min(1.0, len(issues) / 8.0)

    # Text length signal (log-scaled, normalized)
    import math
    length_score = min(1.0, math.log1p(text_length) / math.log1p(20000))

    total = (
        substantive * 0.35
        + legal_density * 0.25
        + institutional * 0.15
        + issue_breadth * 0.10
        + length_score * 0.10
        - template_penalty * 0.15
    ) * canonical_bonus

    return {
        "total_score": round(max(0.0, min(1.0, total)), 4),
        "substantive_score": substantive,
        "legal_density": legal_density,
        "institutional_score": institutional,
        "novelty_score": 0.0,  # filled in by cluster outlier detection
    }


def rank_submissions(
    submissions: list[dict],
) -> list[RankedSubmission]:
    """
    Rank all submissions.

    Each item: {"id": int, "analysis": dict, "text_length": int, "is_canonical": bool}
    Returns sorted list, rank 1 = most signal.
    """
    scored = []
    for sub in submissions:
        scores = score_submission(
            analysis=sub.get("analysis") or {},
            text_length=sub.get("text_length") or 0,
            is_canonical=sub.get("is_canonical", True),
        )
        scored.append((sub["id"], scores))

    scored.sort(key=lambda x: x[1]["total_score"], reverse=True)

    results = []
    for rank, (sid, scores) in enumerate(scored, start=1):
        results.append(
            RankedSubmission(
                submission_id=sid,
                total_score=scores["total_score"],
                substantive_score=scores["substantive_score"],
                legal_density=scores["legal_density"],
                institutional_score=scores["institutional_score"],
                novelty_score=scores["novelty_score"],
                rank=rank,
            )
        )
    return results


def find_outliers(
    submissions: list[dict],
    cluster_memberships: dict[int, int],  # submission_id -> cluster_id
    top_n: int = 10,
) -> list[int]:
    """
    Find high-scoring submissions that are NOT in any major cluster (noise points
    or small clusters) — these are novel or outlier arguments.
    """
    noise_sids = [
        sub["id"]
        for sub in submissions
        if cluster_memberships.get(sub["id"], -1) == -1
    ]
    if not noise_sids:
        return []

    # Score them and return top_n
    noise_scored = []
    for sub in submissions:
        if sub["id"] in noise_sids:
            scores = score_submission(
                analysis=sub.get("analysis") or {},
                text_length=sub.get("text_length") or 0,
                is_canonical=sub.get("is_canonical", True),
            )
            noise_scored.append((sub["id"], scores["total_score"]))

    noise_scored.sort(key=lambda x: x[1], reverse=True)
    return [sid for sid, _ in noise_scored[:top_n]]
