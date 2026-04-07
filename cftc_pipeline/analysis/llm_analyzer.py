"""LLM-based structured per-comment analysis."""
from __future__ import annotations

import json
import logging
from typing import Optional

from openai import OpenAI
from pydantic import ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential

from cftc_pipeline.analysis.prompts import v1_extraction
from cftc_pipeline.analysis.schemas import CommentAnalysis
from cftc_pipeline.config import settings

logger = logging.getLogger(__name__)

PROMPT_REGISTRY = {
    "v1": v1_extraction,
}

_client: Optional[OpenAI] = None


def get_client() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI(api_key=settings.openai_auth_token())
    return _client


def _extract_json(text: str) -> dict:
    """Extract JSON from model response, handling markdown fences."""
    text = text.strip()
    # Strip markdown fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.startswith("```")]
        text = "\n".join(lines).strip()
    return json.loads(text)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _call_llm(system: str, human: str) -> str:
    client = get_client()
    message = client.responses.create(
        model=settings.llm_model,
        max_output_tokens=settings.llm_max_tokens,
        instructions=system,
        input=human,
    )
    return message.output_text


def analyze_submission(
    submission_id: int,
    commenter_name: str,
    organization: str,
    submission_date: str,
    text: str,
    prompt_version: str = None,
) -> tuple[CommentAnalysis, str, str]:
    """
    Analyze a single submission.

    Returns:
        (CommentAnalysis, model_id, prompt_version)
    """
    version = prompt_version or settings.prompt_version
    prompt_module = PROMPT_REGISTRY.get(version)
    if prompt_module is None:
        raise ValueError(f"Unknown prompt version: {version}")

    system, human = prompt_module.build_prompt(
        commenter_name=commenter_name,
        organization=organization,
        submission_date=submission_date,
        text=text,
    )

    if not text or not text.strip():
        # Return a minimal analysis for empty submissions
        return (
            CommentAnalysis(
                summary_short="No text content available.",
                summary_detailed="This submission contained no extractable text.",
                stance="unclear",
                commenter_type="individual",
                substantive_score=0.0,
                confidence=0.0,
                template_likelihood=0.0,
            ),
            settings.llm_model,
            version,
        )

    for attempt in range(3):
        try:
            raw_response = _call_llm(system, human)
            data = _extract_json(raw_response)
            analysis = CommentAnalysis.model_validate(data)
            return analysis, settings.llm_model, version
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning(
                "Attempt %d: failed to parse LLM response for submission %d: %s",
                attempt + 1,
                submission_id,
                exc,
            )
            if attempt == 2:
                raise

    raise RuntimeError("Exhausted retries")  # unreachable


def batch_analyze(
    submissions: list[dict],
    prompt_version: str = None,
    max_concurrent: int = None,
) -> list[tuple[int, CommentAnalysis | Exception]]:
    """
    Analyze multiple submissions.

    Each item in submissions: {"id": int, "commenter_name": str, "organization": str,
                               "submission_date": str, "text": str}

    Returns list of (submission_id, result_or_exception).
    """
    import concurrent.futures

    max_workers = max_concurrent or settings.batch_size_llm
    results = []

    def _analyze_one(sub: dict):
        try:
            analysis, model_id, version = analyze_submission(
                submission_id=sub["id"],
                commenter_name=sub.get("commenter_name", ""),
                organization=sub.get("organization", ""),
                submission_date=str(sub.get("submission_date", "")),
                text=sub.get("text", ""),
                prompt_version=prompt_version,
            )
            return sub["id"], analysis
        except Exception as exc:
            logger.error("Analysis failed for submission %d: %s", sub["id"], exc)
            return sub["id"], exc

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_analyze_one, sub): sub for sub in submissions}
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    return results
