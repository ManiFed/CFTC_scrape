"""Prompt v1: per-comment structured extraction."""

VERSION = "v1"

SYSTEM = """\
You are a regulatory analyst assistant specializing in CFTC rulemaking.
Your task is to analyze public comment submissions and extract structured information.
You must return ONLY valid JSON matching the specified schema — no prose, no markdown fences.
Be precise, cite actual excerpts from the text, and do not fabricate information not present in the comment.
"""

HUMAN_TEMPLATE = """\
Analyze the following public comment submitted to the CFTC and extract the requested fields.

## Comment metadata
- Commenter: {commenter_name}
- Organization: {organization}
- Submission date: {submission_date}

## Comment text
<comment>
{text}
</comment>

## Required JSON output

Return a JSON object with exactly these fields:

{{
  "summary_short": "string — 1-3 sentence summary, ≤100 words",
  "summary_detailed": "string — comprehensive summary capturing all major points, ≤400 words",
  "stance": "support | oppose | mixed | neutral | unclear — stance toward the proposed rule",
  "commenter_type": "individual | company | trade_association | nonprofit | academic | government | other",
  "commenter_name": "string — name as stated in the comment, or from metadata",
  "organization": "string or null — organization name if any",
  "issues": ["list of specific regulatory issues raised"],
  "requested_changes": ["list of specific changes or actions requested"],
  "legal_arguments": ["list of legal arguments or statutory/regulatory citations made"],
  "economic_arguments": ["list of economic or market-impact arguments made"],
  "operational_arguments": ["list of operational or implementation concerns raised"],
  "policy_arguments": ["list of policy or public-interest arguments made"],
  "cited_authorities": ["list of statutes, rules, cases, studies cited by name"],
  "notable_quotes": [
    {{"quote": "verbatim excerpt", "span_hint": "first few words to locate it"}}
  ],
  "template_likelihood": 0.0,
  "substantive_score": 0.0,
  "confidence": 0.0,
  "source_spans": [
    {{"claim": "extracted claim", "excerpt": "verbatim supporting text"}}
  ]
}}

Scoring guidance:
- template_likelihood: 0.0 = fully original; 0.5 = partly templated; 1.0 = pure form letter
- substantive_score: 0.0 = trivial or empty ("I oppose this"); 0.5 = moderate detail; 1.0 = detailed legal/economic analysis
- confidence: your confidence (0-1) that the above extraction is accurate given the text quality

Only include non-empty arrays. Return pure JSON.
"""


def build_prompt(
    commenter_name: str,
    organization: str,
    submission_date: str,
    text: str,
) -> tuple[str, str]:
    """Return (system, human) prompt strings."""
    human = HUMAN_TEMPLATE.format(
        commenter_name=commenter_name or "Unknown",
        organization=organization or "None",
        submission_date=submission_date or "Unknown",
        text=text[:12000],  # truncate to avoid token limits
    )
    return SYSTEM, human
