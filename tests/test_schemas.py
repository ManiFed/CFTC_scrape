"""Tests for LLM analysis schema validation."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from cftc_pipeline.analysis.schemas import CommentAnalysis


VALID_ANALYSIS = {
    "summary_short": "Commenter opposes the rule citing economic harm.",
    "summary_detailed": "The commenter, a regional bank, argues the proposed margin requirements will increase operational costs by 30% and reduce lending capacity for small businesses. They cite the Dodd-Frank Act Section 4s and request a cost-benefit analysis.",
    "stance": "oppose",
    "commenter_type": "company",
    "commenter_name": "First National Bank",
    "organization": "First National Bank",
    "issues": ["margin requirements", "cost-benefit analysis"],
    "requested_changes": ["Conduct full cost-benefit analysis", "Extend compliance deadline by 18 months"],
    "legal_arguments": ["Dodd-Frank Act Section 4s requires cost-benefit analysis"],
    "economic_arguments": ["30% increase in operational costs", "Reduced lending to small businesses"],
    "operational_arguments": ["System upgrades require 18 months minimum"],
    "policy_arguments": ["Rule disproportionately impacts regional banks"],
    "cited_authorities": ["Dodd-Frank Act Section 4s", "CFTC v. Schor, 478 U.S. 833 (1986)"],
    "notable_quotes": [{"quote": "This rule will devastate regional lending.", "span_hint": "This rule will"}],
    "template_likelihood": 0.1,
    "substantive_score": 0.85,
    "confidence": 0.9,
    "source_spans": [{"claim": "30% cost increase", "excerpt": "our analysis shows a 30% increase in operational costs"}],
}


class TestCommentAnalysisSchema:
    def test_valid_analysis_parses(self):
        analysis = CommentAnalysis.model_validate(VALID_ANALYSIS)
        assert analysis.stance == "oppose"
        assert analysis.commenter_type == "company"
        assert len(analysis.issues) == 2
        assert analysis.substantive_score == 0.85

    def test_invalid_stance_rejected(self):
        invalid = {**VALID_ANALYSIS, "stance": "strongly_oppose"}
        with pytest.raises(ValidationError):
            CommentAnalysis.model_validate(invalid)

    def test_invalid_commenter_type_rejected(self):
        invalid = {**VALID_ANALYSIS, "commenter_type": "hedge_fund"}
        with pytest.raises(ValidationError):
            CommentAnalysis.model_validate(invalid)

    def test_score_out_of_range_rejected(self):
        invalid = {**VALID_ANALYSIS, "substantive_score": 1.5}
        with pytest.raises(ValidationError):
            CommentAnalysis.model_validate(invalid)

    def test_negative_score_rejected(self):
        invalid = {**VALID_ANALYSIS, "confidence": -0.1}
        with pytest.raises(ValidationError):
            CommentAnalysis.model_validate(invalid)

    def test_none_lists_become_empty(self):
        minimal = {
            "summary_short": "Short",
            "summary_detailed": "Detailed",
            "stance": "support",
            "commenter_type": "individual",
            "issues": None,
            "template_likelihood": 0.0,
            "substantive_score": 0.0,
            "confidence": 0.0,
        }
        analysis = CommentAnalysis.model_validate(minimal)
        assert analysis.issues == []
        assert analysis.legal_arguments == []

    def test_string_list_field_coerced(self):
        single_item = {**VALID_ANALYSIS, "issues": "single issue as string"}
        analysis = CommentAnalysis.model_validate(single_item)
        assert isinstance(analysis.issues, list)
        assert analysis.issues == ["single issue as string"]

    def test_missing_optional_fields_ok(self):
        minimal = {
            "summary_short": "Short summary",
            "summary_detailed": "Detailed summary",
            "stance": "neutral",
            "commenter_type": "other",
            "template_likelihood": 0.0,
            "substantive_score": 0.0,
            "confidence": 0.0,
        }
        analysis = CommentAnalysis.model_validate(minimal)
        assert analysis.organization is None
        assert analysis.commenter_name == ""

    def test_model_dump_serializable(self):
        import json
        analysis = CommentAnalysis.model_validate(VALID_ANALYSIS)
        dumped = analysis.model_dump()
        # Should be JSON serializable
        serialized = json.dumps(dumped)
        assert '"oppose"' in serialized
