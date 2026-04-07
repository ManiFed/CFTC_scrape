"""Tests for ranking logic."""
from __future__ import annotations

import pytest

from cftc_pipeline.ranking.ranker import rank_submissions, score_submission


BASE_ANALYSIS = {
    "substantive_score": 0.5,
    "legal_arguments": ["arg1", "arg2"],
    "cited_authorities": ["statute1"],
    "commenter_type": "individual",
    "template_likelihood": 0.0,
    "issues": ["issue1", "issue2"],
    "economic_arguments": [],
    "operational_arguments": [],
}


class TestScoreSubmission:
    def test_institutional_scores_higher_than_individual(self):
        individual = {**BASE_ANALYSIS, "commenter_type": "individual"}
        institutional = {**BASE_ANALYSIS, "commenter_type": "trade_association"}

        s_ind = score_submission(individual, text_length=5000, is_canonical=True)
        s_inst = score_submission(institutional, text_length=5000, is_canonical=True)

        assert s_inst["total_score"] > s_ind["total_score"]

    def test_high_template_penalizes_score(self):
        original = {**BASE_ANALYSIS, "template_likelihood": 0.0}
        form_letter = {**BASE_ANALYSIS, "template_likelihood": 1.0}

        s_orig = score_submission(original, text_length=5000, is_canonical=True)
        s_form = score_submission(form_letter, text_length=5000, is_canonical=True)

        assert s_orig["total_score"] > s_form["total_score"]

    def test_non_canonical_penalized(self):
        s_canonical = score_submission(BASE_ANALYSIS, text_length=5000, is_canonical=True)
        s_dup = score_submission(BASE_ANALYSIS, text_length=5000, is_canonical=False)

        assert s_canonical["total_score"] > s_dup["total_score"]

    def test_score_bounded_0_to_1(self):
        for is_canonical in [True, False]:
            for template in [0.0, 0.5, 1.0]:
                analysis = {**BASE_ANALYSIS, "template_likelihood": template}
                scores = score_submission(analysis, text_length=10000, is_canonical=is_canonical)
                assert 0.0 <= scores["total_score"] <= 1.0

    def test_more_legal_args_increases_score(self):
        few_legal = {**BASE_ANALYSIS, "legal_arguments": ["arg1"], "cited_authorities": []}
        many_legal = {**BASE_ANALYSIS, "legal_arguments": ["a", "b", "c", "d", "e"], "cited_authorities": ["s1", "s2", "s3"]}

        s_few = score_submission(few_legal, 5000, True)
        s_many = score_submission(many_legal, 5000, True)

        assert s_many["total_score"] > s_few["total_score"]

    def test_longer_text_slightly_better(self):
        s_short = score_submission(BASE_ANALYSIS, text_length=100, is_canonical=True)
        s_long = score_submission(BASE_ANALYSIS, text_length=10000, is_canonical=True)

        assert s_long["total_score"] > s_short["total_score"]


class TestRankSubmissions:
    def test_returns_correct_count(self):
        subs = [
            {"id": i, "analysis": BASE_ANALYSIS, "text_length": 1000, "is_canonical": True}
            for i in range(5)
        ]
        ranked = rank_submissions(subs)
        assert len(ranked) == 5

    def test_ranks_start_at_1(self):
        subs = [
            {"id": 1, "analysis": BASE_ANALYSIS, "text_length": 1000, "is_canonical": True}
        ]
        ranked = rank_submissions(subs)
        assert ranked[0].rank == 1

    def test_higher_score_gets_lower_rank_number(self):
        low_score = {**BASE_ANALYSIS, "substantive_score": 0.1, "legal_arguments": []}
        high_score = {**BASE_ANALYSIS, "substantive_score": 0.9, "legal_arguments": ["a"] * 8, "cited_authorities": ["s"] * 3, "commenter_type": "trade_association"}

        subs = [
            {"id": 1, "analysis": low_score, "text_length": 100, "is_canonical": True},
            {"id": 2, "analysis": high_score, "text_length": 10000, "is_canonical": True},
        ]
        ranked = rank_submissions(subs)
        rank_by_id = {r.submission_id: r.rank for r in ranked}
        assert rank_by_id[2] < rank_by_id[1]

    def test_empty_analysis_handled(self):
        subs = [{"id": 1, "analysis": None, "text_length": 0, "is_canonical": True}]
        ranked = rank_submissions(subs)
        assert len(ranked) == 1
        assert 0.0 <= ranked[0].total_score <= 1.0
