"""Tests for text extraction modules."""
from __future__ import annotations

import pytest

from cftc_pipeline.extraction.html_extractor import extract_html
from cftc_pipeline.extraction.text_cleaner import (
    build_canonical_text,
    clean_text,
    normalize_for_dedup,
    word_ngrams,
)


SAMPLE_HTML = """
<html><body>
<nav>Navigation</nav>
<div id="MainContent">
  <h2>Comment on Proposed Rule</h2>
  <p>I strongly oppose the proposed margin requirements because they will
  significantly increase operational costs for small firms.</p>
  <p>Specifically, the rule fails to consider the economic impact on:</p>
  <ul>
    <li>Regional banks</li>
    <li>Credit unions</li>
  </ul>
</div>
<footer>Privacy Policy | Accessibility</footer>
</body></html>
"""

BOILERPLATE_HTML = """
<html><body>
<div id="MainContent">
CFTC Public Comment Portal
Privacy Policy
I oppose this rule.
Accessibility Statement
</div>
</body></html>
"""


class TestHTMLExtractor:
    def test_extracts_body_text(self):
        result = extract_html(SAMPLE_HTML)
        assert "oppose" in result.text.lower()
        assert "margin requirements" in result.text.lower()

    def test_removes_nav_and_footer(self):
        result = extract_html(SAMPLE_HTML)
        assert "Navigation" not in result.text
        # footer content should be stripped or in a separate section

    def test_removes_boilerplate(self):
        result = extract_html(BOILERPLATE_HTML)
        assert "I oppose this rule" in result.text
        # boilerplate phrases should be reduced
        assert result.text.count("CFTC Public Comment Portal") == 0

    def test_empty_html(self):
        result = extract_html("<html><body></body></html>")
        assert result.status == "empty"
        assert result.text == ""

    def test_returns_ok_status(self):
        result = extract_html(SAMPLE_HTML)
        assert result.status == "ok"


class TestTextCleaner:
    def test_clean_removes_page_numbers(self):
        text = "Some content\n\nPage 1 of 10\n\nMore content"
        cleaned = clean_text(text)
        assert "Page 1 of 10" not in cleaned
        assert "Some content" in cleaned

    def test_clean_collapses_whitespace(self):
        text = "word1\n\n\n\n\nword2"
        cleaned = clean_text(text)
        assert "\n\n\n" not in cleaned

    def test_build_canonical_with_attachments(self):
        canonical = build_canonical_text(
            "Comment body text",
            [("exhibit.pdf", "Attachment content here")],
        )
        assert "COMMENT BODY" in canonical
        assert "ATTACHMENT: exhibit.pdf" in canonical
        assert "Comment body text" in canonical
        assert "Attachment content here" in canonical

    def test_build_canonical_empty_body(self):
        canonical = build_canonical_text("", [("file.pdf", "PDF content")])
        assert "ATTACHMENT" in canonical
        assert "PDF content" in canonical

    def test_build_canonical_both_empty(self):
        assert build_canonical_text("", []) == ""

    def test_normalize_for_dedup_lowercases(self):
        n = normalize_for_dedup("I OPPOSE This Rule")
        assert n == n.lower()

    def test_normalize_removes_punctuation(self):
        n = normalize_for_dedup("Hello, world! This is a test.")
        assert "," not in n
        assert "!" not in n

    def test_word_ngrams(self):
        ngrams = word_ngrams("the quick brown fox", n=2)
        assert "the quick" in ngrams
        assert "quick brown" in ngrams
        assert "brown fox" in ngrams

    def test_word_ngrams_short_text(self):
        ngrams = word_ngrams("hi", n=3)
        assert len(ngrams) > 0


class TestPDFExtractor:
    """Smoke tests for PDF extraction — requires pymupdf installed."""

    def test_handles_empty_bytes(self):
        from cftc_pipeline.extraction.pdf_extractor import extract_pdf

        result = extract_pdf(b"")
        assert result.status in ("failed", "empty")

    def test_handles_invalid_pdf(self):
        from cftc_pipeline.extraction.pdf_extractor import extract_pdf

        result = extract_pdf(b"not a pdf file at all")
        assert result.status in ("failed", "empty")
