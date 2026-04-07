"""Tests for the CFTC scraper."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from bs4 import BeautifulSoup

from cftc_pipeline.scraper.cftc_scraper import (
    _extract_attachments,
    _parse_list_page,
    _parse_date,
)


SAMPLE_LIST_HTML = """
<html><body>
<table id="ctl00_MainContent_gvCommentList">
  <tr><th>Name</th><th>Organization</th><th>Date</th></tr>
  <tr>
    <td><a href="/PublicComments/ViewComment.aspx?id=12345">Jane Smith</a></td>
    <td>ACME Corp</td>
    <td>01/15/2024</td>
  </tr>
  <tr>
    <td><a href="/PublicComments/ViewComment.aspx?id=12346">Bob Jones</a></td>
    <td></td>
    <td>01/16/2024</td>
  </tr>
</table>
</body></html>
"""

SAMPLE_DETAIL_HTML = """
<html><body>
<div id="MainContent">
  <p>I strongly support this proposed rule because it will protect consumers.</p>
  <p>Please see the attached PDF for detailed analysis.</p>
</div>
<a href="/PublicComments/GetDocument?id=12345&amp;fileName=comment.pdf">comment.pdf</a>
<a href="/PublicComments/GetDocument?id=12345&amp;fileName=exhibit_a.docx">exhibit_a.docx</a>
</body></html>
"""


class TestParseDate:
    def test_us_format(self):
        from datetime import datetime
        result = _parse_date("01/15/2024")
        assert result == datetime(2024, 1, 15)

    def test_iso_format(self):
        from datetime import datetime
        result = _parse_date("2024-01-15")
        assert result == datetime(2024, 1, 15)

    def test_invalid(self):
        assert _parse_date("not a date") is None

    def test_empty(self):
        assert _parse_date("") is None


class TestParseListPage:
    def test_parses_entries(self):
        soup = BeautifulSoup(SAMPLE_LIST_HTML, "lxml")
        entries = list(_parse_list_page(soup))
        assert len(entries) == 2

    def test_entry_fields(self):
        soup = BeautifulSoup(SAMPLE_LIST_HTML, "lxml")
        entries = list(_parse_list_page(soup))
        assert entries[0].external_id == "12345"
        assert entries[0].commenter_name == "Jane Smith"
        assert entries[0].organization == "ACME Corp"
        assert entries[0].submission_date is not None

    def test_empty_organization(self):
        soup = BeautifulSoup(SAMPLE_LIST_HTML, "lxml")
        entries = list(_parse_list_page(soup))
        assert entries[1].organization is None or entries[1].organization == ""


class TestExtractAttachments:
    def test_finds_pdf_and_docx(self):
        soup = BeautifulSoup(SAMPLE_DETAIL_HTML, "lxml")
        atts = _extract_attachments(soup, "https://comments.cftc.gov/PublicComments/ViewComment.aspx?id=12345")
        filenames = [a["filename"] for a in atts]
        assert any("pdf" in f for f in filenames)
        assert any("docx" in f for f in filenames)

    def test_no_duplicate_urls(self):
        soup = BeautifulSoup(SAMPLE_DETAIL_HTML, "lxml")
        atts = _extract_attachments(soup, "https://comments.cftc.gov/")
        urls = [a["url"] for a in atts]
        assert len(urls) == len(set(urls))
