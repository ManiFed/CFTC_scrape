"""Tests for HTTP client behavior around CFTC blocking responses."""
from __future__ import annotations

import requests

from cftc_pipeline.scraper.http_client import _cftc_headers_for_url, _is_retryable_http_error


def test_cftc_headers_added_for_cftc_domain():
    headers = _cftc_headers_for_url("https://comments.cftc.gov/PublicComments/CommentList.aspx?id=7654")
    assert headers["Origin"] == "https://comments.cftc.gov"
    assert "CommentList.aspx" in headers["Referer"]


def test_no_cftc_headers_for_other_domains():
    headers = _cftc_headers_for_url("https://example.com/foo")
    assert headers == {}


def test_retryable_http_status_codes_include_403():
    response = requests.Response()
    response.status_code = 403
    err = requests.HTTPError(response=response)
    assert _is_retryable_http_error(err)


def test_non_retryable_http_status_codes_exclude_404():
    response = requests.Response()
    response.status_code = 404
    err = requests.HTTPError(response=response)
    assert not _is_retryable_http_error(err)
