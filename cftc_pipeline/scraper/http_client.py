"""Shared HTTP client with retry logic and rate limiting."""
from __future__ import annotations

import time
from typing import Optional
from urllib.parse import urlparse

import requests
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from cftc_pipeline.config import settings


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
    )
    return s


_session: Optional[requests.Session] = None


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = _make_session()
    return _session


class RateLimiter:
    def __init__(self, delay: float):
        self.delay = delay
        self._last = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self._last
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self._last = time.monotonic()


_rate_limiter = RateLimiter(settings.request_delay_seconds)


def _is_retryable_http_error(exc: BaseException) -> bool:
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code in {403, 429, 500, 502, 503, 504}
    return False


def _cftc_headers_for_url(url: str, referer: str = "") -> dict[str, str]:
    parsed = urlparse(url)
    if parsed.netloc.lower() != "comments.cftc.gov":
        return {}
    # Use the caller-supplied referer when available, otherwise fall back to the
    # plain list page (still valid for the initial GET).
    effective_referer = referer or "https://comments.cftc.gov/PublicComments/CommentList.aspx"
    return {
        "Origin": "https://comments.cftc.gov",
        "Referer": effective_referer,
    }


@retry(
    retry=retry_if_exception(_is_retryable_http_error),
    stop=stop_after_attempt(settings.max_retries),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def fetch(url: str, method: str = "GET", **kwargs) -> requests.Response:
    """Fetch URL with rate limiting and retry."""
    _rate_limiter.wait()
    session = get_session()
    kwargs.setdefault("timeout", settings.request_timeout_seconds)
    caller_headers = dict(kwargs.pop("headers", {}))
    # Allow callers to pass an explicit Referer via headers; otherwise derive it.
    referer = caller_headers.pop("Referer", caller_headers.pop("referer", ""))
    cftc_headers = _cftc_headers_for_url(url, referer=referer)
    merged = {**cftc_headers, **caller_headers}
    if merged:
        kwargs["headers"] = merged
    resp = session.request(method, url, **kwargs)
    resp.raise_for_status()
    return resp
