"""Shared HTTP client with retry logic and rate limiting."""
from __future__ import annotations

import time
from typing import Optional

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from cftc_pipeline.config import settings


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (compatible; CFTCCommentResearcher/1.0; "
                "+https://github.com/cftc-pipeline)"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
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


@retry(
    retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
    stop=stop_after_attempt(settings.max_retries),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def fetch(url: str, method: str = "GET", **kwargs) -> requests.Response:
    """Fetch URL with rate limiting and retry."""
    _rate_limiter.wait()
    session = get_session()
    kwargs.setdefault("timeout", settings.request_timeout_seconds)
    resp = session.request(method, url, **kwargs)
    resp.raise_for_status()
    return resp
