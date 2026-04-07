"""CFTC public comment site scraper.

The CFTC public comment portal lives at:
  https://comments.cftc.gov/PublicComments/CommentList.aspx?id=<docket_id>

The list page is an ASP.NET WebForms page that uses __VIEWSTATE paging.
We POST with __EVENTARGUMENT=Page$N to advance pages.

Comment detail pages live at:
  https://comments.cftc.gov/PublicComments/ViewComment.aspx?id=<comment_id>&SearchText=

Attachments are linked from the detail page.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterator, Optional
from urllib.parse import urljoin, urlparse, parse_qs

from bs4 import BeautifulSoup

from cftc_pipeline.config import settings
from cftc_pipeline.scraper.http_client import fetch

logger = logging.getLogger(__name__)

BASE = settings.cftc_base_url
LIST_PATH = "/PublicComments/CommentList.aspx"
DETAIL_PATH = "/PublicComments/ViewComment.aspx"


@dataclass
class CommentListEntry:
    """Metadata parsed from a single row on the comment list page."""

    external_id: str
    commenter_name: str
    organization: Optional[str]
    submission_date: Optional[datetime]
    detail_url: str


@dataclass
class CommentDetail:
    """Full metadata and body from a comment detail page."""

    external_id: str
    commenter_name: str
    organization: Optional[str]
    submission_date: Optional[datetime]
    received_date: Optional[datetime]
    body_text: str
    body_html: str
    attachment_urls: list[dict]  # [{"url": ..., "filename": ...}]
    raw_html: bytes


def _extract_viewstate(soup: BeautifulSoup) -> dict:
    """Pull ASP.NET hidden fields needed for paging POSTs."""
    fields = {}
    for name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]:
        tag = soup.find("input", {"name": name})
        if tag:
            fields[name] = tag.get("value", "")
    return fields


def _parse_date(text: str) -> Optional[datetime]:
    text = text.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%B %d, %Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def crawl_comment_list(docket_url: str) -> Iterator[CommentListEntry]:
    """Yield every comment list entry for a docket, handling pagination."""
    page_num = 1
    post_data: dict = {}

    while True:
        logger.info("Fetching list page %d: %s", page_num, docket_url)

        if page_num == 1:
            resp = fetch(docket_url)
        else:
            resp = fetch(docket_url, method="POST", data=post_data)

        html = resp.content
        soup = BeautifulSoup(html, "lxml")

        entries = list(_parse_list_page(soup))
        if not entries:
            logger.info("No entries on page %d — done.", page_num)
            break

        yield from entries

        # Check for next page
        next_link = _find_next_page_link(soup, page_num)
        if next_link is None:
            break

        # Build next POST body
        vstate = _extract_viewstate(soup)
        post_data = {
            **vstate,
            "__EVENTTARGET": "ctl00$MainContent$gvCommentList",
            "__EVENTARGUMENT": f"Page${page_num + 1}",
        }
        page_num += 1


def _parse_list_page(soup: BeautifulSoup) -> Iterator[CommentListEntry]:
    """Parse comment rows from a list page."""
    table = soup.find("table", id=re.compile(r"gvCommentList", re.I))
    if table is None:
        # Fallback: look for any table with comment links
        table = soup.find("table", class_=re.compile(r"grid|comment", re.I))
    if table is None:
        return

    rows = table.find_all("tr")
    for row in rows[1:]:  # skip header
        cells = row.find_all("td")
        if not cells:
            continue
        try:
            entry = _parse_list_row(cells)
            if entry:
                yield entry
        except Exception as exc:
            logger.warning("Failed to parse row: %s", exc)


def _parse_list_row(cells) -> Optional[CommentListEntry]:
    """Parse a single row from the comment list table."""
    # CFTC list columns vary but typically:
    # [0] commenter name (with detail link), [1] organization, [2] date
    if len(cells) < 2:
        return None

    name_cell = cells[0]
    link = name_cell.find("a")
    if not link:
        return None

    href = link.get("href", "")
    if not href:
        return None

    detail_url = urljoin(BASE, href)

    # Extract comment ID from URL
    parsed = urlparse(detail_url)
    qs = parse_qs(parsed.query)
    external_id = qs.get("id", [None])[0] or href

    commenter_name = link.get_text(strip=True) or "Unknown"

    organization = None
    if len(cells) > 1:
        organization = cells[1].get_text(strip=True) or None

    submission_date = None
    if len(cells) > 2:
        submission_date = _parse_date(cells[2].get_text(strip=True))
    elif len(cells) > 1:
        # Some pages have date in col 1
        date_text = cells[-1].get_text(strip=True)
        submission_date = _parse_date(date_text)

    return CommentListEntry(
        external_id=str(external_id),
        commenter_name=commenter_name,
        organization=organization,
        submission_date=submission_date,
        detail_url=detail_url,
    )


def _find_next_page_link(soup: BeautifulSoup, current_page: int) -> Optional[str]:
    """Return next-page link text/argument if one exists."""
    # Look for page number links in the pager row
    pager = soup.find("tr", class_=re.compile(r"pager|GridPager", re.I))
    if pager is None:
        # Try td with page links
        pager = soup.find("td", class_=re.compile(r"pager", re.I))
    if pager is None:
        return None

    links = pager.find_all("a")
    for link in links:
        text = link.get_text(strip=True)
        if text == str(current_page + 1):
            return link.get("href") or "next"
        if text in (">", "Next", "»"):
            return link.get("href") or "next"
    return None


def fetch_comment_detail(entry: CommentListEntry) -> CommentDetail:
    """Fetch and parse a single comment detail page."""
    resp = fetch(entry.detail_url)
    raw_html = resp.content
    soup = BeautifulSoup(raw_html, "lxml")

    # Extract body text
    body_div = (
        soup.find("div", id=re.compile(r"comment.*body|body.*comment", re.I))
        or soup.find("div", class_=re.compile(r"comment.*text|comment.*content", re.I))
        or soup.find("div", id="MainContent")
        or soup.find("main")
    )

    body_html = str(body_div) if body_div else ""
    body_text = body_div.get_text(separator="\n", strip=True) if body_div else ""

    # Extract dates from detail page (may be more precise than list)
    received_date = None
    date_labels = soup.find_all(string=re.compile(r"received|date received", re.I))
    for label in date_labels:
        parent = label.parent
        if parent:
            sibling = parent.find_next_sibling()
            if sibling:
                received_date = _parse_date(sibling.get_text(strip=True))
                if received_date:
                    break

    # Extract attachment links
    attachment_urls = _extract_attachments(soup, entry.detail_url)

    return CommentDetail(
        external_id=entry.external_id,
        commenter_name=entry.commenter_name,
        organization=entry.organization,
        submission_date=entry.submission_date,
        received_date=received_date,
        body_text=body_text,
        body_html=body_html,
        attachment_urls=attachment_urls,
        raw_html=raw_html,
    )


def _extract_attachments(soup: BeautifulSoup, base_url: str) -> list[dict]:
    """Find all attachment links on a detail page."""
    attachments = []
    seen_urls: set[str] = set()

    # Look for links to PDF/DOCX/TXT files
    file_extensions = re.compile(r"\.(pdf|docx?|txt|xlsx?|pptx?)$", re.I)

    for link in soup.find_all("a", href=True):
        href = link["href"]
        if not href:
            continue

        # Check the full href (including query string) and link text for file extensions
        link_text = link.get_text(strip=True)
        if not file_extensions.search(href) and not file_extensions.search(link_text):
            # Also check for download-style URLs
            if "download" not in href.lower() and "attachment" not in href.lower():
                continue

        abs_url = urljoin(base_url, href)
        if abs_url in seen_urls:
            continue
        seen_urls.add(abs_url)

        # Prefer the fileName query param (CFTC pattern), then path, then link text
        qs = parse_qs(urlparse(href).query)
        filename = (
            qs.get("fileName", qs.get("filename", qs.get("FileName", [None])))[0]
            or href.split("/")[-1].split("?")[0]
            or link.get_text(strip=True)
            or "attachment"
        )

        attachments.append(
            {
                "url": abs_url,
                "filename": filename,
                "link_text": link.get_text(strip=True),
            }
        )

    return attachments
