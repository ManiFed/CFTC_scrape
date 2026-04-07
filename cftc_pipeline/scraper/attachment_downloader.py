"""Download and store attachments."""
from __future__ import annotations

import logging
import mimetypes
import re
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse

from cftc_pipeline.scraper.http_client import fetch
from cftc_pipeline.storage import attachment_key, sha256, storage

logger = logging.getLogger(__name__)

EXT_FROM_MIME = {
    "application/pdf": ".pdf",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "text/plain": ".txt",
    "text/html": ".html",
}


def _guess_extension(url: str, content_type: str) -> str:
    # Try from content-type first
    ct = content_type.split(";")[0].strip().lower()
    if ct in EXT_FROM_MIME:
        return EXT_FROM_MIME[ct]
    # From URL
    path = unquote(urlparse(url).path)
    suffix = Path(path).suffix
    if suffix:
        return suffix
    return ".bin"


def _sanitize_filename(name: str) -> str:
    name = re.sub(r"[^\w\-.]", "_", name)
    return name[:200]


def download_attachment(
    url: str,
    docket_id: str,
    external_id: str,
    suggested_filename: Optional[str] = None,
) -> dict:
    """Download one attachment, store it, return metadata dict."""
    logger.info("Downloading attachment: %s", url)

    try:
        resp = fetch(url, stream=True)
    except Exception as exc:
        logger.error("Failed to download %s: %s", url, exc)
        return {
            "url": url,
            "status": "failed",
            "error": str(exc),
            "file_path": None,
            "file_type": None,
            "file_size": None,
            "content_hash": None,
            "filename": suggested_filename,
        }

    content_type = resp.headers.get("content-type", "")
    data = resp.content

    ext = _guess_extension(url, content_type)
    if suggested_filename and "." in suggested_filename:
        filename = _sanitize_filename(suggested_filename)
    else:
        url_path = unquote(urlparse(url).path).split("/")[-1]
        filename = _sanitize_filename(url_path or f"attachment{ext}")
        if not Path(filename).suffix:
            filename += ext

    key = attachment_key(docket_id, external_id, filename)
    file_path = storage.write(key, data)
    content_hash = sha256(data)
    file_type = ext.lstrip(".").lower()

    return {
        "url": url,
        "status": "downloaded",
        "file_path": file_path,
        "file_type": file_type,
        "file_size": len(data),
        "content_hash": content_hash,
        "filename": filename,
        "error": None,
    }
