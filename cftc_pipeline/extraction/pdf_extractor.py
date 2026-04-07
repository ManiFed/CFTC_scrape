"""PDF text extraction using PyMuPDF (primary) and pdfplumber (fallback)."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ExtractionOutput:
    text: str
    page_count: int
    method: str
    status: str  # ok / partial / failed / empty
    error: Optional[str] = None


def extract_pdf(data: bytes) -> ExtractionOutput:
    """Extract text from PDF bytes. Tries PyMuPDF then pdfplumber."""
    result = _try_pymupdf(data)
    if result.status == "ok" and result.text.strip():
        return result

    logger.info("PyMuPDF produced little text, trying pdfplumber")
    fallback = _try_pdfplumber(data)
    if fallback.status == "ok" and len(fallback.text) > len(result.text):
        return fallback

    if result.text.strip():
        return result  # return whatever we have
    return fallback


def _try_pymupdf(data: bytes) -> ExtractionOutput:
    try:
        import fitz  # PyMuPDF

        doc = fitz.open(stream=data, filetype="pdf")
        pages = []
        for i, page in enumerate(doc):
            text = page.get_text("text")
            if text.strip():
                pages.append(f"[Page {i + 1}]\n{text}")
        doc.close()

        full_text = "\n\n".join(pages)
        status = "ok" if full_text.strip() else "empty"
        return ExtractionOutput(
            text=full_text, page_count=len(doc), method="pymupdf", status=status
        )
    except Exception as exc:
        logger.warning("PyMuPDF failed: %s", exc)
        return ExtractionOutput(text="", page_count=0, method="pymupdf", status="failed", error=str(exc))


def _try_pdfplumber(data: bytes) -> ExtractionOutput:
    try:
        import io
        import pdfplumber

        pages = []
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            page_count = len(pdf.pages)
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                if text.strip():
                    pages.append(f"[Page {i + 1}]\n{text}")

        full_text = "\n\n".join(pages)
        status = "ok" if full_text.strip() else "empty"
        return ExtractionOutput(
            text=full_text, page_count=page_count, method="pdfplumber", status=status
        )
    except Exception as exc:
        logger.warning("pdfplumber failed: %s", exc)
        return ExtractionOutput(text="", page_count=0, method="pdfplumber", status="failed", error=str(exc))
