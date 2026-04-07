"""HTML text extraction from comment body HTML."""
from __future__ import annotations

import re

from bs4 import BeautifulSoup

from cftc_pipeline.extraction.pdf_extractor import ExtractionOutput

# Tags whose content should be skipped entirely
SKIP_TAGS = {"script", "style", "nav", "header", "footer", "noscript", "iframe"}

# Boilerplate patterns to strip
BOILERPLATE = [
    re.compile(r"CFTC\s*Public\s*Comment\s*Portal", re.I),
    re.compile(r"Privacy\s*Policy", re.I),
    re.compile(r"Accessibility\s*Statement", re.I),
    re.compile(r"Comments?\s*received\s*by\s*the\s*Commodity\s*Futures", re.I),
]


def extract_html(html: str | bytes) -> ExtractionOutput:
    """Extract clean text from comment page HTML."""
    soup = BeautifulSoup(html, "lxml")

    # Remove noise tags
    for tag in soup(SKIP_TAGS):
        tag.decompose()

    # Try to find the comment body container
    body = (
        soup.find(id=re.compile(r"comment.?(body|text|content)", re.I))
        or soup.find(class_=re.compile(r"comment.?(body|text|content)", re.I))
        or soup.find("div", id="MainContent_divComment")
        or soup.find("div", id="MainContent")
        or soup.find("main")
        or soup.body
    )

    if body is None:
        return ExtractionOutput(text="", page_count=1, method="html", status="empty")

    lines = []
    for element in body.descendants:
        if element.name in ("p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "br"):
            text = element.get_text(separator=" ", strip=True)
            if text:
                lines.append(text)
        elif element.name is None:  # NavigableString
            text = str(element).strip()
            if text and text not in ("\n", "\r\n"):
                lines.append(text)

    full_text = "\n".join(lines)
    full_text = _remove_boilerplate(full_text)
    full_text = _collapse_whitespace(full_text)

    status = "ok" if full_text.strip() else "empty"
    return ExtractionOutput(text=full_text, page_count=1, method="html", status=status)


def _remove_boilerplate(text: str) -> str:
    for pattern in BOILERPLATE:
        text = pattern.sub("", text)
    return text


def _collapse_whitespace(text: str) -> str:
    # Collapse 3+ blank lines to 2
    text = re.sub(r"\n{3,}", "\n\n", text)
    # Collapse multiple spaces
    text = re.sub(r" {2,}", " ", text)
    return text.strip()
