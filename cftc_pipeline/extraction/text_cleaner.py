"""Canonical text cleaning and combination across sources."""
from __future__ import annotations

import re
from typing import Optional


HEADER_FOOTER_PATTERNS = [
    re.compile(r"^\s*page\s+\d+\s+of\s+\d+\s*$", re.I | re.M),
    re.compile(r"^\s*\d+\s*$", re.M),  # lone page numbers
    re.compile(r"confidential\s+treatment\s+requested", re.I),
    re.compile(r"submitted\s+via\s+(regulations\.gov|cftc\.gov|email)", re.I),
]

# Strip these repetitive opening phrases common in form letters
FORM_LETTER_OPENINGS = [
    re.compile(
        r"^(Dear\s+(Chairman|Chair|Commissioner|Secretary|Sir|Madam)[,.]?\s*\n)+",
        re.I | re.M,
    ),
]


def clean_text(raw: str) -> str:
    """Apply standard cleaning rules to extracted text."""
    text = raw

    # Remove header/footer artifacts
    for pattern in HEADER_FOOTER_PATTERNS:
        text = pattern.sub("", text)

    # Collapse whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r" {2,}", " ", text)

    return text.strip()


def build_canonical_text(
    html_text: Optional[str],
    attachment_texts: list[tuple[str, str]],  # [(filename, text), ...]
) -> str:
    """Combine comment body and attachment texts with provenance markers."""
    parts = []

    if html_text and html_text.strip():
        parts.append("=== COMMENT BODY ===\n" + clean_text(html_text))

    for filename, text in attachment_texts:
        if text and text.strip():
            parts.append(f"=== ATTACHMENT: {filename} ===\n" + clean_text(text))

    if not parts:
        return ""

    canonical = "\n\n".join(parts)
    return canonical


def normalize_for_dedup(text: str) -> str:
    """Aggressively normalize text for deduplication comparison."""
    text = text.lower()
    # Remove commenter-specific info
    text = re.sub(r"\b(my name is|i am|sincerely|regards|respectfully)[^\n]*\n?", "", text)
    # Remove punctuation
    text = re.sub(r"[^\w\s]", " ", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def word_ngrams(text: str, n: int = 3) -> set[str]:
    """Return set of word n-grams for similarity hashing."""
    words = text.split()
    if len(words) < n:
        return {text}
    return {" ".join(words[i : i + n]) for i in range(len(words) - n + 1)}
