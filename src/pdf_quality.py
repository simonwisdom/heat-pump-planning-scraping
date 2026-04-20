"""Helpers for assessing whether PDFs are searchable enough for extraction.

These helpers are shared by scripts that:
- infer a coarse document family from portal metadata
- count family-specific keyword hits in extracted text
- classify whether a PDF looks searchable enough for downstream analysis
"""

from __future__ import annotations

import re
from collections.abc import Iterable

KEYWORD_GROUPS: dict[str, tuple[str, ...]] = {
    "decision": (
        "decision",
        "approved",
        "approval",
        "refused",
        "refusal",
        "conditions",
        "granted",
    ),
    "officer_report": (
        "officer report",
        "delegated report",
        "committee report",
        "recommendation",
        "assessment",
    ),
    "noise": (
        "noise",
        "sound",
        " db ",
        "db(a)",
        "bs4142",
        "background level",
        "acoustic",
    ),
    "consultee": (
        "consultee",
        "environmental health",
        "highways",
        "heritage",
        "comment",
        "response",
    ),
    "spec_calc": (
        "mcs 020",
        "manual sound calculator",
        "heat loss",
        "sound power",
        " kw",
        "technical specification",
    ),
}


def infer_document_family(document_type: str | None, description: str | None) -> str:
    """Map portal metadata to a coarse document family for reporting."""
    haystack = " ".join(part for part in [document_type or "", description or ""] if part).lower()

    if any(term in haystack for term in ("decision notice", "decision notice(s)", "decision")):
        return "decision"
    if any(term in haystack for term in ("officer report", "delegated report", "report final")):
        return "officer_report"
    if any(term in haystack for term in ("noise", "sound", "acoustic", "bs4142")):
        return "noise"
    if any(
        term in haystack
        for term in (
            "heat loss",
            "manual sound calculator",
            "technical specification",
            "mcs 020",
        )
    ):
        return "spec_calc"
    if any(term in haystack for term in ("consultee", "environmental health", "comment", "response")):
        return "consultee"
    if any(term in haystack for term in ("drawing", "plan", "elevation", "site location")):
        return "drawing"
    if "application form" in haystack:
        return "application_form"
    return "other"


def count_keyword_hits(text: str, keywords: Iterable[str]) -> int:
    """Count how many distinct keywords are present in the extracted text."""
    lowered = text.lower()
    return sum(1 for keyword in keywords if keyword in lowered)


def classify_pdf_quality(
    *,
    page_count: int,
    pages_with_text: int,
    word_count: int,
    char_count: int,
    keyword_hits: int,
) -> str:
    """Classify text extraction quality for downstream planning purposes."""
    if page_count <= 0:
        return "corrupt_or_unreadable"
    if char_count == 0 or word_count == 0 or pages_with_text == 0:
        return "likely_image_only"

    coverage = pages_with_text / page_count
    sparse_text = word_count < 80 or char_count < 400

    if coverage >= 0.5 and (word_count >= 250 or keyword_hits >= 2):
        return "searchable_good"
    if sparse_text and coverage < 0.5:
        return "likely_image_only"
    return "searchable_poor"


def normalize_text(text: str) -> str:
    """Collapse extracted text for robust counting and keyword search."""
    return re.sub(r"\s+", " ", text or "").strip()
