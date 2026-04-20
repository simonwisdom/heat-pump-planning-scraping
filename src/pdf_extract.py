"""Shared PDF text extraction helpers and backend routing."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable

from src.pdf_quality import classify_pdf_quality, count_keyword_hits, normalize_text

DEFAULT_EXTRACTOR = "pymupdf"
DEFAULT_RESCUE_MIN_CHARS_PER_PAGE = 40.0
DEFAULT_FORCE_RESCUE_FAMILIES = ("noise", "spec_calc")
_PAGE_TEXT_MIN_CHARS = 10
_LOW_TEXT_FALLBACK_MIN_CHAR_GAIN = 500
_LOW_TEXT_FALLBACK_MIN_RATIO = 1.5


@dataclass(frozen=True)
class PdfExtractionResult:
    text: str
    page_count: int
    pages_with_text: int
    word_count: int
    char_count: int
    chars_per_page: float
    keyword_hits: int
    quality: str
    error: str | None
    extractor: str
    fallback_reason: str | None = None
    fallback_error: str | None = None


@dataclass(frozen=True)
class _BackendOutput:
    text: str
    page_count: int
    pages_with_text: int


def supported_extractors() -> tuple[str, ...]:
    return ("pymupdf", "pdfplumber", "docling")


def ensure_extractor_available(extractor: str) -> None:
    """Import the backend up-front so missing deps fail loudly, not per-PDF."""
    if extractor == "pymupdf":
        try:
            import pymupdf  # type: ignore  # noqa: F401
        except ImportError:
            import fitz  # type: ignore  # noqa: F401
    elif extractor == "pdfplumber":
        import pdfplumber  # noqa: F401
    elif extractor == "docling":
        _get_docling_converter()
    else:
        raise ValueError(f"Unsupported extractor: {extractor}")


def extractor_signature(
    *,
    extractor: str,
    rescue_extractor: str | None = None,
    rescue_min_chars_per_page: float = DEFAULT_RESCUE_MIN_CHARS_PER_PAGE,
    force_rescue_families: Iterable[str] = DEFAULT_FORCE_RESCUE_FAMILIES,
) -> str:
    rescue = (rescue_extractor or "").strip() or "none"
    families = ",".join(sorted({family.strip() for family in force_rescue_families if family.strip()})) or "none"
    return f"{extractor}|{rescue}|{rescue_min_chars_per_page:g}|{families}"


def extract_pdf_text(
    pdf_path: Path,
    *,
    keywords: Iterable[str] = (),
    extractor: str = DEFAULT_EXTRACTOR,
    rescue_extractor: str | None = None,
    document_family: str | None = None,
    rescue_min_chars_per_page: float = DEFAULT_RESCUE_MIN_CHARS_PER_PAGE,
    force_rescue_families: Iterable[str] = DEFAULT_FORCE_RESCUE_FAMILIES,
) -> PdfExtractionResult:
    primary = _extract_once(pdf_path, keywords=keywords, extractor=extractor)
    fallback_reason = _choose_fallback_reason(
        primary,
        rescue_extractor=rescue_extractor,
        document_family=document_family,
        rescue_min_chars_per_page=rescue_min_chars_per_page,
        force_rescue_families=force_rescue_families,
    )
    if fallback_reason is None or not rescue_extractor or rescue_extractor == extractor:
        return primary

    fallback = _extract_once(pdf_path, keywords=keywords, extractor=rescue_extractor)
    if _should_use_fallback(primary, fallback, fallback_reason=fallback_reason):
        return replace(fallback, fallback_reason=fallback_reason)

    return replace(primary, fallback_reason=fallback_reason, fallback_error=fallback.error)


def _extract_once(pdf_path: Path, *, keywords: Iterable[str], extractor: str) -> PdfExtractionResult:
    try:
        backend_output = _run_backend(pdf_path, extractor=extractor)
    except Exception as exc:  # noqa: BLE001
        return PdfExtractionResult(
            text="",
            page_count=0,
            pages_with_text=0,
            word_count=0,
            char_count=0,
            chars_per_page=0.0,
            keyword_hits=0,
            quality="error",
            error=f"{type(exc).__name__}: {exc}",
            extractor=extractor,
        )

    normalized = normalize_text(backend_output.text)
    word_count = len(normalized.split())
    char_count = len(normalized)
    keyword_hits = count_keyword_hits(normalized, keywords)
    quality = classify_pdf_quality(
        page_count=backend_output.page_count,
        pages_with_text=backend_output.pages_with_text,
        word_count=word_count,
        char_count=char_count,
        keyword_hits=keyword_hits,
    )
    chars_per_page = round(char_count / backend_output.page_count, 1) if backend_output.page_count else 0.0
    return PdfExtractionResult(
        text=backend_output.text,
        page_count=backend_output.page_count,
        pages_with_text=backend_output.pages_with_text,
        word_count=word_count,
        char_count=char_count,
        chars_per_page=chars_per_page,
        keyword_hits=keyword_hits,
        quality=quality,
        error=None,
        extractor=extractor,
    )


def _run_backend(pdf_path: Path, *, extractor: str) -> _BackendOutput:
    if extractor == "pymupdf":
        return _run_pymupdf(pdf_path)
    if extractor == "pdfplumber":
        return _run_pdfplumber(pdf_path)
    if extractor == "docling":
        return _run_docling(pdf_path)
    raise ValueError(f"Unsupported extractor: {extractor}")


def _run_pymupdf(pdf_path: Path) -> _BackendOutput:
    try:
        import pymupdf  # type: ignore
    except ImportError:  # pragma: no cover - depends on install variant
        import fitz as pymupdf  # type: ignore

    text_parts: list[str] = []
    pages_with_text = 0
    doc = pymupdf.open(pdf_path)
    try:
        page_count = doc.page_count
        for page in doc:
            text = page.get_text("text") or ""
            if len(text.strip()) > _PAGE_TEXT_MIN_CHARS:
                pages_with_text += 1
            text_parts.append(text)
    finally:
        doc.close()
    return _BackendOutput(
        text="\n\n".join(text_parts),
        page_count=page_count,
        pages_with_text=pages_with_text,
    )


def _run_pdfplumber(pdf_path: Path) -> _BackendOutput:
    import pdfplumber

    text_parts: list[str] = []
    pages_with_text = 0
    with pdfplumber.open(pdf_path) as pdf:
        page_count = len(pdf.pages)
        for page in pdf.pages:
            try:
                text = page.extract_text() or ""
            except Exception:
                text = ""
            if len(text.strip()) > _PAGE_TEXT_MIN_CHARS:
                pages_with_text += 1
            text_parts.append(text)
    return _BackendOutput(
        text="\n\n".join(text_parts),
        page_count=page_count,
        pages_with_text=pages_with_text,
    )


def _run_docling(pdf_path: Path) -> _BackendOutput:
    converter = _get_docling_converter()
    result = converter.convert(str(pdf_path))
    doc = result.document
    text = doc.export_to_markdown()
    page_count = _docling_page_count(doc)
    pages_with_text = _docling_pages_with_text(doc, fallback_total=page_count if text.strip() else 0)
    return _BackendOutput(
        text=text,
        page_count=page_count,
        pages_with_text=pages_with_text,
    )


def _get_docling_converter():
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption

    if not hasattr(_get_docling_converter, "_converter"):
        options = PdfPipelineOptions()
        options.do_ocr = True
        options.do_table_structure = True
        _get_docling_converter._converter = DocumentConverter(  # type: ignore[attr-defined]
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=options),
            }
        )
    return _get_docling_converter._converter  # type: ignore[attr-defined]


def _docling_pages_with_text(doc: object, *, fallback_total: int) -> int:
    pages: set[int] = set()
    for attr in ("texts", "tables"):
        items = getattr(doc, attr, None) or []
        for item in items:
            raw_text = getattr(item, "text", None) or ""
            if not raw_text.strip() and attr == "tables":
                raw_text = "x"  # tables count even if text is empty
            if len(raw_text.strip()) <= _PAGE_TEXT_MIN_CHARS and attr == "texts":
                continue
            for prov in getattr(item, "prov", None) or []:
                page_no = getattr(prov, "page_no", None)
                if isinstance(page_no, int):
                    pages.add(page_no)
    if pages:
        return len(pages)
    return fallback_total


def _docling_page_count(doc: object) -> int:
    pages = getattr(doc, "pages", None)
    if isinstance(pages, dict):
        return len(pages)
    if isinstance(pages, list):
        return len(pages)
    if pages is None:
        return 0
    try:
        return len(pages)
    except TypeError:
        return 0


def _choose_fallback_reason(
    primary: PdfExtractionResult,
    *,
    rescue_extractor: str | None,
    document_family: str | None,
    rescue_min_chars_per_page: float,
    force_rescue_families: Iterable[str],
) -> str | None:
    if not rescue_extractor:
        return None

    force_rescue = {family.strip() for family in force_rescue_families if family.strip()}
    if document_family and document_family in force_rescue:
        return "family_override"
    if primary.error is not None:
        return "primary_error"
    if primary.quality == "likely_image_only":
        return "likely_image_only"
    if primary.page_count and primary.chars_per_page < rescue_min_chars_per_page:
        return "low_chars_per_page"
    return None


def _should_use_fallback(
    primary: PdfExtractionResult,
    fallback: PdfExtractionResult,
    *,
    fallback_reason: str,
) -> bool:
    if fallback.error is not None or not fallback.text.strip():
        return False
    if fallback_reason == "family_override":
        return True
    if primary.error is not None:
        return True
    if primary.quality == "likely_image_only" and fallback.char_count > 0:
        return True
    if fallback.quality == "searchable_good" and primary.quality != "searchable_good":
        return True
    if fallback_reason == "low_chars_per_page":
        gain = fallback.char_count - primary.char_count
        ratio = (fallback.char_count / primary.char_count) if primary.char_count else float("inf")
        return gain >= _LOW_TEXT_FALLBACK_MIN_CHAR_GAIN or ratio >= _LOW_TEXT_FALLBACK_MIN_RATIO
    return fallback.char_count > primary.char_count
