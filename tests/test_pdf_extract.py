from pathlib import Path

import src.pdf_extract as pdf_extract


def test_extractor_signature_sorts_force_families():
    assert (
        pdf_extract.extractor_signature(
            extractor="pymupdf",
            rescue_extractor="docling",
            rescue_min_chars_per_page=40,
            force_rescue_families=("spec_calc", "noise"),
        )
        == "pymupdf|docling|40|noise,spec_calc"
    )


def test_extract_pdf_text_uses_forced_family_fallback(monkeypatch):
    def fake_run_backend(pdf_path: Path, *, extractor: str):
        if extractor == "pymupdf":
            return pdf_extract._BackendOutput(text="thin text", page_count=2, pages_with_text=1)
        if extractor == "docling":
            return pdf_extract._BackendOutput(text="rescued table text " * 20, page_count=2, pages_with_text=2)
        raise AssertionError(extractor)

    monkeypatch.setattr(pdf_extract, "_run_backend", fake_run_backend)

    result = pdf_extract.extract_pdf_text(
        Path("dummy.pdf"),
        keywords=("noise",),
        extractor="pymupdf",
        rescue_extractor="docling",
        document_family="noise",
        force_rescue_families=("noise",),
        rescue_min_chars_per_page=40,
    )

    assert result.extractor == "docling"
    assert result.fallback_reason == "family_override"
    assert result.error is None


def test_extract_pdf_text_uses_low_text_fallback(monkeypatch):
    def fake_run_backend(pdf_path: Path, *, extractor: str):
        if extractor == "pymupdf":
            return pdf_extract._BackendOutput(text="small text " * 5, page_count=3, pages_with_text=3)
        if extractor == "docling":
            return pdf_extract._BackendOutput(text="useful recovered text " * 80, page_count=3, pages_with_text=3)
        raise AssertionError(extractor)

    monkeypatch.setattr(pdf_extract, "_run_backend", fake_run_backend)

    result = pdf_extract.extract_pdf_text(
        Path("dummy.pdf"),
        keywords=(),
        extractor="pymupdf",
        rescue_extractor="docling",
        document_family="statement",
        force_rescue_families=("noise",),
        rescue_min_chars_per_page=40,
    )

    assert result.extractor == "docling"
    assert result.fallback_reason == "low_chars_per_page"
    assert result.char_count > 40


def test_extract_pdf_text_rejects_marginal_low_text_fallback(monkeypatch):
    primary_text = "a scanned paragraph with some words " * 20
    marginal_text = primary_text + " plus a few extra words"

    def fake_run_backend(pdf_path: Path, *, extractor: str):
        if extractor == "pymupdf":
            return pdf_extract._BackendOutput(text=primary_text, page_count=20, pages_with_text=20)
        if extractor == "docling":
            return pdf_extract._BackendOutput(text=marginal_text, page_count=20, pages_with_text=20)
        raise AssertionError(extractor)

    monkeypatch.setattr(pdf_extract, "_run_backend", fake_run_backend)

    result = pdf_extract.extract_pdf_text(
        Path("dummy.pdf"),
        keywords=(),
        extractor="pymupdf",
        rescue_extractor="docling",
        document_family="statement",
        force_rescue_families=("noise",),
        rescue_min_chars_per_page=40,
    )

    assert result.extractor == "pymupdf"
    assert result.fallback_reason == "low_chars_per_page"
    assert result.fallback_error is None


class _FakeProv:
    def __init__(self, page_no: int):
        self.page_no = page_no


class _FakeTextItem:
    def __init__(self, text: str, page_no: int):
        self.text = text
        self.prov = [_FakeProv(page_no)]


class _FakeDoc:
    def __init__(self, texts, tables=(), pages=10):
        self.texts = texts
        self.tables = list(tables)
        self.pages = {i: object() for i in range(1, pages + 1)}


def test_docling_pages_with_text_counts_unique_pages_from_prov():
    doc = _FakeDoc(
        texts=[
            _FakeTextItem("a real paragraph with plenty of characters", 1),
            _FakeTextItem("short", 2),  # below threshold, skipped
            _FakeTextItem("another long enough paragraph here", 3),
            _FakeTextItem("duplicate hit for page three with extra words", 3),
        ],
        pages=10,
    )
    assert pdf_extract._docling_pages_with_text(doc, fallback_total=10) == 2


def test_docling_pages_with_text_falls_back_when_no_prov():
    doc = _FakeDoc(texts=[], pages=5)
    assert pdf_extract._docling_pages_with_text(doc, fallback_total=5) == 5
