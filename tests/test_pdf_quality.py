from src.pdf_quality import (
    classify_pdf_quality,
    count_keyword_hits,
    infer_document_family,
    normalize_text,
)


def test_infer_document_family():
    assert infer_document_family("Decision Notice(s)", "REFUSED") == "decision"
    assert infer_document_family("Officers Report Final", "Officer report web") == "officer_report"
    assert infer_document_family("Supporting Information", "ASHP noise calculation") == "noise"
    assert infer_document_family("Supporting Information", "ASHP heat loss calculation") == "spec_calc"
    assert infer_document_family("Consultee Comment", "Environmental Health") == "consultee"
    assert infer_document_family("Drawing", "Site location plan") == "drawing"


def test_count_keyword_hits_distinct():
    text = "Noise assessment with BS4142 and background level data."
    assert count_keyword_hits(text, ("noise", "bs4142", "background level", "missing")) == 3


def test_count_keyword_hits_word_bounded_kw_and_db():
    # "walkway" must not count as " kw"; "dbase" must not count as " db "
    noisy = "walkway to the dbase of a housing estate"
    assert count_keyword_hits(noisy, (" kw", " db ")) == 0
    # Real mentions still count.
    real = "12 kW output measured at 45 db during the survey"
    assert count_keyword_hits(real, (" kw", " db ")) == 2


def test_normalize_text_collapses_whitespace():
    assert normalize_text("A\n\nB\t C") == "A B C"


def test_classify_pdf_quality_good():
    assert (
        classify_pdf_quality(
            page_count=3,
            pages_with_text=3,
            word_count=800,
            char_count=4000,
            keyword_hits=3,
        )
        == "searchable_good"
    )


def test_classify_pdf_quality_image_only():
    assert (
        classify_pdf_quality(
            page_count=4,
            pages_with_text=0,
            word_count=0,
            char_count=0,
            keyword_hits=0,
        )
        == "likely_image_only"
    )


def test_classify_pdf_quality_poor():
    assert (
        classify_pdf_quality(
            page_count=5,
            pages_with_text=2,
            word_count=120,
            char_count=700,
            keyword_hits=0,
        )
        == "searchable_poor"
    )
