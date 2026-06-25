"""Tests for the full-corpus staging input builder."""

from __future__ import annotations

import csv

import scripts.llm.build_corpus_sample as bcs


def _write_manifest(path, rows):
    cols = ["application_uid", "status", "text_path"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def test_docs_bearing_uids_filters_and_dedups_in_order(tmp_path):
    manifest = tmp_path / "summary.csv"
    _write_manifest(
        manifest,
        [
            {"application_uid": "b", "status": "extracted", "text_path": "texts/b1.txt"},
            {"application_uid": "a", "status": "extracted", "text_path": "texts/a1.txt"},
            {"application_uid": "b", "status": "extracted", "text_path": "texts/b2.txt"},  # dup
            {"application_uid": "c", "status": "no_text", "text_path": ""},  # excluded
            {"application_uid": "d", "status": "extracted", "text_path": ""},  # no path -> excluded
            {"application_uid": "", "status": "extracted", "text_path": "texts/x.txt"},  # no uid
        ],
    )
    # first-seen order, deduped, only extracted-with-text
    assert bcs.docs_bearing_uids(manifest) == ["b", "a"]


def test_bucket_decision_reused_from_sampler():
    # the column must mean the same thing as the pilot sampler's buckets
    assert bcs.bucket_decision("Application Refused") == "refused"
    assert bcs.bucket_decision("Grant subject to conditions") == "approved"
    assert bcs.bucket_decision("Withdrawn") == "withdrawn"
    assert bcs.bucket_decision("") == "other"
