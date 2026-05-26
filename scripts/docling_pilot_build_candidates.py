#!/usr/bin/env python3
"""Build candidate list for docling pilot.

Reads /root/full_corpus_texts/summary.csv and filters to:
- status in {no_text, extract_error}
- doc relates to sound/noise/acoustic/MCS (matched in document_type, description, or relative_pdf_path)

Writes two CSVs to /root/docling_pilot/:
- candidates_full.csv: all matching PDFs
- candidates_sample20.csv: 20 PDFs sampled to cover doc-type variety
"""

from __future__ import annotations

import csv
import random
import re
from collections import defaultdict
from pathlib import Path

SUMMARY = Path("/root/full_corpus_texts/summary.csv")
OUT_DIR = Path("/root/docling_pilot")
OUT_DIR.mkdir(parents=True, exist_ok=True)

KEYWORDS = re.compile(r"noise|sound|acoust|\bmcs\b|mcs020|mcs_020", re.IGNORECASE)
SAMPLE_SIZE = 20
SEED = 42


def matches(row: dict) -> bool:
    if row.get("status") not in {"no_text", "extract_error"}:
        return False
    blob = " ".join(
        [
            row.get("document_type", ""),
            row.get("description", ""),
            row.get("relative_pdf_path", ""),
        ]
    )
    return bool(KEYWORDS.search(blob))


def bucket(row: dict) -> str:
    dt = (row.get("document_type") or "").lower()
    desc = (row.get("description") or "").lower()
    path = (row.get("relative_pdf_path") or "").lower()
    blob = f"{dt} {desc} {path}"
    if "mcs" in blob and ("020" in blob or "calc" in blob or "calculator" in blob):
        return "mcs_calculator"
    if "consult" in dt or "consultee" in dt or "statutory reply" in desc:
        return "consultee_response"
    if "noise impact" in desc or "noise impact" in dt:
        return "noise_impact_assessment"
    if "acoustic" in blob:
        return "acoustic_report"
    if "spec" in desc or "manufacturer" in desc or "vaillant" in desc or "daikin" in desc:
        return "manufacturer_spec"
    if "drawing" in dt or "elevation" in desc or "plan" in dt:
        return "drawing_or_plan"
    if "supporting" in dt or "supporting" in desc:
        return "supporting_information"
    if "report" in dt or "report" in desc:
        return "report"
    if "statement" in dt or "statement" in desc:
        return "statement"
    return "other"


def main() -> None:
    with SUMMARY.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        candidates = [r for r in reader if matches(r)]

    print(f"Total matching rows: {len(candidates)}")

    by_status: dict[str, int] = defaultdict(int)
    by_bucket: dict[str, int] = defaultdict(int)
    for r in candidates:
        by_status[r["status"]] += 1
        by_bucket[bucket(r)] += 1
    print("Status:", dict(by_status))
    print("Buckets:")
    for b, n in sorted(by_bucket.items(), key=lambda x: -x[1]):
        print(f"  {n:5d} {b}")

    # Write full candidate list
    full_path = OUT_DIR / "candidates_full.csv"
    fields = list(candidates[0].keys()) + ["bucket"]
    with full_path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in candidates:
            row = dict(r)
            row["bucket"] = bucket(r)
            w.writerow(row)
    print(f"Wrote {full_path} ({len(candidates)} rows)")

    # Stratified sample: aim ~20 covering buckets, weighted by bucket size
    rng = random.Random(SEED)
    bucket_rows: dict[str, list[dict]] = defaultdict(list)
    for r in candidates:
        bucket_rows[bucket(r)].append(r)

    # Take a few from each non-empty bucket, prioritizing high-value buckets
    priority = [
        "mcs_calculator",
        "noise_impact_assessment",
        "acoustic_report",
        "manufacturer_spec",
        "consultee_response",
        "report",
        "statement",
        "supporting_information",
        "other",
        "drawing_or_plan",
    ]
    sample: list[dict] = []
    # First pass: at least 1 from each non-empty priority bucket
    for b in priority:
        if bucket_rows.get(b):
            sample.append(rng.choice(bucket_rows[b]))
    # Fill rest weighted by available
    seen = {id(r) for r in sample}
    pool = [r for r in candidates if id(r) not in seen]
    rng.shuffle(pool)
    while len(sample) < SAMPLE_SIZE and pool:
        sample.append(pool.pop())

    sample = sample[:SAMPLE_SIZE]
    sample_path = OUT_DIR / "candidates_sample20.csv"
    with sample_path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in sample:
            row = dict(r)
            row["bucket"] = bucket(r)
            w.writerow(row)
    print(f"Wrote {sample_path} ({len(sample)} rows)")
    print("\nSample bucket coverage:")
    sample_buckets: dict[str, int] = defaultdict(int)
    for r in sample:
        sample_buckets[bucket(r)] += 1
    for b, n in sorted(sample_buckets.items(), key=lambda x: -x[1]):
        print(f"  {n:3d} {b}")


if __name__ == "__main__":
    main()
