"""Grep the verbatim-quote fields of a schema-extraction run back against the
staged source texts, to catch hallucinated quotes.

Checks key_evidence_quote, council_refusal_quote, building_age_evidence and the
grounding-evidence fields (hp_placement_evidence, hp_mounting_type_evidence,
applicant_acoustic_mitigations_evidence) — the fields the schema promises are
verbatim — against the application
description plus every staged text file for that uid (the full files, not the
clipped slices, so a quote is "grounded" if it appears anywhere in a document
the model was shown part of). Matching is whitespace/quote/dash-insensitive,
and a quote elided with "..." passes if every elided segment is found.

Usage:
    HP_RUN_TAG=v5 [HP_SAMPLE_ROOT=...] python3 scripts/llm/verify_quotes.py

Reads:
    _local/llm_pilot/schema_<tag>_50/results.json
    {SAMPLE_ROOT}/selection.json + {SAMPLE_ROOT}/texts/...
Writes:
    _local/llm_pilot/schema_<tag>_50/quote_check.csv
Exit status: 0 if every populated quote is found, 1 otherwise.
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TAG = os.environ.get("HP_RUN_TAG", "v5")
# HP_RUN_DIR points QA directly at any run dir (e.g. the Batch corpus run);
# otherwise fall back to the pilot tag template.
RUN_DIR = Path(os.environ["HP_RUN_DIR"]) if os.environ.get("HP_RUN_DIR") else ROOT / f"_local/llm_pilot/schema_{TAG}_50"
SAMPLE_ROOT = Path(os.environ.get("HP_SAMPLE_ROOT") or (ROOT / "_local/llm_pilot/schema_v1_50/staging"))
SELECTION_JSON = SAMPLE_ROOT / "selection.json"
TEXTS_DIR = SAMPLE_ROOT / "texts"

QUOTE_FIELDS = [
    "key_evidence_quote",
    "council_refusal_quote",
    "building_age_evidence",
    "hp_placement_evidence",
    "hp_mounting_type_evidence",
    "applicant_acoustic_mitigations_evidence",
]

# PDF extraction and LLM output disagree on quote marks, dashes and whitespace;
# normalise both sides before substring matching.
_CHAR_MAP = str.maketrans(
    {
        "‘": "'",
        "’": "'",
        "“": '"',
        "”": '"',
        "–": "-",
        "—": "-",
        "−": "-",
        " ": " ",
    }
)
_ELLIPSIS = re.compile(r"\[?(?:…|\.\s*\.\s*\.)\]?")
_WS = re.compile(r"\s+")


def norm(s: str) -> str:
    return _WS.sub(" ", s.translate(_CHAR_MAP)).strip().lower()


_EDGE_PUNCT = " \"'.,;:!?()[]-"  # the model often normalises edge punctuation (e.g. "conditions:-" -> "conditions.")
_NGRAM = 5  # words per shingle for the near-verbatim fallback


def _ngram_overlap(seg: str, haystack: str) -> float:
    """Fraction of the segment's word 5-grams present in the haystack."""
    words = seg.split()
    if len(words) < _NGRAM:
        return 1.0 if seg in haystack else 0.0
    shingles = [" ".join(words[i : i + _NGRAM]) for i in range(len(words) - _NGRAM + 1)]
    return sum(s in haystack for s in shingles) / len(shingles)


def quote_status(quote: str, haystack: str) -> str:
    """ok = every >=6-char (ellipsis-split) segment occurs verbatim;
    near_verbatim = lightly reworded/stitched (>=50% of word 5-grams found);
    MISSING = fabricated or heavily rewritten."""
    segments = [norm(seg).strip(_EDGE_PUNCT) for seg in _ELLIPSIS.split(quote)]
    segments = [seg for seg in segments if len(seg) >= 6]
    if not segments:
        return "ok"  # nothing checkable
    if all(seg in haystack for seg in segments):
        return "ok"
    overlap = min(_ngram_overlap(seg, haystack) for seg in segments)
    return "near_verbatim" if overlap >= 0.5 else "MISSING"


def load_haystack(uid: str, description: str, selection: dict) -> str:
    parts = [description or ""]
    for f in selection.get(uid, []):
        src = TEXTS_DIR / Path(f["text_path"]).relative_to("texts")
        if src.exists():
            parts.append(src.read_text(encoding="utf-8", errors="replace"))
    return norm("\n".join(parts))


def main() -> int:
    results = json.loads((RUN_DIR / "results.json").read_text())
    selection = json.loads(SELECTION_JSON.read_text())

    rows: list[dict] = []
    counts = {f: {"ok": 0, "near": 0, "missing": 0, "null": 0} for f in QUOTE_FIELDS}
    for r in results:
        uid = r["uid"]
        haystack = load_haystack(uid, r.get("description", ""), selection)
        for field in QUOTE_FIELDS:
            quote = r.get(field)
            if not quote:
                counts[field]["null"] += 1
                continue
            status = quote_status(quote, haystack)
            key = {"ok": "ok", "near_verbatim": "near", "MISSING": "missing"}[status]
            counts[field][key] += 1
            rows.append(
                {"uid": uid, "authority": r.get("authority_name", ""), "field": field, "status": status, "quote": quote}
            )

    out_path = RUN_DIR / "quote_check.csv"
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["uid", "authority", "field", "status", "quote"])
        w.writeheader()
        w.writerows(rows)

    n_missing = n_near = 0
    print(f"Quote grounding check — {len(results)} apps, run {RUN_DIR.name}\n")
    for field, c in counts.items():
        print(
            f"  {field:40s} ok={c['ok']:3d}  near_verbatim={c['near']:3d}  "
            f"MISSING={c['missing']:3d}  null/empty={c['null']:3d}"
        )
        n_missing += c["missing"]
        n_near += c["near"]
    for status, label in (
        ("MISSING", "NOT found in source texts (likely fabricated)"),
        ("near_verbatim", "near-verbatim (lightly reworded/stitched)"),
    ):
        flagged = [row for row in rows if row["status"] == status]
        if flagged:
            print(f"\n{len(flagged)} quote(s) {label}:")
            for row in flagged:
                print(f"  - {row['uid']} ({row['authority']}) {row['field']}: {row['quote'][:110]!r}")
    print(f"\nDetail -> {out_path}")
    return 1 if n_missing else 0


if __name__ == "__main__":
    sys.exit(main())
