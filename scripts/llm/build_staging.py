#!/usr/bin/env python3
"""Rebuild the per-app schema-extraction staging (selection.json + texts/).

The schema extractor (extract_schema_v1.py) reads a staging dir:
    sample.csv       the chosen uids + sampling columns  (NOT touched here)
    selection.json   uid -> ranked list of {fname, doctype, text_path, wc}
    texts/<rel>      a local copy of each selected document's extracted text

This script regenerates selection.json + texts/ from the corpus manifest, so a
re-run picks up newly extracted documents (e.g. the non-PDF decision letters and
officer reports added by extract_nonpdf_texts.py).

With --base-selection it runs in MERGE mode: apps that gained an extractable
non-PDF document are re-ranked from the current manifest; every other app keeps
its original selection verbatim. This is deliberate — a full rebuild would also
churn the rank-9 tail (drawings/forms/consultee comments are all tied at the
lowest priority, so which ones fill the remaining slots is arbitrary), changing
many apps that gained nothing. Merge mode confines the diff to the apps that
actually have new content.

Runs on the VPS (stdlib only) where the manifest + texts live; the resulting
staging dir is pulled down to replace the local copy.

The ranking MIRRORS scripts/llm/extract_schema_v1.py:rank() — keep the two in
sync. (Kept inline rather than imported because that module pulls in openai at
import time, which isn't installed on the VPS.)

Usage on VPS:
    cd /root/heat-pump-planning-scraping
    .venv/bin/python scripts/llm/build_staging.py \
        --uids-file /root/sample50.uids \
        --corpus-dir /root/full_corpus_texts \
        --out-dir /root/staging50
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from collections import defaultdict
from pathlib import Path

MAX_FILES = 12  # extractor consumes top 10; a little headroom, bounds text copies

# === Ranker (mirror of extract_schema_v1.py) ===============================
DECISION_TYPES = {"decision", "decision notice", "decision letter", "recommendation and reasons report"}
OFFICER_TYPES = {
    "officer report",
    "officer reports",
    "delegated report",
    "report of handling",
    "delegated officer report",
    "case officer report",
    "application committee / delegated report",
    "committee report",
}
REPORT_HINT_TYPES = {"recommendation", "case report", "planning report"}
SOUND_TYPES = {
    "acoustic report",
    "acoustic assessment",
    "noise assessment",
    "sound assessment",
    "noise report",
    "noise impact assessment",
}
HERITAGE_TYPES = {
    "heritage statement",
    "heritage impact assessment",
    "design and access statement",
    "design & access statement",
    "heritage and design statement",
}
DECISION_FN = re.compile(
    r"decision[_ ]notice|decision[_ ]letter|refus(?:al|ed)?|permission|granted|approval|approved", re.I
)
OFFICER_FN = re.compile(
    r"officer|delegated|report[_ ]of[_ ]handling|report[_ ]recommendation|case[_ ]officer|committee[_ ]report", re.I
)
SOUND_FN = re.compile(
    r"acoustic|noise[_ ]?(assessment|report|impact)|sound[_ ]assessment|bs[_ ]?4142|bs[_ ]?8233|mcs[_ ]?020", re.I
)
HERITAGE_FN = re.compile(r"heritage|design[_ ]?and[_ ]?access|listed[_ ]?building", re.I)
HP_FN = re.compile(r"(air[_ ]source|heat[_ ]pump|\bashp\b|brochure)", re.I)


def rank(doctype: str, fname: str) -> tuple[int, int]:
    """Lower = higher priority. Returns (primary, tiebreak)."""
    t = (doctype or "").strip().lower()
    if t in DECISION_TYPES:
        return (0, 0)
    if t in OFFICER_TYPES:
        return (1, 0)
    if t in REPORT_HINT_TYPES:
        return (2, 0)
    if t in SOUND_TYPES:
        return (3, 0)
    if t in HERITAGE_TYPES:
        return (4, 0)
    if DECISION_FN.search(fname):
        return (5, 0)
    if OFFICER_FN.search(fname):
        return (6, 0)
    if SOUND_FN.search(fname):
        return (7, 0)
    if HERITAGE_FN.search(fname):
        return (8, 0)
    return (9, 0 if HP_FN.search(fname) else 1)


# Original (downloaded) extension lives in the manifest's relative_pdf_path column
# (misnamed: it holds the source path for every doc, not just PDFs). An app is
# "touched" by the non-PDF pass iff it has >=1 extracted doc whose source ext is
# not .pdf — those are the only apps merge mode re-ranks.
PDF_EXT = ".pdf"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--uids-file", type=Path, required=True)
    p.add_argument("--corpus-dir", type=Path, default=Path("/root/full_corpus_texts"))
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--max-files", type=int, default=MAX_FILES)
    p.add_argument(
        "--base-selection",
        type=Path,
        default=None,
        help="Original selection.json. Enables MERGE mode: apps with no extracted "
        "non-PDF doc keep their base entry verbatim; only apps that gained one "
        "are re-ranked from the current manifest.",
    )
    return p.parse_args()


def emit(recs: list[dict], corpus_dir: Path, texts_out: Path) -> tuple[list[dict], int, int]:
    """Copy each rec's extracted text into texts_out; return (kept_recs, n_copied, n_missing).

    Strips bookkeeping keys (e.g. _doc_id) so the emitted record is the stable
    4-field shape the schema extractor and the base selection.json both use.
    """
    out, n_files, n_missing = [], 0, 0
    for f in recs:
        rel = Path(f["text_path"]).relative_to("texts")
        src = corpus_dir / f["text_path"]
        if not src.exists():
            n_missing += 1
            continue
        dst = texts_out / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
        n_files += 1
        out.append({"fname": f["fname"], "doctype": f["doctype"], "text_path": f["text_path"], "wc": f.get("wc", 0)})
    return out, n_files, n_missing


def main() -> int:
    args = parse_args()
    uids = {ln.strip() for ln in args.uids_file.read_text(encoding="utf-8").splitlines() if ln.strip()}
    manifest = args.corpus_dir / "summary.csv"
    print(f"uids: {len(uids)}   manifest: {manifest}")

    base: dict[str, list[dict]] = {}
    if args.base_selection:
        base = json.loads(args.base_selection.read_text(encoding="utf-8"))
        print(f"MERGE mode: base selection has {len(base)} uids ({args.base_selection})")

    csv.field_size_limit(1 << 30)
    per_uid: dict[str, list[dict]] = defaultdict(list)
    nonpdf_uids: set[str] = set()
    with manifest.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            uid = row.get("application_uid", "")
            if uid not in uids:
                continue
            if row.get("status") != "extracted":
                continue
            tp = row.get("text_path", "")
            if not tp:
                continue
            if Path(row.get("relative_pdf_path", "") or "").suffix.lower() not in ("", PDF_EXT):
                nonpdf_uids.add(uid)
            fname = Path(tp).name
            doctype = (row.get("document_type") or "").strip().lower()
            try:
                doc_id = int(row.get("document_id", "0"))
            except ValueError:
                doc_id = 0
            per_uid[uid].append(
                {
                    "fname": fname,
                    "doctype": doctype,
                    "text_path": tp,
                    "wc": int(row.get("word_count") or 0),
                    "_doc_id": doc_id,
                }
            )

    texts_out = args.out_dir / "texts"
    texts_out.mkdir(parents=True, exist_ok=True)

    selection: dict[str, list[dict]] = {}
    n_files = n_missing = n_verbatim = n_reranked = 0
    for uid in sorted(uids):
        if base and uid in base and uid not in nonpdf_uids:
            recs = base[uid]  # kept verbatim — no new non-PDF content
            n_verbatim += 1
        else:
            files = per_uid.get(uid, [])
            files.sort(key=lambda f: (*rank(f["doctype"], f["fname"]), f["_doc_id"]))
            recs = files[: args.max_files]
            if base:
                n_reranked += 1
        emitted, nf, nm = emit(recs, args.corpus_dir, texts_out)
        selection[uid] = emitted
        n_files += nf
        n_missing += nm

    (args.out_dir / "selection.json").write_text(json.dumps(selection, indent=1), encoding="utf-8")
    covered = sum(1 for u in uids if selection.get(u))
    print(f"uids with >=1 doc: {covered}/{len(uids)}")
    if base:
        print(f"merge: {n_verbatim} kept verbatim, {n_reranked} re-ranked ({len(nonpdf_uids)} uids have a non-PDF doc)")
    print(f"copied {n_files} text files into {texts_out}  (missing: {n_missing})")
    print(f"wrote {args.out_dir / 'selection.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
