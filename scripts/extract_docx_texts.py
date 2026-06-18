#!/usr/bin/env python3
"""Extract text from downloaded Word .docx files in the ashp.db documents table.

Companion to extract_full_corpus_texts.py, which is PDF-only (its candidate
query filters `LOWER(file_path) LIKE '%.pdf'`). Some councils publish their
decision letters and officer/delegated reports as Word documents, so those docs
were downloaded but never entered the text corpus — silently removing exactly
the decision-reasoning the downstream LLM extraction depends on.

This pass extracts `.docx` with the stdlib only (a .docx is a zip of XML; we
read word/document.xml and strip tags — no OCR, captures table cells) and merges
rows into the same `full_corpus_texts/summary.csv` manifest, in the identical
16-column format, so the staging builder and schema pipeline pick them up.

Old-style binary `.doc` (OLE) files are NOT handled here: they need an external
converter (libreoffice/antiword) that isn't installed. They are counted and
reported so the remaining gap is visible.

The manifest is rewritten atomically (.tmp + os.replace) and keyed by
document_id, so existing PDF rows are preserved and reruns skip done docs.

Usage on VPS:
    cd /root/heat-pump-planning-scraping
    .venv/bin/python scripts/extract_docx_texts.py \
        --docs-root /mnt/planning-docs \
        --output-dir /root/full_corpus_texts
"""

from __future__ import annotations

import argparse
import csv
import html
import os
import re
import sqlite3
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_DB = ROOT / "_local/workstreams/01_heat_pump_applications/data/raw/ashp.db"
DEFAULT_DOCS_ROOT = Path("/mnt/planning-docs")
DEFAULT_OUTPUT = Path("/root/full_corpus_texts")

# Identical to extract_full_corpus_texts.py so rows interleave cleanly.
SUMMARY_FIELDS = [
    "document_id",
    "application_uid",
    "authority_name",
    "reference",
    "document_type",
    "description",
    "relative_pdf_path",
    "text_path",
    "status",
    "page_count",
    "pages_with_text",
    "word_count",
    "char_count",
    "extractor",
    "error",
    "processed_at",
]


@dataclass(frozen=True)
class Candidate:
    document_id: int
    application_uid: str
    authority_name: str
    reference: str
    document_type: str
    description: str
    relative_path: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    p.add_argument("--docs-root", type=Path, default=DEFAULT_DOCS_ROOT)
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    p.add_argument("--limit", type=int, default=None, help="Process at most N candidates")
    p.add_argument("--authority", type=str, default=None)
    p.add_argument("--checkpoint-every", type=int, default=500)
    p.add_argument("--force", action="store_true", help="Reprocess even if already in manifest")
    return p.parse_args()


def load_candidates(db_path: Path, *, authority: str | None, limit: int | None) -> list[Candidate]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        sql = """
            SELECT
                d.id AS document_id,
                d.application_uid,
                a.authority_name,
                a.reference,
                d.document_type,
                d.description,
                d.file_path
            FROM documents d
            JOIN applications a ON a.uid = d.application_uid
            WHERE d.download_status = 'downloaded'
              AND LOWER(d.file_path) LIKE '%.docx'
              AND trim(COALESCE(d.file_path, '')) <> ''
        """
        params: list[str] = []
        if authority:
            sql += " AND a.authority_name = ?"
            params.append(authority)
        sql += " ORDER BY a.authority_name, a.reference, d.id"
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    out: list[Candidate] = []
    for row in rows:
        rel = (row["file_path"] or "").strip().lstrip("/")
        if not rel:
            continue
        out.append(
            Candidate(
                document_id=row["document_id"],
                application_uid=row["application_uid"],
                authority_name=row["authority_name"] or "",
                reference=row["reference"] or "",
                document_type=row["document_type"] or "",
                description=row["description"] or "",
                relative_path=rel,
            )
        )
    if limit is not None:
        out = out[:limit]
    return out


def count_doc_gap(db_path: Path) -> int:
    """How many downloaded .doc (binary, unhandled) files remain."""
    conn = sqlite3.connect(str(db_path))
    try:
        (n,) = conn.execute(
            "SELECT COUNT(*) FROM documents WHERE download_status='downloaded' AND LOWER(file_path) LIKE '%.doc'"
        ).fetchone()
    finally:
        conn.close()
    return int(n)


def load_existing(summary_path: Path) -> dict[int, dict[str, str]]:
    if not summary_path.exists():
        return {}
    out: dict[int, dict[str, str]] = {}
    with summary_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            try:
                key = int(row.get("document_id", ""))
            except ValueError:
                continue
            out[key] = {f: row.get(f, "") for f in SUMMARY_FIELDS}
    return out


def extract_docx_text(path: Path) -> str:
    """Pull all body text from a .docx using stdlib zip + tag strip.

    Reading word/document.xml captures paragraph and table-cell text alike
    (tables are just nested <w:p> runs), which matters because officer/delegated
    reports lay their reasoning out in tables.
    """
    with zipfile.ZipFile(path) as z:
        names = set(z.namelist())
        if "word/document.xml" not in names:
            return ""
        xml = z.read("word/document.xml").decode("utf-8", "replace")
    # Preserve structure: paragraph -> newline, tab -> tab, before stripping tags.
    xml = re.sub(r"<w:tab\b[^>]*/>", "\t", xml)
    xml = re.sub(r"</w:p>", "\n", xml)
    xml = re.sub(r"<w:br\b[^>]*/>", "\n", xml)
    text = re.sub(r"<[^>]+>", "", xml)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def extract_one(cand: Candidate, docs_root: Path, output_dir: Path) -> dict[str, object]:
    src = docs_root / cand.relative_path
    text_rel = Path("texts") / Path(cand.relative_path).with_suffix(".txt")
    text_abs = output_dir / text_rel

    row: dict[str, object] = {
        "document_id": cand.document_id,
        "application_uid": cand.application_uid,
        "authority_name": cand.authority_name,
        "reference": cand.reference,
        "document_type": cand.document_type,
        "description": cand.description,
        "relative_pdf_path": cand.relative_path,
        "text_path": "",
        "status": "",
        "page_count": 0,
        "pages_with_text": 0,
        "word_count": 0,
        "char_count": 0,
        "extractor": "docx-xml",
        "error": "",
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }

    if not src.exists():
        row["status"] = "missing_file"
        row["error"] = f"not found at {src}"
        return row

    try:
        text = extract_docx_text(src)
    except zipfile.BadZipFile as exc:
        row["status"] = "extract_error"
        row["error"] = f"bad_zip: {exc!r}"[:500]
        return row
    except Exception as exc:  # broad: keep the sweep going
        row["status"] = "extract_error"
        row["error"] = f"{exc!r}"[:500]
        return row

    row["char_count"] = len(text)
    row["word_count"] = len(text.split())
    if not text.strip():
        row["status"] = "no_text"
        return row

    text_abs.parent.mkdir(parents=True, exist_ok=True)
    try:
        text_abs.write_text(text, encoding="utf-8")
    except OSError as exc:
        row["status"] = "extract_error"
        row["error"] = f"write: {exc!r}"[:500]
        return row

    row["text_path"] = str(text_rel)
    row["status"] = "extracted"
    return row


def write_summary(summary_path: Path, rows_by_id: dict[int, dict[str, object]]) -> None:
    tmp = summary_path.with_suffix(".csv.tmp")
    ordered = sorted(rows_by_id.values(), key=lambda r: int(r.get("document_id", 0)))
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=SUMMARY_FIELDS)
        w.writeheader()
        for r in ordered:
            w.writerow({k: r.get(k, "") for k in SUMMARY_FIELDS})
    os.replace(tmp, summary_path)


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = args.output_dir / "summary.csv"

    print(f"DB:         {args.db_path}", flush=True)
    print(f"Docs root:  {args.docs_root}", flush=True)
    print(f"Output dir: {args.output_dir}", flush=True)

    candidates = load_candidates(args.db_path, authority=args.authority, limit=args.limit)
    print(f".docx candidates: {len(candidates)}", flush=True)
    doc_gap = count_doc_gap(args.db_path)
    if doc_gap:
        print(
            f"NOTE: {doc_gap} downloaded .doc (binary) files are NOT handled "
            f"(no converter installed) — left for a follow-up pass.",
            flush=True,
        )

    rows_by_id = {} if args.force else load_existing(summary_path)
    print(f"Existing manifest rows: {len(rows_by_id)}", flush=True)

    skip = set()
    if not args.force:
        for c in candidates:
            r = rows_by_id.get(c.document_id)
            if r and r.get("status") == "extracted":
                tp = r.get("text_path", "")
                if tp and (args.output_dir / tp).exists():
                    skip.add(c.document_id)
    todo = [c for c in candidates if c.document_id not in skip]
    print(f"Already extracted (skip): {len(skip)}   To process: {len(todo)}", flush=True)
    if not todo:
        print("Nothing to do.", flush=True)
        return

    t0 = time.time()
    n_ok = n_empty = n_missing = n_err = 0
    for i, cand in enumerate(todo, 1):
        row = extract_one(cand, args.docs_root, args.output_dir)
        rows_by_id[int(row["document_id"])] = row
        status = row.get("status")
        n_ok += status == "extracted"
        n_empty += status == "no_text"
        n_missing += status == "missing_file"
        n_err += status == "extract_error"
        if i % 50 == 0 or i == len(todo):
            rate = i / (time.time() - t0) if time.time() > t0 else 0
            print(
                f"[{i}/{len(todo)}] ok={n_ok} empty={n_empty} miss={n_missing} err={n_err} rate={rate:.1f}/s",
                flush=True,
            )
        if i % args.checkpoint_every == 0:
            write_summary(summary_path, rows_by_id)

    write_summary(summary_path, rows_by_id)
    print("\n=== Done ===", flush=True)
    print(
        f"Processed {len(todo)}: ok={n_ok} empty={n_empty} miss={n_missing} err={n_err} in {(time.time() - t0):.1f}s",
        flush=True,
    )
    print(f"Summary: {summary_path}", flush=True)


if __name__ == "__main__":
    main()
