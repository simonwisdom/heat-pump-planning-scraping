#!/usr/bin/env python3
"""Extract text from every downloaded PDF in the ashp.db documents table.

Reads PDFs from a directory tree that mirrors the gdrive `planning-docs/` layout
(typically an `rclone mount` at `/mnt/planning-docs`) and writes one `.txt` per
PDF under `--output-dir/texts/<same relative path>.txt`. A `summary.csv`
manifest is maintained alongside; reruns skip rows already marked `extracted`.

Designed for the full-corpus sweep — no family filtering, PyMuPDF only by
default, parallel workers.

Usage on VPS:
    cd /root/heat-pump-planning-scraping
    .venv/bin/python scripts/extract_full_corpus_texts.py \
        --pdf-root /mnt/planning-docs \
        --output-dir /root/full_corpus_texts \
        --workers 4
"""

from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_DB = ROOT / "_local/workstreams/01_heat_pump_applications/data/raw/ashp.db"
DEFAULT_PDF_ROOT = Path("/mnt/planning-docs")
DEFAULT_OUTPUT = Path("/root/full_corpus_texts")

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
    relative_pdf_path: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    p.add_argument("--pdf-root", type=Path, default=DEFAULT_PDF_ROOT)
    p.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--limit", type=int, default=None, help="Process at most N candidates")
    p.add_argument("--authority", type=str, default=None)
    p.add_argument("--checkpoint-every", type=int, default=500, help="Flush summary CSV every N completed PDFs")
    p.add_argument("--force", action="store_true", help="Reprocess even if summary says extracted")
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
              AND LOWER(d.file_path) LIKE '%.pdf'
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
                relative_pdf_path=rel,
            )
        )
    if limit is not None:
        out = out[:limit]
    return out


def load_done(summary_path: Path) -> dict[int, dict[str, str]]:
    if not summary_path.exists():
        return {}
    done: dict[int, dict[str, str]] = {}
    with summary_path.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            try:
                key = int(row.get("document_id", ""))
            except ValueError:
                continue
            done[key] = {f: row.get(f, "") for f in SUMMARY_FIELDS}
    return done


def _extract_one(args: tuple[Candidate, str, str]) -> dict[str, object]:
    """Worker function. Imports pymupdf inside to keep parent process light."""
    candidate, pdf_root_str, output_dir_str = args
    pdf_root = Path(pdf_root_str)
    output_dir = Path(output_dir_str)

    pdf_path = pdf_root / candidate.relative_pdf_path
    text_rel = Path("texts") / Path(candidate.relative_pdf_path).with_suffix(".txt")
    text_abs = output_dir / text_rel

    row: dict[str, object] = {
        "document_id": candidate.document_id,
        "application_uid": candidate.application_uid,
        "authority_name": candidate.authority_name,
        "reference": candidate.reference,
        "document_type": candidate.document_type,
        "description": candidate.description,
        "relative_pdf_path": candidate.relative_pdf_path,
        "text_path": "",
        "status": "",
        "page_count": 0,
        "pages_with_text": 0,
        "word_count": 0,
        "char_count": 0,
        "extractor": "pymupdf",
        "error": "",
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }

    if not pdf_path.exists():
        row["status"] = "missing_file"
        row["error"] = f"not found at {pdf_path}"
        return row

    try:
        import pymupdf  # noqa: WPS433

        doc = pymupdf.open(str(pdf_path))
    except Exception as exc:  # broad: PyMuPDF raises many things
        row["status"] = "extract_error"
        row["error"] = f"open: {exc!r}"[:500]
        return row

    try:
        parts: list[str] = []
        pages_with_text = 0
        for page in doc:
            try:
                t = page.get_text() or ""
            except Exception as exc:
                row["error"] = f"page: {exc!r}"[:500]
                t = ""
            if t.strip():
                pages_with_text += 1
            parts.append(t)
        text = "\n".join(parts)
        row["page_count"] = doc.page_count
        row["pages_with_text"] = pages_with_text
        row["char_count"] = len(text)
        row["word_count"] = len(text.split())
    finally:
        doc.close()

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


def write_summary(summary_path: Path, rows: list[dict[str, object]]) -> None:
    tmp = summary_path.with_suffix(".csv.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=SUMMARY_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in SUMMARY_FIELDS})
    os.replace(tmp, summary_path)


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = args.output_dir / "summary.csv"

    print(f"DB:         {args.db_path}", flush=True)
    print(f"PDF root:   {args.pdf_root}", flush=True)
    print(f"Output dir: {args.output_dir}", flush=True)
    print(f"Workers:    {args.workers}", flush=True)

    print("Loading candidates...", flush=True)
    candidates = load_candidates(args.db_path, authority=args.authority, limit=args.limit)
    print(f"Candidates: {len(candidates)}", flush=True)

    done = {} if args.force else load_done(summary_path)
    skip_ids: set[int] = set()
    rows_by_id: dict[int, dict[str, object]] = {}
    for doc_id, row in done.items():
        rows_by_id[doc_id] = dict(row)
        if row.get("status") == "extracted":
            text_path = row.get("text_path", "")
            if text_path and (args.output_dir / text_path).exists():
                skip_ids.add(doc_id)
    print(f"Already extracted (skip): {len(skip_ids)}", flush=True)

    todo = [c for c in candidates if c.document_id not in skip_ids]
    print(f"To process: {len(todo)}", flush=True)
    if not todo:
        if rows_by_id:
            write_summary(summary_path, sorted(rows_by_id.values(), key=lambda r: int(r.get("document_id", 0))))
        print("Nothing to do.", flush=True)
        return

    pdf_root_str = str(args.pdf_root)
    output_dir_str = str(args.output_dir)

    t0 = time.time()
    completed_since_flush = 0
    n_done = 0
    n_extracted = 0
    n_empty = 0
    n_missing = 0
    n_error = 0
    total = len(todo)

    work = ((c, pdf_root_str, output_dir_str) for c in todo)

    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_extract_one, item): item[0] for item in work}
        for fut in as_completed(futures):
            cand = futures[fut]
            try:
                row = fut.result()
            except Exception as exc:
                row = {
                    "document_id": cand.document_id,
                    "application_uid": cand.application_uid,
                    "authority_name": cand.authority_name,
                    "reference": cand.reference,
                    "document_type": cand.document_type,
                    "description": cand.description,
                    "relative_pdf_path": cand.relative_pdf_path,
                    "text_path": "",
                    "status": "extract_error",
                    "page_count": 0,
                    "pages_with_text": 0,
                    "word_count": 0,
                    "char_count": 0,
                    "extractor": "pymupdf",
                    "error": f"worker: {exc!r}"[:500],
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                }
            rows_by_id[int(row["document_id"])] = row
            n_done += 1
            completed_since_flush += 1
            status = row.get("status")
            if status == "extracted":
                n_extracted += 1
            elif status == "no_text":
                n_empty += 1
            elif status == "missing_file":
                n_missing += 1
            elif status == "extract_error":
                n_error += 1

            if n_done % 50 == 0 or n_done == total:
                elapsed = time.time() - t0
                rate = n_done / elapsed if elapsed > 0 else 0
                eta = (total - n_done) / rate if rate > 0 else 0
                print(
                    f"[{n_done}/{total}] "
                    f"ok={n_extracted} empty={n_empty} miss={n_missing} err={n_error} "
                    f"rate={rate:.1f}/s eta={eta / 3600:.1f}h",
                    flush=True,
                )

            if completed_since_flush >= args.checkpoint_every:
                write_summary(summary_path, sorted(rows_by_id.values(), key=lambda r: int(r.get("document_id", 0))))
                completed_since_flush = 0

    write_summary(summary_path, sorted(rows_by_id.values(), key=lambda r: int(r.get("document_id", 0))))

    elapsed = time.time() - t0
    print()
    print("=== Done ===", flush=True)
    print(
        f"Processed: {n_done} in {elapsed / 3600:.2f}h "
        f"(ok={n_extracted}, empty={n_empty}, miss={n_missing}, err={n_error})",
        flush=True,
    )
    print(f"Summary:   {summary_path}", flush=True)
    print(f"Text root: {args.output_dir / 'texts'}", flush=True)


if __name__ == "__main__":
    main()
