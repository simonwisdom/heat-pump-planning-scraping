#!/usr/bin/env python3
"""Extract text from documents that need an external converter, not stdlib.

Third companion to extract_full_corpus_texts.py (PDF-only) and
extract_nonpdf_texts.py (stdlib-readable: docx/odt/rtf/html/txt/eml). This pass
closes the remaining *text-bearing* gap — formats the stdlib can't open:

    .doc .dot   binary Word (OLE)   -> `catdoc`   (apt: catdoc)
    .xls        legacy Excel (BIFF) -> `xls2csv`  (bundled with catdoc)
    .msg        Outlook message     -> `extract_msg` (pip / uv --with extract-msg)

These hold exactly the decision-reasoning (officer/delegated reports, decision
letters, casework emails) the downstream LLM extraction depends on, so dropping
them silently removes signal. Rows merge into the same
`full_corpus_texts/summary.csv` manifest in the identical 16-column format, so
the staging builder and schema pipeline pick them up with no changes.

STILL NOT handled after this pass (left visible, counted in the NOTE):
    .xlsx .xlsm .pptx .ppt   OOXML/binary office  -> would need openpyxl / libreoffice
    .tif .jpg .png .jpeg ... scanned images        -> OCR gap, shared with the PDF pass
    .zip                     archives               -> would need unpack + recurse

The manifest is rewritten atomically (.tmp + os.replace) and keyed by
document_id, so existing PDF/stdlib rows are preserved and reruns skip done docs.

Usage on VPS (catdoc is a system binary; extract-msg comes from uv):
    cd /root/heat-pump-planning-scraping
    apt-get install -y catdoc
    uv run --with extract-msg python scripts/extract_converter_texts.py \
        --docs-root /mnt/planning-docs \
        --output-dir /root/full_corpus_texts
    # priority subset:
    uv run --with extract-msg python scripts/extract_converter_texts.py ... \
        --uids-file /root/sample50.uids
"""

from __future__ import annotations

import argparse
import csv
import html
import os
import re
import shutil
import sqlite3
import subprocess
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

# Optional: only needed when .msg candidates are present. Imported lazily so the
# .doc/.xls path still runs if extract-msg isn't installed.
try:
    import extract_msg as _extract_msg  # type: ignore
except ImportError:  # pragma: no cover - environment-dependent
    _extract_msg = None

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_DB = ROOT / "_local/workstreams/01_heat_pump_applications/data/raw/ashp.db"
DEFAULT_DOCS_ROOT = Path("/mnt/planning-docs")
DEFAULT_OUTPUT = Path("/root/full_corpus_texts")

# Extensions handled here, dispatched by extract_text(). Order is irrelevant.
CONVERTER_EXTS = (".doc", ".dot", ".xls", ".msg")
# Reported-but-unhandled after this pass, so the remaining gap stays visible.
UNHANDLED_EXTS = (".xlsx", ".xlsm", ".pptx", ".ppt")

# Per-file converter timeout. catdoc/xls2csv are sub-second normally; the cap is
# only to stop one pathological file from wedging the whole sweep.
CONVERTER_TIMEOUT = 120

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
    p.add_argument(
        "--uids-file", type=Path, default=None, help="Restrict to application_uids listed one-per-line in this file"
    )
    p.add_argument("--checkpoint-every", type=int, default=500)
    p.add_argument("--force", action="store_true", help="Reprocess even if already in manifest")
    return p.parse_args()


def load_uid_filter(path: Path | None) -> set[str] | None:
    if path is None:
        return None
    return {ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()}


def load_candidates(
    db_path: Path, *, authority: str | None, uids: set[str] | None, limit: int | None
) -> list[Candidate]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        ext_clause = " OR ".join("LOWER(d.file_path) LIKE ?" for _ in CONVERTER_EXTS)
        sql = f"""
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
              AND ({ext_clause})
              AND trim(COALESCE(d.file_path, '')) <> ''
        """
        params: list[str] = [f"%{e}" for e in CONVERTER_EXTS]
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
        # ".doc" LIKE would also match ".docx"/".docm" (handled by the stdlib
        # pass); guard on the true suffix so we don't double-claim those rows.
        if Path(rel).suffix.lower() not in CONVERTER_EXTS:
            continue
        if uids is not None and row["application_uid"] not in uids:
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


def count_unhandled(db_path: Path) -> dict[str, int]:
    conn = sqlite3.connect(str(db_path))
    try:
        out: dict[str, int] = {}
        for ext in UNHANDLED_EXTS:
            (n,) = conn.execute(
                "SELECT COUNT(*) FROM documents WHERE download_status='downloaded' AND LOWER(file_path) LIKE ?",
                (f"%{ext}",),
            ).fetchone()
            out[ext] = int(n)
    finally:
        conn.close()
    return out


def load_existing(summary_path: Path) -> dict[int, dict[str, str]]:
    if not summary_path.exists():
        return {}
    csv.field_size_limit(1 << 30)
    out: dict[int, dict[str, str]] = {}
    with summary_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            try:
                key = int(row.get("document_id", ""))
            except ValueError:
                continue
            out[key] = {f: row.get(f, "") for f in SUMMARY_FIELDS}
    return out


# --- whitespace normalisation shared by all extractors -----------------------
def _clean(text: str) -> str:
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def _run_converter(cmd: list[str]) -> str:
    """Run a converter CLI and return UTF-8 stdout.

    catdoc/xls2csv occasionally exit non-zero on benign warnings while still
    emitting good text on stdout, so we only treat non-zero as fatal when stdout
    is empty — then stderr explains why.
    """
    proc = subprocess.run(cmd, capture_output=True, timeout=CONVERTER_TIMEOUT)
    text = proc.stdout.decode("utf-8", "replace")
    if proc.returncode != 0 and not text.strip():
        err = proc.stderr.decode("utf-8", "replace").strip()
        raise RuntimeError(f"{cmd[0]} rc={proc.returncode}: {err[:300]}")
    return text


def extract_doc(path: Path) -> str:
    """Binary Word (.doc/.dot) via catdoc; -d utf-8 forces UTF-8 output."""
    return _clean(_run_converter(["catdoc", "-d", "utf-8", str(path)]))


def _extract_ooxml_word(path: Path) -> str:
    """Fallback for files with a .doc extension that are really OOXML (.docx).

    Some councils save a Word document and rename it .doc; catdoc refuses these
    ("ZIP archive or Office 2007 or later file"). They match neither the catdoc
    path nor the stdlib pass's `.docx` filter, so they'd be a silent gap. Read
    word/document.xml and strip tags — identical to extract_nonpdf_texts.py.
    """
    with zipfile.ZipFile(path) as z:
        if "word/document.xml" not in z.namelist():
            return ""
        xml = z.read("word/document.xml").decode("utf-8", "replace")
    xml = re.sub(r"<w:tab\b[^>]*/>", "\t", xml)
    xml = re.sub(r"</w:p>", "\n", xml)
    xml = re.sub(r"<w:br\b[^>]*/>", "\n", xml)
    return _clean(html.unescape(re.sub(r"<[^>]+>", "", xml)))


def extract_xls(path: Path) -> str:
    """Legacy Excel (.xls) via xls2csv; -d utf-8 output, comma-separated cells."""
    return _clean(_run_converter(["xls2csv", "-d", "utf-8", "-c", ",", str(path)]))


def extract_msg(path: Path) -> str:
    """Outlook .msg via extract-msg: Subject/From header + plain-text body."""
    if _extract_msg is None:
        raise RuntimeError(
            "extract-msg not installed; run via `uv run --with extract-msg` or `.venv/bin/pip install extract-msg`"
        )
    msg = _extract_msg.openMsg(str(path))
    try:
        subject = msg.subject or ""
        sender = msg.sender or ""
        body = msg.body or ""
    finally:
        try:
            msg.close()
        except Exception:
            pass
    header = f"Subject: {subject}\nFrom: {sender}\n\n"
    return _clean(header + body)


def extract_text(path: Path) -> tuple[str, str]:
    """Dispatch by extension. Returns (text, extractor_tag)."""
    ext = path.suffix.lower()
    if ext in (".doc", ".dot"):
        try:
            return extract_doc(path), "catdoc"
        except RuntimeError as exc:
            # catdoc rejects misnamed OOXML; retry with the stdlib .docx reader.
            if "Office 2007" in str(exc) or "ZIP archive" in str(exc):
                return _extract_ooxml_word(path), "catdoc->ooxml"
            # catdoc rc=69 = valid OLE it can't read as Word; in practice these
            # are mostly Outlook .msg emails saved with a .doc extension, so try
            # the msg reader before giving up. Re-raise the original on failure.
            try:
                text = extract_msg(path)
            except Exception:
                raise exc
            if text.strip():
                return text, "catdoc->msg"
            raise exc
    if ext == ".xls":
        return extract_xls(path), "xls2csv"
    if ext == ".msg":
        return extract_msg(path), "extract-msg"
    return "", "skip"


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
        "extractor": "",
        "error": "",
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }

    if not src.exists():
        row["status"] = "missing_file"
        row["error"] = f"not found at {src}"
        return row

    try:
        text, tag = extract_text(src)
    except subprocess.TimeoutExpired:
        row["status"] = "extract_error"
        row["error"] = f"timeout after {CONVERTER_TIMEOUT}s"
        return row
    except Exception as exc:  # broad: keep the sweep going
        row["status"] = "extract_error"
        row["error"] = f"{exc!r}"[:500]
        return row

    row["extractor"] = tag
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


def check_tools(candidates: list[Candidate]) -> None:
    """Warn (don't abort) if a backend is missing for an ext that's present."""
    exts = {Path(c.relative_path).suffix.lower() for c in candidates}
    if exts & {".doc", ".dot"} and not shutil.which("catdoc"):
        print("WARNING: catdoc not on PATH — .doc/.dot will error. Install with: apt-get install -y catdoc", flush=True)
    if ".xls" in exts and not shutil.which("xls2csv"):
        print("WARNING: xls2csv not on PATH — .xls will error. It ships with the catdoc package.", flush=True)
    if ".msg" in exts and _extract_msg is None:
        print(
            "WARNING: extract-msg not importable — .msg will error. "
            "Run via `uv run --with extract-msg` or pip install extract-msg.",
            flush=True,
        )


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = args.output_dir / "summary.csv"

    print(f"DB:         {args.db_path}", flush=True)
    print(f"Docs root:  {args.docs_root}", flush=True)
    print(f"Output dir: {args.output_dir}", flush=True)

    uids = load_uid_filter(args.uids_file)
    if uids is not None:
        print(f"UID filter: {len(uids)} uids from {args.uids_file}", flush=True)

    candidates = load_candidates(args.db_path, authority=args.authority, uids=uids, limit=args.limit)
    print(f"Candidates: {len(candidates)}", flush=True)
    check_tools(candidates)

    gap = count_unhandled(args.db_path)
    gap_str = ", ".join(f"{ext}={n}" for ext, n in gap.items() if n)
    if gap_str:
        print(
            f"NOTE: still-unhandled office formats (need openpyxl/libreoffice): "
            f"{gap_str}; plus scanned images (OCR gap).",
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
