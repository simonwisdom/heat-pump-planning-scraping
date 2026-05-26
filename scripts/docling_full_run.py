#!/usr/bin/env python3
"""Full docling extraction over the sound/noise/acoustic/MCS candidate pool.

Two-pass strategy:
  1. Default pipeline (fast on PDFs with an embedded text layer)
  2. Forced-OCR retry on any PDFs that come back empty

Resumable: reads results.csv on startup and skips already-completed docs.
Outputs:
  /root/docling_pilot/full_run/texts/<doc_id>.md      (default pass)
  /root/docling_pilot/full_run/texts/<doc_id>.txt
  /root/docling_pilot/full_run/forced_ocr/<doc_id>.md (retry pass for empties)
  /root/docling_pilot/full_run/forced_ocr/<doc_id>.txt
  /root/docling_pilot/full_run/results.csv            (per-doc summary)

Run on VPS via:
    cd /root/heat-pump-planning-scraping
    nohup .venv/bin/python scripts/docling_full_run.py \
        > /root/docling_pilot/full_run/run.log 2>&1 &
"""

from __future__ import annotations

import csv
import gc
import sys
import time
from pathlib import Path

CANDIDATES = Path("/root/docling_pilot/candidates_full.csv")
PDF_ROOT = Path("/mnt/planning-docs")
OUT_DIR = Path("/root/docling_pilot/full_run")
TEXTS_DIR = OUT_DIR / "texts"
FORCED_DIR = OUT_DIR / "forced_ocr"
RESULTS_CSV = OUT_DIR / "results.csv"
CHECKPOINT_EVERY = 25
CONVERTER_RECREATE_EVERY = 40  # release model memory to avoid OOM on small VPS

for d in (TEXTS_DIR, FORCED_DIR):
    d.mkdir(parents=True, exist_ok=True)

FIELDS = [
    "document_id",
    "bucket",
    "authority_name",
    "reference",
    "document_type",
    "description",
    "relative_pdf_path",
    "original_status",
    "pdf_bytes",
    "default_status",
    "default_sec",
    "default_chars",
    "forced_status",
    "forced_sec",
    "forced_chars",
    "final_status",
    "final_chars",
    "page_count",
    "error",
    "processed_at",
]


def load_candidates() -> list[dict]:
    with CANDIDATES.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def load_done() -> dict[str, dict]:
    if not RESULTS_CSV.exists():
        return {}
    done: dict[str, dict] = {}
    with RESULTS_CSV.open(encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            doc_id = r.get("document_id", "")
            if doc_id:
                done[doc_id] = r
    return done


def write_results(rows: list[dict]) -> None:
    tmp = RESULTS_CSV.with_suffix(".csv.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in FIELDS})
    tmp.replace(RESULTS_CSV)


def run_default(converter, pdf_path: Path, doc_id: str) -> tuple[str, float, int, int]:
    """Run default pipeline. Returns (status, elapsed, chars, page_count)."""
    t = time.time()
    try:
        conv = converter.convert(str(pdf_path))
        doc = conv.document
        md = doc.export_to_markdown()
        try:
            txt = doc.export_to_text()
        except AttributeError:
            txt = md
        (TEXTS_DIR / f"{doc_id}.md").write_text(md, encoding="utf-8")
        (TEXTS_DIR / f"{doc_id}.txt").write_text(txt, encoding="utf-8")
        pages = len(doc.pages) if hasattr(doc, "pages") else 0
        status = "ok" if txt.strip() else "empty"
        return status, round(time.time() - t, 2), len(txt), pages
    except Exception as exc:  # noqa: BLE001
        return f"error:{exc!r}"[:200], round(time.time() - t, 2), 0, 0


def run_forced(converter, pdf_path: Path, doc_id: str) -> tuple[str, float, int]:
    t = time.time()
    try:
        conv = converter.convert(str(pdf_path))
        doc = conv.document
        md = doc.export_to_markdown()
        try:
            txt = doc.export_to_text()
        except AttributeError:
            txt = md
        (FORCED_DIR / f"{doc_id}.md").write_text(md, encoding="utf-8")
        (FORCED_DIR / f"{doc_id}.txt").write_text(txt, encoding="utf-8")
        status = "ok" if txt.strip() else "still_empty"
        return status, round(time.time() - t, 2), len(txt)
    except Exception as exc:  # noqa: BLE001
        return f"error:{exc!r}"[:200], round(time.time() - t, 2), 0


def build_converters():
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption

    default_converter = DocumentConverter()

    forced_opts = PdfPipelineOptions()
    forced_opts.do_ocr = True
    if hasattr(forced_opts, "force_full_page_ocr"):
        forced_opts.force_full_page_ocr = True
    if hasattr(forced_opts, "ocr_options") and hasattr(forced_opts.ocr_options, "force_full_page_ocr"):
        forced_opts.ocr_options.force_full_page_ocr = True
    forced_opts.do_table_structure = True
    forced_converter = DocumentConverter(
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=forced_opts)}
    )
    return default_converter, forced_converter


def main() -> None:
    print(">>> Importing docling ...", flush=True)
    default_converter, forced_converter = build_converters()

    print(">>> Loading candidates ...", flush=True)
    candidates = load_candidates()
    done = load_done()
    print(f"    candidates: {len(candidates)}", flush=True)
    print(f"    already done: {len(done)}", flush=True)

    rows_by_id: dict[str, dict] = {k: dict(v) for k, v in done.items()}
    todo = [c for c in candidates if c["document_id"] not in done]
    print(f"    to process: {len(todo)}", flush=True)
    if not todo:
        print("Nothing to do.")
        return

    from datetime import datetime, timezone

    t_start = time.time()
    n_done = 0
    n_ok = 0
    n_empty = 0
    n_forced_recovered = 0
    n_still_empty = 0
    n_error = 0

    for i, cand in enumerate(todo, 1):
        doc_id = cand["document_id"]
        rel = cand["relative_pdf_path"]
        pdf_path = PDF_ROOT / rel

        row: dict = {f: "" for f in FIELDS}
        for k in (
            "document_id",
            "bucket",
            "authority_name",
            "reference",
            "document_type",
            "description",
            "relative_pdf_path",
        ):
            row[k] = cand.get(k, "")
        row["original_status"] = cand.get("status", "")
        row["processed_at"] = datetime.now(timezone.utc).isoformat()

        if not pdf_path.exists():
            row["final_status"] = "missing_file"
            row["error"] = f"not at {pdf_path}"
            rows_by_id[doc_id] = row
            n_error += 1
        else:
            try:
                row["pdf_bytes"] = pdf_path.stat().st_size
            except OSError:
                pass
            d_status, d_sec, d_chars, pages = run_default(default_converter, pdf_path, doc_id)
            row["default_status"] = d_status
            row["default_sec"] = d_sec
            row["default_chars"] = d_chars
            row["page_count"] = pages

            if d_status == "ok":
                row["final_status"] = "ok"
                row["final_chars"] = d_chars
                n_ok += 1
            elif d_status == "empty":
                n_empty += 1
                f_status, f_sec, f_chars = run_forced(forced_converter, pdf_path, doc_id)
                row["forced_status"] = f_status
                row["forced_sec"] = f_sec
                row["forced_chars"] = f_chars
                if f_status == "ok":
                    row["final_status"] = "ok_via_forced_ocr"
                    row["final_chars"] = f_chars
                    n_forced_recovered += 1
                elif f_status == "still_empty":
                    row["final_status"] = "still_empty"
                    n_still_empty += 1
                else:
                    row["final_status"] = "forced_error"
                    row["error"] = f_status
                    n_error += 1
            else:
                row["final_status"] = "default_error"
                row["error"] = d_status
                n_error += 1

            rows_by_id[doc_id] = row

        n_done += 1
        elapsed = time.time() - t_start
        rate = n_done / elapsed if elapsed > 0 else 0
        eta_h = (len(todo) - n_done) / rate / 3600 if rate > 0 else 0
        print(
            f"[{i:4d}/{len(todo)}] {row['final_status']:18s} chars={row.get('final_chars', 0):>6} "
            f"rate={rate * 60:.1f}/min  eta={eta_h:.1f}h  "
            f"ok={n_ok} via_ocr={n_forced_recovered} still_empty={n_still_empty} err={n_error}  "
            f"{rel[:70]}",
            flush=True,
        )

        if n_done % CHECKPOINT_EVERY == 0:
            write_results(sorted(rows_by_id.values(), key=lambda r: r.get("document_id", "")))
            gc.collect()

        if n_done % CONVERTER_RECREATE_EVERY == 0:
            del default_converter, forced_converter
            gc.collect()
            default_converter, forced_converter = build_converters()
            try:
                import resource

                rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                print(f"    [recreated converters; maxrss={rss_kb / 1024:.0f} MB]", flush=True)
            except Exception:
                print("    [recreated converters]", flush=True)

    write_results(sorted(rows_by_id.values(), key=lambda r: r.get("document_id", "")))
    print(
        f"\n>>> Done. processed={n_done} ok={n_ok} via_forced_ocr={n_forced_recovered} "
        f"still_empty={n_still_empty} errors={n_error}  elapsed={(time.time() - t_start) / 3600:.1f}h",
        flush=True,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        sys.exit(1)
