"""Extract text from refused-app decision/officer/notice PDFs.

Reads the manifest of refused-app decision-type docs, runs pdftotext on each
PDF, and writes plain-text alongside in a parallel directory tree. Skips files
that have already been extracted.
"""

from __future__ import annotations

import argparse
import csv
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STAGE = ROOT / "_local/workstreams/01_heat_pump_applications/data/intermediate/refusal_text_analysis"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stage", type=Path, default=DEFAULT_STAGE)
    ap.add_argument("--manifest", type=Path)
    args = ap.parse_args()

    stage: Path = args.stage
    manifest = args.manifest or (stage / "refused_docs_text_manifest.csv")
    pdf_root = stage / "pdfs"
    text_root = stage / "text"
    text_root.mkdir(parents=True, exist_ok=True)

    n_total = n_extracted = n_missing = n_skipped = n_failed = 0
    log_rows = []
    with manifest.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            n_total += 1
            file_path = (row.get("file_path") or "").strip()
            if not file_path:
                continue
            pdf_path = pdf_root / file_path
            text_path = (text_root / file_path).with_suffix(".txt")
            if not pdf_path.exists():
                n_missing += 1
                log_rows.append((row["application_uid"], file_path, "missing_pdf"))
                continue
            if text_path.exists() and text_path.stat().st_size > 0:
                n_skipped += 1
                continue
            text_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                subprocess.run(
                    ["pdftotext", "-layout", "-q", str(pdf_path), str(text_path)],
                    check=True,
                    timeout=120,
                )
                n_extracted += 1
                log_rows.append((row["application_uid"], file_path, "ok"))
            except subprocess.TimeoutExpired:
                n_failed += 1
                log_rows.append((row["application_uid"], file_path, "timeout"))
            except subprocess.CalledProcessError as e:
                n_failed += 1
                log_rows.append((row["application_uid"], file_path, f"err:{e.returncode}"))

    log_path = stage / "extract_log.csv"
    with log_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["application_uid", "file_path", "status"])
        w.writerows(log_rows)

    print(f"manifest rows: {n_total}")
    print(f"  extracted:   {n_extracted}")
    print(f"  skipped:     {n_skipped} (already extracted)")
    print(f"  missing:     {n_missing} (pdf not on disk)")
    print(f"  failed:      {n_failed}")
    print(f"log: {log_path}")


if __name__ == "__main__":
    main()
