#!/usr/bin/env python3
"""Extract text for the proposal's priority planning-document corpus.

This script reads downloaded document metadata from the ASHP SQLite database,
keeps the first-pass target families from the project proposal
(`decision`, `officer_report`, `noise` by default), extracts text with
`PyMuPDF` by default, and writes plain-text files plus a reusable manifest CSV.

It is incremental by default: existing successful rows in `summary.csv` are
reused unless `--force` is passed.

Usage:
    uv run --with pymupdf python scripts/extract_decision_texts.py
"""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import subprocess
import sys
import tempfile
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import DB_PATH, PDF_DIR  # noqa: E402
from src.pdf_extract import (  # noqa: E402
    DEFAULT_EXTRACTOR,
    DEFAULT_FORCE_RESCUE_FAMILIES,
    DEFAULT_RESCUE_MIN_CHARS_PER_PAGE,
    ensure_extractor_available,
    extract_pdf_text,
    extractor_signature,
    supported_extractors,
)
from src.pdf_quality import (  # noqa: E402
    KEYWORD_GROUPS,
)

RCLONE_REMOTE = "nesta-gdrive:nesta/planning-docs/"
RCLONE_FETCH_TIMEOUT_SECONDS = 180
OUTPUT_DIR = ROOT / "_local" / "workstreams" / "01_heat_pump_applications" / "data" / "intermediate" / "decision_texts"
PRIMARY_FAMILIES = ("decision", "officer_report", "noise")
OPTIONAL_FAMILIES = ("consultee", "statement", "spec_calc")
ALL_FAMILIES = PRIMARY_FAMILIES + OPTIONAL_FAMILIES

DECISION_STRONG_PATTERNS = re.compile(
    r"(decision[\s_-]?(notice|letter)?|notice[\s_-]of[\s_-]decision|"
    r"detailed[\s_-]planning[\s_-]permission[\s_-](approve|refuse)|"
    r"full[\s_-](approval|refusal))",
    re.IGNORECASE,
)
DECISION_OUTCOME_PATTERNS = re.compile(
    r"\b(approve|approved|approval|grant|granted|refuse|refused|refusal|reject|rejected)\b",
    re.IGNORECASE,
)
REPORT_PATTERNS = re.compile(
    r"(officer[\s_-]?report|delegated[\s_-]?(decision[\s_-]?)?report|"
    r"officer[\s_-]?delegated[\s_-]?report|report[\s_-]?of[\s_-]?handling|"
    r"report(?:[\s_-]+)handling|handling[\s_-]?report|committee[\s_-]?report|"
    r"officer'?s?[\s_-]?recommendation|recommendation(?:[\s_-]?and[\s_-]?reasons)?[\s_-]?report|"
    r"delegated[\s_-]?report[\s_-]?sheet|officers?[\s_-]?report|"
    r"case[\s_-]?officer[\s_-]?report|board[\s_-]?report|reg[\s_-]?board[\s_-]?report)",
    re.IGNORECASE,
)
CONSULTEE_PATTERNS = re.compile(
    r"(environmental[\s_-]?health|consultee|comment|response|objection|representation|"
    r"highways|heritage|tree[\s_-]?officer|ecology|drainage|designing[\s_-]?out[\s_-]?crime)",
    re.IGNORECASE,
)
NOISE_PATTERNS = re.compile(
    r"(noise|acoustic|sound|bs4142|background[\s_-]?level|mcs.?020)",
    re.IGNORECASE,
)
NOISE_STRONG_TEXT_PATTERNS = re.compile(
    r"(report|assessment|calculator|calculation|survey|study|impact)",
    re.IGNORECASE,
)
STATEMENT_PATTERNS = re.compile(
    r"(planning[\s_-]?statement|design[\s_-]?and[\s_-]?access[\s_-]?statement|"
    r"heritage[\s_-]?statement|supporting[\s_-]?statement)",
    re.IGNORECASE,
)
SPEC_CALC_PATTERNS = re.compile(
    r"(heat[\s_-]?loss|manual[\s_-]?sound[\s_-]?calculator|sound[\s_-]?calculator|"
    r"noise[\s_-]?(calculation|calculator)|mcs.?020|microgeneration[\s_-]?certification|"
    r"technical[\s_-]?spec(?:ification)?|specifications?[\s_-]?air[\s_-]?source[\s_-]?heat[\s_-]?pumps?)",
    re.IGNORECASE,
)
SPEC_CALC_CONTEXT_PATTERNS = re.compile(
    r"(ashp|air[\s_-]?source[\s_-]?heat[\s_-]?pump|heat[\s_-]?pump|mcs|kw\b|cop\b|sound[\s_-]?power)",
    re.IGNORECASE,
)
SPEC_CALC_LOW_VALUE_PATTERNS = re.compile(
    r"(solar|window|glaz(?:ed|ing)?|ev[\s_-]?charger|brochure|catalog(ue)?|datasheet|"
    r"trickle[\s_-]?vent|ventilator|fan[\s_-]?coil|product[\s_-]?data)",
    re.IGNORECASE,
)
LOW_VALUE_PATTERNS = re.compile(
    r"(elevation|floor[\s_-]?plan|site[\s_-]?plan|roof[\s_-]?plan|block[\s_-]?plan|"
    r"location[\s_-]?plan|street[\s_-]?scene|cross[\s_-]?section|section\b|drawing\b|"
    r"application[\s_-]?form|photo(graph)?|site[\s_-]?photos?|image[\s_-]?taken|"
    r"brochure|datasheet|technical[\s_-]?spec|specification|specifications|"
    r"manufacturer|catalog(ue)?|product[\s_-]?data)",
    re.IGNORECASE,
)

SUMMARY_FIELDS = [
    "document_id",
    "application_uid",
    "reference",
    "authority_name",
    "planning_decision",
    "document_family",
    "document_type",
    "description",
    "date_published",
    "relative_pdf_path",
    "pdf_path",
    "text_path",
    "status",
    "extractor",
    "extractor_signature",
    "fallback_reason",
    "fallback_error",
    "page_count",
    "pages_with_text",
    "word_count",
    "char_count",
    "chars_per_page",
    "keyword_hits",
    "quality",
    "error",
    "processed_at",
]


@dataclass(frozen=True)
class CandidateRow:
    document_id: int
    application_uid: str
    reference: str
    authority_name: str
    planning_decision: str
    document_type: str
    description: str
    date_published: str
    file_path: str
    relative_pdf_path: str
    document_family: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DB_PATH, help="SQLite database path")
    parser.add_argument("--pdf-root", type=Path, default=PDF_DIR, help="Root directory for downloaded PDFs")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR, help="Output directory for texts + summary")
    parser.add_argument("--authority", type=str, default=None, help="Restrict to one authority")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of candidate documents")
    parser.add_argument(
        "--families",
        type=str,
        default=",".join(PRIMARY_FAMILIES),
        help=f"Comma-separated families from: {', '.join(ALL_FAMILIES)}",
    )
    parser.add_argument(
        "--extractor",
        type=str,
        default=DEFAULT_EXTRACTOR,
        choices=supported_extractors(),
        help="Primary PDF text extractor",
    )
    parser.add_argument(
        "--rescue-extractor",
        type=str,
        default="",
        choices=("", *supported_extractors()),
        help="Optional fallback extractor for low-text or forced families",
    )
    parser.add_argument(
        "--rescue-min-chars-per-page",
        type=float,
        default=DEFAULT_RESCUE_MIN_CHARS_PER_PAGE,
        help="Fallback to the rescue extractor below this chars/page threshold",
    )
    parser.add_argument(
        "--force-rescue-families",
        type=str,
        default=",".join(DEFAULT_FORCE_RESCUE_FAMILIES),
        help="Comma-separated families that should always use the rescue extractor when configured",
    )
    parser.add_argument("--force", action="store_true", help="Reprocess documents even if summary rows already exist")
    parser.add_argument(
        "--remote",
        type=str,
        default=RCLONE_REMOTE,
        help="rclone remote used as a fallback when a downloaded PDF is missing locally; pass '' to disable",
    )
    return parser.parse_args()


def parse_families(raw: str) -> tuple[str, ...]:
    families = tuple(part.strip() for part in raw.split(",") if part.strip())
    if not families:
        raise ValueError("At least one document family is required")

    invalid = sorted(set(families) - set(ALL_FAMILIES))
    if invalid:
        raise ValueError(f"Unknown document families: {', '.join(invalid)}")

    return families


def parse_force_rescue_families(raw: str) -> tuple[str, ...]:
    families = tuple(part.strip() for part in raw.split(",") if part.strip())
    invalid = sorted(set(families) - set(ALL_FAMILIES))
    if invalid:
        raise ValueError(f"Unknown rescue families: {', '.join(invalid)}")
    return families


def normalise_db_path(file_path: str) -> str:
    raw = (file_path or "").strip()
    if not raw:
        return ""

    path = Path(raw)
    parts = path.parts
    if "pdfs" in parts:
        tail = parts[parts.index("pdfs") + 1 :]
        if tail:
            return str(Path(*tail))

    if raw.startswith("pdfs/"):
        return raw.split("pdfs/", 1)[1]

    return raw.lstrip("/")


def classify_document_family(document_type: str | None, description: str | None, file_path: str | None) -> str | None:
    filename = Path(file_path or "").name
    haystack = " ".join(part for part in [document_type or "", description or "", filename] if part).lower()

    if DECISION_STRONG_PATTERNS.search(haystack):
        return "decision"

    if DECISION_OUTCOME_PATTERNS.search(haystack) and not LOW_VALUE_PATTERNS.search(haystack):
        return "decision"

    if REPORT_PATTERNS.search(haystack):
        return "officer_report"

    if STATEMENT_PATTERNS.search(haystack):
        return "statement"

    if CONSULTEE_PATTERNS.search(haystack):
        return "consultee"

    if NOISE_PATTERNS.search(haystack):
        if LOW_VALUE_PATTERNS.search(haystack) and not NOISE_STRONG_TEXT_PATTERNS.search(haystack):
            return None
        return "noise"

    if SPEC_CALC_PATTERNS.search(haystack):
        if SPEC_CALC_LOW_VALUE_PATTERNS.search(haystack):
            return None
        if SPEC_CALC_CONTEXT_PATTERNS.search(haystack) or "mcs" in haystack:
            return "spec_calc"

    return None


def load_candidates(
    db_path: Path,
    families: tuple[str, ...],
    *,
    authority: str | None = None,
    limit: int | None = None,
) -> list[CandidateRow]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        sql = """
            SELECT
                d.id AS document_id,
                d.application_uid,
                a.reference,
                a.authority_name,
                a.planning_decision,
                d.document_type,
                d.description,
                d.date_published,
                d.file_path
            FROM documents d
            JOIN applications a ON a.uid = d.application_uid
            WHERE d.download_status = 'downloaded'
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

    candidates: list[CandidateRow] = []
    for row in rows:
        relative_pdf_path = normalise_db_path(row["file_path"] or "")
        if not relative_pdf_path:
            continue

        family = classify_document_family(
            row["document_type"],
            row["description"],
            relative_pdf_path,
        )
        if family not in families:
            continue

        candidates.append(
            CandidateRow(
                document_id=row["document_id"],
                application_uid=row["application_uid"],
                reference=row["reference"] or "",
                authority_name=row["authority_name"] or "",
                planning_decision=row["planning_decision"] or "",
                document_type=row["document_type"] or "",
                description=row["description"] or "",
                date_published=row["date_published"] or "",
                file_path=row["file_path"] or "",
                relative_pdf_path=relative_pdf_path,
                document_family=family,
            )
        )

    if limit is not None:
        return candidates[:limit]
    return candidates


def resolve_local_pdf_path(file_path: str, *, pdf_root: Path) -> Path | None:
    candidates: list[Path] = []
    raw_path = Path(file_path) if file_path else None

    if raw_path is not None:
        candidates.append(raw_path)
        if not raw_path.is_absolute():
            candidates.append((ROOT / raw_path).resolve())

    relative_path = normalise_db_path(file_path)
    if relative_path:
        candidates.append(pdf_root / relative_path)

    seen: set[Path] = set()
    for path in candidates:
        resolved = path.resolve(strict=False)
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.exists():
            return resolved

    return None


def fetch_from_remote(remote: str, relative_pdf_path: str, cache_dir: Path) -> Path | None:
    local_path = cache_dir / relative_pdf_path
    local_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        result = subprocess.run(
            ["rclone", "copyto", f"{remote}{relative_pdf_path}", str(local_path)],
            capture_output=True,
            text=True,
            timeout=RCLONE_FETCH_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        print(f"  WARNING: rclone timeout fetching {relative_pdf_path} after {RCLONE_FETCH_TIMEOUT_SECONDS}s")
        return None
    if result.returncode == 0 and local_path.exists():
        return local_path

    stderr = result.stderr.strip()
    print(f"  WARNING: failed to fetch {relative_pdf_path} from remote (exit {result.returncode}: {stderr})")
    return None


def load_existing_summary(summary_path: Path) -> dict[int, dict[str, str]]:
    if not summary_path.exists():
        return {}

    rows: dict[int, dict[str, str]] = {}
    with summary_path.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            document_id = row.get("document_id")
            if not document_id:
                continue
            try:
                key = int(document_id)
            except ValueError:
                continue
            rows[key] = {field: row.get(field, "") for field in SUMMARY_FIELDS}
    return rows


def existing_row_is_reusable(row: dict[str, str], output_dir: Path, *, extractor_run_signature: str) -> bool:
    status = row.get("status", "")
    if row.get("extractor_signature", "") != extractor_run_signature:
        return False
    if status == "missing_local_file":
        return False
    if status == "extracted":
        text_path = row.get("text_path", "")
        return bool(text_path) and (output_dir / text_path).exists()
    return status in {"no_text", "extract_error"}


def summary_row(
    candidate: CandidateRow, pdf_path: Path | None, *, extractor_run_signature: str
) -> dict[str, str | int | float]:
    return {
        "document_id": candidate.document_id,
        "application_uid": candidate.application_uid,
        "reference": candidate.reference,
        "authority_name": candidate.authority_name,
        "planning_decision": candidate.planning_decision,
        "document_family": candidate.document_family,
        "document_type": candidate.document_type,
        "description": candidate.description,
        "date_published": candidate.date_published,
        "relative_pdf_path": candidate.relative_pdf_path,
        "pdf_path": str(pdf_path) if pdf_path is not None else "",
        "text_path": "",
        "status": "",
        "extractor": "",
        "extractor_signature": extractor_run_signature,
        "fallback_reason": "",
        "fallback_error": "",
        "page_count": 0,
        "pages_with_text": 0,
        "word_count": 0,
        "char_count": 0,
        "chars_per_page": 0.0,
        "keyword_hits": 0,
        "quality": "",
        "error": "",
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }


def build_text_relative_path(candidate: CandidateRow, *, extractor: str) -> Path:
    suffix = ".md" if extractor == "docling" else ".txt"
    return Path("texts") / Path(candidate.relative_pdf_path).with_suffix(suffix)


def expected_keywords(document_family: str) -> tuple[str, ...]:
    return KEYWORD_GROUPS.get(document_family, ())


def write_summary(summary_path: Path, rows: list[dict[str, str | int | float]]) -> None:
    with summary_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def print_stats(rows: list[dict[str, str | int | float]], output_dir: Path, summary_path: Path) -> None:
    print()
    print("=== Summary ===")
    print(f"Documents in manifest: {len(rows)}")
    print(f"Texts saved under: {output_dir / 'texts'}")
    print(f"Summary CSV: {summary_path}")
    print()

    status_counts: Counter[str] = Counter()
    quality_counts: Counter[str] = Counter()
    extractor_counts: Counter[str] = Counter()
    word_counts: list[int] = []
    councils: set[str] = set()

    for row in rows:
        status_counts[str(row["status"])] += 1
        if row["quality"]:
            quality_counts[str(row["quality"])] += 1
        if row["extractor"]:
            extractor_counts[str(row["extractor"])] += 1
        try:
            word_count = int(row["word_count"])
        except (TypeError, ValueError):
            word_count = 0
        if word_count > 0:
            word_counts.append(word_count)
        if row["authority_name"]:
            councils.add(str(row["authority_name"]))

    print("Status breakdown:")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")

    if quality_counts:
        print()
        print("Quality breakdown:")
        for quality, count in sorted(quality_counts.items()):
            print(f"  {quality}: {count}")

    if extractor_counts:
        print()
        print("Extractor breakdown:")
        for extractor_name, count in sorted(extractor_counts.items()):
            print(f"  {extractor_name}: {count}")

    if word_counts:
        word_counts.sort()
        print()
        print(f"Word count: min={word_counts[0]}, median={word_counts[len(word_counts) // 2]}, max={word_counts[-1]}")

    print(f"Authorities represented: {len(councils)}")


def main() -> None:
    args = parse_args()
    families = parse_families(args.families)
    force_rescue_families = parse_force_rescue_families(args.force_rescue_families)
    rescue_extractor = args.rescue_extractor or None
    run_signature = extractor_signature(
        extractor=args.extractor,
        rescue_extractor=rescue_extractor,
        rescue_min_chars_per_page=args.rescue_min_chars_per_page,
        force_rescue_families=force_rescue_families,
    )

    print(f"Loading downloaded documents from DB: {args.db_path}")
    print(f"Target families: {', '.join(families)}")
    print(f"Primary extractor: {args.extractor}")
    ensure_extractor_available(args.extractor)
    if rescue_extractor:
        ensure_extractor_available(rescue_extractor)
        families_for_log = ", ".join(force_rescue_families) or "(none)"
        threshold = args.rescue_min_chars_per_page
        print(
            f"Rescue extractor: {rescue_extractor} "
            f"(threshold < {threshold:g} chars/page; forced families: {families_for_log})"
        )
    candidates = load_candidates(
        args.db_path,
        families,
        authority=args.authority,
        limit=args.limit,
    )
    print(f"Found {len(candidates)} candidate documents")
    print()

    if not candidates:
        print("No matching downloaded documents found. Exiting.")
        return

    args.output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = args.output_dir / "summary.csv"
    existing_rows = {} if args.force else load_existing_summary(summary_path)

    rows: list[dict[str, str | int | float]] = []
    reused = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        cache_dir = Path(tmpdir)

        print("Extracting text...")
        for candidate in candidates:
            cached_row = existing_rows.get(candidate.document_id)
            if cached_row and existing_row_is_reusable(
                cached_row,
                args.output_dir,
                extractor_run_signature=run_signature,
            ):
                rows.append(cached_row)
                reused += 1
                print(f"  [SKIP] {candidate.relative_pdf_path}: reusing existing summary row")
                continue

            pdf_path = resolve_local_pdf_path(candidate.file_path, pdf_root=args.pdf_root)
            if pdf_path is None and args.remote:
                pdf_path = fetch_from_remote(args.remote, candidate.relative_pdf_path, cache_dir)

            row = summary_row(candidate, pdf_path, extractor_run_signature=run_signature)

            if pdf_path is None:
                row["status"] = "missing_local_file"
                row["error"] = "PDF missing locally and remote fetch unavailable or failed"
                rows.append(row)
                print(f"  [MISS] {candidate.relative_pdf_path}: no local PDF available")
                continue

            result = extract_pdf_text(
                pdf_path,
                keywords=expected_keywords(candidate.document_family),
                extractor=args.extractor,
                rescue_extractor=rescue_extractor,
                document_family=candidate.document_family,
                rescue_min_chars_per_page=args.rescue_min_chars_per_page,
                force_rescue_families=force_rescue_families,
            )
            row["extractor"] = result.extractor
            row["fallback_reason"] = result.fallback_reason or ""
            row["fallback_error"] = result.fallback_error or ""
            row["page_count"] = result.page_count
            row["pages_with_text"] = result.pages_with_text
            row["word_count"] = result.word_count
            row["char_count"] = result.char_count
            row["chars_per_page"] = result.chars_per_page
            row["keyword_hits"] = result.keyword_hits
            row["quality"] = result.quality
            row["error"] = result.error or ""

            text_content = result.text
            if result.error is not None:
                row["status"] = "extract_error"
                print(f"  [ERR]  {candidate.relative_pdf_path}: {row['error']}")
            elif text_content.strip():
                text_relative_path = build_text_relative_path(candidate, extractor=result.extractor)
                text_output_path = args.output_dir / text_relative_path
                text_output_path.parent.mkdir(parents=True, exist_ok=True)
                text_output_path.write_text(text_content, encoding="utf-8")
                row["text_path"] = str(text_relative_path)
                row["status"] = "extracted"
                fallback_note = f" via {row['extractor']}" if row["extractor"] else ""
                if row["fallback_reason"]:
                    fallback_note += f" ({row['fallback_reason']})"
                print(
                    f"  [OK]   {candidate.relative_pdf_path}: "
                    f"{row['word_count']} words, {row['quality']}{fallback_note}"
                )
            else:
                row["status"] = "no_text"
                print(f"  [EMPTY] {candidate.relative_pdf_path}: {row['quality']} via {row['extractor']}")

            rows.append(row)

    rows.sort(key=lambda row: (str(row["authority_name"]), str(row["reference"]), int(row["document_id"])))
    write_summary(summary_path, rows)

    if reused:
        print()
        print(f"Reused {reused} existing summary rows")

    print_stats(rows, args.output_dir, summary_path)


if __name__ == "__main__":
    main()
