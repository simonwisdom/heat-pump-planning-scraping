#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx>=0.27", "openpyxl>=3.1"]
# ///
"""Fetch the latest DESNZ Boiler Upgrade Scheme xlsx and extract the LA-level sheets to CSV.

Default behaviour discovers the current release from the gov.uk Content API. Pass
``--xlsx PATH`` to extract from a file already on disk, or ``--url URL`` to point at a
specific xlsx URL.

Sheets extracted:
    Q1.2  -> data/external_heat_pump_data/BUS_redemptions_by_LA_cumulative.csv
    A1.7  -> data/external_heat_pump_data/BUS_redemptions_by_LA_by_FY.csv

Run with::

    uv run scripts/refresh_bus.py
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from urllib.parse import urlparse

import httpx
import openpyxl

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data/external_heat_pump_data"
OUT_CUMULATIVE = OUT_DIR / "BUS_redemptions_by_LA_cumulative.csv"
OUT_BY_FY = OUT_DIR / "BUS_redemptions_by_LA_by_FY.csv"

SEARCH_API = (
    "https://www.gov.uk/api/search.json"
    "?q=%22Boiler+Upgrade+Scheme+statistics%22"
    "&filter_format=official_statistics"
    "&order=-public_timestamp"
    "&count=1"
)
CONTENT_API_BASE = "https://www.gov.uk/api/content"

CUMULATIVE_SHEET = "Q1.2"
BY_FY_SHEET = "A1.7"


def discover_xlsx_url() -> tuple[str, str]:
    """Return (xlsx_url, release_title) for the most recent BUS publication on gov.uk."""
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        r = client.get(SEARCH_API)
        r.raise_for_status()
        results = r.json().get("results") or []
        if not results:
            raise RuntimeError("No BUS releases found via gov.uk search.")
        latest = results[0]
        link = latest.get("link") or ""
        title = latest.get("title") or link
        if not link.startswith("/"):
            raise RuntimeError(f"Unexpected search result link: {link!r}")
        r = client.get(f"{CONTENT_API_BASE}{link}")
        r.raise_for_status()
        meta = r.json()

    attachments = meta.get("details", {}).get("attachments", []) or []
    for att in attachments:
        url = (att.get("url") or "").strip()
        if url.lower().endswith(".xlsx"):
            return url, title
    raise RuntimeError(
        f"No xlsx attachment found on the latest release ({title}). "
        "Pass --url with a direct xlsx link, or --xlsx with a local file."
    )


def download(url: str, dest: Path) -> Path:
    print(f"Downloading {url}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=120.0, follow_redirects=True) as client, client.stream("GET", url) as r:
        r.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in r.iter_bytes():
                fh.write(chunk)
    return dest


def find_header_row(rows: list[tuple]) -> int:
    for i, row in enumerate(rows):
        if row and row[0] == "Area Codes":
            return i
    raise RuntimeError("Header row starting with 'Area Codes' not found")


def extract_sheet(wb: openpyxl.Workbook, sheet_name: str, out_path: Path) -> int:
    """Write rows from sheet (header row onwards) to out_path. Returns data-row count."""
    ws = wb[sheet_name]
    rows = list(ws.iter_rows(values_only=True))
    hdr_idx = find_header_row(rows)
    header = list(rows[hdr_idx])
    while header and header[-1] in (None, ""):
        header.pop()
    width = len(header)

    data_rows: list[list] = []
    for row in rows[hdr_idx + 1 :]:
        # Notes/footer follow the data block; an empty area-code cell marks the end.
        if not row or row[0] in (None, ""):
            break
        data_rows.append([row[i] if i < len(row) else None for i in range(width)])

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        for r in data_rows:
            w.writerow(["" if v is None else v for v in r])
    return len(data_rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    src = parser.add_mutually_exclusive_group()
    src.add_argument("--xlsx", type=Path, help="Extract from a local xlsx instead of downloading")
    src.add_argument("--url", help="Direct xlsx URL (skip gov.uk discovery)")
    parser.add_argument(
        "--save-xlsx",
        type=Path,
        help=f"If downloading, save the xlsx here (default: under {OUT_DIR})",
    )
    args = parser.parse_args()

    if args.xlsx:
        if not args.xlsx.exists():
            parser.error(f"--xlsx not found: {args.xlsx}")
        xlsx_path = args.xlsx
    else:
        if args.url:
            url = args.url
        else:
            url, title = discover_xlsx_url()
            print(f"Latest release: {title}")
        name = Path(urlparse(url).path).name or "BUS.xlsx"
        xlsx_path = args.save_xlsx or (OUT_DIR / name)
        download(url, xlsx_path)

    print(f"Extracting from {xlsx_path}")
    wb = openpyxl.load_workbook(xlsx_path, data_only=True, read_only=True)
    try:
        n_cum = extract_sheet(wb, CUMULATIVE_SHEET, OUT_CUMULATIVE)
        print(f"  {CUMULATIVE_SHEET} -> {OUT_CUMULATIVE} ({n_cum} rows)")
        n_fy = extract_sheet(wb, BY_FY_SHEET, OUT_BY_FY)
        print(f"  {BY_FY_SHEET} -> {OUT_BY_FY} ({n_fy} rows)")
    finally:
        wb.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
