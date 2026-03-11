#!/usr/bin/env python3
"""Summarise document backend patterns from the local applications database."""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse


def _add_repo_root_to_path() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "src").is_dir():
            sys.path.insert(0, str(parent))
            return parent
    raise RuntimeError("Could not find repository root containing 'src'")


ROOT = _add_repo_root_to_path()
from src.config import DB_PATH  # noqa: E402


def classify_pattern(url: str) -> str:
    u = url.lower()
    if "/applicationdetails.do" in u and "activetab=documents" in u:
        return "idox-style applicationDetails"
    if "/publisher/mvc/listdocuments" in u:
        return "idox publisher listDocuments"
    if "/planning/planning-documents" in u:
        return "planning-documents backend"
    if "/aniteim.websearch/" in u:
        return "anite/aniteim websearch"
    if "/searchresult/runthirdpartysearch" in u:
        return "custom third-party search"
    return "other pattern"


def summarise(limit_hosts: int) -> tuple[int, list[tuple[str, int]], list[tuple[str, int]]]:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT documentation_url
            FROM applications
            WHERE documentation_url IS NOT NULL AND documentation_url != ''
            """
        ).fetchall()
    finally:
        conn.close()

    patterns = Counter()
    hosts = Counter()
    for row in rows:
        url = row["documentation_url"]
        patterns[classify_pattern(url)] += 1
        hosts[urlparse(url).netloc.lower()] += 1

    return len(rows), patterns.most_common(), hosts.most_common(limit_hosts)


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarise document backend URL patterns")
    parser.add_argument("--top-hosts", type=int, default=20, help="How many hosts to print")
    parser.add_argument("--format", choices=["table", "csv"], default="table")
    args = parser.parse_args()

    total, pattern_rows, host_rows = summarise(args.top_hosts)

    if args.format == "csv":
        writer = csv.writer(sys.stdout)
        writer.writerow(["section", "label", "count", "share"])
        for label, count in pattern_rows:
            writer.writerow(["pattern", label, count, round(count / total, 4)])
        for label, count in host_rows:
            writer.writerow(["host", label, count, round(count / total, 4)])
        return

    print(f"documentation_url rows: {total}")
    print("\nPattern summary:")
    for label, count in pattern_rows:
        print(f"- {label}: {count} ({count / total:.1%})")

    print("\nTop hosts:")
    for label, count in host_rows:
        print(f"- {label}: {count} ({count / total:.1%})")


if __name__ == "__main__":
    main()
