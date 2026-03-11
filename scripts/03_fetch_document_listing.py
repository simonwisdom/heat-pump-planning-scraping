#!/usr/bin/env python3
"""Fetch a document listing from a representative backend and print JSON."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path


def _add_repo_root_to_path() -> None:
    for parent in Path(__file__).resolve().parents:
        if (parent / "src").is_dir():
            sys.path.insert(0, str(parent))
            return
    raise RuntimeError("Could not find repository root containing 'src'")


_add_repo_root_to_path()

from src.agile_scraper import AgileDocumentScraper  # noqa: E402
from src.idox_scraper import IdoxDocumentScraper  # noqa: E402
from src.publisher_scraper import PublisherDocumentScraper  # noqa: E402


async def fetch_idox(docs_url: str) -> list[dict]:
    async with IdoxDocumentScraper() as scraper:
        return await scraper.scrape_documents(docs_url)


async def fetch_publisher(docs_url: str) -> list[dict]:
    async with PublisherDocumentScraper() as scraper:
        return await scraper.scrape_documents(docs_url)


async def fetch_agile(app_id: str, client_name: str) -> list[dict]:
    async with AgileDocumentScraper() as scraper:
        return await scraper.scrape_documents(app_id, client_name)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch a document listing from a planning backend")
    parser.add_argument("--family", required=True, choices=["idox", "publisher", "agile"])
    parser.add_argument("--docs-url", help="Documents URL for idox/publisher")
    parser.add_argument("--app-id", help="Application id for agile")
    parser.add_argument("--client", help="Agile client name, e.g. LDNPA")
    args = parser.parse_args()

    if args.family in {"idox", "publisher"} and not args.docs_url:
        parser.error("--docs-url is required for idox and publisher")
    if args.family == "agile" and (not args.app_id or not args.client):
        parser.error("--app-id and --client are required for agile")

    if args.family == "idox":
        docs = await fetch_idox(args.docs_url)
    elif args.family == "publisher":
        docs = await fetch_publisher(args.docs_url)
    else:
        docs = await fetch_agile(args.app_id, args.client)

    print(json.dumps(docs, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
