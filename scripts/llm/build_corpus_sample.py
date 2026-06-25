#!/usr/bin/env python3
"""Emit the full-corpus staging inputs: sample.csv + uids.txt.

The schema extractor reads a staging dir of sample.csv + selection.json +
texts/. build_staging.py produces selection.json + texts/; this produces the
other half — one sample.csv row per *docs-bearing* app (>=1 extracted document
in the corpus manifest) plus a uids.txt to feed build_staging --uids-file.

Scope = every app with at least one extracted document. No relevance pre-filter:
the user's corpus run extracts all docs-bearing apps, and extract_schema_v1
re-derives hp_relevance itself, so the cheap classifier label is not needed here
(sample.csv's final_label is left blank — it is only a pass-through column).

sample.csv columns match what extract_schema_v1.main()/batch_extract expect:
    uid, authority_name, reference, description,
    planning_decision, decision_date, decision_bucket, final_label

Runs where the manifest + ashp.db live (the VPS). stdlib only.

    python scripts/llm/build_corpus_sample.py \
        --corpus-dir /root/full_corpus_texts \
        --db /root/heat-pump-planning-scraping/_local/workstreams/01_heat_pump_applications/data/raw/ashp.db \
        --out-dir /root/corpus_staging
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

# Reuse the exact decision bucketing the pilot sampler uses, so the bucket column
# means the same thing across sample and corpus runs.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from sample_100_apps import bucket_decision  # noqa: E402

SAMPLE_FIELDS = [
    "uid",
    "authority_name",
    "reference",
    "description",
    "planning_decision",
    "decision_date",
    "decision_bucket",
    "final_label",
]


def docs_bearing_uids(manifest: Path) -> list[str]:
    """Uids with >=1 extracted document, in first-seen order."""
    csv.field_size_limit(1 << 30)
    seen: dict[str, None] = {}
    with manifest.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if row.get("status") != "extracted":
                continue
            uid = row.get("application_uid", "")
            tp = row.get("text_path", "")
            if uid and tp:
                seen.setdefault(uid, None)
    return list(seen)


def load_meta(db: Path, uids: list[str]) -> dict[str, dict]:
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    meta: dict[str, dict] = {}
    CHUNK = 900  # under SQLite's default 999 bound-variable limit
    for i in range(0, len(uids), CHUNK):
        batch = uids[i : i + CHUNK]
        cur = con.execute(
            "SELECT uid, reference, authority_name, planning_decision, "
            "description, decision_date FROM applications WHERE uid IN ({})".format(",".join("?" * len(batch))),
            batch,
        )
        for row in cur:
            meta[row["uid"]] = dict(row)
    con.close()
    return meta


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--corpus-dir", type=Path, default=Path("/root/full_corpus_texts"))
    p.add_argument(
        "--db",
        type=Path,
        default=Path("/root/heat-pump-planning-scraping/_local/workstreams/01_heat_pump_applications/data/raw/ashp.db"),
    )
    p.add_argument("--out-dir", type=Path, required=True)
    args = p.parse_args()

    manifest = args.corpus_dir / "summary.csv"
    uids = docs_bearing_uids(manifest)
    print(f"docs-bearing apps (>=1 extracted doc): {len(uids):,}", flush=True)

    meta = load_meta(args.db, uids)
    n_missing = sum(1 for u in uids if u not in meta)
    if n_missing:
        print(f"  WARNING: {n_missing} uid(s) had no ashp.db row — emitted with blank metadata", flush=True)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    sample_csv = args.out_dir / "sample.csv"
    with sample_csv.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=SAMPLE_FIELDS)
        w.writeheader()
        for uid in uids:
            m = meta.get(uid, {})
            dec = m.get("planning_decision") or ""
            w.writerow(
                {
                    "uid": uid,
                    "authority_name": m.get("authority_name") or "",
                    "reference": m.get("reference") or "",
                    "description": m.get("description") or "",
                    "planning_decision": dec,
                    "decision_date": m.get("decision_date") or "",
                    "decision_bucket": bucket_decision(dec),
                    "final_label": "",
                }
            )

    uids_txt = args.out_dir / "uids.txt"
    uids_txt.write_text("\n".join(uids) + "\n", encoding="utf-8")

    print(f"wrote {sample_csv}  ({len(uids):,} rows)", flush=True)
    print(f"wrote {uids_txt}", flush=True)
    print("Next: build_staging.py --uids-file uids.txt --out-dir <same out-dir>", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
