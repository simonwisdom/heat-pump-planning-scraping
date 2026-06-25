"""Pick 100 stratified apps from the corpus for an LLM classification sample.

Reads:
  - <corpus>/summary.csv (manifest: uid -> text_path, status)
  - the local ashp.db (decision, description)

Writes:
  - <out_dir>/sample.csv  (one row per app, with text-file paths)
  - <out_dir>/texts/...   (copies of just those apps' .txt files)

Paths default to repo-local locations; override the database with the ASHP_DB
environment variable.

Stratification: 35 refused, 35 approved/conditional, 20 withdrawn, 10 other.
Within each bucket, equal split between HP-only and bundled descriptions.
"""

from __future__ import annotations

import csv
import os
import random
import re
import shutil
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

random.seed(2026)

ROOT = Path(__file__).resolve().parents[2]
CORPUS_ROOT = Path(os.environ.get("CORPUS_ROOT", str(ROOT / "_local/full_corpus_texts")))
SUMMARY_CSV = CORPUS_ROOT / "summary.csv"
DB = Path(os.environ.get("ASHP_DB", str(ROOT / "_local/workstreams/01_heat_pump_applications/data/raw/ashp.db")))
OUT_DIR = Path(os.environ.get("SAMPLE_OUT_DIR", str(ROOT / "_local/llm_sample_100")))
OUT_TEXTS = OUT_DIR / "texts"

BUNDLED_RE = re.compile(
    r"\b(extension|extn|loft|conversion|garage|outbuilding|annex|porch|store|"
    r"garden room|demolition|new dwelling|new build|change of use|alteration|"
    r"orangery|conservatory|summerhouse|first floor|two storey|two-storey|"
    r"single storey|single-storey|side extension|rear extension|listed building|"
    r"refurbishment|renovat)",
    re.IGNORECASE,
)

DECISION_BUCKETS = {
    "refused": [
        "refuse",
        "refused",
        "refusal",
        "permission refused",
        "application refused",
        "refuse permission",
    ],
    "approved": [
        "permit",
        "permitted",
        "permission",
        "approve",
        "approved",
        "conditional",
        "grant",  # "Grant subject to conditions" — "granted" missed the bare stem, bucketing it as other
        "granted",
        "discharge",
    ],
    "withdrawn": ["withdrawn", "withdraw"],
}


def bucket_decision(decision: str | None) -> str:
    if not decision:
        return "other"
    d = decision.lower()
    for bucket, keys in DECISION_BUCKETS.items():
        if any(k in d for k in keys):
            return bucket
    return "other"


def is_bundled(description: str | None) -> bool:
    if not description:
        return False
    return bool(BUNDLED_RE.search(description))


def main() -> int:
    print(f"Loading manifest: {SUMMARY_CSV}", flush=True)
    # Map uid -> list of (rel_text_path, document_type, description, status)
    uid_to_texts: dict[str, list[dict]] = defaultdict(list)
    with SUMMARY_CSV.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row["status"] != "extracted":
                continue
            uid = row["application_uid"]
            if not uid:
                continue
            uid_to_texts[uid].append(
                {
                    "text_path": row["text_path"],
                    "document_type": row.get("document_type") or "",
                    "doc_description": row.get("description") or "",
                    "word_count": int(row.get("word_count") or 0),
                }
            )
    print(f"  apps with >=1 extracted text: {len(uid_to_texts):,}", flush=True)

    print(f"Loading decisions/descriptions from: {DB}", flush=True)
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    uid_meta: dict[str, dict] = {}
    cur = con.execute(
        "SELECT uid, reference, authority_name, planning_decision, "
        "planning_application_status, description, decision_date, source_scrape "
        "FROM applications WHERE uid IN ({})".format(",".join("?" * len(uid_to_texts))),
        list(uid_to_texts.keys()),
    )
    for row in cur:
        uid_meta[row["uid"]] = dict(row)
    con.close()
    print(f"  meta rows: {len(uid_meta):,}", flush=True)

    # Build pool per (decision-bucket, bundled?) cell
    pool: dict[tuple[str, str], list[str]] = defaultdict(list)
    for uid, meta in uid_meta.items():
        bucket = bucket_decision(meta.get("planning_decision"))
        bund = "bundled" if is_bundled(meta.get("description")) else "hp_only"
        pool[(bucket, bund)].append(uid)
    print("Pool sizes:")
    for k, v in sorted(pool.items()):
        print(f"  {k}: {len(v):,}")

    # Target sample plan: 100 apps, balanced across decision x bundled
    plan = {
        ("refused", "hp_only"): 18,
        ("refused", "bundled"): 18,
        ("approved", "hp_only"): 25,
        ("approved", "bundled"): 25,
        ("withdrawn", "hp_only"): 5,
        ("withdrawn", "bundled"): 5,
        ("other", "hp_only"): 2,
        ("other", "bundled"): 2,
    }

    sampled_uids: list[str] = []
    for key, n_want in plan.items():
        bucket_uids = pool.get(key, [])
        n_take = min(n_want, len(bucket_uids))
        sampled_uids.extend(random.sample(bucket_uids, n_take))
        print(f"  took {n_take}/{n_want} from {key}")
    print(f"Total sampled: {len(sampled_uids)}")

    # Prepare output
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_TEXTS.mkdir(parents=True, exist_ok=True)

    # Rank PDFs by what they ARE (manifest's document_type column), with
    # filename heuristics only as a tiebreaker. Lower rank = more relevant.
    #
    # The council's own document_type is the strongest signal -- "Decision",
    # "Decision Notice", "Officer Report", "Report of Handling", etc.
    # are exactly the docs we want. Filename keywords sometimes match
    # background papers (e.g. "DECISION_NOTICE_12102022" filed as type
    # "BackGround Papers") so we trust the type column first.

    DECISION_TYPES = {
        "decision",
        "decision notice",
        "decision letter",
        "recommendation and reasons report",
    }
    OFFICER_TYPES = {
        "officer report",
        "officer reports",
        "delegated report",
        "report of handling",
        "delegated officer report",
        "case officer report",
        "application committee / delegated report",
        "committee report",
    }
    REPORT_HINT_TYPES = {
        "recommendation",
        "case report",
        "planning report",
    }
    DECISION_FILENAME_RE = re.compile(
        r"decision[_ ]notice|decision[_ ]letter|refus(?:al|ed)?|"
        r"permission|granted|approval|approved",
        re.IGNORECASE,
    )
    OFFICER_FILENAME_RE = re.compile(
        r"officer|delegated|report[_ ]of[_ ]handling|"
        r"report[_ ]recommendation|case[_ ]officer|committee[_ ]report",
        re.IGNORECASE,
    )

    def rank(doc_type: str, path: str) -> int:
        t = (doc_type or "").strip().lower()
        if t in DECISION_TYPES:
            return 0
        if t in OFFICER_TYPES:
            return 1
        if t in REPORT_HINT_TYPES:
            return 2
        fname = path.rsplit("/", 1)[-1]
        if DECISION_FILENAME_RE.search(fname):
            return 3
        if OFFICER_FILENAME_RE.search(fname):
            return 4
        return 9

    sample_rows = []
    for uid in sampled_uids:
        meta = uid_meta[uid]
        texts = sorted(
            uid_to_texts[uid],
            key=lambda d: (rank(d["document_type"], d["text_path"]), -d["word_count"]),
        )
        # Cap at 8 most-relevant files to keep token cost bounded.
        texts = texts[:8]
        local_paths = []
        for t in texts:
            src = CORPUS_ROOT / t["text_path"]
            if not src.exists():
                continue
            rel = Path(t["text_path"]).relative_to("texts")
            dst = OUT_TEXTS / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            if not dst.exists():
                shutil.copy2(src, dst)
            local_paths.append(str(rel))

        sample_rows.append(
            {
                "uid": uid,
                "reference": meta.get("reference") or "",
                "authority_name": meta.get("authority_name") or "",
                "planning_decision": meta.get("planning_decision") or "",
                "decision_date": meta.get("decision_date") or "",
                "description": meta.get("description") or "",
                "source_scrape": meta.get("source_scrape") or "",
                "decision_bucket": bucket_decision(meta.get("planning_decision")),
                "is_bundled": is_bundled(meta.get("description")),
                "text_paths": "|".join(local_paths),
            }
        )

    sample_csv = OUT_DIR / "sample.csv"
    with sample_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(sample_rows[0].keys()))
        writer.writeheader()
        writer.writerows(sample_rows)
    print(f"\nWrote {len(sample_rows)} rows -> {sample_csv}")
    print(f"Texts dir: {OUT_TEXTS}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
