"""Build a human-review TSV for a schema-extraction run.

Joins the run's results.json to ashp.db for portal/link context and emits a
wide TSV (one row per app) with the full v4 schema plus the documents used and
the live portal + PlanIt links — the artifact reviewers open in a spreadsheet.

Usage:
    HP_RUN_TAG=v4 uv run --with openai --with python-dotenv \
        python3 scripts/llm/build_review_tsv.py

Reads:
    _local/llm_pilot/schema_<tag>_50/results.json
    _local/workstreams/01_heat_pump_applications/data/raw/ashp.db
Writes:
    _local/llm_pilot/schema_<tag>_50/review.tsv
"""

from __future__ import annotations

import csv
import importlib.util
import json
import os
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TAG = os.environ.get("HP_RUN_TAG", "v4")
RUN_DIR = ROOT / f"_local/llm_pilot/schema_{TAG}_50"
DB = ROOT / "_local/workstreams/01_heat_pump_applications/data/raw/ashp.db"

# Optional staging dir holds sample.csv with extra per-uid columns the extractor
# doesn't carry through to results.json (e.g. pre-filter bucket/signals). Default
# matches the extractor's own default; HP_SAMPLE_ROOT overrides.
SAMPLE_ROOT = Path(os.environ.get("HP_SAMPLE_ROOT") or (ROOT / f"_local/llm_pilot/schema_{TAG}_51/staging"))
EXTRA_LEAD_COLS = ["bucket", "prefilter_signals"]

# Pull the canonical field order straight from the extractor so the TSV never
# drifts from the schema.
_spec = importlib.util.spec_from_file_location("ext", ROOT / "scripts/llm/extract_schema_v1.py")
_ext = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ext)
# decision_outcome is dropped from the review view: it duplicates the decision_bucket
# lead column on 48/50 rows (the diffs are prior_approval_* outcomes bucket collapses).
# decision_bucket is the analysis-ready form, so it's the one we keep.
_VIEW_EXCLUDE = {"decision_outcome"}
SCHEMA_FIELDS = [f for f in (list(_ext.T0.keys()) + list(_ext.T1.keys())) if f not in _VIEW_EXCLUDE]


def fmt(v) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, list):
        return "|".join(str(x) for x in v)
    return str(v)


def load_extra_cols() -> dict:
    """Read sample.csv from staging and return uid -> {col: val} for any of
    EXTRA_LEAD_COLS that are present. Silently returns {} if sample.csv is
    missing or lacks any of the extra columns (back-compat for older runs)."""
    sample_csv = SAMPLE_ROOT / "sample.csv"
    if not sample_csv.exists():
        return {}
    out = {}
    with sample_csv.open(encoding="utf-8") as fh:
        rd = csv.DictReader(fh)
        present = [c for c in EXTRA_LEAD_COLS if c in (rd.fieldnames or [])]
        if not present:
            return {}
        for r in rd:
            out[r["uid"]] = {c: r.get(c, "") for c in present}
    return out


def main() -> int:
    results = json.loads((RUN_DIR / "results.json").read_text())
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row

    extras = load_extra_cols()
    extra_cols = list(next(iter(extras.values())).keys()) if extras else []

    lead = (
        ["uid", "authority"] + extra_cols + ["portal_type", "decision_bucket", "planning_decision_raw", "description"]
    )
    tail = ["files_used", "documentation_url", "planit_link"]
    header = lead + SCHEMA_FIELDS + tail

    out_path = RUN_DIR / "review.tsv"
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(header)
        for r in results:
            db_row = con.execute(
                "SELECT portal_type, documentation_url, planit_link FROM applications WHERE uid=?",
                (r["uid"],),
            ).fetchone()
            db_row = db_row or {}
            files = "|".join(f"{f['fname']} [{f['doctype']}]" for f in r.get("_files_used", []))
            extra = extras.get(r["uid"], {})
            row = (
                [
                    r.get("uid", ""),
                    r.get("authority_name", ""),
                ]
                + [extra.get(c, "") for c in extra_cols]
                + [
                    db_row["portal_type"] if db_row else "",
                    r.get("decision_bucket", ""),
                    r.get("planning_decision", ""),
                    (r.get("description", "") or "").replace("\t", " ").replace("\n", " "),
                ]
            )
            row += [fmt(r.get(f)) for f in SCHEMA_FIELDS]
            row += [
                files,
                db_row["documentation_url"] if db_row else "",
                db_row["planit_link"] if db_row else "",
            ]
            w.writerow(row)

    print(f"Wrote {out_path}  ({len(results)} rows, {len(header)} cols)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
