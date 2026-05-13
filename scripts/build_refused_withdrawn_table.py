"""Build a flat TSV of all refused + withdrawn heat-pump apps with their
metadata, edge-case flags, refusal-reason labels (where mined), and links.

One row per application. Output: reports/heat-pump-decisions/refused_withdrawn.tsv

Joins:
  applications (VPS ashp.db)         — base metadata
  rules/edge_case_keywords.toml      — is_listed / is_flat / etc.
  per_app_refusal_labels.csv         — refusal-reason labels from text mining
"""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import tomllib
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "_local/workstreams/01_heat_pump_applications/data/raw/ashp.db"
DEFAULT_LABELS_CSV = ROOT / "reports/heat-pump-decisions/refusal_reasons/per_app_refusal_labels.csv"
DEFAULT_EDGE_RULES = ROOT / "rules/edge_case_keywords.toml"
DEFAULT_TEXT_ROOT = ROOT / "_local/workstreams/01_heat_pump_applications/data/intermediate/refusal_text_analysis/text"
_TEXT_ANALYSIS = "_local/workstreams/01_heat_pump_applications/data/intermediate/refusal_text_analysis"
DEFAULT_MANIFEST = ROOT / f"{_TEXT_ANALYSIS}/refused_docs_text_manifest.csv"
DEFAULT_WITHDRAWN_MANIFEST = ROOT / f"{_TEXT_ANALYSIS}/withdrawn_docs_text_manifest.csv"
DEFAULT_OUTPUT = ROOT / "reports/heat-pump-decisions/refused_withdrawn.tsv"


def load_edge_rules(path: Path) -> dict[str, list[tuple[str, re.Pattern[str]]]]:
    data = tomllib.loads(path.read_text())
    out: dict[str, list[tuple[str, re.Pattern[str]]]] = {}
    for key, val in data.items():
        if not isinstance(val, dict):
            continue
        matches = val.get("match") or []
        out[key] = [(m["field"], re.compile(m["pattern"], re.I)) for m in matches]
    return out


def classify_outcome(decision: str) -> str:
    d = (decision or "").lower()
    if "withdraw" in d:
        return "withdrawn"
    if "refus" in d or "reject" in d or "declin" in d or "denied" in d:
        return "refused"
    return "other"


def derive_app_type_lower(planning_application_type: str | None) -> str:
    """Cheap heuristic — same buckets as the report but minimal regex."""
    t = (planning_application_type or "").lower()
    if any(w in t for w in ("listed", "heritage", "conservation")):
        return "heritage"
    if "outline" in t:
        return "outline"
    if "amendment" in t or "non-material" in t or "minor" in t:
        return "amendment"
    if "condition" in t or "discharge" in t:
        return "conditions"
    if "tree" in t:
        return "trees"
    if "advert" in t:
        return "advertising"
    return "full"


# Inline flag patterns that split / extend the existing edge_case_keywords.toml
# without disturbing the live decision-patterns report.
INLINE_PATTERNS: dict[str, tuple[str, re.Pattern[str]]] = {
    # Property is in / adjoins a conservation area. Separated from listed because
    # the two have different planning implications (CA removes PD; listing
    # requires LBC).
    "is_conservation_area": (
        "description",
        re.compile(r"\bconservation\s+area\b", re.I),
    ),
    # Property is a listed building (or grade I/II/III mentioned). Heritage
    # app-type fallback below catches the LBC application type itself.
    "is_heritage_listed": (
        "_combined",
        re.compile(
            r"\blisted\s+(building|property|dwelling|structure|consent|status|home|house)s?\b"
            r"|\bgrade\s+(i{1,3}|iv|1|2|3)\b",
            re.I,
        ),
    ),
    # Explicit sound / noise assessment evidence — narrower than mentions_noise.
    # Catches BS 4142, MCS 020, "noise assessment", "acoustic report".
    "mentions_sound_assessment": (
        "description",
        re.compile(
            r"\b(noise|sound|acoustic)\s+(assessment|report|survey|test)\b"
            r"|\bBS\s*4142\b"
            r"|\bMCS\s*0?20\b",
            re.I,
        ),
    ),
}


def apply_edge_flags(row: dict, rules: dict[str, list[tuple[str, re.Pattern[str]]]]) -> dict[str, bool]:
    desc = row.get("description") or ""
    app_type_lower = derive_app_type_lower(row.get("planning_application_type"))
    fields = {"description": desc, "app_type_lower": app_type_lower}
    out: dict[str, bool] = {}
    for flag, matches in rules.items():
        out[flag] = any(p.search(fields.get(f, "") or "") for f, p in matches)
    # Apply inline patterns
    for flag, (field, pat) in INLINE_PATTERNS.items():
        if field == "_combined":
            hit = bool(pat.search(desc)) or app_type_lower == "heritage"
        else:
            hit = bool(pat.search(fields.get(field, "") or ""))
        out[flag] = hit
    return out


def days_between(start: str | None, end: str | None) -> int | None:
    if not start or not end:
        return None
    try:
        s = date.fromisoformat(start[:10])
        e = date.fromisoformat(end[:10])
    except ValueError:
        return None
    return (e - s).days


def first_doc_folder(file_path: str | None) -> str:
    """Returns 'Council/Reference/' prefix from a gdrive file_path."""
    if not file_path:
        return ""
    parts = file_path.split("/")
    if len(parts) >= 2:
        return "/".join(parts[:2]) + "/"
    return ""


def extract_top_excerpt(text_root: Path, manifest_rows: list[dict], max_chars: int = 500) -> str:
    """Read the first decision-priority extracted text file and snip a section."""
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from cluster_refusal_reasons import REFUSAL_SECTION_END, REFUSAL_SECTION_START  # type: ignore

    for r in manifest_rows:
        fp = (r.get("file_path") or "").strip()
        if not fp:
            continue
        tp = (text_root / fp).with_suffix(".txt")
        if not tp.exists():
            continue
        try:
            txt = tp.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if not txt.strip():
            continue
        m = REFUSAL_SECTION_START.search(txt)
        if m:
            start = m.end()
            tail = txt[start : start + 4000]
            e = REFUSAL_SECTION_END.search(tail)
            section = tail[: e.start() if e else len(tail)]
            section = re.sub(r"\s+", " ", section).strip()
            if section:
                return section[:max_chars]
        # Fall back to start of doc
        return re.sub(r"\s+", " ", txt[:max_chars]).strip()
    return ""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--labels-csv", type=Path, default=DEFAULT_LABELS_CSV)
    ap.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    ap.add_argument("--withdrawn-manifest", type=Path, default=DEFAULT_WITHDRAWN_MANIFEST)
    ap.add_argument("--edge-rules", type=Path, default=DEFAULT_EDGE_RULES)
    ap.add_argument("--text-root", type=Path, default=DEFAULT_TEXT_ROOT)
    ap.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = ap.parse_args()

    if not args.db.exists():
        ap.error(f"DB not found: {args.db}. Provide --db or run via VPS sync.")
    edge_rules = load_edge_rules(args.edge_rules)

    labels_by_uid: dict[str, dict] = {}
    if args.labels_csv.exists():
        with args.labels_csv.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                labels_by_uid[row["application_uid"]] = row
        print(f"loaded {len(labels_by_uid)} refusal-text rows from {args.labels_csv.name}")
    else:
        print(f"WARN: labels csv not found ({args.labels_csv}) — refusal_labels will be empty")

    # Manifest rows for excerpt extraction (per uid, doc-priority ordered)
    manifest_by_uid: dict[str, list[dict]] = {}
    combined = args.manifest.parent / "combined_text_manifest.csv"
    paths = (combined,) if combined.exists() else (args.manifest, args.withdrawn_manifest)
    for mpath in paths:
        if not mpath.exists():
            continue
        with mpath.open(newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                manifest_by_uid.setdefault(r["application_uid"], []).append(r)

    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT uid, reference, name, description, address_text, postcode,
               authority_name, lat, lng, planning_application_type, app_size,
               planning_application_status, planning_decision,
               start_date, consulted_date, decision_date,
               documentation_url, n_documents, portal_type, portal_base_url,
               planit_link, source_scrape
        FROM applications
        WHERE planning_decision LIKE '%efus%'
           OR planning_decision LIKE '%eject%'
           OR planning_decision LIKE '%eclin%'
           OR planning_decision LIKE '%enied%'
           OR planning_decision LIKE '%ithdraw%'
        """
    )

    out_rows = []
    for r in cur:
        row = dict(r)
        outcome = classify_outcome(row["planning_decision"] or "")
        if outcome not in ("refused", "withdrawn"):
            continue
        flags = apply_edge_flags(row, edge_rules)
        ldata = labels_by_uid.get(row["uid"], {})
        # Boost recall: if the mined refusal-reason labels mention conservation
        # area / listed building / noise, propagate that to the boolean flag
        # even when the application description was silent.
        text_labels = set((ldata.get("labels") or "").split("; "))
        if "heritage_conservation_area" in text_labels:
            flags["is_conservation_area"] = True
        if "heritage_listed" in text_labels:
            flags["is_heritage_listed"] = True
        if "flat_apartment" in text_labels:
            flags["is_flat"] = True
        if "noise" in text_labels:
            flags["mentions_noise"] = True
        manifest_rows = manifest_by_uid.get(row["uid"], [])
        excerpt = extract_top_excerpt(args.text_root, manifest_rows) if manifest_rows else ""
        out_rows.append(
            {
                # IDs
                "application_uid": row["uid"],
                "reference": row["reference"] or "",
                "authority_name": row["authority_name"] or "",
                # Outcome
                "outcome_bucket": outcome,
                "planning_decision_raw": row["planning_decision"] or "",
                "decision_date": row["decision_date"] or "",
                # Timeline
                "start_date": row["start_date"] or "",
                "consulted_date": row["consulted_date"] or "",
                "days_to_decision": days_between(row["start_date"], row["decision_date"]) or "",
                # App context
                "description": (row["description"] or "").replace("\t", " ").replace("\n", " "),
                "address_text": (row["address_text"] or "").replace("\t", " "),
                "postcode": row["postcode"] or "",
                "lat": row["lat"] if row["lat"] is not None else "",
                "lng": row["lng"] if row["lng"] is not None else "",
                "planning_application_type": row["planning_application_type"] or "",
                "app_type_bucket": derive_app_type_lower(row["planning_application_type"]),
                "app_size": row["app_size"] or "",
                "source_scrape": row["source_scrape"] or "",
                # Edge-case flags (split listed/heritage/CA; explicit sound assessment)
                "is_flat": flags.get("is_flat", False),
                "is_conservation_area": flags.get("is_conservation_area", False),
                "is_heritage_listed": flags.get("is_heritage_listed", False),
                "mentions_noise": flags.get("mentions_noise", False),
                "mentions_sound_assessment": flags.get("mentions_sound_assessment", False),
                "is_wind_turbine": flags.get("is_wind_turbine", False),
                # Doc refs
                "planit_url": row["planit_link"] or "",
                "portal_documentation_url": row["documentation_url"] or "",
                "portal_base_url": row["portal_base_url"] or "",
                "portal_type": row["portal_type"] or "",
                "n_documents_listed": row["n_documents"] or 0,
                "gdrive_folder": first_doc_folder(manifest_rows[0]["file_path"]) if manifest_rows else "",
                # Refusal-reason text analysis
                "refusal_labels": ldata.get("labels", ""),
                "n_labels": ldata.get("n_labels", ""),
                "used_refusal_section": ldata.get("used_refusal_section", ""),
                "mentions_heat_pump_in_text": ldata.get("mentions_heat_pump", ""),
                "n_text_docs_extracted": ldata.get("n_docs_used", ""),
                "refusal_text_excerpt": excerpt,
            }
        )

    conn.close()

    fieldnames = list(out_rows[0].keys()) if out_rows else []
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        w.writeheader()
        w.writerows(out_rows)

    n_refused = sum(1 for r in out_rows if r["outcome_bucket"] == "refused")
    n_withdrawn = sum(1 for r in out_rows if r["outcome_bucket"] == "withdrawn")
    n_with_labels = sum(1 for r in out_rows if r["refusal_labels"])
    print(f"wrote {len(out_rows)} rows to {args.output}")
    print(f"  refused:   {n_refused}")
    print(f"  withdrawn: {n_withdrawn}")
    print(f"  with refusal-reason labels: {n_with_labels}")
    print(f"  with gdrive_folder reference: {sum(1 for r in out_rows if r['gdrive_folder'])}")


if __name__ == "__main__":
    main()
