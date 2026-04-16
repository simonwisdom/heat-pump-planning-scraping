#!/usr/bin/env python3
"""Match PINS appeals to planning applications across one or more SQLite DBs.

Runs a tiered matcher against the curated PINS fact_case_appeals parquet:

  T1 ref_lpa         — app reference matches appeal reference AND LPA agrees.
  T2 pc_ref_substr   — site postcode + LPA match AND the appeal's reference
                       appears inside one of the app's reference / uid / name /
                       description fields (or vice versa).
  T3 pc_address_sig  — site postcode + LPA match, the (LPA, postcode) pair has
                       exactly one candidate app and one candidate case, AND the
                       leading address "signature" (house number or first
                       distinctive word) agrees between app and appeal.

T1 and T2 are tagged confidence=high; T3 is confidence=medium.

Artifacts are always written. Use --write-db to additionally refresh each DB's
`appeals` table (T1+T2 by default; T3 only with --include-medium).

    uv run --with pandas --with pyarrow python scripts/pins/pins_06_match_appeals.py
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

repo_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(repo_root))

from src.db import get_db, now_iso, transaction  # noqa: E402

DEFAULT_APPEALS_PARQUET = (
    repo_root
    / "_local/workstreams/03_heat_pump_appeals_qualitative/data/raw"
    / "uk_gov_planning_inspectorate_bulk/curated/fact_case_appeals.parquet"
)
DEFAULT_OUTPUT_DIR = (
    repo_root / "_local/workstreams/03_heat_pump_appeals_qualitative/data/intermediate" / "appeals_matching/combined"
)
DEFAULT_DB_PATHS = [
    repo_root / "_local/workstreams/01_heat_pump_applications/data/raw/ashp.db",
    repo_root / "_local/workstreams/01_heat_pump_applications/data/raw/heat_pump_second_pass.db",
    repo_root / "_local/workstreams/02_sound_assessments_all_apps/data/raw/noise_apps.db",
]

STOPWORDS = {
    "AND",
    "AUTHORITY",
    "BOROUGH",
    "CITY",
    "COUNCIL",
    "COUNTY",
    "DISTRICT",
    "LONDON",
    "METROPOLITAN",
    "OF",
    "ROYAL",
    "THE",
    "UNITARY",
}

CANONICAL_AUTHORITY_ALIASES = {
    "ALLERDALE": "CUMBERLAND",
    "AYLESBURY VALE": "BUCKINGHAMSHIRE",
    "BARROW": "WESTMORLAND FURNESS",
    "BRIGHTON": "BRIGHTON HOVE",
    "CARLISLE": "CUMBERLAND",
    "CHILTERN": "BUCKINGHAMSHIRE",
    "CHILTERN SOUTH BUCKS": "BUCKINGHAMSHIRE",
    "COPELAND": "CUMBERLAND",
    "CORBY": "NORTH NORTHAMPTONSHIRE",
    "CRAVEN": "NORTH YORKSHIRE",
    "DAVENTRY": "WEST NORTHAMPTONSHIRE",
    "EAST NORTHAMPTONSHIRE": "NORTH NORTHAMPTONSHIRE",
    "EDEN": "WESTMORLAND FURNESS",
    "HAMMERSMITH": "HAMMERSMITH FULHAM",
    "HARROGATE": "NORTH YORKSHIRE",
    "KENSINGTON": "KENSINGTON CHELSEA",
    "KETTERING": "NORTH NORTHAMPTONSHIRE",
    "MENDIP": "SOMERSET",
    "NORTHAMPTON": "WEST NORTHAMPTONSHIRE",
    "RICHMOND": "RICHMOND THAMES",
    "RICHMONDSHIRE": "NORTH YORKSHIRE",
    "RYEDALE": "NORTH YORKSHIRE",
    "SCARBOROUGH": "NORTH YORKSHIRE",
    "SEDGEMOOR": "SOMERSET",
    "SELBY": "NORTH YORKSHIRE",
    "SOMERSET WEST TAUNTON": "SOMERSET",
    "SOUTH BUCKS": "BUCKINGHAMSHIRE",
    "SOUTH LAKELAND": "WESTMORLAND FURNESS",
    "SOUTH NORTHAMPTONSHIRE": "WEST NORTHAMPTONSHIRE",
    "SOUTH SOMERSET": "SOMERSET",
    "WELLINGBOROUGH": "NORTH NORTHAMPTONSHIRE",
    "WINDSOR": "WINDSOR MAIDENHEAD",
    "WYCOMBE": "BUCKINGHAMSHIRE",
}

ADDRESS_NOISE_TOKENS = {
    "LAND",
    "AT",
    "OF",
    "REAR",
    "FRONT",
    "BEHIND",
    "FORMER",
    "NEXT",
    "NEAR",
    "OPPOSITE",
    "ADJACENT",
    "ADJOINING",
    "SITE",
    "THE",
    "SIDE",
    "TO",
    "ON",
    "RECEPTION",
    "PLOT",
    "UNIT",
}

POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}[0-9][0-9A-Z]?)\s*([0-9][A-Z]{2})\b")

TIER_PRIORITY = {"T1_ref_lpa": 1, "T2_pc_ref_substr": 2, "T3_pc_address_sig": 3}
TIER_CONFIDENCE = {
    "T1_ref_lpa": "high",
    "T2_pc_ref_substr": "high",
    "T3_pc_address_sig": "medium",
}


@dataclass(frozen=True)
class MatchDecision:
    matched: bool
    method: str | None


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------


def normalize_ref(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    ref = str(value).strip().upper()
    if not ref or ref == "NAN":
        return None
    return ref


def derive_name_ref(name: object) -> str | None:
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return None
    text = str(name).strip()
    if not text:
        return None
    prefix_split = text.split("/", 1)
    return normalize_ref(prefix_split[1] if len(prefix_split) == 2 else text)


def canonicalize_authority(name: object) -> str | None:
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return None
    text = str(name).upper().replace("&", " AND ")
    text = re.sub(r"\bST\b", "SAINT", text)
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    tokens = [t for t in text.split() if t and t not in STOPWORDS]
    canonical = " ".join(tokens)
    if not canonical:
        return None
    return CANONICAL_AUTHORITY_ALIASES.get(canonical, canonical)


def normalize_postcode(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    match = POSTCODE_RE.search(value.upper())
    return f"{match.group(1)} {match.group(2)}" if match else None


def address_signature(address: object) -> str | None:
    """Leading signature of an address: first numeric token (house number) or
    the first non-noise word. Postcode is stripped first. Returns None when the
    address is missing or has no usable tokens."""
    if not isinstance(address, str) or not address.strip():
        return None
    stripped = POSTCODE_RE.sub("", address.upper())
    tokens = re.findall(r"[A-Z0-9]+", stripped)
    for token in tokens:
        if token.isdigit() and 1 <= len(token) <= 5:
            return token
    for token in tokens:
        if token not in ADDRESS_NOISE_TOKENS:
            return token
    return None


def authority_match(app_lpa: object, appeal_lpa: object) -> MatchDecision:
    if not isinstance(app_lpa, str) or not isinstance(appeal_lpa, str):
        return MatchDecision(False, None)
    if app_lpa == appeal_lpa:
        return MatchDecision(True, "lpa_exact")
    app_tokens = set(app_lpa.split())
    appeal_tokens = set(appeal_lpa.split())
    if app_tokens <= appeal_tokens or appeal_tokens <= app_tokens:
        return MatchDecision(True, "lpa_token_subset")
    return MatchDecision(False, None)


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def load_applications_multi(db_paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in db_paths:
        with sqlite3.connect(path) as conn:
            apps = pd.read_sql_query(
                "SELECT uid, reference, name, authority_name, postcode, address_text, description FROM applications",
                conn,
            )
        apps["source_db"] = path.stem
        frames.append(apps)
    combined = pd.concat(frames, ignore_index=True)
    combined["authority_canon"] = combined["authority_name"].map(canonicalize_authority)
    combined["postcode_norm"] = combined["postcode"].map(normalize_postcode)
    combined["address_sig"] = combined["address_text"].map(address_signature)
    combined["uid_ref"] = combined["uid"].map(normalize_ref)
    combined["name_ref"] = combined["name"].map(derive_name_ref)
    combined["reference_ref"] = combined["reference"].map(normalize_ref)
    combined["description_upper"] = combined["description"].fillna("").astype(str).str.upper()
    return combined


def load_appeals(appeals_parquet: Path) -> pd.DataFrame:
    appeals = pd.read_parquet(appeals_parquet)
    appeals = appeals[appeals["case_number"].notna()].copy()
    appeals["case_number_norm"] = appeals["case_number"].map(normalize_ref)
    appeals = appeals[appeals["case_number_norm"].notna()].copy()
    appeals["authority_canon"] = appeals["lpa_name"].map(canonicalize_authority)
    appeals["postcode_norm"] = appeals["site_address"].map(normalize_postcode)
    appeals["address_sig"] = appeals["site_address"].map(address_signature)
    appeals["lpa_app_ref_norm"] = appeals["lpa_application_reference"].map(normalize_ref)
    return appeals


# ---------------------------------------------------------------------------
# Tiered matchers
# ---------------------------------------------------------------------------


def tier1_ref_lpa(apps: pd.DataFrame, appeals: pd.DataFrame) -> pd.DataFrame:
    candidates: list[pd.DataFrame] = []
    for source_col, ref_col in (("uid", "uid_ref"), ("name", "name_ref"), ("reference", "reference_ref")):
        frame = apps[["uid", "source_db", "authority_canon", ref_col]].copy()
        frame = frame.rename(columns={ref_col: "ref_norm"})
        frame = frame[frame["ref_norm"].notna()]
        frame["ref_source"] = source_col
        candidates.append(frame)
    app_refs = pd.concat(candidates, ignore_index=True).drop_duplicates(
        subset=["uid", "source_db", "ref_norm", "ref_source"]
    )

    appeal_refs = appeals[["case_number_norm", "lpa_app_ref_norm", "authority_canon"]].rename(
        columns={
            "case_number_norm": "case_number",
            "lpa_app_ref_norm": "ref_norm",
            "authority_canon": "appeal_authority_canon",
        }
    )
    appeal_refs = appeal_refs[appeal_refs["ref_norm"].notna()]

    hits = app_refs.merge(appeal_refs, on="ref_norm", how="inner")
    hits["lpa_ok"] = [
        authority_match(a, b).matched for a, b in zip(hits["authority_canon"], hits["appeal_authority_canon"])
    ]
    matched = hits[hits["lpa_ok"]].copy()
    matched["tier"] = "T1_ref_lpa"
    matched["match_detail"] = "ref_source=" + matched["ref_source"]
    return matched[["uid", "source_db", "case_number", "tier", "match_detail"]].drop_duplicates()


def _ref_substr_source(app_row: pd.Series, appeal_ref: str) -> str | None:
    for source_name, col in (("uid", "uid_ref"), ("reference", "reference_ref"), ("name", "name_ref")):
        fragment = app_row.get(col)
        if not isinstance(fragment, str) or len(fragment) < 4:
            continue
        if fragment in appeal_ref or appeal_ref in fragment:
            return source_name
        if "/" in fragment:
            tail = fragment.split("/", 1)[1]
            if len(tail) >= 4 and (tail in appeal_ref or appeal_ref in tail):
                return f"{source_name}_tail"
    description = app_row.get("description_upper")
    if isinstance(description, str) and len(appeal_ref) >= 6 and appeal_ref in description:
        return "description"
    return None


def tier2_postcode_ref_substr(apps: pd.DataFrame, appeals: pd.DataFrame) -> pd.DataFrame:
    left = apps[apps["postcode_norm"].notna() & apps["authority_canon"].notna()][
        [
            "uid",
            "source_db",
            "authority_canon",
            "postcode_norm",
            "uid_ref",
            "name_ref",
            "reference_ref",
            "description_upper",
        ]
    ].rename(columns={"authority_canon": "_canon", "postcode_norm": "_pc"})

    right = appeals[appeals["postcode_norm"].notna() & appeals["authority_canon"].notna()][
        ["case_number_norm", "authority_canon", "postcode_norm", "lpa_app_ref_norm"]
    ].rename(
        columns={
            "case_number_norm": "case_number",
            "authority_canon": "_canon",
            "postcode_norm": "_pc",
        }
    )
    right = right[right["lpa_app_ref_norm"].notna()]

    joined = left.merge(right, on=["_canon", "_pc"], how="inner")
    joined["substr_source"] = joined.apply(lambda row: _ref_substr_source(row, row["lpa_app_ref_norm"]), axis=1)
    matched = joined[joined["substr_source"].notna()].copy()
    matched["tier"] = "T2_pc_ref_substr"
    matched["match_detail"] = "pc+ref_substr=" + matched["substr_source"]
    return matched[["uid", "source_db", "case_number", "tier", "match_detail"]].drop_duplicates()


def tier3_postcode_address_signature(apps: pd.DataFrame, appeals: pd.DataFrame) -> pd.DataFrame:
    left = apps[apps["postcode_norm"].notna() & apps["authority_canon"].notna()][
        ["uid", "source_db", "authority_canon", "postcode_norm", "address_sig"]
    ].rename(columns={"authority_canon": "_canon", "postcode_norm": "_pc", "address_sig": "app_sig"})

    right = appeals[appeals["postcode_norm"].notna() & appeals["authority_canon"].notna()][
        ["case_number_norm", "authority_canon", "postcode_norm", "address_sig"]
    ].rename(
        columns={
            "case_number_norm": "case_number",
            "authority_canon": "_canon",
            "postcode_norm": "_pc",
            "address_sig": "appeal_sig",
        }
    )

    joined = left.merge(right, on=["_canon", "_pc"], how="inner")
    app_counts = joined.groupby(["_canon", "_pc"])["uid"].nunique().rename("n_apps")
    case_counts = joined.groupby(["_canon", "_pc"])["case_number"].nunique().rename("n_cases")
    joined = joined.merge(app_counts, on=["_canon", "_pc"]).merge(case_counts, on=["_canon", "_pc"])
    joined = joined[(joined["n_apps"] == 1) & (joined["n_cases"] == 1)]
    joined = joined[joined["app_sig"].notna() & joined["appeal_sig"].notna()]
    joined = joined[joined["app_sig"] == joined["appeal_sig"]]

    matched = joined.copy()
    matched["tier"] = "T3_pc_address_sig"
    matched["match_detail"] = "pc_unique+sig=" + matched["app_sig"].astype(str)
    return matched[["uid", "source_db", "case_number", "tier", "match_detail"]].drop_duplicates()


def resolve_by_priority(tiered: pd.DataFrame) -> pd.DataFrame:
    if tiered.empty:
        return tiered.assign(tier_rank=[])  # pragma: no cover
    tiered = tiered.copy()
    tiered["tier_rank"] = tiered["tier"].map(TIER_PRIORITY)
    tiered = tiered.sort_values(["uid", "source_db", "case_number", "tier_rank"])
    resolved = tiered.drop_duplicates(subset=["uid", "source_db", "case_number"], keep="first")
    return resolved.drop(columns=["tier_rank"])


# ---------------------------------------------------------------------------
# DB write
# ---------------------------------------------------------------------------


def ensure_confidence_column(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(appeals)")}
    if "ashp_match_confidence" not in cols:
        conn.execute("ALTER TABLE appeals ADD COLUMN ashp_match_confidence TEXT")


def build_import_rows(
    appeals: pd.DataFrame,
    accepted: pd.DataFrame,
    imported_at: str,
) -> list[tuple[object, ...]]:
    if not accepted.empty:
        dedup = accepted.copy()
        dedup["_tier_rank"] = dedup["tier"].map(TIER_PRIORITY)
        dedup = (
            dedup.sort_values(["case_number", "_tier_rank"])
            .drop_duplicates(subset=["case_number"], keep="first")
            .drop(columns=["_tier_rank"])
        )
    else:
        dedup = accepted
    lookup = dedup.set_index("case_number")[["uid", "tier", "match_detail"]].to_dict("index")

    rows: list[tuple[object, ...]] = []
    for _, row in appeals.iterrows():
        case = row["case_number_norm"]
        linked = lookup.get(case)
        raw_payload = {
            k: v
            for k, v in {
                "casework_type": row.get("casework_type"),
                "reason_for_appeal": row.get("reason_for_appeal"),
                "lpa_ons_code": row.get("lpa_ons_code"),
                "jurisdiction": row.get("jurisdiction"),
                "development_type": row.get("development_type"),
                "site_green_belt": row.get("site_green_belt"),
                "source_file": row.get("source_file"),
                "sheet_name": row.get("sheet_name"),
                "record_type": row.get("record_type"),
                "case_key": row.get("case_key"),
            }.items()
            if not pd.isna(v)
        }
        confidence = TIER_CONFIDENCE[linked["tier"]] if linked else None
        method = f"{linked['tier']}:{linked['match_detail']}" if linked else None
        rows.append(
            (
                case,
                row.get("site_address"),
                row.get("lpa_name"),
                row.get("appeal_type"),
                row.get("decision_date"),
                row.get("decision_outcome"),
                row.get("inspector_name"),
                row.get("appellant"),
                row.get("agent"),
                row.get("procedure"),
                row.get("development_type"),
                1 if linked else 0,
                method,
                confidence,
                linked["uid"] if linked else None,
                json.dumps(raw_payload, default=str, sort_keys=True),
                imported_at,
            )
        )
    return rows


def write_db(
    db_path: Path,
    appeals: pd.DataFrame,
    accepted_for_db: pd.DataFrame,
) -> dict[str, int]:
    imported_at = now_iso()
    rows = build_import_rows(appeals, accepted_for_db, imported_at)
    conn = get_db(db_path)
    try:
        ensure_confidence_column(conn)
        with transaction(conn):
            conn.execute("DELETE FROM appeals")
            conn.executemany(
                """
                INSERT INTO appeals (
                    case_reference, site_address, lpa_name, appeal_type,
                    decision_date, appeal_decision, inspector_name, appellant_name,
                    agent_name, procedure_type, description,
                    is_ashp_related, ashp_match_method, ashp_match_confidence,
                    linked_application_uid, raw_data_json, imported_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
    finally:
        conn.close()
    return {
        "appeals_rows_inserted": len(rows),
        "linked_appeals_rows": int(accepted_for_db["case_number"].nunique()) if len(accepted_for_db) else 0,
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def write_artifacts(
    output_dir: Path,
    apps: pd.DataFrame,
    appeals: pd.DataFrame,
    all_hits: pd.DataFrame,
    resolved: pd.DataFrame,
    db_write_stats: dict[str, dict[str, int]] | None,
) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    all_hits.sort_values(["tier", "uid", "source_db", "case_number"]).to_csv(
        output_dir / "all_tier_hits.csv", index=False
    )
    resolved.sort_values(["tier", "source_db", "uid", "case_number"]).to_csv(
        output_dir / "resolved_matches.csv", index=False
    )

    summary = {
        "applications_loaded_by_db": apps.groupby("source_db").size().to_dict(),
        "applications_total": int(len(apps)),
        "applications_unique_uids": int(apps["uid"].nunique()),
        "appeals_rows_with_case_number": int(len(appeals)),
        "appeals_with_postcode": int(appeals["postcode_norm"].notna().sum()),
        "appeals_with_lpa_app_ref": int(appeals["lpa_app_ref_norm"].notna().sum()),
        "tier_hits_apps_by_tier": all_hits.groupby("tier")["uid"].nunique().to_dict() if len(all_hits) else {},
        "resolved_matches_apps_total": int(resolved["uid"].nunique()) if len(resolved) else 0,
        "resolved_matches_rows_total": int(len(resolved)),
        "resolved_matches_apps_by_tier": resolved.groupby("tier")["uid"].nunique().to_dict() if len(resolved) else {},
        "resolved_matches_apps_by_db": resolved.groupby("source_db")["uid"].nunique().to_dict()
        if len(resolved)
        else {},
        "resolved_matches_apps_by_db_tier": (
            resolved.groupby(["source_db", "tier"])["uid"].nunique().unstack(fill_value=0).to_dict()
        )
        if len(resolved)
        else {},
        "resolved_matches_apps_by_confidence": resolved["tier"].map(TIER_CONFIDENCE).value_counts().to_dict()
        if len(resolved)
        else {},
        "db_write": db_write_stats or {"skipped": True},
    }
    with (output_dir / "summary.json").open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True, default=str)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tiered PINS appeals matcher.")
    parser.add_argument(
        "--db-path",
        type=Path,
        action="append",
        help="Path to an applications SQLite DB. Repeatable. Defaults to the three standard corpora.",
    )
    parser.add_argument("--appeals-parquet", type=Path, default=DEFAULT_APPEALS_PARQUET)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--write-db",
        action="store_true",
        help="Refresh each DB's appeals table with the resolved matches.",
    )
    parser.add_argument(
        "--include-medium",
        action="store_true",
        help="When writing to DB, also include medium-confidence T3 matches. Off by default.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_paths = args.db_path or DEFAULT_DB_PATHS

    apps = load_applications_multi(db_paths)
    appeals = load_appeals(args.appeals_parquet)

    all_hits = pd.concat(
        [
            tier1_ref_lpa(apps, appeals),
            tier2_postcode_ref_substr(apps, appeals),
            tier3_postcode_address_signature(apps, appeals),
        ],
        ignore_index=True,
    )
    resolved = resolve_by_priority(all_hits)

    db_write_stats: dict[str, dict[str, int]] | None = None
    if args.write_db:
        db_write_stats = {}
        for path in db_paths:
            accepted_for_db = resolved[resolved["source_db"] == path.stem].copy()
            if not args.include_medium:
                accepted_for_db = accepted_for_db[accepted_for_db["tier"].map(TIER_CONFIDENCE) == "high"]
            db_write_stats[path.stem] = write_db(path, appeals, accepted_for_db)

    summary = write_artifacts(args.output_dir, apps, appeals, all_hits, resolved, db_write_stats)

    print(f"APPS_TOTAL={summary['applications_total']}")
    print(f"APPEALS_WITH_CASE_NUMBER={summary['appeals_rows_with_case_number']}")
    for tier, n in summary["resolved_matches_apps_by_tier"].items():
        print(f"RESOLVED_{tier}={n}")
    print(f"RESOLVED_TOTAL_APPS={summary['resolved_matches_apps_total']}")
    for conf, n in summary["resolved_matches_apps_by_confidence"].items():
        print(f"RESOLVED_CONFIDENCE_{conf}={n}")
    print(f"OUTPUT_DIR={args.output_dir}")
    if args.write_db:
        for db_name, stats in (db_write_stats or {}).items():
            print(f"DB_WRITE[{db_name}]_APPEALS={stats['appeals_rows_inserted']} LINKED={stats['linked_appeals_rows']}")


if __name__ == "__main__":
    main()
