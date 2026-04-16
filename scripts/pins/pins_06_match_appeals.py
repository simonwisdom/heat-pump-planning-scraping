#!/usr/bin/env python3
"""Refresh PINS appeals in SQLite and relink matched applications."""

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

from src.config import DB_PATH  # noqa: E402
from src.db import get_db, now_iso, transaction  # noqa: E402

DEFAULT_APPEALS_PARQUET = (
    repo_root
    / "_local"
    / "workstreams"
    / "03_heat_pump_appeals_qualitative"
    / "data"
    / "raw"
    / "uk_gov_planning_inspectorate_bulk"
    / "curated"
    / "fact_case_appeals.parquet"
)
DEFAULT_OUTPUT_DIR = (
    repo_root
    / "_local"
    / "workstreams"
    / "03_heat_pump_appeals_qualitative"
    / "data"
    / "intermediate"
    / "appeals_matching"
    / "full_db"
)

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


@dataclass(frozen=True)
class MatchDecision:
    matched: bool
    method: str | None


APP_REF_SOURCE_RANK = {
    "uid": 1,
    "name": 2,
    "reference": 3,
}

AUTHORITY_MATCH_RANK = {
    "lpa_exact": 1,
    "lpa_token_subset": 2,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh SQLite appeals rows and relink matched ASHP applications.")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DB_PATH,
        help="SQLite database to refresh.",
    )
    parser.add_argument(
        "--appeals-parquet",
        type=Path,
        default=DEFAULT_APPEALS_PARQUET,
        help="Curated PINS case-level parquet.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for match artifacts.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build artifacts and summary without writing back to SQLite.",
    )
    return parser.parse_args()


def normalize_ref(value: object) -> str | None:
    if pd.isna(value):
        return None
    ref = str(value).strip().upper()
    if not ref or ref == "NAN":
        return None
    return ref


def derive_name_ref(name: object) -> str | None:
    if pd.isna(name):
        return None
    text = str(name).strip()
    if not text:
        return None
    prefix_split = text.split("/", 1)
    return normalize_ref(prefix_split[1] if len(prefix_split) == 2 else text)


def canonicalize_authority(name: object) -> str | None:
    if pd.isna(name):
        return None
    text = str(name).upper().replace("&", " AND ")
    text = re.sub(r"\bST\b", "SAINT", text)
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    tokens = [token for token in text.split() if token and token not in STOPWORDS]
    canonical = " ".join(tokens)
    if not canonical:
        return None
    return CANONICAL_AUTHORITY_ALIASES.get(canonical, canonical)


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


def load_applications(db_path: Path) -> pd.DataFrame:
    with sqlite3.connect(db_path) as conn:
        apps = pd.read_sql_query(
            """
            SELECT uid, reference, name, authority_name
            FROM applications
            """,
            conn,
        )
    apps["authority_canon"] = apps["authority_name"].map(canonicalize_authority)
    apps["uid_ref"] = apps["uid"].map(normalize_ref)
    apps["name_ref"] = apps["name"].map(derive_name_ref)
    apps["reference_ref"] = apps["reference"].map(normalize_ref)
    return apps


def load_appeals(appeals_parquet: Path) -> pd.DataFrame:
    appeals = pd.read_parquet(appeals_parquet)
    appeals["case_number_norm"] = appeals["case_number"].map(normalize_ref)
    appeals = appeals[appeals["case_number_norm"].notna()].copy()
    appeals["lpa_application_reference_norm"] = appeals["lpa_application_reference"].map(normalize_ref)
    appeals["authority_canon"] = appeals["lpa_name"].map(canonicalize_authority)
    return appeals


def build_app_ref_candidates(apps: pd.DataFrame) -> pd.DataFrame:
    candidate_frames = []
    for source_name, column_name in (
        ("uid", "uid_ref"),
        ("name", "name_ref"),
        ("reference", "reference_ref"),
    ):
        frame = apps[["uid", "authority_name", "authority_canon", column_name]].copy()
        frame = frame.rename(columns={column_name: "ref_norm"})
        frame = frame[frame["ref_norm"].notna()].copy()
        frame["ref_source"] = source_name
        frame["ref_source_rank"] = APP_REF_SOURCE_RANK[source_name]
        candidate_frames.append(frame)
    candidates = pd.concat(candidate_frames, ignore_index=True)
    candidates = candidates.drop_duplicates(subset=["uid", "ref_norm", "ref_source"], keep="first")
    return candidates


def build_match_candidates(
    app_ref_candidates: pd.DataFrame, appeals: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw_ref_hits = app_ref_candidates.merge(
        appeals[
            [
                "case_number_norm",
                "lpa_application_reference_norm",
                "lpa_name",
                "authority_canon",
            ]
        ].rename(
            columns={
                "case_number_norm": "case_number",
                "lpa_application_reference_norm": "ref_norm",
                "authority_canon": "appeal_authority_canon",
            }
        ),
        on="ref_norm",
        how="inner",
    )
    decisions = [
        authority_match(app_canon, appeal_canon)
        for app_canon, appeal_canon in zip(raw_ref_hits["authority_canon"], raw_ref_hits["appeal_authority_canon"])
    ]
    raw_ref_hits["authority_matched"] = [decision.matched for decision in decisions]
    raw_ref_hits["authority_match_method"] = [decision.method for decision in decisions]
    authority_hits = raw_ref_hits[raw_ref_hits["authority_matched"]].copy()
    authority_hits["authority_match_rank"] = authority_hits["authority_match_method"].map(AUTHORITY_MATCH_RANK)
    authority_hits = authority_hits.sort_values(
        [
            "ref_source_rank",
            "authority_match_rank",
            "uid",
            "case_number",
        ]
    )
    authority_hits = authority_hits.drop_duplicates(subset=["uid", "case_number"], keep="first")
    return raw_ref_hits, authority_hits


def resolve_one_to_one_matches(authority_hits: pd.DataFrame) -> pd.DataFrame:
    accepted_parts: list[pd.DataFrame] = []
    remaining = authority_hits.copy()

    for ref_rank in sorted(remaining["ref_source_rank"].dropna().unique()):
        for authority_rank in sorted(remaining["authority_match_rank"].dropna().unique()):
            tier = remaining[
                (remaining["ref_source_rank"] == ref_rank) & (remaining["authority_match_rank"] == authority_rank)
            ].copy()
            if tier.empty:
                continue
            changed = True
            while changed and not tier.empty:
                app_counts = tier.groupby("uid")["case_number"].nunique()
                case_counts = tier.groupby("case_number")["uid"].nunique()
                winners = tier[tier["uid"].map(app_counts) == 1].copy()
                winners = winners[winners["case_number"].map(case_counts) == 1].copy()
                if winners.empty:
                    changed = False
                    continue
                accepted_parts.append(winners)
                matched_apps = set(winners["uid"])
                matched_cases = set(winners["case_number"])
                remaining = remaining[
                    (~remaining["uid"].isin(matched_apps)) & (~remaining["case_number"].isin(matched_cases))
                ].copy()
                tier = remaining[
                    (remaining["ref_source_rank"] == ref_rank) & (remaining["authority_match_rank"] == authority_rank)
                ].copy()

    if not accepted_parts:
        return authority_hits.iloc[0:0].copy()

    accepted = pd.concat(accepted_parts, ignore_index=True)
    accepted = accepted.sort_values(["ref_source_rank", "authority_match_rank", "uid", "case_number"])
    accepted = accepted.drop_duplicates(subset=["uid", "case_number"], keep="first")
    return accepted


def build_import_rows(
    appeals: pd.DataFrame, accepted_matches: pd.DataFrame, imported_at: str
) -> list[tuple[object, ...]]:
    accepted_map = accepted_matches.set_index("case_number")[["uid", "ref_source", "authority_match_method"]].to_dict(
        "index"
    )

    rows = []
    for _, row in appeals.iterrows():
        case_number = row["case_number_norm"]
        linked = accepted_map.get(case_number)
        raw_payload = {
            key: value
            for key, value in {
                "casework_type": row.get("casework_type"),
                "reason_for_appeal": row.get("reason_for_appeal"),
                "lpa_ons_code": row.get("lpa_ons_code"),
                "jurisdiction": row.get("jurisdiction"),
                "development_type": row.get("development_type"),
                "site_green_belt": row.get("site_green_belt"),
                "is_flooding_an_issue": row.get("is_flooding_an_issue"),
                "is_site_within_an_aonb": row.get("is_site_within_an_aonb"),
                "is_site_within_an_sssi": row.get("is_site_within_an_sssi"),
                "number_of_residences": row.get("number_of_residences"),
                "area_of_site_hectares": row.get("area_of_site_hectares"),
                "floor_space_square_metres": row.get("floor_space_square_metres"),
                "source_file": row.get("source_file"),
                "sheet_name": row.get("sheet_name"),
                "header_confidence": row.get("header_confidence"),
                "record_type": row.get("record_type"),
                "case_key": row.get("case_key"),
            }.items()
            if not pd.isna(value)
        }
        rows.append(
            (
                case_number,
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
                (f"ref_{linked['ref_source']}+{linked['authority_match_method']}" if linked else None),
                linked["uid"] if linked else None,
                json.dumps(raw_payload, default=str, sort_keys=True),
                imported_at,
            )
        )
    return rows


def write_db(db_path: Path, appeals: pd.DataFrame, accepted_matches: pd.DataFrame) -> dict[str, int]:
    imported_at = now_iso()
    rows = build_import_rows(appeals, accepted_matches, imported_at)
    conn = get_db(db_path)
    try:
        with transaction(conn):
            conn.execute("DELETE FROM appeals")
            conn.executemany(
                """
                INSERT INTO appeals (
                    case_reference,
                    site_address,
                    lpa_name,
                    appeal_type,
                    decision_date,
                    appeal_decision,
                    inspector_name,
                    appellant_name,
                    agent_name,
                    procedure_type,
                    description,
                    is_ashp_related,
                    ashp_match_method,
                    linked_application_uid,
                    raw_data_json,
                    imported_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
    finally:
        conn.close()
    return {
        "appeals_rows_inserted": len(rows),
        "linked_appeals_rows": int((accepted_matches["case_number"]).nunique()),
    }


def build_summary(
    apps: pd.DataFrame,
    appeals: pd.DataFrame,
    raw_ref_hits: pd.DataFrame,
    authority_hits: pd.DataFrame,
    accepted_matches: pd.DataFrame,
    db_write_stats: dict[str, int] | None,
) -> dict[str, object]:
    unresolved_after_authority = authority_hits[
        ~authority_hits["uid"].isin(accepted_matches["uid"])
        & ~authority_hits["case_number"].isin(accepted_matches["case_number"])
    ].copy()
    return {
        "applications_total": int(len(apps)),
        "appeals_rows_case_referenceable": int(len(appeals)),
        "appeals_rows_with_lpa_ref": int(appeals["lpa_application_reference_norm"].notna().sum()),
        "raw_ref_hit_rows": int(len(raw_ref_hits)),
        "raw_ref_hit_apps": int(raw_ref_hits["uid"].nunique()),
        "raw_ref_hit_cases": int(raw_ref_hits["case_number"].nunique()),
        "authority_matched_rows": int(len(authority_hits)),
        "authority_matched_apps": int(authority_hits["uid"].nunique()),
        "authority_matched_cases": int(authority_hits["case_number"].nunique()),
        "resolved_match_rows": int(len(accepted_matches)),
        "resolved_match_apps": int(accepted_matches["uid"].nunique()),
        "resolved_match_cases": int(accepted_matches["case_number"].nunique()),
        "resolved_by_ref_source": accepted_matches["ref_source"].value_counts().sort_index().to_dict(),
        "resolved_by_authority_method": accepted_matches["authority_match_method"]
        .value_counts()
        .sort_index()
        .to_dict(),
        "unresolved_authority_matched_rows": int(len(unresolved_after_authority)),
        "unresolved_authority_matched_apps": int(unresolved_after_authority["uid"].nunique()),
        "unresolved_authority_matched_cases": int(unresolved_after_authority["case_number"].nunique()),
        "db_write": db_write_stats or {"skipped": True},
    }


def write_artifacts(
    output_dir: Path,
    raw_ref_hits: pd.DataFrame,
    authority_hits: pd.DataFrame,
    accepted_matches: pd.DataFrame,
    summary: dict[str, object],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_columns = [
        "uid",
        "authority_name",
        "ref_norm",
        "ref_source",
        "case_number",
        "lpa_name",
        "authority_matched",
        "authority_match_method",
    ]
    raw_ref_hits[raw_columns].sort_values(["uid", "ref_source", "case_number"]).to_csv(
        output_dir / "raw_ref_hits.csv", index=False
    )

    authority_columns = [
        "uid",
        "authority_name",
        "ref_norm",
        "ref_source",
        "case_number",
        "lpa_name",
        "authority_match_method",
    ]
    authority_hits.sort_values(["uid", "ref_source_rank", "authority_match_rank", "case_number"])[
        authority_columns
    ].to_csv(output_dir / "authority_matched_candidates.csv", index=False)

    accepted_columns = [
        "uid",
        "authority_name",
        "ref_norm",
        "ref_source",
        "case_number",
        "lpa_name",
        "authority_match_method",
    ]
    accepted_matches[accepted_columns].sort_values(["uid", "case_number"]).to_csv(
        output_dir / "accepted_matches.csv", index=False
    )

    with (output_dir / "summary.json").open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)


def main() -> None:
    args = parse_args()
    apps = load_applications(args.db_path)
    appeals = load_appeals(args.appeals_parquet)
    app_ref_candidates = build_app_ref_candidates(apps)
    raw_ref_hits, authority_hits = build_match_candidates(app_ref_candidates, appeals)
    accepted_matches = resolve_one_to_one_matches(authority_hits)

    db_write_stats = None
    if not args.dry_run:
        db_write_stats = write_db(args.db_path, appeals, accepted_matches)

    summary = build_summary(
        apps=apps,
        appeals=appeals,
        raw_ref_hits=raw_ref_hits,
        authority_hits=authority_hits,
        accepted_matches=accepted_matches,
        db_write_stats=db_write_stats,
    )
    write_artifacts(
        output_dir=args.output_dir,
        raw_ref_hits=raw_ref_hits,
        authority_hits=authority_hits,
        accepted_matches=accepted_matches,
        summary=summary,
    )

    print(f"APPLICATIONS_TOTAL={summary['applications_total']}")
    print(f"APPEALS_IMPORTED_ROWS={summary['appeals_rows_case_referenceable']}")
    print(f"RAW_REF_HIT_APPS={summary['raw_ref_hit_apps']}")
    print(f"AUTHORITY_MATCHED_APPS={summary['authority_matched_apps']}")
    print(f"RESOLVED_MATCH_APPS={summary['resolved_match_apps']}")
    print(f"RESOLVED_MATCH_CASES={summary['resolved_match_cases']}")
    print(f"OUTPUT_DIR={args.output_dir}")
    if args.dry_run:
        print("DB_WRITE=skipped")
    else:
        print(f"DB_WRITE_APPEALS_ROWS={summary['db_write']['appeals_rows_inserted']}")
        print(f"DB_WRITE_LINKED_ROWS={summary['db_write']['linked_appeals_rows']}")


if __name__ == "__main__":
    main()
