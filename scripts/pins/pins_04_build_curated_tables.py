#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd
import yaml

PROVENANCE_COLS = {
    "source_file",
    "source_stem",
    "sheet_name",
    "sheet_slug",
    "header_row_number",
    "header_confidence",
    "typed_row_number",
}


def norm_text(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    text = re.sub(r"\s+", " ", text)
    if text.lower() in {"-", "n/a", "na", "none", "null"}:
        return None
    return text


def to_number(value: object) -> float | None:
    text = norm_text(value)
    if text is None:
        return None
    text = text.replace(",", "")
    text = text.replace("£", "").replace("$", "")
    if text.endswith("%"):
        text = text[:-1]
    text = text.strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def to_iso_date(value: object) -> str | None:
    text = norm_text(value)
    if text is None:
        return None
    dt = pd.to_datetime(text, dayfirst=True, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.strftime("%Y-%m-%d")


def map_decision(value: object) -> str | None:
    text = norm_text(value)
    if text is None:
        return None
    t = text.lower()
    mapping = {
        "allowed": "allowed",
        "appeal allowed": "allowed",
        "allowed in part": "allowed_in_part",
        "split decision": "split_decision",
        "dismissed": "dismissed",
        "appeal dismissed": "dismissed",
        "rejected": "dismissed",
        "invalid": "invalid",
        "withdrawn": "withdrawn",
        "closed": "closed",
    }
    return mapping.get(t, t)


def map_bool(value: object) -> bool | None:
    text = norm_text(value)
    if text is None:
        return None
    t = text.lower()
    if t in {"yes", "y", "true", "1"}:
        return True
    if t in {"no", "n", "false", "0"}:
        return False
    return None


def first_existing(df: pd.DataFrame, aliases: list[str]) -> str | None:
    for c in aliases:
        if c in df.columns:
            return c
    return None


def extract_case_table(
    df: pd.DataFrame,
    aliases: dict[str, list[str]],
    source_file: str,
    sheet_name: str,
    header_confidence: float,
) -> pd.DataFrame:
    out = pd.DataFrame()

    canonical_cols = [
        "case_number",
        "casework_type",
        "appeal_type",
        "reason_for_appeal",
        "lpa_name",
        "lpa_ons_code",
        "lpa_application_reference",
        "site_address",
        "development_type",
        "procedure",
        "received_date",
        "valid_date",
        "start_date",
        "decision_date",
        "decision_outcome",
        "jurisdiction",
        "inspector_name",
        "appellant",
        "agent",
        "site_green_belt",
        "is_flooding_an_issue",
        "is_site_within_an_aonb",
        "is_site_within_an_sssi",
        "number_of_residences",
        "area_of_site_hectares",
        "floor_space_square_metres",
    ]

    for col in canonical_cols:
        src = first_existing(df, aliases.get(col, [col]))
        out[col] = df[src] if src else None

    for c in [
        "case_number",
        "casework_type",
        "appeal_type",
        "reason_for_appeal",
        "lpa_name",
        "lpa_ons_code",
        "lpa_application_reference",
        "site_address",
        "development_type",
        "procedure",
        "jurisdiction",
        "inspector_name",
        "appellant",
        "agent",
    ]:
        out[c] = out[c].map(norm_text)

    for c in ["received_date", "valid_date", "start_date", "decision_date"]:
        out[c] = out[c].map(to_iso_date)

    out["decision_outcome"] = out["decision_outcome"].map(map_decision)

    for c in [
        "site_green_belt",
        "is_flooding_an_issue",
        "is_site_within_an_aonb",
        "is_site_within_an_sssi",
    ]:
        out[c] = out[c].map(map_bool)

    for c in [
        "number_of_residences",
        "area_of_site_hectares",
        "floor_space_square_metres",
    ]:
        out[c] = out[c].map(to_number)

    out["source_file"] = source_file
    out["sheet_name"] = sheet_name
    out["header_confidence"] = header_confidence
    out["record_type"] = "case_level"
    out["case_key"] = out["case_number"]

    # keep rows where there is at least a key or a decision outcome/date
    keep = out["case_number"].notna() | out["decision_outcome"].notna() | out["decision_date"].notna()
    out = out.loc[keep].reset_index(drop=True)
    return out


def build_aggregate_rows(
    df: pd.DataFrame,
    source_file: str,
    sheet_name: str,
    header_confidence: float,
) -> tuple[pd.DataFrame, dict]:
    work = df.copy()
    typed_cols = [c for c in work.columns if c not in PROVENANCE_COLS]
    if not typed_cols:
        return pd.DataFrame(), {"reason": "no_typed_columns"}

    non_null_count = work[typed_cols].notna().sum()
    numeric_scores: dict[str, float] = {}
    numeric_cols: list[str] = []
    for c in typed_cols:
        nn = int(non_null_count[c])
        if nn == 0:
            continue
        parsed = work[c].map(to_number)
        ratio = float(parsed.notna().sum()) / float(nn)
        numeric_scores[c] = ratio
        if nn >= 5 and ratio >= 0.6:
            numeric_cols.append(c)

    if not numeric_cols:
        return pd.DataFrame(), {"reason": "no_numeric_metric_columns"}

    dimension_cols = [c for c in typed_cols if c not in numeric_cols]
    out_parts: list[pd.DataFrame] = []

    for metric_col in numeric_cols:
        part = work[dimension_cols].copy()
        part["metric_name"] = metric_col
        part["metric_value"] = work[metric_col].map(to_number)
        part = part[part["metric_value"].notna()].copy()
        if part.empty:
            continue

        part["source_file"] = source_file
        part["sheet_name"] = sheet_name
        part["header_confidence"] = header_confidence
        part["record_type"] = "aggregate_timeseries"

        out_parts.append(part)

    if not out_parts:
        return pd.DataFrame(), {"reason": "numeric_columns_all_null_after_parse"}

    out = pd.concat(out_parts, ignore_index=True)

    # Canonical dimensions using common names where available
    def pick(row: pd.Series, names: list[str]) -> str | None:
        for n in names:
            if n in row and pd.notna(row[n]):
                v = norm_text(row[n])
                if v is not None:
                    return v
        return None

    out["period"] = out.apply(lambda r: pick(r, ["month", "quarter", "year", "period", "period_end"]), axis=1)
    out["geography"] = out.apply(
        lambda r: pick(
            r,
            [
                "region",
                "lpa_name",
                "local_planning_authority",
                "authority",
                "geography",
            ],
        ),
        axis=1,
    )
    out["geography_code"] = out.apply(
        lambda r: pick(r, ["ons_code", "ons_lpa_code", "lpa_code", "geography_code"]),
        axis=1,
    )
    out["appeal_type"] = out.apply(lambda r: pick(r, ["appeal_type", "casework_type", "type_of_casework"]), axis=1)
    out["procedure"] = out.apply(lambda r: pick(r, ["procedure"]), axis=1)
    out["stage"] = out.apply(lambda r: pick(r, ["stage", "stages"]), axis=1)
    out["service_standard"] = out.apply(
        lambda r: pick(r, ["service_standard", "gov_uk_timeliness", "timeliness"]),
        axis=1,
    )

    # Common metric breakouts
    metric_lower = out["metric_name"].str.lower()
    out["volume_received"] = out["metric_value"].where(metric_lower.str.contains("received"), None)
    out["volume_decided"] = out["metric_value"].where(metric_lower.str.contains("decided"), None)
    out["volume_allowed"] = out["metric_value"].where(metric_lower.str.contains("allowed"), None)
    out["allow_rate"] = out["metric_value"].where(
        metric_lower.str.contains("allow") & metric_lower.str.contains("percent|rate"),
        None,
    )
    out["timeliness_days"] = out["metric_value"].where(metric_lower.str.contains("days"), None)
    out["timeliness_percent"] = out["metric_value"].where(metric_lower.str.contains("percent|%"), None)
    out["metric_unit"] = out["metric_name"].map(
        lambda m: "percent"
        if re.search(r"percent|%", str(m).lower())
        else ("days" if "days" in str(m).lower() else "count")
    )
    out["period_type"] = out["period"].map(
        lambda p: "quarter"
        if p and re.search(r"\bq[1-4]\b", p.lower())
        else ("year" if p and re.fullmatch(r"\d{4}", p) else None)
    )

    canonical = [
        "period",
        "period_type",
        "geography",
        "geography_code",
        "casework_type",
        "appeal_type",
        "procedure",
        "stage",
        "service_standard",
        "metric_name",
        "metric_value",
        "metric_unit",
        "volume_received",
        "volume_decided",
        "volume_allowed",
        "allow_rate",
        "timeliness_days",
        "timeliness_percent",
        "source_file",
        "sheet_name",
        "header_confidence",
        "record_type",
    ]
    for c in canonical:
        if c not in out.columns:
            out[c] = None

    out = out[canonical].copy()
    return out, {
        "reason": "ok",
        "numeric_cols": len(numeric_cols),
        "dimension_cols": len(dimension_cols),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build curated PINS case + aggregate parquet tables from typed sheets."
    )
    parser.add_argument(
        "--classification-csv",
        default="_local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/curation_plan/sheet_classification.csv",
    )
    parser.add_argument(
        "--schema-map-yaml",
        default="_local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/curation_plan/schema_map.yaml",
    )
    parser.add_argument(
        "--output-dir",
        default="_local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/curated",
    )
    args = parser.parse_args()

    cls = pd.read_csv(Path(args.classification_csv).resolve())
    cls = cls[cls["include_in_curated_extract"].eq(True)].copy()

    schema = yaml.safe_load(Path(args.schema_map_yaml).read_text(encoding="utf-8"))
    aliases = schema.get("column_aliases", {})

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    case_parts: list[pd.DataFrame] = []
    agg_parts: list[pd.DataFrame] = []
    issues: list[dict[str, object]] = []

    for _, row in cls.iterrows():
        source_file = str(row["source_file"])
        sheet_name = str(row["sheet_name"])
        classification = str(row["classification"])
        header_confidence = float(row["header_confidence"])
        parquet_path = Path(str(row["output_parquet"]))
        if not parquet_path.exists():
            issues.append(
                {
                    "source_file": source_file,
                    "sheet_name": sheet_name,
                    "classification": classification,
                    "issue": "missing_typed_parquet",
                    "detail": str(parquet_path),
                }
            )
            continue

        df = pd.read_parquet(parquet_path)
        if df.empty:
            issues.append(
                {
                    "source_file": source_file,
                    "sheet_name": sheet_name,
                    "classification": classification,
                    "issue": "empty_typed_parquet",
                    "detail": "",
                }
            )
            continue

        if classification == "case_level":
            part = extract_case_table(df, aliases, source_file, sheet_name, header_confidence)
            if part.empty:
                issues.append(
                    {
                        "source_file": source_file,
                        "sheet_name": sheet_name,
                        "classification": classification,
                        "issue": "case_extract_empty",
                        "detail": "",
                    }
                )
            else:
                case_parts.append(part)
            continue

        if classification == "aggregate_timeseries":
            part, meta = build_aggregate_rows(df, source_file, sheet_name, header_confidence)
            if part.empty:
                issues.append(
                    {
                        "source_file": source_file,
                        "sheet_name": sheet_name,
                        "classification": classification,
                        "issue": "aggregate_extract_empty",
                        "detail": meta.get("reason", ""),
                    }
                )
            else:
                agg_parts.append(part)
            continue

    case_df = pd.concat(case_parts, ignore_index=True) if case_parts else pd.DataFrame()
    agg_df = pd.concat(agg_parts, ignore_index=True) if agg_parts else pd.DataFrame()

    if not case_df.empty:
        case_df.to_parquet(output_dir / "fact_case_appeals.parquet", index=False, engine="pyarrow")
    if not agg_df.empty:
        agg_df.to_parquet(output_dir / "fact_appeals_aggregate.parquet", index=False, engine="pyarrow")

    issues_df = pd.DataFrame(issues)
    if issues_df.empty:
        issues_df = pd.DataFrame(columns=["source_file", "sheet_name", "classification", "issue", "detail"])
    issues_df.to_csv(output_dir / "curation_issues.csv", index=False)

    summary = {
        "included_sheets": int(len(cls)),
        "case_rows": int(len(case_df)),
        "aggregate_rows": int(len(agg_df)),
        "issues_count": int(len(issues_df)),
        "outputs": {
            "fact_case_appeals_parquet": str(output_dir / "fact_case_appeals.parquet"),
            "fact_appeals_aggregate_parquet": str(output_dir / "fact_appeals_aggregate.parquet"),
            "issues_csv": str(output_dir / "curation_issues.csv"),
        },
    }
    (output_dir / "README_curated.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"OUTPUT_DIR={output_dir}")
    print(f"INCLUDED_SHEETS={len(cls)}")
    print(f"CASE_ROWS={len(case_df)}")
    print(f"AGGREGATE_ROWS={len(agg_df)}")
    print(f"ISSUES={len(issues_df)}")


if __name__ == "__main__":
    main()
