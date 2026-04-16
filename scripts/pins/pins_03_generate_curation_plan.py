#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import yaml


def classify_sheet(row: pd.Series) -> tuple[str, str, bool]:
    source_file = str(row["source_file"]).lower()
    sheet_name = str(row["sheet_name"]).lower()
    typed_col_count = int(row["typed_column_count"])
    rows_written = int(row["rows_written"])
    header_conf = float(row["header_confidence"])

    if "casework_database" in source_file or "older_casework" in source_file:
        if sheet_name in {"data set", "dataset"} or (typed_col_count >= 15 and rows_written >= 1000):
            return "case_level", "casework main dataset sheet", header_conf < 0.05
        return "metadata_chart", "casework workbook support/reference sheet", True

    if "table of contents" in sheet_name:
        return "exclude", "index/contents sheet", False

    if "figure" in sheet_name or "graph" in sheet_name or "virtual events" in sheet_name:
        return "metadata_chart", "chart helper/figure sheet", False

    if "annex bqr" in sheet_name or "bqr" in sheet_name:
        return "metadata_chart", "background quality report helper sheet", False

    if sheet_name.startswith("table") or "by procedure" in sheet_name or "by casework type" in sheet_name:
        return (
            "aggregate_timeseries",
            "tabular quarterly statistics",
            header_conf < 0.03,
        )

    if sheet_name.startswith("annex a") or sheet_name.startswith("annex b") or sheet_name.startswith("annex c"):
        if rows_written >= 20 and typed_col_count >= 3:
            return (
                "aggregate_timeseries",
                "annex tabular statistics",
                header_conf < 0.03,
            )
        return "metadata_chart", "annex notes/chart support", True

    if typed_col_count <= 1 and rows_written <= 30:
        return "exclude", "single-cell note/title sheet", False

    if rows_written >= 15 and typed_col_count >= 2:
        return (
            "aggregate_timeseries",
            "default statistical table classification",
            header_conf < 0.03,
        )

    return "metadata_chart", "fallback non-tabular sheet", True


def build_schema_map() -> dict:
    return {
        "version": 1,
        "tables": {
            "fact_case_appeals": {
                "grain": "one row per appeal/case in casework datasets",
                "primary_keys": ["case_number"],
                "required_columns": [
                    "case_number",
                    "decision_date",
                    "decision_outcome",
                    "lpa_name",
                ],
                "canonical_columns": [
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
                ],
            },
            "fact_appeals_aggregate": {
                "grain": "one row per period/geography/appeal_type/metric",
                "primary_keys": [
                    "period",
                    "geography",
                    "appeal_type",
                    "metric_name",
                ],
                "required_columns": ["period", "metric_name", "metric_value"],
                "canonical_columns": [
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
                ],
            },
        },
        "column_aliases": {
            "case_number": ["case_number", "appeal_reference", "case_ref", "reference"],
            "casework_type": ["type_of_casework", "casework_type"],
            "appeal_type": [
                "appeal_type_reason",
                "reason_for_the_appeal",
                "appeal_type",
            ],
            "lpa_name": ["lpa_name", "local_planning_authority", "authority"],
            "lpa_ons_code": ["ons_lpa_code", "ons_code", "lpa_code"],
            "decision_date": ["decision_date", "date_decided"],
            "decision_outcome": [
                "decision",
                "outcome",
                "appeal_outcome",
                "decision_outcome",
            ],
            "received_date": ["received_date", "date_received"],
            "valid_date": ["valid_date", "date_valid"],
            "start_date": ["start_date", "date_started"],
            "procedure": ["procedure"],
            "development_type": ["development_type", "type_detail"],
            "period": ["month", "quarter", "year", "period", "period_end"],
            "geography": ["region", "lpa_name", "authority", "geography"],
            "geography_code": ["ons_code", "ons_lpa_code", "geography_code"],
            "metric_name": ["measure", "metric", "indicator", "service"],
            "metric_value": [
                "value",
                "count",
                "total",
                "percentage",
                "percent",
                "days",
            ],
            "volume_received": [
                "received",
                "applications_received",
                "appeals_received",
            ],
            "volume_decided": ["decided", "applications_decided", "appeals_decided"],
            "volume_allowed": ["allowed", "appeals_allowed"],
            "allow_rate": ["allow_rate", "allowed_percent", "appeals_allowed_percent"],
            "timeliness_days": ["days", "avg_days", "median_days"],
            "timeliness_percent": ["within_target_percent", "service_standard_percent"],
        },
    }


def build_cleaning_rules() -> dict:
    return {
        "version": 1,
        "global": {
            "trim_whitespace": True,
            "collapse_internal_whitespace": True,
            "empty_to_null_values": ["", "-", "n/a", "na", "none", "null"],
            "lowercase_columns": True,
        },
        "dates": {
            "parse_day_first": True,
            "output_format": "YYYY-MM-DD",
            "accepted_patterns": [
                "DD/MM/YYYY",
                "D/M/YYYY",
                "YYYY-MM-DD",
                "Month YYYY",
                "Mon YYYY",
            ],
        },
        "numbers": {
            "remove_thousands_separator": True,
            "strip_percent_sign": True,
            "strip_currency_symbols": True,
            "coerce_invalid_to_null": True,
        },
        "booleans": {
            "true_values": ["yes", "y", "true", "1"],
            "false_values": ["no", "n", "false", "0"],
        },
        "decision_outcome_map": {
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
        },
        "period_rules": {
            "quarter_regex": r"q([1-4])\\s*(\\d{2,4})[-/](\\d{2,4})",
            "month_year_to_period_start": True,
        },
        "row_filters": {
            "drop_if_first_cell_startswith": [
                "source:",
                "notes:",
                "note:",
                "figure",
                "chart",
                "annex",
            ],
            "drop_if_all_numeric_false_and_cell_count_lt": 2,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate curation plan artifacts for PINS XLSX datasets.")
    parser.add_argument(
        "--typed-dictionary",
        default="_local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/parquet_typed/typed_data_dictionary.csv",
    )
    parser.add_argument(
        "--output-dir",
        default="_local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/curation_plan",
    )
    args = parser.parse_args()

    typed_path = Path(args.typed_dictionary).resolve()
    out_dir = Path(args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(typed_path)

    classes: list[dict] = []
    for _, row in df.iterrows():
        cls, reason, review = classify_sheet(row)
        classes.append(
            {
                "source_file": row["source_file"],
                "sheet_name": row["sheet_name"],
                "sheet_slug": row["sheet_slug"],
                "rows_written": int(row["rows_written"]),
                "typed_column_count": int(row["typed_column_count"]),
                "header_row_number": int(row["header_row_number"]),
                "header_confidence": float(row["header_confidence"]),
                "classification": cls,
                "include_in_curated_extract": cls in {"case_level", "aggregate_timeseries"},
                "review_needed": bool(review),
                "classification_reason": reason,
                "output_parquet": row["output_parquet"],
            }
        )

    cls_df = pd.DataFrame(classes).sort_values(by=["classification", "source_file", "sheet_name"], kind="stable")
    cls_df.to_csv(out_dir / "sheet_classification.csv", index=False)

    review_df = cls_df[cls_df["review_needed"].eq(True) | (cls_df["header_confidence"] < 0.05)].copy()
    review_df.to_csv(out_dir / "review_priority_sheets.csv", index=False)

    with open(out_dir / "schema_map.yaml", "w", encoding="utf-8") as f:
        yaml.safe_dump(build_schema_map(), f, sort_keys=False, allow_unicode=False)

    with open(out_dir / "cleaning_rules.yaml", "w", encoding="utf-8") as f:
        yaml.safe_dump(build_cleaning_rules(), f, sort_keys=False, allow_unicode=False)

    summary = {
        "typed_dictionary_input": str(typed_path),
        "sheet_count": int(len(cls_df)),
        "included_sheet_count": int(cls_df["include_in_curated_extract"].sum()),
        "classification_counts": cls_df["classification"].value_counts().to_dict(),
        "review_priority_count": int(len(review_df)),
        "files_written": [
            str(out_dir / "sheet_classification.csv"),
            str(out_dir / "review_priority_sheets.csv"),
            str(out_dir / "schema_map.yaml"),
            str(out_dir / "cleaning_rules.yaml"),
        ],
    }
    (out_dir / "README_plan.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"OUTPUT_DIR={out_dir}")
    print(f"SHEETS={len(cls_df)}")
    print(f"INCLUDED={int(cls_df['include_in_curated_extract'].sum())}")
    print(f"REVIEW_PRIORITY={len(review_df)}")


if __name__ == "__main__":
    main()
