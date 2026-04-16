#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd

MONTH_TOKEN = re.compile(
    r"^(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z_]*[\s_\-]*([0-9]{2,4})?$",
    re.IGNORECASE,
)
ISO_TS_TOKEN = re.compile(r"^\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2}$")
NUMERIC_TOKEN = re.compile(r"^\d+(\.\d+)?$")


def norm_text(v: object) -> str | None:
    if pd.isna(v):
        return None
    s = str(v).strip()
    if not s:
        return None
    s = re.sub(r"\s+", " ", s)
    return s


def slug(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def parse_period_token(token: str) -> str | None:
    t = token.strip().lower().replace("-", "_").replace(" ", "_")
    if ISO_TS_TOKEN.fullmatch(t):
        try:
            dt = pd.to_datetime(t, format="%Y_%m_%d_%H_%M_%S", errors="coerce")
            if pd.isna(dt):
                return None
            return dt.strftime("%Y-%m-01")
        except Exception:
            return None
    m = MONTH_TOKEN.fullmatch(t)
    if not m:
        return None
    mon, yy = m.group(1), m.group(2)
    month_map = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "sept": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }
    month = month_map[mon]
    if yy is None:
        return None
    if len(yy) == 2:
        year = 2000 + int(yy)
    else:
        year = int(yy)
    if year < 1990 or year > 2100:
        return None
    return f"{year:04d}-{month:02d}-01"


def clean_metric_name(raw: str) -> str | None:
    s = slug(raw)
    if not s:
        return None
    # Drop obvious non-metric junk tokens
    if NUMERIC_TOKEN.fullmatch(s):
        return None
    if ISO_TS_TOKEN.fullmatch(s):
        return None
    if MONTH_TOKEN.fullmatch(s):
        return None

    replacements = {
        "s78_planning_appeals_number_decided": "s78_decided_count",
        "s78_planning_appeals_number_allowed": "s78_allowed_count",
        "householder_appeals_number_decided": "householder_decided_count",
        "householder_appeals_number_allowed": "householder_allowed_count",
        "s174_enforcement_notice_appeals_number_decided": "s174_decided_count",
        "s174_enforcement_notice_appeals_quashed_or_granted": "s174_allowed_count",
        "s174_enforcement_notice_appeals_notice_upheld_or_varied": "s174_upheld_or_varied_count",
        "received": "received_count",
        "decided": "decided_count",
        "allowed": "allowed_count",
        "total": "total_count",
        "written_representations": "written_representations_count",
        "hearings": "hearings_count",
        "inquiries": "inquiries_count",
        "all": "all_count",
    }
    return replacements.get(s, s)


def metric_family(metric_name_clean: str) -> str:
    n = metric_name_clean
    if any(k in n for k in ["received", "decided", "allowed", "count", "total"]):
        return "volume"
    if any(k in n for k in ["percent", "rate", "share"]):
        return "rate_percent"
    if "day" in n:
        return "timeliness_days"
    return "other"


def choose_metric_context(row: pd.Series) -> str | None:
    for c in ["appeal_type", "procedure", "stage", "geography"]:
        v = norm_text(row.get(c))
        if v:
            return slug(v)
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean and harmonize aggregate metric names/periods.")
    parser.add_argument(
        "--input-parquet",
        default="_local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/curated/fact_appeals_aggregate.parquet",
    )
    parser.add_argument(
        "--output-dir",
        default="_local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/curated",
    )
    args = parser.parse_args()

    input_path = Path(args.input_parquet).resolve()
    out_dir = Path(args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(input_path)
    df = df.copy()
    df["metric_name_raw"] = df["metric_name"].map(lambda x: norm_text(x) or "")
    df["period_raw"] = df["period"].map(norm_text)

    # Recover period when it was encoded as metric name.
    recovered_period = df["metric_name_raw"].map(parse_period_token)
    df["period_clean"] = df["period_raw"]
    period_missing = df["period_clean"].isna()
    df.loc[period_missing, "period_clean"] = recovered_period[period_missing]

    # Clean metric names and rebuild when raw token was period-like.
    df["metric_name_clean"] = df["metric_name_raw"].map(clean_metric_name)
    period_like_metric = df["metric_name_raw"].map(lambda x: parse_period_token(x) is not None)
    needs_rebuild = df["metric_name_clean"].isna() & period_like_metric
    df.loc[needs_rebuild, "metric_name_clean"] = df.loc[needs_rebuild].apply(lambda r: choose_metric_context(r), axis=1)

    # For still-missing metric names, try context fallback; if none, drop.
    still_missing = df["metric_name_clean"].isna()
    df.loc[still_missing, "metric_name_clean"] = df.loc[still_missing].apply(lambda r: choose_metric_context(r), axis=1)

    df["metric_family"] = df["metric_name_clean"].fillna("").map(metric_family)
    df["period_type_clean"] = df["period_clean"].map(
        lambda p: "month" if isinstance(p, str) and re.fullmatch(r"\d{4}-\d{2}-01", p) else None
    )
    df["quality_flag"] = "ok"

    drop_reason = pd.Series(index=df.index, dtype=object)
    drop_reason[df["metric_name_clean"].isna()] = "missing_metric_name_clean"
    drop_reason[df["metric_value"].isna()] = "missing_metric_value"
    drop_reason[df["metric_name_raw"].str.fullmatch(NUMERIC_TOKEN, na=False)] = "numeric_metric_token"

    # Keep mostly valid rows.
    keep_mask = drop_reason.isna()
    cleaned = df.loc[keep_mask].copy()
    dropped = df.loc[~keep_mask].copy()
    dropped["drop_reason"] = drop_reason.loc[~keep_mask]

    cleaned = cleaned.drop_duplicates(
        subset=[
            "source_file",
            "sheet_name",
            "period_clean",
            "metric_name_clean",
            "metric_value",
            "geography",
            "appeal_type",
            "procedure",
        ],
        keep="first",
    )

    cleaned["metric_name"] = cleaned["metric_name_clean"]
    cleaned["period"] = cleaned["period_clean"]
    cleaned["period_type"] = cleaned["period_type_clean"].combine_first(cleaned["period_type"])

    out_cols = [
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
        "metric_family",
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
        "metric_name_raw",
        "period_raw",
        "quality_flag",
    ]
    for c in out_cols:
        if c not in cleaned.columns:
            cleaned[c] = None
    cleaned = cleaned[out_cols]

    cleaned_path = out_dir / "fact_appeals_aggregate_clean.parquet"
    cleaned.to_parquet(cleaned_path, index=False, engine="pyarrow")

    dropped_path = out_dir / "fact_appeals_aggregate_dropped.csv"
    dropped[
        [
            "source_file",
            "sheet_name",
            "metric_name_raw",
            "period_raw",
            "metric_value",
            "drop_reason",
        ]
    ].to_csv(dropped_path, index=False)

    mapping = (
        cleaned[["metric_name_raw", "metric_name"]]
        .drop_duplicates()
        .sort_values(["metric_name_raw", "metric_name"], kind="stable")
        .reset_index(drop=True)
    )
    mapping_path = out_dir / "metric_name_mapping.csv"
    mapping.to_csv(mapping_path, index=False)

    summary = {
        "input_rows": int(len(df)),
        "cleaned_rows": int(len(cleaned)),
        "dropped_rows": int(len(dropped)),
        "unique_metric_name_raw": int(df["metric_name_raw"].nunique(dropna=True)),
        "unique_metric_name_clean": int(cleaned["metric_name"].nunique(dropna=True)),
        "period_non_null_before": int(df["period_raw"].notna().sum()),
        "period_non_null_after": int(cleaned["period"].notna().sum()),
        "outputs": {
            "cleaned_parquet": str(cleaned_path),
            "dropped_csv": str(dropped_path),
            "metric_mapping_csv": str(mapping_path),
        },
    }
    (out_dir / "README_aggregate_clean.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"INPUT_ROWS={len(df)}")
    print(f"CLEANED_ROWS={len(cleaned)}")
    print(f"DROPPED_ROWS={len(dropped)}")
    print(f"UNIQUE_METRIC_RAW={df['metric_name_raw'].nunique(dropna=True)}")
    print(f"UNIQUE_METRIC_CLEAN={cleaned['metric_name'].nunique(dropna=True)}")
    print(f"PERIOD_NONNULL_BEFORE={df['period_raw'].notna().sum()}")
    print(f"PERIOD_NONNULL_AFTER={cleaned['period'].notna().sum()}")


if __name__ == "__main__":
    main()
