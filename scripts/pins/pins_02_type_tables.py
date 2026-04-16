#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_") or "unknown"


def cell_to_text(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def normalize_col_name(value: str | None, idx: int) -> str:
    if value is None:
        return f"col_{idx + 1:03d}"
    slug = slugify(value)
    if slug in {"", "unknown", "nan"}:
        return f"col_{idx + 1:03d}"
    return slug


def choose_header_row(raw: pd.DataFrame) -> tuple[int, float]:
    scan_limit = min(len(raw), 40)
    best_idx = 0
    best_score = float("-inf")
    scores: list[tuple[int, float]] = []

    for i in range(scan_limit):
        row = raw.iloc[i]
        vals = [cell_to_text(v) for v in row.tolist()]
        non_empty = [v for v in vals if v is not None]
        if not non_empty:
            score = -1000.0
            scores.append((i, score))
            continue

        unique_ratio = len(set(non_empty)) / len(non_empty)
        text_ratio = sum(any(c.isalpha() for c in v) for v in non_empty) / len(non_empty)
        width_score = len(non_empty)
        # Favor broad textual rows likely to be headers.
        score = (width_score * 1.6) + (unique_ratio * 3.0) + (text_ratio * 2.5)
        # Mild preference for earlier rows.
        score -= i * 0.08
        scores.append((i, score))
        if score > best_score:
            best_idx, best_score = i, score

    ranked = sorted(scores, key=lambda x: x[1], reverse=True)
    if len(ranked) == 1:
        confidence = 1.0
    else:
        top = ranked[0][1]
        second = ranked[1][1]
        confidence = max(0.0, min(1.0, (top - second) / (abs(top) + 1e-6)))
    return best_idx, confidence


def dedupe_names(names: list[str]) -> list[str]:
    used: dict[str, int] = {}
    out: list[str] = []
    for n in names:
        count = used.get(n, 0)
        if count == 0:
            out.append(n)
        else:
            out.append(f"{n}_{count + 1}")
        used[n] = count + 1
    return out


def build_typed_tables(input_dir: Path, output_dir: Path) -> dict[str, int]:
    xlsx_files = sorted(input_dir.glob("*.xlsx"))
    output_dir.mkdir(parents=True, exist_ok=True)
    tables_dir = output_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    dictionary_rows: list[dict[str, object]] = []
    total_rows = 0
    sheets_written = 0

    for workbook_path in xlsx_files:
        xls = pd.ExcelFile(workbook_path, engine="openpyxl")
        source_slug = slugify(workbook_path.stem)

        for sheet_name in xls.sheet_names:
            raw = pd.read_excel(
                workbook_path,
                sheet_name=sheet_name,
                header=None,
                dtype=object,
                engine="openpyxl",
            )
            raw = raw.dropna(how="all").dropna(axis=1, how="all")
            if raw.empty:
                continue

            header_idx, confidence = choose_header_row(raw)
            header_vals = [cell_to_text(v) for v in raw.iloc[header_idx].tolist()]
            col_names = [normalize_col_name(v, i) for i, v in enumerate(header_vals)]
            col_names = dedupe_names(col_names)

            data = raw.iloc[header_idx + 1 :].copy()
            if data.empty:
                continue
            data.columns = col_names
            data = data.reset_index(drop=True)

            # Keep rows that have at least one non-empty cell across typed columns.
            typed_cols = list(data.columns)
            mask = ~data.isna().all(axis=1)
            data = data.loc[mask].copy()
            if data.empty:
                continue

            # Cast to strings for robust downstream joins across heterogeneous sheets.
            for c in typed_cols:
                data[c] = data[c].map(cell_to_text)

            data.insert(0, "source_file", workbook_path.name)
            data.insert(1, "source_stem", workbook_path.stem)
            data.insert(2, "sheet_name", sheet_name)
            data.insert(3, "sheet_slug", slugify(sheet_name))
            data.insert(4, "header_row_number", int(header_idx) + 1)
            data.insert(5, "header_confidence", round(float(confidence), 6))
            data.insert(6, "typed_row_number", range(1, len(data) + 1))

            sheet_slug = slugify(sheet_name)
            table_dir = tables_dir / f"source_file={source_slug}" / f"sheet={sheet_slug}"
            table_dir.mkdir(parents=True, exist_ok=True)
            out_path = table_dir / "data.parquet"
            data.to_parquet(out_path, index=False, engine="pyarrow")

            sheets_written += 1
            total_rows += len(data)
            dictionary_rows.append(
                {
                    "source_file": workbook_path.name,
                    "source_stem": workbook_path.stem,
                    "sheet_name": sheet_name,
                    "sheet_slug": sheet_slug,
                    "header_row_number": int(header_idx) + 1,
                    "header_confidence": round(float(confidence), 6),
                    "typed_columns": json.dumps(typed_cols, ensure_ascii=True),
                    "typed_column_count": len(typed_cols),
                    "rows_written": len(data),
                    "output_parquet": str(out_path),
                }
            )

    dictionary = pd.DataFrame.from_records(dictionary_rows)
    dictionary = dictionary.sort_values(by=["source_file", "sheet_name"], kind="stable").reset_index(drop=True)
    dictionary.to_csv(output_dir / "typed_data_dictionary.csv", index=False)
    dictionary.to_parquet(output_dir / "typed_data_dictionary.parquet", index=False, engine="pyarrow")

    summary = {
        "input_directory": str(input_dir),
        "output_directory": str(output_dir),
        "xlsx_files_found": len(xlsx_files),
        "sheets_written": sheets_written,
        "typed_rows_written": total_rows,
        "typed_dataset_path": str(tables_dir),
    }
    (output_dir / "README_typed.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return {
        "xlsx_files": len(xlsx_files),
        "sheets_written": sheets_written,
        "rows_written": total_rows,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build typed Parquet tables from Planning Inspectorate XLSX files.")
    parser.add_argument(
        "--input-dir",
        default="_local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/files",
        help="Directory with XLSX files.",
    )
    parser.add_argument(
        "--output-dir",
        default="_local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/parquet_typed",
        help="Directory for typed Parquet outputs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stats = build_typed_tables(Path(args.input_dir).resolve(), Path(args.output_dir).resolve())
    print(f"XLSX_FILES={stats['xlsx_files']}")
    print(f"SHEETS_WRITTEN={stats['sheets_written']}")
    print(f"TYPED_ROWS_WRITTEN={stats['rows_written']}")
    print(f"OUTPUT_DIR={Path(args.output_dir).resolve()}")


if __name__ == "__main__":
    main()
