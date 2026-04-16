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


def normalize_text(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def build_dataset(input_dir: Path, output_dir: Path) -> tuple[int, int, int]:
    xlsx_files = sorted(input_dir.glob("*.xlsx"))
    output_dir.mkdir(parents=True, exist_ok=True)

    rows_dir = output_dir / "sheet_rows"
    rows_dir.mkdir(parents=True, exist_ok=True)

    dictionary_rows: list[dict[str, object]] = []
    total_sheets = 0
    total_rows = 0

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

            records: list[dict[str, object]] = []
            for row_idx, row in raw.iterrows():
                non_empty_cells: dict[str, str] = {}
                for col_idx, cell in row.items():
                    text = normalize_text(cell)
                    if text is None:
                        continue
                    non_empty_cells[f"c{int(col_idx) + 1:03d}"] = text

                if not non_empty_cells:
                    continue

                records.append(
                    {
                        "source_file": workbook_path.name,
                        "source_stem": workbook_path.stem,
                        "sheet_name": sheet_name,
                        "sheet_slug": slugify(sheet_name),
                        "row_number": int(row_idx) + 1,
                        "non_empty_cell_count": len(non_empty_cells),
                        "row_json": json.dumps(non_empty_cells, ensure_ascii=True),
                    }
                )

            if not records:
                continue

            sheet_slug = slugify(sheet_name)
            sheet_dir = rows_dir / f"source_file={source_slug}" / f"sheet={sheet_slug}"
            sheet_dir.mkdir(parents=True, exist_ok=True)
            parquet_path = sheet_dir / "data.parquet"

            frame = pd.DataFrame.from_records(records)
            frame.to_parquet(parquet_path, index=False, engine="pyarrow")

            total_sheets += 1
            total_rows += len(frame)
            dictionary_rows.append(
                {
                    "source_file": workbook_path.name,
                    "source_stem": workbook_path.stem,
                    "sheet_name": sheet_name,
                    "sheet_slug": sheet_slug,
                    "output_parquet": str(parquet_path),
                    "rows_written": len(frame),
                    "max_non_empty_cells_per_row": int(frame["non_empty_cell_count"].max()),
                    "min_non_empty_cells_per_row": int(frame["non_empty_cell_count"].min()),
                }
            )

    dictionary_df = pd.DataFrame.from_records(dictionary_rows)
    dictionary_df = dictionary_df.sort_values(by=["source_file", "sheet_name"], kind="stable").reset_index(drop=True)
    dictionary_df.to_csv(output_dir / "data_dictionary.csv", index=False)
    dictionary_df.to_parquet(output_dir / "data_dictionary.parquet", index=False, engine="pyarrow")

    summary = {
        "input_directory": str(input_dir),
        "output_directory": str(output_dir),
        "xlsx_files_found": len(xlsx_files),
        "sheets_written": total_sheets,
        "rows_written": total_rows,
        "row_schema": [
            "source_file",
            "source_stem",
            "sheet_name",
            "sheet_slug",
            "row_number",
            "non_empty_cell_count",
            "row_json",
        ],
    }
    (output_dir / "README.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return len(xlsx_files), total_sheets, total_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Planning Inspectorate XLSX files into a normalized Parquet dataset."
    )
    parser.add_argument(
        "--input-dir",
        default="_local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/files",
        help="Directory containing XLSX files.",
    )
    parser.add_argument(
        "--output-dir",
        default="_local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/parquet",
        help="Directory where the Parquet dataset will be written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    xlsx_files, sheets, rows = build_dataset(input_dir, output_dir)
    print(f"XLSX_FILES={xlsx_files}")
    print(f"SHEETS_WRITTEN={sheets}")
    print(f"ROWS_WRITTEN={rows}")
    print(f"OUTPUT_DIR={output_dir}")


if __name__ == "__main__":
    main()
