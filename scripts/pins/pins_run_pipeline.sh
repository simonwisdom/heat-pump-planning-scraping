#!/usr/bin/env bash
set -euo pipefail

# Reproducible pipeline for Planning Inspectorate bulk files already stored in:
# _local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/files
#
# Run from repository root:
#   bash scripts/pins/pins_run_pipeline.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

echo "[1/5] Build raw normalized parquet layer..."
uv run --with pandas --with openpyxl --with pyarrow \
  python scripts/pins/pins_01_parse_xlsx.py \
  --input-dir _local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/files \
  --output-dir _local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/parquet

echo "[2/5] Build typed parquet layer (header inference)..."
uv run --with pandas --with openpyxl --with pyarrow \
  python scripts/pins/pins_02_type_tables.py \
  --input-dir _local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/files \
  --output-dir _local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/parquet_typed

echo "[3/5] Generate curation plan artifacts (classification + schema + rules)..."
python scripts/pins/pins_03_generate_curation_plan.py \
  --typed-dictionary _local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/parquet_typed/typed_data_dictionary.csv \
  --output-dir _local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/curation_plan

echo "[4/5] Build curated fact tables..."
uv run --with pandas --with pyarrow --with pyyaml \
  python scripts/pins/pins_04_build_curated_tables.py \
  --classification-csv _local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/curation_plan/sheet_classification.csv \
  --schema-map-yaml _local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/curation_plan/schema_map.yaml \
  --output-dir _local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/curated

echo "[5/5] Clean aggregate metrics (harmonized v2)..."
uv run --with pandas --with pyarrow \
  python scripts/pins/pins_05_clean_metrics.py \
  --input-parquet _local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/curated/fact_appeals_aggregate.parquet \
  --output-dir _local/workstreams/03_heat_pump_appeals_qualitative/data/raw/uk_gov_planning_inspectorate_bulk/curated

echo "Pipeline complete."
