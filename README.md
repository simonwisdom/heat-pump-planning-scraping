# UK Planning Application Scraper

Scrapes UK planning application metadata from [PlanIt.org.uk](https://www.planit.org.uk), then fetches document listings and files from council portals (Idox, Agile, Publisher, Ocella).

Built for Nesta research into heat pump planning applications, sound assessments, and planning appeals.

## Setup

```bash
uv sync
```

## Project structure

```
src/                        # Library code
  planit_client.py          #   PlanIt API client (search/metadata)
  idox_scraper.py           #   Idox portal scraper (doc listings + zip download)
  agile_scraper.py          #   Agile portal scraper
  publisher_scraper.py      #   Publisher portal scraper
  ocella_scraper.py         #   Ocella portal scraper
  db.py                     #   SQLite schema and CRUD
  config.py                 #   Paths and rate limit settings

scripts/                    # Runnable pipeline scripts
  scrape_applications_ashp.py           # Scrape ASHP apps from PlanIt
  scrape_applications_heat_pump_broad.py # Broader heat pump search
  scrape_applications_noise.py          # Noise/sound assessment apps
  scrape_document_listings.py           # Fetch doc metadata from Idox portals
  download_documents.py                 # Download document zips from Idox
  pins/                                 # Planning Inspectorate appeals pipeline
    pins_01_parse_xlsx.py               #   Raw XLSX -> parquet
    pins_02_type_tables.py              #   Typed parquet with header inference
    pins_03_generate_curation_plan.py   #   Curation plan artifacts
    pins_04_build_curated_tables.py     #   Curated fact tables
    pins_05_clean_metrics.py            #   Clean aggregate metrics
    pins_06_match_appeals.py            #   Match appeals to planning apps
    pins_run_pipeline.sh                #   Run all PINS steps

tests/                      # Test suite
data/                       # Small reference data (authority mapping)
_local/                     # gitignored: data, docs, output (created at runtime)
```

## Usage

Scrape ASHP planning applications:

```bash
uv run python scripts/scrape_applications_ashp.py
uv run python scripts/scrape_applications_ashp.py --year 2024 --dry-run
```

Fetch document listings from Idox portals:

```bash
uv run python scripts/scrape_document_listings.py
```

Download document files:

```bash
uv run python scripts/download_documents.py --limit 50
uv run python scripts/download_documents.py --gdrive-path "gdrive:planning-docs/" --clear-after-sync
```

Data is stored in SQLite databases under `_local/workstreams/`. These are created automatically on first run.

## Authority mapping

Portal-family classification uses `data/buildwithtract_authority_mapping.csv`, derived from [buildwithtract/planning-applications](https://github.com/buildwithtract/planning-applications) (MIT licensed).

## Tests

```bash
uv run pytest
```
