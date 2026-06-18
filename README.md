# UK Planning Application Scraper

Scrapes UK planning application metadata from [PlanIt.org.uk](https://www.planit.org.uk), then fetches document listings and files from council portals (Idox, Agile, Publisher, Ocella).

Built for Nesta research into heat pump planning applications, sound assessments, and planning appeals.

## Setup

```bash
uv sync --extra dev
uv run pre-commit install
```

The `dev` extra includes the `pins` dependencies (pandas, pyarrow, openpyxl)
needed by the Planning Inspectorate appeals pipeline under `scripts/pins/`. To
install just those at runtime without the dev tooling, use `uv sync --extra pins`.

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
  download_documents_idox.py            # Download document zips from Idox
  extract_decision_texts.py             # Text corpus for priority docs (decision notices, reports)
  extract_full_corpus_texts.py          # PDF text extraction across the whole document corpus
  extract_nonpdf_texts.py               # Word/RTF/HTML/email text (+ docx/converter companions)
  llm/                                  # LLM relevance classification + decision-schema extraction
    sample_100_apps.py                  #   Stratified app sample for LLM runs
    classify_sample_100.py              #   3-class heat-pump relevance classifier
    build_staging.py                    #   Build per-app staging (selection.json + texts/)
    extract_schema_v1.py                #   64-field decision-schema extraction
    verify_quotes.py                    #   Ground verbatim-quote fields against source texts
    check_consistency.py                #   Cross-field consistency checks
    build_schema_tsv.py                 #   Reviewer-facing schema TSV
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
uv run python scripts/download_documents_idox.py --limit 50

# Optional: sync to rclone remote as you go
SYNC_REMOTE="myremote:path/to/docs/" SYNC_CLEAR=1 uv run python scripts/download_documents_idox.py
```

Extract machine-readable text for decision notices, officer/delegated reports,
and noise/acoustic documents:

```bash
uv run --with pymupdf python scripts/extract_decision_texts.py
```

To enable OCR/table rescue for scan-heavy or structured PDFs, add a fallback
extractor:

```bash
uv run --with pymupdf --with docling python scripts/extract_decision_texts.py \
  --rescue-extractor docling
```

Run PDF text extraction across the whole document corpus, then the non-PDF
companion passes (Word/RTF/HTML/email, plus binary Office via external
converters), to build the `full_corpus_texts/summary.csv` manifest the LLM
pipeline reads:

```bash
uv run --with pymupdf python scripts/extract_full_corpus_texts.py
uv run python scripts/extract_nonpdf_texts.py
```

Extract a structured 64-field decision schema with an LLM. This stage reads a
stratified sample, builds a per-app staging directory of selected document
texts, calls the model once per app, then validates the output. It needs an
`OPENAI_API_KEY` (read from the environment or a local `.env`):

```bash
export OPENAI_API_KEY=sk-...                       # or put it in .env
uv run python scripts/llm/sample_100_apps.py       # pick the stratified sample
uv run python scripts/llm/build_staging.py         # selection.json + texts/
uv run --with openai --with python-dotenv python scripts/llm/extract_schema_v1.py
uv run python scripts/llm/verify_quotes.py         # ground verbatim-quote fields
uv run python scripts/llm/check_consistency.py     # cross-field consistency checks
```

To enable rclone sync, install [rclone](https://rclone.org/install/) and configure a remote with `rclone config`. Then set `SYNC_REMOTE` to your remote path to automatically sync downloaded files every 50 apps (configurable via `SYNC_EVERY`).

Data is stored in SQLite databases under `_local/workstreams/`. These are created automatically on first run.

For local exploration of the canonical VPS `ashp.db`:

```bash
uv run python scripts/open_ashp_progress_dashboard.py
```

The launcher creates a consistent SQLite snapshot on the VPS first, then copies it locally to avoid corrupt reads from the live WAL-mode database.

## Authority mapping

Portal-family classification uses `data/buildwithtract_authority_mapping.csv`, derived from [buildwithtract/planning-applications](https://github.com/buildwithtract/planning-applications) (MIT licensed).

## Tests

```bash
uv run pytest
```

## License

MIT. See [LICENSE](LICENSE).
