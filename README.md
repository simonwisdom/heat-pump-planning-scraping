# Heat Pump Planning Scraping

Minimal extraction code for pulling heat-pump planning applications from `PlanIt.org.uk`.

## Included

- `workstreams/01_heat_pump_applications/scripts/01_scrape_applications.py`
- `src/config.py`
- `src/db.py`
- `src/planit_client.py`
- `pyproject.toml`

## Usage

```bash
python3 workstreams/01_heat_pump_applications/scripts/01_scrape_applications.py --help
```

Example:

```bash
python3 workstreams/01_heat_pump_applications/scripts/01_scrape_applications.py --year 2021 --search '"air source heat pump"'
```

The scraper stores results in a local SQLite database and deduplicates by `uid`.
