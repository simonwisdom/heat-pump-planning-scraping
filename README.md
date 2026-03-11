# Heat Pump Planning Scraping

Minimal extraction code for pulling heat-pump planning applications from `PlanIt.org.uk`.

## Included

- `workstreams/01_heat_pump_applications/scripts/01_scrape_applications.py`
- `scripts/02_summarise_document_backends.py`
- `scripts/03_fetch_document_listing.py`
- `src/config.py`
- `src/db.py`
- `src/planit_client.py`
- `src/idox_scraper.py`
- `src/agile_scraper.py`
- `src/publisher_scraper.py`
- `data/buildwithtract_authority_mapping.csv`
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

## Follow-on analysis

Summarise document backend patterns already visible in the local database:

```bash
python3 scripts/02_summarise_document_backends.py
```

Fetch a live document listing from a representative backend:

```bash
python3 scripts/03_fetch_document_listing.py --family idox --docs-url 'https://planning.cornwall.gov.uk/online-applications/applicationDetails.do?activeTab=documents&keyVal=R5GHN2FGMTR00'
```

## Authority Mapping

Portal-family enrichment uses `data/buildwithtract_authority_mapping.csv`, derived from:

<https://github.com/buildwithtract/planning-applications>

That upstream project is licensed under MIT. This repository includes a simplified authority-to-portal mapping for public use.
