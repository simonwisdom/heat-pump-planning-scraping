"""Configuration constants for the ASHP planning scraper."""

from pathlib import Path

# Paths (canonical workstream locations)
PROJECT_ROOT = Path(__file__).parent.parent
LOCAL_DIR = PROJECT_ROOT / "_local"
WS1_ROOT = LOCAL_DIR / "workstreams" / "01_heat_pump_applications"
DATA_DIR = WS1_ROOT / "data" / "raw"
DB_PATH = DATA_DIR / "ashp.db"
PDF_DIR = WS1_ROOT / "data" / "intermediate" / "pdfs"

# PlanIt API
PLANIT_BASE_URL = "https://www.planit.org.uk/api"
PLANIT_PAGE_SIZE = 300
PLANIT_MAX_RESULTS = 5000
PLANIT_RATE_LIMIT_REQUESTS = 3
PLANIT_RATE_LIMIT_COOLDOWN_BASE = 90  # seconds
PLANIT_RATE_LIMIT_COOLDOWN_MAX = 600
PLANIT_MIN_REQUEST_GAP = 10.0  # seconds between every request (no bursts)

# Search terms for ASHP applications
ASHP_SEARCH_TERMS = [
    '"air source heat pump"',
    "ASHP",
]

# Year range for historical scraping
SCRAPE_YEAR_START = 2015
SCRAPE_YEAR_END = 2026

# Idox scraping
IDOX_RATE_LIMIT_PER_DOMAIN = 2.0  # seconds between requests to same domain
IDOX_MAX_CONCURRENT_DOMAINS = 3
IDOX_USER_AGENT = "NestaPlanningResearch/1.0 (ASHP planning data research; https://www.nesta.org.uk)"

# Agile Applications API
AGILE_BASE_URL = "https://planningapi.agileapplications.co.uk"
AGILE_RATE_LIMIT_PER_DOMAIN = 0.5  # seconds between requests to Agile API host
AGILE_MAX_CONCURRENT_DOMAINS = 5
