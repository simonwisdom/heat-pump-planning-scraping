"""Portal-type classification for planning applications.

Two layers of classification:

1. Authority lookup against the buildwithtract CSV (curated, per-authority).
2. URL pattern match against ``documentation_url`` (empirical, per-record).

Used at scrape time to populate ``applications.portal_type``. The combined
``classify_portal_type`` prefers the authority verdict when it names a known
portal family, and falls back to URL matching when the authority is unknown
or maps to a generic ``Custom`` ("other") entry.
"""

from __future__ import annotations

import csv
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

_AUTHORITY_ALIASES: dict[str, tuple[str, ...]] = {
    # PlanIt's area_name uses combined/regional labels (e.g. "South West Devon",
    # "Westmorland and Furness (Eden and South Lakeland)") whereas the buildwithtract
    # CSV is keyed on the underlying single-tier councils. Map combined → component(s).
    "south west devon": ("south hams", "west devon"),
    "kensington": ("kensington and chelsea",),
    "glamorgan": ("vale of glamorgan",),
    "westmorland and furness": (
        "westmorland and furness (eden and south lakeland)",
        "westmorland and furness (barrow)",
    ),
}

# Per-authority CSV mapping: ``portal_family`` value → portal_type label we use.
_PORTAL_FAMILY_MAP: dict[str, str] = {
    "Idox": "idox",
    "Northgate": "northgate",
    "Agile": "agile",
    "SmartAdmin": "smartadmin",
    "Arcus": "arcus",
    "NIPP": "nipp",
}

# URL signatures keyed to the portal_type they imply.
#
# Order matters: more specific patterns must come before more permissive ones.
# In particular `publisher` precedes `idox` because the RBKC publisher host
# also contains "idox" in its hostname; `necs_assure` precedes `northgate`
# because Hyndburn serves the NECS Assure UI from a host path that includes
# `/Northgate/`.
_URL_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    # --- portals we have downloaders for ---
    ("publisher", re.compile(r"/publisher/mvc/", re.IGNORECASE)),
    ("necs_assure", re.compile(r"/(?:LP)?Assure/ES/Presentation/Planning/", re.IGNORECASE)),
    ("necs_assure", re.compile(r"/NECSWS/ES/Presentation/Planning/", re.IGNORECASE)),
    ("necs_assure", re.compile(r"/Northgate/ES/Presentation/Planning/", re.IGNORECASE)),
    ("northgate", re.compile(r"/PublicAccess_LIVE/", re.IGNORECASE)),
    ("northgate", re.compile(r"/AniteIM\.WebSearch/", re.IGNORECASE)),
    ("northgate", re.compile(r"ExternalEntryPoint\.aspx", re.IGNORECASE)),
    ("northgate", re.compile(r"/HPRMWebDrawer/", re.IGNORECASE)),
    ("northgate", re.compile(r"/CMWebDrawer/", re.IGNORECASE)),
    ("northgate", re.compile(r"/northgate/documentexplorer/", re.IGNORECASE)),
    ("idox", re.compile(r"/online-applications/", re.IGNORECASE)),
    # --- families with no downloader yet (label only, for inventory) ---
    ("agile", re.compile(r"planning\.agileapplications\.co\.uk/", re.IGNORECASE)),
    ("arcus", re.compile(r"/pr/s/register-view\?[^ ]*c__r=Arcus_BE", re.IGNORECASE)),
    ("planning_register", re.compile(r"planning-register\.co\.uk/", re.IGNORECASE)),
    ("planning_docs", re.compile(r"/planning/planning-documents\?", re.IGNORECASE)),
    ("planning_docs", re.compile(r"/planning/documents\?", re.IGNORECASE)),
    ("planning_docs", re.compile(r"/planningdocuments\?", re.IGNORECASE)),
    ("planning_docs", re.compile(r"/forms/planning/planning-documents", re.IGNORECASE)),
    ("guernsey_direct", re.compile(r"buildingexplorer\.gov\.gg/Northgate/Images/", re.IGNORECASE)),
    ("shale_dialog", re.compile(r"shale\.dialog\.DIALOG_NAME=gfplanning", re.IGNORECASE)),
    (
        "msp_idox",
        re.compile(r"/planning/search-applications#VIEW\?RefType=PLANNINGCASE", re.IGNORECASE),
    ),
    ("planportal", re.compile(r"\.planportal\.co\.uk/", re.IGNORECASE)),
    ("eocella", re.compile(r"tpKey=eOcella", re.IGNORECASE)),
    (
        "planningexplorer_mvc",
        re.compile(r"/PlanningExplorerMVC?/Home/ApplicationDetails", re.IGNORECASE),
    ),
    (
        "planningexplorer_mvc",
        re.compile(r"planningexplorer\.[^/]+/Home/ApplicationDetails", re.IGNORECASE),
    ),
    ("unidoc", re.compile(r"/UniDoc/Document/Search/", re.IGNORECASE)),
    ("eplanningviewer", re.compile(r"/eplanningviewer/", re.IGNORECASE)),
    ("aifusion", re.compile(r"aifusion\.io/", re.IGNORECASE)),
    ("aifusion", re.compile(r"/publicportalviewer/", re.IGNORECASE)),
    ("civica_cx", re.compile(r"civicacx\.co\.uk/", re.IGNORECASE)),
    ("oracle_ords", re.compile(r"/ords/[^/]+/f\?p=Planning", re.IGNORECASE)),
    ("liverpool_doc_explorer", re.compile(r"/DocumentExplorer/Application/folderview", re.IGNORECASE)),
    ("bathnes_custom", re.compile(r"bathnes\.gov\.uk/planningdocuments=", re.IGNORECASE)),
    ("ipswich_custom", re.compile(r"ppc\.ipswich\.gov\.uk/xappndocs", re.IGNORECASE)),
    (
        "jersey_custom",
        re.compile(r"gov\.je/citizen/Planning/Pages/PlanningApplicationDocuments", re.IGNORECASE),
    ),
)

# Portal verdicts that should be treated as "no useful classification" and
# overridden by a URL match if one is available.
_VAGUE_VERDICTS = frozenset({"unknown", "other"})


def load_authority_portal_types(csv_path: Path) -> dict[str, str]:
    """Load the per-authority portal mapping from the buildwithtract CSV."""
    if not csv_path.exists():
        logger.warning("Authority CSV not found: %s", csv_path)
        return {}

    portal_types: dict[str, str] = {}
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            name = row.get("authority_name", "").strip()
            family = row.get("portal_family", "").strip()
            if not name:
                continue
            portal_types[name.lower()] = _PORTAL_FAMILY_MAP.get(family, "other")

    logger.info("Loaded portal types for %s authorities", len(portal_types))
    return portal_types


def classify_authority(authority_name: str | None, portal_types: dict[str, str]) -> str:
    """Classify an authority by fuzzy match against the curated CSV."""
    if not authority_name:
        return "unknown"

    clean = authority_name.lower().strip()
    if clean in portal_types:
        return portal_types[clean]

    for alias in _AUTHORITY_ALIASES.get(clean, ()):
        if alias in portal_types:
            return portal_types[alias]

    for suffix in (" council", " borough", " district", " city"):
        trimmed = clean.replace(suffix, "").strip()
        if trimmed in portal_types:
            return portal_types[trimmed]

    # UKPlanning authority names are CamelCase (no spaces/hyphens/apostrophes).
    camel = clean.replace(" ", "").replace("-", "").replace("'", "")
    if camel in portal_types:
        return portal_types[camel]

    return "unknown"


def classify_url(url: str | None) -> str | None:
    """Match a documentation URL against known portal signatures."""
    if not url:
        return None
    for label, pattern in _URL_PATTERNS:
        if pattern.search(url):
            return label
    return None


def classify_portal_type(
    authority_name: str | None,
    documentation_url: str | None,
    portal_types: dict[str, str],
) -> str:
    """Combined classifier: authority lookup, with URL fallback for vague verdicts.

    The authority CSV is curated and trusted when it names a specific portal
    family. When the authority is unknown to the CSV, or maps to the generic
    ``Custom`` bucket (returned as ``"other"``), the documentation URL is
    matched against known signatures to recover a more specific label.
    """
    authority_verdict = classify_authority(authority_name, portal_types)
    if authority_verdict not in _VAGUE_VERDICTS:
        return authority_verdict

    url_verdict = classify_url(documentation_url)
    if url_verdict is not None:
        return url_verdict

    return authority_verdict
