"""Portal-type classification for planning applications.

Two layers of classification:

1. Authority lookup against the buildwithtract CSV (curated, per-authority).
2. URL pattern match against ``documentation_url`` (empirical, per-record).

Used at scrape time to populate ``applications.portal_type``. The combined
``classify_portal_type`` treats the URL signature as empirical ground truth:
when a ``documentation_url`` matches a known portal family the URL verdict
wins, even if the authority CSV names a different family. The authority
verdict is only used when the URL yields no signature.
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
    # Broad fallback: any ``applicationDetails.do`` URL not already matched
    # above. This catches the many idox deployment variants that don't use
    # the ``/online-applications/`` path — ``/idoxpa-web/``, ``/wam/``,
    # ``/publicaccess/``, ``/newplanningaccess/``, ``/OnlinePlanning/``,
    # ``/PlanningData-live/``, ``/public-access/``, ``/eplanning/``,
    # ``/buildingstandards/``, ``/online/``. Order matters: this pattern is
    # listed after the northgate ``PublicAccess_LIVE`` rule so Northgate
    # still wins for that path.
    ("idox", re.compile(r"/applicationDetails\.do", re.IGNORECASE)),
    # --- named non-Idox families and custom routes ---
    ("agile", re.compile(r"planning\.agileapplications\.co\.uk/", re.IGNORECASE)),
    ("arcus", re.compile(r"/pr/s/register-view\?[^ ]*c__r=Arcus_BE", re.IGNORECASE)),
    ("arcus", re.compile(r"/pr\d+/s/register-view\?[^ ]*c__r=Arcus_BE", re.IGNORECASE)),
    ("planning_register", re.compile(r"planning-register\.co\.uk/", re.IGNORECASE)),
    ("civica_w2", re.compile(r"/Planning/Display/", re.IGNORECASE)),
    ("planning_docs", re.compile(r"/planning/planning-documents\?", re.IGNORECASE)),
    ("planning_docs", re.compile(r"/planning/documents\?", re.IGNORECASE)),
    ("planning_docs", re.compile(r"/planningdocuments\?", re.IGNORECASE)),
    ("planning_docs", re.compile(r"/forms/planning/planning-documents", re.IGNORECASE)),
    ("guernsey_direct", re.compile(r"buildingexplorer\.gov\.gg/Northgate/Images/", re.IGNORECASE)),
    (
        "guernsey_direct",
        re.compile(r"planningexplorer\.gov\.gg/portal/servlets/ApplicationSearchServlet", re.IGNORECASE),
    ),
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
    ("eplanningviewer", re.compile(r"/EDMSExternal/Fred/Index/", re.IGNORECASE)),
    ("aifusion", re.compile(r"aifusion\.io/", re.IGNORECASE)),
    ("aifusion", re.compile(r"/publicportalviewer/", re.IGNORECASE)),
    ("civica_cx", re.compile(r"civicacx\.co\.uk/", re.IGNORECASE)),
    ("oracle_ords", re.compile(r"/ords/[^/]+/f\?p=Planning", re.IGNORECASE)),
    ("mvm", re.compile(r"/MVM/Online/(?:DMS/)?DocumentViewer\.aspx", re.IGNORECASE)),
    ("ocella", re.compile(r"/OcellaWeb/showDocuments", re.IGNORECASE)),
    ("ocella_casetracker", re.compile(r"/casetracker/ocella_crossreference\.asp", re.IGNORECASE)),
    ("liverpool_doc_explorer", re.compile(r"/DocumentExplorer/Application/folderview", re.IGNORECASE)),
    ("bathnes_custom", re.compile(r"bathnes\.gov\.uk/planningdocuments=", re.IGNORECASE)),
    ("ipswich_custom", re.compile(r"ppc\.ipswich\.gov\.uk/xappndocs", re.IGNORECASE)),
    (
        "jersey_custom",
        re.compile(r"gov\.je/citizen/Planning/Pages/PlanningApplicationDocuments", re.IGNORECASE),
    ),
)

# Portal verdicts that should be treated as "no useful classification" so
# callers can distinguish a confident authority verdict from an empty one.
_VAGUE_VERDICTS = frozenset({"unknown", "other"})

# Hosts that have been retired in favour of a live replacement. Any
# ``documentation_url`` referencing the retired host should be rewritten
# before fetching. Used by :func:`normalise_documentation_url` and by the
# scrapers that perform the rewrite at fetch time.
_HOST_REWRITES: dict[str, str] = {
    # Neath Port Talbot moved their Oracle APEX portal off the original host.
    # The retired DNS no longer resolves; the live replacement serves the
    # exact same APEX page paths.
    "appsportal.npt.gov.uk": "appsportal2.npt.gov.uk",
    # Barnsley retired the legacy PlanningExplorerMVC host. The live
    # ``planningexplorer.barnsley.gov.uk`` exposes the same routes minus the
    # ``/PlanningExplorerMVC`` path prefix; rewriting only the host is not
    # sufficient on its own — see ``normalise_documentation_url``.
    "wwwapplications.barnsley.gov.uk": "planningexplorer.barnsley.gov.uk",
}


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


def normalise_documentation_url(url: str | None) -> str | None:
    """Rewrite ``documentation_url`` onto the current live host.

    Returns the input unchanged when no rewrite applies. Currently rewrites:

    * ``appsportal.npt.gov.uk`` → ``appsportal2.npt.gov.uk`` (Oracle ORDS)
    * ``wwwapplications.barnsley.gov.uk/PlanningExplorerMVC/...`` →
      ``planningexplorer.barnsley.gov.uk/...`` (Planning Explorer MVC)

    The scrapers also rewrite at fetch time, so this helper is primarily
    useful for one-off URL backfills (e.g. URL-recovery passes that store the
    canonical form once). Idempotent on already-normalised inputs.
    """
    if not url:
        return url

    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if not host:
        return url

    new_host = _HOST_REWRITES.get(host)
    if new_host is None:
        return url

    # Rebuild netloc preserving any user-info / port that was on the original.
    userinfo = ""
    if parts.username:
        userinfo = parts.username
        if parts.password:
            userinfo += f":{parts.password}"
        userinfo += "@"
    port_suffix = f":{parts.port}" if parts.port else ""
    netloc = f"{userinfo}{new_host}{port_suffix}"

    new_path = parts.path
    if host == "wwwapplications.barnsley.gov.uk" and new_path.startswith("/PlanningExplorerMVC"):
        new_path = new_path[len("/PlanningExplorerMVC") :] or "/"

    scheme = "https" if parts.scheme in ("", "http") else parts.scheme
    return urlunsplit((scheme, netloc, new_path, parts.query, parts.fragment))


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
    """Combined classifier: URL signature wins, authority verdict is fallback.

    The ``documentation_url`` is empirical per-record evidence of which portal
    actually serves a given app's documents; when it matches a known family
    signature that verdict wins over the curated authority CSV. This matters
    when the CSV is out of date (an authority has migrated to a different
    system, or a single authority routes some apps through a different
    portal). The authority verdict is used only when the URL is missing or
    unrecognised.
    """
    url_verdict = classify_url(documentation_url)
    if url_verdict is not None:
        return url_verdict

    return classify_authority(authority_name, portal_types)
