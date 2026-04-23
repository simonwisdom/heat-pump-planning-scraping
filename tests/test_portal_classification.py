"""Tests for src.portal_classification."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.portal_classification import (
    classify_authority,
    classify_portal_type,
    classify_url,
    load_authority_portal_types,
)


@pytest.fixture
def authority_csv(tmp_path: Path) -> Path:
    """Minimal authority CSV covering each portal_family branch and a Custom row."""
    csv_text = "\n".join(
        [
            "authority_name,portal_family,portal_url",
            "Aberdeen,Idox,https://example.com",
            "Bath,Custom,https://example.com",
            "Camden,Northgate,https://example.com",
            "DartfordBorough,SmartAdmin,https://example.com",
            "Eden,Agile,https://example.com",
            "Fareham,Arcus,https://example.com",
            "Glasgow,NIPP,https://example.com",
            "South Hams,Custom,https://southhams.planning-register.co.uk/Search/Advanced",
            "West Devon,Custom,https://westdevon.planning-register.co.uk/Search/Advanced",
        ]
    )
    path = tmp_path / "mapping.csv"
    path.write_text(csv_text + "\n")
    return path


def test_load_authority_portal_types_maps_each_family(authority_csv: Path) -> None:
    mapping = load_authority_portal_types(authority_csv)

    assert mapping["aberdeen"] == "idox"
    assert mapping["bath"] == "other"  # Custom is bucketed as "other"
    assert mapping["camden"] == "northgate"
    assert mapping["dartfordborough"] == "smartadmin"
    assert mapping["eden"] == "agile"
    assert mapping["fareham"] == "arcus"
    assert mapping["glasgow"] == "nipp"


def test_load_authority_portal_types_missing_csv_returns_empty(tmp_path: Path) -> None:
    assert load_authority_portal_types(tmp_path / "missing.csv") == {}


def test_classify_authority_direct_match(authority_csv: Path) -> None:
    mapping = load_authority_portal_types(authority_csv)
    assert classify_authority("Aberdeen", mapping) == "idox"


def test_classify_authority_strips_suffix(authority_csv: Path) -> None:
    mapping = load_authority_portal_types(authority_csv)
    # "Eden" matches after stripping " district"
    assert classify_authority("Eden District", mapping) == "agile"


def test_classify_authority_camelcase_fallback(authority_csv: Path) -> None:
    mapping = load_authority_portal_types(authority_csv)
    assert classify_authority("Dartford Borough", mapping) == "smartadmin"


def test_classify_authority_unknown_when_missing(authority_csv: Path) -> None:
    mapping = load_authority_portal_types(authority_csv)
    assert classify_authority("Atlantis", mapping) == "unknown"
    assert classify_authority(None, mapping) == "unknown"
    assert classify_authority("", mapping) == "unknown"


def test_classify_authority_alias_match(authority_csv: Path) -> None:
    mapping = load_authority_portal_types(authority_csv)
    assert classify_authority("South West Devon", mapping) == "other"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        # Existing-downloader portals
        ("https://planningsearch.rbkc.gov.uk/publisher/mvc/listDocuments?identifier=X", "publisher"),
        ("https://idox.example.gov.uk/online-applications/applicationDetails.do?keyVal=Y", "idox"),
        (
            "https://hbc-edrms.necswscloud.com/PublicAccess_LIVE/SearchResult/RunThirdPartySearch?id=1",
            "northgate",
        ),
        (
            "http://documents.runnymede.gov.uk/AniteIM.WebSearch/ExternalEntryPoint.aspx?x",
            "northgate",  # AniteIM signature wins (publisher pattern checked first)
        ),
        ("http://camdocs.camden.gov.uk/HPRMWebDrawer/PlanRec?q=foo", "northgate"),
        # NECS Assure variants
        (
            "https://planning.broxbourne.gov.uk/LPAssure/ES/Presentation/Planning/OnlinePlanning/x",
            "necs_assure",
        ),
        (
            "https://planningandbuilding.hounslow.gov.uk/NECSWS/ES/Presentation/Planning/OnlinePlanning/y",
            "necs_assure",
        ),
        (
            # Hyndburn: NECS UI served from a path that includes /Northgate/ —
            # must classify as necs_assure, not northgate.
            "https://planning.hyndburnbc.gov.uk/Northgate/ES/Presentation/Planning/OnlinePlanning/z",
            "necs_assure",
        ),
        # Inventory-only families
        (
            "https://southhams.planning-register.co.uk/Planning/Display/0628/26/ARC",
            "planning_register",
        ),
        (
            "https://portal360.argyll-bute.gov.uk/planning/planning-documents?SDescription=21%2F02726",
            "planning_docs",
        ),
        (
            "http://buildingexplorer.gov.gg/Northgate/Images/Planning%20Decisions%20PDFs/X.pdf",
            "guernsey_direct",
        ),
        (
            "http://norwich.example.gov.uk/Planning/dialog.page?org.apache.shale.dialog.DIALOG_NAME=gfplanningsearch",
            "shale_dialog",
        ),
        (
            "https://msp.havering.gov.uk/planning/search-applications#VIEW?RefType=PLANNINGCASE&KeyText=P1",
            "msp_idox",
        ),
        ("https://rotherham.planportal.co.uk/?id=RB2017%2F0419", "planportal"),
        ("http://www1.arun.gov.uk/planrec/index.cfm?tpKey=eOcella&Keyscheme=Planning", "eocella"),
        (
            "https://wwwapplications.barnsley.gov.uk/PlanningExplorerMVC/Home/ApplicationDetails?planningApplicationNumber=2017",
            "planningexplorer_mvc",
        ),
        ("http://unidoc.wiltshire.gov.uk/UniDoc/Document/Search/DSA,918745", "unidoc"),
        (
            "https://apps.stratford.gov.uk/eplanningviewer/Home/index/21_03888_LBC",
            "eplanningviewer",
        ),
        ("https://cbc.aifusion.io/planning/publicViewer.html?caseID=CB", "aifusion"),
        (
            "http://cbstor.centralbedfordshire.gov.uk/publicportalviewer/publicViewer.html?caseID=CB",
            "aifusion",
        ),
        ("https://register.civicacx.co.uk/Erewash/Planning/Details/ShowDetails", "civica_cx"),
        (
            "http://appsportal.npt.gov.uk/ords/idocs12/f?p=Planning:2:0::NO::P2_REFERENCE:P2021",
            "oracle_ords",
        ),
        (
            "http://northgate.liverpool.gov.uk/DocumentExplorer/Application/folderview.aspx?type=MVMPRD",
            "liverpool_doc_explorer",
        ),
        ("https://www.bathnes.gov.uk/planningdocuments=21%2F04908%2FCLPU", "bathnes_custom"),
        ("https://ppc.ipswich.gov.uk/xappndocs.asp?iAppID=21%2F00469%2FFUL", "ipswich_custom"),
        (
            "https://www.gov.je/citizen/Planning/Pages/PlanningApplicationDocuments.aspx?s=1&r=P/2021/1489",
            "jersey_custom",
        ),
    ],
)
def test_classify_url_recognises_signature(url: str, expected: str) -> None:
    assert classify_url(url) == expected


def test_classify_url_returns_none_for_no_match() -> None:
    assert classify_url(None) is None
    assert classify_url("") is None
    assert classify_url("https://example.com/random/path") is None


def test_classify_portal_type_trusts_authority_when_specific(authority_csv: Path) -> None:
    """A specific authority verdict overrides URL pattern matches."""
    mapping = load_authority_portal_types(authority_csv)
    # Aberdeen → idox per CSV; URL would match planning_docs but authority wins.
    result = classify_portal_type(
        "Aberdeen",
        "https://aberdeen.gov.uk/planning/planning-documents?SDescription=X",
        mapping,
    )
    assert result == "idox"


def test_classify_portal_type_falls_back_to_url_when_authority_other(authority_csv: Path) -> None:
    """Custom-bucketed authorities should be refined by URL match."""
    mapping = load_authority_portal_types(authority_csv)
    # Bath maps to "other"; URL matches bathnes_custom.
    result = classify_portal_type(
        "Bath",
        "https://www.bathnes.gov.uk/planningdocuments=21%2F04908",
        mapping,
    )
    assert result == "bathnes_custom"


def test_classify_portal_type_falls_back_to_url_when_authority_unknown(authority_csv: Path) -> None:
    mapping = load_authority_portal_types(authority_csv)
    result = classify_portal_type(
        "Atlantis",
        "https://atlantis.example/online-applications/applicationDetails.do",
        mapping,
    )
    assert result == "idox"


def test_classify_portal_type_refines_alias_to_planning_register(authority_csv: Path) -> None:
    mapping = load_authority_portal_types(authority_csv)
    result = classify_portal_type(
        "South West Devon",
        "https://southhams.planning-register.co.uk/Planning/Display/0628/26/ARC",
        mapping,
    )
    assert result == "planning_register"


def test_classify_portal_type_keeps_vague_verdict_when_url_unmatched(
    authority_csv: Path,
) -> None:
    """If both authority and URL fail, keep the vague verdict from the authority lookup."""
    mapping = load_authority_portal_types(authority_csv)
    assert classify_portal_type("Bath", "https://example.com/totally-unrecognised", mapping) == "other"
    assert classify_portal_type("Atlantis", "https://example.com/totally-unrecognised", mapping) == "unknown"
    assert classify_portal_type("Atlantis", None, mapping) == "unknown"
