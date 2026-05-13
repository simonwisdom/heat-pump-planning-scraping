"""Match PlanIt authority_name strings to ONS LPA codes (April 2022 boundaries).

Produces a CSV at _local/geo/authority_lpa_lookup.csv with one row per
db authority. Authorities mapping to multiple LPAs (joint planning services,
new unitaries) get one row per LPA so apps can be split when rendering.

Status values:
    matched              direct alias or normalised match
    joint                db authority covers >1 LPA; one row per LPA
    new_unitary          post-2022 unitary mapped to its old districts
    no_lpa               county/non-LPA (e.g. Essex CC); cannot map
    out_of_scope         crown dependencies (Jersey, Guernsey, IoM)
    unmatched            still no match — needs manual review
"""

from __future__ import annotations

import csv
import json
import re
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GEO = ROOT / "_local/geo/lpa_uk_buc_2022.min.geojson"
DEFAULT_DB = ROOT / "_local/workstreams/01_heat_pump_applications/data/raw/ashp.db"
OUT = ROOT / "_local/geo/authority_lpa_lookup.csv"


# Direct aliases: db name (lowercased, apostrophes normalised) -> ONS LPA name
ALIAS: dict[str, str] = {
    # Scotland
    "western isles": "Na h-Eileanan Siar",
    "loch lomond": "The Loch Lomond and the Trossachs National Park",
    "shetlands": "Shetland Islands",
    "shetland": "Shetland Islands",
    "orkney": "Orkney Islands",
    "dumfries": "Dumfries and Galloway",
    "aberdeen": "Aberdeen City",
    "edinburgh": "City of Edinburgh",
    "perth": "Perth and Kinross",
    "argyll": "Argyll and Bute",
    "glasgow": "Glasgow City",
    "dundee": "Dundee City",
    "cairngorms": "The Cairngorms National Park",
    # Wales
    "anglesey": "Isle of Anglesey",
    "snowdonia": "Snowdonia National Park",
    "brecon beacons": "Brecon Beacons National Park",
    "rhondda": "Rhondda Cynon Taf",
    "neath": "Neath Port Talbot",
    "glamorgan": "Vale of Glamorgan",
    "pembroke coast": "Pembrokeshire Coast National Park",
    # NI
    "armagh banbridge craigavon": "Armagh City, Banbridge and Craigavon",
    "mid east antrim": "Mid and East Antrim",
    "newry mourne down": "Newry, Mourne and Down",
    "causeway and glens": "Causeway Coast and Glens",
    "derry and strabane": "Derry City and Strabane",
    # English unitaries / cities with non-obvious names
    "bristol": "Bristol, City of",
    "kensington": "Kensington and Chelsea",
    "richmond": "Richmond upon Thames",
    "kingston": "Kingston upon Thames",
    "durham": "County Durham",
    "chester": "Cheshire West and Chester",
    "brighton": "Brighton and Hove",
    "reigate": "Reigate and Banstead",
    "windsor": "Windsor and Maidenhead",
    "bath": "Bath and North East Somerset",
    "basingstoke": "Basingstoke and Deane",
    "tonbridge": "Tonbridge and Malling",
    "kings lynn": "King's Lynn and West Norfolk",
    "stratford on avon": "Stratford-on-Avon",
    "stratford-on-avon": "Stratford-on-Avon",
    "east riding": "East Riding of Yorkshire",
    "herefordshire": "Herefordshire, County of",
    "bcp": "Bournemouth, Christchurch and Poole",
    "rutland": "Rutland",
    "medway": "Medway",
    "isles of scilly": "Isles of Scilly",
    "scilly isles": "Isles of Scilly",
    "telford": "Telford and Wrekin",
    "shepway": "Folkestone and Hythe",
    "city": "City of London",
    "bracknell": "Bracknell Forest",
    "blackburn": "Blackburn with Darwen",
    "southend": "Southend-on-Sea",
    "hull": "Kingston upon Hull, City of",
    "north lincs": "North Lincolnshire",
    "newcastle under lyme": "Newcastle-under-Lyme",
    "north east lincs": "North East Lincolnshire",
    "northumberland (park)": "Northumberland National Park",
    "stoke on trent": "Stoke-on-Trent",
    "broads": "The Broads Authority",
    "london legacy": "London Legacy Development Corporation",
    "barrow": "Barrow-in-Furness",
    "old oak park royal": "Old Oak and Park Royal Development Corporation",
    "new forest (district)": "New Forest",
    "taunton deane": "Somerset West and Taunton",
    # National parks
    "south downs": "South Downs National Park",
    "lake district": "Lake District National Park",
    "peak district": "Peak District National Park",
    "new forest": "New Forest National Park",
    "new forest (park)": "New Forest National Park",
    "north york moors": "North York Moors National Park",
    "yorkshire dales": "Yorkshire Dales National Park",
    "dartmoor": "Dartmoor National Park",
    "exmoor": "Exmoor National Park",
    "northumberland (national park)": "Northumberland National Park",
}

# db name -> list of ONS LPA names (joint planning services)
JOINT: dict[str, list[str]] = {
    "babergh mid suffolk": ["Babergh", "Mid Suffolk"],
    "south norfolk broadland": ["South Norfolk", "Broadland"],
    "south west devon": ["South Hams", "West Devon"],
    "mid kent": ["Maidstone", "Tunbridge Wells", "Swale"],
    "adur and worthing": ["Adur", "Worthing"],
    "east hampshire and havant": ["East Hampshire", "Havant"],
    "east hants and havant": ["East Hampshire", "Havant"],
    "north essex": ["Braintree", "Colchester", "Tendring"],
    "bromsgrove redditch": ["Bromsgrove", "Redditch"],
    "west somerset": ["Somerset West and Taunton"],
}

# db name -> list of ONS LPA names (post-2022 unitary mapped to its old districts).
# These authorities exist in current PlanIt data but the 2022 boundary file
# still has the predecessor districts.
NEW_UNITARY: dict[str, list[str]] = {
    "westmorland and furness": ["South Lakeland", "Eden", "Barrow-in-Furness"],
    "cumberland": ["Allerdale", "Carlisle", "Copeland"],
    "north yorkshire": [
        "Craven",
        "Hambleton",
        "Harrogate",
        "Richmondshire",
        "Ryedale",
        "Scarborough",
        "Selby",
    ],
    "somerset": ["Mendip", "Sedgemoor", "Somerset West and Taunton", "South Somerset"],
    # 2020 reorganisations — old district names may still appear in PlanIt:
    "aylesbury vale": ["Buckinghamshire"],
    "wycombe": ["Buckinghamshire"],
    "chiltern": ["Buckinghamshire"],
    "chiltern south bucks": ["Buckinghamshire"],
    "south bucks": ["Buckinghamshire"],
    "east northamptonshire": ["North Northamptonshire"],
    "kettering": ["North Northamptonshire"],
    "wellingborough": ["North Northamptonshire"],
    "corby": ["North Northamptonshire"],
    "daventry": ["West Northamptonshire"],
    "northampton": ["West Northamptonshire"],
    "south northamptonshire": ["West Northamptonshire"],
}

# County-level / non-LPA records — no boundary in the LPA layer
NO_LPA: set[str] = {
    "essex",
    "northumberland (county)",
    "north yorkshire (county)",
    "kent",
    "lancashire",
    "hertfordshire",
    "hampshire",
    "surrey",
    "warwickshire",
    "leicestershire",
    "lincolnshire",
    "staffordshire",
    "nottinghamshire",
    "derbyshire",
    "worcestershire",
    "oxfordshire",
    "gloucestershire",
    "cambridgeshire",
    "suffolk",
    "norfolk",
    "west sussex",
    "east sussex",
    "cornwall (county)",
    "devon",
}

# Crown dependencies — never had ONS LPA boundaries
OUT_OF_SCOPE: set[str] = {"guernsey", "jersey", "isle of man"}


def normalise(s: str) -> str:
    """Cheap normaliser used as a fallback after alias lookup."""
    s = s.lower().strip()
    s = s.replace("’", "'").replace("‘", "'")
    s = re.sub(r"\s+lpa$", "", s)
    s = s.replace("&", "and")
    s = re.sub(r"^(city of|royal borough of|london borough of|borough of)\s+", "", s)
    s = re.sub(
        r"\s+(council|borough council|district council|city council|county council)$",
        "",
        s,
    )
    s = re.sub(r"[,.()'’`]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def alias_key(s: str) -> str:
    """Lowercase + apostrophe-normalise, no other normalisation. Used for ALIAS lookups."""
    return s.lower().strip().replace("’", "'").replace("‘", "'")


def build_lookup(db_path: Path | None = None) -> tuple[list[dict], list[dict]]:
    db_path = Path(db_path) if db_path else DEFAULT_DB
    geo = json.loads(GEO.read_text())
    code_by_norm: dict[str, tuple[str, str]] = {}
    code_by_alias: dict[str, tuple[str, str]] = {}
    for f in geo["features"]:
        name = f["properties"]["LPA22NM"]
        code = f["properties"]["LPA22CD"]
        # ONS suffix " LPA" — strip for matching
        bare = re.sub(r"\s+LPA$", "", name)
        code_by_alias[alias_key(bare)] = (code, name)
        code_by_norm[normalise(name)] = (code, name)

    def resolve(target_name: str) -> tuple[str, str] | None:
        return code_by_alias.get(alias_key(target_name)) or code_by_norm.get(normalise(target_name))

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """
        SELECT authority_name, COUNT(*) AS apps
        FROM applications
        WHERE authority_name IS NOT NULL AND authority_name != ''
        GROUP BY authority_name
        ORDER BY apps DESC
        """
    ).fetchall()

    matched_rows: list[dict] = []
    diagnostics: list[dict] = []

    for db_name, apps in rows:
        key = alias_key(db_name)

        # 1) Joint planning services -> one row per LPA
        if key in JOINT:
            targets = JOINT[key]
            for t in targets:
                hit = resolve(t)
                if hit:
                    code, ons = hit
                    matched_rows.append(
                        {
                            "db_authority_name": db_name,
                            "apps": apps,
                            "lpa22cd": code,
                            "lpa22nm": ons,
                            "status": "joint",
                        }
                    )
                else:
                    diagnostics.append(
                        {"db_authority_name": db_name, "apps": apps, "status": "joint_lookup_failed", "detail": t}
                    )
            continue

        # 2) New unitary mapped to old districts
        if key in NEW_UNITARY:
            targets = NEW_UNITARY[key]
            for t in targets:
                hit = resolve(t)
                if hit:
                    code, ons = hit
                    matched_rows.append(
                        {
                            "db_authority_name": db_name,
                            "apps": apps,
                            "lpa22cd": code,
                            "lpa22nm": ons,
                            "status": "new_unitary",
                        }
                    )
                else:
                    diagnostics.append(
                        {"db_authority_name": db_name, "apps": apps, "status": "new_unitary_lookup_failed", "detail": t}
                    )
            continue

        # 3) Non-LPA county records
        if key in NO_LPA:
            matched_rows.append(
                {"db_authority_name": db_name, "apps": apps, "lpa22cd": "", "lpa22nm": "", "status": "no_lpa"}
            )
            continue

        # 4) Crown dependencies
        if key in OUT_OF_SCOPE:
            matched_rows.append(
                {"db_authority_name": db_name, "apps": apps, "lpa22cd": "", "lpa22nm": "", "status": "out_of_scope"}
            )
            continue

        # 5) Direct alias → canonical ONS name
        if key in ALIAS:
            hit = resolve(ALIAS[key])
            if hit:
                code, ons = hit
                matched_rows.append(
                    {"db_authority_name": db_name, "apps": apps, "lpa22cd": code, "lpa22nm": ons, "status": "matched"}
                )
                continue

        # 6) Last resort: normalised string match
        hit = resolve(db_name)
        if hit:
            code, ons = hit
            matched_rows.append(
                {"db_authority_name": db_name, "apps": apps, "lpa22cd": code, "lpa22nm": ons, "status": "matched"}
            )
            continue

        # 7) Still nothing
        matched_rows.append(
            {"db_authority_name": db_name, "apps": apps, "lpa22cd": "", "lpa22nm": "", "status": "unmatched"}
        )

    return matched_rows, diagnostics


def write_lookup_csv(db_path: Path | None = None, out_path: Path | None = None) -> Path:
    """Build the lookup against db_path and write the CSV. Returns the output path."""
    rows, _ = build_lookup(db_path)
    target = Path(out_path) if out_path else OUT
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["db_authority_name", "apps", "lpa22cd", "lpa22nm", "status"])
        w.writeheader()
        w.writerows(rows)
    return target


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to ashp.db")
    parser.add_argument("--out", type=Path, default=OUT, help="Path to write CSV")
    args = parser.parse_args()

    rows, diagnostics = build_lookup(args.db)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["db_authority_name", "apps", "lpa22cd", "lpa22nm", "status"])
        w.writeheader()
        w.writerows(rows)

    # Summary
    from collections import Counter

    by_status_authorities = Counter(r["status"] for r in rows)
    seen_db: set[str] = set()
    by_status_apps: Counter[str] = Counter()
    for r in rows:
        # Apps shouldn't be double-counted across joint/new_unitary expansions
        if r["db_authority_name"] in seen_db:
            continue
        seen_db.add(r["db_authority_name"])
        by_status_apps[r["status"]] += r["apps"]

    print("=== authority count by status ===")
    for s, n in sorted(by_status_authorities.items(), key=lambda kv: -kv[1]):
        print(f"  {s:14s}  {n}")
    print()
    print("=== apps by status (db authorities counted once) ===")
    for s, n in sorted(by_status_apps.items(), key=lambda kv: -kv[1]):
        print(f"  {s:14s}  {n}")

    unmatched = [r for r in rows if r["status"] == "unmatched"]
    print(f"\n=== still unmatched: {len(unmatched)} authorities ===")
    for r in sorted(unmatched, key=lambda x: -x["apps"]):
        print(f"  {r['apps']:>5}  {r['db_authority_name']!r}")

    if diagnostics:
        print("\n=== lookup diagnostics ===")
        for d in diagnostics:
            print(f"  {d}")

    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
