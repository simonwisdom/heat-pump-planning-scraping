"""Build a per-LA matrix of planning apps and MCS installs by year.

One row per LA, with year columns for planning apps and MCS installs (2009 to
last full year) plus totals. The row set is the union of LAs present in either
dataset: db authority names from the planning corpus and MCS LA names from the
MCS by-LA-by-year CSV, joined via _local/geo/authority_lpa_lookup.csv with the
same rules used by scripts/analyse_decision_patterns.py.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = Path("/tmp/ashp.db")
REPO_LOCAL_DB = ROOT / "_local/workstreams/01_heat_pump_applications/data/raw/ashp.db"
MCS_LA_BY_YEAR_CSV = ROOT / "data/external_heat_pump_data/MCS_heatpump_installations_by_LA_by_year.csv"
LPA_LOOKUP_CSV = ROOT / "_local/geo/authority_lpa_lookup.csv"
LPA_OVERRIDES_CSV = ROOT / "rules/mcs_lpa_overrides.csv"
AUTHORITY_OVERRIDES_CSV = ROOT / "rules/mcs_authority_overrides.csv"
DEFAULT_OUTPUT = ROOT / "reports/heat-pump-decisions/la_year_matrix.tsv"

# 2008 has no MCS data; 2026 is the current partial year as of writing.
DEFAULT_YEAR_LO = 2009
DEFAULT_YEAR_HI = 2025


def normalise(name: str) -> str:
    return name.strip().lower().replace("’", "'")


def load_lookup_csv(path: Path, key_col: str, value_col: str) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            k = (row.get(key_col) or "").strip().lower()
            v = (row.get(value_col) or "").strip().lower()
            if k and v:
                out[k] = v
    return out


def load_mcs(path: Path) -> tuple[dict[tuple[str, int], int], dict[str, str]]:
    """Return ({(la_normalised, year): count}, {la_normalised: display_name})."""
    counts: dict[tuple[str, int], int] = defaultdict(int)
    display: dict[str, str] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name_raw = (row.get("areaName") or "").strip()
            if not name_raw:
                continue
            try:
                year = int(row["year"])
                count = int(row["installationCount"])
            except (KeyError, ValueError, TypeError):
                continue
            key = normalise(name_raw)
            counts[(key, year)] += count
            display.setdefault(key, name_raw)
    return counts, display


def load_authority_lookup(path: Path, lpa_overrides: dict[str, str]) -> dict[str, list[str]]:
    """Return {db_authority_lower: [mcs_la_normalised, ...]}.

    Mirrors load_authority_lpa_lookup() in analyse_decision_patterns.py.
    """
    out: dict[str, list[str]] = {}
    if not path.exists():
        return out
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            status = (row.get("status") or "").strip()
            if status not in ("matched", "joint", "new_unitary"):
                continue
            db_name = (row.get("db_authority_name") or "").strip().lower()
            lpa_name = (row.get("lpa22nm") or "").strip()
            if lpa_name.endswith(" LPA"):
                lpa_name = lpa_name[:-4]
            if not (db_name and lpa_name):
                continue
            normalised = normalise(lpa_name)
            mapped = lpa_overrides.get(normalised, normalised)
            existing = out.setdefault(db_name, [])
            if mapped not in existing:
                existing.append(mapped)
    return out


def load_planning(db_path: Path) -> dict[tuple[str, int], int]:
    """Return {(authority_lower, year): count}, all source_scrapes."""
    counts: dict[tuple[str, int], int] = defaultdict(int)
    conn = sqlite3.connect(str(db_path))
    cur = conn.execute(
        "SELECT authority_name, substr(start_date,1,4) AS y FROM applications "
        "WHERE start_date IS NOT NULL AND length(start_date) >= 4"
    )
    for authority, y in cur:
        if not authority or not (y and y.isdigit()):
            continue
        counts[(authority.strip().lower(), int(y))] += 1
    conn.close()
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--year-lo", type=int, default=DEFAULT_YEAR_LO)
    parser.add_argument("--year-hi", type=int, default=DEFAULT_YEAR_HI)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    db_path = args.db
    if not db_path.exists():
        if db_path == DEFAULT_DB and REPO_LOCAL_DB.exists():
            print(f"Falling back to {REPO_LOCAL_DB} (this is the ASHP-only snapshot, not merged 30k).")
            db_path = REPO_LOCAL_DB
        else:
            parser.error(f"DB not found: {db_path}")

    lpa_overrides = load_lookup_csv(LPA_OVERRIDES_CSV, "lpa_name_lower", "mcs_la_lower")
    authority_overrides = load_lookup_csv(AUTHORITY_OVERRIDES_CSV, "db_authority_lower", "mcs_la_lower")

    authority_lookup = load_authority_lookup(LPA_LOOKUP_CSV, lpa_overrides)
    for db_name, mcs_name in authority_overrides.items():
        if mcs_name not in authority_lookup.get(db_name, []):
            authority_lookup.setdefault(db_name, []).append(mcs_name)

    mcs_counts, mcs_display = load_mcs(MCS_LA_BY_YEAR_CSV)
    planning_counts = load_planning(db_path)

    years = list(range(args.year_lo, args.year_hi + 1))
    mcs_names = {n for (n, _y) in mcs_counts}

    # Group planning by canonical LA. For each db_authority:
    #   - if it maps to a single MCS LA, attribute its apps there.
    #   - if it maps to >1 MCS LA (joint db_authority like "Adur and Worthing"
    #     covering separate MCS LAs "Adur" and "Worthing"), split apps
    #     proportionally by MCS install share so the row totals reconcile.
    #   - else if the db_authority name itself matches an MCS LA, use that.
    #   - else: standalone planning-only row.
    planning_by_la: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    planning_only_authorities: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    contributors: dict[str, set[str]] = defaultdict(set)  # mcs_la -> {planit_authorities}
    multi_mapped: set[str] = set()  # planit_authorities mapping to >1 MCS LA

    authorities_seen: set[str] = set()
    for (auth_lower, year), n in planning_counts.items():
        authorities_seen.add(auth_lower)

    # Pre-index planning by authority for fast per-authority iteration.
    by_auth: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for (a, y), n in planning_counts.items():
        by_auth[a][y] = n

    for auth_lower in authorities_seen:
        mapped = authority_lookup.get(auth_lower, [])
        if not mapped and auth_lower in mcs_names:
            mapped = [auth_lower]
        if not mapped:
            planning_only_authorities[auth_lower] = dict(by_auth[auth_lower])
            continue
        if len(mapped) > 1:
            multi_mapped.add(auth_lower)
            # Split this authority's planning apps across its mapped MCS LAs
            # proportionally by total MCS installs in the window. Avoids
            # double-counting joint db_authority rows (e.g. "Adur and Worthing").
            mcs_totals = {m: sum(mcs_counts.get((m, y), 0) for y in years) for m in mapped}
            grand = sum(mcs_totals.values())
            if grand > 0:
                weights = {m: mcs_totals[m] / grand for m in mapped}
            else:
                weights = {m: 1.0 / len(mapped) for m in mapped}
            for mcs_la in mapped:
                contributors[mcs_la].add(auth_lower)
                w = weights[mcs_la]
                for y, n in by_auth[auth_lower].items():
                    planning_by_la[mcs_la][y] += int(round(n * w))
        else:
            mcs_la = mapped[0]
            contributors[mcs_la].add(auth_lower)
            for y, n in by_auth[auth_lower].items():
                planning_by_la[mcs_la][y] += n

    # Now build rows. Set of LA rows:
    #   - every MCS LA that has any installs in our year window (display name)
    #   - every MCS LA referenced by an authority mapping (even if zero installs)
    #   - every planning-only authority
    all_mcs_in_window = {n for (n, y) in mcs_counts if args.year_lo <= y <= args.year_hi}
    mcs_la_keys = sorted(all_mcs_in_window | set(planning_by_la.keys()))
    rows = []

    for mcs_la in mcs_la_keys:
        mcs_year_vals = {y: mcs_counts.get((mcs_la, y), 0) for y in years}
        plan_year_vals = planning_by_la.get(mcs_la, {})
        plan_year_vals = {y: plan_year_vals.get(y, 0) for y in years}
        total_mcs = sum(mcs_year_vals.values())
        total_plan = sum(plan_year_vals.values())
        contrib_list = sorted(contributors.get(mcs_la, set()))
        if total_mcs > 0 and total_plan > 0:
            source = "matched"
        elif total_mcs > 0:
            source = "mcs_only"
        else:
            source = "planning_only"
        rows.append(
            {
                "la_name": mcs_display.get(mcs_la, mcs_la),
                "source": source,
                "planit_authorities": "; ".join(contrib_list),
                "n_planit_authorities": len(contrib_list),
                "has_joint_planit_authority": any(a in multi_mapped for a in contrib_list),
                **{f"planning_apps_{y}": plan_year_vals[y] for y in years},
                **{f"mcs_installs_{y}": mcs_year_vals[y] for y in years},
                "total_planning_apps": total_plan,
                "total_mcs_installs": total_mcs,
            }
        )

    for auth_lower, year_vals in planning_only_authorities.items():
        plan_year_vals = {y: year_vals.get(y, 0) for y in years}
        rows.append(
            {
                "la_name": auth_lower,
                "source": "planning_only_no_mcs_match",
                "planit_authorities": auth_lower,
                "n_planit_authorities": 1,
                "has_joint_planit_authority": False,
                **{f"planning_apps_{y}": plan_year_vals[y] for y in years},
                **{f"mcs_installs_{y}": 0 for y in years},
                "total_planning_apps": sum(plan_year_vals.values()),
                "total_mcs_installs": 0,
            }
        )

    rows.sort(key=lambda r: (-r["total_mcs_installs"], -r["total_planning_apps"], r["la_name"]))

    fieldnames = [
        "la_name",
        "source",
        "planit_authorities",
        "n_planit_authorities",
        "has_joint_planit_authority",
        *[f"planning_apps_{y}" for y in years],
        *[f"mcs_installs_{y}" for y in years],
        "total_planning_apps",
        "total_mcs_installs",
    ]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    n_matched = sum(1 for r in rows if r["source"] == "matched")
    n_mcs_only = sum(1 for r in rows if r["source"] == "mcs_only")
    n_plan_only_unmapped = sum(1 for r in rows if r["source"] == "planning_only_no_mcs_match")
    n_plan_no_mcs_in_window = sum(1 for r in rows if r["source"] == "planning_only")
    print(f"Wrote {len(rows)} rows to {args.output}")
    print(f"  matched (both):                       {n_matched}")
    print(f"  MCS-only (no planning apps):          {n_mcs_only}")
    print(f"  planning-only (no MCS in window):     {n_plan_no_mcs_in_window}")
    print(f"  planning-only (db_authority unmapped):{n_plan_only_unmapped}")
    print(f"Year window: {args.year_lo}-{args.year_hi}")


if __name__ == "__main__":
    main()
