"""Cross-field consistency checks over a schema-extraction run's results.json.

Companion to verify_quotes.py (which grounds the verbatim-quote fields). This
script needs no source texts: it flags records whose fields contradict each
other or the schema's own rules, so a human reviews the flagged apps instead of
re-reading all of them. Two severities:

  HARD   — violates an invariant post-processing is supposed to enforce
           (regression guard); any hit exits 1.
  REVIEW — plausible-but-suspicious combination worth a human look; exits 0.

Checks:
  hard_conditions_on_refusal   conditions present on a non-granting outcome
  hard_refusal_ground_stance   hp_refusal_ground set but stance != unacceptable
  review_rating_arithmetic     rating != specific + character correction
  review_exceedance_arithmetic exceedance matches neither rating-background
                               nor rating-limit
  review_limit_double_fill     noise_limit_db AND noise_limit_relative both set
  review_stance_not_assessed   hp_relevant + refused + not_separately_assessed
                               (the audit's most-repeated stance failure)
  review_noise_refusal_stance  refusal cites sound_noise/setback_boundary on an
                               hp_relevant app but stance != unacceptable
  review_reasons_not_subset    refusal reasons not a subset of considerations
  review_wind_turbine_mismatch includes_wind_turbine vs bundled_works disagree
  review_age_without_evidence  building_age set but building_age_evidence null

Usage:
    HP_RUN_TAG=v4_10 python3 scripts/llm/check_consistency.py
Reads:
    _local/llm_pilot/schema_<tag>_50/results.json
Writes:
    _local/llm_pilot/schema_<tag>_50/consistency_check.csv
"""

from __future__ import annotations

import csv
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TAG = os.environ.get("HP_RUN_TAG", "v4_10")
RUN_DIR = ROOT / f"_local/llm_pilot/schema_{TAG}_50"

GRANTING_OUTCOMES = {"approved", "prior_approval_granted"}
REFUSAL_OUTCOMES = {"refused", "prior_approval_refused"}
DB_TOL = 0.6  # dB rounding slack between stated figures


def _num(v) -> float | None:
    return float(v) if isinstance(v, (int, float)) else None


def check_record(r: dict) -> list[tuple[str, str, str]]:
    """Returns (severity, check, detail) tuples for one record."""
    flags: list[tuple[str, str, str]] = []
    outcome = r.get("decision_outcome")
    stance = r.get("hp_component_stance")
    reasons = r.get("council_refusal_reasons") or []
    considerations = r.get("council_considerations") or []

    if outcome not in GRANTING_OUTCOMES:
        leaked = {k: r.get(k) for k in ("n_conditions", "condition_types", "hp_specific_conditions") if r.get(k)}
        if leaked:
            flags.append(("HARD", "hard_conditions_on_refusal", f"outcome={outcome}, {leaked}"))

    if r.get("hp_refusal_ground") and stance != "unacceptable":
        flags.append(("HARD", "hard_refusal_ground_stance", f"ground={r['hp_refusal_ground']}, stance={stance}"))

    specific = _num(r.get("noise_specific_level_db"))
    correction = _num(r.get("noise_character_correction_db"))
    rating = _num(r.get("noise_rating_level_db"))
    if None not in (specific, correction, rating) and abs(specific + correction - rating) > DB_TOL:
        flags.append(
            ("REVIEW", "review_rating_arithmetic", f"specific {specific} + correction {correction} != rating {rating}")
        )

    exceedance = _num(r.get("noise_exceedance_db"))
    limit = _num(r.get("noise_limit_db"))
    backgrounds = [
        b for b in (_num(r.get("noise_background_night_db")), _num(r.get("noise_background_day_db"))) if b is not None
    ]
    if exceedance is not None and rating is not None and (backgrounds or limit is not None):
        candidates = [rating - b for b in backgrounds]
        if limit is not None:
            candidates.append(rating - limit)
        if all(abs(exceedance - c) > DB_TOL for c in candidates):
            flags.append(
                (
                    "REVIEW",
                    "review_exceedance_arithmetic",
                    f"exceedance {exceedance} matches neither rating-background nor rating-limit "
                    f"(rating {rating}, backgrounds {backgrounds}, limit {limit})",
                )
            )

    if r.get("noise_limit_db") is not None and r.get("noise_limit_relative"):
        flags.append(
            (
                "REVIEW",
                "review_limit_double_fill",
                f"limit_db={r['noise_limit_db']} AND limit_relative={r['noise_limit_relative']}",
            )
        )

    if r.get("hp_relevance") == "hp_relevant" and outcome in REFUSAL_OUTCOMES and stance == "not_separately_assessed":
        flags.append(
            ("REVIEW", "review_stance_not_assessed", "hp_relevant app refused but stance=not_separately_assessed")
        )

    if (
        r.get("hp_relevance") == "hp_relevant"
        and outcome in REFUSAL_OUTCOMES
        and stance != "unacceptable"
        and {"sound_noise", "setback_boundary"} & set(reasons)
    ):
        flags.append(
            (
                "REVIEW",
                "review_noise_refusal_stance",
                f"refusal reasons {sorted(set(reasons))} on hp_relevant app but stance={stance}",
            )
        )

    if outcome in REFUSAL_OUTCOMES and reasons and not set(reasons) <= set(considerations):
        flags.append(
            (
                "REVIEW",
                "review_reasons_not_subset",
                f"refusal reasons {sorted(set(reasons) - set(considerations))} missing from considerations",
            )
        )

    has_wt = bool(r.get("includes_wind_turbine"))
    wt_bundled = "wind_turbine" in (r.get("bundled_works") or [])
    if has_wt != wt_bundled:
        flags.append(
            (
                "REVIEW",
                "review_wind_turbine_mismatch",
                f"includes_wind_turbine={has_wt}, bundled_works has wind_turbine={wt_bundled}",
            )
        )

    if r.get("building_age") and not r.get("building_age_evidence"):
        flags.append(("REVIEW", "review_age_without_evidence", f"age={r['building_age']}, evidence=null"))

    return flags


def main() -> int:
    results = json.loads((RUN_DIR / "results.json").read_text())

    rows: list[dict] = []
    for r in results:
        if "_error" in r:
            rows.append(
                {
                    "uid": r.get("uid", ""),
                    "authority": r.get("authority_name", ""),
                    "severity": "HARD",
                    "check": "extraction_error",
                    "detail": r["_error"],
                }
            )
            continue
        for severity, check, detail in check_record(r):
            rows.append(
                {
                    "uid": r["uid"],
                    "authority": r.get("authority_name", ""),
                    "severity": severity,
                    "check": check,
                    "detail": detail,
                }
            )

    out_path = RUN_DIR / "consistency_check.csv"
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["uid", "authority", "severity", "check", "detail"])
        w.writeheader()
        w.writerows(rows)

    n_hard = sum(1 for row in rows if row["severity"] == "HARD")
    print(f"Consistency check — {len(results)} apps, run {RUN_DIR.name}")
    print(f"  {len(rows)} flag(s): {n_hard} HARD, {len(rows) - n_hard} REVIEW\n")
    for row in rows:
        print(f"  [{row['severity']:6s}] {row['uid']} ({row['authority']}) {row['check']}: {row['detail']}")
    print(f"\nDetail -> {out_path}")
    return 1 if n_hard else 0


if __name__ == "__main__":
    sys.exit(main())
