"""Regenerate the reviewer-facing schema TSV from the live extractor schema.

Writes _local/docs/llm_extraction_schema.tsv with columns:
    group / field / type / allowed_values / description / example_from_pilot

Types and allowed values are pulled from extract_schema_v1.py so they cannot
drift from the code; descriptions live in DESC below (update alongside schema
changes). Examples come from pilot runs, first match wins:
    1. the v4.8 30-app run (native field names),
    2. the v5 50-app run via LEGACY name/value mapping (pre-v4.5 noise names),
    3. "(no pilot value yet)".

Usage:
    uv run --with openai --with python-dotenv python3 scripts/llm/build_schema_tsv.py
"""

# ruff: noqa: E501  (DESC is a prose dictionary; one field description per line)

from __future__ import annotations

import csv
import importlib.util
import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "_local/docs/llm_extraction_schema.tsv"
RUNS = [  # newest first; earlier runs only fill fields the newer ones left null
    ROOT / "_local/llm_pilot/schema_v4_12_gpt54mini_low_30_50/results.json",
    ROOT / "_local/llm_pilot/schema_v4_11_50/results.json",
    ROOT / "_local/llm_pilot/schema_v4_9_50/results.json",
    ROOT / "_local/llm_pilot/schema_v4_8_50/results.json",
    ROOT / "_local/llm_pilot/schema_v5_50/results.json",
]

_spec = importlib.util.spec_from_file_location("ext", ROOT / "scripts/llm/extract_schema_v1.py")
_ext = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ext)
SCHEMA = {**_ext.T0, **_ext.T1}

GROUPS = {
    **{
        f: "Workflow / decision"
        for f in [
            "hp_relevance",
            "decision_outcome",
            "hp_component_stance",
            "hp_refusal_ground",
            "summary",
            "key_evidence_quote",
            "council_refusal_reasons",
            "council_refusal_quote",
            "council_considerations",
            "policies_cited",
        ]
    },
    "application_type": "Application / process",
    **{
        f: "Public representations"
        for f in ["public_objections_received", "n_public_objections", "public_objections_grounds"]
    },
    **{
        f: "System + property"
        for f in [
            "hp_type",
            "dwelling_type",
            "building_age",
            "building_age_evidence",
            "hp_placement",
            "hp_mounting_type",
            "n_hp_units",
            "n_dwellings_served",
        ]
    },
    **{f: "HP unit specification" for f in ["hp_manufacturer", "hp_model", "hp_rated_output_kw"]},
    **{
        f: "Designations + heritage"
        for f in ["designations", "listed_status", "lbc_required", "lbc_decision", "lbc_reference"]
    },
    **{
        f: "PDR install facts"
        for f in [
            "install_above_ground_floor",
            "install_on_principal_elevation",
            "install_fronts_highway",
            "install_on_pitched_roof",
            "appearance_concern_level",
            "includes_wind_turbine",
            "alternative_siting_discussed",
        ]
    },
    **{f: "Numeric" for f in ["val_setback_from_edge_m", "val_distance_to_boundary_m", "val_distance_to_receptor_m"]},
    **{
        f: "Sound"
        for f in [
            "noise_assessment_outcome",
            "noise_assessment_method",
            "acoustic_standards_cited",
            "noise_source_power_db",
            "noise_specific_level_db",
            "noise_character_correction_db",
            "noise_rating_level_db",
            "noise_background_day_db",
            "noise_background_night_db",
            "noise_exceedance_db",
            "noise_limit_db",
            "noise_limit_relative",
            "noise_limit_basis",
            "applicant_acoustic_mitigations",
            "noise_nuisance_mentioned",
        ]
    },
    **{
        f: "Conditions + co-application"
        for f in ["n_conditions", "hp_specific_conditions", "condition_types", "bundled_works"]
    },
    "primary_planning_trigger": "Why it needed planning",
}

DESC = {
    "hp_relevance": "How central is the heat pump to the application? hp_relevant = the decision rationale turns on the HP itself. hp_incidental = the HP is a minor part of a larger project and the decision is driven by other works. mixed = the HP is one of several distinct decision grounds. One analytical lens - it doesn't gate any other field.",
    "decision_outcome": "What happened to the application (application-level outcome). The LLM confirms its read against the raw planning_decision string from ashp.db and flags any disagreement for QA. v4.12 drops the ambiguous 'ldc' value (it named a ROUTE - gpt-5.4-mini used it for every certificate app, refused ones included): a refused certificate is 'refused', a granted one 'approved'; the route lives in application_type.",
    "hp_component_stance": "The verdict on the heat pump itself, decoupled from the application-level outcome. An app refused over bats but where Environmental Health had no objection to the HP subject to a noise condition is acceptable_with_condition (counts even when the condition was only recommended, never imposed). Synthesises the sound, appearance and refusal signals into one headline HP variable.",
    "hp_refusal_ground": "NEW in v4.8. Why the heat pump ITSELF was found unacceptable - populated only when hp_component_stance=unacceptable, null otherwise (enforced in post-processing). The dominant ground: noise; appearance (visual/character); heritage (harm to listed-building or conservation-area significance); siting (location/proximity objection not reducible to the others); other. Application-level refusal topics stay in council_refusal_reasons - this answers 'character_appearance_hp vs general'.",
    "summary": "1-2 sentence plain-English summary: what was proposed + what happened.",
    "key_evidence_quote": "Single verbatim line (<=300 chars) from the supplied documents that best supports the overall read of the case - the source-grounding signal. Now machine-checked against the source texts by scripts/llm/verify_quotes.py (ok / near_verbatim / MISSING).",
    "council_refusal_reasons": "Refused apps only (null otherwise, enforced in post-processing). The LPA's own stated reasons, bucketed. unit_size_volume = the unit's physical size/volume/bulk (Class G 0.6 m3 limit checks and merits-based bulk reasoning). Must be a subset of council_considerations.",
    "council_refusal_quote": "Refused only. Verbatim quote of the council's stated reason(s). Quote-checked by verify_quotes.py.",
    "council_considerations": "Outcome-independent list of every material consideration the LPA substantively weighed - fires on approvals too ('no harm to character' => character_appearance). Same vocab as council_refusal_reasons, read as topics not verdicts. Excludes consultee-header boilerplate that isn't actually reasoned about.",
    "policies_cited": "NEW in v4.8. Verbatim development-plan / national-policy identifiers the decision or officer reasoning actually relies on (e.g. 'NPF4 Policy 23', 'LDP2023 Policy P4', 'NPPF paragraph 130'). Max 6, most decision-relevant first. Excludes the boilerplate 'Relevant Policies' header list - only policies the reasoning engages with.",
    "application_type": "The formal planning route, inferred from the reference-code suffix + description (FUL=full, HH=householder, PA/PNH=prior_approval, LDC/CLU=ldc, LBC=lbc, NMA=non_material_amendment, DOC/DISCON='discharge of conditions'=condition_discharge). NMAs refused because the LPA judges the change material get decision_outcome=refused + council_refusal_reasons=[other] + hp_component_stance=not_separately_assessed unless the LPA opined on the HP's merits (v4.8 rule).",
    "public_objections_received": "Were objections/representations received from neighbours/the public. Excludes statutory consultee responses (Environmental Health, Highways, Conservation Officer etc.).",
    "n_public_objections": "Count of public objections when stated - objections, not letters of support. Distinguishes a real count from the boilerplate template line ('any representations that may have been received'). 0 is a valid, meaningful answer; mostly 0 in this corpus.",
    "public_objections_grounds": "What the public objected about - different actor from council_refusal_reasons; populates regardless of outcome.",
    "hp_type": "Heat-pump technology: a2w air-to-water, a2a air-to-air, gshp ground source, wshp water source, hybrid, ac air-conditioning.",
    "dwelling_type": "Residential building form; non-domestic values (commercial = Use Class E retail/office/hospitality, industrial = B2/B8, institutional = Use Class F, agricultural, mixed_use) pair with primary_planning_trigger=non_domestic.",
    "building_age": "Original construction date as a 4-digit year ('1936') or earliest-first range ('1837-1901'). Period language converts deterministically (georgian 1714-1837, victorian 1837-1901, edwardian 1901-1914, interwar 1918-1939, post_war 1945-1979, modern 1980-2010, new_build 2010-2026); centuries/decades map literally ('17th century' -> 1600-1699). Origin, not later alterations. Never inferred from listed status / conservation area / address.",
    "building_age_evidence": "The verbatim phrase (<=120 chars) building_age was taken from. Evidence gate: null here => building_age must also be null. Guards the era-hallucination failure mode; quote-checked by verify_quotes.py.",
    "hp_placement": "Where on the property the unit sits. Orthogonal to mounting.",
    "hp_mounting_type": "How the unit is affixed: ground (pad/plinth), wall (bracket), roof, mixed.",
    "n_hp_units": "Number of heat-pump units.",
    "n_dwellings_served": "Dwellings the HP system serves. Pairs with n_hp_units to separate single-home arrays (both 1) from communal systems (1 unit, many flats) and per-flat arrays (equal counts > 1).",
    "hp_manufacturer": "Brand, normalised (e.g. Mitsubishi, Daikin, Vaillant, Grant, Nibe). Null if no brand named.",
    "hp_model": "Model name/code verbatim, incl. range + alphanumeric code (e.g. 'Ecodan PUZ-WM85VAA', 'aroTHERM plus 10kW'). Null if none named.",
    "hp_rated_output_kw": "The unit's rated thermal output in kW when stated. NOT the dwelling heat-loss/demand figure unless explicitly the same.",
    "designations": "Site designations. Kept individually here; primary_planning_trigger folds the landscape ones into protected_landscape.",
    "listed_status": "The application building's OWN listing - the building the works are physically on. A neighbour's setting harm goes to council_refusal_reasons=heritage, never here.",
    "lbc_required": "Is Listed Building Consent needed for THESE works? A listed building does not automatically need LBC - external plant that doesn't alter the listed fabric is commonly not_required. Null only when listed_status is none/unknown (LBC can't arise).",
    "lbc_decision": "The LBC stream's own outcome, independent of the planning decision_outcome. Null unless an LBC was actually required or lodged.",
    "lbc_reference": "The linked LBC application reference verbatim (e.g. 'UTT/19/2742/LB'); often in the officer report's site-history table.",
    "install_above_ground_floor": "Fact: HP at first-floor level, upper storey, or roof.",
    "install_on_principal_elevation": "Fact: HP on the principal (public-facing) elevation.",
    "install_fronts_highway": "Fact: the wall/roof the unit is on faces a public highway incl. footpath/pavement; excludes private roads/drives. The Class G legal trigger. GPDO-boilerplate trap: extract the property fact, not the legislation citation.",
    "install_on_pitched_roof": "Fact: HP mounted on a pitched roof.",
    "appearance_concern_level": "How far VISUAL/appearance concern escalated in the decision. Only visual/external-appearance/character - noise routes to the sound fields, neighbour disturbance to public_objections_grounds. addressed_by_condition captures 'pushed to the rear / conditioned for appearance' even on approvals.",
    "includes_wind_turbine": "The application also includes a wind turbine (cumulative-noise scenario). Must pair with wind_turbine in bundled_works (v4.7).",
    "alternative_siting_discussed": "NEW in v4.8. True when alternative HP locations were considered - the LPA/conservation officer requested information on alternatives (ground installation, secondary elevation, different roof) or the applicant assessed/defended the chosen location against them. Common in listed-building cases. False only when docs explicitly say alternatives were not considered; null when the topic never arises.",
    "val_setback_from_edge_m": "Distance from unit to nearest external roof/wall edge, metres. (Sparse - 0/50 on pilot.)",
    "val_distance_to_boundary_m": "Distance from unit to the SITE/CURTILAGE BOUNDARY only, metres (the GPDO Class G test). v4.10 pins the definition: distances to a neighbouring window/facade/dwelling do NOT go here (they were misfiled here in 3/10 audited apps) - those belong in val_distance_to_receptor_m.",
    "val_distance_to_receptor_m": "NEW in v4.10. Distance from unit to the nearest NOISE-SENSITIVE RECEPTOR (neighbouring window / facade / premises), metres - the figure acoustic reports usually state.",
    "noise_assessment_outcome": "The pass/fail STATUS of the noise position (was v4.4 sound_assessment_status). Orthogonal to noise_assessment_method.",
    "noise_assessment_method": "HOW the compliance position was reached: measured_on_site = real on-site background survey (BS 4142 proper, LA90 logged - strongest rigour signal); modelled_from_spec = calculated from manufacturer sound-power data with an assumed background (MCS-020 calculator, ISO 9613 model); asserted_only = standard named and compliance claimed with no measurement or calculation shown ('assumed pass'); not_stated.",
    "acoustic_standards_cited": "Standards the documents NAME. Grounding gate (v4.7): a local-plan policy number routes to noise_limit_basis=la_local_plan, and a bare 'MCS-certified installer' mention does NOT imply MCS 020. v4.10 adds iso_1996, bs_7445 and cieh_ioa_guidance (the Joint IOA/CIEH heat-pump noise guidance) - all appeared verbatim in pilot texts with no enum home, which produced unsupported picks.",
    "noise_source_power_db": "The unit's A-weighted SOUND POWER level Lw from the spec/brochure. Source emission, independent of distance/screening - comparable across apps regardless of geometry. NOT the level at a neighbour.",
    "noise_specific_level_db": "BS 4142 Specific Sound Level (LAs) - the level attributable to the HP at the nearest receptor, BEFORE any character correction.",
    "noise_character_correction_db": "The +dB penalty BS 4142 adds for acoustic character (tonality/impulsivity/intermittency). 0 if the report explicitly applies none; null if not discussed.",
    "noise_rating_level_db": "BS 4142 Rating Level (LAr) = specific level + character correction - THE figure compared against the limit/background. For an MCS-020 calculator with no separate correction, the single final-result level IS the rating level.",
    "noise_background_day_db": "Representative background LA90 for DAYTIME (07:00-23:00) when the survey reports it.",
    "noise_background_night_db": "Representative background LA90 for NIGHT (23:00-07:00) - usually the binding period for a 24h-running HP. An MCS-020 assumed background (commonly 40 dB(A), no survey) goes here unless stated otherwise.",
    "noise_exceedance_db": "SIGNED difference rating level minus binding background (negative => rating below background => 'Low Impact'). Captured only when the report states it.",
    "noise_limit_db": "The NUMERIC limit compared against when a single number is stated (e.g. 42 dB(A) MCS-020 PD limit). Null when the limit is purely relative.",
    "noise_limit_relative": "The limit when expressed RELATIVE to background with no single number: 'background-5', 'background', 'background+5'. Null if an absolute number is given.",
    "noise_limit_basis": "Which limit regime applies: mcs_020_pd_limit = national fixed PD limit (42 dB(A) pre-20-Sep-2025, 37 under MCS-020(a) after - version-neutral, number goes in noise_limit_db); bs_4142_background_relative (pairs with noise_limit_relative); la_local_plan = the LPA's own plan policy/SPG; bs_8233_internal = internal-room limit.",
    "applicant_acoustic_mitigations": "Acoustic mitigation features the APPLICANT proposed (council-imposed measures live in condition_types). siting_choice (renamed from relocation in v4.8) = unit located to the rear / away from receptors AS A NOISE MEASURE - covers both moving an existing unit and deliberately quiet siting of a new one; mere placement with no acoustic reasoning attached does not count. v4.10 grounding gate: a value counts only when a document presents the feature AS a noise measure; colour_finish dropped (appearance, not acoustic); low_noise_unit (low-noise model/configuration chosen for acoustic reasons) and absorption (acoustic absorptive panels/lining) added.",
    "noise_nuisance_mentioned": "Existing noise complaint or statutory-nuisance concern referenced in the documents.",
    "n_conditions": "Total conditions on the decision. Null unless the outcome grants (Python-normalised); 0 is reserved for a genuine unconditional grant, keeping per-approval aggregates clean.",
    "hp_specific_conditions": "Short paraphrases (<=120 chars each) of conditions that specifically reference the heat pump. Empty list if none.",
    "condition_types": "Categorical complement to hp_specific_conditions: noise_threshold (numeric limit), noise_post_install_check, noise_maintenance, standards_compliance (MCS/BS4142/IoA), colour_or_screening, relocation_or_position, accord_with_plans (generic as-approved-drawings), heritage_specific, commissioning_only, other. v4.10/v4.11: only conditions IMPOSED BY THIS DECISION count - parent-permission conditions on discharge/variation cases, consultee-recommended conditions on refused apps, and acoustic-report recommendations do not; refusals force-empty this field in post-processing (4/10 audited refusals had confabulated conditions).",
    "bundled_works": "Other works bundled with the HP - the 'what else is the application asking for' stat. wind_turbine added in v4.7 (pairs with includes_wind_turbine).",
    "primary_planning_trigger": "The SINGLE primary reason this application required planning permission, evaluated top-down stopping at the first match. Admin/follow-on records (condition_discharge / NMA / LDC / RM) carry the PARENT permission's trigger or unknown - never invented. Then the non-HP routers (new_build, non_domestic, bundled_development) take precedence; only if none apply does the HP itself become the trigger (listed, conservation_area, article_4, protected_landscape, flat, above_ground_floor, within_1m_boundary, oversized, mcs_noise_fail, front_elevation, pitched_roof, multiple_units, wind_turbine_combo, amenity). mcs_noise_fail is NEW in v4.10: the MCS-020 noise calc fails the PD limit so Class G PD is unavailable. v4.11 adds s73 variation/removal apps to the follow-on rule (they carry the parent permission's trigger). retrospective only for explicit retention cases with no specific trigger.",
}

# Pre-v4.5 noise field names + enum values, for harvesting examples from old runs.
LEGACY = {
    "noise_assessment_outcome": ("sound_assessment_status", {"required_pass": "pass", "required_fail": "fail"}),
    "noise_source_power_db": ("sound_source_power_db", {}),
    "noise_rating_level_db": ("val_sound_level_db", {}),
    "noise_background_night_db": ("sound_background_db", {}),
    "noise_limit_db": ("permitted_limit_db", {}),
    "noise_limit_basis": ("permitted_limit_basis", {"mcs_020_fixed_42": "mcs_020_pd_limit"}),
}
VALUE_MAP = {"relocation": "siting_choice"}


def ftype(spec: dict) -> str:
    t = spec.get("type")
    ts = t if isinstance(t, list) else [t]
    if "array" in ts:
        return "list[enum]" if "enum" in spec.get("items", {}) else "list[str]"
    if "enum" in spec:
        return "enum"
    if "boolean" in ts:
        return "bool"
    if "integer" in ts:
        return "int"
    if "number" in ts:
        return "num"
    return "str"


def allowed(spec: dict) -> str:
    if "enum" in spec:
        return " / ".join(x for x in spec["enum"] if x is not None)
    if "enum" in spec.get("items", {}):
        return " / ".join(spec["items"]["enum"])
    return "-"


def render(v, vmap: dict) -> str:
    if isinstance(v, list):
        return "|".join(VALUE_MAP.get(vmap.get(str(x), str(x)), vmap.get(str(x), str(x))) for x in v)
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, str):
        v = VALUE_MAP.get(vmap.get(v, v), vmap.get(v, v))
    s = str(v).replace("\t", " ").replace("\n", " ")
    return s[:90] + ("…" if len(s) > 90 else "")


def example(field: str, spec: dict, runs: list[list[dict]]) -> str:
    for native, run in enumerate(runs):
        src, vmap = (field, {}) if native == 0 else LEGACY.get(field, (field, {}))
        vals = [r.get(src) for r in run if r.get(src) not in (None, "", [])]
        if not vals:
            continue
        if "enum" in spec:
            # Most common value, preferring an informative one over the
            # silence-markers when any informative value occurred.
            counts = Counter(map(str, vals))
            informative = {
                v: n for v, n in counts.items() if v not in ("unknown", "not_stated", "not_mentioned", "none")
            }
            best = Counter(informative or counts).most_common(1)[0][0]
            return render(best, vmap)
        return render(vals[0], vmap)
    return "(no pilot value yet)"


def main() -> int:
    runs = [json.loads(p.read_text()) for p in RUNS if p.exists()]
    with OUT.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter="\t")
        w.writerow(["group", "field", "type", "allowed_values", "description", "example_from_pilot"])
        for f, spec in SCHEMA.items():
            assert f in GROUPS, f"no group for {f}"
            assert f in DESC, f"no description for {f}"
            w.writerow([GROUPS[f], f, ftype(spec), allowed(spec), DESC[f], example(f, spec, runs)])
    print(f"Wrote {OUT}: {len(SCHEMA)} fields, examples from {len(runs)} run(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
