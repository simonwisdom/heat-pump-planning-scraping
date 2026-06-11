"""v4.12 60-field schema extraction over a stratified sample.

v4.12 (model bake-off support): decision_outcome loses its ambiguous "ldc" value
(a ROUTE, not an outcome — gpt-5.4-mini used it for every LDC app, refused
certificates included, tripping the refusal-reason normaliser; the route lives in
application_type). New HP_REASONING_EFFORT env var: when set, passes
reasoning_effort and omits temperature (GPT-5.x models reject temperature!=0
defaults). Bake-off on the audit ledger: gpt-4.1-mini 25/36, gpt-4.1 ~31/36,
gpt-5.4-mini reasoning=low 31/36 at half gpt-4.1's price (~$26/1k apps std,
~$13 batched).

v4.11 (residuals from re-auditing the v4.10 diff): OFFICER_TYPES gains the
"officers report" / "officer's report" doctype spellings (East Herts' officer
report fell to rank 9 / the 12k clip and kept the superseded Daikin unit);
stance hard-rule now covers refusals aimed at "plant equipment" that includes
the HP, and LDCs refused partly on an HP Class G ground; explicit
no-self-arithmetic example for noise_rating_level_db (Leeds kept outputting
53+5=58); n_hp_units counts heat pumps only (not a bundled AC condenser);
not_mentioned vs submitted_no_outcome clarified (assessment submitted but
council didn't rule on it => submitted_no_outcome); s73 variation/removal apps
added to the no-own-trigger rule (report the parent permission's trigger).

v4.10 (audit-driven; +1 field, vocab changes): responds to a 10-app adversarial
audit of the v4.9 run (6/10 apps had >=1 materially wrong analytical field; all
quote fields verbatim-clean).
- Staging: decision/officer-ranked files (REPORT_RANKS) now sent whole up to
  REPORT_FILE_CHARS=40k (the 12k head-clip cut Caerphilly's Class G analysis ->
  wrong refusal reason, and East Herts' superseded-unit correction -> wrong
  manufacturer). MAX_DOC_CHARS 80k -> 150k so whole reports don't evict rank-9
  files (budget exhaustion silently dropped Southwark's officer assessment and
  Leeds' sound-data sheet). load_doc_text prints a COVERAGE WARNING whenever a
  decision/officer/sound file is dropped or truncated.
- Post-processing: non-granting outcomes now also empty condition_types and
  hp_specific_conditions (v4.9 confabulated conditions on 4/10 refusals from
  parent permissions and "recommended if approved" passages).
- Prompt: hp_component_stance hard rules (HP-only proposal => the application
  verdict IS the HP stance; a refusal reason naming the HP => unacceptable);
  numeric grounding (never self-compute rating/exceedance; reflections
  correction is not a character correction; MCS-020 STEP 6 = specific level,
  FINAL RESULT = rating level); noise_assessment_outcome takes the council/EH
  verdict over the applicant report's own conclusion; noise_assessment_method
  classified by BACKGROUND provenance (surveyed background + calculated specific
  level is still measured_on_site); mitigations grounding gate; hp_type decided
  only on distribution-medium evidence; superseded-spec rule (capture the FINAL
  confirmed unit); insufficiency refusals tag the missing-info TOPIC; Class G
  boundary-rule failures are setback_boundary, not unit_size_volume; conditions
  must be imposed by THIS decision; appearance_concern_level is HP-scoped.
- Schema: new val_distance_to_receptor_m (the figure acoustic reports actually
  state; v4.9 kept misfiling it as val_distance_to_boundary_m, 3/10 apps);
  acoustic_standards_cited += iso_1996, bs_7445, cieh_ioa_guidance;
  applicant_acoustic_mitigations: colour_finish dropped (appearance, not
  acoustic; invited unsupported picks), low_noise_unit + absorption added;
  primary_planning_trigger += mcs_noise_fail (Crawley-type "fails MCS-020 so
  not PD" applications, previously mis-tagged front_elevation).
- Companion script scripts/llm/check_consistency.py flags arithmetic /
  cross-field inconsistencies in a run's results.json for review.

v4.9 (no schema changes): sound-ranked files are sent whole instead of through
the v4.5 value-density window. The window's bare-number density score is won by
appendix raw-measurement logs, not the BS 4142 results tables, so it cropped out
exactly the values the noise_* fields need (caught on Croydon 25/03516/CONR:
sound power / specific / rating / correction all null, and a raw appendix Leq of
42.4 reported as noise_background_night_db when the report's Table 8 modal LA90
night backgrounds are 33/31). SOUND_FILE_CHARS 18k -> 60k (largest staged report
is 59,650 chars), MAX_DOC_CHARS 50k -> 80k so whole reports don't crowd out the
officer report / decision notice. ~5% more input tokens on the 30-app pilot.

v4.8 (remaining review-comment fixes, +3 fields): `policies_cited` (verbatim
development-plan policy identifiers the decision leans on), `hp_refusal_ground`
(WHY the HP itself was unacceptable — only when hp_component_stance=unacceptable,
Python-normalised to null otherwise), `alternative_siting_discussed` (LPA
requested / applicant defended alternative HP locations — the recurring
heritage-case dynamic). The `applicant_acoustic_mitigations` value `relocation`
is renamed `siting_choice` (covers both moved units and deliberately quiet
siting of new ones), and a prompt rule standardises NMAs refused as material:
refused + reasons=["other"] + stance=not_separately_assessed unless the LPA
opined on the HP's merits.

v4.7 (review-comment fixes, no new fields): adds `wind_turbine` to the
bundled_works vocab (paired with the includes_wind_turbine boolean) and a
grounding rule for acoustic_standards_cited — a standard counts only when the
documents NAME it; a local-plan policy number or a bare "MCS-certified
installer" mention does not (the pilot's most-repeated reviewer correction).
Companion script scripts/llm/verify_quotes.py greps the verbatim-quote fields
back against the staged texts to catch hallucinated quotes.

v4.6 adds `unit_size_volume` to the shared council_considerations /
council_refusal_reasons vocab: the unit's physical size/volume/bulk as a topic
the LPA weighed — both Class G 0.6 m³ volume-limit checks (common in LDC/CPU
delegated reports) and merits-based bulk reasoning. Motivated by the PDR scoping
question on raising the flats volume limit from 0.6 to 1.5 m³.

v4.5 overhauls the noise block: clearer `noise_*` field names and a full BS 4142
decomposition. The single `val_sound_level_db` splits into specific level +
character correction + rating level; `sound_background_db` splits into day/night
LA90; a signed `noise_exceedance_db` and a `noise_limit_relative` ("background-5")
are added. `sound_assessment_status` becomes `noise_assessment_outcome`, and a new
orthogonal `noise_assessment_method` (measured_on_site / modelled_from_spec /
asserted_only) distinguishes a real on-site survey from a desk calc from a bare
"complies with BS 4142" assertion ("assumed pass"). Sound-ranked files are now fed
through a value-density window (see results_window) so the dB tables — which sit
past the per-file head-clip in long acoustic reports — actually reach the model.

v4.4 adds council_considerations: an outcome-independent list of every material
consideration the LPA weighed (fires on approvals too, e.g. "no harm to character"),
of which the refusal-only council_refusal_reasons is the subset that drove a refusal.

v4.3 decomposes the LBC field: the single `lbc_outcome` enum is replaced by
`lbc_required` + `lbc_decision` + `lbc_reference`, treating Listed Building Consent
as a separate consent regime from planning permission, and `listed_status` is pinned
to the application building's own listing (not neighbouring setting assets).

v4.2 first added a sound decomposition (fields since renamed in v4.5, above):
separates the unit's source sound power, the predicted level at the receptor, the
background level + its basis (assumed vs measured), and the permitted limit + its
basis (national fixed MCS-020 PD limit -- 42 dB pre-20-Sep-2025, 37 dB under
MCS-020(a) after -- vs site-relative BS 4142 vs LA local-plan).

Reads:
  - {SAMPLE_ROOT}/sample.csv  (50 uids picked from recipe_b)
  - /root/full_corpus_texts/summary.csv  (manifest with document_type per file)
  - /root/full_corpus_texts/texts/...     (extracted text files)

Writes (OUT_DIR keyed by HP_RUN_TAG, e.g. v4 -> schema_v4_50):
  - _local/llm_pilot/schema_<tag>_50/results.json
  - _local/llm_pilot/schema_<tag>_50/results.csv

Differences vs classify_sample_100.py:
  - Re-ranks files per uid using sound+heritage tiers (not the pilot's 5-tier).
  - Per-file char cap lifted from 5,000 -> 12,000.
  - Total budget lifted from 20,000 -> 50,000.
  - Cap on files per app lifted from 8 -> 10.
  - 56-field structured output instead of 6-field relevance classifier.
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

ROOT = Path(__file__).resolve().parents[2]
SAMPLE_ROOT = Path(os.environ.get("HP_SAMPLE_ROOT") or (ROOT / "_local/llm_pilot/schema_v1_50/staging"))
SAMPLE_CSV = SAMPLE_ROOT / "sample.csv"
SELECTION_JSON = SAMPLE_ROOT / "selection.json"
TEXTS_DIR = SAMPLE_ROOT / "texts"

MODEL = os.environ.get("HP_MODEL", "gpt-4.1-mini")
OUT_DIR = ROOT / f"_local/llm_pilot/schema_{os.environ.get('HP_RUN_TAG', 'v2')}_50"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PER_FILE_CHARS = 12_000
# Sound-ranked files are sent whole (up to SOUND_FILE_CHARS). v4.5-v4.8 used a
# value-density window instead, but appendix raw-measurement logs out-dense the
# BS 4142 results tables, so the window locked onto the wrong region (Croydon
# 25/03516/CONR: missed sound power / rating / correction tables, and reported a
# raw appendix Leq as the night background). At gpt-4.1-mini prices the whole
# report costs ~5% more input than the window did. 60k covers the largest report
# seen in staging (59,650 chars).
SOUND_FILE_CHARS = 60_000
# Decision/officer-ranked files get the same whole-file treatment (v4.10): the
# 12k head-clip cut delegated reports exactly before their GPDO clause-by-clause
# analysis (Caerphilly 22/0580/CLPU lost the whole Class G section -> wrong
# refusal reason) and before late officer-report corrections (East Herts
# 3/25/0298/FUL: the confirmed Aro Therm Plus unit sat at char ~21k of a 35k
# report and the model extracted the superseded Daikin from the site plan).
# 40k covers the largest officer report seen in staging (35,076 chars).
REPORT_FILE_CHARS = 40_000
MAX_DOC_CHARS = 150_000
MAX_FILES = 10
MAX_WORKERS = 6

# === Ranker ===
DECISION_TYPES = {"decision", "decision notice", "decision letter", "recommendation and reasons report"}
OFFICER_TYPES = {
    "officer report",
    "officer reports",
    "officers report",  # East Herts spelling — missing it left the officer report at rank 9 / 12k clip
    "officer's report",
    "delegated report",
    "report of handling",
    "delegated officer report",
    "case officer report",
    "application committee / delegated report",
    "committee report",
}
REPORT_HINT_TYPES = {"recommendation", "case report", "planning report"}
SOUND_TYPES = {
    "acoustic report",
    "acoustic assessment",
    "noise assessment",
    "sound assessment",
    "noise report",
    "noise impact assessment",
}
HERITAGE_TYPES = {
    "heritage statement",
    "heritage impact assessment",
    "design and access statement",
    "design & access statement",
    "heritage and design statement",
}

DECISION_FN = re.compile(
    r"decision[_ ]notice|decision[_ ]letter|refus(?:al|ed)?|permission|granted|approval|approved", re.I
)
OFFICER_FN = re.compile(
    r"officer|delegated|report[_ ]of[_ ]handling|report[_ ]recommendation|case[_ ]officer|committee[_ ]report", re.I
)
SOUND_FN = re.compile(
    r"acoustic|noise[_ ]?(assessment|report|impact)|sound[_ ]assessment|bs[_ ]?4142|bs[_ ]?8233|mcs[_ ]?020", re.I
)
HERITAGE_FN = re.compile(r"heritage|design[_ ]?and[_ ]?access|listed[_ ]?building", re.I)
HP_FN = re.compile(r"(air[_ ]source|heat[_ ]pump|\bashp\b|brochure)", re.I)


def rank(doctype: str, fname: str) -> tuple[int, int]:
    """Lower = higher priority. Returns (primary, tiebreak)."""
    t = (doctype or "").strip().lower()
    if t in DECISION_TYPES:
        return (0, 0)
    if t in OFFICER_TYPES:
        return (1, 0)
    if t in REPORT_HINT_TYPES:
        return (2, 0)
    if t in SOUND_TYPES:
        return (3, 0)
    if t in HERITAGE_TYPES:
        return (4, 0)
    if DECISION_FN.search(fname):
        return (5, 0)
    if OFFICER_FN.search(fname):
        return (6, 0)
    if SOUND_FN.search(fname):
        return (7, 0)
    if HERITAGE_FN.search(fname):
        return (8, 0)
    # Rank 9 with HP-specific tiebreaker
    return (9, 0 if HP_FN.search(fname) else 1)


SOUND_RANKS = {3, 7}  # SOUND_TYPES doctype (3) or SOUND_FN filename match (7)
REPORT_RANKS = {0, 1, 2, 5, 6}  # decision notices + officer/delegated reports
# Ranks whose loss/truncation is worth a loud warning: every decision, officer
# and sound document. Heritage statements and rank-9 leftovers clip silently.
CRITICAL_RANKS = REPORT_RANKS | SOUND_RANKS


# === 56-field v4.5 schema (see _local/docs/llm_extraction_schema.md) ===
def ENUM(*xs):
    return {"type": "string", "enum": list(xs)}


def ENUM_NULL(*xs):
    return {"type": ["string", "null"], "enum": list(xs) + [None]}


def LIST_OF(*xs):
    return {"type": "array", "items": {"type": "string", "enum": list(xs)}}


def LIST_OF_NULL(*xs):
    return {"type": ["array", "null"], "items": {"type": "string", "enum": list(xs)}}


T0 = {
    "hp_relevance": ENUM("hp_relevant", "hp_incidental", "mixed"),
    # v4.12: "ldc" removed from this enum — it named a ROUTE, not an outcome, and
    # gpt-5.4-mini used it for every LDC app (refused certificates included), which
    # then tripped the refusal-reason normaliser. The route lives in
    # application_type; a refused certificate is "refused", a granted one "approved".
    "decision_outcome": ENUM(
        "approved",
        "refused",
        "withdrawn",
        "invalid",
        "prior_approval_granted",
        "prior_approval_refused",
        "prior_approval_not_required",
        "pending",
        "other",
    ),
    "hp_component_stance": ENUM(
        "acceptable",
        "acceptable_with_condition",
        "unacceptable",
        "not_separately_assessed",
        "unknown",
    ),
    "hp_refusal_ground": ENUM_NULL("noise", "appearance", "heritage", "siting", "other"),
    "summary": {"type": "string"},
    "key_evidence_quote": {"type": "string"},
    "council_refusal_reasons": LIST_OF_NULL(
        "amenity",
        "character_appearance",
        "heritage",
        "sound_noise",
        "highways",
        "ecology",
        "setback_boundary",
        "unit_size_volume",
        "design_info_insufficient",
        "policy_conflict",
        "other",
    ),
    "council_refusal_quote": {"type": ["string", "null"]},
    "council_considerations": LIST_OF_NULL(
        "amenity",
        "character_appearance",
        "heritage",
        "sound_noise",
        "highways",
        "ecology",
        "setback_boundary",
        "unit_size_volume",
        "design_info_insufficient",
        "policy_conflict",
        "other",
    ),
    "policies_cited": {"type": ["array", "null"], "items": {"type": "string"}},
    "application_type": ENUM(
        "full",
        "householder",
        "prior_approval",
        "ldc",
        "lbc",
        "non_material_amendment",
        "reserved_matters",
        "condition_discharge",
        "other",
        "unknown",
    ),
    "public_objections_received": {"type": ["boolean", "null"]},
    "n_public_objections": {"type": ["integer", "null"]},
    "public_objections_grounds": LIST_OF_NULL("visual_amenity", "noise", "overlooking_privacy", "heritage", "other"),
}

T1 = {
    "hp_type": ENUM_NULL("a2w", "a2a", "gshp", "wshp", "hybrid", "ac", "unknown"),
    "dwelling_type": ENUM_NULL(
        "detached",
        "semi",
        "mid_terrace",
        "end_terrace",
        "flat",
        "duplex",
        "bungalow",
        "maisonette",
        "new_build",
        # --- non-domestic building types (pairs with primary_planning_trigger=non_domestic) ---
        "commercial",  # retail / office / hospitality (Use Class E)
        "industrial",  # storage / manufacturing / warehouse (B2/B8)
        "institutional",  # school, healthcare, community, place of worship (Use Class F)
        "agricultural",  # farm buildings, agricultural holdings
        "mixed_use",  # combined residential + commercial
        "other",
        "unknown",
    ),
    "building_age": {"type": ["string", "null"]},
    "building_age_evidence": {"type": ["string", "null"]},
    "hp_placement": ENUM_NULL("front", "rear", "side", "roof", "gable", "courtyard", "multiple", "unknown"),
    "hp_mounting_type": ENUM_NULL("ground", "wall", "roof", "mixed", "unknown"),
    "n_hp_units": {"type": ["integer", "null"]},
    "n_dwellings_served": {"type": ["integer", "null"]},
    "hp_manufacturer": {"type": ["string", "null"]},
    "hp_model": {"type": ["string", "null"]},
    "hp_rated_output_kw": {"type": ["number", "null"]},
    "designations": LIST_OF_NULL("conservation_area", "article_4", "aonb", "national_park", "world_heritage"),
    "listed_status": ENUM_NULL("grade_1", "grade_2_star", "grade_2", "curtilage", "none", "unknown"),
    "lbc_required": ENUM_NULL("required", "not_required", "unclear"),
    "lbc_decision": ENUM_NULL("granted", "granted_with_conditions", "refused", "pending", "withdrawn"),
    "lbc_reference": {"type": ["string", "null"]},
    "install_above_ground_floor": {"type": ["boolean", "null"]},
    "install_on_principal_elevation": {"type": ["boolean", "null"]},
    "install_fronts_highway": {"type": ["boolean", "null"]},
    "install_on_pitched_roof": {"type": ["boolean", "null"]},
    "appearance_concern_level": ENUM_NULL(
        "not_raised", "raised_not_decisive", "addressed_by_condition", "reason_for_refusal"
    ),
    "includes_wind_turbine": {"type": ["boolean", "null"]},
    "alternative_siting_discussed": {"type": ["boolean", "null"]},
    "val_setback_from_edge_m": {"type": ["number", "null"]},
    "val_distance_to_boundary_m": {"type": ["number", "null"]},
    "val_distance_to_receptor_m": {"type": ["number", "null"]},
    "noise_assessment_outcome": ENUM_NULL("not_required", "pass", "fail", "submitted_no_outcome", "not_mentioned"),
    "noise_assessment_method": ENUM_NULL("measured_on_site", "modelled_from_spec", "asserted_only", "not_stated"),
    "acoustic_standards_cited": LIST_OF_NULL(
        "mcs_020", "mcs_020a", "mcs_020b", "bs_4142", "bs_8233", "iso_9613", "iso_1996", "bs_7445", "cieh_ioa_guidance"
    ),
    "noise_source_power_db": {"type": ["number", "null"]},
    "noise_specific_level_db": {"type": ["number", "null"]},
    "noise_character_correction_db": {"type": ["number", "null"]},
    "noise_rating_level_db": {"type": ["number", "null"]},
    "noise_background_day_db": {"type": ["number", "null"]},
    "noise_background_night_db": {"type": ["number", "null"]},
    "noise_exceedance_db": {"type": ["number", "null"]},
    "noise_limit_db": {"type": ["number", "null"]},
    "noise_limit_relative": {"type": ["string", "null"]},
    "noise_limit_basis": ENUM_NULL(
        "mcs_020_pd_limit",
        "bs_4142_background_relative",
        "la_local_plan",
        "bs_8233_internal",
        "other",
        "not_stated",
    ),
    "applicant_acoustic_mitigations": LIST_OF_NULL(
        "barrier",
        "enclosure",
        "louvre",
        "quiet_mode",
        "low_noise_unit",
        "absorption",
        "anti_vibration",
        "siting_choice",
    ),
    "noise_nuisance_mentioned": {"type": ["boolean", "null"]},
    "n_conditions": {"type": ["integer", "null"]},
    "hp_specific_conditions": {"type": ["array", "null"], "items": {"type": "string"}},
    "condition_types": LIST_OF_NULL(
        "noise_threshold",
        "noise_post_install_check",
        "noise_maintenance",
        "standards_compliance",
        "colour_or_screening",
        "relocation_or_position",
        "accord_with_plans",
        "heritage_specific",
        "commissioning_only",
        "other",
    ),
    "bundled_works": LIST_OF_NULL(
        "solar_pv",
        "battery",
        "ev_charger",
        "wind_turbine",
        "extension",
        "loft",
        "outbuilding",
        "garage_conversion",
        "garden_room",
        "replacement_dwelling",
        "cylinder",
        "demolition",
        "alteration",
        "change_of_use",
        "conservatory_or_porch",
        "outhouse",
    ),
    "primary_planning_trigger": ENUM_NULL(
        # --- HP-driven triggers: the HP itself is why permission was needed ---
        "conservation_area",
        "article_4",
        "protected_landscape",  # AONB / National Park / World Heritage Site / SSSI
        "listed",
        "curtilage_listed",
        "flat",
        "above_ground_floor",
        "within_1m_boundary",
        "oversized",
        "mcs_noise_fail",
        "front_elevation",
        "wind_turbine_combo",
        "multiple_units",
        "pitched_roof",
        "amenity",
        # --- non-HP triggers: the HP is NOT why permission was needed ---
        "bundled_development",  # driven by extension/conversion/demolition; HP incidental
        "new_build",  # new or replacement dwelling; HP part of original build
        "non_domestic",  # commercial/industrial/non-dwellinghouse; Class G PD inapplicable
        "retrospective",  # retention of an already-installed HP
        # --- catch-alls ---
        "other",
        "unknown",
    ),
}

SCHEMA = {
    "type": "object",
    "properties": {**T0, **T1},
    "required": list(T0.keys()) + list(T1.keys()),
    "additionalProperties": False,
}

SYSTEM_PROMPT = textwrap.dedent("""
    You are extracting structured fields from UK planning-application documents
    (description + ranked extracted text from decision notices, officer reports,
    acoustic reports, heritage statements, etc.) for an air-source heat pump
    (ASHP) research corpus.

    Output a JSON object matching the schema. Rules:

    1) FIRST decide `hp_relevance`:
       - hp_relevant: decision rationale turns on the HP itself
       - mixed: bundled with substantial other works AND the HP is one of several
         distinct decision grounds
       - hp_incidental: HP is part of the application but the decision is driven
         by other works (extension, listed building, change of use, etc.)

    2) Attempt ALL fields for EVERY app, regardless of hp_relevance. If the
       documents are silent on a field, set it to null (or "unknown" where the
       enum has that value, or [] for arrays). Do NOT invent values.

    3) ACTOR matters — three different parties, NEVER conflate them:
       - council_* = the LPA's own decision / reasoning.
       - public_*  = third-party / neighbour representations.
       - applicant_* = what the applicant put forward in the application materials.

    4) `council_refusal_reasons` / `council_refusal_quote`: populate ONLY when the
       decision is a refusal, and ONLY from the LPA's stated reasons.
       - Information-insufficiency refusals: tag design_info_insufficient AND the
         TOPIC of the missing information — "insufficient information to assess
         the noise impacts" => ["design_info_insufficient", "sound_noise"].
       - GPDO distance-rule failures: a Class G boundary/curtilage rule failure
         ("within 3 metres of the boundary of the curtilage", "within 1 metre of
         the boundary") is setback_boundary, NOT unit_size_volume —
         unit_size_volume is reserved for the unit's physical size/volume/bulk
         (e.g. the 0.6 cubic metre check).

    4b) `council_considerations`: the controlled-vocab list of EVERY material
        consideration the LPA itself substantively weighed in reaching its
        decision — POPULATE REGARDLESS OF OUTCOME, approvals included. Each value
        names the TOPIC the council assessed, NOT its verdict: include a topic when
        the officer report / decision reasons about it on the merits, whether the
        finding was harm, no harm, or acceptable-subject-to-condition. Example: an
        approval whose officer report concludes "the unit would not harm the
        character or appearance of the building and raises no highway safety
        concerns" => ["character_appearance", "highways"]. Draw only from the same
        vocab as council_refusal_reasons, reading the negatively-framed values as
        TOPICS: design_info_insufficient = design/information sufficiency,
        policy_conflict = development-plan policy compliance. EXCLUDE topics that
        merely appear in a consultee header or boilerplate but are not actually
        reasoned about. On a refusal, council_refusal_reasons MUST be a subset of
        council_considerations.
        - unit_size_volume: the LPA weighed the UNIT's physical size / volume /
          bulk. Two forms BOTH count: (a) a GPDO Class G volume-limit check —
          the officer measures or compares THIS unit against the 0.6 cubic metre
          PD limit (typical in LDC/CPU delegated reports); (b) merits reasoning
          about the unit's size, scale or bulk. Boilerplate trap: the bare
          phrase "0.6 cubic metres" quoted as part of the legislation text,
          with no application to this unit, does NOT count. Size-of-unit
          reasoning that is purely visual ("a bulky addition harming the
          street scene") may ALSO warrant character_appearance — tag both.

    5) PUBLIC REPRESENTATIONS — read the "Public Responses / Representations /
       Consultation" section:
       - public_objections_received: were objections/representations received from
         neighbours or the public. EXCLUDE statutory consultee responses
         (Environmental Health, Highways, Conservation Officer, etc.).
       - n_public_objections: the count when stated. Distinguish a real count
         ("one representation received from local residents") from a boilerplate
         template line ("...any representations that may have been received..."),
         which is NOT a count. Count objections, NOT letters of support. 0 is a
         valid, meaningful answer.
       - public_objections_grounds: what the public objected ABOUT. Distinct from
         council_refusal_reasons (different actor; populates regardless of outcome).

    6) `application_type`: the formal planning route. Infer from the application
       reference-code suffix and the description, e.g. FUL/FULL=full,
       HH/HOUS=householder, PA/PNH/"prior approval"=prior_approval, LDC/CLD/CLU=ldc,
       LBC=lbc, NMA=non_material_amendment, RM=reserved_matters,
       DOC/DISCON/CND/"discharge of conditions"/"approval of details"=condition_discharge.
       Use "unknown" if not determinable.

    7) `applicant_acoustic_mitigations`: acoustic mitigation features the APPLICANT
       put forward (the scheme isn't built yet — these are proposals). Council-imposed
       conditions live separately in `condition_types`.
       GROUNDING GATE: include a value ONLY when a document presents that feature
       AS a noise measure — if you cannot point to acoustic reasoning attached to
       the feature, leave it out.
       - siting_choice = the unit was located to the rear / away from receptors
         AS A NOISE MEASURE — covers both relocating an existing unit and
         deliberately quiet siting of a new one. A unit that merely happens to be
         at the rear, or a distance figure quoted in a noise calculation, does
         NOT count.
       - low_noise_unit = a low-noise model/configuration selected for acoustic
         reasons (e.g. "XL super low noise configuration ... reduces break-out
         noise by 12 dB(A)").
       - absorption = acoustic absorptive panels / lining of surrounding surfaces.

    8) `bundled_works`: controlled-vocab list of other works bundled with the HP.
       Use the enum values; pick the closest or "other" if nothing matches.
       A bundled wind turbine goes in this list as "wind_turbine" AND sets
       includes_wind_turbine=True — keep the two consistent.

    9) `decision_outcome`: reflect what the documents say. Use the raw
       planning_decision string supplied as a hint, but confirm against the text.
       This is the APPLICATION-level outcome. The application ROUTE never goes
       here (it lives in application_type): an LDC/CPU whose certificate was
       refused is "refused"; a certificate granted is "approved".

    9b) `hp_component_stance`: the HP-ELEMENT verdict — the LPA's / consultee's view
        of the heat pump ITSELF, DECOUPLED from decision_outcome. Read officer
        reports and consultee responses (esp. Environmental Health), NOT just the
        decision notice, and capture this even when the application was refused for
        UNRELATED reasons:
        - acceptable             : HP element fine, no HP-specific condition needed.
        - acceptable_with_condition: HP fine SUBJECT TO a condition (e.g. EH "no
          objection to the ASHP subject to a noise condition"). Counts even if that
          condition was only RECOMMENDED on an app refused for other reasons.
        - unacceptable           : the HP itself is found unacceptable / is a reason
          (or recommended reason) for refusal — on noise or appearance grounds.
        - not_separately_assessed: docs don't evaluate the HP on its own merits.
        - unknown                : can't tell from the documents.
        Worked example: app refused because of bats, but EH had "no objection to the
        ASHP subject to a noise condition" => decision_outcome=refused,
        hp_component_stance=acceptable_with_condition.
        TWO HARD RULES:
        - If the heat pump is the ENTIRE proposal (the description is just the HP
          install), the application-level verdict IS the verdict on the HP —
          not_separately_assessed is impossible. A refusal of an HP-only
          application => unacceptable, INCLUDING refusals for insufficient
          information about the HP's impacts.
        - If ANY stated refusal reason names the heat pump or its noise / siting /
          appearance impacts, the stance is unacceptable — even when other works
          attracted their own refusal reasons (e.g. an LDC refused on both an
          outbuilding ground AND the HP's Class G boundary rule => unacceptable,
          because the certificate was refused partly on the HP's own siting).
          "Names the heat pump" includes refusal reasons aimed at the PLANT /
          EQUIPMENT the HP is part of ("the proposed plant equipment would harm
          the appearance...") — when the refused plant includes the HP, the HP
          was refused with it.

    9c) `hp_refusal_ground`: ONLY when hp_component_stance=unacceptable — WHY the
        HP ITSELF was found unacceptable: noise, appearance (visual/character),
        heritage (harm to listed building / conservation area significance),
        siting (location/proximity objection not reducible to the others), other.
        Pick the dominant ground. This is HP-specific — application-level refusal
        topics stay in council_refusal_reasons. null whenever the stance is not
        unacceptable (enforced in post-processing).
        When the HP was refused for INSUFFICIENT INFORMATION, the ground is the
        TOPIC of the missing information ("insufficient information to assess the
        noise impacts" => noise). A GPDO siting-rule failure (Class G boundary /
        elevation rule) => siting.

    9d) NMA REFUSED AS MATERIAL: when a non_material_amendment application is
        refused BECAUSE the LPA judges the change material (s96A), often without
        ruling on the planning merits at all: decision_outcome=refused;
        council_refusal_reasons=["other"] PLUS any merits grounds the LPA did
        state; council_refusal_quote = the materiality finding; and
        hp_component_stance=not_separately_assessed UNLESS the LPA actually
        opined on the HP's own merits.

    10) `key_evidence_quote`: ONE verbatim sentence from the supplied text
        (≤300 chars) that best supports your overall picture of the case.

    11) `hp_specific_conditions`: short paraphrases of conditions that specifically
        reference the heat pump. Each ≤120 chars. Empty list if none.

    12) Numeric fields (val_*_m setbacks, noise dB, counts): capture only values
        the documents explicitly state. Never infer, and NEVER compute a value
        yourself (do not add a penalty to a specific level to produce a rating
        level, and do not subtract levels to produce an exceedance — if the
        documents don't state the number, it is null). The noise dB quantities are
        decomposed in 12c — keep them separate and do not cross-fill one into another.
        The three distance fields are distinct — do not reuse one figure across them:
        - val_distance_to_boundary_m: unit to the SITE/CURTILAGE BOUNDARY only
          (the GPDO Class G test). A distance to a neighbouring window, façade,
          garden or dwelling is NOT a boundary distance.
        - val_distance_to_receptor_m: unit to the nearest NOISE-SENSITIVE RECEPTOR
          (neighbouring window / façade / premises) — the figure acoustic reports
          usually state.
        - val_setback_from_edge_m: a roof-mounted unit's setback from the roof
          edge / parapet. Null for ground/wall installs.

    12c) NOISE DB DECOMPOSITION — a BS 4142 / MCS-020 assessment exposes several
        distinct dB quantities. Capture only stated values:
        - noise_source_power_db: the unit's A-weighted SOUND POWER level (Lw) from
          the spec/brochure (e.g. "64 dB(A) sound power", "Lw"). Source emission,
          independent of distance/screening — NOT the level at a neighbour.
        - noise_specific_level_db: the BS 4142 "Specific Sound Level" (LAs) — the
          level attributable to the heat pump AT the nearest receptor, BEFORE any
          character correction.
        - noise_character_correction_db: the +dB penalty BS 4142 adds to the
          specific level for acoustic character (tonality / impulsivity /
          intermittency), AS APPLIED by the assessment. 0 if the report explicitly
          applies none; null if not discussed. NOT character corrections: a
          correction for façade REFLECTIONS, a distance correction, or a penalty a
          guidance document says SHOULD apply but no assessment actually applied.
        - noise_rating_level_db: the BS 4142 "Rating Level" (LAr) = specific level +
          character correction, AS STATED by an assessment — never your own
          arithmetic. If no document states a rating-level figure, this field MUST
          be null even when a specific level and a possible penalty are both
          stated (EH letter giving "53 dB at the garden" plus "a penalty of 5dB
          should be applied" => specific 53, correction 5, rating NULL — do not
          output 58). THIS is the figure compared against the limit / background.
          MCS-020 calculator mapping: the STEP 6 "sound pressure level of the heat
          pump at the assessment position" is noise_specific_level_db; the FINAL
          RESULT (after combining with the background and the decibel correction)
          is noise_rating_level_db — do NOT put the final result in
          noise_specific_level_db.
        - noise_background_day_db / noise_background_night_db: the representative
          background sound level LA90 for DAYTIME (07:00-23:00) and NIGHT-TIME
          (23:00-07:00). For a 24h-running heat pump the NIGHT figure usually binds.
          Capture both when the survey reports them. A single representative figure
          goes to the period the report ties it to (night if it says "night-time",
          else day); an MCS-020 "assumed background" (commonly 40 dB(A), no survey)
          goes to night unless stated otherwise.
        - noise_exceedance_db: the SIGNED difference rating level − binding
          background (negative => rating below background => "Low Impact"). Capture
          the stated difference (e.g. "-4 dB", "+5 dB") when given.
        - noise_limit_db: the NUMERIC limit compared against, when a single number is
          stated (e.g. 42 dB(A) MCS-020 PD limit). Null when the limit is purely relative.
        - noise_limit_relative: the limit when expressed RELATIVE to background with
          no single number, as a short string: "background-5" (5 dB below background),
          "background" (at/below), "background+5". Null if an absolute number is given.
        - noise_limit_basis: WHICH limit regime applies:
          * mcs_020_pd_limit           — national fixed MCS-020 PD limit (not
            LA-specific): 42 dB(A) pre-20-Sep-2025, 37 dB(A) under MCS-020(a) after.
            Put the number in noise_limit_db; this basis is version-neutral.
          * bs_4142_background_relative — limit relative to background (use with
            noise_limit_relative).
          * la_local_plan              — limit set by the LPA's own plan policy/SPG.
          * bs_8233_internal           — an internal-room limit (BS 8233 / WHO).
          * other / not_stated.

    12d) NOISE OUTCOME vs METHOD — two orthogonal fields:
        - noise_assessment_outcome: the pass/fail STATUS — not_required, pass, fail,
          submitted_no_outcome, not_mentioned. PRECEDENCE: when the council / EH
          states its own verdict on noise compliance, that verdict IS the outcome —
          an applicant report claiming a pass that EH recalculates or rejects as a
          fail => fail. Only when the council is silent use the assessment's own
          conclusion. not_mentioned means noise was never discussed at all; when
          an assessment WAS submitted but the council neither accepted nor decided
          on it (e.g. refused for insufficient noise information without ruling on
          the submitted calc), use submitted_no_outcome.
        - noise_assessment_method: HOW the compliance position was reached
          (orthogonal — an asserted_only claim can still pass or attract a condition).
          Classify by the BACKGROUND's provenance — the plant-specific level is
          almost always calculated (the unit usually isn't installed yet), and that
          does NOT make a surveyed assessment "modelled":
          * measured_on_site   — an actual on-site background SURVEY/measurement was
            taken (BS 4142 proper; LA90 logged over a period), even when the
            plant-specific level is calculated from spec data. Strongest rigour signal.
          * modelled_from_spec — level CALCULATED/modelled from the manufacturer's
            sound-power data with an ASSUMED/standard background (MCS-020 calculator,
            or SoundPLAN/ISO-9613 modelling) — no on-site background measurement.
          * asserted_only      — a standard (BS 4142 / MCS-020) is NAMED and
            compliance CLAIMED, but the documents show no measurement AND no
            calculation ("the unit complies with BS 4142").
          * not_stated         — no noise method described.
        Worked example A (Lambeth BS4142, measured): on-site survey, rep. night
        background 37 dB LA90; Remeha ASHP Lw 64; specific 30 + 3 dB intermittency =
        rating 33; 33 − 37 = −4 → Low Impact; condition limits plant to 5 dB below
        background => noise_source_power_db=64, noise_specific_level_db=30,
        noise_character_correction_db=3, noise_rating_level_db=33,
        noise_background_night_db=37, noise_exceedance_db=-4,
        noise_limit_relative="background-5", noise_limit_basis=bs_4142_background_relative,
        noise_assessment_outcome=pass, noise_assessment_method=measured_on_site.
        Worked example B (Cardiff 25/00470/HSE, MCS-020 calculator): source Lw 55,
        assumed background 40, final/rating level 41, limit 42 (MCS-020 PD) =>
        noise_source_power_db=55, noise_rating_level_db=41, noise_background_night_db=40,
        noise_limit_db=42, noise_limit_basis=mcs_020_pd_limit,
        noise_assessment_method=modelled_from_spec, noise_assessment_outcome=pass.

    12e) `acoustic_standards_cited` — GROUNDING GATE: include a standard ONLY when
        the documents NAME that standard (e.g. "BS 4142:2014", "BS4142", "MCS 020",
        "MCS planning standards", "BS 8233", "ISO 9613", "ISO 1996-2:2017",
        "BS 7445", or the CIEH/IOA heat-pump noise guidance — "Joint IOA and CIEH
        heat pump brief", "Heat Pumps, Professional Advice note" =>
        cieh_ioa_guidance). NOT citations:
        - a local-plan / policy reference ("Policy 33 on Noise and Vibration",
          "NPF4 Policy 23") — that is noise_limit_basis=la_local_plan, not a
          standard citation;
        - a bare "MCS" mention with no noise content ("MCS-certified installer",
          "MCS certificate") — MCS certification alone does NOT imply MCS 020;
        - a generic "noise assessment" / "acoustic report" with no named standard.
        If you cannot point to the standard's name in the supplied text, leave it
        out. Empty list when none are named.

    12b) HP UNIT SPECIFICATION — capture the proposed/installed unit's identity
        from brochures/specifications, the description, and officer reports:
        - hp_manufacturer: the brand, normalised (e.g. "Mitsubishi", "Daikin",
          "Vaillant", "Grant", "Panasonic", "Samsung", "Nibe", "Worcester Bosch").
          Null if no brand is named.
        - hp_model: the model name/code VERBATIM as written, including the range
          and any alphanumeric code (e.g. "Ecodan PUZ-WM85VAA", "aroTHERM plus
          10kW", "Aerona³ HPID10R32", "Altherma 3 Monobloc"). Null if no model
          is named.
        - hp_rated_output_kw: the unit's rated thermal output in kW when stated
          (e.g. "10kW" -> 10). This is the UNIT rating, NOT the dwelling
          heat-loss / heat-demand figure unless those are explicitly the same.
          Null if not stated.
        - SUPERSEDED SPECS: when the documents show the proposed unit CHANGED
          during the application (officer report: "the agent confirmed the
          applicant wishes to install X instead"), capture the FINAL confirmed
          unit, not the one on the original drawings. When sources conflict with
          no stated supersession, prefer the officer report / decision over
          applicant drawings, and note that drawings labelled with one rating
          outweigh a single stray figure elsewhere.
        - hp_type — decide a2w vs a2a only from the heat DISTRIBUTION medium:
          radiators / underfloor heating / hot-water cylinder / "wet system" /
          "provide the hot water and domestic heating" => a2w; internal fan coil
          units / warmed-and-cooled AIR / comfort cooling / a retained gas boiler
          doing the wet heating alongside => a2a (or ac if it is purely an air
          conditioner). A bare "air source heat pump" with no medium evidence =>
          unknown — never default to a2w.

    13) install_* booleans are FACTS about where the HP is installed, independent
        of whether the council took issue:
        - install_above_ground_floor: HP at first-floor level, upper storey, or roof.
        - install_on_principal_elevation: HP on the principal (public-facing) elevation.
        - install_fronts_highway: the wall/roof the unit is on faces a PUBLIC HIGHWAY
          (incl. footpath/pavement); EXCLUDES private roads/drives. This is the Class G
          legal trigger. BEWARE the GPDO-boilerplate trap: the phrase "a wall which
          fronts a highway" appears verbatim in the legislation text — extract the
          PROPERTY FACT, not the rule citation. "Visible from the streetscene" => True;
          a wall facing a private drive => False. Prefer the officer's stated conclusion.
        - install_on_pitched_roof: HP mounted on a pitched roof.
        Leave null only if the documents are silent.

    14) `includes_wind_turbine`: True if the application also includes a wind turbine
        (cumulative-noise scenario). When True, "wind_turbine" must also appear in
        bundled_works (rule 8).

    15) `appearance_concern_level` — route VISUAL vs NOISE, and grade escalation:
        - not_raised             : visual/appearance not raised
        - raised_not_decisive    : raised but did not drive the outcome
        - addressed_by_condition : units pushed to rear / conditioned for appearance
        - reason_for_refusal     : visual/appearance is an actual stated refusal reason
        ONLY about visual / external-appearance / character OF THE HEAT PUMP /
        its plant equipment — appearance concerns about other bundled elements
        (fenestration, an extension) do NOT count. Noise-driven reasoning goes
        to the sound fields (and council_refusal_reasons=sound_noise), NOT here.
        Neighbour disturbance goes to public_objections_grounds. An officer
        merely assessing visual amenity and finding it acceptable, with no
        concern RAISED by anyone, is not_raised.

    16) `building_age` + `building_age_evidence`: the dwelling's ORIGINAL
        construction date, as a single string — either a 4-digit year ("1936")
        or a hyphenated range ("1837-1901"), earliest year first.
        - Populate ONLY from explicit age language about THIS building. If you
          cannot quote a phrase stating its age, set BOTH fields to null.
        - `building_age_evidence`: the verbatim phrase (≤120 chars) the date was
          taken from. null here ⇒ `building_age` MUST also be null.
        - Convert period/stylistic language to its range deterministically:
          georgian 1714-1837; victorian 1837-1901; edwardian 1901-1914;
          interwar 1918-1939; post_war 1945-1979; modern 1980-2010;
          new_build 2010-2026. Centuries/decades map literally: "17th century"
          -> 1600-1699; "late 19th century" -> 1875-1900; "1930s" -> 1930-1939.
          A single stated year stays a single year ("built in 1936" -> "1936").
        - Use ORIGIN, not later alterations/extensions: "18th century origin with
          19th century alterations" -> "1700-1799" (or the stated "dates from
          1780" -> "1780").
        - NEVER infer age from listed status, conservation-area designation,
          address, or locale. Designation alone -> null.
        - Boilerplate/citation trap: "shall be constructed in accordance with the
          approved plans" is NOT a build date; "Carter 1977" is a citation;
          "P1807ii" is a drawing reference. The evidence must state the BUILDING's
          own age.

    17) `hp_mounting_type`: how the unit is affixed — ground (pad/plinth), wall
        (bracket), roof, mixed (multiple units, different mountings), unknown.
        Orthogonal to `hp_placement` (location front/rear/side).

    18) `n_dwellings_served`: dwellings the HP system serves. Single home = 1;
        communal HP for a block = count of flats served; per-flat array =
        n_hp_units == n_dwellings_served, both > 1.
        `n_hp_units` counts HEAT PUMP units only: an air-conditioning condenser
        or other plant installed ALONGSIDE the HP is not an HP unit ("1 new
        external air conditioner condenser unit, and 1 new ASHP unit" =>
        n_hp_units=1).

    19) `condition_types`: categorical complement to hp_specific_conditions. Tag each
        HP-relevant condition: noise_threshold (numeric limit), noise_post_install_check,
        noise_maintenance, standards_compliance (MCS/BS4142/IoA), colour_or_screening,
        relocation_or_position, accord_with_plans (generic "as approved drawings"),
        heritage_specific, commissioning_only, other.
        ONLY conditions IMPOSED BY THIS DECISION count. NOT conditions: a parent
        permission's conditions quoted in a condition-discharge / variation case;
        conditions a consultee RECOMMENDED on an application that was then
        refused (that signal lives in hp_component_stance=acceptable_with_condition);
        mitigation recommendations inside an acoustic report. A refusal imposes no
        conditions — on any refusal these two fields must be empty (also enforced
        in post-processing).

    20) `primary_planning_trigger`: the SINGLE primary reason THIS application
        required planning permission. Work top-down and STOP at the first match —
        the non-HP routers (a-c) take precedence, because the goal is to separate
        HP-driven applications from ones where the HP is incidental.

        FIRST, follow-on / administrative records have NO trigger of their own:
        if application_type is condition_discharge, non_material_amendment, ldc, or
        reserved_matters (discharge/approval of conditions, approval of details, NMA,
        certificate of lawfulness), or the application is a s73 VARIATION/REMOVAL
        of a condition, the consent was already granted (or is asserted not to be
        needed). Report the PARENT permission's trigger when the documents state
        why that permission was needed (a s73 on a school plant permission =>
        non_domestic); otherwise `unknown`. NEVER invent one.

        OTHERWISE work down these routers:
        (a) new_build       — the proposal is a new or replacement dwelling (or a
            multi-unit scheme) and the HP is part of that original build.
        (b) non_domestic    — the building is not a dwellinghouse (commercial,
            industrial, institutional, agricultural, car park, plant compound), so
            householder PD (GPDO Part 14 Class G) never applied. Record the building
            type in `dwelling_type`, not here.
        (c) bundled_development — a householder/full app whose permission is driven by
            OTHER works (extension, garage/loft conversion, demolition, dormers) with
            the HP tacked on; i.e. hp_relevance is hp_incidental or mixed.
        If none of (a)-(c) apply, the HP itself is the reason permission was needed —
        pick the specific GPDO/policy trigger:
          listed / curtilage_listed; conservation_area; article_4;
          protected_landscape (AONB, National Park, World Heritage Site, SSSI);
          flat; above_ground_floor; pitched_roof; front_elevation;
          within_1m_boundary; oversized (exceeds the volume/size limit);
          mcs_noise_fail (the MCS-020 noise calculation FAILS the PD limit, so
          Class G PD is unavailable — e.g. an applicant's calc sheet stating
          "If NO - the air source heat pump will not be permitted development
          ... Fail at 44dB, 2dB over". Prefer this over a siting value when the
          documents state the noise fail is why the application was made);
          multiple_units (more than one HP unit); wind_turbine_combo;
          amenity (general visual-amenity grounds with no specific GPDO limit cited).
        Use `retrospective` only when the app is explicitly a retention/retrospective
        case AND no specific trigger above is evident.
        Use `unknown` when the HP clearly needed permission but the documents do not
        say why. Reserve `other` for a genuine HP trigger that fits none of the above.

    21) `dwelling_type`: for a HOME use the residential values (detached, semi,
        mid_terrace, end_terrace, flat, duplex, bungalow, maisonette, new_build).
        When the building is NOT a dwelling, use the matching non-domestic value
        rather than `other`: commercial (retail/office/hospitality, Use Class E),
        industrial (storage/manufacturing/warehouse, B2/B8), institutional
        (school/healthcare/community/place of worship, Use Class F), agricultural
        (farm buildings), or mixed_use (residential combined with commercial).
        Reserve `other` for a building that fits none of these.

    22) HERITAGE — `listed_status` and the LBC fields. Listed Building Consent (LBC)
        is a SEPARATE consent regime from planning permission; treat them as two
        independent streams.
        - `listed_status`: the APPLICATION BUILDING's OWN listing — the building the
          works are physically ON. If the property itself is listed, record its grade;
          `curtilage` if it is only curtilage-listed; `none` if it is not listed.
          Do NOT borrow a neighbour's grade: where the application building is
          unlisted but the works harm the SETTING of an adjacent listed building,
          listed_status reflects the application building (often `none` or a lower
          grade) and the setting harm goes to council_refusal_reasons=heritage.
        - `lbc_required`: is LBC needed for THESE works? A listed building does NOT
          automatically need LBC. External plant (e.g. an ASHP on a pad in the yard)
          that does not alter the listed fabric is commonly `not_required`. Use
          `required` when works affect a listed building's special interest,
          `unclear` when the documents don't resolve it. Set null ONLY when
          listed_status is `none`/`unknown` (LBC cannot arise).
        - `lbc_decision`: the LBC stream's OWN outcome (granted / granted_with_conditions
          / refused / pending / withdrawn). null unless an LBC was actually required or
          a separate LBC application was lodged. This is INDEPENDENT of decision_outcome:
          the planning application can be refused while LBC is not required, or vice versa.
        - `lbc_reference`: the linked LBC application reference verbatim if one is named
          (e.g. "UTT/19/2742/LB"); often in the officer report's site-history table.
        Worked example (Uttlesford UTT/19/2431/HHF): property is itself Grade II listed,
        in a conservation area; planning permission REFUSED on harm to the setting of an
        adjacent Grade II* building; the separate LBC app UTT/19/2742/LB was closed
        "listed building consent deemed to be not required" => listed_status=grade_2,
        lbc_required=not_required, lbc_decision=null, lbc_reference="UTT/19/2742/LB",
        decision_outcome=refused, application_type=householder.

    23) `policies_cited`: verbatim development-plan / national-policy identifiers
        the LPA's DECISION OR OFFICER REASONING actually relies on (e.g.
        "NPF4 Policy 23", "LDP2023 Policy P4", "Policy 33", "Local Plan Policy
        DM1", "NPPF paragraph 130"). Max 6, most decision-relevant first.
        Boilerplate trap: decision notices and officer reports often open with a
        long "Relevant Policies" list — include ONLY policies the reasoning or
        the stated reasons actually engage with, not every policy enumerated in
        a header. Policies named in the decision notice's reasons/conditions take
        priority over officer-report discussion. Identifiers verbatim as written.
        null when no policy is relied on / determinable.

    24) `alternative_siting_discussed`: True when the documents show ALTERNATIVE
        locations for the HP were considered — the LPA / conservation officer
        REQUESTED information on alternative locations (ground installation,
        secondary elevation, different roof), or the applicant ASSESSED or
        DEFENDED the chosen location against alternatives. Common in listed-
        building cases. False only when the docs explicitly state alternatives
        were NOT considered; null when the topic never arises.

    Return STRICT JSON. No prose outside the JSON.
""").strip()


def load_selection() -> dict[str, list[dict]]:
    """uid -> list of pre-ranked file records from staging."""
    sel = json.loads(SELECTION_JSON.read_text())
    print(f"Loaded selection for {len(sel)} uids from {SELECTION_JSON}", flush=True)
    return sel


def load_doc_text(selected: list[dict], uid: str = "") -> tuple[str, list[dict]]:
    parts: list[str] = []
    used: list[dict] = []
    budget = MAX_DOC_CHARS
    lost: list[str] = []  # critical-rank files the model won't see in full
    for f in selected:
        r = rank(f["doctype"], f["fname"])[0]
        # text_path is "texts/<rel>"; staging has files under TEXTS_DIR with the same <rel>
        rel = Path(f["text_path"]).relative_to("texts")
        src = TEXTS_DIR / rel
        if not src.exists():
            continue
        if budget <= 200:
            if r in CRITICAL_RANKS:
                lost.append(f"{f['fname']} (rank {r}, dropped: budget exhausted)")
            continue
        text = src.read_text(encoding="utf-8", errors="replace")
        # Acoustic reports scatter their dB figures across results tables and
        # appendices, and officer/delegated reports bury GPDO clause analysis and
        # late spec corrections past any head-clip, so both tiers are sent whole
        # (own caps). Only rank-8/9 leftovers get the plain head-clip.
        if r in SOUND_RANKS:
            cap = SOUND_FILE_CHARS
        elif r in REPORT_RANKS:
            cap = REPORT_FILE_CHARS
        else:
            cap = PER_FILE_CHARS
        snippet = text[: min(cap, budget)]
        if r in CRITICAL_RANKS and len(snippet) < len(text):
            lost.append(f"{f['fname']} (rank {r}, truncated {len(snippet):,}/{len(text):,})")
        parts.append(f"=== {f['fname']} [doctype={f['doctype']}] ===\n{snippet}")
        used.append(
            {
                "fname": f["fname"],
                "doctype": f["doctype"],
                "rank": r,
                "chars": len(snippet),
            }
        )
        budget -= len(snippet)
    if lost:
        print(f"  COVERAGE WARNING {uid}: " + "; ".join(lost), flush=True)
    return "\n\n".join(parts), used


# Conditions are imposed only by a positive decision. On any non-granting outcome
# the concept doesn't apply, so n_conditions is null (not 0). 0 is reserved for a
# genuine unconditional grant, keeping aggregates (mean conditions/approval, % with a
# noise condition) clean without every consumer first filtering on decision_outcome.
GRANTING_OUTCOMES = {"approved", "prior_approval_granted"}

# Refusal reasons exist only on a refusal. They are the outcome-independent
# council_considerations subset that DROVE a refusal, so on any non-refusal they
# must be empty/null — otherwise "share of refusals citing noise" aggregates are
# polluted by non-refused apps. (The consideration still lives in council_considerations.)
REFUSAL_OUTCOMES = {"refused", "prior_approval_refused"}


def normalize_conditions(out: dict) -> dict:
    if out.get("decision_outcome") not in GRANTING_OUTCOMES:
        # A refusal imposes no conditions, so the condition lists must be empty
        # too — the v4.9 pilot confabulated condition_types on 4/10 refused apps
        # from parent permissions and "recommended if approved" passages. (An
        # HP-friendly recommended condition still surfaces via
        # hp_component_stance=acceptable_with_condition.)
        out["n_conditions"] = None
        out["condition_types"] = []
        out["hp_specific_conditions"] = []
    if out.get("decision_outcome") not in REFUSAL_OUTCOMES:
        out["council_refusal_reasons"] = None
        out["council_refusal_quote"] = None
    # hp_refusal_ground names WHY the HP itself was unacceptable; on any other
    # stance the question doesn't arise, so it must be null.
    if out.get("hp_component_stance") != "unacceptable":
        out["hp_refusal_ground"] = None
    return out


def extract_one(client: OpenAI, row: dict, uid_files: dict) -> dict:
    selected = uid_files.get(row["uid"], [])[:MAX_FILES]
    doc_text, used = load_doc_text(selected, uid=row["uid"])

    user_msg = textwrap.dedent(f"""
        AUTHORITY: {row["authority_name"]}
        REFERENCE: {row["reference"]}
        APPLICATION DESCRIPTION: {row["description"]}
        PLANNING DECISION (raw status, hint only): {row["planning_decision"]}
        DECISION DATE: {row["decision_date"]}

        EXTRACTED DOCUMENT TEXT (ranked: decision notice first, then officer
        report, sound, heritage, etc.; decision/officer/sound files sent whole,
        other files capped at {PER_FILE_CHARS:,} chars, total cap
        {MAX_DOC_CHARS:,} chars):
        ---
        {doc_text}
        ---
    """).strip()

    kwargs = dict(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {"name": "hp_schema_v1", "schema": SCHEMA, "strict": True},
        },
    )
    # GPT-5.x reasoning models reject temperature!=1; they take reasoning_effort
    # instead. HP_REASONING_EFFORT=none turns reasoning off for extraction runs.
    effort = os.environ.get("HP_REASONING_EFFORT")
    if effort:
        kwargs["reasoning_effort"] = effort
    else:
        kwargs["temperature"] = 0
    resp = client.chat.completions.create(**kwargs)
    out = normalize_conditions(json.loads(resp.choices[0].message.content))
    out["_files_used"] = used
    out["_usage"] = {
        "prompt_tokens": resp.usage.prompt_tokens,
        "completion_tokens": resp.usage.completion_tokens,
    }
    return out


def main() -> int:
    load_dotenv(ROOT / ".env")
    if not os.environ.get("OPENAI_API_KEY"):
        print("OPENAI_API_KEY not in env", file=sys.stderr)
        return 1
    client = OpenAI()

    rows = list(csv.DictReader(SAMPLE_CSV.open(encoding="utf-8")))
    print(f"Loaded {len(rows)} sample rows.", flush=True)

    uid_files = load_selection()

    print(f"Extracting with {MODEL}...", flush=True)
    results: list[dict] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(extract_one, client, r, uid_files): r for r in rows}
        for i, fut in enumerate(as_completed(futs), 1):
            r = futs[fut]
            try:
                out = fut.result()
            except Exception as exc:
                print(f"  [{i:3d}] ERROR {r['authority_name']}/{r['reference']}: {exc}")
                out = {"_error": str(exc)}
            out["uid"] = r["uid"]
            out["reference"] = r["reference"]
            out["authority_name"] = r["authority_name"]
            out["planning_decision"] = r["planning_decision"]
            out["decision_bucket"] = r["decision_bucket"]
            out["description"] = r["description"]
            out["final_label_recipe_b"] = r.get("final_label", "")
            results.append(out)
            if i % 10 == 0 or i == len(rows):
                print(f"  [{i:3d}/{len(rows)}] {time.time() - t0:.1f}s elapsed", flush=True)

    results_path = OUT_DIR / "results.json"
    results_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nFull results -> {results_path}")

    # Flat CSV for spreadsheet review
    flat_fields = (
        ["uid", "authority_name", "reference", "decision_bucket", "final_label_recipe_b"]
        + list(T0.keys())
        + list(T1.keys())
    )
    csv_path = OUT_DIR / "results.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=flat_fields)
        w.writeheader()
        for r in results:
            row = {k: r.get(k, "") for k in flat_fields}
            for k, v in row.items():
                if isinstance(v, list):
                    row[k] = "|".join(str(x) for x in v)
            w.writerow(row)
    print(f"CSV -> {csv_path}")

    # Cost
    total_in = sum(r.get("_usage", {}).get("prompt_tokens", 0) for r in results if isinstance(r.get("_usage"), dict))
    total_out = sum(
        r.get("_usage", {}).get("completion_tokens", 0) for r in results if isinstance(r.get("_usage"), dict)
    )
    # gpt-4.1-mini pricing: $0.40/M input, $1.60/M output
    cost = total_in / 1_000_000 * 0.40 + total_out / 1_000_000 * 1.60
    print(f"\nToken usage: in={total_in:,} out={total_out:,}  est cost (4.1-mini)=${cost:.4f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
