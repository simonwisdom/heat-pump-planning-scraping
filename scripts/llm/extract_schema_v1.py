"""64-field schema extraction over a stratified sample.

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

MODEL = os.environ.get("HP_MODEL", "gpt-5.4-mini")
OUT_DIR = ROOT / f"_local/llm_pilot/schema_{os.environ.get('HP_RUN_TAG', 'v2')}_50"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PER_FILE_CHARS = 12_000
# Sound-ranked files are sent whole (up to SOUND_FILE_CHARS). An earlier
# value-density window was tried instead, but appendix raw-measurement logs out-dense the
# BS 4142 results tables, so the window locked onto the wrong region (Croydon
# 25/03516/CONR: missed sound power / rating / correction tables, and reported a
# raw appendix Leq as the night background). At gpt-4.1-mini prices the whole
# report costs ~5% more input than the window did. 60k covers the largest report
# seen in staging (59,650 chars).
SOUND_FILE_CHARS = 60_000
# Decision/officer-ranked files get the same whole-file treatment: the
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


# === 64-field schema (see _local/docs/llm_extraction_schema.md) ===
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
    # One-sentence rationale for the hp_relevance call — what the app
    # centrally seeks + whether the HP drew its own scrutiny. Auditable, and
    # check_consistency.py can flag it against stance / refusal / conditions.
    "hp_relevance_basis": {"type": "string"},
    # "ldc" is deliberately not a value in this enum — it names a ROUTE, not an
    # outcome, and gpt-5.4-mini used it for every LDC app (refused certificates
    # included), which tripped the refusal-reason normaliser. The route lives in
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
    # Split from the old combined dwelling_type. building_use_class is the
    # coarse category (carries the non-domestic types); dwelling_type is the
    # residential FORM only, null for non-residential buildings.
    "building_use_class": ENUM_NULL(
        "residential",
        "commercial",  # retail / office / hospitality (Use Class E)
        "industrial",  # storage / manufacturing / warehouse (B2/B8)
        "institutional",  # school, healthcare, community, place of worship (Use Class F)
        "agricultural",  # farm buildings, agricultural holdings
        "mixed_use",  # combined residential + commercial
        "other",
        "unknown",
    ),
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
        "other",
        "unknown",
    ),
    "building_age": {"type": ["string", "null"]},
    "building_age_evidence": {"type": ["string", "null"]},
    "hp_placement": ENUM_NULL("front", "rear", "side", "roof", "gable", "courtyard", "multiple", "unknown"),
    "hp_placement_evidence": {"type": ["string", "null"]},
    "hp_mounting_type": ENUM_NULL("ground", "wall", "roof", "mixed", "unknown"),
    "hp_mounting_type_evidence": {"type": ["string", "null"]},
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
    "install_above_ground_floor": ENUM_NULL("yes", "no", "unknown"),
    "install_on_principal_elevation": ENUM_NULL("yes", "no", "unknown"),
    "install_fronts_highway": ENUM_NULL("yes", "no", "unknown"),
    "install_on_pitched_roof": ENUM_NULL("yes", "no", "unknown"),
    "appearance_concern_level": ENUM(
        # Not_raised + raised_not_decisive collapsed -> not_decisive
        # (the boundary generated all 8/30 of this field's run-to-run noise and
        # carried no reporting weight; the two DECISIVE levels are what matter).
        "not_decisive",
        "addressed_by_condition",
        "reason_for_refusal",
    ),
    "includes_wind_turbine": {"type": ["boolean", "null"]},
    # Collapsed to a plain boolean. The old null/False split was a dead
    # category — every run-to-run disagreement (20/30) was False<->null, never
    # involving True. Only "discussed vs not" carries signal.
    "alternative_siting_discussed": {"type": "boolean"},
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
    "applicant_acoustic_mitigations_evidence": {"type": ["string", "null"]},
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
        # Collapsed to the STRUCTURAL axis only — "why was permission needed
        # at all?" The specific GPDO/policy constraint (conservation_area, listed,
        # above_ground_floor, pitched_roof, within_1m_boundary, multiple_units,
        # wind_turbine_combo, ...) is NOT recorded here: it is already captured,
        # more granularly and less noisily, by `designations`, `listed_status`,
        # the `install_*` booleans, `val_distance_to_boundary_m`, `n_hp_units`, and
        # `includes_wind_turbine`. Reconstruct the constraint view at analysis time.
        "hp_needed_permission",  # the HP ITSELF removed PD rights (a GPDO/policy limit applied)
        "bundled_development",  # dwellinghouse driven by OTHER works; HP incidental/mixed
        "new_build",  # new/replacement dwelling; HP part of original build
        "non_domestic",  # not a dwellinghouse; Class G PD never applied
        "retrospective",  # retention of an already-installed HP, no other trigger
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

    1) Decide `hp_relevance`. Anchor on the APPLICATION and on whether the HP drew
       its OWN scrutiny — do NOT anchor on "what the decision turned on" (many apps
       are withdrawn, non-material amendments, condition discharges or s73
       variations with no merits decision to anchor on). "HP-specific scrutiny"
       means the HP attracted any of: its own assessment (e.g. a noise/acoustic
       assessment or an Environmental Health consultation on the HP), its own
       planning condition, its own objection, or its own refusal ground. Decide
       top-down and STOP at the first match:
       - hp_relevant: the HP — together with works that only exist to enable it
         (louvres, a plant enclosure, acoustic screening, a plant room) — is the
         MAIN or ONLY thing the application seeks; OR the HP is the only element
         that drew HP-specific scrutiny.
       - mixed: the application also seeks substantial OTHER works (extension,
         conversion, dormers, a new/replacement dwelling, change of use) AND the
         HP drew HP-specific scrutiny (assessment / condition / objection /
         refusal ground). "Substantial OTHER works" means building works of that
         kind — NOT other low-carbon kit bundled with the HP: roof-flush solar PV,
         a battery, an EV charger or similar microgeneration do NOT make an app
         "mixed". An application for the HP + its enabling works + such renewables,
         with no extension/conversion/new-dwelling, is hp_relevant, not mixed.
       - hp_incidental: the HP is a minor part of a scheme dominated by other
         works AND the HP drew NO HP-specific scrutiny — no HP assessment, no HP
         condition, no HP objection, no HP refusal ground. A consultee (e.g. a
         Parish/Town Council) merely NOTING possible HP noise while recommending
         approval, with no officer assessment/condition/objection/refusal ground,
         is NOT HP-specific scrutiny — such an app stays hp_incidental.
       Consistency check: if hp_component_stance is `unacceptable`, or
       hp_refusal_ground is set, or hp_specific_conditions is non-empty, or
       noise_assessment_outcome is `pass`/`fail`, then the HP drew scrutiny — the
       app is hp_relevant or mixed, NEVER hp_incidental.
    1b) `hp_relevance_basis`: in ONE sentence (<=160 chars) state the deciding
        factor — what the application centrally seeks and whether the HP drew its
        own scrutiny. E.g. "HP one of several works but drew its own noise
        condition => mixed"; "HP minor part of extension scheme, no HP-specific
        assessment => incidental"; "application is solely for the HP + louvres =>
        hp_relevant".

    2) Attempt ALL fields for EVERY app, regardless of hp_relevance. If the
       documents are silent on a field, set it to null (or "unknown" where the
       enum has that value, or [] for arrays). Do NOT invent values.

    2b) ADJACENCY TRAP (applies to every field): never read a value off a phrase
        that is actually about a DIFFERENT attribute. The honest answer when the
        only support is an adjacent-but-different fact is null / "unknown" / [].
        In particular:
        - a distance-to-boundary / distance-to-neighbour figure ("over 2m from
          the rear boundary") is about PROXIMITY, not hp_placement — it does not
          establish front/rear/side;
        - a threshold or limit the unit "must not exceed" is a LIMIT
          (noise_limit_db / noise_limit_relative), NOT a measured emission
          (noise_source_power_db) or receptor level (noise_specific_level_db);
        - noise / vibration from demolition or construction PLANT is NOT the heat
          pump's noise;
        - a battery / PV-inverter / EV-charger brand (e.g. "Tesla Powerwall") is
          NOT the HP's manufacturer or model;
        - siting done to reduce VISUAL impact is NOT an acoustic mitigation.

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
         neighbours or the public. EXCLUDE statutory/standing consultee responses —
         Environmental Health, Highways, Conservation Officer, AND a Parish/Town/
         Community Council or a Ward/County Councillor commenting in their official
         consultee role (these are consultees, NOT members of the public), even when
         they raise a concern. A Parish Council noting HP noise is a consultee
         comment, not a public objection.
       - n_public_objections: the count when stated. Distinguish a real count
         ("one representation received from local residents") from a boilerplate
         template line ("...any representations that may have been received..."),
         which is NOT a count. Count objections, NOT letters of support. 0 is a
         valid, meaningful answer ONLY when the documents affirmatively state none
         were received; if the only representation is a consultee comment and no
         public count is stated, n_public_objections is null, NOT 0.
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
       ACTOR GATE: the feature must be something the APPLICANT adopted/proposed.
       EXCLUDE (i) a measure IMPOSED by a council condition (that is condition_types,
       not an applicant mitigation), and (ii) an ALTERNATIVE a consultee/EH/officer
       merely SUGGESTED or modelled (e.g. "a 3 m acoustic barrier would be needed")
       that the applicant did not take up. Only mitigations the applicant actually
       built into the proposed scheme count.
       - siting_choice = the unit was located to the rear / away from receptors
         AS A NOISE MEASURE — covers both relocating an existing unit and
         deliberately quiet siting of a new one. A unit that merely happens to be
         at the rear, or a distance figure quoted in a noise calculation, does
         NOT count.
       - low_noise_unit = a low-noise model/configuration selected for acoustic
         reasons (e.g. "XL super low noise configuration ... reduces break-out
         noise by 12 dB(A)").
       - absorption = acoustic absorptive panels / lining of surrounding surfaces.
       EVIDENCE: set `applicant_acoustic_mitigations_evidence` to the verbatim
       phrase that presents the feature(s) AS a noise measure (≤200 chars). If you
       cannot quote such a phrase, the list MUST be [] and evidence null. Siting a
       unit to reduce VISUAL impact is not an acoustic mitigation; do not list
       siting_choice for it.

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
        (≤300 chars) that best supports your overall picture of the case. It MUST
        concern the heat pump or its assessment / decision — a line about
        unrelated works (e.g. noise/vibration from demolition plant) is not HP
        evidence.

    11) `hp_specific_conditions`: short paraphrases of conditions that specifically
        reference the heat pump. Each ≤120 chars. Empty list if none.

    12) Numeric fields (val_*_m setbacks, noise dB, counts): capture only values
        the documents explicitly state. A "≥X" / "X or more" / "at least X" phrasing
        records X (the stated minimum). Never infer, and NEVER compute a value
        yourself (do not add a penalty to a specific level to produce a rating
        level, and do not subtract levels to produce an exceedance — if the
        documents don't state the number, it is null).
        RECITED-RULE TRAP: a figure that is the GPDO Class G RULE THRESHOLD being
        stated as the test — "within 1 metre of the boundary", "must be at least 1 m
        from the curtilage boundary", "0.6 cubic metres" — is the LIMIT in the
        legislation, NOT a measured dimension of THIS unit. Record val_distance_to_
        boundary_m / volumes ONLY from a measured statement about the actual unit
        ("the unit sits 2.3 m from the rear boundary"); if the only "1 m" in the text
        is the rule being recited or applied as a pass/fail test, leave the field null.
        The noise dB quantities are
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
          independent of distance/screening — NOT the level at a neighbour. A
          threshold the unit "must not exceed" (e.g. "shall not exceed 54 dB") is a
          LIMIT (noise_limit_db / noise_limit_relative), not the source power.
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
        - noise_limit_basis: WHICH limit regime applies. Decide top-down and STOP at
          the first match; reserve other/not_stated for the genuinely unclassifiable:
          * mcs_020_pd_limit           — national fixed MCS-020 PD limit (not
            LA-specific): 42 dB(A) pre-20-Sep-2025, 37 dB(A) under MCS-020(a) after.
            Put the number in noise_limit_db; this basis is version-neutral.
          * la_local_plan              — the limit is tied to a NAMED development-plan
            policy / SPD / SPG / council standard ("Policy 39", "Policy D20", "the
            council's external-plant standard"), OR is a council-set Noise Rating curve
            target ("NR 35", "NR 45 design target"). A council-set NR target is
            la_local_plan, NOT other. (NR curve targets have no single dB(A) number, so
            leave noise_limit_db null.)
          * bs_4142_background_relative — the limit is expressed RELATIVE to background
            with no fixed number ("X dB below/above background", "0-5 dB below the
            measured background", "rating level not to exceed background"). Set
            noise_limit_relative too: "0-5 dB below background" or "5 dB below
            background" => "background-5"; "at or below background" => "background";
            "5 dB above background" => "background+5". Leave noise_limit_db null.
          * bs_8233_internal           — an internal-room limit (BS 8233 / WHO).
          * other / not_stated.
          TRAP: a sound-POWER cap on the unit itself ("the ASHP shall not exceed 75
          dB(A) sound power", "Lw <= X") is a SOURCE cap to achieve compliance, NOT the
          rating-level limit the noise is judged against — it does NOT go in
          noise_limit_db and does NOT by itself set noise_limit_basis.

    12d) NOISE OUTCOME vs METHOD — two orthogonal fields:
        - noise_assessment_outcome: the pass/fail STATUS — not_required, pass, fail,
          submitted_no_outcome, not_mentioned. PRECEDENCE: when the council / EH
          states its own verdict on noise compliance, that verdict IS the outcome —
          an applicant report claiming a pass that EH recalculates or rejects as a
          fail => fail. Only when the council is silent use the assessment's own
          conclusion. not_mentioned means noise was never discussed at all; when
          an assessment WAS submitted but the council neither accepted nor decided
          on it (e.g. refused for insufficient noise information without ruling on
          the submitted calc), use submitted_no_outcome. An explicit consultee / EH
          statement of NO OBJECTION on noise grounds ("no objection in relation to
          noise from the proposed ASHP") IS a verdict that the noise position is
          acceptable => pass, even without a numeric assessment — not not_mentioned.
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
        - a standard named ONLY inside recited legislation / permitted-development
          criteria — e.g. an MCS reference appearing within a quoted GPDO Class G /
          Scottish Class 6H test, or the PD conditions being recited rather than an
          assessment actually applying the standard to this proposal. A real
          citation needs the documents to USE the standard (an assessment to it, or
          "the proposal complies with MCS Planning Standards" as a finding), not just
          quote the rule that mentions it;
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
        - hp_type — first fix the SOURCE family, then the distribution medium:
          ground loop / borehole / "ground source" => gshp; water source / lake /
          river => wshp; HP + retained fossil boiler sharing the load => hybrid.
          For an AIR-source unit, decide a2w vs a2a from the heat DISTRIBUTION
          medium: internal fan coil units / warmed-and-cooled AIR / comfort cooling
          / pure air conditioner => a2a (or ac if purely an air conditioner);
          radiators / underfloor heating / hot-water cylinder / "wet system" => a2w.
          Default for DWELLINGS: a bare "air source heat pump" serving a
          DWELLINGHOUSE with no distribution evidence => a2w (UK domestic ASHPs are
          air-to-water by default; only an a2a/ac signal above overrides this).
          Reserve `unknown` for when even the SOURCE family (air vs ground vs water)
          is genuinely unclear, or for a NON-dwelling/commercial plant system where
          the medium is unstated (there a2a/VRF is common, so do NOT assume a2w).

    13) install_* are FACTS about where the HP is installed, independent of whether
        the council took issue. Each is "yes" / "no" / "unknown":
        - install_above_ground_floor: HP at first-floor level, upper storey, or roof.
        - install_on_principal_elevation: HP on the principal (public-facing) elevation.
        - install_fronts_highway: the wall/roof the unit is on faces a PUBLIC HIGHWAY
          (incl. footpath/pavement); EXCLUDES private roads/drives. This is the Class G
          legal trigger. BEWARE the GPDO-boilerplate trap: the phrase "a wall which
          fronts a highway" appears verbatim in the legislation text — extract the
          PROPERTY FACT, not the rule citation. "Visible from the streetscene" => yes;
          a wall facing a private drive => no. Prefer the officer's stated conclusion.
        - install_on_pitched_roof: HP mounted on a pitched roof.
        Decide EACH install_* from its OWN evidence, INDEPENDENT of how the unit is
        affixed — do NOT couple them to hp_mounting_type. above_ground_floor turns on
        the LEVEL/storey, principal_elevation on WHICH elevation, fronts_highway on
        whether that elevation faces a public highway, pitched_roof on the SURFACE — the
        fixing method (bracket vs plinth) does not bear on any of them. A ground-standing
        OR ground-level unit is "no" for install_above_ground_floor and "no" for
        install_on_pitched_roof regardless of mounting; a unit on a stated elevation
        answers install_on_principal_elevation from that placement. Answer "unknown" only
        when the unit's OWN level/elevation/surface is genuinely undeterminable — NOT
        merely because hp_mounting_type came out "unknown". Do NOT default to "no".
        Reserve null for when the topic is entirely absent.

    14) `includes_wind_turbine`: True if the application also includes a wind turbine
        (cumulative-noise scenario). When True, "wind_turbine" must also appear in
        bundled_works (rule 8).

    15) `appearance_concern_level` — route VISUAL vs NOISE, and grade escalation.
        THREE levels only (not_raised + raised_not_decisive are collapsed into
        the single non-decisive level, because that boundary was unstable and
        carried no reporting weight — what matters is whether appearance DROVE
        the outcome):
        - not_decisive           : visual/appearance did NOT drive the outcome.
                                   Covers BOTH "never raised" and "raised/assessed
                                   but not decisive" — including an officer merely
                                   assessing visual amenity and finding it
                                   acceptable, and an applicant-volunteered
                                   screening amendment not imposed by the LPA.
        - addressed_by_condition : the LPA IMPOSED a condition controlling the HP's
                                   appearance (units pushed to rear / screening
                                   required by condition).
        - reason_for_refusal     : visual/appearance is an actual stated refusal reason.
        ONLY about visual / external-appearance / character OF THE HEAT PUMP /
        its plant equipment — appearance concerns about other bundled elements
        (fenestration, an extension) do NOT count. Noise-driven reasoning goes
        to the sound fields (and council_refusal_reasons=sound_noise), NOT here.
        Neighbour disturbance goes to public_objections_grounds.
        addressed_by_condition requires an IMPOSED condition that controls the HP's
        appearance. A change the APPLICANT volunteered (e.g. an amended scheme adding
        screening), not imposed by the LPA, is not_decisive — not
        addressed_by_condition. A generic boilerplate materials condition that does
        not name the HP / its plant (e.g. "all new external work to match existing")
        does NOT qualify (it is not_decisive).

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
          -> 1600-1699; "1930s" -> 1930-1939. A century with an early/mid/late
          qualifier maps to that THIRD of the century, not the whole century:
          "late 19th century" -> 1875-1900; "late C17" / "late 17th century" ->
          1670-1699; "early C18" -> 1700-1730; "mid C19" -> 1830-1870. Do NOT widen
          a qualified century back to the full 100-year span.
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
        EVIDENCE: set `hp_mounting_type_evidence` to the verbatim phrase stating the
        mounting (≤200 chars). If you cannot quote one, set hp_mounting_type to
        "unknown" and evidence null — do NOT guess "wall"/"ground" from context.
        ("a timber screened ASHP on the west gable elevation" => ground, evidence
        that phrase; silence on how it is fixed => unknown.)
        These phrases DO state the mounting — capture them as evidence rather than
        defaulting to unknown: "on a plinth / concrete base / slab / pad / paving /
        hardstanding", "ground-standing", "free-standing", "at ground level", "on the
        ground", "stands on the ground" => ground; "wall-mounted", "fixed to / on the
        wall", "on brackets", "bracket-mounted" => wall; "on the (flat/pitched) roof",
        "roof-mounted" => roof.

    17b) `hp_placement`: where on the property the unit sits (front / rear / side /
        roof / gable / courtyard / multiple / unknown). Set `hp_placement_evidence`
        to the verbatim phrase locating it (≤200 chars). A distance figure ("over 2m
        from the rear boundary", "1m from the boundary") is about PROXIMITY, not
        placement — it does NOT establish front/rear/side (rule 2b). If no phrase
        states the location, set hp_placement="unknown" and evidence null.

    17c) `hp_manufacturer` / `hp_model`: the make/model of the HEAT PUMP unit only.
        Ancillary equipment named in the docs — a battery (e.g. "Tesla Powerwall"),
        PV inverter, or EV charger — is NOT the heat pump; if only ancillary brands
        appear, leave both null.

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
        in post-processing). An LDC (certificate of lawfulness) likewise imposes no
        conditions: any "conditions" / "Class G" language in it is the
        permitted-development test being recited, not conditions imposed — leave
        these fields empty for application_type=ldc (also enforced in post-processing).

    20) `primary_planning_trigger`: the STRUCTURAL reason THIS application needed
        planning permission — ONE of six values. This field answers ONLY "why was
        permission required at all", NOT "which GPDO/policy limit applied" (the
        specific constraint — conservation_area, listed, above_ground_floor,
        pitched_roof, within-1m, multiple units, wind turbine — is recorded in
        `designations`, `listed_status`, the `install_*` fields, `n_hp_units`, and
        `includes_wind_turbine`; do NOT try to encode it here). Take the FIRST match
        top-down — the rungs are strict precedence:

        RUNG 0 — follow-on / administrative records (application_type is
          condition_discharge, non_material_amendment, ldc, reserved_matters, or a
          s73 VARIATION/REMOVAL of a condition): classify by the PARENT scheme's
          character — non-dwelling parent => `non_domestic`; new/replacement dwelling
          scheme => `new_build`; dwelling driven by OTHER works with the HP incidental
          => `bundled_development`; the HP itself was why the parent permission was
          needed => `hp_needed_permission`. `unknown` only if the parent character
          genuinely cannot be told.
        RUNG 1 — `non_domestic`: the building is NOT a dwellinghouse (commercial,
          industrial, institutional, agricultural, school, library, leisure centre,
          church). Class G PD never applied. OUTRANKS everything below — a non-dwelling
          that is also listed / in a conservation area / in a National Park is STILL
          `non_domestic`. (A church being CONVERTED to a dwelling is non_domestic.)
        RUNG 2 — `new_build`: a new or replacement dwelling (or multi-unit dwelling
          scheme) with the HP part of that original build.
        RUNG 3 — `bundled_development`: a DWELLINGHOUSE app whose permission is driven
          by OTHER works (extension, conversion, loft/garage, demolition, dormers)
          with the HP tacked on; i.e. hp_relevance is hp_incidental or mixed.
        RUNG 4 — `hp_needed_permission`: the HEAT PUMP ITSELF is why a dwellinghouse
          app needed permission — some GPDO Part 14 Class G limit or a local policy
          removed PD rights (it sits in a conservation area / on a listed building /
          above ground floor / on a pitched roof / within 1m of the boundary / is
          oversized / there are multiple units / an MCS-020 noise calc fails / etc.).
          You do NOT choose which limit — just record that the HP was the reason.
        RUNG 5 — `retrospective`: explicitly a retention case AND no RUNG 1-4 trigger.
        RUNG 6 — `unknown`: HP clearly needed permission but docs don't say why
          (incl. a refusal merely for INSUFFICIENT information). NEVER invent one.

    21) `building_use_class` + `dwelling_type` — two fields:
        - `building_use_class`: the COARSE category — residential (any home),
          commercial (retail/office/hospitality, Use Class E), industrial
          (storage/manufacturing/warehouse, B2/B8), institutional
          (school/healthcare/community/place of worship, Use Class F), agricultural
          (farm buildings), mixed_use (residential + commercial), other, or unknown.
        - `dwelling_type`: the residential building FORM (detached, semi,
          mid_terrace, end_terrace, flat, duplex, bungalow, maisonette, new_build,
          other). Populate ONLY when building_use_class is residential or mixed_use;
          set it null for a non-residential building (the category is carried by
          building_use_class). Use "unknown" when the building IS a home but the
          form isn't determinable — do NOT collapse "we know it's residential" into
          a wrong class.
        Use `new_build` ONLY when the documents show the dwelling is itself new or a
        replacement (the HP is part of the original build); a description of an
        existing house ("a large detached dwelling") takes the matching form
        (detached), NOT new_build.

    21b) `designations`: tag a designation ONLY when the documents state THIS SITE
        carries it. A listed building NEARBY, an adjacent conservation area, or a
        rural / beyond-green-belt location does NOT make the site an AONB, National
        Park or conservation area — those require an explicit statement that the
        site itself lies within one. [] when none is stated for the site.

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
          When the consent was granted SUBJECT TO conditions (any conditions listed on
          the LBC decision notice — heritage, materials, accord-with-plans, etc.), use
          `granted_with_conditions`, NOT a bare `granted`. For an application_type=lbc
          app, the decision notice's conditions ARE the LBC's conditions.
        - `lbc_reference`: the linked LBC application reference verbatim if one is named
          (e.g. "UTT/19/2742/LB"); often in the officer report's site-history table.
        Worked example (Uttlesford UTT/19/2431/HHF): property is itself Grade II listed,
        in a conservation area; planning permission REFUSED on harm to the setting of an
        adjacent Grade II* building; the separate LBC app UTT/19/2742/LB was closed
        "listed building consent deemed to be not required" => listed_status=grade_2,
        lbc_required=not_required, lbc_decision=null, lbc_reference="UTT/19/2742/LB",
        decision_outcome=refused, application_type=householder.

    23) `alternative_siting_discussed`: a plain boolean (no null).
        TRUE when the documents show ALTERNATIVE locations for the HP were
        considered — the LPA / conservation officer REQUESTED information on
        alternative locations (ground installation, secondary elevation,
        different roof), or the applicant ASSESSED or DEFENDED the chosen
        location against alternatives. Common in listed-building cases.
        FALSE in EVERY other case — including when the topic simply never arises.
        Do NOT distinguish "explicitly not considered" from "never mentioned":
        both are FALSE. Only an actual discussion of alternative LOCATIONS makes
        it TRUE.
        NOT alternative-siting discussion (=> FALSE): a request for more DETAIL
        about the chosen location (without any alternative being raised), or
        siting/screening done for VISUAL reasons.

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
        # too — the model otherwise confabulates condition_types on refused apps
        # from parent permissions and "recommended if approved" passages. (An
        # HP-friendly recommended condition still surfaces via
        # hp_component_stance=acceptable_with_condition.)
        out["n_conditions"] = None
        out["condition_types"] = []
        out["hp_specific_conditions"] = []
    if out.get("decision_outcome") not in REFUSAL_OUTCOMES:
        out["council_refusal_reasons"] = None
        out["council_refusal_quote"] = None
    else:
        # The prompt states the invariant "council_refusal_reasons MUST be a
        # subset of council_considerations" (a refusal ground is by definition a topic
        # the LPA weighed). The audit caught the model dropping a ground from
        # considerations while keeping it in the refusal list. Enforce it
        # deterministically: every refusal reason is also a consideration.
        rr = out.get("council_refusal_reasons") or []
        cc = out.get("council_considerations") or []
        out["council_considerations"] = cc + [r for r in rr if r not in cc]
    # An LDC (certificate of lawfulness) is a yes/no ruling on lawfulness,
    # not a grant of permission — it imposes NO planning conditions. The "conditions"
    # language in an LDC recites the Class G permitted-development criteria being
    # tested, not conditions imposed. Force the condition fields empty so they
    # aren't mis-counted (reviewer couldn't count them; they don't exist).
    if out.get("application_type") == "ldc":
        out["n_conditions"] = None
        out["condition_types"] = []
        out["hp_specific_conditions"] = []
    # hp_refusal_ground names WHY the HP itself was unacceptable; on any other
    # stance the question doesn't arise, so it must be null.
    if out.get("hp_component_stance") != "unacceptable":
        out["hp_refusal_ground"] = None
    # Evidence-gated grounding — a subjective value with no supporting
    # verbatim evidence is dropped to its empty option. The model must quote a
    # span; if it can't, fall back rather than assert a positive it can't ground
    # (mirrors the building_age / building_age_evidence gate).
    if not out.get("hp_placement_evidence"):
        out["hp_placement"] = "unknown"
    # ASYMMETRIC mounting gate. An earlier gate erased ANY unquoted
    # mounting to "unknown" to stop the model emitting an unstated "wall". But the
    # held-out audit showed the model UNDER-claims "ground" (it is correct whenever it
    # picks it), and erasing those correct picks cascaded the install_* facts to
    # "unknown". "wall"/"roof"/"mixed" remain the over-claimed values the gate was built
    # to catch, so they still require a verbatim quote; an unquoted "ground" is KEPT
    # (ground is the dominant prior for a domestic ASHP, and the null evidence field
    # still flags it as a prior-based call). This lets the ground coupling below fire.
    if not out.get("hp_mounting_type_evidence") and out.get("hp_mounting_type") in ("wall", "roof", "mixed"):
        out["hp_mounting_type"] = "unknown"
    if not out.get("applicant_acoustic_mitigations_evidence"):
        out["applicant_acoustic_mitigations"] = []
    # A confirmed GROUND mounting fixes two install_* facts deterministically —
    # a ground-standing unit cannot be above the ground floor and cannot be on a pitched
    # roof. This runs AFTER the mounting evidence-gate above, so it fires only on a
    # surviving positive "ground" finding, breaking the cascade that the held-out audit
    # showed dropping these to "unknown" on apps where ground mounting was clearly stated.
    if out.get("hp_mounting_type") == "ground":
        out["install_above_ground_floor"] = "no"
        out["install_on_pitched_roof"] = "no"
    # Dwelling_type is the residential FORM; it can't co-exist with a
    # clearly non-residential use class. (mixed_use / other / unknown may legitimately
    # carry a residential form, so they're left alone.)
    if out.get("building_use_class") in ("commercial", "industrial", "institutional", "agricultural"):
        out["dwelling_type"] = None
        # A non-dwelling structurally never had Class G PD, so the structural
        # trigger is non_domestic regardless of co-present designations. Enforce
        # deterministically (RUNG 1) — this was the model's main perception wobble.
        # Skip follow-on records whose PARENT may have been a dwelling.
        if out.get("application_type") not in (
            "condition_discharge",
            "non_material_amendment",
            "ldc",
            "reserved_matters",
        ):
            out["primary_planning_trigger"] = "non_domestic"
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
    # instead. Defaults to "low" to match the default gpt-5.4-mini model. Set
    # HP_REASONING_EFFORT="" (empty) to fall back to temperature=0 for a
    # non-reasoning model.
    effort = os.environ.get("HP_REASONING_EFFORT", "low")
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
