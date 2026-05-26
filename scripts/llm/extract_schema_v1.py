"""29-field schema extraction over a stratified sample using gpt-4.1-mini.

Reads:
  - {SAMPLE_ROOT}/sample.csv  (50 uids picked from recipe_b)
  - /root/full_corpus_texts/summary.csv  (manifest with document_type per file)
  - /root/full_corpus_texts/texts/...     (extracted text files)

Writes:
  - _local/llm_pilot/schema_v1_50/results.json
  - _local/llm_pilot/schema_v1_50/results.csv

Differences vs classify_sample_100.py:
  - Re-ranks files per uid using sound+heritage tiers (not the pilot's 5-tier).
  - Per-file char cap lifted from 5,000 -> 12,000.
  - Total budget lifted from 20,000 -> 50,000.
  - Cap on files per app lifted from 8 -> 10.
  - 29-field structured output instead of 6-field relevance classifier.
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
MAX_DOC_CHARS = 50_000
MAX_FILES = 10
MAX_WORKERS = 6

# === Ranker ===
DECISION_TYPES = {"decision", "decision notice", "decision letter", "recommendation and reasons report"}
OFFICER_TYPES = {
    "officer report",
    "officer reports",
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


# === 29-field schema ===
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
    "decision_outcome": ENUM(
        "approved",
        "refused",
        "withdrawn",
        "invalid",
        "prior_approval_granted",
        "prior_approval_refused",
        "prior_approval_not_required",
        "ldc",
        "pending",
        "other",
    ),
    "summary": {"type": "string"},
    "key_evidence_quote": {"type": "string"},
    "refusal_reason_buckets": {
        "type": ["array", "null"],
        "items": {
            "type": "string",
            "enum": [
                "amenity",
                "character_appearance",
                "heritage",
                "sound_noise",
                "highways",
                "ecology",
                "setback_boundary",
                "design_info_insufficient",
                "policy_conflict",
                "other",
            ],
        },
    },
    "refusal_reason_quote": {"type": ["string", "null"]},
}

T1 = {
    "hp_type": ENUM_NULL("a2w", "a2a", "gshp", "wshp", "hybrid", "ac", "unknown"),
    "dwelling_type": ENUM_NULL(
        "detached",
        "semi",
        "mid_terrace",
        "end_terrace",
        "flat",
        "bungalow",
        "maisonette",
        "new_build",
        "other",
        "unknown",
    ),
    "era": ENUM_NULL("georgian", "victorian", "edwardian", "interwar", "post_war", "modern", "new_build", "unknown"),
    "placement": ENUM_NULL("front", "rear", "side", "roof", "gable", "courtyard", "multiple", "unknown"),
    "mounting_type": ENUM_NULL("ground", "wall", "roof", "mixed", "unknown"),
    "n_heat_pump_units": {"type": ["integer", "null"]},
    "n_dwellings_served": {"type": ["integer", "null"]},
    "designations": {
        "type": ["array", "null"],
        "items": {
            "type": "string",
            "enum": ["conservation_area", "article_4", "aonb", "national_park", "world_heritage"],
        },
    },
    "listed_status": ENUM_NULL("grade_1", "grade_2_star", "grade_2", "curtilage", "none", "unknown"),
    "lbc_outcome": ENUM_NULL("granted", "refused", "not_required", "required_not_decided", "not_applicable"),
    "install_above_ground_floor": {"type": ["boolean", "null"]},
    "install_on_principal_elevation": {"type": ["boolean", "null"]},
    "install_on_pitched_roof": {"type": ["boolean", "null"]},
    "concern_amenity_or_character": {"type": ["boolean", "null"]},
    "bundles_wind_turbine": {"type": ["boolean", "null"]},
    "val_hp_enclosure_volume_m3": {"type": ["number", "null"]},
    "val_setback_from_edge_m": {"type": ["number", "null"]},
    "val_distance_to_boundary_m": {"type": ["number", "null"]},
    "sound_assessment_status": ENUM_NULL(
        "not_required", "required_pass", "required_fail", "submitted_no_outcome", "not_mentioned"
    ),
    "acoustic_standards_cited": {
        "type": ["array", "null"],
        "items": {"type": "string", "enum": ["mcs_020", "mcs_020a", "mcs_020b", "bs_4142", "bs_8233", "iso_9613"]},
    },
    "val_db_measured": {"type": ["number", "null"]},
    "acoustic_mitigation_proposed": {
        "type": ["array", "null"],
        "items": {
            "type": "string",
            "enum": ["barrier", "enclosure", "louvre", "quiet_mode", "anti_vibration", "colour_finish", "relocation"],
        },
    },
    "noise_complaint_or_nuisance_mentioned": {"type": ["boolean", "null"]},
    "n_conditions": {"type": ["integer", "null"]},
    "hp_specific_conditions": {"type": ["array", "null"], "items": {"type": "string"}},
    "condition_types": {
        "type": ["array", "null"],
        "items": {
            "type": "string",
            "enum": [
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
            ],
        },
    },
    "co_application_features": {
        "type": ["array", "null"],
        "items": {
            "type": "string",
            "enum": [
                "solar_pv",
                "battery",
                "ev_charger",
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
            ],
        },
    },
    "primary_planning_trigger": ENUM_NULL(
        "conservation_area",
        "article_4",
        "listed",
        "curtilage_listed",
        "flat",
        "above_ground_floor",
        "within_1m_boundary",
        "oversized",
        "front_elevation",
        "wind_turbine_combo",
        "multiple_units",
        "pitched_roof",
        "amenity",
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
       documents are silent on a field, set it to null (or "unknown" where
       the enum has that value, or [] for arrays). Do NOT invent values.

    3) `acoustic_mitigation_proposed` is mitigation features the APPLICANT
       has put forward in the application materials (the scheme isn't built
       yet — these are proposals). Council-imposed conditions live separately
       in `condition_types` (noise_threshold, noise_maintenance, etc.).

    4) `co_application_features` is the controlled-vocab list of other works
       bundled with the HP. Use the enum values; if a work doesn't match any
       (e.g. a specialised use class change), pick the closest or "other"-
       equivalent — there is no free-text companion field.

    5) `decision_outcome` should reflect what the documents say. Use the raw
       planning_decision string supplied as a hint, but don't blindly copy it —
       confirm against the text.

    6) `key_evidence_quote` is ONE verbatim sentence from the supplied text
       (≤300 chars) that best supports your overall picture of the case.

    7) `hp_specific_conditions`: short paraphrases of any conditions
       that specifically reference the heat pump. Each ≤120 chars. Empty list
       if none.

    8) Numeric val_* fields: capture only values the documents explicitly
       state (e.g. "the unit will sit 0.8 m from the boundary"). Don't infer.
       - val_hp_enclosure_volume_m3: the physical volume of the HP outdoor
         unit itself, typically 0.3–1.5 m³. NOT floor area, room area, room
         volume, or building volume. Leave null unless the unit's own volume
         is stated.

    9) install_* booleans are FACTS about where the HP is being installed,
       independent of whether the council took issue with it:
       - install_above_ground_floor: True if the HP is at first-floor level,
         upper storey, or roof-mounted.
       - install_on_principal_elevation: True if the HP is on the principal
         (public-facing) elevation of the building.
       - install_on_pitched_roof: True if the HP is mounted on a pitched roof.
       - bundles_wind_turbine: True if the application also includes a wind
         turbine.
       Leave null only if the documents are silent on the question.

    10) concern_amenity_or_character is the one exception: it is True only
        when the council's reasoning engages with amenity / external
        appearance / visual impact / character (not when the property simply
        has visible features).

    11) era: building construction period. Anchor dates:
        - georgian   : pre-1837
        - victorian  : 1837–1901
        - edwardian  : 1901–1914
        - interwar   : 1918–1939
        - post_war   : 1945–1979
        - modern     : 1980–2010
        - new_build  : post-2010 (also relates to dwelling_type="new_build")
        - unknown    : not stated or unclear
        Use stylistic language in the docs ("Victorian terrace", "1930s semi",
        "interwar suburban") to infer; do not guess from address alone.

    12) mounting_type: how the unit is physically affixed.
        - ground : pad / plinth / on the ground
        - wall   : wall-mounted bracket
        - roof   : fixed on a roof surface or roof structure
        - mixed  : multiple units with different mountings
        - unknown
        Orthogonal to `placement` (which is about location front/rear/side).

    13) n_dwellings_served: number of dwellings the HP system serves.
        - Single home: 1
        - Communal HP for a block: count of flats served
        - Per-flat array: n_heat_pump_units = n_dwellings_served, both > 1

    14) condition_types: categorical complement to hp_specific_conditions.
        For each condition relevant to the HP, tag it as one of:
        - noise_threshold         : numeric noise limit specified
        - noise_post_install_check: verification required after install
        - noise_maintenance       : ongoing maintenance of mitigation
        - standards_compliance    : must follow MCS / BS4142 / IoA guidance
        - colour_or_screening     : paint, fence, planting
        - relocation_or_position  : unit must be moved or specifically sited
        - accord_with_plans       : generic "as per approved drawings"
        - heritage_specific       : LBC pair / fabric protection
        - commissioning_only      : one-off check before occupation
        - other

    Return STRICT JSON. No prose outside the JSON.
""").strip()


def load_selection() -> dict[str, list[dict]]:
    """uid -> list of pre-ranked file records from staging."""
    sel = json.loads(SELECTION_JSON.read_text())
    print(f"Loaded selection for {len(sel)} uids from {SELECTION_JSON}", flush=True)
    return sel


def load_doc_text(selected: list[dict]) -> tuple[str, list[dict]]:
    parts: list[str] = []
    used: list[dict] = []
    budget = MAX_DOC_CHARS
    for f in selected:
        if budget <= 200:
            break
        # text_path is "texts/<rel>"; staging has files under TEXTS_DIR with the same <rel>
        rel = Path(f["text_path"]).relative_to("texts")
        src = TEXTS_DIR / rel
        if not src.exists():
            continue
        text = src.read_text(encoding="utf-8", errors="replace")
        per_file = min(PER_FILE_CHARS, budget)
        snippet = text[:per_file]
        parts.append(f"=== {f['fname']} [doctype={f['doctype']}] ===\n{snippet}")
        used.append(
            {
                "fname": f["fname"],
                "doctype": f["doctype"],
                "rank": rank(f["doctype"], f["fname"])[0],
                "chars": len(snippet),
            }
        )
        budget -= len(snippet)
    return "\n\n".join(parts), used


def extract_one(client: OpenAI, row: dict, uid_files: dict) -> dict:
    selected = uid_files.get(row["uid"], [])[:MAX_FILES]
    doc_text, used = load_doc_text(selected)

    user_msg = textwrap.dedent(f"""
        AUTHORITY: {row["authority_name"]}
        REFERENCE: {row["reference"]}
        APPLICATION DESCRIPTION: {row["description"]}
        PLANNING DECISION (raw status, hint only): {row["planning_decision"]}
        DECISION DATE: {row["decision_date"]}

        EXTRACTED DOCUMENT TEXT (ranked: decision notice first, then officer
        report, sound, heritage, etc.; per-file cap {PER_FILE_CHARS:,} chars,
        total cap {MAX_DOC_CHARS:,} chars):
        ---
        {doc_text}
        ---
    """).strip()

    resp = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {"name": "hp_schema_v1", "schema": SCHEMA, "strict": True},
        },
        temperature=0,
    )
    out = json.loads(resp.choices[0].message.content)
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
