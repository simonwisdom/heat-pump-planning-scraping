"""3-class HP-relevance classifier over the 100-app stratified sample.

Reads /tmp/llm_sample_100/sample.csv + texts/, calls gpt-4o-mini per app,
writes per-app JSON + summary table.

Run:
    uv run --with openai --with python-dotenv \
        python scripts/llm/classify_sample_100.py
"""

from __future__ import annotations

import csv
import json
import os
import sys
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

ROOT = Path(__file__).resolve().parents[2]
SAMPLE_ROOT = Path("/tmp/llm_sample_100")
SAMPLE_CSV = SAMPLE_ROOT / "sample.csv"
TEXTS_DIR = SAMPLE_ROOT / "texts"

MODEL = os.environ.get("HP_MODEL", "gpt-4o-mini")
RUN_TAG = os.environ.get("HP_RUN_TAG", "")  # e.g. "v2" for the re-pick rerun
OUT_DIR = ROOT / "_local/llm_pilot/sample_100" / (f"{MODEL}-{RUN_TAG}" if RUN_TAG else MODEL)
OUT_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_PATH = OUT_DIR / "results.json"
RESULTS_CSV = OUT_DIR / "results.csv"

# Per-file cap so a single 25k-char brochure can't eat the whole prompt
# budget and crowd out the actual decision letter.
PER_FILE_CHARS = 5000
MAX_DOC_CHARS = 20000
MAX_WORKERS = 8

SYSTEM_PROMPT = textwrap.dedent("""
    You read UK planning-application documents (decision notices and officer or
    delegated reports) and classify whether the air-source heat pump (ASHP) is
    the actual subject of the planning decision.

    Choose exactly ONE label for hp_relevance:

      - hp_relevant : The decision turns on the heat pump itself. The planning
        authority's reasoning specifically addresses the HP -- its noise, siting,
        visual impact, compliance with permitted-development criteria, listed-
        building harm caused specifically by the HP, etc. If the HP element were
        removed from the application, the outcome would plausibly change. This
        applies whether the application is HP-only or HP plus minor associated
        works.

      - mixed : The application bundles the HP with other substantial works
        (extension, listed-building works, change of use, garage, solar, etc.)
        AND the decision rationale engages with the HP as one of several distinct
        grounds. Both the HP and other works contribute to the outcome.

      - hp_incidental : The application includes a HP, but the planning decision
        is driven by the other works (extension, listed building, etc.) and the
        HP is barely engaged with in the decision rationale, or not at all. The
        HP is along for the ride. Use this label whether the HP is briefly
        mentioned or not mentioned in the decision text.

    Also output:
      - decision_basis : heat_pump_specific | other_works_specific | mixed | unclear | no_decision_yet
      - evidence_quote : ONE short verbatim quote from the supplied text that best
        supports the chosen label. Keep under 300 chars. If the documents don't
        contain a decision rationale (e.g. pre-decision), set this to "" and use
        no_decision_yet for decision_basis.
      - reasoning : 1-2 sentences explaining the call.
      - confidence : high | medium | low
      - bundled_with : short list of other substantial works mentioned in the
        application scope (e.g. ["extension", "garage"]), or [] if HP only.

    Return STRICT JSON matching the schema. No prose outside the JSON.
""").strip()

SCHEMA = {
    "type": "object",
    "properties": {
        "hp_relevance": {"type": "string", "enum": ["hp_relevant", "mixed", "hp_incidental"]},
        "decision_basis": {
            "type": "string",
            "enum": ["heat_pump_specific", "other_works_specific", "mixed", "unclear", "no_decision_yet"],
        },
        "evidence_quote": {"type": "string"},
        "reasoning": {"type": "string"},
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        "bundled_with": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["hp_relevance", "decision_basis", "evidence_quote", "reasoning", "confidence", "bundled_with"],
    "additionalProperties": False,
}


def load_docs(text_paths: list[str]) -> tuple[str, list[str]]:
    parts: list[str] = []
    used: list[str] = []
    budget = MAX_DOC_CHARS
    for rel in text_paths:
        if budget <= 200:
            break
        src = TEXTS_DIR / rel
        if not src.exists():
            continue
        text = src.read_text(encoding="utf-8", errors="replace")
        # Cap each file so a verbose doc can't crowd out higher-ranked docs.
        per_file = min(PER_FILE_CHARS, budget)
        snippet = text[:per_file]
        parts.append(f"=== {src.name} ===\n{snippet}")
        used.append(src.name)
        budget -= len(snippet)
    return "\n\n".join(parts), used


def classify_one(client: OpenAI, row: dict) -> dict:
    text_paths = [p for p in row["text_paths"].split("|") if p]
    doc_text, files_used = load_docs(text_paths)
    user_msg = textwrap.dedent(f"""
        AUTHORITY: {row["authority_name"]}
        REFERENCE: {row["reference"]}
        APPLICATION DESCRIPTION: {row["description"]}
        PLANNING DECISION (raw status): {row["planning_decision"]}
        DECISION DATE: {row["decision_date"]}

        EXTRACTED DOCUMENT TEXT (decision notice first, then officer/delegated
        report, then other docs; truncated to {MAX_DOC_CHARS} chars total):
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
            "json_schema": {"name": "hp_relevance", "schema": SCHEMA, "strict": True},
        },
    )
    if not MODEL.startswith("gpt-5") and not MODEL.startswith("o"):
        kwargs["temperature"] = 0
    effort = os.environ.get("HP_REASONING_EFFORT")
    if effort and (MODEL.startswith("gpt-5") or MODEL.startswith("o")):
        kwargs["reasoning_effort"] = effort
    resp = client.chat.completions.create(**kwargs)
    out = json.loads(resp.choices[0].message.content)
    out["_files_used"] = files_used
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
    print(f"Loaded {len(rows)} sample rows. Classifying with {MODEL}...", flush=True)

    results: list[dict] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(classify_one, client, r): r for r in rows}
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
            out["is_bundled"] = r["is_bundled"]
            out["description"] = r["description"]
            results.append(out)
            if i % 10 == 0 or i == len(rows):
                print(f"  [{i:3d}/{len(rows)}] {time.time() - t0:.1f}s elapsed", flush=True)

    RESULTS_PATH.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nFull results -> {RESULTS_PATH}")

    # CSV with flat columns for spreadsheet review
    csv_fields = [
        "uid",
        "authority_name",
        "reference",
        "decision_bucket",
        "is_bundled",
        "planning_decision",
        "hp_relevance",
        "decision_basis",
        "confidence",
        "bundled_with",
        "evidence_quote",
        "reasoning",
        "description",
    ]
    with RESULTS_CSV.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=csv_fields)
        w.writeheader()
        for r in results:
            row = {k: r.get(k, "") for k in csv_fields}
            if isinstance(row["bundled_with"], list):
                row["bundled_with"] = ", ".join(row["bundled_with"])
            w.writerow(row)
    print(f"CSV       -> {RESULTS_CSV}")

    # Summary tables
    from collections import Counter

    labels = Counter(r.get("hp_relevance", "ERROR") for r in results)
    print("\nLabel distribution:")
    for k, v in labels.most_common():
        print(f"  {k:<14} {v:>3}")

    cross = Counter()
    for r in results:
        cross[(r.get("decision_bucket", "?"), r.get("hp_relevance", "ERROR"))] += 1
    print("\nDecision bucket x predicted label:")
    print(f"  {'decision':<11} {'hp_relevant':>11} {'mixed':>7} {'hp_incidental':>14}")
    for bucket in ["refused", "approved", "withdrawn", "other"]:
        a = cross.get((bucket, "hp_relevant"), 0)
        b = cross.get((bucket, "mixed"), 0)
        c = cross.get((bucket, "hp_incidental"), 0)
        print(f"  {bucket:<11} {a:>11} {b:>7} {c:>14}")

    bund_cross = Counter()
    for r in results:
        bund = "bundled" if str(r.get("is_bundled", "")).lower() in ("true", "1") else "hp_only"
        bund_cross[(bund, r.get("hp_relevance", "ERROR"))] += 1
    print("\nDescription-bundled? x predicted label:")
    for bund in ["hp_only", "bundled"]:
        a = bund_cross.get((bund, "hp_relevant"), 0)
        b = bund_cross.get((bund, "mixed"), 0)
        c = bund_cross.get((bund, "hp_incidental"), 0)
        print(f"  desc={bund:<7} hp_relevant={a:>3} mixed={b:>3} hp_incidental={c:>3}")

    low_conf = [r for r in results if r.get("confidence") == "low"]
    print(f"\nLow-confidence cases: {len(low_conf)}")
    for r in low_conf:
        print(
            f"  {r['authority_name']:>18} / {r['reference']:<22} -> "
            f"{r.get('hp_relevance', '?')}: {r.get('reasoning', '')[:120]}"
        )

    total_in = sum(r.get("_usage", {}).get("prompt_tokens", 0) for r in results if isinstance(r.get("_usage"), dict))
    total_out = sum(
        r.get("_usage", {}).get("completion_tokens", 0) for r in results if isinstance(r.get("_usage"), dict)
    )
    # gpt-4o-mini pricing: $0.15/M input, $0.60/M output (2026 rates).
    cost = total_in / 1_000_000 * 0.15 + total_out / 1_000_000 * 0.60
    print(f"\nToken usage: in={total_in:,} out={total_out:,}  est cost=${cost:.4f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
