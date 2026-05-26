"""Recipe B: dual-classify with gpt-4.1-mini + gpt-4o-mini.

Merge per-app, treat as high-confidence when both models agree on the 2-class
(hp_affects vs hp_incidental) cut. Otherwise mark needs_review.

Output:
  - _local/llm_pilot/sample_100/recipe_b/merged.csv  (one row per app)
  - stdout summary
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "_local/llm_pilot/sample_100"
A_PATH = BASE / "gpt-4.1-mini/results.json"
B_PATH = BASE / "gpt-4o-mini-v2/results.json"
OUT_DIR = BASE / "recipe_b"
OUT_DIR.mkdir(parents=True, exist_ok=True)

A = {r["uid"]: r for r in json.loads(A_PATH.read_text())}
B = {r["uid"]: r for r in json.loads(B_PATH.read_text())}


def collapse(lbl):
    return "hp_affects" if lbl in {"hp_relevant", "mixed"} else "hp_incidental"


merged = []
for uid, a in A.items():
    b = B[uid]
    la, lb = a["hp_relevance"], b["hp_relevance"]
    ca, cb = collapse(la), collapse(lb)
    agree_2 = ca == cb
    agree_3 = la == lb
    # Choose surfaced label: if 3-class match -> that label. Otherwise:
    #   - both in hp_affects (R/M) -> pick the more informative (mixed if either says mixed)
    #   - 2-class disagree -> mark unresolved (still output gpt-4.1-mini's pick as primary)
    if agree_3:
        final = la
    elif agree_2:
        # both hp_affects but differ between hp_relevant/mixed
        final = "mixed" if "mixed" in {la, lb} else la
    else:
        final = la  # use gpt-4.1-mini primary; needs_review will be set

    merged.append(
        {
            "uid": uid,
            "authority_name": a["authority_name"],
            "reference": a["reference"],
            "decision_bucket": a["decision_bucket"],
            "is_bundled": a["is_bundled"],
            "planning_decision": a["planning_decision"],
            "description": a["description"],
            "label_4_1_mini": la,
            "label_4o_mini": lb,
            "agree_3class": agree_3,
            "agree_2class": agree_2,
            "needs_review": not agree_2,
            "final_label": final,
            "confidence": "high" if agree_2 else "needs_review",
            "evidence_quote_4_1_mini": a["evidence_quote"],
            "evidence_quote_4o_mini": b["evidence_quote"],
            "reasoning_4_1_mini": a["reasoning"],
            "reasoning_4o_mini": b["reasoning"],
        }
    )

with (OUT_DIR / "merged.csv").open("w", newline="", encoding="utf-8") as fh:
    writer = csv.DictWriter(fh, fieldnames=list(merged[0].keys()))
    writer.writeheader()
    writer.writerows(merged)

# Summary
n = len(merged)
agree_3 = sum(1 for m in merged if m["agree_3class"])
agree_2 = sum(1 for m in merged if m["agree_2class"])
needs_review = n - agree_2
print(f"Sample size: {n}")
print(f"3-class agreement: {agree_3}/{n} = {agree_3}%")
print(f"2-class agreement: {agree_2}/{n} = {agree_2}%")
print(f"needs_review (2-class disagree): {needs_review}/{n} = {needs_review}%")

# Final label distribution among high-confidence apps
hc = [m for m in merged if not m["needs_review"]]
print(f"\nFinal labels (high-confidence subset, n={len(hc)}):")
for k, v in Counter(m["final_label"] for m in hc).most_common():
    print(f"  {k:<14} {v}")

# Decision bucket × final label, high-conf only
print("\nHigh-confidence: decision_bucket x final_label")
cells = Counter((m["decision_bucket"], m["final_label"]) for m in hc)
for bucket in ["refused", "approved", "withdrawn", "other"]:
    r = cells.get((bucket, "hp_relevant"), 0)
    m_ = cells.get((bucket, "mixed"), 0)
    i = cells.get((bucket, "hp_incidental"), 0)
    print(f"  {bucket:<11} hp_relevant={r:>3}  mixed={m_:>3}  hp_incidental={i:>3}")

# needs_review subset
print(f"\nneeds_review cases ({needs_review}):")
for m in merged:
    if not m["needs_review"]:
        continue
    print(f"  [{m['decision_bucket']:<9}] {m['authority_name']} / {m['reference']}")
    print(f"      4.1-mini: {m['label_4_1_mini']:<14}  reasoning: {m['reasoning_4_1_mini'][:120]}")
    print(f"      4o-mini : {m['label_4o_mini']:<14}  reasoning: {m['reasoning_4o_mini'][:120]}")
    print(f"      desc: {m['description'][:120]}")

# Cost projection at corpus scale (18,935 apps), Batch API
TOK_IN = 5_225 + 5_055  # per-app combined for both runs
TOK_OUT = 107 + 153  # per-app combined (4o-mini + 4.1-mini)
N_CORPUS = 18_935
# Batch rates: gpt-4o-mini $0.075/$0.30, gpt-4.1-mini $0.20/$0.80
batch_4o = N_CORPUS * (5_225 * 0.075 + 107 * 0.30) / 1_000_000
batch_4_1 = N_CORPUS * (5_055 * 0.20 + 153 * 0.80) / 1_000_000
print(f"\nCorpus projection (N={N_CORPUS:,}, Batch API):")
print(f"  gpt-4o-mini  : ${batch_4o:.2f}")
print(f"  gpt-4.1-mini : ${batch_4_1:.2f}")
print(f"  combined     : ${batch_4o + batch_4_1:.2f}")
