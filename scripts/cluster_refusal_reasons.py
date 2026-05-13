"""Cluster refusal reasons from extracted decision-notice / officer-report text.

For each refused application we have one or more extracted .txt files (from
extract_refusal_text.py). This script:
  - Loads the rules in rules/refusal_reason_keywords.toml
  - Concatenates per-app text (preferring decision_notice > decision > officer
    report > everything else)
  - Tags each app with the set of refusal-reason labels whose regex matches
  - Writes per-app labels + a corpus-wide tally

A document can match multiple rules: refusals usually cite 2-4 grounds.
"""

from __future__ import annotations

import argparse
import csv
import re
import tomllib
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STAGE = ROOT / "_local/workstreams/01_heat_pump_applications/data/intermediate/refusal_text_analysis"
DEFAULT_RULES = ROOT / "rules/refusal_reason_keywords.toml"
DEFAULT_OUTPUT = ROOT / "reports/heat-pump-decisions/refusal_reasons"

# Doc-type ordering for the per-app text concatenation. Higher up = more
# authoritative source of refusal language; later docs are appended after.
DOC_PRIORITY_PATTERNS = [
    (re.compile(r"decision\s*notice", re.I), 0),
    (re.compile(r"appeal\s*decision", re.I), 1),
    (re.compile(r"\bdecision\b", re.I), 2),
    (re.compile(r"officer\s*report|delegated\s*report|case\s*officer|committee\s*report|handling\s*report", re.I), 3),
    (re.compile(r"\breport\b", re.I), 4),
    (re.compile(r"refus", re.I), 5),
]

# Anchors that mark the start of the literal "Reasons for refusal" list in a
# decision notice. If found, restrict regex matching to just that section —
# avoids false positives from background policy citations in long reports.
REFUSAL_SECTION_START = re.compile(
    r"(?i)(reasons?\s+for\s+refusal|grounds?\s+for\s+refusal|refusal\s+reasons?|hereby\s+refuse[s]?\s+planning\s+permission|REFUSED\s+for\s+the\s+following\s+reason)"
)
# Things that typically end the refusal-reasons section. The appeal-rights
# boilerplate ("If you are aggrieved... Planning Inspectorate...") is the most
# common contaminator — it injects 'access' / 'parking' / 'traffic' false
# positives, so we cut hard before it.
REFUSAL_SECTION_END = re.compile(
    r"(?i)("
    r"informative\s*note|informatives?|advice\s+to\s+applicant|right\s+of\s+appeal|"
    r"notes?\s+to\s+applicant|signed[:\s]|date[d]?\s*:\s*\d|"
    r"application\s+plans|approved\s+plans|"
    r"planning\s+inspectorate|appeal\s+form|"
    r"gov\.uk/(planning-inspectorate|appeal)|"
    r"if\s+(you\s+are\s+aggrieved|the\s+applicant\s+is\s+aggrieved)|"
    r"section\s+78\s+of\s+the\s+town"
    r")"
)

# Whether the per-app text mentions the heat pump in proximity to refusal
# language. Used to flag "compound" apps where refusal may be about an
# extension/conversion, not the heat pump itself.
HEAT_PUMP_TOKENS = re.compile(r"(?i)(heat\s*pump|\bASHP\b|air\s*source|GSHP|ground\s*source)")


def extract_refusal_section(text: str) -> str | None:
    """Return the slice of text covering the formal 'Reasons for refusal' list,
    or None if no such anchor is found."""
    m = REFUSAL_SECTION_START.search(text)
    if not m:
        return None
    start = m.end()
    tail = text[start : start + 8000]  # cap section size
    end_match = REFUSAL_SECTION_END.search(tail)
    end = end_match.start() if end_match else len(tail)
    return tail[:end]


def doc_priority(doc_type: str) -> int:
    for pat, score in DOC_PRIORITY_PATTERNS:
        if pat.search(doc_type):
            return score
    return 99


def load_rules(path: Path) -> list[tuple[str, re.Pattern[str]]]:
    data = tomllib.loads(path.read_text())
    out = []
    for rule in data.get("rule", []):
        label = rule["label"]
        pat = re.compile(rule["pattern"])
        out.append((label, pat))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stage", type=Path, default=DEFAULT_STAGE)
    ap.add_argument("--manifest", type=Path)
    ap.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    ap.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = ap.parse_args()

    stage: Path = args.stage
    manifest = args.manifest or (stage / "refused_docs_text_manifest.csv")
    text_root = stage / "text"

    rules = load_rules(args.rules)
    print(f"loaded {len(rules)} rules from {args.rules.name}")

    # Group manifest rows by app, ordered by doc priority.
    by_app: dict[str, list[dict]] = defaultdict(list)
    with manifest.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            uid = row.get("application_uid") or ""
            if not uid:
                continue
            by_app[uid].append(row)

    print(f"{len(by_app)} refused apps in manifest")

    per_app_rows = []
    label_counts: Counter[str] = Counter()
    label_pair_counts: Counter[tuple[str, str]] = Counter()
    n_with_text = 0
    n_missing = 0

    for uid, rows in by_app.items():
        rows_sorted = sorted(rows, key=lambda r: doc_priority(r.get("document_type", "")))
        texts: list[str] = []
        n_docs_used = 0
        total_chars = 0
        for row in rows_sorted:
            fp = (row.get("file_path") or "").strip()
            if not fp:
                continue
            tp = (text_root / fp).with_suffix(".txt")
            if not tp.exists():
                continue
            try:
                content = tp.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            if not content.strip():
                continue
            texts.append(content)
            n_docs_used += 1
            total_chars += len(content)
        if not texts:
            n_missing += 1
            continue
        n_with_text += 1
        joined = "\n".join(texts)
        # Prefer the formal "Reasons for refusal" section when present. This
        # cuts down on false positives from boilerplate policy citations in
        # long officer reports.
        section = extract_refusal_section(joined)
        text_for_matching = section if section else joined
        used_section = section is not None
        mentions_heat_pump = bool(HEAT_PUMP_TOKENS.search(text_for_matching))
        matched: list[str] = []
        for label, pat in rules:
            if pat.search(text_for_matching):
                matched.append(label)
        for lab in matched:
            label_counts[lab] += 1
        for i, a in enumerate(matched):
            for b in matched[i + 1 :]:
                label_pair_counts[tuple(sorted([a, b]))] += 1
        per_app_rows.append(
            {
                "application_uid": uid,
                "authority_name": rows_sorted[0].get("authority_name", ""),
                "reference": rows_sorted[0].get("reference", ""),
                "planning_decision": rows_sorted[0].get("planning_decision", ""),
                "n_docs_used": n_docs_used,
                "total_chars": total_chars,
                "used_refusal_section": used_section,
                "mentions_heat_pump": mentions_heat_pump,
                "n_labels": len(matched),
                "labels": "; ".join(matched),
            }
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Per-app classifications
    out_app = args.output_dir / "per_app_refusal_labels.csv"
    with out_app.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "application_uid",
                "authority_name",
                "reference",
                "planning_decision",
                "n_docs_used",
                "total_chars",
                "used_refusal_section",
                "mentions_heat_pump",
                "n_labels",
                "labels",
            ],
        )
        w.writeheader()
        w.writerows(per_app_rows)

    # Corpus-wide tally
    out_tally = args.output_dir / "label_tally.csv"
    with out_tally.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["label", "n_apps", "pct_of_text_apps"])
        for lab, n in label_counts.most_common():
            pct = round(100.0 * n / n_with_text, 1) if n_with_text else 0.0
            w.writerow([lab, n, pct])

    # Co-occurrence pairs
    out_pairs = args.output_dir / "label_pairs.csv"
    with out_pairs.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["label_a", "label_b", "n_apps"])
        for (a, b), n in label_pair_counts.most_common(50):
            w.writerow([a, b, n])

    print(f"apps with at least one text doc: {n_with_text}")
    print(f"apps with no extractable text:   {n_missing}")
    print(f"wrote {out_app}")
    print(f"wrote {out_tally}")
    print(f"wrote {out_pairs}")
    print()
    print("Top labels:")
    for lab, n in label_counts.most_common(15):
        pct = 100.0 * n / max(1, n_with_text)
        print(f"  {lab:30s}  {n:5d}  {pct:5.1f}%")


if __name__ == "__main__":
    main()
