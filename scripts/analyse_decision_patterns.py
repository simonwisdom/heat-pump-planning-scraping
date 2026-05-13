#!/usr/bin/env python3
# ruff: noqa: E501  -- HTML/SVG template strings are kept on one line so the source can be searched against the rendered report.
"""Generate ASHP approval/refusal analysis tables and an HTML report."""

from __future__ import annotations

import argparse
import csv
import html
import json
import math
import os
import re
import sqlite3
import statistics
import subprocess
import tomllib
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

ROOT = Path(__file__).resolve().parents[1]
# /tmp/ashp.db is the fresh VPS snapshot written by ashp_local_explore.refresh_local_db()
# and used by the dashboard wrapper. The repo-local copy lags and lacks the merged
# broad_hp rows + source_scrape column, so we default to the snapshot path.
DEFAULT_DB = Path("/tmp/ashp.db")
REPO_LOCAL_DB = ROOT / "_local/workstreams/01_heat_pump_applications/data/raw/ashp.db"
DEFAULT_OUTPUT_DIR = ROOT / "reports/heat-pump-decisions"
RULES_DIR = ROOT / "rules"
MCS_YEARLY_COMBINED = ROOT / "data/MCS_installation_timeline_chart_data/Yearly_Installation_Timeline_Records.csv"
MCS_YEARLY_ASHP = (
    ROOT / "data/MCS_installation_timeline_chart_data/Yearly_Installation_Timeline_Records_Air_Source_Heat_Pump.csv"
)
MCS_LA_BY_YEAR = ROOT / "data/external_heat_pump_data/MCS_heatpump_installations_by_LA_by_year.csv"
LPA_LOOKUP = ROOT / "_local/geo/authority_lpa_lookup.csv"

# Quoted search terms passed to the PlanIt API for each scrape.
# Kept here as plain strings so the dashboard can show what was searched without
# importing the scraper modules (which require third-party deps to load).
ASHP_SEARCH_TERM_LABELS = ['"air source heat pump"', "ASHP"]
BROAD_HEAT_PUMP_SEARCH_TERM_LABELS = [
    '"heat pump" OR "heat pumps"',
    '"ground source heat pump" OR "ground source heat pumps" OR GSHP',
    '"water source heat pump" OR "water source heat pumps" OR WSHP',
]


def _git_short_sha() -> str | None:
    """Return the short SHA of HEAD, or None if not in a git checkout."""
    try:
        out = subprocess.run(
            ["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    sha = out.stdout.strip()
    return sha or None


def _git_dirty() -> bool:
    """True if the working tree has uncommitted changes."""
    try:
        out = subprocess.run(
            ["git", "-C", str(ROOT), "status", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
    return bool(out.stdout.strip())


def _db_meta(db_path: Path) -> dict:
    """Return mtime + applications row count for the source DB."""
    meta: dict = {"path": str(db_path)}
    try:
        meta["modified"] = datetime.fromtimestamp(os.path.getmtime(db_path), tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M UTC"
        )
    except OSError:
        meta["modified"] = None
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            row = conn.execute("SELECT COUNT(*) FROM applications").fetchone()
            meta["row_count"] = int(row[0]) if row else None
    except sqlite3.Error:
        meta["row_count"] = None
    return meta


def format_search_terms_label(source_scrape: str) -> str:
    """Human-readable list of PlanIt search terms used for ``source_scrape``."""
    if source_scrape == "ashp":
        terms = ASHP_SEARCH_TERM_LABELS
    elif source_scrape == "broad_hp":
        terms = BROAD_HEAT_PUMP_SEARCH_TERM_LABELS
    elif source_scrape == "all":
        terms = ASHP_SEARCH_TERM_LABELS + BROAD_HEAT_PUMP_SEARCH_TERM_LABELS
    else:
        return ""
    return ", ".join(f"<code>{html.escape(t)}</code>" for t in terms)


@dataclass
class Rule:
    """One ordered classification rule loaded from a TOML rule file."""

    label: str
    pattern: re.Pattern[str]
    why: str = ""
    raw_pattern: str = ""


def _load_ordered_rules(toml_path: Path) -> list[Rule]:
    """Load an ordered [[rule]] table from a TOML file."""
    with toml_path.open("rb") as f:
        data = tomllib.load(f)
    out: list[Rule] = []
    for entry in data.get("rule", []):
        out.append(
            Rule(
                label=entry["label"],
                pattern=re.compile(entry["pattern"], re.I),
                why=entry.get("why", ""),
                raw_pattern=entry["pattern"],
            )
        )
    if not out:
        raise ValueError(f"No [[rule]] entries found in {toml_path}")
    return out


def _load_lookup_csv(csv_path: Path, key_col: str, value_col: str) -> dict[str, str]:
    """Load a two-column CSV into a dict."""
    import csv as _csv  # local alias to avoid shadowing the top-level csv import

    out: dict[str, str] = {}
    with csv_path.open(newline="") as f:
        for row in _csv.DictReader(f):
            out[row[key_col]] = row[value_col]
    return out


DECISION_RULES: list[Rule] = _load_ordered_rules(RULES_DIR / "decision_rules.toml")
_DECISION_FIRE_COUNTS: Counter[str] = Counter()


POSTCODE_REGION = _load_lookup_csv(RULES_DIR / "postcode_to_region.csv", "postcode_area", "region")
AUTHORITY_REGION_FALLBACK = _load_lookup_csv(RULES_DIR / "authority_region_fallback.csv", "authority", "region")


@dataclass
class App:
    uid: str
    authority: str
    raw_decision: str
    decision: str
    decision_year: str
    start_year: str
    app_type: str
    region: str
    portal_type: str
    has_documentation_url: bool
    has_positive_document_count: bool
    is_listed: bool
    is_flat: bool
    is_wind_turbine: bool
    mentions_noise: bool
    source_scrape: str


def normalise_decision(raw: str | None) -> str:
    if not raw:
        _DECISION_FIRE_COUNTS["__no_decision__"] += 1
        return "no_decision"
    for rule in DECISION_RULES:
        if rule.pattern.search(raw):
            _DECISION_FIRE_COUNTS[rule.label] += 1
            return rule.label
    _DECISION_FIRE_COUNTS["__other__"] += 1
    return "other"


def postcode_to_region(postcode: str | None) -> str:
    if not postcode:
        return "Unknown"
    match = re.match(r"^([A-Z]{1,2})", postcode.strip().upper())
    if not match:
        return "Unknown"
    letters = match.group(1)
    return POSTCODE_REGION.get(letters) or (POSTCODE_REGION.get(letters[0]) if len(letters) > 1 else None) or "Unknown"


def pct(numerator: int | float, denominator: int | float) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator * 100


def fmt_pct(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.1f}%"


def fmt_int(value: int) -> str:
    return f"{value:,}"


def year_from(value: str | None) -> str:
    return value[:4] if value else "Unknown"


_APP_TYPE_RULES: list[Rule] = _load_ordered_rules(RULES_DIR / "app_type_rules.toml")
_APP_TYPE_FIRE_COUNTS: Counter[str] = Counter()
_APP_TYPE_SOURCE_COUNTS: Counter[str] = Counter()


@dataclass
class EdgeCaseMatch:
    field: str
    pattern: re.Pattern[str]
    raw_pattern: str


@dataclass
class EdgeCaseRule:
    flag: str
    matches: list[EdgeCaseMatch]
    why: str = ""


def _load_edge_case_rules(toml_path: Path) -> list[EdgeCaseRule]:
    """Load edge-case keyword flags from a TOML file.

    Each top-level table is one flag containing one or more [[<flag>.match]]
    sub-tables, each with a `field` and `pattern`. A flag fires if any of its
    matches matches its named field. Flags are independent (an app can fire
    several).
    """
    with toml_path.open("rb") as f:
        data = tomllib.load(f)
    out: list[EdgeCaseRule] = []
    for flag, entry in data.items():
        if not isinstance(entry, dict):
            continue
        match_specs = entry.get("match", [])
        if not match_specs:
            continue
        out.append(
            EdgeCaseRule(
                flag=flag,
                matches=[
                    EdgeCaseMatch(
                        field=m["field"],
                        pattern=re.compile(m["pattern"], re.I),
                        raw_pattern=m["pattern"],
                    )
                    for m in match_specs
                ],
                why=entry.get("why", ""),
            )
        )
    return out


EDGE_CASE_RULES: list[EdgeCaseRule] = _load_edge_case_rules(RULES_DIR / "edge_case_keywords.toml")
_EDGE_CASE_FIRE_COUNTS: Counter[str] = Counter()


def _evaluate_edge_cases(app_type: str, description: str) -> dict[str, bool]:
    """Apply each edge-case rule and return {flag_name: True/False}."""
    field_values = {
        "app_type_lower": app_type.lower(),
        "description": description,
    }
    out: dict[str, bool] = {}
    for rule in EDGE_CASE_RULES:
        hit = any(m.pattern.search(field_values.get(m.field, "")) for m in rule.matches)
        out[rule.flag] = hit
        if hit:
            _EDGE_CASE_FIRE_COUNTS[rule.flag] += 1
    return out


def classify_app_type_fallback(raw_json_type: str | None, description: str) -> str:
    """Best-effort canonical app-type label when planning_application_type is missing.

    Searches both the raw council-portal `application_type` and the application
    description for canonical-bucket signals, using the ordered rules in
    rules/app_type_rules.toml. Combining the two sources avoids a noisy raw
    value (e.g. "Status" or "Deemed Regs 3") from short-circuiting a clear
    signal in the description. Defaults to "Full" if nothing matches.
    """
    haystack = " ".join(s for s in (raw_json_type, description) if s and s.strip())
    if not haystack.strip():
        _APP_TYPE_FIRE_COUNTS["__default_full__"] += 1
        return "Full"
    for rule in _APP_TYPE_RULES:
        if rule.pattern.search(haystack):
            _APP_TYPE_FIRE_COUNTS[rule.label] += 1
            return rule.label
    _APP_TYPE_FIRE_COUNTS["__default_full__"] += 1
    return "Full"


def _extract_json_app_type(other_fields_json: str | None) -> str | None:
    if not other_fields_json:
        return None
    try:
        data = json.loads(other_fields_json)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    value = data.get("application_type")
    return value.strip() if isinstance(value, str) and value.strip() else None


def load_apps(db_path: Path, source_scrape: str) -> list[App]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(applications)").fetchall()}
    if "source_scrape" in columns and source_scrape != "all":
        rows = conn.execute(
            """
            SELECT uid, authority_name, planning_decision, decision_date, start_date,
                   planning_application_type, postcode, description, source_scrape,
                   portal_type, documentation_url, n_documents, other_fields_json
            FROM applications
            WHERE source_scrape = ?
            """,
            (source_scrape,),
        ).fetchall()
    elif "source_scrape" in columns:
        rows = conn.execute(
            """
            SELECT uid, authority_name, planning_decision, decision_date, start_date,
                   planning_application_type, postcode, description, source_scrape,
                   portal_type, documentation_url, n_documents, other_fields_json
            FROM applications
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT uid, authority_name, planning_decision, decision_date, start_date,
                   planning_application_type, postcode, description,
                   portal_type, documentation_url, n_documents, other_fields_json
            FROM applications
            """
        ).fetchall()
    conn.close()

    apps = []
    for row in rows:
        desc = row["description"] or ""
        raw_pat = (row["planning_application_type"] or "").strip()
        if raw_pat:
            app_type = raw_pat
            _APP_TYPE_SOURCE_COUNTS["structured_field"] += 1
        else:
            json_blob = row["other_fields_json"] if "other_fields_json" in row.keys() else None
            app_type = classify_app_type_fallback(_extract_json_app_type(json_blob), desc)
            _APP_TYPE_SOURCE_COUNTS["regex_fallback"] += 1
        portal_type = row["portal_type"] or "unknown"
        documentation_url = row["documentation_url"] or ""
        n_documents = row["n_documents"] if row["n_documents"] is not None else 0
        edges = _evaluate_edge_cases(app_type, desc)
        apps.append(
            App(
                uid=row["uid"],
                authority=row["authority_name"] or "Unknown",
                raw_decision=row["planning_decision"] or "",
                decision=normalise_decision(row["planning_decision"]),
                decision_year=year_from(row["decision_date"]),
                start_year=year_from(row["start_date"]),
                app_type=app_type,
                region=postcode_to_region(row["postcode"]),
                portal_type=portal_type,
                has_documentation_url=bool(documentation_url.strip()),
                has_positive_document_count=int(n_documents) > 0,
                is_listed=edges.get("is_listed", False),
                is_flat=edges.get("is_flat", False),
                is_wind_turbine=edges.get("is_wind_turbine", False),
                mentions_noise=edges.get("mentions_noise", False),
                source_scrape=row["source_scrape"]
                if "source_scrape" in row.keys() and row["source_scrape"]
                else source_scrape,
            )
        )

    # Resolve "Unknown" regions: prefer the authority's modal postcode-derived
    # region; fall back to the explicit AUTHORITY_REGION_FALLBACK map for the
    # handful of authorities whose apps never carry a postcode in the DB.
    region_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for app in apps:
        if app.region != "Unknown":
            region_counts[app.authority][app.region] += 1
    authority_modal: dict[str, str] = {
        authority: counter.most_common(1)[0][0] for authority, counter in region_counts.items()
    }
    for app in apps:
        if app.region == "Unknown":
            resolved = authority_modal.get(app.authority) or AUTHORITY_REGION_FALLBACK.get(app.authority)
            if resolved:
                app.region = resolved
    return apps


def count_decisions(apps: Iterable[App]) -> Counter[str]:
    return Counter(app.decision for app in apps)


def row_from_counts(label_key: str, label: str, counts: Counter[str]) -> dict[str, int | float | str | None]:
    total = sum(counts.values())
    approved = counts["approved"]
    refused = counts["refused"]
    decided = approved + refused
    return {
        label_key: label,
        "total": total,
        "approved": approved,
        "refused": refused,
        "withdrawn": counts["withdrawn"],
        "discharge": counts["discharge"],
        "split": counts["split"],
        "pending": counts["pending"],
        "other": counts["other"],
        "no_decision": counts["no_decision"],
        "decided": decided,
        "approval_rate": pct(approved, decided),
        "refusal_rate": pct(refused, decided),
        "no_decision_share": pct(counts["no_decision"], total),
    }


def grouped_table(apps: list[App], label_key: str, key_fn: Callable[[App], str], min_total: int = 1) -> list[dict]:
    groups: dict[str, Counter[str]] = defaultdict(Counter)
    for app in apps:
        groups[key_fn(app)][app.decision] += 1
    rows = [
        row_from_counts(label_key, key, counts) for key, counts in groups.items() if sum(counts.values()) >= min_total
    ]
    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def serialise_rows(rows: list[dict]) -> list[dict]:
    out = []
    for row in rows:
        clean = {}
        for key, value in row.items():
            if isinstance(value, float):
                clean[key] = round(value, 1)
            else:
                clean[key] = value
        out.append(clean)
    return out


def compute_council_refusal_rates(apps: list["App"], min_decided: int = 50) -> list[dict]:
    """Per-council raw refusal rates for councils with at least ``min_decided`` decisions.

    Each row carries the council name, its dominant region, decided/refused counts,
    refusal rate (percent), and no-decision share. Sorted by refusal rate desc.
    """
    by_council: dict[str, list[App]] = defaultdict(list)
    for app in apps:
        by_council[app.authority].append(app)

    rows = []
    for authority, council_apps in by_council.items():
        decided_apps = [a for a in council_apps if a.decision in ("approved", "refused")]
        n_decided = len(decided_apps)
        if n_decided < min_decided:
            continue
        n_refused = sum(1 for a in decided_apps if a.decision == "refused")
        no_decision = sum(1 for a in council_apps if a.decision == "no_decision")
        total_apps = len(council_apps)
        region_counts: Counter[str] = Counter(a.region for a in decided_apps)
        dominant_region = region_counts.most_common(1)[0][0] if region_counts else "Unknown"
        rows.append(
            {
                "authority": authority,
                "dominant_region": dominant_region,
                "n_decided": n_decided,
                "n_refused": n_refused,
                "refusal_rate": (n_refused / n_decided) * 100,
                "no_decision_share": (no_decision / total_apps * 100) if total_apps else 0.0,
            }
        )
    rows.sort(key=lambda r: -r["refusal_rate"])
    return rows


def _nice_axis_step(value_range: float, target_ticks: int = 6) -> float:
    """Pick a "nice" tick step (1, 2, or 5 × 10^n) covering ``value_range``.

    Used to keep axis labels human-readable (e.g. 0/10/20/...) instead of
    landing on awkward fractions of the range.
    """
    if value_range <= 0:
        return 1.0
    raw_step = value_range / max(1, target_ticks)
    magnitude = 10 ** math.floor(math.log10(raw_step))
    for nice_mult in (1, 2, 5, 10):
        step = nice_mult * magnitude
        if step >= raw_step:
            return step
    return 10 * magnitude


def svg_histogram(
    values: list[float],
    bin_width: float = 2.0,
    ref_line: float | None = None,
    width: int = 760,
    height: int = 240,
    x_label: str = "Refusal rate (%)",
) -> str:
    """Render a histogram of ``values`` as an SVG string."""
    if not values:
        return ""
    pad_l, pad_r, pad_t, pad_b = 52, 20, 22, 42
    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b

    lo = math.floor(min(values) / bin_width) * bin_width
    hi = math.ceil(max(values) / bin_width) * bin_width
    if hi <= lo:
        hi = lo + bin_width
    n_bins = max(1, int(round((hi - lo) / bin_width)))
    counts = [0] * n_bins
    for v in values:
        idx = min(n_bins - 1, max(0, int((v - lo) / bin_width)))
        counts[idx] += 1
    max_count = max(counts) or 1

    bar_w = inner_w / n_bins
    bars = []
    for i, c in enumerate(counts):
        x = pad_l + i * bar_w
        bar_h = c / max_count * inner_h
        y = pad_t + inner_h - bar_h
        bars.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w - 1:.1f}" height="{bar_h:.1f}" />')

    grid = []
    for frac in (0, 0.25, 0.5, 0.75, 1):
        tick = max_count * frac
        y = pad_t + inner_h - frac * inner_h
        grid.append(f'<line x1="{pad_l}" x2="{width - pad_r}" y1="{y:.1f}" y2="{y:.1f}" class="grid" />')
        grid.append(f'<text x="{pad_l - 8}" y="{y + 4:.1f}" text-anchor="end">{tick:.0f}</text>')

    x_ticks = []
    x_step = _nice_axis_step(hi - lo, target_ticks=6)
    first_tick = math.ceil(lo / x_step) * x_step
    tick_value = first_tick
    while tick_value <= hi + 1e-9:
        frac = (tick_value - lo) / (hi - lo) if hi > lo else 0
        x = pad_l + frac * inner_w
        # Drop trailing ".0" when the step is a whole number.
        label = f"{tick_value:.0f}" if abs(tick_value - round(tick_value)) < 1e-6 else f"{tick_value:g}"
        x_ticks.append(f'<text x="{x:.1f}" y="{height - 22}" text-anchor="middle">{label}</text>')
        tick_value += x_step
    x_ticks.append(
        f'<text x="{pad_l + inner_w / 2:.1f}" y="{height - 6}" text-anchor="middle">{html.escape(x_label)}</text>'
    )

    ref_svg = ""
    if ref_line is not None and lo <= ref_line <= hi:
        rx = pad_l + (ref_line - lo) / (hi - lo) * inner_w
        ref_svg = (
            f'<line x1="{rx:.1f}" x2="{rx:.1f}" y1="{pad_t}" y2="{pad_t + inner_h}" class="ref" />'
            f'<text x="{rx + 4:.1f}" y="{pad_t + 12}" class="ref-label">national mean {ref_line:.1f}%</text>'
        )

    return f"""
    <svg class="chart histogram" viewBox="0 0 {width} {height}" role="img" aria-label="Distribution of council refusal rates">
      <g class="axis">{"".join(grid)}</g>
      {"".join(bars)}
      {ref_svg}
      <g class="x-labels">{"".join(x_ticks)}</g>
    </svg>
    """


def svg_line_chart(
    rows: list[dict],
    x_key: str,
    y_key: str,
    width: int = 760,
    height: int = 300,
    title: str = "",
    y_label: str = "",
) -> str:
    data = [(str(row[x_key]), row[y_key]) for row in rows if isinstance(row[y_key], (int, float))]
    if not data:
        return ""
    pad_l, pad_r, pad_t, pad_b = 70, 20, 30 if title else 22, 42
    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b
    max_y = max(10, max(float(y) for _, y in data))
    x_step = inner_w / max(1, len(data) - 1)

    points = []
    for i, (_, y) in enumerate(data):
        x = pad_l + i * x_step
        y_pos = pad_t + inner_h - (float(y) / max_y * inner_h)
        points.append((x, y_pos))
    polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    x_labels = "\n".join(
        f'<text x="{pad_l + i * x_step:.1f}" y="{height - 12}" text-anchor="middle">{html.escape(label)}</text>'
        for i, (label, _) in enumerate(data)
    )
    y_ticks = []
    for frac in (0, 0.25, 0.5, 0.75, 1):
        tick_value = max_y * frac
        y = pad_t + inner_h - frac * inner_h
        y_ticks.append(f'<line x1="{pad_l}" x2="{width - pad_r}" y1="{y:.1f}" y2="{y:.1f}" class="grid" />')
        y_ticks.append(f'<text x="{pad_l - 8}" y="{y + 4:.1f}" text-anchor="end">{tick_value:.0f}%</text>')
    circles = "\n".join(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.5" />' for x, y in points)
    title_svg = f'<text x="{pad_l}" y="18" class="chart-title">{html.escape(title)}</text>' if title else ""
    y_label_svg = ""
    if y_label:
        cy = pad_t + inner_h / 2
        y_label_svg = (
            f'<text x="16" y="{cy:.1f}" text-anchor="middle" '
            f'transform="rotate(-90 16 {cy:.1f})" class="axis-title">{html.escape(y_label)}</text>'
        )
    return f"""
    <svg class="chart line-chart" viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title or "Line chart")}">
      {title_svg}
      {y_label_svg}
      <g class="axis">{"".join(y_ticks)}</g>
      <polyline points="{polyline}" />
      {circles}
      <g class="x-labels">{x_labels}</g>
    </svg>
    """


def load_mcs_yearly(csv_path: Path) -> dict[int, int]:
    """Load yearly MCS-certified installation totals from the chart-data CSV."""
    out: dict[int, int] = {}
    if not csv_path.exists():
        return out
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            year_str = (row.get("Year") or "").strip()
            count_str = (row.get("MCS Certified Installations Total") or "").strip()
            if not year_str or not count_str:
                continue
            try:
                out[int(year_str)] = int(count_str)
            except ValueError:
                continue
    return out


def _normalise_la_name(name: str) -> str:
    """Lowercase + replace curly apostrophe with straight one.

    The lookup CSV stores names with U+2019 (e.g. "King’s Lynn..."), the MCS
    CSV with U+0027 ("King's Lynn..."). Without normalising, the join silently
    fails and the LA shows up with zero MCS installs.
    """
    return name.strip().lower().replace("’", "'")


LPA_TO_MCS_OVERRIDE = _load_lookup_csv(RULES_DIR / "mcs_lpa_overrides.csv", "lpa_name_lower", "mcs_la_lower")


DB_AUTHORITY_TO_MCS = _load_lookup_csv(RULES_DIR / "mcs_authority_overrides.csv", "db_authority_lower", "mcs_la_lower")


def load_mcs_la_by_year(csv_path: Path) -> dict[tuple[int, str], int]:
    """Return {(year, lad_name_lower): installation_count}."""
    out: dict[tuple[int, str], int] = {}
    if not csv_path.exists():
        return out
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                year = int(row["year"])
                count = int(row["installationCount"])
            except (KeyError, ValueError, TypeError):
                continue
            name = _normalise_la_name(row.get("areaName") or "")
            if not name:
                continue
            key = (year, name)
            out[key] = out.get(key, 0) + count
    return out


def load_authority_lpa_lookup(csv_path: Path) -> dict[str, list[str]]:
    """Return {planit_authority_lower: [lpa_name_lower_without_LPA_suffix, ...]}.

    LPA names are also passed through ``LPA_TO_MCS_OVERRIDE`` so that pre-2023
    districts that no longer carry MCS data are translated to the post-2023
    unitary that does.
    """
    out: dict[str, list[str]] = {}
    if not csv_path.exists():
        return out
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            status = (row.get("status") or "").strip()
            if status not in ("matched", "joint", "new_unitary"):
                continue
            db_name = (row.get("db_authority_name") or "").strip().lower()
            lpa_name = (row.get("lpa22nm") or "").strip()
            if lpa_name.endswith(" LPA"):
                lpa_name = lpa_name[:-4]
            if not (db_name and lpa_name):
                continue
            normalised = _normalise_la_name(lpa_name)
            mapped = LPA_TO_MCS_OVERRIDE.get(normalised, normalised)
            existing = out.setdefault(db_name, [])
            if mapped not in existing:
                existing.append(mapped)
    # Augment with direct db→MCS overrides for authorities missing from the CSV.
    for db_name, mcs_name in DB_AUTHORITY_TO_MCS.items():
        if mcs_name not in out.get(db_name, []):
            out.setdefault(db_name, []).append(mcs_name)
    return out


def compute_region_coverage(
    apps: list["App"],
    council_coverage: list[dict],
) -> list[dict]:
    """Roll up matched-authority MCS vs planning apps to region.

    Each authority is assigned the region most apps in that authority sit in
    (postcode-derived). The rollup only includes authorities for which we
    matched MCS LA names, so it is comparable to per-council coverage.
    """
    if not council_coverage:
        return []
    region_by_authority: dict[str, str] = {}
    region_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for a in apps:
        region_counts[a.authority][a.region] += 1
    for authority, counter in region_counts.items():
        region_by_authority[authority] = counter.most_common(1)[0][0]

    totals: dict[str, dict[str, int]] = defaultdict(lambda: {"mcs": 0, "planning": 0, "n_authorities": 0})
    for row in council_coverage:
        region = region_by_authority.get(row["authority"], "Unknown")
        totals[region]["mcs"] += int(row["mcs_installs"])
        totals[region]["planning"] += int(row["planning_apps"])
        totals[region]["n_authorities"] += 1

    rows = []
    for region, agg in totals.items():
        mcs = agg["mcs"]
        planning = agg["planning"]
        rows.append(
            {
                "region": region,
                "n_authorities": agg["n_authorities"],
                "mcs_installs": mcs,
                "planning_apps": planning,
                "coverage_pct": (planning / mcs * 100) if mcs else None,
            }
        )
    rows.sort(key=lambda r: -(r["coverage_pct"] or 0))
    return rows


def compute_council_coverage(
    apps: list["App"],
    mcs_la_by_year: dict[tuple[int, str], int],
    authority_lookup: dict[str, list[str]],
    year_range: tuple[int, int],
) -> list[dict]:
    """Per-council planning apps vs MCS installs, restricted to year_range (inclusive)."""
    if not mcs_la_by_year:
        return []
    y_lo, y_hi = year_range
    apps_per_council: dict[str, int] = defaultdict(int)
    decided_per_council: dict[str, int] = defaultdict(int)
    for a in apps:
        if not a.start_year or not a.start_year.isdigit():
            continue
        y = int(a.start_year)
        if y < y_lo or y > y_hi:
            continue
        apps_per_council[a.authority] += 1
        if a.decision in ("approved", "refused"):
            decided_per_council[a.authority] += 1

    mcs_names = {n for (_y, n) in mcs_la_by_year}
    rows = []
    for authority, n_apps in apps_per_council.items():
        lpa_names = authority_lookup.get(authority.lower(), [])
        if not lpa_names and authority.lower() in mcs_names:
            lpa_names = [authority.lower()]
        if not lpa_names:
            continue
        mcs_total = sum(mcs_la_by_year.get((y, n), 0) for y in range(y_lo, y_hi + 1) for n in lpa_names)
        if mcs_total == 0:
            continue
        rows.append(
            {
                "authority": authority,
                "matched_lpas": "; ".join(lpa_names),
                "mcs_installs": mcs_total,
                "planning_apps": n_apps,
                "decided_apps": decided_per_council[authority],
                "coverage_pct": n_apps / mcs_total * 100,
                "year_range": f"{y_lo}–{y_hi}",
            }
        )
    rows.sort(key=lambda r: -r["mcs_installs"])
    return rows


def svg_dual_line_chart(
    rows: list[dict],
    x_key: str,
    y1_key: str,
    y2_key: str,
    y1_label: str,
    y2_label: str,
    width: int = 760,
    height: int = 300,
) -> str:
    """Two-series line chart on a shared linear y-axis."""
    data = [
        (str(row[x_key]), float(row[y1_key]), float(row[y2_key]))
        for row in rows
        if isinstance(row.get(y1_key), (int, float)) and isinstance(row.get(y2_key), (int, float))
    ]
    if not data:
        return ""
    pad_l, pad_r, pad_t, pad_b = 60, 20, 56, 42
    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b
    max_y_raw = max(1.0, max(max(y1, y2) for _, y1, y2 in data))
    # Pick a "nice" step (1, 2, or 5 × 10^n) targeting ~6 ticks, then ceil the
    # axis maximum to a multiple of that step. Keeps tick labels readable
    # (e.g. 0/10k/20k/...) instead of off-by-quarter values.
    target_ticks = 6
    raw_step = max_y_raw / target_ticks
    magnitude = 10 ** math.floor(math.log10(raw_step))
    for nice_mult in (1, 2, 5, 10):
        y_step = nice_mult * magnitude
        if y_step >= raw_step:
            break
    max_y = math.ceil(max_y_raw / y_step) * y_step
    x_step = inner_w / max(1, len(data) - 1)

    def _points(idx: int) -> list[tuple[float, float]]:
        return [
            (pad_l + i * x_step, pad_t + inner_h - (vals[idx] / max_y * inner_h))
            for i, vals in enumerate((row[1], row[2]) for row in data)
        ]

    pts1 = [(pad_l + i * x_step, pad_t + inner_h - (y1 / max_y * inner_h)) for i, (_, y1, _) in enumerate(data)]
    pts2 = [(pad_l + i * x_step, pad_t + inner_h - (y2 / max_y * inner_h)) for i, (_, _, y2) in enumerate(data)]
    poly1 = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts1)
    poly2 = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts2)
    circles1 = "".join(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" />' for x, y in pts1)
    circles2 = "".join(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" />' for x, y in pts2)

    y_ticks = []
    n_steps = int(round(max_y / y_step))
    for i in range(n_steps + 1):
        v = i * y_step
        frac = v / max_y if max_y else 0
        y = pad_t + inner_h - frac * inner_h
        y_ticks.append(f'<line x1="{pad_l}" x2="{width - pad_r}" y1="{y:.1f}" y2="{y:.1f}" class="grid" />')
        if v >= 1000:
            label = f"{v / 1000:.0f}k"
        else:
            label = f"{v:.0f}"
        y_ticks.append(f'<text x="{pad_l - 8}" y="{y + 4:.1f}" text-anchor="end">{label}</text>')

    x_labels = "".join(
        f'<text x="{pad_l + i * x_step:.1f}" y="{height - 22}" text-anchor="middle">{html.escape(label)}</text>'
        for i, (label, _, _) in enumerate(data)
    )

    legend = (
        f'<g class="legend" transform="translate({pad_l},6)">'
        f'<line x1="0" x2="22" y1="6" y2="6" class="series-1" /><text x="28" y="10">{html.escape(y1_label)}</text>'
        f'<line x1="0" x2="22" y1="24" y2="24" class="series-2" /><text x="28" y="28">{html.escape(y2_label)}</text>'
        f"</g>"
    )

    return f"""
    <svg class="chart dual-line" viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(y1_label)} vs {html.escape(y2_label)} by year">
      <g class="axis">{"".join(y_ticks)}</g>
      <polyline class="series-1" points="{poly1}" />
      <polyline class="series-2" points="{poly2}" />
      <g class="series-1-pts">{circles1}</g>
      <g class="series-2-pts">{circles2}</g>
      <g class="x-labels">{x_labels}</g>
      {legend}
    </svg>
    """


def svg_hbar_chart(
    data: list[tuple[str, float]],
    width: int = 760,
    row_h: int = 26,
    ref_line: float | None = None,
    ref_label: str | None = None,
    aria_label: str = "",
) -> str:
    """Horizontal bar chart with a percentage axis and optional reference line."""
    if not data:
        return ""
    pad_l, pad_r, pad_t, pad_b = 170, 60, 22, 28
    n = len(data)
    height = pad_t + n * row_h + pad_b
    inner_w = width - pad_l - pad_r
    raw_max = max(float(v) for _, v in data)
    if ref_line is not None:
        raw_max = max(raw_max, float(ref_line))
    nice_steps = [5, 10, 15, 20, 25, 30, 40, 50, 75, 100]
    axis_max = next((s for s in nice_steps if s >= raw_max), 100)

    def x_for(v: float) -> float:
        return pad_l + (v / axis_max) * inner_w

    chart_top = pad_t
    chart_bot = pad_t + n * row_h
    # Pick a tick step that yields whole-number labels and ~5–7 ticks.
    tick_step = next((s for s in (1, 2, 5, 10, 20, 25) if axis_max / s <= 7), 50)
    ticks = []
    t = 0.0
    while t <= axis_max + 1e-6:
        ticks.append(t)
        t += tick_step
    grid = []
    for t in ticks:
        x = x_for(t)
        grid.append(f'<line x1="{x:.1f}" x2="{x:.1f}" y1="{chart_top}" y2="{chart_bot}" class="grid" />')
        grid.append(f'<text x="{x:.1f}" y="{chart_bot + 16}" text-anchor="middle">{t:.0f}%</text>')

    bars = []
    for i, (label, value) in enumerate(data):
        y = chart_top + i * row_h
        bw = max(0.0, x_for(float(value)) - pad_l)
        bars.append(
            f'<text class="row-label" x="{pad_l - 10}" y="{y + 17}" text-anchor="end">{html.escape(label)}</text>'
        )
        bars.append(f'<rect x="{pad_l}" y="{y + 5}" width="{bw:.1f}" height="16" rx="2" />')
        bars.append(f'<text class="value-label" x="{pad_l + bw + 6:.1f}" y="{y + 17}">{float(value):.1f}%</text>')

    ref = ""
    if ref_line is not None:
        rx = x_for(float(ref_line))
        ref = f'<line x1="{rx:.1f}" x2="{rx:.1f}" y1="{chart_top}" y2="{chart_bot}" class="ref" />'
        if ref_label:
            # Place the label in the top margin so a long top bar can't cover it.
            ref += f'<text class="ref-label" x="{rx + 4:.1f}" y="{chart_top - 6}">{html.escape(ref_label)}</text>'

    return f"""
    <svg class="chart hbar-chart" viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(aria_label)}">
      <g class="axis">{"".join(grid)}</g>
      {ref}
      {"".join(bars)}
    </svg>
    """


def html_table(rows: list[dict], columns: list[tuple[str, str]], limit: int | None = None) -> str:
    display_rows = rows[:limit] if limit else rows
    header = "".join(f"<th>{html.escape(label)}</th>" for _, label in columns)
    body = []
    for row in display_rows:
        cells = []
        for key, _ in columns:
            value = row.get(key, "")
            if isinstance(value, float):
                value = fmt_pct(value) if key.endswith("_rate") or key.endswith("_share") else f"{value:.1f}"
            elif isinstance(value, int):
                value = fmt_int(value)
            cells.append(f"<td>{html.escape(str(value))}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def _render_decision_rules_table(fired: dict[str, int]) -> str:
    rows_html = []
    for rule in DECISION_RULES:
        n = fired.get(rule.label, 0)
        rows_html.append(
            f"<tr><td><code>{html.escape(rule.label)}</code></td>"
            f"<td><code>{html.escape(rule.raw_pattern)}</code></td>"
            f"<td>{fmt_int(n)}</td></tr>"
        )
    for key, label in [("__no_decision__", "blank"), ("__other__", "no match")]:
        n = fired.get(key, 0)
        if n:
            rows_html.append(
                f"<tr><td><code>{html.escape(label)}</code></td><td>&mdash;</td><td>{fmt_int(n)}</td></tr>"
            )
    return (
        '<table class="rule-fire"><thead><tr><th>Bucket</th><th>Pattern</th><th>Apps</th>'
        "</tr></thead><tbody>" + "".join(rows_html) + "</tbody></table>"
    )


def _render_app_type_rules_table(fired: dict[str, int]) -> str:
    rows_html = []
    for rule in _APP_TYPE_RULES:
        n = fired.get(rule.label, 0)
        rows_html.append(
            f"<tr><td><code>{html.escape(rule.label)}</code></td>"
            f"<td><code>{html.escape(rule.raw_pattern)}</code></td>"
            f"<td>{fmt_int(n)}</td></tr>"
        )
    n_default = fired.get("__default_full__", 0)
    if n_default:
        rows_html.append(f"<tr><td><code>default Full</code></td><td>&mdash;</td><td>{fmt_int(n_default)}</td></tr>")
    return (
        '<table class="rule-fire"><thead><tr><th>Bucket</th><th>Pattern</th>'
        "<th>Apps (fallback only)</th></tr></thead><tbody>" + "".join(rows_html) + "</tbody></table>"
    )


def _render_app_type_source_table(source_counts: dict[str, int]) -> str:
    structured = source_counts.get("structured_field", 0)
    fallback = source_counts.get("regex_fallback", 0)
    total = structured + fallback
    if total == 0:
        return ""
    s_pct = structured / total * 100
    f_pct = fallback / total * 100
    return (
        "<table><thead><tr><th>Source</th><th>Apps</th><th>Share</th>"
        "</tr></thead><tbody>"
        f"<tr><td>PlanIt structured field</td>"
        f"<td>{fmt_int(structured)}</td><td>{s_pct:.1f}%</td></tr>"
        f"<tr><td>Regex fallback</td>"
        f"<td>{fmt_int(fallback)}</td><td>{f_pct:.1f}%</td></tr>"
        "</tbody></table>"
    )


def _render_edge_case_rules_table(fired: dict[str, int]) -> str:
    rows_html = []
    for rule in EDGE_CASE_RULES:
        n = fired.get(rule.flag, 0)
        match_html = "<br>".join(
            f"<code>{html.escape(m.field)}</code> ~ <code>{html.escape(m.raw_pattern)}</code>" for m in rule.matches
        )
        rows_html.append(
            f"<tr><td><code>{html.escape(rule.flag)}</code></td><td>{match_html}</td><td>{fmt_int(n)}</td></tr>"
        )
    return (
        '<table class="rule-fire"><thead><tr><th>Flag</th><th>Match clauses</th>'
        "<th>Apps fired</th></tr></thead><tbody>" + "".join(rows_html) + "</tbody></table>"
    )


def _render_outcome_mix_section(summary: dict) -> str:
    """Stacked bar + table showing decision-bucket mix as % of all applications.

    The KPI tiles and most breakdown tables denominate by `decided` (approved +
    refused), which deliberately excludes withdrawn so withdrawals don't
    dilute the refusal rate. This panel restores the % of total framing for
    readers comparing against legacy summaries that quoted shares of all
    applications (approved/refused/withdrawn/undecided/other).
    """
    breakdown = summary.get("decision_breakdown", {}) or {}
    total = sum(int(v) for v in breakdown.values()) or 1
    groups = [
        ("approved", "Approved", int(breakdown.get("approved", 0))),
        ("refused", "Refused", int(breakdown.get("refused", 0))),
        ("withdrawn", "Withdrawn", int(breakdown.get("withdrawn", 0))),
        (
            "undecided",
            "Undecided (no decision recorded or pending)",
            int(breakdown.get("no_decision", 0)) + int(breakdown.get("pending", 0)),
        ),
        (
            "other",
            "Other (condition discharge, split, unclassified)",
            int(breakdown.get("discharge", 0)) + int(breakdown.get("split", 0)) + int(breakdown.get("other", 0)),
        ),
    ]
    segs = []
    rows = []
    for cls, label, n in groups:
        share = n / total * 100
        segs.append(
            f'<span class="seg seg-{cls}" style="width: {share:.2f}%" '
            f'title="{html.escape(label)} {fmt_int(n)} ({share:.1f}%)"></span>'
        )
        rows.append(
            f"<tr>"
            f'<td><span class="legend-dot dot-{cls}"></span>{html.escape(label)}</td>'
            f"<td>{fmt_int(n)}</td>"
            f"<td>{share:.1f}%</td>"
            f"</tr>"
        )
    return (
        '<section class="outcome-mix" aria-label="Outcome mix as share of all applications">'
        "<h3>Outcome mix (% of all applications)</h3>"
        '<p class="note">The refusal rate elsewhere in this report is calculated over '
        "decided (approved + refused) applications only &mdash; withdrawn and undecided are "
        "excluded so they don&rsquo;t dilute the rate. This panel puts every bucket on the "
        "same denominator.</p>"
        f'<div class="stacked-bar" role="img" aria-label="Outcome mix">{"".join(segs)}</div>'
        f'<table class="mix-table"><thead><tr><th>Outcome</th><th>Apps</th><th>Share</th>'
        f"</tr></thead><tbody>{''.join(rows)}</tbody></table>"
        "</section>"
    )


def render_html(
    output_dir: Path,
    summary: dict,
    tables: dict[str, list[dict]],
    panel_context: dict | None = None,
) -> str:
    by_start_year = tables["by_start_year"]
    by_region = tables["by_region"]
    by_council = tables.get("by_council", [])
    edge_cases = tables.get("edge_cases", [])
    by_application_type = tables.get("by_application_type", [])
    generated = html.escape(summary["generated_at"])
    panel_context = panel_context or {}
    council_min_decided = panel_context.get("council_min_decided", 50)
    national_refusal_rate = panel_context.get("national_refusal_rate")
    mcs_yearly = panel_context.get("mcs_yearly", {}) or {}
    mcs_label = panel_context.get("mcs_label", "MCS-certified installs")
    council_coverage = panel_context.get("council_coverage", []) or []
    region_coverage = panel_context.get("region_coverage", []) or []
    search_terms_label = panel_context.get("search_terms_label", "")

    report_title = html.escape(summary["report_title"])
    corpus_label = html.escape(summary["corpus_label"])

    rules_fired = summary.get("rules_fired", {})
    decision_rules_table = _render_decision_rules_table(rules_fired.get("decision", {}))
    app_type_source_table = _render_app_type_source_table(rules_fired.get("app_type_source", {}))
    app_type_rules_table = _render_app_type_rules_table(rules_fired.get("app_type", {}))
    edge_case_rules_table = _render_edge_case_rules_table(rules_fired.get("edge_case", {}))

    council_rates = [r["refusal_rate"] for r in by_council]
    if council_rates:
        median_rate = statistics.median(council_rates)
        if len(council_rates) >= 4:
            q1 = statistics.quantiles(council_rates, n=4)[0]
            q3 = statistics.quantiles(council_rates, n=4)[2]
            iqr_str = f"middle half {q1:.1f}&ndash;{q3:.1f}%"
        else:
            iqr_str = "too few councils to summarise the spread"
        headline_a = (
            f"Refusal rates vary widely across the {len(council_rates)} councils with at least {council_min_decided} "
            f"decisions &mdash; median {median_rate:.1f}%, {iqr_str}, max {max(council_rates):.1f}%."
        )
    else:
        headline_a = "No councils meet the inclusion threshold for this dataset."

    panel_a_svg = svg_histogram(
        council_rates,
        bin_width=2.0,
        ref_line=national_refusal_rate,
        x_label="Council refusal rate (%)",
    )

    exclusion_note = f"<p class='note'>Excludes councils with fewer than {council_min_decided} decisions.</p>"

    # Top-10 councils by raw refusal rate (n >= council_min_decided).
    high_excess_section = ""
    top_councils = by_council[:10]
    if top_councils:
        top_rows_html = "".join(
            f"<tr>"
            f"<td>{html.escape(r['authority'])}</td>"
            f"<td>{html.escape(r['dominant_region'])}</td>"
            f"<td>{fmt_int(r['n_decided'])}</td>"
            f"<td>{r['refusal_rate']:.1f}%</td>"
            f"</tr>"
            for r in top_councils
        )
        high_excess_section = (
            f"<h3>Councils with the highest refusal rates (top 10, n &ge; {council_min_decided})</h3>"
            f'<p class="note">Raw refusal rates &mdash; not adjusted for case mix or region. Treat individual '
            f"councils as candidates for follow-up rather than verdicts: a high rate may reflect a tougher mix of "
            f"applications (e.g. more conservation-area or flat-related cases) rather than a stricter authority.</p>"
            f"<table><thead><tr>"
            f"<th>Council</th><th>Region</th><th>Decisions</th><th>Refusal rate</th>"
            f"</tr></thead><tbody>{top_rows_html}</tbody></table>"
        )

    # Edge cases section
    edge_section = ""
    if edge_cases and national_refusal_rate is not None:
        baseline = national_refusal_rate

        edge_rows_html = "".join(
            f"<tr>"
            f"<td>{html.escape(r['subset'])}</td>"
            f"<td>{fmt_int(r['total'])}</td>"
            f"<td>{fmt_int(r['decided'])}</td>"
            f"<td>{(r['refusal_rate'] if isinstance(r['refusal_rate'], (int, float)) else 0):.1f}%</td>"
            f"</tr>"
            for r in edge_cases
            if isinstance(r.get("decided"), (int, float)) and r["decided"] > 0
        )
        edge_section = (
            f"<h3>Edge cases flagged in the proposal</h3>"
            f"<p>The project proposal called out four groups of applications to look at specifically &mdash; "
            f"listed buildings, flats, wind turbines, and (for the sound-assessment workstream) noise mentions. "
            f"These groups are picked out by searching application descriptions and addresses for keywords, and "
            f"each is compared to the {baseline:.1f}% national average. Listed buildings and conservation areas "
            f"are actually refused <em>less</em> often than average &mdash; these applications tend to come in well "
            f"prepared, often with extra acoustic or visual mitigation. Applications on flats and the few mentioning "
            f"wind turbines are refused more often. Wind turbine numbers are small, so treat that figure as a hint "
            f"rather than a hard finding.</p>"
            f"<table><thead><tr>"
            f"<th>Group</th><th>Apps</th><th>Decisions</th><th>Refusal rate</th>"
            f"</tr></thead><tbody>{edge_rows_html}</tbody></table>"
            f'<p class="note">Data: <code>edge_cases.csv</code></p>'
            f'<p class="note">Groups are identified by keywords on the application&rsquo;s description and address &mdash; '
            f"&ldquo;listed&rdquo;, &ldquo;grade I/II&rdquo;, or &ldquo;conservation area&rdquo; for heritage; "
            f"&ldquo;flat&rdquo;, &ldquo;apartment&rdquo;, or &ldquo;block&rdquo; for flats; explicit mention for the "
            f"rest. An application can fall into more than one group. The listed / heritage flag is a superset of the "
            f"Heritage consent type above &mdash; it also catches Full applications that mention a conservation area "
            f"or listed-building context, which is why the two refusal rates land close together.</p>"
        )

    # By application type section
    app_type_section = ""
    if by_application_type and national_refusal_rate is not None:
        rows = [r for r in by_application_type if isinstance(r.get("decided"), (int, float)) and r["decided"] > 0]
        # Sort by refusal rate descending so the worst-refusing types lead the table.
        rows = sorted(
            rows,
            key=lambda r: -(r["refusal_rate"] if isinstance(r.get("refusal_rate"), (int, float)) else 0),
        )
        baseline = national_refusal_rate

        # Definitions sourced from Planning Portal consent-type pages and the relevant
        # statutes (Town and Country Planning Act 1990; Planning (Listed Buildings and
        # Conservation Areas) Act 1990). Buckets are PlanIt's canonical labels — see
        # `_APP_TYPE_RULES` for which raw council-portal types map into each.
        app_type_descriptions = {
            "Full": "Detailed proposals for development not covered by permitted-development rights; includes householder works (extensions, conservatories, etc.).",
            "Outline": "Establishes whether the principle of a development is acceptable; specific “reserved matters” are settled in a later application.",
            "Amendment": "Changes to a granted permission — non-material amendments (s.96A) or variations of conditions (s.73, Town and Country Planning Act 1990).",
            "Conditions": "Formal approval (discharge) of details required by conditions on a granted permission, including reserved matters after an outline consent.",
            "Heritage": "Listed Building Consent for works affecting a building of special architectural or historical interest (Planning (Listed Buildings and Conservation Areas) Act 1990).",
            "Trees": "Works to trees protected by a Tree Preservation Order, or s.211 notice for trees in a conservation area.",
            "Other": "Non-standard routes (certificates of lawfulness, prior-approval observations, neighbouring-authority notifications, etc.).",
        }
        at_rows_html = "".join(
            f"<tr>"
            f"<td>{html.escape(str(r['application_type']))}</td>"
            f"<td>{fmt_int(r['total'])}</td>"
            f"<td>{fmt_int(r['decided'])}</td>"
            f"<td>{(r['refusal_rate'] if isinstance(r['refusal_rate'], (int, float)) else 0):.1f}%</td>"
            f"</tr>"
            for r in rows
        )
        # Glossary entries in the same order as the table, only for types present.
        glossary_html = "".join(
            f"<dt>{html.escape(str(r['application_type']))}</dt>"
            f"<dd>{html.escape(app_type_descriptions.get(str(r['application_type']), ''))}</dd>"
            for r in rows
            if app_type_descriptions.get(str(r["application_type"]))
        )
        # Pull out high-refusal types for the narrative.
        notable = [
            r
            for r in rows
            if isinstance(r.get("refusal_rate"), (int, float))
            and r["refusal_rate"] - baseline >= 5
            and int(r.get("decided", 0)) >= 100
        ]
        notable_str = (
            ", ".join(f"{r['application_type']} ({r['refusal_rate']:.1f}%)" for r in notable)
            or "no application types more than 5 pp above baseline"
        )
        app_type_bar_svg = svg_hbar_chart(
            [
                (str(r["application_type"]), float(r["refusal_rate"]))
                for r in rows
                if isinstance(r.get("refusal_rate"), (int, float))
            ],
            ref_line=baseline,
            ref_label=f"national mean {baseline:.1f}%",
            aria_label="Refusal rate by application type",
        )
        app_type_section = (
            f"<h3>By application type</h3>"
            f"<p>The mix of application types matters: some are refused much more often than the "
            f"{baseline:.1f}% national average. Outline applications and amendments stand out: {notable_str}. "
            f"(Trees ranks higher but on only 11 decisions, so the rate is unreliable.)</p>"
            f'<div class="chart-wrap">{app_type_bar_svg}</div>'
            f"<table><thead><tr>"
            f"<th>Application type</th><th>Apps</th><th>Decisions</th><th>Refusal rate</th>"
            f"</tr></thead><tbody>{at_rows_html}</tbody></table>"
            f'<p class="note">Data: <code>by_application_type.csv</code></p>'
            f"<details><summary>What each application type means</summary>"
            f"<dl>{glossary_html}</dl>"
            f'<p class="note">Definitions taken from the '
            f'<a href="https://www.planningportal.co.uk/planning/planning-applications/consent-types/" target="_blank" rel="noopener">Planning Portal consent-type pages</a> '
            f'and the <a href="https://www.legislation.gov.uk/ukpga/1990/8/contents" target="_blank" rel="noopener">Town and Country Planning Act 1990</a> '
            f"(s.96A non-material amendments, s.73 variation of conditions, s.211 trees in conservation areas) and the "
            f'<a href="https://www.legislation.gov.uk/ukpga/1990/9/contents" target="_blank" rel="noopener">Planning (Listed Buildings and Conservation Areas) Act 1990</a>.</p>'
            f"</details>"
        )

    nat_rate_for_intro = national_refusal_rate if national_refusal_rate is not None else 0.0
    gaps_intro = (
        f"Refusal rates by region, compared to the {nat_rate_for_intro:.1f}% national average. "
        "London and the East of England refuse most often. Scotland is the most lenient by some margin."
    )

    region_bar_svg = svg_hbar_chart(
        [
            (str(r["region"]), float(r["refusal_rate"]))
            for r in by_region
            if isinstance(r.get("refusal_rate"), (int, float))
        ],
        ref_line=national_refusal_rate,
        ref_label=f"national mean {nat_rate_for_intro:.1f}%" if national_refusal_rate is not None else None,
        aria_label="Refusal rate by region",
    )

    # Pick out the two most-recent years actually shown in the trend table so
    # the caveat names the right years even when the report is regenerated in
    # a future calendar year. The current calendar year is already filtered
    # out upstream in main().
    recent_years = [str(r["start_year"]) for r in by_start_year if str(r["start_year"]).isdigit()]
    if len(recent_years) >= 2:
        provisional_label = f"{recent_years[-2]} and {recent_years[-1]}"
    elif recent_years:
        provisional_label = recent_years[-1]
    else:
        provisional_label = "the most recent"
    trend_caveat = (
        "Refusal rates have risen noticeably for applications submitted from 2022 onwards. "
        "But a growing share of recent applications haven&rsquo;t had a decision recorded yet &mdash; this is "
        "almost certainly councils still working through them, not a real shift in policy &mdash; so the "
        f"{provisional_label} figures are provisional. (The current calendar year is excluded from this "
        "panel because most of those applications haven&rsquo;t been decided yet.)"
    )

    coverage_rows: list[dict] = []
    coverage_svg = ""
    coverage_intro = ""
    if mcs_yearly:
        apps_by_year = {int(r["start_year"]): int(r["total"]) for r in by_start_year if str(r["start_year"]).isdigit()}
        # Restrict to overlap years; drop the current year if it's clearly partial.
        latest_full = max((y for y in mcs_yearly if y < datetime.now(timezone.utc).year), default=None)
        for year in sorted(set(apps_by_year) & set(mcs_yearly)):
            if latest_full is not None and year > latest_full:
                continue
            mcs = mcs_yearly[year]
            apps = apps_by_year[year]
            coverage_rows.append(
                {
                    "year": year,
                    "mcs": mcs,
                    "apps": apps,
                    "coverage_pct": (apps / mcs * 100) if mcs else None,
                }
            )
        if coverage_rows:
            coverage_svg = svg_dual_line_chart(
                coverage_rows,
                x_key="year",
                y1_key="mcs",
                y2_key="apps",
                y1_label=mcs_label,
                y2_label="Planning apps in this dataset",
            )
            recent = coverage_rows[-1]
            recent_pct = recent["coverage_pct"]
            mean_pct = statistics.mean(r["coverage_pct"] for r in coverage_rows if r["coverage_pct"] is not None)
            coverage_intro = (
                f"To gauge the share of installs that need planning permission, we compare planning applications "
                f"against MCS &mdash; the certification scheme that records every heat pump fitted by an accredited "
                f"installer in the UK, whether or not planning was needed. The chart below puts the two side by "
                f"side: certified installs in red, planning applications in green.<br><br>"
                f"In {recent['year']} there were <strong>{recent['apps']:,} planning applications</strong> for heat "
                f"pumps versus <strong>{recent['mcs']:,} certified installations</strong>. So only about "
                f"<strong>{recent_pct:.1f}%</strong> of installs needed planning permission. The average across "
                f"{coverage_rows[0]['year']}&ndash;{recent['year']} is roughly {mean_pct:.0f}%."
            )

    council_cov_section = ""
    if council_coverage:
        cov_rates = [r["coverage_pct"] for r in council_coverage]
        median_cov = statistics.median(cov_rates)
        if len(cov_rates) >= 4:
            q1 = statistics.quantiles(cov_rates, n=4)[0]
            q3 = statistics.quantiles(cov_rates, n=4)[2]
            iqr_cov = f"the middle half between {q1:.1f}% and {q3:.1f}%"
        else:
            iqr_cov = "too few councils to summarise the spread"
        # Worst-coverage councils by absolute install volume gap (top 10).
        ranked = sorted(council_coverage, key=lambda r: -(r["mcs_installs"] - r["planning_apps"]))
        worst_rows = [
            {
                "authority": r["authority"],
                "mcs_installs": r["mcs_installs"],
                "planning_apps": r["planning_apps"],
                "coverage_share": r["coverage_pct"],
            }
            for r in ranked[:10]
        ]
        worst_table = html_table(
            worst_rows,
            [
                ("authority", "Council"),
                ("mcs_installs", "Certified installs"),
                ("planning_apps", "Planning apps"),
                ("coverage_share", "Share via planning"),
            ],
        )
        year_range_label = council_coverage[0].get("year_range", "")
        council_cov_section = (
            f"<h3>Share needing planning, by council</h3>"
            f"<p>Across the {len(council_coverage)} councils we can match to MCS install records "
            f"({year_range_label}), a typical council saw <strong>{median_cov:.1f}%</strong> of installs go through "
            f"planning ({iqr_cov}). At the low end, planning records are a poor proxy for install activity. "
            f"The 10 councils with the largest install-to-application gap are below.</p>"
            f"{worst_table}"
            f'<p class="note">Full numbers in <code>per_council_coverage.csv</code>.</p>'
        )

    region_coverage_section = ""
    if region_coverage:
        rc_rows_html = "".join(
            f"<tr>"
            f"<td>{html.escape(r['region'])}</td>"
            f"<td>{fmt_int(r['n_authorities'])}</td>"
            f"<td>{fmt_int(r['mcs_installs'])}</td>"
            f"<td>{fmt_int(r['planning_apps'])}</td>"
            f"<td>{(r['coverage_pct'] or 0):.1f}%</td>"
            f"</tr>"
            for r in region_coverage
        )
        rc_rates = [r["coverage_pct"] for r in region_coverage if r["coverage_pct"] is not None]
        rc_high = max(region_coverage, key=lambda r: r["coverage_pct"] or 0)
        rc_low = min(region_coverage, key=lambda r: r["coverage_pct"] or 0)
        rc_spread = (
            f"The share that needs planning permission varies a lot by region: "
            f"<strong>{rc_high['coverage_pct']:.1f}%</strong> in {rc_high['region']} versus only "
            f"<strong>{rc_low['coverage_pct']:.1f}%</strong> in {rc_low['region']}"
            if rc_rates and rc_high["region"] != rc_low["region"]
            else "The share that needs planning permission is roughly even across regions"
        )
        region_coverage_section = (
            f"<h3>Share needing planning, by region</h3>"
            f"<p>{rc_spread}. This reflects how often local conditions (flats, conservation areas, listed "
            f"buildings, planning policies) push an install out of permitted development and into the planning "
            f"system.</p>"
            f"<details><summary>Full per-region table</summary>"
            f"<table><thead><tr>"
            f"<th>Region</th><th>Councils</th><th>Certified installs</th><th>Planning apps</th><th>Share needing planning</th>"
            f"</tr></thead><tbody>{rc_rows_html}</tbody></table>"
            f'<p class="note">London&rsquo;s figure is inflated: inner London has few MCS-certified installs '
            f"and heat pumps there are usually bundled into larger applications (extensions, basement digs, "
            f"listed-building consents) that would have needed planning anyway. Full numbers in "
            f"<code>per_region_coverage.csv</code>.</p>"
            f"</details>"
        )

    if coverage_svg and coverage_rows:
        # Rename coverage_pct -> coverage_share so html_table formats it as a percentage.
        # Year is stringified so html_table doesn't comma-separate it as a thousands-int.
        table_rows = [
            {"year": str(r["year"]), "mcs": r["mcs"], "apps": r["apps"], "coverage_share": r["coverage_pct"]}
            for r in coverage_rows
        ]
        coverage_table = html_table(
            table_rows,
            [
                ("year", "Year"),
                ("mcs", "Certified installs"),
                ("apps", "Planning apps"),
                ("coverage_share", "Share via planning"),
            ],
        )
        search_terms_html = (
            (
                f" Planning apps come from the "
                f'<a href="https://www.planit.org.uk/" target="_blank" rel="noopener">PlanIt API</a>, '
                f"searched with: {search_terms_label}."
            )
            if search_terms_label
            else (
                " Planning apps come from the "
                '<a href="https://www.planit.org.uk/" target="_blank" rel="noopener">PlanIt API</a>.'
            )
        )
        coverage_section = (
            f"<h3>Over time</h3>"
            f"<p>{coverage_intro}</p>"
            f'<div class="grid">'
            f'<div class="chart-wrap">{coverage_svg}</div>'
            f"<div>{coverage_table}</div>"
            f"</div>"
            f'<p class="note">MCS data: {html.escape(mcs_label)} (yearly totals from the '
            f'<a href="https://datadashboard.mcscertified.com/InstallationInsights" target="_blank" rel="noopener">MCS Data Dashboard</a>).'
            f"{search_terms_html} "
            f"The current calendar year is excluded as partial.</p>"
            f'<p class="note">Air-to-air coverage was spot-checked separately: 118 apps '
            f'(~0.4% of the corpus) already mention "air to air" / "air-to-air" — 86% caught via the '
            f'broad "heat pump" query — and the "A2A" abbreviation never appears in UK planning '
            f"descriptions, so a dedicated A2A scrape would add little.</p>"
        )
    else:
        coverage_section = ""

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{report_title}</title>
  <style>
    :root {{
      --ink: #17211b;
      --muted: #52635a;
      --line: #d9e1dd;
      --panel: #f7f9f8;
      --accent: #1f7a5a;
      --accent-2: #a33b2f;
      --bg: #ffffff;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font: 15px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    main {{ max-width: 1080px; margin: 0 auto; padding: 34px 24px 56px; }}
    h1 {{ font-size: 34px; line-height: 1.1; margin: 0 0 10px; letter-spacing: 0; }}
    h2 {{ font-size: 22px; margin: 34px 0 12px; letter-spacing: 0; }}
    h3 {{ font-size: 16px; margin: 24px 0 8px; letter-spacing: 0; }}
    p {{ max-width: 760px; color: var(--muted); }}
    .kpis {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 24px 0 24px; }}
    .kpi {{ border: 1px solid var(--line); border-radius: 8px; padding: 14px 16px; background: var(--panel); }}
    .kpi .value {{ display: block; font-size: 26px; line-height: 1.2; font-weight: 700; color: var(--ink); }}
    .kpi .label {{ display: block; margin-top: 4px; color: var(--muted); font-size: 13px; }}
    .grid {{ display: grid; grid-template-columns: minmax(0, 1fr) minmax(280px, 0.72fr); gap: 22px; align-items: start; }}
    .chart-wrap {{ border: 1px solid var(--line); border-radius: 8px; padding: 12px; background: #fff; overflow: hidden; }}
    .chart {{ width: 100%; height: auto; display: block; }}
    .line-chart polyline {{ fill: none; stroke: var(--accent-2); stroke-width: 3; }}
    .line-chart circle {{ fill: var(--accent-2); }}
    .chart text {{ fill: var(--muted); font-size: 12px; }}
    .chart text.chart-title {{ fill: var(--ink); font-size: 13px; font-weight: 600; }}
    .chart text.axis-title {{ fill: var(--ink); font-size: 12px; font-weight: 500; }}
    .axis .grid {{ stroke: var(--line); stroke-width: 1; }}
    .bar-chart rect {{ fill: var(--accent); }}
    .histogram rect {{ fill: var(--accent-2); }}
    .histogram .ref {{ stroke: var(--ink); stroke-width: 1.5; stroke-dasharray: 4 3; }}
    .histogram .ref-label {{ fill: var(--ink); font-size: 11px; }}
    .hbar-chart rect {{ fill: var(--accent-2); }}
    .hbar-chart .ref {{ stroke: var(--ink); stroke-width: 1.5; stroke-dasharray: 4 3; }}
    .hbar-chart .ref-label {{ fill: var(--ink); font-size: 11px; }}
    .hbar-chart .row-label {{ fill: var(--ink); font-size: 12px; }}
    .hbar-chart .value-label {{ fill: var(--ink); font-size: 12px; }}
    details {{ margin: 6px 0 18px; }}
    details > summary {{ cursor: pointer; color: var(--muted); font-size: 13px; padding: 4px 0; }}
    details[open] > summary {{ margin-bottom: 6px; }}
    .dual-line polyline {{ fill: none; stroke-width: 2.5; }}
    .dual-line polyline.series-1 {{ stroke: var(--accent-2); }}
    .dual-line polyline.series-2 {{ stroke: var(--accent); }}
    .dual-line .series-1-pts circle {{ fill: var(--accent-2); }}
    .dual-line .series-2-pts circle {{ fill: var(--accent); }}
    .dual-line .legend line.series-1 {{ stroke: var(--accent-2); stroke-width: 2.5; }}
    .dual-line .legend line.series-2 {{ stroke: var(--accent); stroke-width: 2.5; }}
    .dual-line .legend text {{ fill: var(--ink); font-size: 12px; }}
    .sparkline-row {{ display: grid; grid-template-columns: minmax(160px, 0.4fr) minmax(0, 1fr); gap: 12px; align-items: center; margin: 8px 0; }}
    .sparkline-label strong {{ display: block; }}
    .chart-wrap.sparkline {{ padding: 4px; }}
    h4 {{ font-size: 14px; margin: 14px 0 6px; color: var(--ink); }}
    table {{ border-collapse: collapse; width: 100%; margin: 10px 0 18px; font-size: 14px; }}
    th, td {{ border-bottom: 1px solid var(--line); padding: 8px 9px; text-align: right; vertical-align: top; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ color: var(--muted); font-weight: 600; background: var(--panel); }}
    table.rule-fire th, table.rule-fire td {{ text-align: left; }}
    table.rule-fire th:last-child, table.rule-fire td:last-child {{ text-align: right; white-space: nowrap; }}
    table.rule-fire td:nth-child(2) {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12.5px; word-break: break-all; }}
    .note {{ font-size: 13px; color: var(--muted); }}
    code {{ background: var(--panel); padding: 2px 5px; border-radius: 4px; }}
    dl {{ margin: 8px 0 4px; font-size: 14px; }}
    dt {{ font-weight: 600; margin-top: 8px; }}
    dt:first-child {{ margin-top: 0; }}
    dd {{ margin: 2px 0 0 0; color: var(--muted); }}
    .outcome-mix {{ margin: 18px 0 28px; }}
    .outcome-mix h3 {{ margin-top: 4px; }}
    .stacked-bar {{ display: flex; height: 28px; border-radius: 4px; overflow: hidden; margin: 12px 0 14px; border: 1px solid var(--line); background: var(--panel); }}
    .stacked-bar .seg {{ display: block; height: 100%; }}
    .stacked-bar .seg-approved {{ background: var(--accent); }}
    .stacked-bar .seg-refused {{ background: var(--accent-2); }}
    .stacked-bar .seg-withdrawn {{ background: #b88a4a; }}
    .stacked-bar .seg-undecided {{ background: #6e7e76; }}
    .stacked-bar .seg-other {{ background: #c5cdc8; }}
    .mix-table {{ max-width: 520px; }}
    .mix-table td:first-child, .mix-table th:first-child {{ padding-left: 0; }}
    .legend-dot {{ display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 8px; vertical-align: middle; border: 1px solid rgba(0,0,0,0.06); }}
    .legend-dot.dot-approved {{ background: var(--accent); }}
    .legend-dot.dot-refused {{ background: var(--accent-2); }}
    .legend-dot.dot-withdrawn {{ background: #b88a4a; }}
    .legend-dot.dot-undecided {{ background: #6e7e76; }}
    .legend-dot.dot-other {{ background: #c5cdc8; }}
    @media (max-width: 820px) {{
      main {{ padding: 24px 16px 42px; }}
      .kpis, .grid {{ grid-template-columns: 1fr; }}
      h1 {{ font-size: 28px; }}
    }}
    @media print {{
      @page {{ margin: 16mm 12mm; }}
      body {{ font-size: 10.5pt; }}
      main {{ max-width: none; padding: 0; }}
      h1 {{ font-size: 26px; }}
      h2 {{ font-size: 19px; margin-top: 22px; }}
      .kpis {{ grid-template-columns: repeat(4, minmax(0, 1fr)) !important; gap: 8px; margin: 14px 0 18px; }}
      .kpi {{ padding: 10px 12px; }}
      .kpi .value {{ font-size: 19px; }}
      .grid {{ grid-template-columns: minmax(0, 1fr) !important; gap: 12px; }}
      .grid > .chart-wrap {{ max-width: 600px; }}
      h1, h2, h3, h4 {{ page-break-after: avoid; break-after: avoid; }}
      h2 + p, h3 + p, h4 + p {{ page-break-before: avoid; break-before: avoid; }}
      .chart-wrap, .sparkline-row {{ page-break-inside: avoid; break-inside: avoid; }}
      table {{ page-break-inside: auto; }}
      thead {{ display: table-header-group; }}
      tr, tr > * {{ page-break-inside: avoid; break-inside: avoid; }}
      p, li {{ orphans: 3; widows: 3; }}
      details > summary {{ display: none; }}
      a {{ color: inherit; }}
    }}
  </style>
</head>
<body>
<main>
  <h1>{report_title}</h1>
  <p>{corpus_label} planning applications. Generated {generated}.</p>

  <section class="kpis" aria-label="Headline metrics">
    <div class="kpi"><span class="value">{fmt_int(summary["total_apps"])}</span><span class="label">Applications</span></div>
    <div class="kpi"><span class="value">{fmt_int(summary["decided_apps"])}</span><span class="label">Approved or refused</span></div>
    <div class="kpi"><span class="value">{fmt_int(summary["approved_apps"])}</span><span class="label">Approved <span class="kpi-pct">({fmt_pct(summary["approval_rate"])} of decisions)</span></span></div>
    <div class="kpi"><span class="value">{fmt_int(summary["refused_apps"])}</span><span class="label">Refused <span class="kpi-pct">({fmt_pct(summary["refusal_rate"])} of decisions)</span></span></div>
  </section>

  <h2>When do heat-pump installs need planning permission?</h2>
  <p>Most heat pumps don&rsquo;t need planning permission &mdash; they&rsquo;re allowed automatically as long as they meet certain rules on size, noise, and distance from neighbours (the planning system calls this &ldquo;permitted development&rdquo;). The rest do need a planning application, and that&rsquo;s what this dataset captures.</p>
  <p class="note">Sources for the permitted-development rules: Planning Portal &mdash; <a href="https://www.planningportal.co.uk/permission/common-projects/heat-pumps/planning-permission-air-source-heat-pump/" target="_blank" rel="noopener">air source</a> and <a href="https://www.planningportal.co.uk/permission/common-projects/heat-pumps" target="_blank" rel="noopener">ground/water source</a> heat pumps (England); <a href="https://www.gov.wales/planning-permission-heat-pumps" target="_blank" rel="noopener">gov.wales</a> (Wales). The underlying legislation is the <a href="https://www.legislation.gov.uk/uksi/2015/596/schedule/2" target="_blank" rel="noopener">Town and Country Planning (General Permitted Development) (England) Order 2015, Sch. 2</a> (Part 14, Classes G&ndash;H). England&rsquo;s rules were eased on 29 May 2025 (1 m boundary rule removed; size limit raised to 1.5 m&sup3;), as part of the <a href="https://www.gov.uk/government/publications/warm-homes-plan/warm-homes-plan-html" target="_blank" rel="noopener">Warm Homes Plan</a> (DESNZ, last updated 18 March 2026).</p>

  {coverage_section}

  {region_coverage_section}

  {_render_outcome_mix_section(summary)}

  <h2>How do outcomes vary?</h2>
  <p>Of the applications that do go through planning, around 93% are approved and 7% are refused. Most go through, so the interesting variation is in the small share that doesn&rsquo;t &mdash; the breakdowns below show refusal rates by region, by council, by type of application, by property type, and by year submitted.</p>

  <h3>By region</h3>
  <p>{gaps_intro}</p>
  <div class="chart-wrap">{region_bar_svg}</div>
  {html_table(by_region, [("region", "Region"), ("total", "Apps"), ("decided", "Decisions"), ("refusal_rate", "Refusal rate")])}
  <p class="note">Data: <code>by_region.csv</code></p>

  <h3>By council</h3>
  <p>{headline_a}</p>
  <div class="chart-wrap">{panel_a_svg}</div>
  {exclusion_note}
  <p class="note">Data: <code>by_council.csv</code></p>

  {app_type_section}

  {edge_section}

  <h3>By year submitted</h3>
  <p>{trend_caveat}</p>
  <div class="grid">
    <div class="chart-wrap">{svg_line_chart(by_start_year, "start_year", "refusal_rate", title="Refusal rate by year submitted", y_label="Refusal rate")}</div>
    <div>{html_table(by_start_year, [("start_year", "Year submitted"), ("total", "Apps"), ("refusal_rate", "Refusal rate"), ("no_decision_share", "No decision yet")])}</div>
  </div>
  <p class="note">Data: <code>by_start_year.csv</code></p>

  <h2>Appendix</h2>
  {high_excess_section}
  {council_cov_section}

  <h2>Method</h2>
  <p>Each application&rsquo;s outcome is taken from the council&rsquo;s own decision text and grouped into approved, refused, withdrawn, or other categories using a list of keyword rules. Refusal rates are calculated only over applications that have an approval or refusal recorded &mdash; withdrawn, pending, and unrecorded outcomes are left out so they don&rsquo;t distort the rate. Regions are worked out from the application&rsquo;s postcode; for the small share of records with no postcode, the region is filled in from the council&rsquo;s other applications (or, for a handful of councils whose records carry no postcode at all, from a manual council&rarr;region map).</p>

  <h3>How rules fire</h3>
  <p>Every classification decision in this report is driven by a small set of regex rules kept in <code>rules/</code>. The tables below show how many applications hit each rule on this run, so reviewers can see the consequences of every rule before arguing with its pattern.</p>

  <h4>Outcome buckets (<code>rules/decision_rules.toml</code>)</h4>
  <p class="note">Rules run in order against each council&rsquo;s free-text <code>planning_decision</code> string; the first match wins. <code>__no_decision__</code> = blank field; <code>__other__</code> = no rule matched.</p>
  {decision_rules_table}

  <h4>Application type (<code>rules/app_type_rules.toml</code>)</h4>
  <p class="note">Most apps come from PlanIt&rsquo;s already-canonical <code>planning_application_type</code> field. The regex rules below only fire for the minority of rows where that field is blank.</p>
  {app_type_source_table}
  {app_type_rules_table}

  <h4>Edge-case keyword flags (<code>rules/edge_case_keywords.toml</code>)</h4>
  <p class="note">Each flag is independent; an application can fire several.</p>
  {edge_case_rules_table}
</main>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--source-scrape",
        default="all",
        help="Source scrape to analyse. Defaults to 'all' (combined ASHP + broader heat-pump dataset). "
        "'ashp' or 'broad_hp' restrict to a single scrape.",
    )
    parser.add_argument(
        "--council-min-decided",
        dest="council_min_decided",
        type=int,
        default=50,
        help="Minimum decided apps for a council to appear in the council panel and appendix.",
    )
    args = parser.parse_args()

    if not args.db.exists():
        if args.db == DEFAULT_DB and REPO_LOCAL_DB.exists():
            print(
                f"Default DB {args.db} not found; falling back to repo-local "
                f"{REPO_LOCAL_DB}. Run scripts/open_ashp_progress_dashboard.py once to "
                f"refresh /tmp/ashp.db with the merged 30k corpus."
            )
            args.db = REPO_LOCAL_DB
        else:
            parser.error(f"Database not found: {args.db}")

    apps = load_apps(args.db, args.source_scrape)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    decision_counts = count_decisions(apps)
    overall = row_from_counts("scope", args.source_scrape, decision_counts)
    raw_mapping_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for app in apps:
        raw_mapping_counts[app.raw_decision or "(empty)"][app.decision] += 1

    # Drop the current calendar year from the trend: most rows in the current
    # year haven't been decided yet, so the refusal-rate figure is unstable and
    # the chart misleads. The coverage chart already excludes the current year
    # for the same reason.
    current_year_str = str(datetime.now(timezone.utc).year)
    by_start_year = grouped_table(apps, "start_year", lambda app: app.start_year)
    by_start_year = sorted(
        (r for r in by_start_year if r["start_year"] not in ("Unknown", current_year_str)),
        key=lambda r: str(r["start_year"]),
    )
    by_region = grouped_table(apps, "region", lambda app: app.region)
    by_region = sorted(by_region, key=lambda r: (r["refusal_rate"] is None, -(r["refusal_rate"] or 0)))
    by_council = compute_council_refusal_rates(apps, min_decided=args.council_min_decided)
    by_application_type = grouped_table(apps, "application_type", lambda app: app.app_type)
    by_application_type = sorted(by_application_type, key=lambda r: -int(r["total"]))

    edge_cases = [
        row_from_counts("subset", "Listed / heritage flag", count_decisions(app for app in apps if app.is_listed)),
        row_from_counts(
            "subset", "No listed / heritage flag", count_decisions(app for app in apps if not app.is_listed)
        ),
        row_from_counts("subset", "Flat / apartment", count_decisions(app for app in apps if app.is_flat)),
        row_from_counts("subset", "Not flat / apartment", count_decisions(app for app in apps if not app.is_flat)),
        row_from_counts(
            "subset", "Wind turbine mentioned", count_decisions(app for app in apps if app.is_wind_turbine)
        ),
        row_from_counts(
            "subset", "Noise / sound mentioned", count_decisions(app for app in apps if app.mentions_noise)
        ),
    ]

    decision_mapping = []
    for raw, counts in raw_mapping_counts.items():
        normalised = counts.most_common(1)[0][0]
        decision_mapping.append({"raw_decision": raw, "normalised": normalised, "count": sum(counts.values())})
    decision_mapping = sorted(decision_mapping, key=lambda r: -int(r["count"]))

    # Primary CSVs: one per visible table in the report so reviewers can verify
    # each chart against the row-level data.
    tables = {
        "by_start_year": by_start_year,
        "by_region": by_region,
        "by_council": by_council,
        "by_application_type": by_application_type,
        "edge_cases": edge_cases,
        "decision_mapping": decision_mapping,
    }
    for name, rows in tables.items():
        write_csv(args.output_dir / f"{name}.csv", serialise_rows(rows))

    if args.source_scrape == "all":
        report_title = "Heat Pump Decision Patterns"
        corpus_label = "ASHP and broader heat-pump"
    elif args.source_scrape == "ashp":
        report_title = "ASHP Decision Patterns"
        corpus_label = "ASHP"
    else:
        report_title = f"{args.source_scrape} Decision Patterns"
        corpus_label = args.source_scrape

    db_meta = _db_meta(args.db)
    git_sha = _git_short_sha()
    summary = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "db_path": str(args.db),
        "db_modified": db_meta.get("modified"),
        "db_row_count": db_meta.get("row_count"),
        "git_commit": git_sha,
        "git_dirty": _git_dirty(),
        "source_scrape": args.source_scrape,
        "report_title": report_title,
        "corpus_label": corpus_label,
        "total_apps": overall["total"],
        "decided_apps": overall["decided"],
        "approved_apps": overall["approved"],
        "refused_apps": overall["refused"],
        "approval_rate": overall["approval_rate"],
        "refusal_rate": overall["refusal_rate"],
        "no_decision_apps": overall["no_decision"],
        "decision_breakdown": dict(decision_counts),
        "council_min_decided": args.council_min_decided,
        "n_councils_in_panel": len(by_council),
        "rules_fired": {
            "decision": dict(_DECISION_FIRE_COUNTS),
            "app_type": dict(_APP_TYPE_FIRE_COUNTS),
            "app_type_source": dict(_APP_TYPE_SOURCE_COUNTS),
            "edge_case": dict(_EDGE_CASE_FIRE_COUNTS),
        },
    }
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if args.source_scrape == "ashp":
        mcs_yearly = load_mcs_yearly(MCS_YEARLY_ASHP)
        mcs_label = "MCS-certified ASHP installs"
    else:
        mcs_yearly = load_mcs_yearly(MCS_YEARLY_COMBINED)
        mcs_label = "MCS-certified installs (all heat-pump types)"

    # Per-council coverage vs MCS LA-by-year. Year range = years present in BOTH
    # the corpus and MCS, dropping the current (partial) calendar year. The corpus
    # only goes back to 2015; using MCS's wider span would inflate the denominator.
    mcs_la_by_year = load_mcs_la_by_year(MCS_LA_BY_YEAR)
    authority_lookup = load_authority_lpa_lookup(LPA_LOOKUP)
    council_coverage: list[dict] = []
    if mcs_la_by_year:
        current_year = datetime.now(timezone.utc).year
        corpus_years = {int(r["start_year"]) for r in by_start_year if str(r["start_year"]).isdigit()}
        mcs_years = {y for (y, _n) in mcs_la_by_year}
        overlap_years = sorted(y for y in (corpus_years & mcs_years) if y < current_year)
        if overlap_years:
            council_coverage = compute_council_coverage(
                apps,
                mcs_la_by_year,
                authority_lookup,
                year_range=(min(overlap_years), max(overlap_years)),
            )
    if council_coverage:
        write_csv(args.output_dir / "per_council_coverage.csv", serialise_rows(council_coverage))

    region_coverage = compute_region_coverage(apps, council_coverage)
    if region_coverage:
        write_csv(args.output_dir / "per_region_coverage.csv", serialise_rows(region_coverage))

    panel_context = {
        "council_panel": by_council,
        "council_min_decided": args.council_min_decided,
        "national_refusal_rate": overall["refusal_rate"],
        "mcs_yearly": mcs_yearly,
        "mcs_label": mcs_label,
        "council_coverage": council_coverage,
        "region_coverage": region_coverage,
        "search_terms_label": format_search_terms_label(args.source_scrape),
    }
    (args.output_dir / "index.html").write_text(
        render_html(args.output_dir, summary, tables, panel_context), encoding="utf-8"
    )

    print(f"Analysed {len(apps):,} applications from {args.db}")
    print(f"Wrote report to {args.output_dir / 'index.html'}")


if __name__ == "__main__":
    main()
