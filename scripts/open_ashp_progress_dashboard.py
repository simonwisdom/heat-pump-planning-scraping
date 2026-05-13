#!/usr/bin/env python3
"""Pull the canonical VPS ashp.db, render a local coverage dashboard, and serve it."""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ashp_local_explore import LOCAL_DB, ensure_local_views, refresh_local_db
from src.authority_lpa_lookup import write_lookup_csv

ANALYSE_SCRIPT = ROOT / "scripts" / "analyse_decision_patterns.py"


def _load_analyse_module():
    """Dynamically load analyse_decision_patterns.py as a module."""
    if "analyse_decision_patterns" in sys.modules:
        return sys.modules["analyse_decision_patterns"]
    spec = importlib.util.spec_from_file_location("analyse_decision_patterns", ANALYSE_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {ANALYSE_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # required so @dataclass can resolve cls.__module__
    spec.loader.exec_module(module)
    return module


OUTPUT_DIR = ROOT / "_local" / "output" / "authority_progress_dashboard"
OUTPUT_HTML = OUTPUT_DIR / "index.html"
LPA_GEOJSON = ROOT / "_local" / "geo" / "lpa_uk_buc_2022.min.geojson"
LPA_LOOKUP_CSV = ROOT / "_local" / "geo" / "authority_lpa_lookup.csv"


def load_choropleth_assets() -> tuple[dict, list[dict]]:
    """Return (geojson_obj, lookup_rows). Empty stubs if files are missing."""
    geo: dict = {"type": "FeatureCollection", "features": []}
    lookup: list[dict] = []
    if LPA_GEOJSON.exists():
        geo = json.loads(LPA_GEOJSON.read_text())
    if LPA_LOOKUP_CSV.exists():
        with LPA_LOOKUP_CSV.open() as fh:
            lookup = list(csv.DictReader(fh))
    return geo, lookup


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="Dashboard bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8020, help="Dashboard port (default: 8020)")
    parser.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Reuse the existing /tmp/ashp.db instead of pulling a fresh VPS snapshot",
    )
    parser.add_argument(
        "--build-only", action="store_true", help="Render the dashboard HTML but do not start a local web server"
    )
    return parser.parse_args()


def fetch_dashboard_rows(db_path: Path) -> list[dict]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                authority_name,
                portal_type,
                authority_portal,
                lat,
                lng,
                application_count,
                success_count,
                partial_count,
                no_docs_available_count,
                failed_count,
                not_attempted_count,
                apps_with_downloaded_docs,
                apps_download_phase_done,
                pct_with_downloaded_docs,
                pct_download_phase_done,
                coverage_bucket,
                download_attempt_count,
                last_attempted_at,
                documents_listed_total,
                files_downloaded_total,
                bytes_downloaded_total
            FROM authority_portal_download_progress_map
            ORDER BY
                (application_count - apps_download_phase_done) DESC,
                pct_download_phase_done ASC,
                application_count DESC,
                authority_name,
                portal_type
            """
        ).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows]


def dashboard_html(
    rows: list[dict],
    *,
    generated_at: str,
    db_path: Path,
    geojson: dict,
    lookup: list[dict],
) -> str:
    payload = json.dumps(rows, separators=(",", ":"))
    geojson_payload = json.dumps(geojson, separators=(",", ":"))
    lookup_payload = json.dumps(lookup, separators=(",", ":"))
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ASHP Coverage Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <style>
    :root {{
      --panel: rgba(255, 252, 246, 0.92);
      --panel-solid: #fffaf2;
      --ink: #1e1f1d;
      --muted: #5f625c;
      --line: rgba(56, 52, 43, 0.14);
      --accent: #1f201d;
      --shadow: 0 10px 28px rgba(83, 71, 43, 0.08);
      --radius-lg: 18px;
    }}
    * {{ box-sizing: border-box; }}
    html, body {{ height: 100%; margin: 0; }}
    body {{
      font-family: "IBM Plex Sans", ui-sans-serif, sans-serif;
      color: var(--ink);
      overflow: hidden;
      background:
        radial-gradient(circle at top left, rgba(224, 179, 86, 0.14), transparent 24rem),
        radial-gradient(circle at bottom right, rgba(45, 112, 170, 0.10), transparent 28rem),
        linear-gradient(180deg, #f6f1e8, #efe8dc 52%, #ece5da);
    }}
    .shell {{
      display: grid;
      grid-template-rows: auto 1fr;
      gap: 12px;
      height: 100vh;
      padding: 12px;
    }}
    .topbar {{
      display: grid;
      grid-template-columns: auto 1fr auto;
      grid-template-rows: auto auto;
      align-items: center;
      gap: 10px 22px;
      background: var(--panel);
      border: 1px solid rgba(255, 255, 255, 0.55);
      border-radius: var(--radius-lg);
      box-shadow: var(--shadow);
      padding: 10px 18px 12px;
      backdrop-filter: blur(10px);
    }}
    .stacked-bar {{
      grid-column: 1 / -1;
      display: flex;
      width: 100%;
      height: 10px;
      border-radius: 999px;
      overflow: hidden;
      background: rgba(40, 42, 38, 0.08);
    }}
    .stacked-seg {{ height: 100%; transition: flex-grow 180ms ease; }}
    .family-bar-fail {{
      position: absolute;
      top: 0;
      bottom: 0;
      background: #c83a2f;
    }}
    .popup-section {{ margin-top: 10px; }}
    .popup-section-head {{
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      font-size: 0.66rem;
      margin-bottom: 6px;
    }}
    .popup-comp-row {{
      display: grid;
      grid-template-columns: 14px 1fr auto;
      gap: 8px;
      align-items: center;
      padding: 2px 0;
      font-size: 0.82rem;
    }}
    .popup-comp-row strong {{
      font-weight: 600;
      letter-spacing: -0.02em;
    }}
    .popup-portal-row {{
      display: flex;
      justify-content: space-between;
      padding: 3px 0;
      font-size: 0.8rem;
      border-top: 1px solid var(--line);
    }}
    .popup-portal-row:first-child {{ border-top: 0; }}
    .popup-portal-row strong {{ font-weight: 600; }}
    .marker-tooltip {{
      background: rgba(20, 22, 18, 0.92);
      color: #fffdf8;
      border: 0;
      border-radius: 6px;
      padding: 3px 8px;
      font-size: 0.76rem;
      font-weight: 500;
      box-shadow: 0 4px 10px rgba(20, 22, 18, 0.18);
    }}
    .marker-tooltip::before {{ display: none; }}
    .worktodo-row {{
      display: grid;
      grid-template-columns: 12px 1fr auto;
      gap: 10px;
      align-items: center;
      padding: 6px 2px;
      border-top: 1px solid var(--line);
    }}
    .worktodo-row:first-child {{ border-top: 0; }}
    .worktodo-label {{ font-size: 0.84rem; }}
    .worktodo-label small {{
      display: block;
      color: var(--muted);
      font-size: 0.72rem;
      margin-top: 2px;
    }}
    .worktodo-count {{ text-align: right; white-space: nowrap; }}
    .worktodo-count strong {{
      display: block;
      font-size: 0.9rem;
      letter-spacing: -0.02em;
    }}
    .worktodo-count small {{
      color: var(--muted);
      font-size: 0.72rem;
    }}
    .tab-group {{
      display: inline-flex;
      gap: 2px;
      padding: 3px;
      background: rgba(40, 42, 38, 0.06);
      border-radius: 999px;
      border: 1px solid var(--line);
      margin-bottom: 8px;
    }}
    .tab-btn {{
      border: 0;
      background: transparent;
      color: var(--muted);
      padding: 4px 10px;
      border-radius: 999px;
      font: inherit;
      font-size: 0.78rem;
      cursor: pointer;
    }}
    .tab-btn[data-active="true"] {{
      background: var(--accent);
      color: #fffdf8;
    }}
    .repr-group {{ margin-top: 6px; }}
    .repr-group-title {{
      color: var(--muted);
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 6px;
    }}
    .repr-row {{
      display: grid;
      grid-template-columns: 1fr auto;
      grid-template-rows: auto auto;
      column-gap: 10px;
      padding: 5px 0;
      border-top: 1px solid var(--line);
      cursor: pointer;
    }}
    .repr-row:first-child {{ border-top: 0; }}
    .repr-row:hover {{ background: rgba(255, 255, 255, 0.55); }}
    .repr-row[data-highlighted="true"] {{
      background: #fffcf3;
      border-radius: 6px;
    }}
    .repr-name {{
      font-weight: 600;
      font-size: 0.86rem;
    }}
    .repr-meta {{
      color: var(--muted);
      font-size: 0.74rem;
      text-align: left;
      margin-top: 1px;
    }}
    .repr-pct {{
      font-weight: 700;
      font-size: 0.9rem;
      letter-spacing: -0.02em;
      text-align: right;
    }}
    .repr-bar {{
      grid-column: 1 / -1;
      margin-top: 4px;
      position: relative;
      height: 4px;
      border-radius: 999px;
      background: rgba(40, 42, 38, 0.08);
      overflow: hidden;
    }}
    .repr-bar-fill {{
      position: absolute;
      inset: 0 auto 0 0;
      background: #1a9850;
      border-radius: 999px;
    }}
    .map-zoom-buttons {{
      display: inline-flex;
      gap: 4px;
      padding: 3px;
      background: rgba(40, 42, 38, 0.06);
      border-radius: 999px;
      border: 1px solid var(--line);
    }}
    .map-zoom-buttons button {{
      border: 0;
      background: transparent;
      color: var(--muted);
      padding: 4px 10px;
      border-radius: 999px;
      font: inherit;
      font-size: 0.78rem;
      cursor: pointer;
    }}
    .map-zoom-buttons button:hover {{
      background: rgba(255, 255, 255, 0.6);
      color: var(--ink);
    }}
    .brand .eyebrow {{
      display: inline-block;
      padding: 3px 8px;
      border-radius: 999px;
      background: rgba(24, 27, 22, 0.06);
      color: var(--muted);
      font-size: 10.5px;
      letter-spacing: 0.09em;
      text-transform: uppercase;
    }}
    .brand h1 {{
      margin: 4px 0 0;
      font-size: 1.05rem;
      letter-spacing: -0.02em;
    }}
    .kpis {{
      display: flex;
      gap: 28px;
      justify-content: center;
      flex-wrap: wrap;
    }}
    .kpi {{
      display: grid;
      gap: 2px;
      justify-items: start;
    }}
    .kpi-value {{
      font-size: 1.3rem;
      font-weight: 700;
      letter-spacing: -0.03em;
    }}
    .kpi-label {{
      color: var(--muted);
      font-size: 0.7rem;
      text-transform: uppercase;
      letter-spacing: 0.09em;
    }}
    .meta {{
      display: grid;
      gap: 2px;
      font-size: 0.74rem;
      color: var(--muted);
      text-align: right;
    }}
    .meta code {{
      font-family: "IBM Plex Mono", ui-monospace, monospace;
      font-size: 0.7rem;
    }}
    .layout {{
      display: grid;
      grid-template-columns: minmax(340px, 400px) 1fr;
      gap: 12px;
      min-height: 0;
    }}
    .sidebar {{
      display: grid;
      gap: 12px;
      min-height: 0;
      overflow-y: auto;
      padding-right: 2px;
      align-content: start;
    }}
    .block {{
      background: var(--panel);
      border: 1px solid rgba(255, 255, 255, 0.55);
      border-radius: var(--radius-lg);
      box-shadow: var(--shadow);
      padding: 14px 16px;
      backdrop-filter: blur(10px);
    }}
    .block-head {{
      display: grid;
      gap: 2px;
      margin-bottom: 10px;
    }}
    .block-head h2 {{
      margin: 0;
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.11em;
      color: var(--muted);
    }}
    .block-caption {{
      color: var(--muted);
      font-size: 0.8rem;
    }}
    .family-list {{
      display: grid;
      gap: 2px;
      max-height: 360px;
      overflow-y: auto;
    }}
    .family-row {{
      display: grid;
      grid-template-columns: 1fr auto;
      grid-template-rows: auto auto;
      column-gap: 10px;
      padding: 8px 10px;
      border-radius: 10px;
      cursor: pointer;
      border: 1px solid transparent;
      transition: background 90ms ease;
    }}
    .family-row:hover {{ background: rgba(255, 255, 255, 0.55); }}
    .family-row[data-selected="true"] {{
      background: #fffcf3;
      border-color: var(--line);
      box-shadow: 0 4px 10px rgba(56, 52, 43, 0.06);
    }}
    .family-name {{
      font-weight: 600;
      font-size: 0.9rem;
    }}
    .family-meta {{
      color: var(--muted);
      font-size: 0.76rem;
      text-align: right;
      white-space: nowrap;
    }}
    .family-bar {{
      grid-column: 1 / -1;
      margin-top: 6px;
      position: relative;
      height: 5px;
      border-radius: 999px;
      background: rgba(40, 42, 38, 0.08);
      overflow: hidden;
    }}
    .family-bar-fill {{
      position: absolute;
      inset: 0 auto 0 0;
      background: var(--accent);
      border-radius: 999px;
    }}
    .family-row[data-key="all"] {{
      border-bottom: 1px dashed var(--line);
      padding-bottom: 12px;
      margin-bottom: 4px;
      border-radius: 10px;
    }}
    .family-row[data-key="all"] .family-bar-fill {{
      background: #1a9850;
    }}
    .filters {{
      display: grid;
      gap: 10px;
    }}
    .search-input {{
      width: 100%;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.78);
      border-radius: 10px;
      padding: 9px 12px;
      font: inherit;
      font-size: 0.9rem;
      color: var(--ink);
    }}
    .chip-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }}
    .chip {{
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.68);
      color: var(--ink);
      border-radius: 999px;
      padding: 5px 10px;
      font: inherit;
      font-size: 0.8rem;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      cursor: pointer;
    }}
    .chip[data-active="true"] {{
      background: var(--accent);
      color: #fffdf8;
      border-color: var(--accent);
    }}
    .swatch {{
      width: 10px;
      height: 10px;
      border-radius: 999px;
      border: 1px solid rgba(0, 0, 0, 0.12);
      flex: 0 0 10px;
    }}
    .gap-list {{
      display: grid;
      gap: 2px;
      max-height: 340px;
      overflow-y: auto;
    }}
    .gap-row {{
      display: grid;
      grid-template-columns: 1fr auto;
      align-items: center;
      column-gap: 10px;
      padding: 8px 10px;
      border-radius: 10px;
      cursor: pointer;
      border: 1px solid transparent;
    }}
    .gap-row:hover {{ background: rgba(255, 255, 255, 0.55); }}
    .gap-row[data-selected="true"] {{
      background: #fffcf3;
      border-color: var(--line);
    }}
    .gap-primary {{
      font-size: 0.88rem;
      font-weight: 600;
    }}
    .gap-secondary {{
      color: var(--muted);
      font-size: 0.76rem;
      display: flex;
      align-items: center;
      gap: 6px;
      margin-top: 2px;
    }}
    .gap-stat {{ text-align: right; white-space: nowrap; }}
    .gap-stat strong {{
      display: block;
      font-size: 0.92rem;
      letter-spacing: -0.02em;
    }}
    .gap-stat small {{
      color: var(--muted);
      font-size: 0.72rem;
    }}
    .map-panel {{
      display: grid;
      grid-template-rows: auto 1fr;
      gap: 10px;
      background: var(--panel);
      border: 1px solid rgba(255, 255, 255, 0.55);
      border-radius: var(--radius-lg);
      box-shadow: var(--shadow);
      padding: 12px;
      min-height: 0;
      backdrop-filter: blur(10px);
    }}
    .map-toolbar {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      flex-wrap: wrap;
      align-items: center;
    }}
    .legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      color: var(--muted);
      font-size: 0.8rem;
    }}
    .legend-item {{ display: inline-flex; align-items: center; gap: 6px; }}
    .map-caption {{
      color: var(--muted);
      font-size: 0.8rem;
      text-align: right;
    }}
    #map {{
      width: 100%;
      height: 100%;
      min-height: 0;
      border-radius: 14px;
      overflow: hidden;
      border: 1px solid rgba(36, 40, 31, 0.12);
    }}
    .empty {{
      padding: 18px;
      border: 1px dashed var(--line);
      border-radius: 12px;
      color: var(--muted);
      text-align: center;
      background: rgba(255, 255, 255, 0.55);
      font-size: 0.86rem;
    }}
    .leaflet-popup-content-wrapper {{ border-radius: 14px; }}
    .popup {{
      min-width: 220px;
      font-family: "IBM Plex Sans", ui-sans-serif, sans-serif;
    }}
    .popup h3 {{
      margin: 0 0 4px;
      font-size: 0.95rem;
      letter-spacing: -0.02em;
    }}
    .popup p {{
      margin: 0 0 10px;
      color: var(--muted);
      font-size: 0.8rem;
    }}
    .popup-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
      font-size: 0.8rem;
    }}
    .popup-grid strong {{
      display: block;
      font-size: 0.92rem;
      margin-top: 1px;
      letter-spacing: -0.02em;
    }}
    @supports (height: 100dvh) {{
      .shell {{ height: 100dvh; }}
    }}
    @media (max-width: 1100px) {{
      body {{ overflow: auto; }}
      .shell {{ height: auto; min-height: 100vh; }}
      .layout {{ grid-template-columns: 1fr; }}
      .sidebar {{ overflow: visible; }}
      .family-list, .gap-list {{ max-height: none; }}
      #map {{ min-height: 55vh; }}
      .topbar {{ grid-template-columns: 1fr; gap: 8px; text-align: left; }}
      .kpis {{ justify-content: flex-start; }}
      .meta {{ text-align: left; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <header class="topbar">
      <div class="brand">
        <span class="eyebrow">ASHP</span>
        <h1 id="brand-title">Document download coverage</h1>
      </div>
      <div class="kpis" id="kpis"></div>
      <div class="meta">
        <div><span>Updated </span><strong>{generated_at}</strong></div>
        <div><span>DB </span><code>{db_path}</code></div>
      </div>
      <div class="stacked-bar" id="stacked-bar" title="Application-level composition across the current view"></div>
    </header>
    <main class="layout">
      <aside class="sidebar">
        <section class="block">
          <div class="block-head">
            <h2>Portal families</h2>
            <span class="block-caption">Click to isolate. Bar shows phase-done share. Families sorted by apps still missing.</span>
          </div>
          <div id="family-list" class="family-list"></div>
        </section>
        <section class="block">
          <div class="block-head">
            <h2>Current view</h2>
            <span class="block-caption" id="view-summary"></span>
          </div>
          <div class="filters">
            <input id="search" class="search-input" type="search" placeholder="Search authority">
            <div id="coverage-chips" class="chip-row"></div>
          </div>
        </section>
        <section class="block">
          <div class="block-head">
            <h2>Remaining work</h2>
            <span class="block-caption">Apps still missing phase-done, grouped by the effort to close them.</span>
          </div>
          <div id="worktodo"></div>
        </section>
        <section class="block">
          <div class="block-head">
            <h2>Where to focus</h2>
            <span class="block-caption" id="gaps-caption">Top councils in the current view.</span>
          </div>
          <div class="tab-group" id="gaps-tabs">
            <button type="button" class="tab-btn" data-value="biggest" data-active="true">Biggest gaps</button>
            <button type="button" class="tab-btn" data-value="easiest" data-active="false">Easiest wins</button>
          </div>
          <div id="gap-list" class="gap-list"></div>
        </section>
        <section class="block">
          <div class="block-head">
            <h2>Representativeness</h2>
            <span class="block-caption">Global breakdown (ignores filters). Hover a row to highlight on the map.</span>
          </div>
          <div class="repr-group">
            <div class="repr-group-title">By UK nation</div>
            <div id="repr-nation"></div>
          </div>
          <div class="repr-group">
            <div class="repr-group-title">By authority size</div>
            <div id="repr-size"></div>
          </div>
        </section>
      </aside>
      <section class="map-panel">
        <div class="map-toolbar">
          <div class="legend" id="legend"></div>
          <div class="map-zoom-buttons" id="map-zoom-buttons">
            <button type="button" data-zoom="uk">UK</button>
            <button type="button" data-zoom="England">Eng</button>
            <button type="button" data-zoom="Scotland">Scot</button>
            <button type="button" data-zoom="Wales">Wales</button>
            <button type="button" data-zoom="Northern Ireland">NI</button>
          </div>
          <div class="map-caption" id="map-caption"></div>
        </div>
        <div id="map"></div>
      </section>
    </main>
  </div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const rawRows = {payload};
    const lpaGeo = {geojson_payload};
    const lpaLookup = {lookup_payload};

    const coverageMeta = {{
      phase_done: {{ label: "Phase done", color: "#1a9850" }},
      mostly_done: {{ label: "Mostly done", color: "#86c05d" }},
      in_progress: {{ label: "In progress", color: "#f0a94b" }},
      failed_only: {{ label: "Failed only", color: "#c83a2f" }},
      not_started: {{ label: "Not started", color: "#7f7f7f" }},
    }};

    const state = {{
      search: "",
      family: "all",
      coverage: new Set(Object.keys(coverageMeta)),
      selected: null,
      gapsView: "biggest",
      highlightedNation: null,
      mapStyle: "boundaries",
    }};

    // Build authority <-> LPA cross-indices from the lookup CSV.
    // share = 1 / number of LPAs an authority maps to, so joint authorities split apps evenly.
    const lpasByAuthority = new Map();
    const authoritiesByLPA = new Map();
    {{
      const counts = new Map();
      lpaLookup.forEach((r) => {{
        if (!r.lpa22cd) return;  // no_lpa / out_of_scope rows
        counts.set(r.db_authority_name, (counts.get(r.db_authority_name) ?? 0) + 1);
      }});
      lpaLookup.forEach((r) => {{
        if (!r.lpa22cd) return;
        const share = 1 / counts.get(r.db_authority_name);
        const entry = {{ lpa22cd: r.lpa22cd, lpa22nm: r.lpa22nm, status: r.status, share }};
        if (!lpasByAuthority.has(r.db_authority_name)) lpasByAuthority.set(r.db_authority_name, []);
        lpasByAuthority.get(r.db_authority_name).push(entry);
        if (!authoritiesByLPA.has(r.lpa22cd)) authoritiesByLPA.set(r.lpa22cd, []);
        authoritiesByLPA.get(r.lpa22cd).push({{ db_authority_name: r.db_authority_name, share }});
      }});
    }}

    const effortMeta = {{
      retry: {{ label: "One retry away", color: "#86c05d", desc: "mostly-done or all-failed councils — a re-run closes them" }},
      continue: {{ label: "Work in flight", color: "#f0a94b", desc: "partial progress under 70%" }},
      greenfield: {{ label: "Never started", color: "#7f7f7f", desc: "no download attempts yet" }},
    }};

    function effortOf(bucket) {{
      if (bucket === "mostly_done" || bucket === "failed_only") return "retry";
      if (bucket === "in_progress") return "continue";
      if (bucket === "not_started") return "greenfield";
      return null;
    }}

    function nationOf(lat, lng) {{
      if (lat == null || lng == null) return "Other";
      if (lat >= 54.0 && lat <= 55.5 && lng >= -8.3 && lng <= -5.4) return "Northern Ireland";
      if (lat > 55.0) return "Scotland";
      if (lat >= 51.3 && lat <= 53.7 && lng >= -5.4 && lng <= -2.7) return "Wales";
      if (lat >= 49.5 && lat <= 56.2 && lng >= -5.4 && lng <= 2.0) return "England";
      return "Other";
    }}

    function sizeBucketOf(count) {{
      if (count >= 100) return "Large (≥100 apps)";
      if (count >= 20) return "Medium (20–99)";
      return "Small (<20)";
    }}

    const NATION_ORDER = ["England", "Scotland", "Wales", "Northern Ireland", "Other"];
    const SIZE_ORDER = ["Large (≥100 apps)", "Medium (20–99)", "Small (<20)"];

    function formatInt(value) {{
      return new Intl.NumberFormat("en-GB").format(value ?? 0);
    }}
    function formatPct(value) {{
      return `${{Math.round((value ?? 0) * 100)}}%`;
    }}

    function enrichRow(row) {{
      row.key = `a:${{row.authority_name}}|p:${{row.portal_type}}`;
      row.missing_applications = row.application_count - row.apps_download_phase_done;
      row.coverage_bucket = deriveBucket(row);
      row.color = coverageMeta[row.coverage_bucket]?.color ?? "#7f7f7f";
      row.coverage_label = coverageMeta[row.coverage_bucket]?.label ?? row.coverage_bucket;
      row.portal_label = row.portal_type;
      row.effort = effortOf(row.coverage_bucket);
      row.nation = nationOf(row.lat, row.lng);
      row.size_bucket = sizeBucketOf(row.application_count);
      return row;
    }}

    function deriveBucket(unit) {{
      const pct = unit.application_count ? unit.apps_download_phase_done / unit.application_count : 0;
      if (unit.application_count > 0 && pct >= 0.95) return "phase_done";
      if (unit.application_count > 0 && pct >= 0.70) return "mostly_done";
      if ((unit.apps_with_downloaded_docs ?? 0) > 0) return "in_progress";
      if ((unit.download_attempt_count ?? 0) > 0) return "failed_only";
      return "not_started";
    }}

    function buildAuthorityUnits(rows) {{
      const byAuthority = new Map();
      rows.forEach((row) => {{
        const existing = byAuthority.get(row.authority_name);
        if (existing) {{
          existing.application_count += row.application_count;
          existing.apps_download_phase_done += row.apps_download_phase_done;
          existing.apps_with_downloaded_docs += row.apps_with_downloaded_docs ?? 0;
          existing.download_attempt_count += row.download_attempt_count ?? 0;
          existing.files_downloaded_total += row.files_downloaded_total ?? 0;
          existing.success_count += row.success_count ?? 0;
          existing.partial_count += row.partial_count ?? 0;
          existing.no_docs_available_count += row.no_docs_available_count ?? 0;
          existing.failed_count += row.failed_count ?? 0;
          existing.not_attempted_count += row.not_attempted_count ?? 0;
          existing.portal_types.add(row.portal_type);
          existing.portal_rows.push(row);
          return;
        }}
        byAuthority.set(row.authority_name, {{
          authority_name: row.authority_name,
          portal_type: "aggregate",
          lat: row.lat,
          lng: row.lng,
          application_count: row.application_count,
          apps_download_phase_done: row.apps_download_phase_done,
          apps_with_downloaded_docs: row.apps_with_downloaded_docs ?? 0,
          download_attempt_count: row.download_attempt_count ?? 0,
          files_downloaded_total: row.files_downloaded_total ?? 0,
          success_count: row.success_count ?? 0,
          partial_count: row.partial_count ?? 0,
          no_docs_available_count: row.no_docs_available_count ?? 0,
          failed_count: row.failed_count ?? 0,
          not_attempted_count: row.not_attempted_count ?? 0,
          portal_types: new Set([row.portal_type]),
          portal_rows: [row],
        }});
      }});
      return Array.from(byAuthority.values()).map((unit) => {{
        unit.pct_download_phase_done = unit.application_count ? unit.apps_download_phase_done / unit.application_count : 0;
        unit.missing_applications = unit.application_count - unit.apps_download_phase_done;
        unit.coverage_bucket = deriveBucket(unit);
        unit.color = coverageMeta[unit.coverage_bucket].color;
        unit.coverage_label = coverageMeta[unit.coverage_bucket].label;
        unit.key = `a:${{unit.authority_name}}`;
        const portalList = Array.from(unit.portal_types).sort();
        unit.portal_label = portalList.length === 1 ? portalList[0] : `${{portalList.length}} portals`;
        unit.portal_list = portalList;
        unit.effort = effortOf(unit.coverage_bucket);
        unit.nation = nationOf(unit.lat, unit.lng);
        unit.size_bucket = sizeBucketOf(unit.application_count);
        return unit;
      }});
    }}

    function buildFamilySummaries(rows) {{
      const byFamily = new Map();
      rows.forEach((row) => {{
        const existing = byFamily.get(row.portal_type);
        if (existing) {{
          existing.application_count += row.application_count;
          existing.apps_download_phase_done += row.apps_download_phase_done;
          existing.failed_count += row.failed_count ?? 0;
          existing.authorities.add(row.authority_name);
          return;
        }}
        byFamily.set(row.portal_type, {{
          portal_type: row.portal_type,
          application_count: row.application_count,
          apps_download_phase_done: row.apps_download_phase_done,
          failed_count: row.failed_count ?? 0,
          authorities: new Set([row.authority_name]),
        }});
      }});
      return Array.from(byFamily.values()).map((f) => ({{
        portal_type: f.portal_type,
        application_count: f.application_count,
        apps_download_phase_done: f.apps_download_phase_done,
        failed_count: f.failed_count,
        pct_done: f.application_count ? f.apps_download_phase_done / f.application_count : 0,
        pct_failed: f.application_count ? f.failed_count / f.application_count : 0,
        missing: f.application_count - f.apps_download_phase_done,
        authority_count: f.authorities.size,
      }})).sort((a, b) => b.missing - a.missing);
    }}

    const rows = rawRows.map(enrichRow);
    const authorityUnits = buildAuthorityUnits(rows);
    const familySummaries = buildFamilySummaries(rows);
    const totalApps = authorityUnits.reduce((s, u) => s + u.application_count, 0);
    const totalDone = authorityUnits.reduce((s, u) => s + u.apps_download_phase_done, 0);

    const map = L.map("map", {{ zoomControl: true }});
    L.tileLayer("https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      maxZoom: 19,
      referrerPolicy: "strict-origin-when-cross-origin"
    }}).addTo(map);
    const markerLayer = L.layerGroup().addTo(map);
    const polygonLayer = L.geoJSON(null, {{ style: () => ({{ color: "rgba(20,24,20,0.35)", weight: 0.6, fillOpacity: 0.7 }}) }}).addTo(map);
    const markerByKey = new Map();
    const markerUnitByKey = new Map();
    const polygonByCode = new Map();
    const polygonAggByCode = new Map();
    let hasFitBounds = false;

    function currentUnits() {{
      if (state.family === "all") return authorityUnits;
      return rows.filter((r) => r.portal_type === state.family);
    }}

    function filteredUnits() {{
      const search = state.search.trim().toLowerCase();
      return currentUnits()
        .filter((u) => state.coverage.has(u.coverage_bucket))
        .filter((u) => !search || u.authority_name.toLowerCase().includes(search))
        .sort((a, b) =>
          (b.missing_applications - a.missing_applications) ||
          (a.pct_download_phase_done - b.pct_download_phase_done) ||
          (b.application_count - a.application_count) ||
          a.authority_name.localeCompare(b.authority_name)
        );
    }}

    function markerRadius(unit) {{
      return Math.max(6, Math.min(28, 5 + Math.sqrt(unit.application_count) * 1.3));
    }}

    function popupHtml(unit) {{
      const subtitle = state.family === "all"
        ? `${{unit.portal_label}} · ${{unit.coverage_label}}`
        : `${{unit.portal_type}} · ${{unit.coverage_label}}`;
      const compositionSource = [
        {{ label: "Phase done", value: (unit.success_count ?? 0) + (unit.no_docs_available_count ?? 0), color: coverageMeta.phase_done.color }},
        {{ label: "Partial", value: unit.partial_count ?? 0, color: coverageMeta.in_progress.color }},
        {{ label: "Failed", value: unit.failed_count ?? 0, color: coverageMeta.failed_only.color }},
        {{ label: "Never tried", value: unit.not_attempted_count ?? 0, color: coverageMeta.not_started.color }},
      ];
      const composition = compositionSource
        .filter((c) => c.value > 0)
        .map((c) => `<div class="popup-comp-row"><span class="swatch" style="background:${{c.color}}"></span><span>${{c.label}}</span><strong>${{formatInt(c.value)}}</strong></div>`)
        .join("");
      let portalBreakdown = "";
      if (state.family === "all" && unit.portal_rows && unit.portal_rows.length > 1) {{
        const sortedPortals = [...unit.portal_rows].sort((a, b) => b.application_count - a.application_count);
        portalBreakdown = `
          <div class="popup-section">
            <div class="popup-section-head">By portal</div>
            ${{sortedPortals.map((r) => `
              <div class="popup-portal-row">
                <span>${{r.portal_type}}</span>
                <span><strong>${{formatInt(r.application_count)}}</strong> · ${{formatPct(r.pct_download_phase_done)}} done</span>
              </div>
            `).join("")}}
          </div>
        `;
      }}
      return `
        <div class="popup">
          <h3>${{unit.authority_name}}</h3>
          <p>${{subtitle}}</p>
          <div class="popup-section">
            <div class="popup-section-head">Applications (${{formatInt(unit.application_count)}})</div>
            ${{composition || `<div class="popup-comp-row"><span></span><span>No breakdown available</span><strong>—</strong></div>`}}
          </div>
          ${{portalBreakdown}}
          <div class="popup-section">
            <div class="popup-section-head">Files captured</div>
            <div class="popup-comp-row"><span></span><span>Downloaded</span><strong>${{formatInt(unit.files_downloaded_total ?? 0)}}</strong></div>
          </div>
        </div>
      `;
    }}

    function renderWorkToDo(units) {{
      const tally = {{ retry: 0, continue: 0, greenfield: 0 }};
      const authorities = {{ retry: new Set(), continue: new Set(), greenfield: new Set() }};
      units.forEach((u) => {{
        const e = effortOf(u.coverage_bucket);
        if (!e) return;
        tally[e] += u.missing_applications;
        authorities[e].add(u.authority_name);
      }});
      const total = tally.retry + tally.continue + tally.greenfield;
      const segs = ["retry", "continue", "greenfield"].map((k) => ({{
        key: k,
        label: effortMeta[k].label,
        color: effortMeta[k].color,
        desc: effortMeta[k].desc,
        count: tally[k],
        authorities: authorities[k].size,
      }}));
      const bar = segs.filter((s) => s.count > 0).map((s) =>
        `<div class="stacked-seg" style="flex-grow:${{s.count}}; background:${{s.color}}" title="${{s.label}}: ${{formatInt(s.count)}} apps"></div>`
      ).join("");
      const rows = segs.map((s) => `
        <div class="worktodo-row">
          <span class="swatch" style="background:${{s.color}}"></span>
          <div class="worktodo-label">
            <div>${{s.label}}</div>
            <small>${{s.desc}}</small>
          </div>
          <div class="worktodo-count">
            <strong>${{formatInt(s.count)}}</strong>
            <small>${{formatInt(s.authorities)}} councils · ${{formatPct(total ? s.count / total : 0)}}</small>
          </div>
        </div>
      `).join("");
      document.getElementById("worktodo").innerHTML =
        `<div class="stacked-bar" style="margin-bottom:10px;">${{bar || '<div class="stacked-seg" style="flex-grow:1; background:#1a9850"></div>'}}</div>${{rows}}`;
    }}

    function renderRepresentativeness() {{
      function renderGroup(containerId, grouping, order) {{
        const byKey = new Map();
        authorityUnits.forEach((u) => {{
          const k = grouping(u);
          if (!byKey.has(k)) byKey.set(k, {{ apps: 0, done: 0, authorities: 0 }});
          const g = byKey.get(k);
          g.apps += u.application_count;
          g.done += u.apps_download_phase_done;
          g.authorities += 1;
        }});
        const rows = order
          .filter((k) => byKey.has(k))
          .map((k) => {{
            const g = byKey.get(k);
            return {{
              key: k,
              apps: g.apps,
              done: g.done,
              pct: g.apps ? g.done / g.apps : 0,
              authorities: g.authorities,
            }};
          }});
        const container = document.getElementById(containerId);
        container.innerHTML = rows.map((r) => `
          <div class="repr-row" data-nation="${{r.key}}" data-highlighted="${{state.highlightedNation === r.key}}">
            <div>
              <div class="repr-name">${{r.key}}</div>
              <div class="repr-meta">${{formatInt(r.authorities)}} authorities · ${{formatInt(r.apps)}} apps</div>
            </div>
            <div class="repr-pct">${{formatPct(r.pct)}}</div>
            <div class="repr-bar">
              <div class="repr-bar-fill" style="width:${{Math.max(2, r.pct * 100)}}%"></div>
            </div>
          </div>
        `).join("");
        if (containerId === "repr-nation") {{
          container.querySelectorAll(".repr-row").forEach((row) => {{
            row.addEventListener("mouseenter", () => {{
              state.highlightedNation = row.dataset.nation;
              updateNationHighlight();
            }});
            row.addEventListener("mouseleave", () => {{
              state.highlightedNation = null;
              updateNationHighlight();
            }});
            row.addEventListener("click", () => {{
              zoomToNation(row.dataset.nation);
            }});
          }});
        }}
      }}
      renderGroup("repr-nation", (u) => u.nation, NATION_ORDER);
      renderGroup("repr-size", (u) => u.size_bucket, SIZE_ORDER);
    }}

    function updateNationHighlight() {{
      document.querySelectorAll("#repr-nation .repr-row").forEach((row) => {{
        row.dataset.highlighted = state.highlightedNation === row.dataset.nation ? "true" : "false";
      }});
      markerByKey.forEach((marker, key) => {{
        const unit = markerUnitByKey.get(key);
        if (!unit) return;
        const dim = state.highlightedNation != null && unit.nation !== state.highlightedNation;
        const isSelected = state.selected === key;
        marker.setStyle({{
          weight: isSelected ? 3 : 1.2,
          fillOpacity: dim ? 0.12 : (isSelected ? 1 : 0.82),
          opacity: dim ? 0.25 : 1,
        }});
      }});
    }}

    function zoomToNation(nation) {{
      const targets = authorityUnits.filter((u) => u.nation === nation && u.lat != null && u.lng != null);
      if (!targets.length) return;
      const bounds = L.latLngBounds(targets.map((u) => [u.lat, u.lng]));
      if (bounds.isValid()) map.fitBounds(bounds.pad(0.15));
    }}

    function zoomToUK() {{
      const bounds = L.latLngBounds(authorityUnits
        .filter((u) => u.lat != null && u.lng != null)
        .map((u) => [u.lat, u.lng]));
      if (bounds.isValid()) map.fitBounds(bounds.pad(0.1));
    }}

    function renderStackedBar(units) {{
      const done = units.reduce((s, u) => s + (u.success_count ?? 0) + (u.no_docs_available_count ?? 0), 0);
      const partial = units.reduce((s, u) => s + (u.partial_count ?? 0), 0);
      const failed = units.reduce((s, u) => s + (u.failed_count ?? 0), 0);
      const notAttempted = units.reduce((s, u) => s + (u.not_attempted_count ?? 0), 0);
      const total = done + partial + failed + notAttempted;
      const el = document.getElementById("stacked-bar");
      if (!total) {{ el.innerHTML = ""; return; }}
      const segments = [
        {{ label: "Phase done", value: done, color: coverageMeta.phase_done.color }},
        {{ label: "Partial", value: partial, color: coverageMeta.in_progress.color }},
        {{ label: "Failed", value: failed, color: coverageMeta.failed_only.color }},
        {{ label: "Never tried", value: notAttempted, color: coverageMeta.not_started.color }},
      ];
      el.innerHTML = segments
        .filter((s) => s.value > 0)
        .map((s) => `<div class="stacked-seg" style="flex-grow:${{s.value}}; background:${{s.color}}" title="${{s.label}}: ${{formatInt(s.value)}} apps · ${{formatPct(s.value / total)}}"></div>`)
        .join("");
    }}

    function renderKpis(units) {{
      const applications = units.reduce((s, u) => s + u.application_count, 0);
      const done = units.reduce((s, u) => s + u.apps_download_phase_done, 0);
      const missing = applications - done;
      const pct = applications ? done / applications : 0;
      const authorities = new Set(units.map((u) => u.authority_name)).size;
      const kpis = [
        {{ label: "Phase done", value: formatPct(pct) }},
        {{ label: "Missing apps", value: formatInt(missing) }},
        {{ label: "Applications", value: formatInt(applications) }},
        {{ label: "Authorities", value: formatInt(authorities) }},
      ];
      document.getElementById("kpis").innerHTML = kpis.map((k) => `
        <div class="kpi">
          <div class="kpi-value">${{k.value}}</div>
          <div class="kpi-label">${{k.label}}</div>
        </div>
      `).join("");
    }}

    function renderFamilyList() {{
      const container = document.getElementById("family-list");
      const globalFailed = familySummaries.reduce((s, f) => s + f.failed_count, 0);
      const globalFailedPct = totalApps ? globalFailed / totalApps : 0;
      const failBadge = (pct) => pct >= 0.005 ? ` · <span style="color:#c83a2f">${{formatPct(pct)}} failed</span>` : "";
      const all = {{
        key: "all",
        label: "All families",
        apps: totalApps,
        pct: totalApps ? totalDone / totalApps : 0,
        pct_failed: globalFailedPct,
        meta: `${{formatInt(authorityUnits.length)}} authorities${{failBadge(globalFailedPct)}}`,
      }};
      const familyRows = familySummaries.map((f) => ({{
        key: f.portal_type,
        label: f.portal_type,
        apps: f.application_count,
        pct: f.pct_done,
        pct_failed: f.pct_failed,
        meta: `${{formatInt(f.authority_count)}} authorities · ${{formatInt(f.missing)}} missing${{failBadge(f.pct_failed)}}`,
      }}));
      const entries = [all, ...familyRows];
      container.innerHTML = entries.map((entry) => {{
        const donePct = Math.max(0, entry.pct * 100);
        const failPct = Math.max(0, (entry.pct_failed ?? 0) * 100);
        return `
        <div class="family-row" data-key="${{entry.key}}" data-selected="${{state.family === entry.key}}">
          <div>
            <div class="family-name">${{entry.label}}</div>
            <div class="family-meta" style="text-align:left; margin-top:2px;">${{entry.meta}}</div>
          </div>
          <div class="family-meta">
            <div><strong>${{formatPct(entry.pct)}}</strong></div>
            <div>${{formatInt(entry.apps)}} apps</div>
          </div>
          <div class="family-bar">
            <div class="family-bar-fill" style="width:${{Math.max(2, donePct)}}%"></div>
            ${{failPct > 0 ? `<div class="family-bar-fail" style="left:${{donePct}}%; width:${{failPct}}%"></div>` : ""}}
          </div>
        </div>
        `;
      }}).join("");
      container.querySelectorAll(".family-row").forEach((row) => {{
        row.addEventListener("click", () => {{
          state.family = row.dataset.key;
          state.selected = null;
          hasFitBounds = false;
          render();
        }});
      }});
    }}

    function renderCoverageChips() {{
      const container = document.getElementById("coverage-chips");
      container.innerHTML = Object.entries(coverageMeta).map(([key, meta]) => `
        <button type="button" class="chip" data-key="${{key}}" data-active="${{state.coverage.has(key)}}">
          <span class="swatch" style="background:${{meta.color}}"></span>
          <span>${{meta.label}}</span>
        </button>
      `).join("");
      container.querySelectorAll(".chip").forEach((chip) => {{
        chip.addEventListener("click", () => {{
          const key = chip.dataset.key;
          if (state.coverage.has(key) && state.coverage.size > 1) {{
            state.coverage.delete(key);
          }} else {{
            state.coverage.add(key);
          }}
          render();
        }});
      }});
    }}

    function syncCoverageChips() {{
      document.querySelectorAll("#coverage-chips .chip").forEach((chip) => {{
        chip.dataset.active = state.coverage.has(chip.dataset.key) ? "true" : "false";
      }});
    }}

    function renderLegend() {{
      const items = Object.entries(coverageMeta).map(([, meta]) => `
          <span class="legend-item">
            <span class="swatch" style="background:${{meta.color}}"></span>
            <span>${{meta.label}}</span>
          </span>
        `);
      if (state.mapStyle === "boundaries") {{
        items.push(`
          <span class="legend-item">
            <span class="swatch" style="background:#d8d6cf;opacity:0.55"></span>
            <span>No data</span>
          </span>`);
      }}
      document.getElementById("legend").innerHTML = items.join("");
    }}

    function renderMapCaption(units) {{
      const el = document.getElementById("map-caption");
      let hint;
      if (state.mapStyle === "boundaries") {{
        const drawn = polygonAggByCode.size;
        const total = lpaGeo.features.length;
        hint = state.family === "all"
          ? `${{formatInt(drawn)}} of ${{formatInt(total)}} LPA polygons coloured`
          : `${{state.family}} only · ${{formatInt(drawn)}} of ${{formatInt(total)}} LPAs`;
      }} else {{
        hint = state.family === "all"
          ? "One marker per council · portals aggregated"
          : `Only ${{state.family}} authorities shown`;
      }}
      el.textContent = `${{hint}} · ${{formatInt(units.length)}} in view`;
    }}

    function makeEmptyAgg(code) {{
      return {{
        lpa22cd: code, lpa22nm: "",
        application_count: 0, apps_download_phase_done: 0, apps_with_downloaded_docs: 0,
        success_count: 0, partial_count: 0, no_docs_available_count: 0, failed_count: 0,
        not_attempted_count: 0, download_attempt_count: 0, files_downloaded_total: 0,
        contributing: [],
      }};
    }}

    function aggregateUnitsByLPA(units) {{
      const byLpa = new Map();
      units.forEach((u) => {{
        const lpas = lpasByAuthority.get(u.authority_name);
        if (!lpas) return;
        lpas.forEach(({{ lpa22cd, share }}) => {{
          let agg = byLpa.get(lpa22cd);
          if (!agg) {{ agg = makeEmptyAgg(lpa22cd); byLpa.set(lpa22cd, agg); }}
          agg.application_count += u.application_count * share;
          agg.apps_download_phase_done += u.apps_download_phase_done * share;
          agg.apps_with_downloaded_docs += (u.apps_with_downloaded_docs ?? 0) * share;
          agg.success_count += (u.success_count ?? 0) * share;
          agg.partial_count += (u.partial_count ?? 0) * share;
          agg.no_docs_available_count += (u.no_docs_available_count ?? 0) * share;
          agg.failed_count += (u.failed_count ?? 0) * share;
          agg.not_attempted_count += (u.not_attempted_count ?? 0) * share;
          agg.download_attempt_count += (u.download_attempt_count ?? 0) * share;
          agg.files_downloaded_total += (u.files_downloaded_total ?? 0) * share;
          agg.contributing.push({{ name: u.authority_name, apps: u.application_count * share }});
        }});
      }});
      byLpa.forEach((agg) => {{
        ["application_count", "apps_download_phase_done", "apps_with_downloaded_docs",
         "success_count", "partial_count", "no_docs_available_count", "failed_count",
         "not_attempted_count", "download_attempt_count", "files_downloaded_total"].forEach((k) => {{
           agg[k] = Math.round(agg[k]);
         }});
        agg.pct_download_phase_done = agg.application_count
          ? agg.apps_download_phase_done / agg.application_count : 0;
        agg.missing_applications = agg.application_count - agg.apps_download_phase_done;
        agg.coverage_bucket = deriveBucket(agg);
        agg.color = coverageMeta[agg.coverage_bucket].color;
        agg.coverage_label = coverageMeta[agg.coverage_bucket].label;
      }});
      return byLpa;
    }}

    function polygonPopupHtml(agg) {{
      const compositionSource = [
        {{ label: "Phase done", value: (agg.success_count ?? 0) + (agg.no_docs_available_count ?? 0), color: coverageMeta.phase_done.color }},
        {{ label: "Partial", value: agg.partial_count ?? 0, color: coverageMeta.in_progress.color }},
        {{ label: "Failed", value: agg.failed_count ?? 0, color: coverageMeta.failed_only.color }},
        {{ label: "Never tried", value: agg.not_attempted_count ?? 0, color: coverageMeta.not_started.color }},
      ];
      const composition = compositionSource
        .filter((c) => c.value > 0)
        .map((c) => `<div class="popup-comp-row"><span class="swatch" style="background:${{c.color}}"></span><span>${{c.label}}</span><strong>${{formatInt(c.value)}}</strong></div>`)
        .join("");
      const contribs = agg.contributing.length > 1
        ? `<div class="popup-section">
            <div class="popup-section-head">From PlanIt authorities</div>
            ${{agg.contributing.slice().sort((a, b) => b.apps - a.apps).map((c) => `<div class="popup-portal-row"><span>${{c.name}}</span><strong>${{formatInt(Math.round(c.apps))}}</strong></div>`).join("")}}
           </div>`
        : "";
      return `
        <div class="popup">
          <h3>${{agg.lpa22nm}}</h3>
          <p>${{agg.coverage_label}} · ${{formatPct(agg.pct_download_phase_done)}} done</p>
          <div class="popup-section">
            <div class="popup-section-head">Applications (${{formatInt(agg.application_count)}})</div>
            ${{composition || `<div class="popup-comp-row"><span></span><span>No breakdown</span><strong>—</strong></div>`}}
          </div>
          ${{contribs}}
        </div>
      `;
    }}

    function renderPolygons(units) {{
      markerLayer.clearLayers();
      markerByKey.clear();
      markerUnitByKey.clear();
      polygonLayer.clearLayers();
      polygonByCode.clear();
      polygonAggByCode.clear();

      const aggByLpa = aggregateUnitsByLPA(units);
      lpaGeo.features.forEach((feature) => {{
        const code = feature.properties.LPA22CD;
        const name = feature.properties.LPA22NM.replace(/\s+LPA$/, "");
        const agg = aggByLpa.get(code);
        const isSelected = agg && state.selected === `lpa:${{code}}`;
        const style = agg
          ? {{ fillColor: agg.color, color: isSelected ? "#1e1f1d" : "rgba(20,24,20,0.35)",
               weight: isSelected ? 2.2 : 0.6, fillOpacity: isSelected ? 0.92 : 0.78 }}
          : {{ fillColor: "#d8d6cf", color: "rgba(20,24,20,0.18)", weight: 0.4, fillOpacity: 0.35 }};
        const layer = L.geoJSON(feature, {{ style }});
        if (agg) {{
          agg.lpa22nm = name;
          layer.bindTooltip(`${{name}} · ${{formatInt(agg.application_count)}} apps · ${{formatInt(agg.apps_with_downloaded_docs)}} with docs · ${{formatPct(agg.pct_download_phase_done)}} done`,
            {{ direction: "top", opacity: 1, className: "marker-tooltip", sticky: true }});
          layer.bindPopup(polygonPopupHtml(agg));
          const key = `lpa:${{code}}`;
          layer.on("click", () => {{ state.selected = key; updatePolygonSelection(); }});
          polygonByCode.set(code, layer);
          polygonAggByCode.set(code, agg);
        }} else {{
          layer.bindTooltip(`${{name}} · no apps in view`,
            {{ direction: "top", opacity: 1, className: "marker-tooltip", sticky: true }});
        }}
        layer.addTo(polygonLayer);
      }});

      if (!hasFitBounds) {{
        const bounds = polygonLayer.getBounds();
        if (bounds.isValid()) {{ map.fitBounds(bounds.pad(0.05)); hasFitBounds = true; }}
      }}
    }}

    function updatePolygonSelection() {{
      polygonByCode.forEach((layer, code) => {{
        const agg = polygonAggByCode.get(code);
        const isSelected = state.selected === `lpa:${{code}}`;
        layer.setStyle({{
          fillColor: agg.color,
          color: isSelected ? "#1e1f1d" : "rgba(20,24,20,0.35)",
          weight: isSelected ? 2.2 : 0.6,
          fillOpacity: isSelected ? 0.92 : 0.78,
        }});
      }});
    }}

    function renderMarkers(units) {{
      polygonLayer.clearLayers();
      polygonByCode.clear();
      polygonAggByCode.clear();
      markerLayer.clearLayers();
      markerByKey.clear();
      markerUnitByKey.clear();
      if (!units.length) return;
      units.forEach((unit) => {{
        if (unit.lat == null || unit.lng == null) return;
        const selected = state.selected === unit.key;
        const marker = L.circleMarker([unit.lat, unit.lng], {{
          radius: markerRadius(unit),
          color: "rgba(20, 24, 20, 0.4)",
          weight: selected ? 3 : 1.2,
          fillColor: unit.color,
          fillOpacity: selected ? 1 : 0.82,
          bubblingMouseEvents: false,
        }});
        marker.bindPopup(popupHtml(unit));
        marker.bindTooltip(`${{unit.authority_name}} · ${{formatInt(unit.application_count)}} apps · ${{formatInt(unit.apps_with_downloaded_docs ?? 0)}} with docs · ${{formatPct(unit.pct_download_phase_done)}} done`, {{
          direction: "top",
          opacity: 1,
          className: "marker-tooltip",
          offset: [0, -4],
        }});
        marker.on("click", () => {{
          state.selected = unit.key;
          updateSelection();
        }});
        marker.addTo(markerLayer);
        markerByKey.set(unit.key, marker);
        markerUnitByKey.set(unit.key, unit);
      }});
      if (!hasFitBounds) {{
        const points = units.filter((u) => u.lat != null && u.lng != null).map((u) => [u.lat, u.lng]);
        if (points.length) {{
          const bounds = L.latLngBounds(points);
          if (bounds.isValid()) {{
            map.fitBounds(bounds.pad(0.14));
            hasFitBounds = true;
          }}
        }}
      }}
    }}

    function renderGapList(units) {{
      let top;
      const caption = document.getElementById("gaps-caption");
      if (state.gapsView === "easiest") {{
        top = units
          .filter((u) => u.coverage_bucket === "mostly_done")
          .sort((a, b) => b.missing_applications - a.missing_applications)
          .slice(0, 12);
        caption.textContent = "Mostly-done councils — a re-run closes them. Ranked by apps unlocked.";
      }} else {{
        top = units.slice(0, 12);
        caption.textContent = "Top councils in the current view by apps still missing phase-done coverage.";
      }}
      const el = document.getElementById("gap-list");
      if (!top.length) {{
        el.innerHTML = `<div class="empty">${{state.gapsView === "easiest" ? "No mostly-done councils in the current view." : "No councils match the current filter."}}</div>`;
        return;
      }}
      el.innerHTML = top.map((unit) => {{
        const portalBit = state.family === "all" ? unit.portal_label : unit.portal_type;
        return `
        <div class="gap-row" data-key="${{unit.key}}" data-selected="${{state.selected === unit.key}}">
          <div>
            <div class="gap-primary">${{unit.authority_name}}</div>
            <div class="gap-secondary">
              <span class="swatch" style="background:${{unit.color}}"></span>
              <span>${{portalBit}} · ${{unit.coverage_label}}</span>
            </div>
          </div>
          <div class="gap-stat">
            <strong>${{formatInt(unit.missing_applications)}}</strong>
            <small>${{formatPct(unit.pct_download_phase_done)}} done</small>
          </div>
        </div>
        `;
      }}).join("");
      el.querySelectorAll(".gap-row").forEach((row) => {{
        row.addEventListener("click", () => {{
          const unit = top.find((u) => u.key === row.dataset.key);
          if (!unit) return;
          state.selected = unit.key;
          updateSelection();
          const marker = markerByKey.get(unit.key);
          if (marker) {{
            map.setView(marker.getLatLng(), Math.max(map.getZoom(), 8), {{ animate: true }});
            marker.openPopup();
          }}
        }});
      }});
    }}

    function updateSelection() {{
      markerByKey.forEach((marker, key) => {{
        const isSelected = state.selected === key;
        marker.setStyle({{
          weight: isSelected ? 3 : 1.2,
          fillOpacity: isSelected ? 1 : 0.82,
        }});
      }});
      document.querySelectorAll("#gap-list .gap-row").forEach((row) => {{
        row.dataset.selected = state.selected === row.dataset.key ? "true" : "false";
      }});
    }}

    function renderSummary(units) {{
      const apps = units.reduce((s, u) => s + u.application_count, 0);
      const authorityCount = new Set(units.map((u) => u.authority_name)).size;
      const scope = state.family === "all" ? "All families" : `Family: ${{state.family}}`;
      document.getElementById("view-summary").textContent =
        `${{scope}} · ${{formatInt(apps)}} apps · ${{formatInt(authorityCount)}} authorities in view`;
    }}

    function render() {{
      const units = filteredUnits();
      renderKpis(units);
      renderStackedBar(units);
      renderWorkToDo(units);
      renderFamilyList();
      renderSummary(units);
      if (state.mapStyle === "boundaries") renderPolygons(units);
      else renderMarkers(units);
      renderGapList(units);
      renderLegend();
      renderMapCaption(units);
      syncCoverageChips();
      syncMapStyleToggle();
      renderRepresentativeness();
      syncGapsTabs();
    }}

    function syncMapStyleToggle() {{
      document.querySelectorAll("#map-style-toggle .tab-btn").forEach((btn) => {{
        btn.dataset.active = btn.dataset.style === state.mapStyle ? "true" : "false";
      }});
    }}

    function syncGapsTabs() {{
      document.querySelectorAll("#gaps-tabs .tab-btn").forEach((btn) => {{
        btn.dataset.active = btn.dataset.value === state.gapsView ? "true" : "false";
      }});
    }}

    renderLegend();
    renderCoverageChips();

    document.getElementById("search").addEventListener("input", (event) => {{
      state.search = event.target.value;
      render();
    }});

    document.querySelectorAll("#gaps-tabs .tab-btn").forEach((btn) => {{
      btn.addEventListener("click", () => {{
        state.gapsView = btn.dataset.value;
        render();
      }});
    }});

    document.querySelectorAll("#map-zoom-buttons button").forEach((btn) => {{
      btn.addEventListener("click", () => {{
        const target = btn.dataset.zoom;
        if (target === "uk") zoomToUK();
        else zoomToNation(target);
      }});
    }});

    document.querySelectorAll("#map-style-toggle .tab-btn").forEach((btn) => {{
      btn.addEventListener("click", () => {{
        if (state.mapStyle === btn.dataset.style) return;
        state.mapStyle = btn.dataset.style;
        state.selected = null;
        hasFitBounds = false;
        render();
      }});
    }});
    render();
  </script>
</body>
</html>
"""


def write_dashboard(
    rows: list[dict],
    *,
    db_path: Path,
) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    if LPA_GEOJSON.exists():
        write_lookup_csv(db_path=db_path)
    geojson, lookup = load_choropleth_assets()
    OUTPUT_HTML.write_text(
        dashboard_html(
            rows,
            generated_at=generated_at,
            db_path=db_path,
            geojson=geojson,
            lookup=lookup,
        ),
        encoding="utf-8",
    )
    return OUTPUT_HTML


def serve_dashboard(host: str, port: int) -> int:
    cmd = [
        sys.executable,
        "-m",
        "http.server",
        str(port),
        "--bind",
        host,
        "--directory",
        str(OUTPUT_DIR),
    ]
    print(f"Serving dashboard at http://{host}:{port}/")
    print(f"HTML: {OUTPUT_HTML}")
    try:
        return subprocess.run(cmd, check=False).returncode
    except KeyboardInterrupt:
        return 130


def main() -> int:
    args = parse_args()
    if args.skip_refresh:
        ensure_local_views(LOCAL_DB)
    else:
        refresh_local_db()

    rows = fetch_dashboard_rows(LOCAL_DB)
    if not rows:
        raise SystemExit("No rows found in authority_portal_download_progress_map")

    write_dashboard(rows, db_path=LOCAL_DB)
    if args.build_only:
        print(f"Built dashboard HTML at {OUTPUT_HTML}")
        return 0
    return serve_dashboard(args.host, args.port)


if __name__ == "__main__":
    raise SystemExit(main())
