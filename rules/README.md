# `rules/` — classification rules used by the decision-patterns report

Every judgment call made by `scripts/analyse_decision_patterns.py` lives in
this folder. The script is a thin engine: it loads these files, applies them
to the planning-applications database, and emits the report.

If you want to argue with a finding in
`reports/heat-pump-decisions/index.html`, this is the folder to look at.

The "How rules fire" section of the report shows how many applications each
rule caught on the latest run, so you can see the *consequences* of every
rule before changing it.

## File index

| File | What it controls | Format |
| --- | --- | --- |
| `decision_rules.toml` | How the council's free-text decision string maps to `approved` / `refused` / `withdrawn` / `discharge` / `split` / `pending` / `other`. | TOML, ordered list of `[[rule]]` entries. First match wins. |
| `app_type_rules.toml` | Fallback regex rules for application type when PlanIt's structured field is blank. Most rows skip this — the structured field is used directly. | TOML, ordered list of `[[rule]]` entries. First match wins. Default = `Full`. |
| `edge_case_keywords.toml` | Per-app boolean flags (`is_listed`, `is_flat`, `is_wind_turbine`, `mentions_noise`). | TOML, one table per flag with `[[match]]` clauses. Independent flags. |
| `postcode_to_region.csv` | Postcode area → UK region (e.g. `SW` → `London`). | CSV, two columns. |
| `authority_region_fallback.csv` | Manual override for councils whose apps never carry a postcode. | CSV, two columns. Used only after postcode and modal-region lookups fail. |
| `mcs_lpa_overrides.csv` | LPA name → MCS LA name, for post-2023 unitary reorgs (e.g. Allerdale → Cumberland). Powers the "Share needing planning, by council" join. | CSV, two columns. |
| `mcs_authority_overrides.csv` | Direct DB-authority → MCS LA fallback for cases the LPA lookup can't match. | CSV, two columns. |

## How to change a rule

1. Edit the relevant file. Each TOML rule has a `why` field — please update it
   if your change shifts the rationale.
2. Re-run `python3 scripts/analyse_decision_patterns.py`.
3. Open `reports/heat-pump-decisions/index.html` and check:
   - **How rules fire** appendix — did the row count for the rule you touched
     change as you expected?
   - **Headline KPI tiles** — did totals shift by more or less than you intended?
   - **`decision_mapping.csv`** (alongside the report) — for decision-rule
     changes, this lists each raw decision string and the bucket it landed in,
     sorted by frequency. The right place to spot mis-classifications.

## Conventions

- **Order matters in `[[rule]]` lists.** `split` runs before `refused` so
  "Approved in part, refused in part" doesn't get bucketed as a refusal. The
  `why` field on each rule says what the priority is guarding against.
- **Patterns are case-insensitive** (`re.I` is applied at load time).
- **`'`-quoted strings in TOML are raw** — don't double-escape backslashes.
  Use `'\b\w+\b'` not `"\\b\\w+\\b"`.
- **`fields = ["app_type_lower", ...]`** in edge-case rules: `app_type_lower`
  is the canonical app-type bucket, lowercased. `description` is the
  application's free-text description. No other fields are exposed.
