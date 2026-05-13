# Refusal-Reason Text Analysis

What planners actually cite when they refuse heat pump applications, extracted
from decision notices and officer reports.

## Pipeline

1. `scripts/extract_refusal_text.py` — pulls decision-type docs for refused
   apps from gdrive via rclone, runs `pdftotext` on each.
2. `scripts/cluster_refusal_reasons.py` — finds the formal "Reasons for
   refusal" section in each doc, tags it against
   `rules/refusal_reason_keywords.toml`.

A single refusal almost always cites multiple grounds (2-4 typical), so each
app can carry multiple labels.

## Corpus coverage

- 1,592 refused apps on VPS `ashp.db`
- 912 have at least one downloaded decision/report/officer-type doc (57%)
- 882 extracted to plain text (96% of those)
- 496 have a formally-headed "Reasons for refusal" section (56% of extracted)
- 216 also explicitly name the heat pump within that section — the **"clean
  cohort"** where the refusal is least likely to be about a co-applied
  extension, roof alteration, etc.

The 680 refused apps with no downloaded docs are blocked portals (WAF, CAPTCHA,
Lightning Aura) or not-yet-attempted — same population as the wider download
gaps documented in FINDINGS.md §25.

## Headline result — clean cohort (n=216)

| Refusal ground | % of refusals |
|---|---:|
| Noise | 60% |
| Neighbour amenity (often paired with noise) | 54% |
| Design / scale / siting | 51% |
| Visual amenity / character | 43% |
| Heritage – conservation area | 29% |
| Heritage – listed building | 23% |
| Flat / apartment context | 13% |
| Permitted Development non-compliance | 13% |
| Biodiversity / flood / greenbelt / highways / trees | <7% each |

`policy_general` (citation of NPPF / Local Plan policy numbers) fires on 82% of
apps — it's a marker of officer language, not a substantive ground.

## Two refusal patterns dominate, and they co-occur

| Pair | Apps |
|---|---:|
| neighbour_amenity + noise | 102 |
| design_scale_siting + visual_amenity | 79 |
| design_scale_siting + noise | 63 |
| heritage_conservation_area + visual_amenity | 51 |
| heritage_conservation_area + heritage_listed | 42 |

**Pattern A — "it'll be too noisy":** noise + neighbour amenity (~47% of clean
cohort).

**Pattern B — "it'll look wrong":** design/siting + visual amenity (~37%).
Heritage is a *modifier* that escalates B rather than a standalone driver
(conservation area + visual amenity co-occur ~24%).

Exotic concerns (highways, biodiversity, flooding, trees) are real but
collectively under 1-in-6 refusals.

## Implications for the "invisible" no-apply population

Refusal grounds map almost entirely to things a prospective applicant *can
screen for themselves before applying*:

- "Is my house in a conservation area or listed?" → public registers
- "Can I site the unit ≥1m from boundary, away from the street?" → PD rule + MCS 020
- "Will noise at the neighbour boundary stay under 42 dB(A)?" → MCS 020 calc

This is consistent with the 93.5% approval rate among applicants who proceed:
the people who *do* apply have already self-screened past these issues, and
the ones who can't screen past them simply don't apply.

**Policy read:** MCS 020 (heat pump planning standard) targets exactly the
issues that drive refusal — siting and noise. The largest planning lever
isn't refusal-rate reduction; it's making the self-screen easier and PD
coverage broader.

## Files

- `per_app_refusal_labels.csv` — one row per app: labels assigned, doc count,
  flags for "used focused refusal section" and "refusal text mentions heat
  pump".
- `label_tally.csv` — corpus-wide label counts.
- `label_pairs.csv` — top 50 co-occurring label pairs.

## Known limits

- 30 apps have docs but they're TIF/RTF/DOC — pdftotext can't read those.
  Adding `libreoffice --headless --convert-to txt` would recover most.
- The 680 refused apps with no downloaded docs are systematically blocked
  portal types, so this analysis is biased toward councils with scrape-friendly
  portals (Idox, Northgate). Heavily-blocked councils (Salesforce-based,
  WAF-protected) are underrepresented.
- Regex-based clustering will miss novel phrasings. A spot-check of ~10
  matches per label is recommended before quoting numbers externally.
- Co-applied applications (heat pump + extension): the "mentions_heat_pump"
  filter helps but doesn't fully isolate cases where the refusal grounds are
  about the *extension*, not the heat pump. ~40% of refusals across the full
  corpus look like this based on app-type breakdown.
