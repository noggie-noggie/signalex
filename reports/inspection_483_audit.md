# Signalex Pharma — FDA 483 / Inspection Audit Report

**Date:** 2026-05-04  
**Auditor:** Signalex internal audit  
**No AI calls made.**

---

## Executive Summary

The "483 / Inspection" count showing **0** in the dashboard is **accurate for the current dataset**. Zero records with `source_type === "inspection_finding"` exist in `citation_database.json`. This is a **data pipeline gap**, not a UI bug.

The static pill count of **62** visible in the sidebar HTML is stale — it originated from a prior build in which EFSA scientific opinion records were mislabelled as `inspection_finding` before `fix_source_types()` correction was applied. Those records no longer exist in the current database.

---

## 1. Citation Data Counts

**File:** `reports/citation_database.json` (8,864,221 bytes, **2,853 records**)  
`data/citation_database.json` is identical in structure.

### By source_type

| source_type | count |
|---|---|
| device_enforcement | 1,000 |
| warning_letter | 725 |
| drug_enforcement | 705 |
| scientific_opinion | 257 |
| food_enforcement | 78 |
| import_alert | 40 |
| compliance_action | 25 |
| safety_alert | 10 |
| guidance | 7 |
| recall | 6 |
| **inspection_finding** | **0** |

### By authority (top 6)

| authority | count |
|---|---|
| FDA | ~2,200 |
| EFSA | ~400 |
| TGA | ~150 |
| MHRA | ~80 |
| BfR | ~20 |
| Other | ~3 |

*(Exact counts in `inspection_483_audit.json`)*

---

## 2. 483/Inspection Term Search Results

**260 records** across all citation fields match 483/inspection-related terms.

**None have `source_type === "inspection_finding"`.**

All 260 are classified as:

| source_type | matched count | reason |
|---|---|---|
| warning_letter | ~205 | FDA warning letters reference "Form FDA 483" in enriched_text — these are enforcement letters that cite 483 observations as context |
| drug_enforcement | ~50 | Recall notices triggered by 483 observations appear in raw_listing_summary/violation_details |
| compliance_action | ~3 | MHRA records where "EIR" appears in summary (Establishment Inspection Report coincidence) |
| safety_alert | ~2 | TGA records with incidental "EIR" term |

**Key finding:** FDA warning letters correctly cite 483 observations as their basis, but those letters are appropriately filed as `warning_letter`, not `inspection_finding`. A standalone 483 observation record would need to come from a dedicated FDA 483 database scrape — which does not currently exist in the pipeline.

---

## 3. Scraper Coverage

### inspection_finding source_type origin

**Scraper A — `citation_report.py` `scrape_fda_483()` (lines 752–814)**

- Target URL: `https://www.fda.gov/food/compliance-enforcement-food/fda-food-inspection-observation-detail-report`
- Method: BeautifulSoup HTML table parse
- Assigns: `source_type = "inspection_finding"`, `authority = "FDA"`
- **Status: NOT wired into the main pipeline.** This function exists in `citation_report.py` but is never called by `citation_fetcher.py`'s `run_full_pipeline()`. Zero FDA 483 records are produced.
- Additional concern: The target URL is an FDA food-specific inspection page. A proper pharma 483 scrape would target `https://www.accessdata.fda.gov/scripts/iceci/inspections/irei/index.cfm` (FDA Inspection Classification Database).

**Scraper B — `citation_fetcher.py` MHRA scraper**

- Target: `gov.uk/government/publications?keywords=gmp+non-compliance`
- Initially assigns `source_type = "inspection_finding"` to MHRA findings
- **`fix_source_types()` (lines 2317–2364) corrects mislabels:**
  - EFSA/BfR records → `scientific_opinion`
  - MHRA `gov.uk/guidance/*` URLs → `guidance`
  - After correction, all MHRA records that survive as `inspection_finding` appear to be zero in the current build

**Conclusion:** No genuine FDA Form 483 observation records are being scraped. `inspection_finding` count = 0 is accurate.

---

## 4. UI Mapping

### Sidebar pill (signals.html line 157)

```html
<div class="pf-pill" data-pf="srctype" data-val="inspection_finding">
  <span class="filter-label">483 / Inspection</span>
  <span class="pf-ct">62</span>  ← STALE STATIC HTML
</div>
```

- The `62` was baked at build time from a prior dataset containing mislabelled EFSA records
- `syncPharmaSidebarCounts()` dynamically updates `.pf-ct` after `loadPharmaCitations()` completes — so the pill correctly shows **0** at runtime after JS runs
- The label "483 / Inspection" implies standalone FDA 483 records exist — **misleading**

### KPI card pk-483 (signals.html lines 461–463)

```html
<div id="pk-483" ...>
  <div class="kpi-label">483 Observations</div>
  <div class="kpi-val">62</div>  ← updated to 0 at runtime by _setKpi()
  <div class="kpi-sub">EFSA + FDA findings</div>  ← wrong/stale
</div>
```

- Runtime JS correctly shows **0** via `_setKpi('pk-483', insp)`
- Static label "EFSA + FDA findings" is incorrect — EFSA records were mislabels, now corrected

---

## 5. Fixes Applied

### A. Sidebar pill — label corrected + dynamic zero-hide

- Renamed: `"483 / Inspection"` → `"Inspection Findings"`
- Initial hardcoded count: `62` → `0`
- `syncPharmaSidebarCounts()` now hides srctype pills with count 0 (`el.style.display = n > 0 ? '' : 'none'`)
- Effect: pill is hidden at runtime since 0 `inspection_finding` records exist

### B. KPI pk-483 — label and subtext corrected

- Renamed: `"483 Observations"` → `"Inspection Findings"`
- Subtext: `"EFSA + FDA findings"` → dynamically set to `"No records in current dataset"` (or count when records exist)
- Title tooltip updated to explain what the field covers and that no standalone 483 scraper is currently active

### C. KPI pk-high — P1 group vs raw count separated

- Renamed: `"High Priority Actions"` → `"P1 Action Groups"`
- KPI value: cluster-primary P1 records (deduplicated action groups)
- Subtext: dynamically shows raw P1 citation count
- Added `id="pk-high-sub"` to the subtext element for dynamic updates

---

## 6. P1 Count Analysis

| Metric | Count | Source |
|---|---|---|
| Raw P1 citations (full database) | **84** | `getPharmaCitations()` — used by sidebar Priority filter |
| Deduplicated P1 records (after `normalisePharmaCitationKey`) | ~70–80 | `filteredCits()` with no active filters |
| P1 cluster-primary records (action groups) | **32** | `cluster_primary !== false` within deduplicated set |

**Before:** KPI "High Priority Actions" showed raw deduplicated P1 count — inconsistent with sidebar showing raw count.

**After:** KPI "P1 Action Groups" shows **32** (grouped action groups); subtext shows **"84 raw P1 citations"**. Sidebar filter still shows 84 (raw, matches pill click behaviour).

---

## 7. Recommended Source Model (for future scraper work)

Use these canonical `source_type` values:

| source_type | description |
|---|---|
| `warning_letter` | FDA Warning Letters, MHRA Warning Letters |
| `inspection_finding` | Standalone FDA Form 483 observations, MHRA inspection findings |
| `drug_enforcement` | FDA drug recalls and enforcement actions |
| `device_enforcement` | FDA device recalls and enforcement actions |
| `import_alert` | FDA Import Alerts (DWPE) |
| `recall` | General product recalls |
| `compliance_action` | MHRA/TGA compliance/licensing actions |
| `guidance` | Regulatory guidance documents |
| `scientific_opinion` | EFSA/BfR scientific opinions |

**Do not** treat FDA 483 observations as `warning_letter`. A proper FDA 483 scrape should target:
- FDA Inspection Classification Database: `https://www.accessdata.fda.gov/scripts/iceci/inspections/irei/index.cfm`
- FDA 483 Observation Database (where available per FOIA)

---

## 8. Validation Summary

| Question | Answer |
|---|---|
| Is 483/Inspection = 0 accurate? | **Yes** — zero `inspection_finding` records in current dataset |
| Are there 483-like records hiding under another source_type? | **Yes** — 260 records reference 483 terms in body text (warning letters citing 483 observations as basis), correctly filed as `warning_letter` or `drug_enforcement` |
| Is the UI label misleading? | **Yes (was)** — "483 / Inspection" implied standalone 483 records; now corrected to "Inspection Findings" and hidden when count = 0 |
| Is the static count of 62 misleading? | **Yes** — stale from prior mislabelled EFSA records, now dynamically updated and hidden |
| What exact fix was applied? | Sidebar pill renamed + hidden when 0; KPI relabelled with correct tooltip; P1 KPI split into groups vs raw |
| Were any AI calls made? | **No** |

---

## Files Inspected

- `reports/citation_database.json` — primary citation source
- `data/citation_database.json` — secondary/mirror (identical structure)
- `reports/citation_fetcher.py` — main scraper pipeline + `fix_source_types()`
- `reports/citation_report.py` — contains unwired `scrape_fda_483()` (lines 752–814)
- `signals.html` — sidebar pill, KPI cards, `syncPharmaSidebarCounts()`
- `pharma.js` — `renderPharmaOverview()` KPI counters
- `core.js` — `_ENFORCEMENT_SOURCE_TYPES` definition
- `reports/inspection_483_audit.json` — machine-readable audit output (2,853 total, 260 matched)
