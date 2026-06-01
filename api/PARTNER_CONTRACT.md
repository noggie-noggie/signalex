# Signalex API — Partner Integration Contract

This document is the authoritative reference for frontend engineers integrating
against the Signalex read-only API. It defines base URLs, endpoints, field
semantics, filtering, and current limitations.

**Version:** 0.2.0  
**Last updated:** 2026-05-23  
**Status:** Development — no auth, local only

---

## Base URLs

| Environment | Base URL |
|-------------|----------|
| Local development | `http://localhost:8000` |
| Production (placeholder) | `https://api.signalex.com.au` |

All paths below are relative to the base URL.

---

## Data model overview

The API serves two independent data sources:

| Source | Endpoint prefix | Backing store | Domains covered |
|--------|----------------|---------------|-----------------|
| Signals DB | `/api/signals`, `/api/food/*` | `data/signals.db` (SQLite) | `vms`, `food` |
| Citation DB | `/api/citations` | `reports/citation_database.json` (JSON, in-memory) | pharma |

### Domain values in `signals`

| `domain` | Meaning | Signal count |
|----------|---------|-------------|
| `vms` | Vitamins, minerals & supplements intelligence — scraped from TGA, FDA, PubMed, clinical trials, ARTG, EFSA, etc. | 1,117 |
| `food` | Food safety and product intelligence — FSANZ recalls, FSANZ regulatory updates, Open Food Facts products | 81 |
| `pharma` | Pharmaceutical regulatory intelligence | **0 in signals.db** — served via `/api/citations` instead (see below) |

> **Pharma note:** Pharma records live in `reports/citation_database.json` (2,853 citations).
> They are accessible via `/api/citations` and `/api/citations/{id}`.
> They are **not** in `signals.db` and will return `total: 0` from `/api/signals?domain=pharma`.
> A future migration will consolidate pharma into `signals.db`.

---

## Endpoints

### Health and metadata

#### `GET /api/health`
Quick liveness check. Returns signal and citation record counts.

```json
{
  "status": "ok",
  "signals": 1198,
  "citations": 2853
}
```

#### `GET /api/meta`
Dataset metadata for dashboard headers.

```json
{
  "lastUpdated": "2026-05-22T10:00:00+00:00",
  "signalCount": 1198,
  "citationCount": 2853,
  "sourceCount": 11
}
```

#### `GET /api/schema`
Field lists, filter parameter reference, and one sample record of each type.
Use this during integration to inspect live field names without reading source code.

---

### Signals (VMS + Food combined)

#### `GET /api/signals`

Paginated list of signals. Filters by `domain` to separate VMS from food.

**Query parameters:**

| Parameter | Maps to column | Example | Notes |
|-----------|----------------|---------|-------|
| `domain` | `domain` | `vms`, `food`, `pharma` | Primary domain filter |
| `source` | `source_label` | `pubmed`, `artg`, `food_fsanz_recalls` | Filter by scraper source |
| `severity` | `severity` | `high`, `medium`, `low` | Partial match |
| `sentiment` | `sentiment` | `positive`, `negative`, `neutral` | Partial match |
| `ingredient` | `ingredient_name` | `melatonin` | Partial match |
| `category` | `event_type` | `recall`, `safety_alert` | Partial match |
| `include_noise` | `is_noise` | `true`, `false` | Default **false** — rows with `is_noise=1` are hidden. Pass `true` to include them. |
| `include_low_quality_sources` | `source_label` | `true`, `false` | Default **false** — `biorxiv` and `europe_pmc` are hidden from `domain=vms` default views (see note below). Pass `true` to include them. |
| `limit` | — | `50` | Max 500, default 50 |
| `offset` | — | `0` | Pagination offset |

**VMS default visibility note**

Two sources are excluded from `domain=vms` responses by default:

| Source | Reason |
|--------|--------|
| `biorxiv` | Current stored dataset is 100% off-topic for VMS (preprints pulled without ingredient pre-filter). |
| `europe_pmc` | ~90% of stored rows are non-VMS molecular biology. Scraper query is being tightened. |

These sources are still in the database and accessible via:
- `?source=biorxiv` or `?source=europe_pmc` — always returns those rows regardless of other flags
- `?domain=vms&include_low_quality_sources=true` — includes them in the normal VMS result set

This flag has **no effect** on `domain=food` or other domains.

**Response envelope:**

```json
{
  "total": 1117,
  "limit": 50,
  "offset": 0,
  "results": [ /* signal objects */ ]
}
```

**VMS examples:**
```bash
# Default — clean dashboard-ready view (no noise, no low-quality sources)
curl "http://localhost:8000/api/signals?domain=vms&limit=10"

# Include noise rows (audit / debug)
curl "http://localhost:8000/api/signals?domain=vms&include_noise=true&limit=10"

# Include biorxiv and europe_pmc
curl "http://localhost:8000/api/signals?domain=vms&include_low_quality_sources=true&limit=10"

# Explicit source lookup — always returns that source (bypasses visibility defaults)
curl "http://localhost:8000/api/signals?source=biorxiv&limit=10"
curl "http://localhost:8000/api/signals?source=europe_pmc&limit=10"

curl "http://localhost:8000/api/signals?domain=vms&severity=high&limit=10"
curl "http://localhost:8000/api/signals?source=pubmed&ingredient=melatonin&limit=10"
```

**Food example:**
```bash
curl "http://localhost:8000/api/signals?domain=food&limit=10"
```

#### `GET /api/signals/summary`
Aggregate counts by `source_label`, `severity`, and `sentiment` across all signals.

---

### Citations (Pharma)

#### `GET /api/citations`

Paginated list of pharma citation intelligence records.

**Query parameters:**

| Parameter | Example | Notes |
|-----------|---------|-------|
| `authority` | `FDA`, `TGA` | Partial match |
| `category` | `GMP`, `labelling` | Partial match |
| `facility_type` | `manufacturer` | Partial match |
| `source_type` | `warning_letter` | Partial match |
| `severity` | `high` | Partial match |
| `company` | `Pfizer` | Partial match |
| `priority` | `high` | Partial match |
| `limit` | `50` | Max 500, default 50 |
| `offset` | `0` | Pagination offset |

```bash
curl "http://localhost:8000/api/citations?authority=FDA&severity=high&limit=10"
curl "http://localhost:8000/api/citations?category=GMP&limit=20"
```

#### `GET /api/citations/summary`
Aggregate counts by authority, category, severity, and facility type.

#### `GET /api/citations/{id}`
Single citation by id hash or zero-based list index.

```bash
curl "http://localhost:8000/api/citations/f4833c3a1521"
curl "http://localhost:8000/api/citations/0"
```

---

### Food domain endpoints

> **Recommended usage:** Call `/api/food/dashboard` for the initial food dashboard
> load — it returns all six data categories in a single request. Use the specific
> endpoints (`/api/food/recalls`, `/api/food/rules`, `/api/food/products`,
> `/api/food/signals`) for drilldown views, search, and pagination.

#### `GET /api/food/dashboard`

Single-request dashboard payload. Use this for the first load.

```bash
curl "http://localhost:8000/api/food/dashboard"
```

**Response structure:**

```json
{
  "overview": {
    "signals":     81,
    "risks":        5,
    "newLaunches": 30,
    "ruleUpdates": 16
  },
  "recentSignals":      [ /* last 20 food signals, any type */ ],
  "recalls":            [ /* FSANZ recall signals, up to 50 */ ],
  "ruleUpdates":        [ /* FSANZ regulatory update signals, up to 50 */ ],
  "competitorProducts": [ /* Open Food Facts product signals, up to 50 */ ],
  "claimSignals":       [ /* signals with non-empty claim field, up to 50 */ ],
  "ingredientTrends":   [ { "ingredient": "peanut", "count": 4 } ]
}
```

#### `GET /api/food/signals`

All food signals with optional filters.

| Parameter | Maps to | Notes |
|-----------|---------|-------|
| `severity` | `severity` | Partial match |
| `signal_type` | `signal_type` | `recall`, `rule_update`, `new_product`, `claim_signal` |
| `source` | `source_label` | `food_fsanz_recalls`, `food_fsanz_updates`, `open_food_facts` |
| `ingredient` | `ingredient_name` | Partial match |
| `allergen` | `allergen` | Partial match |
| `company` | `company` | Partial match |
| `limit` / `offset` | — | Pagination |

```bash
curl "http://localhost:8000/api/food/signals?severity=high&limit=20"
curl "http://localhost:8000/api/food/signals?allergen=peanut"
curl "http://localhost:8000/api/food/signals?signal_type=recall"
```

#### `GET /api/food/recalls`

FSANZ food recall signals only.

| Parameter | Notes |
|-----------|-------|
| `allergen` | Partial match on allergen field |
| `severity` | `high`, `medium`, `low` |
| `company` | Partial match on company name |
| `limit` / `offset` | Pagination |

```bash
curl "http://localhost:8000/api/food/recalls"
curl "http://localhost:8000/api/food/recalls?allergen=peanut"
curl "http://localhost:8000/api/food/recalls?severity=high"
```

#### `GET /api/food/rules`

FSANZ regulatory standards and update signals only.

| Parameter | Notes |
|-----------|-------|
| `severity` | `high`, `medium`, `low` |
| `keyword` | Partial match on title and summary |
| `limit` / `offset` | Pagination |

```bash
curl "http://localhost:8000/api/food/rules"
curl "http://localhost:8000/api/food/rules?keyword=allergen"
curl "http://localhost:8000/api/food/rules?keyword=maximum+level"
```

#### `GET /api/food/products`

Open Food Facts competitor product records only.

| Parameter | Notes |
|-----------|-------|
| `brand` | Partial match on brand field |
| `ingredient` | Partial match on ingredient_name |
| `allergen` | Partial match on allergen field |
| `claim` | Partial match on claim/label field |
| `limit` / `offset` | Pagination |

```bash
curl "http://localhost:8000/api/food/products"
curl "http://localhost:8000/api/food/products?brand=Musashi"
curl "http://localhost:8000/api/food/products?claim=organic"
curl "http://localhost:8000/api/food/products?allergen=milk"
```

#### `POST /api/food/claims/guide`

Deterministic food claim concept guidance. **v1 — no AI, no regulatory approval.**

Takes a claim string, food type, and market (default: Australia) and returns structured
guidance: risk classification, claim pathways, safer wording, wording to avoid,
competitor product examples, related FSANZ rules, and related VMS evidence.

**Request body (JSON):**

```json
{
  "claim":     "Supports gut health",
  "food_type": "yoghurt drink",
  "market":    "Australia",
  "refresh":   false
}
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `claim` | string | Yes | The claim text to assess |
| `food_type` | string | Yes | Product format, e.g. "yoghurt drink", "protein bar" |
| `market` | string | No | Default `"Australia"` |
| `refresh` | boolean | No | Default `false`. Pass `true` to bypass cache and regenerate. |

**Response fields:**

| Field | Type | Notes |
|-------|------|-------|
| `assessment_level` | string | Always `"concept_guidance"` in v1 |
| `cached` | boolean | `true` if response was served from cache |
| `input` | object | Echo of the request inputs |
| `status` | string | See status values table below |
| `confidence` | string | `"high"`, `"medium"`, or `"low"` — classifier confidence in the assessment |
| `theme` | string \| null | Detected claim theme. `"therapeutic_or_disease_claim"` if therapeutic terms detected. See themes below. |
| `claim_type` | string | `"health_claim"`, `"nutrient_content_claim"`, `"therapeutic / disease-related claim"`, or `"unknown"` |
| `risk_level` | string | `"low"`, `"medium"`, `"high"`, or `"unknown"` |
| `risk_reasons` | array | High-risk or therapeutic terms detected in the claim |
| `summary` | string | Plain-English summary of the risk assessment |
| `food_type_fit` | string | `"low"`, `"medium"`, `"high"`, or `"unsuitable"` — how well the food type suits the claim theme |
| `claim_pathways` | array | Regulatory pathways available for this theme and food type. Empty for therapeutic claims. |
| `possible_ingredients` | array | Ingredients that could support the claim |
| `missing_information` | array | Always populated. Product details needed before a full assessment. |
| `safer_wording` | array | Suggested lower-risk alternative claim wordings |
| `avoid_wording` | array | Wording that raises regulatory risk |
| `reframe_suggestions` | array | For therapeutic/disease claims: suggestions for reformulating to an acceptable direction. Empty for non-therapeutic claims. |
| `competitor_examples` | array | Open Food Facts product signals matching the claim/theme |
| `related_rules` | array | FSANZ regulatory update signals matching the claim/theme |
| `related_evidence` | array | VMS domain signals (scientific evidence) matching the claim/theme |
| `next_questions` | array | Questions to answer before proceeding |
| `disclaimer` | string | Fixed concept-guidance disclaimer |

**Status values:**

| `status` | Meaning |
|----------|---------|
| `not_recommended` | Therapeutic or disease claim detected. Not a valid food claim pathway. Reformulation required. |
| `high_risk_wording` | High-risk terms detected but not a full therapeutic claim. Wording needs review before use. |
| `needs_nutrition_check` | Nutrient content claim (low_sugar, high_protein) — requires nutrition panel verification. |
| `needs_product_details` | Medium-risk function claim. Formulation, ingredient levels, and evidence needed. |
| `needs_evidence_review` | Claim theme not recognised. More product detail or evidence context needed. |
| `low_risk_direction` | Lower-risk claim direction. Pathway and threshold checks recommended before use. |

**Confidence values:**

| `confidence` | Meaning |
|--------------|---------|
| `high` | Therapeutic terms clearly detected, or exact theme match with high food_type_fit |
| `medium` | Theme matched but food_type_fit is medium or low |
| `low` | No theme matched — claim may be too vague or novel |

**Therapeutic / disease claim behaviour:**

When `theme = "therapeutic_or_disease_claim"`:
- `status` is always `not_recommended`
- `food_type_fit` is always `"unsuitable"`
- `claim_pathways` is always `[]` (no food claim pathway exists)
- `reframe_suggestions` is populated with reformulation guidance
- `safer_wording` is populated with context-appropriate safer alternatives (gut-specific wording for IBS/digestive claims, etc.)
- `missing_information` is still fully populated — product context is needed even for reformulation guidance
- `confidence` is `"high"` — the classifier is certain this is a therapeutic claim

High-risk terms that trigger this classification include: `treat`, `treats`, `treating`, `cure`, `cures`, `prevent`, `prevents`, `heal`, `heals`, `repair`, `repairs`, `reduce inflammation`, `reduces inflammation`, `anti-inflammatory`, `ibs`, `arthritis`, `anxiety`, `depression`, `disease`, `infection`, `antiviral`.

**Examples:**

```bash
# Gut health claim on a yoghurt drink
curl -X POST "http://localhost:8000/api/food/claims/guide" \
  -H "Content-Type: application/json" \
  -d '{"claim":"Supports gut health","food_type":"yoghurt drink","market":"Australia"}'

# Muscle recovery claim on a protein bar
curl -X POST "http://localhost:8000/api/food/claims/guide" \
  -H "Content-Type: application/json" \
  -d '{"claim":"Supports muscle recovery","food_type":"protein bar","market":"Australia"}'

# High-risk therapeutic claim
curl -X POST "http://localhost:8000/api/food/claims/guide" \
  -H "Content-Type: application/json" \
  -d '{"claim":"Treats IBS","food_type":"yoghurt drink","market":"Australia"}'

# Force regeneration (bypass cache)
curl -X POST "http://localhost:8000/api/food/claims/guide" \
  -H "Content-Type: application/json" \
  -d '{"claim":"Supports gut health","food_type":"yoghurt drink","refresh":true}'
```

**Important limitations:**

- v1 is **fully deterministic** — no AI, no LLM, no Claude API calls.
- Results are **concept guidance only**. They are not regulatory advice or approval.
- Final claim acceptability depends on formulation, ingredient levels, serving size, nutrition panel, exact wording, evidence, and regulatory review.
- Repeated identical requests return `cached: true` unless `refresh: true` is passed. Cache keys are versioned — a server-side version bump automatically invalidates prior cached responses without requiring manual cache clearing.
- The guidance covers 10 initial themes: `gut_health`, `immunity`, `energy`, `muscle_recovery`, `hydration`, `bone_health`, `antioxidant`, `heart_health`, `low_sugar`, `high_protein`. Claims with therapeutic/disease terms return `theme: "therapeutic_or_disease_claim"` regardless of other keyword matches.
- Claims not matching any theme or therapeutic term return `theme: null`, `risk_level: "medium"`, and `status: "needs_evidence_review"` as a conservative default.

---

### Ingredients

#### `GET /api/ingredients`

Top 200 ingredients by signal count with sentiment breakdown.
Covers all domains (VMS + food combined).

```bash
curl "http://localhost:8000/api/ingredients"
```

**Response:**
```json
[
  {
    "ingredient": "melatonin",
    "count": 14,
    "sentiment_breakdown": { "positive": 8, "neutral": 4, "negative": 2 }
  }
]
```

---

## Signal field definitions

These fields appear on every object in `results` arrays from `/api/signals` and
all `/api/food/*` endpoints.

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer | Auto-increment primary key |
| `source_id` | string | SHA-256 hash of (source + url + title). Stable dedup key. |
| `domain` | string | `vms`, `food`, or `pharma`. Primary domain classifier. |
| `source_label` | string | Scraper source identifier. See source labels below. |
| `event_type` | string | Signal classification. See event types below. |
| `signal_type` | string | Alias for event_type in food context. Same column. |
| `severity` | string | `high`, `medium`, `low`, or empty. Heuristic or classifier-assigned. |
| `title` | string | Title of the source record or page. |
| `summary` | string | Classifier-generated or scraped summary. |
| `url` | string | Canonical source URL. |
| `authority` | string | Issuing health authority (`tga`, `fda`, `fsanz`, etc.). |
| `scraped_at` | string | ISO 8601 UTC timestamp of when the record was scraped. |
| `ingredient_name` | string | Primary ingredient identified by classifier. |
| `product_category` | string | Broad product category (supplement, medicine, food, etc.). |
| `sentiment` | string | `positive`, `neutral`, `negative`, or empty. |
| `recommended_action` | string | Classifier or scraper suggested response action. |
| `why_it_matters` | string | Classifier business-impact narrative. |
| `ai_summary` | string | Extended AI-generated business summary. |
| `allergen` | string | *(food domain)* Comma-separated allergens present or declared. |
| `brand` | string | *(food domain)* Brand name of the product. |
| `product_name` | string | *(food domain)* Full product name. |
| `claim` | string | *(food domain)* Health or label claims (e.g. organic, gluten-free). |
| `company` | string | *(food domain)* Manufacturer or recalling company. |
| `is_noise` | integer | `1` if classifier flagged as not VMS-relevant. |

### Source label values

| `source_label` | Domain | Source |
|----------------|--------|--------|
| `pubmed` | vms | PubMed scientific literature |
| `clinical_trials` | vms | ClinicalTrials.gov |
| `europe_pmc` | vms | Europe PMC literature |
| `cochrane` | vms | Cochrane systematic reviews |
| `biorxiv` | vms | bioRxiv preprints |
| `semantic_scholar` | vms | Semantic Scholar |
| `artg` | vms | TGA ARTG product listings |
| `tga_consultations` | vms | TGA regulatory consultations |
| `adverse_events` | vms | FDA CAERS + TGA DAEN adverse events |
| `efsa` | vms | EFSA journal publications |
| `food_fsanz_recalls` | food | FSANZ food recall alerts |
| `food_fsanz_updates` | food | FSANZ regulatory standards updates |
| `open_food_facts` | food | Open Food Facts product database |

### Event type values

| `event_type` | Typical domain | Meaning |
|--------------|----------------|---------|
| `recall` | vms, food | Product recall notice |
| `safety_alert` | vms | Regulatory safety advisory |
| `warning` | vms | Formal warning letter or notice |
| `new_listing` | vms | New ARTG product registration |
| `rule_update` | food | FSANZ standards or regulatory change |
| `new_product` | food | New product record from Open Food Facts |
| `claim_signal` | food | Product with health or label claims |
| `other` | vms | Classified signal without a specific type |

---

## Recommended frontend integration patterns

### Initial dashboard load

```
GET /api/food/dashboard          → food overview + all six lists in one call
GET /api/signals?domain=vms&limit=20  → VMS recent signals
GET /api/citations/summary       → pharma authority/category breakdown
GET /api/meta                    → header counts (signals, citations, last updated)
```

### Drilldown / search

```
GET /api/food/recalls?allergen=peanut          → filtered recall list
GET /api/food/products?brand=Musashi           → brand search
GET /api/food/rules?keyword=maximum+level      → standards keyword search
GET /api/signals?domain=vms&source=pubmed&ingredient=omega-3   → ingredient evidence
GET /api/citations?authority=FDA&severity=high → pharma enforcement drilldown
```

### Pagination

All list endpoints support `limit` (max 500) and `offset`.

```
GET /api/signals?domain=vms&limit=50&offset=0    # page 1
GET /api/signals?domain=vms&limit=50&offset=50   # page 2
```

---

## Known limitations

### No authentication
All endpoints are public with no API key or token requirement. Do not expose
the API on a public network until an auth layer is added (API key header, JWT,
or IP allowlist at the reverse-proxy level).

### CORS open in development
`allow_origins=["*"]` is set in `api/server.py` for local development convenience.
This must be restricted to specific origins before production deployment.

### Pharma not yet in signals.db
`/api/signals?domain=pharma` always returns `total: 0`. Pharma intelligence is
served exclusively from `/api/citations` (backed by `reports/citation_database.json`).
A future migration will consolidate pharma into `signals.db` so that all domains
can be queried uniformly. Until then, the frontend must query both endpoints
separately for a complete picture.

### Open Food Facts products are not confirmed new launches
Records from `source_label=open_food_facts` with `event_type=new_product` reflect
products that exist in the Open Food Facts database and were recently modified —
not necessarily products that launched this week. A `first_seen` column would be
needed to distinguish genuine new launches from routine data updates.

### Food summaries may contain raw HTML
Some `food_fsanz_updates` records ingested before the HTML cleanup migration
may have raw HTML fragments in the `summary` field. Run
`python migrations/clean_food_fsanz_html.py` to clean existing rows. New
records from `food_fsanz_updates` are cleaned at ingestion time.

### Signal counts grow on each pipeline run
`INSERT OR IGNORE` deduplication is by `source_id` (URL + title hash). If a
source changes a URL or title, a new row is inserted. Counts are not stable
across pipeline runs.

### No real-time updates
The API is read-only and does not push updates. Poll `/api/meta` to detect
when `lastUpdated` changes, then refresh cached data.

---

## Running the pipeline manually

```bash
# VMS + science sources
python main.py --pipeline

# Food domain only
python -m scheduler.food_pipeline

# First-time DB setup (idempotent)
python migrations/backfill_vms_domain.py
```
