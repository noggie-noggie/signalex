# Food API Contract

Frontend-facing contract for the Signalex Food API.

**Version:** 1.0  
**Date:** 2026-06-24

## Base URLs

| Environment | Base URL |
|---|---|
| Local development | `http://localhost:8000` |
| Production placeholder | `https://api.signalex.com.au` |

All requests and responses use JSON. Extra response properties may be added;
the frontend should read only the supported fields documented here.

## Shared types

### Paginated response

```json
{
  "total": 81,
  "limit": 20,
  "offset": 0,
  "results": []
}
```

Supported fields: `total`, `limit`, `offset`, `results`.

### Food signal

Objects returned in signal arrays support this frontend-safe subset:

```json
{
  "id": 2693,
  "domain": "food",
  "source_label": "food_fsanz_recalls",
  "signal_type": "recall",
  "event_type": "recall",
  "title": "Product recall title",
  "summary": "Reason and affected market.",
  "url": "https://source.example/item",
  "scraped_at": "2026-05-22T10:54:00+00:00",
  "severity": "low",
  "authority": "fsanz",
  "company": "Example Company",
  "brand": "Example Brand",
  "product_name": "Example Product",
  "ingredient_name": "",
  "allergen": "",
  "claim": "",
  "product_category": "food recall",
  "recommended_action": "Review the source notice."
}
```

Supported fields are the fields above. String fields may be empty. Treat
`signal_type` as the primary type; `event_type` is a compatibility alias.

Do not rely on undocumented fields, database column order, internal IDs other
than `id`, AI/classification metadata, sentiment fields, cache fields,
`digest_sent`, `created_at`, `is_noise`, or `noise_reason`.

## Endpoints

### `GET /api/health`

Purpose: check whether the API's Food data store and supporting datasets are
available.

Query parameters: none.

Example:

```http
GET http://localhost:8000/api/health
```

Response shape:

```json
{
  "status": "ok",
  "signals": {
    "exists": true,
    "readable": true,
    "total": 1490,
    "food": 81,
    "vms": 1409
  },
  "citations": {
    "exists": true,
    "loaded": true,
    "total": 2853,
    "sourceOfTruthFor": "pharma"
  },
  "warnings": [],
  "errors": []
}
```

Stable fields:

- `status`: `ok`, `warning`, or `error`
- `signals.exists`, `signals.readable`, `signals.food`
- `warnings[]`, `errors[]`

Avoid relying on `signals.path`, `citations.path`, or current record counts as
fixed values.

Empty/error behaviour:

- Healthy or warning state: HTTP 200.
- Missing or unreadable key data file: HTTP 503 with `status: "error"` and
  details in `errors`.

### `GET /api/food/dashboard`

Purpose: initial Food dashboard load. Returns overview counts and bounded recent
lists in one request.

Query parameters: none.

Example:

```http
GET http://localhost:8000/api/food/dashboard
```

Response shape:

```json
{
  "overview": {
    "signals": 81,
    "risks": 5,
    "newLaunches": 40,
    "ruleUpdates": 16
  },
  "recentSignals": [],
  "recalls": [],
  "ruleUpdates": [],
  "competitorProducts": [],
  "claimSignals": [],
  "ingredientTrends": [
    { "ingredient": "peanut", "count": 4 }
  ]
}
```

Stable fields:

- `overview.signals`, `overview.risks`, `overview.newLaunches`,
  `overview.ruleUpdates`
- All six arrays shown above
- Food signal subset for records inside the signal arrays
- `ingredientTrends[].ingredient`, `ingredientTrends[].count`

Avoid treating `newLaunches` as confirmed launch dates; product records indicate
items observed or updated in Open Food Facts.

Empty/error behaviour:

- No matching data: counts are zero and arrays are empty.
- Missing signal database: HTTP 200 with the same empty shape plus `note`.
- Unexpected query failure: HTTP 500.

### `GET /api/food/recalls`

Purpose: paginated FSANZ Food recall list.

Query parameters:

| Parameter | Type | Notes |
|---|---|---|
| `allergen` | string | Partial match |
| `severity` | string | Partial match; normally `high`, `medium`, or `low` |
| `company` | string | Partial match |
| `limit` | integer | Default 50; minimum 1; maximum 500 |
| `offset` | integer | Default 0; minimum 0 |

Example:

```http
GET http://localhost:8000/api/food/recalls?allergen=peanut&limit=20&offset=0
```

Response: paginated response containing Food signal objects.

Stable fields: pagination envelope plus the Food signal subset.

Avoid relying on undocumented signal properties or assuming `allergen` is
always populated; the recall reason may instead appear in `summary`.

Empty/error behaviour:

- No matches: HTTP 200 with `total: 0` and `results: []`.
- Invalid `limit` or `offset`: HTTP 422.
- Missing database: HTTP 200 empty response plus `note`.
- Unexpected failure: HTTP 500.

### `GET /api/food/rules`

Purpose: paginated FSANZ standards and regulatory updates.

Query parameters:

| Parameter | Type | Notes |
|---|---|---|
| `severity` | string | Partial match |
| `keyword` | string | Partial match against title or summary |
| `limit` | integer | Default 50; minimum 1; maximum 500 |
| `offset` | integer | Default 0; minimum 0 |

Example:

```http
GET http://localhost:8000/api/food/rules?keyword=allergen&limit=20
```

Response: paginated response containing Food signal objects.

Stable fields: pagination envelope plus the Food signal subset.

Avoid relying on internal classification or AI fields.

Empty/error behaviour is the same as `/api/food/recalls`.

### `GET /api/food/products`

Purpose: paginated product intelligence from Open Food Facts.

Query parameters:

| Parameter | Type | Notes |
|---|---|---|
| `brand` | string | Partial match |
| `ingredient` | string | Partial match against primary ingredient |
| `allergen` | string | Partial match |
| `claim` | string | Partial match against stored label claims |
| `limit` | integer | Default 50; minimum 1; maximum 500 |
| `offset` | integer | Default 0; minimum 0 |

Example:

```http
GET http://localhost:8000/api/food/products?brand=Musashi&limit=20
```

Response: paginated response containing Food signal objects.

Stable fields: pagination envelope plus the Food signal subset, especially
`brand`, `product_name`, `product_category`, `title`, `summary`, and `url`.

Avoid assuming every product has populated ingredient, allergen, or claim data.
Do not describe these records as confirmed recent launches.

Empty/error behaviour is the same as `/api/food/recalls`.

### `GET /api/signals?domain=food`

Purpose: generic paginated Food signal feed. Always include `domain=food`.

Query parameters:

| Parameter | Type | Notes |
|---|---|---|
| `domain` | string | Required by contract; use `food` |
| `source` | string | Partial match, e.g. `food_fsanz_recalls` |
| `severity` | string | Partial match |
| `sentiment` | string | Partial match; often empty for Food |
| `ingredient` | string | Partial match |
| `category` | string | Maps to event type, e.g. `recall`, `rule_update` |
| `limit` | integer | Default 50; minimum 1; maximum 500 |
| `offset` | integer | Default 0; minimum 0 |

Example:

```http
GET http://localhost:8000/api/signals?domain=food&category=recall&limit=20
```

Response: paginated response containing Food signal objects.

Stable fields: pagination envelope plus the Food signal subset.

Avoid omitting `domain=food`; without it the endpoint can return non-Food
records. Avoid relying on VMS-only flags or fields.

Empty/error behaviour is the same as `/api/food/recalls`.

### `POST /api/food/claims/guide`

Purpose: deterministic concept guidance for a proposed Food claim. It is not
regulatory approval or legal advice.

Request body:

```json
{
  "claim": "Supports gut health",
  "food_type": "yoghurt drink",
  "market": "Australia",
  "refresh": false
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `claim` | string | Yes | Proposed claim wording |
| `food_type` | string | Yes | Product format |
| `market` | string | No | Defaults to `Australia` |
| `refresh` | boolean | No | Defaults to `false`; bypasses cached guidance |

Example:

```http
POST http://localhost:8000/api/food/claims/guide
Content-Type: application/json

{"claim":"Supports gut health","food_type":"yoghurt drink","market":"Australia"}
```

Response shape:

```json
{
  "assessment_level": "concept_guidance",
  "cached": false,
  "input": {
    "claim": "Supports gut health",
    "food_type": "yoghurt drink",
    "market": "Australia"
  },
  "status": "needs_product_details",
  "confidence": "high",
  "theme": "gut_health",
  "claim_type": "health_claim",
  "risk_level": "medium",
  "risk_reasons": [],
  "summary": "Plain-language assessment.",
  "food_type_fit": "high",
  "claim_pathways": [
    {
      "name": "Probiotic route",
      "description": "Pathway summary.",
      "requirements": []
    }
  ],
  "possible_ingredients": [],
  "missing_information": [],
  "safer_wording": [],
  "avoid_wording": [],
  "reframe_suggestions": [],
  "competitor_examples": [],
  "related_rules": [],
  "related_evidence": [],
  "next_questions": [],
  "disclaimer": "Concept guidance only..."
}
```

Stable fields: every top-level field shown above; within `claim_pathways`, rely
on `name`, `description`, and `requirements`. Supporting-record arrays use the
common fields `id`, `title`, `summary`, `url`, `scraped_at`, `severity`,
`source_label`, and `domain`; product examples may also provide
`product_name`, `brand`, and `ingredient_name`.

Possible `status` values:

- `not_recommended`
- `high_risk_wording`
- `needs_nutrition_check`
- `needs_product_details`
- `needs_evidence_review`
- `low_risk_direction`

Avoid using `cached` as a quality indicator. Do not present `confidence` as
regulatory certainty, and do not depend on undocumented properties inside
supporting records.

Empty/error behaviour:

- Supporting arrays may be empty without making the assessment invalid.
- Missing or blank `claim` or `food_type`: HTTP 422 with a `detail` error.
- Malformed JSON or invalid field types: HTTP 422.
- Unexpected server failure: HTTP 500.

## CORS

`CORS_ORIGINS` is a comma-separated list of exact browser origins. Origins
contain scheme, hostname, and optional port, but no path.

Local development origins on ports 3000 and 5173 are already allowed. A local
setting may be:

```env
CORS_ORIGINS=http://localhost:5173
```

Production should contain only the deployed frontend origin or origins:

```env
CORS_ORIGINS=https://app.example.com,https://www.app.example.com
```

Restart the API after changing the environment variable. Do not use `*` for
production.

## Frontend integration

### Fetch helper

```js
const API_BASE =
  import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

async function apiFetch(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, options);
  const body = await response.json().catch(() => null);

  if (!response.ok) {
    const message =
      body?.detail ?? body?.errors?.join(", ") ?? `API error ${response.status}`;
    throw new Error(message);
  }

  return body;
}
```

Examples:

```js
const dashboard = await apiFetch("/api/food/dashboard");

const recalls = await apiFetch(
  "/api/food/recalls?allergen=peanut&limit=20&offset=0"
);

const guidance = await apiFetch("/api/food/claims/guide", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    claim: "Supports gut health",
    food_type: "yoghurt drink",
    market: "Australia"
  })
});
```

### Pagination and filters

- `limit` is page size; `offset` is the number of records skipped.
- Next offset: `offset + results.length`.
- More records exist when `offset + results.length < total`.
- Filters are partial text matches. URL-encode values with `URLSearchParams`.
- Debounce text searches to avoid a request on every keystroke.

### Loading, empty, and error states

- Show loading UI while a request is pending.
- Treat `results: []` or an empty dashboard array as a normal empty state.
- For HTTP 422, show validation guidance near the relevant form/filter.
- For HTTP 503, show that Food data is temporarily unavailable.
- For HTTP 500/network failures, show retry UI and retain the user's filters or
  claim form values.
- Optionally call `/api/health` for a status banner, not before every request.

### Page-to-endpoint mapping

| Frontend page | Primary endpoint |
|---|---|
| Food dashboard | `GET /api/food/dashboard` |
| Recalls list | `GET /api/food/recalls` |
| Rules/regulatory updates | `GET /api/food/rules` |
| Product intelligence | `GET /api/food/products` |
| Combined Food signal feed | `GET /api/signals?domain=food` |
| Claim guidance form | `POST /api/food/claims/guide` |
| Service/status banner | `GET /api/health` |
