# Signalex Read-Only API

FastAPI layer exposing regulatory intelligence data from:
- `reports/citation_database.json` — loaded into memory at startup
- `data/signals.db` — SQLite, new read-only connection per request

**No auth yet.** Read endpoints use read-only database connections. The food
claim guidance endpoint writes only to its SQLite response cache.

---

## Database setup

Before starting the API on a fresh clone, apply the domain backfill migration
so that `domain` filtering works correctly:

```bash
python migrations/backfill_vms_domain.py
```

This is safe to re-run — already-tagged rows are never modified.  
The script only updates blank-domain rows matched by conservative VMS source
or authority rules. Unmatched rows are reported and left untouched.

---

## Start locally

```bash
# From the repo root:
uvicorn api.server:app --reload --port 8000
```

Interactive docs: http://localhost:8000/docs  
OpenAPI JSON: http://localhost:8000/openapi.json

---

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Signal + citation counts, server status |
| GET | `/api/meta` | Dataset metadata (last updated, record counts) |
| GET | `/api/schema` | Field lists, filter params, sample records |
| GET | `/api/citations` | List citations with optional filters |
| GET | `/api/citations/summary` | Aggregate counts by authority/category/severity |
| GET | `/api/citations/{id}` | Single citation by id hash or list index |
| GET | `/api/signals` | List signals with optional filters |
| GET | `/api/signals/summary` | Aggregate counts by source/severity/sentiment |
| GET | `/api/ingredients` | Top ingredients by signal count + sentiment |

---

## Query parameters

### `/api/citations`
| Param | Type | Description |
|-------|------|-------------|
| `authority` | string | Partial match on issuing authority (e.g. `FDA`, `TGA`) |
| `category` | string | Partial match on citation category |
| `facility_type` | string | Partial match on facility type |
| `source_type` | string | Partial match on source type |
| `severity` | string | Partial match on severity |
| `company` | string | Partial match on company name |
| `priority` | string | Partial match on priority field |
| `limit` | int | Max results (default 50, max 500) |
| `offset` | int | Pagination offset (default 0) |

All string filters are case-insensitive partial matches.

### `/api/signals`
| Param | Type | Description |
|-------|------|-------------|
| `domain` | string | Maps to `source_label` column |
| `source` | string | Maps to `source_label` column |
| `severity` | string | Direct match on `severity` |
| `sentiment` | string | Direct match on `sentiment` |
| `ingredient` | string | Maps to `ingredient_name` column |
| `category` | string | Maps to `event_type` column |
| `limit` | int | Max results (default 50, max 500) |
| `offset` | int | Pagination offset (default 0) |

---

## curl examples

```bash
# Health check
curl http://localhost:8000/api/health

# Dataset metadata
curl http://localhost:8000/api/meta

# Field lists + sample records
curl http://localhost:8000/api/schema

# Citations from FDA, high severity
curl "http://localhost:8000/api/citations?authority=FDA&severity=high&limit=10"

# Citation summary breakdown
curl http://localhost:8000/api/citations/summary

# Single citation by id
curl http://localhost:8000/api/citations/f4833c3a1521

# Signals from PubMed
curl "http://localhost:8000/api/signals?source=pubmed&limit=20"

# Signals summary
curl http://localhost:8000/api/signals/summary

# Top ingredients by signal count
curl http://localhost:8000/api/ingredients
```

---

## Pagination

All list endpoints support `limit` and `offset`:

```bash
# Page 1 (items 0–49)
curl "http://localhost:8000/api/citations?limit=50&offset=0"

# Page 2 (items 50–99)
curl "http://localhost:8000/api/citations?limit=50&offset=50"
```

Response envelope:
```json
{
  "total": 2853,
  "limit": 50,
  "offset": 0,
  "results": [...]
}
```

---

## CORS

CORS permits GET, POST, and OPTIONS. Local development origins on ports 3000
and 5173 are always allowed. Add external frontend origins through the
comma-separated `CORS_ORIGINS` environment variable:

```bash
CORS_ORIGINS=https://frontend.example.com,https://www.frontend.example.com
```

The API does not default to a wildcard origin.

## Domain storage

- Food and VMS records are stored in `data/signals.db`.
- New classified signals default to `domain=vms`.
- Pharma remains in `reports/citation_database.json` and is served by
  `/api/citations`.
- `/api/signals?domain=pharma` is not the Pharma source of truth and normally
  returns zero records.

---

## Authentication

There is no authentication or API key enforcement yet.

**Before public deployment**, add one of:
- API key header validation (FastAPI `Security` dependency)
- OAuth2 / JWT middleware
- IP allowlist at the reverse-proxy level (nginx/Caddy)

The API is read-only and contains no PII, but enforcement citation data may be commercially sensitive.

---

## Architecture notes

- Citations JSON is loaded **once at startup** into memory (`_citations` list). To reload after a data update, restart the server.
- SQLite connections are **opened and closed per request** using a read-only URI (`?mode=ro`). No connection pool needed for the expected request volume.
- Signal table column names are **cached after the first request** (`_SIGNALS_COLUMNS`). Unknown filter params are silently ignored — no crash.
- All SQL uses **parameterised queries** only. No user input is interpolated into SQL strings.
