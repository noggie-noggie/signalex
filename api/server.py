"""
api/server.py — Read-only FastAPI layer for Signalex regulatory intelligence data.

Data sources (read-only):
  - reports/citation_database.json  → loaded once at startup, kept in memory
  - data/signals.db                 → new SQLite connection per request (read-only URI)

Does NOT import config.py, scheduler, scrapers, classifier, or analytics modules.
Does NOT write to any database or file.

Run:
    uvicorn api.server:app --reload --port 8000

Endpoints:
    GET /api/health
    GET /api/meta
    GET /api/citations              ?authority=&category=&facility_type=&source_type=&severity=&company=&priority=&limit=50&offset=0
    GET /api/citations/summary
    GET /api/citations/{id}
    GET /api/signals                ?domain=&source=&severity=&sentiment=&ingredient=&category=&limit=50&offset=0
    GET /api/signals/summary
    GET /api/ingredients
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# ---------------------------------------------------------------------------
# Paths — resolved relative to the repo root (one level above this file)
# ---------------------------------------------------------------------------
_ROOT            = Path(__file__).parent.parent   # ~/vms-intel
_SIGNALS_DB      = _ROOT / "data" / "signals.db"
_CITATIONS_JSON  = _ROOT / "reports" / "citation_database.json"

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Signalex Read-Only API",
    description="Read-only regulatory intelligence endpoints. No auth yet.",
    version="0.1.0",
)

# CORS — currently open for local development.
# TODO: Before production deployment, restrict allow_origins to specific domains, e.g.:
#   allow_origins=["https://your-signalex-domain.com"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # <-- restrict in production
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Citations — loaded once at startup, held in memory
# ---------------------------------------------------------------------------
_citations: list[dict] = []
_citations_meta: dict  = {}


@app.on_event("startup")
def _load_citations() -> None:
    """Load citation_database.json into memory at startup."""
    global _citations, _citations_meta
    if not _CITATIONS_JSON.exists():
        _citations      = []
        _citations_meta = {"error": f"File not found: {_CITATIONS_JSON}"}
        return
    try:
        raw             = json.loads(_CITATIONS_JSON.read_text(encoding="utf-8"))
        _citations      = raw.get("citations", [])
        _citations_meta = {k: v for k, v in raw.items() if k != "citations"}
    except Exception as exc:
        _citations      = []
        _citations_meta = {"error": str(exc)}


# ---------------------------------------------------------------------------
# SQLite helpers — read-only, new connection per request
# ---------------------------------------------------------------------------

# Cached set of column names in the signals table; populated on first use.
_SIGNALS_COLUMNS: set[str] = set()


def _get_conn() -> sqlite3.Connection:
    """Return a read-only SQLite connection to signals.db."""
    conn = sqlite3.connect(f"file:{_SIGNALS_DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_columns() -> set[str]:
    """Return (and cache) the set of column names in the signals table."""
    global _SIGNALS_COLUMNS
    if not _SIGNALS_COLUMNS and _SIGNALS_DB.exists():
        conn = _get_conn()
        try:
            rows = conn.execute("PRAGMA table_info(signals)").fetchall()
            _SIGNALS_COLUMNS = {r[1] for r in rows}
        finally:
            conn.close()
    return _SIGNALS_COLUMNS


# ---------------------------------------------------------------------------
# GET /api/health
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health():
    """Quick health check. Returns signal and citation counts."""
    sig_count = 0
    if _SIGNALS_DB.exists():
        try:
            conn      = _get_conn()
            sig_count = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
            conn.close()
        except Exception:
            pass
    return {
        "status":    "ok",
        "signals":   sig_count,
        "citations": len(_citations),
    }


# ---------------------------------------------------------------------------
# GET /api/meta
# ---------------------------------------------------------------------------

@app.get("/api/meta")
def meta():
    """Dataset metadata: last updated timestamp, record counts, source count."""
    sig_count    = 0
    source_count = 0
    if _SIGNALS_DB.exists():
        try:
            conn         = _get_conn()
            sig_count    = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
            source_count = conn.execute(
                "SELECT COUNT(DISTINCT source_label) FROM signals "
                "WHERE source_label IS NOT NULL AND source_label != ''"
            ).fetchone()[0]
            conn.close()
        except Exception:
            pass
    last_updated = (
        _citations_meta.get("embed_generated_at")
        or _citations_meta.get("generated_at", "")
    )
    return {
        "lastUpdated":   last_updated,
        "signalCount":   sig_count,
        "citationCount": len(_citations),
        "sourceCount":   source_count,
    }


# ---------------------------------------------------------------------------
# Citation helpers
# ---------------------------------------------------------------------------

def _cit_match(c: dict, field: str, value: str) -> bool:
    """Case-insensitive partial match against a citation field. Safe on missing fields."""
    v = c.get(field)
    if v is None:
        return False
    return value.lower() in str(v).lower()


# ---------------------------------------------------------------------------
# GET /api/citations/summary  (declared BEFORE /{id} to avoid route shadowing)
# ---------------------------------------------------------------------------

@app.get("/api/citations/summary")
def citations_summary():
    """Aggregate counts by authority, category, severity, and facility_type."""
    by_authority     = defaultdict(int)
    by_category      = defaultdict(int)
    by_severity      = defaultdict(int)
    by_facility_type = defaultdict(int)

    for c in _citations:
        by_authority    [c.get("authority",     "") or ""] += 1
        by_category     [c.get("category",      "") or ""] += 1
        by_severity     [c.get("severity",      "") or ""] += 1
        by_facility_type[c.get("facility_type", "") or ""] += 1

    return {
        "total":            len(_citations),
        "by_authority":     dict(sorted(by_authority.items(),     key=lambda x: -x[1])),
        "by_category":      dict(sorted(by_category.items(),      key=lambda x: -x[1])),
        "by_severity":      dict(sorted(by_severity.items(),      key=lambda x: -x[1])),
        "by_facility_type": dict(sorted(by_facility_type.items(), key=lambda x: -x[1])),
    }


# ---------------------------------------------------------------------------
# GET /api/citations
# ---------------------------------------------------------------------------

@app.get("/api/citations")
def citations(
    authority:     Optional[str] = None,
    category:      Optional[str] = None,
    facility_type: Optional[str] = None,
    source_type:   Optional[str] = None,
    severity:      Optional[str] = None,
    company:       Optional[str] = None,
    priority:      Optional[str] = None,
    limit:  int = Query(default=50,  ge=1, le=500),
    offset: int = Query(default=0,   ge=0),
):
    """
    List citations with optional case-insensitive partial-match filters.
    All filter params are optional. Max limit 500.
    """
    results = _citations

    # Apply each filter — safe: missing fields simply don't match
    for field, value in [
        ("authority",     authority),
        ("category",      category),
        ("facility_type", facility_type),
        ("source_type",   source_type),
        ("severity",      severity),
        ("company",       company),
        ("priority",      priority),
    ]:
        if value:
            results = [c for c in results if _cit_match(c, field, value)]

    total = len(results)
    page  = results[offset : offset + limit]
    return {"total": total, "limit": limit, "offset": offset, "results": page}


# ---------------------------------------------------------------------------
# GET /api/citations/{id}
# ---------------------------------------------------------------------------

@app.get("/api/citations/{cit_id}")
def citation_by_id(cit_id: str):
    """
    Return a single citation by its 'id' field (string hash).
    Falls back to zero-based list index if cit_id is an integer string.
    Returns 404 if not found.
    """
    # Primary: match the 'id' field (string hash like 'f4833c3a1521')
    for c in _citations:
        if str(c.get("id", "")) == cit_id:
            return c

    # Fallback: integer index
    try:
        idx = int(cit_id)
        if 0 <= idx < len(_citations):
            return _citations[idx]
    except (ValueError, TypeError):
        pass

    raise HTTPException(status_code=404, detail=f"Citation '{cit_id}' not found.")


# ---------------------------------------------------------------------------
# Signals — column mapping for API params → DB columns
#
# 'domain' has no direct DB equivalent; mapped to source_label (pubmed,
# clinical_trials, etc.).  'category' is mapped to event_type (closest
# available column).  Any param whose mapped column doesn't exist in the
# schema is silently ignored — no crash.
# ---------------------------------------------------------------------------

_SIGNAL_PARAM_MAP: dict[str, str] = {
    "domain":     "source_label",    # no 'domain' column; maps to source_label
    "source":     "source_label",
    "ingredient": "ingredient_name",
    "category":   "event_type",      # no 'category' column; maps to event_type
}


# ---------------------------------------------------------------------------
# GET /api/signals/summary  (declared BEFORE parameterised routes)
# ---------------------------------------------------------------------------

@app.get("/api/signals/summary")
def signals_summary():
    """
    Aggregate signal counts by source_label, severity, and sentiment.
    Returns empty dicts for any grouping column that doesn't exist.
    """
    if not _SIGNALS_DB.exists():
        return {
            "total":        0,
            "by_source":    {},
            "by_severity":  {},
            "by_sentiment": {},
            "note":         "signals.db not found",
        }

    columns = _ensure_columns()
    conn    = _get_conn()
    try:
        total = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]

        def _group(col: str) -> dict:
            if col not in columns:
                return {}
            rows = conn.execute(
                f"SELECT {col}, COUNT(*) FROM signals GROUP BY {col} ORDER BY COUNT(*) DESC"
            ).fetchall()
            return {(r[0] or ""): r[1] for r in rows}

        return {
            "total":        total,
            "by_source":    _group("source_label"),
            "by_severity":  _group("severity"),
            "by_sentiment": _group("sentiment"),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /api/signals
# ---------------------------------------------------------------------------

@app.get("/api/signals")
def signals(
    domain:     Optional[str] = None,
    source:     Optional[str] = None,
    severity:   Optional[str] = None,
    sentiment:  Optional[str] = None,
    ingredient: Optional[str] = None,
    category:   Optional[str] = None,
    limit:  int = Query(default=50,  ge=1, le=500),
    offset: int = Query(default=0,   ge=0),
):
    """
    List signals from SQLite with optional filters. Parameterized SQL only.
    'domain' maps to source_label; 'category' maps to event_type.
    Any filter whose mapped column doesn't exist is silently ignored.
    Max limit 500.
    """
    if not _SIGNALS_DB.exists():
        return {
            "total": 0, "limit": limit, "offset": offset,
            "results": [], "note": "signals.db not found",
        }

    columns = _ensure_columns()
    where_clauses: list[str] = []
    params:        list      = []

    for param_name, value in [
        ("domain",     domain),
        ("source",     source),
        ("severity",   severity),
        ("sentiment",  sentiment),
        ("ingredient", ingredient),
        ("category",   category),
    ]:
        if not value:
            continue
        col = _SIGNAL_PARAM_MAP.get(param_name, param_name)
        if col not in columns:
            # Column doesn't exist in this schema — skip safely
            continue
        where_clauses.append(f"{col} LIKE ?")
        params.append(f"%{value}%")

    where_sql  = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    count_sql  = f"SELECT COUNT(*) FROM signals {where_sql}"
    select_sql = (
        f"SELECT * FROM signals {where_sql} "
        f"ORDER BY scraped_at DESC LIMIT ? OFFSET ?"
    )

    conn = _get_conn()
    try:
        total = conn.execute(count_sql, params).fetchone()[0]
        rows  = conn.execute(select_sql, params + [limit, offset]).fetchall()
        return {
            "total":   total,
            "limit":   limit,
            "offset":  offset,
            "results": [dict(r) for r in rows],
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /api/schema
# ---------------------------------------------------------------------------

@app.get("/api/schema")
def schema():
    """
    Describe available fields, filterable params, and sample records.
    Useful for front-end developers and API consumers building queries.
    """
    # Citation fields — static list; supplemented from live data if available
    citation_fields = [
        "id", "authority", "source_type", "company", "date", "category",
        "severity", "summary", "url", "product_type", "country", "facility_type",
        "priority", "issue_type", "failure_modes", "ingredient_name",
        "ingredient_cluster", "action_required", "inspection_risk",
        "market_significance", "australia_relevance", "australia_reasoning",
        "relevance_to_vms", "signal_type", "ingredient_relevance",
        "potential_impact", "trend_relevance", "sentiment",
        "sentiment_confidence", "sentiment_reasoning", "ai_summary",
        "clean_title", "why_it_matters", "recommended_action",
    ]

    # Signal fields — derived from actual signals.db PRAGMA (real column names)
    signal_fields = [
        "id", "source_id", "authority", "url", "title", "scraped_at",
        "ingredient_name", "event_type", "severity", "summary",
        "source_label", "product_category", "competitor_signal",
        "market_significance", "australia_relevance", "australia_reasoning",
        "relevance_to_vms", "signal_type", "ingredient_relevance",
        "potential_impact", "trend_relevance", "sentiment",
        "sentiment_confidence", "sentiment_reasoning", "created_at",
        "digest_sent", "ai_summary", "clean_title", "why_it_matters",
        "recommended_action", "inspection_risk", "is_noise", "noise_reason",
    ]

    citation_filters = [
        "authority", "category", "facility_type", "source_type",
        "severity", "company", "priority",
    ]

    signal_filters = [
        "domain (→ source_label)",
        "source (→ source_label)",
        "severity",
        "sentiment",
        "ingredient (→ ingredient_name)",
        "category (→ event_type)",
    ]

    # Sample citation from loaded data
    sample_citation: dict = _citations[0] if _citations else {}

    # Merge any extra keys from live data into the static citation_fields list
    if _citations:
        known = set(citation_fields)
        for k in _citations[0].keys():
            if k not in known:
                citation_fields.append(k)

    # Sample signal from SQLite
    sample_signal: dict = {}
    if _SIGNALS_DB.exists():
        try:
            conn = _get_conn()
            row  = conn.execute("SELECT * FROM signals LIMIT 1").fetchone()
            conn.close()
            if row:
                sample_signal = dict(row)
        except Exception:
            pass

    return {
        "citationFields":  citation_fields,
        "signalFields":    signal_fields,
        "citationFilters": citation_filters,
        "signalFilters":   signal_filters,
        "sampleCitation":  sample_citation,
        "sampleSignal":    sample_signal,
    }


# ---------------------------------------------------------------------------
# GET /api/ingredients
# ---------------------------------------------------------------------------

@app.get("/api/ingredients")
def ingredients():
    """
    Top ingredients by signal count, with sentiment breakdown.
    Returns a note dict if the ingredient_name column doesn't exist.
    """
    if not _SIGNALS_DB.exists():
        return {"results": [], "note": "signals.db not found"}

    columns = _ensure_columns()
    if "ingredient_name" not in columns:
        return {"results": [], "note": "No ingredient column found in signals database."}

    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT
                ingredient_name,
                COUNT(*)                                                  AS cnt,
                SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END)  AS pos,
                SUM(CASE WHEN sentiment = 'neutral'  THEN 1 ELSE 0 END)  AS neu,
                SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END)  AS neg
            FROM signals
            WHERE ingredient_name IS NOT NULL
              AND ingredient_name != ''
              AND ingredient_name != 'unknown'
            GROUP BY ingredient_name
            ORDER BY cnt DESC
            LIMIT 200
            """
        ).fetchall()
        return [
            {
                "ingredient": r["ingredient_name"],
                "count":      r["cnt"],
                "sentiment_breakdown": {
                    "positive": r["pos"] or 0,
                    "neutral":  r["neu"] or 0,
                    "negative": r["neg"] or 0,
                },
            }
            for r in rows
        ]
    finally:
        conn.close()
