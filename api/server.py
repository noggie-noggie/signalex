"""
api/server.py — Read-only FastAPI layer for Signalex regulatory intelligence data.

Data sources:
  - reports/citation_database.json  → loaded once at startup, kept in memory
  - data/signals.db                 → new SQLite connection per request (read-only URI)

Does NOT import config.py, scheduler, scrapers, classifier, or analytics modules.
Read endpoints do not write. Food claim guidance may write to its response cache.

Run:
    uvicorn api.server:app --reload --port 8000

Endpoints:
    GET /api/health
    GET /api/meta
    GET /api/citations              ?authority=&category=&facility_type=&source_type=&severity=&company=&priority=&limit=50&offset=0
    GET /api/citations/summary
    GET /api/citations/{id}
    GET /api/signals                ?domain=&source=&severity=&sentiment=&ingredient=&category=&include_noise=false&include_low_quality_sources=false&limit=50&offset=0
    GET /api/signals/summary
    GET /api/ingredients
    POST /api/food/claims/guide     Deterministic food claim concept guidance (v1, no AI)
    GET /api/food/claim-pathways    Deterministic food claim pathway cards
    POST /api/food/claim-review     Deterministic free-text food claim review
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from pydantic import BaseModel

# Food claim guidance services (deterministic, no AI)
from services.food_claims.classifier import classify_claim
from services.food_claims.pathways   import get_claim_pathways
from services.food_claims.retriever  import retrieve_supporting_signals
from services.food_claims.cache      import make_input_hash, get_cached_guidance, save_guidance
from services.food_claim_pathways import get_claim_pathway, list_claim_pathways, normalize_claim_key
from services.food_claim_review import review_food_claim
from services.food_taxonomy import enrich_food_signal
from services.food_duplicates import filter_visible_food_duplicates

# ---------------------------------------------------------------------------
# Paths — resolved relative to the repo root (one level above this file)
# ---------------------------------------------------------------------------
_ROOT            = Path(__file__).parent.parent   # ~/vms-intel
_SIGNALS_DB      = _ROOT / "data" / "signals.db"
_CITATIONS_JSON  = _ROOT / "reports" / "citation_database.json"
load_dotenv(_ROOT / ".env")

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Signalex Read-Only API",
    description="Read-only regulatory intelligence endpoints. No auth yet.",
    version="0.2.0",
)

_LOCAL_CORS_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5173",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
]


def _cors_origins() -> list[str]:
    """Return local origins plus comma-separated CORS_ORIGINS values."""
    configured = [
        origin.strip().rstrip("/")
        for origin in os.getenv("CORS_ORIGINS", "").split(",")
        if origin.strip()
    ]
    return list(dict.fromkeys([*_LOCAL_CORS_ORIGINS, *configured]))


app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
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
def health(response: Response):
    """Check that both API data stores exist, load, and can be queried."""
    errors: list[str] = []
    warnings: list[str] = []
    signal_count = 0
    food_count = 0
    vms_count = 0

    signals_exists = _SIGNALS_DB.exists()
    signals_readable = False
    if not signals_exists:
        errors.append(f"Missing signals database: {_SIGNALS_DB}")
    else:
        try:
            conn = _get_conn()
            try:
                signal_count = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
                food_count = conn.execute(
                    "SELECT COUNT(*) FROM signals WHERE domain = 'food'"
                ).fetchone()[0]
                vms_count = conn.execute(
                    "SELECT COUNT(*) FROM signals WHERE domain = 'vms'"
                ).fetchone()[0]
                blank_domains = conn.execute(
                    "SELECT COUNT(*) FROM signals WHERE domain IS NULL OR domain = ''"
                ).fetchone()[0]
                signals_readable = True
                if blank_domains:
                    warnings.append(
                        f"{blank_domains} signal row(s) have no domain; run the VMS backfill."
                    )
            finally:
                conn.close()
        except Exception as exc:
            errors.append(f"Signals database is not readable: {exc}")

    citations_exists = _CITATIONS_JSON.exists()
    citations_loaded = citations_exists and "error" not in _citations_meta
    if not citations_exists:
        errors.append(f"Missing citation database: {_CITATIONS_JSON}")
    elif not citations_loaded:
        errors.append(
            f"Citation database failed to load: {_citations_meta.get('error', 'unknown error')}"
        )

    if errors:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {
        "status": "error" if errors else ("warning" if warnings else "ok"),
        "signals": {
            "path": str(_SIGNALS_DB),
            "exists": signals_exists,
            "readable": signals_readable,
            "total": signal_count,
            "food": food_count,
            "vms": vms_count,
        },
        "citations": {
            "path": str(_CITATIONS_JSON),
            "exists": citations_exists,
            "loaded": citations_loaded,
            "total": len(_citations),
            "sourceOfTruthFor": "pharma",
        },
        "warnings": warnings,
        "errors": errors,
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
    "domain":     "domain",          # real column now — filters by domain (vms/food/pharma)
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
    include_noise: bool = Query(
        default=False,
        description="Include rows where is_noise=1. Default false (noise rows hidden).",
    ),
    include_low_quality_sources: bool = Query(
        default=False,
        description=(
            "Include biorxiv and europe_pmc in domain=vms results. "
            "Default false — these sources are hidden from VMS default views "
            "due to low current relevance. Has no effect on other domains. "
            "Passing source=biorxiv or source=europe_pmc explicitly always returns those rows."
        ),
    ),
    limit:  int = Query(default=50,  ge=1, le=500),
    offset: int = Query(default=0,   ge=0),
):
    """
    List signals from SQLite with optional filters. Parameterised SQL only.

    Default behaviour (domain=vms):
      - Rows with is_noise=1 are excluded unless include_noise=true.
      - biorxiv and europe_pmc are excluded unless include_low_quality_sources=true
        or source= is set explicitly to one of those values.

    food domain and /api/citations are unaffected by these flags.
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

    # ── Standard column filters (LIKE-based) ─────────────────────────────────
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

    # ── is_noise filter (default: hide noise rows) ────────────────────────────
    if not include_noise and "is_noise" in columns:
        where_clauses.append("(is_noise = 0 OR is_noise IS NULL)")

    # ── Low-quality source filter (VMS default views only) ────────────────────
    # Applied when:
    #   • domain=vms is explicitly requested
    #   • source= is NOT set (if the caller pins a source we always honour it)
    #   • include_low_quality_sources is false
    _LOW_QUALITY_SOURCES = ("biorxiv", "europe_pmc")
    if (
        domain == "vms"
        and source is None
        and not include_low_quality_sources
        and "source_label" in columns
    ):
        placeholders = ", ".join("?" * len(_LOW_QUALITY_SOURCES))
        where_clauses.append(f"source_label NOT IN ({placeholders})")
        params.extend(_LOW_QUALITY_SOURCES)

    where_sql  = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    count_sql  = f"SELECT COUNT(*) FROM signals {where_sql}"
    select_sql = (
        f"SELECT * FROM signals {where_sql} "
        f"ORDER BY scraped_at DESC LIMIT ? OFFSET ?"
    )

    conn = _get_conn()
    try:
        if domain == "food":
            rows = conn.execute(
                f"SELECT * FROM signals {where_sql} ORDER BY scraped_at DESC",
                params,
            ).fetchall()
            results = [enrich_food_signal(dict(r)) for r in rows]
            if not include_noise:
                results = _visible_food_rows(results)
                results = filter_visible_food_duplicates(results)
            total, results = _page_enriched(results, limit, offset)
        else:
            total = conn.execute(count_sql, params).fetchone()[0]
            rows  = conn.execute(select_sql, params + [limit, offset]).fetchall()
            results = [dict(r) for r in rows]
        return {
            "total":   total,
            "limit":   limit,
            "offset":  offset,
            "results": results,
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
        "market", "category", "product_type", "ingredient", "issue_area",
        "claim_theme", "source_type", "dashboard_section", "impact", "momentum",
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


# ===========================================================================
# FOOD CLAIM GUIDANCE  (v1 — deterministic, no AI)
# ===========================================================================

_CLAIM_DISCLAIMER = (
    "Concept guidance only. Final claim support depends on formulation, "
    "ingredient levels, serving size, nutrition panel, exact wording, "
    "evidence and regulatory review."
)

# Always included in missing_information so the frontend always has a
# complete checklist, even for not_recommended claims.
_UNIVERSAL_MISSING_INFO: list[str] = [
    "Exact proposed wording for the claim",
    "Full ingredient list",
    "Amount per serve for key active ingredients",
    "Serving size",
    "Nutrition information panel",
    "Target consumer (general population, active adults, children, etc.)",
    "Claim placement (packaging, website, advertising)",
    "Supporting evidence for the claim",
    "Market / country of intended sale",
]

# Reframe suggestions returned for all therapeutic/disease claims.
_REFRAME_SUGGESTIONS: list[str] = [
    "Consider moving away from disease or treatment wording.",
    "Reframe toward general wellbeing or normal function if appropriate and substantiated.",
    "Use product-specific evidence and regulatory review before progressing.",
]

# Safer wording shown when a therapeutic claim has digestive/gut context.
_GUT_SAFE_WORDING: list[str] = [
    "Supports digestive wellbeing",
    "Contains live cultures",
    "Contains fibre to support digestive health",
    "Made with fermented dairy cultures",
]

# Gut-context detector — used to decide whether to surface gut-specific safe wording.
_GUT_TERMS: frozenset[str] = frozenset({
    "gut", "digestive", "digestion", "ibs", "bowel", "bloating",
    "microbiome", "intestin", "stomach", "probiotic", "prebiotic",
    "fibre", "fiber", "ferment",
})

# Themes whose status should be needs_nutrition_check (nutrient content claims
# that require a nutrition panel check before any claim can be made).
_NUTRITION_CHECK_THEMES: frozenset[str] = frozenset({"low_sugar", "high_protein"})

_CLAIM_SUMMARY_TEMPLATES: dict[str, str] = {
    # keyed by theme name (checked first) or risk_level (fallback)
    "therapeutic_or_disease_claim": (
        "This claim contains therapeutic or disease-related language that is not "
        "permitted for food products under Australian food law. Claims that treat, "
        "cure, prevent or refer to specific medical conditions require a therapeutic "
        "goods (TGA) regulatory pathway, not a food claim pathway. Immediate "
        "reformulation of the claim wording is required before any use."
    ),
    "low": (
        "This claim theme appears lower-risk and aligns with common nutrient content "
        "or general wellbeing claim categories. Review the pathways below to confirm "
        "your product meets the relevant ingredient thresholds."
    ),
    "medium": (
        "This claim theme is in common use but requires substantiation. Review the "
        "recommended pathways and ensure the product formulation supports the specific "
        "claim before use."
    ),
    "high": (
        "One or more high-risk terms have been detected in this claim. Review the "
        "risk reasons carefully and consider reformulating before use."
    ),
    "unknown": (
        "The claim could not be matched to a recognised theme. "
        "Please provide more detail about the intended health benefit."
    ),
}


class FoodClaimRequest(BaseModel):
    claim:      str
    food_type:  str
    market:     str  = "Australia"
    refresh:    bool = False


class FoodClaimReviewRequest(BaseModel):
    claim_text:   str
    food_type:    str = ""
    jurisdiction: str = "AU/NZ"
    claim_location: Optional[str] = None
    serving_size: Optional[dict] = None
    use_ai:       bool = False
    force_ai:     bool = False


@app.get("/api/food/claim-pathways")
def food_claim_pathways(claim: Optional[str] = None):
    """
    Deterministic food claim pathway cards for frontend display.

    - No query: returns all pathways.
    - claim=...: returns one normalised pathway, accepting spaces, hyphens,
      underscores, and display wording such as "High in protein".
    """
    if not claim:
        pathways = list_claim_pathways()
        return {"total": len(pathways), "results": pathways}

    pathway = get_claim_pathway(claim)
    if pathway is None:
        normalised = normalize_claim_key(claim)
        raise HTTPException(
            status_code=404,
            detail={
                "status": "not_found",
                "claim": normalised,
                "message": f"No food claim pathway found for '{claim}'.",
            },
        )
    return pathway


@app.post("/api/food/claim-review")
def food_claim_review(body: FoodClaimReviewRequest, request: Request):
    """Deterministic free-text food claim assessment. Phase 1: no AI."""
    claim_text = (body.claim_text or "").strip()
    if not claim_text:
        raise HTTPException(status_code=422, detail="'claim_text' is required and must not be empty.")
    return review_food_claim(
        claim_text=claim_text,
        food_type=(body.food_type or "").strip(),
        jurisdiction=(body.jurisdiction or "AU/NZ").strip(),
        claim_location=body.claim_location,
        serving_size=body.serving_size,
        use_ai=body.use_ai,
        force_ai=body.force_ai,
        client_ip=request.client.host if request.client else None,
    )


@app.post("/api/food/claims/guide")
def food_claim_guide(body: FoodClaimRequest):
    """
    v1 Food Claim Concept Guidance — deterministic, no AI.

    Takes a claim, food_type, and market (default Australia) and returns
    structured concept guidance: risk level, claim pathways, safer wording,
    competitor examples, related FSANZ rules, and related VMS evidence.

    Responses are cached by input hash (includes CACHE_VERSION).
    Pass refresh=true to bypass cache and regenerate.
    """
    claim     = (body.claim     or "").strip()
    food_type = (body.food_type or "").strip()
    market    = (body.market    or "Australia").strip()

    if not claim:
        raise HTTPException(status_code=422, detail="'claim' is required and must not be empty.")
    if not food_type:
        raise HTTPException(status_code=422, detail="'food_type' is required and must not be empty.")

    input_hash = make_input_hash(claim, food_type, market)

    # ── Cache check ───────────────────────────────────────────────────────────
    if not body.refresh:
        cached = get_cached_guidance(input_hash)
        if cached is not None:
            cached["cached"] = True
            return cached

    # ── Classify ──────────────────────────────────────────────────────────────
    classification  = classify_claim(claim)
    theme           = classification["theme"]
    claim_type      = classification["claim_type"]
    risk_level      = classification["risk_level"]
    risk_reasons    = classification["risk_reasons"]
    is_therapeutic  = classification["is_therapeutic"]

    # ── Pathways ──────────────────────────────────────────────────────────────
    # Therapeutic/disease claims get a fixed stub — no pathway is available.
    if is_therapeutic:
        pathway_data: dict = {
            "food_type_fit":        "unsuitable",
            "claim_pathways":       [],
            "possible_ingredients": [],
            "safer_wording":        [],
            "avoid_wording":        [],
            "missing_information":  [],
            "next_questions":       [],
        }
    else:
        pathway_data = get_claim_pathways(theme, food_type)

    # ── Ensure missing_information is always fully populated ──────────────────
    existing_info = set(pathway_data["missing_information"])
    merged_info   = list(pathway_data["missing_information"])
    for item in _UNIVERSAL_MISSING_INFO:
        if item not in existing_info:
            merged_info.append(item)
    pathway_data["missing_information"] = merged_info

    # ── DB retrieval ──────────────────────────────────────────────────────────
    # Use the detected gut keyword or theme name as search term for therapeutic claims
    retrieval_keyword = claim if not is_therapeutic else (
        theme.replace("_", " ") if theme else claim
    )
    db_data = retrieve_supporting_signals(retrieval_keyword, theme)

    # ── Status ────────────────────────────────────────────────────────────────
    if is_therapeutic:
        status = "not_recommended"
    elif risk_level == "high":
        status = "high_risk_wording"
    elif theme in _NUTRITION_CHECK_THEMES:
        status = "needs_nutrition_check"
    elif theme is None:
        status = "needs_evidence_review"
    elif pathway_data["missing_information"]:
        status = "needs_product_details"
    else:
        status = "low_risk_direction"

    # ── Confidence ────────────────────────────────────────────────────────────
    if is_therapeutic:
        confidence = "high"   # high confidence it's a therapeutic claim
    elif theme is not None and pathway_data["food_type_fit"] == "high":
        confidence = "high"
    elif theme is not None:
        confidence = "medium"
    else:
        confidence = "low"

    # ── Summary ───────────────────────────────────────────────────────────────
    # Prefer theme-specific template, fall back to risk_level template.
    summary = (
        _CLAIM_SUMMARY_TEMPLATES.get(theme)
        or _CLAIM_SUMMARY_TEMPLATES.get(risk_level)
        or _CLAIM_SUMMARY_TEMPLATES["unknown"]
    )

    # ── Reframe suggestions (therapeutic claims only) ─────────────────────────
    reframe_suggestions: list[str] = []
    safer_wording = list(pathway_data["safer_wording"])

    if is_therapeutic:
        reframe_suggestions = list(_REFRAME_SUGGESTIONS)
        # Surface gut-specific safer wording when the claim has digestive context
        claim_lower = claim.lower()
        if any(term in claim_lower for term in _GUT_TERMS):
            safer_wording = _GUT_SAFE_WORDING[:]

    # ── Assemble response ─────────────────────────────────────────────────────
    response: dict = {
        "assessment_level":     "concept_guidance",
        "cached":               False,
        "input": {
            "claim":      claim,
            "food_type":  food_type,
            "market":     market,
        },
        "status":               status,
        "confidence":           confidence,
        "theme":                theme,
        "claim_type":           claim_type,
        "risk_level":           risk_level,
        "risk_reasons":         risk_reasons,
        "summary":              summary,
        "food_type_fit":        pathway_data["food_type_fit"],
        "claim_pathways":       pathway_data["claim_pathways"],
        "possible_ingredients": pathway_data["possible_ingredients"],
        "missing_information":  pathway_data["missing_information"],
        "safer_wording":        safer_wording,
        "avoid_wording":        pathway_data["avoid_wording"],
        "reframe_suggestions":  reframe_suggestions,
        "competitor_examples":  db_data["competitor_examples"],
        "related_rules":        db_data["related_rules"],
        "related_evidence":     db_data["related_evidence"],
        "next_questions":       pathway_data["next_questions"],
        "disclaimer":           _CLAIM_DISCLAIMER,
    }

    # ── Cache ─────────────────────────────────────────────────────────────────
    save_guidance(input_hash, claim, food_type, market, response)

    return response


# ===========================================================================
# FOOD DOMAIN ENDPOINTS
# All food signals share domain='food' in the signals table.
# ===========================================================================

def _food_conn() -> sqlite3.Connection:
    """Read-only connection; identical to _get_conn() — separate alias for clarity."""
    return _get_conn()


def _food_db_available() -> bool:
    return _SIGNALS_DB.exists()


def _food_columns() -> set[str]:
    return _ensure_columns()


def _enrich_food_rows(rows) -> list[dict]:
    return [enrich_food_signal(dict(r)) for r in rows]


def _visible_food_rows(rows: list[dict]) -> list[dict]:
    return [
        r for r in rows
        if int(r.get("is_noise") or 0) != 1
        and r.get("dashboard_section") != "excluded"
    ]


def _visible_unique_food_rows(rows: list[dict]) -> list[dict]:
    return filter_visible_food_duplicates(_visible_food_rows(rows))


def _page_enriched(rows: list[dict], limit: int, offset: int) -> tuple[int, list[dict]]:
    total = len(rows)
    return total, rows[offset : offset + limit]


# ---------------------------------------------------------------------------
# GET /api/food/dashboard
# ---------------------------------------------------------------------------

@app.get("/api/food/dashboard")
def food_dashboard():
    """
    Aggregate food-domain overview + recent signals by category.

    Returns:
      overview.signals        — total food signals
      overview.risks          — high/critical severity food signals
      overview.newLaunches    — new_product signals (Open Food Facts)
      overview.ruleUpdates    — rule_update signals (FSANZ updates)
      recentSignals           — last 20 food signals
      recalls                 — food recall signals (fsanz_recalls)
      ruleUpdates             — rule_update signals
      competitorProducts      — open_food_facts product signals
      claimSignals            — signals with a non-empty claim field
      ingredientTrends        — top ingredients in food signals
    """
    if not _food_db_available():
        empty: dict = {
            "overview": {"signals": 0, "risks": 0, "newLaunches": 0, "ruleUpdates": 0},
            "recentSignals": [], "recalls": [], "ruleUpdates": [],
            "competitorProducts": [], "claimSignals": [], "ingredientTrends": [],
            "note": "signals.db not found",
        }
        return empty

    cols = _food_columns()
    has_domain  = "domain"       in cols
    has_claim   = "claim"        in cols

    conn = _food_conn()
    try:
        domain_filter = "domain = 'food'" if has_domain else "source_label LIKE 'food_%' OR source_label = 'open_food_facts'"

        all_rows = _enrich_food_rows(conn.execute(
            f"SELECT * FROM signals WHERE {domain_filter} "
            f"ORDER BY scraped_at DESC"
        ).fetchall())
        all_rows = _visible_unique_food_rows(all_rows)

        # Overview counts
        total = len(all_rows)

        risks = len([
            r for r in all_rows
            if (r.get("severity") or "").lower() in {"high", "critical", "severe"}
        ])

        recent_signals = all_rows[:20]
        recalls = [r for r in all_rows if r.get("dashboard_section") == "recalls_safety"][:50]
        rule_updates = [r for r in all_rows if r.get("dashboard_section") == "regulatory_updates"][:50]
        competitor_products = [r for r in all_rows if r.get("source_label") == "open_food_facts"][:50]
        new_launches = len([r for r in all_rows if r.get("signal_type") == "product_launch"])
        rule_updates_count = len(rule_updates)

        # Claim signals (have a non-empty claim field)
        claim_signals = [
            r for r in all_rows
            if r.get("dashboard_section") == "claims_labelling"
            or (has_claim and r.get("claim"))
        ][:50]

        # Ingredient trends (top ingredients within visible food rows)
        ingredient_counts = Counter(
            r.get("ingredient_name")
            for r in all_rows
            if r.get("ingredient_name")
        )
        ingredient_trends = [
            {"ingredient": ingredient, "count": count}
            for ingredient, count in ingredient_counts.most_common(30)
        ]

        return {
            "overview": {
                "signals":    total,
                "risks":      risks,
                "newLaunches": new_launches,
                "ruleUpdates": rule_updates_count,
            },
            "recentSignals":      recent_signals,
            "recalls":            recalls,
            "ruleUpdates":        rule_updates,
            "competitorProducts": competitor_products,
            "claimSignals":       claim_signals,
            "ingredientTrends":   ingredient_trends,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /api/food/signals
# ---------------------------------------------------------------------------

@app.get("/api/food/signals")
def food_signals(
    severity:   Optional[str] = None,
    signal_type: Optional[str] = None,
    source:     Optional[str] = None,
    ingredient: Optional[str] = None,
    allergen:   Optional[str] = None,
    company:    Optional[str] = None,
    limit:  int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0,  ge=0),
):
    """
    List food-domain signals with optional filters. Max limit 500.

    Filters: severity, signal_type, source (maps to source_label),
             ingredient (maps to ingredient_name), allergen, company.
    """
    if not _food_db_available():
        return {"total": 0, "limit": limit, "offset": offset,
                "results": [], "note": "signals.db not found"}

    cols = _food_columns()
    has_domain  = "domain"   in cols
    has_allergen = "allergen" in cols
    has_company  = "company"  in cols

    domain_clause = "domain = 'food'" if has_domain else \
        "(source_label LIKE 'food_%' OR source_label = 'open_food_facts')"

    where_clauses = [domain_clause]
    params: list = []

    filters = [
        ("severity",   "severity",        True),
        ("source",     "source_label",     True),
        ("ingredient", "ingredient_name",  True),
    ]
    if has_allergen:
        filters.append(("allergen", "allergen", True))
    if has_company:
        filters.append(("company",  "company",  True))

    for param_val, col, use_like in filters:
        value = locals().get(param_val.replace(" ", "_"))
        if value and col in cols:
            where_clauses.append(f"{col} LIKE ?")
            params.append(f"%{value}%")

    where_sql = "WHERE " + " AND ".join(where_clauses)

    conn = _food_conn()
    try:
        rows = conn.execute(
            f"SELECT * FROM signals {where_sql} ORDER BY scraped_at DESC",
            params,
        ).fetchall()
        results = _visible_unique_food_rows(_enrich_food_rows(rows))
        if signal_type:
            results = [r for r in results if signal_type.lower() in r.get("signal_type", "").lower()]
        total, page = _page_enriched(results, limit, offset)
        return {
            "total":   total,
            "limit":   limit,
            "offset":  offset,
            "results": page,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /api/food/products
# ---------------------------------------------------------------------------

@app.get("/api/food/products")
def food_products(
    brand:      Optional[str] = None,
    ingredient: Optional[str] = None,
    allergen:   Optional[str] = None,
    claim:      Optional[str] = None,
    limit:  int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0,  ge=0),
):
    """
    List food product signals from Open Food Facts.
    Filters: brand, ingredient, allergen, claim.
    """
    if not _food_db_available():
        return {"total": 0, "limit": limit, "offset": offset,
                "results": [], "note": "signals.db not found"}

    cols = _food_columns()
    has_domain  = "domain"   in cols
    has_brand   = "brand"    in cols
    has_allergen = "allergen" in cols
    has_claim    = "claim"    in cols

    domain_clause = "domain = 'food'" if has_domain else "source_label = 'open_food_facts'"
    where_clauses = [domain_clause, "source_label = 'open_food_facts'"]
    params: list = []

    if brand and has_brand and "brand" in cols:
        where_clauses.append("brand LIKE ?")
        params.append(f"%{brand}%")
    if ingredient and "ingredient_name" in cols:
        where_clauses.append("ingredient_name LIKE ?")
        params.append(f"%{ingredient}%")
    if allergen and has_allergen:
        where_clauses.append("allergen LIKE ?")
        params.append(f"%{allergen}%")
    if claim and has_claim:
        where_clauses.append("claim LIKE ?")
        params.append(f"%{claim}%")

    where_sql = "WHERE " + " AND ".join(where_clauses)

    conn = _food_conn()
    try:
        rows  = conn.execute(
            f"SELECT * FROM signals {where_sql} ORDER BY scraped_at DESC",
            params,
        ).fetchall()
        results = _visible_unique_food_rows(_enrich_food_rows(rows))
        total, page = _page_enriched(results, limit, offset)
        return {
            "total":   total,
            "limit":   limit,
            "offset":  offset,
            "results": page,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /api/food/recalls
# ---------------------------------------------------------------------------

@app.get("/api/food/recalls")
def food_recalls(
    allergen: Optional[str] = None,
    severity: Optional[str] = None,
    company:  Optional[str] = None,
    limit:  int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0,  ge=0),
):
    """
    List food recall signals from FSANZ.
    Filters: allergen, severity, company.
    """
    if not _food_db_available():
        return {"total": 0, "limit": limit, "offset": offset,
                "results": [], "note": "signals.db not found"}

    cols = _food_columns()
    has_domain   = "domain"   in cols
    has_allergen = "allergen" in cols
    has_company  = "company"  in cols

    domain_clause = "domain = 'food'" if has_domain else "source_label = 'food_fsanz_recalls'"
    where_clauses = [domain_clause]
    params: list = []

    if allergen and has_allergen:
        where_clauses.append("allergen LIKE ?")
        params.append(f"%{allergen}%")
    if severity:
        where_clauses.append("severity LIKE ?")
        params.append(f"%{severity}%")
    if company and has_company:
        where_clauses.append("company LIKE ?")
        params.append(f"%{company}%")

    where_sql = "WHERE " + " AND ".join(where_clauses)

    conn = _food_conn()
    try:
        rows  = conn.execute(
            f"SELECT * FROM signals {where_sql} ORDER BY scraped_at DESC",
            params,
        ).fetchall()
        results = [
            r for r in _visible_unique_food_rows(_enrich_food_rows(rows))
            if r.get("dashboard_section") == "recalls_safety"
        ]
        total, page = _page_enriched(results, limit, offset)
        return {
            "total":   total,
            "limit":   limit,
            "offset":  offset,
            "results": page,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /api/food/rules
# ---------------------------------------------------------------------------

@app.get("/api/food/rules")
def food_rules(
    severity: Optional[str] = None,
    keyword:  Optional[str] = None,
    limit:  int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0,  ge=0),
):
    """
    List FSANZ food standards / regulatory update signals.
    Filters: severity, keyword (partial match on title or summary).
    """
    if not _food_db_available():
        return {"total": 0, "limit": limit, "offset": offset,
                "results": [], "note": "signals.db not found"}

    cols = _food_columns()
    has_domain = "domain" in cols

    domain_clause = "domain = 'food'" if has_domain else "source_label = 'food_fsanz_updates'"
    where_clauses = [domain_clause]
    params: list = []

    if severity:
        where_clauses.append("severity LIKE ?")
        params.append(f"%{severity}%")
    if keyword:
        where_clauses.append("(title LIKE ? OR summary LIKE ?)")
        params.extend([f"%{keyword}%", f"%{keyword}%"])

    where_sql = "WHERE " + " AND ".join(where_clauses)

    conn = _food_conn()
    try:
        rows  = conn.execute(
            f"SELECT * FROM signals {where_sql} ORDER BY scraped_at DESC",
            params,
        ).fetchall()
        results = [
            r for r in _visible_unique_food_rows(_enrich_food_rows(rows))
            if r.get("dashboard_section") == "regulatory_updates"
        ]
        total, page = _page_enriched(results, limit, offset)
        return {
            "total":   total,
            "limit":   limit,
            "offset":  offset,
            "results": page,
        }
    finally:
        conn.close()
