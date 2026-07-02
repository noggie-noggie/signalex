"""
analytics/db.py — SQLite persistence layer for classified signals + sentiment.

All new pipeline signals are stored here alongside their sentiment scores,
trend metadata, and feedback annotations.  The TinyDB store (storage/signals.py)
is left intact for the existing pipeline; this module adds alongside it.

Schema:
  signals      — one row per ClassifiedSignal + sentiment + extras
  feedback     — user feedback on classification quality (mirrors data/feedback.json)
"""

from __future__ import annotations

import csv
import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import config
from services.food_supplement_filter import is_supplement_like_food

if TYPE_CHECKING:
    from classifier.claude import ClassifiedSignal

logger = logging.getLogger(__name__)

DB_PATH = config.DATA_DIR / "signals.db"
FEEDBACK_JSON = config.DATA_DIR / "feedback.json"
ACCURACY_CSV = config.DATA_DIR / "accuracy_log.csv"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id               TEXT    UNIQUE,
    authority               TEXT,
    url                     TEXT,
    title                   TEXT,
    scraped_at              TEXT,
    ingredient_name         TEXT,
    event_type              TEXT,
    severity                TEXT,
    summary                 TEXT,
    source_label            TEXT,
    product_category        TEXT,
    competitor_signal       INTEGER DEFAULT 0,
    market_significance     TEXT,
    australia_relevance     TEXT,
    australia_reasoning     TEXT,
    relevance_to_vms        TEXT,
    signal_type             TEXT,
    ingredient_relevance    TEXT,
    potential_impact        TEXT,
    trend_relevance         TEXT,
    sentiment               TEXT,
    sentiment_confidence    REAL    DEFAULT 0.0,
    sentiment_reasoning     TEXT,
    ai_summary              TEXT    DEFAULT '',
    clean_title             TEXT    DEFAULT '',
    why_it_matters          TEXT    DEFAULT '',
    recommended_action      TEXT    DEFAULT '',
    inspection_risk         TEXT    DEFAULT '',
    is_noise                INTEGER DEFAULT 0,
    noise_reason            TEXT    DEFAULT '',
    created_at              TEXT,
    digest_sent             INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS feedback (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       TEXT,
    rating          TEXT,   -- correct | incorrect | partially_correct
    correction_note TEXT,
    created_at      TEXT
);
"""


def get_conn() -> sqlite3.Connection:
    """Return a connection to signals.db, creating the schema if needed."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    # Migrations: add columns introduced after initial schema
    for col, defn in [
        ("ai_summary",         "TEXT    DEFAULT ''"),
        ("clean_title",        "TEXT    DEFAULT ''"),
        ("why_it_matters",     "TEXT    DEFAULT ''"),
        ("recommended_action", "TEXT    DEFAULT ''"),
        ("inspection_risk",    "TEXT    DEFAULT ''"),
        ("is_noise",           "INTEGER DEFAULT 0"),
        ("noise_reason",       "TEXT    DEFAULT ''"),
        # Food domain columns (v2 — safe to add, harmless for VMS/pharma rows)
        ("domain",             "TEXT    DEFAULT ''"),
        ("company",            "TEXT    DEFAULT ''"),
        ("brand",              "TEXT    DEFAULT ''"),
        ("product_name",       "TEXT    DEFAULT ''"),
        ("allergen",           "TEXT    DEFAULT ''"),
        ("claim",              "TEXT    DEFAULT ''"),
    ]:
        try:
            conn.execute(f"ALTER TABLE signals ADD COLUMN {col} {defn}")
            conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists
    return conn


# ---------------------------------------------------------------------------
# Signal persistence
# ---------------------------------------------------------------------------

def save_signal(sig: "ClassifiedSignal") -> bool:
    """
    Insert a ClassifiedSignal into signals.db.
    VMS is the default domain; an explicitly supplied domain is preserved.
    Returns True if inserted, False if source_id already existed.
    """
    payload = sig.model_dump()
    payload["domain"] = payload.get("domain") or getattr(sig, "domain", None) or "vms"
    payload["competitor_signal"] = int(sig.competitor_signal)
    payload["is_noise"] = int(sig.is_noise)
    payload["created_at"] = datetime.now(timezone.utc).isoformat()

    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO signals (
                source_id, authority, url, title, scraped_at,
                ingredient_name, event_type, severity, summary,
                source_label, product_category, competitor_signal,
                market_significance, australia_relevance, australia_reasoning,
                relevance_to_vms, signal_type, ingredient_relevance,
                potential_impact, trend_relevance,
                sentiment, sentiment_confidence, sentiment_reasoning,
                ai_summary, clean_title, why_it_matters, recommended_action,
                inspection_risk, is_noise, noise_reason, created_at, domain
            ) VALUES (
                :source_id, :authority, :url, :title, :scraped_at,
                :ingredient_name, :event_type, :severity, :summary,
                :source_label, :product_category, :competitor_signal,
                :market_significance, :australia_relevance, :australia_reasoning,
                :relevance_to_vms, :signal_type, :ingredient_relevance,
                :potential_impact, :trend_relevance,
                :sentiment, :sentiment_confidence, :sentiment_reasoning,
                :ai_summary, :clean_title, :why_it_matters, :recommended_action,
                :inspection_risk, :is_noise, :noise_reason, :created_at, :domain
            )
            """,
            payload,
        )
        inserted = conn.execute("SELECT changes()").fetchone()[0] > 0
        conn.commit()
        return inserted
    finally:
        conn.close()


def save_food_signal(d: dict) -> bool:
    """
    Insert a food-domain signal from a plain dict (no ClassifiedSignal needed).
    Returns True if inserted, False if source_id already existed.

    Expected keys (all optional except source_id):
      source_id, title, summary, url, scraped_at, severity, signal_type,
      source_label, domain, company, brand, product_name, ingredient_name,
      allergen, claim, product_category, recommended_action, authority
    """
    now = datetime.now(timezone.utc).isoformat()
    source_label = d.get("source", "") or d.get("source_label", "")
    if source_label == "open_food_facts" and is_supplement_like_food(d):
        logger.info(
            "Skipping Open Food Facts supplement-like product for Food domain: %s",
            d.get("title") or d.get("product_name") or d.get("source_id"),
        )
        return False

    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO signals (
                source_id, authority, url, title, scraped_at,
                ingredient_name, event_type, severity, summary,
                source_label, product_category, signal_type,
                recommended_action, created_at,
                domain, company, brand, product_name, allergen, claim
            ) VALUES (
                :source_id, :authority, :url, :title, :scraped_at,
                :ingredient_name, :event_type, :severity, :summary,
                :source_label, :product_category, :signal_type,
                :recommended_action, :created_at,
                :domain, :company, :brand, :product_name, :allergen, :claim
            )
            """,
            {
                "source_id":         d.get("source_id", ""),
                "authority":         d.get("authority", "food"),
                "url":               d.get("url", ""),
                "title":             d.get("title", ""),
                "scraped_at":        d.get("scraped_at") or now,
                "ingredient_name":   d.get("ingredient", "") or d.get("ingredient_name", ""),
                "event_type":        d.get("signal_type", "") or d.get("event_type", ""),
                "severity":          d.get("severity", ""),
                "summary":           d.get("summary", ""),
                "source_label":      d.get("source", "") or d.get("source_label", ""),
                "product_category":  d.get("category", "") or d.get("product_category", ""),
                "signal_type":       d.get("signal_type", ""),
                "recommended_action": d.get("recommended_action", ""),
                "created_at":        now,
                "domain":            d.get("domain", "food"),
                "company":           d.get("company", ""),
                "brand":             d.get("brand", ""),
                "product_name":      d.get("product_name", ""),
                "allergen":          d.get("allergen", ""),
                "claim":             d.get("claim", ""),
            },
        )
        inserted = conn.execute("SELECT changes()").fetchone()[0] > 0
        conn.commit()
        return inserted
    finally:
        conn.close()


def save_food_signals_batch(signals: list[dict]) -> int:
    """Save a batch of food-domain dicts. Returns count of new insertions."""
    return sum(1 for s in signals if save_food_signal(s))


def save_signals_batch(signals: list["ClassifiedSignal"]) -> int:
    """Save a batch, skipping duplicates. Returns count of new insertions."""
    return sum(1 for s in signals if save_signal(s))


def update_sentiment(
    source_id: str,
    sentiment: str,
    confidence: float,
    reasoning: str,
) -> None:
    """Update the sentiment columns for an existing signal row."""
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE signals
            SET sentiment=?, sentiment_confidence=?, sentiment_reasoning=?
            WHERE source_id=?
            """,
            (sentiment, confidence, reasoning, source_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_signals_since(days: int = 30) -> list[dict]:
    """Return all signals scraped within the last `days` days, newest first."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM signals WHERE scraped_at >= ? ORDER BY scraped_at DESC",
            (cutoff,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_signals_missing_sentiment() -> list[dict]:
    """Return signals that have no sentiment classification yet."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM signals WHERE (sentiment IS NULL OR sentiment = '') ORDER BY scraped_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def signal_exists(source_id: str) -> bool:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT 1 FROM signals WHERE source_id=?", (source_id,)
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def url_exists(url: str) -> bool:
    """Check if a signal with this URL already exists (cross-source dedup)."""
    conn = get_conn()
    try:
        row = conn.execute("SELECT 1 FROM signals WHERE url=?", (url,)).fetchone()
        return row is not None
    finally:
        conn.close()


def update_ai_summary(source_id: str, ai_summary: str) -> None:
    """Write the AI business-impact summary for a signal."""
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE signals SET ai_summary=? WHERE source_id=?",
            (ai_summary, source_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_signals_missing_ai_summary() -> list[dict]:
    """Return signals that have no AI summary yet."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM signals WHERE (ai_summary IS NULL OR ai_summary = '') ORDER BY scraped_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Feedback persistence
# ---------------------------------------------------------------------------

def save_feedback(source_id: str, rating: str, correction_note: str = "") -> None:
    """Append a feedback record to both SQLite and data/feedback.json."""
    now = datetime.now(timezone.utc).isoformat()

    # SQLite
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO feedback (source_id, rating, correction_note, created_at) VALUES (?,?,?,?)",
            (source_id, rating, correction_note, now),
        )
        conn.commit()
    finally:
        conn.close()

    # JSON mirror
    records: list[dict] = []
    if FEEDBACK_JSON.exists():
        try:
            records = json.loads(FEEDBACK_JSON.read_text())
        except json.JSONDecodeError:
            records = []
    records.append({"source_id": source_id, "rating": rating, "correction_note": correction_note, "created_at": now})
    FEEDBACK_JSON.write_text(json.dumps(records, indent=2))


def get_recent_feedback(limit: int = 20) -> list[dict]:
    """Return the most recent `limit` feedback records (corrections only)."""
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT f.source_id, f.rating, f.correction_note, f.created_at,
                   s.title, s.ingredient_name, s.event_type, s.severity, s.summary
            FROM feedback f
            LEFT JOIN signals s ON s.source_id = f.source_id
            WHERE f.rating IN ('incorrect', 'partially_correct')
            ORDER BY f.created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def append_accuracy_log(run_date: str, total: int, correct: int, incorrect: int, partial: int) -> None:
    """Append a row to data/accuracy_log.csv."""
    write_header = not ACCURACY_CSV.exists()
    with open(ACCURACY_CSV, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["run_date", "total_reviewed", "correct", "incorrect", "partially_correct", "accuracy_pct"])
        acc = round(correct / total * 100, 1) if total else 0.0
        writer.writerow([run_date, total, correct, incorrect, partial, acc])
