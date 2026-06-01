"""
services/food_claims/cache.py — SQLite cache for food claim guidance responses.

Table: food_claim_guidance_cache
  id                  INTEGER PRIMARY KEY AUTOINCREMENT
  input_hash          TEXT UNIQUE NOT NULL
  claim               TEXT
  claim_normalized    TEXT
  food_type           TEXT
  food_type_normalized TEXT
  market              TEXT
  response_json       TEXT
  created_at          TEXT
  updated_at          TEXT

Public API
----------
make_input_hash(claim, food_type, market) -> str
get_cached_guidance(input_hash)           -> dict | None
save_guidance(input_hash, claim, food_type, market, response) -> None
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_DB_PATH = Path(__file__).parent.parent.parent / "data" / "signals.db"

# Bump this string whenever the response shape changes in a way that makes
# old cached responses invalid. Including it in the hash naturally bypasses
# all entries written under previous versions without touching the table.
CACHE_VERSION = "food_claim_guidance_v1"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS food_claim_guidance_cache (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    input_hash           TEXT    UNIQUE NOT NULL,
    claim                TEXT    NOT NULL DEFAULT '',
    claim_normalized     TEXT    NOT NULL DEFAULT '',
    food_type            TEXT    NOT NULL DEFAULT '',
    food_type_normalized TEXT    NOT NULL DEFAULT '',
    market               TEXT    NOT NULL DEFAULT '',
    response_json        TEXT    NOT NULL DEFAULT '{}',
    created_at           TEXT    NOT NULL DEFAULT '',
    updated_at           TEXT    NOT NULL DEFAULT ''
)
"""


def _normalise(text: str) -> str:
    """Lowercase, collapse whitespace, strip punctuation for stable cache keys."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_rw_conn() -> Optional[sqlite3.Connection]:
    """Return a read-write connection to signals.db, creating the cache table if needed."""
    if not _DB_PATH.exists():
        return None
    try:
        conn = sqlite3.connect(str(_DB_PATH))
        conn.row_factory = sqlite3.Row
        conn.execute(_CREATE_TABLE_SQL)
        conn.commit()
        return conn
    except Exception:
        return None


def make_input_hash(claim: str, food_type: str, market: str) -> str:
    """
    Produce a stable 24-char hex hash from the normalised input triple plus
    CACHE_VERSION. Bumping CACHE_VERSION invalidates all prior cached entries
    without requiring any table migration.
    """
    key = "\x00".join([
        CACHE_VERSION,
        _normalise(claim),
        _normalise(food_type),
        _normalise(market),
    ])
    return hashlib.sha256(key.encode()).hexdigest()[:24]


def get_cached_guidance(input_hash: str) -> Optional[dict]:
    """
    Return the cached response dict for input_hash, or None if not cached.
    """
    conn = _get_rw_conn()
    if conn is None:
        return None
    try:
        row = conn.execute(
            "SELECT response_json FROM food_claim_guidance_cache WHERE input_hash = ?",
            (input_hash,),
        ).fetchone()
        if row:
            return json.loads(row["response_json"])
        return None
    except Exception:
        return None
    finally:
        conn.close()


def save_guidance(
    input_hash: str,
    claim: str,
    food_type: str,
    market: str,
    response: dict,
) -> None:
    """
    Insert or replace the guidance response for input_hash.
    Sets created_at on first insert; always updates updated_at.
    """
    conn = _get_rw_conn()
    if conn is None:
        return
    try:
        now = _now_iso()
        # Check if row already exists (to preserve created_at)
        existing = conn.execute(
            "SELECT created_at FROM food_claim_guidance_cache WHERE input_hash = ?",
            (input_hash,),
        ).fetchone()
        created_at = existing["created_at"] if existing else now

        conn.execute(
            """
            INSERT INTO food_claim_guidance_cache
                (input_hash, claim, claim_normalized, food_type, food_type_normalized,
                 market, response_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(input_hash) DO UPDATE SET
                response_json        = excluded.response_json,
                updated_at           = excluded.updated_at
            """,
            (
                input_hash,
                claim,
                _normalise(claim),
                food_type,
                _normalise(food_type),
                market,
                json.dumps(response, ensure_ascii=False),
                created_at,
                now,
            ),
        )
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()
