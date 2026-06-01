"""
services/food_claims/retriever.py — Retrieve supporting evidence from signals.db.

No AI. Parameterised SQL only. Never crashes on missing columns.

Public API
----------
retrieve_supporting_signals(claim: str, theme: str | None) -> dict
    Returns competitor_examples, related_rules, related_evidence (max 5 each).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

_DB_PATH = Path(__file__).parent.parent.parent / "data" / "signals.db"

_MAX_PER_CATEGORY = 5

# ---------------------------------------------------------------------------
# Columns that may or may not exist — checked at query time
# ---------------------------------------------------------------------------
_OPTIONAL_COLS = {"ingredient_name", "product_name", "brand", "claim", "summary"}


def _get_conn() -> Optional[sqlite3.Connection]:
    """Return read-only connection, or None if DB is unavailable."""
    if not _DB_PATH.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{_DB_PATH}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None


def _get_columns(conn: sqlite3.Connection) -> set[str]:
    """Return set of column names in the signals table."""
    try:
        rows = conn.execute("PRAGMA table_info(signals)").fetchall()
        return {r[1] for r in rows}
    except Exception:
        return set()


def _build_keyword_clause(
    search_text: str,
    available_cols: set[str],
    params: list,
) -> str:
    """
    Build a WHERE fragment that searches search_text across available text columns.
    Returns empty string if no columns are available for searching.
    Appends required params in-place.
    """
    search_cols = []
    for col in ["title", "summary", "ingredient_name", "product_name", "brand", "claim"]:
        if col in available_cols:
            search_cols.append(col)

    if not search_cols or not search_text.strip():
        return ""

    pattern = f"%{search_text.strip().lower()}%"
    fragments = [f"LOWER({col}) LIKE ?" for col in search_cols]
    params.extend([pattern] * len(fragments))
    return "(" + " OR ".join(fragments) + ")"


def _row_to_dict(row: sqlite3.Row) -> dict:
    """Convert sqlite3.Row to plain dict with only non-empty string fields."""
    d = dict(row)
    return {k: v for k, v in d.items() if v is not None and v != ""}


def _search_signals(
    conn: sqlite3.Connection,
    available_cols: set[str],
    domain: str,
    source_label: str,
    keyword: str,
    limit: int,
) -> list[dict]:
    """
    Search signals for a given domain + source_label using keyword.
    Returns up to `limit` results as plain dicts.
    """
    params: list = [domain, source_label]
    keyword_clause = _build_keyword_clause(keyword, available_cols, params)

    where = "WHERE domain = ? AND source_label = ?"
    if keyword_clause:
        where += f" AND {keyword_clause}"

    sql = (
        f"SELECT id, source_id, title, summary, url, scraped_at, severity, "
        f"{'ingredient_name, ' if 'ingredient_name' in available_cols else ''}"
        f"{'product_name, ' if 'product_name' in available_cols else ''}"
        f"{'brand, ' if 'brand' in available_cols else ''}"
        f"source_label, domain "
        f"FROM signals {where} "
        f"ORDER BY scraped_at DESC LIMIT ?"
    )
    params.append(limit)

    try:
        rows = conn.execute(sql, params).fetchall()
        return [_row_to_dict(r) for r in rows]
    except Exception:
        return []


def _search_vms(
    conn: sqlite3.Connection,
    available_cols: set[str],
    keyword: str,
    limit: int,
) -> list[dict]:
    """Search VMS signals (any source_label) for related evidence."""
    params: list = ["vms"]
    keyword_clause = _build_keyword_clause(keyword, available_cols, params)

    where = "WHERE domain = 'vms'"
    if "is_noise" in available_cols:
        where += " AND (is_noise = 0 OR is_noise IS NULL)"
    if keyword_clause:
        where += f" AND {keyword_clause}"

    sql = (
        f"SELECT id, source_id, title, summary, url, scraped_at, severity, source_label, "
        f"{'ingredient_name, ' if 'ingredient_name' in available_cols else ''}"
        f"domain "
        f"FROM signals {where} "
        f"ORDER BY scraped_at DESC LIMIT ?"
    )
    params.append(limit)

    try:
        rows = conn.execute(sql, params).fetchall()
        return [_row_to_dict(r) for r in rows]
    except Exception:
        return []


def retrieve_supporting_signals(claim: str, theme: Optional[str]) -> dict:
    """
    Retrieve up to 5 records from each of three categories:
      competitor_examples — food domain, open_food_facts source
      related_rules       — food domain, food_fsanz_updates source
      related_evidence    — vms domain (any source, noise excluded)

    Parameters
    ----------
    claim : str         — raw claim text for keyword search
    theme : str | None  — theme name (e.g. "gut_health") used as fallback keyword

    Returns
    -------
    dict with keys: competitor_examples, related_rules, related_evidence
    Each is a list[dict] of signal records (max 5 items).
    """
    empty = {
        "competitor_examples": [],
        "related_rules":       [],
        "related_evidence":    [],
    }

    conn = _get_conn()
    if conn is None:
        return empty

    try:
        available_cols = _get_columns(conn)

        # Build search keyword: prefer claim text; fall back to theme name
        keyword = claim.strip() if claim and claim.strip() else (theme or "")
        # Use a shorter keyword for theme-only fallback (replace underscores with spaces)
        if not claim.strip() and theme:
            keyword = theme.replace("_", " ")

        competitor_examples = _search_signals(
            conn, available_cols,
            domain="food",
            source_label="open_food_facts",
            keyword=keyword,
            limit=_MAX_PER_CATEGORY,
        )

        related_rules = _search_signals(
            conn, available_cols,
            domain="food",
            source_label="food_fsanz_updates",
            keyword=keyword,
            limit=_MAX_PER_CATEGORY,
        )

        related_evidence = _search_vms(
            conn, available_cols,
            keyword=keyword,
            limit=_MAX_PER_CATEGORY,
        )

        return {
            "competitor_examples": competitor_examples,
            "related_rules":       related_rules,
            "related_evidence":    related_evidence,
        }

    except Exception:
        return empty
    finally:
        conn.close()
