"""Deterministic duplicate detection for Food-domain signals."""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit, urlunsplit


DUPLICATE_NOISE_PREFIX = "duplicate food signal: kept id"


@dataclass(frozen=True)
class DuplicateGroup:
    key: str
    kept_id: int
    duplicate_ids: list[int]
    reason: str
    rows: list[dict[str, Any]]


def canonical_food_url(url: str | None) -> str:
    """Return a stable URL key for duplicate detection."""
    raw = (url or "").strip().lower()
    if not raw:
        return ""
    parts = urlsplit(raw)
    path = re.sub(r"/+", "/", parts.path or "").rstrip("/")
    path = re.sub(r"-\d+$", "", path)
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def normalise_food_title(title: str | None) -> str:
    """Return a stable title key for duplicate detection."""
    text = (title or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    return re.sub(r"\s+", " ", text).strip()


def scraped_date_key(row: dict[str, Any]) -> str:
    """Return YYYY-MM-DD when available, otherwise a blank date bucket."""
    value = str(row.get("scraped_at") or row.get("created_at") or "").strip()
    if not value:
        return ""
    if len(value) >= 10 and re.match(r"\d{4}-\d{2}-\d{2}", value[:10]):
        return value[:10]
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return value[:10]


def duplicate_keys_for_row(row: dict[str, Any]) -> list[tuple[str, str]]:
    """Return priority-ordered duplicate keys for one food signal row."""
    domain = str(row.get("domain") or "").strip().lower()
    source = str(row.get("source_label") or row.get("authority") or "").strip().lower()
    if domain != "food" or not source:
        return []

    canonical_url = canonical_food_url(row.get("url"))
    title = normalise_food_title(row.get("title"))
    date_key = scraped_date_key(row)
    keys: list[tuple[str, str]] = []
    if canonical_url:
        keys.append(("url", f"{domain}|{source}|{canonical_url}"))
    if title:
        keys.append(("title_date", f"{domain}|{source}|{title}|{date_key}"))
    return keys


def _completeness_score(row: dict[str, Any]) -> int:
    fields = [
        "title",
        "summary",
        "url",
        "source_label",
        "event_type",
        "signal_type",
        "dashboard_section",
        "ai_summary",
        "recommended_action",
    ]
    return sum(1 for field in fields if row.get(field))


def _choose_keeper(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Consistently keep the lowest id, using completeness only as tie-breaker."""
    return sorted(
        rows,
        key=lambda row: (
            int(row.get("id") or 0),
            -_completeness_score(row),
        ),
    )[0]


def find_food_duplicate_groups(rows: list[dict[str, Any]]) -> list[DuplicateGroup]:
    """Find duplicate food rows using URL first, then title/date fallback."""
    assigned_duplicate_ids: set[int] = set()
    groups: list[DuplicateGroup] = []

    for key_type in ("url", "title_date"):
        buckets: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            row_id = int(row.get("id") or 0)
            if row_id in assigned_duplicate_ids:
                continue
            for candidate_type, key in duplicate_keys_for_row(row):
                if candidate_type == key_type:
                    buckets.setdefault(key, []).append(row)
                    break

        for key, bucket in buckets.items():
            unique_rows = {
                int(row.get("id") or 0): row
                for row in bucket
                if int(row.get("id") or 0) > 0
            }
            if len(unique_rows) < 2:
                continue
            group_rows = list(unique_rows.values())
            keeper = _choose_keeper(group_rows)
            kept_id = int(keeper.get("id") or 0)
            duplicate_ids = sorted(
                int(row.get("id") or 0)
                for row in group_rows
                if int(row.get("id") or 0) != kept_id
            )
            if not duplicate_ids:
                continue
            assigned_duplicate_ids.update(duplicate_ids)
            groups.append(
                DuplicateGroup(
                    key=key,
                    kept_id=kept_id,
                    duplicate_ids=duplicate_ids,
                    reason=f"{key_type} duplicate",
                    rows=sorted(group_rows, key=lambda row: int(row.get("id") or 0)),
                )
            )
    return groups


def filter_visible_food_duplicates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return rows with duplicate Food signals removed."""
    duplicate_ids = {
        duplicate_id
        for group in find_food_duplicate_groups(rows)
        for duplicate_id in group.duplicate_ids
    }
    return [row for row in rows if int(row.get("id") or 0) not in duplicate_ids]


def load_food_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    return [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM signals WHERE domain = 'food' ORDER BY scraped_at DESC"
        ).fetchall()
    ]


def mark_food_duplicates(conn: sqlite3.Connection, *, apply: bool = False) -> list[DuplicateGroup]:
    """Find duplicates and optionally mark duplicate rows as noise."""
    groups = find_food_duplicate_groups(load_food_rows(conn))
    if apply:
        for group in groups:
            reason = f"{DUPLICATE_NOISE_PREFIX} {group.kept_id}"
            for duplicate_id in group.duplicate_ids:
                conn.execute(
                    """
                    UPDATE signals
                       SET is_noise = 1,
                           noise_reason = ?
                     WHERE id = ?
                       AND domain = 'food'
                    """,
                    (reason, duplicate_id),
                )
        conn.commit()
    return groups
