"""Deterministic duplicate detection for Food-domain signals."""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit, urlunsplit


DUPLICATE_NOISE_PREFIX = "duplicate food signal: kept id"
OFF_CATEGORY_DUPLICATE_REASON = "Excluded from food launch: duplicate Open Food Facts category signal"


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


def _normalise_list_value(value: Any) -> str:
    if isinstance(value, list):
        return ",".join(sorted(str(item).strip().lower() for item in value if str(item).strip()))
    return str(value or "").strip().lower()


def _brand_key(row: dict[str, Any]) -> str:
    brand = str(row.get("brand") or row.get("company") or "").strip().lower()
    if brand:
        return re.sub(r"\s+", " ", brand)
    title = str(row.get("title") or row.get("product_name") or "").lower()
    if "red bull" in title:
        return "red bull"
    if re.search(r"(^|[^a-z0-9])v([^a-z0-9]|$)", title) and "energy" in title:
        return "v"
    if "monster" in title:
        return "monster"
    title = re.split(r"\s+(?:-|–|—|â€“|â€”)\s+", title, maxsplit=1)[0]
    return re.sub(r"[^a-z0-9]+", " ", title).strip()


def _product_family_key(row: dict[str, Any]) -> str:
    text = " ".join(
        str(row.get(field) or "").lower()
        for field in ("title", "product_name", "summary", "product_category")
    )
    brand = _brand_key(row)
    if "red bull" in text or brand == "red bull":
        return "red_bull_energy_drink"
    if brand == "v" and ("energy" in text or "caffeine" in text):
        return "v_energy_drink"
    if "v energy" in text:
        return "v_energy_drink"
    if ("monster" in text or brand == "monster") and ("energy" in text or "caffeine" in text):
        return "monster_energy_drink"
    if "energy drink" in text or ("caffeine" in text and "beverage" in text):
        return "energy_drink"
    words = re.findall(r"[a-z0-9]+", text)
    stop = {"the", "and", "with", "flavour", "flavored", "natural", "ingredients"}
    return " ".join(word for word in words[:8] if word not in stop)


def _is_energy_drink_family(family: str) -> bool:
    return family in {
        "red_bull_energy_drink",
        "v_energy_drink",
        "monster_energy_drink",
        "energy_drink",
    }


def _category_family_product_signature(row: dict[str, Any], family: str) -> str:
    product_type = _normalise_list_value(row.get("product_type"))
    if _is_energy_drink_family(family):
        return "energy_drink"
    return product_type


def _category_family_claim_signature(row: dict[str, Any], family: str) -> str:
    claim_theme = _normalise_list_value(row.get("claim_theme"))
    if _is_energy_drink_family(family):
        return "energy_drink_monitoring"
    return claim_theme


def _off_category_family_key(row: dict[str, Any]) -> str:
    if str(row.get("source_label") or "").lower() != "open_food_facts":
        return ""
    if row.get("dashboard_section") != "category_signals":
        return ""
    if int(row.get("is_noise") or 0) == 1:
        return ""
    brand = _brand_key(row)
    family = _product_family_key(row)
    if not brand or not family:
        return ""
    return "|".join(
        [
            "off_category_family",
            brand,
            family,
            _category_family_claim_signature(row, family),
            _category_family_product_signature(row, family),
            str(row.get("dashboard_section") or ""),
        ]
    )


def find_open_food_facts_category_duplicate_groups(rows: list[dict[str, Any]]) -> list[DuplicateGroup]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = _off_category_family_key(row)
        if key:
            buckets.setdefault(key, []).append(row)
    groups: list[DuplicateGroup] = []
    for key, bucket in buckets.items():
        unique_rows = {
            int(row.get("id") or 0): row
            for row in bucket
            if int(row.get("id") or 0) > 0
        }
        if len(unique_rows) < 2:
            continue
        group_rows = list(unique_rows.values())
        keeper = sorted(
            group_rows,
            key=lambda row: (
                -_completeness_score(row),
                int(row.get("id") or 0),
            ),
        )[0]
        kept_id = int(keeper.get("id") or 0)
        duplicate_ids = sorted(
            int(row.get("id") or 0)
            for row in group_rows
            if int(row.get("id") or 0) != kept_id
        )
        groups.append(
            DuplicateGroup(
                key=key,
                kept_id=kept_id,
                duplicate_ids=duplicate_ids,
                reason="open_food_facts category family duplicate",
                rows=sorted(group_rows, key=lambda row: int(row.get("id") or 0)),
            )
        )
    return groups


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
    duplicate_ids.update(
        duplicate_id
        for group in find_open_food_facts_category_duplicate_groups(rows)
        for duplicate_id in group.duplicate_ids
    )
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
    rows = load_food_rows(conn)
    groups = [
        *find_food_duplicate_groups(rows),
        *find_open_food_facts_category_duplicate_groups(rows),
    ]
    if apply:
        for group in groups:
            reason = (
                OFF_CATEGORY_DUPLICATE_REASON
                if group.reason == "open_food_facts category family duplicate"
                else f"{DUPLICATE_NOISE_PREFIX} {group.kept_id}"
            )
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
