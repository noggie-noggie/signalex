"""
migrations/clean_food_text_contamination.py — clean VMS wording from Food rows.

Safe by default: dry-run mode reports changes and does not update the database.

Usage:
    python migrations/clean_food_text_contamination.py
    python migrations/clean_food_text_contamination.py --apply
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.food_text_sanitizer import (
    CLEAR_IF_CONTAMINATED_FIELDS,
    contains_food_text_contamination,
    food_safe_recommended_action,
    sanitize_food_summary,
)
from services.food_taxonomy import classify_food_signal


DB_PATH = ROOT / "data" / "signals.db"


def _food_rows(conn: sqlite3.Connection) -> list[dict]:
    conn.row_factory = sqlite3.Row
    return [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM signals WHERE domain = 'food' ORDER BY scraped_at DESC"
        ).fetchall()
    ]


def _updates_for_row(row: dict) -> dict[str, str]:
    taxonomy = classify_food_signal(row)
    enriched = {**row, **taxonomy}
    updates: dict[str, str] = {}

    for field in CLEAR_IF_CONTAMINATED_FIELDS:
        if contains_food_text_contamination(row.get(field)):
            updates[field] = ""

    if contains_food_text_contamination(row.get("recommended_action")):
        updates["recommended_action"] = food_safe_recommended_action(enriched)

    if contains_food_text_contamination(row.get("summary")):
        updates["summary"] = sanitize_food_summary(row.get("summary"))

    return updates


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clean VMS/supplement business wording from Food rows."
    )
    parser.add_argument("--apply", action="store_true", help="Apply updates. Omit for dry-run.")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"ERROR: signals database not found: {DB_PATH}")
        return 1

    conn = sqlite3.connect(DB_PATH)
    try:
        rows = _food_rows(conn)
        candidates = [
            (row, _updates_for_row(row))
            for row in rows
            if _updates_for_row(row)
        ]

        print("Food text contamination cleanup")
        print("================================")
        print("mode:", "APPLY" if args.apply else "DRY RUN")
        print(f"total food signals: {len(rows)}")
        print(f"rows needing cleanup: {len(candidates)}")

        if candidates:
            print("\ncandidates:")
            for row, updates in candidates:
                print(
                    "  "
                    f"id={row.get('id')} "
                    f"fields={','.join(updates)} "
                    f"title={row.get('title')}"
                )

        if not args.apply:
            print("\nNo changes made. Re-run with --apply to clean rows.")
            return 0

        for row, updates in candidates:
            assignments = ", ".join(f"{field} = ?" for field in updates)
            values = list(updates.values()) + [row["id"]]
            conn.execute(
                f"UPDATE signals SET {assignments} WHERE id = ? AND domain = 'food'",
                values,
            )
        conn.commit()

        print(f"\nrows cleaned: {len(candidates)}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
