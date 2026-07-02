"""
migrations/audit_food_text_contamination.py — read-only Food text contamination audit.

Reports VMS/supplement-specific business wording in Food-facing narrative fields.

Usage:
    python migrations/audit_food_text_contamination.py
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.food_text_sanitizer import SANITIZED_FIELDS, contains_food_text_contamination
from services.food_taxonomy import enrich_food_signal


DB_PATH = ROOT / "data" / "signals.db"


def _food_rows() -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        return [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM signals WHERE domain = 'food' ORDER BY scraped_at DESC"
            ).fetchall()
        ]
    finally:
        conn.close()


def _contaminated_fields(row: dict) -> list[str]:
    return [
        field for field in SANITIZED_FIELDS
        if contains_food_text_contamination(row.get(field))
    ]


def main() -> int:
    if not DB_PATH.exists():
        print(f"ERROR: signals database not found: {DB_PATH}")
        return 1

    rows = _food_rows()
    enriched = [enrich_food_signal(row) for row in rows]
    visible_ids = {
        row.get("id")
        for row in enriched
        if int(row.get("is_noise") or 0) != 1
        and row.get("dashboard_section") != "excluded"
    }

    contaminated = [
        (row, _contaminated_fields(row))
        for row in rows
        if _contaminated_fields(row)
    ]
    visible_contaminated = [
        (row, fields)
        for row, fields in contaminated
        if row.get("id") in visible_ids
    ]

    print("Food text contamination audit")
    print("=============================")
    print(f"total food signals: {len(rows)}")
    print(f"visible customer-facing food signals: {len(visible_ids)}")
    print(f"total food rows with VMS/supplement wording: {len(contaminated)}")
    print(f"visible food rows with VMS/supplement wording: {len(visible_contaminated)}")

    print("\ncontaminated rows:")
    if not contaminated:
        print("  none")
    else:
        for row, fields in contaminated:
            visible = "yes" if row.get("id") in visible_ids else "no"
            print(
                "  "
                f"id={row.get('id')} "
                f"visible={visible} "
                f"source={row.get('source_label')} "
                f"fields={','.join(fields)} "
                f"title={row.get('title')}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
