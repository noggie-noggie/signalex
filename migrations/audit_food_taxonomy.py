"""
migrations/audit_food_taxonomy.py — read-only Food taxonomy audit.

Prints dashboard taxonomy counts and legacy rows that look like recalls despite
being stored under older regulatory/update labels.

Usage:
    python migrations/audit_food_taxonomy.py
"""

from __future__ import annotations

import sqlite3
from collections import Counter
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.food_taxonomy import enrich_food_signal, possible_misclassified_recall


DB_PATH = ROOT / "data" / "signals.db"


def main() -> int:
    if not DB_PATH.exists():
        print(f"ERROR: signals database not found: {DB_PATH}")
        return 1

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        raw_rows = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM signals WHERE domain = 'food' ORDER BY scraped_at DESC"
            ).fetchall()
        ]
    finally:
        conn.close()

    rows = [enrich_food_signal(row) for row in raw_rows]
    visible_rows = [
        row for row in rows
        if int(row.get("is_noise") or 0) != 1
        and row.get("dashboard_section") != "excluded"
    ]
    by_signal_type = Counter(row.get("signal_type") or "" for row in rows)
    by_dashboard_section = Counter(row.get("dashboard_section") or "" for row in visible_rows)
    missing_signal_type = [row for row in rows if not row.get("signal_type")]
    missing_dashboard_section = [row for row in rows if not row.get("dashboard_section")]
    weak_category = [row for row in rows if row.get("category") == ["other"]]
    weak_product_type = [row for row in rows if not row.get("product_type")]
    weak_examples = [
        row for row in rows
        if row.get("category") == ["other"] or not row.get("product_type")
    ][:10]
    possible_misclassified = [
        (raw, enrich_food_signal(raw))
        for raw in raw_rows
        if possible_misclassified_recall(raw)
    ]

    print("Food taxonomy audit")
    print("===================")
    print(f"total food signals: {len(rows)}")
    print(f"visible customer-facing food signals: {len(visible_rows)}")

    print("\ncount by signal_type:")
    for key, count in sorted(by_signal_type.items()):
        print(f"  {key or '(missing)'}: {count}")

    print("\nvisible count by dashboard_section:")
    for key, count in sorted(by_dashboard_section.items()):
        print(f"  {key or '(missing)'}: {count}")

    print(f"\nmissing signal_type: {len(missing_signal_type)}")
    print(f"missing dashboard_section: {len(missing_dashboard_section)}")
    print(f'category ["other"]: {len(weak_category)}')
    print(f"empty product_type: {len(weak_product_type)}")

    print("\ntop weakly classified records:")
    if not weak_examples:
        print("  none")
    else:
        for row in weak_examples:
            print(
                "  "
                f"id={row.get('id')} "
                f"category={row.get('category')} "
                f"product_type={row.get('product_type')} "
                f"title={row.get('title')}"
            )

    print("\npossible legacy regulatory/update rows now classified as recalls:")
    if not possible_misclassified:
        print("  none")
    else:
        for raw, row in possible_misclassified:
            print(
                "  "
                f"id={row.get('id')} "
                f"stored_type={raw.get('signal_type') or raw.get('event_type')} "
                f"source={row.get('source_label')} "
                f"title={row.get('title')}"
            )

    recall_leaks = [
        row for row in rows
        if row.get("signal_type") == "recall"
        and row.get("dashboard_section") != "recalls_safety"
    ]
    opportunity_leaks = [
        row for row in rows
        if row.get("dashboard_section") == "market_opportunities"
        and row.get("signal_type") in {"recall", "safety_alert", "regulatory_update", "consultation"}
    ]

    print(f"\nrecalls outside recalls_safety: {len(recall_leaks)}")
    print(f"risk/regulatory rows inside market_opportunities: {len(opportunity_leaks)}")

    return 1 if missing_signal_type or missing_dashboard_section or recall_leaks or opportunity_leaks else 0


if __name__ == "__main__":
    raise SystemExit(main())
