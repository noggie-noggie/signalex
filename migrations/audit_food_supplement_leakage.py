"""
migrations/audit_food_supplement_leakage.py — read-only Food supplement leakage audit.

Reports Open Food Facts rows stored under domain='food' that look like dietary
supplements/VMS products and should be hidden from the Food-only launch.

Usage:
    python migrations/audit_food_supplement_leakage.py
"""

from __future__ import annotations

import sqlite3
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.food_supplement_filter import supplement_filter_decision
from services.food_taxonomy import enrich_food_signal


DB_PATH = ROOT / "data" / "signals.db"


def _rows() -> list[dict]:
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


def main() -> int:
    if not DB_PATH.exists():
        print(f"ERROR: signals database not found: {DB_PATH}")
        return 1

    rows = _rows()
    enriched = [enrich_food_signal(row) for row in rows]
    visible = [
        row for row in enriched
        if int(row.get("is_noise") or 0) != 1
        and row.get("dashboard_section") != "excluded"
    ]
    off_rows = [row for row in rows if row.get("source_label") == "open_food_facts"]
    decisions = [(row, supplement_filter_decision(row)) for row in off_rows]
    suspected = [row for row, decision in decisions if decision.excluded]
    allowed = [row for row, decision in decisions if not decision.excluded]
    allowed_but_noise = [
        (row, decision)
        for row, decision in decisions
        if not decision.excluded and int(row.get("is_noise") or 0) == 1
    ]
    excluded_but_visible = [
        (row, decision)
        for row, decision in decisions
        if decision.excluded and int(row.get("is_noise") or 0) != 1
    ]

    print("Food supplement leakage audit")
    print("=============================")
    print(f"total food signals: {len(rows)}")
    print(f"visible customer-facing food signals: {len(visible)}")
    print(f"Open Food Facts food records: {len(off_rows)}")
    print(f"excluded Open Food Facts products: {len(suspected)}")
    print(f"allowed Open Food Facts products: {len(allowed)}")
    print(f"allowed_by_classifier_but_is_noise=1: {len(allowed_but_noise)}")
    print(f"excluded_by_classifier_but_is_noise=0: {len(excluded_but_visible)}")

    print("\ncount by source_label:")
    for source, count in sorted(Counter(row.get("source_label") or "" for row in rows).items()):
        print(f"  {source or '(missing)'}: {count}")

    print("\nvisible count by dashboard_section:")
    for section, count in sorted(Counter(row.get("dashboard_section") or "" for row in visible).items()):
        print(f"  {section or '(missing)'}: {count}")

    print("\nexcluded Open Food Facts products:")
    if not suspected:
        print("  none")
    else:
        for row, decision in decisions:
            if not decision.excluded:
                continue
            print(
                "  "
                f"id={row.get('id')} "
                f"title={row.get('title')} "
                f"product_category={row.get('product_category')} "
                f"source_label={row.get('source_label')} "
                f"is_noise={row.get('is_noise')} "
                f"reason={decision.reason}"
            )

    print("\nallowed Open Food Facts products:")
    if not allowed:
        print("  none")
    else:
        for row, decision in decisions:
            if decision.excluded:
                continue
            print(
                "  "
                f"id={row.get('id')} "
                f"title={row.get('title')} "
                f"product_category={row.get('product_category')} "
                f"is_noise={row.get('is_noise')} "
                f"reason={decision.reason}"
            )

    print("\nnoise/classifier mismatches:")
    if not allowed_but_noise and not excluded_but_visible:
        print("  none")
    else:
        for row, decision in allowed_but_noise:
            print(
                "  "
                f"type=allowed_but_noise "
                f"id={row.get('id')} "
                f"title={row.get('title')} "
                f"is_noise={row.get('is_noise')} "
                f"noise_reason={row.get('noise_reason')} "
                f"classifier_reason={decision.reason}"
            )
        for row, decision in excluded_but_visible:
            print(
                "  "
                f"type=excluded_but_visible "
                f"id={row.get('id')} "
                f"title={row.get('title')} "
                f"is_noise={row.get('is_noise')} "
                f"noise_reason={row.get('noise_reason')} "
                f"classifier_reason={decision.reason}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
