"""
migrations/clean_food_supplement_leakage.py — mark Food supplement leakage rows.

Safe by default: dry-run mode prints affected rows and makes no changes.

Usage:
    python migrations/clean_food_supplement_leakage.py
    python migrations/clean_food_supplement_leakage.py --apply

The script never deletes rows.  It only marks suspected Open Food Facts
supplement/VMS-like rows in domain='food' as noise.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.food_supplement_filter import NOISE_REASON, is_supplement_like_food


DB_PATH = ROOT / "data" / "signals.db"


def _candidate_rows(conn: sqlite3.Connection) -> list[dict]:
    conn.row_factory = sqlite3.Row
    rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT *
            FROM signals
            WHERE domain = 'food'
              AND source_label = 'open_food_facts'
            ORDER BY scraped_at DESC
            """
        ).fetchall()
    ]
    return [row for row in rows if is_supplement_like_food(row)]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Mark Open Food Facts supplement leakage rows as noise."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply updates. Omit for dry-run.",
    )
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"ERROR: signals database not found: {DB_PATH}")
        return 1

    conn = sqlite3.connect(DB_PATH)
    try:
        before_total = conn.execute(
            "SELECT COUNT(*) FROM signals WHERE domain = 'food'"
        ).fetchone()[0]
        before_noise = conn.execute(
            """
            SELECT COUNT(*)
            FROM signals
            WHERE domain = 'food'
              AND source_label = 'open_food_facts'
              AND is_noise = 1
            """
        ).fetchone()[0]
        candidates = _candidate_rows(conn)

        print("Food supplement leakage cleanup")
        print("================================")
        print("mode:", "APPLY" if args.apply else "DRY RUN")
        print(f"food total before: {before_total}")
        print(f"Open Food Facts noise rows before: {before_noise}")
        print(f"suspected supplement candidates: {len(candidates)}")

        if candidates:
            print("\ncandidates:")
            for row in candidates:
                print(
                    "  "
                    f"id={row.get('id')} "
                    f"title={row.get('title')} "
                    f"product_category={row.get('product_category')} "
                    f"is_noise={row.get('is_noise')}"
                )

        if not args.apply:
            print("\nNo changes made. Re-run with --apply to mark rows as noise.")
            return 0

        for row in candidates:
            conn.execute(
                """
                UPDATE signals
                SET is_noise = 1,
                    noise_reason = ?
                WHERE id = ?
                  AND domain = 'food'
                  AND source_label = 'open_food_facts'
                """,
                (NOISE_REASON, row["id"]),
            )
        conn.commit()

        after_noise = conn.execute(
            """
            SELECT COUNT(*)
            FROM signals
            WHERE domain = 'food'
              AND source_label = 'open_food_facts'
              AND is_noise = 1
            """
        ).fetchone()[0]

        print(f"\nrows marked as noise: {len(candidates)}")
        print(f"Open Food Facts noise rows after: {after_noise}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
