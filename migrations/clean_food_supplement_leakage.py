"""
migrations/clean_food_supplement_leakage.py — sync Food supplement leakage flags.

Safe by default: dry-run mode prints planned changes and makes no updates.

Usage:
    python migrations/clean_food_supplement_leakage.py
    python migrations/clean_food_supplement_leakage.py --apply

The script never deletes rows and only touches Open Food Facts records in the
Food domain.  It synchronizes is_noise/noise_reason with the current supplement
classifier:

  - excluded/supplement-like -> is_noise=1 and supplement leakage reason
  - allowed food format      -> is_noise=0 only when the existing reason is the
                                old supplement leakage reason

Unrelated noise reasons are preserved.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.food_supplement_filter import (
    NOISE_REASON,
    supplement_filter_decision,
)


DB_PATH = ROOT / "data" / "signals.db"
_SUPPLEMENT_REASON_MARKERS = [
    "supplement leakage",
    "supplement / vms-like",
    "dietary supplement",
    "vms-like product",
]


def _is_supplement_noise_reason(reason: str | None) -> bool:
    text = (reason or "").strip().lower()
    return bool(text) and any(marker in text for marker in _SUPPLEMENT_REASON_MARKERS)


def _off_food_rows(conn: sqlite3.Connection) -> list[dict]:
    conn.row_factory = sqlite3.Row
    return [
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


def _planned_actions(rows: list[dict]) -> list[tuple[str, dict, str]]:
    actions: list[tuple[str, dict, str]] = []
    for row in rows:
        decision = supplement_filter_decision(row)
        is_noise = int(row.get("is_noise") or 0) == 1
        reason = row.get("noise_reason") or ""

        if decision.excluded:
            if not is_noise or reason != NOISE_REASON:
                actions.append(("mark_noise", row, decision.reason))
            continue

        if is_noise and _is_supplement_noise_reason(reason):
            actions.append(("clear_noise", row, decision.reason))

    return actions


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Synchronize Open Food Facts Food supplement leakage noise flags."
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
        rows = _off_food_rows(conn)
        actions = _planned_actions(rows)
        mark_noise = [item for item in actions if item[0] == "mark_noise"]
        clear_noise = [item for item in actions if item[0] == "clear_noise"]

        before_noise = conn.execute(
            """
            SELECT COUNT(*)
            FROM signals
            WHERE domain = 'food'
              AND source_label = 'open_food_facts'
              AND is_noise = 1
            """
        ).fetchone()[0]

        print("Food supplement leakage cleanup")
        print("================================")
        print("mode:", "APPLY" if args.apply else "DRY RUN")
        print(f"Open Food Facts food records: {len(rows)}")
        print(f"Open Food Facts noise rows before: {before_noise}")
        print(f"rows to mark as noise: {len(mark_noise)}")
        print(f"rows to clear supplement-noise flag: {len(clear_noise)}")

        if actions:
            print("\nplanned changes:")
            for action, row, reason in actions:
                print(
                    "  "
                    f"action={action} "
                    f"id={row.get('id')} "
                    f"is_noise={row.get('is_noise')} "
                    f"title={row.get('title')} "
                    f"reason={reason}"
                )

        if not args.apply:
            print("\nNo changes made. Re-run with --apply to synchronize flags.")
            return 0

        for action, row, _reason in actions:
            if action == "mark_noise":
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
            elif action == "clear_noise":
                conn.execute(
                    """
                    UPDATE signals
                    SET is_noise = 0,
                        noise_reason = ''
                    WHERE id = ?
                      AND domain = 'food'
                      AND source_label = 'open_food_facts'
                    """,
                    (row["id"],),
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

        print(f"\nrows marked as noise: {len(mark_noise)}")
        print(f"rows cleared from supplement-noise: {len(clear_noise)}")
        print(f"Open Food Facts noise rows after: {after_noise}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
