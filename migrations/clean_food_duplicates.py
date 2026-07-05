"""Mark duplicate Food-domain signals as noise.

Dry-run by default:
    python migrations/clean_food_duplicates.py

Apply changes:
    python migrations/clean_food_duplicates.py --apply
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.food_duplicates import mark_food_duplicates


DB_PATH = ROOT / "data" / "signals.db"


def _print_groups(groups) -> None:
    print(f"duplicate_groups={len(groups)}")
    for group in groups:
        print(
            f"kept_id={group.kept_id} "
            f"duplicate_ids={group.duplicate_ids} "
            f"reason={group.reason} "
            f"key={group.key}"
        )
        for row in group.rows:
            print(
                f"  id={row.get('id')} "
                f"title={row.get('title')!r} "
                f"url={row.get('url')!r} "
                f"source_label={row.get('source_label')!r} "
                f"is_noise={row.get('is_noise')} "
                f"noise_reason={row.get('noise_reason')!r}"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Mark duplicate rows as noise.")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"ERROR: signals database not found: {DB_PATH}")
        return 1

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        groups = mark_food_duplicates(conn, apply=args.apply)
    finally:
        conn.close()

    print("mode=apply" if args.apply else "mode=dry-run")
    _print_groups(groups)
    if not args.apply:
        print("No changes made. Re-run with --apply to mark duplicates as noise.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
