"""Read-only audit for duplicate Food-domain signals."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.food_duplicates import find_food_duplicate_groups, load_food_rows
from services.food_taxonomy import enrich_food_signal


DB_PATH = ROOT / "data" / "signals.db"


def main() -> int:
    if not DB_PATH.exists():
        print(f"ERROR: signals database not found: {DB_PATH}")
        return 1

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        raw_rows = load_food_rows(conn)
    finally:
        conn.close()

    groups = find_food_duplicate_groups(raw_rows)
    enriched_by_id = {int(row.get("id") or 0): enrich_food_signal(row) for row in raw_rows}
    visible_duplicate_groups = []
    for group in groups:
        visible_duplicate_ids = [
            row_id
            for row_id in [group.kept_id, *group.duplicate_ids]
            if int(enriched_by_id.get(row_id, {}).get("is_noise") or 0) != 1
            and enriched_by_id.get(row_id, {}).get("dashboard_section") != "excluded"
        ]
        if len(visible_duplicate_ids) > 1:
            visible_duplicate_groups.append((group, visible_duplicate_ids))

    print(f"food_rows={len(raw_rows)}")
    print(f"duplicate_groups_count={len(groups)}")
    print(f"visible_duplicates_count={len(visible_duplicate_groups)}")

    for group, visible_ids in visible_duplicate_groups[:50]:
        print(
            f"kept_id={group.kept_id} duplicate_ids={group.duplicate_ids} "
            f"visible_ids={visible_ids} reason={group.reason} key={group.key}"
        )
        for row in group.rows:
            print(
                f"  id={row.get('id')} "
                f"title={row.get('title')!r} "
                f"url={row.get('url')!r} "
                f"source_label={row.get('source_label')!r}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
