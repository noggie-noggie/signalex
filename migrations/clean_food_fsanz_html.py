"""
migrations/clean_food_fsanz_html.py — Strip raw HTML from food_fsanz_updates summaries.

Background
----------
The FSANZ RSS <description> element contains raw HTML with schema.org author
annotations (<span typeof="schema:Person">, <time>, etc.).  The initial
ingestion stored this markup verbatim in the summary column.

This migration re-parses every affected summary through BeautifulSoup,
removes author noise, and writes back clean plain text.

Scope
-----
  - Only rows: domain='food' AND source_label='food_fsanz_updates'
  - Only rows: summary LIKE '%<%'  (contains an HTML tag)
  - Fields updated: summary only
  - Fields never touched: id, source_id, title, url, domain, source_label,
    event_type, authority, scraped_at, severity, and all other columns

Usage
-----
    python migrations/clean_food_fsanz_html.py          # apply
    python migrations/clean_food_fsanz_html.py --dry-run  # preview, no write

Exit codes
----------
  0 — migration applied (or nothing to do on re-run)
  1 — error (transaction rolled back)
  2 — dry-run, no changes written
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent
_DB_PATH   = _REPO_ROOT / "data" / "signals.db"

# ---------------------------------------------------------------------------
# HTML stripping — identical logic to scrapers/food_fsanz_updates.py
# ---------------------------------------------------------------------------

def _strip_html(text: str) -> str:
    """Strip HTML tags and schema.org author noise from a text fragment."""
    if not text or "<" not in text:
        return text.strip()[:1000]

    from bs4 import BeautifulSoup  # local import — not needed at module level

    soup = BeautifulSoup(text, "lxml")

    # Remove schema.org Person annotations (internal FSANZ usernames)
    for el in soup.find_all(attrs={"typeof": "schema:Person"}):
        el.decompose()
    # Remove spans that became empty after the above
    for el in soup.find_all("span"):
        if not el.get_text(strip=True):
            el.decompose()

    clean = soup.get_text(separator=" ", strip=True)
    clean = re.sub(r"\s+", " ", clean).strip()
    clean = clean[:1000]
    # Remove any dangling partial tag the 1000-char cap may have exposed
    clean = re.sub(r"<[^>]*$", "", clean).strip()
    return clean


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(dry_run: bool = False) -> int:
    if not _DB_PATH.exists():
        print(f"ERROR: database not found at {_DB_PATH}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row

    try:
        # Identify affected rows
        rows = conn.execute(
            "SELECT id, summary FROM signals"
            " WHERE domain='food'"
            "   AND source_label='food_fsanz_updates'"
            "   AND summary LIKE '%<%'"
        ).fetchall()

        total_affected = len(rows)
        print(f"  Rows with HTML in summary: {total_affected}")

        if total_affected == 0:
            print("  Already clean — nothing to do.")
            return 0

        # Build cleaned versions and show before/after samples
        updates: list[tuple[str, int]] = []
        for r in rows:
            clean = _strip_html(r["summary"])
            updates.append((clean, r["id"]))

        print(f"\n  Before/after samples (first 3 affected rows):")
        for i, (row, (clean, _)) in enumerate(zip(rows[:3], updates[:3])):
            print(f"\n  [{i+1}] id={row['id']}")
            print(f"    BEFORE: {repr(row['summary'][:120])}")
            print(f"    AFTER:  {repr(clean[:120])}")

        if dry_run:
            print(f"\n  DRY RUN — {total_affected} rows would be updated, no changes written.")
            return 2

        # Apply in a transaction
        conn.execute("BEGIN")
        try:
            conn.executemany(
                "UPDATE signals SET summary=? WHERE id=?",
                updates,
            )
            conn.execute("COMMIT")
        except Exception as exc:
            conn.execute("ROLLBACK")
            print(f"\nERROR: transaction rolled back — {exc}", file=sys.stderr)
            return 1

        # Verify no HTML remains
        remaining = conn.execute(
            "SELECT COUNT(*) FROM signals"
            " WHERE domain='food'"
            "   AND source_label='food_fsanz_updates'"
            "   AND summary LIKE '%<%'"
        ).fetchone()[0]

        print(f"\n  Rows updated:             {len(updates)}")
        print(f"  Rows still containing HTML: {remaining}")

        if remaining:
            print("  WARNING: some rows still contain HTML — manual review needed.",
                  file=sys.stderr)
            return 1

        print("  OK — all food_fsanz_updates summaries are clean.")
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Strip raw HTML from food_fsanz_updates summary fields"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing to the database.",
    )
    args = parser.parse_args()

    print("=" * 55)
    print("  Clean food_fsanz_updates HTML migration")
    print(f"  DB: {_DB_PATH}")
    print("=" * 55)

    code = run(dry_run=args.dry_run)
    print("=" * 55)
    sys.exit(code)
