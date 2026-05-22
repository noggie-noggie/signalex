"""
migrations/backfill_vms_domain.py — Backfill domain='vms' for untagged signals.

Background
----------
The domain column was added to data/signals.db after initial data collection.
Food signals were tagged at ingestion (domain='food').  All other signals
originated from VMS/supplement intelligence scrapers and must be tagged
domain='vms' so the API can filter by domain correctly.

Rules (deterministic, safe to re-run)
--------------------------------------
Rule 1 — known VMS source labels:
  UPDATE signals SET domain='vms'
  WHERE (domain IS NULL OR domain='')
    AND source_label IN (
      'artg','pubmed','clinical_trials','europe_pmc','cochrane',
      'biorxiv','semantic_scholar','tga_consultations','adverse_events','efsa'
    )

Rule 2 — early-pipeline TGA/FDA rows (source_label was not yet populated):
  UPDATE signals SET domain='vms'
  WHERE (domain IS NULL OR domain='')
    AND (source_label IS NULL OR source_label='')
    AND LOWER(authority) IN ('tga','fda')

Both statements are wrapped in a single transaction.
Rows already tagged (domain='food', domain='vms', etc.) are never touched.

Usage
-----
    python migrations/backfill_vms_domain.py         # apply
    python migrations/backfill_vms_domain.py --dry-run  # preview only

Exit codes
----------
  0  — migration applied (or already fully applied on re-run)
  1  — empty/null domain rows remain after migration (unexpected)
  2  — dry-run: rows that would be updated are printed, no changes made
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).parent.parent
_DB_PATH   = _REPO_ROOT / "data" / "signals.db"

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
_EMPTY_DOMAIN = "(domain IS NULL OR domain='')"

_RULE1_WHERE = (
    f"{_EMPTY_DOMAIN} "
    "AND source_label IN ("
    "'artg','pubmed','clinical_trials','europe_pmc','cochrane',"
    "'biorxiv','semantic_scholar','tga_consultations','adverse_events','efsa'"
    ")"
)
_RULE2_WHERE = (
    f"{_EMPTY_DOMAIN} "
    "AND (source_label IS NULL OR source_label='') "
    "AND LOWER(authority) IN ('tga','fda')"
)

_RULE1_UPDATE = f"UPDATE signals SET domain='vms' WHERE {_RULE1_WHERE}"
_RULE2_UPDATE = f"UPDATE signals SET domain='vms' WHERE {_RULE2_WHERE}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count(conn: sqlite3.Connection, where: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM signals WHERE {where}").fetchone()[0]


def _domain_summary(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        "SELECT COALESCE(NULLIF(domain,''),'(empty)') AS d, COUNT(*) n "
        "FROM signals GROUP BY d ORDER BY d"
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _print_summary(label: str, summary: dict[str, int]) -> None:
    print(f"\n  {label}")
    for domain, count in summary.items():
        marker = " <-- needs backfill" if domain == "(empty)" else ""
        print(f"    {domain:<12}  {count:>6}{marker}")
    print(f"    {'TOTAL':<12}  {sum(summary.values()):>6}")


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
        # ---- Before state ------------------------------------------------
        before = _domain_summary(conn)
        _print_summary("Before", before)

        rule1_pending = _count(conn, _RULE1_WHERE)
        rule2_pending = _count(conn, _RULE2_WHERE)

        print(f"\n  Pending updates:")
        print(f"    Rule 1 (source_label match)  {rule1_pending:>6} rows")
        print(f"    Rule 2 (authority fallback)   {rule2_pending:>6} rows")

        if rule1_pending == 0 and rule2_pending == 0:
            print("\n  Already fully applied — nothing to do.")
            # Verify no strays remain
            empty = _count(conn, _EMPTY_DOMAIN)
            if empty:
                print(f"\n  WARNING: {empty} row(s) with empty domain remain "
                      f"and are not covered by either rule.", file=sys.stderr)
                return 1
            return 0

        if dry_run:
            print("\n  DRY RUN — no changes written.")
            return 2

        # ---- Apply in a transaction --------------------------------------
        conn.execute("BEGIN")
        try:
            r1 = conn.execute(_RULE1_UPDATE)
            r2 = conn.execute(_RULE2_UPDATE)
            conn.execute("COMMIT")
        except Exception as exc:
            conn.execute("ROLLBACK")
            print(f"\nERROR: transaction rolled back — {exc}", file=sys.stderr)
            return 1

        # ---- After state -------------------------------------------------
        after = _domain_summary(conn)
        _print_summary("After", after)

        print(f"\n  Rows updated:")
        print(f"    Rule 1  {r1.rowcount:>6}")
        print(f"    Rule 2  {r2.rowcount:>6}")
        print(f"    Total   {r1.rowcount + r2.rowcount:>6}")

        # ---- Verify no empty domain rows remain --------------------------
        empty_after = _count(conn, _EMPTY_DOMAIN)
        if empty_after:
            print(
                f"\n  ERROR: {empty_after} row(s) still have empty domain "
                f"after migration — manual review required.",
                file=sys.stderr,
            )
            return 1

        print("\n  OK — zero empty/null domain rows remain.")
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill domain='vms' for untagged signals in data/signals.db"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print pending row counts without making any changes.",
    )
    args = parser.parse_args()

    print("=" * 55)
    print("  VMS domain backfill migration")
    print(f"  DB: {_DB_PATH}")
    print("=" * 55)

    code = run(dry_run=args.dry_run)
    print("=" * 55)
    sys.exit(code)
