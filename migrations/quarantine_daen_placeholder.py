"""Hide only the legacy generated DAEN fallback, never an actual adverse event."""
import argparse
import sqlite3
from pathlib import Path

from analytics.db import DB_PATH
from scrapers.base import BaseScraper
from scrapers.adverse_events import _DAEN_URL


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--db", type=Path, help="Existing target DB; required for apply")
    p.add_argument("--backup-dir", type=Path)
    args = p.parse_args()
    if args.apply and not (args.db and args.backup_dir):
        p.error("--apply requires --db and --backup-dir")
    db = args.db or DB_PATH
    if args.apply:
        from maintenance.reconcile import backup
        backup(db, args.backup_dir)
    key = BaseScraper._make_source_id("adverse_events", _DAEN_URL + ":fallback")
    predicate = """domain='vms' AND source_label='adverse_events' AND source_id=? AND url=?
                   AND title='TGA DAEN: Complementary Medicine Adverse Event Monitoring'
                   AND coalesce(is_noise,0)=0"""
    with sqlite3.connect(db.resolve().as_uri() + ("?mode=rw" if args.apply else "?mode=ro"), uri=True) as c:
        ids = c.execute("SELECT id FROM signals WHERE " + predicate, (key, _DAEN_URL)).fetchall()
        if args.apply:
            c.execute("UPDATE signals SET is_noise=1, noise_reason='DAEN monitoring placeholder; no observed event' WHERE "
                      + predicate, (key, _DAEN_URL))
        print("excluded" if args.apply else "would exclude", ids)


if __name__ == "__main__":
    main()
