"""Preview VMS-only repairs. Never create, replace, delete or renumber signals."""
import argparse
from collections import Counter
from pathlib import Path
import sqlite3

from maintenance.reconcile import backup
from services.vms_evidence import historical_changes


def repair(conn, apply=False):
    conn.row_factory = sqlite3.Row
    counts = Counter()
    for record in conn.execute("""SELECT * FROM signals WHERE domain='vms' AND coalesce(is_noise,0)=0
            AND coalesce(source_label,'') NOT IN ('europe_pmc','biorxiv')
            ORDER BY id"""):
        edits = historical_changes(dict(record))
        counts.update(edits.keys())
        if apply and edits:
            conn.execute("UPDATE signals SET " + ",".join(k+"=?" for k in edits) +
                         " WHERE id=? AND domain='vms' AND coalesce(is_noise,0)=0", (*edits.values(), record["id"]))
    return counts


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path, required=True)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--backup-dir", type=Path)
    a = p.parse_args()
    if a.apply:
        if not a.backup_dir:
            p.error("--apply requires --backup-dir")
        backup(a.db, a.backup_dir)
    with sqlite3.connect(a.db.resolve().as_uri() + ("?mode=rw" if a.apply else "?mode=ro"), uri=True) as conn:
        if a.apply:
            conn.execute("BEGIN IMMEDIATE")
        print("applied" if a.apply else "preview", dict(repair(conn, a.apply)))


if __name__ == "__main__":
    main()
