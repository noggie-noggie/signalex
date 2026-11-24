"""Verify a VMS-only maintenance transaction against its own SQLite backup, read-only."""
import argparse
from pathlib import Path
import sqlite3


def verify(db, before):
    with sqlite3.connect(db.resolve().as_uri()+"?mode=ro", uri=True) as c:
        c.execute("ATTACH DATABASE ? AS prior", (before.resolve().as_uri()+"?mode=ro",))
        if c.execute("PRAGMA integrity_check").fetchall() != [("ok",)] or c.execute("PRAGMA foreign_key_check").fetchone():
            raise ValueError("Database integrity failure")
        for a,b in [("main","prior"),("prior","main")]:
            if c.execute(f"SELECT * FROM {a}.signals WHERE domain IS NOT 'vms' EXCEPT SELECT * FROM {b}.signals LIMIT 1").fetchone():
                raise ValueError("A non-VMS row changed")
        if c.execute("SELECT id,source_id,domain,created_at FROM prior.signals EXCEPT SELECT id,source_id,domain,created_at FROM main.signals LIMIT 1").fetchone():
            raise ValueError("Lost or changed original identity")
        for schema in ("prior","main"):
            base = f" FROM {schema}.signals WHERE domain='vms'"
            print(schema, "VMS by source:", dict(c.execute("SELECT coalesce(nullif(source_label,''),authority),count(*)"+base+" GROUP BY 1")))
            visible = base+" AND coalesce(is_noise,0)=0 AND source_label NOT IN ('biorxiv','europe_pmc')"
            print(schema, "eligible, missing summary, missing action, missing why:",
                  c.execute("SELECT count(*),sum(trim(coalesce(summary,''))=''),sum(trim(coalesce(recommended_action,''))=''),sum(trim(coalesce(why_it_matters,''))='')"+visible).fetchone())
        print("PASS: original identities retained; all non-VMS rows identical")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", required=True, type=Path)
    p.add_argument("--before", required=True, type=Path)
    a = p.parse_args()
    verify(a.db, a.before)
