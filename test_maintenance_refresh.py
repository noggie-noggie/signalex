"""End-to-end maintenance regressions. Synthetic collectors and disposable SQLite only."""
import contextlib
from datetime import date
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from analytics import db
from maintenance import refresh as r

DAY = date(2026, 1, 2)
RECALL = "food_fsanz_recalls"
UPDATE = "food_fsanz_updates"
OFF = "open_food_facts"


def row(s):
    title, path = {
        RECALL: ("Bread recall due to undeclared milk","https://www.foodstandards.gov.au/food-recalls/recall-alert/bread"),
        UPDATE: ("Call for comment on young child formula","https://www.foodstandards.gov.au/media/formula"),
        OFF: ("Rokeby — High Protein Smoothie","https://world.openfoodfacts.org/product/12345"),
        "pubmed": ("Creatine supplementation and exercise","https://pubmed.ncbi.nlm.nih.gov/12345/"),
    }[s]
    identity = "12345" if s == OFF else path+"::"+title[:80] if s in r.FOOD else path
    return dict(source_id=hashlib.sha256(f"{s}::{identity}".encode()).hexdigest()[:16], title=title, url=path,
                authority="fsanz" if s in (RECALL,UPDATE) else s, source_label=s,
                domain="food" if s in r.FOOD else "vms", scraped_at=str(DAY)+"T12:00:00Z",
                summary={RECALL:"The recall notice identifies undeclared milk in the affected bread.",
                         UPDATE:"FSANZ requests comments on its proposed young child formula standard.",
                         OFF:"Ingredients: low fat milk. High protein smoothie."}.get(s,title),
                signal_type={RECALL:"recall",UPDATE:"rule_update",OFF:"claim_signal"}.get(s,"research"),
                category="protein smoothies" if s==OFF else "", claim="high protein" if s==OFF else "",
                product_name=title, ingredient="Low fat milk, cocoa and honey" if s==OFF else "")


class RefreshControls(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.db = self.root/"existing.db"
        self.state = self.root/"state"
        with patch.object(db,"DB_PATH",self.db), contextlib.closing(db.get_conn()) as c, c:
            c.execute("INSERT INTO signals(domain,source_id,summary) VALUES('pharma','untouchable','original')")
        self.addCleanup(patch.stopall)
        patch.dict(os.environ,{r.ENABLE:"true"}).start()
        self.bad = set()
        self.rows = {}

    def collect(self,s,folder,dbpath,lo,hi):
        ok = s not in self.bad
        bounded = s in r.BOUNDED
        r.save(folder/(s+".json"), dict(source=s,status="parsed" if ok else "incomplete",
            error=None, rows=self.rows.get(s,[row(s)]),incomplete_queries=[] if ok else ["page2"],
            http=[["source",200 if ok else 503]], checked_at=r.utc(), since=str(lo),until=str(hi),
            coverage="query_window_verified" if bounded and ok else "not_verified"))
        return 0 if ok and bounded else 2

    def go(self, sources, publish=True, **kw):
        with contextlib.redirect_stdout(io.StringIO()):
            return r.run(self.db,self.state,sources,DAY,DAY,publish=publish,collector=self.collect,**kw)

    def query(self,sql):
        with contextlib.closing(r.connect(self.db)) as c:
            return c.execute(sql).fetchall()

    def test_food_and_vms_auto_publish_and_idempotent_replay(self):
        self.rows[RECALL]=[row(RECALL),row(RECALL)]
        sources=[RECALL,UPDATE,OFF,"pubmed"]
        self.assertEqual(self.go(sources,publish=False),2)  # snapshot coverage is explicitly partial
        self.assertEqual(self.query("SELECT count(*) FROM signals"),[(1,)])
        run = next(self.state.glob("*/plan.json")).parent.name
        self.collect = lambda *a: self.fail("replay must not collect")
        self.go(sources,replay=run)
        before=self.query("SELECT id,source_id,domain,summary FROM signals ORDER BY id")
        self.assertEqual(len(before),5)
        self.assertEqual(before[0][1:],("untouchable","pharma","original"))
        self.go(sources,replay=run)
        self.assertEqual(before,self.query("SELECT id,source_id,domain,summary FROM signals ORDER BY id"))
        self.assertEqual(self.query("SELECT count(*) FROM maintenance_refresh_receipts"),[(4,)])
        receipts=[json.loads(x[0]) for x in self.query("SELECT result FROM maintenance_refresh_receipts ORDER BY rowid")]
        with contextlib.closing(r.connect(Path(receipts[0]["backup"]))) as c:
            self.assertEqual(c.execute("PRAGMA integrity_check").fetchone(),("ok",))
            self.assertEqual(c.execute("SELECT count(*) FROM signals").fetchone(),(1,))
        self.assertEqual(receipts[0]["repeated"],1)
        self.assertEqual(receipts[2]["coverage"],"observations_only")

    def test_failed_source_no_write_and_other_sources_continue(self):
        self.bad.add(RECALL)
        self.go([RECALL,"pubmed"],daily=True)
        self.assertEqual(self.query("SELECT source FROM maintenance_refresh_receipts"),[("pubmed",)])
        report=r.read(next(self.state.glob("*/food_fsanz_recalls.apply.json")))
        self.assertEqual(report["status"],"failed")
        self.assertEqual(self.query("SELECT count(*) FROM signals WHERE domain='food'"),[(0,)])

    def test_ambiguous_and_wrong_domain_held(self):
        self.rows["pubmed"]=[row("pubmed"),dict(row("pubmed"),title="Conflicting observation")]
        self.rows[RECALL]=[dict(row(RECALL),domain="pharma")]
        self.go(["pubmed",RECALL])
        self.assertEqual(self.query("SELECT count(*) FROM signals"),[(1,)])
        self.assertEqual(len(r.read(next(self.state.glob("*/pubmed.holds.json")))),2)

    def test_backup_failure_and_out_of_scope_write_roll_back(self):
        with patch.object(r,"backup",side_effect=ValueError("backup check failed")):
            self.assertEqual(self.go([RECALL]),2)
        real=r.merge
        def unsafe(c,s,rows):
            real(c,s,rows)
            c.execute("UPDATE signals SET summary='wrong' WHERE domain='pharma'")
        with patch.object(r,"merge",side_effect=unsafe):
            self.assertEqual(self.go([RECALL]),2)
        self.assertEqual(self.query("SELECT domain,summary FROM signals"),[("pharma","original")])
        self.assertFalse(self.query("SELECT name FROM sqlite_master WHERE name='maintenance_refresh_receipts'"))

    def test_disabled_and_competing_process_with_different_state(self):
        with patch.dict(os.environ,{r.ENABLE:"false"}), self.assertRaisesRegex(ValueError,"disabled"):
            self.go([RECALL])
        with r.exclusive(self.db):
            proc=subprocess.run([sys.executable,"-m","maintenance.refresh","daily","--db",str(self.db),
                                 "--state",str(self.root/"other"),"--since",str(DAY)],
                                capture_output=True,text=True,timeout=20)
            self.assertNotEqual(proc.returncode,0)
            self.assertIn("already running",proc.stderr)
        self.assertFalse((self.root/"other").exists())

    def test_tampered_bundle_and_cross_database_replay_rejected(self):
        self.go([RECALL],publish=False)
        folder=next(self.state.glob("*/plan.json")).parent
        r.save(folder/(RECALL+".json"),{})
        self.assertEqual(self.go([RECALL],replay=folder.name),2)
        with patch.object(self,"db",self.root/"other.db"):
            self.db.touch()
            with self.assertRaisesRegex(ValueError,"another database"):
                self.go([RECALL],replay=folder.name)
        self.assertEqual(self.query("SELECT count(*) FROM signals"),[(1,)])

    def test_daily_recollection_fsanz_alias_and_gap_accounting(self):
        first = row(RECALL)
        alias = dict(first,url=first["url"]+"-0")
        alias["source_id"] = hashlib.sha256(f'{RECALL}::{alias["url"]}::{alias["title"][:80]}'.encode()).hexdigest()[:16]
        self.rows[RECALL]=[first,alias]
        self.go([RECALL],daily=True)
        original=self.query("SELECT id,source_id,created_at FROM signals ORDER BY id")
        self.go([RECALL],daily=True)
        self.assertEqual(len(original),2)
        self.assertEqual(self.query("SELECT id,source_id,created_at FROM signals ORDER BY id"),original)
        with contextlib.closing(r.connect(self.db)) as c:
            history=r.receipts(c)
        self.assertEqual(history[-1]["counts"],{"unchanged":1,"duplicate_skipped":1})
        self.assertEqual(r.gaps(RECALL,DAY,date(2026,1,4),history),[["2026-01-03","2026-01-04"]])
        self.assertEqual(r.gaps(OFF,DAY,DAY,history),[[str(DAY),str(DAY)]])

    def test_observed_food_snapshot_never_overrides_partial_page_failure(self):
        self.go([OFF])
        with contextlib.closing(r.connect(self.db)) as c:
            old=r.receipts(c)
        self.bad.add(OFF)
        self.rows[OFF]=[dict(row(OFF),summary="Replacement text from partial query")]
        self.assertEqual(self.go([OFF]),2)
        with contextlib.closing(r.connect(self.db)) as c:
            self.assertEqual(r.receipts(c),old)
            self.assertEqual(c.execute("SELECT summary FROM signals WHERE domain='food'").fetchone()[0],row(OFF)["summary"])

    def test_scoped_promotion_rolls_back_if_live_effects_changed(self):
        self.go([RECALL],publish=False)
        run=next(self.state.glob("*/plan.json")).parent.name
        raw=row(RECALL)
        with contextlib.closing(r.connect(self.db,"rw")) as c,c:
            c.execute("""INSERT INTO signals(domain,authority,source_label,source_id,url,title,summary)
                         VALUES('food','fsanz',?,?,?,?,?)""",(RECALL,raw["source_id"],raw["url"],raw["title"],"Intervening review"))
        self.assertEqual(self.go([RECALL],replay=run),2)
        self.assertEqual(self.query("SELECT summary FROM signals WHERE domain='food'"),[("Intervening review",)])


if __name__ == "__main__":
    unittest.main()
