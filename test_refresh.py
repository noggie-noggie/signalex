"""Offline safety contracts for the Food/VMS maintenance path."""
from contextlib import closing
import tempfile
import json
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

from analytics import db
from maintenance.reconcile import backup, merge, prepared
from scrapers.fsanz_archive import listings
from scrapers.food_fsanz_recalls import _recall_cards
from scrapers.open_food_facts import OpenFoodFactsScraper
from services.food_taxonomy import enrich_food_signal
from maintenance.collect import retry_delay
from maintenance.vms_history import repair
from scrapers.window_pages import trial_pages, pmids, europe_pages
from services.vms_evidence import classify
from services.food_duplicates import canonical_food_url, mark_food_duplicates


class RefreshTests(unittest.TestCase):
    def test_source_facts_are_attributed_and_not_model_outputs(self):
        raw = dict(title="Creatine supplementation and resistance training", source_id="1",
                   url="https://clinicaltrials.gov/study/NCT12345678", authority="clinical_trials",
                   inspection_risk="not verified", is_noise=0)
        fact = classify("clinical_trials", raw)
        self.assertTrue(fact["summary"].startswith("ClinicalTrials.gov registration:"))
        self.assertNotIn("output_tokens", fact)
        self.assertNotIn("inspection_risk", prepared("clinical_trials", fact))
        with self.assertRaises(ValueError):
            prepared("clinical_trials", dict(fact, domain="food"))
        for changes in ({"url":"https://example.com/NCT12345678"}, {"title":"Prescription drug trial"},
                        {"title":"Oxygen supplementation in an ambulance"}, {"authority":"pubmed"}):
            self.assertIsNone(classify("clinical_trials", dict(raw, **changes)))

    def test_legacy_scheduler_does_not_require_ai_or_publish_failed_reviews(self):
        from scheduler import jobs
        raw = dict(authority="pubmed",title="Creatine supplementation",source_id="s",
                   url="https://pubmed.ncbi.nlm.nih.gov/10/",scraped_at="2026-01-01")
        with patch.dict("os.environ", {"VMS_LEGACY_AI_ENABLED":"false"}), patch.object(jobs,"SignalClassifier") as ai:
            self.assertIsNone(jobs._legacy_classifier())
            results = jobs._publishable(None,[raw,dict(raw,title="Ambiguous")],"pubmed")
            self.assertEqual(len(results),1)
            self.assertEqual(results[0].output_tokens,0)
            ai.assert_not_called()

    def test_historical_repair_is_scoped_and_repeatable(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(db,"DB_PATH",Path(directory)/"signals.db"), closing(db.get_conn()) as conn:
            for domain, title, summary in [("vms","Dietary supplement trial",""),("vms","Prescription drug",""),
                                           ("food","Prescription drug",""),("vms","Analyst assessment","Original")]:
                conn.execute("INSERT INTO signals(domain,title,summary,source_id,authority,source_label,url) VALUES(?,?,?,?,?,?,?)",
                             (domain,title,summary,domain+title,"pubmed","pubmed","https://pubmed.ncbi.nlm.nih.gov/42/"))
            stats = repair(conn)
            self.assertEqual(stats,repair(conn,True))
            self.assertEqual(repair(conn),{})
            self.assertEqual(stats["summary"],1)
            self.assertEqual(stats["is_noise"],1)
            self.assertEqual(conn.execute("SELECT summary FROM signals WHERE title='Analyst assessment'").fetchone()[0],"Original")
            self.assertFalse(conn.execute("SELECT is_noise FROM signals WHERE domain='food'").fetchone()[0])

    def test_trial_archive_reconciles_counts_and_rejects_repeats(self):
        def page(ids, **kw):
            r = Mock()
            r.json.return_value = dict(totalCount=2, studies=[{"protocolSection":{"identificationModule":{"nctId":i}}} for i in ids], **kw)
            return r
        for ids, valid in [(["b"],True), (["a"],False), ([],False)]:
            session = Mock()
            session.get.side_effect = [page(["a"],nextPageToken="next"),page(ids)]
            if valid:
                self.assertEqual(len(list(trial_pages(session,"url",{}))),2)
                self.assertEqual(session.get.call_args.kwargs["params"]["pageToken"],"next")
            else:
                with self.assertRaises(ValueError):
                    list(trial_pages(session,"url",{}))

    def test_ncbi_count_and_zero_query(self):
        def page(count, ids):
            return Mock(content=f"<eSearchResult><Count>{count}</Count><IdList>{''.join('<Id>'+i+'</Id>' for i in ids)}</IdList></eSearchResult>".encode())
        session = Mock()
        session.get.side_effect = [page(2,["1"]), page(2,["2"]), page(0,[])]
        self.assertEqual(pmids(session,"url",{}),["1","2"])
        self.assertEqual(pmids(session,"url",{}),[])
        session.get.side_effect = [page(2,["1"]),page(2,[])]
        with self.assertRaises(ValueError):
            pmids(session,"url",{})

    def test_europe_archive_does_not_accept_truncated_result(self):
        s = Mock()
        s.get.return_value.json.return_value = {"hitCount":2,"resultList":{"result":[{"source":"MED","id":"1","title":"A"}]}}
        with self.assertRaises(ValueError):
            list(europe_pages(s,"url",{}))

    def test_rate_limit_and_distinct_year_slugs(self):
        self.assertEqual(retry_delay("3600",0),3600)
        self.assertGreater(retry_delay("invalid",0),60)
        self.assertNotEqual(canonical_food_url("https://www.foodstandards.gov.au/update-2025"),
                            canonical_food_url("https://www.foodstandards.gov.au/update-2026"))

    def test_stage_never_advances_a_failed_window(self):
        from maintenance import stage_daily
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root/"existing.db"
            database.touch()
            state = root/"state"
            argv = ["stage","--db",str(database),"--state",str(state),"--first-day","2026-01-01","--source","clinical_trials"]
            with patch("sys.argv",argv), patch.object(stage_daily.subprocess,"run",return_value=Mock(returncode=2)):
                self.assertEqual(stage_daily.main(),2)
                self.assertFalse((state/"clinical_trials.cursor").exists())
            def success(cmd, **kw):
                manifest = Path(cmd[4])/"clinical_trials.json"
                manifest.write_text(json.dumps({"coverage":"query_window_verified","until":cmd[-1]}))
                return Mock(returncode=0)
            with patch("sys.argv",argv), patch.object(stage_daily.subprocess,"run",side_effect=success):
                stage_daily.main()
                self.assertEqual((state/"clinical_trials.cursor").read_text(),"2026-01-07")

    def test_consultation_navigation_is_not_evidence(self):
        from scrapers.tga_consultations import TGAConsultationsScraper
        scraper = TGAConsultationsScraper({})
        with patch.object(scraper, "_http_get",return_value='<article><a href="/">Home</a><p>Dietary supplement consultation</p></article>'):
            self.assertEqual(scraper.fetch_raw(),[])

    def test_semantic_scholar_does_not_certify_truncated_search(self):
        from scrapers.semantic_scholar import SemanticScholarScraper
        scraper = SemanticScholarScraper({})
        with patch.object(scraper, "_get_session") as session:
            session.return_value.get.return_value.json.return_value = {"total":12,"offset":0,"data":[{"paperId":"p"}]}
            with self.assertRaises(ValueError):
                list(scraper._pages({}, {}, True))

    def test_manual_exclusion_is_never_used_as_keeper(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(db,"DB_PATH",Path(directory)/"signals.db"), closing(db.get_conn()) as conn:
            for i,noise in enumerate((1,0,0),1):
                conn.execute("INSERT INTO signals(id,domain,source_label,title,url,is_noise,noise_reason) VALUES(?,'food','food_fsanz_recalls','Bread recall','https://example.org/a',?,?)",
                             (i,noise,"operator" if noise else ""))
            groups = mark_food_duplicates(conn, apply=True)
            self.assertEqual(groups[0].kept_id,2)
            self.assertEqual(conn.execute("SELECT noise_reason FROM signals WHERE id=1").fetchone()[0],"operator")

    def test_merge_is_idempotent_and_preserves_identity(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(db, "DB_PATH", Path(directory)/"signals.db"), closing(db.get_conn()) as conn:
            row = dict(domain="food", authority="fsanz", source_label="food_fsanz_recalls",
                       source_id="s", url="https://example.org/a", title="Recall", summary="First", signal_type="recall")
            source = row["source_label"]
            self.assertEqual(merge(conn, source, [row]), {"new":1})
            old = conn.execute("SELECT id,created_at FROM signals").fetchone()
            self.assertEqual(merge(conn, source, [dict(row, source_id="alias", summary="Updated")]), {"updated":1})
            self.assertEqual(merge(conn, source, [dict(row, summary="Updated")]), {"unchanged":1})
            self.assertEqual(tuple(conn.execute("SELECT id,created_at FROM signals").fetchone()), tuple(old))
            conn.commit()
            self.assertTrue(backup(db.DB_PATH, Path(directory)/"backups").is_file())
            conn.execute("UPDATE signals SET domain='pharma'")
            with self.assertRaises(ValueError):
                merge(conn, source, [row])

    def test_no_vms_fallback_and_domain_scoped_enrichment(self):
        with self.assertRaises(ValueError):
            prepared("tga", {"summary":"", "output_tokens":0})
        with tempfile.TemporaryDirectory() as directory, patch.object(db, "DB_PATH", Path(directory)/"signals.db"):
            with closing(db.get_conn()) as conn, conn:
                conn.executemany("INSERT INTO signals(domain) VALUES(?)", [(d,) for d in ("food","pharma","vms")])
            for fn in [db.get_signals_missing_ai_summary, db.get_signals_missing_sentiment]:
                self.assertEqual([r["domain"] for r in fn()], ["vms"])

    def test_archive_walk_uses_source_date_and_hazard(self):
        html = """<article class="recall-card"><h3><a href="/food-recalls/recall-alert/a">Bread</a></h3>
        <p class="recall-card__business">Baker</p><span class="recall-card__type-label">Undeclared allergen</span>
        <span class="recall-card__date">19 November 2026</span></article>"""
        s = Mock()
        s.get.side_effect = [Mock(content=(html+'<a rel="next" href="?page=1">Next</a>').encode()),
                             Mock(content=html.replace("November","July").encode())]
        since = date(2026,8,24)
        with patch("scrapers.fsanz_archive.time.sleep"):
            rows = [r for soup in listings(s, "https://www.foodstandards.gov.au/food-recalls/recall-alert",
                                           since, ".recall-card__date") for r in _recall_cards(soup,since)]
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]["company"],"Baker")
        self.assertTrue(rows[0]["scraped_at"].startswith("2026-11-19"))
        self.assertIn("Undeclared allergen",rows[0]["summary"])

    def test_off_rejects_ignored_query_parameters(self):
        s = Mock()
        p = dict(code="111", product_name="Protein bar", countries_tags=["en:france"],categories_tags=["en:protein-bars"])
        s.get.return_value.json.return_value = {"products":[p,dict(p,countries_tags=["en:australia"],categories_tags=["en:snacks"])]}
        result, done = OpenFoodFactsScraper()._fetch_products(s, {"categories_tags":"en:protein-bars","page_size":20})
        self.assertEqual(result,[])
        self.assertTrue(done)

    def test_raw_ingredients_are_not_a_protein_claim(self):
        r = enrich_food_signal(dict(domain="food", title="Obento — Panko bread crumbs", source_label="open_food_facts",
                                    summary="Ingredients: wheat flour, soy protein, natural flavour", event_type="new_product"))
        self.assertNotIn("high_protein",r["claim_theme"])
        self.assertNotEqual(r["dashboard_section"],"market_opportunities")

    def test_gm_wheat_consultation_is_not_a_recall(self):
        row = enrich_food_signal(dict(domain="food", source_label="food_fsanz_updates",
                                     title="Call for comment on GM wheat", summary="The food safety assessment is open for comment."))
        self.assertEqual(row["fsanz_content_type"], "consultation")
        self.assertEqual(row["signal_type"], "consultation")

    def test_vegetarian_energy_drink_is_a_product_observation(self):
        row = enrich_food_signal(dict(domain="food", source_label="open_food_facts",
                                     title="Red Bull Energy Drink", claim="vegetarian", brand="Red Bull",
                                     summary="Ingredients: water, sugar, caffeine.", signal_type="claim_signal"))
        self.assertEqual(row["dashboard_section"], "category_signals")
        self.assertIn("not a confirmed market trend",row["summary"])

    def test_unavailable_daen_does_not_generate_a_signal(self):
        from scrapers.adverse_events import AdverseEventsScraper
        scraper = AdverseEventsScraper({})
        with patch.object(scraper, "_http_get", side_effect=ConnectionError):
            self.assertEqual(scraper._scrape_daen(), [])


if __name__ == "__main__":
    unittest.main()
