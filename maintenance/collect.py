"""Fetch ONE source to an exclusive staging file. No classification, alerts or live DB writes.

Example: python -m maintenance.collect food_fsanz_recalls /tmp/private-run
The manifest records parser output, not a claim of archive completeness.
"""
import argparse
import importlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import time
from datetime import date, datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from tempfile import NamedTemporaryFile
from urllib.parse import urlsplit
from unittest.mock import patch

import requests

import config
from analytics import db


SOURCES = {
    "food_fsanz_recalls": "food_fsanz_recalls.FoodFSANZRecallsScraper",
    "food_fsanz_updates": "food_fsanz_updates.FoodFSANZUpdatesScraper",
    "open_food_facts": "open_food_facts.OpenFoodFactsScraper",
    "tga": "tga.TGAScraper", "fda": "fda.FDAScraper", "artg": "artg.ARTGScraper",
    "tga_consultations": "tga_consultations.TGAConsultationsScraper",
    "advisory_committee": "advisory_committees.AdvisoryCommitteesScraper",
    "adverse_events": "adverse_events.AdverseEventsScraper",
    "pubmed": "pubmed.PubMedScraper", "europe_pmc": "europe_pmc.EuropePMCScraper",
    "cochrane": "cochrane.CochraneScraper", "clinical_trials": "clinical_trials.ClinicalTrialsScraper",
    "who_ictrp": "who_ictrp.WHOICTRPScraper", "efsa": "efsa_journal.EFSAJournalScraper",
    "biorxiv": "biorxiv.BiorxivScraper", "semantic_scholar": "semantic_scholar.SemanticScholarScraper",
    "iherb": "retail.iHerbScraper", "chemist_warehouse": "retail.ChemistWarehouseScraper",
}
FOOD = {"food_fsanz_recalls", "food_fsanz_updates", "open_food_facts"}
WINDOW_SOURCES = {"clinical_trials", "pubmed", "cochrane", "europe_pmc", "biorxiv", "semantic_scholar", "tga_consultations"}


def retry_delay(value, attempt):
    if value:
        try:
            return max(0, int(value))
        except ValueError:
            try:
                return max(0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
            except (ValueError, TypeError, OverflowError):
                return 61  # ambiguous server instruction: pause, don't hammer
    return 2 ** (attempt + 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", choices=SOURCES)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--days", type=int, default=92)
    parser.add_argument("--since", type=date.fromisoformat)
    parser.add_argument("--until", type=date.fromisoformat)
    parser.add_argument("--db", type=Path, default=db.DB_PATH)
    parser.add_argument("--off-category", choices=("protein-bars", "energy-drinks", "plant-based-foods-and-beverages"))
    args = parser.parse_args()
    source = args.source
    today = datetime.now(timezone.utc).date()
    until = args.until or today
    since = args.since or until - timedelta(days=args.days)
    if since > until or until > today or args.days < 0 or not args.db.is_file():
        parser.error("Require existing DB and valid non-future date interval")
    if (args.since or args.until) and source not in WINDOW_SOURCES | FOOD:
        parser.error("Explicit bounded archive windows are not supported for this source")
    os.umask(0o077)
    args.directory.mkdir(exist_ok=True, parents=True)
    module, name = SOURCES[source].split(".")
    mod = importlib.import_module("scrapers." + module)
    scraper = getattr(mod, name)() if source in FOOD else getattr(mod, name)(config.SCRAPER_CONFIG.get(source, {}))
    if source in WINDOW_SOURCES:
        scraper.window = since, until
    if args.off_category:
        if source != "open_food_facts":
            parser.error("--off-category is only valid for Open Food Facts")
        scraper.categories = (args.off_category,)
        scraper.archive_page_size = 40
    # Only expand windows; keep existing per-page caps/rate limits.
    config.SIGNAL_LOOKBACK_DAYS = max(config.SIGNAL_LOOKBACK_DAYS, args.days)
    for k in ("_LOOKBACK_DAYS", "_ARTG_LOOKBACK_DAYS"):
        if hasattr(mod, k):
            setattr(mod, k, max(getattr(mod, k), args.days))
    http = []
    paused = set()
    last_request = {}
    send = requests.Session.send

    def observed(session, req, **kw):
        host = urlsplit(req.url).hostname
        if host in paused:
            raise requests.RequestException("Host paused for this run")
        # OFF search permits far fewer requests than product detail.
        delay = 7 if host and host.endswith("openfoodfacts.org") else 0.5
        time.sleep(max(0, delay - (time.monotonic() - last_request.get(host, 0))))
        last_request[host] = time.monotonic()
        for attempt in range(3):
            try:
                resp = send(session, req, **kw)
            except (requests.Timeout, requests.ConnectionError) as exc:
                if req.method != "GET" or attempt == 2:
                    http.append([host, type(exc).__name__])
                    raise
                time.sleep(max(delay, retry_delay(None, attempt)))
                continue
            wait = retry_delay(resp.headers.get("Retry-After"), attempt)
            if (req.method == "GET" and resp.status_code in (429,502,503,504) and
                    attempt < 2 and wait <= 60):
                resp.close()
                time.sleep(max(delay, wait))
                continue
            http.append([host, resp.status_code])
            last_request[host] = time.monotonic()
            if resp.status_code in (401,403,429) or (resp.status_code >= 400 and resp.headers.get("Retry-After")):
                paused.add(host)
            return resp

    # Dedupe/lookups in scientific scrapers normally call a schema-writing
    # connection factory. Use a read-only snapshot for this collection.
    def read_conn():
        return sqlite3.connect(args.db.resolve().as_uri() + "?mode=ro", uri=True)

    out = args.directory / (source + ".json")
    if out.exists():
        parser.error("Manifest already exists; use a fresh staging directory")
    with NamedTemporaryFile(mode="w", dir=args.directory, suffix=".partial", delete=False, encoding="utf8") as stream:
        error = None
        rows = []
        try:
            with patch.object(db, "get_conn", read_conn), patch.object(requests.Session, "send", observed):
                if source in FOOD:
                    rows = scraper.run(since=since)
                    rows = [r for r in rows if since <= date.fromisoformat(r["scraped_at"][:10]) <= until]
                else:
                    rows = scraper.run()
        except Exception as exc:
            error = type(exc).__name__
            if type(exc) is ValueError and str(exc).startswith(("EFetch ", "Invalid PubMed ", "PubMed ",
                    "Single PubMed ", "Subdivided PubMed ", "Trial ", "Europe PMC ", "Repeated or ", "Missing PubMed ",
                    "Preprint ", "Unexpected preprint ", "Incomplete preprint ", "Missing TGA ", "Unexpected TGA ",
                    "TGA consultation ", "Semantic Scholar ", "Unverified paper ", "Incomplete Semantic Scholar ")):
                error += ": " + str(exc)[:120]
        incomplete = getattr(scraper, "incomplete_queries", [])
        clean = bool(http) and not error and not incomplete and all(isinstance(s, int) and s < 400 for _, s in http)
        verified = bool(clean and (source == "food_fsanz_recalls" or
                                   source in WINDOW_SOURCES and getattr(scraper, "completed_queries", 0)))
        status = "parsed" if clean and (rows or verified) else "incomplete"
        data = dict(source=source, checked_at=datetime.now(timezone.utc).isoformat(),
                    since=since.isoformat(), until=until.isoformat(), completed_queries=getattr(scraper, "completed_queries", None),
                    coverage="query_window_verified" if verified else "not_verified",
                    status=status, error=error, incomplete_queries=incomplete, off_category=args.off_category,
                    http=http, rows=rows)
        json.dump(data, stream, ensure_ascii=False)
    os.link(stream.name, out)
    os.unlink(stream.name)
    print(source, status, len(rows), sorted({str(i) for _, i in http}), error, flush=True)
    return 0 if verified else 2


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    raise SystemExit(main())
