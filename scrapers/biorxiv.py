"""
scrapers/biorxiv.py — Preprints from bioRxiv and medRxiv via the biorxiv.org API.

API docs: https://api.biorxiv.org/

All signals are tagged PREPRINT — classifier is instructed to treat as unverified.
Severity is capped at one level below a published paper.

Uses date-range retrieval and keyword filtering (API has no keyword search).
Also uses the web search as a fallback to find ingredient-specific preprints.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote_plus

from scrapers.base import BaseScraper, RawSignal

logger = logging.getLogger(__name__)

_API_BASE       = "https://api.biorxiv.org/details"
_LOOKBACK_DAYS  = 30
_PAGE_SIZE      = 100
_WATCHLIST_PATH = Path(__file__).parent.parent / "config" / "ingredients_watchlist.json"

_VMS_TERMS = {
    "supplement", "vitamin", "mineral", "herbal", "botanical", "nutraceutical",
    "probiotic", "omega-3", "omega3", "collagen", "melatonin", "creatine",
    "ashwagandha", "turmeric", "curcumin", "cbd", "cannabidiol", "magnesium",
    "biotin", "antioxidant", "adaptogen", "prebiotic", "microbiome",
    "nootropic", "nmn", "nad", "coenzyme q10", "coq10", "resveratrol",
}


def _load_keywords() -> set[str]:
    terms = set(_VMS_TERMS)
    try:
        with _WATCHLIST_PATH.open(encoding="utf-8") as fh:
            data = json.load(fh)
        for ing in data.get("ingredients", []):
            terms.add(ing.strip().lower())
    except Exception:
        pass
    return terms


def _is_vms_relevant(title: str, abstract: str) -> bool:
    combined = (title + " " + (abstract or "")).lower()
    return any(kw in combined for kw in _load_keywords())


class BiorxivScraper(BaseScraper):
    authority = "biorxiv"

    def fetch_raw(self) -> list[RawSignal]:
        from analytics.db import url_exists

        date_to   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        date_from = (datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        interval  = f"{date_from}/{date_to}"

        signals: list[RawSignal] = []
        seen_dois: set[str] = set()

        # Try the date-range API for both servers
        for server in ("biorxiv", "medrxiv"):
            logger.info("%s: fetching preprints %s", server, interval)
            cursor  = 0
            fetched = 0
            max_items = 500

            while fetched < max_items:
                time.sleep(0.5)
                try:
                    url = f"{_API_BASE}/{server}/{interval}/{cursor}/{_PAGE_SIZE}"
                    resp = self._get_session().get(url, timeout=30)
                    if resp.status_code != 200:
                        break
                    data = resp.json()

                    messages   = data.get("messages", [{}])
                    status_msg = (messages[0].get("status", "") if messages else "").lower()
                    if "ok" not in status_msg and "found" not in status_msg:
                        break

                    collection = data.get("collection", [])
                    if not collection:
                        break

                    for item in collection:
                        doi      = item.get("doi", "")
                        title    = (item.get("title") or "").strip()
                        abstract = item.get("abstract") or ""
                        authors  = item.get("authors") or ""
                        category = item.get("category") or ""
                        version  = str(item.get("version", "1"))
                        pub_date = item.get("date") or ""

                        if not doi or doi in seen_dois:
                            continue
                        seen_dois.add(doi)

                        if not _is_vms_relevant(title, abstract):
                            continue

                        canonical_url = f"https://doi.org/{doi}"
                        if url_exists(canonical_url):
                            continue

                        body_text = (
                            f"[PREPRINT — NOT PEER REVIEWED]\n"
                            f"Server: {server.upper()}\nDOI: {doi}\n"
                            f"Version: {version}\nCategory: {category}\n"
                            f"Authors: {str(authors)[:300]}\nDate: {pub_date}\n\n"
                            f"Abstract:\n{abstract[:2500] or 'No abstract.'}"
                        )

                        signals.append(RawSignal(
                            source_id  = self._make_source_id("biorxiv", canonical_url),
                            authority  = "biorxiv",
                            url        = canonical_url,
                            title      = f"[PREPRINT] {title}"[:300],
                            body_text  = body_text[:4000],
                            scraped_at = self._now_iso(),
                        ))

                    fetched += len(collection)
                    if len(collection) < _PAGE_SIZE:
                        break
                    cursor += _PAGE_SIZE

                except Exception:
                    logger.exception("%s: fetch failed at cursor %d", server, cursor)
                    break

        # Fallback: search biorxiv.org HTML for specific ingredient terms if API returned nothing
        if not signals:
            logger.info("bioRxiv: API returned 0 VMS signals, trying web search fallback")
            try:
                watchlist = list(_load_keywords())[:8]
                for term in watchlist:
                    time.sleep(1.5)
                    try:
                        search_url = f"https://www.biorxiv.org/search/{quote_plus(term + ' supplement')}"
                        resp = self._get_session().get(search_url, timeout=30)
                        if resp.status_code == 200:
                            for sig in self._parse_html_search(resp.text, term, seen_dois, url_exists):
                                signals.append(sig)
                    except Exception:
                        logger.debug("bioRxiv web search failed for %r", term)
            except Exception:
                pass

        logger.info("bioRxiv/medRxiv: %d VMS-relevant preprints fetched", len(signals))
        return signals

    def _parse_html_search(self, html: str, term: str, seen: set, url_exists_fn) -> list[RawSignal]:
        signals = []
        # Extract DOIs from bioRxiv HTML search results
        doi_pattern   = re.compile(r'(?:href|content)=["\'](?:https://www\.biorxiv\.org)?/content/(10\.\d+/[^\s"\'<>v]+)(?:v\d+)?["\']', re.I)
        title_pattern = re.compile(r'<span[^>]*class=["\'][^"\']*highwire-cite-title[^"\']*["\'][^>]*>([^<]+)', re.I)

        dois   = [m.group(1) for m in doi_pattern.finditer(html)]
        titles = [re.sub(r'\s+', ' ', t).strip() for t in title_pattern.findall(html)]

        for doi, title in zip(dois[:5], titles[:5] + [""] * 5):
            if doi in seen:
                continue
            seen.add(doi)
            url = f"https://doi.org/{doi}"
            if url_exists_fn(url):
                continue
            signals.append(RawSignal(
                source_id  = self._make_source_id("biorxiv", url),
                authority  = "biorxiv",
                url        = url,
                title      = f"[PREPRINT] {title}"[:300] if title else f"[PREPRINT] bioRxiv: {term}",
                body_text  = f"[PREPRINT — NOT PEER REVIEWED]\nServer: BIORXIV\nDOI: {doi}\nSearch term: {term}",
                scraped_at = self._now_iso(),
            ))
        return signals
