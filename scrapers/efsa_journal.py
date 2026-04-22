"""
scrapers/efsa_journal.py — Scientific opinions and assessments from EFSA.

Uses EFSA's open data search API and publications RSS feed to find
VMS-relevant opinions, assessments, and scientific reports.

EFSA publications: https://www.efsa.europa.eu/en/publications
EFSA open data:    https://data.efsa.europa.eu/
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

from scrapers.base import BaseScraper, RawSignal

logger = logging.getLogger(__name__)

_LOOKBACK_DAYS = 365  # EFSA opinions take time — look back 1 year
_MAX_PER_QUERY = 5
_WATCHLIST_PATH = Path(__file__).parent.parent / "config" / "ingredients_watchlist.json"

# EFSA open data API for publications
_OPENDATA_URL = "https://open.efsa.europa.eu/api/public/publications"

# EFSA topics relevant to VMS
_EFSA_TOPICS = [
    "food supplements",
    "vitamins",
    "minerals",
    "botanical substances",
    "novel foods",
    "health claims",
    "nutrient reference values",
    "maximum tolerable intake",
    "herbal preparations",
    "dietary supplements",
]


def _load_watchlist_terms() -> list[str]:
    try:
        with _WATCHLIST_PATH.open(encoding="utf-8") as fh:
            data = json.load(fh)
        return [ing.strip() for ing in data.get("ingredients", []) if ing.strip() and not ing.startswith("_")]
    except Exception:
        return []


class EFSAJournalScraper(BaseScraper):
    authority = "efsa"

    def fetch_raw(self) -> list[RawSignal]:
        from analytics.db import url_exists

        signals: list[RawSignal] = []
        seen_urls: set[str] = set()

        date_from = (datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

        all_terms = _EFSA_TOPICS + _load_watchlist_terms()
        logger.info("EFSA: searching %d terms", len(all_terms))

        for term in all_terms:
            time.sleep(0.8)
            try:
                # Try EFSA open data API
                params = {
                    "q":        term,
                    "size":     str(_MAX_PER_QUERY),
                    "from":     "0",
                    "sort":     "publicationDate:desc",
                    "dateFrom": date_from,
                }
                resp = self._get_session().get(_OPENDATA_URL, params=params, timeout=30)

                if resp.status_code == 200:
                    try:
                        data = resp.json()
                        items = data.get("hits", {}).get("hits", []) or data.get("results", []) or []
                        for item in items:
                            src = item.get("_source", item)
                            for sig in self._parse_item(src, term, seen_urls, url_exists):
                                signals.append(sig)
                        if items:
                            continue
                    except (ValueError, KeyError, TypeError):
                        pass

                # Fallback: EFSA publications search page
                search_url = f"https://www.efsa.europa.eu/en/search?query_string={quote(term)}&field_publication_type=All"
                resp2 = self._get_session().get(search_url, timeout=30)
                if resp2.status_code == 200:
                    for sig in self._parse_html(resp2.text, term, seen_urls, url_exists):
                        signals.append(sig)

            except Exception:
                logger.exception("EFSA: failed for term %r", term[:50])

        # Fetch EFSA recent publications RSS
        try:
            for sig in self._fetch_efsa_rss(seen_urls, url_exists):
                signals.append(sig)
        except Exception:
            logger.debug("EFSA RSS fetch failed")

        logger.info("EFSA: %d signals fetched", len(signals))
        return signals

    def _parse_item(self, item: dict, term: str, seen: set, url_exists_fn) -> list[RawSignal]:
        title    = (item.get("title") or item.get("name") or "").strip()
        url      = item.get("url") or item.get("link") or item.get("doi") or ""
        abstract = item.get("abstract") or item.get("summary") or item.get("description") or ""
        pub_date = item.get("publicationDate") or item.get("date") or ""

        if not title or not url:
            return []
        if not url.startswith("http"):
            url = f"https://www.efsa.europa.eu{url}"
        if url in seen or url_exists_fn(url):
            return []
        seen.add(url)

        body_text = (
            f"Source: EFSA Journal / EFSA Scientific Opinion\n"
            f"URL: {url}\nPublished: {pub_date}\nSearch term: {term}\n\n"
            f"{abstract[:2000] or 'See full publication at EFSA.'}"
        )
        return [RawSignal(
            source_id  = self._make_source_id("efsa", url),
            authority  = "efsa",
            url        = url,
            title      = title[:300],
            body_text  = body_text[:4000],
            scraped_at = self._now_iso(),
        )]

    def _parse_html(self, html: str, term: str, seen: set, url_exists_fn) -> list[RawSignal]:
        signals = []
        # EFSA search result links pattern
        link_pat  = re.compile(r'href=["\']((?:https://www\.efsa\.europa\.eu)?/en/(?:efsajournal|publications?|scientific-opinion)[^"\'?#]+)["\']', re.I)
        title_pat = re.compile(r'<h[23][^>]*class=["\'][^"\']*(?:title|heading)[^"\']*["\'][^>]*>([^<]{10,250})', re.I)

        links  = []
        for m in link_pat.finditer(html):
            href = m.group(1)
            if not href.startswith("http"):
                href = f"https://www.efsa.europa.eu{href}"
            links.append(href)

        titles = [re.sub(r'\s+', ' ', t).strip() for t in title_pat.findall(html)]

        for url, title in zip(links[:_MAX_PER_QUERY], titles[:_MAX_PER_QUERY] + [""] * _MAX_PER_QUERY):
            if url in seen or url_exists_fn(url):
                continue
            seen.add(url)
            signals.append(RawSignal(
                source_id  = self._make_source_id("efsa", url),
                authority  = "efsa",
                url        = url,
                title      = title[:300] if title else f"EFSA Publication: {term}",
                body_text  = f"Source: EFSA Publications\nURL: {url}\nSearch term: {term}\nAuthority: European Food Safety Authority",
                scraped_at = self._now_iso(),
            ))
        return signals

    def _fetch_efsa_rss(self, seen: set, url_exists_fn) -> list[RawSignal]:
        """Fetch EFSA's latest outputs via their publications RSS."""
        signals = []
        rss_urls = [
            "https://www.efsa.europa.eu/en/rss/scientific-outputs.xml",
            "https://www.efsa.europa.eu/sites/default/files/feed/science.xml",
        ]
        html = ""
        for rss_url in rss_urls:
            try:
                resp = self._get_session().get(rss_url, timeout=20)
                if resp.status_code == 200:
                    html = resp.text
                    break
            except Exception:
                continue

        if not html:
            return []

        # Parse RSS items
        item_pattern = re.compile(r'<item>(.*?)</item>', re.DOTALL | re.I)
        for item_match in item_pattern.finditer(html):
            item_text = item_match.group(1)
            link  = re.search(r'<link>([^<]+)</link>', item_text)
            title = re.search(r'<title>(?:<!\[CDATA\[)?([^<\]]+)(?:\]\]>)?</title>', item_text)
            desc  = re.search(r'<description>(?:<!\[CDATA\[)?([^<\]]+)(?:\]\]>)?</description>', item_text)

            if not link or not title:
                continue
            url   = link.group(1).strip()
            ttext = title.group(1).strip()
            dtext = desc.group(1).strip() if desc else ""

            if not url or url in seen or url_exists_fn(url):
                continue

            # Filter for VMS relevance
            combined = (ttext + dtext).lower()
            vms_kws = {"supplement", "vitamin", "mineral", "herbal", "botanical", "nutrient", "novel food", "food additive"}
            if not any(kw in combined for kw in vms_kws):
                continue

            seen.add(url)
            signals.append(RawSignal(
                source_id  = self._make_source_id("efsa", url),
                authority  = "efsa",
                url        = url,
                title      = ttext[:300],
                body_text  = f"Source: EFSA Scientific Output\nURL: {url}\n\n{dtext[:2000]}",
                scraped_at = self._now_iso(),
            ))

        return signals
