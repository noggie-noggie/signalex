"""
scrapers/who_ictrp.py — Trial registrations from WHO ICTRP (non-US registries).

WHO ICTRP aggregates trials from ANZCTR (Australia), EU CTR, ISRCTN, ChiCTR,
CTRI and other national registries. Focus on non-US registries that won't
appear in ClinicalTrials.gov.

Uses the WHO ICTRP public search portal.
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

_ICTRP_SEARCH = "https://trialsearch.who.int/Search.aspx"
_LOOKBACK_DAYS = 90
_WATCHLIST_PATH = Path(__file__).parent.parent / "config" / "ingredients_watchlist.json"

# Focus on Australian and non-US registries
_AU_TERMS = [
    "dietary supplement Australia",
    "complementary medicine ANZCTR",
    "vitamin mineral Australia clinical",
    "herbal supplement Australia",
]

_EU_TERMS = [
    "dietary supplement European",
    "nutraceutical clinical trial EU",
    "herbal medicine European Union",
]


def _load_watchlist_terms() -> list[str]:
    try:
        with _WATCHLIST_PATH.open(encoding="utf-8") as fh:
            data = json.load(fh)
        return [ing.strip() for ing in data.get("ingredients", []) if ing.strip() and not ing.startswith("_")]
    except Exception:
        return []


class WHOICTRPScraper(BaseScraper):
    authority = "who_ictrp"

    def fetch_raw(self) -> list[RawSignal]:
        from analytics.db import url_exists

        queries = _AU_TERMS + _EU_TERMS + [f"{t} supplement" for t in _load_watchlist_terms()[:8]]
        signals: list[RawSignal] = []
        seen_ids: set[str] = set()

        logger.info("WHO ICTRP: searching %d terms", len(queries))

        for term in queries:
            time.sleep(1.5)  # ICTRP portal needs generous pacing
            try:
                # Try WHO ICTRP search — it returns HTML
                resp = self._get_session().get(
                    _ICTRP_SEARCH,
                    params={"query": term, "RecordsToReturn": "10"},
                    timeout=45,
                )
                if resp.status_code not in (200, 302):
                    # Fallback: try ANZCTR directly for Australian focus
                    for sig in self._fetch_anzctr(term, seen_ids, url_exists):
                        signals.append(sig)
                    continue

                html = resp.text
                for sig in self._parse_ictrp_html(html, term, seen_ids, url_exists):
                    signals.append(sig)

            except Exception:
                logger.exception("WHO ICTRP: failed for term %r", term[:50])
                # Fallback to ANZCTR
                try:
                    for sig in self._fetch_anzctr(term, seen_ids, url_exists):
                        signals.append(sig)
                except Exception:
                    logger.exception("ANZCTR fallback also failed for %r", term[:50])

        logger.info("WHO ICTRP: %d signals fetched", len(signals))
        return signals

    def _parse_ictrp_html(self, html: str, term: str, seen: set, url_exists_fn) -> list[RawSignal]:
        signals = []
        # ICTRP result rows typically contain trial IDs like ACTRN..., ISRCTN..., etc.
        id_pattern  = re.compile(r'(ACTRN\d+|ISRCTN\d+|EUCTR[\w-]+|ChiCTR[\w-]+|CTRI/\S+)', re.I)
        link_pattern = re.compile(r'href=["\']([^"\']*trialsearch[^"\']+|[^"\']*Trial2\.aspx[^"\']+)["\']', re.I)

        trial_ids = id_pattern.findall(html)[:8]
        links     = link_pattern.findall(html)[:8]

        for trial_id in trial_ids:
            if trial_id in seen:
                continue
            seen.add(trial_id)
            url = f"https://trialsearch.who.int/Trial2.aspx?TrialID={trial_id}"
            if url_exists_fn(url):
                continue
            signals.append(RawSignal(
                source_id  = self._make_source_id("who_ictrp", url),
                authority  = "who_ictrp",
                url        = url,
                title      = f"WHO ICTRP Trial: {trial_id}",
                body_text  = f"Trial ID: {trial_id}\nSearch term: {term}\nSource: WHO ICTRP",
                scraped_at = self._now_iso(),
            ))

        return signals

    def _fetch_anzctr(self, term: str, seen: set, url_exists_fn) -> list[RawSignal]:
        """Fallback: query ANZCTR (Australian New Zealand Clinical Trials Registry)."""
        signals = []
        try:
            resp = self._get_session().get(
                "https://www.anzctr.org.au/TrialSearch.aspx#&&isBasic=True",
                params={"searchTxt": term, "isBasic": "True"},
                timeout=30,
            )
            if resp.status_code != 200:
                return []
            html = resp.text
            actrn_pattern = re.compile(r'ACTRN(\d+)')
            title_pattern = re.compile(r'<td[^>]*class=["\'][^"\']*title[^"\']*["\'][^>]*>\s*<a[^>]*>([^<]+)</a>', re.I)

            ids    = [f"ACTRN{m.group(1)}" for m in actrn_pattern.finditer(html)][:5]
            titles = [t.strip() for t in title_pattern.findall(html)][:5]

            for actrn, title in zip(ids, titles + [""] * len(ids)):
                if actrn in seen:
                    continue
                seen.add(actrn)
                url = f"https://www.anzctr.org.au/Trial/Registration/TrialReview.aspx?id={actrn.replace('ACTRN','')}"
                if url_exists_fn(url):
                    continue
                signals.append(RawSignal(
                    source_id  = self._make_source_id("who_ictrp", url),
                    authority  = "who_ictrp",
                    url        = url,
                    title      = title[:300] if title else f"ANZCTR Trial {actrn}",
                    body_text  = f"Trial ID: {actrn}\nRegistry: ANZCTR (Australia/New Zealand)\nSearch term: {term}",
                    scraped_at = self._now_iso(),
                ))
        except Exception:
            logger.debug("ANZCTR fetch failed for %r", term[:50])
        return signals
