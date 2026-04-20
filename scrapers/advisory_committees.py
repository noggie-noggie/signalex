"""
scrapers/advisory_committees.py — Scrape FDA and EMA advisory committee calendars.

Sources:
  FDA: https://www.fda.gov/advisory-committees/advisory-committee-calendar
  EMA: https://www.ema.europa.eu/en/committees/committee-meeting-dates

Extracts meeting date, committee name, and agenda items.
Flags any agenda items mentioning supplements, botanicals, or VMS ingredients.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, RawSignal

logger = logging.getLogger(__name__)

_FDA_CALENDAR_URL = "https://www.fda.gov/advisory-committees/advisory-committee-calendar"
_EMA_CALENDAR_URL = "https://www.ema.europa.eu/en/about-us/committees-working-parties/scientific-committees/scientific-committee-meetings"
_EMA_CALENDAR_FALLBACK = "https://www.ema.europa.eu/en/committees/committee-meeting-dates"

# Keywords that indicate VMS relevance in agenda items
_VMS_KEYWORDS = [
    "supplement", "botanical", "herbal", "vitamin", "mineral", "nutraceutical",
    "dietary", "natural product", "phytotherapy", "traditional medicine",
    "omega", "probiotic", "melatonin", "kava", "echinacea", "valerian",
    "coenzyme", "coq10", "adapto", "ashwagandha", "turmeric", "curcumin",
    "green tea", "ginkgo", "ginseng", "elderberry", "cannabidiol", "cbd",
]


class AdvisoryCommitteesScraper(BaseScraper):
    """
    Scrapes FDA and EMA advisory committee calendars for upcoming meetings.
    Returns signals for any meeting agenda items with VMS ingredient relevance.
    """

    authority = "advisory_committee"

    def fetch_raw(self) -> list[RawSignal]:
        signals: list[RawSignal] = []

        # FDA advisory committee calendar
        try:
            signals.extend(self._scrape_fda())
        except Exception:
            logger.exception("Advisory committees: FDA scrape failed")

        # EMA committee meeting dates
        try:
            signals.extend(self._scrape_ema())
        except Exception:
            logger.exception("Advisory committees: EMA scrape failed")

        logger.info("Advisory committees: %d signals fetched", len(signals))
        return signals

    # ------------------------------------------------------------------
    # FDA
    # ------------------------------------------------------------------

    def _scrape_fda(self) -> list[RawSignal]:
        html = self._http_get(_FDA_CALENDAR_URL)
        soup = BeautifulSoup(html, "lxml")
        signals: list[RawSignal] = []
        seen: set[str] = set()

        # FDA calendar is typically a table or accordion of meetings
        # Try table rows first
        for row in soup.select("table tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            row_text = row.get_text(separator=" ", strip=True)
            if not self._is_vms_relevant(row_text):
                continue

            a_tag = row.find("a", href=True)
            url = _FDA_CALENDAR_URL
            if a_tag:
                href = a_tag["href"]
                url = href if href.startswith("http") else f"https://www.fda.gov{href}"
            if url in seen:
                continue
            seen.add(url)

            # Extract date from first cell
            date_str = cells[0].get_text(strip=True)
            # Extract committee + agenda from remaining cells
            committee = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            agenda    = " | ".join(c.get_text(strip=True) for c in cells[2:]) if len(cells) > 2 else ""

            title = f"FDA Advisory Committee: {committee[:100]}"
            body_text = (
                f"Source: FDA Advisory Committee Calendar\n"
                f"Committee: {committee}\n"
                f"Meeting Date: {date_str}\n"
                f"Agenda Items: {agenda[:500]}\n"
                f"URL: {url}"
            )
            signals.append(RawSignal(
                source_id  = self._make_source_id("advisory_committee", f"fda::{url}::{date_str}"),
                authority  = "advisory_committee",
                url        = url,
                title      = title[:300],
                body_text  = body_text[:4000],
                scraped_at = self._now_iso(),
            ))

        # Fallback: parse any meeting links on the page
        if not signals:
            for a in soup.find_all("a", href=True):
                text = a.get_text(strip=True)
                if not text or not self._is_vms_relevant(text):
                    continue
                href = a["href"]
                url = href if href.startswith("http") else f"https://www.fda.gov{href}"
                if url in seen:
                    continue
                seen.add(url)
                body_text = (
                    f"Source: FDA Advisory Committee Calendar\n"
                    f"Agenda Item: {text}\n"
                    f"URL: {url}"
                )
                signals.append(RawSignal(
                    source_id  = self._make_source_id("advisory_committee", f"fda::{url}"),
                    authority  = "advisory_committee",
                    url        = url,
                    title      = f"FDA Advisory: {text[:150]}",
                    body_text  = body_text[:4000],
                    scraped_at = self._now_iso(),
                ))

        logger.info("FDA advisory committee: %d VMS-relevant signals", len(signals))
        return signals

    # ------------------------------------------------------------------
    # EMA
    # ------------------------------------------------------------------

    def _scrape_ema(self) -> list[RawSignal]:
        html = None
        for url in (_EMA_CALENDAR_URL, _EMA_CALENDAR_FALLBACK):
            try:
                html = self._http_get(url)
                break
            except Exception:
                logger.debug("EMA URL %s failed, trying next", url)
        if html is None:
            logger.warning("EMA advisory committee calendar unavailable (all URLs failed)")
            return []
        soup = BeautifulSoup(html, "lxml")
        signals: list[RawSignal] = []
        seen: set[str] = set()

        # EMA meeting dates are typically in tables grouped by committee
        current_committee = ""

        for elem in soup.find_all(["h2", "h3", "h4", "tr", "li"]):
            tag = elem.name
            text = elem.get_text(strip=True)

            # Track current committee section heading
            if tag in ("h2", "h3", "h4"):
                current_committee = text
                continue

            if not text or not self._is_vms_relevant(text + " " + current_committee):
                continue

            a_tag = elem.find("a", href=True)
            url = _EMA_CALENDAR_URL
            if a_tag:
                href = a_tag["href"]
                url = href if href.startswith("http") else f"https://www.ema.europa.eu{href}"
            if url in seen:
                continue
            seen.add(url)

            date_match = re.search(r"\d{1,2}[–\-]\d{1,2}\s+\w+\s+\d{4}|\d{1,2}\s+\w+\s+\d{4}", text)
            date_str = date_match.group() if date_match else ""

            title = f"EMA {current_committee}: {text[:100]}"
            body_text = (
                f"Source: EMA Committee Meeting Calendar\n"
                f"Committee: {current_committee}\n"
                f"Meeting: {text[:300]}\n"
                f"Date: {date_str}\n"
                f"URL: {url}"
            )
            signals.append(RawSignal(
                source_id  = self._make_source_id("advisory_committee", f"ema::{url}::{date_str}"),
                authority  = "advisory_committee",
                url        = url,
                title      = title[:300],
                body_text  = body_text[:4000],
                scraped_at = self._now_iso(),
            ))

        logger.info("EMA advisory committee: %d VMS-relevant signals", len(signals))
        return signals

    @staticmethod
    def _is_vms_relevant(text: str) -> bool:
        t = text.lower()
        return any(kw in t for kw in _VMS_KEYWORDS)
