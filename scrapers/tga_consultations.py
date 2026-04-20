"""
scrapers/tga_consultations.py — Scrape TGA public consultations page.

Source: https://www.tga.gov.au/resources/consultations

Extracts consultation title, status (open/closed), dates, and linked
documents. Filters for consultations relevant to complementary medicines,
dietary supplements, or VMS ingredients.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, RawSignal

logger = logging.getLogger(__name__)

_CONSULTATIONS_URL = "https://www.tga.gov.au/resources/consultations"

# Keywords to filter for VMS relevance in consultation titles
_VMS_KEYWORDS = [
    "complementary", "supplement", "vitamin", "mineral", "herbal",
    "botanical", "listed medicine", "ingredient", "schedule", "scheduling",
    "natural", "traditional", "nutraceutical", "dietary", "food", "kava",
    "melatonin", "omega", "probiotic", "toxicology", "safety assessment",
]


class TGAConsultationsScraper(BaseScraper):
    """
    Scrapes the TGA consultations hub page for open and recent closed
    consultations relevant to VMS ingredients and complementary medicines.
    """

    authority = "tga_consultations"

    def fetch_raw(self) -> list[RawSignal]:
        try:
            html = self._http_get(_CONSULTATIONS_URL)
        except Exception:
            logger.exception("TGA consultations: failed to fetch %s", _CONSULTATIONS_URL)
            return []

        soup = BeautifulSoup(html, "lxml")
        signals: list[RawSignal] = []

        # TGA consultations page lists items in various table/list structures.
        # Parse all links whose text looks like a consultation title.
        seen_urls: set[str] = set()

        # Try structured table rows first
        for row in soup.select("table tr"):
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            signal = self._parse_table_row(cells, seen_urls)
            if signal:
                signals.append(signal)

        # Fallback: parse article/section cards
        if not signals:
            for article in soup.select("article, .views-row, .item-list li"):
                signal = self._parse_card(article, seen_urls)
                if signal:
                    signals.append(signal)

        # Last resort: grab all consultation-looking links on the page
        if not signals:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(strip=True)
                if not text or len(text) < 15:
                    continue
                if "consultation" not in href.lower() and not self._is_vms_relevant(text):
                    continue
                full_url = href if href.startswith("http") else f"https://www.tga.gov.au{href}"
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)
                signals.append(self._make_signal(
                    title=text[:300],
                    url=full_url,
                    status="unknown",
                    date_str="",
                    body_text=f"Title: {text}\nURL: {full_url}",
                ))

        # Filter to VMS-relevant signals only
        relevant = [s for s in signals if self._is_vms_relevant(s["title"] + " " + s["body_text"])]
        logger.info("TGA Consultations: %d/%d signals are VMS-relevant", len(relevant), len(signals))
        return relevant

    def _parse_table_row(self, cells: list, seen_urls: set) -> RawSignal | None:
        """Parse a table row with title/status/date cells."""
        if len(cells) < 2:
            return None
        title_cell = cells[0]
        a_tag = title_cell.find("a", href=True)
        if not a_tag:
            return None
        href = a_tag["href"]
        full_url = href if href.startswith("http") else f"https://www.tga.gov.au{href}"
        if full_url in seen_urls:
            return None
        seen_urls.add(full_url)

        title   = a_tag.get_text(strip=True)[:300]
        status  = cells[1].get_text(strip=True) if len(cells) > 1 else ""
        date_str = cells[2].get_text(strip=True) if len(cells) > 2 else ""

        body_text = (
            f"Consultation: {title}\n"
            f"Status: {status}\n"
            f"Dates: {date_str}\n"
            f"URL: {full_url}"
        )
        return self._make_signal(title, full_url, status, date_str, body_text)

    def _parse_card(self, element, seen_urls: set) -> RawSignal | None:
        """Parse a card/article element."""
        a_tag = element.find("a", href=True)
        if not a_tag:
            return None
        href = a_tag["href"]
        full_url = href if href.startswith("http") else f"https://www.tga.gov.au{href}"
        if full_url in seen_urls:
            return None
        seen_urls.add(full_url)

        title    = a_tag.get_text(strip=True)[:300]
        text     = element.get_text(separator=" ", strip=True)
        status   = "open" if "open" in text.lower() else ("closed" if "closed" in text.lower() else "unknown")
        date_match = re.search(r"\d{1,2}\s+\w+\s+\d{4}", text)
        date_str = date_match.group() if date_match else ""

        body_text = (
            f"Consultation: {title}\n"
            f"Status: {status}\n"
            f"Date: {date_str}\n"
            f"Context: {text[:500]}\n"
            f"URL: {full_url}"
        )
        return self._make_signal(title, full_url, status, date_str, body_text)

    def _make_signal(
        self,
        title: str,
        url: str,
        status: str,
        date_str: str,
        body_text: str,
    ) -> RawSignal:
        return RawSignal(
            source_id  = self._make_source_id("tga_consultations", url),
            authority  = "tga_consultations",
            url        = url,
            title      = title,
            body_text  = body_text[:4000],
            scraped_at = self._now_iso(),
        )

    @staticmethod
    def _is_vms_relevant(text: str) -> bool:
        t = text.lower()
        return any(kw in t for kw in _VMS_KEYWORDS)
