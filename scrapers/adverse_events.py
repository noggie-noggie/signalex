"""
scrapers/adverse_events.py — Scrape FDA CAERS and TGA DAEN adverse event data.

FDA CAERS (CFSAN Adverse Event Reporting System):
  Landing page: https://www.fda.gov/food/compliance-enforcement-food/cfsan-adverse-event-reporting-system-caers
  CAERS publishes data as CSV/ZIP downloads; we scrape the landing page for
  the most recent summary statistics and download links.

TGA DAEN (Database of Adverse Event Notifications):
  Landing page: https://apps.tga.gov.au/PROD/DAEN/daen-entry.aspx
  The DAEN search form requires POST parameters; we extract summary stats
  from the landing page and any pre-built reports linked there.

Both sources are scraped for:
  - Product name
  - Adverse event type
  - Date
  - Outcome severity
"""

from __future__ import annotations

import io
import logging
import re
import zipfile
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, RawSignal

logger = logging.getLogger(__name__)

_CAERS_URL = "https://www.fda.gov/food/compliance-enforcement-food/cfsan-adverse-event-reporting-system-caers"
_DAEN_URL  = "https://apps.tga.gov.au/PROD/DAEN/daen-entry.aspx"

# VMS-related keywords to filter adverse event descriptions
_VMS_KEYWORDS = [
    "supplement", "vitamin", "mineral", "herbal", "botanical", "dietary",
    "nutraceutical", "natural product", "probiotic", "protein powder",
    "energy drink", "weight loss", "sports nutrition",
]


class AdverseEventsScraper(BaseScraper):
    """
    Scrapes FDA CAERS and TGA DAEN landing pages for adverse event summaries.

    Since both sites require complex interactions to access full data, we:
    1. Parse the landing page for any linked summary tables or reports.
    2. Extract visible adverse event information in text form.
    3. Look for downloadable data files (CSV/ZIP) and parse their headers.
    """

    authority = "adverse_events"

    def fetch_raw(self) -> list[RawSignal]:
        signals: list[RawSignal] = []

        try:
            signals.extend(self._scrape_caers())
        except Exception:
            logger.exception("Adverse events: CAERS scrape failed")

        try:
            signals.extend(self._scrape_daen())
        except Exception:
            logger.exception("Adverse events: DAEN scrape failed")

        logger.info("Adverse events: %d signals fetched", len(signals))
        return signals

    # ------------------------------------------------------------------
    # FDA CAERS
    # ------------------------------------------------------------------

    def _scrape_caers(self) -> list[RawSignal]:
        html = self._http_get(_CAERS_URL)
        soup = BeautifulSoup(html, "lxml")
        signals: list[RawSignal] = []

        # Extract all text content sections about adverse events
        page_text = soup.get_text(separator="\n", strip=True)

        # Look for downloadable data file links
        data_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            # Look for CSV, ZIP, or data file links
            if any(ext in href.lower() for ext in [".csv", ".zip", ".xlsx"]):
                full_url = href if href.startswith("http") else f"https://www.fda.gov{href}"
                data_links.append((text, full_url))

        # Build a signal from the landing page itself with summary context
        summary_sections = self._extract_sections(soup, _VMS_KEYWORDS)

        if summary_sections or data_links:
            body_parts = ["FDA CFSAN Adverse Event Reporting System (CAERS) Summary\n"]
            body_parts.extend(summary_sections[:5])
            if data_links:
                body_parts.append("\nAvailable Data Files:")
                for link_text, link_url in data_links[:5]:
                    body_parts.append(f"  - {link_text}: {link_url}")

            signals.append(RawSignal(
                source_id  = self._make_source_id("adverse_events", _CAERS_URL),
                authority  = "adverse_events",
                url        = _CAERS_URL,
                title      = "FDA CAERS: Dietary Supplement Adverse Event Reports",
                body_text  = "\n".join(body_parts)[:4000],
                scraped_at = self._now_iso(),
            ))

        # Try to fetch and parse the most recent data file if available
        for link_text, link_url in data_links[:2]:
            try:
                file_signals = self._parse_data_file(link_url, link_text, "caers")
                signals.extend(file_signals[:10])  # cap at 10 signals per file
            except Exception:
                logger.debug("Could not parse data file %s", link_url)

        logger.info("CAERS: %d signals", len(signals))
        return signals

    def _parse_data_file(self, url: str, label: str, source: str) -> list[RawSignal]:
        """Attempt to download and parse a CSV/ZIP data file for adverse events."""
        signals = []
        try:
            resp = self._get_session().get(url, timeout=60, stream=True)
            resp.raise_for_status()
            content = resp.content

            # Handle ZIP files
            if url.lower().endswith(".zip") or content[:2] == b"PK":
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(".csv"):
                            csv_content = zf.read(name).decode("utf-8", errors="replace")
                            signals.extend(self._parse_csv_adverse_events(csv_content, url, source))
                            break
            elif url.lower().endswith(".csv"):
                csv_content = content.decode("utf-8", errors="replace")
                signals.extend(self._parse_csv_adverse_events(csv_content, url, source))

        except Exception:
            logger.debug("Data file parse failed for %s", url)

        return signals

    def _parse_csv_adverse_events(self, csv_text: str, source_url: str, source: str) -> list[RawSignal]:
        """Parse adverse event CSV rows into RawSignals."""
        import csv
        signals = []
        reader = csv.DictReader(io.StringIO(csv_text))
        rows = list(reader)

        for i, row in enumerate(rows[:20]):  # cap at 20 rows per file
            product = (
                row.get("PRODUCT", "") or row.get("Product", "") or
                row.get("product_name", "") or row.get("PRODUCT_NAME", "")
            ).strip()
            event_type = (
                row.get("REACTIONS", "") or row.get("Reactions", "") or
                row.get("event_type", "") or row.get("AE_TYPE", "")
            ).strip()
            outcome = (
                row.get("OUTCOMES", "") or row.get("Outcomes", "") or
                row.get("outcome", "") or row.get("OUTCOME", "")
            ).strip()
            date = (
                row.get("DATE", "") or row.get("Date", "") or
                row.get("report_date", "") or row.get("REPORT_DATE", "")
            ).strip()

            if not product or not self._is_vms_relevant(product + " " + event_type):
                continue

            title = f"Adverse Event: {product[:100]}"
            body_text = (
                f"Source: {source.upper()} Adverse Event Report\n"
                f"Product: {product}\n"
                f"Adverse Event / Reactions: {event_type}\n"
                f"Outcome: {outcome}\n"
                f"Report Date: {date}\n"
                f"All fields: {dict(list(row.items())[:10])}"
            )

            signals.append(RawSignal(
                source_id  = self._make_source_id("adverse_events", f"{source_url}::{product}::{i}"),
                authority  = "adverse_events",
                url        = source_url,
                title      = title,
                body_text  = body_text[:4000],
                scraped_at = self._now_iso(),
            ))

        return signals

    # ------------------------------------------------------------------
    # TGA DAEN
    # ------------------------------------------------------------------

    def _scrape_daen(self) -> list[RawSignal]:
        """Scrape the TGA DAEN landing page for adverse event summaries."""
        try:
            html = self._http_get(_DAEN_URL)
        except Exception:
            logger.warning("TGA DAEN: could not fetch landing page (may require POST/JS)")
            return self._daen_fallback()

        soup = BeautifulSoup(html, "lxml")
        summary_sections = self._extract_sections(soup, _VMS_KEYWORDS)

        if not summary_sections:
            return self._daen_fallback()

        body_text = (
            "TGA Database of Adverse Event Notifications (DAEN) — Supplement Reports\n\n"
            + "\n".join(summary_sections[:5])
        )

        signal = RawSignal(
            source_id  = self._make_source_id("adverse_events", _DAEN_URL),
            authority  = "adverse_events",
            url        = _DAEN_URL,
            title      = "TGA DAEN: Complementary Medicine Adverse Event Notifications",
            body_text  = body_text[:4000],
            scraped_at = self._now_iso(),
        )
        return [signal]

    def _daen_fallback(self) -> list[RawSignal]:
        """Return a placeholder signal when DAEN cannot be scraped dynamically."""
        body_text = (
            "TGA DAEN (Database of Adverse Event Notifications) monitors adverse events "
            "for complementary medicines including vitamins, minerals, and herbal products "
            "sold in Australia. The DAEN requires form-based POST interactions. "
            "Periodic summary reports are available at: https://www.tga.gov.au/reporting-problems"
        )
        return [RawSignal(
            source_id  = self._make_source_id("adverse_events", _DAEN_URL + ":fallback"),
            authority  = "adverse_events",
            url        = _DAEN_URL,
            title      = "TGA DAEN: Complementary Medicine Adverse Event Monitoring",
            body_text  = body_text,
            scraped_at = self._now_iso(),
        )]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _extract_sections(self, soup: BeautifulSoup, keywords: list[str]) -> list[str]:
        """Extract text sections from a page that contain VMS-relevant keywords."""
        sections = []
        for elem in soup.find_all(["p", "li", "td", "div"]):
            text = elem.get_text(strip=True)
            if len(text) < 20 or len(text) > 1000:
                continue
            if self._is_vms_relevant(text):
                sections.append(text)
        return sections

    @staticmethod
    def _is_vms_relevant(text: str) -> bool:
        t = text.lower()
        return any(kw in t for kw in _VMS_KEYWORDS)
