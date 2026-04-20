"""
scrapers/fda.py — Scraper for the US Food & Drug Administration.

Two data sources:

  1. Recalls / market withdrawals / safety alerts  (Drupal DataTables AJAX)
     URL: https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts
     AJAX: https://www.fda.gov/datatables/views/ajax
     Yields: dietary supplement recalls, market withdrawals, contamination
     notices, undeclared drug substances (sildenafil, tadalafil, etc.).

     The visible page renders 10 rows via JavaScript; the full 870-row dataset
     is fetched from the Drupal DataTables AJAX endpoint as JSON.  We request
     200 rows per page and filter to rows whose Product Type contains
     "dietary supplement".

  2. FDA Dietary Supplements hub page  (plain HTML, BeautifulSoup)
     URL: https://www.fda.gov/food/dietary-supplements
     Yields: safety alerts, warning letters, and advisories linked from
     the hub's content sections.  This catches items that may not appear
     in the recalls table (e.g. ingredient-level advisories, import alerts).

HTML/API structure notes (verified against live site 2026-Q1):
  - Recalls AJAX: GET /datatables/views/ajax with Drupal view parameters.
    Response is {"draw":N,"recordsTotal":N,"data":[[html,html,...], ...]}.
    Each row has 8 HTML-cell columns: Date, Brand, Product, Type, Reason,
    Company, Terminated, Excerpt.  Links are embedded as <a href> in col 1/2.
  - Hub page: <div class="lcds-description-list__item"> groups contain <a>
    links to sub-pages; we collect ones whose link text / URL implies safety.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

from bs4 import BeautifulSoup, Tag

import config
from .base import BaseScraper, RawSignal

logger = logging.getLogger(__name__)

# Drupal DataTables view parameters — extracted from drupalSettings JS blob.
# If FDA redesigns the page, re-extract view_dom_id from the JS source.
_VIEW_DOM_ID = (
    "c90843863b89760a7934602fcb03935aff73bccc54c425b13f481d8d66cce73c"
)
_AJAX_URL = "https://www.fda.gov/datatables/views/ajax"

# Product-type substrings that indicate a dietary supplement row.
_SUPPLEMENT_KEYWORDS = frozenset([
    "dietary supplement",
    "dietary supplements",
])

# Link-text / URL fragments that suggest a safety-relevant sub-page on the hub.
# Must appear in the URL path (not just link text) to avoid nav/menu false positives.
_SAFETY_URL_KEYWORDS = re.compile(
    r"alert|warning|tainted|contamina|adverse|enforcement|recall|import-alert",
    re.IGNORECASE,
)

# Short generic link texts that are navigation items, not content pages.
_NAV_TEXT_BLOCKLIST = re.compile(
    r"^(recalls?(\s*,\s*market\s+withdrawals.*)?|safety(\s+alerts?)?|"
    r"warning letters?|inspections?\s*(and\s+compliance)?|compliance|"
    r"guidance|regulations?|news|contact|about|home|search|resources?)$",
    re.IGNORECASE,
)


class FDAScraper(BaseScraper):
    authority = "fda"

    def fetch_raw(self) -> list[RawSignal]:
        signals: list[RawSignal] = []
        signals.extend(self._fetch_recalls())
        signals.extend(self._fetch_hub_safety_links())
        return signals

    # ------------------------------------------------------------------
    # Source 1: Recalls / market withdrawals (DataTables AJAX)
    # ------------------------------------------------------------------

    def _fetch_recalls(self) -> list[RawSignal]:
        """
        Fetch dietary-supplement rows from the FDA recalls DataTables endpoint.

        Paginates through all rows (200 per page) until every row is older
        than SIGNAL_LOOKBACK_DAYS or all pages are exhausted.  Only rows with
        Product Type containing "dietary supplement" are returned.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=config.SIGNAL_LOOKBACK_DAYS)
        signals: list[RawSignal] = []
        start = 0
        page_size = 200
        total = None

        while True:
            rows, total = self._fetch_recalls_page(start, page_size)
            if not rows:
                break

            stop_early = False
            for row in rows:
                prod_type = self._cell_text(row[3]).lower()
                if not any(kw in prod_type for kw in _SUPPLEMENT_KEYWORDS):
                    continue

                pub_date = self._parse_date_str(self._cell_text(row[0]))
                # Rows are sorted newest-first; stop once we're past the window
                if pub_date is not None and pub_date < cutoff:
                    stop_early = True
                    break

                brand   = self._cell_text(row[1])
                product = self._cell_text(row[2])
                reason  = self._cell_text(row[4])
                company = self._cell_text(row[5])

                # Prefer brand-name link; fall back to product-description link
                href = self._cell_link(row[1]) or self._cell_link(row[2]) or ""
                url  = (
                    href if href.startswith("http")
                    else config.SCRAPER_CONFIG["fda"]["base_url"] + href
                    if href else config.SCRAPER_CONFIG["fda"]["base_url"]
                )

                title = f"{brand} — {product}" if brand else product
                date_label = pub_date.strftime("%Y-%m-%d") if pub_date else "unknown date"
                body_text = "\n".join(filter(None, [
                    f"Published: {date_label}",
                    f"Product type: {self._cell_text(row[3])}",
                    f"Recall reason: {reason}",
                    f"Company: {company}",
                ]))

                signals.append(RawSignal(
                    source_id  = self._make_source_id(self.authority, url),
                    authority  = self.authority,
                    url        = url,
                    title      = title,
                    body_text  = body_text,
                    scraped_at = self._now_iso(),
                ))

            start += page_size
            if stop_early or (total is not None and start >= total):
                break

        logger.info("FDA recalls: %d supplement signal(s) within lookback window", len(signals))
        return signals

    def _fetch_recalls_page(self, start: int, length: int) -> tuple[list, int | None]:
        """
        GET one page from the Drupal DataTables AJAX endpoint.
        Returns (rows, total_records).  rows is a list of 8-column HTML lists.
        """
        params = {
            "_drupal_ajax": "1",
            "_wrapper_format": "drupal_ajax",
            "pager_element": "0",
            "view_args": "",
            "view_base_path": "safety/recalls-market-withdrawals-safety-alerts/datatables-data",
            "view_display_id": "recall_datatable_block_1",
            "view_dom_id": _VIEW_DOM_ID,
            "view_name": "recall_solr_index",
            "view_path": "/safety/recalls-market-withdrawals-safety-alerts",
            "start": start,
            "length": length,
            "draw": (start // length) + 1,
        }
        # Override Accept header for this JSON request
        session = self._get_session()
        resp = session.get(
            _AJAX_URL,
            params=params,
            headers={"Accept": "application/json, text/javascript, */*",
                     "X-Requested-With": "XMLHttpRequest"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", []), data.get("recordsTotal")

    # ------------------------------------------------------------------
    # Source 2: FDA Dietary Supplements hub (plain HTML)
    # ------------------------------------------------------------------

    def _fetch_hub_safety_links(self) -> list[RawSignal]:
        """
        Scrape the FDA dietary supplements hub page and follow links whose
        text or URL suggests a safety-relevant sub-page.

        The hub at /food/dietary-supplements lists links to sub-pages for
        safety alerts, warning letters, import alerts, etc.  We collect
        those links, then fetch each sub-page and return it as a RawSignal
        (the classifier will determine the specific event type from the body).

        This is intentionally conservative — we don't follow deep links into
        individual warning letters, only the listing pages themselves.
        """
        hub_url = self.config["hub_url"]
        logger.info("FDA: fetching dietary supplements hub from %s", hub_url)

        try:
            html = self._http_get(hub_url)
        except Exception as exc:
            logger.warning("FDA hub fetch failed: %s", exc)
            return []

        soup = BeautifulSoup(html, "lxml")
        signals: list[RawSignal] = []
        seen: set[str] = set()

        for a in soup.find_all("a", href=True):
            href: str = a["href"]
            link_text = a.get_text(strip=True)

            # Only follow internal FDA links
            if not href.startswith("/") and "fda.gov" not in href:
                continue
            # Must look safety-related based on the URL path (avoids nav false positives)
            if not _SAFETY_URL_KEYWORDS.search(href):
                continue
            # Drop generic single-word nav labels
            if _NAV_TEXT_BLOCKLIST.match(link_text):
                continue
            # Must have a meaningful title (at least 10 chars)
            if len(link_text) < 10:
                continue

            url = (
                href if href.startswith("http")
                else self.config["base_url"] + href
            )
            if url in seen:
                continue
            seen.add(url)

            source_id = self._make_source_id(self.authority, url)
            signals.append(RawSignal(
                source_id  = source_id,
                authority  = self.authority,
                url        = url,
                title      = link_text or url,
                body_text  = f"FDA dietary supplements safety page: {link_text}\nURL: {url}",
                scraped_at = self._now_iso(),
            ))

        logger.info("FDA hub: %d safety-related link(s) found", len(signals))
        return signals

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _cell_text(html: str) -> str:
        """Strip HTML tags from a DataTables cell and return plain text."""
        if not html:
            return ""
        # Suppress BeautifulSoup filename warning for short strings
        if len(html) < 10 and "<" not in html:
            return html.strip()
        return BeautifulSoup(html, "lxml").get_text(strip=True)

    @staticmethod
    def _cell_link(html: str) -> str:
        """Extract the href from the first <a> tag in a DataTables cell."""
        if not html or "<a" not in html:
            return ""
        soup = BeautifulSoup(html, "lxml")
        a = soup.find("a", href=True)
        return a["href"] if a else ""

    def _parse_date_str(self, date_str: str) -> datetime | None:
        """
        Parse FDA date formats into timezone-aware datetime.

        Handles:
          "03/28/2026"           — MM/DD/YYYY (DataTables date column)
          "2026-03-28T04:00:00Z" — ISO 8601 (from <time datetime=> inside cell)
        """
        date_str = date_str.strip()
        # ISO — from <time> element inside the cell
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except ValueError:
            pass
        # MM/DD/YYYY
        try:
            return datetime.strptime(date_str, "%m/%d/%Y").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
        return None
