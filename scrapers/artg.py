"""
scrapers/artg.py — Scraper for ARTG new medicine listings.

Monitors the Australian Register of Therapeutic Goods for products listed in
the last 30 days.  Only "Medicines" registration types are returned (excludes
devices, biologicals, and other therapeutic goods).

Approach:
  1. Fetch the base ARTG page → extract recent ARTG entry links (Drupal static HTML).
  2. For each entry, fetch the individual entry page to read:
       - ARTG Date, Registration Type, Therapeutic good type, Sponsor,
         Product name, Product details (ingredients where available).
  3. Filter for Medicines that were listed within the last 30 days.
  4. Build one RawSignal per qualifying entry.

Note: The ARTG search UI is JS-rendered, but the base listing page and all
individual entry pages (/resources/artg/<id>) are plain HTML, so Playwright
is not required for this approach.

The WAF on www.tga.gov.au blocks rapid filtered requests.  Polite per-request
delays keep the scraper within rate limits.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta, timezone

from bs4 import BeautifulSoup, Tag

import config
from .base import BaseScraper, RawSignal

logger = logging.getLogger(__name__)

# ARTG entry-page URL pattern: /resources/artg/<numeric-id>
_ARTG_ENTRY_RE = re.compile(r"^/resources/artg/(\d+)$")

# Registration types we care about for VMS intelligence
_MEDICINE_TYPES = frozenset([
    "medicine",
    "listed medicine",
    "registered medicine",
    "complementary medicine",
    "listed complementary medicine",
    "registered complementary medicine",
    "otc medicine",
])

# How far back to look for new listings
_ARTG_LOOKBACK_DAYS = 30


class ARTGScraper(BaseScraper):
    """
    Scrapes the TGA ARTG register for newly listed medicines.

    The scraper collects the ~25 most-recently-updated entries visible on the
    base ARTG page, then fetches each individual entry page for structured
    data (date, type, sponsor, product details).  Entries older than
    _ARTG_LOOKBACK_DAYS or non-medicine types are skipped.
    """

    authority = "artg"

    def fetch_raw(self) -> list[RawSignal]:
        base_url = self.config.get("artg_search_url", "https://www.tga.gov.au/resources/artg")
        logger.info("ARTG: fetching recent entries from %s", base_url)

        # Step 1: collect entry links from the base page
        entry_links = self._get_entry_links(base_url)
        if not entry_links:
            logger.warning("ARTG: no entry links found on base page — selectors may have changed")
            return []

        logger.info("ARTG: found %d entry links to inspect", len(entry_links))

        # Step 2: fetch each entry page; filter by type and date
        cutoff = datetime.now(timezone.utc) - timedelta(days=_ARTG_LOOKBACK_DAYS)
        signals: list[RawSignal] = []

        for artg_id, href in entry_links:
            time.sleep(0.8)  # polite rate limit — TGA WAF is sensitive
            try:
                signal = self._parse_entry_page(artg_id, href, cutoff)
                if signal is not None:
                    signals.append(signal)
            except Exception as exc:
                logger.debug("ARTG: failed to parse entry %s: %s", artg_id, exc)

        logger.info("ARTG: %d medicine listing(s) within last %d days", len(signals), _ARTG_LOOKBACK_DAYS)
        return signals

    # ------------------------------------------------------------------
    # Step 1: collect entry links from the base ARTG page
    # ------------------------------------------------------------------

    def _get_entry_links(self, base_url: str) -> list[tuple[str, str]]:
        """
        Fetch the base ARTG page and return (artg_id, full_url) pairs for
        all ARTG entry links found.
        """
        try:
            html = self._http_get(base_url)
        except Exception as exc:
            logger.error("ARTG: failed to fetch base page: %s", exc)
            return []

        soup = BeautifulSoup(html, "lxml")
        results: list[tuple[str, str]] = []
        seen: set[str] = set()

        for a in soup.find_all("a", href=True):
            href: str = a["href"]
            m = _ARTG_ENTRY_RE.match(href)
            if not m:
                continue
            artg_id = m.group(1)
            if artg_id in seen:
                continue
            seen.add(artg_id)
            full_url = "https://www.tga.gov.au" + href
            results.append((artg_id, full_url))

        return results

    # ------------------------------------------------------------------
    # Step 2: parse individual entry pages
    # ------------------------------------------------------------------

    def _parse_entry_page(
        self, artg_id: str, url: str, cutoff: datetime
    ) -> RawSignal | None:
        """
        Fetch /resources/artg/<id> and extract structured fields.
        Returns None if the entry is a non-medicine type, older than cutoff,
        or if the page fails to load.
        """
        html = self._http_get(url)
        soup = BeautifulSoup(html, "lxml")

        fields = self._extract_fields(soup)

        # --- Filter: registration / therapeutic good type ---
        reg_type = fields.get("registration_type", "").lower()
        tg_type  = fields.get("therapeutic_good_type", "").lower()

        is_medicine = (
            any(kw in reg_type for kw in ("medicine", "complementary"))
            or any(kw in tg_type for kw in ("medicine", "complementary"))
        )
        if not is_medicine:
            logger.debug("ARTG: skipping %s — type '%s'/'%s'", artg_id, reg_type, tg_type)
            return None

        # --- Filter: date ---
        artg_date = self._parse_artg_date(fields.get("artg_date", ""))
        if artg_date is not None and artg_date < cutoff:
            logger.debug("ARTG: skipping %s — date %s older than cutoff", artg_id, artg_date)
            return None

        # --- Build signal ---
        product_name = fields.get("product_name") or fields.get("artg_name", "")
        sponsor      = fields.get("sponsor", "")
        date_label   = artg_date.strftime("%Y-%m-%d") if artg_date else "unknown date"

        title = f"{product_name} ({artg_id})"

        body_parts = [
            f"ARTG ID: {artg_id}",
            f"Date listed: {date_label}",
            f"Sponsor: {sponsor}",
            f"Registration type: {fields.get('registration_type', '')}",
            f"Therapeutic good type: {fields.get('therapeutic_good_type', '')}",
        ]
        if fields.get("formulation"):
            body_parts.append(f"Formulation: {fields['formulation']}")
        if fields.get("active_ingredients"):
            body_parts.append(f"Active ingredients: {fields['active_ingredients']}")
        if fields.get("product_details"):
            body_parts.append(f"Product details: {fields['product_details'][:500]}")

        return RawSignal(
            source_id  = self._make_source_id(self.authority, url),
            authority  = self.authority,
            url        = url,
            title      = title,
            body_text  = "\n".join(body_parts),
            scraped_at = self._now_iso(),
        )

    def _extract_fields(self, soup: BeautifulSoup) -> dict[str, str]:
        """
        Extract structured fields from an ARTG entry page.

        TGA entry pages render a definition list of labelled fields using a
        repeated pattern:
            <div class="field__label">Field Name</div>
            <div class="field__item">Value</div>

        Falls back to a <table> pattern if the definition list is absent.
        """
        fields: dict[str, str] = {}

        # Pattern A: .field__label / .field__item pairs (Drupal field display)
        labels = soup.select(".field__label, .field-label")
        if labels:
            for label_el in labels:
                label = label_el.get_text(strip=True).rstrip(":").lower().replace(" ", "_")
                # Sibling or next element is the value
                value_el = (
                    label_el.find_next_sibling(class_=re.compile(r"field__item|field-item"))
                    or label_el.find_next_sibling()
                )
                if value_el:
                    fields[label] = value_el.get_text(" ", strip=True)
            if fields:
                return self._normalise_fields(fields)

        # Pattern B: <th>/<td> pairs in a table
        for row in soup.select("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                label = th.get_text(strip=True).rstrip(":").lower().replace(" ", "_")
                fields[label] = td.get_text(" ", strip=True)
        if fields:
            return self._normalise_fields(fields)

        # Pattern C: fallback — scan heading-like elements for key:value patterns
        main = soup.select_one("main, [role=main], #content, .content")
        if main:
            text = main.get_text(" ")
            for m in re.finditer(r"(ARTG (?:ID|Name|Date)|Product name|Sponsor|Therapeutic good type|Registration Type)\s*\n?\s*([^\n]{3,100})", text):
                label = m.group(1).lower().replace(" ", "_")
                fields[label] = m.group(2).strip()

        return self._normalise_fields(fields)

    @staticmethod
    def _normalise_fields(raw: dict[str, str]) -> dict[str, str]:
        """Map raw Drupal field keys to consistent internal names."""
        mapping = {
            "artg_id":                    "artg_id",
            "artg_name":                  "artg_name",
            "product_name":               "product_name",
            "artg_date":                  "artg_date",
            "registration_type":          "registration_type",
            "therapeutic_good_type":      "therapeutic_good_type",
            "sponsor":                    "sponsor",
            "formulation":                "formulation",
            "active_ingredients":         "active_ingredients",
            "ingredients":                "active_ingredients",
            "product_details":            "product_details",
        }
        result = {}
        for k, v in raw.items():
            canonical = mapping.get(k, k)
            result[canonical] = v
        return result

    def _parse_artg_date(self, date_str: str) -> datetime | None:
        """
        Parse ARTG date strings.

        Handles:
          "31 March 2026"   — long form (page display)
          "31 Mar 2026"     — short form
          "2026-03-31"      — ISO
        """
        date_str = date_str.strip()
        if not date_str:
            return None
        for fmt in ("%d %B %Y", "%d %b %Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str[:20], fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None
