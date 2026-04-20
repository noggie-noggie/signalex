"""
scrapers/tga.py — Scraper for the Australian Therapeutic Goods Administration.

Two data sources:
  1. Safety alerts & recalls  (plain HTML, BeautifulSoup)
     URL: https://www.tga.gov.au/news/safety-alerts-and-product-recalls/safety-alerts
     Yields: ingredient warnings, product bans, advisory notices.

  2. ARTG new/cancelled listings  (JS-rendered — Playwright stub)
     URL: https://www.tga.gov.au/resources/artg
     Yields: new complementary medicine registrations, cancellations.
     NOTE: The ARTG search page is rendered by JavaScript, so a real browser
     is required.  _fetch_artg_listings() is left as a documented stub until
     Playwright is wired in.

HTML structure notes (verified against live site 2024-Q4, Drupal CMS):
  - Each alert is an <article> element.
  - Title is in the first <h3> or <h2> child, containing an <a> with href.
  - Publication date is in a <time> element (datetime="" attribute = ISO date).
  - Teaser text is in the first <p> inside the article body.
  - Relative hrefs must be prefixed with base_url.

If selectors break, inspect https://www.tga.gov.au/news/safety-alerts-... in
a browser DevTools and update the CSS selectors in _fetch_safety_alerts().
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

from bs4 import BeautifulSoup, Tag

import config
from .base import BaseScraper, RawSignal

logger = logging.getLogger(__name__)


class TGAScraper(BaseScraper):
    authority = "tga"

    def fetch_raw(self) -> list[RawSignal]:
        signals: list[RawSignal] = []
        signals.extend(self._fetch_safety_alerts())
        signals.extend(self._fetch_artg_listings())
        return signals

    # ------------------------------------------------------------------
    # Source 1: Safety alerts (plain HTML)
    # ------------------------------------------------------------------

    def _fetch_safety_alerts(self) -> list[RawSignal]:
        """
        Scrape the TGA safety alerts listing page.

        Each <article> on the page becomes one RawSignal.  Articles older than
        SIGNAL_LOOKBACK_DAYS are silently skipped.  Articles with unparseable
        dates are included (conservative — don't drop unknown-age items).

        body_text is the teaser paragraph from the listing page.  Full article
        text can be fetched later by the classifier pipeline if needed (add a
        follow-link step before calling Claude).
        """
        url = self.config["alerts_url"]
        logger.info("TGA: fetching safety alerts from %s", url)

        html = self._http_get(url)
        soup = BeautifulSoup(html, "lxml")

        # TGA Drupal theme: each alert is an <article class="node--alert node--summary">.
        # Fallback to any <article> on the page if the specific class changes.
        articles: list[Tag] = (
            soup.select("article.node--alert.node--summary")
            or soup.select("article")
        )

        if not articles:
            logger.warning("TGA: no article elements found on alerts page — selectors may need updating")
            return []

        logger.info("TGA: found %d article elements on alerts page", len(articles))

        signals: list[RawSignal] = []
        cutoff = datetime.now(timezone.utc) - timedelta(days=config.SIGNAL_LOOKBACK_DAYS)

        for article in articles:
            signal = self._parse_alert_article(article, cutoff)
            if signal is not None:
                signals.append(signal)

        logger.info("TGA: %d alert signal(s) within lookback window", len(signals))
        return signals

    def _parse_alert_article(self, article: Tag, cutoff: datetime) -> RawSignal | None:
        """
        Extract fields from a single <article> element on the alerts listing page.
        Returns None if the article is too old or has no usable title/URL.
        """
        # --- Title + URL ---
        # TGA structure: <h3 class="summary__title"><a href="/relative/path">Title</a></h3>
        # Fallback to any <h3> or <h2> with a link.
        link_tag = (
            article.select_one("h3.summary__title a[href]")
            or article.select_one("h2.summary__title a[href]")
            or article.select_one("h3 a[href]")
            or article.select_one("h2 a[href]")
        )
        if not link_tag:
            return None

        title = link_tag.get_text(strip=True)
        if not title:
            return None

        href: str = link_tag["href"]
        article_url = href if href.startswith("http") else self.config["base_url"] + href

        # --- Publication date ---
        pub_date = self._extract_date(article)

        # Skip if older than lookback window (but include if date is unknown).
        if pub_date is not None and pub_date < cutoff:
            return None

        # --- Alert type label (e.g. "Safety alerts", "Product recalls") ---
        type_el = article.select_one(
            ".field--name-field-alert-type, .summary__info, div.block-field-blocknodealertfield-alert-type"
        )
        alert_type = type_el.get_text(strip=True) if type_el else ""

        # --- Teaser / summary text ---
        # TGA uses <div class="field--name-field-summary"> for the teaser paragraph.
        teaser_el = article.select_one(
            ".field--name-field-summary, .summary__summary, .health-field__item"
        )
        teaser = teaser_el.get_text(" ", strip=True) if teaser_el else ""

        date_label = pub_date.strftime("%Y-%m-%d") if pub_date else "unknown date"
        parts = [f"Published: {date_label}"]
        if alert_type:
            parts.append(f"Type: {alert_type}")
        if teaser:
            parts.append(teaser)
        body_text = "\n".join(parts)

        return RawSignal(
            source_id=self._make_source_id(self.authority, article_url),
            authority=self.authority,
            url=article_url,
            title=title,
            body_text=body_text,
            scraped_at=self._now_iso(),
        )

    # ------------------------------------------------------------------
    # Source 2: ARTG new/cancelled listings (JS-rendered — stub)
    # ------------------------------------------------------------------

    def _fetch_artg_listings(self) -> list[RawSignal]:
        """
        Scrape the ARTG register for recently listed or cancelled complementary
        medicines (vitamins, minerals, herbal, homeopathic).

        The ARTG search UI is rendered by JavaScript, so requests + BS4 alone
        cannot retrieve results.  This stub documents what the implementation
        should do once Playwright is integrated.

        TODO (Playwright implementation):
          1. from playwright.sync_api import sync_playwright
          2. Launch chromium headless.
          3. Navigate to config["artg_search_url"].
          4. Select "Complementary medicines" in the product type filter.
          5. Sort by "Date listed" descending.
          6. Iterate pages; for each row collect:
               - ARTG number (unique ID)
               - Product name
               - Sponsor name
               - Date listed / date cancelled
               - Product type / schedule
          7. Filter rows where listed/cancelled date >= cutoff.
          8. Build one RawSignal per row; set body_text to
             "{product_name} | Sponsor: {sponsor} | ARTG: {artg_no} | {date}"
          9. Close browser.

        Alternative (no Playwright): TGA publishes a monthly ARTG extract as
        a CSV/ZIP at https://www.tga.gov.au/resources/artg/download-artg-extract
        Parsing the CSV is simpler and doesn't require a browser — consider this
        if near-real-time is not required.
        """
        logger.debug("TGA: ARTG listings scraper not yet implemented (requires Playwright)")
        return []

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _extract_date(self, article: Tag) -> datetime | None:
        """
        Try multiple strategies to extract a publication date from an article tag.

        Strategy order:
          1. <time datetime="..."> attribute — most reliable (ISO format).
          2. <time> inner text.
          3. Any element whose class name contains "date" or "time".
          4. Regex scan of the whole article text for a recognisable date pattern.
        """
        # Strategy 1 & 2: <time> element
        time_tag = article.find("time")
        if time_tag:
            iso_attr = time_tag.get("datetime", "")
            if iso_attr:
                parsed = self._parse_date_str(iso_attr)
                if parsed:
                    return parsed
            parsed = self._parse_date_str(time_tag.get_text(strip=True))
            if parsed:
                return parsed

        # Strategy 3: class-name heuristic
        date_el = article.find(class_=re.compile(r"\bdate\b|\btime\b", re.I))
        if date_el:
            parsed = self._parse_date_str(date_el.get_text(strip=True))
            if parsed:
                return parsed

        # Strategy 4: regex over full article text
        text = article.get_text(" ")
        match = re.search(
            r"\b(\d{1,2})\s+(January|February|March|April|May|June|July|August"
            r"|September|October|November|December)\s+(\d{4})\b",
            text,
            re.IGNORECASE,
        )
        if match:
            parsed = self._parse_date_str(match.group(0))
            if parsed:
                return parsed

        return None

    def _parse_date_str(self, date_str: str) -> datetime | None:
        """
        Try common TGA date formats.  Returns None if none match.

        Handles:
          "2024-03-15"           — ISO date (from <time datetime=>)
          "2024-03-15T10:30:00"  — ISO datetime
          "15 March 2024"        — TGA long-form
          "15 Mar 2024"          — TGA short-form
        """
        date_str = date_str.strip()
        # Try ISO 8601 with timezone first (handles "2026-03-02T12:00:00Z")
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except ValueError:
            pass

        # Naive formats — attach UTC so comparisons work consistently
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d %B %Y", "%d %b %Y"):
            try:
                naive = datetime.strptime(date_str[:20], fmt)
                return naive.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None
