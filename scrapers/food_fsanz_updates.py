"""
scrapers/food_fsanz_updates.py — FSANZ food standards regulatory updates.

Sources (in priority order):
  1. RSS feed:   https://www.foodstandards.gov.au/rss.xml  (proposals, applications, news)
  2. Media page: https://www.foodstandards.gov.au/media   (media releases, calls for comment)

Returns normalised food-signal dicts tagged as signal_type="rule_update".
Each dict is suitable for direct insertion via analytics.db.save_food_signal().
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_RSS_URL      = "https://www.foodstandards.gov.au/rss.xml"
_MEDIA_URL    = "https://www.foodstandards.gov.au/media"
_BASE_URL     = "https://www.foodstandards.gov.au"
_SOURCE_LABEL = "food_fsanz_updates"
_DOMAIN       = "food"
_AUTHORITY    = "fsanz"

_HIGH_KEYWORDS = [
    "ban", "prohibited", "mandatory recall", "safety alert",
    "urgent", "immediate", "toxin", "contamination",
]
_MEDIUM_KEYWORDS = [
    "proposal", "amendment", "variation", "new standard", "review",
    "consultation", "call for comment", "maximum level", "permitted",
    "approved", "rejected", "gazettal", "application",
]


def _make_source_id(url: str, title: str) -> str:
    key = f"{_SOURCE_LABEL}::{url}::{title[:80]}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _infer_severity(text: str) -> str:
    t = text.lower()
    if any(k in t for k in _HIGH_KEYWORDS):
        return "high"
    if any(k in t for k in _MEDIUM_KEYWORDS):
        return "medium"
    return "low"


def _parse_rfc2822(date_str: str) -> str:
    """Parse RFC 2822 pubDate into ISO string. Returns now on failure."""
    try:
        return parsedate_to_datetime(date_str).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (compatible; SignalexBot/1.0; "
            "+https://signalex.io/bot)"
        ),
        "Accept": "text/html,application/xhtml+xml,application/rss+xml,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
    })
    return s


def _scrape_rss(session: requests.Session) -> list[dict]:
    """Parse the FSANZ RSS feed for standards proposals, applications, and news."""
    try:
        resp = session.get(_RSS_URL, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("FSANZ updates RSS: fetch failed — %s", exc)
        return []

    soup = BeautifulSoup(resp.content, "xml")
    items = soup.find_all("item")
    if not items:
        soup  = BeautifulSoup(resp.content, "lxml")
        items = soup.find_all("item")

    records: list[dict] = []
    for item in items:
        title_el = item.find("title")
        link_el  = item.find("link")
        desc_el  = item.find("description")
        pub_el   = item.find("pubDate")

        title   = title_el.get_text(strip=True) if title_el else ""
        url     = link_el.get_text(strip=True)  if link_el  else _RSS_URL
        summary = desc_el.get_text(strip=True)  if desc_el  else ""
        pub     = _parse_rfc2822(pub_el.get_text()) if pub_el else _now_iso()

        if not title:
            continue

        severity = _infer_severity(title + " " + summary)
        records.append({
            "source_id":          _make_source_id(url, title),
            "domain":             _DOMAIN,
            "signal_type":        "rule_update",
            "source_label":       _SOURCE_LABEL,
            "authority":          _AUTHORITY,
            "title":              title[:300],
            "summary":            summary[:1000],
            "url":                url,
            "scraped_at":         pub,
            "severity":           severity,
            "company":            "",
            "brand":              "",
            "product_name":       "",
            "allergen":           "",
            "category":           "fsanz regulatory update",
            "recommended_action": (
                "Review standard change for compliance impact on "
                "product labels, formulations, or permitted ingredients."
            ),
        })

    logger.info("FSANZ updates RSS: %d records", len(records))
    return records


def _scrape_media_page(session: requests.Session) -> list[dict]:
    """Scrape the FSANZ media page for calls for comment and media releases."""
    try:
        resp = session.get(_MEDIA_URL, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("FSANZ updates media page: fetch failed — %s", exc)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    records: list[dict] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)

        if not text or len(text) < 10 or len(text) > 300:
            continue
        if not href.startswith("/media/") and not href.startswith("/news"):
            continue

        full_url = _BASE_URL + href if href.startswith("/") else href
        if full_url in seen:
            continue
        seen.add(full_url)

        parent  = a.find_parent(["li", "div", "article", "p"])
        summary = parent.get_text(separator=" ", strip=True)[:500] if parent else text
        severity = _infer_severity(text + " " + summary)

        records.append({
            "source_id":          _make_source_id(full_url, text),
            "domain":             _DOMAIN,
            "signal_type":        "rule_update",
            "source_label":       _SOURCE_LABEL,
            "authority":          _AUTHORITY,
            "title":              text[:300],
            "summary":            summary[:1000],
            "url":                full_url,
            "scraped_at":         _now_iso(),
            "severity":           severity,
            "company":            "",
            "brand":              "",
            "product_name":       "",
            "allergen":           "",
            "category":           "fsanz news",
            "recommended_action": (
                "Review for compliance or market positioning impact."
            ),
        })

    logger.info("FSANZ updates media: %d records", len(records))
    return records[:50]


class FoodFSANZUpdatesScraper:
    """Scraper for FSANZ regulatory standards updates and consultations."""

    def run(self) -> list[dict]:
        session = _get_session()
        records: list[dict] = []

        # 1. RSS feed (primary)
        try:
            records.extend(_scrape_rss(session))
        except Exception:
            logger.exception("FSANZ updates: RSS scrape failed")

        # 2. Media page (supplementary)
        try:
            records.extend(_scrape_media_page(session))
        except Exception:
            logger.exception("FSANZ updates: media page scrape failed")

        # Deduplicate by source_id
        seen:   set[str]   = set()
        unique: list[dict] = []
        for r in records:
            if r["source_id"] not in seen:
                seen.add(r["source_id"])
                unique.append(r)

        logger.info("FSANZ updates: %d unique signals", len(unique))
        return unique
