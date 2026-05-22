"""
scrapers/food_fsanz_updates.py — FSANZ food standards regulatory updates.

Sources:
  News/media:   https://www.foodstandards.gov.au/media/Pages/media-releases.aspx
  Standards:    https://www.foodstandards.gov.au/food-safety/food-standards-code
  Consultations: https://www.foodstandards.gov.au/consultations

Returns normalised food-signal dicts tagged as signal_type="rule_update".
Each dict is suitable for direct insertion via analytics.db.save_food_signal().
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_NEWS_URL          = "https://www.foodstandards.gov.au/news-and-media"
_STANDARDS_URL     = "https://www.foodstandards.gov.au/food-safety/food-standards-code"
_CONSULTATIONS_URL = "https://www.foodstandards.gov.au/consultations"
_BASE_URL          = "https://www.foodstandards.gov.au"
_SOURCE_LABEL      = "food_fsanz_updates"
_DOMAIN            = "food"
_AUTHORITY         = "fsanz"

# Keywords that lift severity to "medium" (i.e. actively changing something)
_MEDIUM_KEYWORDS = [
    "amendment", "variation", "new standard", "proposal", "review",
    "consultation", "gazettal", "mandatory", "prohibited", "ban",
    "maximum level", "permitted", "approved", "rejected",
]


def _make_source_id(url: str, title: str) -> str:
    key = f"{_SOURCE_LABEL}::{url}::{title[:80]}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _infer_severity(text: str) -> str:
    t = text.lower()
    if any(k in t for k in ["ban", "prohibited", "mandatory recall", "safety alert"]):
        return "high"
    if any(k in t for k in _MEDIUM_KEYWORDS):
        return "medium"
    return "low"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (compatible; SignalexBot/1.0; "
            "+https://signalex.io/bot)"
        ),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
    })
    return s


# ---------------------------------------------------------------------------
# Scrapers
# ---------------------------------------------------------------------------

def _scrape_page(session: requests.Session, url: str, signal_type: str, category: str) -> list[dict]:
    """Generic FSANZ page scraper — extracts linked items from news/standards pages."""
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("FSANZ updates: fetch failed for %s — %s", url, exc)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    records: list[dict] = []
    seen_urls: set[str] = set()

    # Collect all links that look like content pages (not nav/footer)
    for a in soup.find_all("a", href=True):
        href  = a["href"]
        text  = a.get_text(strip=True)
        if not text or len(text) < 10 or len(text) > 300:
            continue

        # Resolve relative URLs
        if href.startswith("/"):
            full_url = _BASE_URL + href
        elif href.startswith("http"):
            full_url = href
        else:
            continue

        # Skip navigation, anchors, external social links
        if "#" in href or "facebook" in href or "twitter" in href:
            continue
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        # Only keep links that look like content (path has /news, /consultation,
        # /standard, /variation, or similar meaningful segments)
        path = href.lower()
        if not any(seg in path for seg in [
            "news", "media", "consult", "standard", "variation",
            "amendment", "review", "gazettal", "proposal",
        ]):
            continue

        # Pull a brief description from the parent element
        parent  = a.find_parent(["li", "p", "article", "div"])
        summary = (parent.get_text(separator=" ", strip=True)[:500]
                   if parent else text)

        severity = _infer_severity(text + " " + summary)

        records.append({
            "source_id":         _make_source_id(full_url, text),
            "domain":            _DOMAIN,
            "signal_type":       signal_type,
            "source_label":      _SOURCE_LABEL,
            "authority":         _AUTHORITY,
            "title":             text[:300],
            "summary":           summary[:1000],
            "url":               full_url,
            "scraped_at":        _now_iso(),
            "severity":          severity,
            "company":           "",
            "brand":             "",
            "product_name":      "",
            "allergen":          "",
            "category":          category,
            "recommended_action": (
                "Review standard change for compliance impact on product labels, "
                "formulations, or permitted ingredients."
            ),
        })

    logger.info("FSANZ updates (%s): %d records from %s", signal_type, len(records), url)
    return records[:50]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

class FoodFSANZUpdatesScraper:
    """Scraper for FSANZ regulatory standards updates and consultations."""

    def run(self) -> list[dict]:
        """Fetch and return normalised food rule-update signal dicts."""
        session = _get_session()
        records: list[dict] = []

        # 1. News and media releases
        try:
            records.extend(_scrape_page(session, _NEWS_URL, "rule_update", "fsanz news"))
        except Exception:
            logger.exception("FSANZ updates: news scrape failed")

        # 2. Active consultations
        try:
            records.extend(_scrape_page(session, _CONSULTATIONS_URL, "rule_update", "consultation"))
        except Exception:
            logger.exception("FSANZ updates: consultations scrape failed")

        # Deduplicate by source_id
        seen: set[str] = set()
        unique: list[dict] = []
        for r in records:
            if r["source_id"] not in seen:
                seen.add(r["source_id"])
                unique.append(r)

        logger.info("FSANZ updates: %d unique signals", len(unique))
        return unique
