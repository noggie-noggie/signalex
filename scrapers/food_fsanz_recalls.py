"""
scrapers/food_fsanz_recalls.py — FSANZ food recall notices.

Source: Food Standards Australia New Zealand (FSANZ)
  Recalls page: https://www.foodstandards.gov.au/food-safety/food-recalls
  RSS feed:     https://www.foodstandards.gov.au/rss/recall.xml  (if available)

Returns a list of normalised food-signal dicts — no classifier call needed.
Each dict is suitable for direct insertion via analytics.db.save_food_signal().

Fields returned per record:
  source_id, domain, signal_type, source_label, authority,
  title, summary, url, scraped_at, severity,
  company, brand, product_name, allergen, category, recommended_action
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_RECALLS_URL  = "https://www.foodstandards.gov.au/food-safety/food-recalls"
_RSS_URL      = "https://www.foodstandards.gov.au/rss/recall.xml"
_BASE_URL     = "https://www.foodstandards.gov.au"
_SOURCE_LABEL = "food_fsanz_recalls"
_DOMAIN       = "food"
_AUTHORITY    = "fsanz"

# Severity heuristics based on recall reason keywords
_HIGH_KEYWORDS = [
    "undeclared allergen", "listeria", "salmonella", "e. coli",
    "foreign object", "glass", "metal", "pathogen", "contamination",
    "undeclared", "hepatitis",
]
_MEDIUM_KEYWORDS = [
    "labelling", "mislabelled", "incorrect", "wrong ingredient",
    "packaging", "spoilage", "mould", "quality",
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
# RSS feed parser — preferred if available
# ---------------------------------------------------------------------------

def _scrape_rss(session: requests.Session) -> list[dict]:
    """Attempt to fetch and parse the FSANZ recall RSS feed."""
    try:
        resp = session.get(_RSS_URL, timeout=30)
        resp.raise_for_status()
    except Exception:
        logger.debug("FSANZ recalls: RSS feed not available, falling back to HTML")
        return []

    soup = BeautifulSoup(resp.text, "xml")
    items = soup.find_all("item")
    if not items:
        # Try lxml's html parser as fallback for malformed XML
        soup = BeautifulSoup(resp.text, "lxml")
        items = soup.find_all("item")

    if not items:
        return []

    records: list[dict] = []
    for item in items[:50]:
        title   = (item.find("title") or item.find("title")).get_text(strip=True) if item.find("title") else ""
        link    = item.find("link")
        url     = link.get_text(strip=True) if link else _RECALLS_URL
        desc_el = item.find("description")
        summary = desc_el.get_text(strip=True) if desc_el else ""
        pub_el  = item.find("pubDate")
        date    = pub_el.get_text(strip=True) if pub_el else _now_iso()

        if not title:
            continue

        severity = _infer_severity(title + " " + summary)
        records.append({
            "source_id":         _make_source_id(url, title),
            "domain":            _DOMAIN,
            "signal_type":       "recall",
            "source_label":      _SOURCE_LABEL,
            "authority":         _AUTHORITY,
            "title":             title[:300],
            "summary":           summary[:1000],
            "url":               url,
            "scraped_at":        _now_iso(),
            "severity":          severity,
            "company":           "",
            "brand":             "",
            "product_name":      title[:200],
            "allergen":          _extract_allergen(title + " " + summary),
            "category":          "food recall",
            "recommended_action": "Review product inventory; check lot numbers against recall notice.",
        })

    logger.info("FSANZ recalls RSS: %d records", len(records))
    return records


# ---------------------------------------------------------------------------
# HTML page parser — fallback
# ---------------------------------------------------------------------------

def _scrape_html(session: requests.Session) -> list[dict]:
    """Scrape the FSANZ food recalls HTML page."""
    try:
        resp = session.get(_RECALLS_URL, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("FSANZ recalls HTML: fetch failed — %s", exc)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    records: list[dict] = []

    # FSANZ recall pages typically list recalls in tables or definition lists.
    # Try several common structures.

    # Pattern 1: <table> with recall rows
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows[1:]:   # skip header
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            texts = [c.get_text(strip=True) for c in cells]
            link  = row.find("a", href=True)
            url   = (_BASE_URL + link["href"]) if link and link["href"].startswith("/") else (link["href"] if link else _RECALLS_URL)
            title = texts[0] if texts else ""
            if not title or len(title) < 5:
                continue
            summary    = " | ".join(t for t in texts[1:5] if t)
            severity   = _infer_severity(title + " " + summary)
            company    = texts[2] if len(texts) > 2 else ""
            allergen   = _extract_allergen(title + " " + summary)
            records.append({
                "source_id":         _make_source_id(url, title),
                "domain":            _DOMAIN,
                "signal_type":       "recall",
                "source_label":      _SOURCE_LABEL,
                "authority":         _AUTHORITY,
                "title":             title[:300],
                "summary":           summary[:1000],
                "url":               url,
                "scraped_at":        _now_iso(),
                "severity":          severity,
                "company":           company[:200],
                "brand":             "",
                "product_name":      title[:200],
                "allergen":          allergen,
                "category":          "food recall",
                "recommended_action": "Review product inventory; check lot numbers against recall notice.",
            })

    if records:
        logger.info("FSANZ recalls HTML (table): %d records", len(records))
        return records[:100]

    # Pattern 2: article / list-item approach
    for article in soup.find_all(["article", "li", "div"], class_=re.compile(r"recall|result|item|product", re.I)):
        link  = article.find("a", href=True)
        if not link:
            continue
        href  = link["href"]
        url   = (_BASE_URL + href) if href.startswith("/") else href
        title = link.get_text(strip=True) or article.get_text(strip=True)[:100]
        if not title or len(title) < 5:
            continue
        summary  = article.get_text(separator=" ", strip=True)[:500]
        severity = _infer_severity(title + " " + summary)
        records.append({
            "source_id":         _make_source_id(url, title),
            "domain":            _DOMAIN,
            "signal_type":       "recall",
            "source_label":      _SOURCE_LABEL,
            "authority":         _AUTHORITY,
            "title":             title[:300],
            "summary":           summary[:1000],
            "url":               url,
            "scraped_at":        _now_iso(),
            "severity":          severity,
            "company":           "",
            "brand":             "",
            "product_name":      title[:200],
            "allergen":          _extract_allergen(title + " " + summary),
            "category":          "food recall",
            "recommended_action": "Review product inventory; check lot numbers against recall notice.",
        })

    logger.info("FSANZ recalls HTML (article): %d records", len(records))
    return records[:100]


def _extract_allergen(text: str) -> str:
    """Extract allergen mentions from recall text."""
    allergens = [
        "peanut", "tree nut", "milk", "egg", "wheat", "gluten", "soy",
        "fish", "shellfish", "sesame", "lupin", "sulphite", "mustard",
        "celery", "mollusc", "crustacean", "almond", "cashew", "walnut",
        "pistachio", "macadamia",
    ]
    t = text.lower()
    found = [a for a in allergens if a in t]
    return ", ".join(found) if found else ""


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

class FoodFSANZRecallsScraper:
    """Scraper for FSANZ food recall notices."""

    def run(self) -> list[dict]:
        """Fetch and return normalised food recall signal dicts."""
        session = _get_session()

        # Try RSS first (cleaner data); fall back to HTML
        records = _scrape_rss(session)
        if not records:
            records = _scrape_html(session)

        # Deduplicate by source_id
        seen: set[str] = set()
        unique: list[dict] = []
        for r in records:
            if r["source_id"] not in seen:
                seen.add(r["source_id"])
                unique.append(r)

        logger.info("FSANZ recalls: %d unique signals", len(unique))
        return unique
