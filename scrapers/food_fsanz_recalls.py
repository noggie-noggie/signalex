"""
scrapers/food_fsanz_recalls.py — FSANZ food recall notices.

Source: Food Standards Australia New Zealand (FSANZ)
  Recall alerts: https://www.foodstandards.gov.au/food-recalls/recall-alert

Each entry on the listing page links to an individual recall page at
/food-recalls/recall-alert/{slug}. The listing page also shows a brief
description in the link's parent context, giving enough data without
fetching every individual page.

Returns a list of normalised food-signal dicts — no classifier call needed.
Each dict is suitable for direct insertion via analytics.db.save_food_signal().
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from scrapers.fsanz_archive import detail_summary, listings, published

logger = logging.getLogger(__name__)

_RECALLS_URL  = "https://www.foodstandards.gov.au/food-recalls/recall-alert"
_BASE_URL     = "https://www.foodstandards.gov.au"
_SOURCE_LABEL = "food_fsanz_recalls"
_DOMAIN       = "food"
_AUTHORITY    = "fsanz"

_HIGH_KEYWORDS = [
    "listeria", "salmonella", "e. coli", "hepatitis", "pathogen",
    "foreign object", "glass", "metal", "toxin", "botulism", "undeclared allergen",
]
_MEDIUM_KEYWORDS = [
    "allergen", "mislabelled", "incorrect", "packaging error",
    "spoilage", "mould", "contamination", "microbial",
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


def _extract_allergen(text: str) -> str:
    allergens = [
        "peanut", "tree nut", "milk", "egg", "wheat", "gluten", "soy",
        "fish", "shellfish", "sesame", "lupin", "sulphite", "mustard",
        "celery", "mollusc", "crustacean", "almond", "cashew", "walnut",
        "pistachio", "macadamia",
    ]
    t = text.lower()
    found = [a for a in allergens if a in t]
    return ", ".join(found) if found else ""


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


def _scrape_recall_listing(session: requests.Session, since=None) -> list[dict]:
    """
    Scrape the FSANZ recall-alert listing page.
    Individual recalls are linked as /food-recalls/recall-alert/{slug}.
    The link's parent element holds a short description (reason, state, etc.).
    """
    records = []
    for soup in listings(session, _RECALLS_URL, since, ".recall-card__date"):
        batch = _recall_cards(soup, since)
        if since:
            nodes = soup.select(".recall-card__date")
            dates = [published(n) for n in nodes]
            if None in dates or len(batch) != sum(d.date() >= since for d in dates):
                raise ValueError("FSANZ dated recall cards did not reconcile")
        records.extend(batch)
    if since:
        for record in records:
            record["summary"] = (record["summary"] + ". " + detail_summary(session, record["url"]))[:1000]
            record["severity"] = _infer_severity(record["summary"])
    return records

def _recall_cards(soup: BeautifulSoup, since=None) -> list[dict]:
    records: list[dict] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Only individual recall pages, not navigation links
        if not href.startswith("/food-recalls/recall-alert/"):
            continue

        full_url = _BASE_URL + href
        title    = a.get_text(strip=True)
        if not title or full_url in seen:
            continue
        seen.add(full_url)

        # Context: grab the parent block for the recall description
        parent  = a.find_parent(["li", "div", "article", "p", "td"])
        context = parent.get_text(separator=" ", strip=True) if parent else title
        # Remove the title from the start of context to avoid duplication
        summary = context.replace(title, "", 1).strip().lstrip("–- ").strip()
        if not summary:
            summary = context[:500]

        card = a.find_parent("article", class_="recall-card")
        date = published(card.select_one(".recall-card__date")) if card else None
        if since and (not date or date.date() < since):
            continue
        if card:
            summary = " — ".join(n.get_text(" ", strip=True) for n in card.select(
                ".recall-card__type-label, .recall-card__business, .recall-card__location-pill"))
        severity = _infer_severity(title + " " + summary)
        allergen = _extract_allergen(title + " " + summary)

        # Try to extract company from title: usually "Company - Product - Size"
        parts   = [p.strip() for p in title.split(" - ")]
        company = parts[0] if len(parts) > 1 else ""
        product = " - ".join(parts[1:]) if len(parts) > 1 else title
        if card and card.select_one(".recall-card__business"):
            company = card.select_one(".recall-card__business").get_text(" ", strip=True)
            product = title

        records.append({
            "source_id":          _make_source_id(full_url, title),
            "domain":             _DOMAIN,
            "signal_type":        "recall",
            "source_label":       _SOURCE_LABEL,
            "authority":          _AUTHORITY,
            "title":              title[:300],
            "summary":            summary[:1000],
            "url":                full_url,
            "scraped_at":         date.isoformat() if date else _now_iso(),
            "severity":           severity,
            "company":            company[:200],
            "brand":              company[:200],
            "product_name":       product[:300],
            "allergen":           allergen,
            "category":           "food recall",
            "recommended_action": (
                "Check lot numbers against recall notice. "
                "Remove affected product from sale and notify affected consumers."
            ),
        })

    logger.info("FSANZ recalls: %d records scraped", len(records))
    return records


class FoodFSANZRecallsScraper:
    """Scraper for FSANZ food recall notices."""

    def run(self, since=None) -> list[dict]:
        session = _get_session()
        return _scrape_recall_listing(session, since)
