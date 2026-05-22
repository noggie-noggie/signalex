"""
scrapers/open_food_facts.py — Open Food Facts product intelligence.

Source: Open Food Facts (https://world.openfoodfacts.org/)
  Public JSON API — no authentication required.
  Rate limit: polite use; we fetch one page of 20 products per scraper run.

Queries:
  1. Recent Australian dietary supplements / health foods
  2. Products with allergen alerts
  3. Products with nutrition/health claims

Returns normalised food-signal dicts tagged as signal_type="new_product"
or signal_type="claim_signal" depending on content.

Each dict is suitable for direct insertion via analytics.db.save_food_signal().
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)

_API_BASE     = "https://world.openfoodfacts.org/api/v2/search"
_SOURCE_LABEL = "open_food_facts"
_DOMAIN       = "food"
_AUTHORITY    = "open_food_facts"

# Fields to request from the API — minimise payload size
_FIELDS = ",".join([
    "code", "product_name", "brands", "categories_tags",
    "ingredients_text", "allergens_tags", "labels_tags",
    "nutriments", "last_modified_t", "url",
    "countries_tags", "quantity", "nutrition_grade_fr",
])

# Categories we consider relevant (dietary supplements, health foods, sports nutrition)
_RELEVANT_CATEGORIES = {
    "en:dietary-supplements", "en:health-foods", "en:vitamins",
    "en:minerals", "en:sports-nutrition", "en:protein-supplements",
    "en:herbal-supplements", "en:probiotics", "en:omega-3",
    "en:fish-oil-supplements", "en:meal-replacements",
    "en:weight-management-products", "en:energy-drinks",
}

# Labels that indicate a claim signal
_CLAIM_LABELS = {
    "en:organic", "en:no-additives", "en:gluten-free", "en:vegan",
    "en:vegetarian", "en:no-sugar", "en:low-fat", "en:high-protein",
    "en:non-gmo", "en:natural", "en:free-range",
}


def _make_source_id(code: str) -> str:
    key = f"{_SOURCE_LABEL}::{code}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ts_to_iso(ts: Any) -> str:
    """Convert Unix timestamp to ISO string, or return now."""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except Exception:
        return _now_iso()


def _get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "SignalexBot/1.0 (regulatory intelligence; "
            "contact: info@signalex.io)"
        ),
        "Accept": "application/json",
    })
    return s


def _infer_severity(product: dict) -> str:
    """Assign severity based on allergen presence or nutrition grade."""
    allergens = product.get("allergens_tags", [])
    if allergens:
        return "medium"
    grade = product.get("nutrition_grade_fr", "")
    if grade in ("d", "e"):
        return "medium"
    return "low"


def _extract_allergens(product: dict) -> str:
    """Return comma-separated allergen list from allergens_tags."""
    tags = product.get("allergens_tags", [])
    # Strip "en:" prefix for readability
    cleaned = [t.replace("en:", "").replace("-", " ") for t in tags if t.startswith("en:")]
    return ", ".join(cleaned) if cleaned else ""


def _extract_claims(product: dict) -> str:
    """Return comma-separated health/label claims."""
    labels = product.get("labels_tags", [])
    claims = [
        lbl.replace("en:", "").replace("-", " ")
        for lbl in labels
        if lbl.lower() in _CLAIM_LABELS or "health" in lbl or "organic" in lbl
    ]
    return ", ".join(claims) if claims else ""


def _is_relevant(product: dict) -> bool:
    """True if product falls into a category we track."""
    cats = set(product.get("categories_tags", []))
    return bool(cats & _RELEVANT_CATEGORIES)


def _product_to_signal(product: dict) -> dict:
    """Convert an Open Food Facts product dict to a food signal dict."""
    code         = product.get("code", "")
    product_name = product.get("product_name", "") or ""
    brand        = product.get("brands", "") or ""
    ingredients  = product.get("ingredients_text", "") or ""
    allergens    = _extract_allergens(product)
    claims       = _extract_claims(product)
    cats         = product.get("categories_tags", [])
    category     = cats[0].replace("en:", "").replace("-", " ") if cats else "food product"
    last_mod     = product.get("last_modified_t")
    scraped_at   = _ts_to_iso(last_mod) if last_mod else _now_iso()
    url          = product.get("url") or f"https://world.openfoodfacts.org/product/{code}"
    severity     = _infer_severity(product)

    # Determine signal type
    signal_type = "claim_signal" if claims else "new_product"

    title = f"{brand + ' — ' if brand else ''}{product_name or 'Unknown product'}"
    summary_parts = []
    if ingredients:
        summary_parts.append(f"Ingredients: {ingredients[:300]}")
    if allergens:
        summary_parts.append(f"Allergens: {allergens}")
    if claims:
        summary_parts.append(f"Claims/labels: {claims}")
    summary = " | ".join(summary_parts) if summary_parts else "Open Food Facts product record."

    # Extract primary ingredient (first word of ingredients list as proxy)
    primary_ingredient = ""
    if ingredients:
        first = ingredients.split(",")[0].strip()
        primary_ingredient = first[:100] if len(first) < 100 else ""

    return {
        "source_id":         _make_source_id(code or url),
        "domain":            _DOMAIN,
        "signal_type":       signal_type,
        "source_label":      _SOURCE_LABEL,
        "authority":         _AUTHORITY,
        "title":             title[:300],
        "summary":           summary[:1000],
        "url":               url,
        "scraped_at":        scraped_at,
        "severity":          severity,
        "company":           brand[:200],
        "brand":             brand[:200],
        "product_name":      (product_name or "")[:300],
        "ingredient":        primary_ingredient,
        "allergen":          allergens[:500],
        "claim":             claims[:500],
        "category":          category[:200],
        "recommended_action": (
            "Monitor competitor product claims and ingredient lists for "
            "positioning and compliance opportunities."
        ),
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

class OpenFoodFactsScraper:
    """Scraper for Open Food Facts product intelligence."""

    def run(self) -> list[dict]:
        """Fetch and return normalised food product signal dicts."""
        session  = _get_session()
        records: list[dict] = []

        # Query 1: Recent Australian dietary supplements
        records.extend(self._fetch_products(
            session,
            params={
                "categories_tags_en": "dietary-supplements",
                "countries_tags_en":  "australia",
                "sort_by":            "last_modified_t",
                "page_size":          "20",
                "fields":             _FIELDS,
            },
        ))

        # Query 2: Recent Australian health foods / vitamins
        records.extend(self._fetch_products(
            session,
            params={
                "categories_tags_en": "vitamins",
                "countries_tags_en":  "australia",
                "sort_by":            "last_modified_t",
                "page_size":          "20",
                "fields":             _FIELDS,
            },
        ))

        # Deduplicate by source_id
        seen:   set[str]  = set()
        unique: list[dict] = []
        for r in records:
            if r["source_id"] not in seen:
                seen.add(r["source_id"])
                unique.append(r)

        logger.info("Open Food Facts: %d unique signals", len(unique))
        return unique

    def _fetch_products(
        self,
        session: requests.Session,
        params: dict,
    ) -> list[dict]:
        """Fetch one page of products and convert to signal dicts."""
        try:
            resp = session.get(_API_BASE, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Open Food Facts: API request failed — %s", exc)
            return []

        products = data.get("products", [])
        if not products:
            logger.debug("Open Food Facts: empty result page (params=%s)", params)
            return []

        signals = []
        for p in products:
            try:
                sig = _product_to_signal(p)
                if sig["title"].strip() and sig["title"] != " — ":
                    signals.append(sig)
            except Exception:
                logger.debug("Open Food Facts: skipping malformed product record")

        logger.info("Open Food Facts: %d products from query %s", len(signals), params.get("categories_tags_en", ""))
        return signals
