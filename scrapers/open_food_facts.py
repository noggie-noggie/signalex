"""
scrapers/open_food_facts.py — Open Food Facts product intelligence.

Source: Open Food Facts (https://world.openfoodfacts.org/)
  Public JSON API — no authentication required.
  Rate limit: polite use; we fetch one page of 20 products per scraper run.

Queries:
  1. Recent Australian protein/snack foods
  2. Recent Australian beverages / functional drinks
  3. Recent Australian plant-based foods

Returns normalised food-signal dicts tagged as signal_type="new_product"
or signal_type="claim_signal" depending on content.

Each dict is suitable for direct insertion via analytics.db.save_food_signal().
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests

from services.food_supplement_filter import is_supplement_like_food

logger = logging.getLogger(__name__)

_API_BASE     = "https://world.openfoodfacts.org/api/v2/search"
_SOURCE_LABEL = "open_food_facts"
_DOMAIN       = "food"
_AUTHORITY    = "open_food_facts"

# Fields to request from the API — minimise payload size
_FIELDS = ",".join([
    "code", "product_name", "brands", "categories_tags",
    "ingredients_text", "allergens_tags", "labels_tags",
    "last_modified_t", "url",
    "countries_tags", "quantity", "nutrition_grade_fr",
])

# Categories we consider relevant for the Food-only launch. Deliberately
# excludes dietary supplements, vitamins, minerals, capsules/tablets, etc.
_RELEVANT_CATEGORIES = {
    "en:protein-bars", "en:snacks", "en:energy-drinks",
    "en:beverages", "en:plant-based-foods-and-beverages",
    "en:plant-based-milks", "en:kombuchas", "en:yogurts",
    "en:fermented-drinks", "en:meal-replacements",
}

_EXCLUDED_CATEGORIES = {
    "en:dietary-supplements", "en:vitamins", "en:minerals",
    "en:protein-supplements", "en:herbal-supplements", "en:omega-3",
    "en:fish-oil-supplements", "en:multivitamins",
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

    categories = ("protein-bars", "energy-drinks", "plant-based-foods-and-beverages")
    archive_page_size = 100

    def run(self, since=None) -> list[dict]:
        """Fetch and return normalised food product signal dicts."""
        session  = _get_session()
        records: list[dict] = []
        self.incomplete_queries = []

        for idx, category in enumerate(self.categories):
            if idx:
                time.sleep(7)  # search rate limit: <=10 requests/minute
            for page in range(1, 11 if since else 2):
                if page > 1:
                    time.sleep(7)
                params = dict(categories_tags=f"en:{category}", countries_tags="en:australia",
                              sort_by="last_modified_t", page_size=self.archive_page_size if since else 20,
                              page=page, fields=_FIELDS)
                try:
                    batch, done = self._fetch_products(session, params, since)
                except (requests.RequestException, ValueError) as exc:
                    self.incomplete_queries.append(category)
                    response = getattr(exc, "response", None)
                    if response is not None and (response.status_code in (403, 429) or response.headers.get("Retry-After")):
                        return list({r["source_id"]: r for r in records}.values())
                    break
                records.extend(batch)
                if done:
                    break
            else:
                if since:
                    self.incomplete_queries.append(category)

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
        since=None,
    ) -> tuple[list[dict], bool]:
        """Fetch one page of products and convert to signal dicts."""
        try:
            resp = session.get(_API_BASE, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Open Food Facts: API request failed — %s", exc)
            raise

        products = data.get("products", [])
        if not products:
            logger.debug("Open Food Facts: empty result page (params=%s)", params)
            if "products" not in data:
                raise ValueError("OFF search response missing products")
            return [], True

        signals = []
        dates = []
        for p in products:
            try:
                if since:
                    ts = p.get("last_modified_t")
                    if not ts:
                        raise ValueError("Missing product modification timestamp")
                    dates.append(datetime.fromtimestamp(int(ts), timezone.utc).date())
                    if dates[-1] < since:
                        continue
                if params["categories_tags"] not in (p.get("categories_tags") or []) or "en:australia" not in (p.get("countries_tags") or []):
                    continue
                if is_supplement_like_food(p):
                    logger.info(
                        "Open Food Facts: excluding supplement-like product %r",
                        p.get("product_name", "") or p.get("code", ""),
                    )
                    continue
                if not _is_relevant(p):
                    continue
                sig = _product_to_signal(p)
                if is_supplement_like_food(sig):
                    logger.info(
                        "Open Food Facts: excluding supplement-like signal %r",
                        sig["title"],
                    )
                    continue
                if sig["title"].strip() and sig["title"] != " — ":
                    signals.append(sig)
            except Exception:
                if since:
                    raise
                logger.debug("Open Food Facts: skipping malformed product record")

        logger.info("Open Food Facts: %d products from query %s", len(signals), params.get("categories_tags_en", ""))
        if since and dates != sorted(dates, reverse=True):
            raise ValueError("OFF did not honour modification-date ordering")
        exhausted = len(products) < params["page_size"]
        if since and "count" in data:
            exhausted = int(params["page"]) * int(params["page_size"]) >= int(data["count"])
        return signals, exhausted or bool(dates and dates[-1] < since)
