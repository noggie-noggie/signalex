"""
scrapers/retail.py — Retail channel intelligence scrapers.

Two sources:

  1. iHerb AU new products  (https://au.iherb.com/new-products)
     The /c/new-products category page is Cloudflare-protected (403).
     However https://au.iherb.com/new-products (no /c/ prefix) is a
     server-rendered HTML page that returns 200 and embeds full product data
     in data-cart-info JSON attributes and data-ga-* attributes on each
     product's anchor element.
     Extracts: product name, brand, price, product URL.

  2. Chemist Warehouse vitamins  (sitemap + product page __NEXT_DATA__)
     CW's category pages load products client-side via an undiscoverable
     internal API.  However:
       a) CW publishes a products sitemap at
          https://static.chemistwarehouse.com.au/AMS/sitemap/cwh/products.xml
          with ~29k product slugs (no lastmod dates).
       b) Individual product pages (/buy/<id>/<slug>) return 200 with full
          product data in __NEXT_DATA__ props.pageProps.product.product.
     Strategy: filter the sitemap for VMS-relevant slugs, fetch the top N
     product pages, and extract name, category, description.
     "New arrivals" filtering is approximate (highest numeric IDs tend to be
     newer) since CW does not publish date-added in the sitemap or page data.
"""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Iterator

import requests
from bs4 import BeautifulSoup

import config
from .base import BaseScraper, RawSignal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# iHerb constants
# ---------------------------------------------------------------------------

# This URL is server-rendered and not Cloudflare-protected.
_IHERB_NEW_URL = "https://au.iherb.com/new-products"

# Max products to return per run (the page shows ~48 "Add to basket" items
# plus up to 96 product link anchors with GA metadata).
_IHERB_MAX_PRODUCTS = 30

# ---------------------------------------------------------------------------
# Chemist Warehouse constants
# ---------------------------------------------------------------------------

_CW_SITEMAP_URL  = "https://static.chemistwarehouse.com.au/AMS/sitemap/cwh/products.xml"
_CW_PRODUCT_BASE = "https://www.chemistwarehouse.com.au"

# Slug keywords that identify VMS (vitamins, minerals, supplements) products.
_CW_VMS_KEYWORDS = [
    "vitamin", "mineral", "supplement", "omega", "fish-oil", "probiotic",
    "collagen", "magnesium", "calcium", "zinc", "iron", "protein", "creatine",
    "turmeric", "elderberry", "melatonin", "coenzyme", "coq10", "b12",
    "folate", "biotin", "ashwagandha", "valerian", "echinacea", "glucosamine",
]

# How many CW product pages to fetch per run (rate-limited).
_CW_MAX_PRODUCTS = 20

# Polite delay between individual product page fetches.
_CW_FETCH_DELAY = 1.2  # seconds


# ===========================================================================
# iHerb scraper
# ===========================================================================

class iHerbScraper(BaseScraper):
    """
    Scrapes iHerb AU new products via the server-rendered /new-products page.

    Product data is extracted from two sources on the page:
      - data-ga-* attributes on product anchor elements (brand, product ID,
        price, GA position)
      - [itemprop=name] spans for the product title
      - a[itemprop=url] for the canonical product URL

    Products are presented without individual "date added" timestamps; all
    items on this page are iHerb-designated new arrivals for AU.
    """

    authority = "iherb"

    def fetch_raw(self) -> list[RawSignal]:
        logger.info("iHerb: fetching new products from %s", _IHERB_NEW_URL)
        try:
            html = self._http_get(_IHERB_NEW_URL)
        except Exception as exc:
            logger.warning("iHerb: fetch failed: %s", exc)
            return []

        soup = BeautifulSoup(html, "lxml")
        signals = self._parse_products(soup)

        if not signals:
            logger.warning("iHerb: no products extracted — page structure may have changed")
        else:
            logger.info("iHerb: extracted %d new product(s)", len(signals))
        return signals

    def _parse_products(self, soup: BeautifulSoup) -> list[RawSignal]:
        """
        Extract products from the /new-products page.

        iHerb embeds product metadata in data-ga-* attributes on the main
        product anchor (a.absolute-link[itemprop=url]).  Each anchor has:
          data-ga-brand-name   — brand string
          data-ga-product-id   — iHerb product ID
          data-ga-discount-price — price (AUD, no currency symbol)
          href / itemprop=url  — canonical product URL
          title / aria-label   — full product name

        The "Add to basket" button siblings carry richer data in data-cart-info
        JSON but are not always present for every product, so we key off the
        anchor elements.
        """
        seen: set[str] = set()
        signals: list[RawSignal] = []

        # Primary: product anchors with GA metadata
        for a in soup.select("a.absolute-link[itemprop=url][data-ga-product-id]"):
            product_id = a.get("data-ga-product-id", "")
            if product_id in seen:
                continue
            seen.add(product_id)

            name  = (a.get("aria-label") or a.get("title") or "").strip()
            if not name:
                # Fall back to the itemprop=name sibling
                container = a.parent
                name_el = container.select_one("[itemprop=name]") if container else None
                name = name_el.get_text(strip=True) if name_el else ""
            if not name:
                continue

            brand = a.get("data-ga-brand-name", "").strip()
            price_raw = a.get("data-ga-discount-price", "")
            price = f"AU${price_raw}" if price_raw else ""
            url   = a.get("href", _IHERB_NEW_URL)
            if not url.startswith("http"):
                url = "https://au.iherb.com" + url

            # Enrich from the data-cart-info button if present in the same container
            cart_info = self._find_cart_info(a)
            if cart_info and not price:
                price = cart_info.get("listPrice", "")

            signals.append(RawSignal(
                source_id  = self._make_source_id(self.authority, url),
                authority  = self.authority,
                url        = url,
                title      = name,
                body_text  = self._build_body(name, brand, price),
                scraped_at = self._now_iso(),
            ))

            if len(signals) >= _IHERB_MAX_PRODUCTS:
                break

        # Fallback: if no GA anchors found, try itemprop=name spans
        if not signals:
            signals.extend(self._parse_fallback(soup, seen))

        return signals

    @staticmethod
    def _find_cart_info(anchor) -> dict:
        """
        Walk up from the product anchor to find a sibling "Add to basket"
        button with data-cart-info JSON, and return its first lineItem dict.
        """
        container = anchor.parent
        for _ in range(5):
            if container is None:
                break
            btn = container.select_one("button[data-cart-info]")
            if btn:
                try:
                    info = json.loads(btn["data-cart-info"])
                    items = info.get("lineItems", [])
                    return items[0] if items else {}
                except (json.JSONDecodeError, KeyError):
                    pass
            container = container.parent
        return {}

    def _parse_fallback(self, soup: BeautifulSoup, seen: set) -> list[RawSignal]:
        """
        Fallback: extract from [itemprop=name] + nearest a[href*=/pr/].
        Used when GA anchor structure is absent.
        """
        signals = []
        for name_el in soup.select("[itemprop=name]"):
            name = name_el.get_text(strip=True)
            if not name or len(name) < 5:
                continue
            # Find the nearest product link
            link = name_el.find_parent("a") or name_el.find_next("a", href=re.compile(r"/pr/"))
            url  = link["href"] if link else _IHERB_NEW_URL
            if not url.startswith("http"):
                url = "https://au.iherb.com" + url
            if url in seen:
                continue
            seen.add(url)
            signals.append(RawSignal(
                source_id  = self._make_source_id(self.authority, url),
                authority  = self.authority,
                url        = url,
                title      = name,
                body_text  = self._build_body(name, "", ""),
                scraped_at = self._now_iso(),
            ))
            if len(signals) >= _IHERB_MAX_PRODUCTS:
                break
        return signals

    @staticmethod
    def _build_body(name: str, brand: str, price: str) -> str:
        parts = [f"Product: {name}"]
        if brand:
            parts.append(f"Brand: {brand}")
        if price:
            parts.append(f"Price (AUD): {price}")
        parts.append("Source: iHerb AU new products (https://au.iherb.com/new-products)")
        return "\n".join(parts)


# ===========================================================================
# Chemist Warehouse scraper
# ===========================================================================

class ChemistWarehouseScraper(BaseScraper):
    """
    Scrapes Chemist Warehouse for VMS product intelligence via their sitemap.

    Approach:
      1. Download the CW products sitemap XML (~29k URLs, ~3.8MB).
      2. Filter product slugs containing VMS-relevant keywords.
      3. Sort filtered URLs so highest numeric product IDs (likely newer) come first.
      4. Fetch up to _CW_MAX_PRODUCTS individual product pages, each of which
         returns full product data in __NEXT_DATA__ (no JS rendering required).
      5. Build RawSignals from name, description, category type.

    Limitation: CW does not publish "date added" in the sitemap or page data,
    so we cannot strictly filter to "new in last N days".  Sorting by highest
    product ID approximates recency.
    """

    authority = "chemist_warehouse"

    def fetch_raw(self) -> list[RawSignal]:
        logger.info("CW: loading product sitemap from %s", _CW_SITEMAP_URL)
        try:
            vms_urls = list(self._get_vms_urls())
        except Exception as exc:
            logger.warning("CW: sitemap fetch failed: %s", exc)
            return []

        logger.info("CW: %d VMS-relevant product URLs found", len(vms_urls))
        if not vms_urls:
            return []

        signals: list[RawSignal] = []
        for i, url in enumerate(vms_urls[:_CW_MAX_PRODUCTS]):
            if i > 0:
                time.sleep(_CW_FETCH_DELAY)
            try:
                signal = self._fetch_product_page(url)
                if signal:
                    signals.append(signal)
            except Exception as exc:
                logger.debug("CW: failed to fetch %s: %s", url, exc)

        logger.info("CW: extracted %d product signal(s)", len(signals))
        return signals

    def _get_vms_urls(self) -> Iterator[str]:
        """
        Stream-parse the CW products sitemap and yield VMS-relevant product
        URLs sorted by descending numeric product ID (proxy for recency).
        """
        resp = self._get_session().get(_CW_SITEMAP_URL, timeout=30, stream=True)
        resp.raise_for_status()
        content = resp.content  # ~3.8MB, read fully for xml parsing

        soup = BeautifulSoup(content, "xml")
        all_locs = [loc.text.strip() for loc in soup.find_all("loc")]

        # Filter for VMS-relevant slugs
        vms = [
            u for u in all_locs
            if any(kw in u.lower() for kw in _CW_VMS_KEYWORDS)
        ]

        # Sort by descending numeric ID extracted from /buy/<id>/<slug>
        def _product_id(url: str) -> int:
            m = re.search(r"/buy/(\d+)/", url)
            return int(m.group(1)) if m else 0

        yield from sorted(vms, key=_product_id, reverse=True)

    def _fetch_product_page(self, url: str) -> RawSignal | None:
        """
        Fetch a CW product page and extract product data from __NEXT_DATA__.

        CW product pages embed full product data in:
          __NEXT_DATA__ → props → pageProps → product → product
        Fields used: name, description, categories, type, variants[0].sku
        """
        html = self._http_get(url)
        soup = BeautifulSoup(html, "lxml")

        script = soup.find("script", id="__NEXT_DATA__")
        if not script or not script.string:
            return None

        try:
            data = json.loads(script.string)
        except json.JSONDecodeError:
            return None

        try:
            pp   = data["props"]["pageProps"]
            prod = pp["product"]["product"]
        except (KeyError, TypeError):
            return None

        name = prod.get("name", "").strip()
        if not name:
            return None

        # Category / type
        prod_type   = prod.get("type", "")
        categories  = prod.get("categories", [])
        cat_names   = [c.get("slug", "") for c in categories if isinstance(c, dict)]

        # Brand — in variants[0].attributes
        brand = self._extract_brand(prod)

        # Price — in pageProps.product.prices
        price = self._extract_price(pp)

        # Description — strip HTML tags
        raw_desc = prod.get("description", "")
        desc = BeautifulSoup(raw_desc, "lxml").get_text(" ", strip=True)[:400] if raw_desc else ""

        body_parts = [f"Product: {name}"]
        if brand:
            body_parts.append(f"Brand: {brand}")
        if price:
            body_parts.append(f"Price (AUD): {price}")
        if prod_type:
            body_parts.append(f"Category type: {prod_type}")
        if cat_names:
            body_parts.append(f"Categories: {', '.join(cat_names[:5])}")
        if desc:
            body_parts.append(f"Description: {desc}")
        body_parts.append(f"Source: Chemist Warehouse vitamins ({url})")

        return RawSignal(
            source_id  = self._make_source_id(self.authority, url),
            authority  = self.authority,
            url        = url,
            title      = name,
            body_text  = "\n".join(body_parts),
            scraped_at = self._now_iso(),
        )

    @staticmethod
    def _extract_brand(prod: dict) -> str:
        variants = prod.get("variants", [])
        if not variants or not isinstance(variants, list):
            return ""
        attrs = variants[0].get("attributes", []) if variants else []
        for attr in attrs:
            if isinstance(attr, dict) and attr.get("key") == "brand":
                vals = attr.get("value", [])
                if vals and isinstance(vals, list):
                    return str(vals[0])
        return ""

    @staticmethod
    def _extract_price(pp: dict) -> str:
        """
        CW price structure: prices is a list of dicts, each with:
          { "value": {"amount": 13.99, "currencyCode": "AUD"}, "type": "cwr-au-price", ... }
        We prefer the discounted/sale price type, falling back to the first entry.
        """
        # CW prices structure: [{"sku":"...", "price": {"value": {"amount": 16.79, ...}, "rrp": {...}}}]
        try:
            prices = pp["product"]["prices"]
            if not isinstance(prices, list) or not prices:
                return ""
            price_obj = prices[0].get("price", {})
            amount = price_obj.get("value", {}).get("amount")
            rrp    = price_obj.get("rrp",   {}).get("amount")
            if amount is not None:
                price_str = f"AU${amount:.2f}"
                if rrp and float(rrp) != float(amount):
                    price_str += f" (RRP AU${float(rrp):.2f})"
                return price_str
        except (KeyError, TypeError, IndexError, AttributeError, ValueError):
            pass
        return ""
