"""
scrapers/pubmed.py — Fetch VMS-relevant research articles from PubMed via NCBI E-utilities.

Uses the free E-utilities API (no key needed for <3 req/sec):
  esearch.fcgi  — returns PMIDs matching a query
  efetch.fcgi   — returns article XML for a list of PMIDs

Query coverage (30-day lookback):
  Safety (6)     — adverse effects, toxicity, drug interactions, hepatotoxicity, recalls, contamination
  Efficacy (4)   — clinical trials, vitamin/mineral efficacy, probiotic evidence
  Regulatory (4) — supplement regulation, TGA, FDA, EFSA
  Emerging (5)   — nootropics, peptides, GLP-1, mushroom, adaptogens
  Watchlist (N)  — one query per ingredient in config/ingredients_watchlist.json

  Before expansion:  5 queries ×  5 max results = up to  25 unique articles/run
  After expansion:  19 static + 15 default watchlist = 34 queries × 5 = up to 170 unique articles/run

Ingredient watchlist: config/ingredients_watchlist.json  (edit freely, no code changes needed)
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from xml.etree import ElementTree as ET

from scrapers.base import BaseScraper, RawSignal

logger = logging.getLogger(__name__)

# NCBI E-utilities base URLs
_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
_EFETCH_URL  = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

# ── Static query groups ────────────────────────────────────────────────────────

# Safety — adverse effects, toxicity, interactions
_SAFETY_QUERIES = [
    "supplement adverse effect",
    "supplement toxicity",
    "supplement drug interaction",
    "herbal hepatotoxicity",
    "dietary supplement recall",
    "supplement contamination",
]

# Efficacy — clinical evidence
_EFFICACY_QUERIES = [
    "supplement clinical trial",
    "vitamin efficacy",
    "mineral supplementation",
    "probiotic clinical",
]

# Regulatory — agency-specific monitoring
_REGULATORY_QUERIES = [
    "supplement regulation",
    "TGA complementary medicine",
    "FDA dietary supplement",
    "EFSA novel food",
]

# Emerging — newer categories gaining regulatory attention
_EMERGING_QUERIES = [
    "nootropic supplement",
    "peptide supplement",
    "GLP-1 supplement",
    "mushroom supplement",
    "adaptogen supplement",
]

_STATIC_QUERIES: list[str] = (
    _SAFETY_QUERIES
    + _EFFICACY_QUERIES
    + _REGULATORY_QUERIES
    + _EMERGING_QUERIES
)

# Max results per query (keep low to stay under rate limits)
_MAX_RESULTS_PER_QUERY = 5

# Only look back this many days for new articles
_LOOKBACK_DAYS = 30

# Path to ingredient watchlist config
_WATCHLIST_PATH = Path(__file__).parent.parent / "config" / "ingredients_watchlist.json"


def _load_watchlist_queries() -> list[str]:
    """
    Load ingredients from config/ingredients_watchlist.json and build one
    PubMed query per ingredient using the configured suffix.

    Returns an empty list (with a warning) if the file is missing or malformed.
    """
    try:
        with _WATCHLIST_PATH.open(encoding="utf-8") as fh:
            data = json.load(fh)
        ingredients: list[str] = data.get("ingredients", [])
        suffix: str = data.get("query_suffix", "supplement").strip()
        queries = []
        for ing in ingredients:
            ing = ing.strip()
            if not ing or ing.startswith("_"):
                continue
            # Avoid doubling the suffix if the ingredient already contains it
            if suffix and suffix.lower() not in ing.lower():
                queries.append(f"{ing} {suffix}")
            else:
                queries.append(ing)
        logger.info(
            "PubMed watchlist: loaded %d ingredient queries from %s",
            len(queries),
            _WATCHLIST_PATH.name,
        )
        return queries
    except FileNotFoundError:
        logger.warning(
            "PubMed: %s not found — skipping ingredient-level queries",
            _WATCHLIST_PATH,
        )
        return []
    except Exception:
        logger.exception("PubMed: failed to load ingredients_watchlist.json")
        return []


class PubMedScraper(BaseScraper):
    """
    Scrapes PubMed for recent VMS-relevant research.

    Returns one RawSignal per article, with title, abstract, authors,
    journal, publication date, and MeSH terms in body_text.
    """

    authority = "pubmed"

    def fetch_raw(self) -> list[RawSignal]:
        watchlist_queries = _load_watchlist_queries()
        all_queries = _STATIC_QUERIES + watchlist_queries

        logger.info(
            "PubMed: running %d queries (%d static + %d watchlist), "
            "up to %d results each",
            len(all_queries),
            len(_STATIC_QUERIES),
            len(watchlist_queries),
            _MAX_RESULTS_PER_QUERY,
        )

        signals: list[RawSignal] = []
        seen_pmids: set[str] = set()

        date_from = (
            datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)
        ).strftime("%Y/%m/%d")
        date_to = datetime.now(timezone.utc).strftime("%Y/%m/%d")

        for query in all_queries:
            time.sleep(0.4)  # stay under 3 req/sec between search queries
            try:
                pmids = self._search_pmids(query, date_from, date_to)
                logger.info("PubMed query %r → %d PMIDs", query[:50], len(pmids))

                for pmid in pmids:
                    if pmid in seen_pmids:
                        continue
                    seen_pmids.add(pmid)

                    try:
                        signal = self._fetch_article(pmid)
                        if signal:
                            signals.append(signal)
                        time.sleep(0.35)  # stay under 3 req/sec
                    except Exception:
                        logger.exception("PubMed efetch failed for PMID %s", pmid)

            except Exception:
                logger.exception("PubMed esearch failed for query %r", query[:50])

        logger.info(
            "PubMed: %d unique article signals fetched (from %d queries, "
            "%d-day window)",
            len(signals),
            len(all_queries),
            _LOOKBACK_DAYS,
        )
        return signals

    def _search_pmids(self, query: str, date_from: str, date_to: str) -> list[str]:
        """Return a list of PMIDs matching the query within the date range."""
        params = {
            "db":         "pubmed",
            "term":       query,
            "datetype":   "pdat",
            "mindate":    date_from,
            "maxdate":    date_to,
            "retmax":     str(_MAX_RESULTS_PER_QUERY),
            "retmode":    "xml",
            "usehistory": "n",
        }
        resp = self._get_session().get(_ESEARCH_URL, params=params, timeout=30)
        resp.raise_for_status()

        root = ET.fromstring(resp.text)
        return [id_elem.text for id_elem in root.findall(".//Id") if id_elem.text]

    def _fetch_article(self, pmid: str) -> RawSignal | None:
        """Fetch full article metadata for one PMID and return a RawSignal."""
        params = {
            "db":      "pubmed",
            "id":      pmid,
            "retmode": "xml",
            "rettype": "abstract",
        }
        resp = self._get_session().get(_EFETCH_URL, params=params, timeout=30)
        resp.raise_for_status()

        root = ET.fromstring(resp.text)
        article = root.find(".//PubmedArticle")
        if article is None:
            return None

        # Title
        title_elem = article.find(".//ArticleTitle")
        title = (title_elem.text or "").strip() if title_elem is not None else f"PubMed PMID {pmid}"

        # Abstract
        abstract_parts = article.findall(".//AbstractText")
        abstract = " ".join(
            (elem.text or "") for elem in abstract_parts if elem.text
        ).strip()

        # Authors
        authors = []
        for author in article.findall(".//Author")[:5]:
            last  = author.findtext("LastName", "")
            first = author.findtext("ForeName", "")
            if last:
                authors.append(f"{last} {first}".strip())
        author_str = "; ".join(authors) + (" et al." if len(authors) == 5 else "")

        # Journal + date
        journal  = (
            article.findtext(".//Journal/Title", "")
            or article.findtext(".//Journal/ISOAbbreviation", "")
        )
        pub_year  = article.findtext(".//PubDate/Year", "")
        pub_month = article.findtext(".//PubDate/Month", "")
        pub_date  = f"{pub_year}-{pub_month}" if pub_month else pub_year

        # MeSH terms
        mesh_terms = [
            desc.text
            for desc in article.findall(".//MeshHeading/DescriptorName")
            if desc.text
        ]
        mesh_str = "; ".join(mesh_terms[:10]) if mesh_terms else "None"

        url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

        body_text = (
            f"PMID: {pmid}\n"
            f"Authors: {author_str}\n"
            f"Journal: {journal}\n"
            f"Publication Date: {pub_date}\n"
            f"MeSH Terms: {mesh_str}\n\n"
            f"Abstract:\n{abstract or 'No abstract available.'}"
        )

        return RawSignal(
            source_id  = self._make_source_id("pubmed", url),
            authority  = "pubmed",
            url        = url,
            title      = title[:300],
            body_text  = body_text[:4000],
            scraped_at = self._now_iso(),
        )
