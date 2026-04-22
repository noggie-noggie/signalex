"""
scrapers/cochrane.py — Cochrane systematic reviews via PubMed/Europe PMC index.

Cochrane Library blocks direct scraping. Instead we query PubMed and Europe PMC
for papers published in "Cochrane Database of Systematic Reviews" — the same
corpus, fully accessible via NCBI E-utilities.

Reviews concluding "insufficient evidence" or "harmful" are flagged HIGH severity
by the classifier.
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

_ESEARCH_URL  = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
_EFETCH_URL   = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
_LOOKBACK_DAYS = 365  # Cochrane reviews publish slowly — look back 1 year
_MAX_PER_QUERY = 5
_WATCHLIST_PATH = Path(__file__).parent.parent / "config" / "ingredients_watchlist.json"

_COCHRANE_QUERIES = [
    "dietary supplement[ti]",
    "vitamin supplementation[ti] systematic review",
    "mineral supplement[ti] meta-analysis",
    "herbal medicine[ti] Cochrane",
    "probiotic[ti] systematic review",
    "omega-3[ti] meta-analysis",
    "collagen supplement[ti]",
    "magnesium supplementation[ti] systematic review",
]

# Journal restriction to Cochrane Database of Systematic Reviews
_JOURNAL_FILTER = "Cochrane Database Syst Rev[Journal]"


def _load_watchlist_terms() -> list[str]:
    try:
        with _WATCHLIST_PATH.open(encoding="utf-8") as fh:
            data = json.load(fh)
        return [ing.strip() for ing in data.get("ingredients", []) if ing.strip() and not ing.startswith("_")]
    except Exception:
        return []


class CochraneScraper(BaseScraper):
    authority = "cochrane"

    def fetch_raw(self) -> list[RawSignal]:
        from analytics.db import url_exists

        watchlist = _load_watchlist_terms()
        queries = _COCHRANE_QUERIES + [f"{t}[ti] {_JOURNAL_FILTER}" for t in watchlist[:10]]

        date_from = (datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)).strftime("%Y/%m/%d")
        date_to   = datetime.now(timezone.utc).strftime("%Y/%m/%d")

        signals: list[RawSignal] = []
        seen_pmids: set[str] = set()

        logger.info("Cochrane (via PubMed): running %d queries", len(queries))

        for query in queries:
            time.sleep(0.4)
            full_query = f"({query}) AND {_JOURNAL_FILTER}" if _JOURNAL_FILTER not in query else query
            try:
                params = {
                    "db": "pubmed", "term": full_query,
                    "datetype": "pdat", "mindate": date_from, "maxdate": date_to,
                    "retmax": str(_MAX_PER_QUERY), "retmode": "xml", "usehistory": "n",
                }
                resp = self._get_session().get(_ESEARCH_URL, params=params, timeout=30)
                resp.raise_for_status()
                root = ET.fromstring(resp.text)
                pmids = [el.text for el in root.findall(".//Id") if el.text]
                logger.info("Cochrane query %r → %d PMIDs", query[:60], len(pmids))

                for pmid in pmids:
                    if pmid in seen_pmids:
                        continue
                    seen_pmids.add(pmid)
                    time.sleep(0.35)
                    try:
                        sig = self._fetch_review(pmid, url_exists)
                        if sig:
                            signals.append(sig)
                    except Exception:
                        logger.exception("Cochrane efetch failed for PMID %s", pmid)

            except Exception:
                logger.exception("Cochrane esearch failed for %r", query[:60])

        logger.info("Cochrane: %d systematic review signals fetched", len(signals))
        return signals

    def _fetch_review(self, pmid: str, url_exists_fn) -> RawSignal | None:
        params = {"db": "pubmed", "id": pmid, "retmode": "xml", "rettype": "abstract"}
        resp = self._get_session().get(_EFETCH_URL, params=params, timeout=30)
        resp.raise_for_status()

        root = ET.fromstring(resp.text)
        article = root.find(".//PubmedArticle")
        if article is None:
            return None

        title_elem = article.find(".//ArticleTitle")
        title = (title_elem.text or "").strip() if title_elem is not None else f"Cochrane Review PMID {pmid}"

        abstract_parts = article.findall(".//AbstractText")
        abstract = " ".join((el.text or "") for el in abstract_parts if el.text).strip()

        authors = []
        for au in article.findall(".//Author")[:5]:
            last = au.findtext("LastName", "")
            if last:
                authors.append(last + " " + au.findtext("ForeName", ""))
        author_str = "; ".join(a.strip() for a in authors)

        doi_elem = article.find(".//ArticleId[@IdType='doi']")
        doi = doi_elem.text.strip() if doi_elem is not None and doi_elem.text else ""

        url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
        if url_exists_fn(url):
            return None

        pub_year  = article.findtext(".//PubDate/Year", "")
        pub_month = article.findtext(".//PubDate/Month", "")

        body_text = (
            f"[COCHRANE SYSTEMATIC REVIEW]\n"
            f"PMID: {pmid}\nDOI: {doi or 'N/A'}\n"
            f"Authors: {author_str}\nPublished: {pub_year}-{pub_month}\n\n"
            f"Abstract:\n{abstract or 'See full review at Cochrane Library.'}"
        )

        return RawSignal(
            source_id  = self._make_source_id("cochrane", url),
            authority  = "cochrane",
            url        = url,
            title      = title[:300],
            body_text  = body_text[:4000],
            scraped_at = self._now_iso(),
        )
