"""
scrapers/semantic_scholar.py — Academic papers via Semantic Scholar API.

API docs: https://api.semanticscholar.org/graph/v1

Free tier: 100 requests per 5 minutes (no key needed).
Focuses on:
  - Highly-cited papers (>10 citations) for established evidence
  - Recent papers (0 citations, last 30 days) for emerging signals

Deduplicates against PubMed and Europe PMC by checking existing URLs/DOIs.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scrapers.base import BaseScraper, RawSignal

logger = logging.getLogger(__name__)

_API_URL       = "https://api.semanticscholar.org/graph/v1/paper/search"
_LOOKBACK_DAYS = 30
_MAX_PER_QUERY = 6
_RATE_LIMIT_SLEEP = 5.0  # conservative: 1 req/5s to avoid 429s
_WATCHLIST_PATH = Path(__file__).parent.parent / "config" / "ingredients_watchlist.json"

_EXTRA_QUERIES = [
    "dietary supplement safety clinical",
    "vitamin toxicity adverse effects",
    "herbal supplement drug interaction",
    "nutraceutical efficacy randomized trial",
]

_FIELDS = "paperId,externalIds,title,abstract,authors,year,citationCount,fieldsOfStudy,openAccessPdf,publicationDate"


def _load_watchlist_terms() -> list[str]:
    try:
        with _WATCHLIST_PATH.open(encoding="utf-8") as fh:
            data = json.load(fh)
        suffix = data.get("query_suffix", "supplement")
        return [
            f"{ing.strip()} {suffix}" if suffix.lower() not in ing.lower() else ing.strip()
            for ing in data.get("ingredients", [])
            if ing.strip() and not ing.startswith("_")
        ]
    except Exception:
        return []


class SemanticScholarScraper(BaseScraper):
    authority = "semantic_scholar"

    def fetch_raw(self) -> list[RawSignal]:
        from analytics.db import url_exists

        queries = _EXTRA_QUERIES + _load_watchlist_terms()
        window = getattr(self, "window", None)
        if window and len(queries) == len(_EXTRA_QUERIES):
            raise ValueError("Missing Semantic Scholar watchlist")
        self.completed_queries = 0
        cutoff_year = datetime.now(timezone.utc).year - 1  # papers from last ~2 years
        recent_cutoff = (datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

        signals: list[RawSignal] = []
        seen_ids: set[str] = set()

        logger.info("Semantic Scholar: running %d queries", len(queries))

        for query in queries:
            time.sleep(_RATE_LIMIT_SLEEP)
            try:
                params = {
                    "query":     query,
                    "fields":    _FIELDS,
                    "limit":     str(_MAX_PER_QUERY),
                    # Focus on recent or highly cited
                    "year":      f"{cutoff_year}-",
                }
                headers = {"User-Agent": "Signalex/1.0 (regulatory intelligence; research use)"}
                if os.getenv("SEMANTIC_SCHOLAR_API_KEY"):
                    headers["x-api-key"] = os.environ["SEMANTIC_SCHOLAR_API_KEY"]
                if window:
                    params.pop("year")
                    params.update(publicationDateOrYear=f"{window[0]}:{window[1]}", limit=100)
                for paper in self._pages(params, headers, window):
                    paper_id    = paper.get("paperId", "")
                    title       = (paper.get("title") or "").strip()
                    abstract    = paper.get("abstract") or ""
                    year        = paper.get("year") or ""
                    cite_count  = paper.get("citationCount") or 0
                    pub_date    = paper.get("publicationDate") or ""
                    fields      = paper.get("fieldsOfStudy") or []
                    oa_pdf      = (paper.get("openAccessPdf") or {}).get("url") or ""
                    ext_ids     = paper.get("externalIds") or {}

                    pmid = ext_ids.get("PubMed") or ext_ids.get("PMID") or ""
                    doi  = ext_ids.get("DOI") or ""

                    if window and (not title or not paper_id or not str(window[0]) <= pub_date <= str(window[1])):
                        raise ValueError("Unverified paper identity or publication date")
                    if not title or paper_id in seen_ids:
                        continue
                    seen_ids.add(paper_id)

                    # Determine canonical URL — prefer PubMed URL for dedup
                    if pmid:
                        url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
                    elif doi:
                        url = f"https://doi.org/{doi}"
                    else:
                        url = f"https://www.semanticscholar.org/paper/{paper_id}"

                    if not window and url_exists(url):
                        continue

                    # Filter: keep highly-cited OR recent (last 30 days)
                    is_recent  = pub_date >= recent_cutoff if pub_date else False
                    is_notable = cite_count >= 10
                    if not window and not is_recent and not is_notable and cite_count > 0:
                        continue

                    authors = paper.get("authors") or []
                    author_str = "; ".join(
                        a.get("name", "") for a in authors[:5] if a.get("name")
                    )
                    if len(authors) > 5:
                        author_str += " et al."

                    body_text = (
                        f"Paper ID: {paper_id}\nPMID: {pmid or 'N/A'}\nDOI: {doi or 'N/A'}\n"
                        f"Year: {year}\nCitation count: {cite_count}\n"
                        f"Fields: {', '.join(fields[:5]) or 'N/A'}\n"
                        f"Authors: {author_str}\n"
                        f"{'Open Access: ' + oa_pdf if oa_pdf else ''}\n\n"
                        f"Abstract:\n{abstract[:2500] or 'No abstract available.'}"
                    )

                    signals.append(RawSignal(
                        source_id  = self._make_source_id("semantic_scholar", url),
                        authority  = "semantic_scholar",
                        url        = url,
                        title      = title[:300],
                        body_text  = body_text.strip()[:4000],
                        scraped_at = self._now_iso(),
                    ))

                self.completed_queries += 1
            except Exception as exc:
                if window:
                    raise
                if getattr(getattr(exc, "response", None), "status_code", None) in (401,403,429):
                    break
                logger.exception("Semantic Scholar: query failed: %r", query[:50])

        logger.info("Semantic Scholar: %d signals fetched", len(signals))
        return signals

    def _pages(self, params, headers, window):
        seen = set()
        total = None
        while True:
            resp = self._get_session().get(_API_URL, params=dict(params, offset=len(seen)),
                                           headers=headers, timeout=30, allow_redirects=False)
            resp.raise_for_status()
            data = resp.json()
            batch = data["data"]
            if window:
                count = int(data["total"])
                if count > 1000 or (total is not None and count != total) or int(data["offset"]) != len(seen):
                    raise ValueError("Semantic Scholar search capped or count changed; narrow window")
                total = count
            for paper in batch:
                if window and paper["paperId"] in seen:
                    raise ValueError("Repeated paper")
                seen.add(paper["paperId"])
                yield paper
            if not window or len(seen) == total:
                return
            if not batch or data.get("next") != len(seen):
                raise ValueError("Incomplete Semantic Scholar search")
            time.sleep(_RATE_LIMIT_SLEEP)
