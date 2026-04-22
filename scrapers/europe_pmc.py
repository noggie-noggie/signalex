"""
scrapers/europe_pmc.py — Fetch VMS-relevant research from Europe PMC REST API.

Europe PMC indexes PubMed, PMC, preprints, and European journals.
API docs: https://europepmc.org/RestfulWebService

Deduplicates against existing PubMed signals: if a paper has a PMID and a signal
with that PubMed URL already exists in the DB, it is skipped.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scrapers.base import BaseScraper, RawSignal

logger = logging.getLogger(__name__)

_API_URL      = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
_LOOKBACK_DAYS = 30
_MAX_PER_QUERY = 8
_WATCHLIST_PATH = Path(__file__).parent.parent / "config" / "ingredients_watchlist.json"

_EXTRA_QUERIES = [
    "dietary supplement European Union regulation",
    "herbal medicine safety assessment",
    "novel food authorisation supplement",
    "vitamin mineral supplement safety",
    "preprint supplement efficacy",
    "nutraceutical clinical evidence",
]


def _load_watchlist() -> list[str]:
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
        logger.warning("Europe PMC: could not load watchlist")
        return []


class EuropePMCScraper(BaseScraper):
    authority = "europe_pmc"

    def fetch_raw(self) -> list[RawSignal]:
        from analytics.db import url_exists

        queries = _EXTRA_QUERIES + _load_watchlist()
        date_from = (datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

        signals: list[RawSignal] = []
        seen_ids: set[str] = set()
        dedup_skipped = 0

        logger.info("Europe PMC: running %d queries", len(queries))

        for query in queries:
            time.sleep(0.5)
            try:
                params = {
                    "query":       f"{query} AND (FIRST_PDATE:[{date_from} TO *])",
                    "resultType":  "core",
                    "pageSize":    str(_MAX_PER_QUERY),
                    "format":      "json",
                    "sort":        "P_PDATE_D desc",
                }
                resp = self._get_session().get(_API_URL, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()

                for item in data.get("resultList", {}).get("result", []):
                    pmid  = item.get("pmid", "")
                    pmcid = item.get("pmcid", "")
                    doi   = item.get("doi", "")
                    title = (item.get("title") or "").strip().rstrip(".")
                    if not title:
                        continue

                    # Canonical URL: prefer PubMed URL (for cross-source dedup)
                    if pmid:
                        url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
                    elif doi:
                        url = f"https://doi.org/{doi}"
                    elif pmcid:
                        url = f"https://europepmc.org/article/PMC/{pmcid}"
                    else:
                        url = f"https://europepmc.org/search#query={title[:60].replace(' ','+')}"

                    if url in seen_ids:
                        continue
                    seen_ids.add(url)

                    # Skip if this URL already exists in DB (catches PubMed overlaps)
                    if url_exists(url):
                        dedup_skipped += 1
                        continue

                    source_id = self._make_source_id("europe_pmc", url)

                    abstract = item.get("abstractText") or ""
                    authors  = item.get("authorString") or ""
                    journal  = item.get("journalTitle") or item.get("bookOrReportDetails", {}).get("publisher", "")
                    pub_date = item.get("firstPublicationDate") or item.get("pubYear") or ""
                    src_type = item.get("source", "MED")  # MED=PubMed, PPR=preprint, etc.

                    body_text = (
                        f"PMID: {pmid or 'N/A'}\nDOI: {doi or 'N/A'}\nSource type: {src_type}\n"
                        f"Authors: {authors}\nJournal: {journal}\nDate: {pub_date}\n\n"
                        f"Abstract:\n{abstract or 'No abstract available.'}"
                    )

                    signals.append(RawSignal(
                        source_id  = source_id,
                        authority  = "europe_pmc",
                        url        = url,
                        title      = title[:300],
                        body_text  = body_text[:4000],
                        scraped_at = self._now_iso(),
                    ))

            except Exception:
                logger.exception("Europe PMC: query failed: %r", query[:50])

        logger.info(
            "Europe PMC: %d signals fetched, %d skipped (existing PubMed overlap)",
            len(signals), dedup_skipped,
        )
        return signals
