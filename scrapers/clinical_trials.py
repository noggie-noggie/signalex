"""
scrapers/clinical_trials.py — Trial registrations from ClinicalTrials.gov API v2.

API docs: https://clinicaltrials.gov/data-api/api

Searches for new registrations or status changes (last 90 days) related to
VMS ingredients and dietary supplements. New safety trials are flagged HIGH.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scrapers.base import BaseScraper, RawSignal

logger = logging.getLogger(__name__)

_API_URL       = "https://clinicaltrials.gov/api/v2/studies"
_LOOKBACK_DAYS = 90
_MAX_PER_QUERY = 8
_WATCHLIST_PATH = Path(__file__).parent.parent / "config" / "ingredients_watchlist.json"

_EXTRA_QUERIES = [
    "dietary supplement safety",
    "complementary medicine clinical trial",
    "nutraceutical efficacy",
    "herbal supplement adverse effects",
    "vitamin D supplementation randomized",
    "probiotic randomized controlled trial",
    "omega-3 supplementation clinical",
]

_SPONSOR_VMS_KEYWORDS = {
    "blackmores", "swisse", "natures way", "now foods", "solgar", "garden of life",
    "nordic naturals", "jarrow", "thorne", "pure encapsulations", "metagenics",
    "natrol", "vitacost", "nature made", "centrum", "usana", "herbalife",
}


def _load_watchlist_terms() -> list[str]:
    try:
        with _WATCHLIST_PATH.open(encoding="utf-8") as fh:
            data = json.load(fh)
        return [ing.strip() for ing in data.get("ingredients", []) if ing.strip() and not ing.startswith("_")]
    except Exception:
        return []


def _is_vms_sponsor(sponsor: str) -> bool:
    s = sponsor.lower()
    return any(kw in s for kw in _SPONSOR_VMS_KEYWORDS)


class ClinicalTrialsScraper(BaseScraper):
    authority = "clinical_trials"

    def fetch_raw(self) -> list[RawSignal]:
        from analytics.db import url_exists

        queries = _EXTRA_QUERIES + _load_watchlist_terms()
        date_from = (datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

        signals: list[RawSignal] = []
        seen_nct: set[str] = set()

        logger.info("ClinicalTrials.gov: running %d queries (last %d days)", len(queries), _LOOKBACK_DAYS)

        for query in queries:
            time.sleep(0.5)
            try:
                params = {
                    "query.term":    query,
                    "filter.advanced": f"AREA[StartDate]RANGE[{date_from},MAX]",
                    "pageSize":      str(_MAX_PER_QUERY),
                    "format":        "json",
                    "fields":        (
                        "NCTId,BriefTitle,OfficialTitle,OverallStatus,Phase,"
                        "InterventionName,LeadSponsorName,Condition,"
                        "StartDate,CompletionDate,BriefSummary,StudyType"
                    ),
                }
                resp = self._get_session().get(_API_URL, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()

                for study in data.get("studies", []):
                    proto = study.get("protocolSection", {})
                    id_mod     = proto.get("identificationModule", {})
                    status_mod = proto.get("statusModule", {})
                    desc_mod   = proto.get("descriptionModule", {})
                    design_mod = proto.get("designModule", {})
                    arms_mod   = proto.get("armsInterventionsModule", {})
                    sponsor_mod = proto.get("sponsorCollaboratorsModule", {})

                    nct_id = id_mod.get("nctId", "")
                    if not nct_id or nct_id in seen_nct:
                        continue
                    seen_nct.add(nct_id)

                    url = f"https://clinicaltrials.gov/study/{nct_id}"
                    if url_exists(url):
                        continue

                    title  = id_mod.get("briefTitle") or id_mod.get("officialTitle") or f"Trial {nct_id}"
                    status = status_mod.get("overallStatus", "")
                    phase  = ", ".join(design_mod.get("phases", [])) if design_mod.get("phases") else ""
                    sponsor = sponsor_mod.get("leadSponsor", {}).get("name", "")

                    interventions = [
                        iv.get("name", "")
                        for iv in arms_mod.get("interventions", [])
                        if iv.get("name")
                    ][:5]
                    conditions = proto.get("conditionsModule", {}).get("conditions", [])[:5]
                    start_date  = status_mod.get("startDateStruct", {}).get("date", "")
                    end_date    = status_mod.get("completionDateStruct", {}).get("date", "")
                    summary     = desc_mod.get("briefSummary", "")

                    competitive = _is_vms_sponsor(sponsor)

                    body_text = (
                        f"NCT ID: {nct_id}\nStatus: {status}\nPhase: {phase or 'N/A'}\n"
                        f"Sponsor: {sponsor}{'  [VMS COMPANY]' if competitive else ''}\n"
                        f"Interventions: {', '.join(interventions) or 'See study'}\n"
                        f"Conditions: {', '.join(conditions) or 'See study'}\n"
                        f"Start: {start_date}  Completion: {end_date}\n\n"
                        f"Summary:\n{summary[:2000] or 'No summary available.'}"
                    )

                    signals.append(RawSignal(
                        source_id  = self._make_source_id("clinical_trials", url),
                        authority  = "clinical_trials",
                        url        = url,
                        title      = title[:300],
                        body_text  = body_text[:4000],
                        scraped_at = self._now_iso(),
                    ))

            except Exception:
                logger.exception("ClinicalTrials: query failed: %r", query[:50])

        logger.info("ClinicalTrials.gov: %d signals fetched", len(signals))
        return signals
