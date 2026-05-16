"""
reports/pharma_intelligence.py — Optional AI intelligence pass for pharma citations.

Called from citation_fetcher.py after the enrichment pass.
Only runs Claude on citations where:
  - enrichment_status == "success"
  - len(enriched_text) > 250
  - enrichment_confidence >= 0.7
  - ai_summary is missing, OR enriched_text_hash has changed, OR force_reclassify=True

Results are cached in reports/ai_cache.json keyed by (source_id, enriched_text_hash).
A max_calls safety cap prevents runaway API usage on first runs.

Usage:
    from reports.pharma_intelligence import PharmaIntelligenceEnricher
    enricher = PharmaIntelligenceEnricher(max_calls=25)
    citations = enricher.enrich_batch(citations)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from reports.citation_fetcher import Citation

logger = logging.getLogger("pharma_intelligence")

REPORTS_DIR   = Path(__file__).parent
AI_CACHE_PATH = REPORTS_DIR / "ai_cache.json"

_MIN_ENRICHED_LEN            = 250   # base gate — required for ANY AI call
_MIN_ENRICHED_LEN_LOW_PRI    = 450   # P3/P4 records must clear this higher bar
_MIN_CONFIDENCE              = 0.7
_MIN_AI_CONFIDENCE           = 0.5   # discard Claude response if below this
_MIN_ENRICHED_LEN_IMPORT_ALERT = 500 # import alerts only get AI with substantial text

# Boilerplate patterns that indicate the text has no analysable violation detail.
# These typically appear in OpenFDA enforcement notices that are just SKU/lot lists.
_BOILERPLATE_RE = re.compile(
    r"(?:"
    r"firm initiated recall|"
    r"subject to recall at the retail level|"
    r"voluntary recall|"
    r"for more information|"
    r"customers who purchased"
    r")",
    re.I,
)
_BOILERPLATE_DENSITY_THRESHOLD = 3   # >= this many boilerplate phrase matches → skip

_SYSTEM_PROMPT = """\
You are a regulatory intelligence analyst specialising in pharmaceutical GMP enforcement and
the Australian vitamins, minerals and supplements (VMS) industry.

Given a regulatory enforcement action with its full extracted text, provide a concise analysis.
Respond ONLY with valid JSON matching this schema exactly — no markdown, no preamble:

{
  "ai_summary": "<2-3 sentence business-impact summary of the enforcement action>",
  "ai_what_matters": "<1-2 sentences on why this matters to the Australian VMS industry specifically>",
  "ai_recommended_action": "<one concrete action a compliance team should take>",
  "ai_confidence": <float 0.0-1.0, your confidence in the quality of this analysis>
}

Rules:
- Be specific about the violation and the company. Do not use generic filler language.
- If confidence is below 0.7, still return valid JSON and set ai_confidence accordingly.
- If the text is too redacted or unclear to analyse, set ai_confidence < 0.5 and explain briefly.
- Never fabricate facts. Only state what the provided text supports.
"""


def _cache_key(source_id: str, enriched_text_hash: str) -> str:
    return f"{source_id}:{enriched_text_hash}"


def _is_boilerplate(text: str) -> bool:
    """Return True when the enriched text is mostly enforcement-notice boilerplate
    with no analysable violation detail (OpenFDA lot/SKU lists, generic recall notices).
    Only suppresses records with >= _BOILERPLATE_DENSITY_THRESHOLD matches."""
    if not text:
        return False
    return len(_BOILERPLATE_RE.findall(text)) >= _BOILERPLATE_DENSITY_THRESHOLD


def _queue_tier(c: "Citation") -> int:
    """Return AI queue priority tier (1 = highest).

    Tier 1: P1, no AI summary, enriched text present
    Tier 2: P2, no AI summary, enriched text present
    Tier 3: cluster primary (cluster_primary=True), no AI summary
    Tier 4: market_relevance_au in (direct/indirect/reference) AND priority P1/P2
    Tier 5: warning_letter with enrichment success/cached
    Tier 6: everything else that passes base gates
    """
    has_ai      = bool(c.ai_summary)
    has_enriched = len(c.enriched_text or "") > _MIN_ENRICHED_LEN
    priority    = c.priority or "P4"
    is_primary  = getattr(c, "cluster_primary", True)

    if not has_ai and has_enriched and priority == "P1":
        return 1
    if not has_ai and has_enriched and priority == "P2":
        return 2
    if is_primary and not has_ai:
        return 3
    if (c.market_relevance_au in ("direct", "indirect", "reference")
            and priority in ("P1", "P2")):
        return 4
    if (c.source_type == "warning_letter"
            and c.enrichment_status in ("success", "cached")):
        return 5
    return 6


_TIER_KEYS = {1: "t1", 2: "t2", 3: "t3", 4: "t4", 5: "t5", 6: "t6"}


def _load_ai_cache() -> dict:
    if AI_CACHE_PATH.exists():
        try:
            return json.loads(AI_CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_ai_cache(cache: dict) -> None:
    AI_CACHE_PATH.write_text(json.dumps(cache, indent=2), encoding="utf-8")


def _get_anthropic():
    try:
        import anthropic
        key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not key:
            env_path = REPORTS_DIR.parent / ".env"
            if env_path.exists():
                for line in env_path.read_text().splitlines():
                    if line.startswith("ANTHROPIC_API_KEY="):
                        key = line.split("=", 1)[1].strip().strip('"\'')
                        os.environ["ANTHROPIC_API_KEY"] = key
                        break
        return anthropic.Anthropic(api_key=key)
    except Exception as exc:
        logger.warning("Could not initialise Anthropic client: %s", exc)
        return None


class PharmaIntelligenceEnricher:
    """
    Runs an optional AI intelligence pass on enriched citations.

    Gating (all must be true to call Claude):
      - enrichment_status == "success"
      - len(enriched_text) > 250
      - enrichment_confidence >= 0.7
      - ai_summary missing, OR enriched_text_hash changed, OR force_reclassify=True

    Safety cap: at most max_calls Claude API calls per run.
    """

    def __init__(
        self,
        model:         str = "claude-haiku-4-5-20251001",
        max_calls:     int = 25,
        force_reclassify: bool = False,
    ) -> None:
        self.model            = model
        self.max_calls        = max_calls
        self.force_reclassify = force_reclassify
        self._client          = None
        self._calls_made      = 0
        self._calls_accepted  = 0
        self._calls_discarded = 0

    def _client_or_none(self):
        if self._client is None:
            self._client = _get_anthropic()
        return self._client

    def _should_classify(self, c: "Citation", cache: dict) -> bool:
        """Return True if this citation meets gating conditions and needs a new AI call."""
        if c.enrichment_status not in ("success", "cached"):
            return False
        if len(c.enriched_text or "") <= _MIN_ENRICHED_LEN:
            return False
        if c.enrichment_confidence < _MIN_CONFIDENCE:
            return False

        key = _cache_key(c.id, c.enriched_text_hash)
        if key in cache:
            return False

        if c.ai_summary and not self.force_reclassify:
            return False

        # ── Deprioritisation / skip guards ────────────────────────────────
        priority   = c.priority or "P4"
        is_primary = getattr(c, "cluster_primary", True)
        has_au     = c.market_relevance_au in ("direct", "indirect", "reference")

        # Import alerts need substantial text to be worth calling AI
        if c.source_type == "import_alert":
            if len(c.enriched_text or "") <= _MIN_ENRICHED_LEN_IMPORT_ALERT:
                return False

        # P3/P4 records are deprioritised unless they have AU relevance, are a cluster
        # primary, or have rich enriched text (> low-priority threshold)
        if priority in ("P3", "P4") and not is_primary and not has_au:
            if len(c.enriched_text or "") <= _MIN_ENRICHED_LEN_LOW_PRI:
                return False

        # Skip records whose text is mostly enforcement boilerplate
        if _is_boilerplate(c.enriched_text or ""):
            return False

        return True

    def _call_claude(self, c: "Citation") -> Optional[dict]:
        """
        Call Claude with the enriched text and return parsed JSON dict,
        or None if the call fails or the response is below confidence threshold.
        """
        client = self._client_or_none()
        if client is None:
            return None

        prompt = (
            f"Authority: {c.authority}\n"
            f"Source type: {c.source_type}\n"
            f"Company: {c.company or 'unknown'}\n"
            f"Date: {c.date or 'unknown'}\n"
            f"Category: {c.primary_gmp_category or c.category or 'unknown'}\n"
            f"Failure mode: {c.failure_mode or 'unknown'}\n\n"
            f"Enforcement text:\n{c.enriched_text[:1800]}"
        )

        try:
            resp = client.messages.create(
                model=self.model,
                max_tokens=512,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.content[0].text.strip()
            # Strip markdown fences if present
            if raw.startswith("```"):
                raw = "\n".join(raw.split("\n")[1:])
                raw = raw.rstrip("`").strip()
            parsed = json.loads(raw)

            if parsed.get("ai_confidence", 0) < _MIN_AI_CONFIDENCE:
                logger.debug("AI confidence too low for %s (%.2f) — skipping", c.id, parsed.get("ai_confidence", 0))
                return None

            return {
                "ai_summary":            str(parsed.get("ai_summary", ""))[:500],
                "ai_what_matters":       str(parsed.get("ai_what_matters", ""))[:400],
                "ai_recommended_action": str(parsed.get("ai_recommended_action", ""))[:300],
                "ai_confidence":         float(parsed.get("ai_confidence", 0.0)),
                "ai_run_at":             datetime.now(timezone.utc).isoformat(),
            }
        except json.JSONDecodeError as exc:
            logger.debug("AI response not valid JSON for %s: %s", c.id, exc)
        except Exception as exc:
            logger.warning("Claude API error for %s: %s", c.id, exc)
        return None

    def enrich_batch(
        self,
        citations: list["Citation"],
        dry_run: bool = False,
        tracker: Optional[Any] = None,
    ) -> list["Citation"]:
        """
        Apply AI intelligence pass to eligible citations.
        Updates ai_* fields in place (returns new list of Citation objects).
        Cache prevents re-classification when enriched_text_hash is unchanged.

        tracker: optional AiCallTracker from citation_fetcher — used to log and
                 count all Claude calls across the full pipeline.
        """
        from reports.citation_fetcher import Citation as Cit

        cache = _load_ai_cache()
        eligible = [c for c in citations if self._should_classify(c, cache)]

        # Sort by tier so the budget is spent on the highest-value records first
        eligible.sort(key=lambda c: (_queue_tier(c), c.id))

        # Count cache hits: citations that pass quality gates but are already cached
        if tracker is not None:
            _cached_hits = sum(
                1 for c in citations
                if (c.enrichment_status in ("success", "cached")
                    and len(c.enriched_text or "") > _MIN_ENRICHED_LEN
                    and c.enrichment_confidence >= _MIN_CONFIDENCE
                    and (not c.ai_summary or self.force_reclassify)
                    and _cache_key(c.id, c.enriched_text_hash) in cache)
            )
            tracker.ai_eligible_count += len(eligible) + _cached_hits
            tracker.ai_skipped_cached += _cached_hits

            # Report queue composition to tracker
            from collections import Counter as _Ctr
            tier_counts = _Ctr(_queue_tier(c) for c in eligible)
            tracker.ai_queue_p1_count              = tier_counts[1]
            tracker.ai_queue_p2_count              = tier_counts[2]
            tracker.ai_queue_cluster_primary_count = tier_counts[3]
            tracker.ai_queue_au_relevant_count     = tier_counts[4]
            tracker.ai_queue_warning_letter_count  = tier_counts[5]
            tracker.ai_queue_low_priority_count    = tier_counts[6]

        logger.info(
            "AI pass: %d eligible (after filtering+sorting) of %d total "
            "[t1=%d t2=%d t3=%d t4=%d t5=%d t6=%d] max_calls=%d",
            len(eligible), len(citations),
            sum(1 for c in eligible if _queue_tier(c) == 1),
            sum(1 for c in eligible if _queue_tier(c) == 2),
            sum(1 for c in eligible if _queue_tier(c) == 3),
            sum(1 for c in eligible if _queue_tier(c) == 4),
            sum(1 for c in eligible if _queue_tier(c) == 5),
            sum(1 for c in eligible if _queue_tier(c) == 6),
            self.max_calls,
        )

        if dry_run:
            logger.info("DRY-RUN: AI cache not written, citations not updated")
            return citations

        updates: dict[str, dict] = {}
        accepted_by_tier: dict[str, int] = {}
        discarded_by_tier: dict[str, int] = {}

        for c in eligible:
            if self._calls_made >= self.max_calls:
                logger.info("AI pass: max_calls=%d reached — stopping early", self.max_calls)
                if tracker is not None:
                    tracker.ai_skipped_due_to_max_calls += len(eligible) - self._calls_made
                break

            tier_key = _TIER_KEYS[_queue_tier(c)]

            if tracker is not None:
                tracker.log_call(c.id, "pharma_intelligence")

            result = self._call_claude(c)
            self._calls_made += 1

            if result:
                self._calls_accepted += 1
                accepted_by_tier[tier_key] = accepted_by_tier.get(tier_key, 0) + 1
                key = _cache_key(c.id, c.enriched_text_hash)
                cache[key] = result
                updates[c.id] = result
                logger.debug("AI enriched %s (confidence=%.2f)", c.id, result["ai_confidence"])
            else:
                self._calls_discarded += 1
                discarded_by_tier[tier_key] = discarded_by_tier.get(tier_key, 0) + 1
                logger.debug("AI pass produced no result for %s", c.id)

            # Polite pacing — Haiku has high rate limits but be conservative
            if self._calls_made < len(eligible) and self._calls_made < self.max_calls:
                time.sleep(0.3)

        _save_ai_cache(cache)
        logger.info(
            "AI pass complete: %d calls made, %d accepted, %d discarded (low confidence)",
            self._calls_made, self._calls_accepted, self._calls_discarded,
        )

        if tracker is not None:
            tracker.ai_results_accepted += self._calls_accepted
            tracker.ai_results_discarded_low_confidence += self._calls_discarded
            tracker.ai_accepted_by_tier  = accepted_by_tier
            tracker.ai_discarded_by_tier = discarded_by_tier

        # Also apply anything already in cache (from prior runs)
        for c in citations:
            key = _cache_key(c.id, c.enriched_text_hash)
            if key in cache and c.id not in updates:
                updates[c.id] = cache[key]

        # Rebuild citations with AI fields applied
        result_list: list["Cit"] = []
        for c in citations:
            if c.id in updates:
                u = updates[c.id]
                updated = asdict(c)
                updated.update(u)
                # Populate stable display fields from accepted AI result.
                # Preserve existing values unless force_reclassify (human-reviewed data wins).
                if not c.decision_summary or self.force_reclassify:
                    updated["decision_summary"] = (
                        u.get("ai_what_matters") or u.get("ai_summary") or ""
                    )[:400]
                if not c.recommended_action or self.force_reclassify:
                    updated["recommended_action"] = (u.get("ai_recommended_action") or "")[:300]
                updated["classification_confidence"] = float(u.get("ai_confidence", 0.0))
                updated["is_noise"] = bool(u.get("ai_is_noise", False))
                result_list.append(Cit(**updated))
            else:
                # No accepted AI result: ensure AI-specific display fields are cleared
                updated = asdict(c)
                updated["classification_confidence"] = 0.0
                updated["decision_summary"]    = c.decision_summary    or ""
                updated["recommended_action"]  = c.recommended_action  or ""
                result_list.append(Cit(**updated))
        return result_list
