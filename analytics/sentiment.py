"""
analytics/sentiment.py — Sentiment analysis for classified VMS signals.

For every classified signal, runs a Claude API call to determine:
  - sentiment:    positive | neutral | negative
  - confidence:   0.0 – 1.0
  - reasoning:    one-sentence explanation

Also builds ingredient-level aggregations:
  - Rolling 30-day sentiment score per ingredient (confidence-weighted avg
    where positive=1.0, neutral=0.0, negative=-1.0).
  - Rising risk flag: if an ingredient's sentiment shifts > 0.3 toward negative
    over 14 days, flag as "rising_risk".

Results are persisted back to signals.db via analytics.db.update_sentiment().
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import anthropic

import config
from analytics.db import (
    get_signals_missing_sentiment,
    get_signals_missing_ai_summary,
    get_signals_since,
    update_sentiment,
    update_ai_summary,
)
from classifier.claude import ClassifiedSignal

logger = logging.getLogger(__name__)

# Sentiment scores for weighted average computation
_SENTIMENT_SCORE = {"positive": 1.0, "neutral": 0.0, "negative": -1.0}

_SENTIMENT_SYSTEM_PROMPT = """\
You are a sentiment analyst for a VMS (vitamins, minerals, supplements) regulatory intelligence platform.

You will receive a classified regulatory signal — the title and summary of a regulatory action,
research finding, adverse event report, or market listing.

Assess the sentiment of this signal from the perspective of a VMS company:
  - positive  = good news for industry (new approvals, positive safety data, permissive regulation)
  - neutral   = informational, no clear positive or negative market implication
  - negative  = bad news for industry (safety alerts, bans, adverse events, restrictive regulation)

Respond with a single JSON object — no markdown, no prose. Use exactly these keys:
  "sentiment"   : string  — one of: positive | neutral | negative
  "confidence"  : number  — 0.0 to 1.0 (how confident you are in the assessment)
  "reasoning"   : string  — one sentence explaining your assessment
  "ai_summary"  : string  — one sentence explaining the business or regulatory implication for a VMS company,
                            NOT just restating the finding — explain "so what does this mean for us?"
                            Focus on: risk exposure, market opportunity, regulatory action required, or competitive implication.

Example:
{"sentiment":"negative","confidence":0.92,"reasoning":"The TGA safety alert about contaminated supplements directly threatens consumer trust and may trigger product withdrawals.","ai_summary":"VMS companies with herbal joint products should review sourcing from similar supply chains and prepare consumer communications, as TGA enforcement typically triggers category-wide scrutiny."}
"""

_AI_SUMMARY_SYSTEM_PROMPT = """\
You are a business intelligence analyst for a VMS (vitamins, minerals, supplements) company.

Given a regulatory signal, write one sentence explaining its business or regulatory implication
for a VMS company — NOT just restating the finding. Answer "so what does this mean for us?"

Focus on: risk exposure, market opportunity, required action, or competitive implication.

Respond with a single JSON object:
  "ai_summary" : string — one sentence, plain business English

Example:
{"ai_summary":"Companies selling high-dose vitamin D products should add allergen cross-contamination controls and review labelling before the next TGA audit cycle."}
"""


# ---------------------------------------------------------------------------
# Per-signal sentiment classification
# ---------------------------------------------------------------------------

def classify_sentiment_batch(signals: list[dict]) -> list[dict]:
    """
    Classify sentiment for a list of signal dicts (from the DB).
    Updates the DB and returns the enriched dicts.
    """
    if not signals:
        return []

    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    enriched = []

    for i, sig in enumerate(signals, 1):
        logger.info(
            "Sentiment %d/%d: %s",
            i, len(signals), (sig.get("title") or "")[:60],
        )
        sentiment, confidence, reasoning, ai_summary = _classify_one(client, sig)

        update_sentiment(sig["source_id"], sentiment, confidence, reasoning)
        if ai_summary:
            update_ai_summary(sig["source_id"], ai_summary)
        sig = dict(sig)
        sig.update(sentiment=sentiment, sentiment_confidence=confidence,
                   sentiment_reasoning=reasoning, ai_summary=ai_summary)
        enriched.append(sig)

    return enriched


def _classify_one(client: anthropic.Anthropic, sig: dict) -> tuple[str, float, str, str]:
    """Run one sentiment API call. Returns (sentiment, confidence, reasoning, ai_summary)."""
    prompt = (
        f"Title: {sig.get('title', '')}\n"
        f"Ingredient: {sig.get('ingredient_name', 'unknown')}\n"
        f"Event Type: {sig.get('event_type', '')}\n"
        f"Severity: {sig.get('severity', '')}\n"
        f"Authority: {sig.get('authority', '')}\n"
        f"Summary: {sig.get('summary', '')}"
    )
    try:
        response = client.messages.create(
            model      = config.CLAUDE_MODEL,
            max_tokens = 400,
            system     = _SENTIMENT_SYSTEM_PROMPT,
            messages   = [{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        parsed = _parse_json(raw)
        sentiment  = parsed.get("sentiment", "neutral")
        confidence = float(parsed.get("confidence", 0.5))
        reasoning  = parsed.get("reasoning", "")
        ai_summary = parsed.get("ai_summary", "")
        return sentiment, min(1.0, max(0.0, confidence)), reasoning, ai_summary

    except anthropic.APIError as exc:
        logger.error("Sentiment API error for %s: %s", sig.get("source_id", "?"), exc)
        return "neutral", 0.0, "", ""


def _parse_json(raw: str) -> dict:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        import re
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            try:
                return json.loads(m.group())
            except json.JSONDecodeError:
                pass
    return {}


# ---------------------------------------------------------------------------
# Ingredient-level aggregations
# ---------------------------------------------------------------------------

@dataclass
class IngredientSentiment:
    ingredient:    str
    score_30d:     float     # confidence-weighted average, -1.0 to 1.0
    score_16d_ago: float     # score 14 days before "today" (days 16–30)
    score_recent:  float     # score for the last 14 days
    signal_count:  int
    is_rising_risk: bool     # True if shift > 0.3 toward negative
    shift:          float    # score_recent - score_16d_ago (negative = worsening)


@dataclass
class SentimentReport:
    ingredient_summaries: list[IngredientSentiment] = field(default_factory=list)
    rising_risk:          list[str]                 = field(default_factory=list)
    overall_distribution: dict[str, int]            = field(default_factory=dict)
    generated_at:         str                       = ""


def build_sentiment_report() -> SentimentReport:
    """
    Compute ingredient-level 30-day sentiment scores and flag rising risks.
    Reads from signals.db (last 30 days only).
    """
    signals = get_signals_since(days=30)
    if not signals:
        return SentimentReport(generated_at=datetime.now(timezone.utc).isoformat())

    cutoff_14d = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()

    # Group signals by ingredient
    by_ingredient: dict[str, list[dict]] = defaultdict(list)
    for sig in signals:
        ing = (sig.get("ingredient_name") or "unknown").lower().strip()
        if ing and ing != "unknown" and sig.get("sentiment"):
            by_ingredient[ing].append(sig)

    # Overall distribution
    distribution: dict[str, int] = defaultdict(int)
    for sig in signals:
        if sig.get("sentiment"):
            distribution[sig["sentiment"]] += 1

    summaries: list[IngredientSentiment] = []
    for ing, sigs in by_ingredient.items():
        if len(sigs) < 2:
            continue

        recent_sigs   = [s for s in sigs if s.get("scraped_at", "") >= cutoff_14d]
        historic_sigs = [s for s in sigs if s.get("scraped_at", "") < cutoff_14d]

        score_30d     = _weighted_score(sigs)
        score_recent  = _weighted_score(recent_sigs)  if recent_sigs  else score_30d
        score_historic = _weighted_score(historic_sigs) if historic_sigs else score_30d

        shift = score_recent - score_historic
        is_rising_risk = shift < -0.3  # negative = worsening sentiment

        summaries.append(IngredientSentiment(
            ingredient     = ing,
            score_30d      = round(score_30d, 3),
            score_16d_ago  = round(score_historic, 3),
            score_recent   = round(score_recent, 3),
            signal_count   = len(sigs),
            is_rising_risk = is_rising_risk,
            shift          = round(shift, 3),
        ))

    summaries.sort(key=lambda x: x.score_30d)  # most negative first

    rising_risk = [s.ingredient for s in summaries if s.is_rising_risk]

    if rising_risk:
        logger.info("Sentiment: rising risk ingredients: %s", ", ".join(rising_risk[:5]))

    return SentimentReport(
        ingredient_summaries = summaries,
        rising_risk          = rising_risk,
        overall_distribution = dict(distribution),
        generated_at         = datetime.now(timezone.utc).isoformat(),
    )


def _weighted_score(signals: list[dict]) -> float:
    """Compute confidence-weighted average sentiment score. Returns 0.0 if empty."""
    if not signals:
        return 0.0
    total_weight = 0.0
    weighted_sum = 0.0
    for sig in signals:
        s     = sig.get("sentiment", "neutral") or "neutral"
        conf  = float(sig.get("sentiment_confidence") or 0.5)
        score = _SENTIMENT_SCORE.get(s, 0.0)
        weighted_sum += score * conf
        total_weight += conf
    return weighted_sum / total_weight if total_weight > 0 else 0.0


# ---------------------------------------------------------------------------
# Entry point: classify all unsent signals then build report
# ---------------------------------------------------------------------------

def backfill_ai_summaries() -> int:
    """
    Generate ai_summary for signals that have sentiment but no ai_summary.
    Returns count of signals updated.
    """
    pending = get_signals_missing_ai_summary()
    if not pending:
        logger.info("AI summaries: all signals already have ai_summary")
        return 0

    logger.info("AI summaries: backfilling %d signals", len(pending))
    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    updated = 0

    for i, sig in enumerate(pending, 1):
        logger.info("AI summary %d/%d: %s", i, len(pending), (sig.get("title") or "")[:60])
        prompt = (
            f"Title: {sig.get('title', '')}\n"
            f"Ingredient: {sig.get('ingredient_name', 'unknown')}\n"
            f"Event Type: {sig.get('event_type', '')}\n"
            f"Severity: {sig.get('severity', '')}\n"
            f"Authority: {sig.get('authority', '')}\n"
            f"Sentiment: {sig.get('sentiment', '')}\n"
            f"Summary: {sig.get('summary', '')}"
        )
        try:
            response = client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=200,
                system=_AI_SUMMARY_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            parsed = _parse_json(response.content[0].text.strip())
            ai_summary = parsed.get("ai_summary", "")
            if ai_summary:
                update_ai_summary(sig["source_id"], ai_summary)
                updated += 1
        except Exception as exc:
            logger.error("AI summary error for %s: %s", sig.get("source_id", "?"), exc)

    logger.info("AI summaries: updated %d signals", updated)
    return updated


def run_sentiment_analysis() -> SentimentReport:
    """
    1. Classify sentiment for any signals missing it (also generates ai_summary).
    2. Backfill ai_summary for signals that have sentiment but missing ai_summary.
    3. Build and return the full ingredient sentiment report.
    """
    pending = get_signals_missing_sentiment()
    if pending:
        logger.info("Sentiment: classifying %d signals without sentiment", len(pending))
        classify_sentiment_batch(pending)
    else:
        logger.info("Sentiment: all signals already have sentiment data")

    # Backfill ai_summary for older signals that predate this feature
    backfill_ai_summaries()

    return build_sentiment_report()
