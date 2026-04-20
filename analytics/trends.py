"""
analytics/trends.py — Rolling trend detection for VMS signals.

On each pipeline run:
  1. Load all signals from the last 30 days from signals.db.
  2. Compute 7-day and 30-day rolling counts per ingredient and per event_type.
  3. Detect spikes: 7-day count > 2x the 30-day daily average for an ingredient.
  4. Detect claim shifts: if an ingredient moves from mostly efficacy_claim to
     safety_concern signals over the last 30 days, flag as a claim_shift.

Returns:
  TrendReport with:
    - trending_ingredients: list of dicts with ingredient + counts + change
    - claim_shift_alerts: list of dicts with ingredient + shift description
    - rolling_counts: full 30-day counts per ingredient (for digest rendering)
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

from analytics.db import get_signals_since

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TrendingIngredient:
    ingredient: str
    count_7d:   int
    count_30d:  int
    avg_daily_30d: float
    spike_ratio:   float    # count_7d / (avg_daily_30d * 7)


@dataclass
class ClaimShiftAlert:
    ingredient:          str
    old_dominant_type:   str
    new_dominant_type:   str
    old_count:           int
    new_count:           int
    description:         str


@dataclass
class TrendReport:
    trending_ingredients: list[TrendingIngredient] = field(default_factory=list)
    claim_shift_alerts:   list[ClaimShiftAlert]    = field(default_factory=list)
    rolling_counts:       dict                     = field(default_factory=dict)
    generated_at:         str                      = ""


# ---------------------------------------------------------------------------
# Detection engine
# ---------------------------------------------------------------------------

def run_trend_detection() -> TrendReport:
    """
    Main entry point. Loads recent signals from SQLite and computes trends.
    Returns a TrendReport.
    """
    signals_30d = get_signals_since(days=30)
    cutoff_7d   = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

    if not signals_30d:
        logger.info("Trend detection: no signals in last 30 days")
        return TrendReport(generated_at=datetime.now(timezone.utc).isoformat())

    # Separate 7-day and 30-day windows
    signals_7d = [s for s in signals_30d if s.get("scraped_at", "") >= cutoff_7d]

    # Count per ingredient
    counts_30d: dict[str, int] = defaultdict(int)
    counts_7d:  dict[str, int] = defaultdict(int)

    for sig in signals_30d:
        ing = (sig.get("ingredient_name") or "unknown").lower().strip()
        if ing and ing != "unknown":
            counts_30d[ing] += 1

    for sig in signals_7d:
        ing = (sig.get("ingredient_name") or "unknown").lower().strip()
        if ing and ing != "unknown":
            counts_7d[ing] += 1

    # Rolling counts for digest
    rolling_counts = {
        ing: {"7d": counts_7d.get(ing, 0), "30d": counts_30d[ing]}
        for ing in counts_30d
    }

    # Spike detection
    trending: list[TrendingIngredient] = []
    for ing, count_30d in counts_30d.items():
        if count_30d < 2:  # Skip very sparse ingredients
            continue
        avg_daily = count_30d / 30.0
        count_7d  = counts_7d.get(ing, 0)
        expected_7d = avg_daily * 7

        if expected_7d > 0 and count_7d > 2 * expected_7d:
            spike_ratio = count_7d / expected_7d
            trending.append(TrendingIngredient(
                ingredient    = ing,
                count_7d      = count_7d,
                count_30d     = count_30d,
                avg_daily_30d = round(avg_daily, 2),
                spike_ratio   = round(spike_ratio, 2),
            ))

    trending.sort(key=lambda x: x.spike_ratio, reverse=True)

    # Claim shift detection
    claim_shifts = _detect_claim_shifts(signals_30d)

    report = TrendReport(
        trending_ingredients = trending,
        claim_shift_alerts   = claim_shifts,
        rolling_counts       = rolling_counts,
        generated_at         = datetime.now(timezone.utc).isoformat(),
    )

    if trending:
        logger.info(
            "Trend detection: %d trending ingredient(s): %s",
            len(trending),
            ", ".join(t.ingredient for t in trending[:5]),
        )
    if claim_shifts:
        logger.info(
            "Trend detection: %d claim shift(s): %s",
            len(claim_shifts),
            ", ".join(c.ingredient for c in claim_shifts[:3]),
        )

    return report


def _detect_claim_shifts(signals: list[dict]) -> list[ClaimShiftAlert]:
    """
    For each ingredient, compare the dominant signal_type in the first half
    of the 30-day window vs. the second half.

    A claim shift is flagged when an ingredient moves from a majority of
    efficacy_claim signals to a majority of safety_concern signals (or vice versa).
    """
    if not signals:
        return []

    # Sort by scraped_at
    sorted_signals = sorted(signals, key=lambda s: s.get("scraped_at", ""))
    mid_idx = len(sorted_signals) // 2
    first_half  = sorted_signals[:mid_idx]
    second_half = sorted_signals[mid_idx:]

    def dominant_type(sigs: list[dict], ingredient: str) -> tuple[str, int]:
        """Return (dominant_signal_type, count) for an ingredient in a window."""
        type_counts: dict[str, int] = defaultdict(int)
        for s in sigs:
            ing = (s.get("ingredient_name") or "").lower().strip()
            if ing != ingredient:
                continue
            stype = s.get("signal_type") or s.get("event_type") or "other"
            type_counts[stype] += 1
        if not type_counts:
            return "other", 0
        best = max(type_counts, key=type_counts.__getitem__)
        return best, type_counts[best]

    # Collect all ingredients with sufficient signal volume
    ing_counts: dict[str, int] = defaultdict(int)
    for s in signals:
        ing = (s.get("ingredient_name") or "").lower().strip()
        if ing and ing != "unknown":
            ing_counts[ing] += 1

    shifts: list[ClaimShiftAlert] = []
    for ing, total in ing_counts.items():
        if total < 3:  # Need at least 3 signals to detect a shift
            continue

        old_type, old_count = dominant_type(first_half, ing)
        new_type, new_count = dominant_type(second_half, ing)

        # Meaningful shift: types differ and both have at least 1 signal
        if old_type == new_type or old_count == 0 or new_count == 0:
            continue

        # Prioritise shifts toward safety_concern
        is_notable = (
            (new_type == "safety_concern" and old_type in ("efficacy_claim", "other")) or
            (old_type == "efficacy_claim" and new_type == "safety_concern")
        )

        if is_notable or (old_type != new_type and old_count >= 2 and new_count >= 2):
            description = (
                f"{ing.title()} signals shifted from predominantly '{old_type}' "
                f"({old_count} signals in first half) to '{new_type}' "
                f"({new_count} signals in second half) over the last 30 days."
            )
            shifts.append(ClaimShiftAlert(
                ingredient        = ing,
                old_dominant_type = old_type,
                new_dominant_type = new_type,
                old_count         = old_count,
                new_count         = new_count,
                description       = description,
            ))

    return shifts
