"""
services/food_claims/classifier.py — Deterministic food claim theme classifier.

No AI. No external calls. Loads keyword/theme data from
data_static/food_claim_pathways.json and matches against normalised claim text.

Public API
----------
classify_claim(claim: str) -> dict
    Returns theme, claim_type, risk_level, risk_reasons, is_therapeutic.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Optional

_DATA_PATH = Path(__file__).parent.parent.parent / "data_static" / "food_claim_pathways.json"

# ---------------------------------------------------------------------------
# Therapeutic / disease terms — trigger theme override to
# "therapeutic_or_disease_claim" regardless of keyword match score.
# Checked via substring match on normalised claim text.
# ---------------------------------------------------------------------------
_THERAPEUTIC_TERMS: list[str] = [
    # treatment verbs
    "treat", "treats", "treating",
    "cure", "cures", "curing",
    "prevent", "prevents", "preventing",
    "heal", "heals", "healing",
    "repair", "repairs", "repairing",
    # inflammation
    "reduce inflammation", "reduces inflammation", "reducing inflammation",
    "anti-inflammatory",
    # named conditions
    "ibs", "irritable bowel syndrome",
    "arthritis", "anxiety", "depression",
    "disease", "diseases", "infection", "infections",
    "antiviral",
]

# Pre-compiled word-boundary patterns — used by both therapeutic detection and
# high-risk reason generation.  Word boundaries prevent "heal" matching
# "health", "treat" matching "treatment", etc.
@lru_cache(maxsize=None)
def _therapeutic_patterns() -> list[re.Pattern]:
    return [re.compile(r"\b" + re.escape(t) + r"\b") for t in _THERAPEUTIC_TERMS]


@lru_cache(maxsize=None)
def _global_risk_patterns() -> list[tuple[str, re.Pattern]]:
    """Return (term, compiled_pattern) pairs for every global high-risk term."""
    return [
        (t, re.compile(r"\b" + re.escape(t) + r"\b"))
        for t in _GLOBAL_HIGH_RISK_TERMS
    ]

# ---------------------------------------------------------------------------
# Additional high-risk terms (generate risk_reasons but do not override theme
# on their own — they are already covered if _THERAPEUTIC_TERMS fires first).
# ---------------------------------------------------------------------------
_GLOBAL_HIGH_RISK_TERMS: list[str] = [
    "treat", "treats", "treating",
    "cure", "cures",
    "prevent", "prevents",
    "heal", "heals",
    "repair", "repairs",
    "reduces inflammation", "reduce inflammation", "anti-inflammatory",
    "eliminates",
    # conditions
    "ibs", "irritable bowel syndrome",
    "arthritis", "anxiety", "depression",
    "cancer", "diabetes", "heart disease", "cardiovascular disease",
    "stroke", "hypertension", "osteoporosis",
    "disease", "infection", "antiviral",
    # stronger therapeutic language
    "clinically proven to cure", "medically proven", "therapeutic",
    "pharmaceutical", "medicine", "medication",
]


def _load_data() -> dict:
    """Load pathway data. Returns empty dict on failure — classifier degrades gracefully."""
    try:
        return json.loads(_DATA_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _normalise(text: str) -> str:
    """Lowercase, collapse whitespace, strip punctuation except apostrophes/hyphens."""
    text = text.lower()
    text = re.sub(r"[^\w\s\-']", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _match_theme(normalised: str, themes: dict) -> tuple[Optional[str], int]:
    """
    Return (best_theme, match_count) by counting keyword hits per theme.
    Ties broken by whichever theme appears first in iteration order.
    """
    best_theme: Optional[str] = None
    best_count = 0

    for theme_name, theme_data in themes.items():
        keywords: list[str] = theme_data.get("keywords", [])
        count = sum(1 for kw in keywords if kw.lower() in normalised)
        if count > best_count:
            best_count = count
            best_theme = theme_name

    return best_theme, best_count


def _is_therapeutic_claim(normalised: str) -> bool:
    """
    Return True if the claim contains any therapeutic/disease term.

    Uses word-boundary regex to prevent false positives such as:
      "heal" matching "health", "treat" matching "treatment".
    """
    return any(p.search(normalised) for p in _therapeutic_patterns())


def _detect_high_risk(normalised: str, theme_data: Optional[dict]) -> list[str]:
    """
    Return list of risk reasons found in the claim text.

    Uses word-boundary matching to avoid false positives such as
    "heal" matching "health" or "treat" matching "treatment".
    Checks global high-risk terms first, then theme-specific ones.
    """
    reasons: list[str] = []

    for term, pattern in _global_risk_patterns():
        if pattern.search(normalised):
            reasons.append(f"High-risk term detected: '{term}'")

    if theme_data:
        for term in theme_data.get("high_risk_terms", []):
            pat = re.compile(r"\b" + re.escape(term.lower()) + r"\b")
            if pat.search(normalised):
                reasons.append(f"Theme-specific high-risk term detected: '{term}'")

    # Deduplicate while preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for r in reasons:
        if r not in seen:
            seen.add(r)
            deduped.append(r)
    return deduped


def classify_claim(claim: str) -> dict:
    """
    Classify a food claim string deterministically.

    Parameters
    ----------
    claim : str
        Raw claim text from the user, e.g. "Supports gut health".

    Returns
    -------
    dict with keys:
        theme           str | None   — matched theme name, or None if unrecognised.
                                       "therapeutic_or_disease_claim" if therapeutic
                                       terms are detected.
        claim_type      str          — e.g. "health_claim", "nutrient_content_claim",
                                       "therapeutic / disease-related claim", "unknown"
        risk_level      str          — "low", "medium", "high"
        risk_reasons    list[str]    — reasons for elevated risk
        is_therapeutic  bool         — True if therapeutic/disease terms detected
    """
    if not claim or not claim.strip():
        return {
            "theme":          None,
            "claim_type":     "unknown",
            "risk_level":     "unknown",
            "risk_reasons":   ["No claim text provided."],
            "is_therapeutic": False,
        }

    normalised = _normalise(claim)
    data       = _load_data()
    themes     = data.get("themes", {})

    # --- Therapeutic / disease detection (takes priority over theme matching) --
    if _is_therapeutic_claim(normalised):
        risk_reasons = _detect_high_risk(normalised, None)
        return {
            "theme":          "therapeutic_or_disease_claim",
            "claim_type":     "therapeutic / disease-related claim",
            "risk_level":     "high",
            "risk_reasons":   risk_reasons,
            "is_therapeutic": True,
        }

    # --- Theme matching -------------------------------------------------------
    best_theme, match_count = _match_theme(normalised, themes)
    theme_data = themes.get(best_theme) if best_theme else None

    # No theme matched at all
    if match_count == 0:
        best_theme = None
        theme_data = None

    # --- Risk detection -------------------------------------------------------
    risk_reasons = _detect_high_risk(normalised, theme_data)

    # --- Risk level -----------------------------------------------------------
    if risk_reasons:
        risk_level = "high"
    elif theme_data:
        risk_level = theme_data.get("default_risk_level", "medium")
    else:
        risk_level = "medium"  # unrecognised theme — conservative default

    # --- Claim type -----------------------------------------------------------
    if theme_data:
        claim_type = theme_data.get("claim_type", "health_claim")
    else:
        claim_type = "unknown"

    return {
        "theme":          best_theme,
        "claim_type":     claim_type,
        "risk_level":     risk_level,
        "risk_reasons":   risk_reasons,
        "is_therapeutic": False,
    }
