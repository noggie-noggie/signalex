"""
services/food_claims/classifier.py — Deterministic food claim theme classifier.

No AI. No external calls. Loads keyword/theme data from
data_static/food_claim_pathways.json and matches against normalised claim text.

Public API
----------
classify_claim(claim: str) -> dict
    Returns theme, claim_type, risk_level, risk_reasons.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

_DATA_PATH = Path(__file__).parent.parent.parent / "data_static" / "food_claim_pathways.json"

# ---------------------------------------------------------------------------
# High-risk terms that apply universally regardless of theme
# ---------------------------------------------------------------------------
_GLOBAL_HIGH_RISK_TERMS: list[str] = [
    "treats", "cures", "prevents", "heals", "repairs",
    "reduces inflammation", "eliminates",
    # specific conditions
    "ibs", "irritable bowel syndrome",
    "arthritis", "anxiety", "depression",
    "cancer", "diabetes", "heart disease", "cardiovascular disease",
    "stroke", "hypertension", "osteoporosis",
    # therapeutic language
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


def _detect_high_risk(normalised: str, theme_data: Optional[dict]) -> list[str]:
    """
    Return list of risk reasons found in the claim text.
    Checks global high-risk terms first, then theme-specific ones.
    """
    reasons: list[str] = []

    for term in _GLOBAL_HIGH_RISK_TERMS:
        if term.lower() in normalised:
            reasons.append(f"High-risk term detected: '{term}'")

    if theme_data:
        for term in theme_data.get("high_risk_terms", []):
            if term.lower() in normalised:
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
        theme           str | None   — matched theme name, or None if unrecognised
        claim_type      str          — e.g. "health_claim", "nutrient_content_claim", "unknown"
        risk_level      str          — "low", "medium", "high"
        risk_reasons    list[str]    — list of reasons for elevated risk
    """
    if not claim or not claim.strip():
        return {
            "theme": None,
            "claim_type": "unknown",
            "risk_level": "unknown",
            "risk_reasons": ["No claim text provided."],
        }

    normalised = _normalise(claim)
    data       = _load_data()
    themes     = data.get("themes", {})

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
        "theme":        best_theme,
        "claim_type":   claim_type,
        "risk_level":   risk_level,
        "risk_reasons": risk_reasons,
    }
