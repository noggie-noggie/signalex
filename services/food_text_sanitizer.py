"""
services/food_text_sanitizer.py — remove VMS/supplement business wording from Food.

Food launch responses must not show VMS/supplement-specific commercial language
in customer-facing Food rows.  This module sanitizes narrative fields while
leaving factual titles and recall/regulatory details intact where possible.
"""

from __future__ import annotations

import re
from typing import Any


CLEAR_IF_CONTAMINATED_FIELDS = [
    "ai_summary",
    "sentiment_reasoning",
    "why_it_matters",
    "inspection_risk",
]

SANITIZED_FIELDS = [
    *CLEAR_IF_CONTAMINATED_FIELDS,
    "recommended_action",
    "summary",
]

_CONTAMINATION_PATTERNS = [
    re.compile(r"\bvms\b", re.IGNORECASE),
    re.compile(r"\bvms\s+(?:companies|products|brands|manufacturers)\b", re.IGNORECASE),
    re.compile(r"\bvms\s+and\s+nutrition\s+companies\b", re.IGNORECASE),
    re.compile(r"\bcore\s+vms\b", re.IGNORECASE),
    re.compile(r"\bsupplement\s+(?:industry|companies|company|manufacturers|manufacturer|brands|brand|gmp|products|product)\b", re.IGNORECASE),
    re.compile(r"\bsupplements\s+(?:industry|companies|manufacturers|brands|gmp|products)\b", re.IGNORECASE),
]

_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")

_GENERIC_ACTIONS = {
    "recalls_safety": (
        "Review the recall notice, check affected products and lots, assess related "
        "supplier/product exposure, and update internal food safety or quality teams "
        "if relevant."
    ),
    "regulatory_updates": (
        "Review the update for potential impact on product formulation, labelling, "
        "claims, market access, or compliance obligations."
    ),
    "claims_labelling": (
        "Review product wording, label context, and substantiation before using "
        "similar claims."
    ),
    "market_opportunities": (
        "Review the product/category signal for relevance to your category, "
        "positioning, and product pipeline."
    ),
}

_DEFAULT_ACTION = (
    "Review this signal for relevance to food safety, labelling, regulatory "
    "compliance, claims, product quality, or category strategy."
)


def _norm(value: Any) -> str:
    return str(value or "")


def contains_food_text_contamination(value: Any) -> bool:
    """Return True when text contains VMS/supplement business wording."""
    text = _norm(value)
    if not text.strip():
        return False
    return any(pattern.search(text) for pattern in _CONTAMINATION_PATTERNS)


def food_safe_recommended_action(row: dict[str, Any]) -> str:
    """Return a generic Food-safe action based on enriched dashboard section."""
    section = _norm(row.get("dashboard_section")).strip()
    signal_type = _norm(row.get("signal_type")).strip()
    if section in _GENERIC_ACTIONS:
        return _GENERIC_ACTIONS[section]
    if signal_type == "recall":
        return _GENERIC_ACTIONS["recalls_safety"]
    if signal_type in {"regulatory_update", "consultation"}:
        return _GENERIC_ACTIONS["regulatory_updates"]
    if signal_type in {"claim_risk", "labelling_issue"}:
        return _GENERIC_ACTIONS["claims_labelling"]
    if signal_type in {"product_launch", "market_opportunity", "category_trend"}:
        return _GENERIC_ACTIONS["market_opportunities"]
    return _DEFAULT_ACTION


def sanitize_food_summary(value: Any) -> str:
    """
    Remove contaminated sentences from a summary.

    If sentence splitting cannot preserve anything useful, return an empty
    string.  Factual FSANZ recall details stay intact when they are in separate
    sentences from contaminated business commentary.
    """
    text = _norm(value).strip()
    if not text:
        return ""
    if not contains_food_text_contamination(text):
        return text

    sentences = _SENTENCE_SPLIT_RE.split(text)
    clean = [
        sentence.strip()
        for sentence in sentences
        if sentence.strip() and not contains_food_text_contamination(sentence)
    ]
    return " ".join(clean).strip()


def sanitize_food_text_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of a Food row with contaminated narrative fields sanitized."""
    sanitized = dict(row)

    for field in CLEAR_IF_CONTAMINATED_FIELDS:
        if contains_food_text_contamination(sanitized.get(field)):
            sanitized[field] = ""

    if contains_food_text_contamination(sanitized.get("recommended_action")):
        sanitized["recommended_action"] = food_safe_recommended_action(sanitized)

    if contains_food_text_contamination(sanitized.get("summary")):
        sanitized["summary"] = sanitize_food_summary(sanitized.get("summary"))

    return sanitized
