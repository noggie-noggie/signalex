"""
services/food_relevance_filter.py — relevance scoring for FSANZ Food updates.

Filters corporate/admin/newsletter content out of customer-facing Food
regulatory intelligence while preserving safety, consultation, standards,
scientific, surveillance, claims/labelling, and public-health items.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FoodRelevanceResult:
    content_type: str
    score: int
    visible: bool
    reason: str
    confidence: str


_POSITIVE_TERMS: list[tuple[str, int, str]] = [
    ("recall", 8, "recall/safety"),
    ("food safety", 7, "food safety"),
    ("allergen", 7, "allergen"),
    ("contamination", 7, "contamination"),
    ("contaminant", 7, "contaminant"),
    ("toxin", 7, "toxin"),
    ("pathogen", 7, "pathogen"),
    ("public health", 6, "public health"),
    ("consultation", 6, "consultation"),
    ("call for comment", 6, "consultation"),
    ("proposal", 5, "proposal"),
    ("application", 5, "application"),
    ("food standards code", 7, "food standards code"),
    ("standard amendment", 7, "standard amendment"),
    ("amendment", 5, "amendment"),
    ("labelling", 6, "labelling"),
    ("labeling", 6, "labelling"),
    ("health star rating", 7, "claims/labelling"),
    ("novel food", 6, "novel food"),
    ("infant formula", 7, "infant formula"),
    ("additive", 5, "additive"),
    ("processing aid", 6, "processing aid"),
    ("maximum level", 7, "maximum level"),
    ("survey findings", 7, "surveillance"),
    ("survey of", 5, "surveillance"),
    ("microbiological", 7, "microbiological"),
    ("antimicrobial resistance", 7, "surveillance"),
    ("mrl", 6, "chemical risk"),
    ("chemical risk", 7, "chemical risk"),
    ("cadmium", 7, "chemical risk"),
    ("cereulide", 7, "toxin"),
    ("e. coli", 7, "pathogen"),
    ("listeria", 7, "pathogen"),
    ("salmonella", 7, "pathogen"),
]

_NEGATIVE_TERMS: list[tuple[str, int, str]] = [
    ("ceo year in review", -8, "corporate/admin"),
    ("year in review", -7, "corporate/admin"),
    ("annual report", -7, "corporate/admin"),
    ("annual review", -7, "corporate/admin"),
    ("corporate plan", -7, "corporate/admin"),
    ("board update", -6, "corporate/admin"),
    ("board communique", -6, "corporate/admin"),
    ("staff update", -6, "corporate/admin"),
    ("newsletter", -7, "newsletter"),
    ("food standards news", -7, "newsletter"),
    ("conference", -5, "event"),
    ("speech", -5, "event"),
    ("award", -5, "event"),
    ("holiday message", -7, "corporate/admin"),
    ("generic media", -5, "generic media"),
    ("media update", -5, "generic media"),
    ("latest news from fsanz", -7, "newsletter"),
]

_ADMIN_TITLE_TERMS = [
    "ceo year in review",
    "year in review",
    "annual report",
    "annual review",
    "corporate plan",
    "board update",
    "board communique",
    "staff update",
    "holiday message",
]

_HIGH_VALUE_TITLE_TERMS = [
    "recall",
    "consultation",
    "call for comment",
    "contamination",
    "toxin",
    "pathogen",
    "allergen",
    "labelling",
    "labeling",
    "health star rating",
    "maximum level",
    "cadmium",
    "mrl",
    "survey findings",
    "microbiological",
    "food standards code",
    "amendment",
    "processing aid",
]


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _text(row: dict[str, Any]) -> str:
    return " ".join(
        _norm(row.get(field))
        for field in ["title", "summary", "source_label", "product_category", "url"]
    ).lower()


def _title_text(row: dict[str, Any]) -> str:
    return _norm(row.get("title")).lower()


def _has(text: str, term: str) -> bool:
    return term in text


def _content_type(text: str, matched_positive: list[str], matched_negative: list[str]) -> str:
    if any(_has(text, term) for term in ["recall", "contamination", "toxin", "pathogen", "food safety", "allergen"]):
        return "recall_safety"
    if any(_has(text, term) for term in ["call for comment", "consultation"]):
        return "consultation"
    if any(_has(text, term) for term in ["health star rating", "labelling", "labeling", "claim"]):
        return "claims_labelling"
    if any(_has(text, term) for term in ["survey findings", "survey of", "microbiological", "antimicrobial resistance"]):
        return "surveillance_report"
    if any(_has(text, term) for term in ["scientific", "assessment", "risk assessment"]):
        return "scientific_assessment"
    if any(_has(text, term) for term in ["newsletter", "food standards news"]):
        return "newsletter"
    if any(_has(text, term) for term in ["conference", "speech", "award"]):
        return "event_announcement"
    if matched_negative and not matched_positive:
        return "corporate_admin"
    if matched_positive:
        return "regulatory_update"
    return "noise"


def classify_fsanz_update_relevance(row: dict[str, Any]) -> FoodRelevanceResult:
    """Score and classify an FSANZ update row for Food launch visibility."""
    text = _text(row)
    title = _title_text(row)
    score = 0
    matched_positive: list[str] = []
    matched_negative: list[str] = []

    for term, points, reason in _POSITIVE_TERMS:
        if _has(text, term):
            score += points
            matched_positive.append(reason)

    for term, points, reason in _NEGATIVE_TERMS:
        if _has(text, term):
            score += points
            matched_negative.append(reason)

    content_type = _content_type(text, matched_positive, matched_negative)
    visible = score >= 3

    forced_admin_title = any(_has(title, term) for term in _ADMIN_TITLE_TERMS)
    title_has_high_value_topic = any(_has(title, term) for term in _HIGH_VALUE_TITLE_TERMS)

    # High-value regulatory/safety terms override generic media/news wording.
    if matched_positive and any(
        reason in {
            "recall/safety",
            "food safety",
            "contamination",
            "toxin",
            "pathogen",
            "consultation",
            "food standards code",
            "claims/labelling",
            "chemical risk",
            "surveillance",
        }
        for reason in matched_positive
    ):
        visible = True

    if not matched_positive and matched_negative:
        visible = False

    if forced_admin_title and not title_has_high_value_topic:
        visible = False
        content_type = "corporate_admin"

    confidence = "high"
    if -2 <= score <= 4:
        confidence = "low"
    elif 5 <= score <= 7:
        confidence = "medium"

    if forced_admin_title and not title_has_high_value_topic:
        reason = "negative: corporate/admin title without specific regulatory topic"
    elif matched_positive:
        reason = "positive: " + ", ".join(sorted(set(matched_positive)))
    elif matched_negative:
        reason = "negative: " + ", ".join(sorted(set(matched_negative)))
    else:
        reason = "no specific food regulatory relevance terms"

    return FoodRelevanceResult(
        content_type=content_type,
        score=score,
        visible=visible,
        reason=reason,
        confidence=confidence,
    )
