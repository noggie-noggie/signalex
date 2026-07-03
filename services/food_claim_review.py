"""
services/food_claim_review.py — deterministic free-text food claim review.

Phase 1 is deliberately rule-based: no AI calls, no external services.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from services.food_claim_pathways import DISCLAIMER, get_claim_pathway


_FRONTEND_FIELDS = [
    "claim_text",
    "display_claim",
    "risk_level",
    "claim_type",
    "headline",
    "assessment",
    "regulatory_context",
    "recommended_pathways",
    "wording_to_avoid",
    "missing_information",
    "safer_wording",
    "evidence_requirements",
    "recommended_action",
    "matched_themes",
    "disclaimer",
    "ai_used",
]

_THERAPEUTIC_PATTERNS: list[tuple[str, str]] = [
    ("IBS", r"\bibs\b|\birritable bowel syndrome\b"),
    ("diabetes", r"\bdiabetes\b|\bdiabetic\b"),
    ("arthritis", r"\barthritis\b"),
    ("anxiety", r"\banxiety\b"),
    ("depression", r"\bdepression\b"),
    ("inflammation", r"\binflammation\b|\binflammatory\b"),
    ("pain", r"\bpain\b"),
    ("injury", r"\binjury\b|\binjuries\b"),
    ("cure", r"\bcure[s]?\b|\bcuring\b"),
    ("treat", r"\btreat[s]?\b|\btreating\b"),
    ("prevent disease", r"\bprevent[s]?\s+(disease|illness|infection)\b"),
    ("repair muscle damage", r"\brepair[s]?\s+muscle\s+damage\b"),
    ("relieve symptoms", r"\brelieve[s]?\s+symptoms?\b"),
    ("clinical", r"\bclinical\b|\bclinically\b"),
    ("therapeutic", r"\btherapeutic\b"),
]

_THEME_PATTERNS: list[tuple[str, str, str]] = [
    ("high_protein", "High in protein", r"\b(high\s+in\s+protein|high\s+protein|protein\s+source|source\s+of\s+protein|protein)\b"),
    ("source_of_fibre", "Source of fibre", r"\b(fibre|fiber|high\s+fibre|high\s+fiber|source\s+of\s+fibre|source\s+of\s+fiber)\b"),
    ("low_sugar", "Low sugar", r"\b(low\s+sugar|reduced\s+sugar|no\s+sugar|sugar\s+free|no\s+added\s+sugar)\b"),
    ("gut_health", "Gut health", r"\b(gut\s+health|digestive\s+wellbeing|digestive\s+health|digestion|probiotic|prebiotic|live\s+cultures?)\b"),
    ("energy", "Energy", r"\b(energy|active\s+lifestyle|active\s+lifestyles|vitality)\b"),
]

_THEME_TO_PATHWAY = {
    "high_protein": "high_protein",
    "source_of_fibre": "gut_health",
    "low_sugar": "low_sugar",
    "gut_health": "gut_health",
    "energy": "energy",
}

_UNCLASSIFIED_MISSING_INFO = [
    "Exact proposed wording on the claim",
    "Full ingredient list",
    "Nutrition information panel",
    "Amount per serve for key ingredients",
    "Target consumer and population",
    "Product format and serving size",
    "Evidence or substantiation intended to support the claim",
]

_THERAPEUTIC_SAFER_WORDING = [
    "Supports digestive wellbeing",
    "Contains live cultures",
    "Contains fibre to support digestive health",
    "Supports general wellbeing",
]

_THERAPEUTIC_AVOID_VARIANTS = [
    "Treats IBS",
    "Cures IBS",
    "Relieves IBS symptoms",
    "Treats disease",
    "Prevents disease",
    "Repairs muscle damage",
    "Speeds injury recovery",
    "Clinically treats symptoms",
    "Therapeutic support",
]


@dataclass(frozen=True)
class ThemeMatch:
    key: str
    display: str


def _normalise_text(value: str | None) -> str:
    text = (value or "").strip().lower()
    text = text.replace("_", " ").replace("-", " ")
    text = re.sub(r"[^a-z0-9\s']", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _detect_therapeutic_terms(normalised: str) -> list[str]:
    terms: list[str] = []
    for label, pattern in _THERAPEUTIC_PATTERNS:
        if re.search(pattern, normalised):
            terms.append(label)
    return terms


def _detect_themes(normalised: str) -> list[ThemeMatch]:
    matches: list[ThemeMatch] = []
    for key, display, pattern in _THEME_PATTERNS:
        if re.search(pattern, normalised):
            matches.append(ThemeMatch(key=key, display=display))
    return matches


def _empty_response(claim_text: str, display_claim: str) -> dict[str, Any]:
    return {
        "claim_text": claim_text,
        "display_claim": display_claim,
        "risk_level": "review_required",
        "claim_type": "unclassified_food_claim",
        "headline": "Claim needs review",
        "assessment": (
            "This claim was not matched to a deterministic food claim pathway. "
            "Review the exact wording, ingredients, nutrition panel, product "
            "format, and supporting evidence before use."
        ),
        "regulatory_context": (
            "No regulatory certainty is inferred. The claim should be reviewed "
            "against the product formulation, label context, and applicable AU/NZ "
            "food standards."
        ),
        "recommended_pathways": [],
        "wording_to_avoid": [],
        "missing_information": list(_UNCLASSIFIED_MISSING_INFO),
        "safer_wording": [],
        "evidence_requirements": [],
        "recommended_action": (
            "Collect the missing product and substantiation details, then review "
            "the claim before using it in customer-facing materials."
        ),
        "matched_themes": [],
        "disclaimer": DISCLAIMER,
        "ai_used": False,
    }


def review_food_claim(
    claim_text: str,
    food_type: str = "",
    jurisdiction: str = "AU/NZ",
) -> dict[str, Any]:
    """Return deterministic free-text food claim assessment."""
    raw_claim = (claim_text or "").strip()
    normalised = _normalise_text(raw_claim)
    display_claim = raw_claim or "Unspecified claim"
    therapeutic_terms = _detect_therapeutic_terms(normalised)
    theme_matches = _detect_themes(normalised)

    if therapeutic_terms:
        avoid = list(dict.fromkeys([*therapeutic_terms, *_THERAPEUTIC_AVOID_VARIANTS]))
        return {
            "claim_text": raw_claim,
            "display_claim": display_claim,
            "risk_level": "high",
            "claim_type": "therapeutic_or_disease_related_claim",
            "headline": "High-risk therapeutic or disease-related wording",
            "assessment": (
                f"The claim '{raw_claim}' contains disease, symptom, clinical, "
                "or therapeutic-style wording "
                f"({', '.join(therapeutic_terms)}). This is not suitable as a "
                "food claim without specialist regulatory review."
            ),
            "regulatory_context": (
                f"In {jurisdiction}, food claims should not imply treatment, cure, "
                "prevention, or management of diseases or medical conditions. "
                "IBS and similar condition-specific wording should be removed from "
                "food claim copy."
            ),
            "recommended_pathways": [],
            "wording_to_avoid": avoid,
            "missing_information": [
                "Exact proposed wording on the claim",
                "Product format and target consumer",
                "Full ingredient list",
                "Evidence or substantiation intended to support any replacement wording",
            ],
            "safer_wording": list(_THERAPEUTIC_SAFER_WORDING),
            "evidence_requirements": [],
            "recommended_action": (
                "Remove disease-specific or therapeutic wording and reframe the "
                "claim as general wellbeing or nutrition-content wording only where "
                "the formulation and substantiation support it."
            ),
            "matched_themes": [match.key for match in theme_matches],
            "disclaimer": DISCLAIMER,
            "ai_used": False,
        }

    if not theme_matches:
        return _empty_response(raw_claim, display_claim)

    primary = theme_matches[0]
    pathway_key = _THEME_TO_PATHWAY.get(primary.key)
    pathway = get_claim_pathway(pathway_key or primary.key) if pathway_key else None
    if not pathway:
        response = _empty_response(raw_claim, display_claim)
        response["matched_themes"] = [match.key for match in theme_matches]
        return response

    risk_level = pathway["risk_level"]
    if primary.key == "high_protein":
        risk_level = "medium"

    return {
        "claim_text": raw_claim,
        "display_claim": pathway["display_claim"],
        "risk_level": risk_level,
        "claim_type": pathway["claim_type"],
        "headline": f"{pathway['display_claim']} pathway identified",
        "assessment": (
            f"The free-text claim maps to the {pathway['display_claim']} pathway. "
            "Use the recommended pathway details to check whether the product "
            "formulation, label, and substantiation support the proposed wording."
        ),
        "regulatory_context": pathway["regulatory_context"],
        "recommended_pathways": pathway["recommended_pathways"],
        "wording_to_avoid": pathway["wording_to_avoid"],
        "missing_information": pathway["missing_information"],
        "safer_wording": pathway["safer_wording"],
        "evidence_requirements": pathway["evidence_requirements"],
        "recommended_action": pathway["recommended_action"],
        "matched_themes": [match.key for match in theme_matches],
        "disclaimer": DISCLAIMER,
        "ai_used": False,
    }


def response_field_names() -> list[str]:
    """Expose expected frontend field names for tests."""
    return list(_FRONTEND_FIELDS)
