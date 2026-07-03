"""
services/food_claim_review.py — deterministic free-text food claim review.

Phase 1 is deliberately rule-based: no AI calls, no external services.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from services.food_claim_pathways import DISCLAIMER, get_claim_pathway
from services.openai_claim_review import maybe_enhance_claim_review


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
    "context",
    "disclaimer",
    "ai_used",
    "ai_available",
    "ai_quota_remaining",
    "ai_quota_reset",
    "assessment_mode",
    "cache_hit",
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

_FOOD_TYPE_MAP = {
    "yoghurt": "yoghurt",
    "yogurt": "yoghurt",
    "protein bar": "protein_bar",
    "protein_bar": "protein_bar",
    "beverage": "beverage",
    "drink": "beverage",
    "snack": "snack",
    "cereal": "cereal",
    "infant toddler food": "infant_toddler_food",
    "infant / toddler food": "infant_toddler_food",
    "infant/toddler food": "infant_toddler_food",
    "supplement like food": "supplement_like_food",
    "supplement-like food": "supplement_like_food",
    "supplement_like_food": "supplement_like_food",
    "plant based": "plant_based",
    "plant-based": "plant_based",
    "plant_based": "plant_based",
    "sauce condiment": "sauce_condiment",
    "sauce / condiment": "sauce_condiment",
    "sauce/condiment": "sauce_condiment",
    "sauce_condiment": "sauce_condiment",
    "other": "other",
}

_CLAIM_LOCATION_MAP = {
    "front_of_pack": "front_of_pack",
    "front of pack": "front_of_pack",
    "back_of_pack": "back_of_pack",
    "back of pack": "back_of_pack",
    "marketing_advertising": "marketing_advertising",
    "marketing advertising": "marketing_advertising",
    "marketing / advertising": "marketing_advertising",
    "marketing/advertising": "marketing_advertising",
}

_SERVING_SIZE_UNITS = {"g", "kg", "mg", "mL", "L"}

_LOCATION_ACTION_NOTES = {
    "front_of_pack": (
        "Front-of-pack wording should be short, specific, and closely tied to "
        "substantiated nutrition or general health wording."
    ),
    "back_of_pack": (
        "Back-of-pack wording may include more context, conditions, or qualifying "
        "information, but still needs support."
    ),
    "marketing_advertising": (
        "Marketing and advertising claims are consumer-facing and should avoid "
        "exaggeration, disease implications, clinical implications, or benefits "
        "beyond the evidence."
    ),
}

_SUPPLEMENT_LIKE_CAUTION = (
    "Because the product is described as supplement-like, review whether it is "
    "properly classified as a food or may fall into therapeutic/supplement-style "
    "presentation depending on ingredients, dosage form, and claims."
)


@dataclass(frozen=True)
class ThemeMatch:
    key: str
    display: str


def _normalise_text(value: str | None) -> str:
    text = (value or "").strip().lower()
    text = text.replace("_", " ").replace("-", " ")
    text = re.sub(r"[^a-z0-9\s']", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _normalise_food_type(value: str | None) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    lowered = re.sub(r"\s+", " ", text.replace("_", " ").strip().lower())
    return _FOOD_TYPE_MAP.get(lowered, lowered.replace(" ", "_"))


def _normalise_claim_location(value: str | None) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    lowered = re.sub(r"\s+", " ", text.replace("_", " ").strip().lower())
    return _CLAIM_LOCATION_MAP.get(lowered, "")


def _normalise_serving_size(value: Any) -> dict[str, str] | None:
    if not isinstance(value, dict):
        return None
    amount = str(value.get("value", "")).strip()
    raw_unit = str(value.get("unit", "")).strip()
    unit_lookup = {"g": "g", "kg": "kg", "mg": "mg", "ml": "mL", "l": "L"}
    unit = unit_lookup.get(raw_unit.lower(), raw_unit)
    if not amount or unit not in _SERVING_SIZE_UNITS:
        return None
    return {"value": amount, "unit": unit}


def _build_context(
    food_type: str = "",
    claim_location: str | None = None,
    serving_size: Any = None,
) -> dict[str, Any]:
    return {
        "food_type": _normalise_food_type(food_type),
        "claim_location": _normalise_claim_location(claim_location),
        "serving_size": _normalise_serving_size(serving_size),
    }


def _remove_missing_information(items: list[str], context: dict[str, Any]) -> list[str]:
    out: list[str] = []
    food_type = context.get("food_type") or ""
    has_specific_food_type = food_type and food_type not in {"other", "unknown"}
    has_serving = context.get("serving_size") is not None
    has_location = bool(context.get("claim_location"))
    for item in items:
        lowered = item.lower()
        if has_serving and "serving size" in lowered:
            continue
        if (
            has_specific_food_type
            and not has_serving
            and "product format" in lowered
            and "serving size" in lowered
        ):
            out.append("Serving size")
            continue
        if has_specific_food_type and (
            "product format" in lowered
            or "food type" in lowered
            or "product matrix" in lowered
        ):
            continue
        if has_location and (
            "where the claim" in lowered
            or "claim location" in lowered
            or "claim will appear" in lowered
        ):
            continue
        out.append(item)
    return out


def _adjust_response_for_context(response: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    adjusted = dict(response)
    adjusted["context"] = context
    missing = list(adjusted.get("missing_information") or [])
    if not context.get("claim_location"):
        missing.append("Where the claim will appear on pack, label, website, or advertising")
    adjusted["missing_information"] = _remove_missing_information(
        list(dict.fromkeys(missing)),
        context,
    )

    action_parts = [adjusted.get("recommended_action", "").strip()]
    location_note = _LOCATION_ACTION_NOTES.get(context.get("claim_location") or "")
    if location_note:
        action_parts.append(location_note)
    if context.get("food_type") == "supplement_like_food":
        action_parts.append(_SUPPLEMENT_LIKE_CAUTION)
        regulatory_context = adjusted.get("regulatory_context", "")
        if _SUPPLEMENT_LIKE_CAUTION not in regulatory_context:
            adjusted["regulatory_context"] = f"{regulatory_context} {_SUPPLEMENT_LIKE_CAUTION}".strip()
    adjusted["recommended_action"] = " ".join(part for part in action_parts if part)
    return adjusted


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
    claim_location: str | None = None,
    serving_size: Any = None,
    use_ai: bool = False,
    force_ai: bool = False,
    client_ip: str | None = None,
) -> dict[str, Any]:
    """Return deterministic free-text food claim assessment."""
    raw_claim = (claim_text or "").strip()
    normalised = _normalise_text(raw_claim)
    display_claim = raw_claim or "Unspecified claim"
    context = _build_context(food_type, claim_location, serving_size)
    therapeutic_terms = _detect_therapeutic_terms(normalised)
    theme_matches = _detect_themes(normalised)

    if therapeutic_terms:
        avoid = list(dict.fromkeys([*therapeutic_terms, *_THERAPEUTIC_AVOID_VARIANTS]))
        response = {
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
        response = _adjust_response_for_context(response, context)
        return maybe_enhance_claim_review(
            response,
            claim_text=raw_claim,
            food_type=context.get("food_type") or food_type,
            jurisdiction=jurisdiction,
            context=context,
            use_ai=use_ai,
            force_ai=force_ai,
            client_ip=client_ip,
        )

    if not theme_matches:
        response = _empty_response(raw_claim, display_claim)
        response = _adjust_response_for_context(response, context)
        return maybe_enhance_claim_review(
            response,
            claim_text=raw_claim,
            food_type=context.get("food_type") or food_type,
            jurisdiction=jurisdiction,
            context=context,
            use_ai=use_ai,
            force_ai=force_ai,
            client_ip=client_ip,
        )

    primary = theme_matches[0]
    pathway_key = _THEME_TO_PATHWAY.get(primary.key)
    pathway = get_claim_pathway(pathway_key or primary.key) if pathway_key else None
    if not pathway:
        response = _empty_response(raw_claim, display_claim)
        response["matched_themes"] = [match.key for match in theme_matches]
        response = _adjust_response_for_context(response, context)
        return maybe_enhance_claim_review(
            response,
            claim_text=raw_claim,
            food_type=context.get("food_type") or food_type,
            jurisdiction=jurisdiction,
            context=context,
            use_ai=use_ai,
            force_ai=force_ai,
            client_ip=client_ip,
        )

    risk_level = pathway["risk_level"]
    if primary.key == "high_protein":
        risk_level = "medium"

    response = {
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
    response = _adjust_response_for_context(response, context)
    return maybe_enhance_claim_review(
        response,
        claim_text=raw_claim,
        food_type=context.get("food_type") or food_type,
        jurisdiction=jurisdiction,
        context=context,
        use_ai=use_ai,
        force_ai=force_ai,
        client_ip=client_ip,
    )


def response_field_names() -> list[str]:
    """Expose expected frontend field names for tests."""
    return list(_FRONTEND_FIELDS)
