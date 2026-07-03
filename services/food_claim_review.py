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
    "possible_supporting_routes",
    "wording_to_avoid",
    "missing_information",
    "safer_wording",
    "evidence_requirements",
    "recommended_action",
    "matched_themes",
    "multi_claim",
    "claim_breakdown",
    "overall_note",
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
    ("insomnia", r"\binsomnia\b"),
    ("infection", r"\binfection[s]?\b"),
    ("cold", r"\bcolds?\b"),
    ("flu", r"\bflu\b"),
    ("cure", r"\bcure[s]?\b|\bcuring\b"),
    ("treat", r"\btreat[s]?\b|\btreating\b"),
    ("heals or repairs body tissue", r"\b(heal[s]?|repair[s]?|rebuild[s]?|reverse[s]?)\s+(muscle|collagen|joint[s]?|cartilage|gut|gut\s+lining|tissue|injur(?:y|ies)|damage|skin|bone[s]?)\b"),
    ("body tissue repair", r"\b(cartilage|joint[s]?|collagen|muscle|gut|skin|bone[s]?)\s+(repair|healing|regeneration)\b"),
    ("cartilage repair", r"\bcartilage\s+(repair|rebuild|regeneration)\b"),
    ("joint repair", r"\bjoint[s]?\s+(repair|rebuild|healing)\b"),
    ("collagen repair", r"\bcollagen\s+(repair|rebuild|healing)\b"),
    ("leaky gut", r"\bleaky\s+gut\b"),
    ("hormone balance", r"\bhormone\s+balance\b"),
    ("pain relief", r"\bpain\s+relief\b"),
    ("anti-inflammatory", r"\banti\s+inflammatory\b|\banti-inflammatory\b"),
    ("lowers cholesterol", r"\blower[s]?\s+cholesterol\b|\breduce[s]?\s+cholesterol\b"),
    ("rapid fat burning", r"\b(burn[s]?\s+fat\s+fast|rapid\s+fat\s+loss|melts?\s+fat)\b"),
    ("prevent disease", r"\bprevent[s]?\s+(disease|illness|infection)\b"),
    ("prevents colds", r"\bprevent[s]?\s+colds?\b"),
    ("prevents flu", r"\bprevent[s]?\s+flu\b"),
    ("fights infection", r"\bfight[s]?\s+infection[s]?\b"),
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
    ("immunity", "Immunity", r"\b(improv(?:e|es|ing)\s+immunity|helps?\s+immunity|supports?\s+immunity|immune\s+support|immune\s+health|supports?\s+immune\s+function|boosts?\s+immunity|strengthens?\s+immunity|improv(?:e|es|ing)\s+immune\s+system|immune\s+defen[cs]e|immunity|immune\s+system)\b"),
    ("muscle_performance", "Muscle / performance", r"\b(muscle\s+(strength|function|performance|recovery|maintenance)|supports?\s+muscle|builds?\s+muscle|active\s+lifestyle|sports?\s+performance|exercise\s+performance)\b"),
    ("collagen_skin", "Collagen / skin / beauty", r"\b(collagen\s+(formation|support|production)|supports?\s+collagen|skin\s+(structure|health|elasticity)|beauty|hair|nails?)\b"),
    ("joint_bone", "Joint / bone", r"\b(joint\s+health|supports?\s+joints?|bone\s+health|supports?\s+bones?|calcium|vitamin\s+d|cartilage)\b"),
    ("energy", "Energy / fatigue", r"\b(energy|boosts?\s+energy|fatigue|tiredness|active\s+lifestyle|active\s+lifestyles|vitality)\b"),
    ("weight_satiety", "Weight management / satiety", r"\b(weight\s+management|healthy\s+weight|satiety|fuller\s+for\s+longer|appetite|burns?\s+fat|fat\s+burn)\b"),
    ("brain_focus_mood", "Brain / focus / mood", r"\b(focus|concentration|mental\s+performance|clarity|brain\s+health|mood|stress)\b"),
    ("sleep_calm", "Sleep / calm", r"\b(sleep|calm|relaxation|relax|restful)\b"),
    ("heart_cholesterol", "Heart / cholesterol", r"\b(heart\s+health|cardiovascular|cholesterol|omega\s*3|plant\s+sterol|beta\s+glucan)\b"),
    ("blood_sugar", "Blood sugar / glycaemic", r"\b(blood\s+sugar|glycaemic|glycemic|glucose|low\s+gi)\b"),
    ("hydration_electrolytes", "Hydration / electrolytes", r"\b(hydration|hydrate|electrolytes?|sodium|potassium|magnesium|fluid\s+balance)\b"),
]

_THEME_TO_PATHWAY = {
    "high_protein": "high_protein",
    "source_of_fibre": "gut_health",
    "low_sugar": "low_sugar",
    "gut_health": "gut_health",
    "immunity": "immunity",
    "energy": "energy",
}

_RISK_RANK = {
    "low": 0,
    "review_required": 1,
    "medium": 2,
    "high": 3,
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

_IBS_AVOID_WORDING = [
    "IBS",
    "Treats IBS",
    "Cures IBS",
    "Relieves IBS symptoms",
    "Reduces IBS symptoms",
]

_REPAIR_HEALING_AVOID_WORDING = [
    "Repairs collagen",
    "Heals joints",
    "Heals skin",
    "Repairs muscle damage",
    "Speeds injury recovery",
    "Repairs body tissue",
    "Rebuilds cartilage",
    "Reverses ageing",
    "Repairs wrinkles",
    "Pain relief",
    "Anti-inflammatory",
]

_GENERIC_THERAPEUTIC_AVOID_WORDING = [
    "Treats disease",
    "Prevents disease",
    "Clinically treats symptoms",
    "Therapeutic support",
]

_IMMUNITY_WORDING_TO_AVOID = [
    "Improves immunity",
    "Boosts immunity",
    "Strengthens immunity",
    "Prevents colds",
    "Prevents flu",
    "Fights infection",
    "Immune defence against illness",
    "Clinically improves immune response",
]

_IMMUNITY_SAFER_WORDING = [
    "Supports normal immune function",
    "Supports immune health",
    "Contains nutrients that support normal immune function",
    "Supports general wellbeing",
]

_IMMUNITY_RECOMMENDED_ACTION = (
    "Use softer wording such as 'supports normal immune function' only where "
    "the product has a relevant vitamin, mineral, or ingredient basis and "
    "substantiation. Avoid 'improves', 'boosts', or disease-prevention wording."
)

_CONDITIONAL_ROUTE_NOTE = (
    "These routes are conditional. They depend on the actual formulation, "
    "amount per serve, label context, and substantiation."
)

_GUT_HEALTH_ROUTE_NAMES_BY_FOOD_TYPE = {
    "yoghurt": {"Live cultures pathway", "Fermented food pathway"},
    "beverage": {"Live cultures pathway", "Fibre pathway", "Fermented food pathway"},
    "snack": {"Fibre pathway"},
    "cereal": {"Fibre pathway"},
    "plant_based": {"Fibre pathway", "Fermented food pathway"},
}

_FAMILY_GUIDANCE: dict[str, dict[str, Any]] = {
    "muscle_performance": {
        "risk_level": "medium",
        "claim_type": "general_health_or_function_claim",
        "recommended_pathways": [
            {
                "name": "Protein / muscle function route",
                "description": "May be supportable where protein or relevant minerals are present at meaningful levels and the wording stays about normal muscle function or maintenance.",
                "requirements": [
                    "Protein, magnesium, calcium, or electrolyte basis identified",
                    "Amount per serve declared or substantiated",
                    "No injury recovery, repair, anabolic, or rapid muscle-gain implication",
                ],
            }
        ],
        "wording_to_avoid": ["Repairs muscle damage", "Speeds injury recovery", "Builds muscle fast", "Anabolic", "Rapid muscle gain"],
        "safer_wording": ["Supports muscle function", "Supports active lifestyles", "Protein contributes to muscle maintenance"],
        "evidence_requirements": ["Protein or mineral amount per serve", "Nutrition information panel", "Target consumer and use context"],
        "recommended_action": "Check the nutrient basis and avoid injury recovery, repair, anabolic, or rapid muscle-gain wording.",
    },
    "collagen_skin": {
        "risk_level": "medium",
        "claim_type": "general_health_or_function_claim",
        "recommended_pathways": [
            {
                "name": "Collagen / skin support route",
                "description": "May be possible where the product has a relevant collagen, vitamin C, protein, or nutrient basis and the claim stays about normal structure or support.",
                "requirements": [
                    "Relevant ingredient or nutrient basis identified",
                    "Amount per serve declared or substantiated",
                    "No repair, healing, anti-ageing reversal, or treatment implication",
                ],
            }
        ],
        "wording_to_avoid": ["Repairs collagen", "Heals skin", "Reverses ageing", "Repairs wrinkles", "Heals joints"],
        "safer_wording": ["Supports collagen formation", "Supports skin structure", "Contains nutrients that support normal skin health"],
        "evidence_requirements": ["Collagen/nutrient amount per serve", "Ingredient form and source", "Substantiation for skin/collagen wording"],
        "recommended_action": "Use support-style wording only where ingredient levels and substantiation fit. Avoid repair, healing, or reversal language.",
    },
    "joint_bone": {
        "risk_level": "medium",
        "claim_type": "general_health_or_function_claim",
        "recommended_pathways": [
            {
                "name": "Joint / bone support route",
                "description": "May be possible for normal joint or bone support where calcium, vitamin D, protein, or another relevant basis is present.",
                "requirements": [
                    "Relevant nutrient or ingredient present at meaningful level",
                    "Amount per serve declared or substantiated",
                    "No arthritis, pain relief, cartilage repair, or injury-treatment implication",
                ],
            }
        ],
        "wording_to_avoid": ["Treats arthritis", "Pain relief", "Rebuilds cartilage", "Repairs joints", "Prevents bone loss"],
        "safer_wording": ["Supports joint health", "Supports bone health", "Contains calcium to support normal bones"],
        "evidence_requirements": ["Calcium/vitamin D/protein or ingredient levels", "Nutrition information panel", "Target consumer context"],
        "recommended_action": "Keep wording to normal joint or bone support and avoid pain, arthritis, cartilage repair, or treatment implications.",
    },
    "energy": {
        "risk_level": "medium",
        "claim_type": "general_health_or_function_claim",
        "recommended_pathways": [
            {
                "name": "Energy metabolism route",
                "description": "May be possible where B vitamins, iron, magnesium, carbohydrate, or another relevant basis supports normal energy metabolism.",
                "requirements": [
                    "Relevant nutrient or ingredient basis identified",
                    "Amount per serve declared or substantiated",
                    "No disease, chronic fatigue, stimulant, or exaggerated energy promise",
                ],
            }
        ],
        "wording_to_avoid": ["Cures tiredness", "Treats fatigue", "Boosts energy instantly", "Stimulant effect", "Fixes low energy"],
        "safer_wording": ["Supports normal energy metabolism", "Supports active lifestyles", "Contains nutrients that support energy metabolism"],
        "evidence_requirements": ["B vitamin/iron/magnesium/carbohydrate basis", "Amount per serve", "Nutrition information panel"],
        "recommended_action": "Check the nutrient basis and soften energy wording. Avoid disease-like fatigue treatment or exaggerated stimulation claims.",
    },
    "weight_satiety": {
        "risk_level": "medium",
        "claim_type": "general_health_or_function_claim",
        "recommended_pathways": [
            {
                "name": "Satiety / weight management route",
                "description": "May be possible where protein, fibre, portion format, or product context supports general satiety or weight-management wording.",
                "requirements": [
                    "Protein, fibre, energy, or satiety basis identified",
                    "Serving size and nutrition panel available",
                    "No rapid fat loss, medical weight-loss, or treatment implication",
                ],
            }
        ],
        "wording_to_avoid": ["Burns fat fast", "Rapid weight loss", "Melts fat", "Treats obesity", "Appetite suppressant"],
        "safer_wording": ["Supports weight management", "Helps you feel fuller for longer", "A source of fibre/protein"],
        "evidence_requirements": ["Energy, protein, fibre and serving size", "Reference product/context if comparative", "Substantiation for satiety wording"],
        "recommended_action": "Keep weight wording general and evidence-based. Avoid rapid fat-loss or medical weight-treatment claims.",
    },
    "brain_focus_mood": {
        "risk_level": "review_required",
        "claim_type": "general_health_or_function_claim",
        "recommended_pathways": [
            {
                "name": "Focus / mood support route",
                "description": "May require careful review where caffeine, B vitamins, magnesium, or other ingredients are used to support general focus or wellbeing.",
                "requirements": [
                    "Relevant ingredient or nutrient basis identified",
                    "Amount per serve and consumer context known",
                    "No anxiety, depression, clinical mood, or cognitive treatment implication",
                ],
            }
        ],
        "wording_to_avoid": ["Treats anxiety", "Treats depression", "Clinically improves mood", "Fixes brain fog", "ADHD support"],
        "safer_wording": ["Supports focus", "Supports mental performance", "Supports general wellbeing"],
        "evidence_requirements": ["Ingredient/nutrient basis and amount", "Caffeine level if relevant", "Target consumer context"],
        "recommended_action": "Review cognitive and mood wording carefully. Avoid mental-health, clinical, or treatment implications.",
    },
    "sleep_calm": {
        "risk_level": "review_required",
        "claim_type": "general_health_or_function_claim",
        "recommended_pathways": [
            {
                "name": "Sleep / calm support route",
                "description": "May require careful review where ingredients are positioned for general relaxation or calm rather than treating sleep disorders.",
                "requirements": [
                    "Ingredient basis and amount per serve identified",
                    "Consumer context and timing of use known",
                    "No insomnia, sedative, treatment, or clinical sleep implication",
                ],
            }
        ],
        "wording_to_avoid": ["Treats insomnia", "Cures sleeplessness", "Sedative effect", "Clinically improves sleep", "Stops anxiety"],
        "safer_wording": ["Supports relaxation", "Supports calm", "Part of an evening routine"],
        "evidence_requirements": ["Ingredient basis and amount", "Serving size", "Substantiation for sleep/calm wording"],
        "recommended_action": "Use cautious relaxation-style wording and avoid insomnia, sedative, anxiety, or treatment implications.",
    },
    "heart_cholesterol": {
        "risk_level": "medium",
        "claim_type": "general_health_or_function_claim",
        "recommended_pathways": [
            {
                "name": "Heart health support route",
                "description": "May be possible where the product has a relevant nutrient or permitted ingredient basis and wording avoids disease treatment.",
                "requirements": [
                    "Relevant omega-3, fibre, plant sterol, nutrient, or ingredient basis identified",
                    "Amount per serve declared or substantiated",
                    "No heart disease, blood pressure treatment, or cholesterol-lowering implication unless specifically reviewed",
                ],
            }
        ],
        "wording_to_avoid": ["Lowers cholesterol", "Prevents heart disease", "Lowers blood pressure", "Unclogs arteries", "Prevents heart attack"],
        "safer_wording": ["Supports heart health", "Contains nutrients that support general wellbeing", "A source of omega-3"],
        "evidence_requirements": ["Relevant nutrient/ingredient level", "Nutrition information panel", "Permitted-claim review if cholesterol wording is intended"],
        "recommended_action": "Review heart wording carefully. Avoid cholesterol, blood pressure, or disease-risk wording unless a specific permitted pathway is confirmed.",
    },
    "blood_sugar": {
        "risk_level": "review_required",
        "claim_type": "general_health_or_function_claim",
        "recommended_pathways": [
            {
                "name": "Blood sugar / glycaemic route",
                "description": "May be possible for low-GI or general healthy blood sugar wording only with strong product-specific nutrition support.",
                "requirements": [
                    "GI, carbohydrate, sugar, fibre, or formulation basis identified",
                    "Nutrition panel and serving size available",
                    "No diabetes, insulin management, or treatment implication",
                ],
            }
        ],
        "wording_to_avoid": ["Controls blood sugar", "Manages diabetes", "Stabilises insulin", "Diabetic approved", "Prevents glucose spikes"],
        "safer_wording": ["Supports healthy blood sugar as part of a balanced diet", "Low GI", "No added sugar"],
        "evidence_requirements": ["Sugars/carbohydrate/fibre levels", "GI data if relevant", "Nutrition information panel"],
        "recommended_action": "Review blood sugar wording conservatively and avoid diabetes or insulin-management implications.",
    },
    "hydration_electrolytes": {
        "risk_level": "review_required",
        "claim_type": "general_health_or_function_claim",
        "recommended_pathways": [
            {
                "name": "Hydration / electrolyte route",
                "description": "May be supportable where electrolyte levels and use context support general hydration wording.",
                "requirements": [
                    "Sodium, potassium, magnesium, or electrolyte levels declared",
                    "Serving size and use context identified",
                    "No medical dehydration or treatment implication",
                ],
            }
        ],
        "wording_to_avoid": ["Treats dehydration", "Medical hydration", "Cures dehydration", "Clinical rehydration"],
        "safer_wording": ["Supports hydration", "Contains electrolytes", "Helps replace electrolytes lost through sweat"],
        "evidence_requirements": ["Electrolyte levels per serve", "Serving size", "Use context such as exercise or general hydration"],
        "recommended_action": "Check electrolyte levels and intended use context. Avoid medical rehydration or dehydration-treatment wording.",
    },
}

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


def _high_risk_regulatory_context(jurisdiction: str, therapeutic_terms: list[str]) -> str:
    terms = {term.lower() for term in therapeutic_terms}
    repair_markers = {
        "heals or repairs body tissue",
        "body tissue repair",
        "cartilage repair",
        "joint repair",
        "collagen repair",
        "repair muscle damage",
        "injury",
    }
    if "ibs" in terms:
        return (
            f"In {jurisdiction}, food claims should not imply treatment, cure, "
            "prevention, or management of diseases or medical conditions. "
            "IBS and similar condition-specific wording should be removed from "
            "food claim copy."
        )
    if terms & repair_markers:
        return (
            f"In {jurisdiction}, food claims should not imply treatment, cure, "
            "prevention, repair, or management of diseases, injuries, symptoms, "
            "or body damage. Repair or healing language should be removed unless "
            "specialist regulatory review confirms it is appropriate."
        )
    return (
        f"In {jurisdiction}, food claims should not imply treatment, cure, "
        "prevention, or management of diseases, medical conditions, or symptoms. "
        "Disease-specific wording should be removed from food claim copy."
    )


def _high_risk_wording_to_avoid(therapeutic_terms: list[str]) -> list[str]:
    terms = {term.lower() for term in therapeutic_terms}
    repair_markers = {
        "heals or repairs body tissue",
        "body tissue repair",
        "cartilage repair",
        "joint repair",
        "collagen repair",
        "repair muscle damage",
        "injury",
        "pain relief",
        "anti-inflammatory",
    }
    avoid: list[str] = list(therapeutic_terms)
    if "ibs" in terms:
        avoid.extend(_IBS_AVOID_WORDING)
    if terms & repair_markers:
        avoid.extend(_REPAIR_HEALING_AVOID_WORDING)
    avoid.extend(_GENERIC_THERAPEUTIC_AVOID_WORDING)
    return list(dict.fromkeys(avoid))


def _detect_themes(normalised: str) -> list[ThemeMatch]:
    matches: list[ThemeMatch] = []
    for key, display, pattern in _THEME_PATTERNS:
        if re.search(pattern, normalised):
            matches.append(ThemeMatch(key=key, display=display))
    return matches


def _has_theme_or_therapeutic_signal(value: str) -> bool:
    normalised = _normalise_text(value)
    return bool(_detect_themes(normalised) or _detect_therapeutic_terms(normalised))


def _split_claim_phrases(claim_text: str) -> list[str]:
    """Split free-text copy into reviewable claim phrases."""
    raw = (claim_text or "").strip()
    if not raw:
        return []

    primary_parts = [
        part.strip(" \t\r\n,")
        for part in re.split(r"[.;\n]+", raw)
        if part.strip(" \t\r\n,")
    ]
    expanded: list[str] = []
    benefit_tail = (
        r"immunity|immune\s+support|immune\s+health|gut\s+health|digestive\s+health|"
        r"digestive\s+wellbeing|active\s+lifestyle|energy|fatigue|tiredness|"
        r"muscle\s+strength|muscle\s+function|muscle\s+recovery|collagen|"
        r"collagen\s+formation|skin\s+structure|joint\s+health|joints?|bone\s+health|"
        r"hydration|electrolytes?|focus|mood|sleep|calm|heart\s+health|"
        r"blood\s+sugar|healthy\s+blood\s+sugar|weight\s+management|satiety"
    )
    support_verb = r"supports?|helps?|helps\s+support|promotes|maintains|improves?|boosts?|strengthens?|reduces?|repairs?|heals?|rebuilds?"
    for part in primary_parts:
        comma_parts = [
            item.strip(" \t\r\n,")
            for item in re.split(r",\s+", part)
            if item.strip(" \t\r\n,")
        ]
        if len(comma_parts) > 1 and sum(_has_theme_or_therapeutic_signal(item) for item in comma_parts) > 1:
            candidates = comma_parts
        else:
            candidates = [part]

        for candidate in candidates:
            match = re.match(
                rf"^(?P<verb>{support_verb})\s+(?P<first>.+?)\s+and\s+(?P<second>{benefit_tail})$",
                candidate,
                flags=re.IGNORECASE,
            )
            if match:
                verb = match.group("verb")
                expanded.append(f"{verb} {match.group('first').strip()}")
                expanded.append(f"{verb} {match.group('second').strip()}")
                continue

            joined_benefit = re.match(
                rf"^(?P<first>.+?)\s+and\s+(?P<verb>{support_verb})\s+(?P<second>{benefit_tail})$",
                candidate,
                flags=re.IGNORECASE,
            )
            if joined_benefit:
                first = joined_benefit.group("first").strip()
                second = f"{joined_benefit.group('verb')} {joined_benefit.group('second').strip()}"
                if _has_theme_or_therapeutic_signal(first) and _has_theme_or_therapeutic_signal(second):
                    expanded.append(first)
                    expanded.append(second)
                    continue

            benefit_and_benefit = re.match(
                rf"^(?P<first>.+?)\s+and\s+(?P<second>{benefit_tail})$",
                candidate,
                flags=re.IGNORECASE,
            )
            if benefit_and_benefit:
                first = benefit_and_benefit.group("first").strip()
                second = benefit_and_benefit.group("second").strip()
                if _has_theme_or_therapeutic_signal(first) and _has_theme_or_therapeutic_signal(second):
                    expanded.append(first)
                    expanded.append(second)
                    continue
            expanded.append(candidate)

    deduped: list[str] = []
    seen: set[str] = set()
    for phrase in expanded:
        clean = re.sub(r"\s+", " ", phrase).strip()
        key = clean.lower()
        if clean and key not in seen:
            seen.add(key)
            deduped.append(clean)
    return deduped


def _breakdown_for_phrase(phrase: str) -> dict[str, Any]:
    normalised = _normalise_text(phrase)
    therapeutic_terms = _detect_therapeutic_terms(normalised)
    theme_matches = _detect_themes(normalised)
    themes = [match.key for match in theme_matches]
    if therapeutic_terms:
        return {
            "claim": phrase,
            "risk_level": "high",
            "claim_type": "therapeutic_or_disease_related_claim",
            "matched_themes": themes,
            "recommended_action": (
                "Remove disease-specific or therapeutic wording and reframe as "
                "general wellbeing or nutrition-content wording only where "
                "supportable."
            ),
        }

    if "high_protein" in themes:
        pathway = get_claim_pathway("high_protein") or {}
        return {
            "claim": phrase,
            "risk_level": "medium",
            "claim_type": "nutrition_content_claim",
            "matched_themes": themes,
            "recommended_action": pathway.get(
                "recommended_action",
                "Check protein amount, source, nutrition panel declaration, and substantiation before use.",
            ),
        }

    if "immunity" in themes:
        return {
            "claim": phrase,
            "risk_level": "medium" if re.search(r"\b(improv(?:e|es|ing)|boosts?|strengthens?)\b", normalised) else "review_required",
            "claim_type": "general_health_or_function_claim",
            "matched_themes": themes,
            "recommended_action": _IMMUNITY_RECOMMENDED_ACTION,
        }

    if "gut_health" in themes:
        return {
            "claim": phrase,
            "risk_level": "review_required",
            "claim_type": "general_health_or_function_claim",
            "matched_themes": themes,
            "recommended_action": (
                "Review product formulation and substantiation. Avoid "
                "disease-specific digestive claims."
            ),
        }

    for theme in themes:
        guidance = _FAMILY_GUIDANCE.get(theme)
        if guidance:
            return {
                "claim": phrase,
                "risk_level": guidance["risk_level"],
                "claim_type": guidance["claim_type"],
                "matched_themes": themes,
                "recommended_action": guidance["recommended_action"],
            }

    if themes:
        return {
            "claim": phrase,
            "risk_level": "review_required",
            "claim_type": "general_health_or_function_claim",
            "matched_themes": themes,
            "recommended_action": (
                "Review product formulation, label context, and substantiation "
                "before using this consumer-facing claim."
            ),
        }

    return {
        "claim": phrase,
        "risk_level": "review_required",
        "claim_type": "unclassified_food_claim",
        "matched_themes": [],
        "recommended_action": (
            "Review exact wording, product context, and substantiation before use."
        ),
    }


def _build_claim_breakdown(claim_text: str) -> tuple[bool, list[dict[str, Any]], str]:
    phrases = _split_claim_phrases(claim_text)
    if not phrases and claim_text.strip():
        phrases = [claim_text.strip()]
    breakdown = [_breakdown_for_phrase(phrase) for phrase in phrases]
    multi_claim = len(breakdown) > 1
    note = (
        "Multiple claims were detected. Review each claim separately because each "
        "may require different substantiation."
        if multi_claim
        else ""
    )
    return multi_claim, breakdown, note


def _highest_breakdown_risk(breakdown: list[dict[str, Any]]) -> str | None:
    if not breakdown:
        return None
    return max(
        (str(item.get("risk_level") or "review_required") for item in breakdown),
        key=lambda risk: _RISK_RANK.get(risk, _RISK_RANK["review_required"]),
    )


def _apply_claim_breakdown(response: dict[str, Any], claim_text: str) -> dict[str, Any]:
    multi_claim, breakdown, note = _build_claim_breakdown(claim_text)
    adjusted = dict(response)
    adjusted["multi_claim"] = multi_claim
    adjusted["claim_breakdown"] = breakdown
    adjusted["overall_note"] = note
    highest_risk = _highest_breakdown_risk(breakdown)
    if highest_risk and _RISK_RANK.get(highest_risk, 1) > _RISK_RANK.get(str(adjusted.get("risk_level")), 1):
        adjusted["risk_level"] = highest_risk
    if multi_claim and highest_risk == "high":
        adjusted["risk_level"] = "high"
    return adjusted


def _merge_unique(existing: list[Any], additions: list[Any]) -> list[Any]:
    merged: list[Any] = []
    seen: set[str] = set()
    for item in [*existing, *additions]:
        key = str(item).strip().lower()
        if key and key not in seen:
            seen.add(key)
            merged.append(item)
    return merged


def _append_pathway_if_missing(existing: list[dict[str, Any]], pathway: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not pathway:
        return existing
    out = list(existing)
    seen = {str(item.get("name", "")).strip().lower() for item in out if isinstance(item, dict)}
    for item in pathway.get("recommended_pathways", []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip().lower()
        if name and name not in seen:
            seen.add(name)
            out.append(item)
    return out


def _dedupe_routes(routes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for route in routes:
        if not isinstance(route, dict):
            continue
        name = str(route.get("name", "")).strip()
        if not name or name.lower() in seen:
            continue
        seen.add(name.lower())
        out.append(route)
    return out


def _routes_from_pathway(pathway: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not pathway:
        return []
    return [route for route in pathway.get("recommended_pathways", []) if isinstance(route, dict)]


def _split_main_and_supporting_routes(
    pathway: dict[str, Any] | None,
    main_names: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    main: list[dict[str, Any]] = []
    supporting: list[dict[str, Any]] = []
    for route in _routes_from_pathway(pathway):
        if route.get("name") in main_names:
            main.append(route)
        else:
            supporting.append(route)
    return _dedupe_routes(main), _dedupe_routes(supporting)


def _filter_gut_routes_for_context(
    routes: list[dict[str, Any]],
    *,
    food_type: str = "",
    claim_text: str = "",
) -> list[dict[str, Any]]:
    if not routes:
        return []
    normalised_claim = _normalise_text(claim_text)
    allowed_names = set(_GUT_HEALTH_ROUTE_NAMES_BY_FOOD_TYPE.get(food_type, set()))
    if re.search(r"\b(live\s+cultures?|probiotic|yogh?urt|kefir|fermented|cultured)\b", normalised_claim):
        allowed_names.update({"Live cultures pathway", "Fermented food pathway"})
    if re.search(r"\b(fibre|fiber|prebiotic|inulin|wholegrain)\b", normalised_claim):
        allowed_names.add("Fibre pathway")
    if not allowed_names:
        return []
    return [route for route in routes if route.get("name") in allowed_names]


def _append_supporting_routes_note(response: dict[str, Any]) -> dict[str, Any]:
    adjusted = dict(response)
    routes = list(adjusted.get("possible_supporting_routes") or [])
    adjusted["possible_supporting_routes"] = _dedupe_routes(routes)
    if adjusted["possible_supporting_routes"]:
        note = adjusted.get("overall_note") or ""
        if _CONDITIONAL_ROUTE_NOTE not in note:
            adjusted["overall_note"] = f"{note} {_CONDITIONAL_ROUTE_NOTE}".strip()
    return adjusted


def _enrich_response_for_detected_themes(
    response: dict[str, Any],
    *,
    context: dict[str, Any] | None = None,
    claim_text: str = "",
) -> dict[str, Any]:
    adjusted = dict(response)
    context = context or {}
    themes = set(adjusted.get("matched_themes") or [])
    for item in adjusted.get("claim_breakdown") or []:
        themes.update(item.get("matched_themes") or [])

    supporting_routes = list(adjusted.get("possible_supporting_routes") or [])

    if "immunity" in themes:
        immunity = get_claim_pathway("immunity")
        adjusted["wording_to_avoid"] = _merge_unique(
            list(adjusted.get("wording_to_avoid") or []),
            _IMMUNITY_WORDING_TO_AVOID,
        )
        adjusted["safer_wording"] = _merge_unique(
            list(adjusted.get("safer_wording") or []),
            _IMMUNITY_SAFER_WORDING,
        )
        main_routes, conditional_routes = _split_main_and_supporting_routes(
            immunity,
            {"Immunity support route"},
        )
        adjusted["recommended_pathways"] = _dedupe_routes(
            [*list(adjusted.get("recommended_pathways") or []), *main_routes]
        )
        supporting_routes.extend(conditional_routes)
        if adjusted.get("claim_type") == "unclassified_food_claim":
            adjusted["claim_type"] = "general_health_or_function_claim"

    if "gut_health" in themes:
        gut_health = get_claim_pathway("gut_health")
        gut_routes = _filter_gut_routes_for_context(
            _routes_from_pathway(gut_health),
            food_type=str(context.get("food_type") or ""),
            claim_text=claim_text,
        )
        supporting_routes.extend(gut_routes)

    family_themes = [theme for theme in themes if theme in _FAMILY_GUIDANCE]
    for theme in family_themes:
        guidance = _FAMILY_GUIDANCE[theme]
        adjusted["wording_to_avoid"] = _merge_unique(
            list(adjusted.get("wording_to_avoid") or []),
            list(guidance.get("wording_to_avoid") or []),
        )
        adjusted["safer_wording"] = _merge_unique(
            list(adjusted.get("safer_wording") or []),
            list(guidance.get("safer_wording") or []),
        )
        adjusted["evidence_requirements"] = _merge_unique(
            list(adjusted.get("evidence_requirements") or []),
            list(guidance.get("evidence_requirements") or []),
        )
        adjusted["recommended_pathways"] = _dedupe_routes(
            [
                *list(adjusted.get("recommended_pathways") or []),
                *list(guidance.get("recommended_pathways") or []),
            ]
        )
        if adjusted.get("claim_type") == "unclassified_food_claim":
            adjusted["claim_type"] = guidance["claim_type"]
        if adjusted.get("risk_level") == "review_required" and guidance.get("risk_level") == "medium":
            adjusted["risk_level"] = "medium"
        if not adjusted.get("recommended_action") or adjusted.get("recommended_action", "").startswith("Collect the missing"):
            adjusted["recommended_action"] = guidance["recommended_action"]

    adjusted["possible_supporting_routes"] = supporting_routes
    return _append_supporting_routes_note(adjusted)


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
        "possible_supporting_routes": [],
        "wording_to_avoid": [],
        "missing_information": list(_UNCLASSIFIED_MISSING_INFO),
        "safer_wording": [],
        "evidence_requirements": [],
        "recommended_action": (
            "Collect the missing product and substantiation details, then review "
            "the claim before using it in customer-facing materials."
        ),
        "matched_themes": [],
        "multi_claim": False,
        "claim_breakdown": [],
        "overall_note": "",
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
        avoid = _high_risk_wording_to_avoid(therapeutic_terms)
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
            "regulatory_context": _high_risk_regulatory_context(jurisdiction, therapeutic_terms),
            "recommended_pathways": [],
            "possible_supporting_routes": [],
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
            "multi_claim": False,
            "claim_breakdown": [],
            "overall_note": "",
            "disclaimer": DISCLAIMER,
            "ai_used": False,
        }
        response = _apply_claim_breakdown(response, raw_claim)
        response = _enrich_response_for_detected_themes(response, context=context, claim_text=raw_claim)
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
        response = _apply_claim_breakdown(response, raw_claim)
        response = _enrich_response_for_detected_themes(response, context=context, claim_text=raw_claim)
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
        response = _apply_claim_breakdown(response, raw_claim)
        response = _enrich_response_for_detected_themes(response, context=context, claim_text=raw_claim)
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
        "possible_supporting_routes": [],
        "wording_to_avoid": pathway["wording_to_avoid"],
        "missing_information": pathway["missing_information"],
        "safer_wording": pathway["safer_wording"],
        "evidence_requirements": pathway["evidence_requirements"],
        "recommended_action": pathway["recommended_action"],
        "matched_themes": [match.key for match in theme_matches],
        "multi_claim": False,
        "claim_breakdown": [],
        "overall_note": "",
        "disclaimer": DISCLAIMER,
        "ai_used": False,
    }
    response = _apply_claim_breakdown(response, raw_claim)
    response = _enrich_response_for_detected_themes(response, context=context, claim_text=raw_claim)
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
