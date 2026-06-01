"""
services/food_claims/pathways.py — Pathway and ingredient lookup for food claims.

No AI. No external calls. Pure in-memory lookup from
data_static/food_claim_pathways.json.

Public API
----------
get_claim_pathways(theme: str, food_type: str) -> dict
    Returns pathways, ingredients, safer/avoid wording, missing info,
    next questions, and a food_type_fit assessment.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

_DATA_PATH = Path(__file__).parent.parent.parent / "data_static" / "food_claim_pathways.json"

# ---------------------------------------------------------------------------
# food_type_fit rules  (theme -> list of (keyword_fragments, fit_level))
# Checked in order; first match wins. Falls through to "medium" default.
# ---------------------------------------------------------------------------
_FIT_RULES: dict[str, list[tuple[list[str], str]]] = {
    "gut_health": [
        (["yoghurt", "yogurt", "kefir", "cultured", "fermented", "probiotic drink"], "high"),
        (["drink", "beverage", "shot"], "medium"),
        (["bar", "snack", "biscuit", "chip"], "medium"),
        (["supplement", "capsule", "tablet", "powder"], "medium"),
    ],
    "immunity": [
        (["drink", "juice", "smoothie", "shot"], "high"),
        (["yoghurt", "yogurt"], "medium"),
        (["supplement", "capsule", "tablet", "powder"], "high"),
        (["bar", "snack"], "medium"),
    ],
    "energy": [
        (["drink", "energy drink", "beverage", "shot"], "high"),
        (["bar", "snack", "cereal", "oat"], "high"),
        (["supplement", "capsule", "tablet"], "medium"),
    ],
    "muscle_recovery": [
        (["protein bar", "protein shake", "protein drink", "protein powder"], "high"),
        (["yoghurt", "yogurt", "dairy"], "high"),
        (["bar", "snack"], "medium"),
        (["drink", "beverage"], "medium"),
        (["supplement", "capsule", "tablet", "powder"], "high"),
    ],
    "hydration": [
        (["drink", "beverage", "sports drink", "electrolyte", "water", "coconut water"], "high"),
        (["tablet", "powder", "sachet"], "high"),
        (["bar", "snack", "food"], "low"),
    ],
    "bone_health": [
        (["milk", "dairy", "yoghurt", "yogurt", "cheese"], "high"),
        (["plant-based milk", "almond milk", "oat milk", "soy milk"], "high"),
        (["supplement", "capsule", "tablet", "powder"], "high"),
        (["bar", "snack", "cereal"], "medium"),
    ],
    "antioxidant": [
        (["juice", "drink", "smoothie", "shot", "tea", "coffee"], "high"),
        (["bar", "snack", "chocolate"], "medium"),
        (["supplement", "capsule", "tablet", "powder"], "high"),
    ],
    "heart_health": [
        (["spread", "margarine"], "high"),
        (["oat", "oats", "porridge", "muesli", "cereal"], "high"),
        (["milk", "dairy", "yoghurt", "yogurt"], "medium"),
        (["supplement", "capsule", "fish oil", "omega"], "high"),
        (["bar", "snack"], "medium"),
    ],
    "low_sugar": [
        # low_sugar applies to almost any packaged food
        (["drink", "beverage", "juice", "yoghurt", "yogurt", "bar",
          "snack", "cereal", "sauce", "condiment", "dairy", "dessert"], "high"),
    ],
    "high_protein": [
        (["protein bar", "protein shake", "protein drink", "protein powder"], "high"),
        (["yoghurt", "yogurt", "greek yoghurt", "dairy"], "high"),
        (["bar", "snack", "cereal"], "medium"),
        (["meat", "fish", "egg", "legume", "bean"], "high"),
        (["drink", "beverage", "smoothie"], "medium"),
    ],
}

_DEFAULT_FIT = "medium"


def _load_data() -> dict:
    """Load pathway JSON. Returns empty dict on failure."""
    try:
        return json.loads(_DATA_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _assess_food_type_fit(theme: Optional[str], food_type: str) -> str:
    """
    Return 'low', 'medium', or 'high' based on how well food_type suits the theme.

    Logic is purely rule-based — see _FIT_RULES above.
    """
    if not theme or not food_type:
        return _DEFAULT_FIT

    ft_lower = food_type.lower().strip()
    rules    = _FIT_RULES.get(theme, [])

    for keywords, fit_level in rules:
        if any(kw in ft_lower for kw in keywords):
            return fit_level

    return _DEFAULT_FIT


def get_claim_pathways(theme: Optional[str], food_type: str) -> dict:
    """
    Return pathway guidance for a given theme and food_type.

    Parameters
    ----------
    theme     : str | None  — theme name from classifier, e.g. "gut_health"
    food_type : str         — user-supplied food type, e.g. "yoghurt drink"

    Returns
    -------
    dict with keys:
        food_type_fit         str          — "low", "medium", "high"
        claim_pathways        list[dict]   — pathway objects (name, description, requirements)
        possible_ingredients  list[str]
        safer_wording         list[str]
        avoid_wording         list[str]
        missing_information   list[str]
        next_questions        list[str]
    """
    data       = _load_data()
    themes     = data.get("themes", {})
    theme_data = themes.get(theme) if theme else None

    food_type_fit = _assess_food_type_fit(theme, food_type)

    if not theme_data:
        return {
            "food_type_fit":        _DEFAULT_FIT,
            "claim_pathways":       [],
            "possible_ingredients": [],
            "safer_wording":        [],
            "avoid_wording":        [],
            "missing_information":  [
                "Could not identify a recognised claim theme. Please provide more detail."
            ],
            "next_questions": [
                "What is the primary health benefit the product is intended to support?",
                "What key ingredients are in the product?",
            ],
        }

    # Filter pathways to ones suitable for this food_type (if suitable_food_types is set)
    raw_pathways: list[dict] = theme_data.get("pathways", [])
    ft_lower = food_type.lower().strip()

    suitable_pathways: list[dict] = []
    for pw in raw_pathways:
        suitable = pw.get("suitable_food_types", [])
        # Include if no filter set, or food_type matches at least one suitable type
        if not suitable or any(s in ft_lower for s in suitable):
            suitable_pathways.append({
                "name":         pw.get("name", ""),
                "description":  pw.get("description", ""),
                "requirements": pw.get("requirements", []),
            })

    # If no pathways matched the food_type, fall back to all pathways
    if not suitable_pathways:
        suitable_pathways = [
            {
                "name":         pw.get("name", ""),
                "description":  pw.get("description", ""),
                "requirements": pw.get("requirements", []),
            }
            for pw in raw_pathways
        ]

    return {
        "food_type_fit":        food_type_fit,
        "claim_pathways":       suitable_pathways,
        "possible_ingredients": theme_data.get("possible_ingredients", []),
        "safer_wording":        theme_data.get("safer_wording", []),
        "avoid_wording":        theme_data.get("avoid_wording", []),
        "missing_information":  theme_data.get("information_needed", []),
        "next_questions":       theme_data.get("next_questions", []),
    }
