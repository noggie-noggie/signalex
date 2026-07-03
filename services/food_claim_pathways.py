"""
services/food_claim_pathways.py — deterministic food claim pathway cards.

This service presents static claim pathway data in a frontend-card-friendly
shape.  It does not call AI and does not mutate the source JSON.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

DATA_PATH = Path(__file__).parent.parent / "data_static" / "food_claim_pathways.json"
DISCLAIMER = "Not a legal or final substantiation assessment."

_ALIASES = {
    "highprotein": "high_protein",
    "highinprotein": "high_protein",
    "protein": "high_protein",
}

_DISPLAY_NAMES = {
    "gut_health": "Gut health",
    "immunity": "Immunity",
    "energy": "Energy",
    "muscle_recovery": "Muscle recovery",
    "hydration": "Hydration",
    "bone_health": "Bone health",
    "antioxidant": "Antioxidant",
    "heart_health": "Heart health",
    "low_sugar": "Low sugar",
    "high_protein": "High in protein",
}

_REGULATORY_CONTEXT = {
    "high_protein": (
        "Protein claims generally require nutrition-content substantiation and "
        "careful wording under Australian food standards. Muscle maintenance or "
        "growth wording should be supported by product-specific nutrition data."
    )
}

_RECOMMENDED_ACTION = {
    "high_protein": (
        "Confirm protein quantity, source, amino acid quality, nutrition panel "
        "declaration, and exact claim wording before using the claim."
    )
}

_HIGH_PROTEIN_CARD = {
    "claim": "high_protein",
    "display_claim": "High in protein",
    "risk_level": "medium",
    "claim_type": "nutrition_content_claim",
    "regulatory_context": _REGULATORY_CONTEXT["high_protein"],
    "recommended_pathways": [
        {
            "name": "Protein route",
            "description": "Protein contributes to the maintenance and growth of muscle mass.",
            "requirements": [
                "Minimum protein per serve for meaningful claim",
                "Complete amino acid profile or declared essential amino acids",
                "High-quality protein source",
                "Declared on nutrition information panel",
            ],
        }
    ],
    "wording_to_avoid": [
        "Repairs muscle damage",
        "Treats injury",
        "Speeds injury recovery",
        "Prevents muscle loss",
        "Builds muscle mass fast",
        "Anabolic",
        "Rapid muscle gain",
    ],
    "missing_information": [
        "Protein content and source per serve",
        "Amino acid profile or essential amino acids",
        "Magnesium content and form",
        "Electrolyte levels",
        "Target consumer and population",
        "Exact proposed wording on the claim",
        "Full ingredient list",
        "Amount per serve for key active ingredients",
    ],
    "safer_wording": [
        "High in protein",
        "Supports active lifestyles",
        "Protein contributes to muscle maintenance",
    ],
    "evidence_requirements": [
        "Protein content per serve",
        "Protein source and quality",
        "Amino acid profile or essential amino acids",
        "Nutrition information panel declaration",
        "Substantiation for any muscle maintenance or growth wording",
    ],
    "recommended_action": _RECOMMENDED_ACTION["high_protein"],
    "disclaimer": DISCLAIMER,
}


def normalize_claim_key(value: str | None) -> str:
    """Normalize claim aliases across case, spaces, hyphens, and underscores."""
    text = (value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    compact = text.replace("_", "")
    return _ALIASES.get(compact, text)


@lru_cache(maxsize=1)
def _load_data() -> dict[str, Any]:
    try:
        return json.loads(DATA_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"themes": {}}


def _normalise_pathway(theme: str, data: dict[str, Any]) -> dict[str, Any]:
    if theme == "high_protein":
        return dict(_HIGH_PROTEIN_CARD)

    pathways = [
        {
            "name": pathway.get("name", ""),
            "description": pathway.get("description", ""),
            "requirements": pathway.get("requirements", []),
        }
        for pathway in data.get("pathways", [])
    ]

    evidence_requirements: list[str] = []
    for pathway in pathways:
        evidence_requirements.extend(pathway.get("requirements", []))

    display_claim = _DISPLAY_NAMES.get(theme, theme.replace("_", " ").title())
    return {
        "claim": theme,
        "display_claim": display_claim,
        "risk_level": data.get("default_risk_level", "medium"),
        "claim_type": data.get("claim_type", "health_claim"),
        "regulatory_context": _REGULATORY_CONTEXT.get(
            theme,
            "Review the proposed wording, product format, ingredient levels, and substantiation before use.",
        ),
        "recommended_pathways": pathways,
        "wording_to_avoid": data.get("avoid_wording", []),
        "missing_information": data.get("information_needed", []),
        "safer_wording": data.get("safer_wording", []),
        "evidence_requirements": list(dict.fromkeys(evidence_requirements)),
        "recommended_action": _RECOMMENDED_ACTION.get(
            theme,
            "Review this pathway against the exact product formulation, label context, and proposed claim wording.",
        ),
        "disclaimer": DISCLAIMER,
    }


def list_claim_pathways() -> list[dict[str, Any]]:
    """Return all known claim pathway cards."""
    themes = _load_data().get("themes", {})
    return [_normalise_pathway(theme, data) for theme, data in themes.items()]


def get_claim_pathway(claim: str) -> dict[str, Any] | None:
    """Return one claim pathway card, or None when no pathway is known."""
    key = normalize_claim_key(claim)
    themes = _load_data().get("themes", {})
    if key not in themes and key != "high_protein":
        return None
    return _normalise_pathway(key, themes.get(key, {}))
