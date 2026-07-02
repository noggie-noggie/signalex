"""
services/food_taxonomy.py — deterministic Food signal taxonomy enrichment.

This module does not write to the database.  It converts existing Food rows into
frontend-safe taxonomy fields for dashboard filtering and onboarding relevance.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from services.food_supplement_filter import is_supplement_like_food


SIGNAL_TYPES = {
    "recall",
    "safety_alert",
    "regulatory_update",
    "consultation",
    "claim_risk",
    "labelling_issue",
    "ingredient_watch",
    "product_launch",
    "category_trend",
    "market_opportunity",
    "evidence_signal",
}

DASHBOARD_SECTIONS = {
    "recalls_safety",
    "regulatory_updates",
    "claims_labelling",
    "ingredient_watch",
    "category_signals",
    "market_opportunities",
}


_RECALL_TERMS = [
    "recall",
    "fsanz recall",
    "undeclared",
    "allergen",
    "contamination",
    "foreign matter",
    "foreign object",
    "microbial",
    "salmonella",
    "listeria",
    "e. coli",
    "incorrect label",
    "mould",
    "hepatitis",
    "toxin contamination",
]

_REGULATORY_TERMS = [
    "consultation",
    "standard",
    "food code",
    "amendment",
    "proposal",
    "application",
    "gazette",
    "gazettal",
    "regulation",
    "permitted",
    "maximum level",
    "labelling requirement",
    "call for comment",
    "approved changes",
]

_CLAIM_TERMS = [
    "claim",
    "health claim",
    "nutrition claim",
    "therapeutic",
    "disease",
    "supports",
    "boosts",
    "improves",
    "gut health",
    "immunity",
    "energy",
    "low sugar",
    "high protein",
]

_HIGH_RISK_DISEASE_TERMS = [
    "treats",
    "prevents",
    "cures",
    "ibs",
    "arthritis",
    "diabetes",
    "anxiety",
    "depression",
]

_ISSUE_PATTERNS = [
    ("undeclared_allergen", ["undeclared allergen", "undeclared", "allergen"]),
    ("microbial_contamination", ["microbial", "salmonella", "listeria", "e. coli", "mould", "hepatitis"]),
    ("foreign_matter", ["foreign matter", "foreign object", "glass", "metal", "plastic", "rubber"]),
    ("toxin_contamination", ["toxin", "cereulide"]),
    ("chemical_contamination", ["chemical", "cadmium", "monocrotophos", "prohibited substance"]),
    ("incorrect_labelling", ["incorrect label", "incorrect labelling", "mislabelled", "labelling", "label"]),
    ("composition_standard", ["standard", "composition", "food code", "maximum level", "permitted", "processing aid"]),
    ("claims_wording", ["claim", "health claim", "nutrition claim", "therapeutic", "supports", "boosts", "improves"]),
    ("substantiation", ["substantiation", "evidence", "supporting evidence"]),
    ("novel_ingredient", ["novel", "new source", "human-identical milk oligosaccharides"]),
    ("additive_use", ["additive", "processing aid", "enzyme", "amylase", "dextransucrase", "phospholipase"]),
    ("country_of_origin", ["country of origin"]),
    ("sustainability_claim", ["sustainability", "sustainable"]),
    ("category_growth", ["growth", "trend", "white space", "category"]),
]

_CLAIM_THEME_PATTERNS = [
    ("gut_health", ["gut health", "digest", "digestive", "probiotic", "prebiotic"]),
    ("immunity", ["immunity", "immune", "vitamin c"]),
    ("energy", ["energy", "caffeine", "pre workout", "berocca"]),
    ("hydration", ["hydration", "hydralyte", "electrolyte"]),
    ("high_protein", ["high protein", "protein", "whey", "caseinate"]),
    ("low_sugar", ["low sugar", "no sugar", "no added sugar", "lowcarb"]),
    ("source_of_fibre", ["fibre", "fiber", "inulin", "oat fiber"]),
    ("natural", ["natural", "organic"]),
    ("clean_label", ["clean label", "no additives"]),
    ("plant_based", ["plant based", "vegan", "vegetarian"]),
    ("free_from", ["free from", "gluten-free", "gluten free", "dairy free"]),
    ("kids_nutrition", ["infant", "kids", "children", "young children"]),
    ("sustainability", ["sustainable", "sustainability"]),
]

_PRODUCT_TYPE_PATTERNS = [
    ("kombucha", ["kombucha"]),
    ("fermented_drink", ["kombucha", "kefir"]),
    ("yoghurt_drink", ["yoghurt drink", "yogurt drink", "protein smoothie"]),
    ("protein_bar", ["protein bar", "clif bar", "protein crisp", "high protein bar"]),
    ("plant_based_milk", ["plant based milk", "almond milk", "soy milk", "oat milk", "high protein almond"]),
    ("energy_drink", ["energy drink", "pre workout", "berocca", "hydralyte"]),
    ("infant_formula", ["infant formula", "formulated supplementary foods for young children"]),
    ("infant_snack", ["infant snack", "baby snack", "kids snack"]),
    ("sauce", ["sauce"]),
    ("ready_meal", ["ready meal", "bowl", "prepared meal", "marinara mix", "topokki"]),
    ("frozen_food", ["frozen", "sorbet", "frozen dessert", "ice cream"]),
    ("frozen_dessert", ["sorbet", "frozen dessert", "ice cream"]),
    ("bakery_product", ["bread", "bakery", "bakehouse", "cake", "wafer"]),
    ("confectionery", ["chocolate", "caramel", "confectionery", "allen's", "allens", "inside outs", "inside-outs", "gummies", "pastilles", "lolly", "lollies", "candy"]),
    ("snack_food", ["snack", "chips", "dukkah", "seaweed", "bar", "bubble bars", "wafer"]),
    ("seafood", ["seafood", "oyster", "oysters", "fish", "mussel", "marinara"]),
    ("meat_product", ["turkey", "bacon", "ham", "pork", "meat", "raw retail meats"]),
    ("dairy_product", ["milk", "cheese", "ricotta", "whey", "caseinate"]),
    ("beverage", ["beverage", "drink", "smoothie", "water", "coconut water"]),
    ("ingredient", ["powder", "protein isolate", "ingredient", "garlic powder", "lupin protein isolate", "mushroom", "coconut", "formulated foods"]),
    ("additive", ["additive", "processing aid", "enzyme", "amylase", "dextransucrase", "phospholipase"]),
]

_CATEGORY_PATTERNS = [
    ("beverages", ["beverage", "drink", "smoothie", "water", "hydralyte", "berocca", "pre workout", "kombucha", "kefir"]),
    ("dairy_chilled", ["milk", "cheese", "ricotta", "yoghurt", "yogurt"]),
    ("bakery_snacks_confectionery", ["bread", "snack", "bar", "chocolate", "confectionery", "dukkah", "allen's", "allens", "inside outs", "inside-outs", "gummies", "pastilles", "wafer", "cake", "lolly", "lollies", "candy"]),
    ("prepared_meals_pantry", ["ready meal", "bowl", "pantry", "seaweed", "topokki", "marinara mix", "garlic powder", "coconut", "mushroom"]),
    ("meat_seafood_animal", ["seafood", "turkey", "ham", "oyster", "oysters", "fish oil", "fish", "egg", "pork", "bacon", "meat", "mussel"]),
    ("plant_based", ["plant based", "vegan", "vegetarian", "almond", "soy"]),
    ("infant_kids_family", ["infant", "children", "kids", "young children", "formulated foods"]),
    ("sports_protein_functional", ["protein", "sports", "pre workout", "whey", "musashi", "clif", "muscle"]),
    ("ingredients_additives", ["ingredient", "additive", "processing aid", "enzyme", "powder"]),
]

_CATEGORY_FROM_PRODUCT_TYPE = {
    "beverage": "beverages",
    "energy_drink": "beverages",
    "fermented_drink": "beverages",
    "yoghurt_drink": "dairy_chilled",
    "dairy_product": "dairy_chilled",
    "confectionery": "bakery_snacks_confectionery",
    "snack_food": "bakery_snacks_confectionery",
    "protein_bar": "sports_protein_functional",
    "bakery_product": "bakery_snacks_confectionery",
    "ready_meal": "prepared_meals_pantry",
    "sauce": "prepared_meals_pantry",
    "frozen_food": "prepared_meals_pantry",
    "frozen_dessert": "bakery_snacks_confectionery",
    "seafood": "meat_seafood_animal",
    "meat_product": "meat_seafood_animal",
    "plant_based_milk": "plant_based",
    "infant_formula": "infant_kids_family",
    "infant_snack": "infant_kids_family",
    "ingredient": "ingredients_additives",
    "additive": "ingredients_additives",
}

_INGREDIENT_HINTS = [
    "peanut",
    "milk",
    "gluten",
    "soy",
    "egg",
    "cashew",
    "lupin",
    "listeria",
    "salmonella",
    "e. coli",
    "cereulide",
    "cadmium",
    "caffeine",
    "whey protein",
    "soy protein",
    "fish oil",
    "coconut",
    "garlic",
]


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _text(row: dict[str, Any]) -> str:
    fields = [
        "source_label",
        "authority",
        "title",
        "summary",
        "product_name",
        "product_category",
        "brand",
        "company",
        "ingredient_name",
        "allergen",
        "claim",
        "url",
    ]
    return " ".join(_norm(row.get(f)) for f in fields).lower()


def _content_text(row: dict[str, Any]) -> str:
    """Food content text, excluding source/url fields that cause false matches."""
    fields = [
        "title",
        "summary",
        "product_name",
        "product_category",
        "brand",
        "company",
        "ingredient_name",
        "allergen",
        "claim",
    ]
    return " ".join(_norm(row.get(f)) for f in fields).lower()


def _contains_any(text: str, terms: list[str]) -> bool:
    return any(term in text for term in terms)


def _tags_from_patterns(text: str, patterns: list[tuple[str, list[str]]]) -> list[str]:
    tags: list[str] = []
    for tag, terms in patterns:
        if _contains_any(text, terms):
            tags.append(tag)
    return tags


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        value = value.strip()
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _source_type(row: dict[str, Any]) -> str:
    source = _norm(row.get("source_label"))
    if source == "food_fsanz_recalls":
        return "regulatory_recall"
    if source == "food_fsanz_updates":
        return "regulatory_update"
    if source == "open_food_facts":
        return "product_database"
    return source or "unknown"


def _recall_issue_area(text: str) -> list[str]:
    """Precise issue tags for recalls, avoiding regulatory false positives."""
    if _contains_any(text, ["foreign matter", "foreign object", "glass", "metal", "plastic", "rubber", "mussel shell"]):
        return ["foreign_matter", "food_safety"]
    if _contains_any(text, ["undeclared allergen", "undeclared", "allergen"]):
        return ["undeclared_allergen", "incorrect_labelling", "food_safety"]
    if _contains_any(text, ["toxin", "cereulide"]):
        return ["toxin_contamination", "food_safety"]
    if _contains_any(text, ["chemical", "cadmium", "monocrotophos", "prohibited substance"]):
        return ["chemical_contamination", "food_safety"]
    if _contains_any(text, ["microbial", "salmonella", "listeria", "e. coli", "mould", "hepatitis", "viral"]):
        return ["microbial_contamination", "food_safety"]
    if _contains_any(text, ["unintended fermentation", "fermentation"]):
        tags = ["food_safety"]
        if _contains_any(text, ["microbial", "mould", "yeast"]):
            tags.insert(0, "microbial_contamination")
        return tags
    return ["food_safety"]


def classify_food_signal(row: dict[str, Any]) -> dict[str, Any]:
    """
    Return deterministic Food taxonomy fields for a signal row.

    Existing DB values are treated as evidence, not as the final frontend
    taxonomy.  This lets FSANZ update rows that mention recalls become
    recalls_safety without rewriting historical data.
    """
    text = _text(row)
    content_text = _content_text(row)
    source = _norm(row.get("source_label"))
    existing_type = _norm(row.get("signal_type") or row.get("event_type")).lower()
    claim = _norm(row.get("claim"))
    is_supplement_leakage = source == "open_food_facts" and is_supplement_like_food(row)

    is_recall = (
        source == "food_fsanz_recalls"
        or existing_type == "recall"
        or ("recall" in text and _contains_any(text, _RECALL_TERMS))
    )
    is_claim = bool(claim) or existing_type in {"claim_signal", "claim_risk"} or _contains_any(text, _CLAIM_TERMS)
    is_regulatory = source == "food_fsanz_updates" or existing_type in {"rule_update", "regulatory_update", "consultation"}
    is_product = source == "open_food_facts" or existing_type in {"new_product", "product_launch"}

    if is_recall:
        signal_type = "recall"
        dashboard_section = "recalls_safety"
    elif is_supplement_leakage:
        signal_type = "excluded"
        dashboard_section = "excluded"
    elif is_regulatory:
        signal_type = "consultation" if "consultation" in text or "call for comment" in text else "regulatory_update"
        dashboard_section = "regulatory_updates"
    elif is_claim:
        signal_type = "claim_risk"
        dashboard_section = "claims_labelling"
    elif is_product:
        signal_type = "product_launch"
        dashboard_section = "market_opportunities"
    else:
        signal_type = "category_trend"
        dashboard_section = "category_signals"

    issue_area = _recall_issue_area(content_text) if is_recall else _tags_from_patterns(content_text, _ISSUE_PATTERNS)
    if is_claim:
        issue_area.extend(["claims_wording", "substantiation"])
    if not issue_area and is_product and not is_supplement_leakage:
        issue_area.append("category_growth")

    claim_theme = _tags_from_patterns(content_text, _CLAIM_THEME_PATTERNS)
    if _contains_any(content_text, _HIGH_RISK_DISEASE_TERMS):
        issue_area.extend(["claims_wording", "substantiation"])

    product_type = _tags_from_patterns(content_text, _PRODUCT_TYPE_PATTERNS)
    if not product_type and is_product:
        product_type = ["ingredient" if "ingredient" in text else "other"]

    category = _tags_from_patterns(content_text, _CATEGORY_PATTERNS)
    for tag in product_type:
        mapped_category = _CATEGORY_FROM_PRODUCT_TYPE.get(tag)
        if mapped_category:
            category.append(mapped_category)
    if not category:
        category = ["other"]

    ingredients = []
    stored_ingredient = _norm(row.get("ingredient_name"))
    if stored_ingredient:
        ingredients.append(stored_ingredient)
    allergen = _norm(row.get("allergen"))
    if allergen:
        ingredients.extend([part.strip() for part in re.split(r"[,;/]", allergen) if part.strip()])
    for hint in _INGREDIENT_HINTS:
        if hint in text:
            ingredients.append(hint)

    severity = _norm(row.get("severity")).lower()
    if is_recall or severity in {"high", "critical", "severe"}:
        impact = "high"
    elif is_regulatory or is_claim or severity == "medium":
        impact = "medium"
    else:
        impact = "low"

    momentum = "active"
    scraped_at = _norm(row.get("scraped_at"))
    if scraped_at:
        try:
            dt = datetime.fromisoformat(scraped_at.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age_days = (datetime.now(timezone.utc) - dt).days
            if age_days > 365:
                momentum = "stable"
            elif age_days > 90:
                momentum = "watch"
        except ValueError:
            pass

    return {
        "market": ["australia", "new_zealand"] if _norm(row.get("authority")).lower() == "fsanz" else ["australia"],
        "category": _unique(category),
        "product_type": _unique(product_type),
        "ingredient": _unique(ingredients),
        "issue_area": _unique(issue_area),
        "claim_theme": _unique(claim_theme),
        "signal_type": signal_type,
        "source_type": _source_type(row),
        "dashboard_section": dashboard_section,
        "impact": impact,
        "momentum": momentum,
    }


def enrich_food_signal(row: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of row with frontend taxonomy fields applied."""
    if _norm(row.get("domain")).lower() != "food":
        return dict(row)
    enriched = dict(row)
    enriched.update(classify_food_signal(row))
    # Keep event_type aligned for Food API consumers while retaining legacy rows
    # in the database unchanged.
    enriched["event_type"] = enriched["signal_type"]
    return enriched


def possible_misclassified_recall(row: dict[str, Any]) -> bool:
    """True for Food rows whose stored type/source looks regulatory but taxonomy is recall."""
    if _norm(row.get("domain")).lower() != "food":
        return False
    taxonomy = classify_food_signal(row)
    stored = _norm(row.get("signal_type") or row.get("event_type")).lower()
    return taxonomy["signal_type"] == "recall" and stored not in {"recall"}
