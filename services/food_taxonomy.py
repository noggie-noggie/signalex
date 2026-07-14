"""
services/food_taxonomy.py — deterministic Food signal taxonomy enrichment.

This module does not write to the database.  It converts existing Food rows into
frontend-safe taxonomy fields for dashboard filtering and onboarding relevance.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any

from services.food_relevance_filter import classify_fsanz_update_relevance
from services.food_supplement_filter import is_supplement_like_food
from services.food_text_sanitizer import sanitize_food_text_fields


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
    ("protein_bar", ["protein bar", "protein crisp", "high protein bar"]),
    ("plant_based_milk", ["plant based milk", "plant milk", "almond milk", "soy milk", "oat milk", "macadamia milk", "high protein almond"]),
    ("meat_alternative", ["notburger", "not burger", "meat alternative", "meat-free burger", "meat free burger", "plant based burger", "plant-based burger", "veggie burger"]),
    ("dairy_alternative", ["dairy alternative", "milk alternative", "plant based milk", "plant milk", "almond milk", "soy milk", "oat milk", "macadamia milk"]),
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
    "meat_alternative": "plant_based",
    "dairy_alternative": "plant_based",
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

_MEANINGFUL_OPPORTUNITY_CLAIM_THEMES = {
    "gut_health",
    "immunity",
    "high_protein",
    "low_sugar",
    "source_of_fibre",
    "natural",
    "clean_label",
    "free_from",
    "kids_nutrition",
    "sustainability",
}

_MEANINGFUL_OPPORTUNITY_PRODUCT_TYPES = {
    "protein_bar",
    "yoghurt_drink",
    "fermented_drink",
    "kombucha",
    "plant_based_milk",
    "meat_alternative",
    "dairy_alternative",
}

_GENERIC_PRODUCT_TYPES = {"other", "egg", "meat_product", "seafood", "ingredient"}

_GENERIC_PRODUCT_OPPORTUNITY_REVIEW_TERMS = [
    "energy drink",
    "red bull",
    "monster",
    "v energy",
    "fresh farm cage eggs",
    "cage eggs",
]

_UNKNOWN_PRODUCT_TERMS = [
    "unknown product",
    "product unknown",
]

_ALCOHOL_TERMS = [
    "whisky",
    "whiskey",
    "scotch",
    "vodka",
    "gin",
    "rum",
    "tequila",
    "bourbon",
    "wine",
    "beer",
    "cider",
    "liqueur",
    "alcoholic beverage",
]

_GENERIC_VISIBLE_REVIEW_TERMS = [
    "mi goreng",
    "fried noodles",
    "instant noodles",
    "potato crisps",
    "crisps",
    "chips",
    "potato chips",
    "corn chips",
    "savoury snack",
    "savory snack",
    "cheese & onion",
    "cheese and onion",
    "wraps lite",
    "plain wraps",
    "wraps",
    "peri-peri rub",
    "peri peri rub",
    "garlic peri",
    "fresh farm cage eggs",
    "cage eggs",
    "farm cage eggs",
    "caramel latte",
    "instant coffee",
    "cornflour",
    "gluten free cornflour",
    "high fibre wholemeal sandwich bread",
]

_LOW_QUALITY_OFF_REASON = "Excluded from food launch: low-quality Open Food Facts record"

_LOW_QUALITY_FRAGMENT_TERMS = [
    "ingredients: h",
    "ingredients h",
    "ingredient: h",
    "ingredient h",
    "ingrdients",
    "ingredints",
    "ingedients",
    "caffiene",
    "taur1ne",
    "glucuronolact0ne",
]

_FOREIGN_LANGUAGE_TERMS = [
    "ingredienti",
    "ingredientes",
    "valori nutrizionali",
    "informazioni nutrizionali",
    "può contenere",
    "puo contenere",
    "puede contener",
    "contiene glutine",
    "sin gluten",
    "aceite de",
    "harina de",
    "prodotto in italia",
    "fabricado en",
]

_AU_NZ_RELEVANCE_TERMS = [
    "australia",
    "new zealand",
    "australian",
    "woolworths",
    "coles",
    "aldi australia",
    "fsanz",
]

_GENERIC_SAVOURY_SNACK_TERMS = [
    "potato crisps",
    "potato chips",
    "corn chips",
    "crisps",
    "chips",
    "cheese & onion",
    "cheese and onion",
    "savoury snack",
    "savory snack",
]

_FUNCTIONAL_SNACK_EVIDENCE_TERMS = [
    "protein bar",
    "protein snack",
    "protein crisps",
    "protein chips",
    "high protein",
    "source of protein",
    "low sugar",
    "no sugar",
    "source of fibre",
    "source of fiber",
    "high fibre",
    "high fiber",
    "gut health",
    "probiotic",
    "prebiotic",
    "functional snack",
    "meat alternative",
    "dairy alternative",
    "plant based burger",
    "plant-based burger",
    "notburger",
]

_GENERIC_RETAILER_PRIVATE_LABEL_TERMS = [
    "woolworths",
    "woolworths bakery",
]

_PRIVATE_LABEL_STRONG_INTELLIGENCE_TERMS = [
    "probiotic",
    "prebiotic",
    "gut health",
    "functional beverage",
    "protein bar",
    "protein smoothie",
    "plant milk",
    "dairy alternative",
    "meat alternative",
    "notburger",
    "novel ingredient",
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


def _contains_alcohol_term(text: str) -> bool:
    for term in _ALCOHOL_TERMS:
        if " " in term:
            if term in text:
                return True
            continue
        if re.search(rf"(?<![a-z]){re.escape(term)}(?![a-z])", text):
            return True
    return False


def _is_generic_savoury_snack(text: str) -> bool:
    return _contains_any(text, _GENERIC_SAVOURY_SNACK_TERMS)


def _looks_like_ocr_garbage(value: str) -> bool:
    text = (value or "").strip()
    if not text:
        return False
    lower = text.lower()
    if re.search(r"\bingredients?\s*:\s*[a-z]\s*(?:$|[|.;,])", lower):
        return True
    if _contains_any(lower, _LOW_QUALITY_FRAGMENT_TERMS):
        return True
    if re.search(r"(.)\1{5,}", text):
        return True
    if re.search(r"[�]{1,}|Ã.|â€|â€™|â€œ|â€", text):
        return True
    alpha_count = len(re.findall(r"[A-Za-z]", text))
    useful_count = len(re.findall(r"[A-Za-z0-9\s,.;:%/&()\-']", text))
    if len(text) >= 20 and useful_count / max(len(text), 1) < 0.65:
        return True
    if alpha_count < 4 and len(text) > 12:
        return True
    return False


def _mostly_raw_ingredient_dump(summary: str, text: str) -> bool:
    raw = (summary or "").strip().lower()
    if not raw.startswith(("ingredients:", "ingredients ")):
        return False
    if _contains_any(
        text,
        [
            "protein",
            "protein bar",
            "energy bar",
            "snack bar",
            "fibre",
            "fiber",
            "high protein",
            "source of fibre",
            "source of fiber",
            "low sugar",
            "lowcarb",
            "natural flavour",
            "natural flavor",
            "energy drink",
            "caffeine",
            "smoothie",
            "plant milk",
            "meat alternative",
            "notburger",
            "probiotic",
            "prebiotic",
        ],
    ):
        return False
    return len(raw) > 140 and raw.count(",") >= 8


def _open_food_facts_low_quality_reason(row: dict[str, Any], text: str) -> str:
    summary = _norm(row.get("summary"))
    ingredient_name = _norm(row.get("ingredient_name"))
    product_category = _norm(row.get("product_category")).lower()
    title = _norm(row.get("title"))
    product_name = _norm(row.get("product_name"))

    if _contains_any(f"{summary.lower()} {ingredient_name.lower()}", _LOW_QUALITY_FRAGMENT_TERMS):
        return _LOW_QUALITY_OFF_REASON
    if ingredient_name and len(ingredient_name.strip()) <= 2:
        return _LOW_QUALITY_OFF_REASON
    if _looks_like_ocr_garbage(summary) or _looks_like_ocr_garbage(ingredient_name):
        return _LOW_QUALITY_OFF_REASON
    if _mostly_raw_ingredient_dump(summary, text):
        return _LOW_QUALITY_OFF_REASON
    if _contains_any(text, _FOREIGN_LANGUAGE_TERMS) and not _contains_any(text, _AU_NZ_RELEVANCE_TERMS):
        return _LOW_QUALITY_OFF_REASON
    if product_category and _contains_any(product_category, ["italy", "italia", "spain", "españa", "espana"]):
        return _LOW_QUALITY_OFF_REASON
    if not title and not product_name:
        return _LOW_QUALITY_OFF_REASON
    return ""


def _has_functional_snack_evidence(text: str, claim_theme: list[str], product_type: list[str]) -> bool:
    themes = set(claim_theme)
    if "protein_bar" in product_type:
        return True
    if themes & {"gut_health", "source_of_fibre"}:
        return True
    if "high_protein" in themes and _contains_any(
        text,
        ["high protein", "source of protein", "protein bar", "protein snack", "protein crisps", "protein chips"],
    ):
        return True
    if "low_sugar" in themes and _contains_any(text, ["functional", "protein", "fibre", "fiber"]):
        return True
    return _contains_any(text, _FUNCTIONAL_SNACK_EVIDENCE_TERMS)


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


def _is_explicit_labelling_failure(text: str) -> bool:
    return _contains_any(
        text,
        [
            "undeclared allergen",
            "undeclared",
            "mislabelled",
            "mislabeled",
            "incorrect label",
            "incorrect labelling",
            "incorrect labeling",
            "labelling failure",
            "labeling failure",
        ],
    )


def _non_recall_issue_area(text: str, source: str) -> list[str]:
    tags = _tags_from_patterns(text, _ISSUE_PATTERNS)
    if source == "open_food_facts" and not _is_explicit_labelling_failure(text):
        tags = [
            tag for tag in tags
            if tag not in {"undeclared_allergen", "incorrect_labelling"}
        ]
    return tags


def _open_food_facts_exclusion_reason(text: str) -> str:
    if _contains_any(text, _UNKNOWN_PRODUCT_TERMS):
        return "Excluded from food launch: unknown Open Food Facts product"
    include_alcohol = os.getenv("FOOD_INCLUDE_ALCOHOL_PRODUCTS", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not include_alcohol and _contains_alcohol_term(text):
        return "Excluded from food launch: alcohol product"
    return ""


def _is_energy_drink_signal(product_type: list[str], text: str) -> bool:
    return "energy_drink" in product_type or _contains_any(
        text,
        ["energy drink", "caffeine", "red bull", "monster energy", "v energy"],
    )


def _is_generic_private_label_product(text: str, product_type: list[str]) -> bool:
    if not _contains_any(text, _GENERIC_RETAILER_PRIVATE_LABEL_TERMS):
        return False
    if set(product_type) & {
        "energy_drink",
        "protein_bar",
        "yoghurt_drink",
        "fermented_drink",
        "kombucha",
        "plant_based_milk",
        "meat_alternative",
        "dairy_alternative",
    }:
        return False
    return not _contains_any(text, _PRIVATE_LABEL_STRONG_INTELLIGENCE_TERMS)


def _is_generic_open_food_facts_product(
    text: str,
    claim_theme: list[str],
    product_type: list[str],
    has_meaningful_market_relevance: bool,
) -> bool:
    """True for OFF rows too weak/noisy for the Food launch dashboard."""
    if _is_energy_drink_signal(product_type, text):
        return False
    if _is_generic_private_label_product(text, product_type):
        return True
    if has_meaningful_market_relevance:
        return False
    if set(product_type) & _MEANINGFUL_OPPORTUNITY_PRODUCT_TYPES:
        return False
    if "protein_bar" in product_type:
        return False
    if _contains_any(text, ["fortified", "added vitamins", "added minerals", "nutrition fortification"]):
        return False

    themes = set(claim_theme)
    if themes and themes <= {"plant_based", "natural"}:
        return True
    if _contains_any(text, _GENERIC_VISIBLE_REVIEW_TERMS):
        return True
    if _is_generic_savoury_snack(text) and not _has_functional_snack_evidence(text, claim_theme, product_type):
        return True
    if not themes and not set(product_type) & {
        "energy_drink",
        "protein_bar",
        "yoghurt_drink",
        "fermented_drink",
        "kombucha",
        "plant_based_milk",
        "meat_alternative",
        "dairy_alternative",
        "infant_formula",
        "infant_snack",
    }:
        return True
    return False


def _has_meaningful_market_relevance(
    claim_theme: list[str],
    product_type: list[str],
    category: list[str],
    text: str,
) -> bool:
    if "energy_drink" in product_type:
        themes = set(claim_theme)
        if themes & {"plant_based", "gut_health"}:
            return True
        if _contains_any(
            text,
            [
                "functional beverage",
                "functional drink",
                "probiotic",
                "prebiotic",
                "gut health",
                "novel ingredient",
                "clean label",
            ],
        ):
            return True
        if "low_sugar" in themes and _contains_any(
            text,
            [
                "functional",
                "novel",
                "probiotic",
                "prebiotic",
                "gut health",
                "plant based",
                "plant-based",
                "clean label",
            ],
        ):
            return True
        return False

    if _is_generic_savoury_snack(text):
        return _has_functional_snack_evidence(text, claim_theme, product_type)

    if _contains_any(text, _GENERIC_PRODUCT_OPPORTUNITY_REVIEW_TERMS):
        return bool(
            set(claim_theme) & _MEANINGFUL_OPPORTUNITY_CLAIM_THEMES
            or _contains_any(
                text,
                [
                    "functional beverage",
                    "functional drink",
                    "probiotic",
                    "prebiotic",
                    "gut health",
                    "high protein",
                    "source of fibre",
                    "source of fiber",
                    "low sugar",
                    "no sugar",
                    "clean label",
                    "novel ingredient",
                    "alternative meat",
                    "meat alternative",
                    "dairy alternative",
                    "milk alternative",
                    "notburger",
                    "not burger",
                ],
            )
        )
    if set(claim_theme) & _MEANINGFUL_OPPORTUNITY_CLAIM_THEMES:
        return True
    if set(product_type) & _MEANINGFUL_OPPORTUNITY_PRODUCT_TYPES:
        return True
    return _contains_any(
        text,
        [
            "functional beverage",
            "functional drink",
            "gut health",
            "probiotic",
            "prebiotic",
            "high protein",
            "low sugar",
            "source of fibre",
            "source of fiber",
            "clean label",
            "novel ingredient",
            "alternative meat",
            "meat alternative",
            "dairy alternative",
            "milk alternative",
            "notburger",
            "not burger",
        ],
    )


def _affected_product_types(text: str, category: list[str], product_type: list[str]) -> list[str]:
    affected: list[str] = []
    if _contains_any(text, ["young child formula", "infant formula", "infant", "toddler"]):
        affected.append("infant_and_young_child_foods")
    if _contains_any(text, ["cell-cultured", "cell cultured", "cultured duck", "novel food"]):
        affected.append("novel_foods_and_alternative_proteins")
    if _contains_any(text, ["health star rating", "hsr"]):
        affected.append("packaged_foods_with_nutrition_labelling")
    if _contains_any(text, ["allergen", "undeclared"]):
        affected.append("allergen_sensitive_products")
    if _contains_any(text, ["cadmium", "maximum level", "contaminant"]):
        affected.append("products_with_contaminant_limits")
    if _contains_any(text, ["additive", "processing aid", "enzyme"]):
        affected.append("products_using_additives_or_processing_aids")
    affected.extend(category)
    affected.extend(product_type)
    return _unique(affected)


def _food_why_it_matters(
    row: dict[str, Any],
    *,
    text: str,
    dashboard_section: str,
    issue_area: list[str],
    affected: list[str],
) -> str:
    existing = _norm(row.get("why_it_matters"))
    if existing and dashboard_section != "regulatory_updates":
        return existing
    if dashboard_section == "recalls_safety":
        if "undeclared_allergen" in issue_area:
            return "This recall can create immediate allergen exposure risk and may require product, supplier, and label checks."
        if any(tag in issue_area for tag in ["toxin_contamination", "microbial_contamination", "chemical_contamination", "foreign_matter"]):
            return "This recall signals a food safety risk that may require rapid exposure checks across related products, ingredients, or suppliers."
        return "This recall may affect food safety, quality assurance, supplier controls, or customer communications."
    if _contains_any(text, ["young child formula", "infant formula"]):
        return "This consultation may affect infant and young-child nutrition products, including formulation, claims, labelling, and compliance watchpoints."
    if _contains_any(text, ["cell-cultured", "cell cultured", "cultured duck", "novel food"]):
        return "This update is relevant to novel foods and alternative proteins, including approval pathways and market-entry timing."
    if _contains_any(text, ["health star rating", "hsr"]):
        return "This update may affect nutrition labelling, reformulation strategy, and front-of-pack positioning for packaged foods."
    if _contains_any(text, ["allergen", "labelling", "label"]):
        return "This update may affect label controls, allergen risk management, claims wording, or compliance obligations."
    if dashboard_section == "regulatory_updates":
        return "This FSANZ item may affect food standards, formulation decisions, claims, labelling, market access, or compliance monitoring."
    if dashboard_section == "market_opportunities":
        return "This product signal may indicate a relevant claim, ingredient, or category trend worth monitoring for food innovation."
    if dashboard_section == "category_signals":
        return "This product signal is useful for light category monitoring, but should not be treated as a major opportunity on its own."
    return existing


def _food_recommended_action(
    row: dict[str, Any],
    *,
    text: str,
    dashboard_section: str,
    issue_area: list[str],
) -> str:
    existing = _norm(row.get("recommended_action"))
    if existing and dashboard_section not in {"regulatory_updates", "recalls_safety"}:
        return existing
    if dashboard_section == "recalls_safety":
        return "Review the recall notice, check affected products and lots, assess supplier/product exposure, and update food safety or quality teams if relevant."
    if _contains_any(text, ["young child formula", "infant formula"]):
        return "Monitor consultation outcomes and review any exposure across young-child nutrition formulation, labelling, claims, and compliance plans."
    if _contains_any(text, ["cell-cultured", "cell cultured", "cultured duck", "novel food"]):
        return "Track the approval pathway and assess implications for alternative-protein strategy, competitor monitoring, and novel food compliance."
    if _contains_any(text, ["health star rating", "hsr"]):
        return "Review potential implications for nutrition labelling, reformulation, score modelling, and front-of-pack communications."
    if dashboard_section == "regulatory_updates":
        return "Review the update for potential impact on formulation, labelling, claims, market access, or compliance obligations."
    if dashboard_section == "market_opportunities":
        return "Review the product/category signal for relevance to claims, positioning, ingredients, and product pipeline decisions."
    return existing or "Review this signal for relevance to food safety, labelling, regulatory compliance, claims, product quality, or category strategy."


def _food_impact(
    *,
    text: str,
    dashboard_section: str,
    issue_area: list[str],
    severity: str,
    is_regulatory: bool,
    is_claim: bool,
    is_product: bool,
    has_meaningful_market_relevance: bool,
) -> str:
    if dashboard_section == "excluded":
        return "low"
    if severity in {"high", "critical", "severe"}:
        return "high"
    if dashboard_section == "recalls_safety":
        if (
            {"undeclared_allergen", "toxin_contamination", "microbial_contamination", "chemical_contamination"}
            & set(issue_area)
            or _contains_any(text, ["infant formula", "young child", "serious health risk"])
        ):
            return "high"
        return "medium"
    if is_regulatory:
        if _contains_any(
            text,
            [
                "young child formula",
                "infant formula",
                "health star rating",
                "novel food",
                "cell-cultured",
                "cell cultured",
                "allergen",
                "labelling",
                "maximum level",
                "contaminant",
                "food standards code",
                "standard amendment",
            ],
        ):
            return "high"
        return "medium"
    if is_claim:
        return "medium"
    if is_product and has_meaningful_market_relevance:
        if _contains_any(text, ["notburger", "novel ingredient", "functional beverage", "gut health", "high protein"]):
            return "medium"
        return "low"
    return "low"


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
    off_exclusion_reason = (
        _open_food_facts_exclusion_reason(content_text)
        if source == "open_food_facts"
        else ""
    )
    if source == "open_food_facts" and not off_exclusion_reason:
        off_exclusion_reason = _open_food_facts_low_quality_reason(row, content_text)
    fsanz_relevance = (
        classify_fsanz_update_relevance(row)
        if source == "food_fsanz_updates"
        else None
    )

    is_recall = (
        source == "food_fsanz_recalls"
        or existing_type == "recall"
        or ("recall" in text and _contains_any(text, _RECALL_TERMS))
    )
    is_claim = (
        bool(claim)
        or existing_type in {"claim_signal", "claim_risk"}
        or (source != "open_food_facts" and _contains_any(text, _CLAIM_TERMS))
    )
    is_regulatory = source == "food_fsanz_updates" or existing_type in {"rule_update", "regulatory_update", "consultation"}
    is_product = source == "open_food_facts" or existing_type in {"new_product", "product_launch"}

    issue_area = _recall_issue_area(content_text) if is_recall else _non_recall_issue_area(content_text, source)
    if is_claim:
        issue_area.extend(["claims_wording", "substantiation"])
    if not issue_area and is_product and not is_supplement_leakage:
        issue_area.append("category_growth")

    claim_theme = _tags_from_patterns(content_text, _CLAIM_THEME_PATTERNS)
    if (
        "high_protein" in claim_theme
        and _is_generic_savoury_snack(content_text)
        and not _has_functional_snack_evidence(content_text, claim_theme, product_type=[])
    ):
        claim_theme = [theme for theme in claim_theme if theme != "high_protein"]
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

    has_meaningful_market_relevance = _has_meaningful_market_relevance(
        claim_theme,
        product_type,
        category,
        content_text,
    )
    is_generic_off_product = (
        source == "open_food_facts"
        and not off_exclusion_reason
        and not is_supplement_leakage
        and _is_generic_open_food_facts_product(
            content_text,
            claim_theme,
            product_type,
            has_meaningful_market_relevance,
        )
    )
    if is_generic_off_product:
        off_exclusion_reason = "Excluded from food launch: generic Open Food Facts product"

    if is_recall:
        signal_type = "recall"
        dashboard_section = "recalls_safety"
    elif is_supplement_leakage:
        signal_type = "excluded"
        dashboard_section = "excluded"
    elif off_exclusion_reason:
        signal_type = "excluded"
        dashboard_section = "excluded"
    elif fsanz_relevance is not None and not fsanz_relevance.visible:
        signal_type = "excluded"
        dashboard_section = "excluded"
    elif is_regulatory:
        signal_type = "consultation" if "consultation" in text or "call for comment" in text else "regulatory_update"
        dashboard_section = "regulatory_updates"
    elif is_claim:
        signal_type = "claim_risk"
        dashboard_section = "claims_labelling"
    elif is_product and has_meaningful_market_relevance:
        signal_type = "product_launch"
        dashboard_section = "market_opportunities"
    elif is_product:
        signal_type = "category_trend"
        dashboard_section = "category_signals"
    else:
        signal_type = "category_trend"
        dashboard_section = "category_signals"

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

    affected_product_types = _affected_product_types(content_text, _unique(category), _unique(product_type))
    severity = _norm(row.get("severity")).lower()
    impact = _food_impact(
        text=content_text,
        dashboard_section=dashboard_section,
        issue_area=_unique(issue_area),
        severity=severity,
        is_regulatory=is_regulatory,
        is_claim=is_claim,
        is_product=is_product,
        has_meaningful_market_relevance=has_meaningful_market_relevance,
    )

    momentum = "active"
    if dashboard_section == "excluded":
        momentum = "stable"
    scraped_at = _norm(row.get("scraped_at"))
    if scraped_at and dashboard_section != "excluded":
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

    result = {
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
        "affected_product_types": affected_product_types,
        "why_it_matters": _food_why_it_matters(
            row,
            text=content_text,
            dashboard_section=dashboard_section,
            issue_area=_unique(issue_area),
            affected=affected_product_types,
        ),
        "recommended_action": _food_recommended_action(
            row,
            text=content_text,
            dashboard_section=dashboard_section,
            issue_area=_unique(issue_area),
        ),
    }
    if fsanz_relevance is not None:
        result.update({
            "fsanz_content_type": fsanz_relevance.content_type,
            "food_relevance_score": fsanz_relevance.score,
            "food_relevance_reason": fsanz_relevance.reason,
            "food_relevance_confidence": fsanz_relevance.confidence,
        })
    if off_exclusion_reason:
        result["noise_reason"] = off_exclusion_reason
    if dashboard_section == "excluded":
        result["is_noise"] = 1
    return result


def enrich_food_signal(row: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of row with frontend taxonomy fields applied."""
    if _norm(row.get("domain")).lower() != "food":
        return dict(row)
    enriched = dict(row)
    enriched.update(classify_food_signal(row))
    # Keep event_type aligned for Food API consumers while retaining legacy rows
    # in the database unchanged.
    enriched["event_type"] = enriched["signal_type"]
    return sanitize_food_text_fields(enriched)


def possible_misclassified_recall(row: dict[str, Any]) -> bool:
    """True for Food rows whose stored type/source looks regulatory but taxonomy is recall."""
    if _norm(row.get("domain")).lower() != "food":
        return False
    taxonomy = classify_food_signal(row)
    stored = _norm(row.get("signal_type") or row.get("event_type")).lower()
    return taxonomy["signal_type"] == "recall" and stored not in {"recall"}
