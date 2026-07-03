"""
services/food_supplement_filter.py — exclude VMS-like products from Food launch.

Open Food Facts contains both ordinary foods and dietary supplements.  For the
Food-only launch, supplement/tablet/capsule products must not appear in the
Food API, even if they came from Open Food Facts.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


NOISE_REASON = "Excluded from food launch: dietary supplement / VMS-like product"


@dataclass(frozen=True)
class SupplementFilterDecision:
    excluded: bool
    reason: str


_SUPPLEMENT_CATEGORIES = [
    "dietary supplement",
    "dietary supplements",
    "vitamins",
    "minerals",
    "protein supplement",
    "protein supplements",
    "herbal supplement",
    "herbal supplements",
    "fish oil supplement",
    "mineral supplement",
    "multivitamin",
]

_FOOD_FORMAT_TERMS = [
    "protein bar",
    "protein wafer",
    "snack bar",
    "energy bar",
    "cereal bar",
    "wafer",
    "smoothie",
    "yoghurt drink",
    "yogurt drink",
    "milk drink",
    "almond milk",
    "plant milk",
    "energy drink",
    "plant-based food",
    "plant based food",
    "plant-based beverage",
    "plant based beverage",
    "plant based foods and beverages",
    "high protein almond",
    "high-protein food",
    "high protein food",
    "low-sugar snack",
    "low sugar snack",
]

_HARD_EXCLUDE_TERMS = [
    "dietary supplement",
    "supplement",
    "mineral supplement",
    "multivitamin",
    "ostelin",
    "nature's own",
    "natures own",
    "nature’s own",
    "nature's way",
    "natures way",
    "nature’s way",
    "blackmores",
    "swisse",
    "berocca",
    "hydralyte",
    "fish oil",
    "omega-3",
    "omega 3",
    "nmn",
    "magnesium",
    "zinc",
    "folic acid",
    "b12",
    "vitamin b12",
    "vitamin c",
    "vitamin d",
    "vitamin d3",
    "greens powder",
    "pre-workout",
    "pre workout",
    "creatine",
]

_DOSAGE_FORM_TERMS = [
    "tablet",
    "tablets",
    "capsule",
    "capsules",
    "softgel",
    "softgels",
]

_VITAMIN_TERMS = [
    "vitamin",
    "vitamin d",
    "vitamin d3",
    "d3",
    "1000iu",
    " iu",
    "mcg",
]

_POWDER_TERMS = [
    "protein powder",
    "whey isolate powder",
    "whey protein isolate powder",
    "greens powder",
    "powder",
]

_FOOD_FORMAT_COMPATIBLE_NUTRIENT_TERMS = {
    "magnesium",
    "zinc",
    "folic acid",
    "b12",
    "vitamin b12",
    "vitamin c",
    "vitamin d",
    "vitamin d3",
}

_SUPPLEMENT_GUMMY_TERMS = [
    "vita gummies",
    "vitamin gummies",
    "supplement gummies",
    "gummies supplement",
]


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _join_values(values: Any) -> str:
    if isinstance(values, (list, tuple, set)):
        return " ".join(_norm(v) for v in values)
    return _norm(values)


def _product_text(record: dict[str, Any]) -> str:
    fields = [
        "title",
        "product_name",
        "product_category",
        "category",
        "brand",
        "company",
        "summary",
        "ingredient_name",
        "ingredient",
        "claim",
        "labels",
        "labels_tags",
        "categories_tags",
        "ingredients_text",
    ]
    return " ".join(_join_values(record.get(field)) for field in fields).lower()


def _category_text(record: dict[str, Any]) -> str:
    fields = ["product_category", "category", "categories_tags"]
    return " ".join(_join_values(record.get(field)) for field in fields).lower().replace("-", " ")


def _has_word(text: str, term: str) -> bool:
    return re.search(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", text) is not None


def _matched_term(text: str, terms: list[str]) -> str:
    for term in terms:
        if term in text:
            return term
    return ""


def _matched_word(text: str, terms: list[str]) -> str:
    for term in terms:
        if _has_word(text, term):
            return term
    return ""


def supplement_filter_decision(record: dict[str, Any]) -> SupplementFilterDecision:
    """
    Return an exclusion decision and reason for Open Food Facts food records.

    The detector intentionally requires strong supplement evidence for generic
    vitamin mentions so normal fortified foods, e.g. breakfast cereal with
    added vitamins, are not excluded.
    """
    text = _product_text(record)
    category_text = _category_text(record)
    food_format = _matched_term(text, _FOOD_FORMAT_TERMS)

    dosage_form = _matched_word(text, _DOSAGE_FORM_TERMS)
    vitamin_marker = _matched_term(text, _VITAMIN_TERMS)
    if dosage_form and vitamin_marker:
        return SupplementFilterDecision(True, f"dosage form + vitamin marker: {dosage_form}, {vitamin_marker.strip()}")

    powder_term = _matched_term(text, _POWDER_TERMS)
    if powder_term and _contains_supplement_context(text):
        return SupplementFilterDecision(True, f"supplement powder: {powder_term}")

    gummy_term = _matched_term(text, _SUPPLEMENT_GUMMY_TERMS)
    if gummy_term:
        return SupplementFilterDecision(True, f"supplement gummies: {gummy_term}")

    # Plain "gummies" can be confectionery, so only exclude when paired with
    # vitamin/supplement evidence.
    if "gummies" in text and vitamin_marker:
        return SupplementFilterDecision(True, f"vitamin gummies: {vitamin_marker.strip()}")

    hard_term = _matched_term(text, _HARD_EXCLUDE_TERMS)
    if hard_term:
        if hard_term in {"dietary supplement", "supplement"} and food_format and hard_term in category_text:
            return SupplementFilterDecision(False, f"allowed food format despite supplement category: {food_format}")
        if hard_term in _FOOD_FORMAT_COMPATIBLE_NUTRIENT_TERMS and food_format:
            return SupplementFilterDecision(False, f"allowed food format with nutrient fortification: {food_format}")
        if hard_term == "supplement" and food_format:
            return SupplementFilterDecision(False, f"allowed food format despite broad supplement wording: {food_format}")
        return SupplementFilterDecision(True, f"strong supplement term: {hard_term}")

    supplement_category = _matched_term(category_text, _SUPPLEMENT_CATEGORIES)
    if supplement_category:
        if food_format:
            return SupplementFilterDecision(False, f"allowed food format despite supplement category: {food_format}")
        return SupplementFilterDecision(True, f"supplement category: {supplement_category}")

    # IU/mcg alone can appear in nutrition context; require vitamin/D3 context.
    if ("1000iu" in text or " iu" in text or "mcg" in text) and (
        "vitamin" in text or " d3" in text
    ):
        return SupplementFilterDecision(True, "vitamin dosage marker")

    if food_format:
        return SupplementFilterDecision(False, f"allowed food format: {food_format}")

    return SupplementFilterDecision(False, "no supplement exclusion evidence")


def _contains_supplement_context(text: str) -> bool:
    return any(
        term in text
        for term in [
            "protein powder",
            "whey isolate powder",
            "whey protein isolate powder",
            "greens powder",
            "dietary supplement",
            "supplement",
            "serving size",
            "scoop",
            "sachet",
        ]
    )


def is_supplement_like_food(record: dict[str, Any]) -> bool:
    """Return True for Open Food Facts records that should be excluded from Food."""
    return supplement_filter_decision(record).excluded
