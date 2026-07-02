"""
services/food_supplement_filter.py — exclude VMS-like products from Food launch.

Open Food Facts contains both ordinary foods and dietary supplements.  For the
Food-only launch, supplement/tablet/capsule products must not appear in the
Food API, even if they came from Open Food Facts.
"""

from __future__ import annotations

import re
from typing import Any


NOISE_REASON = "Excluded from food launch: dietary supplement / VMS-like product"

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

_STRONG_SUPPLEMENT_TERMS = [
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
    "fish oil",
    "omega-3",
    "omega 3",
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


def is_supplement_like_food(record: dict[str, Any]) -> bool:
    """
    Return True for Open Food Facts records that should be excluded from Food.

    The detector intentionally requires strong supplement evidence for generic
    vitamin mentions so normal fortified foods, e.g. breakfast cereal with
    added vitamins, are not excluded.
    """
    text = _product_text(record)
    category_text = _category_text(record)

    if any(term in category_text for term in _SUPPLEMENT_CATEGORIES):
        return True

    if any(term in text for term in _STRONG_SUPPLEMENT_TERMS):
        return True

    has_dosage_form = any(_has_word(text, term) for term in _DOSAGE_FORM_TERMS)
    has_vitamin_marker = any(term in text for term in _VITAMIN_TERMS)
    if has_dosage_form and has_vitamin_marker:
        return True

    if any(term in text for term in _SUPPLEMENT_GUMMY_TERMS):
        return True

    # Plain "gummies" can be confectionery, so only exclude it when paired with
    # vitamin/supplement evidence.
    if "gummies" in text and has_vitamin_marker:
        return True

    # IU/mcg alone can appear in nutrition context; require vitamin/D3 context.
    if ("1000iu" in text or " iu" in text or "mcg" in text) and (
        "vitamin" in text or " d3" in text
    ):
        return True

    return False
