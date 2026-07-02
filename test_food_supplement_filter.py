"""Regression tests for excluding supplement leakage from Food launch data."""

import unittest

from services.food_supplement_filter import is_supplement_like_food
from services.food_taxonomy import classify_food_signal


class FoodSupplementFilterTests(unittest.TestCase):
    def test_natures_own_vitamin_d3_is_supplement_leakage(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "title": "Nature's Own — Vitamin D3 1000IU",
            "product_name": "Vitamin D3 1000IU",
            "product_category": "dietary supplements",
            "summary": "Open Food Facts product record.",
        }
        self.assertTrue(is_supplement_like_food(row))
        taxonomy = classify_food_signal(row)
        self.assertEqual(taxonomy["dashboard_section"], "excluded")
        self.assertEqual(taxonomy["signal_type"], "excluded")

    def test_ostelin_d3_tablets_are_supplement_leakage(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "title": "Ostelin — Vitamin D3",
            "product_name": "Vitamin D3 tablets",
            "product_category": "dietary supplements",
            "summary": "Contains vitamin D3 25 mcg tablets.",
        }
        self.assertTrue(is_supplement_like_food(row))
        self.assertEqual(classify_food_signal(row)["dashboard_section"], "excluded")

    def test_open_food_facts_dietary_supplement_category_is_excluded(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "title": "Example Brand — Multivitamin Capsules",
            "product_name": "Multivitamin Capsules",
            "product_category": "dietary supplements",
        }
        self.assertTrue(is_supplement_like_food(row))

    def test_normal_fortified_food_with_vitamin_mentions_is_not_excluded(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "title": "Breakfast Cereal with added vitamins",
            "product_name": "Fortified Breakfast Cereal",
            "product_category": "breakfast cereals",
            "summary": "Contains wheat, sugar, added vitamins and minerals as nutrition fortification.",
        }
        self.assertFalse(is_supplement_like_food(row))
        self.assertNotEqual(classify_food_signal(row)["dashboard_section"], "excluded")


if __name__ == "__main__":
    unittest.main()
