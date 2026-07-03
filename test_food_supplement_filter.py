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

    def test_blackmores_fish_oil_is_excluded(self):
        self.assertTrue(is_supplement_like_food({
            "title": "Blackmores — Fish Oil 1000",
            "product_name": "Fish Oil 1000",
            "product_category": "dietary supplements",
        }))

    def test_berocca_is_excluded(self):
        self.assertTrue(is_supplement_like_food({
            "title": "Berocca — Berocca",
            "product_name": "Berocca",
            "product_category": "dietary supplements",
        }))

    def test_hydralyte_is_excluded(self):
        self.assertTrue(is_supplement_like_food({
            "title": "Hydralyte — Hydralyte Orange",
            "product_name": "Hydralyte Orange",
            "product_category": "dietary supplements",
        }))

    def test_protein_powder_is_excluded(self):
        self.assertTrue(is_supplement_like_food({
            "title": "Example Whey Protein Isolate Powder",
            "product_name": "Whey Protein Isolate Powder",
            "product_category": "sports nutrition",
        }))

    def test_clif_bar_is_not_excluded(self):
        self.assertFalse(is_supplement_like_food({
            "title": "Clif Bar — Cool Mint Chocolate Natural Flavour Energy Bar",
            "product_name": "Cool Mint Chocolate Natural Flavour Energy Bar",
            "product_category": "snacks",
            "summary": "Ingredients: oats, soy protein isolate. Allergens: soybeans.",
        }))

    def test_rokeby_protein_smoothie_is_not_excluded(self):
        self.assertFalse(is_supplement_like_food({
            "title": "Rokeby — Protein Smoothie Choc Honeycomb",
            "product_name": "Protein Smoothie Choc Honeycomb",
            "product_category": "beverages and beverages preparations",
            "summary": "Ingredients: low fat milk.",
        }))

    def test_generic_protein_bar_food_format_is_not_excluded_for_protein(self):
        self.assertFalse(is_supplement_like_food({
            "title": "Example High Protein Bar",
            "product_name": "High Protein Bar",
            "product_category": "dietary supplements",
            "summary": "Snack bar with chocolate and peanuts.",
        }))

    def test_energy_drink_is_not_excluded_for_energy(self):
        self.assertFalse(is_supplement_like_food({
            "title": "Example Energy Drink",
            "product_name": "Energy Drink",
            "product_category": "beverages",
            "summary": "Carbonated energy drink.",
        }))


if __name__ == "__main__":
    unittest.main()
