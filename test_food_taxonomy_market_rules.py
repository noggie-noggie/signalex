"""Regression tests for Food market-opportunity taxonomy precision."""

import unittest

from services.food_text_sanitizer import contains_food_text_contamination
from services.food_taxonomy import enrich_food_signal


class FoodTaxonomyMarketRulesTests(unittest.TestCase):
    def test_open_food_facts_declared_allergens_are_not_undeclared_allergens(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "signal_type": "new_product",
            "title": "Rokeby — Protein Smoothie Choc Honeycomb",
            "summary": "Ingredients: milk, cocoa. | Allergens: milk",
            "product_name": "Protein Smoothie Choc Honeycomb",
            "product_category": "beverages and beverages preparations",
            "allergen": "milk",
        }
        result = enrich_food_signal(row)
        self.assertNotIn("undeclared_allergen", result["issue_area"])
        self.assertNotIn("incorrect_labelling", result["issue_area"])

    def test_generic_cenovis_eggs_are_not_market_opportunity(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "signal_type": "new_product",
            "title": "Cenovis — Fresh farm cage eggs",
            "product_name": "Fresh farm cage eggs",
            "product_category": "farming products",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["dashboard_section"], "category_signals")
        self.assertEqual(result["signal_type"], "category_trend")
        self.assertEqual(result["impact"], "low")

    def test_high_protein_smoothie_can_remain_market_opportunity(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "signal_type": "new_product",
            "title": "ROKEBY — Protein Smoothie",
            "summary": "Ingredients: low fat milk. High protein smoothie.",
            "product_name": "Protein Smoothie",
            "product_category": "beverages and beverages preparations",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["dashboard_section"], "market_opportunities")
        self.assertIn("high_protein", result["claim_theme"])
        self.assertEqual(result["impact"], "medium")

    def test_claim_signal_protein_bar_remains_claims_labelling_not_allergen_failure(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "signal_type": "claim_signal",
            "title": "Clif Bar — Cool Mint Chocolate Natural Flavour Energy Bar",
            "summary": "Ingredients: soy protein isolate. | Allergens: gluten, soybeans",
            "product_name": "Cool Mint Chocolate Natural Flavour Energy Bar",
            "product_category": "snacks",
            "allergen": "gluten, soybeans",
            "claim": "natural",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["dashboard_section"], "claims_labelling")
        self.assertNotIn("undeclared_allergen", result["issue_area"])
        self.assertIn("natural", result["claim_theme"])

    def test_existing_text_sanitisation_still_applies(self):
        row = {
            "domain": "food",
            "source_label": "food_fsanz_recalls",
            "authority": "fsanz",
            "signal_type": "recall",
            "title": "Example recall",
            "summary": "The recall is due to foreign matter.",
            "ai_summary": "VMS companies should review the supplement industry.",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["ai_summary"], "")
        self.assertFalse(contains_food_text_contamination(result["ai_summary"]))

    def test_supplement_leakage_still_excluded(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "title": "Ostelin — Vitamin D3 1000IU",
            "product_name": "Vitamin D3 1000IU",
            "product_category": "dietary supplements",
        }
        self.assertEqual(enrich_food_signal(row)["dashboard_section"], "excluded")


if __name__ == "__main__":
    unittest.main()
