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

    def test_red_bull_energy_drink_is_category_signal(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "signal_type": "new_product",
            "title": "Red Bull — Energy Drink",
            "summary": "Carbonated energy drink with caffeine and natural flavours.",
            "product_name": "Energy Drink",
            "product_category": "beverages and beverages preparations",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["dashboard_section"], "category_signals")
        self.assertNotEqual(result["dashboard_section"], "market_opportunities")
        self.assertEqual(result["impact"], "low")
        self.assertIn("category_growth", result["issue_area"])
        self.assertIn("energy", result["claim_theme"])
        self.assertIn("natural", result["claim_theme"])

    def test_energy_drink_with_only_energy_claim_is_category_signal(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "signal_type": "new_product",
            "title": "Example Brand - Energy Drink",
            "summary": "Energy drink with caffeine.",
            "product_name": "Energy Drink",
            "product_category": "beverages",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["dashboard_section"], "category_signals")
        self.assertEqual(result["impact"], "low")
        self.assertEqual(result["signal_type"], "category_trend")

    def test_monster_energy_drink_is_category_signal(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "signal_type": "new_product",
            "title": "Monster — Energy Drink",
            "summary": "Energy drink.",
            "product_name": "Monster Energy",
            "product_category": "beverages",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["dashboard_section"], "category_signals")
        self.assertEqual(result["signal_type"], "category_trend")

    def test_v_energy_drink_is_category_signal(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "signal_type": "new_product",
            "title": "V — Energy Drink",
            "summary": "Carbonated energy drink.",
            "product_name": "V Energy Drink",
            "product_category": "beverages",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["dashboard_section"], "category_signals")
        self.assertEqual(result["impact"], "low")

    def test_clif_high_protein_bar_can_remain_market_opportunity(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "signal_type": "new_product",
            "title": "Clif — High Protein Bar",
            "summary": "High protein snack bar.",
            "product_name": "High Protein Bar",
            "product_category": "snacks",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["dashboard_section"], "market_opportunities")
        self.assertIn("high_protein", result["claim_theme"])

    def test_generic_product_launch_without_claim_theme_is_category_signal(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "signal_type": "new_product",
            "title": "Example Brand — Plain Crackers",
            "summary": "Plain crackers.",
            "product_name": "Plain Crackers",
            "product_category": "snacks",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["dashboard_section"], "category_signals")
        self.assertEqual(result["signal_type"], "category_trend")

    def test_sports_brand_wafer_without_claim_theme_is_category_signal(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "signal_type": "new_product",
            "title": "Musashi — protons wafer",
            "summary": "Wafer snack.",
            "product_name": "protons wafer",
            "product_category": "dietary supplements",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["dashboard_section"], "category_signals")
        self.assertEqual(result["impact"], "low")

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

    def test_unknown_open_food_facts_product_is_excluded(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "title": "Unknown product",
            "product_name": "Unknown product",
            "product_category": "plant based foods and beverages",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["dashboard_section"], "excluded")
        self.assertEqual(result["is_noise"], 1)

    def test_johnnie_walker_whisky_is_excluded_by_default(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "title": "Johnnie Walker — Johnnie Walker Red Scotch whisky",
            "product_name": "Johnnie Walker Red Scotch whisky",
            "product_category": "beverages",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["dashboard_section"], "excluded")
        self.assertEqual(result["is_noise"], 1)

    def test_indomie_plant_based_category_is_not_market_opportunity(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "title": "indomie — Mi goreng Fried Noodles",
            "product_name": "Mi goreng Fried Noodles",
            "product_category": "plant based foods and beverages",
        }
        result = enrich_food_signal(row)
        self.assertIn("plant_based", result["claim_theme"])
        self.assertNotEqual(result["dashboard_section"], "market_opportunities")

    def test_tayto_crisps_are_not_market_opportunity(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "title": "TAYTO SNACKS — CHEESE & ONION FLAVOUR POTATO CRISPS",
            "product_name": "CHEESE & ONION FLAVOUR POTATO CRISPS",
            "product_category": "plant based foods and beverages",
        }
        result = enrich_food_signal(row)
        self.assertNotEqual(result["dashboard_section"], "market_opportunities")

    def test_nandos_rub_is_not_market_opportunity(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "title": "Nando's — Garlic Peri-peri rub",
            "product_name": "Garlic Peri-peri rub",
            "product_category": "plant based foods and beverages",
        }
        result = enrich_food_signal(row)
        self.assertNotEqual(result["dashboard_section"], "market_opportunities")

    def test_generic_plant_based_alone_is_not_market_opportunity(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "title": "Example Brand — Vegan Plain Rice",
            "product_name": "Vegan Plain Rice",
            "product_category": "plant based foods and beverages",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["claim_theme"], ["plant_based"])
        self.assertNotEqual(result["dashboard_section"], "market_opportunities")

    def test_notburger_remains_visible_as_plant_based_innovation(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "title": "NotCo — NotBurger",
            "product_name": "NotBurger",
            "product_category": "plant based foods and beverages",
        }
        result = enrich_food_signal(row)
        self.assertNotEqual(result["dashboard_section"], "excluded")
        self.assertIn(result["dashboard_section"], {"market_opportunities", "category_signals"})
        self.assertIn("meat_alternative", result["product_type"])

    def test_macadamia_milk_remains_visible_as_dairy_alternative(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "title": "Suncoast — Macadamia milk",
            "product_name": "Macadamia milk",
            "product_category": "plant based foods and beverages",
        }
        result = enrich_food_signal(row)
        self.assertNotEqual(result["dashboard_section"], "excluded")
        self.assertIn(result["dashboard_section"], {"market_opportunities", "category_signals"})
        self.assertIn("plant_based_milk", result["product_type"])

    def test_high_fibre_bread_remains_visible_claim_signal(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "title": "Woolworths Bakery — High Fibre Wholemeal Sandwich Bread",
            "product_name": "High Fibre Wholemeal Sandwich Bread",
            "product_category": "breads",
        }
        result = enrich_food_signal(row)
        self.assertNotEqual(result["dashboard_section"], "excluded")
        self.assertIn(result["dashboard_section"], {"claims_labelling", "market_opportunities"})
        self.assertIn("source_of_fibre", result["claim_theme"])

    def test_protein_peanut_butter_remains_functional_opportunity(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "title": "Mayver's — Protein peanut butter",
            "product_name": "Protein peanut butter",
            "product_category": "spreads",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["dashboard_section"], "market_opportunities")
        self.assertIn("high_protein", result["claim_theme"])

    def test_crumble_text_does_not_trigger_rum_alcohol_exclusion(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "title": "SAM's PANTRY — APPLE CRUMBLE FOOD PROTEIN BAR",
            "product_name": "APPLE CRUMBLE FOOD PROTEIN BAR",
            "product_category": "snacks",
        }
        result = enrich_food_signal(row)
        self.assertNotEqual(result["dashboard_section"], "excluded")
        self.assertEqual(result["dashboard_section"], "market_opportunities")


if __name__ == "__main__":
    unittest.main()
