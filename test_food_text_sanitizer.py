"""Regression tests for removing VMS/supplement wording from Food responses."""

import unittest

from services.food_supplement_filter import is_supplement_like_food
from services.food_text_sanitizer import contains_food_text_contamination
from services.food_taxonomy import enrich_food_signal


class FoodTextSanitizerTests(unittest.TestCase):
    def test_food_ai_summary_with_vms_companies_is_cleared(self):
        row = {
            "domain": "food",
            "source_label": "food_fsanz_recalls",
            "authority": "fsanz",
            "signal_type": "recall",
            "title": "Example Food Recall",
            "summary": "The recall is due to foreign matter.",
            "ai_summary": "VMS companies should review exposure.",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["ai_summary"], "")
        self.assertNotIn("VMS companies", result["ai_summary"])

    def test_food_sentiment_reasoning_with_supplement_industry_is_cleared(self):
        row = {
            "domain": "food",
            "source_label": "food_fsanz_updates",
            "authority": "fsanz",
            "signal_type": "rule_update",
            "title": "Call for comment on labelling",
            "summary": "Food labelling consultation.",
            "sentiment_reasoning": "The supplement industry may face new obligations.",
        }
        result = enrich_food_signal(row)
        self.assertEqual(result["sentiment_reasoning"], "")
        self.assertFalse(contains_food_text_contamination(result["sentiment_reasoning"]))

    def test_contaminated_recommended_action_gets_food_safe_replacement(self):
        row = {
            "domain": "food",
            "source_label": "food_fsanz_recalls",
            "authority": "fsanz",
            "signal_type": "recall",
            "title": "Example Food Recall",
            "summary": "The recall is due to undeclared allergen.",
            "recommended_action": "Supplement companies should review supplier exposure.",
        }
        result = enrich_food_signal(row)
        self.assertIn("Review the recall notice", result["recommended_action"])
        self.assertFalse(contains_food_text_contamination(result["recommended_action"]))

    def test_summary_removes_only_contaminated_sentence(self):
        row = {
            "domain": "food",
            "source_label": "food_fsanz_recalls",
            "authority": "fsanz",
            "signal_type": "recall",
            "title": "Example Food Recall",
            "summary": (
                "The recall is due to foreign matter in the product. "
                "VMS products should review supplier risk."
            ),
        }
        result = enrich_food_signal(row)
        self.assertEqual(
            result["summary"],
            "The recall is due to foreign matter in the product.",
        )

    def test_supplement_leakage_exclusion_remains_intact(self):
        row = {
            "domain": "food",
            "source_label": "open_food_facts",
            "authority": "open_food_facts",
            "title": "Ostelin — Vitamin D3 1000IU",
            "product_name": "Vitamin D3 1000IU",
            "product_category": "dietary supplements",
        }
        self.assertTrue(is_supplement_like_food(row))
        self.assertEqual(enrich_food_signal(row)["dashboard_section"], "excluded")


if __name__ == "__main__":
    unittest.main()
