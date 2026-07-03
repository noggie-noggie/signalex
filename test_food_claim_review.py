"""Regression tests for deterministic free-text food claim review."""

import unittest

from services.food_claim_review import response_field_names, review_food_claim


class FoodClaimReviewTests(unittest.TestCase):
    def assert_frontend_fields(self, body):
        for field in response_field_names():
            self.assertIn(field, body)

    def test_ibs_support_is_high_risk_therapeutic(self):
        body = review_food_claim("ibs support", food_type="Yoghurt", jurisdiction="AU/NZ")
        self.assert_frontend_fields(body)
        self.assertEqual(body["risk_level"], "high")
        self.assertEqual(body["claim_type"], "therapeutic_or_disease_related_claim")
        self.assertIn("IBS", body["assessment"])
        self.assertEqual(body["recommended_pathways"], [])
        self.assertIn("Supports digestive wellbeing", body["safer_wording"])
        self.assertIn("Contains live cultures", body["safer_wording"])
        self.assertIn("Contains fibre to support digestive health", body["safer_wording"])
        self.assertFalse(body["ai_used"])

    def test_high_in_protein_uses_high_protein_pathway(self):
        body = review_food_claim("High in protein", food_type="protein bar")
        self.assert_frontend_fields(body)
        self.assertEqual(body["risk_level"], "medium")
        self.assertEqual(body["display_claim"], "High in protein")
        self.assertIn("high_protein", body["matched_themes"])
        self.assertGreater(len(body["recommended_pathways"]), 0)
        self.assertGreater(len(body["wording_to_avoid"]), 0)
        self.assertGreater(len(body["safer_wording"]), 0)
        self.assertFalse(body["ai_used"])

    def test_unknown_harmless_wording_returns_review_required(self):
        body = review_food_claim("Fresh bright taste", food_type="drink")
        self.assert_frontend_fields(body)
        self.assertEqual(body["risk_level"], "review_required")
        self.assertEqual(body["claim_type"], "unclassified_food_claim")
        self.assertEqual(body["recommended_pathways"], [])
        self.assertGreater(len(body["missing_information"]), 0)
        self.assertFalse(body["ai_used"])


if __name__ == "__main__":
    unittest.main()
