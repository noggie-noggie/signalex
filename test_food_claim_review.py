"""Regression tests for deterministic free-text food claim review."""

import os
from unittest.mock import patch
import unittest

from services.food_claim_review import response_field_names, review_food_claim
from services.openai_claim_review import _reset_ai_state_for_tests


class FoodClaimReviewTests(unittest.TestCase):
    def setUp(self):
        _reset_ai_state_for_tests()

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
        self.assertIn("context", body)

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
        self.assertEqual(body["context"]["food_type"], "protein_bar")

    def test_unknown_harmless_wording_returns_review_required(self):
        body = review_food_claim("Fresh bright taste", food_type="drink")
        self.assert_frontend_fields(body)
        self.assertEqual(body["risk_level"], "review_required")
        self.assertEqual(body["claim_type"], "unclassified_food_claim")
        self.assertEqual(body["recommended_pathways"], [])
        self.assertGreater(len(body["missing_information"]), 0)
        self.assertFalse(body["ai_used"])
        self.assertEqual(body["assessment_mode"], "deterministic")
        self.assertFalse(body["cache_hit"])

    def test_ai_disabled_returns_deterministic_response(self):
        with patch.dict(os.environ, {"FOOD_CLAIM_REVIEW_AI_ENABLED": "false", "OPENAI_API_KEY": "test"}):
            with patch("services.openai_claim_review._call_openai") as call:
                body = review_food_claim("Fresh bright taste", food_type="drink", use_ai=True)
        self.assertEqual(body["risk_level"], "review_required")
        self.assertFalse(body["ai_used"])
        call.assert_not_called()

    def test_ai_enabled_missing_key_does_not_crash(self):
        with patch.dict(os.environ, {"FOOD_CLAIM_REVIEW_AI_ENABLED": "true", "OPENAI_API_KEY": ""}, clear=False):
            with patch("services.openai_claim_review._call_openai") as call:
                body = review_food_claim("Fresh bright taste", food_type="drink", use_ai=True)
        self.assertEqual(body["risk_level"], "review_required")
        self.assertFalse(body["ai_used"])
        self.assertFalse(body["ai_available"])
        call.assert_not_called()

    def test_high_protein_does_not_call_ai_by_default(self):
        with patch.dict(os.environ, {"FOOD_CLAIM_REVIEW_AI_ENABLED": "true", "OPENAI_API_KEY": "test"}):
            with patch("services.openai_claim_review._call_openai") as call:
                body = review_food_claim("High in protein", food_type="protein bar", use_ai=True)
        self.assertEqual(body["display_claim"], "High in protein")
        self.assertFalse(body["ai_used"])
        call.assert_not_called()

    def test_ibs_support_does_not_call_ai_by_default(self):
        with patch.dict(os.environ, {"FOOD_CLAIM_REVIEW_AI_ENABLED": "true", "OPENAI_API_KEY": "test"}):
            with patch("services.openai_claim_review._call_openai") as call:
                body = review_food_claim("ibs support", food_type="Yoghurt", use_ai=True)
        self.assertEqual(body["risk_level"], "high")
        self.assertFalse(body["ai_used"])
        call.assert_not_called()

    def test_unknown_vague_claim_attempts_ai_when_enabled_and_key_present(self):
        ai_payload = {
            "assessment": "AI-assisted wording review remains non-final and requires substantiation.",
            "safer_wording": ["Supports everyday wellbeing"],
            "missing_information": ["Full formulation"],
            "recommended_action": "Review formulation and evidence before use.",
            "matched_themes": ["general_wellbeing"],
        }
        with patch.dict(os.environ, {"FOOD_CLAIM_REVIEW_AI_ENABLED": "true", "OPENAI_API_KEY": "test"}):
            with patch("services.openai_claim_review._call_openai", return_value=ai_payload) as call:
                body = review_food_claim("supports balance", food_type="drink", use_ai=True)
        self.assertTrue(body["ai_used"])
        self.assertTrue(body["ai_available"])
        self.assertEqual(body["assessment_mode"], "ai_assisted")
        self.assertFalse(body["cache_hit"])
        self.assertEqual(body["assessment"], ai_payload["assessment"])
        self.assertEqual(body["matched_themes"], ["general_wellbeing"])
        call.assert_called_once()

    def test_openai_package_missing_falls_back(self):
        with patch.dict(os.environ, {"FOOD_CLAIM_REVIEW_AI_ENABLED": "true", "OPENAI_API_KEY": "test"}):
            with patch("services.openai_claim_review._call_openai", side_effect=ImportError("missing")):
                body = review_food_claim("supports balance", food_type="drink", use_ai=True)
        self.assertEqual(body["risk_level"], "review_required")
        self.assertFalse(body["ai_used"])

    def test_ai_cache_prevents_second_call(self):
        ai_payload = {
            "assessment": "Cached AI review.",
            "safer_wording": ["General wellbeing"],
            "missing_information": ["Nutrition panel"],
            "recommended_action": "Review before use.",
            "matched_themes": ["general_wellbeing"],
        }
        with patch.dict(os.environ, {"FOOD_CLAIM_REVIEW_AI_ENABLED": "true", "OPENAI_API_KEY": "test"}):
            with patch("services.openai_claim_review._call_openai", return_value=ai_payload) as call:
                first = review_food_claim("supports balance", food_type="drink", use_ai=True, client_ip="1.2.3.4")
                second = review_food_claim("supports balance", food_type="drink", use_ai=True, client_ip="1.2.3.4")
        self.assertTrue(first["ai_used"])
        self.assertTrue(second["ai_used"])
        self.assertFalse(first["cache_hit"])
        self.assertTrue(second["cache_hit"])
        self.assertEqual(first["ai_quota_remaining"], second["ai_quota_remaining"])
        self.assertEqual(call.call_count, 1)

    def test_quota_exceeded_falls_back_deterministic(self):
        with patch.dict(os.environ, {
            "FOOD_CLAIM_REVIEW_AI_ENABLED": "true",
            "OPENAI_API_KEY": "test",
            "FOOD_CLAIM_REVIEW_AI_MAX_DAILY": "0",
        }):
            with patch("services.openai_claim_review._call_openai") as call:
                body = review_food_claim("supports balance", food_type="drink", use_ai=True)
        self.assertFalse(body["ai_used"])
        self.assertFalse(body["ai_available"])
        self.assertEqual(body["assessment_mode"], "deterministic")
        self.assertEqual(body["ai_quota_remaining"], 0)
        self.assertIn("upgrade_prompt", body)
        call.assert_not_called()

    def test_per_ip_quota_exceeded_falls_back_deterministic(self):
        ai_payload = {
            "assessment": "AI one.",
            "safer_wording": ["General wellbeing"],
            "missing_information": ["Nutrition panel"],
            "recommended_action": "Review before use.",
            "matched_themes": ["general_wellbeing"],
        }
        with patch.dict(os.environ, {
            "FOOD_CLAIM_REVIEW_AI_ENABLED": "true",
            "OPENAI_API_KEY": "test",
            "FOOD_CLAIM_REVIEW_AI_MAX_DAILY": "10",
            "FOOD_CLAIM_REVIEW_AI_MAX_PER_IP_DAILY": "1",
            "FOOD_CLAIM_REVIEW_AI_CACHE_ENABLED": "false",
        }):
            with patch("services.openai_claim_review._call_openai", return_value=ai_payload) as call:
                first = review_food_claim("supports balance one", food_type="drink", use_ai=True, client_ip="1.2.3.4")
                second = review_food_claim("supports balance two", food_type="drink", use_ai=True, client_ip="1.2.3.4")
        self.assertTrue(first["ai_used"])
        self.assertFalse(second["ai_used"])
        self.assertEqual(second["ai_quota_remaining"], 0)
        self.assertEqual(call.call_count, 1)

    def test_claim_location_front_of_pack_normalises(self):
        body = review_food_claim(
            "High in protein",
            food_type="Protein bar",
            claim_location="front_of_pack",
        )
        self.assertEqual(body["context"]["claim_location"], "front_of_pack")
        self.assertIn("Front-of-pack wording", body["recommended_action"])

    def test_claim_location_display_label_normalises(self):
        body = review_food_claim(
            "High in protein",
            food_type="Protein bar",
            claim_location="Front of pack",
        )
        self.assertEqual(body["context"]["claim_location"], "front_of_pack")

    def test_valid_serving_size_is_echoed_and_filters_missing_information(self):
        body = review_food_claim(
            "Fresh bright taste",
            food_type="Beverage",
            serving_size={"value": "15", "unit": "g"},
        )
        self.assertEqual(body["context"]["serving_size"], {"value": "15", "unit": "g"})
        self.assertTrue(all("serving size" not in item.lower() for item in body["missing_information"]))

    def test_invalid_serving_size_does_not_crash(self):
        body = review_food_claim(
            "Fresh bright taste",
            food_type="Beverage",
            serving_size={"value": "15", "unit": "oz"},
        )
        self.assertIsNone(body["context"]["serving_size"])
        self.assertEqual(body["risk_level"], "review_required")

    def test_supplement_like_food_adds_classification_caution(self):
        body = review_food_claim(
            "Supports wellbeing",
            food_type="Supplement-like food",
        )
        self.assertEqual(body["context"]["food_type"], "supplement_like_food")
        self.assertIn("supplement-like", body["recommended_action"])
        self.assertIn("therapeutic/supplement-style", body["regulatory_context"])

    def test_context_passed_to_ai_helper(self):
        ai_payload = {
            "assessment": "AI context aware review.",
            "safer_wording": ["Supports wellbeing"],
            "missing_information": ["Nutrition panel"],
            "recommended_action": "Review before use.",
            "matched_themes": ["general_wellbeing"],
        }
        with patch.dict(os.environ, {"FOOD_CLAIM_REVIEW_AI_ENABLED": "true", "OPENAI_API_KEY": "test"}):
            with patch("services.openai_claim_review._call_openai", return_value=ai_payload) as call:
                body = review_food_claim(
                    "supports balance",
                    food_type="Yoghurt",
                    claim_location="Marketing / Advertising",
                    serving_size={"value": "150", "unit": "g"},
                    use_ai=True,
                )
        self.assertTrue(body["ai_used"])
        _, _claim, food_type, _jurisdiction, context = call.call_args.args
        self.assertEqual(food_type, "yoghurt")
        self.assertEqual(context["claim_location"], "marketing_advertising")
        self.assertEqual(context["serving_size"], {"value": "150", "unit": "g"})


if __name__ == "__main__":
    unittest.main()
