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
        self.assertIn("IBS and similar condition-specific wording", body["regulatory_context"])
        self.assertIn("Treats IBS", body["wording_to_avoid"])
        self.assertIn("Cures IBS", body["wording_to_avoid"])
        self.assertIn("Relieves IBS symptoms", body["wording_to_avoid"])
        self.assertIn("Reduces IBS symptoms", body["wording_to_avoid"])
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
        self.assertEqual(body["possible_supporting_routes"], [])
        self.assertGreater(len(body["wording_to_avoid"]), 0)
        self.assertGreater(len(body["safer_wording"]), 0)
        self.assertFalse(body["ai_used"])
        self.assertEqual(body["context"]["food_type"], "protein_bar")
        self.assertFalse(body["multi_claim"])
        self.assertEqual(len(body["claim_breakdown"]), 1)

    def test_unknown_harmless_wording_returns_review_required(self):
        body = review_food_claim("Fresh bright taste", food_type="drink")
        self.assert_frontend_fields(body)
        self.assertEqual(body["risk_level"], "review_required")
        self.assertEqual(body["claim_type"], "unclassified_food_claim")
        self.assertEqual(body["recommended_pathways"], [])
        self.assertEqual(body["possible_supporting_routes"], [])
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

    def test_multi_claim_high_protein_gut_health_immunity_breakdown(self):
        body = review_food_claim(
            "High in protein. Supports gut health and immunity.",
            food_type="Yoghurt",
        )
        self.assert_frontend_fields(body)
        self.assertTrue(body["multi_claim"])
        self.assertEqual(body["risk_level"], "medium")
        claims = [item["claim"] for item in body["claim_breakdown"]]
        self.assertIn("High in protein", claims)
        self.assertIn("Supports gut health", claims)
        self.assertIn("Supports immunity", claims)
        by_claim = {item["claim"]: item for item in body["claim_breakdown"]}
        self.assertEqual(by_claim["High in protein"]["claim_type"], "nutrition_content_claim")
        self.assertIn("high_protein", by_claim["High in protein"]["matched_themes"])
        self.assertEqual(by_claim["Supports gut health"]["claim_type"], "general_health_or_function_claim")
        self.assertIn("gut_health", by_claim["Supports gut health"]["matched_themes"])
        self.assertEqual(by_claim["Supports immunity"]["claim_type"], "general_health_or_function_claim")
        self.assertIn("immunity", by_claim["Supports immunity"]["matched_themes"])
        self.assertIn("Multiple claims were detected", body["overall_note"])

    def test_multi_claim_ibs_and_gut_health_overall_high(self):
        body = review_food_claim("Supports IBS and gut health", food_type="Yoghurt")
        self.assert_frontend_fields(body)
        self.assertTrue(body["multi_claim"])
        self.assertEqual(body["risk_level"], "high")
        claim_types = [item["claim_type"] for item in body["claim_breakdown"]]
        self.assertIn("therapeutic_or_disease_related_claim", claim_types)
        self.assertIn("general_health_or_function_claim", claim_types)
        self.assertFalse(body["ai_used"])

    def test_multi_claim_comma_list_includes_improves_immunity(self):
        body = review_food_claim("gut health, high in protein, improves immunity", food_type="Yoghurt")
        self.assert_frontend_fields(body)
        self.assertTrue(body["multi_claim"])
        claims = [item["claim"] for item in body["claim_breakdown"]]
        self.assertIn("gut health", claims)
        self.assertIn("high in protein", claims)
        self.assertIn("improves immunity", claims)
        immunity = {item["claim"]: item for item in body["claim_breakdown"]}["improves immunity"]
        self.assertEqual(immunity["claim_type"], "general_health_or_function_claim")
        self.assertIn("immunity", immunity["matched_themes"])
        self.assertIn("supports normal immune function", immunity["recommended_action"])
        self.assertIn("Improves immunity", body["wording_to_avoid"])
        self.assertIn("Supports normal immune function", body["safer_wording"])
        self.assertTrue(any(pathway["name"] == "Immunity support route" for pathway in body["recommended_pathways"]))
        main_route_names = [pathway["name"] for pathway in body["recommended_pathways"]]
        self.assertNotIn("Vitamin C route", main_route_names)
        self.assertNotIn("Zinc route", main_route_names)
        self.assertNotIn("Vitamin D route", main_route_names)
        supporting_route_names = [pathway["name"] for pathway in body["possible_supporting_routes"]]
        self.assertIn("Vitamin C route", supporting_route_names)
        self.assertIn("Zinc route", supporting_route_names)
        self.assertIn("Vitamin D route", supporting_route_names)
        self.assertIn("Live cultures route", supporting_route_names)
        self.assertIn("These routes are conditional", body["overall_note"])

    def test_multi_claim_and_join_includes_helps_immunity(self):
        body = review_food_claim("supports gut health, high in protein and helps immunity", food_type="Yoghurt")
        self.assert_frontend_fields(body)
        self.assertTrue(body["multi_claim"])
        claims = [item["claim"] for item in body["claim_breakdown"]]
        self.assertIn("supports gut health", claims)
        self.assertIn("high in protein", claims)
        self.assertIn("helps immunity", claims)
        immunity = {item["claim"]: item for item in body["claim_breakdown"]}["helps immunity"]
        self.assertIn("immunity", immunity["matched_themes"])

    def test_boosts_immunity_adds_avoid_wording(self):
        body = review_food_claim("boosts immunity", food_type="Beverage")
        self.assert_frontend_fields(body)
        self.assertIn("immunity", body["matched_themes"])
        self.assertIn("Boosts immunity", body["wording_to_avoid"])
        self.assertIn("Supports normal immune function", body["safer_wording"])

    def test_prevents_colds_is_high_risk_therapeutic(self):
        body = review_food_claim("prevents colds", food_type="Beverage")
        self.assert_frontend_fields(body)
        self.assertEqual(body["risk_level"], "high")
        self.assertEqual(body["claim_type"], "therapeutic_or_disease_related_claim")
        self.assertIn("prevents colds", [term.lower() for term in body["wording_to_avoid"]])
        self.assertEqual(body["recommended_pathways"], [])

    def test_multi_claim_does_not_overload_recommended_pathways(self):
        body = review_food_claim("gut health, high in protein, improves immunity", food_type="Yoghurt")
        main_route_names = [pathway["name"] for pathway in body["recommended_pathways"]]
        self.assertEqual(main_route_names, ["Protein route", "Immunity support route"])
        supporting_route_names = [pathway["name"] for pathway in body["possible_supporting_routes"]]
        self.assertIn("Vitamin C route", supporting_route_names)
        self.assertIn("Live cultures pathway", supporting_route_names)

    def test_supports_muscle_strength_claim_family(self):
        body = review_food_claim("supports muscle strength", food_type="Protein bar")
        self.assert_frontend_fields(body)
        self.assertIn(body["risk_level"], {"medium", "review_required"})
        self.assertEqual(body["claim_type"], "general_health_or_function_claim")
        self.assertIn("muscle_performance", body["matched_themes"])
        self.assertTrue(any(route["name"] == "Protein / muscle function route" for route in body["recommended_pathways"]))

    def test_repairs_muscle_damage_remains_high_risk(self):
        body = review_food_claim("repairs muscle damage", food_type="Protein bar")
        self.assert_frontend_fields(body)
        self.assertEqual(body["risk_level"], "high")
        self.assertEqual(body["claim_type"], "therapeutic_or_disease_related_claim")
        self.assertEqual(body["recommended_pathways"], [])
        self.assertIn("Repair or healing language should be removed", body["regulatory_context"])
        self.assertNotIn("IBS", body["regulatory_context"])
        self.assertIn("Repairs muscle damage", body["wording_to_avoid"])
        self.assertIn("Speeds injury recovery", body["wording_to_avoid"])
        self.assertNotIn("Treats IBS", body["wording_to_avoid"])
        self.assertNotIn("Cures IBS", body["wording_to_avoid"])
        self.assertNotIn("Relieves IBS symptoms", body["wording_to_avoid"])

    def test_supports_collagen_formation_claim_family(self):
        body = review_food_claim("supports collagen formation", food_type="Beverage")
        self.assert_frontend_fields(body)
        self.assertIn(body["risk_level"], {"medium", "review_required"})
        self.assertEqual(body["claim_type"], "general_health_or_function_claim")
        self.assertIn("collagen_skin", body["matched_themes"])
        self.assertTrue(any(route["name"] == "Collagen / skin support route" for route in body["recommended_pathways"]))

    def test_repairs_collagen_and_heals_joints_high_risk(self):
        body = review_food_claim("repairs collagen and heals joints", food_type="Beverage")
        self.assert_frontend_fields(body)
        self.assertTrue(body["multi_claim"])
        self.assertEqual(body["risk_level"], "high")
        self.assertEqual(body["claim_type"], "therapeutic_or_disease_related_claim")
        self.assertEqual(body["recommended_pathways"], [])
        self.assertIn("Repair or healing language should be removed", body["regulatory_context"])
        self.assertNotIn("IBS", body["regulatory_context"])
        self.assertIn("Repairs collagen", body["wording_to_avoid"])
        self.assertIn("Heals joints", body["wording_to_avoid"])
        self.assertIn("Rebuilds cartilage", body["wording_to_avoid"])
        self.assertNotIn("Treats IBS", body["wording_to_avoid"])
        self.assertNotIn("Cures IBS", body["wording_to_avoid"])
        self.assertNotIn("Relieves IBS symptoms", body["wording_to_avoid"])

    def test_supports_hydration_with_electrolytes(self):
        body = review_food_claim("supports hydration with electrolytes", food_type="Beverage")
        self.assert_frontend_fields(body)
        self.assertIn("hydration_electrolytes", body["matched_themes"])
        self.assertTrue(any(route["name"] == "Hydration / electrolyte route" for route in body["recommended_pathways"]))

    def test_boosts_energy_and_reduces_fatigue_multi_claim(self):
        body = review_food_claim("boosts energy and reduces fatigue", food_type="Beverage")
        self.assert_frontend_fields(body)
        self.assertTrue(body["multi_claim"])
        themes = [theme for item in body["claim_breakdown"] for theme in item["matched_themes"]]
        self.assertIn("energy", themes)
        self.assertTrue(any(route["name"] == "Energy metabolism route" for route in body["recommended_pathways"]))
        self.assertIn("Treats fatigue", body["wording_to_avoid"])

    def test_supports_focus_and_mood_multi_claim(self):
        body = review_food_claim("supports focus and mood", food_type="Snack")
        self.assert_frontend_fields(body)
        self.assertTrue(body["multi_claim"])
        themes = [theme for item in body["claim_breakdown"] for theme in item["matched_themes"]]
        self.assertIn("brain_focus_mood", themes)
        self.assertTrue(any(route["name"] == "Focus / mood support route" for route in body["recommended_pathways"]))

    def test_helps_sleep_claim_family(self):
        body = review_food_claim("helps sleep", food_type="Beverage")
        self.assert_frontend_fields(body)
        self.assertIn("sleep_calm", body["matched_themes"])
        self.assertTrue(any(route["name"] == "Sleep / calm support route" for route in body["recommended_pathways"]))

    def test_supports_heart_health_claim_family(self):
        body = review_food_claim("supports heart health", food_type="Snack")
        self.assert_frontend_fields(body)
        self.assertIn("heart_cholesterol", body["matched_themes"])
        self.assertTrue(any(route["name"] == "Heart health support route" for route in body["recommended_pathways"]))

    def test_supports_healthy_blood_sugar_claim_family(self):
        body = review_food_claim("supports healthy blood sugar", food_type="Cereal")
        self.assert_frontend_fields(body)
        self.assertIn("blood_sugar", body["matched_themes"])
        self.assertTrue(any(route["name"] == "Blood sugar / glycaemic route" for route in body["recommended_pathways"]))

    def test_multi_claim_family_breakdown_for_each_detected_claim(self):
        body = review_food_claim(
            "supports muscle function, supports collagen formation, supports hydration with electrolytes",
            food_type="Beverage",
        )
        self.assert_frontend_fields(body)
        self.assertTrue(body["multi_claim"])
        claims = [item["claim"] for item in body["claim_breakdown"]]
        self.assertIn("supports muscle function", claims)
        self.assertIn("supports collagen formation", claims)
        self.assertIn("supports hydration with electrolytes", claims)


if __name__ == "__main__":
    unittest.main()
