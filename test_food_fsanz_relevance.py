"""Regression tests for FSANZ update relevance gating."""

import unittest

from services.food_taxonomy import enrich_food_signal


def _fsanz_update(title: str, summary: str = "") -> dict:
    return {
        "domain": "food",
        "source_label": "food_fsanz_updates",
        "authority": "fsanz",
        "signal_type": "rule_update",
        "event_type": "rule_update",
        "title": title,
        "summary": summary,
    }


class FoodFSANZRelevanceTests(unittest.TestCase):
    def test_ceo_year_in_review_is_excluded_noise(self):
        result = enrich_food_signal(_fsanz_update("CEO year in review"))
        self.assertEqual(result["dashboard_section"], "excluded")
        self.assertEqual(result["signal_type"], "excluded")
        self.assertEqual(result["impact"], "low")
        self.assertEqual(result["momentum"], "stable")
        self.assertEqual(result["is_noise"], 1)
        self.assertEqual(result["fsanz_content_type"], "corporate_admin")

    def test_generic_food_standards_news_is_excluded(self):
        result = enrich_food_signal(
            _fsanz_update("Food Standards NewsRead the latest news from FSANZ.")
        )
        self.assertEqual(result["dashboard_section"], "excluded")
        self.assertEqual(result["fsanz_content_type"], "newsletter")

    def test_food_standards_news_with_specific_topic_is_visible(self):
        result = enrich_food_signal(
            _fsanz_update(
                "Food Standards News: call for comment on allergen labelling",
                "FSANZ seeks comment on allergen labelling requirements.",
            )
        )
        self.assertNotEqual(result["dashboard_section"], "excluded")
        self.assertIn(result["dashboard_section"], {"regulatory_updates", "claims_labelling"})

    def test_health_star_rating_call_for_comment_remains_visible(self):
        result = enrich_food_signal(
            _fsanz_update("Call for comment on the Health Star Rating system")
        )
        self.assertNotEqual(result["dashboard_section"], "excluded")
        self.assertEqual(result["signal_type"], "consultation")

    def test_cadmium_maximum_level_call_for_comment_remains_visible(self):
        result = enrich_food_signal(
            _fsanz_update("Call for comment on maximum level for cadmium in NT Blacklip Rock oysters")
        )
        self.assertNotEqual(result["dashboard_section"], "excluded")
        self.assertEqual(result["signal_type"], "consultation")

    def test_recall_related_media_statement_remains_recalls_safety(self):
        result = enrich_food_signal(
            _fsanz_update(
                "Media statement on recall of infant formula due to potential toxin contamination",
                "Two companies have recalled infant formula products nationally.",
            )
        )
        self.assertEqual(result["dashboard_section"], "recalls_safety")
        self.assertEqual(result["signal_type"], "recall")

    def test_food_safety_survey_findings_remain_visible(self):
        result = enrich_food_signal(
            _fsanz_update(
                "FSANZ releases findings from national survey of antimicrobial resistance in raw retail meats",
                "Survey findings inform public health and microbiological risk monitoring.",
            )
        )
        self.assertNotEqual(result["dashboard_section"], "excluded")
        self.assertEqual(result["fsanz_content_type"], "surveillance_report")


if __name__ == "__main__":
    unittest.main()
