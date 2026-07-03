"""Regression tests for deterministic food claim pathway cards."""

import unittest

from services.food_claim_pathways import (
    get_claim_pathway,
    list_claim_pathways,
    normalize_claim_key,
)


class FoodClaimPathwayTests(unittest.TestCase):
    def test_list_claim_pathways_returns_list(self):
        pathways = list_claim_pathways()
        self.assertGreater(len(pathways), 0)
        self.assertIn("claim", pathways[0])

    def test_high_protein_pathway_shape(self):
        pathway = get_claim_pathway("high_protein")
        self.assertIsNotNone(pathway)
        assert pathway is not None
        self.assertEqual(pathway["claim"], "high_protein")
        self.assertEqual(pathway["display_claim"], "High in protein")
        self.assertEqual(pathway["risk_level"], "medium")
        for field in [
            "recommended_pathways",
            "wording_to_avoid",
            "missing_information",
            "safer_wording",
        ]:
            self.assertIn(field, pathway)
            self.assertGreater(len(pathway[field]), 0)

    def test_high_in_protein_matches_high_protein(self):
        self.assertEqual(normalize_claim_key("High in protein"), "high_protein")
        pathway = get_claim_pathway("High in protein")
        self.assertIsNotNone(pathway)
        assert pathway is not None
        self.assertEqual(pathway["claim"], "high_protein")

    def test_unknown_claim_returns_none(self):
        self.assertIsNone(get_claim_pathway("moon dust"))


if __name__ == "__main__":
    unittest.main()
