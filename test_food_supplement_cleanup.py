"""Regression tests for Open Food Facts supplement noise-flag synchronization."""

import unittest

from migrations.clean_food_supplement_leakage import _planned_actions
from services.food_supplement_filter import NOISE_REASON


class FoodSupplementCleanupTests(unittest.TestCase):
    def _actions_by_id(self, rows):
        return {row["id"]: action for action, row, _reason in _planned_actions(rows)}

    def test_allowed_protein_bar_previously_marked_noise_is_cleared(self):
        rows = [{
            "id": 1,
            "title": "Example High Protein Bar",
            "product_name": "High Protein Bar",
            "product_category": "dietary supplements",
            "is_noise": 1,
            "noise_reason": NOISE_REASON,
        }]
        self.assertEqual(self._actions_by_id(rows)[1], "clear_noise")

    def test_excluded_pre_workout_becomes_noise(self):
        rows = [{
            "id": 2,
            "title": "Muscle Nation — Legacy Sport Pre Workout",
            "product_name": "Legacy Sport Pre Workout",
            "product_category": "beverages and beverages preparations",
            "is_noise": 0,
            "noise_reason": "",
        }]
        self.assertEqual(self._actions_by_id(rows)[2], "mark_noise")

    def test_ostelin_vitamin_d_remains_noise(self):
        rows = [{
            "id": 3,
            "title": "Ostelin — Vitamin D3 1000IU",
            "product_name": "Vitamin D3 1000IU",
            "product_category": "dietary supplements",
            "is_noise": 1,
            "noise_reason": NOISE_REASON,
        }]
        self.assertEqual(self._actions_by_id(rows), {})

    def test_clif_bar_remains_not_noise(self):
        rows = [{
            "id": 4,
            "title": "Clif Bar — Cool Mint Chocolate Natural Flavour Energy Bar",
            "product_name": "Cool Mint Chocolate Natural Flavour Energy Bar",
            "product_category": "snacks",
            "is_noise": 0,
            "noise_reason": "",
        }]
        self.assertEqual(self._actions_by_id(rows), {})

    def test_unrelated_noise_reason_is_preserved_for_allowed_product(self):
        rows = [{
            "id": 5,
            "title": "Example High Protein Bar",
            "product_name": "High Protein Bar",
            "product_category": "snacks",
            "is_noise": 1,
            "noise_reason": "Manual exclusion: duplicate source record",
        }]
        self.assertEqual(self._actions_by_id(rows), {})


if __name__ == "__main__":
    unittest.main()
