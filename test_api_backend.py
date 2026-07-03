"""Focused regression tests for API health, CORS, and domain contracts."""

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.server import _cors_origins, app
from services.food_taxonomy import classify_food_signal


class ApiBackendTests(unittest.TestCase):
    def test_configured_cors_origins_are_added(self):
        with patch.dict(
            "os.environ",
            {"CORS_ORIGINS": "https://food.example.com, https://www.food.example.com/"},
        ):
            origins = _cors_origins()
        self.assertIn("http://localhost:5173", origins)
        self.assertIn("https://food.example.com", origins)
        self.assertIn("https://www.food.example.com", origins)

    def test_food_and_pharma_endpoints(self):
        with TestClient(app) as client:
            for path in [
                "/api/food/dashboard",
                "/api/food/recalls?limit=1",
                "/api/food/rules?limit=1",
                "/api/food/products?limit=1",
                "/api/food/claim-pathways",
                "/api/signals?domain=food&limit=1",
                "/api/citations?limit=1",
            ]:
                with self.subTest(path=path):
                    self.assertEqual(client.get(path).status_code, 200)

            food_response = client.get("/api/signals?domain=food&limit=1")
            first = food_response.json()["results"][0]
            for field in [
                "market",
                "category",
                "product_type",
                "ingredient",
                "issue_area",
                "claim_theme",
                "signal_type",
                "source_type",
                "dashboard_section",
                "impact",
                "momentum",
            ]:
                self.assertIn(field, first)

    def test_food_taxonomy_reclassifies_recall_like_updates(self):
        row = {
            "domain": "food",
            "source_label": "food_fsanz_updates",
            "authority": "fsanz",
            "signal_type": "rule_update",
            "event_type": "rule_update",
            "title": "Media statement on recall of infant formula due to potential toxin contamination",
            "summary": "Two companies have recalled infant formula products nationally.",
        }
        taxonomy = classify_food_signal(row)
        self.assertEqual(taxonomy["signal_type"], "recall")
        self.assertEqual(taxonomy["dashboard_section"], "recalls_safety")
        self.assertIn("food_safety", taxonomy["issue_area"])

    @patch("api.server.save_guidance", lambda *args, **kwargs: None)
    @patch("api.server.get_cached_guidance", lambda _input_hash: None)
    def test_food_claim_post_and_cors_preflight(self):
        with TestClient(app) as client:
            preflight = client.options(
                "/api/food/claims/guide",
                headers={
                    "Origin": "http://localhost:5173",
                    "Access-Control-Request-Method": "POST",
                    "Access-Control-Request-Headers": "content-type",
                },
            )
            self.assertEqual(preflight.status_code, 200)
            self.assertIn("POST", preflight.headers["access-control-allow-methods"])

            response = client.post(
                "/api/food/claims/guide",
                json={
                    "claim": "Supports gut health",
                    "food_type": "yoghurt drink",
                    "market": "Australia",
                },
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                response.json()["assessment_level"],
                "concept_guidance",
            )

    def test_food_claim_pathways_returns_list(self):
        with TestClient(app) as client:
            response = client.get("/api/food/claim-pathways")
            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertGreater(body["total"], 0)
            self.assertIsInstance(body["results"], list)
            self.assertIn("claim", body["results"][0])

    def test_food_claim_pathways_high_protein(self):
        with TestClient(app) as client:
            response = client.get("/api/food/claim-pathways?claim=high_protein")
            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertEqual(body["claim"], "high_protein")
            self.assertEqual(body["display_claim"], "High in protein")
            self.assertEqual(body["risk_level"], "medium")
            for field in [
                "recommended_pathways",
                "wording_to_avoid",
                "missing_information",
                "safer_wording",
            ]:
                self.assertIn(field, body)
                self.assertGreater(len(body[field]), 0)

    def test_food_claim_pathways_matches_display_claim(self):
        with TestClient(app) as client:
            response = client.get(
                "/api/food/claim-pathways",
                params={"claim": "High in protein"},
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["claim"], "high_protein")

    def test_food_claim_pathways_unknown_claim_returns_controlled_not_found(self):
        with TestClient(app) as client:
            response = client.get("/api/food/claim-pathways?claim=moon_dust")
            self.assertEqual(response.status_code, 404)
            detail = response.json()["detail"]
            self.assertEqual(detail["status"], "not_found")
            self.assertEqual(detail["claim"], "moon_dust")

    def test_health_reports_all_data_sources(self):
        with TestClient(app) as client:
            response = client.get("/api/health")
            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertTrue(body["signals"]["readable"])
            self.assertGreaterEqual(body["signals"]["food"], 0)
            self.assertGreaterEqual(body["signals"]["vms"], 0)
            self.assertTrue(body["citations"]["loaded"])
            self.assertEqual(body["citations"]["sourceOfTruthFor"], "pharma")


if __name__ == "__main__":
    unittest.main()
