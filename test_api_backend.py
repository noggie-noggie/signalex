"""Focused regression tests for API health, CORS, and domain contracts."""

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.server import _cors_origins, app


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
                "/api/signals?domain=food&limit=1",
                "/api/citations?limit=1",
            ]:
                with self.subTest(path=path):
                    self.assertEqual(client.get(path).status_code, 200)

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
