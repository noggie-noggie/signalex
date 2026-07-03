"""Regression tests for optional Food AI enrichment configuration."""

from __future__ import annotations

import importlib
import os
import sys
import types
import unittest
from unittest.mock import Mock, patch


def _reload_config_with_env(env: dict[str, str]):
    with patch.dict(os.environ, env, clear=True):
        sys.modules.pop("config", None)
        import config

        return importlib.reload(config)


class FoodAIConfigTests(unittest.TestCase):
    def test_importing_config_without_anthropic_key_does_not_fail(self):
        config = _reload_config_with_env({
            "FOOD_AI_ENRICHMENT_ENABLED": "false",
            "ANTHROPIC_API_KEY": "",
            "OPENAI_API_KEY": "",
        })

        self.assertFalse(config.FOOD_AI_ENRICHMENT_ENABLED)
        self.assertEqual(config.ANTHROPIC_API_KEY, "")

    def test_food_pipeline_starts_with_enrichment_disabled_and_no_anthropic_key(self):
        fake_recalls = types.ModuleType("scrapers.food_fsanz_recalls")
        fake_updates = types.ModuleType("scrapers.food_fsanz_updates")
        fake_off = types.ModuleType("scrapers.open_food_facts")
        fake_recalls.FoodFSANZRecallsScraper = Mock()
        fake_updates.FoodFSANZUpdatesScraper = Mock()
        fake_off.OpenFoodFactsScraper = Mock()

        with patch.dict(os.environ, {
            "FOOD_AI_ENRICHMENT_ENABLED": "false",
            "ANTHROPIC_API_KEY": "",
            "OPENAI_API_KEY": "",
        }, clear=True), patch.dict(sys.modules, {
            "scrapers.food_fsanz_recalls": fake_recalls,
            "scrapers.food_fsanz_updates": fake_updates,
            "scrapers.open_food_facts": fake_off,
        }):
            sys.modules.pop("config", None)
            sys.modules.pop("scheduler.food_pipeline", None)
            import scheduler.food_pipeline as food_pipeline

            importlib.reload(food_pipeline.config)
            food_pipeline = importlib.reload(food_pipeline)

            scraper_instance = Mock()
            scraper_instance.run.return_value = []
            scraper_class = Mock(return_value=scraper_instance)

            with patch.object(food_pipeline, "FoodFSANZRecallsScraper", scraper_class), \
                 patch.object(food_pipeline, "FoodFSANZUpdatesScraper", scraper_class), \
                 patch.object(food_pipeline, "OpenFoodFactsScraper", scraper_class), \
                 patch.object(food_pipeline, "save_food_signals_batch", return_value=0):
                summary = food_pipeline.run_food_pipeline()

        self.assertEqual(summary["total_new"], 0)
        self.assertEqual(summary["source_counts"], {
            "fsanz_recalls": 0,
            "fsanz_updates": 0,
            "open_food_facts": 0,
        })

    def test_anthropic_enrichment_requires_anthropic_key(self):
        config = _reload_config_with_env({
            "FOOD_AI_ENRICHMENT_ENABLED": "true",
            "AI_PROVIDER": "anthropic",
            "ANTHROPIC_API_KEY": "",
            "OPENAI_API_KEY": "",
        })

        with self.assertRaisesRegex(RuntimeError, "ANTHROPIC_API_KEY"):
            config.validate_food_ai_config()

    def test_openai_enrichment_requires_openai_key(self):
        config = _reload_config_with_env({
            "FOOD_AI_ENRICHMENT_ENABLED": "true",
            "AI_PROVIDER": "openai",
            "ANTHROPIC_API_KEY": "",
            "OPENAI_API_KEY": "",
        })

        with self.assertRaisesRegex(RuntimeError, "OPENAI_API_KEY"):
            config.validate_food_ai_config()


if __name__ == "__main__":
    unittest.main()
