"""
scheduler/food_pipeline.py — Food domain ingestion pipeline.

Entry point: run_food_pipeline()

Workflow:
  1. Run FoodFSANZRecallsScraper   — FSANZ food recall notices
  2. Run FoodFSANZUpdatesScraper   — FSANZ standards/regulatory updates
  3. Run OpenFoodFactsScraper      — competitor product intelligence
  4. Persist all records to SQLite via analytics.db.save_food_signal()
  5. Return a summary dict

Each signal is tagged domain='food' so it is cleanly separated from
existing VMS and pharma signals in the signals table.

Usage:
    python -c "from scheduler.food_pipeline import run_food_pipeline; run_food_pipeline()"
    python main.py --food-pipeline   (if wired into main.py)
"""

from __future__ import annotations

import concurrent.futures
import logging
from datetime import datetime, timezone

import config
from scrapers.food_fsanz_recalls import FoodFSANZRecallsScraper
from scrapers.food_fsanz_updates import FoodFSANZUpdatesScraper
from scrapers.open_food_facts import OpenFoodFactsScraper
from analytics.db import save_food_signals_batch

logger = logging.getLogger(__name__)

_SOURCE_TIMEOUT_SECS = 3 * 60   # 3 min per scraper source


def run_food_pipeline() -> dict:
    """
    Run the full food ingestion pipeline.

    Returns a summary dict:
    {
        "started_at":      str (ISO),
        "elapsed_seconds": float,
        "source_counts":   { scraper_label: int_new_signals },
        "total_new":       int,
    }
    """
    started_at = datetime.now(timezone.utc)
    logger.info("Food pipeline started at %s", started_at.isoformat())
    config.validate_food_ai_config()

    scraper_jobs = [
        ("fsanz_recalls",  FoodFSANZRecallsScraper),
        ("fsanz_updates",  FoodFSANZUpdatesScraper),
        ("open_food_facts", OpenFoodFactsScraper),
    ]

    source_counts: dict[str, int] = {}

    for label, ScraperClass in scraper_jobs:
        logger.info("Food pipeline: running %s", label)
        try:
            scraper = ScraperClass()

            # Per-source timeout — skip source if it hangs
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                fut = ex.submit(scraper.run)
                try:
                    records = fut.result(timeout=_SOURCE_TIMEOUT_SECS)
                except concurrent.futures.TimeoutError:
                    logger.error(
                        "Food pipeline: %s timed out after %ds — skipping",
                        label, _SOURCE_TIMEOUT_SECS,
                    )
                    source_counts[label] = 0
                    continue

            logger.info("Food pipeline: %s returned %d records", label, len(records))
            saved = save_food_signals_batch(records)
            source_counts[label] = saved
            logger.info("Food pipeline: %s — %d new signals stored", label, saved)

        except Exception:
            logger.exception("Food pipeline: %s failed", label)
            source_counts[label] = 0

    total_new  = sum(source_counts.values())
    elapsed    = (datetime.now(timezone.utc) - started_at).total_seconds()

    summary = {
        "started_at":      started_at.isoformat(),
        "elapsed_seconds": round(elapsed, 1),
        "source_counts":   source_counts,
        "total_new":       total_new,
    }

    _print_summary(summary)
    return summary


def _print_summary(s: dict) -> None:
    print("\n" + "=" * 55)
    print("  FOOD PIPELINE RUN SUMMARY")
    print(f"  {s['started_at']}  ({s['elapsed_seconds']}s)")
    print("=" * 55)
    for src, count in s["source_counts"].items():
        print(f"   {src:30s}  {count:4d} new")
    print(f"   {'TOTAL':30s}  {s['total_new']:4d} new")
    print("=" * 55 + "\n")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    run_food_pipeline()
