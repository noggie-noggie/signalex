"""
scrapers/fda_australia.py — FDA signals filtered for Australian market relevance.

Wraps the existing FDAScraper with a second Claude classification pass that
assesses how likely each FDA notice is to flow through to the TGA within
12 months.

Only signals with australia_relevance = "high" or "medium" are returned.
This keeps the Australia-focused digest concise and actionable.

Usage:
    from scrapers.fda_australia import FDAAustraliaFilter

    filt = FDAAustraliaFilter()
    classified_signals = filt.fetch_and_classify()
    # All returned signals have australia_relevance in ("high", "medium")
"""

from __future__ import annotations

import logging

import config
from scrapers.fda import FDAScraper
from classifier.claude import SignalClassifier, ClassifiedSignal

logger = logging.getLogger(__name__)


class FDAAustraliaFilter:
    """
    Fetch FDA dietary supplement signals and filter to those most likely to
    affect the Australian market.

    Steps:
      1. Run FDAScraper to collect raw FDA signals.
      2. Pass each signal through SignalClassifier.classify_fda_australia()
         which runs two Claude calls:
           a. Standard classification (severity, ingredient, event type)
           b. Australia-relevance assessment (australia_relevance + reasoning)
      3. Return only signals where australia_relevance in ("high", "medium").
    """

    def __init__(self) -> None:
        self._fda = FDAScraper(config.SCRAPER_CONFIG["fda"])
        self._classifier = SignalClassifier()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch_and_classify(self) -> list[ClassifiedSignal]:
        """
        Run the full pipeline: scrape → classify → filter.
        Returns ClassifiedSignal list, all with australia_relevance high/medium.
        """
        logger.info("FDA-Australia: fetching FDA signals")
        raw_signals = self._fda.run()
        logger.info("FDA-Australia: %d raw signal(s) to assess", len(raw_signals))

        if not raw_signals:
            return []

        classified = self._classifier.classify_batch_fda_australia(raw_signals)

        relevant = [
            s for s in classified
            if s.australia_relevance in ("high", "medium")
        ]
        logger.info(
            "FDA-Australia: %d/%d signal(s) with high/medium AU relevance",
            len(relevant), len(classified),
        )
        return relevant

    def fetch_raw(self):
        """
        Compatibility shim — returns raw FDA signals without classification.
        Call fetch_and_classify() for the full pipeline.
        """
        return self._fda.run()
