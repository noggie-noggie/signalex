"""Scrape TGA, classify all signals with Claude, print results."""
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

import config
config.SIGNAL_LOOKBACK_DAYS = 365   # wide window so we see all 6 alerts

from scrapers.tga import TGAScraper
from classifier.claude import SignalClassifier

TGA_CONFIG = {
    "base_url": "https://www.tga.gov.au",
    "alerts_url": "https://www.tga.gov.au/safety/safety-monitoring-and-information/safety-alerts",
}

print("Scraping TGA…")
signals = TGAScraper(TGA_CONFIG)._fetch_safety_alerts()
print(f"  {len(signals)} signal(s) fetched\n")

print("Classifying with Claude…\n")
classifier = SignalClassifier()
classified = classifier.classify_batch(signals)

SEV_COLOUR = {"high": "🔴", "medium": "🟡", "low": "🟢"}

total_in  = sum(c.input_tokens  for c in classified)
total_out = sum(c.output_tokens for c in classified)

print("=" * 70)
print(f"CLASSIFIED SIGNALS  ({len(classified)} total | "
      f"{total_in} in / {total_out} out tokens)")
print("=" * 70)

for c in classified:
    icon = SEV_COLOUR.get(c.severity, "⚪")
    print(f"\n{icon} [{c.severity.upper()}]  {c.event_type}")
    print(f"   Title:      {c.title}")
    print(f"   Ingredient: {c.ingredient_name}")
    print(f"   Summary:    {c.summary}")
    print(f"   URL:        {c.url}")
    print(f"   source_id:  {c.source_id}  "
          f"(tokens: {c.input_tokens}↑ {c.output_tokens}↓)")

print(f"\nTotal tokens used: {total_in + total_out}")
