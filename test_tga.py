"""Quick smoke-test for the TGA scraper — run directly, no pytest needed."""
import json
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

# Minimal config slice so TGAScraper can be instantiated without a .env file
TGA_CONFIG = {
    "base_url": "https://www.tga.gov.au",
    "alerts_url": "https://www.tga.gov.au/safety/safety-monitoring-and-information/safety-alerts",
}

# Temporarily patch SIGNAL_LOOKBACK_DAYS to a wide window so we definitely
# get results even if no alerts were published in the last 7 days.
import config
config.SIGNAL_LOOKBACK_DAYS = 365

from scrapers.tga import TGAScraper

scraper = TGAScraper(TGA_CONFIG)
signals = scraper._fetch_safety_alerts()

print(f"\n{'='*60}")
print(f"TGA Safety Alerts — {len(signals)} signal(s) returned")
print(f"{'='*60}\n")

for i, s in enumerate(signals[:10], 1):          # show first 10
    print(f"[{i}] {s['title']}")
    print(f"    URL:        {s['url']}")
    print(f"    source_id:  {s['source_id']}")
    print(f"    scraped_at: {s['scraped_at']}")
    print(f"    body_text:  {s['body_text'][:120]}...")
    print()

if len(signals) > 10:
    print(f"... and {len(signals) - 10} more.")

if not signals:
    print("No signals returned — check selector warnings above.")
    sys.exit(1)
