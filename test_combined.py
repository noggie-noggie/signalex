"""
test_combined.py — Run TGA + FDA scrapers, classify all signals, print table.
"""
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

import config
config.SIGNAL_LOOKBACK_DAYS = 90   # 3-month window to capture enough FDA rows

from scrapers.tga import TGAScraper
from scrapers.fda import FDAScraper
from classifier.claude import SignalClassifier

TGA_CFG = config.SCRAPER_CONFIG["tga"]
FDA_CFG = config.SCRAPER_CONFIG["fda"]

# ── Scrape ────────────────────────────────────────────────────────────────
print("Scraping TGA…")
tga_signals = TGAScraper(TGA_CFG)._fetch_safety_alerts()
print(f"  {len(tga_signals)} TGA signal(s)\n")

print("Scraping FDA recalls…")
fda_scraper  = FDAScraper(FDA_CFG)
fda_recalls  = fda_scraper._fetch_recalls()
print(f"  {len(fda_recalls)} FDA recall signal(s)")

print("Scraping FDA hub safety links…")
fda_hub = fda_scraper._fetch_hub_safety_links()
print(f"  {len(fda_hub)} FDA hub signal(s)\n")

all_signals = tga_signals + fda_recalls + fda_hub
print(f"Total raw signals: {len(all_signals)}\n")

# ── Classify ──────────────────────────────────────────────────────────────
print("Classifying with Claude…")
classified = SignalClassifier().classify_batch(all_signals)
total_tokens = sum(c.input_tokens + c.output_tokens for c in classified)
print(f"  Done — {total_tokens} tokens used\n")

# ── Display ───────────────────────────────────────────────────────────────
SEV = {"high": "🔴", "medium": "🟡", "low": "🟢"}
COL_W = 18

def trunc(s, n):
    return (s[:n-1] + "…") if len(s) > n else s

# Header
print("=" * 100)
print(f"{'SRCE':<6} {'SEV':<8} {'TYPE':<16} {'INGREDIENT':<20} {'TITLE':<38} {'DATE'}")
print("-" * 100)

for c in sorted(classified, key=lambda x: ("high","medium","low","other").index(x.severity) if x.severity in ("high","medium","low") else 3):
    icon  = SEV.get(c.severity, "⚪")
    title = trunc(c.title, 36)
    ingr  = trunc(c.ingredient_name or "—", 18)
    etype = trunc(c.event_type.replace("_"," "), 14)
    # extract date from body_text first line
    date_line = c.scraped_at[:10]
    print(f"{c.authority.upper():<6} {icon} {c.severity:<6} {etype:<16} {ingr:<20} {title:<38} {date_line}")

print("=" * 100)
print(f"\nTotal: {len(classified)} classified  |  {total_tokens} tokens  |"
      f"  🔴 {sum(1 for c in classified if c.severity=='high')} high  "
      f"  🟡 {sum(1 for c in classified if c.severity=='medium')} medium  "
      f"  🟢 {sum(1 for c in classified if c.severity=='low')} low")
