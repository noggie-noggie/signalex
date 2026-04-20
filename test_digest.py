"""
test_digest.py — Scrape TGA → classify → render digest → preview → optionally send.

Usage:
  python test_digest.py            # preview only (writes digest_preview.html)
  python test_digest.py --send     # preview then send via SMTP
"""

import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

import config
config.SIGNAL_LOOKBACK_DAYS = 90   # 3-month window — enough for a meaningful digest

from scrapers.tga import TGAScraper
from scrapers.fda import FDAScraper
from scrapers.artg import ARTGScraper
from classifier.claude import SignalClassifier
from digest.email_sender import DigestSender

parser = argparse.ArgumentParser()
parser.add_argument("--send", action="store_true", help="Send the email after previewing")
args = parser.parse_args()

# ── 1. Scrape ──────────────────────────────────────────────────────────────
print("Scraping TGA…")
tga_signals = TGAScraper(config.SCRAPER_CONFIG["tga"])._fetch_safety_alerts()
print(f"  {len(tga_signals)} TGA signal(s)")

print("Scraping FDA…")
fda = FDAScraper(config.SCRAPER_CONFIG["fda"])
fda_signals = fda._fetch_recalls() + fda._fetch_hub_safety_links()
print(f"  {len(fda_signals)} FDA signal(s)")

print("Scraping ARTG…")
artg_signals = ARTGScraper(config.SCRAPER_CONFIG["artg"]).fetch_raw()
print(f"  {len(artg_signals)} ARTG signal(s)")

signals = tga_signals + fda_signals + artg_signals
print(f"  {len(signals)} total\n")

# ── 2. Classify ────────────────────────────────────────────────────────────
print("Classifying with Claude…")
classified = SignalClassifier().classify_batch(signals)
total_tokens = sum(c.input_tokens + c.output_tokens for c in classified)
print(f"  Done. {total_tokens} tokens used\n")

# ── 3. Render preview ─────────────────────────────────────────────────────
sender = DigestSender()
html   = sender.render_html(classified)
text   = sender.render_text(classified)

preview_path = Path("digest_preview.html")
preview_path.write_text(html, encoding="utf-8")
print(f"HTML preview written to: {preview_path.resolve()}")
print(f"  Open in a browser to inspect before sending.\n")

# ── 4. Print plain-text version ───────────────────────────────────────────
print("=" * 60)
print("PLAIN TEXT VERSION")
print("=" * 60)
print(text)

# ── 5. Optionally send ────────────────────────────────────────────────────
if args.send:
    print("\nSending email…")
    try:
        sender.send(classified)
        print(f"  Sent to: {', '.join(config.EMAIL_RECIPIENTS)}")
    except Exception as e:
        print(f"  Send failed: {e}", file=sys.stderr)
        sys.exit(1)
else:
    print("\nRun with --send to deliver the email.")
    print(f"  Recipients: {', '.join(config.EMAIL_RECIPIENTS)}")
