"""
test_all_sources.py — Combined pipeline test for all scrapers + alert system.

Runs:
  1. TGA safety alerts scraper
  2. ARTG new medicines scraper
  3. FDA dietary supplement scraper
  4. FDA → Australia relevance filter
  5. iHerb AU new products scraper
  6. Chemist Warehouse vitamins scraper

Then classifies all signals and shows:
  - Combined signal count by source
  - HIGH severity / HIGH market significance signals (instant alert candidates)
  - Preview of Slack message for the top signal
  - Preview of the enhanced daily digest email layout (plain text)
"""

import logging
import os
import sys
import time
from collections import Counter

# ---------------------------------------------------------------------------
# Ensure the venv is on sys.path when running directly
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
# Quiet the noisy scrapers during the test run
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

logger = logging.getLogger("test_all_sources")

import config
from scrapers.tga import TGAScraper
from scrapers.fda import FDAScraper
from scrapers.artg import ARTGScraper
from scrapers.retail import iHerbScraper, ChemistWarehouseScraper
from scrapers.fda_australia import FDAAustraliaFilter
from classifier.claude import SignalClassifier, ClassifiedSignal
from alerts.dispatcher import AlertDispatcher


def run_scraper(name: str, scraper_fn) -> list:
    """Run a single scraper/filter, returning results with error handling."""
    logger.info("── Running %s ──", name)
    t0 = time.time()
    try:
        results = scraper_fn()
        elapsed = time.time() - t0
        logger.info("%s: %d result(s) in %.1fs", name, len(results), elapsed)
        return results
    except Exception:
        logger.exception("%s failed", name)
        return []


def main() -> None:
    print("\n" + "=" * 65)
    print("  SIGNALIX — COMBINED SOURCE TEST")
    print("=" * 65 + "\n")

    classifier = SignalClassifier()
    dispatcher = AlertDispatcher()

    # ── 1. Run all scrapers ──────────────────────────────────────────

    tga_cfg   = config.SCRAPER_CONFIG["tga"]
    artg_cfg  = config.SCRAPER_CONFIG["artg"]
    fda_cfg   = config.SCRAPER_CONFIG["fda"]
    ih_cfg    = config.SCRAPER_CONFIG["iherb"]
    cw_cfg    = config.SCRAPER_CONFIG["chemist_warehouse"]

    tga_raw   = run_scraper("TGA Safety Alerts",   lambda: TGAScraper(tga_cfg).run())
    artg_raw  = run_scraper("ARTG New Listings",   lambda: ARTGScraper(artg_cfg).run())
    fda_raw   = run_scraper("FDA Dietary Supps",   lambda: FDAScraper(fda_cfg).run())
    iherb_raw = run_scraper("iHerb AU",            lambda: iHerbScraper(ih_cfg).run())
    cw_raw    = run_scraper("Chemist Warehouse",   lambda: ChemistWarehouseScraper(cw_cfg).run())

    # ── 2. Classify raw signals ──────────────────────────────────────

    all_classified: list[ClassifiedSignal] = []

    if tga_raw:
        print(f"\n[Classifying {len(tga_raw)} TGA signal(s)...]")
        classified = classifier.classify_batch(tga_raw)
        for s in classified:
            s.source_label = "tga"
        all_classified.extend(classified)

    if artg_raw:
        print(f"\n[Classifying {len(artg_raw)} ARTG signal(s)...]")
        all_classified.extend(classifier.classify_batch_artg(artg_raw))

    if fda_raw:
        print(f"\n[Classifying {len(fda_raw)} FDA signal(s) (standard + AU relevance)...]")
        all_classified.extend(classifier.classify_batch_fda_australia(fda_raw))

    if iherb_raw:
        print(f"\n[Classifying {len(iherb_raw)} iHerb signal(s)...]")
        all_classified.extend(classifier.classify_batch_retail(iherb_raw))

    if cw_raw:
        print(f"\n[Classifying {len(cw_raw)} Chemist Warehouse signal(s)...]")
        all_classified.extend(classifier.classify_batch_retail(cw_raw))

    total = len(all_classified)

    # ── 3. Combined signal count by source ──────────────────────────

    print("\n" + "=" * 65)
    print("  SIGNAL COUNT BY SOURCE")
    print("=" * 65)

    source_counts: Counter = Counter()
    for s in all_classified:
        key = s.source_label or s.authority
        source_counts[key] += 1

    source_labels = {
        "tga":               "TGA Safety Alerts",
        "artg":              "ARTG New Listings",
        "fda_australia":     "FDA → Australia Signals",
        "fda":               "FDA (general)",
        "iherb":             "iHerb AU",
        "chemist_warehouse": "Chemist Warehouse",
    }

    for key, label in source_labels.items():
        n = source_counts.get(key, 0)
        print(f"  {label:<30} {n:>4} signal(s)")

    print(f"\n  {'TOTAL':<30} {total:>4} signal(s)\n")

    # ── 4. Instant-alert candidates ──────────────────────────────────

    urgent = [
        s for s in all_classified
        if s.severity == "high" or s.market_significance == "high"
    ]

    print("=" * 65)
    print(f"  INSTANT ALERT CANDIDATES ({len(urgent)} signal(s))")
    print("=" * 65)

    if not urgent:
        print("  No HIGH severity or HIGH market significance signals found.\n")
    else:
        for s in urgent:
            sev_badge = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(s.severity, "⚪")
            mkt_badge = " ⚡HIGH-MKT" if s.market_significance == "high" else ""
            comp_badge = " 🎯COMPETITOR" if s.competitor_signal else ""
            print(f"\n  {sev_badge} [{s.severity.upper()}]{mkt_badge}{comp_badge}")
            print(f"  Title:      {s.title[:70]}")
            print(f"  Ingredient: {s.ingredient_name}")
            print(f"  Source:     {source_labels.get(s.source_label or s.authority, s.authority)}")
            print(f"  Summary:    {s.summary[:100]}")
            if s.australia_relevance:
                print(f"  AU Relevance: {s.australia_relevance.upper()} — {s.australia_reasoning[:80]}")
            print(f"  URL:        {s.url}")
        print()

    # ── 5. Slack message preview ─────────────────────────────────────

    print("=" * 65)
    print("  SLACK MESSAGE PREVIEW")
    print("=" * 65)

    preview_signal = (
        urgent[0] if urgent
        else all_classified[0] if all_classified
        else None
    )

    if preview_signal:
        slack_msg = dispatcher.preview_slack(preview_signal)
        print()
        print("  ┌─────────────────────────────────────────────────────┐")
        for line in slack_msg.split("\n"):
            print(f"  │ {line}")
        print("  └─────────────────────────────────────────────────────┘")
        print()
        if not config.SLACK_WEBHOOK_URL:
            print("  (SLACK_WEBHOOK_URL not set — set in .env to enable live posting)\n")
    else:
        print("  No signals to preview.\n")

    # ── 6. Digest email preview (plain text) ─────────────────────────

    print("=" * 65)
    print("  DAILY DIGEST EMAIL PREVIEW (plain text layout)")
    print("=" * 65 + "\n")

    if all_classified:
        digest_text = dispatcher.preview_digest_text(all_classified)
        # Print first 80 lines to keep output manageable
        lines = digest_text.split("\n")
        for line in lines[:80]:
            print(line)
        if len(lines) > 80:
            print(f"\n  ... ({len(lines) - 80} more lines)")
    else:
        print("  No signals to include in digest.\n")

    # ── Summary ──────────────────────────────────────────────────────

    print("\n" + "=" * 65)
    print("  TEST COMPLETE")
    print("=" * 65)
    print(f"  Total signals classified: {total}")
    print(f"  Instant alert candidates: {len(urgent)}")
    print(f"  Token usage (approx):     {sum(s.input_tokens + s.output_tokens for s in all_classified):,}")
    print()

    if not all_classified:
        print("  NOTE: No signals were returned. Check scraper logs above for details.")
        print("  TGA and FDA scrapers require live network access to tga.gov.au / fda.gov")
        print("  Retail scrapers (iHerb, CW) require Playwright for bot-protected sites.\n")


if __name__ == "__main__":
    main()
