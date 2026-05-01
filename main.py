"""
main.py — Entry point for the VMS regulatory intelligence platform.

Run modes:
  python main.py                  — start the scheduler (long-running process)
  python main.py --scrape-now     — run scrapers immediately then exit
  python main.py --digest-now     — send digest immediately then exit
  python main.py --pipeline       — run full integrated pipeline: scrape + classify
                                    + store in SQLite + sentiment + trends + custom
                                    alerts + digest, then print summary and exit
  python main.py --refresh        — safe data refresh: all scrapers → classify →
                                    sentiment → trends → alerts → patch data blob
                                    in signals.html (layout/CSS/JS untouched)
  python main.py --trends         — run trend detection and print report
  python main.py --sentiment      — run sentiment analysis on unsent signals
  python main.py --feedback       — open the interactive classification review CLI

The --scrape-now and --digest-now flags are useful for testing and for
one-off backfills without waiting for the scheduled run time.
"""

import argparse
import logging
import signal as _signal
import sys

_REFRESH_TIMEOUT_SECS = 20 * 60  # 20 minutes hard stop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="VMS Regulatory Intelligence Platform")
    parser.add_argument("--scrape-now",  action="store_true", help="Run scrapers immediately and exit")
    parser.add_argument("--digest-now",  action="store_true", help="Send digest immediately and exit")
    parser.add_argument("--pipeline",    action="store_true", help="Run full integrated pipeline and exit")
    parser.add_argument("--refresh",     action="store_true", help="Safe refresh: scrape+classify+analytics then patch signals.html data blob only")
    parser.add_argument("--trends",      action="store_true", help="Run trend detection and print report")
    parser.add_argument("--sentiment",   action="store_true", help="Run sentiment analysis on pending signals")
    parser.add_argument("--feedback",    action="store_true", help="Open interactive classification review CLI")
    args = parser.parse_args()

    if args.scrape_now:
        from scheduler.jobs import scrape_and_classify
        logger.info("Running scrape & classify on demand…")
        scrape_and_classify()
        return

    if args.digest_now:
        from scheduler.jobs import send_digest
        logger.info("Sending digest on demand…")
        send_digest()
        return

    if args.pipeline:
        from scheduler.jobs import run_full_pipeline
        logger.info("Running full integrated pipeline…")
        run_full_pipeline()
        return

    if args.refresh:
        _run_refresh()
        return

    if args.trends:
        from analytics.trends import run_trend_detection
        report = run_trend_detection()
        _print_trend_report(report)
        return

    if args.sentiment:
        from analytics.sentiment import run_sentiment_analysis
        report = run_sentiment_analysis()
        _print_sentiment_report(report)
        return

    if args.feedback:
        from analytics.feedback import interactive_review
        interactive_review()
        return

    # Default: start the blocking scheduler
    from scheduler.jobs import build_scheduler
    scheduler = build_scheduler()
    logger.info("Scheduler started. Press Ctrl+C to stop.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


def _run_refresh() -> None:
    """
    Safe pipeline refresh: scrape → classify → sentiment → trends → alerts →
    replace ONLY the SIGNALEX DATA block in signals.html.
    Layout, CSS, and JS logic are never touched.

    Hard-stops after _REFRESH_TIMEOUT_SECS (20 min) and prints
    SAFE REFRESH FAILED: <reason> on any error.
    """
    def _alarm_handler(signum, frame):
        raise TimeoutError(f"Pipeline exceeded {_REFRESH_TIMEOUT_SECS // 60}-minute safety limit")

    _signal.signal(_signal.SIGALRM, _alarm_handler)
    _signal.alarm(_REFRESH_TIMEOUT_SECS)

    try:
        from scheduler.jobs import run_full_pipeline
        from generate_signals import update_data_blob

        logger.info("Running full pipeline (scrape + classify + analytics)…")
        summary = run_full_pipeline()

        logger.info("Patching signals.html data block…")
        result = update_data_blob()

        _signal.alarm(0)  # cancel timeout

        print("\n" + "=" * 65)
        print("  SAFE REFRESH COMPLETE")
        print("=" * 65)
        print(f"\n  New signals: {summary['total_new']}")
        for src, ct in summary["source_counts"].items():
            if ct:
                print(f"    {src:30s}  {ct:4d} new")
        print(f"\n  signals.html data block updated (layout/CSS/JS untouched)")
        print(f"    Signals:      {result['signals']}")
        print(f"    Citations:    {result['citations']}")
        print(f"    Last updated: {result['last_updated']}")
        print("=" * 65 + "\n")

    except (TimeoutError, KeyboardInterrupt) as exc:
        _signal.alarm(0)
        reason = str(exc) if str(exc) else type(exc).__name__
        print(f"\nSAFE REFRESH FAILED: {reason}\n", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        _signal.alarm(0)
        logger.exception("Refresh pipeline failed")
        print(f"\nSAFE REFRESH FAILED: {exc}\n", file=sys.stderr)
        sys.exit(1)


def _print_trend_report(report) -> None:
    print("\n" + "="*60)
    print("  TREND DETECTION REPORT")
    print(f"  Generated: {report.generated_at}")
    print("="*60)

    if report.trending_ingredients:
        print(f"\n📈 Trending Ingredients ({len(report.trending_ingredients)}):")
        for t in report.trending_ingredients:
            print(f"  • {t.ingredient:30s}  7d={t.count_7d:3d}  30d={t.count_30d:3d}  spike=×{t.spike_ratio:.1f}")
    else:
        print("\n  No trending ingredients detected.")

    if report.claim_shift_alerts:
        print(f"\n⚠️  Claim Shifts ({len(report.claim_shift_alerts)}):")
        for c in report.claim_shift_alerts:
            print(f"  • {c.ingredient}: {c.old_dominant_type} → {c.new_dominant_type}")
            print(f"    {c.description}")
    else:
        print("\n  No claim shifts detected.")


def _print_sentiment_report(report) -> None:
    print("\n" + "="*60)
    print("  SENTIMENT ANALYSIS REPORT")
    print(f"  Generated: {report.generated_at}")
    print("="*60)

    dist = report.overall_distribution
    total = sum(dist.values())
    print(f"\nDistribution (n={total}):")
    for stype in ("positive", "neutral", "negative"):
        count = dist.get(stype, 0)
        pct   = round(count / total * 100, 1) if total else 0
        bar   = "█" * int(pct / 5)
        print(f"  {stype:10s} {count:4d} ({pct:5.1f}%)  {bar}")

    if report.rising_risk:
        print(f"\n🔴 Rising Risk Ingredients ({len(report.rising_risk)}):")
        for s in report.ingredient_summaries:
            if s.is_rising_risk:
                print(f"  • {s.ingredient:30s}  30d={s.score_30d:+.2f}  recent={s.score_recent:+.2f}  shift={s.shift:+.2f}")
    else:
        print("\n  No rising risk ingredients detected.")


if __name__ == "__main__":
    main()
