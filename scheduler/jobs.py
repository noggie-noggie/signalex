"""
scheduler/jobs.py — APScheduler job definitions + full pipeline runner.

Recurring jobs:
  1. scrape_and_classify  — runs every 6 hours (SCRAPER_CRON).
  2. send_digest          — runs once daily (DIGEST_CRON).

One-shot function:
  3. run_full_pipeline    — called by `python main.py --pipeline`.
     Runs all scrapers (original + new), persists to SQLite, runs sentiment
     analysis, trend detection, evaluates custom alert rules, then prints a
     full pipeline summary.
"""

from __future__ import annotations

import concurrent.futures
import logging
from datetime import datetime, timezone

_SOURCE_TIMEOUT_SECS = 5 * 60   # 5 min per scraper source

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

import config
from scrapers.tga import TGAScraper
from scrapers.fda import FDAScraper
from scrapers.artg import ARTGScraper
from scrapers.pubmed import PubMedScraper
from scrapers.tga_consultations import TGAConsultationsScraper
from scrapers.advisory_committees import AdvisoryCommitteesScraper
from scrapers.adverse_events import AdverseEventsScraper
from scrapers.europe_pmc import EuropePMCScraper
from scrapers.cochrane import CochraneScraper
from scrapers.clinical_trials import ClinicalTrialsScraper
from scrapers.who_ictrp import WHOICTRPScraper
from scrapers.efsa_journal import EFSAJournalScraper
from scrapers.biorxiv import BiorxivScraper
from scrapers.semantic_scholar import SemanticScholarScraper
from classifier.claude import SignalClassifier
from alerts.dispatcher import AlertDispatcher
from storage.signals import SignalStore          # existing TinyDB store (unchanged)
from analytics.db import save_signals_batch     # new SQLite store
from analytics.feedback import build_few_shot_examples, log_accuracy_run

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: classify with feedback-enhanced prompts
# ---------------------------------------------------------------------------

def _classify_with_feedback(
    classifier: SignalClassifier,
    raw_signals: list,
    source_label: str,
) -> list:
    """
    Classify raw signals using the appropriate classifier method.
    Injects recent feedback examples into the classifier's system prompts.
    """
    few_shot = build_few_shot_examples(limit=20)

    # Temporarily patch system prompts if we have few-shot examples
    if few_shot:
        _patch_prompts(classifier, few_shot)

    try:
        if source_label == "artg":
            return classifier.classify_batch_artg(raw_signals)
        if source_label == "pubmed":
            return classifier.classify_batch_pubmed(raw_signals)
        if source_label == "tga_consultations":
            return classifier.classify_batch_tga_consultation(raw_signals)
        if source_label == "advisory_committee":
            return classifier.classify_batch_advisory_committee(raw_signals)
        if source_label == "adverse_events":
            return classifier.classify_batch_adverse_event(raw_signals)
        if source_label == "europe_pmc":
            return classifier.classify_batch_europe_pmc(raw_signals)
        if source_label == "cochrane":
            return classifier.classify_batch_cochrane(raw_signals)
        if source_label == "clinical_trials":
            return classifier.classify_batch_clinical_trials(raw_signals)
        if source_label == "who_ictrp":
            return classifier.classify_batch_who_ictrp(raw_signals)
        if source_label == "efsa":
            return classifier.classify_batch_efsa(raw_signals)
        if source_label == "biorxiv":
            return classifier.classify_batch_biorxiv(raw_signals)
        if source_label == "semantic_scholar":
            return classifier.classify_batch_semantic_scholar(raw_signals)
        return classifier.classify_batch(raw_signals)
    finally:
        if few_shot:
            _unpatch_prompts(classifier)


# Prompt patching helpers — inject few-shot examples into the classifier's
# system prompts at runtime without permanently modifying the module.

import classifier.claude as _claude_module

_ORIGINAL_SYSTEM   = None
_PATCHED           = False

def _patch_prompts(classifier: SignalClassifier, few_shot: str) -> None:
    global _ORIGINAL_SYSTEM, _PATCHED
    if _PATCHED:
        return
    _ORIGINAL_SYSTEM = _claude_module._SYSTEM_PROMPT
    _claude_module._SYSTEM_PROMPT = few_shot + "\n\n" + _claude_module._SYSTEM_PROMPT
    _PATCHED = True

def _unpatch_prompts(classifier: SignalClassifier) -> None:
    global _ORIGINAL_SYSTEM, _PATCHED
    if not _PATCHED:
        return
    _claude_module._SYSTEM_PROMPT = _ORIGINAL_SYSTEM
    _PATCHED = False


# ---------------------------------------------------------------------------
# Job 1: scrape_and_classify (existing sources only — backwards compatible)
# ---------------------------------------------------------------------------

def scrape_and_classify() -> None:
    """
    Job 1: Scrape original sources, classify with Claude, persist.
    Unchanged from original behaviour; also saves to SQLite now.
    """
    store      = SignalStore()
    classifier = SignalClassifier()

    scrapers_cfg = [
        ("tga",  TGAScraper),
        ("fda",  FDAScraper),
        ("artg", ARTGScraper),
    ]

    all_classified = []

    for key, ScraperClass in scrapers_cfg:
        if not config.SCRAPER_CONFIG.get(key, {}).get("enabled", False):
            continue
        try:
            scraper = ScraperClass(config.SCRAPER_CONFIG[key])
            logger.info("Running scraper: %s", scraper.authority)
            raw_signals = scraper.run()
            logger.info("%s returned %d raw signal(s)", scraper.authority, len(raw_signals))

            classified = _classify_with_feedback(classifier, raw_signals, key)
            saved_tinydb = store.save_batch(classified)
            saved_sqlite = save_signals_batch(classified)
            logger.info(
                "Saved %d (TinyDB) / %d (SQLite) new signal(s) from %s",
                saved_tinydb, saved_sqlite, scraper.authority,
            )
            all_classified.extend(classified)

        except Exception:
            logger.exception("Scraper %s failed", key)

    # Fire instant alerts for any HIGH signals
    if all_classified:
        dispatcher = AlertDispatcher()
        dispatcher.check_and_dispatch(all_classified)

    return all_classified


# ---------------------------------------------------------------------------
# Job 2: send_digest (unchanged)
# ---------------------------------------------------------------------------

def send_digest() -> None:
    try:
        from digest.email_sender import DigestSender
        DigestSender().send()
        logger.info("Digest sent successfully.")
    except Exception:
        logger.exception("Digest send failed")


# ---------------------------------------------------------------------------
# Job 3: run_full_pipeline
# ---------------------------------------------------------------------------

def run_full_pipeline() -> dict:
    """
    Full integrated pipeline:
      1. Run all scrapers (original + new)
      2. Classify with feedback-enhanced prompts
      3. Persist to SQLite
      4. Run sentiment analysis
      5. Run trend detection
      6. Evaluate custom alert rules
      7. Fire instant custom alerts
      8. Print pipeline summary
      9. Return summary dict for programmatic use
    """
    from analytics.sentiment import run_sentiment_analysis
    from analytics.trends import run_trend_detection
    from alerts.custom_alerts import CustomAlertEvaluator
    from alerts.dispatcher import AlertDispatcher

    started_at = datetime.now(timezone.utc)
    logger.info("Full pipeline started at %s", started_at.isoformat())

    classifier  = SignalClassifier()
    store       = SignalStore()
    dispatcher  = AlertDispatcher()

    # ------------------------------------------------------------------
    # Scraper registry — original + new
    # ------------------------------------------------------------------
    scraper_jobs = [
        # (key_or_label, ScraperClass, config_key_or_None, classifier_method)
        ("tga",                 TGAScraper,                "tga",  "default"),
        ("fda",                 FDAScraper,                "fda",  "default"),
        ("artg",                ARTGScraper,               "artg", "artg"),
        ("pubmed",              PubMedScraper,             None,   "pubmed"),
        ("tga_consultations",   TGAConsultationsScraper,   None,   "tga_consultations"),
        ("advisory_committee",  AdvisoryCommitteesScraper, None,   "advisory_committee"),
        ("adverse_events",      AdverseEventsScraper,      None,   "adverse_events"),
        # New scientific sources
        ("europe_pmc",          EuropePMCScraper,          None,   "europe_pmc"),
        ("cochrane",            CochraneScraper,           None,   "cochrane"),
        ("clinical_trials",     ClinicalTrialsScraper,     None,   "clinical_trials"),
        ("who_ictrp",           WHOICTRPScraper,           None,   "who_ictrp"),
        ("efsa",                EFSAJournalScraper,        None,   "efsa"),
        ("biorxiv",             BiorxivScraper,            None,   "biorxiv"),
        ("semantic_scholar",    SemanticScholarScraper,    None,   "semantic_scholar"),
    ]

    source_counts:  dict[str, int] = {}
    all_classified: list = []

    for label, ScraperClass, cfg_key, classify_method in scraper_jobs:
        # Skip if explicitly disabled in SCRAPER_CONFIG
        if cfg_key and not config.SCRAPER_CONFIG.get(cfg_key, {}).get("enabled", True):
            logger.info("Skipping disabled scraper: %s", label)
            continue

        try:
            cfg = config.SCRAPER_CONFIG.get(cfg_key, {}) if cfg_key else {}
            scraper = ScraperClass(cfg)
            logger.info("Running scraper: %s", label)

            # Per-source timeout — skip source if it hangs beyond limit
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _ex:
                _fut = _ex.submit(scraper.run)
                try:
                    raw_signals = _fut.result(timeout=_SOURCE_TIMEOUT_SECS)
                except concurrent.futures.TimeoutError:
                    logger.error(
                        "Scraper %s timed out after %ds — skipping",
                        label, _SOURCE_TIMEOUT_SECS,
                    )
                    source_counts[label] = 0
                    continue

            logger.info("%s: %d raw signal(s)", label, len(raw_signals))

            classified = _classify_with_feedback(classifier, raw_signals, classify_method)
            saved_sqlite = save_signals_batch(classified)
            # Also save to TinyDB for backwards-compat
            store.save_batch(classified)

            source_counts[label] = saved_sqlite
            all_classified.extend(classified)
            logger.info("%s: %d new signal(s) stored", label, saved_sqlite)

        except Exception:
            logger.exception("Scraper %s failed", label)
            source_counts[label] = 0

    total_new = sum(source_counts.values())
    logger.info("Scraping complete: %d total new signals across %d sources", total_new, len(source_counts))

    # ------------------------------------------------------------------
    # Sentiment analysis
    # ------------------------------------------------------------------
    logger.info("Running sentiment analysis…")
    sentiment_report = run_sentiment_analysis()

    # ------------------------------------------------------------------
    # Trend detection
    # ------------------------------------------------------------------
    logger.info("Running trend detection…")
    trend_report = run_trend_detection()

    # ------------------------------------------------------------------
    # Custom alert rules
    # ------------------------------------------------------------------
    logger.info("Evaluating custom alert rules…")
    evaluator    = CustomAlertEvaluator()
    fired_alerts = evaluator.evaluate(all_classified, trend_report, sentiment_report)
    evaluator.dispatch_instant(fired_alerts, dispatcher)

    # ------------------------------------------------------------------
    # Log accuracy
    # ------------------------------------------------------------------
    try:
        log_accuracy_run()
    except Exception:
        logger.debug("Accuracy log update failed (no feedback yet)")

    # ------------------------------------------------------------------
    # Digest with enriched sections
    # ------------------------------------------------------------------
    if all_classified:
        try:
            html = dispatcher.render_digest_html(
                all_classified,
                trend_report=trend_report,
                sentiment_report=sentiment_report,
                fired_alerts=fired_alerts,
            )
            preview_path = config.BASE_DIR / "digest_preview.html"
            preview_path.write_text(html)
            logger.info("Digest preview saved to %s", preview_path)
        except Exception:
            logger.exception("Digest rendering failed")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    summary = {
        "started_at":       started_at.isoformat(),
        "elapsed_seconds":  round(elapsed, 1),
        "source_counts":    source_counts,
        "total_new":        total_new,
        "trending":         [t.ingredient for t in trend_report.trending_ingredients],
        "claim_shifts":     [c.ingredient for c in trend_report.claim_shift_alerts],
        "rising_risk":      sentiment_report.rising_risk,
        "sentiment_dist":   sentiment_report.overall_distribution,
        "fired_alerts":     [(a.rule_name, a.matched_item) for a in fired_alerts],
    }

    _print_pipeline_summary(summary)
    return summary


def _print_pipeline_summary(s: dict) -> None:
    print("\n" + "="*65)
    print("  FULL PIPELINE RUN SUMMARY")
    print(f"  {s['started_at']}  ({s['elapsed_seconds']}s)")
    print("="*65)

    print(f"\n📥 New signals by source:")
    for src, count in s["source_counts"].items():
        print(f"   {src:30s}  {count:4d} new")
    print(f"   {'TOTAL':30s}  {s['total_new']:4d} new")

    print(f"\n📈 Trending ingredients: ", end="")
    print(", ".join(s["trending"]) if s["trending"] else "none detected")

    print(f"\n🔄 Claim shifts: ", end="")
    print(", ".join(s["claim_shifts"]) if s["claim_shifts"] else "none detected")

    print(f"\n🔴 Rising risk ingredients: ", end="")
    print(", ".join(s["rising_risk"]) if s["rising_risk"] else "none detected")

    dist = s.get("sentiment_dist", {})
    total = sum(dist.values())
    if total:
        print(f"\n💬 Sentiment distribution (n={total}):")
        for stype in ("positive", "neutral", "negative"):
            count = dist.get(stype, 0)
            pct   = round(count / total * 100, 1) if total else 0
            print(f"   {stype:10s}  {count:4d}  ({pct:.1f}%)")

    if s["fired_alerts"]:
        print(f"\n🔔 Custom alert rules fired ({len(s['fired_alerts'])}):")
        for rule, match in s["fired_alerts"]:
            print(f"   • {rule}: {match[:60]}")
    else:
        print(f"\n🔔 No custom alert rules fired")

    print("="*65 + "\n")


# ---------------------------------------------------------------------------
# Scheduler builder
# ---------------------------------------------------------------------------

def build_scheduler() -> BlockingScheduler:
    scheduler = BlockingScheduler()

    scheduler.add_job(
        scrape_and_classify,
        CronTrigger(**config.SCRAPER_CRON),
        id="scrape_and_classify",
        name="Scrape & classify regulatory signals",
        misfire_grace_time=300,
    )

    scheduler.add_job(
        send_digest,
        CronTrigger(**config.DIGEST_CRON),
        id="send_digest",
        name="Send daily digest email",
        misfire_grace_time=300,
    )

    return scheduler
