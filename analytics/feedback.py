"""
analytics/feedback.py — Human feedback loop for classification accuracy.

Usage:
    python -m analytics.feedback --review      # interactive feedback CLI
    python -m analytics.feedback --stats       # print accuracy summary

The feedback loop:
  1. Shows recent classifications to a human reviewer.
  2. Stores ratings (correct / incorrect / partially_correct) + optional notes.
  3. On pipeline runs, injects the last 20 corrections as few-shot examples
     into the Claude classification prompt to improve accuracy over time.

Data storage:
  - SQLite feedback table (via analytics.db)
  - data/feedback.json mirror
  - data/accuracy_log.csv for tracking accuracy over time
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from analytics.db import (
    append_accuracy_log,
    get_conn,
    get_recent_feedback,
    save_feedback,
    get_signals_since,
    ACCURACY_CSV,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Few-shot prompt injection
# ---------------------------------------------------------------------------

def build_few_shot_examples(limit: int = 20) -> str:
    """
    Return a formatted string of recent corrections to prepend to classification
    prompts, helping Claude learn from past mistakes.

    Format:
      --- CORRECTION EXAMPLES (from human feedback) ---
      Example 1 (was incorrect):
        Title: ...
        Original classification: event_type=X, severity=Y, ingredient=Z
        Correction: ...
      ---
    """
    corrections = get_recent_feedback(limit=limit)
    if not corrections:
        return ""

    lines = ["--- CORRECTION EXAMPLES (from human feedback — please learn from these) ---"]
    for i, fb in enumerate(corrections, 1):
        title     = fb.get("title") or "Unknown"
        ingredient = fb.get("ingredient_name") or "unknown"
        event_type = fb.get("event_type") or "other"
        severity   = fb.get("severity") or "low"
        summary    = fb.get("summary") or ""
        rating     = fb.get("rating") or "incorrect"
        note       = fb.get("correction_note") or ""

        lines.append(f"\nExample {i} (marked as {rating}):")
        lines.append(f"  Title: {title[:100]}")
        lines.append(f"  Classification given: ingredient={ingredient}, event_type={event_type}, severity={severity}")
        if summary:
            lines.append(f"  Summary given: {summary[:100]}")
        if note:
            lines.append(f"  Correction note: {note}")

    lines.append("--- END CORRECTION EXAMPLES ---\n")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Accuracy stats
# ---------------------------------------------------------------------------

def compute_accuracy_stats() -> dict:
    """Return accuracy statistics from the feedback table."""
    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN rating='correct' THEN 1 ELSE 0 END) as correct,
                SUM(CASE WHEN rating='incorrect' THEN 1 ELSE 0 END) as incorrect,
                SUM(CASE WHEN rating='partially_correct' THEN 1 ELSE 0 END) as partial
            FROM feedback
        """).fetchone()
        return dict(row) if row else {"total": 0, "correct": 0, "incorrect": 0, "partial": 0}
    finally:
        conn.close()


def log_accuracy_run() -> None:
    """Append a row to accuracy_log.csv for the current pipeline run."""
    stats = compute_accuracy_stats()
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    append_accuracy_log(
        run_date  = run_date,
        total     = stats.get("total", 0),
        correct   = stats.get("correct", 0),
        incorrect = stats.get("incorrect", 0),
        partial   = stats.get("partial", 0),
    )


# ---------------------------------------------------------------------------
# Interactive review CLI
# ---------------------------------------------------------------------------

def interactive_review() -> None:
    """
    Show recent classified signals and prompt the user for feedback.
    Saves ratings to the DB.
    """
    print("\n" + "="*60)
    print("  VMS Signal Classification Review")
    print("  Rate each classification: c=correct, i=incorrect, p=partial, s=skip, q=quit")
    print("="*60 + "\n")

    signals = get_signals_since(days=7)
    if not signals:
        print("No signals in the last 7 days to review.")
        return

    reviewed = 0
    for sig in signals[:50]:  # cap at 50 signals per review session
        print(f"\n{'─'*50}")
        print(f"Title:      {sig.get('title', 'N/A')[:90]}")
        print(f"Source:     {sig.get('source_label') or sig.get('authority', 'N/A')}")
        print(f"Ingredient: {sig.get('ingredient_name', 'N/A')}")
        print(f"Event Type: {sig.get('event_type', 'N/A')}")
        print(f"Severity:   {sig.get('severity', 'N/A')}")
        if sig.get("summary"):
            print(f"Summary:    {sig['summary'][:120]}")
        if sig.get("sentiment"):
            print(f"Sentiment:  {sig['sentiment']} (confidence: {sig.get('sentiment_confidence', 0):.2f})")

        while True:
            choice = input("\nRating [c/i/p/s/q]: ").strip().lower()
            if choice in ("q", "quit"):
                print(f"\nReviewed {reviewed} signal(s). Exiting.")
                _print_stats()
                return
            if choice in ("s", "skip", ""):
                break
            if choice in ("c", "correct"):
                save_feedback(sig["source_id"], "correct")
                reviewed += 1
                break
            if choice in ("i", "incorrect"):
                note = input("Correction note (optional, press Enter to skip): ").strip()
                save_feedback(sig["source_id"], "incorrect", note)
                reviewed += 1
                break
            if choice in ("p", "partial"):
                note = input("Correction note (optional, press Enter to skip): ").strip()
                save_feedback(sig["source_id"], "partially_correct", note)
                reviewed += 1
                break
            print("Invalid input. Use c, i, p, s, or q.")

    print(f"\nReviewed {reviewed} signal(s).")
    _print_stats()
    log_accuracy_run()


def _print_stats() -> None:
    stats = compute_accuracy_stats()
    total = stats.get("total", 0)
    if total == 0:
        print("\nNo feedback recorded yet.")
        return
    correct = stats.get("correct", 0)
    acc = round(correct / total * 100, 1)
    print(f"\nAccuracy stats: {total} rated — {correct} correct ({acc}%) · "
          f"{stats.get('incorrect', 0)} incorrect · {stats.get('partial', 0)} partial")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="VMS Classification Feedback Tool")
    parser.add_argument("--review", action="store_true", help="Interactively review recent classifications")
    parser.add_argument("--stats",  action="store_true", help="Print accuracy statistics")
    args = parser.parse_args()

    if args.stats:
        _print_stats()
        if ACCURACY_CSV.exists():
            print(f"\nAccuracy log: {ACCURACY_CSV}")
        return

    if args.review:
        interactive_review()
        return

    parser.print_help()


if __name__ == "__main__":
    main()
