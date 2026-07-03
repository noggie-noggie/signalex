"""
migrations/audit_food_taxonomy.py — read-only Food taxonomy audit.

Prints dashboard taxonomy counts and legacy rows that look like recalls despite
being stored under older regulatory/update labels.

Usage:
    python migrations/audit_food_taxonomy.py
"""

from __future__ import annotations

import sqlite3
import re
from collections import Counter
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.food_taxonomy import enrich_food_signal, possible_misclassified_recall


DB_PATH = ROOT / "data" / "signals.db"


def main() -> int:
    if not DB_PATH.exists():
        print(f"ERROR: signals database not found: {DB_PATH}")
        return 1

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        raw_rows = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM signals WHERE domain = 'food' ORDER BY scraped_at DESC"
            ).fetchall()
        ]
    finally:
        conn.close()

    rows = [enrich_food_signal(row) for row in raw_rows]
    visible_rows = [
        row for row in rows
        if int(row.get("is_noise") or 0) != 1
        and row.get("dashboard_section") != "excluded"
    ]
    by_signal_type = Counter(row.get("signal_type") or "" for row in rows)
    by_dashboard_section = Counter(row.get("dashboard_section") or "" for row in visible_rows)
    missing_signal_type = [row for row in rows if not row.get("signal_type")]
    missing_dashboard_section = [row for row in rows if not row.get("dashboard_section")]
    weak_category = [row for row in rows if row.get("category") == ["other"]]
    weak_product_type = [row for row in rows if not row.get("product_type")]
    weak_examples = [
        row for row in rows
        if row.get("category") == ["other"] or not row.get("product_type")
    ][:10]
    possible_misclassified = [
        (raw, enrich_food_signal(raw))
        for raw in raw_rows
        if possible_misclassified_recall(raw)
    ]
    fsanz_updates = [
        row for row in rows
        if row.get("source_label") == "food_fsanz_updates"
    ]
    visible_fsanz_updates = [
        row for row in fsanz_updates
        if int(row.get("is_noise") or 0) != 1
        and row.get("dashboard_section") != "excluded"
    ]
    excluded_fsanz_updates = [
        row for row in fsanz_updates
        if int(row.get("is_noise") or 0) == 1
        or row.get("dashboard_section") == "excluded"
    ]
    low_confidence_fsanz_updates = [
        row for row in fsanz_updates
        if row.get("food_relevance_confidence") == "low"
        and row.get("dashboard_section") != "excluded"
    ]
    visible_off_rows = [
        row for row in visible_rows
        if row.get("source_label") == "open_food_facts"
    ]
    off_by_dashboard_section = Counter(row.get("dashboard_section") or "" for row in visible_off_rows)
    off_market_opportunities = [
        row for row in visible_off_rows
        if row.get("dashboard_section") == "market_opportunities"
    ]
    off_category_signals = [
        row for row in visible_off_rows
        if row.get("dashboard_section") == "category_signals"
    ]
    generic_opportunity_terms = [
        "energy drink",
        "red bull",
        "monster",
        "v energy",
        "fresh farm cage eggs",
        "cage eggs",
        "mi goreng",
        "fried noodles",
        "potato crisps",
        "cheese & onion",
        "cheese and onion",
        "wraps lite",
        "peri-peri rub",
        "peri peri rub",
        "garlic peri",
    ]
    alcohol_terms = [
        "whisky",
        "whiskey",
        "scotch",
        "vodka",
        "gin",
        "rum",
        "tequila",
        "bourbon",
        "wine",
        "beer",
        "cider",
        "liqueur",
        "alcoholic beverage",
    ]
    unknown_terms = ["unknown product", "product unknown"]
    def off_text(row: dict) -> str:
        return " ".join(
            str(row.get(field) or "").lower()
            for field in ("title", "summary", "product_name", "product_category")
        )
    def contains_term(text: str, term: str) -> bool:
        if " " in term:
            return term in text
        return re.search(rf"(?<![a-z]){re.escape(term)}(?![a-z])", text) is not None

    generic_product_opportunities = [
        row for row in off_market_opportunities
        if any(term in off_text(row) for term in generic_opportunity_terms)
    ]
    generic_visible_off_records = [
        row for row in visible_off_rows
        if any(term in off_text(row) for term in generic_opportunity_terms)
    ]
    visible_alcohol_off_records = [
        row for row in visible_off_rows
        if any(contains_term(off_text(row), term) for term in alcohol_terms)
    ]
    visible_unknown_off_records = [
        row for row in visible_off_rows
        if any(term in off_text(row) for term in unknown_terms)
    ]
    excluded_off_rows = [
        row for row in rows
        if row.get("source_label") == "open_food_facts"
        and (
            int(row.get("is_noise") or 0) == 1
            or row.get("dashboard_section") == "excluded"
        )
    ]

    print("Food taxonomy audit")
    print("===================")
    print(f"total food signals: {len(rows)}")
    print(f"visible customer-facing food signals: {len(visible_rows)}")

    print("\ncount by signal_type:")
    for key, count in sorted(by_signal_type.items()):
        print(f"  {key or '(missing)'}: {count}")

    print("\nvisible count by dashboard_section:")
    for key, count in sorted(by_dashboard_section.items()):
        print(f"  {key or '(missing)'}: {count}")

    print(f"\nmissing signal_type: {len(missing_signal_type)}")
    print(f"missing dashboard_section: {len(missing_dashboard_section)}")
    print(f'category ["other"]: {len(weak_category)}')
    print(f"empty product_type: {len(weak_product_type)}")

    print("\ntop weakly classified records:")
    if not weak_examples:
        print("  none")
    else:
        for row in weak_examples:
            print(
                "  "
                f"id={row.get('id')} "
                f"category={row.get('category')} "
                f"product_type={row.get('product_type')} "
                f"title={row.get('title')}"
            )

    print("\npossible legacy regulatory/update rows now classified as recalls:")
    if not possible_misclassified:
        print("  none")
    else:
        for raw, row in possible_misclassified:
            print(
                "  "
                f"id={row.get('id')} "
                f"stored_type={raw.get('signal_type') or raw.get('event_type')} "
                f"source={row.get('source_label')} "
                f"title={row.get('title')}"
            )

    recall_leaks = [
        row for row in rows
        if row.get("signal_type") == "recall"
        and row.get("dashboard_section") != "recalls_safety"
    ]
    opportunity_leaks = [
        row for row in rows
        if row.get("dashboard_section") == "market_opportunities"
        and row.get("signal_type") in {"recall", "safety_alert", "regulatory_update", "consultation"}
    ]

    print(f"\nrecalls outside recalls_safety: {len(recall_leaks)}")
    print(f"risk/regulatory rows inside market_opportunities: {len(opportunity_leaks)}")

    print("\nOpen Food Facts visible records by dashboard_section:")
    if not off_by_dashboard_section:
        print("  none")
    else:
        for key, count in sorted(off_by_dashboard_section.items()):
            print(f"  {key or '(missing)'}: {count}")

    print("\nOpen Food Facts market_opportunities titles:")
    if not off_market_opportunities:
        print("  none")
    else:
        for row in off_market_opportunities:
            print(
                "  "
                f"id={row.get('id')} "
                f"impact={row.get('impact')} "
                f"claim_theme={row.get('claim_theme')} "
                f"product_type={row.get('product_type')} "
                f"title={row.get('title')}"
            )

    print("\nOpen Food Facts category_signals titles:")
    if not off_category_signals:
        print("  none")
    else:
        for row in off_category_signals:
            print(
                "  "
                f"id={row.get('id')} "
                f"impact={row.get('impact')} "
                f"claim_theme={row.get('claim_theme')} "
                f"product_type={row.get('product_type')} "
                f"title={row.get('title')}"
            )

    print("\ngeneric Open Food Facts records inside market_opportunities for manual review:")
    if not generic_product_opportunities:
        print("  none")
    else:
        for row in generic_product_opportunities:
            print(
                "  "
                f"id={row.get('id')} "
                f"claim_theme={row.get('claim_theme')} "
                f"product_type={row.get('product_type')} "
                f"title={row.get('title')}"
            )

    print("\ngeneric Open Food Facts records visible for manual review:")
    if not generic_visible_off_records:
        print("  none")
    else:
        for row in generic_visible_off_records:
            print(
                "  "
                f"id={row.get('id')} "
                f"section={row.get('dashboard_section')} "
                f"claim_theme={row.get('claim_theme')} "
                f"product_type={row.get('product_type')} "
                f"title={row.get('title')}"
            )

    print("\nalcohol Open Food Facts records visible for manual review:")
    if not visible_alcohol_off_records:
        print("  none")
    else:
        for row in visible_alcohol_off_records:
            print(
                "  "
                f"id={row.get('id')} "
                f"section={row.get('dashboard_section')} "
                f"title={row.get('title')}"
            )

    print("\nunknown Open Food Facts records visible for manual review:")
    if not visible_unknown_off_records:
        print("  none")
    else:
        for row in visible_unknown_off_records:
            print(
                "  "
                f"id={row.get('id')} "
                f"section={row.get('dashboard_section')} "
                f"title={row.get('title')}"
            )

    print("\nOpen Food Facts excluded/noise titles and reasons:")
    if not excluded_off_rows:
        print("  none")
    else:
        for row in excluded_off_rows:
            print(
                "  "
                f"id={row.get('id')} "
                f"reason={row.get('noise_reason') or '(not set)'} "
                f"title={row.get('title')}"
            )

    print("\nFSANZ update relevance:")
    print(f"  visible FSANZ updates: {len(visible_fsanz_updates)}")
    print(f"  excluded FSANZ updates: {len(excluded_fsanz_updates)}")

    print("\nexcluded FSANZ updates:")
    if not excluded_fsanz_updates:
        print("  none")
    else:
        for row in excluded_fsanz_updates:
            print(
                "  "
                f"id={row.get('id')} "
                f"type={row.get('fsanz_content_type')} "
                f"score={row.get('food_relevance_score')} "
                f"reason={row.get('food_relevance_reason')} "
                f"title={row.get('title')}"
            )

    print("\nlow-confidence visible FSANZ updates for manual review:")
    if not low_confidence_fsanz_updates:
        print("  none")
    else:
        for row in low_confidence_fsanz_updates:
            print(
                "  "
                f"id={row.get('id')} "
                f"type={row.get('fsanz_content_type')} "
                f"score={row.get('food_relevance_score')} "
                f"reason={row.get('food_relevance_reason')} "
                f"title={row.get('title')}"
            )

    return 1 if missing_signal_type or missing_dashboard_section or recall_leaks or opportunity_leaks else 0


if __name__ == "__main__":
    raise SystemExit(main())
