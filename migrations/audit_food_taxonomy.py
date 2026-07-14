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

from services.food_duplicates import find_open_food_facts_category_duplicate_groups
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
    impact_by_dashboard_section = Counter(
        (row.get("dashboard_section") or "", row.get("impact") or "")
        for row in visible_rows
    )
    high_impact_rows = [row for row in visible_rows if row.get("impact") == "high"]
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
        "farm cage eggs",
        "mi goreng",
        "fried noodles",
        "instant noodles",
        "potato crisps",
        "crisps",
        "chips",
        "potato chips",
        "cheese & onion",
        "cheese and onion",
        "wraps lite",
        "plain wraps",
        "wraps",
        "peri-peri rub",
        "peri peri rub",
        "garlic peri",
        "caramel latte",
        "instant coffee",
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
            for field in ("title", "summary", "product_name", "product_category", "brand", "company")
        )
    def off_brand(row: dict) -> str:
        brand = str(row.get("brand") or row.get("company") or "").strip().lower()
        if brand:
            return re.sub(r"\s+", " ", brand)
        title = str(row.get("title") or row.get("product_name") or "").lower()
        title = re.split(r"\s+[â€”—-]+\s+|\s+-\s+", title, maxsplit=1)[0]
        return re.sub(r"[^a-z0-9]+", " ", title).strip() or "(unknown)"
    def contains_term(text: str, term: str) -> bool:
        if " " in term:
            return term in text
        return re.search(rf"(?<![a-z]){re.escape(term)}(?![a-z])", text) is not None

    generic_product_opportunities = [
        row for row in off_market_opportunities
        if any(term in off_text(row) for term in generic_opportunity_terms)
    ]
    generic_snack_terms = [
        "potato crisps",
        "potato chips",
        "corn chips",
        "crisps",
        "chips",
        "cheese & onion",
        "cheese and onion",
        "savoury snack",
        "savory snack",
    ]
    generic_snack_opportunities = [
        row for row in off_market_opportunities
        if any(term in off_text(row) for term in generic_snack_terms)
        and "protein_bar" not in (row.get("product_type") or [])
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
    off_category_signals_by_brand = Counter(off_brand(row) for row in off_category_signals)
    visible_energy_brand_rows = [
        row for row in off_category_signals
        if any(term in off_text(row) for term in ["red bull", "v energy", "monster"])
    ]
    visible_retailer_private_label_rows = [
        row for row in visible_off_rows
        if any(term in off_text(row) for term in ["woolworths", "woolworths bakery"])
    ]
    excluded_off_rows = [
        row for row in rows
        if row.get("source_label") == "open_food_facts"
        and (
            int(row.get("is_noise") or 0) == 1
            or row.get("dashboard_section") == "excluded"
        )
    ]
    low_quality_off_rows = [
        row for row in excluded_off_rows
        if row.get("noise_reason") == "Excluded from food launch: low-quality Open Food Facts record"
    ]
    suspicious_raw_off_rows = [
        row for row in rows
        if row.get("source_label") == "open_food_facts"
        and (
            "ingredients: h" in off_text(row)
            or "ingredient h" in off_text(row)
            or "ingrdients" in off_text(row)
            or "ingredints" in off_text(row)
            or "ingedients" in off_text(row)
            or "caffiene" in off_text(row)
            or "taur1ne" in off_text(row)
            or "glucuronolact0ne" in off_text(row)
            or "ingredienti" in off_text(row)
            or "ingredientes" in off_text(row)
            or re.search(r"\bingredients?\s*:\s*[a-z]\s*(?:$|[|.;,])", off_text(row))
            or (str(row.get("summary") or "").lower().startswith("ingredients:") and str(row.get("summary") or "").count(",") >= 8)
        )
    ]
    visible_suspicious_raw_off_rows = [
        row for row in suspicious_raw_off_rows
        if int(row.get("is_noise") or 0) != 1
        and row.get("dashboard_section") != "excluded"
    ]
    foreign_off_exclusions = [
        row for row in low_quality_off_rows
        if any(term in off_text(row) for term in ["ingredienti", "ingredientes", "prodotto in italia", "fabricado en"])
    ]
    off_category_duplicate_groups = find_open_food_facts_category_duplicate_groups(visible_off_rows)

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

    print("\nimpact distribution by dashboard_section:")
    if not impact_by_dashboard_section:
        print("  none")
    else:
        for (section, impact), count in sorted(impact_by_dashboard_section.items()):
            print(f"  {section or '(missing)'} / {impact or '(missing)'}: {count}")

    print("\nhigh-impact rows:")
    if not high_impact_rows:
        print("  none")
    else:
        for row in high_impact_rows:
            print(
                "  "
                f"id={row.get('id')} "
                f"section={row.get('dashboard_section')} "
                f"source={row.get('source_label')} "
                f"title={row.get('title')}"
            )

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

    print("\nOpen Food Facts category_signals visible records by brand:")
    if not off_category_signals_by_brand:
        print("  none")
    else:
        for brand, count in sorted(off_category_signals_by_brand.items()):
            print(f"  {brand}: {count}")

    print("\nremaining Red Bull/V/Monster category_signals visible rows:")
    if not visible_energy_brand_rows:
        print("  none")
    else:
        for row in visible_energy_brand_rows:
            print(
                "  "
                f"id={row.get('id')} "
                f"brand={off_brand(row)} "
                f"title={row.get('title')} "
                f"summary={(row.get('summary') or '')[:100]}"
            )

    print("\nvisible retailer/private-label Open Food Facts rows:")
    if not visible_retailer_private_label_rows:
        print("  none")
    else:
        for row in visible_retailer_private_label_rows:
            print(
                "  "
                f"id={row.get('id')} "
                f"section={row.get('dashboard_section')} "
                f"claim_theme={row.get('claim_theme')} "
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

    print("\ngeneric snack Open Food Facts records inside market_opportunities for manual review:")
    if not generic_snack_opportunities:
        print("  none")
    else:
        for row in generic_snack_opportunities:
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

    print("\nOpen Food Facts low-quality exclusions:")
    if not low_quality_off_rows:
        print("  none")
    else:
        for row in low_quality_off_rows:
            print(
                "  "
                f"id={row.get('id')} "
                f"title={row.get('title')} "
                f"summary={(row.get('summary') or '')[:120]}"
            )

    print("\nOpen Food Facts category duplicate groups collapsed:")
    if not off_category_duplicate_groups:
        print("  none")
    else:
        for group in off_category_duplicate_groups:
            print(
                "  "
                f"kept_id={group.kept_id} duplicate_ids={group.duplicate_ids} key={group.key}"
            )

    print("\nrows with suspicious raw ingredient/OCR text:")
    if not suspicious_raw_off_rows:
        print("  none")
    else:
        for row in suspicious_raw_off_rows:
            print(
                "  "
                f"id={row.get('id')} "
                f"reason={row.get('noise_reason') or '(visible)'} "
                f"title={row.get('title')} "
                f"summary={(row.get('summary') or '')[:120]}"
            )

    print("\nvisible rows with suspicious raw ingredient/OCR text:")
    if not visible_suspicious_raw_off_rows:
        print("  none")
    else:
        for row in visible_suspicious_raw_off_rows:
            print(
                "  "
                f"id={row.get('id')} "
                f"section={row.get('dashboard_section')} "
                f"title={row.get('title')} "
                f"summary={(row.get('summary') or '')[:120]}"
            )

    print("\nnon-English/foreign-market Open Food Facts exclusions:")
    if not foreign_off_exclusions:
        print("  none")
    else:
        for row in foreign_off_exclusions:
            print(
                "  "
                f"id={row.get('id')} "
                f"title={row.get('title')} "
                f"summary={(row.get('summary') or '')[:120]}"
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
