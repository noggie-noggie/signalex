"""Review or apply one locally collected source. Never replace the DB or an existing ID."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
from urllib.parse import urlsplit

from analytics.db import DB_PATH
from maintenance.collect import FOOD, SOURCES
from services.food_duplicates import canonical_food_url, normalise_food_title
from services.food_supplement_filter import is_supplement_like_food
from services.vms_evidence import classify as source_fact


def backup(db, folder):
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    dest = folder / ("pre-refresh-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + ".db")
    dest.touch(mode=0o600, exist_ok=False)
    with sqlite3.connect(db.resolve().as_uri() + "?mode=ro", uri=True) as src, sqlite3.connect(dest) as dst:
        src.execute("BEGIN")
        src.backup(dst)
        if dst.execute("pragma integrity_check").fetchall() != [("ok",)] or dst.execute("pragma foreign_key_check").fetchone():
            raise ValueError("Backup integrity failed")
        digest = lambda conn: hashlib.sha256("\n".join(conn.iterdump()).encode()).digest()
        if digest(src) != digest(dst):
            raise ValueError("Backup contents differ")
    print("verified backup", dest, hashlib.sha256(dest.read_bytes()).hexdigest())
    return dest


def prepared(source, raw):
    if source in FOOD:
        if raw.get("source_label") != source or raw.get("domain") != "food":
            raise ValueError("Food source/domain mismatch")
        if source == "open_food_facts" and is_supplement_like_food(raw):
            return None
        data = {k: raw[k] for k in ("source_id", "authority", "url", "title", "summary", "scraped_at",
                                    "source_label", "company", "brand", "product_name", "allergen",
                                    "claim", "signal_type", "severity", "recommended_action") if k in raw}
        data.update(domain="food", product_category=raw.get("category", ""),
                    ingredient_name=raw.get("ingredient", ""), event_type=raw["signal_type"])
        return data
    if (source not in SOURCES or raw.get("domain") not in (None,"","vms")
            or raw.get("authority") != source or not (raw.get("summary") or "").strip()):
        raise ValueError("Refusing unclassified/fallback VMS row")
    # Re-derive allowed fields; token counts or a hand-set flag are not evidence.
    raw = source_fact(source, raw)
    if not raw:
        raise ValueError("No publishable source-supported VMS classification")
    fields = ("source_id authority url title scraped_at summary severity ingredient_name event_type "
              "signal_type product_category market_significance relevance_to_vms ingredient_relevance "
              "potential_impact trend_relevance competitor_signal australia_relevance australia_reasoning "
              "clean_title why_it_matters recommended_action inspection_risk is_noise noise_reason").split()
    return dict({k: raw[k] for k in fields if k in raw}, domain="vms", source_label=source)


def merge(conn, source, rows):
    counts = Counter()
    for raw in rows:
        d = prepared(source, raw)
        if d is None:
            counts["excluded_before_insert"] += 1
            continue
        if not d.get("source_id") or not d.get("title") or urlsplit(d.get("url", "")).scheme not in {"http", "https"}:
            raise ValueError("Missing source identity")
        old = conn.execute("SELECT * FROM signals WHERE source_id=?", (d["source_id"],)).fetchone()
        if old and (old["domain"] != d["domain"] or (old["source_label"] or old["authority"]) != source):
            raise ValueError("ID collision across sources/domains")
        if not old and source in FOOD:
            matches = conn.execute("""SELECT * FROM signals WHERE domain=? AND
                coalesce(nullif(source_label,''),authority)=? AND url=? ORDER BY id""",
                (d["domain"], source, d["url"])).fetchall()
            if len(matches) > 1:
                counts["ambiguous_skipped"] += 1
                continue
            old = next(iter(matches), None)
        if not old and source in {"pubmed", "cochrane", "europe_pmc", "clinical_trials"} and conn.execute(
            "SELECT 1 FROM signals WHERE domain='vms' AND url=? LIMIT 1", (d["url"],)
        ).fetchone():
            counts["duplicate_skipped"] += 1
            continue
        if not old and source.startswith("food_fsanz_"):
            # Different Drupal alias for the same article. Never merge distinct titles by suffix alone.
            if any(canonical_food_url(r["url"]) == canonical_food_url(d["url"]) and
                   normalise_food_title(r["title"]) == normalise_food_title(d["title"])
                   for r in conn.execute("SELECT url,title FROM signals WHERE domain='food' AND source_label=?", (source,))):
                counts["duplicate_skipped"] += 1
                continue
        if not old:
            d["created_at"] = datetime.now(timezone.utc).isoformat()
            keys = ",".join(d)
            conn.execute(f"INSERT INTO signals ({keys}) VALUES ({','.join('?' for _ in d)})", tuple(d.values()))
            counts["new"] += 1
        else:
            # Keep operator exclusions, identity, enrichment and original observation time.
            keep = {"domain", "source_label", "authority", "source_id", "url", "scraped_at", "is_noise", "noise_reason"}
            if source == "open_food_facts" and str(d.get("scraped_at", "")) < str(old["scraped_at"] or ""):
                counts["older_skipped"] += 1
                continue
            changes = {k:v for k,v in d.items() if k not in keep and v not in (None, "") and old[k] != v}
            if source == "open_food_facts" and d.get("scraped_at") and d["scraped_at"] > (old["scraped_at"] or ""):
                changes["scraped_at"] = d["scraped_at"]
            if source not in FOOD:
                # Source-fact imports must not downgrade an analyst's prior assessment.
                changes = {k:v for k,v in changes.items() if not old[k] or k == "title" or
                           (k == "summary" and old[k] == (source_fact(source, dict(old)) or {}).get("summary"))}
            if source not in FOOD and d.get("is_noise") and not old["is_noise"]:
                changes.update(is_noise=1, noise_reason=d.get("noise_reason") or "VMS relevance review")
            if changes:
                assignments = ",".join(k+"=?" for k in changes)
                conn.execute(f"UPDATE signals SET {assignments} WHERE id=? AND domain=?",
                             (*changes.values(), old["id"], d["domain"]))
            counts["updated" if changes else "unchanged"] += 1
    if source in FOOD and (counts["new"] or counts["updated"]):
        from services.food_duplicates import mark_food_duplicates
        mark_food_duplicates(conn, apply=True, commit=False, sync_quality=True, source=source)
    return counts


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("manifest", type=Path, nargs="?")
    p.add_argument("--db", type=Path, default=DB_PATH)
    p.add_argument("--backup-dir", type=Path)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--accept-partial-off", action="store_true",
                   help="Import only validated OFF products from successful pages; does not certify coverage.")
    args = p.parse_args()
    if args.apply and not any(a == "--db" or a.startswith("--db=") for a in sys.argv[1:]):
        p.error("Specify the existing target database explicitly with --db")
    os.umask(0o077)
    if not args.db.is_file():
        p.error("Existing database required")
    if not args.manifest:
        if not args.backup_dir:
            p.error("--backup-dir is required")
        backup(args.db, args.backup_dir)
    if not args.manifest:
        return
    data = json.loads(args.manifest.read_text(encoding="utf8"))
    source = data["source"]
    if (source not in FOOD and data.get("classified") is True and not data["rows"] and
            data["status"] == "parsed" and data.get("coverage") == "query_window_verified"):
        print(source, "no publishable records; held for review:", len(data.get("held", [])))
        return
    if args.apply and data.get("coverage") != "query_window_verified" and not (
        source == "open_food_facts" and args.accept_partial_off
    ):
        p.error("Coverage not verified; refusing automatic publication")
    partial_off = args.accept_partial_off and source == "open_food_facts" and bool(data.get("incomplete_queries"))
    if source not in SOURCES or (data["status"] != "parsed" and not partial_off) or not data["rows"]:
        p.error("Review incomplete/empty source separately; not importing")
    if source not in FOOD and data.get("classified") is not True:
        p.error("Raw VMS candidates require successful classification before merge")
    if args.apply:
        if not args.backup_dir:
            p.error("--backup-dir is required")
        backup(args.db, args.backup_dir)
    with sqlite3.connect(args.db.resolve().as_uri() + "?mode=rw", uri=True) as c:
        c.row_factory = sqlite3.Row
        c.execute("BEGIN IMMEDIATE")
        result = merge(c, source, data["rows"])
        if args.apply:
            c.commit()
        else:
            c.rollback()
    print(source, "applied" if args.apply else "rolled back preview", dict(result))


if __name__ == "__main__":
    main()
