"""Read-only Food/VMS counts using customer visibility rules. Optionally compare a verified backup."""
import argparse
from collections import Counter
import json
import sqlite3
from pathlib import Path

from analytics.db import DB_PATH
from services.food_duplicates import filter_visible_food_duplicates
from services.food_taxonomy import enrich_food_signal
from services.food_text_sanitizer import contains_food_text_contamination


def snapshot(path):
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as c:
        c.row_factory = sqlite3.Row
        if [r[0] for r in c.execute("pragma integrity_check")] != ["ok"] or c.execute("pragma foreign_key_check").fetchone():
            raise RuntimeError("SQLite integrity failure")
        rows = [dict(r) for r in c.execute("select * from signals where domain in ('food','vms') order by scraped_at desc")]
        by_domain = dict(c.execute("select coalesce(domain,''),count(*) from signals group by domain"))
    sources = {}
    for r in rows:
        key = r["domain"] + "/" + (r["source_label"] or r["authority"])
        count, date = sources.get(key, (0, ""))
        sources[key] = count+1, max(date, (r["created_at"] or "")[:10])
    food = [enrich_food_signal(r) for r in rows if r["domain"] == "food"]
    eligible = [r for r in food if not r["is_noise"] and r["dashboard_section"] != "excluded"]
    visible = filter_visible_food_duplicates(eligible)
    vms = [r for r in rows if r["domain"] == "vms" and not r["is_noise"] and (r["summary"] or "").strip()
           and r["source_label"] not in ("europe_pmc","biorxiv")]
    fields = ("summary","why_it_matters","recommended_action")
    def counts(items):
        return dict(sorted(Counter(items).items()))
    return rows, dict(domains=by_domain, sources=dict(sorted(sources.items())), visible_food=len(visible),
                     food_ineligible=len(food)-len(eligible), additional_duplicates_hidden=len(eligible)-len(visible),
                     food_sections=counts(r["dashboard_section"] for r in visible),
                     food_impact=counts(r["impact"] for r in visible), food_types=counts(r["signal_type"] for r in visible),
                     food_visible_vms_wording=sum(any(contains_food_text_contamination(r.get(k)) for k in
                        (*fields,"ai_summary","sentiment_reasoning","inspection_risk")) for r in visible),
                     food_visible_off=counts(r["dashboard_section"] for r in visible if r["source_label"]=="open_food_facts"),
                     vms_visible=len(vms), vms_severity=counts(r["severity"] for r in vms),
                     missing_text={name:{k:sum(not (r.get(k) or "").strip() for r in items) for k in fields}
                                   for name,items in (("food",visible),("vms",vms))})


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path, default=DB_PATH)
    p.add_argument("--before", type=Path)
    args = p.parse_args()
    rows, after = snapshot(args.db)
    if args.before:
        initial, before = snapshot(args.before)
        print("BEFORE", json.dumps(before, indent=2))
        index = {r["id"]:r for r in rows}
        if any(r["id"] not in index or any(index[r["id"]][k] != r[k] for k in ("domain","source_id","created_at")) for r in initial):
            raise RuntimeError("Existing identity changed")
        changed = [r for r in initial if r != index[r["id"]]]
        print("Existing rows updated", Counter((r["domain"], r["source_label"] or r["authority"]) for r in changed))
    print("SNAPSHOT", json.dumps(after, indent=2))


if __name__ == "__main__":
    main()
