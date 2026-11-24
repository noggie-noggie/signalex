"""Explicit, budget-bounded VMS classification into a new review file, never the DB."""
import argparse
import json
import os
from pathlib import Path

import config
from services.vms_evidence import classify as source_fact
from maintenance.collect import FOOD, SOURCES


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("manifest", type=Path)
    p.add_argument("--output", type=Path, help="Alternate new private review file; never overwrite")
    p.add_argument("--max-calls", type=int, default=0, help="Opt in to paid drafts for unresolved records, for human review only")
    args = p.parse_args()
    os.umask(0o077)
    data = json.loads(args.manifest.read_text(encoding="utf8"))
    source = data["source"]
    if source not in SOURCES or source in FOOD or data["status"] != "parsed" or data.get("classified"):
        p.error("Requires a successful, raw VMS collection")
    if args.max_calls < 0 or (args.max_calls and not config.ANTHROPIC_API_KEY):
        p.error("Paid drafting requires a nonnegative budget and configured Anthropic credentials")
    classifier = None
    method = {
        "tga_consultations": "tga_consultation", "adverse_events": "adverse_event",
        "advisory_committee": "advisory_committee", "chemist_warehouse": "retail", "iherb": "retail",
    }.get(source, source)
    output = args.output or args.manifest.with_suffix(".classified.json")
    if output.exists():
        p.error("Output already exists")
    rows = []
    held = []
    calls = 0
    for raw in data["rows"]:
        fact = source_fact(source, raw)
        if fact:
            rows.append(fact)
            continue
        hold = {"source_id": raw["source_id"], "reason": "Manual evidence/relevance review required"}
        if calls < args.max_calls:
            from classifier.claude import SignalClassifier
            classifier = classifier or SignalClassifier()
            classify = classifier.classify if source in {"tga","fda"} else getattr(classifier, "classify_"+method)
            calls += 1
            result = classify(raw)
            if result.summary.strip() and result.output_tokens:
                hold["draft"] = result.model_dump()
        held.append(hold)
    with output.open("x", encoding="utf8") as stream:
        json.dump(dict(data, classified=True, rows=rows, held=held), stream)
    print("Source-supported:", len(rows), "held:", len(held), "paid calls:", calls, output)


if __name__ == "__main__":
    main()
