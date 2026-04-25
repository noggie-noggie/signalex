"""
generate_signals.py — Export signal data to JSON files for the dashboard.

Reads:
  - data/signals.db               → live regulatory/research signals (SQLite)
  - reports/citation_database.json → compliance citations

Writes (data/ directory — signals.html fetches these at runtime):
  data/signals_export.json    — all signals from SQLite (array)
  data/citations_export.json  — compliance citations (array)
  data/meta.json              — lastUpdated timestamp + counts

signals.html is NEVER modified by this script.

Usage:
  python generate_signals.py          # export JSON files
  from generate_signals import export_json_files; export_json_files()
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

BASE     = Path(__file__).parent
DATA_DIR = BASE / "data"
CIT_PATH = BASE / "reports" / "citation_database.json"

SIGNALS_JSON_PATH   = DATA_DIR / "signals_export.json"
CITATIONS_JSON_PATH = DATA_DIR / "citations_export.json"
META_JSON_PATH      = DATA_DIR / "meta.json"


def export_json_files(days: int = 30) -> dict:
    """
    Export signals + citations to data/*.json.
    Returns a summary dict: {signals, citations, last_updated}.
    signals.html is not touched.
    """
    from analytics.db import get_signals_since

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    signals_raw = get_signals_since(days)

    cit_raw  = json.loads(CIT_PATH.read_text(encoding="utf-8"))
    citations = cit_raw.get("citations", [])

    last_updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    meta = {
        "lastUpdated":   last_updated,
        "signalCount":   len(signals_raw),
        "citationCount": len(citations),
    }

    SIGNALS_JSON_PATH.write_text(
        json.dumps(signals_raw, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )
    CITATIONS_JSON_PATH.write_text(
        json.dumps(citations, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )
    META_JSON_PATH.write_text(
        json.dumps(meta, separators=(",", ":"), indent=2),
        encoding="utf-8",
    )

    logger.info(
        "Exported %d signals, %d citations → data/",
        len(signals_raw), len(citations),
    )
    return {
        "signals":      len(signals_raw),
        "citations":    len(citations),
        "last_updated": last_updated,
    }


# ── CLI entry-point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    result = export_json_files()
    print(
        f"\nJSON export complete\n"
        f"  Signals:      {result['signals']}  → data/signals_export.json\n"
        f"  Citations:    {result['citations']}  → data/citations_export.json\n"
        f"  Meta:                    → data/meta.json\n"
        f"  Last updated: {result['last_updated']}\n"
    )
