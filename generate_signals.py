"""
generate_signals.py — Refresh the data block in signals.html.

Reads:
  - data/signals.db               → live regulatory/research signals (SQLite)
  - reports/citation_database.json → compliance citations

Patches signals.html by replacing ONLY the block between:
  // === SIGNALEX DATA START ===
  ...
  // === SIGNALEX DATA END ===

All HTML, CSS, and JavaScript outside that block is left untouched.

Usage:
  python generate_signals.py          # refresh signals.html in-place
  from generate_signals import update_data_blob; update_data_blob()
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

BASE      = Path(__file__).parent
HTML_PATH = BASE / "signals.html"
CIT_PATH  = BASE / "reports" / "citation_database.json"

_DATA_BLOCK_RE = re.compile(
    r'// === SIGNALEX DATA START ===\n.*?\n// === SIGNALEX DATA END ===',
    re.DOTALL,
)


def build_data_block(days: int = 30) -> str:
    """Return the replacement block (markers + fresh data)."""
    from analytics.db import get_signals_since

    signals_raw = get_signals_since(days)

    cit_raw   = json.loads(CIT_PATH.read_text(encoding="utf-8"))
    citations = cit_raw.get("citations", [])

    last_updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    meta = {
        "lastUpdated":   last_updated,
        "signalCount":   len(signals_raw),
        "citationCount": len(citations),
    }

    signals_json   = json.dumps(signals_raw, separators=(",", ":"), ensure_ascii=False)
    citations_json = json.dumps(citations,   separators=(",", ":"), ensure_ascii=False)
    meta_json      = json.dumps(meta,         separators=(",", ":"))

    return (
        "// === SIGNALEX DATA START ===\n"
        f"const SIGNALS = {signals_json};\n"
        f"const CITATIONS = {citations_json};\n"
        f"const SIGNALEX_META = {meta_json};\n"
        "// === SIGNALEX DATA END ==="
    )


def update_data_blob(html_path: Path | None = None, days: int = 30) -> dict:
    """
    Patch the data block in signals.html between the SIGNALEX markers.
    Returns a summary dict: {signals, citations, last_updated, html_path}.
    signals.html layout, CSS, and JS logic are never touched.
    """
    path = Path(html_path) if html_path else HTML_PATH

    original = path.read_text(encoding="utf-8")
    if not _DATA_BLOCK_RE.search(original):
        raise RuntimeError(
            f"Marker block not found in {path}.\n"
            "Expected: // === SIGNALEX DATA START === … // === SIGNALEX DATA END ==="
        )

    new_block = build_data_block(days=days)
    patched   = _DATA_BLOCK_RE.sub(lambda _: new_block, original, count=1)

    if patched == original:
        logger.warning("Data block unchanged — signals.html not rewritten")
    else:
        path.write_text(patched, encoding="utf-8")
        size_kb = round(len(patched) / 1024)
        logger.info("signals.html updated (%d KB)", size_kb)

    meta_match = re.search(r'const SIGNALEX_META = ({.*?});', new_block)
    meta = json.loads(meta_match.group(1)) if meta_match else {}

    return {
        "signals":      meta.get("signalCount", 0),
        "citations":    meta.get("citationCount", 0),
        "last_updated": meta.get("lastUpdated", ""),
        "html_path":    str(path),
    }


# ── CLI entry-point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    result = update_data_blob()
    print(
        f"\nsignals.html data block refreshed\n"
        f"  Signals:      {result['signals']}\n"
        f"  Citations:    {result['citations']}\n"
        f"  Last updated: {result['last_updated']}\n"
        f"  File:         {result['html_path']}\n"
    )
