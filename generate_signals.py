"""
generate_signals.py — Update the embedded JSON data blob in signals.html.

Reads:
  - data/signals.db               → live regulatory/research signals (SQLite)
  - reports/citation_database.json → compliance citations

Patches:
  signals.html — replaces ONLY the <script id="signalex-data"> block.
  All HTML, CSS, and JavaScript outside that block is left untouched.

Usage:
  python generate_signals.py          # update signals.html in-place
  from generate_signals import update_data_blob; update_data_blob()
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

BASE     = Path(__file__).parent
HTML_PATH = BASE / "signals.html"
CIT_PATH  = BASE / "reports" / "citation_database.json"

# Regex that matches the entire signalex-data script block (including tags)
_DATA_BLOCK_RE = re.compile(
    r'<script id="signalex-data">.*?</script>',
    re.DOTALL,
)


def build_data_blob(days: int = 30) -> str:
    """
    Load fresh signals + citations, return the full replacement
    <script id="signalex-data">…</script> string.
    """
    from analytics.db import get_signals_since

    # Raw DB records — same format the dashboard JS expects
    signals_raw = get_signals_since(days)

    # Citations — direct from JSON, no transformation needed
    cit_raw = json.loads(CIT_PATH.read_text(encoding="utf-8"))
    citations = cit_raw.get("citations", [])

    # META
    last_updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    meta = {
        "lastUpdated":   last_updated,
        "signalCount":   len(signals_raw),
        "citationCount": len(citations),
    }

    signals_json    = json.dumps(signals_raw, separators=(",", ":"), ensure_ascii=False)
    citations_json  = json.dumps(citations,   separators=(",", ":"), ensure_ascii=False)
    meta_json       = json.dumps(meta,         separators=(",", ":"))

    return (
        '<script id="signalex-data">\n'
        f'const SIGNALS_DATA = {signals_json};\n'
        f'const CITATIONS_DATA = {citations_json};\n'
        f'const META = {meta_json};\n'
        '</script>'
    )


def update_data_blob(html_path: Path | None = None, days: int = 30) -> dict:
    """
    Patch the signalex-data block in signals.html.
    Returns a summary dict: {signals, citations, last_updated, html_path}.
    """
    path = Path(html_path) if html_path else HTML_PATH

    original = path.read_text(encoding="utf-8")
    if not _DATA_BLOCK_RE.search(original):
        raise RuntimeError(
            f"No <script id=\"signalex-data\"> block found in {path}. "
            "Run the one-time migration first."
        )

    new_block = build_data_blob(days=days)
    patched   = _DATA_BLOCK_RE.sub(new_block, original, count=1)

    if patched == original:
        logger.warning("Data blob unchanged — signals.html not rewritten")
    else:
        path.write_text(patched, encoding="utf-8")
        size_kb = round(len(patched) / 1024)
        logger.info("signals.html updated (%d KB)", size_kb)

    # Parse META from the new block for the summary
    meta_match = re.search(r'const META = ({.*?});', new_block)
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
        f"\nsignals.html data blob refreshed\n"
        f"  Signals:      {result['signals']}\n"
        f"  Citations:    {result['citations']}\n"
        f"  Last updated: {result['last_updated']}\n"
        f"  File:         {result['html_path']}\n"
    )
