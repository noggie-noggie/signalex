"""
analytics/ingredient_aliases.py — Canonical ingredient resolution.

Provides a read-time normalisation layer that groups alias strings
(as stored verbatim in ingredient_name) under a single canonical label.

Design constraints
------------------
- No DB changes.  ingredient_name is preserved exactly as written.
- No classifier changes.  Aliases accumulate naturally over time.
- Resolution is pure functional: canonical(raw) -> str, no side effects.
- Covers only the 9 highest-fragmentation clusters identified in the
  2026-06 alias audit.  Additional clusters can be added incrementally.

Usage
-----
    from analytics.ingredient_aliases import canonical

    canon = canonical(signal["ingredient_name"])  # e.g. "Probiotics"
    key   = canon.lower()                         # for grouping / dict keys
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Alias map — keyed by normalised lowercase of the raw ingredient_name string.
# Values are the canonical display label (title-case, human-readable).
# ---------------------------------------------------------------------------
#
# Normalisation applied before lookup:
#   1. strip whitespace
#   2. lower-case
#   3. β → beta, α → alpha
#
# Fallback: if no direct hit, the trailing parenthetical is stripped and the
# remainder is looked up again (e.g. "vitamin D3 (cholecalciferol)" → "vitamin d3").

_ALIAS_MAP: dict[str, str] = {

    # ── Probiotics ────────────────────────────────────────────────────────────
    # Audit: 6 aliases, 47 combined signals (42 visible without this fix)
    "probiotics":               "Probiotics",
    "probiotic":                "Probiotics",
    "probiotic (biohm)":        "Probiotics",
    "bifidobacterium":          "Probiotics",
    "lactobacillus":            "Probiotics",
    "lactobacillus acidophilus":"Probiotics",
    "lactobacillus rhamnosus":  "Probiotics",
    "lactobacillus reuteri":    "Probiotics",
    "probiotics (lactobacillus plantarum, lactobacillus casei)": "Probiotics",

    # ── Vitamin D ─────────────────────────────────────────────────────────────
    # Audit: 5 aliases, 30 combined signals (23 visible without this fix)
    "vitamin d":                "Vitamin D",
    "vitamin d3":               "Vitamin D",
    "vitamin d2":               "Vitamin D",
    "cholecalciferol":          "Vitamin D",
    "ergocalciferol":           "Vitamin D",
    "vitamin d3 (cholecalciferol)": "Vitamin D",
    "25-hydroxyvitamin d":      "Vitamin D",
    "calcifediol":              "Vitamin D",

    # ── Omega-3 ───────────────────────────────────────────────────────────────
    # Audit: 7 aliases, 29 combined signals (23 visible without this fix)
    "omega-3":                  "Omega-3",
    "omega 3":                  "Omega-3",
    "omega-3 fatty acids":      "Omega-3",
    "omega-3 (dha)":            "Omega-3",
    "fish oil":                 "Omega-3",
    "fish oil (omega-3)":       "Omega-3",
    "epa":                      "Omega-3",
    "dha":                      "Omega-3",
    "epa/dha":                  "Omega-3",
    "dha/epa":                  "Omega-3",
    "docosahexaenoic acid":     "Omega-3",
    "docosahexaenoic acid (dha)": "Omega-3",
    "eicosapentaenoic acid":    "Omega-3",
    "eicosapentaenoic acid (epa)": "Omega-3",
    "n-3 fatty acids":          "Omega-3",

    # ── CBD / Cannabidiol ─────────────────────────────────────────────────────
    # Audit: 5 aliases, 15 combined signals (6 visible without this fix)
    "cbd":                      "CBD",
    "cannabidiol":              "CBD",
    "cannabidiol (cbd)":        "CBD",
    "cbd (cannabidiol)":        "CBD",
    "cbd cannabidiol":          "CBD",
    "cannabidiol cbd":          "CBD",
    # "cannabis" intentionally excluded — THC/plant signals are not CBD supplement signals
    "hemp":                     "CBD",
    "cbg":                      "CBD",

    # ── Turmeric / Curcumin ───────────────────────────────────────────────────
    # Audit: 2 aliases, 15 combined signals (9 visible without this fix)
    "turmeric":                 "Turmeric / Curcumin",
    "curcumin":                 "Turmeric / Curcumin",
    "curcuminoids":             "Turmeric / Curcumin",
    "curcuma longa":            "Turmeric / Curcumin",
    "turmeric curcumin":        "Turmeric / Curcumin",
    "curcumin (turmeric)":      "Turmeric / Curcumin",

    # ── NMN ───────────────────────────────────────────────────────────────────
    # Audit: 4 aliases, 5 combined signals (2 visible without this fix)
    "nmn":                      "NMN",
    "nmn (nicotinamide mononucleotide)": "NMN",
    "nicotinamide mononucleotide": "NMN",
    "nicotinamide mononucleotide (nmn)": "NMN",
    "beta-nicotinamide mononucleotide": "NMN",
    "beta-nicotinamide mononucleotide (beta-nmn)": "NMN",   # after β→beta
    "b-nicotinamide mononucleotide": "NMN",

    # ── NAD+ ──────────────────────────────────────────────────────────────────
    # Audit: 3 aliases, 4 combined signals (2 visible without this fix)
    "nad+":                     "NAD+",
    "nad":                      "NAD+",
    "nad supplement":           "NAD+",
    "nad+ (nicotinamide riboside/nicotinamide mononucleotide)": "NAD+",
    "nad precursor":            "NAD+",
    "nad+ precursor":           "NAD+",

    # ── Creatine ──────────────────────────────────────────────────────────────
    # Audit: 2 aliases, 11 combined signals (10 visible without this fix)
    "creatine":                 "Creatine",
    "creatine monohydrate":     "Creatine",
    "creatine hcl":             "Creatine",
    "creatine ethyl ester":     "Creatine",

    # ── Iron ──────────────────────────────────────────────────────────────────
    # Audit: 3 aliases, 15 combined signals (13 visible without this fix)
    "iron":                     "Iron",
    "ferrous sulfate":          "Iron",
    "ferrous sulfate (oral iron)": "Iron",
    "ferrous fumarate":         "Iron",
    "ferric":                   "Iron",
    "heme iron":                "Iron",
    "iron (ferrous sulfate)":   "Iron",
    "ferrous bisglycinate":     "Iron",
}

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def canonical(raw: str) -> str:
    """
    Return the canonical ingredient name for a raw ingredient_name string.

    Parameters
    ----------
    raw : str
        The raw string as stored in the ingredient_name column, e.g.
        "Nicotinamide Mononucleotide (NMN)" or "probiotic (BIOHM)".

    Returns
    -------
    str
        Canonical display label (e.g. "NMN", "Probiotics") if the raw
        string maps to a known cluster, otherwise the original string
        stripped of leading/trailing whitespace.

    Notes
    -----
    - The original ingredient_name is never modified in the database.
    - Resolution is case-insensitive and β/α-aware.
    - A trailing parenthetical is stripped and the remainder re-checked
      as a fallback, so "vitamin D3 (Cholecalciferol)" → "Vitamin D"
      without needing an explicit map entry for every variant.
    """
    if not raw or not raw.strip():
        return raw

    # Normalise
    key = raw.strip().lower()
    key = key.replace("β", "beta").replace("α", "alpha")
    key = re.sub(r"\s+", " ", key)

    # Direct lookup
    if key in _ALIAS_MAP:
        return _ALIAS_MAP[key]

    # Strip trailing parenthetical and retry
    # e.g. "vitamin d3 (cholecalciferol)" → "vitamin d3"
    stripped = re.sub(r"\s*\([^)]*\)\s*$", "", key).strip()
    if stripped and stripped != key and stripped in _ALIAS_MAP:
        return _ALIAS_MAP[stripped]

    # No alias found — return the original string (whitespace-trimmed only)
    return raw.strip()
