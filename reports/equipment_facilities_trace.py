"""
Trace audit for Equipment & facilities focus click path.

Simulates the JS filter chain:
  1. unifiedFilteredCitations({ noiseFilter:true })  (isValidEnforcementItem + isLowValueContent)
  2. catFilter = 'Equipment & facilities'              (pCatFilter from setOverviewFocus)
  3. normalisePharmaCitationKey deduplication          (filteredCits())

For every displayed record reports WHY it appears and flags suspicious ones.

Outputs:
  reports/equipment_facilities_trace.json
  reports/equipment_facilities_trace.md

Usage:
  python3 reports/equipment_facilities_trace.py
"""

import json
import re
import sys
from collections import Counter
from pathlib import Path

DB_PATH  = Path(__file__).parent / "citation_database.json"
OUT_JSON = Path(__file__).parent / "equipment_facilities_trace.json"
OUT_MD   = Path(__file__).parent / "equipment_facilities_trace.md"

# ── Evidence terms for Equipment & facilities ─────────────────────────────────
EVIDENCE_TERMS = [
    "equipment", "facilit", "calibration", "maintenance", "hvac",
    "utilities", "water system", "premises", "sanitation", "qualification",
    "installation", "repair", "preventive",
]

# ── Noise / enforcement filter constants (mirrors core.js) ───────────────────
_NON_ENF = frozenset(["scientific_opinion", "guidance", "regulatory_update", "consultation"])
_ENF     = frozenset([
    "warning_letter", "inspection_finding", "recall", "import_alert",
    "safety_alert", "483", "compliance_action",
    "drug_enforcement", "device_enforcement", "food_enforcement",
])
_JUNK_EXACT  = frozenset(["about", "comics", "subscribe", "newsletter", "publications", "resources", "other"])
_JUNK_START  = ["subscribe to", "press release", "general information", "to the medium", "newsletter", "comics", "more about"]
_ENF_VERBS   = ["warning", "inspection", "recall", "violation", "alert", "detention",
                "contamination", "adulterat", "mislabel", "misbrand", "defect", "unsafe",
                "prohibited", "unapproved", "finding", "enforcement", "non-compliance",
                "gmp", "cgmp", "gdp", "import"]
_NOISE_PATS  = ["about", "more information", "subscribe", "report", "publication",
                "resource", "education", "conference", "opinion", "communication"]

# ── _TREND_FILTER_MAP entry for Equipment & facilities ────────────────────────
EQ_CATS = {"Equipment & facilities", "Equipment / Facilities"}
EQ_FMS  = {"equipment_facilities", "equipment_facility", "cleaning_validation", "calibration"}


def is_low_value(c):
    text = ((c.get("title") or "") + " " + (c.get("summary") or "")).lower()
    return any(p in text for p in _NOISE_PATS)


def is_valid_enforcement(c):
    st = (c.get("source_type") or "")
    if st in _NON_ENF:
        return False
    if st not in _ENF:
        return False
    summary = (c.get("summary") or "").strip()
    text = (summary + " " + (c.get("category") or "")).lower()
    if summary.lower() in _JUNK_EXACT:
        return False
    if any(text.startswith(p) for p in _JUNK_START):
        return False
    words = [w for w in summary.split() if w]
    if len(words) < 5 and not any(v in text for v in _ENF_VERBS):
        return False
    return True


def normalise_pharma_citation_key(c):
    """Mirrors normalisePharmaCitationKey in core.js."""
    raw_text = (c.get("summary") or "") + " " + (c.get("violation_details") or "")
    family = get_pharma_summary_family(raw_text)
    summary_part = family or (c.get("summary") or "").strip().lower()[:100]
    return "|".join([
        c.get("authority") or "",
        c.get("source_type") or "",
        c.get("category") or "",
        (c.get("company") or "").lower(),
        summary_part,
    ])


def get_pharma_summary_family(text):
    t = text or ""
    if re.search(r"family smoking prevention|tobacco control act", t, re.I):
        return "tobacco_compliance"
    if re.search(r"cgmp", t, re.I) and re.search(r"adulterat", t, re.I):
        return "cgmp_adulterated"
    if re.search(r"adulterated.*misbranded|misbranded.*adulterated", t, re.I):
        return "adulterated_misbranded"
    if re.search(r"adulterated", t, re.I) and not re.search(r"cgmp", t, re.I):
        return "adulterated_misbranded"
    if re.search(r"unapproved new drug", t, re.I):
        return "unapproved_new_drug"
    if re.search(r"foreign supplier verification|fsvp", t, re.I):
        return "fsvp"
    if re.search(r"import alert|detention without physical examination", t, re.I):
        return "import_alert_dwpe"
    return None


def has_evidence(c):
    ev = list(c.get("category_evidence") or []) + list(c.get("failure_mode_evidence") or [])
    text = " ".join(str(e) for e in ev).lower()
    return any(t in text for t in EVIDENCE_TERMS)


def match_reason(c):
    """Return human-readable explanation of why this record matches Equipment & facilities."""
    pgmc = (c.get("primary_gmp_category") or "")
    cat  = (c.get("category") or "")
    fm   = (c.get("failure_mode") or "")
    reasons = []

    if pgmc in EQ_CATS:
        reasons.append(f"primary_gmp_category='{pgmc}'")
    elif cat in EQ_CATS:
        reasons.append(f"legacy category='{cat}' (no primary_gmp_category match)")

    if fm in EQ_FMS:
        reasons.append(f"failure_mode='{fm}'")

    if not reasons:
        reasons.append("UNKNOWN — should not appear here")

    return "; ".join(reasons)


def url_quality(url):
    if not url:
        return "missing"
    u = url.lower()
    if u.startswith("https://api.fda.gov"):
        return "api_endpoint"
    if "accessdata.fda.gov/scripts/ires" in u:
        return "search_landing"
    return "direct_detail"


def classify_suspicion(c):
    """Return suspicion level and reason."""
    status = c.get("classification_status") or ""
    ev_ok  = has_evidence(c)
    pgmc   = c.get("primary_gmp_category") or ""
    cat    = c.get("category") or ""

    flags = []

    if status == "unconfirmed":
        flags.append("classification_status=unconfirmed")
    if not ev_ok:
        flags.append("no equipment/facility evidence terms in category_evidence or failure_mode_evidence")
    if not pgmc and cat in EQ_CATS:
        flags.append("matched on legacy category only (no primary_gmp_category)")
    if not (c.get("category_evidence") or []) and not (c.get("failure_mode_evidence") or []):
        flags.append("category_evidence and failure_mode_evidence both empty")

    if not flags:
        return "ok", []
    if "classification_status=unconfirmed" in flags:
        return "suspicious", flags
    if len(flags) >= 2:
        return "suspicious", flags
    return "weak", flags


def main():
    with open(DB_PATH) as f:
        raw = json.load(f)
    cits = raw["citations"]

    # ── Step 1: noise filter (mirrors unifiedFilteredCitations noiseFilter:true) ──
    step1 = [c for c in cits
             if not is_low_value(c) and is_valid_enforcement(c) and not c.get("is_noise")]

    # ── Step 2: catFilter = 'Equipment & facilities' ──────────────────────────
    # Mirrors: legacyCat !== catFilter && newCat !== catFilter → reject
    step2 = [c for c in step1
             if (c.get("category") or "Other") == "Equipment & facilities"
             or (c.get("primary_gmp_category") or "") == "Equipment & facilities"]

    # ── Step 3: deduplicate (normalisePharmaCitationKey) ─────────────────────
    seen_keys = set()
    step3 = []
    for c in step2:
        k = normalise_pharma_citation_key(c)
        if k not in seen_keys:
            seen_keys.add(k)
            step3.append(c)

    # ── Build trace records ───────────────────────────────────────────────────
    records = []
    for c in step3:
        suspicion, flags = classify_suspicion(c)
        records.append({
            "id":                    c.get("id") or c.get("citation_id") or "",
            "company":               c.get("company") or c.get("entity") or "",
            "authority":             c.get("authority") or "",
            "source_type":           c.get("source_type") or "",
            "url":                   c.get("url") or "",
            "url_quality":           url_quality(c.get("url") or ""),
            "summary":               (c.get("clean_title") or c.get("summary") or "")[:200],
            "raw_listing_summary":   (c.get("raw_listing_summary") or "")[:200],
            "primary_gmp_category":  c.get("primary_gmp_category") or "",
            "legacy_category":       c.get("category") or "",
            "category_confidence":   c.get("category_confidence") or 0.0,
            "category_evidence":     c.get("category_evidence") or [],
            "failure_mode":          c.get("failure_mode") or "",
            "failure_mode_confidence": c.get("failure_mode_confidence") or 0.0,
            "failure_mode_evidence": c.get("failure_mode_evidence") or [],
            "classification_status": c.get("classification_status") or "",
            "priority":              c.get("priority") or "",
            "cluster_id":            c.get("cluster_id") or "",
            "cluster_primary":       c.get("cluster_primary"),
            "cluster_size":          c.get("cluster_size") or 1,
            "match_reason":          match_reason(c),
            "has_evidence":          has_evidence(c),
            "suspicion":             suspicion,
            "suspicion_flags":       flags,
        })

    # ── Summary stats ─────────────────────────────────────────────────────────
    total           = len(records)
    confirmed       = sum(1 for r in records if r["classification_status"] == "confirmed")
    provisional     = sum(1 for r in records if r["classification_status"] == "provisional")
    unconfirmed     = sum(1 for r in records if r["classification_status"] == "unconfirmed")
    evidence_backed = sum(1 for r in records if r["has_evidence"])
    suspicious      = [r for r in records if r["suspicion"] == "suspicious"]
    weak            = [r for r in records if r["suspicion"] == "weak"]

    url_q = Counter(r["url_quality"] for r in records)
    auth_ct = Counter(r["authority"] for r in records)

    match_types = Counter()
    for r in records:
        mr = r["match_reason"]
        if "primary_gmp_category" in mr:
            match_types["primary_gmp_category"] += 1
        elif "legacy category" in mr:
            match_types["legacy_category_only"] += 1
        if "failure_mode" in mr:
            match_types["failure_mode"] += 1

    summary = {
        "db_total":                len(cits),
        "after_noise_filter":      len(step1),
        "after_eq_catfilter":      len(step2),
        "after_deduplication":     total,
        "confirmed":               confirmed,
        "provisional":             provisional,
        "unconfirmed":             unconfirmed,
        "evidence_backed":         evidence_backed,
        "suspicious_count":        len(suspicious),
        "weak_count":              len(weak),
        "match_types":             dict(match_types),
        "url_quality_counts":      dict(url_q),
        "authority_counts":        dict(auth_ct.most_common()),
    }

    output = {
        "generated": "2026-05-04",
        "focus_label": "Equipment & facilities",
        "filter_obj": {"primary_gmp_category": "Equipment & facilities"},
        "note": (
            "Counts are from citation_database.json. "
            "signals.html may have slightly different embedded data (live CITATIONS count differs). "
            "Filter logic mirrors JS unifiedFilteredCitations + filteredCits() deduplication."
        ),
        "summary": summary,
        "first_50_records": records[:50],
        "all_suspicious": suspicious,
        "all_weak": weak,
    }

    with open(OUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

    # ── Markdown report ───────────────────────────────────────────────────────
    lines = [
        "# Equipment & facilities — Trace Audit",
        "",
        f"Generated: 2026-05-04  |  Source: citation_database.json",
        "",
        "## Filter chain",
        "",
        "| Step | Description | Count |",
        "|------|-------------|-------|",
        f"| 0    | Full DB                                           | {len(cits):,} |",
        f"| 1    | After noise filter (isValidEnforcementItem)        | {len(step1):,} |",
        f"| 2    | After catFilter='Equipment & facilities'           | {len(step2):,} |",
        f"| 3    | After normalisePharmaCitationKey deduplication    | {total:,} |",
        "",
        "## Evidence & trust summary",
        "",
        "| Metric | Count |",
        "|--------|-------|",
        f"| confirmed               | {confirmed} |",
        f"| provisional             | {provisional} |",
        f"| unconfirmed             | {unconfirmed} |",
        f"| evidence-backed         | {evidence_backed} |",
        f"| suspicious              | {len(suspicious)} |",
        f"| weak (mild flags only)  | {len(weak)} |",
        "",
        "Evidence-backed = confirmed/provisional AND has ≥1 equipment/facility evidence term.",
        "",
        "## Match reason breakdown",
        "",
    ]
    for k, v in match_types.items():
        lines.append(f"- **{k}**: {v}")
    lines += [
        "",
        "## URL quality",
        "",
    ]
    for k, v in url_q.items():
        lines.append(f"- **{k}**: {v}")
    lines += [
        "",
        "## Authority breakdown",
        "",
    ]
    for k, v in auth_ct.most_common():
        lines.append(f"- **{k}**: {v}")

    if suspicious:
        lines += [
            "",
            "## Top suspicious records",
            "",
            "Records with no evidence terms OR unconfirmed classification.",
            "",
        ]
        for r in suspicious[:20]:
            lines += [
                f"### {r['authority']} | {r['source_type']} | {r['classification_status']}",
                f"- Company: {r['company']}",
                f"- Summary: {r['summary'][:150]}",
                f"- primary_gmp_category: {r['primary_gmp_category']}",
                f"- Match reason: {r['match_reason']}",
                f"- category_evidence: {r['category_evidence']}",
                f"- failure_mode_evidence: {r['failure_mode_evidence']}",
                f"- Flags: {'; '.join(r['suspicion_flags'])}",
                "",
            ]

    lines += [
        "## First 50 displayed records",
        "",
        "| # | Authority | Source type | Status | Evidence? | Match reason | Company |",
        "|---|-----------|-------------|--------|-----------|--------------|---------|",
    ]
    for i, r in enumerate(records[:50], 1):
        ev = "✓" if r["has_evidence"] else "✗"
        sus = f" ⚠ {r['suspicion']}" if r["suspicion"] != "ok" else ""
        lines.append(
            f"| {i} | {r['authority']} | {r['source_type']} | "
            f"{r['classification_status']}{sus} | {ev} | "
            f"{r['match_reason'][:60]} | {r['company'][:40]} |"
        )

    lines += [
        "",
        "---",
        "_Generated by reports/equipment_facilities_trace.py_",
    ]

    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print("=== Equipment & facilities Trace Audit ===")
    print(f"DB total:           {len(cits):,}")
    print(f"After noise filter: {len(step1):,}")
    print(f"After catFilter:    {len(step2):,}")
    print(f"After dedup:        {total:,}")
    print()
    print(f"confirmed:          {confirmed}")
    print(f"provisional:        {provisional}")
    print(f"unconfirmed:        {unconfirmed}")
    print(f"evidence-backed:    {evidence_backed}")
    print(f"suspicious:         {len(suspicious)}")
    print(f"weak:               {len(weak)}")
    print()
    print("URL quality:", dict(url_q))
    print("Authority:   ", dict(auth_ct.most_common()))
    print()
    print(f"Outputs: {OUT_JSON}")
    print(f"         {OUT_MD}")


if __name__ == "__main__":
    main()
