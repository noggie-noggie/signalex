"""
reports/audit_pharma_classification.py — Classification trust audit for pharma citations.

Reads reports/citation_database.json and produces:
  reports/classification_audit.json   — machine-readable full audit
  reports/classification_audit.md     — human-readable summary

Does NOT call Claude or modify any data. Read-only.

Usage:
    python reports/audit_pharma_classification.py
    python reports/audit_pharma_classification.py --top 30
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPORTS_DIR = Path(__file__).parent
DB_PATH     = REPORTS_DIR / "citation_database.json"
OUT_JSON    = REPORTS_DIR / "classification_audit.json"
OUT_MD      = REPORTS_DIR / "classification_audit.md"

# ---------------------------------------------------------------------------
# Evidence term sets — per category (Task 3 spec)
# ---------------------------------------------------------------------------
_CATEGORY_EVIDENCE: dict[str, list[str]] = {
    "Deviation management": [
        "deviation", "nonconformance", "non-conformance", "capa",
        "corrective action", "preventive action", "investigation",
        "failure investigation", "root cause", "oos",
        "out-of-specification", "out of specification",
        "complaint investigation", "discrepancy investigation",
        "discrepancy", "nonconformity", "non-conformity",
    ],
    "Quality management system": [
        "quality unit", "quality system", "quality management system", "qms",
        "management review", "quality oversight", "quality agreement",
        "change control system", "complaint system", "capa system",
        "quality assurance", "pharmaceutical quality system", "pqs",
        "quality manual", "quality policy",
    ],
    "Computerised systems validation": [
        "computer system validation", "csv", "computerized system",
        "computerised system", "electronic record", "electronic signature",
        "audit trail", "access control", "data integrity",
        "part 11", "annex 11", "21 cfr part 11", "software validation",
        "user access", "system backup",
    ],
    "Equipment & facilities": [
        "equipment", "facility", "facilities", "calibration", "maintenance",
        "cleaning validation", "hvac", "utilities", "water system",
        "water for injection", "purified water", "premises", "sanitation",
        "compressed air", "environmental control", "cleanroom",
        "preventive maintenance", "equipment qualification",
    ],
    "Documentation & record keeping": [
        "batch record", "master record", "sop", "standard operating procedure",
        "procedure", "documentation", "recordkeeping", "record keeping",
        "logbook", "certificate of analysis", "coa", "batch manufacturing record",
        "master batch record", "protocol", "data recording",
    ],
    "Labelling & claims": [
        "label", "labelling", "labeling", "misbranded", "misbranding",
        "claim", "disease claim", "false or misleading", "unapproved claim",
        "label mix-up", "incorrect label", "undeclared ingredient",
        "health claim", "therapeutic claim",
    ],
    "Sterility assurance": [
        "sterility", "sterile", "aseptic", "endotoxin", "bioburden",
        "sterility testing", "sterility failure", "terminal sterilization",
        "sterilisation", "depyrogenation", "sterility assurance",
    ],
    "Contamination & sterility": [
        "contamination", "contaminated", "microbial", "salmonella", "listeria",
        "e. coli", "mold", "yeast", "foreign material", "foreign matter",
        "particulate", "cross-contamination", "elemental impurity",
        "heavy metal", "nitrosamine", "ndma", "ndea",
    ],
    "GMP violations": [
        "cgmp", "good manufacturing practice", "gmp violation",
        "current good manufacturing", "21 cfr 211", "21 cfr 212",
        "eu gmp", "ich q7", "non-compliance with gmp",
    ],
    "Ingredient safety": [
        "undeclared", "adulterant", "adulterated", "identity", "purity",
        "potency", "nitrosamine", "impurity", "undeclared drug substance",
        "pharmaceutical ingredient", "active ingredient",
    ],
    "Training & competency": [
        "training", "competency", "qualified person", "qualification",
        "personnel", "gmp training", "training programme", "training records",
        "operator training",
    ],
    "Supply chain & procurement": [
        "supplier", "vendor", "procurement", "raw material supplier",
        "fsvp", "foreign supplier", "contract manufacturer",
        "supply chain", "incoming material", "supplier qualification",
        "vendor qualification",
    ],
}

# ---------------------------------------------------------------------------
# Source type guidance-detection patterns
# ---------------------------------------------------------------------------
_GUIDANCE_URL_RE = re.compile(
    r"/guidance/|/guidelines?/|/consultations?/|/scientific.opinions?/|"
    r"onlinelibrary\.wiley\.com|efsa\.europa\.eu|bfr\.bund\.de",
    re.I,
)
_GUIDANCE_TITLE_RE = re.compile(
    r"\bguideline\b|\bguidance\b|\bconsultation\b|\bopinion\b"
    r"|\bassessment\b|\bmonitoring report\b|\bscientific report\b"
    r"|\bscientific statement\b|\btechnical report\b|\bpeer review\b"
    r"|\bflavour\b|\bflavoring\b|\bflavouring\b",
    re.I,
)
_ENFORCEMENT_TITLE_RE = re.compile(
    r"\bnon.compliance\b|\bdeficiency\b|\bviolation\b|\benforcement\b"
    r"|\binspection finding\b|\bwarning letter\b|\brecall\b"
    r"|\bsuspension\b|\binjunction\b|\bdefective medicine\b",
    re.I,
)

# ---------------------------------------------------------------------------
# Suspicious-record detection rules
# ---------------------------------------------------------------------------

def _get_all_text(c: dict) -> str:
    """Combined text for evidence checking."""
    return " ".join(filter(None, [
        c.get("enriched_text", ""),
        c.get("violation_details", ""),
        c.get("raw_listing_summary", ""),
        c.get("summary", ""),
    ]))


def _get_listing_text(c: dict) -> str:
    """Listing-level text only (no enriched page text)."""
    return " ".join(filter(None, [
        c.get("violation_details", ""),
        c.get("raw_listing_summary", ""),
        c.get("summary", ""),
    ]))


def _find_evidence(c: dict, category: str) -> list[str]:
    """Return matched evidence phrases for a category from all available text."""
    terms = _CATEGORY_EVIDENCE.get(category, [])
    if not terms:
        return []
    text = _get_all_text(c).lower()
    return [t for t in terms if t in text]


def _has_enriched_evidence(c: dict, category: str) -> bool:
    """True if evidence found specifically in enriched_text (not just listing)."""
    terms = _CATEGORY_EVIDENCE.get(category, [])
    if not terms:
        return False
    et = (c.get("enriched_text") or "").lower()
    return bool(et) and any(t in et for t in terms)


def _is_guidance_record(c: dict) -> bool:
    """True when URL/title signals guidance/scientific-opinion content."""
    title_text = f"{c.get('summary', '')} {c.get('violation_details', '')}"
    return bool(_GUIDANCE_URL_RE.search(c.get("url", ""))
                or _GUIDANCE_TITLE_RE.search(title_text))


def _is_enforcement_record(c: dict) -> bool:
    title_text = f"{c.get('summary', '')} {c.get('violation_details', '')}"
    return bool(_ENFORCEMENT_TITLE_RE.search(title_text))


def check_suspicious(c: dict) -> list[str]:
    """
    Return a list of reason strings for why a record is suspicious.
    Empty list = clean record.
    """
    reasons: list[str] = []
    cat     = c.get("primary_gmp_category", "")
    fm      = c.get("failure_mode", "")
    st      = c.get("source_type", "")
    text    = _get_all_text(c).lower()
    conf    = c.get("classification_confidence", 0.0) or 0.0
    fm_conf = c.get("failure_mode_confidence", 0.0)   or 0.0

    # R1: Deviation management with no deviation evidence
    if cat == "Deviation management":
        ev = _find_evidence(c, "Deviation management")
        if not ev:
            reasons.append(
                "Deviation management category but no deviation/CAPA/OOS/investigation evidence found"
            )

    # R2: Quality management system record that looks like guidance, not enforcement
    if cat == "Quality management system":
        if _is_guidance_record(c) and not _is_enforcement_record(c):
            reasons.append(
                "Quality management system on a guidance/regulatory-update document (not enforcement)"
            )

    # R3: source_type inspection_finding but content is guidance/opinion/consultation
    if st == "inspection_finding":
        if _is_guidance_record(c) and not _is_enforcement_record(c):
            reasons.append(
                f"source_type=inspection_finding but URL/title indicates guidance or scientific opinion "
                f"(authority={c.get('authority','')})"
            )

    # R4: Specific category assigned but no evidence phrase present
    has_evidence_terms = bool(_CATEGORY_EVIDENCE.get(cat))
    if has_evidence_terms and cat not in ("Other / Insufficient Detail",):
        ev = _find_evidence(c, cat)
        if not ev and conf < 0.5:
            reasons.append(
                f"Category '{cat}' assigned but no evidence phrases found and "
                f"classification_confidence={conf:.2f}"
            )

    # R5: EFSA/BfR records classified as inspection_finding
    if st == "inspection_finding" and c.get("authority") in ("EFSA", "BfR"):
        reasons.append(
            f"{c.get('authority')} record classified as inspection_finding — "
            "should be scientific_opinion or regulatory_update"
        )

    # R6: failure_mode is specific but confidence is very low
    if (fm and fm not in ("insufficient_detail", "")
            and fm_conf < 0.2
            and cat not in ("Other / Insufficient Detail",)):
        reasons.append(
            f"failure_mode='{fm}' (conf={fm_conf:.2f}) is very low confidence for a specific mode"
        )

    # R7: MHRA gov.uk/guidance URLs as inspection_finding
    if st == "inspection_finding" and "gov.uk/guidance" in (c.get("url") or ""):
        reasons.append(
            "MHRA gov.uk/guidance URL classified as inspection_finding — should be guidance"
        )

    return reasons


# ---------------------------------------------------------------------------
# Per-category summary builder
# ---------------------------------------------------------------------------

def _category_summary(citations: list[dict], category: str, top_n: int = 20) -> dict:
    subset = [c for c in citations if c.get("primary_gmp_category") == category]
    if not subset:
        return {"total": 0}

    with_enriched = [c for c in subset if c.get("enriched_text")]
    with_evidence = [c for c in subset if _find_evidence(c, category)]
    confirmed_ev  = [c for c in subset if _has_enriched_evidence(c, category)]
    no_evidence   = [c for c in subset if not _find_evidence(c, category)]
    low_conf      = [c for c in subset
                     if (c.get("classification_confidence") or 0.0) < 0.5
                     and (c.get("failure_mode_confidence") or 0.0) < 0.5]

    # Suspicious examples
    suspicious = []
    for c in subset:
        reasons = check_suspicious(c)
        if reasons:
            suspicious.append({
                "id":                   c.get("id"),
                "company":              c.get("company", ""),
                "authority":            c.get("authority", ""),
                "source_type":          c.get("source_type", ""),
                "url":                  c.get("url", ""),
                "raw_listing_summary":  (c.get("raw_listing_summary") or "")[:200],
                "summary":              (c.get("summary") or "")[:200],
                "violation_details":    (c.get("violation_details") or "")[:200],
                "enriched_text_excerpt":(c.get("enriched_text") or "")[:200],
                "primary_gmp_category": c.get("primary_gmp_category", ""),
                "failure_mode":         c.get("failure_mode", ""),
                "category_confidence":  c.get("classification_confidence", 0.0),
                "failure_mode_confidence": c.get("failure_mode_confidence", 0.0),
                "reasons_flagged":      reasons,
            })

    return {
        "total":              len(subset),
        "with_enriched_text": len(with_enriched),
        "with_any_evidence":  len(with_evidence),
        "with_enriched_evidence": len(confirmed_ev),
        "no_evidence":        len(no_evidence),
        "low_confidence":     len(low_conf),
        "suspicious_count":   len(suspicious),
        "top_suspicious":     suspicious[:top_n],
    }


# ---------------------------------------------------------------------------
# Source-type audit
# ---------------------------------------------------------------------------

def _sourcetype_audit(citations: list[dict]) -> dict:
    insp = [c for c in citations if c.get("source_type") == "inspection_finding"]
    guidance_mislabelled = [
        c for c in insp
        if _is_guidance_record(c) and not _is_enforcement_record(c)
    ]
    efsa_mislabelled = [c for c in insp if c.get("authority") == "EFSA"]
    bfr_mislabelled  = [c for c in insp if c.get("authority") == "BfR"]
    mhra_guidance    = [
        c for c in insp
        if c.get("authority") == "MHRA" and "gov.uk/guidance" in (c.get("url") or "")
    ]

    by_auth = Counter(c.get("authority") for c in insp)
    return {
        "total_inspection_finding": len(insp),
        "by_authority":             dict(by_auth),
        "guidance_mislabelled_count": len(guidance_mislabelled),
        "efsa_mislabelled_count":   len(efsa_mislabelled),
        "bfr_mislabelled_count":    len(bfr_mislabelled),
        "mhra_guidance_count":      len(mhra_guidance),
        "guidance_mislabelled_examples": [
            {
                "id":        c.get("id"),
                "authority": c.get("authority"),
                "url":       c.get("url", "")[:120],
                "summary":   (c.get("summary") or "")[:120],
                "primary_gmp_category": c.get("primary_gmp_category"),
                "priority":  c.get("priority"),
            }
            for c in guidance_mislabelled[:25]
        ],
    }


# ---------------------------------------------------------------------------
# Deviation management deep audit
# ---------------------------------------------------------------------------

def _deviation_audit(citations: list[dict]) -> dict:
    devs = [c for c in citations if c.get("primary_gmp_category") == "Deviation management"]
    confirmed = [c for c in devs if _find_evidence(c, "Deviation management")]
    suspicious = [c for c in devs if not _find_evidence(c, "Deviation management")]

    return {
        "total":           len(devs),
        "confirmed_evidence_count": len(confirmed),
        "suspicious_count": len(suspicious),
        "suspicious_examples": [
            {
                "id":             c.get("id"),
                "authority":      c.get("authority"),
                "source_type":    c.get("source_type"),
                "company":        c.get("company", ""),
                "priority":       c.get("priority"),
                "summary":        (c.get("summary") or "")[:150],
                "violation_details": (c.get("violation_details") or "")[:150],
                "enriched_text_excerpt": (c.get("enriched_text") or "")[:200],
                "failure_mode":   c.get("failure_mode"),
                "fm_confidence":  c.get("failure_mode_confidence", 0.0),
            }
            for c in suspicious[:10]
        ],
    }


# ---------------------------------------------------------------------------
# QMS audit — guidance vs real enforcement
# ---------------------------------------------------------------------------

def _qms_audit(citations: list[dict]) -> dict:
    qms = [c for c in citations if c.get("primary_gmp_category") == "Quality management system"]
    guidance_records = [c for c in qms if _is_guidance_record(c) and not _is_enforcement_record(c)]
    enforcement      = [c for c in qms if _is_enforcement_record(c)]
    ambiguous        = [c for c in qms if c not in guidance_records and c not in enforcement]
    suspicious       = [c for c in qms if check_suspicious(c)]

    return {
        "total":               len(qms),
        "likely_guidance":     len(guidance_records),
        "likely_enforcement":  len(enforcement),
        "ambiguous":           len(ambiguous),
        "suspicious_count":    len(suspicious),
        "guidance_examples":   [
            {
                "id":        c.get("id"),
                "authority": c.get("authority"),
                "source_type": c.get("source_type"),
                "priority":  c.get("priority"),
                "summary":   (c.get("summary") or "")[:150],
                "url":       (c.get("url") or "")[:100],
            }
            for c in guidance_records[:10]
        ],
    }


# ---------------------------------------------------------------------------
# P1/P2 trust check — should these be high priority?
# ---------------------------------------------------------------------------

def _p1p2_trust_audit(citations: list[dict]) -> dict:
    high = [c for c in citations if c.get("priority") in ("P1", "P2")]
    unconfirmed_high = [
        c for c in high
        if (c.get("classification_confidence") or 0.0) < 0.5
        and not (c.get("enriched_text") and len(c.get("enriched_text", "")) > 100)
    ]
    guidance_high = [
        c for c in high
        if c.get("source_type") == "inspection_finding"
        and _is_guidance_record(c) and not _is_enforcement_record(c)
    ]

    return {
        "total_p1":            sum(1 for c in citations if c.get("priority") == "P1"),
        "total_p2":            sum(1 for c in citations if c.get("priority") == "P2"),
        "p1p2_unconfirmed_classification": len(unconfirmed_high),
        "p1p2_guidance_mislabelled":       len(guidance_high),
        "sample_unconfirmed_p1p2": [
            {
                "id":               c.get("id"),
                "authority":        c.get("authority"),
                "source_type":      c.get("source_type"),
                "priority":         c.get("priority"),
                "primary_gmp_category": c.get("primary_gmp_category"),
                "failure_mode":     c.get("failure_mode"),
                "summary":          (c.get("summary") or "")[:150],
                "classification_confidence": c.get("classification_confidence", 0.0),
                "has_enriched_text": bool(c.get("enriched_text")),
            }
            for c in unconfirmed_high[:20]
        ],
    }


# ---------------------------------------------------------------------------
# Classification status distribution (new fields if present, otherwise inferred)
# ---------------------------------------------------------------------------

def _infer_classification_status(c: dict) -> str:
    """Infer classification_status for records that predate the evidence fields."""
    # If the field is already set, use it
    if c.get("classification_status"):
        return c["classification_status"]

    cat  = c.get("primary_gmp_category", "")
    ev   = _find_evidence(c, cat)
    conf = c.get("classification_confidence") or 0.0
    et   = c.get("enriched_text") or ""

    if not ev and cat not in ("Other / Insufficient Detail", ""):
        return "unconfirmed"
    if ev and (len(et) > 200 or conf >= 0.5):
        return "confirmed"
    return "provisional"


# ---------------------------------------------------------------------------
# Main audit function
# ---------------------------------------------------------------------------

def build_audit(citations: list[dict], top_n: int = 20) -> dict:
    total = len(citations)
    cats  = Counter(c.get("primary_gmp_category", "") for c in citations)
    fms   = Counter(c.get("failure_mode", "")         for c in citations)
    pris  = Counter(c.get("priority", "")             for c in citations)
    stypes = Counter(c.get("source_type", "")         for c in citations)
    auths  = Counter(c.get("authority", "")           for c in citations)

    # Classification status distribution
    statuses = Counter(_infer_classification_status(c) for c in citations)

    # Overall suspicious count
    total_suspicious = sum(1 for c in citations if check_suspicious(c))
    total_guidance_mislabelled = sum(
        1 for c in citations
        if c.get("source_type") == "inspection_finding"
        and _is_guidance_record(c) and not _is_enforcement_record(c)
    )

    # Per-category summaries for key categories
    key_cats = [
        "Deviation management",
        "Quality management system",
        "Computerised systems validation",
        "Equipment & facilities",
        "Documentation & record keeping",
        "Labelling & claims",
        "Sterility assurance",
        "Contamination & sterility",
        "GMP violations",
        "Ingredient safety",
        "Training & competency",
        "Supply chain & procurement",
    ]
    per_category = {cat: _category_summary(citations, cat, top_n) for cat in key_cats}

    return {
        "generated_at":  datetime.now(timezone.utc).isoformat(),
        "total_records": total,
        "overview": {
            "total_suspicious_records":        total_suspicious,
            "guidance_mislabelled_count":       total_guidance_mislabelled,
            "classification_status_distribution": dict(statuses),
            "source_type_distribution":         dict(stypes),
            "authority_distribution":           dict(auths),
            "priority_distribution":            dict(pris),
        },
        "source_type_audit":     _sourcetype_audit(citations),
        "deviation_management":  _deviation_audit(citations),
        "quality_management_system": _qms_audit(citations),
        "p1p2_trust_audit":      _p1p2_trust_audit(citations),
        "per_category_summaries": per_category,
        "top_categories":        dict(cats.most_common(20)),
        "top_failure_modes":     dict(fms.most_common(15)),
    }


# ---------------------------------------------------------------------------
# Markdown report renderer
# ---------------------------------------------------------------------------

def render_markdown(audit: dict) -> str:
    lines: list[str] = []
    a = lines.append

    a("# Pharma Classification Trust Audit")
    a(f"\n**Generated:** {audit['generated_at']}")
    a(f"**Total records:** {audit['total_records']}")

    ov = audit["overview"]
    a("\n## Overview")
    a(f"- **Suspicious records:** {ov['total_suspicious_records']}")
    a(f"- **Guidance mislabelled as inspection_finding:** {ov['guidance_mislabelled_count']}")
    a("")
    a("### Classification Status (inferred)")
    for st, cnt in sorted(ov["classification_status_distribution"].items(), key=lambda x: -x[1]):
        a(f"- {st}: **{cnt}**")
    a("")
    a("### Priority Distribution")
    for p in ("P1", "P2", "P3", "P4"):
        a(f"- {p}: {ov['priority_distribution'].get(p, 0)}")
    a("")
    a("### Source Type Distribution")
    for st, cnt in sorted(ov["source_type_distribution"].items(), key=lambda x: -x[1]):
        a(f"- {st}: {cnt}")

    # Source type audit
    st_audit = audit["source_type_audit"]
    a("\n## Source Type Audit")
    a(f"**Total inspection_finding records:** {st_audit['total_inspection_finding']}")
    a(f"**By authority:** {st_audit['by_authority']}")
    a(f"**Guidance mislabelled as inspection_finding:** {st_audit['guidance_mislabelled_count']}")
    a(f"  - EFSA: {st_audit['efsa_mislabelled_count']}")
    a(f"  - BfR: {st_audit['bfr_mislabelled_count']}")
    a(f"  - MHRA gov.uk/guidance: {st_audit['mhra_guidance_count']}")
    if st_audit["guidance_mislabelled_examples"]:
        a("\n### Top mislabelled examples (guidance as inspection_finding)")
        for ex in st_audit["guidance_mislabelled_examples"][:10]:
            a(f"- `{ex['id']}` [{ex['authority']}] {ex['summary'][:80]}")
            a(f"  URL: {ex['url'][:100]}")

    # Deviation management
    dm = audit["deviation_management"]
    a("\n## Deviation Management")
    a(f"- Total: {dm['total']}")
    a(f"- With evidence: {dm['confirmed_evidence_count']}")
    a(f"- **Suspicious (no evidence): {dm['suspicious_count']}**")
    if dm["suspicious_examples"]:
        a("\n### Top suspicious Deviation management records")
        for ex in dm["suspicious_examples"][:10]:
            a(f"- `{ex['id']}` [{ex['authority']} / {ex['source_type']}] {ex['company']}")
            a(f"  Summary: {ex['summary'][:100]}")
            a(f"  violation_details: {ex['violation_details'][:100]}")
            if ex.get("enriched_text_excerpt"):
                a(f"  enriched_text: {ex['enriched_text_excerpt'][:100]}")

    # QMS
    qms = audit["quality_management_system"]
    a("\n## Quality Management System")
    a(f"- Total: {qms['total']}")
    a(f"- Likely guidance documents: {qms['likely_guidance']}")
    a(f"- Likely enforcement: {qms['likely_enforcement']}")
    a(f"- Ambiguous: {qms['ambiguous']}")
    a(f"- **Suspicious: {qms['suspicious_count']}**")
    if qms["guidance_examples"]:
        a("\n### Guidance examples miscategorised under QMS")
        for ex in qms["guidance_examples"][:10]:
            a(f"- `{ex['id']}` [{ex['authority']} / {ex['source_type']}] (P{ex['priority']}) {ex['summary'][:100]}")

    # P1/P2 trust
    p12 = audit["p1p2_trust_audit"]
    a("\n## P1/P2 Trust Audit")
    a(f"- P1 total: {p12['total_p1']}")
    a(f"- P2 total: {p12['total_p2']}")
    a(f"- **P1/P2 with unconfirmed classification:** {p12['p1p2_unconfirmed_classification']}")
    a(f"- **P1/P2 guidance mislabelled as enforcement:** {p12['p1p2_guidance_mislabelled']}")
    if p12["sample_unconfirmed_p1p2"]:
        a("\n### Sample unconfirmed P1/P2 records")
        a("| id | auth | source_type | priority | category | failure_mode | conf | enriched |")
        a("|---|---|---|---|---|---|---|---|")
        for ex in p12["sample_unconfirmed_p1p2"][:20]:
            enr = "Y" if ex["has_enriched_text"] else "N"
            conf = f"{ex['classification_confidence']:.2f}"
            a(f"| `{ex['id']}` | {ex['authority']} | {ex['source_type']} | {ex['priority']} "
              f"| {ex['primary_gmp_category'][:30]} | {ex['failure_mode'][:20]} | {conf} | {enr} |")

    # Per-category summary table
    a("\n## Per-Category Summary")
    a("| Category | Total | Enriched | Any evidence | Confirmed (enriched) | No evidence | Suspicious |")
    a("|---|---|---|---|---|---|---|")
    for cat, summ in audit["per_category_summaries"].items():
        if summ.get("total", 0) == 0:
            continue
        a(f"| {cat} | {summ['total']} | {summ['with_enriched_text']} "
          f"| {summ['with_any_evidence']} | {summ['with_enriched_evidence']} "
          f"| {summ['no_evidence']} | {summ['suspicious_count']} |")

    # Per-category suspicious detail
    a("\n## Category Suspicious Record Detail")
    for cat, summ in audit["per_category_summaries"].items():
        if not summ.get("top_suspicious"):
            continue
        a(f"\n### {cat} — {summ['suspicious_count']} suspicious of {summ['total']}")
        for ex in summ["top_suspicious"][:10]:
            a(f"\n**`{ex['id']}`** [{ex['authority']} / {ex['source_type']}] {ex['company']}")
            for r in ex["reasons_flagged"]:
                a(f"  - ⚠ {r}")
            if ex.get("summary"):
                a(f"  Summary: {ex['summary'][:120]}")
            if ex.get("enriched_text_excerpt"):
                a(f"  Enriched: {ex['enriched_text_excerpt'][:120]}")

    a("\n---")
    a("*Generated by audit_pharma_classification.py — read-only, no data modified.*")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(top_n: int = 20) -> None:
    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found. Run citation_fetcher.py first.", file=sys.stderr)
        sys.exit(1)

    print(f"Loading {DB_PATH} …")
    raw = json.loads(DB_PATH.read_text(encoding="utf-8"))
    citations: list[dict] = raw.get("citations", [])
    if not citations:
        print("ERROR: no citations in database.", file=sys.stderr)
        sys.exit(1)
    print(f"  {len(citations)} records loaded")

    print("Running audit …")
    audit = build_audit(citations, top_n=top_n)

    # Write JSON
    OUT_JSON.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    print(f"  JSON: {OUT_JSON}")

    # Write Markdown
    md = render_markdown(audit)
    OUT_MD.write_text(md, encoding="utf-8")
    print(f"  Markdown: {OUT_MD}")

    # Print summary to stdout
    ov = audit["overview"]
    dm = audit["deviation_management"]
    st = audit["source_type_audit"]
    qms = audit["quality_management_system"]
    p12 = audit["p1p2_trust_audit"]

    print("\n" + "=" * 60)
    print("  CLASSIFICATION TRUST AUDIT SUMMARY")
    print("=" * 60)
    print(f"  Total records:                {audit['total_records']}")
    print(f"  Suspicious records:           {ov['total_suspicious_records']}")
    print(f"  Guidance as inspection_find:  {ov['guidance_mislabelled_count']}")
    print()
    print("  Classification status (inferred):")
    for st_k, cnt in sorted(ov["classification_status_distribution"].items(), key=lambda x: -x[1]):
        print(f"    {st_k:<20}  {cnt:>5}")
    print()
    print(f"  Deviation management:         {dm['total']} total")
    print(f"    confirmed evidence:         {dm['confirmed_evidence_count']}")
    print(f"    suspicious (no evidence):   {dm['suspicious_count']}")
    print()
    print(f"  QMS:                          {qms['total']} total")
    print(f"    likely guidance:            {qms['likely_guidance']}")
    print(f"    likely enforcement:         {qms['likely_enforcement']}")
    print()
    print(f"  inspection_finding records:   {st['total_inspection_finding']}")
    print(f"    EFSA mislabelled:           {st['efsa_mislabelled_count']}")
    print(f"    BfR mislabelled:            {st['bfr_mislabelled_count']}")
    print(f"    MHRA guidance mislabelled:  {st['mhra_guidance_count']}")
    print()
    print(f"  P1 total:                     {p12['total_p1']}")
    print(f"  P2 total:                     {p12['total_p2']}")
    print(f"  P1/P2 unconfirmed class.:     {p12['p1p2_unconfirmed_classification']}")
    print(f"  P1/P2 guidance mislabelled:   {p12['p1p2_guidance_mislabelled']}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pharma classification trust audit")
    parser.add_argument("--top", type=int, default=20, help="Max suspicious examples per category")
    args = parser.parse_args()
    main(top_n=args.top)
