"""
reports/citation_fetcher.py — Citation Database Builder

Fetches regulatory citations from FDA, TGA, MHRA, EFSA, and BfR over the
last 12 months across all pharmaceutical manufacturing categories, classifies
each using Claude, and writes reports/citation_database.json.

Sources:
  FDA
    - Warning Letters (all pharma categories, supplement filter removed)
    - Drug Enforcement API (OpenFDA) — 700+ prescription/OTC recalls
    - Food/Supplement Enforcement API (OpenFDA)
    - Device Enforcement API (OpenFDA) — Class II/III recalls
    - Import Alerts (ialist.html) — pharma/biologics/supplement prefixes
  TGA
    - Market Actions (recalls, withdrawals)
    - Compliance & Enforcement actions
    - Safety Alerts
    - Safety Updates
  MHRA  Drug Safety Update + GMP publications (gov.uk)
  EFSA  Scientific publications — supplement/contaminant/food safety opinions
  BfR   German Federal Institute press releases and risk opinions

Usage:
    python -m reports.citation_fetcher
    python reports/citation_fetcher.py
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
import argparse
import concurrent.futures
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Ensure project root is on sys.path so 'reports' package is importable
# whether this file is run directly (python reports/citation_fetcher.py)
# or imported as a module.
# ---------------------------------------------------------------------------
import sys as _sys
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("citation_fetcher")

# ---------------------------------------------------------------------------
# Paths & time window
# ---------------------------------------------------------------------------
REPORTS_DIR = Path(__file__).parent
OUTPUT_JSON = REPORTS_DIR / "citation_database.json"

NOW        = datetime.now(timezone.utc)
CUTOFF_12M = NOW - timedelta(days=365)
CUTOFF_90D = NOW - timedelta(days=90)

ENRICH_CACHE_PATH = REPORTS_DIR / "enrichment_cache.json"
ENRICH_CACHE_TTL_DAYS = 30

# ---------------------------------------------------------------------------
# Classification categories (expanded from VMS-only to full pharma scope)
# ---------------------------------------------------------------------------
CATEGORIES = [
    # Core GMP / quality
    "GMP violations",
    "Documentation & record keeping",
    "Training & competency",
    "Quality management system",
    "Deviation management",
    "Change control",
    "Batch release",
    "Stability programme",
    # Manufacturing-specific
    "Sterility assurance",
    "Aseptic processing",
    "Environmental monitoring",
    "Container closure integrity",
    "Parenteral manufacturing",
    "API synthesis and control",
    "Biological product specific",
    "Computerised systems validation",
    # Supply / procurement
    "Supply chain & procurement",
    "Cold chain and storage",
    # Regulatory / product
    "Labelling & claims",
    "Contamination & sterility",
    "Equipment & facilities",
    "Ingredient safety",
    # Catch-all
    "Other",
]

# ---------------------------------------------------------------------------
# Facility types
# ---------------------------------------------------------------------------
FACILITY_TYPES = [
    "Supplement / Nutraceutical",
    "Sterile / Parenteral",
    "Biologics / Vaccine",
    "API Manufacturer",
    "CMO",
    "Medical Device",
    "Compounding",
    "General Pharma",
]

_FACILITY_RULES: list[tuple[str, list[str]]] = [
    ("Supplement / Nutraceutical", [
        "supplement", "dietary supplement", "nutraceutical", "vitamin", "mineral",
        "herbal", "botanical", "probiotic", "food supplement", "complementary medicine",
        "sports nutrition", "omega-3", "fish oil", "protein powder",
    ]),
    ("Sterile / Parenteral", [
        "sterile", "aseptic", "parenteral", "injectable", "lyophilized", "lyophilisation",
        "fill-finish", "fill finish", "vial", "ampoule", "ampule", "autoclave",
        "endotoxin", "depyrogenation", "cleanroom", "iso-5", "isolator",
    ]),
    ("Biologics / Vaccine", [
        "biologic", "biologics", "vaccine", "viral", "cell culture", "fermentation",
        "purification", "monoclonal antibody", "plasma", "blood product", "gene therapy",
        "recombinant", "biosimilar", "mab", "immunoglobulin", "antigen", "cber",
    ]),
    ("API Manufacturer", [
        "active pharmaceutical ingredient", "api", "active substance", "drug substance",
        "synthesis", "chemical synthesis", "starting material", "intermediate",
    ]),
    ("CMO", [
        "contract manufacturer", "contract manufacturing", "cmo", "cdmo",
        "contract development", "contract organization", "third-party manufacturer",
    ]),
    ("Medical Device", [
        "medical device", "in vitro diagnostic", "ivd", "implant", "catheter",
        "stent", "surgical instrument", "class ii device", "class iii device",
        "510(k)", "pma", "510k",
    ]),
    ("Compounding", [
        "compounding", "compounded", "compound pharmacy", "503b", "503a",
        "hospital compounding", "outsourcing facility",
    ]),
]

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class Citation:
    id:               str
    authority:        str   # FDA | TGA | MHRA | EFSA | BfR
    source_type:      str   # warning_letter | drug_enforcement | device_enforcement |
                            # food_enforcement | import_alert | compliance_action |
                            # inspection_finding | recall | safety_alert
    company:          str
    date:             str   # "YYYY-MM-DD" or ""
    category:         str   # one of CATEGORIES (kept for backwards compat)
    severity:         str   # high | medium | low (kept for backwards compat)
    summary:          str
    url:              str
    product_type:     str   # human-readable product descriptor
    country:          str
    facility_type:    str   # one of FACILITY_TYPES
    violation_details: str  # raw text, ≤500 chars (kept for backwards compat)
    # ── Enrichment fields ─────────────────────────────────────────────
    raw_listing_summary:   str   = ""    # original violation_details preserved here
    enriched_text:         str   = ""    # body text from detail page (up to 2000 chars)
    enrichment_status:     str   = ""    # success | cached | not_applicable | failed
    enrichment_source:     str   = ""    # fda_detail_page | tga_notice | none
    enrichment_confidence: float = 0.0
    enrichment_error:      str   = ""
    enriched_text_hash:    str   = ""    # sha256[:16] of enriched_text[:100]
    # ── Multi-label classification ────────────────────────────────────
    primary_gmp_category:      str  = ""
    secondary_gmp_categories:  list = field(default_factory=list)
    failure_mode:              str  = ""
    failure_mode_confidence:   float = 0.0
    # ── Multi-dimensional severity ────────────────────────────────────
    regulatory_severity: str   = ""   # critical | high | medium | low
    operational_severity: str  = ""   # critical | high | medium | low
    inspection_risk:     str   = ""   # immediate | elevated | standard | informational
    market_relevance_au: str   = ""   # direct | indirect | reference
    priority:            str   = ""   # P1 | P2 | P3 | P4
    severity_reason:     str   = ""
    # ── Risk direction (deterministic, no AI) ────────────────────────
    regulatory_pressure:               str = ""  # increasing | stable | decreasing
    signal_direction:                  str = ""  # escalating | holding | resolving
    recurrence_count_company_90d:      int = 0
    recurrence_count_category_90d:     int = 0
    recurrence_count_failure_mode_90d: int = 0
    # ── AI intelligence fields (set by pharma_intelligence.py) ───────
    ai_summary:            str   = ""
    ai_what_matters:       str   = ""
    ai_recommended_action: str   = ""
    ai_confidence:         float = 0.0
    ai_run_at:             str   = ""
    # ── Derived intelligence fields ───────────────────────────────────
    decision_summary:          str   = ""   # human-readable combined verdict
    recommended_action:        str   = ""   # top compliance action for AU VMS teams
    classification_confidence: float = 0.0  # overall confidence in classification
    is_noise:                  bool  = False  # True if record has insufficient detail
    # ── Evidence-backed classification fields (set by classify_with_evidence()) ──
    category_confidence:      float = 0.0   # keyword-evidence confidence for primary_gmp_category
    category_evidence:        list  = field(default_factory=list)   # matched phrases → category
    failure_mode_evidence:    list  = field(default_factory=list)   # matched phrases → failure_mode
    classification_basis:     str   = "unknown"   # enriched_text | raw_listing_summary | legacy
    classification_status:    str   = "unconfirmed"  # confirmed | provisional | unconfirmed
    # ── Clustering fields (set by compute_clusters(), Step 7) ────────
    cluster_id:       str  = ""   # sha256[:12] of grouping key
    cluster_size:     int  = 1    # number of records in cluster (1 = singleton)
    cluster_primary:  bool = True # True for the representative record
    cluster_label:    str  = ""   # display label, e.g. "Medline Industries — sterility_failure"
    cluster_reason:   str  = ""   # short explanation, e.g. "5 records, 2025-03–2025-04"
    cluster_priority: str  = ""   # highest priority among cluster members

    def __post_init__(self) -> None:
        # Guarantee raw_listing_summary is always populated at construction.
        # Priority: explicit value > summary > violation_details.
        if not self.raw_listing_summary:
            self.raw_listing_summary = (
                self.summary or self.violation_details or ""
            )[:500]


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
_SESSION: Optional[requests.Session] = None


def get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-AU,en-US;q=0.9,en;q=0.8",
        })
    return _SESSION


def http_get(url: str, timeout: int = 30, **kwargs) -> requests.Response:
    for attempt in range(3):
        try:
            resp = get_session().get(url, timeout=timeout, **kwargs)
            if resp.status_code < 500:
                return resp
            logger.warning("HTTP %s for %s (attempt %d)", resp.status_code, url, attempt + 1)
        except requests.RequestException as exc:
            logger.warning("Request failed for %s: %s (attempt %d)", url, exc, attempt + 1)
        if attempt < 2:
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to fetch {url} after 3 attempts")


def make_id(authority: str, key: str) -> str:
    return hashlib.md5(f"{authority}::{key}".encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------
def parse_date(text: str, prefer_mdy: bool = False) -> Optional[datetime]:
    """Parse a date string into a timezone-aware datetime.

    Args:
        text: Raw date string from a regulatory page.
        prefer_mdy: Set True for sources that use MM/DD/YYYY (e.g. FDA).
            When False (default) DD/MM/YYYY is tried first for ambiguous
            slash-separated dates; when True MM/DD/YYYY takes priority.
            ISO and alphabetical-month formats are never ambiguous, so
            prefer_mdy has no effect on them.
    """
    if not text:
        return None
    text = text.strip()
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    slash_fmts = ("%m/%d/%Y", "%d/%m/%Y") if prefer_mdy else ("%d/%m/%Y", "%m/%d/%Y")
    for fmt in (
        "%Y%m%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d",
        "%d %B %Y", "%d %b %Y", "%B %d, %Y", "%b %d, %Y",
        *slash_fmts, "%d-%m-%Y",
    ):
        try:
            dt = datetime.strptime(text[:30], fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def date_str(dt: Optional[datetime]) -> str:
    return dt.strftime("%Y-%m-%d") if dt else ""


# ---------------------------------------------------------------------------
# Severity inference
# ---------------------------------------------------------------------------
_HIGH_RE = re.compile(
    r"death|fatal|hospitali|life.threatening|serious adverse|serious risk|"
    r"class.?i\b(?!.*class.?ii)|salmonella|listeria|e\. coli|clostridium|"
    r"undeclared drug|sildenafil|tadalafil|sibutramine|sarms|dmaa|anabolic|"
    r"urgent|immediate cessation|injunction|seizure|counterfeit|falsified|"
    r"sterility failure|contaminated batch|endotoxin exceedance|"
    r"nitrosamine|carcinogen|genotoxic",
    re.I,
)
_LOW_RE = re.compile(
    r"guidance document|information update|scientific opinion|"
    r"surveillance report|monitoring report|advisory notice",
    re.I,
)


def infer_severity(text: str, source_type: str) -> str:
    if _HIGH_RE.search(text):
        return "high"
    if source_type in ("warning_letter", "483_observation", "inspection_finding",
                       "drug_enforcement", "device_enforcement", "import_alert"):
        return "medium"
    if _LOW_RE.search(text):
        return "low"
    return "medium"


# ---------------------------------------------------------------------------
# Facility type inference
# ---------------------------------------------------------------------------
def infer_facility_type(text: str) -> str:
    lower = text.lower()
    for ftype, keywords in _FACILITY_RULES:
        if any(kw in lower for kw in keywords):
            return ftype
    return "General Pharma"


# ---------------------------------------------------------------------------
# Claude classification
# ---------------------------------------------------------------------------
_anthropic_client = None


def get_anthropic():
    global _anthropic_client
    if _anthropic_client is None:
        try:
            import anthropic
            key = os.environ.get("ANTHROPIC_API_KEY", "")
            if not key:
                env_path = Path(__file__).parent.parent / ".env"
                if env_path.exists():
                    for line in env_path.read_text().splitlines():
                        if line.startswith("ANTHROPIC_API_KEY="):
                            key = line.split("=", 1)[1].strip().strip('"\'')
                            os.environ["ANTHROPIC_API_KEY"] = key
                            break
            _anthropic_client = anthropic.Anthropic(api_key=key)
        except Exception as exc:
            logger.warning("Could not initialise Anthropic client: %s", exc)
    return _anthropic_client


_CATEGORY_PROMPT = """You are a pharmaceutical regulatory compliance classifier.

Classify this regulatory citation into EXACTLY ONE of these categories:
- GMP violations
- Documentation & record keeping
- Training & competency
- Quality management system
- Deviation management
- Change control
- Batch release
- Stability programme
- Sterility assurance
- Aseptic processing
- Environmental monitoring
- Container closure integrity
- Parenteral manufacturing
- API synthesis and control
- Biological product specific
- Computerised systems validation
- Supply chain & procurement
- Cold chain and storage
- Labelling & claims
- Contamination & sterility
- Equipment & facilities
- Ingredient safety
- Other

Citation text:
{text}

Reply with ONLY the category name, nothing else."""

# ---------------------------------------------------------------------------
# Tobacco / ENDS guard
# Tobacco Control Act WLs use legal terms ("adulterated", "misbranded",
# "seizure", "injunction") that look like GMP/recall keywords but carry no
# pharmaceutical product-safety meaning.  Any record flagged by _TOBACCO_RE
# is treated as low-detail unless _PHARMA_SAFETY_RE also fires.
# ---------------------------------------------------------------------------
_TOBACCO_RE = re.compile(
    r"tobacco control act|family smoking prevention|tobacco product\b|"
    r"\bends\b|electronic nicotine delivery|nicotine delivery system|"
    r"\be-cigarette|\bvape\b|vaping|e-vapor|e-liquid|hookah|cigar\b|"
    r"menthol cigarette|modified risk tobacco",
    re.I,
)

# Concrete pharmaceutical/product-safety terms required to escape tobacco override
_PHARMA_SAFETY_RE = re.compile(
    r"class\s+i\s+recall|class\s+ii\s+recall|mandatory recall|urgent recall|"
    r"sterility failure|microbial contamination|salmonella|listeria|"
    r"heavy metal|lead content|arsenic|nitrosamine|undeclared drug|"
    r"sildenafil|tadalafil|sibutramine|serious adverse event|patient harm|"
    r"hospitaliz|death|fatal|carcinogen|genotoxic",
    re.I,
)


def _is_tobacco_only(text: str) -> bool:
    """Return True when text signals a tobacco/ENDS WL with no pharma safety content."""
    return bool(_TOBACCO_RE.search(text)) and not bool(_PHARMA_SAFETY_RE.search(text))


def get_context_text(c: "Citation") -> str:
    """Return all available text fields combined — used for exclusion guards (tobacco, safety).
    Unlike get_best_classification_text(), this never discards raw_listing_summary even when
    enriched_text is present, so the 'Family Smoking Prevention' header is always visible."""
    parts = [
        c.raw_listing_summary or "",
        c.summary or "",
        c.violation_details or "",
        c.enriched_text or "",
    ]
    return " ".join(p for p in parts if p)


def get_entity_label(c: "Citation") -> str:
    """Return the best available display label for a citation's responsible entity.
    Falls back through progressively less specific fields so blank-company records
    (TGA/MHRA programme notices, import alerts, authority-level actions) are still
    identifiable without mutating the original company field."""
    return (
        c.company
        or getattr(c, "facility_name", "")
        or c.product_type
        or f"Unknown entity — {c.authority} {c.source_type}"
    )


def _date_bucket_for_clustering(c: Citation) -> str:
    """Return a deterministic date bucket string for cluster grouping.

    - import_alert: never grouped — use record id as unique bucket
    - warning_letter: 14-day buckets (YYYY-MM-E for days 1–14, YYYY-MM-L for 15+)
    - all others (enforcement, etc.): 30-day bucket = YYYY-MM
    """
    if c.source_type == "import_alert":
        return c.id
    date = c.date or ""
    if len(date) < 7:
        return date or c.id
    ym = date[:7]
    if c.source_type == "warning_letter":
        try:
            day = int(date[8:10]) if len(date) >= 10 else 1
        except ValueError:
            day = 1
        return f"{ym}-{'E' if day <= 14 else 'L'}"
    return ym


_CLUSTER_PRIORITY_ORDER: dict[str, int] = {"P1": 0, "P2": 1, "P3": 2, "P4": 3, "": 4}


def compute_clusters(citations: list[Citation]) -> list[Citation]:
    """Assign cluster fields to every citation.

    Grouping key: entity_label + authority + source_type + failure_mode + date_bucket.
    Single-record groups stay cluster_size=1 with cluster_primary=True and no cluster_id.
    Primary selection: highest priority > has AI summary > newest date > stable id.
    """
    from collections import defaultdict as _dd

    groups: dict[str, list[Citation]] = _dd(list)
    id_to_key: dict[str, str] = {}

    for c in citations:
        label = get_entity_label(c).lower().strip()
        bucket = _date_bucket_for_clustering(c)
        key = "|".join([
            label,
            (c.authority or "").lower(),
            (c.source_type or "").lower(),
            (c.failure_mode or "").lower(),
            bucket,
        ])
        groups[key].append(c)
        id_to_key[c.id] = key

    def _primary_sort_key(c: Citation) -> tuple:
        prio = _CLUSTER_PRIORITY_ORDER.get(c.priority or "", 4)
        no_ai = 0 if (c.classification_confidence >= 0.5 and c.decision_summary) else 1
        date_neg = -_date_sort_key(c.date)
        return (prio, no_ai, date_neg, c.id)

    result_map: dict[str, Citation] = {}

    for key, members in groups.items():
        cluster_size = len(members)

        if cluster_size == 1:
            # Singleton — leave cluster fields at defaults
            result_map[members[0].id] = members[0]
            continue

        cluster_id = hashlib.sha256(key.encode()).hexdigest()[:12]
        sorted_members = sorted(members, key=_primary_sort_key)
        primary = sorted_members[0]

        # Label: entity + failure mode (skip generic/empty modes)
        entity = get_entity_label(primary)
        fm_str = (primary.failure_mode or "").replace("_", " ")
        cluster_label = (
            f"{entity} — {fm_str}"
            if fm_str and fm_str not in ("insufficient detail", "")
            else entity
        )[:120]

        # Cluster priority = best among members
        cluster_priority = min(
            (m.priority or "P4" for m in members),
            key=lambda p: _CLUSTER_PRIORITY_ORDER.get(p, 4),
            default="",
        )

        dates = sorted(m.date for m in members if m.date)
        if len(dates) >= 2:
            cluster_reason = f"{cluster_size} records, {dates[0][:7]}–{dates[-1][:7]}"
        elif dates:
            cluster_reason = f"{cluster_size} records, {dates[0][:7]}"
        else:
            cluster_reason = f"{cluster_size} records"

        for c in members:
            updated = asdict(c)
            updated["cluster_id"]       = cluster_id
            updated["cluster_size"]     = cluster_size
            updated["cluster_primary"]  = (c.id == primary.id)
            updated["cluster_label"]    = cluster_label
            updated["cluster_reason"]   = cluster_reason
            updated["cluster_priority"] = cluster_priority
            result_map[c.id] = Citation(**updated)

    return [result_map[c.id] for c in citations]


_KEYWORD_RULES: list[tuple[str, list[str]]] = [
    ("Sterility assurance",         ["sterility", "sterile assurance", "sar", "sterility failure"]),
    ("Aseptic processing",          ["aseptic", "aseptic technique", "aseptic fill", "grade a", "grade b", "cleanroom"]),
    ("Environmental monitoring",    ["environmental monitoring", "em program", "em data", "viable particle", "bioburden"]),
    ("Container closure integrity", ["container closure", "cci", "seal integrity", "leachable", "extractable"]),
    ("Parenteral manufacturing",    ["parenteral", "injectable", "lyophilized", "fill-finish", "vial", "ampoule"]),
    ("API synthesis and control",   ["api", "active substance", "drug substance", "synthesis", "starting material"]),
    ("Biological product specific", ["biologic", "cell culture", "fermentation", "viral clearance", "purification", "mab"]),
    ("Computerised systems validation", ["csv", "computer", "software", "audit trail", "21 cfr part 11", "electronic record"]),
    ("Deviation management",        ["deviation", "oos", "out of specification", "ooc", "investigation", "capa"]),
    ("Change control",              ["change control", "change management"]),
    ("Batch release",               ["batch release", "batch disposition", "lot release", "certificate of analysis", "qp release"]),
    ("Stability programme",         ["stability", "shelf life", "expiry", "degradation", "accelerated stability"]),
    ("Cold chain and storage",      ["cold chain", "refrigerated", "frozen", "temperature excursion", "2-8", "cryogenic"]),
    ("Training & competency",       ["training", "competency", "qualified person", "qualification", "personnel"]),
    ("Documentation & record keeping", ["documentation", "record", "batch record", "sop", "logbook", "data integrity"]),
    ("GMP violations",              ["cgmp", "good manufacturing practice", "gmp violation"]),
    ("Labelling & claims",          ["label", "claim", "misleading", "unapproved claim", "false claim"]),
    ("Contamination & sterility",   ["contamination", "microbial", "salmonella", "listeria", "foreign material", "particulate"]),
    ("Supply chain & procurement",  ["supplier", "vendor", "procurement", "raw material", "fsvp", "contract"]),
    ("Equipment & facilities",      ["equipment", "facility", "calibration", "sanitation", "cleaning", "maintenance"]),
    ("Quality management system",   ["quality system", "qms", "quality management", "audit", "pharmaceutical quality"]),
    ("Ingredient safety",           ["undeclared", "adulterant", "identity", "purity", "potency", "nitrosamine", "impurity"]),
]


_FAILURE_MODE_RULES: list[tuple[str, list[str]]] = [
    ("contamination_microbial",  ["microbial contamination", "bioburden", "salmonella", "listeria",
                                  "e. coli", "mold", "yeast", "bacterial", "microbial limits",
                                  "microbial testing"]),
    ("contamination_chemical",   ["chemical contamination", "nitrosamine", "solvent residue",
                                  "heavy metal", "heavy metals", "lead contamination",
                                  "lead impurity", "blood lead", "lead levels",
                                  "arsenic contamination", "arsenic impurity",
                                  "mercury contamination", "cadmium contamination",
                                  "genotoxic impurity", "elemental impurity",
                                  "residual solvent", "benzene contamination",
                                  "toxic element", "ndma", "ndea", "nmba"]),
    ("contamination_foreign",    ["foreign material", "foreign matter", "particulate",
                                  "glass particle", "metal fragment", "visible particle",
                                  "foreign body"]),
    ("inadequate_testing",       ["out-of-specification", "oos", "failed testing",
                                  "inadequate testing", "analytical method", "method validation",
                                  "purity", "potency", "identity test", "lab investigation"]),
    ("inadequate_stability",     ["stability", "shelf life", "expiry", "degradation",
                                  "accelerated stability", "stability data", "stability programme",
                                  "stability testing"]),
    ("inadequate_documentation", ["data integrity", "audit trail", "falsification",
                                  "data manipulation", "batch record incomplete", "logbook",
                                  "electronic record", "sop not followed", "records not maintained"]),
    ("equipment_calibration",    ["calibration", "equipment qualification", "iq oq pq",
                                  "instrument calibration", "equipment maintenance",
                                  "preventive maintenance", "unqualified equipment"]),
    ("cleaning_validation",      ["cleaning validation", "cleaning procedure",
                                  "cross-contamination prevention", "cleaning effectiveness",
                                  "equipment cleaning", "cleaning agent"]),
    ("sterility_assurance",      ["sterility assurance", "sterility failure", "sterility testing",
                                  "sterile manufacturing", "terminal sterilization", "sterility",
                                  "endotoxin", "aseptic"]),
    ("supplier_qualification",   ["supplier qualification", "vendor qualification", "supplier audit",
                                  "raw material supplier", "contract manufacturer qualification",
                                  "fsvp", "foreign supplier", "unqualified supplier"]),
    ("out_of_specification",     ["out of specification", "oos result", "non-conforming",
                                  "failed release", "batch failure", "spec exceedance",
                                  "release failure"]),
    ("process_validation",       ["process validation", "validation failure", "unvalidated process",
                                  "process control", "in-process testing", "cpv",
                                  "validation protocol"]),
    ("labelling_error",          ["labelling error", "misbranding", "mislabeled", "false claim",
                                  "misleading label", "incorrect label", "unapproved claim",
                                  "label mix-up", "undeclared"]),
    ("import_detention",         ["import alert", "import detention", "detention without examination",
                                  "automatic detention", "dwpe", "import refusal"]),
    ("recall_voluntary",         ["voluntary recall", "class iii recall", "voluntary removal",
                                  "market withdrawal", "precautionary recall"]),
    ("recall_mandatory",         ["class i recall", "class ii recall", "mandatory recall",
                                  "urgent recall"]),
    ("adverse_event_cluster",    ["adverse event", "serious adverse", "hospitaliz", "death",
                                  "injury report", "adr cluster", "medwatch", "serious injury"]),
    ("facility_hygiene",         ["facility sanitation", "pest control", "facility maintenance",
                                  "building maintenance", "facility hygiene", "sanitation failure",
                                  "pest infestation"]),
]


# ---------------------------------------------------------------------------
# Per-category evidence terms — required to confirm a category assignment.
# A category is only "confirmed" when at least one of these phrases is present
# in the available text.  A match in enriched_text = confirmed; match only in
# listing-level text = provisional; no match = unconfirmed.
# ---------------------------------------------------------------------------
_CATEGORY_EVIDENCE_TERMS: dict[str, list[str]] = {
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
    ],
    "Computerised systems validation": [
        "computer system validation", "csv", "computerized system",
        "computerised system", "electronic record", "electronic signature",
        "audit trail", "access control", "data integrity",
        "part 11", "annex 11", "21 cfr part 11", "software validation",
    ],
    "Equipment & facilities": [
        "equipment", "facility", "facilities", "calibration", "maintenance",
        "cleaning validation", "hvac", "utilities", "water system",
        "premises", "sanitation", "cleanroom", "preventive maintenance",
        "equipment qualification",
    ],
    "Documentation & record keeping": [
        "batch record", "master record", "sop", "standard operating procedure",
        "procedure", "documentation", "recordkeeping", "record keeping",
        "logbook", "certificate of analysis", "coa",
        "batch manufacturing record", "protocol",
    ],
    "Labelling & claims": [
        "label", "labelling", "labeling", "misbranded", "misbranding",
        "claim", "disease claim", "false or misleading", "unapproved claim",
        "label mix-up", "incorrect label", "health claim",
    ],
    "Sterility assurance": [
        "sterility", "sterile", "aseptic", "endotoxin", "bioburden",
        "sterility testing", "sterility failure", "terminal sterilization",
        "sterilisation", "depyrogenation",
    ],
    "Contamination & sterility": [
        "contamination", "contaminated", "microbial", "salmonella", "listeria",
        "e. coli", "mold", "yeast", "foreign material", "foreign matter",
        "particulate", "cross-contamination",
    ],
    "GMP violations": [
        "cgmp", "good manufacturing practice", "gmp violation",
        "current good manufacturing", "21 cfr 211",
        "eu gmp", "non-compliance with gmp",
    ],
    "Ingredient safety": [
        "undeclared", "adulterant", "adulterated", "identity", "purity",
        "potency", "nitrosamine", "impurity", "undeclared drug substance",
    ],
    "Training & competency": [
        "training", "competency", "qualified person", "qualification",
        "personnel", "gmp training", "training programme",
    ],
    "Supply chain & procurement": [
        "supplier", "vendor", "procurement", "raw material supplier",
        "fsvp", "foreign supplier", "contract manufacturer",
        "supply chain", "supplier qualification",
    ],
    "Aseptic processing": [
        "aseptic", "aseptic technique", "aseptic fill", "grade a",
        "grade b", "cleanroom", "laminar air flow", "media fill",
    ],
    "Environmental monitoring": [
        "environmental monitoring", "em program", "em data",
        "viable particle", "bioburden monitoring", "air sample",
    ],
    "Stability programme": [
        "stability", "shelf life", "expiry", "degradation",
        "accelerated stability", "stability data",
    ],
}

# Source-type patterns — guidance / scientific content NOT enforcement
_GUIDANCE_URL_RE = re.compile(
    r"/guidance/|/guidelines?/|/consultations?/|/scientific.opinions?/|"
    r"onlinelibrary\.wiley\.com|efsa\.europa\.eu|bfr\.bund\.de",
    re.I,
)
_GUIDANCE_TITLE_RE = re.compile(
    r"\bguideline\b|\bguidance\b|\bconsultation\b|\bopinion\b"
    r"|\bassessment\b|\bmonitoring report\b|\bscientific report\b"
    r"|\bscientific statement\b|\btechnical report\b|\bpeer review\b"
    r"|\bflavour(?:ing)?\b|\bfeed additive\b",
    re.I,
)
_ENFORCEMENT_SIGNAL_RE = re.compile(
    r"\bnon.compliance\b|\bdeficiency\b|\bviolation\b|\benforcement\b"
    r"|\bwarning letter\b|\brecall\b|\bsuspension\b|\binjunction\b"
    r"|\bdefective medicine\b|\binspection finding\b",
    re.I,
)


def _is_guidance_source(c: "Citation") -> bool:
    """Return True when URL/title signals guidance/scientific-opinion, not enforcement."""
    title = f"{c.summary or ''} {c.violation_details or ''}"
    return bool(
        _GUIDANCE_URL_RE.search(c.url or "")
        or _GUIDANCE_TITLE_RE.search(title)
    )


def _is_enforcement_source(c: "Citation") -> bool:
    title = f"{c.summary or ''} {c.violation_details or ''}"
    return bool(_ENFORCEMENT_SIGNAL_RE.search(title))


def classify_with_evidence(c: "Citation", best_text: str) -> dict:
    """
    Return evidence-backed classification fields for a citation.

    Computes:
      category_confidence     — fraction of evidence terms present
      category_evidence       — matched phrases supporting primary_gmp_category
      failure_mode_evidence   — matched phrases supporting failure_mode
      classification_basis    — which text field drove the classification
      classification_status   — confirmed | provisional | unconfirmed

    Rules:
      confirmed   — evidence phrase(s) found in enriched_text (richest source)
      provisional — evidence phrase(s) found only in listing-level text, OR
                    enriched evidence but confidence < 0.4
      unconfirmed — no evidence phrase found for the assigned category, OR
                    category is Other/Insufficient Detail
    """
    cat = c.primary_gmp_category or ""
    fm  = c.failure_mode          or ""
    ev_terms = _CATEGORY_EVIDENCE_TERMS.get(cat, [])
    fm_terms_map = {mode: kws for mode, kws in _FAILURE_MODE_RULES}

    # Determine which text drove the original classification
    has_enriched = (
        c.enrichment_status in ("success", "cached")
        and len(c.enriched_text or "") > 100
    )
    if has_enriched:
        basis = "enriched_text"
    elif c.raw_listing_summary:
        basis = "raw_listing_summary"
    elif c.violation_details:
        basis = "violation_details"
    elif c.summary:
        basis = "summary"
    else:
        basis = "unknown"

    # No-evidence catch-all
    if cat in ("Other / Insufficient Detail", "") or not ev_terms:
        return {
            "category_confidence":   0.0,
            "category_evidence":     [],
            "failure_mode_evidence": [],
            "classification_basis":  basis,
            "classification_status": "unconfirmed",
        }

    # Check for guidance/scientific sources regardless of category
    is_guidance = _is_guidance_source(c) and not _is_enforcement_source(c)
    if is_guidance and cat not in ("Labelling & claims", "Ingredient safety",
                                    "Contamination & sterility"):
        return {
            "category_confidence":   0.0,
            "category_evidence":     [],
            "failure_mode_evidence": [],
            "classification_basis":  basis,
            "classification_status": "unconfirmed",
        }

    # Category evidence — check both enriched and listing text
    enriched_lower  = (c.enriched_text or "").lower()
    listing_lower   = " ".join(filter(None, [
        c.raw_listing_summary, c.violation_details, c.summary,
    ])).lower()
    full_lower      = best_text.lower()

    cat_ev_enriched  = [t for t in ev_terms if t in enriched_lower] if has_enriched else []
    cat_ev_listing   = [t for t in ev_terms if t in listing_lower]
    cat_ev_all       = [t for t in ev_terms if t in full_lower]

    if ev_terms:
        cat_conf = round(min(1.0, len(cat_ev_all) / max(len(ev_terms) * 0.25, 1)), 2)
    else:
        cat_conf = 0.0

    # Failure mode evidence
    fm_kws = fm_terms_map.get(fm, []) if fm else []
    fm_ev  = [kw for kw in fm_kws if kw in full_lower]

    # Determine classification_status
    if cat_ev_enriched:
        status = "confirmed"
    elif cat_ev_listing and cat_conf >= 0.3:
        status = "provisional"
    elif cat_ev_all:
        status = "provisional"
    else:
        status = "unconfirmed"

    return {
        "category_confidence":   cat_conf,
        "category_evidence":     cat_ev_all[:10],   # cap stored phrases at 10
        "failure_mode_evidence": fm_ev[:8],
        "classification_basis":  basis,
        "classification_status": status,
    }


def multi_label_classify(text: str) -> tuple[str, list[str]]:
    """
    Return (primary_category, secondary_categories) using keyword match counts.
    Primary = highest-scoring category; secondary = next up to 3 with any match.
    """
    lower = text.lower()
    scores: dict[str, int] = {}
    for cat, keywords in _KEYWORD_RULES:
        count = sum(1 for kw in keywords if kw in lower)
        if count > 0:
            scores[cat] = scores.get(cat, 0) + count
    if not scores:
        return "Other / Insufficient Detail", []
    ranked = sorted(scores.items(), key=lambda x: -x[1])
    primary = ranked[0][0]
    secondary = [cat for cat, _ in ranked[1:4]]
    return primary, secondary


# Word-boundary patterns for single-token chemical contaminant terms that would
# produce substring false positives if matched with plain `in lower` (e.g. "lead"
# matching "could lead to", "benzene" is safe but kept here for consistency).
_CHEM_BOUNDARY_RE = re.compile(
    r"\bbenzene\b|\bndma\b|\bndea\b|\bnmba\b|\barsenic\b|\bmercury\b|\bcadmium\b",
    re.I,
)


def classify_failure_mode(text: str, context_text: str = "") -> tuple[str, float]:
    """
    Return (failure_mode, confidence) from deterministic keyword rules.
    Confidence = matched_count / (total_keywords * 0.3), capped at 1.0.

    context_text — combined all-field text used for tobacco/exclusion guards.
    If omitted, text is used for both positive matching and guard checks
    (backward-compatible default).
    """
    guard_text = context_text if context_text else text
    if _is_tobacco_only(guard_text):
        return "insufficient_detail", 0.1

    lower = text.lower()
    best_mode = ""
    best_score = 0.0
    for mode, keywords in _FAILURE_MODE_RULES:
        if mode == "contamination_chemical":
            # Multi-word terms: plain substring match is safe (no ambiguous substrings).
            # Single-token chemical names: require word-boundary match to avoid
            # "lead" → "could lead to" style false positives.
            matched = sum(1 for kw in keywords if " " in kw and kw in lower)
            if _CHEM_BOUNDARY_RE.search(text):
                matched += 1
        else:
            matched = sum(1 for kw in keywords if kw in lower)
        if matched > 0:
            confidence = min(1.0, matched / max(len(keywords) * 0.3, 1))
            if confidence > best_score:
                best_score = confidence
                best_mode = mode
    return best_mode, round(best_score, 2)


def classify_with_claude(text: str, fallback: str = "GMP violations") -> str:
    client = get_anthropic()
    if client is None:
        return keyword_classify(text, fallback)
    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=32,
            messages=[{"role": "user", "content": _CATEGORY_PROMPT.format(text=text[:1200])}],
        )
        response = msg.content[0].text.strip()
        for cat in CATEGORIES:
            if response.lower() == cat.lower():
                return cat
        lower_resp = response.lower()
        for cat in CATEGORIES:
            if any(word in lower_resp for word in cat.lower().split() if len(word) > 3):
                return cat
        return keyword_classify(text, fallback)
    except Exception as exc:
        logger.debug("Claude classify error: %s", exc)
        return keyword_classify(text, fallback)


def keyword_classify(text: str, fallback: str = "GMP violations") -> str:
    lower = text.lower()
    for cat, keywords in _KEYWORD_RULES:
        if any(kw in lower for kw in keywords):
            return cat
    return fallback


# ---------------------------------------------------------------------------
# AI call tracker — global counter enforced across all pipeline phases
# ---------------------------------------------------------------------------

class AiCallTracker:
    """
    Single source of truth for all Claude API calls in the pipeline.
    Scrapers must NOT call Claude — all classification is keyword-only at
    scrape time. This tracker records calls only from the intelligence pass.
    """

    def __init__(
        self,
        no_ai:        bool          = False,
        max_ai_calls: int           = 25,
        sample:       Optional[int] = None,
    ) -> None:
        self.no_ai        = no_ai
        self.max_ai_calls = max_ai_calls
        self.sample       = sample
        # Phase-level call counts (scraping must always remain 0)
        self.scraping_phase_ai_calls     = 0
        self.enrichment_phase_ai_calls   = 0
        self.intelligence_phase_ai_calls = 0
        # Skip accounting
        self.ai_skipped_due_to_no_ai     = 0
        self.ai_skipped_due_to_max_calls = 0
        self.ai_skipped_due_to_sample    = 0
        self.ai_skipped_cached           = 0
        self.ai_eligible_count           = 0
        # Result accounting
        self.ai_results_accepted                  = 0
        self.ai_results_discarded_low_confidence  = 0
        # Queue composition (set by pharma_intelligence after sorting)
        self.ai_queue_p1_count              = 0
        self.ai_queue_p2_count              = 0
        self.ai_queue_cluster_primary_count = 0
        self.ai_queue_au_relevant_count     = 0
        self.ai_queue_warning_letter_count  = 0
        self.ai_queue_low_priority_count    = 0
        # Per-tier accepted / discarded
        self.ai_accepted_by_tier:   dict[str, int] = {}
        self.ai_discarded_by_tier:  dict[str, int] = {}

    @property
    def total_ai_calls(self) -> int:
        return (
            self.scraping_phase_ai_calls
            + self.enrichment_phase_ai_calls
            + self.intelligence_phase_ai_calls
        )

    def log_call(self, citation_id: str, phase: str) -> None:
        """Record a Claude call, print a tagged console line, and increment phase counter."""
        n = self.total_ai_calls + 1
        msg = f"[AI] Calling Claude for citation {citation_id}, phase={phase}, call {n}/{self.max_ai_calls}"
        print(msg)
        logger.info(msg)
        if phase == "scraping":
            self.scraping_phase_ai_calls += 1
        elif phase == "enrichment":
            self.enrichment_phase_ai_calls += 1
        else:
            self.intelligence_phase_ai_calls += 1

    def as_dict(self) -> dict:
        return {
            "scraping_phase_ai_calls":     self.scraping_phase_ai_calls,
            "enrichment_phase_ai_calls":   self.enrichment_phase_ai_calls,
            "intelligence_phase_ai_calls": self.intelligence_phase_ai_calls,
            "total_ai_calls":              self.total_ai_calls,
            "ai_skipped_due_to_no_ai":     self.ai_skipped_due_to_no_ai,
            "ai_skipped_due_to_max_calls": self.ai_skipped_due_to_max_calls,
            "ai_skipped_due_to_sample":    self.ai_skipped_due_to_sample,
            "ai_skipped_cached":                         self.ai_skipped_cached,
            "ai_eligible_count":                         self.ai_eligible_count,
            "ai_results_accepted":                       self.ai_results_accepted,
            "ai_results_discarded_low_confidence":       self.ai_results_discarded_low_confidence,
            "ai_queue_p1_count":              self.ai_queue_p1_count,
            "ai_queue_p2_count":              self.ai_queue_p2_count,
            "ai_queue_cluster_primary_count": self.ai_queue_cluster_primary_count,
            "ai_queue_au_relevant_count":     self.ai_queue_au_relevant_count,
            "ai_queue_warning_letter_count":  self.ai_queue_warning_letter_count,
            "ai_queue_low_priority_count":    self.ai_queue_low_priority_count,
            "ai_accepted_by_tier":            self.ai_accepted_by_tier,
            "ai_discarded_by_tier":           self.ai_discarded_by_tier,
        }


# ---------------------------------------------------------------------------
# Enrichment cache helpers
# ---------------------------------------------------------------------------

def _load_enrich_cache() -> dict:
    if ENRICH_CACHE_PATH.exists():
        try:
            return json.loads(ENRICH_CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_enrich_cache(cache: dict) -> None:
    ENRICH_CACHE_PATH.write_text(json.dumps(cache, indent=2), encoding="utf-8")


def _cache_entry_valid(entry: dict) -> bool:
    try:
        fetched = datetime.fromisoformat(entry["fetched_at"])
        if fetched.tzinfo is None:
            fetched = fetched.replace(tzinfo=timezone.utc)
        return (NOW - fetched).days < ENRICH_CACHE_TTL_DAYS
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Detail-page fetching (ported from citation_report.py)
# ---------------------------------------------------------------------------

_VIOLATION_ANCHOR = re.compile(
    r"(?:significant violations?|violations? are as follows|findings? are as follows"
    r"|observations? are as follows|during (?:our|the) inspection"
    r"|we observed the following|the following observations?"
    r"|you failed to|failure to|failed to (?:establish|maintain|implement|develop|document)"
    r"|were not in compliance|is not in compliance|did not (?:develop|establish|maintain)"
    r"|you have not|following deficiencies)[:\s]+",
    re.I,
)


def _fetch_fda_wl_text(url: str) -> tuple[str, str]:
    """
    Fetch FDA warning letter detail page and extract violation text.
    Returns (enriched_text, error_message). Timeout is 10s (conservative).
    """
    try:
        resp = get_session().get(url, timeout=10)
        if resp.status_code != 200:
            return "", f"HTTP {resp.status_code}"
        soup = BeautifulSoup(resp.text, "lxml")
        main = soup.select_one("main, article, .main-content, #main-content")
        if not main:
            return "", "no main content element"
        text = main.get_text(" ", strip=True)
        text = re.sub(r"\(b\)\(\d+\)", "[redacted]", text)
        text = re.sub(r"\s{2,}", " ", text)

        m = _VIOLATION_ANCHOR.search(text)
        if m:
            start = max(0, m.start())
            snippet = text[start:start + 2000].strip()
            sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z])", snippet)
            return " ".join(sentences[:8]).strip()[:2000], ""

        for keyword in ("violation", "inspection finding", "failed to", "you have not"):
            idx = text.lower().find(keyword)
            if idx >= 0:
                start = max(0, text.rfind(". ", max(0, idx - 200), idx) + 2)
                return text[start:start + 2000].strip(), ""

        dear_idx = text.find("Dear ")
        if dear_idx >= 0:
            return text[dear_idx:dear_idx + 2000].strip(), ""

        return text[:2000], ""
    except Exception as exc:
        return "", str(exc)


def _fetch_tga_text(url: str) -> tuple[str, str]:
    """
    Fetch TGA alert/compliance page and extract substantive body text.
    Returns (enriched_text, error_message).
    """
    try:
        resp = get_session().get(url, timeout=10)
        if resp.status_code != 200:
            return "", f"HTTP {resp.status_code}"
        soup = BeautifulSoup(resp.text, "lxml")
        main = soup.select_one("main, article, .field--name-body, .node__content")
        if not main:
            return "", "no main content element"
        paragraphs = [
            p.get_text(" ", strip=True)
            for p in main.find_all("p")
            if len(p.get_text(strip=True)) > 60
        ]
        return " ".join(paragraphs[:6])[:2000], ""
    except Exception as exc:
        return "", str(exc)


def _enrich_one(c: Citation, cache: dict) -> dict:
    """
    Enrich a single citation. Returns a dict of enrichment field values.
    Cache-first: if a valid cache entry exists, return it immediately.
    """
    # Cache hit
    if c.id in cache and _cache_entry_valid(cache[c.id]):
        entry = cache[c.id]
        return {
            "enriched_text":         entry.get("enriched_text", ""),
            "enrichment_status":     "cached",
            "enrichment_source":     entry.get("enrichment_source", ""),
            "enrichment_confidence": entry.get("enrichment_confidence", 0.0),
            "enrichment_error":      "",
            "enriched_text_hash":    entry.get("enriched_text_hash", ""),
        }

    # Determine enrichment path
    if c.source_type == "warning_letter" and c.authority == "FDA":
        text, error = _fetch_fda_wl_text(c.url)
        if text:
            h = hashlib.sha256(text[:100].encode()).hexdigest()[:16]
            entry = {
                "enriched_text": text, "enrichment_status": "success",
                "enrichment_source": "fda_detail_page", "enrichment_confidence": 1.0,
                "enrichment_error": "", "enriched_text_hash": h,
                "fetched_at": NOW.isoformat(),
            }
        else:
            entry = {
                "enriched_text": "", "enrichment_status": "failed",
                "enrichment_source": "fda_detail_page", "enrichment_confidence": 0.0,
                "enrichment_error": error or "unknown", "enriched_text_hash": "",
                "fetched_at": NOW.isoformat(),
            }
    elif c.authority == "TGA" and c.source_type in ("compliance_action", "safety_alert", "recall"):
        text, error = _fetch_tga_text(c.url)
        if text:
            h = hashlib.sha256(text[:100].encode()).hexdigest()[:16]
            entry = {
                "enriched_text": text, "enrichment_status": "success",
                "enrichment_source": "tga_notice", "enrichment_confidence": 0.85,
                "enrichment_error": "", "enriched_text_hash": h,
                "fetched_at": NOW.isoformat(),
            }
        else:
            entry = {
                "enriched_text": "", "enrichment_status": "failed",
                "enrichment_source": "tga_notice", "enrichment_confidence": 0.0,
                "enrichment_error": error or "unknown", "enriched_text_hash": "",
                "fetched_at": NOW.isoformat(),
            }
    else:
        entry = {
            "enriched_text": "", "enrichment_status": "not_applicable",
            "enrichment_source": "none", "enrichment_confidence": 0.2,
            "enrichment_error": "", "enriched_text_hash": "",
            "fetched_at": NOW.isoformat(),
        }

    cache[c.id] = entry
    return {k: v for k, v in entry.items() if k != "fetched_at"}


def enrich_batch_parallel(
    citations: list[Citation],
    cache: dict,
    max_workers: int = 3,
) -> list[Citation]:
    """
    Enrich citations with detail-page text. Cache-first for all sources.
    Uses conservative parallelism (default 3 workers) to avoid rate limits.
    """
    to_enrich = [
        c for c in citations
        if (c.source_type == "warning_letter" and c.authority == "FDA")
        or (c.authority == "TGA" and c.source_type in ("compliance_action", "safety_alert", "recall"))
    ]

    cached_count  = sum(1 for c in to_enrich if c.id in cache and _cache_entry_valid(cache[c.id]))
    to_fetch      = [c for c in to_enrich if c.id not in cache or not _cache_entry_valid(cache[c.id])]
    not_applicable = [c for c in citations if c not in to_enrich]

    logger.info(
        "Enrichment: %d to process (%d cached, %d to fetch), %d not applicable",
        len(to_enrich), cached_count, len(to_fetch), len(not_applicable),
    )

    results: dict[str, dict] = {}

    # Cached entries — instant
    for c in to_enrich:
        if c.id in cache and _cache_entry_valid(cache[c.id]):
            results[c.id] = _enrich_one(c, cache)

    # Uncached — parallel fetch
    if to_fetch:
        done = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_enrich_one, c, cache): c for c in to_fetch}
            for fut in concurrent.futures.as_completed(futures):
                c = futures[fut]
                try:
                    results[c.id] = fut.result()
                except Exception as exc:
                    results[c.id] = {
                        "enriched_text": "", "enrichment_status": "failed",
                        "enrichment_source": "unknown", "enrichment_confidence": 0.0,
                        "enrichment_error": str(exc), "enriched_text_hash": "",
                    }
                done += 1
                if done % 25 == 0:
                    logger.info("  … enriched %d/%d", done, len(to_fetch))
                if done < len(to_fetch):
                    time.sleep(0.5)   # polite pacing — avoid FDA 429

    # Apply enrichment fields back to Citation objects
    enriched_citations: list[Citation] = []
    for c in citations:
        if c.id in results:
            r = results[c.id]
            updated = asdict(c)
            updated.update({
                "raw_listing_summary":   c.violation_details,
                "enriched_text":         r.get("enriched_text", ""),
                "enrichment_status":     r.get("enrichment_status", ""),
                "enrichment_source":     r.get("enrichment_source", ""),
                "enrichment_confidence": r.get("enrichment_confidence", 0.0),
                "enrichment_error":      r.get("enrichment_error", ""),
                "enriched_text_hash":    r.get("enriched_text_hash", ""),
            })
            enriched_citations.append(Citation(**updated))
        else:
            updated = asdict(c)
            updated.update({
                "raw_listing_summary":   c.violation_details,
                "enrichment_status":     "not_applicable",
                "enrichment_source":     "none",
                "enrichment_confidence": 0.2,
            })
            enriched_citations.append(Citation(**updated))

    return enriched_citations


# ---------------------------------------------------------------------------
# Scraper 1 — FDA Warning Letters (all pharmaceutical categories)
# ---------------------------------------------------------------------------
_FDA_WL_URL = (
    "https://www.fda.gov/inspections-compliance-enforcement-and-criminal-investigations"
    "/compliance-actions-and-activities/warning-letters"
)

# No supplement filter — accept all pharma/device/food WLs
def scrape_fda_warning_letters() -> list[Citation]:
    logger.info("FDA Warning Letters: fetching …")
    results: list[Citation] = []
    try:
        page_resp = http_get(_FDA_WL_URL, timeout=40)
        if page_resp.status_code == 403:
            logger.warning("FDA WL: 403 rate-limited, skipping")
            return results

        dom_id    = re.search(r'"view_dom_id"\s*:\s*"([a-f0-9]+)"', page_resp.text)
        view_name = re.search(r'"view_name"\s*:\s*"([^"]+)"',        page_resp.text)
        disp_id   = re.search(r'"view_display_id"\s*:\s*"([^"]+)"',  page_resp.text)

        if dom_id:
            ajax = _fda_wl_ajax(
                dom_id=dom_id.group(1),
                view_name=view_name.group(1) if view_name else "warning_letter_solr_index",
                display_id=disp_id.group(1) if disp_id else "warning_letter_solr_block",
            )
            if ajax:
                logger.info("FDA Warning Letters (AJAX): %d", len(ajax))
                return ajax
        results.extend(_fda_wl_html(page_resp.text))
    except Exception as exc:
        logger.warning("FDA WL error: %s", exc)

    logger.info("FDA Warning Letters: %d citations", len(results))
    return results


def _fda_wl_ajax(dom_id: str, view_name: str, display_id: str) -> list[Citation]:
    AJAX_URL  = "https://www.fda.gov/datatables/views/ajax"
    PAGE_SIZE = 200   # fewer requests = less chance of rate-limiting
    results: list[Citation] = []
    seen: set[str] = set()

    for start in range(0, 4000, PAGE_SIZE):
        if start:
            time.sleep(0.5)
        payload = {
            "view_name":        view_name,
            "view_display_id":  display_id,
            "view_dom_id":      dom_id,
            "pager_element":    "0",
            "page":             "0",
            "start":            str(start),
            "length":           str(PAGE_SIZE),
        }
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type":     "application/x-www-form-urlencoded",
            "Referer":          _FDA_WL_URL,
        }
        try:
            resp = get_session().post(AJAX_URL, data=payload, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("FDA WL AJAX start=%d error: %s", start, exc)
            break

        rows = data.get("data", [])
        if not rows:
            break

        # Table columns (verified against live FDA site 2026-Q2):
        #   0: Posted Date   1: Letter Issue Date   2: Company Name (has link)
        #   3: Issuing Office   4: Subject   5: Response Letter
        #   6: Closeout Letter  7: Excerpt
        stop = False
        for row in rows:
            cells = row if isinstance(row, list) else list(row.values())
            if not cells:
                continue
            date_raw  = BeautifulSoup(str(cells[0]), "lxml").get_text(strip=True)
            comp_soup = BeautifulSoup(str(cells[2]), "lxml") if len(cells) > 2 else None
            company   = comp_soup.get_text(strip=True) if comp_soup else ""
            link      = comp_soup.find("a") if comp_soup else None
            href      = link.get("href", "") if link else ""
            url       = href if href.startswith("http") else f"https://www.fda.gov{href}" if href else ""
            if not url:
                continue
            title     = BeautifulSoup(str(cells[4]), "lxml").get_text(strip=True) if len(cells) > 4 else company
            excerpt   = BeautifulSoup(str(cells[7]), "lxml").get_text(strip=True) if len(cells) > 7 else ""
            dt        = parse_date(date_raw, prefer_mdy=True)
            if dt and dt < CUTOFF_12M:
                stop = True
                break
            if url in seen:
                continue
            seen.add(url)
            raw      = " ".join(filter(None, [title, company, excerpt]))
            ftype    = infer_facility_type(raw)
            cat      = keyword_classify(raw)
            sev      = infer_severity(raw, "warning_letter")
            results.append(Citation(
                id=make_id("FDA_WL", url),
                authority="FDA", source_type="warning_letter",
                company=company, date=date_str(dt),
                category=cat, severity=sev, summary=title,
                url=url, product_type="FDA Warning Letter",
                country="United States", facility_type=ftype,
                violation_details=raw[:500],
            ))
        if stop:
            break

    return results


def _fda_wl_html(html: str) -> list[Citation]:
    # Table columns: 0=Posted Date, 1=Letter Issue Date, 2=Company Name (link),
    #                3=Issuing Office, 4=Subject, 5=Response, 6=Closeout, 7=Excerpt
    results: list[Citation] = []
    soup = BeautifulSoup(html, "lxml")
    for row in soup.select("table tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        link = cells[2].find("a")
        if not link:
            continue
        company  = cells[2].get_text(strip=True)
        href     = link.get("href", "")
        url      = href if href.startswith("http") else f"https://www.fda.gov{href}"
        title    = cells[4].get_text(strip=True)
        date_raw = cells[0].get_text(strip=True)
        excerpt  = cells[7].get_text(strip=True) if len(cells) > 7 else ""
        dt       = parse_date(date_raw, prefer_mdy=True)
        if dt and dt < CUTOFF_12M:
            break
        raw   = " ".join(filter(None, [title, company, excerpt]))
        ftype = infer_facility_type(raw)
        cat   = keyword_classify(raw)
        sev   = infer_severity(raw, "warning_letter")
        results.append(Citation(
            id=make_id("FDA_WL", url),
            authority="FDA", source_type="warning_letter",
            company=company, date=date_str(dt),
            category=cat, severity=sev, summary=title,
            url=url, product_type="FDA Warning Letter",
            country="United States", facility_type=ftype,
            violation_details=raw[:500],
        ))
    return results


# ---------------------------------------------------------------------------
# Scraper 2 — OpenFDA Enforcement APIs (drug, device, food/supplement)
# ---------------------------------------------------------------------------
def _openfda_enforcement(
    endpoint: str,
    product_label: str,
    default_facility_type: str,
    extra_search: str = "",
) -> list[Citation]:
    """Generic OpenFDA /enforcement.json fetcher. Returns all records in window."""
    cutoff_str = CUTOFF_12M.strftime("%Y%m%d")
    search = f"recall_initiation_date:[{cutoff_str}+TO+29991231]"
    if extra_search:
        search += f"+AND+{extra_search}"
    base = f"https://api.fda.gov/{endpoint}/enforcement.json"
    results: list[Citation] = []
    skip = 0
    page_size = 100

    while True:
        url = f"{base}?search={search}&limit={page_size}&skip={skip}&sort=recall_initiation_date:desc"
        try:
            resp = http_get(url, timeout=30)
            if resp.status_code == 404:
                break   # endpoint exhausted or no results
            data = resp.json()
        except Exception as exc:
            logger.warning("OpenFDA %s error (skip=%d): %s", endpoint, skip, exc)
            break

        total   = data.get("meta", {}).get("results", {}).get("total", 0)
        records = data.get("results", [])
        if not records:
            break

        for rec in records:
            title    = rec.get("product_description", "")[:200]
            company  = rec.get("recalling_firm", "")
            reason   = rec.get("reason_for_recall", "")
            recall_n = rec.get("recall_number", "")
            date_raw = rec.get("recall_initiation_date", rec.get("report_date", ""))
            dt       = parse_date(date_raw)
            if dt and dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt and dt < CUTOFF_12M:
                continue
            _rn_clean = (recall_n or "").strip()
            if _rn_clean and _rn_clean.upper() != "N/A":
                rec_url = (
                    f"https://www.accessdata.fda.gov/scripts/ires/"
                    f"?action=RecallAction&RecallNumber={_rn_clean}"
                )
            else:
                rec_url = url  # fall back to the API query URL
            raw   = f"{title} {reason} {company}"
            ftype = infer_facility_type(raw) if default_facility_type == "auto" else default_facility_type
            # Override with auto if raw signals a different type
            if default_facility_type != "auto":
                detected = infer_facility_type(raw)
                if detected != "General Pharma":
                    ftype = detected

            cls = rec.get("classification", "").lower()
            if "class i" in cls and "class ii" not in cls:
                sev = "high"
            elif "class ii" in cls:
                sev = "medium"
            else:
                sev = infer_severity(raw, f"{endpoint.split('/')[0]}_enforcement")

            cat = keyword_classify(raw)
            results.append(Citation(
                id=make_id(f"FDA_{endpoint.split('/')[0].upper()}", recall_n or rec_url),
                authority="FDA",
                source_type=f"{endpoint.split('/')[0]}_enforcement",
                company=company, date=date_str(dt),
                category=cat, severity=sev,
                summary=(reason[:300] or title),
                url=rec_url, product_type=product_label,
                country=rec.get("country", "United States"),
                facility_type=ftype,
                violation_details=raw[:500],
            ))

        skip += page_size
        if skip >= total or skip >= 1000:  # cap at 1000 per endpoint
            break
        time.sleep(0.3)

    logger.info("OpenFDA %s: %d records fetched", endpoint, len(results))
    return results


def scrape_fda_drug_enforcement() -> list[Citation]:
    logger.info("FDA Drug Enforcement: fetching …")
    return _openfda_enforcement("drug", "Human Drug Product", "General Pharma")


def scrape_fda_device_enforcement() -> list[Citation]:
    logger.info("FDA Device Enforcement: fetching …")
    return _openfda_enforcement("device", "Medical Device", "Medical Device")


def scrape_fda_food_enforcement() -> list[Citation]:
    logger.info("FDA Food/Supplement Enforcement: fetching …")
    extra = (
        "product_description:(vitamin+OR+supplement+OR+probiotic"
        "+OR+mineral+OR+herbal+OR+botanical+OR+omega+OR+protein+OR+creatine"
        "+OR+amino+OR+collagen)"
    )
    return _openfda_enforcement("food", "Food / Dietary Supplement", "Supplement / Nutraceutical",
                                extra_search=extra)


# ---------------------------------------------------------------------------
# Scraper 3 — FDA Import Alerts (pharma/biologics/supplement prefixes)
# ---------------------------------------------------------------------------
_FDA_IA_BASE    = "https://www.accessdata.fda.gov/cms_ia"
_FDA_IA_LIST    = f"{_FDA_IA_BASE}/ialist.html"

# Import alert number prefixes relevant to pharma manufacturing:
# 56 = drugs (compliance), 57 = biologics/blood, 66 = drugs from non-compliant firms
# 88 = unapproved new drugs, 89 = dietary supplements, 99 = miscellaneous unapproved
# 63 = medical devices
_IA_PHARMA_PREFIXES = {"56-", "57-", "63-", "66-", "88-", "89-", "99-"}


def scrape_fda_import_alerts() -> list[Citation]:
    logger.info("FDA Import Alerts: fetching …")
    results: list[Citation] = []
    try:
        resp = http_get(_FDA_IA_LIST, timeout=25)
        if resp.status_code != 200:
            logger.warning("FDA Import Alerts: HTTP %s", resp.status_code)
            return results

        soup = BeautifulSoup(resp.text, "lxml")
        rows = soup.select("table tr")[1:]   # skip header

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            alert_num  = cells[0].get_text(strip=True)
            alert_type = cells[1].get_text(strip=True)  # e.g. DWPE
            date_raw   = cells[2].get_text(strip=True)
            alert_name = cells[3].get_text(strip=True)
            link       = row.find("a")
            href       = link["href"] if link else ""
            detail_url = f"{_FDA_IA_BASE}/{href}" if href and not href.startswith("http") else href

            # Filter to pharma-relevant prefixes
            if not any(alert_num.startswith(p) for p in _IA_PHARMA_PREFIXES):
                continue

            dt = parse_date(date_raw)
            if dt and dt < CUTOFF_12M:
                continue

            raw   = f"{alert_num} {alert_name}"
            ftype = infer_facility_type(raw)
            # Prefix-based defaults when keywords don't resolve
            if ftype == "General Pharma":
                if alert_num.startswith("57-"):
                    ftype = "Biologics / Vaccine"
                elif alert_num.startswith("89-"):
                    ftype = "Supplement / Nutraceutical"
                elif alert_num.startswith("63-"):
                    ftype = "Medical Device"

            cat = keyword_classify(raw)
            sev = infer_severity(raw, "import_alert")
            results.append(Citation(
                id=make_id("FDA_IA", alert_num),
                authority="FDA",
                source_type="import_alert",
                company="",
                date=date_str(dt),
                category=cat,
                severity=sev,
                summary=alert_name[:300],
                url=detail_url or _FDA_IA_LIST,
                product_type=f"Import Alert {alert_type}",
                country="United States",
                facility_type=ftype,
                violation_details=raw[:500],
            ))
    except Exception as exc:
        logger.warning("FDA Import Alerts error: %s", exc)

    logger.info("FDA Import Alerts: %d citations", len(results))
    return results


# ---------------------------------------------------------------------------
# Scraper 4 — TGA (expanded to all product types)
# ---------------------------------------------------------------------------
_TGA_BASE = "https://www.tga.gov.au"
_TGA_SOURCES = [
    # (url, default_source_type)
    (f"{_TGA_BASE}/safety/recalls-and-other-market-actions/market-actions", "recall"),
    (f"{_TGA_BASE}/safety/compliance-and-enforcement",                       "compliance_action"),
    (f"{_TGA_BASE}/safety/safety-monitoring-and-information/safety-alerts",  "safety_alert"),
    (f"{_TGA_BASE}/news/safety-updates",                                     "safety_alert"),
]


def scrape_tga() -> list[Citation]:
    logger.info("TGA: fetching citations …")
    results: list[Citation] = []
    seen: set[str] = set()

    for page_url, default_src in _TGA_SOURCES:
        try:
            resp = http_get(page_url, timeout=25)
            if resp.status_code != 200:
                logger.warning("TGA: %s returned %s", page_url, resp.status_code)
                continue

            soup     = BeautifulSoup(resp.text, "lxml")
            articles = soup.select("article")
            # Skip first article which is usually a nav/breadcrumb stub
            page_cits = 0
            for art in articles[1:]:
                link    = (
                    art.select_one("h2 a[href]")
                    or art.select_one("h3 a[href]")
                    or art.select_one(".summary__title a[href]")
                    or art.select_one("a[href]")
                )
                time_el = art.select_one("time")
                if not link or not time_el:
                    continue
                title    = link.get_text(strip=True)
                href     = link["href"]
                item_url = href if href.startswith("http") else _TGA_BASE + href
                dt       = parse_date(time_el.get("datetime", ""))
                if dt and dt < CUTOFF_12M:
                    continue
                if item_url in seen:
                    continue
                seen.add(item_url)

                teaser  = art.select_one(".field--name-field-summary, p")
                summary = teaser.get_text(" ", strip=True) if teaser else ""
                raw     = f"{title} {summary}"

                lower_title = title.lower()
                if "recall" in lower_title:
                    src_type = "recall"
                elif "alert" in lower_title or "advisory" in lower_title:
                    src_type = "safety_alert"
                else:
                    src_type = default_src

                ftype = infer_facility_type(raw)
                cat   = keyword_classify(raw)
                sev   = infer_severity(raw, src_type)

                # Extract company from title heuristic
                company = ""
                m = re.search(r"(?:by|issued to|sponsor:?)\s+([A-Z][A-Za-z\s&,\.]{3,40})", raw)
                if m:
                    company = m.group(1).strip()

                results.append(Citation(
                    id=make_id("TGA", item_url),
                    authority="TGA", source_type=src_type,
                    company=company, date=date_str(dt),
                    category=cat, severity=sev,
                    summary=(summary or title)[:300],
                    url=item_url,
                    product_type="Therapeutic Good (AU)",
                    country="Australia",
                    facility_type=ftype,
                    violation_details=raw[:500],
                ))
                page_cits += 1

            logger.info("TGA %s: %d citations", page_url.rsplit("/", 1)[-1], page_cits)
        except Exception as exc:
            logger.warning("TGA error for %s: %s", page_url, exc)

    logger.info("TGA: %d total citations", len(results))
    return results


# ---------------------------------------------------------------------------
# Scraper 5 — MHRA (UK) — Drug Safety Update + GMP publications
# ---------------------------------------------------------------------------
_MHRA_BASE      = "https://www.gov.uk"
_MHRA_DSU_URL   = "https://www.gov.uk/drug-safety-update"
_MHRA_PUB_URLS  = [
    (
        "https://www.gov.uk/government/publications"
        "?departments%5B%5D=medicines-and-healthcare-products-regulatory-agency"
        "&keywords=gmp+non-compliance",
        "inspection_finding",
    ),
    (
        "https://www.gov.uk/government/publications"
        "?departments%5B%5D=medicines-and-healthcare-products-regulatory-agency"
        "&keywords=defective+medicine",
        "compliance_action",
    ),
]

_MHRA_RE = re.compile(
    r"vitamin|mineral|supplement|herbal|botanical|probiotic|omega|"
    r"fish oil|collagen|protein|weight loss|food supplement|nutraceutical|"
    r"gmp|good manufacturing|non.compliance|defective|falsified|counterfeit|"
    r"sterile|aseptic|parenteral|biologic|vaccine|api|drug substance|"
    r"nitrosamine|impurity|recall|withdrawal|alert|safety|device",
    re.I,
)


def scrape_mhra() -> list[Citation]:
    logger.info("MHRA: fetching citations …")
    results: list[Citation] = []
    seen: set[str] = set()

    # Drug Safety Update — paginated
    for page in range(4):
        url = f"{_MHRA_DSU_URL}?page={page}" if page else _MHRA_DSU_URL
        try:
            resp = http_get(url, timeout=25)
            if resp.status_code != 200:
                break
            soup  = BeautifulSoup(resp.text, "lxml")
            items = soup.select("li.gem-c-document-list__item")
            if not items:
                break
            stop = False
            for item in items:
                link    = item.select_one("a[href]")
                time_el = item.select_one("time")
                desc_el = item.select_one(".gem-c-document-list__item-description, p")
                if not link:
                    continue
                title    = link.get_text(strip=True)
                href     = link["href"]
                item_url = href if href.startswith("http") else _MHRA_BASE + href
                date_raw = time_el.get("datetime", "") if time_el else ""
                dt       = parse_date(date_raw)
                if dt and dt < CUTOFF_12M:
                    stop = True
                    break
                desc = desc_el.get_text(" ", strip=True) if desc_el else ""
                raw  = f"{title} {desc}"
                if not _MHRA_RE.search(raw):
                    continue
                if item_url in seen:
                    continue
                seen.add(item_url)
                ftype = infer_facility_type(raw)
                cat   = keyword_classify(raw)
                sev   = infer_severity(raw, "compliance_action")
                results.append(Citation(
                    id=make_id("MHRA_DSU", item_url),
                    authority="MHRA", source_type="compliance_action",
                    company="", date=date_str(dt),
                    category=cat, severity=sev,
                    summary=(desc or title)[:300],
                    url=item_url,
                    product_type="Medicine / Supplement (UK)",
                    country="United Kingdom",
                    facility_type=ftype,
                    violation_details=raw[:500],
                ))
            if stop:
                break
            time.sleep(0.4)
        except Exception as exc:
            logger.warning("MHRA DSU page %d error: %s", page, exc)
            break

    # GMP non-compliance + defective medicines publications
    for pub_url, src_type in _MHRA_PUB_URLS:
        try:
            resp = http_get(pub_url, timeout=25)
            if resp.status_code != 200:
                continue
            soup  = BeautifulSoup(resp.text, "lxml")
            items = soup.select("li.gem-c-document-list__item")
            for item in items:
                link    = item.select_one("a[href]")
                time_el = item.select_one("time")
                desc_el = item.select_one(".gem-c-document-list__item-description, p")
                if not link:
                    continue
                title    = link.get_text(strip=True)
                href     = link["href"]
                item_url = href if href.startswith("http") else _MHRA_BASE + href
                date_raw = time_el.get("datetime", "") if time_el else ""
                dt       = parse_date(date_raw)
                if dt and dt < CUTOFF_12M:
                    continue
                desc = desc_el.get_text(" ", strip=True) if desc_el else ""
                raw  = f"{title} {desc}"
                if item_url in seen:
                    continue
                seen.add(item_url)
                ftype = infer_facility_type(raw)
                cat   = keyword_classify(raw)
                # Reclassify gov.uk/guidance URLs: they are guidance pages, not inspection findings.
                # MHRA actual GMP non-compliance letters live at /government/publications/ with
                # enforcement titles; guidance pages use /guidance/ URL segment.
                mhra_src_type = src_type
                if (mhra_src_type == "inspection_finding"
                        and ("/guidance/" in item_url
                             or _GUIDANCE_TITLE_RE.search(raw))
                        and not _ENFORCEMENT_SIGNAL_RE.search(raw)):
                    mhra_src_type = "guidance"
                results.append(Citation(
                    id=make_id("MHRA_GMP", item_url),
                    authority="MHRA", source_type=mhra_src_type,
                    company="", date=date_str(dt),
                    category=cat,
                    severity=infer_severity(raw, src_type),
                    summary=(desc or title)[:300],
                    url=item_url,
                    product_type="Medicine / GMP (UK)",
                    country="United Kingdom",
                    facility_type=ftype,
                    violation_details=raw[:500],
                ))
        except Exception as exc:
            logger.warning("MHRA pubs error for %s: %s", pub_url, exc)

    logger.info("MHRA: %d citations", len(results))
    return results


# ---------------------------------------------------------------------------
# Scraper 6 — EFSA (paginated publications)
# ---------------------------------------------------------------------------
_EFSA_BASE = "https://www.efsa.europa.eu"

_EFSA_RE = re.compile(
    r"vitamin|mineral|supplement|botanical|herbal|probiotic|health claim|"
    r"omega.?3|fish oil|collagen|amino acid|astaxanthin|melatonin|coenzyme|"
    r"glucosamine|l.carnitine|selenium|zinc|magnesium|iron|calcium|copper|"
    r"chromium|iodine|novel food|cannabis|hemp|cbd|"
    r"contaminant|heavy metal|mycotoxin|aflatoxin|cadmium|lead|arsenic|"
    r"pesticide|food additive|food enzyme|feed additive|"
    r"tolerable upper|maximum level|safe intake|upper intake|dietary intake|"
    r"nutrient|safety opinion|nda|risk assessment",
    re.I,
)

_EFSA_DATE_RE = re.compile(
    r"\b(\d{1,2})\s+(January|February|March|April|May|June|"
    r"July|August|September|October|November|December)\s+(\d{4})\b",
    re.I,
)


def scrape_efsa() -> list[Citation]:
    logger.info("EFSA: fetching publications …")
    results: list[Citation] = []
    seen: set[str] = set()

    for page in range(40):
        url = (
            f"{_EFSA_BASE}/en/publications"
            if page == 0
            else f"{_EFSA_BASE}/en/publications?page={page}"
        )
        try:
            resp = http_get(url, timeout=25)
            if resp.status_code != 200:
                break
            soup  = BeautifulSoup(resp.text, "lxml")
            items = soup.select(".views-row")
            if not items:
                break

            stop = False
            for item in items:
                link = item.select_one("a[href]")
                if not link:
                    continue
                title    = link.get_text(strip=True)
                href     = link["href"]
                item_url = href if href.startswith("http") else _EFSA_BASE + href

                item_text = item.get_text(" ", strip=True)
                m         = _EFSA_DATE_RE.search(item_text)
                date_raw  = m.group(0) if m else ""
                dt        = parse_date(date_raw)
                if dt and dt < CUTOFF_12M:
                    stop = True
                    break

                raw = f"{title} {item_text[:400]}"
                if not _EFSA_RE.search(raw):
                    continue
                if item_url in seen:
                    continue
                seen.add(item_url)

                type_m   = re.match(
                    r"^(Scientific Opinion|Statement|Technical Report|"
                    r"Event report|Scientific Report|News|Guidance)",
                    item_text,
                )
                pub_type = type_m.group(1) if type_m else "Scientific Output"
                # EFSA publications are scientific outputs, not enforcement findings.
                # Map each pub type to the correct source_type.
                if "News" in pub_type:
                    src_type = "compliance_action"
                elif "Guidance" in pub_type:
                    src_type = "guidance"
                else:
                    # Scientific Opinion, Statement, Technical Report, Event report, etc.
                    src_type = "scientific_opinion"
                cat      = keyword_classify(raw)
                sev      = infer_severity(raw, src_type)
                results.append(Citation(
                    id=make_id("EFSA", item_url),
                    authority="EFSA", source_type=src_type,
                    company="", date=date_str(dt),
                    category=cat, severity=sev,
                    summary=title[:300],
                    url=item_url,
                    product_type=f"EFSA {pub_type}",
                    country="EU",
                    facility_type="Supplement / Nutraceutical",
                    violation_details=raw[:500],
                ))

            if stop:
                logger.info("EFSA: hit 12-month cutoff at page %d", page)
                break
            time.sleep(0.4)
        except Exception as exc:
            logger.warning("EFSA page %d error: %s", page, exc)
            break

    logger.info("EFSA: %d citations", len(results))
    return results


# ---------------------------------------------------------------------------
# Scraper 7 — BfR (German Federal Institute for Risk Assessment)
# ---------------------------------------------------------------------------
_BFR_BASE = "https://www.bfr.bund.de"

_BFR_RE = re.compile(
    r"vitamin|mineral|supplement|herbal|botanical|probiotic|omega.?3|"
    r"fish oil|collagen|protein|creatine|weight loss|food supplement|"
    r"nutraceutical|contaminant|heavy metal|mycotoxin|food safety|"
    r"risk assessment|maximum|tolerable|upper level|safety|warning|recall|"
    r"pesticide|additive|flavouring",
    re.I,
)

_BFR_SOURCES = [
    f"{_BFR_BASE}/en/press_information.html",
    f"{_BFR_BASE}/en/publications.html",
    f"{_BFR_BASE}/en/",
]


def scrape_bfr() -> list[Citation]:
    logger.info("BfR: fetching citations …")
    results: list[Citation] = []
    seen: set[str] = set()

    for source_url in _BFR_SOURCES:
        try:
            resp = http_get(source_url, timeout=25)
            if resp.status_code != 200:
                logger.warning("BfR: %s returned %s", source_url, resp.status_code)
                continue
            soup  = BeautifulSoup(resp.text, "lxml")
            items = soup.select("article")
            logger.info("BfR %s: %d articles", source_url.rsplit("/", 1)[-1] or "home", len(items))
            for item in items:
                link = item.select_one("a[href]")
                if not link:
                    continue
                title    = re.sub(r"^Read", "", link.get_text(strip=True)).strip()
                href     = link["href"]
                item_url = href if href.startswith("http") else _BFR_BASE + href
                body_text = item.get_text(" ", strip=True)
                date_raw  = ""
                time_el   = item.select_one("time")
                if time_el:
                    date_raw = time_el.get("datetime", time_el.get_text(strip=True))
                else:
                    dm = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", body_text)
                    if dm:
                        date_raw = dm.group(1)
                dt = parse_date(date_raw)
                if dt and dt < CUTOFF_12M:
                    continue
                raw = f"{title} {body_text[:400]}"
                if not _BFR_RE.search(raw):
                    continue
                if item_url in seen:
                    continue
                seen.add(item_url)
                cat_m     = re.search(r"Category\s+([A-Za-z /]+?)(?:\d|$|\n)", body_text)
                src_label = cat_m.group(1).strip() if cat_m else "BfR Publication"
                # BfR publications are risk assessments and opinions, not inspection findings.
                if any(w in src_label.lower() for w in ["press", "faq", "comms"]):
                    src_type = "compliance_action"
                elif any(w in src_label.lower() for w in ["consultation", "opinion", "assessment", "review"]):
                    src_type = "scientific_opinion"
                elif "guidance" in src_label.lower():
                    src_type = "guidance"
                else:
                    # Default for BfR publications: scientific opinion / risk assessment
                    src_type = "scientific_opinion"
                cat   = keyword_classify(raw)
                sev   = infer_severity(raw, src_type)
                ftype = infer_facility_type(raw)
                results.append(Citation(
                    id=make_id("BFR", item_url),
                    authority="BfR", source_type=src_type,
                    company="", date=date_str(dt),
                    category=cat, severity=sev,
                    summary=title[:300],
                    url=item_url,
                    product_type="Food Supplement / Food Safety (DE)",
                    country="Germany",
                    facility_type=ftype,
                    violation_details=raw[:500],
                ))
        except Exception as exc:
            logger.warning("BfR error for %s: %s", source_url, exc)

    logger.info("BfR: %d citations", len(results))
    return results


# ---------------------------------------------------------------------------
# Best-text selector for post-enrichment classification
# ---------------------------------------------------------------------------

def get_best_classification_text(c: Citation) -> str:
    """
    Return the richest available text for deterministic classification.
    Prefer enriched_text when meaningful; fall back to raw listing text.
    """
    if c.enrichment_status in ("success", "cached") and len(c.enriched_text or "") > 250:
        return c.enriched_text
    return c.raw_listing_summary or c.violation_details or c.summary or ""


# ---------------------------------------------------------------------------
# Multi-dimensional severity computation
# ---------------------------------------------------------------------------

# Failure modes that indicate direct patient/product safety risk
_HIGH_RISK_FAILURE_MODES = frozenset({
    "sterility_assurance",
    "contamination_microbial",
    "contamination_chemical",
    "contamination_foreign",
    "adverse_event_cluster",
    "recall_mandatory",
    "out_of_specification",
})

_CRITICAL_RE = re.compile(
    r"\brecall\b|injunction|seizure|class.?i\b(?!.*class.?ii)|death|fatal|"
    r"counterfeit|falsified|carcinogen|genotoxic",
    re.I,
)
_CONTRACT_MFG_RE = re.compile(r"contract manufactur|cmo|cdmo|outsourc", re.I)
_SEV_ORDER = ["low", "medium", "high", "critical"]


def compute_multidim_severity(c: Citation) -> dict:
    """
    Return severity-dimension fields from deterministic rules. No AI calls.
    Original c.severity field is preserved; new fields are additive.
    """
    text = f"{c.summary} {c.violation_details} {c.enriched_text}"
    tl   = text.lower()

    # regulatory_severity
    if c.source_type == "warning_letter":
        reg_sev = "critical" if _CRITICAL_RE.search(text) else "high"
    elif c.source_type in ("drug_enforcement", "device_enforcement", "food_enforcement"):
        if "class i" in tl and "class ii" not in tl:
            reg_sev = "critical"
        elif "class ii" in tl:
            reg_sev = "high"
        else:
            reg_sev = "medium"
    elif c.source_type == "import_alert":
        reg_sev = "high" if "automatic detention" in tl else "medium"
    elif c.source_type == "recall":
        if _CRITICAL_RE.search(text) or ("class i" in tl and "class ii" not in tl):
            reg_sev = "critical"
        elif "class ii" in tl:
            reg_sev = "high"
        else:
            reg_sev = "medium"
    elif c.source_type == "safety_alert":
        reg_sev = "high" if any(w in tl for w in ("urgent", "immediate", "serious")) else "medium"
    else:
        reg_sev = c.severity or "medium"

    # operational_severity: elevate one level for contract manufacturers
    op_sev = reg_sev
    if _CONTRACT_MFG_RE.search(f"{c.company} {c.facility_type}"):
        idx = _SEV_ORDER.index(reg_sev)
        op_sev = _SEV_ORDER[min(idx + 1, len(_SEV_ORDER) - 1)]

    # inspection_risk
    if c.source_type == "warning_letter":
        insp_risk = "immediate"
    elif c.source_type in ("import_alert", "inspection_finding", "compliance_action"):
        insp_risk = "elevated"
    else:
        insp_risk = "standard"

    # market_relevance_au
    if c.authority in ("TGA", "MHRA", "BfR"):
        mkt_au = "direct"
    elif c.authority == "EFSA":
        mkt_au = "reference"
    elif c.facility_type == "Supplement / Nutraceutical":
        mkt_au = "indirect"
    else:
        mkt_au = "reference"

    # ── Priority ──────────────────────────────────────────────────────────
    # "low detail" = no specific category OR failure mode identified —
    # prevents weak/boilerplate records from inheriting high priority.
    is_low_detail = (
        c.primary_gmp_category in ("Other / Insufficient Detail", "")
        and c.failure_mode in ("insufficient_detail", "")
    )
    au_relevant = mkt_au in ("direct", "indirect")

    # P1 trigger A: high-confidence safety-critical failure mode.
    # recall_mandatory on a warning_letter is a legal classification term, not
    # a product-safety event — only treat it as high-risk when a pharma-safety
    # signal is also present in the text.
    _is_wl_recall_mandatory = (
        c.failure_mode == "recall_mandatory"
        and c.source_type == "warning_letter"
    )
    has_critical_failure = (
        c.failure_mode in _HIGH_RISK_FAILURE_MODES
        and c.failure_mode_confidence >= 0.7
        and not (_is_wl_recall_mandatory and not _PHARMA_SAFETY_RE.search(text))
    )
    # P1 trigger B: critical-classification recall/enforcement action
    is_critical_recall = (
        reg_sev == "critical"
        and c.source_type in (
            "recall", "drug_enforcement", "device_enforcement", "food_enforcement",
        )
    )

    if has_critical_failure or is_critical_recall:
        priority = "P1"
    elif op_sev in ("high", "critical") and au_relevant and not is_low_detail:
        # High operational impact AND directly AU-relevant product/authority
        priority = "P1"
    elif reg_sev == "critical":
        priority = "P2"  # critical reg severity but detail or AU relevance is limited
    elif reg_sev == "high" and not is_low_detail:
        priority = "P2"  # solid regulatory signal with meaningful classification
    elif (reg_sev == "high" or insp_risk == "immediate") and is_low_detail:
        priority = "P3"  # WL/immediate-risk source but no actionable detail
    elif reg_sev == "medium" and not is_low_detail:
        priority = "P3"
    else:
        priority = "P4"

    # severity_reason
    parts: list[str] = []
    src_label = {
        "warning_letter": "Warning Letter",
        "import_alert": "Import Alert",
        "drug_enforcement": "Drug Enforcement",
        "device_enforcement": "Device Enforcement",
        "food_enforcement": "Food Enforcement",
        "recall": "Recall",
        "compliance_action": "Compliance Action",
        "inspection_finding": "Inspection Finding",
        "safety_alert": "Safety Alert",
    }.get(c.source_type, c.source_type)
    parts.append(f"{c.authority} {src_label}")
    if c.facility_type and c.facility_type != "General Pharma":
        parts.append(c.facility_type)
    if mkt_au == "direct":
        parts.append("direct AU relevance")
    elif mkt_au == "indirect":
        parts.append("indirect AU relevance")
    if has_critical_failure:
        parts.append(f"{c.failure_mode} (conf={c.failure_mode_confidence:.2f})")
    if is_low_detail:
        parts.append("low detail")

    return {
        "regulatory_severity": reg_sev,
        "operational_severity": op_sev,
        "inspection_risk": insp_risk,
        "market_relevance_au": mkt_au,
        "priority": priority,
        "severity_reason": "; ".join(parts)[:200],
    }


# ---------------------------------------------------------------------------
# Low-detail priority cap
# Applied after AI fields are settled (post Step 5).
# Records with failure_mode=insufficient_detail and no AI confidence should
# not inherit P1/P2 from source-type severity alone.
# ---------------------------------------------------------------------------

# Explicit product-safety terms that justify keeping a low-detail record at P1/P2
# even without a confirmed failure mode.
_LOW_DETAIL_HIGH_RISK_RE = re.compile(
    r"\bsterility\b|\bmicrobial\b|\bcontamination\b"
    r"|\bbenzene\b|\bnitrosamine\b|\bndma\b|\bndea\b|\bnmba\b"
    r"|\bheavy metal|\btoxic element"
    r"|\bundeclared drug\b|\bsildenafil\b|\btadalafil\b|\bsibutramine\b"
    r"|\bsarms\b|\bdmaa\b"
    r"|class\s+i\s+recall"
    r"|\bserious adverse event\b|\bpatient harm\b|\bdeath\b"
    r"|\bhospitali[sz]",
    re.I,
)


def fix_source_types(citations: list[Citation]) -> list[Citation]:
    """
    Post-scrape pass: correct source_type for guidance/scientific records mislabelled
    as inspection_finding.

    Affects records where:
      - source_type is inspection_finding
      - URL or title/summary indicates guidance or scientific-opinion content
      - No enforcement signal (non-compliance, deficiency, recall) in the text

    Mapping:
      - EFSA records → scientific_opinion (always; EFSA does not issue inspection findings)
      - BfR records → scientific_opinion (BfR issues risk assessments, not inspections)
      - MHRA gov.uk/guidance/* URLs → guidance
      - Other guidance-pattern content → guidance
    """
    result: list[Citation] = []
    fixed = 0
    for c in citations:
        if c.source_type != "inspection_finding":
            result.append(c)
            continue

        new_type = None
        if c.authority == "EFSA":
            new_type = "scientific_opinion"
        elif c.authority == "BfR":
            new_type = "scientific_opinion"
        elif c.authority == "MHRA" and "/guidance/" in (c.url or ""):
            new_type = "guidance"
        elif _is_guidance_source(c) and not _is_enforcement_source(c):
            new_type = "guidance"

        if new_type:
            fixed += 1
            updated = asdict(c)
            updated["source_type"] = new_type
            result.append(Citation(**updated))
        else:
            result.append(c)

    if fixed:
        logger.info(
            "fix_source_types: corrected %d inspection_finding records "
            "(EFSA/BfR → scientific_opinion, guidance URLs → guidance)",
            fixed,
        )
    return result


def apply_low_detail_priority_cap(citations: list[Citation]) -> list[Citation]:
    """
    Cap low-detail records at P3 unless explicit high-risk evidence is present.

    A record is subject to the cap when ALL of:
      - failure_mode == "insufficient_detail"
      - failure_mode_confidence <= 0.10
      - no accepted AI: decision_summary blank AND classification_confidence < 0.5

    Exceptions — record may remain at P1/P2 if ANY of:
      1. AI accepted: classification_confidence >= 0.7 AND decision_summary populated
      2. Source text contains an explicit high-risk product-safety term
         (sterility, microbial, contamination, NDMA, benzene, heavy metal, …)
      3. source_type in enforcement/recall AND operational_severity in (high, critical)
      4. market_relevance_au == "direct" AND regulatory_severity == "high"

    Generic legal boilerplate (adulterated, misbranded, CGMP, violation,
    seizure, injunction) and warning_letter source_type alone do NOT override.
    """
    result: list[Citation] = []
    for c in citations:
        if c.priority not in ("P1", "P2"):
            result.append(c)
            continue

        # Only apply to true low-detail records
        is_low_detail = (
            c.failure_mode == "insufficient_detail"
            and c.failure_mode_confidence <= 0.10
        )
        if not is_low_detail:
            result.append(c)
            continue

        has_ai = (
            c.classification_confidence >= 0.5
            and bool(c.decision_summary)
        )
        if has_ai:
            result.append(c)
            continue

        # Check exceptions against full context text
        context = get_context_text(c)

        # Exception 1: high-confidence AI (>=0.7 already covered by has_ai above,
        # but explicit here for clarity)
        ai_exception = (
            c.classification_confidence >= 0.7
            and bool(c.decision_summary)
        )

        # Exception 2: explicit high-risk safety terms in source text
        text_exception = bool(_LOW_DETAIL_HIGH_RISK_RE.search(context))

        # Exception 3: enforcement/recall source with high/critical operational severity
        enforcement_exception = (
            c.source_type in (
                "drug_enforcement", "device_enforcement",
                "food_enforcement", "recall",
            )
            and c.operational_severity in ("high", "critical")
        )

        # Exception 4: direct AU relevance + high regulatory severity
        au_exception = (
            c.market_relevance_au == "direct"
            and c.regulatory_severity == "high"
        )

        if ai_exception or text_exception or enforcement_exception or au_exception:
            result.append(c)
            continue

        # Cap to P3
        updated = asdict(c)
        reason = updated.get("severity_reason", c.severity_reason or "")
        updated["priority"] = "P3"
        updated["severity_reason"] = (reason + "; capped P3 — low detail, no safety signal")[:200]
        result.append(Citation(**updated))

    return result


# ---------------------------------------------------------------------------
# Classification trust priority cap
# ---------------------------------------------------------------------------

# Source types that are informational/scientific — never enforcement findings
_NON_ENFORCEMENT_SOURCE_TYPES = frozenset({
    "guidance", "scientific_opinion", "regulatory_update", "consultation",
})


def apply_classification_trust_cap(citations: list[Citation]) -> list[Citation]:
    """
    Cap priority based on classification trustworthiness.

    Rules (applied in order; first match wins):
      1. Non-enforcement source types (guidance, scientific_opinion, regulatory_update):
         cap at P4 unless the text contains explicit high-risk safety terms.
         Rationale: guidance documents are not enforcement findings; P1/P2 is misleading.

      2. classification_status == "unconfirmed" AND not a strong enforcement source:
         cap at P3. The category cannot be trusted for top-risk reporting.

    Exemptions (never capped):
      - Records already at P3 or P4 (cap would have no effect)
      - Records where _LOW_DETAIL_HIGH_RISK_RE fires (explicit safety signal in text)
      - Strong enforcement sources: warning_letter, drug_enforcement, device_enforcement,
        recall, import_alert (these have source-type gravity that overrides unconfirmed cat.)
    """
    _STRONG_ENFORCEMENT = frozenset({
        "warning_letter", "drug_enforcement", "device_enforcement",
        "food_enforcement", "recall", "import_alert",
    })
    result: list[Citation] = []
    capped_non_enf = 0
    capped_unconf  = 0

    for c in citations:
        if c.priority in ("P3", "P4", ""):
            result.append(c)
            continue

        context = get_context_text(c)
        has_safety = bool(_LOW_DETAIL_HIGH_RISK_RE.search(context))

        # Rule 1: non-enforcement source type
        if c.source_type in _NON_ENFORCEMENT_SOURCE_TYPES:
            if has_safety:
                result.append(c)
                continue
            new_pri = "P4"
            capped_non_enf += 1
            updated = asdict(c)
            reason  = updated.get("severity_reason", c.severity_reason or "")
            updated["priority"] = new_pri
            updated["severity_reason"] = (
                reason + f"; capped {new_pri} — non-enforcement source type ({c.source_type})"
            )[:200]
            result.append(Citation(**updated))
            continue

        # Rule 2: unconfirmed classification + not strong enforcement
        if (c.classification_status == "unconfirmed"
                and c.source_type not in _STRONG_ENFORCEMENT
                and not has_safety):
            new_pri = "P3"
            capped_unconf += 1
            updated = asdict(c)
            reason  = updated.get("severity_reason", c.severity_reason or "")
            updated["priority"] = new_pri
            updated["severity_reason"] = (
                reason + "; capped P3 — unconfirmed category, non-enforcement source"
            )[:200]
            result.append(Citation(**updated))
            continue

        result.append(c)

    total_capped = capped_non_enf + capped_unconf
    if total_capped:
        logger.info(
            "Classification trust cap: %d records capped "
            "(%d non-enforcement source, %d unconfirmed category)",
            total_capped, capped_non_enf, capped_unconf,
        )
    return result


# ---------------------------------------------------------------------------
# Recurrence counts and risk direction
# ---------------------------------------------------------------------------

def compute_recurrence(citations: list[Citation]) -> list[Citation]:
    """
    Post-classification pass: compute 90-day recurrence counts and
    per-company signal direction. Purely deterministic.
    """
    cutoff_90_str = CUTOFF_90D.strftime("%Y-%m-%d")
    cutoff_45_str = (NOW - timedelta(days=45)).strftime("%Y-%m-%d")
    cutoff_60_str = (NOW - timedelta(days=60)).strftime("%Y-%m-%d")

    # Build 90-day lookup tables
    company_dates:  dict[str, list[str]] = {}
    cat_counts_90:  dict[str, int] = {}
    fm_counts_90:   dict[str, int] = {}
    cat_recent_45:  dict[str, int] = {}
    cat_prior_45:   dict[str, int] = {}

    for c in citations:
        if not c.date or c.date < cutoff_90_str:
            continue
        co_key = (c.company or "").strip().lower()
        if co_key:
            company_dates.setdefault(co_key, []).append(c.date)
        if c.primary_gmp_category:
            cat_counts_90[c.primary_gmp_category] = cat_counts_90.get(c.primary_gmp_category, 0) + 1
            if c.date >= cutoff_45_str:
                cat_recent_45[c.primary_gmp_category] = cat_recent_45.get(c.primary_gmp_category, 0) + 1
            else:
                cat_prior_45[c.primary_gmp_category] = cat_prior_45.get(c.primary_gmp_category, 0) + 1
        if c.failure_mode:
            fm_counts_90[c.failure_mode] = fm_counts_90.get(c.failure_mode, 0) + 1

    def _reg_pressure(cat: str) -> str:
        if not cat:
            return "stable"
        r = cat_recent_45.get(cat, 0)
        p = cat_prior_45.get(cat, 0)
        if p == 0:
            return "stable"
        ratio = r / p
        return "increasing" if ratio > 1.3 else ("decreasing" if ratio < 0.7 else "stable")

    result: list[Citation] = []
    for c in citations:
        co_key = (c.company or "").strip().lower()
        company_cit_dates = company_dates.get(co_key, [])

        rec_company = max(0, len(company_cit_dates) - 1) if co_key else 0
        rec_cat = max(0, cat_counts_90.get(c.primary_gmp_category, 0) - 1) if c.primary_gmp_category else 0
        rec_fm  = max(0, fm_counts_90.get(c.failure_mode, 0) - 1)           if c.failure_mode          else 0

        if co_key and len(company_cit_dates) >= 2:
            sig_dir = "escalating"
        elif co_key and c.date and c.date < cutoff_60_str:
            sig_dir = "resolving"
        else:
            sig_dir = "holding"

        updated = asdict(c)
        updated.update({
            "recurrence_count_company_90d":      rec_company,
            "recurrence_count_category_90d":     rec_cat,
            "recurrence_count_failure_mode_90d": rec_fm,
            "signal_direction":                  sig_dir,
            "regulatory_pressure":               _reg_pressure(c.primary_gmp_category),
        })

        # Escalate to P1 for companies with repeat violations in 90 days
        if rec_company >= 2 and c.regulatory_severity == "high" and c.priority in ("P2", "P3"):
            updated["priority"] = "P1"
            reason = updated.get("severity_reason", c.severity_reason or "")
            updated["severity_reason"] = (reason + "; repeat company recurrence (escalated to P1)")[:200]

        result.append(Citation(**updated))
    return result


# ---------------------------------------------------------------------------
# Audit report
# ---------------------------------------------------------------------------

_REQUIRED_INTEL_FIELDS = (
    "primary_gmp_category", "regulatory_severity", "operational_severity",
    "inspection_risk", "market_relevance_au", "priority",
)


def _build_audit_report(
    citations: list[Citation],
    failed: list[str],
    tracker: Optional[AiCallTracker] = None,
    ai_import_error: str = "",
    ai_pass_attempted: bool = False,
    ai_pass_completed: bool = False,
    sample_stats: Optional[dict] = None,
) -> dict:
    from collections import Counter
    enrich_status = Counter(c.enrichment_status for c in citations)
    priority_dist = Counter(c.priority          for c in citations if c.priority)
    cat_dist      = Counter(c.primary_gmp_category for c in citations if c.primary_gmp_category)
    fm_dist       = Counter(c.failure_mode       for c in citations if c.failure_mode)
    pres_dist     = Counter(c.regulatory_pressure for c in citations if c.regulatory_pressure)
    ai_total      = sum(1 for c in citations if c.ai_summary)
    ai_high_conf  = sum(1 for c in citations if c.ai_confidence >= 0.7)
    missing_intel = sum(
        1 for c in citations
        if any(not getattr(c, f, "") for f in _REQUIRED_INTEL_FIELDS)
    )
    raw_populated = sum(1 for c in citations if c.raw_listing_summary)
    ds_populated  = sum(1 for c in citations if c.decision_summary)
    ra_populated  = sum(1 for c in citations if c.recommended_action)
    ai_accepted   = tracker.ai_results_accepted if tracker else ai_total
    ai_discarded  = tracker.ai_results_discarded_low_confidence if tracker else 0

    # P1/P2 company+failure_mode clusters — using get_entity_label so blank-company
    # records (TGA/MHRA notices, import alerts) are identifiable in the output.
    cluster_counts: dict[tuple, dict] = {}
    for c in citations:
        if c.priority not in ("P1", "P2"):
            continue
        label = get_entity_label(c)
        key   = (label, c.authority, c.source_type, c.failure_mode or "")
        if key not in cluster_counts:
            cluster_counts[key] = {"P1": 0, "P2": 0}
        cluster_counts[key][c.priority] += 1

    clusters = [
        {
            "entity_label":  k[0],
            "authority":     k[1],
            "source_type":   k[2],
            "failure_mode":  k[3],
            "count_p1":      v["P1"],
            "count_p2":      v["P2"],
            "total":         v["P1"] + v["P2"],
        }
        for k, v in cluster_counts.items()
        if v["P1"] + v["P2"] >= 2
    ]
    clusters.sort(key=lambda x: -x["total"])

    # ── Citation clustering statistics ────────────────────────────────────
    from collections import defaultdict as _dd2
    _cluster_groups: dict[str, list[Citation]] = _dd2(list)
    for c in citations:
        if c.cluster_id and c.cluster_size > 1:
            _cluster_groups[c.cluster_id].append(c)

    total_clusters     = len(_cluster_groups)
    grouped_records    = sum(len(v) for v in _cluster_groups.values())
    p1_cluster_count   = sum(
        1 for v in _cluster_groups.values()
        if any(m.priority == "P1" for m in v)
    )
    p2_cluster_count   = sum(
        1 for v in _cluster_groups.values()
        if any(m.priority == "P2" for m in v) and not any(m.priority == "P1" for m in v)
    )

    top_clusters_list = []
    for cid, members in sorted(_cluster_groups.items(), key=lambda x: -len(x[1]))[:10]:
        primary = next((m for m in members if m.cluster_primary), members[0])
        dates   = sorted(m.date for m in members if m.date)
        top_clusters_list.append({
            "cluster_id":   cid,
            "size":         len(members),
            "label":        primary.cluster_label,
            "authority":    primary.authority,
            "source_type":  primary.source_type,
            "failure_mode": primary.failure_mode or "",
            "priority":     primary.cluster_priority or primary.priority or "",
            "date_range":   f"{dates[0][:10]}–{dates[-1][:10]}" if len(dates) >= 2 else (dates[0][:10] if dates else ""),
            "p1_count":     sum(1 for m in members if m.priority == "P1"),
            "p2_count":     sum(1 for m in members if m.priority == "P2"),
        })

    return {
        "generated_at":    NOW.isoformat(),
        "total_citations": len(citations),
        "enrichment": {
            "success":        enrich_status.get("success",        0),
            "cached":         enrich_status.get("cached",         0),
            "failed":         enrich_status.get("failed",         0),
            "not_applicable": enrich_status.get("not_applicable", 0),
        },
        "ai_pass": {
            "attempted":       ai_pass_attempted,
            "completed":       ai_pass_completed,
            "import_error":    ai_import_error or None,
            "total_with_ai":   ai_total,
            "confidence_high": ai_high_conf,
            "confidence_low":  ai_total - ai_high_conf,
        },
        "ai_call_tracking":     tracker.as_dict() if tracker else {},
        "sample":               sample_stats or {},
        "data_quality": {
            "missing_required_intelligence_fields_count": missing_intel,
            "raw_listing_summary_populated":              raw_populated,
            "raw_listing_summary_blank":                  len(citations) - raw_populated,
            "decision_summary_populated_count":           ds_populated,
            "recommended_action_populated_count":         ra_populated,
            "ai_results_accepted_count":                  ai_accepted,
            "ai_results_discarded_low_confidence_count":  ai_discarded,
        },
        "priority_breakdown":   dict(priority_dist),
        "top_categories":       dict(cat_dist.most_common(10)),
        "top_failure_modes":    dict(fm_dist.most_common(5)),
        "regulatory_pressure":  dict(pres_dist),
        "p1p2_clusters":        clusters[:20],
        "clustering": {
            "total_clusters":          total_clusters,
            "multi_record_clusters":   total_clusters,
            "grouped_records_count":   grouped_records,
            "p1_clusters":             p1_cluster_count,
            "p2_clusters":             p2_cluster_count,
            "top_10_clusters":         top_clusters_list,
        },
        "failed_sources":       failed,
    }


def _print_audit_report(audit: dict) -> None:
    SEP = "=" * 65
    print(f"\n{SEP}")
    print("  CITATION ENRICHMENT AUDIT")
    print(f"  {audit['generated_at']}")
    print(SEP)
    print(f"  Total citations:  {audit['total_citations']}")

    e = audit["enrichment"]
    total_attempted = e["success"] + e["cached"] + e["failed"]
    pct = lambda n: f"{round(n / total_attempted * 100, 1)}%" if total_attempted else "—"
    print(f"\n  Enrichment (FDA WL + TGA):")
    print(f"    attempted:       {total_attempted}")
    print(f"    success:         {e['success']:>4}  ({pct(e['success'])})")
    print(f"    cached:          {e['cached']:>4}  ({pct(e['cached'])})")
    print(f"    failed:          {e['failed']:>4}  ({pct(e['failed'])})")
    print(f"    not applicable:  {e['not_applicable']:>4}")

    ai = audit["ai_pass"]
    print(f"\n  AI intelligence pass:")
    print(f"    attempted:       {ai.get('attempted', False)}")
    print(f"    completed:       {ai.get('completed', False)}")
    if ai.get("import_error"):
        print(f"    import_error:    {ai['import_error']}")
    print(f"    total with AI:   {ai['total_with_ai']}")
    print(f"    confidence ≥0.7: {ai['confidence_high']}")
    print(f"    confidence <0.7: {ai['confidence_low']}")

    ct = audit.get("ai_call_tracking", {})
    if ct:
        print(f"\n  AI call tracking:")
        print(f"    total_ai_calls:              {ct.get('total_ai_calls', 0):>4}  ← MUST BE 0 in --no-ai/--dry-run")
        print(f"    scraping_phase_ai_calls:     {ct.get('scraping_phase_ai_calls', 0):>4}  ← MUST ALWAYS BE 0")
        print(f"    enrichment_phase_ai_calls:   {ct.get('enrichment_phase_ai_calls', 0):>4}")
        print(f"    intelligence_phase_ai_calls: {ct.get('intelligence_phase_ai_calls', 0):>4}")
        print(f"    ai_eligible_count:           {ct.get('ai_eligible_count', 0):>4}")
        print(f"    ai_skipped_cached:           {ct.get('ai_skipped_cached', 0):>4}")
        print(f"    ai_skipped_due_to_no_ai:     {ct.get('ai_skipped_due_to_no_ai', 0):>4}")
        print(f"    ai_skipped_due_to_max_calls: {ct.get('ai_skipped_due_to_max_calls', 0):>4}")
        print(f"    ai_skipped_due_to_sample:    {ct.get('ai_skipped_due_to_sample', 0):>4}")
        if ct.get("ai_queue_p1_count") is not None:
            print(f"\n  AI queue composition (after filtering + sorting):")
            print(f"    tier 1 — P1 no-AI with enriched text:  {ct.get('ai_queue_p1_count', 0):>4}")
            print(f"    tier 2 — P2 no-AI with enriched text:  {ct.get('ai_queue_p2_count', 0):>4}")
            print(f"    tier 3 — cluster primary, no AI:       {ct.get('ai_queue_cluster_primary_count', 0):>4}")
            print(f"    tier 4 — AU-relevant P1/P2:            {ct.get('ai_queue_au_relevant_count', 0):>4}")
            print(f"    tier 5 — warning letters enriched:     {ct.get('ai_queue_warning_letter_count', 0):>4}")
            print(f"    tier 6 — lower priority / fallback:    {ct.get('ai_queue_low_priority_count', 0):>4}")
        abt = ct.get("ai_accepted_by_tier", {})
        dbt = ct.get("ai_discarded_by_tier", {})
        if abt or dbt:
            print(f"\n  Accepted / discarded by tier:")
            tiers = {"t1":"P1 enriched","t2":"P2 enriched","t3":"cluster primary",
                     "t4":"AU-relevant","t5":"warning letter","t6":"low priority"}
            print(f"    {'tier':<20}  {'accept':>7}  {'discard':>8}")
            for k, lbl in tiers.items():
                a = abt.get(k, 0); d = dbt.get(k, 0)
                if a or d:
                    print(f"    {lbl:<20}  {a:>7}  {d:>8}")

    ss = audit.get("sample", {})
    if ss:
        print(f"\n  Sample ({ss.get('sample_strategy','?')}):")
        print(f"    input_count:         {ss.get('sample_input_count', 0):>5}")
        print(f"    output_count:        {ss.get('sample_output_count', 0):>5}")
        print(f"    warning_letters:     {ss.get('sample_warning_letter_count', 0):>5}")
        print(f"    import_alerts:       {ss.get('sample_import_alert_count', 0):>5}")
        print(f"    enrichable_total:    {ss.get('sample_enrichable_count', 0):>5}")
        print(f"    future_dated_total:  {ss.get('sample_future_dated_count', 0):>5}")

    dq = audit.get("data_quality", {})
    if dq:
        print(f"\n  Data quality (processed sample):")
        print(f"    raw_listing_summary populated:             {dq.get('raw_listing_summary_populated', 0):>4}")
        print(f"    raw_listing_summary blank:                 {dq.get('raw_listing_summary_blank', 0):>4}")
        print(f"    missing required intel fields:             {dq.get('missing_required_intelligence_fields_count', 0):>4}")
        print(f"    decision_summary populated:                {dq.get('decision_summary_populated_count', 0):>4}")
        print(f"    recommended_action populated:              {dq.get('recommended_action_populated_count', 0):>4}")
        print(f"    ai_results_accepted:                       {dq.get('ai_results_accepted_count', 0):>4}")
        print(f"    ai_results_discarded (low confidence):     {dq.get('ai_results_discarded_low_confidence_count', 0):>4}")

    print(f"\n  Priority breakdown:")
    for p in ("P1", "P2", "P3", "P4"):
        print(f"    {p}:  {audit['priority_breakdown'].get(p, 0):>4}")

    print(f"\n  Top categories (primary_gmp_category):")
    for cat, n in list(audit["top_categories"].items())[:8]:
        print(f"    {n:>4}  {cat}")

    print(f"\n  Top failure modes:")
    for fm, n in list(audit["top_failure_modes"].items())[:5]:
        print(f"    {n:>4}  {fm}")

    clusters = audit.get("p1p2_clusters", [])
    if clusters:
        print(f"\n  P1/P2 entity+failure_mode clusters (≥2 records):")
        hdr = f"  {'n':>3}  {'entity':<42}  {'auth':<5}  {'source_type':<20}  {'failure_mode':<28}  P1  P2"
        print(f"  {hdr}")
        print(f"  {'-'*len(hdr)}")
        for cl in clusters:
            label = cl["entity_label"][:42]
            # Format blank-company records as "Unknown entity — AUTH source_type"
            if not label or label.startswith("Unknown entity"):
                label = f"Unknown entity — {cl['authority']} {cl['source_type']}"
            label = label[:42]
            print(
                f"  {cl['total']:>3}  {label:<42}  {cl['authority']:<5}  "
                f"{cl['source_type']:<20}  {cl['failure_mode']:<28}  "
                f"{cl['count_p1']:>2}  {cl['count_p2']:>2}"
            )

    cl_stats = audit.get("clustering", {})
    if cl_stats:
        print(f"\n  Citation clustering (Step 7):")
        print(f"    multi-record clusters:  {cl_stats.get('total_clusters', 0):>4}")
        print(f"    records grouped:        {cl_stats.get('grouped_records_count', 0):>4}")
        print(f"    P1 clusters:            {cl_stats.get('p1_clusters', 0):>4}")
        print(f"    P2 clusters:            {cl_stats.get('p2_clusters', 0):>4}")
        top10 = cl_stats.get("top_10_clusters", [])
        if top10:
            print(f"\n    Top clusters by size:")
            print(f"    {'sz':>3}  {'label':<50}  {'pri':<3}  {'date_range':<23}  P1  P2")
            print(f"    {'-'*100}")
            for tc in top10:
                lbl = tc.get("label", "")[:50]
                print(
                    f"    {tc.get('size',0):>3}  {lbl:<50}  {tc.get('priority',''):<3}  "
                    f"{tc.get('date_range',''):<23}  {tc.get('p1_count',0):>2}  {tc.get('p2_count',0):>2}"
                )

    pres = audit.get("regulatory_pressure", {})
    if pres:
        print(f"\n  Regulatory pressure (90d category trend):")
        for label in ("increasing", "stable", "decreasing"):
            print(f"    {label:<12}  {pres.get(label, 0):>4}")

    if audit.get("failed_sources"):
        print(f"\n  Failed sources ({len(audit['failed_sources'])}):")
        for f in audit["failed_sources"]:
            print(f"    ✗ {f}")

    print(SEP + "\n")


# ---------------------------------------------------------------------------
# Sample strategy helpers
# ---------------------------------------------------------------------------

_ENRICH_SOURCE_TYPES = frozenset({
    "warning_letter", "compliance_action", "inspection_finding", "recall", "safety_alert",
})
_WL_URL_MARKER  = "/warning-letters/"
_TGA_URL_MARKER = "tga.gov.au"


def _date_sort_key(date_str: str) -> int:
    """Return YYYYMMDD int for sorting; 0 for missing/invalid dates."""
    if not date_str:
        return 0
    try:
        return int(date_str.replace("-", ""))
    except ValueError:
        return 0


def _sample_enrichment_tier(c: Citation) -> int:
    """
    Priority tier for 'enrichable' sample strategy.
    Lower value = sampled first. Future-dated records are pushed to the back.
    """
    today_str = NOW.strftime("%Y-%m-%d")
    is_future = bool(c.date) and c.date > today_str

    if c.source_type == "warning_letter" and _WL_URL_MARKER in c.url:
        tier = 0
    elif c.source_type in ("compliance_action", "inspection_finding") and _TGA_URL_MARKER in c.url:
        tier = 1
    elif c.source_type in ("drug_enforcement", "device_enforcement", "food_enforcement"):
        tier = 2
    elif c.source_type in ("recall", "safety_alert", "compliance_action", "inspection_finding"):
        tier = 3
    elif c.source_type == "import_alert":
        tier = 4
    else:
        tier = 5

    return tier + (10 if is_future else 0)


def _apply_sample_strategy(
    citations: list[Citation],
    n: int,
    strategy: str,
) -> tuple[list[Citation], dict]:
    """
    Return (work_set[:n], stats_dict) according to the chosen strategy.

    Strategies:
      recent     — most-recent-first (backwards-compatible default)
      enrichable — FDA WLs and TGA records first; future-dated records last
      mixed      — blend of enrichable tiers and recency
    """
    today_str = NOW.strftime("%Y-%m-%d")

    if strategy == "enrichable":
        ordered = sorted(
            citations,
            key=lambda c: (_sample_enrichment_tier(c), -_date_sort_key(c.date)),
        )
    elif strategy == "mixed":
        ordered = sorted(
            citations,
            key=lambda c: (_sample_enrichment_tier(c) // 3, -_date_sort_key(c.date)),
        )
    else:  # "recent" — default; backwards-compatible
        ordered = sorted(citations, key=lambda c: c.date or "0000-00-00", reverse=True)

    work_set = ordered[:n]

    stats = {
        "sample_strategy":             strategy,
        "sample_input_count":          len(citations),
        "sample_output_count":         len(work_set),
        "sample_enrichable_count":     sum(1 for c in citations if c.source_type in _ENRICH_SOURCE_TYPES),
        "sample_warning_letter_count": sum(1 for c in work_set if c.source_type == "warning_letter"),
        "sample_import_alert_count":   sum(1 for c in work_set if c.source_type == "import_alert"),
        "sample_future_dated_count":   sum(1 for c in citations if c.date and c.date > today_str),
    }
    return work_set, stats


# ---------------------------------------------------------------------------
# HTML injection
# ---------------------------------------------------------------------------
def _inject_data_into_html(data: dict) -> None:
    html_path = REPORTS_DIR / "citation_search.html"
    if not html_path.exists():
        logger.warning("citation_search.html not found — skipping inline injection")
        return
    html      = html_path.read_text(encoding="utf-8")
    SENTINEL  = "<!-- CITATION_DATA_SCRIPT -->"
    json_str  = json.dumps(data, separators=(",", ":"))
    block     = f"{SENTINEL}\n<script>var CITATION_DATA={json_str};</script>"
    existing  = re.compile(
        r"<!-- CITATION_DATA_SCRIPT -->\n<script>var CITATION_DATA=.*?;</script>",
        re.DOTALL,
    )
    if existing.search(html):
        html = existing.sub(lambda _: block, html)
    else:
        html = html.replace(SENTINEL, block)
    html_path.write_text(html, encoding="utf-8")
    logger.info("Injected %d citations into citation_search.html", data["total"])


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run(
    dry_run:         bool          = False,
    no_ai:           bool          = False,
    sample:          Optional[int] = None,
    max_ai_calls:    int           = 25,
    enrich_workers:  int           = 3,
    sample_strategy: str           = "recent",
) -> None:
    """
    Full citation pipeline.

    Args:
        dry_run:         Run all steps but do NOT overwrite citation_database.json.
        no_ai:           Skip the AI intelligence pass entirely.
        sample:          Limit enrichment + AI to the first N citations (for testing).
        max_ai_calls:    Safety cap on total Claude API calls in the AI pass.
        enrich_workers:  Parallel workers for detail-page fetching (default 3).
        sample_strategy: How to pick the sample — recent | enrichable | mixed.
    """
    from collections import Counter

    mode_tags = []
    if dry_run:           mode_tags.append("DRY-RUN")
    if no_ai:             mode_tags.append("NO-AI")
    if sample is not None: mode_tags.append(f"SAMPLE={sample}")
    mode_str = " ".join(mode_tags) or "FULL"

    logger.info("=== Citation Fetcher — %s | 12-month window ===", mode_str)
    logger.info("Cutoff: %s", CUTOFF_12M.strftime("%Y-%m-%d"))

    tracker = AiCallTracker(no_ai=no_ai, max_ai_calls=max_ai_calls, sample=sample)

    scrapers = [
        ("FDA Warning Letters",        scrape_fda_warning_letters),
        ("FDA Drug Enforcement",       scrape_fda_drug_enforcement),
        ("FDA Device Enforcement",     scrape_fda_device_enforcement),
        ("FDA Food/Supplement Enf.",   scrape_fda_food_enforcement),
        ("FDA Import Alerts",          scrape_fda_import_alerts),
        ("TGA",                        scrape_tga),
        ("MHRA",                       scrape_mhra),
        ("EFSA",                       scrape_efsa),
        ("BfR",                        scrape_bfr),
    ]

    failed: list[str] = []
    all_citations: list[Citation] = []

    for name, fn in scrapers:
        try:
            results = fn()
            all_citations.extend(results)
            logger.info("%-30s %d citations", name + ":", len(results))
        except Exception as exc:
            logger.error("%-30s FAILED — %s", name + ":", exc)
            failed.append(f"{name}: {exc}")

    # Deduplicate + sort
    seen_ids: set[str] = set()
    unique: list[Citation] = []
    for c in all_citations:
        if c.id not in seen_ids:
            seen_ids.add(c.id)
            unique.append(c)
    unique.sort(key=lambda c: c.date or "0000-00-00", reverse=True)
    logger.info("Deduplicated: %d unique citations", len(unique))

    # Early source-type fix: correct EFSA/BfR/MHRA guidance records before enrichment
    # so the enrichment path (which checks source_type) sees correct values.
    unique = fix_source_types(unique)

    # Sample slice for enrichment/classification (full corpus always scraped)
    sample_stats: dict = {}
    if sample is not None:
        work_set, sample_stats = _apply_sample_strategy(unique, sample, sample_strategy)
        tracker.ai_skipped_due_to_sample = len(unique) - len(work_set)
        logger.info(
            "Sample mode (%s): %d of %d citations (WL=%d IA=%d enrichable_total=%d future=%d)",
            sample_strategy,
            len(work_set), len(unique),
            sample_stats.get("sample_warning_letter_count", 0),
            sample_stats.get("sample_import_alert_count", 0),
            sample_stats.get("sample_enrichable_count", 0),
            sample_stats.get("sample_future_dated_count", 0),
        )
    else:
        work_set = unique

    # ── Step 1: Enrichment (cache-first; FDA WL + TGA detail pages) ─────────
    # Run before classification so classify_failure_mode sees full violation text.
    logger.info("Step 1/5: Detail-page enrichment (cache-first, %d workers)…", enrich_workers)
    enrich_cache = _load_enrich_cache()
    enriched_raw = enrich_batch_parallel(work_set, enrich_cache, max_workers=enrich_workers)
    _save_enrich_cache(enrich_cache)
    logger.info("Enrichment cache saved (%d entries)", len(enrich_cache))

    # ── Step 2: Multi-label classification + failure mode (best available text) ──
    logger.info("Step 2/5: Multi-label GMP classification + failure mode…")
    classified: list[Citation] = []
    for c in enriched_raw:
        text         = get_best_classification_text(c)
        context_text = get_context_text(c)  # all fields combined — for exclusion guards

        # Tobacco/ENDS guard: uses context_text (all fields) so the
        # "Family Smoking Prevention" header is visible even when enriched_text
        # is selected as the positive-classification input.
        if _is_tobacco_only(context_text):
            primary      = "Other / Insufficient Detail"
            secondary    = []
            fm           = "insufficient_detail"
            fm_conf      = 0.1
            is_noise_flag = False
        else:
            primary, secondary = multi_label_classify(text)
            fm, fm_conf = classify_failure_mode(text, context_text=context_text)

            # Fallbacks: never leave processed records with empty intelligence fields
            is_thin = len(text.strip()) < 30
            if not primary or primary == "Other / Insufficient Detail":
                primary = "Other / Insufficient Detail"
                is_noise_flag = is_thin
            else:
                is_noise_flag = False
            if not fm:
                fm      = "insufficient_detail"
                fm_conf = 0.1

        # Build a temporary Citation with the new category/FM so classify_with_evidence
        # can inspect c.primary_gmp_category and c.failure_mode.
        temp_c = Citation(**{**asdict(c), "primary_gmp_category": primary, "failure_mode": fm})
        ev_fields = classify_with_evidence(temp_c, text)

        updated = asdict(c)
        updated.update({
            "primary_gmp_category":     primary,
            "secondary_gmp_categories": secondary,
            "failure_mode":             fm,
            "failure_mode_confidence":  fm_conf,
            "is_noise":                 is_noise_flag,
            # classification_confidence is AI-only; leave at dataclass default (0.0)
            **ev_fields,
        })
        classified.append(Citation(**updated))

    # ── Step 3: Multi-dimensional severity (now has failure_mode available) ──
    logger.info("Step 3/5: Multi-dimensional severity…")
    severity_applied: list[Citation] = []
    for c in classified:
        sev_fields = compute_multidim_severity(c)
        updated = asdict(c)
        updated.update(sev_fields)
        severity_applied.append(Citation(**updated))

    # ── Step 4: Recurrence + risk direction (may escalate priority) ──────────
    logger.info("Step 4/5: Recurrence counts + risk direction…")
    final = compute_recurrence(severity_applied)

    # ── Step 4b: Pre-cluster (before AI) so cluster_primary is available ─────
    # compute_clusters() is deterministic — run here to set cluster_primary so the
    # AI queue can prioritise cluster representative records.  Step 7 re-runs it
    # after AI fields are settled (priority can shift the primary selection).
    final = compute_clusters(final)

    # ── Step 5: AI intelligence pass (gated, optional) ────────────────────
    ai_import_error   = ""
    ai_pass_attempted = False
    ai_pass_completed = False

    if not no_ai and not dry_run:
        logger.info("Step 5/5: AI intelligence pass (max_calls=%d)…", max_ai_calls)
        ai_pass_attempted = True
        try:
            from reports.pharma_intelligence import PharmaIntelligenceEnricher
            enricher = PharmaIntelligenceEnricher(max_calls=max_ai_calls)
            final = enricher.enrich_batch(final, dry_run=False, tracker=tracker)
            ai_pass_completed = True
        except ImportError as exc:
            ai_import_error = str(exc)
            logger.error(
                "[AI] Import failed — %s. "
                "Ensure project root is on sys.path. AI pass skipped.",
                exc,
            )
        except Exception as exc:
            ai_import_error = str(exc)
            logger.warning("AI intelligence pass failed: %s", exc)
    elif dry_run and not no_ai:
        logger.info("Step 5/5: AI intelligence pass skipped (--dry-run)")
        tracker.ai_skipped_due_to_no_ai = len(work_set)
    else:
        logger.info("Step 5/5: AI intelligence pass skipped (--no-ai)")
        tracker.ai_skipped_due_to_no_ai = len(work_set)

    # ── Step 6a: Source-type reclassification ─────────────────────────────
    # Fix guidance/scientific records mislabelled as inspection_finding.
    # Run before priority caps so subsequent caps see correct source_type.
    logger.info("Step 6a: Source-type trust reclassification…")
    final = fix_source_types(final)

    # ── Step 6b: Low-detail priority cap ──────────────────────────────────
    # Applied after AI fields are settled so the AI-exception path fires correctly.
    from collections import Counter as _Counter
    before_cap = _Counter(c.priority for c in final)
    final = apply_low_detail_priority_cap(final)
    after_cap  = _Counter(c.priority for c in final)
    capped = sum(
        max(before_cap.get(p, 0) - after_cap.get(p, 0), 0)
        for p in ("P1", "P2")
    )
    if capped:
        logger.info(
            "Step 6b: Low-detail cap applied — %d records moved to P3 "
            "(P1: %d→%d, P2: %d→%d)",
            capped,
            before_cap.get("P1", 0), after_cap.get("P1", 0),
            before_cap.get("P2", 0), after_cap.get("P2", 0),
        )

    # ── Step 6c: Classification trust cap ─────────────────────────────────
    # Cap non-enforcement source types at P4; unconfirmed categories at P3.
    logger.info("Step 6c: Classification trust cap…")
    before_trust = _Counter(c.priority for c in final)
    final = apply_classification_trust_cap(final)
    after_trust  = _Counter(c.priority for c in final)
    trust_capped = sum(
        max(before_trust.get(p, 0) - after_trust.get(p, 0), 0)
        for p in ("P1", "P2", "P3")
    )
    if trust_capped:
        logger.info(
            "Step 6c: Trust cap applied — %d records capped "
            "(P1: %d→%d, P2: %d→%d, P3: %d→%d)",
            trust_capped,
            before_trust.get("P1", 0), after_trust.get("P1", 0),
            before_trust.get("P2", 0), after_trust.get("P2", 0),
            before_trust.get("P3", 0), after_trust.get("P3", 0),
        )

    # ── Step 7: Citation clustering ────────────────────────────────────────
    before_cluster = len(final)
    final = compute_clusters(final)
    multi = sum(1 for c in final if c.cluster_size > 1 and c.cluster_primary)
    grouped = sum(1 for c in final if c.cluster_size > 1)
    if multi:
        logger.info(
            "Step 7: Clustering — %d multi-record clusters (%d records grouped) from %d total",
            multi, grouped, before_cluster,
        )

    # ── Audit report ───────────────────────────────────────────────────────
    audit = _build_audit_report(
        final, failed, tracker,
        ai_import_error=ai_import_error,
        ai_pass_attempted=ai_pass_attempted,
        ai_pass_completed=ai_pass_completed,
        sample_stats=sample_stats if sample is not None else None,
    )
    _print_audit_report(audit)
    audit_path = REPORTS_DIR / "citation_audit.json"
    audit_path.write_text(json.dumps(audit, indent=2))
    logger.info("Audit written to %s", audit_path)

    if dry_run:
        logger.info("DRY-RUN: citation_database.json NOT written. Use --write to persist.")
        return

    # ── Write output ──────────────────────────────────────────────────────
    # Merge sample back into full corpus (unenriched entries get original fields)
    if sample is not None:
        enriched_map = {c.id: c for c in final}
        output_set = [enriched_map.get(c.id, c) for c in unique]
    else:
        output_set = final

    output = {
        "generated_at": NOW.isoformat(),
        "cutoff":        CUTOFF_12M.strftime("%Y-%m-%d"),
        "total":         len(output_set),
        "citations":     [asdict(c) for c in output_set],
    }
    OUTPUT_JSON.write_text(json.dumps(output, indent=2))
    logger.info("Wrote %d citations to %s", len(output_set), OUTPUT_JSON)
    _inject_data_into_html(output)

    # Legacy summary (authority / facility / category / severity)
    by_auth  = Counter(c.authority     for c in output_set)
    by_ftype = Counter(c.facility_type for c in output_set)
    by_cat   = Counter(c.category      for c in output_set)
    by_sev   = Counter(c.severity      for c in output_set)

    SEP = "=" * 62
    print(f"\n{SEP}")
    print("  CITATION DATABASE SUMMARY")
    print(SEP)
    print(f"  Total citations:  {len(output_set)}")
    print(f"\n  By Authority:")
    for auth, n in sorted(by_auth.items(), key=lambda x: -x[1]):
        print(f"    {auth:<10} {n:>4}")
    print(f"\n  Top 10 Categories:")
    for cat, n in by_cat.most_common(10):
        print(f"    {n:>4}  {cat}")
    print(f"\n  By Severity:")
    for sev in ("high", "medium", "low"):
        print(f"    {sev:<8} {by_sev.get(sev, 0):>4}")
    if failed:
        print(f"\n  Failed sources ({len(failed)}):")
        for f in failed:
            print(f"    ✗ {f}")
    print(SEP)
    print(f"\n  Output: {OUTPUT_JSON}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Citation Database Builder — scrape, enrich, classify, and write citation_database.json"
    )
    parser.add_argument(
        "--dry-run",    action="store_true",
        help="Run full pipeline but do NOT write citation_database.json (audit report only)",
    )
    parser.add_argument(
        "--no-ai",      action="store_true",
        help="Skip the AI intelligence pass (enrichment + deterministic fields still run)",
    )
    parser.add_argument(
        "--sample",     type=int, default=None, metavar="N",
        help="Limit enrichment and AI to the first N citations (for test runs)",
    )
    parser.add_argument(
        "--max-ai-calls", type=int, default=25, metavar="N",
        help="Safety cap on total Claude API calls in the AI pass (default: 25)",
    )
    parser.add_argument(
        "--enrich-workers", type=int, default=3, metavar="N",
        help="Parallel workers for detail-page fetching (default: 3, max recommended: 4)",
    )
    parser.add_argument(
        "--sample-strategy", default="recent",
        choices=["recent", "enrichable", "mixed"],
        help=(
            "How to pick the --sample set. "
            "'recent' = most-recent-first (default); "
            "'enrichable' = FDA WLs and TGA records first, future-dated last; "
            "'mixed' = blend of both."
        ),
    )
    args = parser.parse_args()
    run(
        dry_run=args.dry_run,
        no_ai=args.no_ai,
        sample=args.sample,
        max_ai_calls=args.max_ai_calls,
        enrich_workers=args.enrich_workers,
        sample_strategy=args.sample_strategy,
    )
