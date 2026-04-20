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
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

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
    category:         str   # one of CATEGORIES
    severity:         str   # high | medium | low
    summary:          str
    url:              str
    product_type:     str   # human-readable product descriptor
    country:          str
    facility_type:    str   # one of FACILITY_TYPES
    violation_details: str  # raw text, ≤500 chars


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
    ("Labelling & claims",          ["label", "misbranding", "mislabeled", "claim", "misleading"]),
    ("Contamination & sterility",   ["contamination", "microbial", "salmonella", "listeria", "foreign material", "particulate"]),
    ("Supply chain & procurement",  ["supplier", "vendor", "procurement", "raw material", "fsvp", "contract"]),
    ("Equipment & facilities",      ["equipment", "facility", "calibration", "sanitation", "cleaning", "maintenance"]),
    ("Quality management system",   ["quality system", "qms", "quality management", "audit", "pharmaceutical quality"]),
    ("Ingredient safety",           ["undeclared", "adulterant", "identity", "purity", "potency", "nitrosamine", "impurity"]),
]


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
            cat      = classify_with_claude(raw)
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
        cat   = classify_with_claude(raw)
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
            rec_url = (
                f"https://www.accessdata.fda.gov/scripts/ires/"
                f"?action=RecallAction&RecallNumber={recall_n}"
                if recall_n else url
            )
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

            cat = classify_with_claude(raw)
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

            cat = classify_with_claude(raw)
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
                cat   = classify_with_claude(raw)
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
                cat   = classify_with_claude(raw)
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
                cat   = classify_with_claude(raw)
                results.append(Citation(
                    id=make_id("MHRA_GMP", item_url),
                    authority="MHRA", source_type=src_type,
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
                src_type = "compliance_action" if "News" in pub_type else "inspection_finding"
                cat      = classify_with_claude(raw)
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
                src_type  = (
                    "compliance_action"
                    if any(w in src_label.lower() for w in ["press", "faq", "comms"])
                    else "inspection_finding"
                )
                cat   = classify_with_claude(raw)
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
# Main
# ---------------------------------------------------------------------------
def run() -> None:
    from collections import Counter

    logger.info("=== Citation Fetcher starting — full pharma scope, 12-month window ===")
    logger.info("Cutoff: %s", CUTOFF_12M.strftime("%Y-%m-%d"))

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

    # Deduplicate
    seen_ids: set[str] = set()
    unique: list[Citation] = []
    for c in all_citations:
        if c.id not in seen_ids:
            seen_ids.add(c.id)
            unique.append(c)

    # Sort: most recent first
    unique.sort(key=lambda c: c.date or "0000-00-00", reverse=True)

    output = {
        "generated_at": NOW.isoformat(),
        "cutoff":        CUTOFF_12M.strftime("%Y-%m-%d"),
        "total":         len(unique),
        "citations":     [asdict(c) for c in unique],
    }
    OUTPUT_JSON.write_text(json.dumps(output, indent=2))
    logger.info("Wrote %d citations to %s", len(unique), OUTPUT_JSON)
    _inject_data_into_html(output)

    # ── Summary report ────────────────────────────────────────────────────
    by_auth  = Counter(c.authority      for c in unique)
    by_ftype = Counter(c.facility_type  for c in unique)
    by_cat   = Counter(c.category       for c in unique)
    by_sev   = Counter(c.severity       for c in unique)

    SEP = "=" * 62
    print(f"\n{SEP}")
    print("  CITATION DATABASE SUMMARY")
    print(SEP)
    print(f"  Total citations:  {len(unique)}")

    print(f"\n  By Authority:")
    for auth, n in sorted(by_auth.items(), key=lambda x: -x[1]):
        print(f"    {auth:<10} {n:>4}")

    print(f"\n  By Facility Type:")
    for ft, n in sorted(by_ftype.items(), key=lambda x: -x[1]):
        print(f"    {ft:<30} {n:>4}")

    print(f"\n  Top 10 Violation Categories:")
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
    run()
