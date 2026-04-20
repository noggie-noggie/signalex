"""
reports/citation_report.py — Citation Intelligence Report

Pulls regulatory citations from TGA, FDA, EFSA, and EMA covering the last
90 days, analyses cross-jurisdiction patterns, and generates
citation_report.html in the project root with Signalex dark-navy branding.

Usage:
    python -m reports.citation_report
    # or
    python reports/citation_report.py
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("citation_report")

# ---------------------------------------------------------------------------
# Paths & time windows
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
REPORTS_DIR  = Path(__file__).parent
OUTPUT_HTML  = PROJECT_ROOT / "citation_report.html"

NOW              = datetime.now(timezone.utc)
CUTOFF_90        = NOW - timedelta(days=90)   # full lookback
CUTOFF_30        = NOW - timedelta(days=30)   # "recent" window for trend
REPORT_DATE_STR  = NOW.strftime("%d %B %Y")

# ---------------------------------------------------------------------------
# Supplement ingredient vocabulary
# ---------------------------------------------------------------------------
INGREDIENT_PATTERNS: list[tuple[str, list[str]]] = [
    # Vitamins
    ("Vitamin D",         ["vitamin d", "cholecalciferol", "ergocalciferol", "calcitriol", "vitamin d3", "vitamin d2"]),
    ("Vitamin C",         ["vitamin c", "ascorbic acid", "ascorbate", "l-ascorbic"]),
    ("Vitamin A",         ["vitamin a", "retinol", "retinyl", "beta-carotene", "betacarotene"]),
    ("Vitamin E",         ["vitamin e", "tocopherol", "tocotrienol"]),
    ("Vitamin K",         ["vitamin k", "phylloquinone", "menaquinone", "mk-7", "mk-4"]),
    ("Vitamin B12",       ["vitamin b12", "b-12", "cobalamin", "cyanocobalamin", "methylcobalamin"]),
    ("Vitamin B6",        ["vitamin b6", "pyridoxine", "pyridoxal"]),
    ("Folate/Folic Acid", ["folate", "folic acid", "5-mthf", "methylfolate"]),
    ("Biotin",            ["biotin", "vitamin h"]),
    ("Niacin",            ["niacin", "nicotinic acid", "niacinamide", "vitamin b3"]),
    ("Riboflavin",        ["riboflavin", "vitamin b2"]),
    ("Thiamine",          ["thiamine", "vitamin b1"]),
    # Minerals
    ("Zinc",              ["zinc", " zn ", "zinc oxide", "zinc gluconate", "zinc citrate"]),
    ("Magnesium",         ["magnesium", "mg2+", "magnesium oxide", "magnesium glycinate"]),
    ("Calcium",           ["calcium", "calcium carbonate", "calcium citrate"]),
    ("Iron",              ["iron", "ferrous", "ferric", "iron deficiency"]),
    ("Selenium",          ["selenium", "selenomethionine", "sodium selenate"]),
    ("Iodine",            ["iodine", "iodide", "potassium iodide"]),
    ("Chromium",          ["chromium", "chromium picolinate"]),
    ("Copper",            ["copper", "cupric"]),
    ("Manganese",         ["manganese"]),
    ("Boron",             ["boron"]),
    # Omega/Fats
    ("Omega-3 / Fish Oil",["omega-3", "omega 3", "fish oil", "dha", "epa", "docosahexaenoic", "eicosapentaenoic", "krill oil", "cod liver"]),
    # Probiotics
    ("Probiotics",        ["probiotic", "lactobacillus", "bifidobacterium", "saccharomyces", "gut health"]),
    # Popular botanicals
    ("Ashwagandha",       ["ashwagandha", "withania somnifera", "withanolide"]),
    ("Turmeric/Curcumin", ["turmeric", "curcumin", "curcuminoid"]),
    ("Valerian",          ["valerian", "valeriana"]),
    ("St. John's Wort",   ["st. john", "st john", "hypericum", "hypericin"]),
    ("Echinacea",         ["echinacea"]),
    ("Ginkgo",            ["ginkgo", "ginkgolide"]),
    ("Ginseng",           ["ginseng", "panax", "ginsenoside"]),
    ("Ginger",            ["ginger", "zingiber"]),
    ("Garlic",            ["garlic", "allicin", "allium sativum"]),
    ("Milk Thistle",      ["milk thistle", "silymarin", "silybum"]),
    ("Berberine",         ["berberine"]),
    ("Quercetin",         ["quercetin"]),
    ("Resveratrol",       ["resveratrol"]),
    ("Melatonin",         ["melatonin"]),
    ("CoQ10",             ["coq10", "coenzyme q10", "ubiquinol", "ubiquinone"]),
    ("Collagen",          ["collagen"]),
    # Sports / weight
    ("Creatine",          ["creatine", "creatinine"]),
    ("Protein / Whey",    ["whey protein", "protein powder", "casein protein"]),
    ("L-Carnitine",       ["l-carnitine", "carnitine", "acetyl-l-carnitine"]),
    ("BCAA",              ["bcaa", "branched chain", "leucine", "isoleucine", "valine"]),
    ("Caffeine",          ["caffeine"]),
    ("Creatine",          ["creatine"]),
    ("Pre-workout",       ["pre-workout", "pre workout"]),
    # Stimulants / controlled
    ("Ephedra",           ["ephedra", "ephedrine", "ma huang"]),
    ("DMAA",              ["dmaa", "1,3-dimethylamylamine", "methylhexanamine"]),
    ("DMHA",              ["dmha", "2-aminoisoheptane", "octodrine"]),
    ("Synephrine",        ["synephrine", "bitter orange", "citrus aurantium"]),
    ("Yohimbe",           ["yohimbe", "yohimbine"]),
    ("Kratom",            ["kratom", "mitragyna"]),
    ("Kava",              ["kava", "piper methysticum"]),
    ("SARMs",             ["sarm", "selective androgen receptor", "ostarine", "ligandrol", "rad-140", "andarine"]),
    ("DHEA",              ["dhea", "dehydroepiandrosterone"]),
    ("Testosterone",      ["testosterone", "anabolic steroid"]),
    # Novel / weight loss
    ("GLP-1 Analogues",   ["glp-1", "semaglutide", "liraglutide", "tirzepatide", "ozempic", "wegovy"]),
    ("CBD / Hemp",        ["cbd", "cannabidiol", "hemp extract", "cannabis"]),
    ("5-HTP",             ["5-htp", "5-hydroxytryptophan"]),
    ("NAC",               ["nac", "n-acetyl cysteine", "n-acetylcysteine"]),
    ("Glucosamine",       ["glucosamine"]),
    ("Chondroitin",       ["chondroitin"]),
    # Amino acids
    ("L-Arginine",        ["l-arginine", "arginine"]),
    ("L-Glutamine",       ["l-glutamine", "glutamine"]),
    ("L-Lysine",          ["l-lysine", "lysine"]),
    # Weight loss generics
    ("Weight Loss Product",["weight loss", "slimming", "fat burner", "thermogenic"]),
    # Heavy metals / contamination
    ("Heavy Metals",      ["lead", "arsenic", "mercury", "cadmium", "heavy metal"]),
    ("Microbiological",   ["salmonella", "e. coli", "listeria", "staphylococcus", "microbial contamination"]),
]

# GMP / inspection violation categories
GMP_CATEGORIES: list[tuple[str, list[str]]] = [
    ("Manufacturing Procedures",   ["manufacturing procedure", "production process", "batch record", "manufacturing control"]),
    ("Contamination / Foreign Material", ["contamination", "foreign material", "foreign matter", "microbial", "cross-contamination"]),
    ("Labelling / Misbranding",    ["label", "misbranding", "mislabeled", "false claim", "misleading label", "unapproved claim"]),
    ("Quality Control / Testing",  ["quality control", "qc testing", "analytical testing", "out-of-specification", "oos result"]),
    ("Documentation / Recordkeeping", ["record", "documentation", "coa", "certificate of analysis", "master formula", "sop"]),
    ("Equipment / Facilities",     ["equipment", "facility", "sanitation", "cleaning validation", "calibration", "maintenance"]),
    ("Supplier Qualification",     ["supplier", "vendor qualification", "raw material supplier", "contract manufacturer", "fsvp", "foreign supplier", "importer"]),
    ("Identity / Purity Testing",  ["identity test", "purity", "adulterant", "undeclared ingredient", "active pharmaceutical", "drug substance"]),
    ("Undeclared Drug Substance",  ["undeclared", "sildenafil", "tadalafil", "sibutramine", "phenolphthalein", "fluoxetine"]),
    ("CGMP Non-compliance",        ["cgmp", "current good manufacturing", "gmp violation", "gmp non-compliance"]),
]

# ---------------------------------------------------------------------------
# Citation dataclass
# ---------------------------------------------------------------------------
@dataclass
class Citation:
    id:            str
    title:         str
    url:           str
    ingredient:    str          # extracted ingredient name, "Unknown" if none found
    citation_type: str          # safety_alert | recall | warning_letter | GMP_violation | inspection_finding | ban
    authority:     str          # TGA | FDA | EFSA | EMA
    severity:      str          # high | medium | low
    date:          Optional[datetime]
    company:       str = ""
    summary:       str = ""
    gmp_category:  str = ""     # populated for GMP_violation / inspection_finding
    raw_text:      str = field(default="", repr=False)

    @property
    def date_str(self) -> str:
        return self.date.strftime("%Y-%m-%d") if self.date else "Unknown"

    @property
    def is_recent(self) -> bool:
        """True if citation falls in the last 30 days."""
        return self.date is not None and self.date >= CUTOFF_30

# ---------------------------------------------------------------------------
# HTTP session
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


def html_to_text(fragment: str) -> str:
    """Safely parse an HTML fragment to plain text, handling non-HTML strings."""
    if not fragment:
        return ""
    # If no HTML tags present, return as-is (avoids BS4 MarkupResemblesLocatorWarning)
    if "<" not in fragment and ">" not in fragment:
        return fragment.strip()
    return BeautifulSoup(fragment, "lxml").get_text(" ", strip=True)


def http_get(url: str, timeout: int = 30, **kwargs) -> requests.Response:
    """GET with retry on 5xx / connection errors. Returns response for all 4xx."""
    for attempt in range(3):
        try:
            resp = get_session().get(url, timeout=timeout, **kwargs)
            if resp.status_code < 500:
                return resp   # callers handle 4xx themselves
            logger.warning("HTTP %s for %s (attempt %d)", resp.status_code, url, attempt + 1)
        except requests.RequestException as exc:
            logger.warning("Request failed for %s: %s (attempt %d)", url, exc, attempt + 1)
        if attempt < 2:
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to fetch {url} after 3 attempts")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_date(text: str) -> Optional[datetime]:
    """Try common date formats, always return timezone-aware datetime."""
    if not text:
        return None
    text = text.strip()
    # Try ISO first
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%d %B %Y",
        "%d %b %Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d-%m-%Y",
    ):
        try:
            naive = datetime.strptime(text[:30], fmt)
            return naive.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def extract_date_from_tag(tag: Tag) -> Optional[datetime]:
    """Extract date from a BeautifulSoup tag using multiple strategies."""
    # 1. <time datetime="...">
    time_el = tag.find("time")
    if time_el:
        dt = parse_date(time_el.get("datetime", "") or time_el.get_text(strip=True))
        if dt:
            return dt
    # 2. Any element with class containing "date"
    date_el = tag.find(class_=re.compile(r"\bdate\b", re.I))
    if date_el:
        dt = parse_date(date_el.get_text(strip=True))
        if dt:
            return dt
    # 3. Regex over text
    text = tag.get_text(" ")
    m = re.search(
        r"\b(\d{1,2})\s+(January|February|March|April|May|June|July|August"
        r"|September|October|November|December)\s+(\d{4})\b",
        text, re.I,
    )
    if m:
        dt = parse_date(m.group(0))
        if dt:
            return dt
    m2 = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", text)
    if m2:
        dt = parse_date(m2.group(0))
        if dt:
            return dt
    return None


def extract_ingredient(text: str) -> str:
    """Return the first matching ingredient name found in `text`, or 'Unknown'."""
    lower = text.lower()
    for canonical, keywords in INGREDIENT_PATTERNS:
        for kw in keywords:
            if kw in lower:
                return canonical
    return "Unknown"


def extract_gmp_category(text: str) -> str:
    """Return the best-matching GMP violation category, or ''."""
    lower = text.lower()
    for cat, keywords in GMP_CATEGORIES:
        for kw in keywords:
            if kw in lower:
                return cat
    return ""


def infer_severity(text: str, citation_type: str) -> str:
    """Classify severity based on keywords and citation type."""
    lower = text.lower()
    HIGH_WORDS = [
        "death", "hospitali", "life-threatening", "serious adverse", "serious risk",
        "critical", "urgent", "immediate", "ban", "banned", "seizure", "injunction",
        "salmonella", "listeria", "e. coli", "counterfeit", "undeclared drug",
        "sildenafil", "tadalafil", "sibutramine", "sarms", "dmaa", "dmha",
        "anabolic steroid", "recall class i", "class i recall",
    ]
    LOW_WORDS = [
        "opinion", "information", "update", "guidance", "review", "assessment",
        "monitoring", "surveillance", "report",
    ]
    if any(w in lower for w in HIGH_WORDS):
        return "high"
    if citation_type in ("ban", "recall") and any(w in lower for w in ["class i", "serious", "life"]):
        return "high"
    if citation_type in ("warning_letter", "GMP_violation"):
        return "medium"
    if citation_type == "recall":
        return "medium"
    if any(w in lower for w in LOW_WORDS):
        return "low"
    return "medium"


def make_id(authority: str, url: str) -> str:
    import hashlib
    return hashlib.md5(f"{authority}::{url}".encode()).hexdigest()[:10]


# ---------------------------------------------------------------------------
# Scraper 1: TGA Safety Alerts
# ---------------------------------------------------------------------------
def scrape_tga_alerts(cutoff: datetime) -> list[Citation]:
    """Scrape https://www.tga.gov.au/safety/.../safety-alerts"""
    BASE = "https://www.tga.gov.au"
    URL  = f"{BASE}/safety/safety-monitoring-and-information/safety-alerts"
    logger.info("TGA: fetching safety alerts …")
    results: list[Citation] = []
    try:
        resp = http_get(URL)
        soup = BeautifulSoup(resp.text, "lxml")
        articles = soup.select("article") or soup.select(".views-row")
        logger.info("TGA alerts: %d articles found", len(articles))
        for art in articles:
            link = (
                art.select_one("h3.summary__title a[href]")
                or art.select_one("h2.summary__title a[href]")
                or art.select_one("h3 a[href]")
                or art.select_one("h2 a[href]")
                or art.select_one("a[href]")
            )
            if not link:
                continue
            title = link.get_text(strip=True)
            href  = link.get("href", "")
            url   = href if href.startswith("http") else BASE + href
            date  = extract_date_from_tag(art)
            if date and date < cutoff:
                continue
            # Teaser
            teaser_el = art.select_one(
                ".field--name-field-summary, .summary__summary, p"
            )
            summary = teaser_el.get_text(" ", strip=True) if teaser_el else ""
            raw = f"{title} {summary}"
            ingredient    = extract_ingredient(raw)
            citation_type = "ban" if "ban" in title.lower() else "safety_alert"
            severity      = infer_severity(raw, citation_type)
            results.append(Citation(
                id=make_id("TGA", url),
                title=title, url=url, ingredient=ingredient,
                citation_type=citation_type, authority="TGA",
                severity=severity, date=date, summary=summary, raw_text=raw,
            ))
    except Exception as exc:
        logger.warning("TGA alerts scraper error: %s", exc)
    logger.info("TGA alerts: %d citations in window", len(results))
    return results


# ---------------------------------------------------------------------------
# Scraper 2: TGA Recalls & Advisories
# ---------------------------------------------------------------------------
def scrape_tga_recalls(cutoff: datetime) -> list[Citation]:
    """Scrape https://www.tga.gov.au/news/recalls-alerts-and-safety-advisories"""
    BASE = "https://www.tga.gov.au"
    URL  = f"{BASE}/news/recalls-alerts-and-safety-advisories"
    logger.info("TGA: fetching recalls & advisories …")
    results: list[Citation] = []
    try:
        resp = http_get(URL)
        soup = BeautifulSoup(resp.text, "lxml")
        articles = soup.select("article") or soup.select(".views-row")
        logger.info("TGA recalls: %d articles found", len(articles))
        for art in articles:
            link = (
                art.select_one("h3 a[href]")
                or art.select_one("h2 a[href]")
                or art.select_one("a[href]")
            )
            if not link:
                continue
            title = link.get_text(strip=True)
            href  = link.get("href", "")
            url   = href if href.startswith("http") else BASE + href
            date  = extract_date_from_tag(art)
            if date and date < cutoff:
                continue
            teaser_el = art.select_one(".field--name-field-summary, p")
            summary = teaser_el.get_text(" ", strip=True) if teaser_el else ""
            raw = f"{title} {summary}"
            ingredient    = extract_ingredient(raw)
            lower_title   = title.lower()
            if "recall" in lower_title:
                citation_type = "recall"
            elif "advisory" in lower_title or "advisory" in summary.lower():
                citation_type = "safety_alert"
            else:
                citation_type = "recall"
            severity = infer_severity(raw, citation_type)
            results.append(Citation(
                id=make_id("TGA_recall", url),
                title=title, url=url, ingredient=ingredient,
                citation_type=citation_type, authority="TGA",
                severity=severity, date=date, summary=summary, raw_text=raw,
            ))
    except Exception as exc:
        logger.warning("TGA recalls scraper error: %s", exc)
    logger.info("TGA recalls: %d citations in window", len(results))
    return results


# ---------------------------------------------------------------------------
# Scraper 3: FDA — OpenFDA Enforcement API (recalls + enforcement)
# ---------------------------------------------------------------------------
def scrape_fda_enforcement(cutoff: datetime) -> list[Citation]:
    """
    Pull dietary supplement enforcement actions from OpenFDA.
    Searches product_description for supplement keywords and filters by date.
    API docs: https://open.fda.gov/apis/food/enforcement/
    """
    logger.info("FDA: fetching enforcement actions via OpenFDA API …")
    results: list[Citation] = []
    cutoff_str = cutoff.strftime("%Y%m%d")
    # OpenFDA food/enforcement uses recall_initiation_date not report_date
    url = (
        "https://api.fda.gov/food/enforcement.json"
        "?search=product_description:(vitamin+OR+supplement+OR+probiotic"
        "+OR+mineral+OR+herbal+OR+botanical+OR+omega+OR+protein+OR+creatine"
        "+OR+amino+OR+collagen)"
        f"+AND+recall_initiation_date:[{cutoff_str}+TO+29991231]"
        "&limit=100&sort=recall_initiation_date:desc"
    )
    try:
        resp = http_get(url)
        data = resp.json()
        records = data.get("results", [])
        logger.info("FDA enforcement: %d records", len(records))
        for rec in records:
            title       = rec.get("product_description", "FDA Enforcement")[:200]
            company     = rec.get("recalling_firm", "")
            reason      = rec.get("reason_for_recall", "")
            recall_num  = rec.get("recall_number", "")
            date_str    = rec.get("recall_initiation_date", rec.get("report_date", ""))
            date = parse_date(date_str) if date_str else None
            # parse_date returns naive; make aware for comparison
            if date and date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            if date and date < cutoff:
                continue
            rec_url = (
                f"https://www.accessdata.fda.gov/scripts/ires/"
                f"?action=RecallAction&RecallNumber={recall_num}"
            )
            raw         = f"{title} {reason}"
            ingredient  = extract_ingredient(raw)
            class_code  = rec.get("classification", "").lower()
            if "class i" in class_code and "class ii" not in class_code:
                severity = "high"
            elif "class ii" in class_code:
                severity = "medium"
            else:
                severity = infer_severity(raw, "recall")
            results.append(Citation(
                id=make_id("FDA_enf", recall_num or rec_url),
                title=title, url=rec_url, ingredient=ingredient,
                citation_type="recall", authority="FDA",
                severity=severity, date=date, company=company,
                summary=reason, raw_text=raw,
            ))
    except Exception as exc:
        logger.warning("FDA enforcement API error: %s", exc)
    logger.info("FDA enforcement: %d citations", len(results))
    return results


# ---------------------------------------------------------------------------
# Scraper 4: FDA Warning Letters (Drupal DataTables AJAX)
# ---------------------------------------------------------------------------
_FDA_WL_PAGE = (
    "https://www.fda.gov/inspections-compliance-enforcement-and-criminal-investigations"
    "/compliance-actions-and-activities/warning-letters"
)
_SUPPLEMENT_TERMS = re.compile(
    r"dietary supplement|nutraceutical|GMP|cgmp|good manufacturing|"
    r"vitamin|mineral|herbal|botanical|probiotic|protein powder|sports nutrition",
    re.I,
)

def scrape_fda_warning_letters(cutoff: datetime) -> list[Citation]:
    """
    Fetch FDA warning letters, filter for dietary supplement / GMP violations.
    Tries Drupal DataTables AJAX; falls back to plain HTML pagination.
    """
    logger.info("FDA: fetching warning letters …")
    results: list[Citation] = []

    # --- Attempt 1: extract Drupal DataTables params and hit AJAX ---
    try:
        page_resp = http_get(_FDA_WL_PAGE)
        if page_resp.status_code == 403:
            logger.warning("FDA WL page returned 403 (rate limited) — skipping warning letters this run")
            return results
        dom_id    = _extract_drupal_dom_id(page_resp.text)
        view_name, display_id = _extract_drupal_view_ids(page_resp.text)
        if dom_id:
            ajax_results = _fetch_fda_datatable(dom_id, view_name, display_id, cutoff)
            if ajax_results:
                results.extend(ajax_results)
                logger.info("FDA warning letters (AJAX): %d", len(results))
                return results
        else:
            logger.warning("FDA WL: could not extract DataTables DOM ID from page")
    except Exception as exc:
        logger.warning("FDA WL AJAX path failed: %s", exc)

    # --- Attempt 2: plain HTML parse of the listing page ---
    try:
        page_resp = http_get(_FDA_WL_PAGE)
        if page_resp.status_code != 403:
            results.extend(_parse_fda_wl_html(page_resp.text, cutoff))
    except Exception as exc:
        logger.warning("FDA warning letters HTML fallback error: %s", exc)

    logger.info("FDA warning letters: %d citations", len(results))
    return results


def _extract_drupal_dom_id(html: str) -> str:
    """Parse drupalSettings JS blob for a DataTables view dom_id."""
    m = re.search(r'"view_dom_id"\s*:\s*"([a-f0-9]+)"', html)
    return m.group(1) if m else ""


def _extract_drupal_view_ids(html: str) -> tuple[str, str]:
    """Extract view_name and view_display_id from drupalSettings."""
    m_name    = re.search(r'"view_name"\s*:\s*"([^"]+)"', html)
    m_display = re.search(r'"view_display_id"\s*:\s*"([^"]+)"', html)
    return (m_name.group(1) if m_name else ""), (m_display.group(1) if m_display else "")


def _fetch_fda_datatable(dom_id: str, view_name: str, display_id: str,
                          cutoff: datetime) -> list[Citation]:
    """
    Paginate FDA warning letters via Drupal DataTables AJAX.
    Uses start/length offset pagination (not page-number pagination).
    Scans until dates fall below the 90-day cutoff.
    """
    AJAX_URL   = "https://www.fda.gov/datatables/views/ajax"
    PAGE_SIZE  = 10
    results    = []
    seen_urls: set[str] = set()

    for batch_start in range(0, 2000, PAGE_SIZE):  # max 2000 rows
        if batch_start > 0:
            time.sleep(0.3)   # polite pacing — avoid FDA rate-limit (429)
        payload = {
            "view_name": view_name or "warning_letter_solr_index",
            "view_display_id": display_id or "warning_letter_solr_block",
            "view_dom_id": dom_id,
            "pager_element": "0",
            "page": "0",
            "start": str(batch_start),
            "length": str(PAGE_SIZE),
        }
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": _FDA_WL_PAGE,
        }
        resp = get_session().post(AJAX_URL, data=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        data     = resp.json()
        rows_raw = data.get("data", [])
        if not rows_raw:
            break

        page_results, stop = _parse_fda_wl_rows(rows_raw, cutoff, seen_urls)
        results.extend(page_results)
        if stop:
            break

    return results


def _extract_datatable_rows(data) -> list[list[str]]:
    """Extract cell HTML arrays from a Drupal DataTables AJAX response."""
    if isinstance(data, dict):
        rows = data.get("data", [])
    elif isinstance(data, list):
        # Drupal may return array of command objects
        for item in data:
            if isinstance(item, dict) and item.get("command") == "insert":
                html = item.get("data", "")
                soup = BeautifulSoup(html, "lxml")
                return [
                    [str(td) for td in tr.find_all("td")]
                    for tr in soup.find_all("tr")
                    if tr.find("td")
                ]
        rows = []
    else:
        rows = []
    return rows


_WL_SUPPLEMENT_SUBJECT = re.compile(
    r"Dietary Supplement|Dietary supplement|dietary supplement|"
    r"CGMP/Dietary|Supplement/Adulterated|Supplement/Misbranded|"
    r"Infant Formula|FSVP|Food Safety|Undeclared",
    re.I,
)


def _parse_fda_wl_rows(rows: list, cutoff: datetime,
                        seen_urls: set) -> tuple[list[Citation], bool]:
    """
    Parse cell arrays from Drupal DataTables AJAX response.
    Row format: [0]=post_date [1]=letter_date [2]=company+link [3]=issuing_office [4]=subject
    """
    results = []
    stop    = False

    for row in rows:
        if len(row) < 3:
            continue

        # Cell 0: post_date HTML (<time datetime="...">)
        date_html = row[0] if "<" in str(row[0]) else ""
        if date_html:
            date_soup = BeautifulSoup(date_html, "lxml")
            time_el   = date_soup.find("time")
            date_str  = (time_el.get("datetime") or time_el.get_text(strip=True)) if time_el else html_to_text(date_html)
        else:
            date_str = str(row[0]).strip()
        date = parse_date(date_str) if date_str else None

        if date and date < cutoff:
            stop = True
            break

        # Cell 2: company + link
        company_soup = BeautifulSoup(row[2], "lxml")
        company      = company_soup.get_text(strip=True)
        link_el      = company_soup.find("a", href=True)
        wl_url       = urljoin("https://www.fda.gov", link_el["href"]) if link_el else _FDA_WL_PAGE

        # Skip if already seen (duplicate from pagination)
        if wl_url in seen_urls:
            continue
        seen_urls.add(wl_url)

        # Cell 3: issuing office
        issuing_office = html_to_text(row[3]) if len(row) > 3 else ""

        # Cell 4: subject/violation type
        subject = html_to_text(row[4]) if len(row) > 4 else html_to_text(row[-1])

        raw = f"{subject} {company} {issuing_office}"

        # Filter for supplement/food/GMP-related letters
        if not _WL_SUPPLEMENT_SUBJECT.search(raw) and not _SUPPLEMENT_TERMS.search(raw):
            continue

        ingredient  = extract_ingredient(raw)
        gmp_cat     = extract_gmp_category(raw)
        ctype       = "GMP_violation" if ("GMP" in subject.upper() or "CGMP" in subject.upper() or gmp_cat) else "warning_letter"
        severity    = infer_severity(raw, ctype)

        results.append(Citation(
            id=make_id("FDA_WL", wl_url),
            title=f"{company} — {subject}"[:200] if subject else company,
            url=wl_url, ingredient=ingredient,
            citation_type=ctype, authority="FDA",
            severity=severity, date=date, company=company,
            summary=subject, gmp_category=gmp_cat, raw_text=raw,
        ))
    return results, stop


def _parse_fda_wl_html(html: str, cutoff: datetime) -> list[Citation]:
    """Fallback: parse warning letters from plain HTML table."""
    results = []
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return results
    for row in table.select("tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        date_str = cells[0].get_text(strip=True)
        date     = parse_date(date_str)
        if date and date < cutoff:
            continue
        link_el  = cells[1].find("a")
        subject  = cells[1].get_text(strip=True)
        company  = cells[3].get_text(strip=True) if len(cells) > 3 else ""
        url      = urljoin("https://www.fda.gov", link_el["href"]) if link_el else _FDA_WL_PAGE
        raw      = f"{subject} {company}"
        if not _SUPPLEMENT_TERMS.search(raw):
            continue
        ingredient = extract_ingredient(raw)
        gmp_cat    = extract_gmp_category(raw)
        ctype      = "GMP_violation" if "GMP" in raw.upper() or gmp_cat else "warning_letter"
        severity   = infer_severity(raw, ctype)
        results.append(Citation(
            id=make_id("FDA_WL_html", url),
            title=subject, url=url, ingredient=ingredient,
            citation_type=ctype, authority="FDA",
            severity=severity, date=date, company=company,
            summary=subject, gmp_category=gmp_cat, raw_text=raw,
        ))
    return results


# ---------------------------------------------------------------------------
# Scraper 5: FDA 483 Inspection Observations
# ---------------------------------------------------------------------------
_FDA_483_URL = (
    "https://www.fda.gov/food/compliance-enforcement-food"
    "/fda-food-inspection-observation-detail-report"
)

def scrape_fda_483(cutoff: datetime) -> list[Citation]:
    """
    Fetch FDA 483 inspection citations from the observation detail report page.
    The page links to Excel/PDF reports; we parse what's available in HTML.
    """
    logger.info("FDA: fetching 483 inspection observations …")
    results: list[Citation] = []
    try:
        resp = http_get(_FDA_483_URL)
        soup = BeautifulSoup(resp.text, "lxml")
        # Look for links to inspection reports or embedded tables
        tables = soup.find_all("table")
        for table in tables:
            rows = table.select("tbody tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) < 3:
                    continue
                date_str   = cells[0].get_text(strip=True)
                date       = parse_date(date_str)
                if date and date < cutoff:
                    continue
                observation = " ".join(c.get_text(strip=True) for c in cells)
                if not observation.strip():
                    continue
                ingredient = extract_ingredient(observation)
                gmp_cat    = extract_gmp_category(observation)
                severity   = infer_severity(observation, "inspection_finding")
                link_el    = row.find("a")
                url        = urljoin("https://www.fda.gov", link_el["href"]) if link_el else _FDA_483_URL
                company    = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                results.append(Citation(
                    id=make_id("FDA_483", url + observation[:30]),
                    title=observation[:150],
                    url=url, ingredient=ingredient,
                    citation_type="inspection_finding",
                    authority="FDA", severity=severity,
                    date=date, company=company,
                    summary=observation[:300],
                    gmp_category=gmp_cat or "CGMP Non-compliance",
                    raw_text=observation,
                ))

        # Also look for downloadable file links and note them
        for link in soup.select("a[href]"):
            href = link.get("href", "")
            if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv"]):
                logger.info("FDA 483: found downloadable report at %s", href)
                # Try to fetch CSV/Excel — placeholder for future implementation
                break

    except Exception as exc:
        logger.warning("FDA 483 scraper error: %s", exc)

    logger.info("FDA 483: %d inspection citations", len(results))
    return results


# ---------------------------------------------------------------------------
# Scraper 6: EFSA News & Opinions
# ---------------------------------------------------------------------------
_EFSA_NEWS_URL = "https://www.efsa.europa.eu/en/news"
_SUPPLEMENT_TERMS_EFSA = re.compile(
    r"vitamin|mineral|supplement|botanical|herbal|nutrient|food safety|"
    r"contaminant|pesticide|heavy metal|mycotoxin|probiotic|health claim",
    re.I,
)

def scrape_efsa_news(cutoff: datetime) -> list[Citation]:
    """Scrape EFSA news listing for supplement-relevant items."""
    logger.info("EFSA: fetching news …")
    results: list[Citation] = []
    try:
        resp = http_get(_EFSA_NEWS_URL)
        soup = BeautifulSoup(resp.text, "lxml")
        # EFSA uses a listing with <article> or <li> items
        items = (
            soup.select("article")
            or soup.select(".listing__item")
            or soup.select(".news-item")
            or soup.select("li.node--type-news")
        )
        logger.info("EFSA: %d items found", len(items))
        for item in items:
            link = item.find("a", href=True)
            if not link:
                continue
            title = link.get_text(strip=True) or item.find(re.compile(r"h[23]")).get_text(strip=True) if item.find(re.compile(r"h[23]")) else ""
            if not title:
                title = item.get_text(strip=True)[:120]
            href  = link.get("href", "")
            url   = href if href.startswith("http") else "https://www.efsa.europa.eu" + href
            date  = extract_date_from_tag(item)
            if date and date < cutoff:
                continue
            teaser_el = item.select_one("p, .field--name-field-summary, .summary")
            summary   = teaser_el.get_text(" ", strip=True) if teaser_el else ""
            raw       = f"{title} {summary}"
            if not _SUPPLEMENT_TERMS_EFSA.search(raw):
                continue
            ingredient    = extract_ingredient(raw)
            citation_type = "safety_alert" if any(w in raw.lower() for w in ["risk", "hazard", "unsafe", "warning", "recall"]) else "safety_alert"
            severity      = infer_severity(raw, citation_type)
            results.append(Citation(
                id=make_id("EFSA", url),
                title=title, url=url, ingredient=ingredient,
                citation_type=citation_type, authority="EFSA",
                severity=severity, date=date, summary=summary, raw_text=raw,
            ))
    except Exception as exc:
        logger.warning("EFSA news scraper error: %s", exc)
    logger.info("EFSA: %d citations", len(results))
    return results


# ---------------------------------------------------------------------------
# Scraper 7: EMA GMP Non-compliance Statements via EudraGMDP
# ---------------------------------------------------------------------------
_EMA_BASE = "https://www.ema.europa.eu"
_EMA_URLS = [
    # EMA news (primary listing)
    f"{_EMA_BASE}/en/news",
    # HMPC herbal opinions
    f"{_EMA_BASE}/en/medicines/herbal-medicines",
]

_EMA_GMP_SEARCH = (
    "https://www.ema.europa.eu/en/search"
    "?search_api_fulltext=GMP+non-compliance+supplement+ingredient"
    "&f[0]=ema_type:human_medicine"
)

_EMA_SUPPLEMENT_TERMS = re.compile(
    r"vitamin|mineral|supplement|botanical|herbal|nutrient|food|"
    r"GMP|gmp|non-compliance|inspection|manufacturing|recall|withdrawal|"
    r"contamination|heavy metal|mycotoxin|probiotic|omega",
    re.I,
)


def scrape_ema_noncompliance(cutoff: datetime) -> list[Citation]:
    """
    Scrape EMA for GMP non-compliance, inspection findings, and
    supplement/herbal medicine-related regulatory actions.
    Tries multiple EMA pages since the direct non-compliance list URL
    has changed structure.
    """
    logger.info("EMA: fetching non-compliance & inspection data …")
    results: list[Citation] = []

    # --- Attempt 1: EMA news page filtered for GMP/supplement terms ---
    ema_news_urls = [
        "https://www.ema.europa.eu/en/news-events/news",
        "https://www.ema.europa.eu/en/news-events/latest-news",
    ]
    for url in [f"{_EMA_BASE}/en/news"]:
        try:
            resp = http_get(url)
            if resp.status_code >= 400:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            items = (
                soup.select("article")
                or soup.select(".ecl-content-item")
                or soup.select(".listing-item")
                or soup.select("li.ecl-list__item")
                or soup.select(".views-row")
            )
            logger.info("EMA news: %d items at %s", len(items), url)
            for item in items:
                # EMA: title in <h3>/<h4> with link, or directly in <a>
                heading = item.find(re.compile(r"h[2-4]"))
                link    = (heading.find("a", href=True) if heading else None) or item.find("a", href=True)
                if not link:
                    continue
                title = (heading.get_text(strip=True) if heading else "") or link.get_text(strip=True)
                if not title or len(title) < 10:
                    continue
                href     = link.get("href", "")
                item_url = href if href.startswith("http") else f"{_EMA_BASE}{href}"
                date  = extract_date_from_tag(item)
                if date and date < cutoff:
                    continue
                teaser_el = item.select_one("p, .ecl-content-item__description, .summary")
                summary   = teaser_el.get_text(" ", strip=True) if teaser_el else ""
                raw       = f"{title} {summary}"
                if not _EMA_SUPPLEMENT_TERMS.search(raw):
                    continue
                ingredient  = extract_ingredient(raw)
                gmp_cat     = extract_gmp_category(raw) or ("CGMP Non-compliance" if "GMP" in raw.upper() else "")
                ctype       = "GMP_violation" if gmp_cat else "inspection_finding"
                severity    = infer_severity(raw, ctype)
                results.append(Citation(
                    id=make_id("EMA_news", item_url),
                    title=title, url=item_url, ingredient=ingredient,
                    citation_type=ctype, authority="EMA",
                    severity=severity, date=date,
                    summary=summary, gmp_category=gmp_cat, raw_text=raw,
                ))
            if results:
                break
        except Exception as exc:
            logger.debug("EMA news scraper error for %s: %s", url, exc)

    # --- Attempt 2: HMPC herbal opinions ---
    try:
        resp = http_get("https://www.ema.europa.eu/en/medicines/herbal-medicines")
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "lxml")
            items = soup.select("article, .views-row, .ecl-content-item")
            for item in items[:30]:
                link = item.find("a", href=True)
                if not link:
                    continue
                title = link.get_text(strip=True)
                href  = link.get("href", "")
                item_url = href if href.startswith("http") else "https://www.ema.europa.eu" + href
                date  = extract_date_from_tag(item)
                if date and date < cutoff:
                    continue
                raw = title
                ingredient = extract_ingredient(raw)
                if ingredient == "Unknown":
                    continue
                results.append(Citation(
                    id=make_id("EMA_herbal", item_url),
                    title=title, url=item_url, ingredient=ingredient,
                    citation_type="safety_alert", authority="EMA",
                    severity=infer_severity(raw, "safety_alert"),
                    date=date, summary=title, raw_text=raw,
                ))
    except Exception as exc:
        logger.debug("EMA herbal medicines scraper error: %s", exc)

    logger.info("EMA: %d citations", len(results))
    return results


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------
def deduplicate(citations: list[Citation]) -> list[Citation]:
    """Remove exact-ID duplicates; keep the first occurrence."""
    seen: set[str] = set()
    out: list[Citation] = []
    for c in citations:
        if c.id not in seen:
            seen.add(c.id)
            out.append(c)
    return out


# ---------------------------------------------------------------------------
# Detail fetching — enrich citations with actual finding text
# ---------------------------------------------------------------------------

# Patterns to locate the violations / findings paragraph in FDA letters
_VIOLATION_ANCHOR = re.compile(
    r"(?:significant violations?|violations? are as follows|findings? are as follows"
    r"|observations? are as follows|during (?:our|the) inspection"
    r"|we observed the following|the following observations?"
    r"|you failed to|failure to|failed to (?:establish|maintain|implement|develop|document)"
    r"|were not in compliance|is not in compliance|did not (?:develop|establish|maintain)"
    r"|you have not|following deficiencies)[:\s]+",
    re.I,
)
_PARAGRAPH_END = re.compile(r"\.\s+(?=[A-Z]|\d+\.)")


def fetch_fda_wl_finding(citation: Citation) -> str:
    """
    Fetch an FDA warning letter page and extract the key violation text.
    Returns a 2–4 sentence summary of the specific finding.
    Falls back to the subject line if the page can't be fetched.
    """
    if citation.url == _FDA_WL_PAGE or not citation.url.startswith("http"):
        return citation.summary

    try:
        resp = get_session().get(citation.url, timeout=20)
        if resp.status_code != 200:
            return citation.summary
        soup  = BeautifulSoup(resp.text, "lxml")
        main  = soup.select_one("main, article, .main-content, #main-content")
        if not main:
            return citation.summary
        text  = main.get_text(" ", strip=True)

        # Clean standard FDA FOIA redactions for readability
        text = re.sub(r"\(b\)\(\d+\)", "[redacted]", text)
        text = re.sub(r"\s{2,}", " ", text)

        # Extract violations section
        m = _VIOLATION_ANCHOR.search(text)
        if m:
            # Include the anchor phrase itself for context
            start    = max(0, m.start())
            snippet  = text[start:start + 900].strip()
            # Cut at a natural sentence boundary (no more than 5 sentences)
            sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z])", snippet)
            finding   = " ".join(sentences[:5]).strip()
            return finding[:700] if finding else citation.summary

        # Fallback 1: first paragraph containing "violation" or "inspection" or "failed"
        for keyword in ("violation", "inspection finding", "inspection of", "failed to", "you have not"):
            idx = text.lower().find(keyword)
            if idx >= 0:
                # Go back to start of sentence
                start = max(0, text.rfind(". ", max(0, idx - 200), idx) + 2)
                snippet = text[start:start + 600].strip()
                sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z])", snippet)
                good = [s.strip() for s in sentences if len(s.strip()) >= 40 and " " in s]
                if good:
                    return good[0][:600]

        # Fallback 2: body text after salutation
        dear_idx = text.find("Dear ")
        if dear_idx >= 0:
            # Skip the salutation line itself
            after_dear = text[dear_idx:dear_idx + 800]
            paragraphs = [p.strip() for p in after_dear.split("  ") if len(p.strip()) > 60]
            if len(paragraphs) > 1:
                return paragraphs[1][:400]

        return citation.summary
    except Exception:
        return citation.summary


def fetch_tga_finding(citation: Citation) -> str:
    """Fetch TGA alert page and return first substantive paragraph."""
    if not citation.url.startswith("http"):
        return citation.summary
    try:
        resp = get_session().get(citation.url, timeout=20)
        if resp.status_code != 200:
            return citation.summary
        soup = BeautifulSoup(resp.text, "lxml")
        main = soup.select_one("main, article, .field--name-body, .node__content")
        if not main:
            return citation.summary
        paragraphs = [p.get_text(" ", strip=True) for p in main.find_all("p") if len(p.get_text(strip=True)) > 60]
        return " ".join(paragraphs[:3])[:600] if paragraphs else citation.summary
    except Exception:
        return citation.summary


def enrich_citations(citations: list[Citation], max_workers: int = 8) -> list[Citation]:
    """
    Parallel-fetch full finding text for all citations that need enrichment.
    FDA WL: fetch actual letter.  TGA: fetch article page.
    Others (OpenFDA, EFSA) already have good summaries.
    """
    to_fetch = [
        c for c in citations
        if c.authority in ("FDA",) and c.citation_type in ("warning_letter", "GMP_violation")
        and c.url != _FDA_WL_PAGE
    ]
    tga_fetch = [
        c for c in citations
        if c.authority == "TGA" and not c.summary or len(c.summary) < 80
    ]

    logger.info("Enriching %d FDA warning letters + %d TGA alerts (parallel fetch) …",
                len(to_fetch), len(tga_fetch))

    def do_fetch(c: Citation) -> tuple[str, str]:
        if c.authority == "FDA":
            return c.id, fetch_fda_wl_finding(c)
        else:
            return c.id, fetch_tga_finding(c)

    all_to_fetch = to_fetch + tga_fetch
    findings: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(do_fetch, c): c for c in all_to_fetch}
        done = 0
        for fut in as_completed(futures):
            try:
                cid, text = fut.result()
                if text:
                    findings[cid] = text
            except Exception:
                pass
            done += 1
            if done % 10 == 0:
                logger.info("  … enriched %d/%d", done, len(all_to_fetch))

    # Apply enriched findings back to citations
    enriched = []
    for c in citations:
        if c.id in findings and findings[c.id]:
            # Rebuild dataclass with updated summary and gmp_category
            new_summary = findings[c.id]
            new_gmp = extract_gmp_category(new_summary) or c.gmp_category
            enriched.append(Citation(
                id=c.id, title=c.title, url=c.url, ingredient=c.ingredient,
                citation_type=c.citation_type, authority=c.authority,
                severity=c.severity, date=c.date, company=c.company,
                summary=new_summary, gmp_category=new_gmp, raw_text=c.raw_text,
            ))
        else:
            enriched.append(c)

    logger.info("Enrichment complete. %d citations updated.", len(findings))
    return enriched


# ---------------------------------------------------------------------------
# Analysis engine
# ---------------------------------------------------------------------------
def analyse(citations: list[Citation]) -> dict:
    """Compute all analytics needed for the report sections."""
    recent   = [c for c in citations if c.is_recent]
    earlier  = [c for c in citations if not c.is_recent]

    # --- Ingredient frequency ---
    ing_all    = Counter(c.ingredient for c in citations if c.ingredient != "Unknown")
    ing_recent = Counter(c.ingredient for c in recent    if c.ingredient != "Unknown")
    ing_early  = Counter(c.ingredient for c in earlier   if c.ingredient != "Unknown")

    # Per-authority breakdown per ingredient
    ing_by_auth: dict[str, Counter] = defaultdict(Counter)
    for c in citations:
        if c.ingredient != "Unknown":
            ing_by_auth[c.ingredient][c.authority] += 1

    # --- Top ingredients with authority breakdown ---
    top_ingredients = []
    for ing, total in ing_all.most_common(20):
        auth_counts = ing_by_auth[ing]
        # Trend: recent vs earlier (normalised per day)
        r_count = ing_recent.get(ing, 0)
        e_count = ing_early.get(ing, 0)
        # Trending if recent rate > 1.5× earlier rate (per-day basis)
        recent_rate = r_count / 30
        early_rate  = e_count / 60
        if early_rate == 0:
            trend = "↑" if r_count > 0 else "—"
        elif recent_rate >= early_rate * 1.5:
            trend = "↑"
        elif recent_rate <= early_rate * 0.5:
            trend = "↓"
        else:
            trend = "→"
        # Multi-jurisdiction flag
        multi_juris = len([v for v in auth_counts.values() if v > 0]) >= 2
        top_ingredients.append({
            "ingredient": ing,
            "tga": auth_counts.get("TGA", 0),
            "fda": auth_counts.get("FDA", 0),
            "efsa": auth_counts.get("EFSA", 0),
            "ema": auth_counts.get("EMA", 0),
            "total": total,
            "trend": trend,
            "multi_jurisdiction": multi_juris,
        })

    # --- Citation type breakdown ---
    type_counts = Counter(c.citation_type for c in citations)

    # --- Severity breakdown ---
    severity_counts = Counter(c.severity for c in citations)

    # --- Authority breakdown ---
    authority_counts = Counter(c.authority for c in citations)

    # --- GMP violation categories ---
    gmp_cats = Counter(
        c.gmp_category for c in citations
        if c.gmp_category and c.citation_type in ("GMP_violation", "inspection_finding")
    )

    # --- Cross-jurisdiction flags ---
    cross_juris_ingredients = [
        row for row in top_ingredients if row["multi_jurisdiction"]
    ]

    # --- Warning letter targets ---
    wl_companies: Counter = Counter()
    wl_details: dict[str, list] = defaultdict(list)
    for c in citations:
        if c.citation_type in ("warning_letter", "GMP_violation") and c.company:
            wl_companies[c.company] += 1
            wl_details[c.company].append({
                "title": c.title,
                "url": c.url,
                "date": c.date_str,
                "ingredient": c.ingredient,
                "gmp_category": c.gmp_category,
                "summary": c.summary,
            })

    # --- Trending ingredients (last 30 days spike) ---
    trending = [
        row for row in top_ingredients
        if row["trend"] == "↑" and (ing_recent.get(row["ingredient"], 0) >= 2
                                      or ing_all.get(row["ingredient"], 0) >= 3)
    ]

    # --- Low-activity ingredients (cited in prior 60 but not recent 30) ---
    low_activity = [
        {"ingredient": ing, "last_date": max(
            (c.date for c in citations if c.ingredient == ing and c.date is not None),
            default=None,
        )}
        for ing in ing_early
        if ing not in ing_recent and ing_early[ing] >= 2
    ]
    low_activity.sort(key=lambda x: x["last_date"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)

    # --- Executive summary stats ---
    total = len(citations)
    high_count = severity_counts.get("high", 0)
    n_authorities = len(authority_counts)
    n_multi = len(cross_juris_ingredients)
    top_ing = top_ingredients[0]["ingredient"] if top_ingredients else "None"

    return {
        "total": total,
        "high_count": high_count,
        "n_authorities": n_authorities,
        "n_multi_jurisdiction": n_multi,
        "top_ingredient": top_ing,
        "top_ingredients": top_ingredients,
        "type_counts": type_counts,
        "severity_counts": severity_counts,
        "authority_counts": authority_counts,
        "gmp_cats": gmp_cats,
        "cross_juris_ingredients": cross_juris_ingredients,
        "wl_companies": wl_companies,
        "wl_details": wl_details,
        "trending": trending,
        "low_activity": low_activity[:10],
        "recent_count": len(recent),
    }


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------
BRAND = {
    "bg_primary":   "#0D1B2A",
    "bg_secondary": "#0F2336",
    "border":       "#1E3A5F",
    "teal":         "#0D9488",
    "teal_dark":    "#0D3D35",
    "text_primary": "#F8FAFC",
    "text_mid":     "#E2E8F0",
    "text_muted":   "#94A3B8",
    "text_dim":     "#64748B",
    "high":         "#DC2626",
    "medium":       "#D97706",
    "low":          "#059669",
    "multi_bg":     "#0A2E2B",
    "multi_border": "#0D9488",
}


def _severity_badge(sev: str) -> str:
    colour = {"high": BRAND["high"], "medium": BRAND["medium"], "low": BRAND["low"]}.get(sev, "#64748B")
    return f'<span style="background:{colour};color:#fff;padding:3px 10px;border-radius:999px;font-size:11px;font-weight:700;letter-spacing:.04em">{sev.upper()}</span>'


def _authority_badge(auth: str) -> str:
    return f'<span style="background:{BRAND["teal_dark"]};color:{BRAND["teal"]};padding:3px 9px;border-radius:999px;font-size:11px;font-weight:600">{auth}</span>'


def _trend_arrow(trend: str) -> str:
    colours = {"↑": "#0D9488", "↓": "#DC2626", "→": "#D97706", "—": "#475569"}
    return f'<span style="color:{colours.get(trend,"#64748B")};font-weight:700;font-size:16px">{trend}</span>'


def _section_header(title: str, badge_text: str = "") -> str:
    badge = (
        f'<span style="background:{BRAND["teal_dark"]};color:{BRAND["teal"]};'
        f'padding:3px 10px;border-radius:999px;font-size:11px;font-weight:600;margin-left:10px">'
        f'{badge_text}</span>'
    ) if badge_text else ""
    return f"""
<div style="display:flex;align-items:center;margin-bottom:20px;padding-bottom:10px;border-bottom:1px solid {BRAND['border']}">
  <div style="width:4px;height:22px;background:{BRAND['teal']};border-radius:2px;margin-right:12px;flex-shrink:0"></div>
  <span style="font-size:15px;font-weight:700;color:{BRAND['text_mid']};letter-spacing:.01em">{title}</span>{badge}
</div>"""


def generate_html(citations: list[Citation], analysis: dict) -> str:
    B = BRAND
    total       = analysis["total"]
    high_count  = analysis["high_count"]
    sev_counts  = analysis["severity_counts"]
    auth_counts = analysis["authority_counts"]
    type_counts = analysis["type_counts"]

    # ------------------------------------------------------------------
    # a) Executive summary
    # ------------------------------------------------------------------
    top5 = ", ".join(r["ingredient"] for r in analysis["top_ingredients"][:5])
    n_multi = analysis["n_multi_jurisdiction"]
    n_recent = analysis["recent_count"]
    exec_summary = (
        f"In the 90-day period to {REPORT_DATE_STR}, automated monitoring identified <strong>{total} regulatory citations</strong> "
        f"across {analysis['n_authorities']} authorities (TGA, FDA, EFSA, EMA), with <strong>{high_count} high-severity actions</strong> "
        f"requiring immediate attention. "
        f"The most-cited ingredients were <strong>{top5}</strong>, with <strong>{n_multi} ingredient(s)</strong> flagged across multiple jurisdictions — "
        f"representing the highest regulatory risk profile. "
        f"Citation activity <strong>{'increased' if n_recent > (total - n_recent) * 0.6 else 'remained steady'}</strong> "
        f"in the most recent 30-day window ({n_recent} of {total} citations), "
        f"with {type_counts.get('GMP_violation', 0) + type_counts.get('inspection_finding', 0)} manufacturing/inspection citations "
        f"indicating sustained pharma audit pressure on the supplement supply chain."
    )

    # ------------------------------------------------------------------
    # b) Top cited ingredients — grouped with actual citation links
    # ------------------------------------------------------------------
    # Build ingredient → citations lookup
    ing_citations: dict[str, list[Citation]] = defaultdict(list)
    for c in citations:
        if c.ingredient != "Unknown":
            ing_citations[c.ingredient].append(c)

    CTYPE_SHORT = {
        "safety_alert": "Safety Alert", "recall": "Recall",
        "warning_letter": "Warning Letter", "GMP_violation": "GMP Violation",
        "inspection_finding": "Inspection", "ban": "Ban",
    }

    ingredients_table = ""
    for i, row in enumerate(analysis["top_ingredients"], 1):
        ing   = row["ingredient"]
        cits  = sorted(ing_citations.get(ing, []),
                       key=lambda c: c.date or datetime.min.replace(tzinfo=timezone.utc),
                       reverse=True)
        multi_badge = (
            f'<span style="background:{B["teal_dark"]};color:{B["teal"]};padding:2px 7px;'
            f'border-radius:4px;font-size:10px;font-weight:700;margin-left:8px">MULTI-JURIS</span>'
        ) if row["multi_jurisdiction"] else ""
        row_bg = f"background:{B['multi_bg']};" if row["multi_jurisdiction"] else f"background:{B['bg_primary']};"

        # Authority count pills
        auth_pills = " ".join(
            f'<span style="background:{B["bg_secondary"]};border:1px solid {B["border"]};'
            f'color:{B["text_muted"]};padding:2px 9px;border-radius:999px;font-size:11px;font-weight:600">'
            f'{a} {row[a.lower()]}</span>'
            for a in ["TGA", "FDA", "EFSA", "EMA"] if row[a.lower()] > 0
        )

        # Citation rows
        cit_rows = ""
        for c in cits:
            sev_col  = {"high": B["high"], "medium": B["medium"], "low": B["low"]}.get(c.severity, B["medium"])
            ctype    = CTYPE_SHORT.get(c.citation_type, c.citation_type)
            # Short finding — first sentence only
            finding  = ""
            if c.summary:
                first = re.split(r"(?<=[.!?])\s+(?=[A-Z])", c.summary.strip())
                finding = first[0][:180] if first else c.summary[:180]
            cit_rows += f"""
<tr style="border-top:1px solid {B['border']}">
  <td style="padding:8px 12px 8px 28px;width:80px">
    <span style="background:{sev_col}22;color:{sev_col};border:1px solid {sev_col}55;
                 padding:2px 7px;border-radius:4px;font-size:10px;font-weight:700;
                 white-space:nowrap">{c.severity.upper()}</span>
  </td>
  <td style="padding:8px 12px">
    <a href="{c.url}" target="_blank"
       style="font-size:13px;color:{B['text_mid']};font-weight:600;text-decoration:none;
              line-height:1.35;display:block"
       onmouseover="this.style.color='{B['teal']}'" onmouseout="this.style.color='{B['text_mid']}'"
    >{c.title}</a>
    {(f'<div style="font-size:11px;color:' + B['text_dim'] + ';margin-top:3px;line-height:1.45">' + finding + '</div>') if finding else ""}
  </td>
  <td style="padding:8px 12px;white-space:nowrap;vertical-align:top">
    {_authority_badge(c.authority)}
  </td>
  <td style="padding:8px 12px;white-space:nowrap;vertical-align:top">
    <span style="font-size:11px;color:{B['text_dim']}">{ctype}</span>
  </td>
  <td style="padding:8px 12px;white-space:nowrap;vertical-align:top;font-size:11px;color:{B['text_dim']}">{c.date_str}</td>
</tr>"""

        ingredients_table += f"""
<div style="{row_bg}border:1px solid {B['border']};border-radius:8px;margin-bottom:12px;overflow:hidden">
  <div style="padding:12px 16px;display:flex;align-items:center;gap:12px;flex-wrap:wrap;
              border-bottom:1px solid {B['border']}">
    <span style="font-size:13px;font-weight:700;color:{B['text_dim']};min-width:24px">{i}</span>
    <span style="font-size:15px;font-weight:700;color:{B['text_primary']}">{ing}</span>
    {multi_badge}
    <span style="margin-left:auto;display:flex;gap:6px;flex-wrap:wrap">{auth_pills}</span>
    <span style="display:flex;align-items:center;gap:6px">
      <span style="font-size:20px;font-weight:800;color:{B['teal']}">{row['total']}</span>
      <span style="font-size:11px;color:{B['text_dim']}">citations</span>
      {_trend_arrow(row['trend'])}
    </span>
  </div>
  <table style="width:100%;border-collapse:collapse">
    <tbody>{cit_rows}</tbody>
  </table>
</div>"""

    if not ingredients_table:
        ingredients_table = f'<p style="color:{B["text_dim"]};font-size:13px">No ingredient-specific citations in the 90-day window.</p>'

    ingredients_table += f"""
<div style="margin-top:8px;font-size:11px;color:{B['text_dim']}">
  <span style="background:{B['multi_bg']};border-left:3px solid {B['teal']};padding:2px 8px;
               border-radius:2px;margin-right:8px">MULTI-JURIS</span>
  Ingredient flagged by 2+ regulatory authorities — elevated risk profile
</div>"""

    # ------------------------------------------------------------------
    # c) Pharma audit hotspots — GMP/inspection bar chart
    # ------------------------------------------------------------------
    gmp_cats  = analysis["gmp_cats"]
    max_count = max(gmp_cats.values(), default=1)
    gmp_bars  = ""
    for cat, count in gmp_cats.most_common(10):
        pct  = int(count / max_count * 100)
        gmp_bars += f"""
<div style="margin-bottom:14px">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:5px">
    <span style="font-size:13px;color:{B['text_mid']};font-weight:500">{cat}</span>
    <span style="font-size:13px;color:{B['teal']};font-weight:700">{count}</span>
  </div>
  <div style="background:{B['bg_primary']};border-radius:4px;height:8px;overflow:hidden">
    <div style="background:{B['teal']};width:{pct}%;height:100%;border-radius:4px;transition:width .3s"></div>
  </div>
</div>"""

    if not gmp_bars:
        gmp_bars = f'<p style="color:{B["text_dim"]};font-size:13px">No GMP/inspection citations found in the 90-day window.</p>'

    # Type-of-citation breakdown pills
    type_pills = ""
    type_labels = {
        "safety_alert": "Safety Alert", "recall": "Recall",
        "warning_letter": "Warning Letter", "GMP_violation": "GMP Violation",
        "inspection_finding": "Inspection Finding", "ban": "Ban",
    }
    for ct, count in type_counts.most_common():
        type_pills += (
            f'<span style="background:{B["bg_primary"]};border:1px solid {B["border"]};'
            f'color:{B["text_muted"]};padding:5px 14px;border-radius:999px;font-size:12px;'
            f'font-weight:600;display:inline-block;margin:0 4px 6px 0">'
            f'{type_labels.get(ct, ct)} <strong style="color:{B["teal"]}">{count}</strong></span>'
        )

    # ------------------------------------------------------------------
    # d) Cross-jurisdiction flags — priority cards
    # ------------------------------------------------------------------
    cross_cards = ""
    for row in analysis["cross_juris_ingredients"][:8]:
        authorities = [a for a in ["TGA", "FDA", "EFSA", "EMA"] if row[a.lower()] > 0]
        auth_badges = " ".join(_authority_badge(a) for a in authorities)
        cross_cards += f"""
<div style="background:{B['multi_bg']};border:1px solid {B['multi_border']};border-radius:8px;
            padding:16px 20px;margin-bottom:14px">
  <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;margin-bottom:8px">
    <span style="font-size:16px;font-weight:700;color:{B['text_primary']}">{row['ingredient']}</span>
    <span style="background:{B['high']};color:#fff;padding:4px 12px;border-radius:999px;font-size:11px;font-weight:700">HIGH RISK</span>
  </div>
  <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px">{auth_badges}</div>
  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:8px">
    {"".join(f'<div style="background:{B["bg_secondary"]};border-radius:6px;padding:8px;text-align:center"><div style="font-size:20px;font-weight:800;color:{B["teal"]}">{row[a.lower()]}</div><div style="font-size:10px;color:{B["text_dim"]};text-transform:uppercase;letter-spacing:.04em">{a}</div></div>' for a in ["TGA","FDA","EFSA","EMA"])}
  </div>
  <div style="margin-top:10px;display:flex;align-items:center;gap:8px">
    <span style="font-size:12px;color:{B['text_dim']}">Total citations:</span>
    <span style="font-size:14px;font-weight:700;color:{B['text_primary']}">{row['total']}</span>
    <span style="margin-left:8px;font-size:12px;color:{B['text_dim']}">Trend:</span>
    {_trend_arrow(row['trend'])}
  </div>
</div>"""

    if not cross_cards:
        cross_cards = f'<p style="color:{B["text_dim"]};font-size:13px">No cross-jurisdiction ingredients identified in the 90-day window.</p>'

    # ------------------------------------------------------------------
    # e) Warning letter targets
    # ------------------------------------------------------------------
    wl_rows = ""
    for company, count in analysis["wl_companies"].most_common(15):
        details    = analysis["wl_details"][company]
        latest     = details[0] if details else {}
        latest_url = latest.get("url", "#")

        # Unique GMP categories across all letters for this company
        gmp_cats_co = ", ".join(filter(None, dict.fromkeys(
            d.get("gmp_category", "") for d in details
        )))[:120] or "—"

        # Pull the most informative sentence from the finding text.
        raw_summary = latest.get("summary", "") or ""
        title_str   = latest.get("title", "")

        # Remove boilerplate lead-ins
        clean = re.sub(
            r"^.*?(?:violations? are as follows|findings? are as follows"
            r"|observations? are as follows|significant violations? (?:include|were)[^:]*:)[:\s]+",
            "", raw_summary, flags=re.I,
        ).strip()

        # Split into sentences; keep only ones that:
        #   • start with a capital letter (not mid-phrase)
        #   • are at least 40 chars (long enough to be a real sentence)
        #   • don't end with a bare colon, number, or list marker like ": 1."
        #   • contain at least one space (not a slash-separated category like "CGMP/QSR/Adulterated")
        candidates = re.split(r"(?<=[.!?])\s+(?=[A-Z\d])", clean)
        good = [
            s.strip() for s in candidates
            if len(s.strip()) >= 40
            and s.strip()[0].isupper()
            and " " in s.strip()
            and not re.search(r"[:\d]\s*$", s.strip())
            and not re.match(r"^[A-Z/]+/[A-Z]", s.strip())  # skip "CGMP/QSR/..." patterns
        ]

        if good:
            snippet = good[0][:220]
        elif clean and len(clean) >= 40 and " " in clean and not re.match(r"^[A-Z]+/", clean):
            snippet = clean[:220]
        elif raw_summary and len(raw_summary) > len(title_str):
            snippet = raw_summary[:220]
        else:
            # Last resort: parse subject from title (e.g. "Company — CGMP/Dietary Supplement/Adulterated")
            subj = re.sub(r"^[^—–-]+[—–-]\s*", "", title_str).strip()
            snippet = subj if subj and subj != title_str else ""

        wl_rows += f"""
<tr style="border-bottom:1px solid {B['border']}">
  <td style="padding:12px 12px;font-weight:600;color:{B['text_mid']};font-size:13px;vertical-align:top">
    <a href="{latest_url}" target="_blank" style="color:{B['text_mid']};text-decoration:none"
       onmouseover="this.style.color='{B['teal']}'" onmouseout="this.style.color='{B['text_mid']}'">{company}</a>
  </td>
  <td style="padding:12px 12px;color:{B['text_muted']};font-size:12px;white-space:nowrap;vertical-align:top">{latest.get('date','—')}</td>
  <td style="padding:12px 12px;color:{B['text_muted']};font-size:12px;vertical-align:top">{latest.get('ingredient','—')}</td>
  <td style="padding:12px 12px;vertical-align:top">
    <div style="font-size:11px;font-weight:700;color:{B['teal']};text-transform:uppercase;
                letter-spacing:.04em">{gmp_cats_co}</div>
    <div style="font-size:12px;color:{B['text_dim']};line-height:1.5;margin-top:4px">{snippet}</div>
  </td>
  <td style="padding:12px 12px;text-align:center;color:{B['teal']};font-weight:700;font-size:14px;vertical-align:top">{count}</td>
</tr>"""

    if not wl_rows:
        wl_rows = f'<tr><td colspan="5" style="padding:20px;text-align:center;color:{B["text_dim"]};font-size:13px">No warning letter targets identified in the 90-day window.</td></tr>'

    wl_table = f"""
<table style="width:100%;border-collapse:collapse">
  <thead>
    <tr style="background:{B['bg_primary']};border-bottom:2px solid {B['teal']}">
      <th style="padding:10px 12px;text-align:left;font-size:11px;color:{B['text_dim']};font-weight:600;letter-spacing:.06em;text-transform:uppercase">Company</th>
      <th style="padding:10px 12px;text-align:left;font-size:11px;color:{B['text_dim']};font-weight:600;letter-spacing:.06em;text-transform:uppercase">Latest Date</th>
      <th style="padding:10px 12px;text-align:left;font-size:11px;color:{B['text_dim']};font-weight:600;letter-spacing:.06em;text-transform:uppercase">Ingredient</th>
      <th style="padding:10px 12px;text-align:left;font-size:11px;color:{B['text_dim']};font-weight:600;letter-spacing:.06em;text-transform:uppercase">Citation Reason</th>
      <th style="padding:10px 12px;text-align:center;font-size:11px;color:{B['text_dim']};font-weight:600;letter-spacing:.06em;text-transform:uppercase">Citations</th>
    </tr>
  </thead>
  <tbody>{wl_rows}</tbody>
</table>"""

    # ------------------------------------------------------------------
    # f) Trending now
    # ------------------------------------------------------------------
    trending_cards = ""
    for row in analysis["trending"][:8]:
        trending_cards += f"""
<div style="background:{B['bg_primary']};border:1px solid {B['border']};border-radius:6px;
            padding:14px 16px;margin-bottom:10px;display:flex;align-items:center;gap:12px">
  <div style="flex:1">
    <div style="font-size:14px;font-weight:700;color:{B['text_mid']}">{row['ingredient']}</div>
    <div style="font-size:11px;color:{B['text_dim']};margin-top:3px">
      {row['tga']} TGA &nbsp;·&nbsp; {row['fda']} FDA &nbsp;·&nbsp; {row['efsa']} EFSA &nbsp;·&nbsp; {row['ema']} EMA
    </div>
  </div>
  <div style="text-align:right">
    <div style="font-size:11px;color:{B['text_dim']};margin-bottom:2px">30-day citations</div>
    <div style="font-size:22px;font-weight:800;color:{B['teal']}">{row['total']}</div>
  </div>
  {_trend_arrow("↑")}
</div>"""

    if not trending_cards:
        trending_cards = f'<p style="color:{B["text_dim"]};font-size:13px">No significant ingredient trends detected in the last 30 days.</p>'

    # ------------------------------------------------------------------
    # g) Low-activity ingredients
    # ------------------------------------------------------------------
    low_rows = ""
    for item in analysis["low_activity"][:10]:
        last_date = item["last_date"].strftime("%Y-%m-%d") if item["last_date"] else "—"
        low_rows += f"""
<div style="display:flex;justify-content:space-between;align-items:center;
            padding:10px 12px;border-bottom:1px solid {B['border']}">
  <span style="font-size:13px;color:{B['text_muted']}">{item['ingredient']}</span>
  <span style="font-size:12px;color:{B['text_dim']}">Last cited: {last_date}</span>
</div>"""

    if not low_rows:
        low_rows = f'<p style="color:{B["text_dim"]};font-size:13px;padding:12px">No low-activity ingredients identified.</p>'

    # ------------------------------------------------------------------
    # Severity + authority summary bar
    # ------------------------------------------------------------------
    sev_bar = (
        f'<span style="background:{B["high"]};color:#fff;padding:5px 14px;border-radius:999px;font-size:12px;font-weight:600">HIGH &nbsp;{sev_counts.get("high",0)}</span> '
        f'<span style="background:{B["medium"]};color:#fff;padding:5px 14px;border-radius:999px;font-size:12px;font-weight:600">MEDIUM &nbsp;{sev_counts.get("medium",0)}</span> '
        f'<span style="background:{B["low"]};color:#fff;padding:5px 14px;border-radius:999px;font-size:12px;font-weight:600">LOW &nbsp;{sev_counts.get("low",0)}</span>'
    )
    auth_bar = " ".join(
        f'<span style="background:{B["teal_dark"]};color:{B["teal"]};padding:5px 14px;border-radius:999px;font-size:12px;font-weight:600">{a} &nbsp;{n}</span>'
        for a, n in sorted(auth_counts.items())
    )

    # ------------------------------------------------------------------
    # h) Full Citation Index — every citation with source link + finding
    # ------------------------------------------------------------------
    CTYPE_LABEL = {
        "safety_alert": "Safety Alert", "recall": "Recall",
        "warning_letter": "Warning Letter", "GMP_violation": "GMP Violation",
        "inspection_finding": "Inspection Finding", "ban": "Ban",
    }
    CTYPE_COLOUR = {
        "safety_alert": B["high"], "recall": B["high"],
        "warning_letter": B["medium"], "GMP_violation": B["medium"],
        "inspection_finding": B["medium"], "ban": B["high"],
    }

    # Sort by authority, then date desc
    sorted_citations = sorted(
        citations,
        key=lambda c: (c.authority, -(c.date.timestamp() if c.date else 0)),
    )

    citation_cards = ""
    for c in sorted_citations:
        sev_col   = {"high": B["high"], "medium": B["medium"], "low": B["low"]}.get(c.severity, B["medium"])
        ctype_col = CTYPE_COLOUR.get(c.citation_type, B["medium"])
        ctype_lbl = CTYPE_LABEL.get(c.citation_type, c.citation_type)

        # Finding text — truncated with show-more handled by CSS
        finding   = (c.summary or c.title or "No detail available.").strip()
        # Highlight GMP category if present
        gmp_badge = (
            f'<span style="background:{B["bg_primary"]};border:1px solid {B["border"]};'
            f'color:{B["text_muted"]};padding:2px 8px;border-radius:4px;font-size:10px;'
            f'font-weight:600;margin-left:6px">{c.gmp_category}</span>'
        ) if c.gmp_category else ""

        company_line = (
            f'<span style="color:{B["text_dim"]};font-size:11px">'
            f'<strong style="color:{B["text_muted"]}">Company:</strong> {c.company}</span> &nbsp;·&nbsp; '
        ) if c.company else ""

        citation_cards += f"""
<div style="border-left:3px solid {sev_col};background:{B['bg_primary']};border-radius:0 6px 6px 0;
            padding:14px 18px;margin-bottom:10px;border:1px solid {B['border']};
            border-left:3px solid {sev_col}">
  <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:8px">
    <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
      {_severity_badge(c.severity)}
      {_authority_badge(c.authority)}
      <span style="background:{ctype_col}22;color:{ctype_col};border:1px solid {ctype_col}55;
                   padding:2px 8px;border-radius:4px;font-size:10px;font-weight:700;
                   letter-spacing:.04em;text-transform:uppercase">{ctype_lbl}</span>
      {gmp_badge}
    </div>
    <span style="font-size:11px;color:{B['text_dim']};white-space:nowrap;flex-shrink:0">{c.date_str}</span>
  </div>
  <div style="font-size:14px;font-weight:700;color:{B['text_mid']};line-height:1.35;margin-bottom:6px">
    <a href="{c.url}" target="_blank" style="color:{B['text_mid']};text-decoration:none"
       onmouseover="this.style.color='{B['teal']}'" onmouseout="this.style.color='{B['text_mid']}'">{c.title}</a>
  </div>
  <div style="font-size:11px;color:{B['text_dim']};margin-bottom:8px">
    {company_line}<strong style="color:{B['text_muted']}">Ingredient:</strong>
    <span style="color:{B['teal']};font-weight:600"> {c.ingredient}</span>
  </div>
  <div style="font-size:13px;color:{B['text_muted']};line-height:1.6;border-top:1px solid {B['border']};
              padding-top:8px;margin-top:4px">{finding}</div>
  <div style="margin-top:10px">
    <a href="{c.url}" target="_blank"
       style="color:{B['teal']};font-size:11px;font-weight:600;text-decoration:none">
      View source →
    </a>
  </div>
</div>"""

    # ------------------------------------------------------------------
    # Assemble full HTML
    # ------------------------------------------------------------------
    high_banner = (
        f'<div style="background:#7F1D1D;color:#fff;padding:14px 28px;display:flex;'
        f'align-items:center;gap:10px"><span style="font-size:20px">\u26a0\ufe0f</span>'
        f'<strong style="font-size:15px;letter-spacing:.01em">'
        f'{high_count} HIGH severity citations require immediate review.</strong></div>'
    ) if high_count > 0 else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Regulatory Intelligence Agent — {REPORT_DATE_STR}</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{ margin:0; padding:32px 16px; background:{B['bg_primary']};
           font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif; }}
    a {{ color:{B['teal']}; text-decoration:none; }}
    a:hover {{ text-decoration:underline; }}
    table {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif; }}
    tr:hover td {{ background:rgba(13,148,136,.06); }}
  </style>
</head>
<body>
<div style="max-width:860px;margin:0 auto">

  <!-- HEADER -->
  <div style="background:{B['bg_primary']};border-radius:10px 10px 0 0;border-bottom:3px solid {B['teal']};padding:18px 28px;min-height:80px;box-sizing:border-box">
    <div style="display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap">
      <div style="flex-shrink:0">
        <div style="font-size:22px;font-weight:700;color:{B['text_primary']};line-height:1.2;">Regulatory Intelligence Agent</div>
        <div style="font-size:12px;color:{B['text_dim']};margin-top:4px;">Automated monitoring &mdash; TGA, FDA, ARTG</div>
      </div>
      <div style="text-align:right;flex-shrink:0">
        <div style="font-size:12px;color:{B['text_muted']};letter-spacing:.04em;text-transform:uppercase">{REPORT_DATE_STR}</div>
        <div style="font-size:36px;font-weight:800;color:{B['text_primary']};line-height:1.1;margin-top:4px">{total}</div>
        <div style="font-size:13px;color:{B['text_muted']};margin-top:2px">Citations &nbsp;·&nbsp; 90-Day Window</div>
        <div style="font-size:11px;color:{B['text_dim']};margin-top:4px">{analysis['n_authorities']} Authorities &nbsp;·&nbsp; {analysis['n_multi_jurisdiction']} Cross-Jurisdiction Flags</div>
      </div>
    </div>
  </div>

  <!-- HIGH ALERT BANNER -->
  {high_banner}

  <!-- SEVERITY + AUTHORITY BAR -->
  <div style="background:{B['bg_secondary']};padding:14px 28px;border-bottom:1px solid {B['border']};display:flex;gap:8px;flex-wrap:wrap;align-items:center">
    <span style="font-size:11px;font-weight:600;color:{B['text_dim']};text-transform:uppercase;letter-spacing:.06em;margin-right:4px">Severity</span>
    {sev_bar}
    <span style="width:1px;height:20px;background:{B['border']};margin:0 8px"></span>
    <span style="font-size:11px;font-weight:600;color:{B['text_dim']};text-transform:uppercase;letter-spacing:.06em;margin-right:4px">Source</span>
    {auth_bar}
  </div>

  <!-- MAIN CONTENT -->
  <div style="background:{B['bg_secondary']};padding:28px;border-radius:0 0 10px 10px;border:1px solid {B['border']};border-top:none">

    <!-- a) Executive Summary -->
    <div style="margin-bottom:36px">
      {_section_header("Executive Summary — 90-Day Period")}
      <div style="background:{B['bg_primary']};border-radius:8px;padding:20px 24px;border:1px solid {B['border']}">
        <p style="font-size:15px;color:{B['text_mid']};line-height:1.65;margin:0">{exec_summary}</p>
        <div style="margin-top:16px;display:flex;gap:8px;flex-wrap:wrap">{type_pills}</div>
      </div>
    </div>

    <!-- b) Top Cited Ingredients -->
    <div style="margin-bottom:36px">
      {_section_header("Top Cited Ingredients", "Last 90 Days")}
      {ingredients_table}
    </div>

    <!-- c) Pharma Audit Hotspots -->
    <div style="margin-bottom:36px">
      {_section_header("Pharma Audit Hotspots", "GMP & Inspection Findings")}
      <div style="background:{B['bg_primary']};border-radius:8px;padding:20px 24px;border:1px solid {B['border']}">
        {gmp_bars}
      </div>
    </div>

    <!-- d) Cross-Jurisdiction Flags -->
    <div style="margin-bottom:36px">
      {_section_header("Cross-Jurisdiction Flags", f"{analysis['n_multi_jurisdiction']} Ingredients")}
      {cross_cards}
    </div>

    <!-- e) Warning Letter Targets -->
    <div style="margin-bottom:36px">
      {_section_header("Warning Letter Targets", "FDA Enforcement")}
      <div style="background:{B['bg_primary']};border-radius:8px;overflow:hidden;border:1px solid {B['border']}">
        {wl_table}
      </div>
    </div>

    <!-- f) Trending Now -->
    <div style="margin-bottom:36px">
      {_section_header("Trending Now", "Rising in Last 30 Days")}
      {trending_cards}
    </div>

    <!-- g) Low Activity -->
    <div style="margin-bottom:36px">
      {_section_header("Low Activity Ingredients", "Previously Cited, Now Quiet")}
      <div style="background:{B['bg_primary']};border-radius:8px;border:1px solid {B['border']};overflow:hidden">
        {low_rows}
      </div>
    </div>

    <!-- h) Full Citation Index -->
    <div style="margin-bottom:20px">
      {_section_header("Full Citation Index", f"{total} Citations — All Sources")}
      <p style="font-size:12px;color:{B['text_dim']};margin:0 0 16px 0">
        Every citation in the 90-day window with source link, actual finding text, and classification.
        Sorted by authority then date descending.
      </p>
      {citation_cards}
    </div>

  </div>

  <!-- FOOTER -->
  <div style="text-align:center;padding:20px;font-size:11px;color:{B['text_dim']}">
    {REPORT_DATE_STR} &nbsp;·&nbsp;
    Sources:
    <a href="https://www.tga.gov.au/safety/safety-monitoring-and-information/safety-alerts" target="_blank" style="color:{B['teal']}">TGA Safety Alerts</a> ·
    <a href="https://www.tga.gov.au/news/recalls-alerts-and-safety-advisories" target="_blank" style="color:{B['teal']}">TGA Recalls</a> ·
    <a href="https://api.fda.gov/food/enforcement.json" target="_blank" style="color:{B['teal']}">FDA OpenFDA</a> ·
    <a href="https://www.fda.gov/inspections-compliance-enforcement-and-criminal-investigations/compliance-actions-and-activities/warning-letters" target="_blank" style="color:{B['teal']}">FDA Warning Letters</a> ·
    <a href="https://www.efsa.europa.eu/en/news" target="_blank" style="color:{B['teal']}">EFSA</a> ·
    <a href="https://www.ema.europa.eu/en/news" target="_blank" style="color:{B['teal']}">EMA</a>
    &nbsp;·&nbsp; 90-day lookback
  </div>

</div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Console summary table
# ---------------------------------------------------------------------------
def print_summary_table(citations: list[Citation], analysis: dict) -> None:
    """Print a formatted summary table to stdout."""
    W = 80
    line = "─" * W
    print(f"\n{'SIGNALEX CITATION INTELLIGENCE REPORT':^{W}}")
    print(f"{'90-Day Window to ' + REPORT_DATE_STR:^{W}}")
    print(line)

    # Overview
    sev = analysis["severity_counts"]
    auth = analysis["authority_counts"]
    print(f"  Total citations : {analysis['total']}")
    print(f"  HIGH severity   : {sev.get('high', 0)}   MEDIUM: {sev.get('medium', 0)}   LOW: {sev.get('low', 0)}")
    print(f"  By authority    : " + "  ".join(f"{a}: {n}" for a, n in sorted(auth.items())))
    print(f"  Cross-juris     : {analysis['n_multi_jurisdiction']} ingredients flagged by 2+ authorities")
    print(line)

    # Top ingredients
    print(f"  {'TOP CITED INGREDIENTS':}")
    header = f"  {'#':<4}{'Ingredient':<28}{'TGA':>5}{'FDA':>5}{'EFSA':>6}{'EMA':>5}{'Total':>7}  Trend  Multi"
    print(header)
    print(f"  {'─'*70}")
    for i, row in enumerate(analysis["top_ingredients"][:15], 1):
        multi = " ★" if row["multi_jurisdiction"] else ""
        print(
            f"  {i:<4}{row['ingredient']:<28}"
            f"{row['tga']:>5}{row['fda']:>5}{row['efsa']:>6}{row['ema']:>5}"
            f"{row['total']:>7}  {row['trend']:^5}{multi}"
        )
    print(line)

    # Citation type breakdown
    print("  CITATION TYPE BREAKDOWN:")
    type_labels = {
        "safety_alert": "Safety Alert", "recall": "Recall",
        "warning_letter": "Warning Letter", "GMP_violation": "GMP Violation",
        "inspection_finding": "Inspection Finding", "ban": "Ban",
    }
    for ct, count in analysis["type_counts"].most_common():
        label = type_labels.get(ct, ct)
        bar   = "█" * min(count, 40)
        print(f"  {label:<25} {count:>4}  {bar}")
    print(line)

    # GMP hotspots
    if analysis["gmp_cats"]:
        print("  TOP GMP VIOLATION CATEGORIES:")
        for cat, count in analysis["gmp_cats"].most_common(6):
            bar = "█" * min(count, 30)
            print(f"  {cat:<40} {count:>3}  {bar}")
        print(line)

    # Cross-jurisdiction
    if analysis["cross_juris_ingredients"]:
        print("  CROSS-JURISDICTION FLAGS (Highest Risk):")
        for row in analysis["cross_juris_ingredients"][:5]:
            auths = [a for a in ["TGA","FDA","EFSA","EMA"] if row[a.lower()] > 0]
            print(f"  ★  {row['ingredient']:<28}  {' + '.join(auths)}  ({row['total']} citations)")
        print(line)

    # Trending
    if analysis["trending"]:
        print("  TRENDING NOW (Rising in Last 30 Days):")
        for row in analysis["trending"][:5]:
            print(f"  ↑  {row['ingredient']:<28}  {row['total']} total citations")
        print(line)

    print(f"\n  HTML report saved to: {OUTPUT_HTML}\n")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def main() -> None:
    logger.info("=" * 60)
    logger.info("Signalex Citation Intelligence Report")
    logger.info("90-day window: %s → %s",
                CUTOFF_90.strftime("%Y-%m-%d"), NOW.strftime("%Y-%m-%d"))
    logger.info("=" * 60)

    # --- Collect from all sources ---
    all_citations: list[Citation] = []

    logger.info("[1/6] TGA Safety Alerts")
    all_citations.extend(scrape_tga_alerts(CUTOFF_90))

    logger.info("[2/6] TGA Recalls & Advisories")
    all_citations.extend(scrape_tga_recalls(CUTOFF_90))

    logger.info("[3/6] FDA Enforcement (OpenFDA API)")
    all_citations.extend(scrape_fda_enforcement(CUTOFF_90))

    logger.info("[4/6] FDA Warning Letters")
    all_citations.extend(scrape_fda_warning_letters(CUTOFF_90))

    logger.info("[5/6] EFSA News & Opinions")
    all_citations.extend(scrape_efsa_news(CUTOFF_90))

    logger.info("[6/6] EMA Non-compliance Statements")
    all_citations.extend(scrape_ema_noncompliance(CUTOFF_90))

    logger.info("Raw citations collected: %d", len(all_citations))

    # --- Deduplicate ---
    citations = deduplicate(all_citations)
    logger.info("After deduplication: %d", len(citations))

    # --- Enrich: fetch actual finding text from letter/alert pages ---
    citations = enrich_citations(citations)

    # --- Analyse ---
    analysis = analyse(citations)

    # --- Generate HTML ---
    html = generate_html(citations, analysis)

    # Save to project root
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    logger.info("HTML report saved: %s", OUTPUT_HTML)

    # Also save a copy to reports/
    reports_copy = REPORTS_DIR / "citation_report.html"
    reports_copy.write_text(html, encoding="utf-8")

    # --- Print console summary ---
    print_summary_table(citations, analysis)


if __name__ == "__main__":
    main()
