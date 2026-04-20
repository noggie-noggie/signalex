"""
generate_signals.py — Generate the unified Signalex Intelligence Hub (signals.html).

Reads:
  - data/signals.db         → 30 regulatory/research/adverse-event signals
  - reports/citation_database.json → 2869 compliance citations

Outputs:
  signals.html — self-contained single-file dashboard with embedded JSON + inline JS.
"""

import json
from pathlib import Path
from datetime import datetime, timezone

# ── Load data ────────────────────────────────────────────────────────────────

BASE = Path(__file__).parent

# Signals from SQLite
from analytics.db import get_signals_since, get_conn
from analytics.trends import run_trend_detection
from analytics.sentiment import build_sentiment_report

signals_raw = get_signals_since(30)

# Compute analytics
trend_report     = run_trend_detection()
sentiment_report = build_sentiment_report()

# ── Enrichment helpers ───────────────────────────────────────────────────────

_GENERIC_ING = {
    "unknown", "general", "herbal and dietary supplements",
    "dietary supplement", "supplement", "supplements", "",
}

# Country patterns searched in summary + title text
_COUNTRY_PATTERNS = [
    ("bahrain", "🇧🇭", "Bahrain"), ("bahraini", "🇧🇭", "Bahrain"),
    ("united states", "🇺🇸", "USA"), (" u.s.", "🇺🇸", "USA"),
    ("u.s. health", "🇺🇸", "USA"), ("us health", "🇺🇸", "USA"),
    ("american ", "🇺🇸", "USA"),
    ("australia", "🇦🇺", "Australia"), ("australian", "🇦🇺", "Australia"),
    ("china", "🇨🇳", "China"), ("chinese", "🇨🇳", "China"),
    ("europe", "🇪🇺", "Europe"), ("european", "🇪🇺", "Europe"),
    ("japan", "🇯🇵", "Japan"), ("japanese", "🇯🇵", "Japan"),
    ("korea", "🇰🇷", "Korea"), ("korean", "🇰🇷", "Korea"),
    ("canada", "🇨🇦", "Canada"), ("canadian", "🇨🇦", "Canada"),
    ("uk ", "🇬🇧", "UK"), ("united kingdom", "🇬🇧", "UK"),
    ("india", "🇮🇳", "India"), ("indian", "🇮🇳", "India"),
    ("mice ", "🐭", "Animal study"), ("mouse ", "🐭", "Animal study"),
    ("rat ", "🐭", "Animal study"), ("murine", "🐭", "Animal study"),
]

_SOURCE_BASE_CONTEXT = {
    "tga":               ("🇦🇺", "TGA · Australia"),
    "tga_consultations": ("🇦🇺", "TGA · Australia"),
    "artg":              ("🇦🇺", "ARTG · Australia"),
    "fda":               ("🇺🇸", "FDA · USA"),
    "fda_australia":     ("🇺🇸", "FDA → AU"),
    "advisory_committee":("🌐", "Advisory · Global"),
    "adverse_events":    ("🌐", "CAERS/DAEN"),
    "pubmed":            ("🔬", "PubMed · Research"),
}

_RISK_PHRASES = [
    "liver toxicity", "liver injury", "liver damage", "hepatotox",
    "recall", "recalled", "voluntary recall",
    "adverse event", "adverse effects",
    "hospitalisation", "hospitalized", "hospitali",
    "risk to health", "serious risk",
    "contamination", "contaminated",
    "drug-induced", "dili",
    "death", "fatal",
    "undisclosed", "undeclared",
    "ban", "banned", "restricted",
    "warning", "caution",
]

def _smart_ingredient(raw_ing: str, title: str, summary: str, source: str) -> str:
    """Return the most specific ingredient/product name possible."""
    ing = (raw_ing or "").strip()

    # Shorten parenthetical overloaded names
    if len(ing) > 50:
        paren = ing.find("(")
        if paren > 6:
            ing = ing[:paren].strip()

    if ing.lower() in _GENERIC_ING:
        # 1. Product name from title dashes  e.g. "Artri Ajo King — tablets"
        for sep in (" — ", " - ", ": "):
            if sep in title:
                part = title.split(sep)[0].strip()
                if 3 < len(part) < 50 and part.lower() not in _GENERIC_ING:
                    return part

        # 2. Known VMS keywords in combined text
        text = (title + " " + summary).lower()
        candidates = [
            "turmeric", "curcumin", "kava", "melatonin", "omega-3", "fish oil",
            "vitamin d", "vitamin c", "vitamin e", "vitamin b", "zinc", "magnesium",
            "iron", "calcium", "collagen", "creatine", "probiotics", "probiotic",
            "echinacea", "valerian", "st. john", "ginkgo", "ginseng", "elderberry",
            "green tea", "ashwagandha", "psyllium", "lactobacillus", "testosterone",
            "sirolimus", "rapamycin", "homosalate", "oxybenzone", "peptide",
            "aller-c", "kian pee wan", "artri ajo",
            "hair supplement", "hair growth", "nutrafol", "viviscal",
        ]
        for c in candidates:
            if c in text:
                return c.title()

        # 3. TGA consultation → "Poisons Standard"
        if source == "tga_consultations":
            return "Poisons Standard"

        # 4. Fallback: first 4 words of title
        words = [w for w in title.split() if len(w) > 2][:4]
        return " ".join(words) if words else "Unknown"

    return ing.title()


def _context_label(source: str, title: str, summary: str) -> str:
    """Return 'FLAG Source · Country' string."""
    flag, base = _SOURCE_BASE_CONTEXT.get(source, ("📋", source))
    if source == "pubmed":
        text = (summary + " " + title).lower()
        for pattern, cflag, country in _COUNTRY_PATTERNS:
            if pattern in text:
                return f"{cflag} PubMed · {country}"
    return f"{flag} {base}"


def _risk_desc(smart_ing: str, summary: str, event_type: str) -> str:
    """Return 'Ingredient — Risk phrase' for risk watch cards."""
    text = summary.lower()
    for phrase in _RISK_PHRASES:
        if phrase in text:
            return f"{smart_ing} — {phrase.replace('-',' ').title()}"
    label = event_type.replace("_", " ").title() if event_type and event_type != "other" else "Signal"
    return f"{smart_ing} — {label}"


# ── Build compact signals list ────────────────────────────────────────────────

signals = []
for s in signals_raw:
    src     = s.get("source_label") or s.get("authority", "")
    title   = s.get("title", "")
    summary = s.get("summary", "")
    raw_ing = s.get("ingredient_name", "")

    smart_ing = _smart_ingredient(raw_ing, title, summary, src)
    ctx_label = _context_label(src, title, summary)
    risk_desc = _risk_desc(smart_ing, summary, s.get("event_type", ""))

    signals.append({
        "id":          s.get("source_id", ""),
        "source":      src,
        "authority":   s.get("authority", ""),
        "title":       title,
        "ingredient":  smart_ing,
        "raw_ing":     raw_ing,
        "event_type":  s.get("event_type", ""),
        "severity":    s.get("severity", "low"),
        "summary":     summary,
        "sentiment":   s.get("sentiment", ""),
        "sent_conf":   round(s.get("sentiment_confidence") or 0, 2),
        "sent_reason": s.get("sentiment_reasoning", ""),
        "url":         s.get("url", ""),
        "scraped_at":  (s.get("scraped_at") or "")[:10],
        "relevance":   s.get("relevance_to_vms", ""),
        "signal_type": s.get("signal_type", ""),
        "potential_impact": s.get("potential_impact", ""),
        "trend_relevance":  s.get("trend_relevance", ""),
        "market_sig":  s.get("market_significance", ""),
        "au_relevance":s.get("australia_relevance", ""),
        "ctx":         ctx_label,
        "risk_desc":   risk_desc,
    })

# Sort signals: severity (high first), then scraped_at desc
SEV_ORDER = {"high": 0, "medium": 1, "low": 2}
signals.sort(key=lambda s: (SEV_ORDER.get(s["severity"], 3), s["scraped_at"]), reverse=False)
signals.sort(key=lambda s: SEV_ORDER.get(s["severity"], 3))

# Load citations — strip to display fields only
cit_raw = json.loads((BASE / "reports/citation_database.json").read_text())
citations = []
for c in cit_raw.get("citations", []):
    citations.append({
        "id":       c.get("id", ""),
        "auth":     c.get("authority", ""),
        "date":     c.get("date", ""),
        "cat":      c.get("category", ""),
        "sev":      c.get("severity", ""),
        "summary":  c.get("summary", ""),
        "company":  c.get("company", ""),
        "facility": c.get("facility_type", ""),
        "product":  c.get("product_type", ""),
        "url":      c.get("url", ""),
        "country":  c.get("country", ""),
    })

# Analytics payloads — filter "general" from trending, map to smart names
_SKIP_TRENDING = {"general"}

# Build a lookup: raw_ing → smart_ing (use the first matching signal)
_ing_map: dict[str, str] = {}
for s in signals:
    raw = s["raw_ing"].lower().strip()
    if raw and raw not in _ing_map:
        _ing_map[raw] = s["ingredient"]

trending = [
    {
        "ingredient": _ing_map.get(t.ingredient.lower(), t.ingredient).title(),
        "raw": t.ingredient,
        "count_7d": t.count_7d, "count_30d": t.count_30d,
        "ratio": t.spike_ratio, "avg_daily": t.avg_daily_30d
    }
    for t in trend_report.trending_ingredients
    if t.ingredient.lower() not in _SKIP_TRENDING
]
claim_shifts = [
    {"ingredient": c.ingredient, "from": c.old_dominant_type, "to": c.new_dominant_type,
     "old_count": c.old_count, "new_count": c.new_count, "desc": c.description}
    for c in trend_report.claim_shift_alerts
]
ing_sentiment = [
    {"ingredient": s.ingredient, "score_30d": s.score_30d, "score_recent": s.score_recent,
     "shift": s.shift, "n": s.signal_count, "rising": s.is_rising_risk}
    for s in sentiment_report.ingredient_summaries
]
sentiment_dist = sentiment_report.overall_distribution

# Stats
from collections import Counter
src_counts  = dict(Counter(s["source"] for s in signals))
sev_counts  = dict(Counter(s["severity"] for s in signals))
sent_counts = dict(Counter(s["sentiment"] for s in signals if s["sentiment"]))
cit_auth_counts = dict(Counter(c["auth"] for c in citations))

generated_at = datetime.now(timezone.utc).strftime("%d %B %Y %H:%M UTC")

# ── Embed JSON ───────────────────────────────────────────────────────────────

SIGNALS_JSON   = json.dumps(signals,   separators=(",", ":"), ensure_ascii=False)
CITATIONS_JSON = json.dumps(citations, separators=(",", ":"), ensure_ascii=False)
TRENDING_JSON  = json.dumps(trending,  separators=(",", ":"))
SHIFTS_JSON    = json.dumps(claim_shifts, separators=(",", ":"))
ING_SENT_JSON  = json.dumps(ing_sentiment, separators=(",", ":"))
SENT_DIST_JSON = json.dumps(sentiment_dist, separators=(",", ":"))
STATS_JSON     = json.dumps({
    "total_signals":  len(signals),
    "total_citations": len(citations),
    "src_counts":     src_counts,
    "sev_counts":     sev_counts,
    "sent_counts":    sent_counts,
    "cit_auth_counts": cit_auth_counts,
    "generated_at":   generated_at,
}, separators=(",", ":"))

# ── HTML ─────────────────────────────────────────────────────────────────────

HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Signalex Intelligence Hub — VMS · Pharma · Retail</title>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
:root{{
  --navy:#0D1B2A;--navy-mid:#0F2336;--navy-lite:#1A2F47;
  --border:#1E3A5F;--teal:#0D9488;--teal-dim:#0B7C72;--teal-bg:#0D3D35;
  --slate:#94A3B8;--slate-dim:#64748B;--white:#F8FAFC;--off:#E2E8F0;--off2:#CBD5E1;
  --red:#DC2626;--red-bg:#2D0A0A;--amber:#D97706;--amber-bg:#2D1A0A;
  --green:#059669;--green-bg:#0A2D1A;
  --purple:#7C3AED;--blue:#2563EB;--indigo:#4F46E5;
  --pos-c:#059669;--neu-c:#64748B;--neg-c:#DC2626;
  --pos-bg:#0A2D1A;--neu-bg:#1A2236;--neg-bg:#2D0A0A;
  --topbar-h:89px;--maintab-h:44px;
}}
html{{scroll-behavior:smooth}}
body{{background:var(--navy);color:var(--off);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;min-height:100vh;font-size:14px;line-height:1.5}}
a{{color:var(--teal);text-decoration:none}}a:hover{{text-decoration:underline}}
h1,h2,h3,h4{{font-family:Georgia,'Times New Roman',serif;font-weight:700;color:var(--white)}}
button{{cursor:pointer;border:none;font-family:inherit;font-size:inherit}}

/* ── Sticky header ────────────────────────────────────── */
#topbar{{
  position:sticky;top:0;z-index:100;background:var(--navy);
  border-bottom:2px solid var(--teal);
}}
.topbar-inner{{max-width:1280px;margin:0 auto;padding:10px 20px}}
.topbar-row1{{display:flex;align-items:center;gap:16px;flex-wrap:wrap}}
.brand{{display:flex;align-items:center;gap:10px;flex-shrink:0}}
.brand-name{{font-family:Georgia,serif;font-size:20px;font-weight:700;color:var(--white)}}
.brand-pill{{font-size:9px;background:var(--teal);color:#fff;padding:2px 7px;border-radius:99px;letter-spacing:.06em;text-transform:uppercase;font-family:sans-serif}}
.search-wrap{{flex:1;min-width:200px;max-width:440px;position:relative}}
.search-wrap input{{
  width:100%;background:var(--navy-mid);border:1px solid var(--border);
  border-radius:6px;padding:7px 12px 7px 34px;color:var(--white);font-size:13px;
  outline:none;transition:border .15s
}}
.search-wrap input:focus{{border-color:var(--teal)}}
.search-wrap .si{{position:absolute;left:10px;top:50%;transform:translateY(-50%);color:var(--slate-dim);font-size:14px}}
.stats-pills{{display:flex;gap:6px;flex-wrap:wrap;margin-left:auto}}
.stat-pill{{background:var(--navy-mid);border:1px solid var(--border);border-radius:6px;padding:4px 10px;font-size:11px;color:var(--slate);white-space:nowrap}}
.stat-pill b{{color:var(--white)}}

/* ── Filter row ────────────────────────────────────────── */
.filter-row{{display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding-top:4px;border-top:1px solid var(--border)}}
.filter-label{{font-size:10px;font-weight:700;color:var(--slate-dim);text-transform:uppercase;letter-spacing:.06em;flex-shrink:0}}
.filter-group{{display:flex;gap:4px;flex-wrap:wrap}}
.ftoggle{{
  font-size:11px;font-weight:600;padding:3px 10px;border-radius:99px;
  background:var(--navy-mid);border:1px solid var(--border);color:var(--slate);
  transition:all .12s
}}
.ftoggle.active{{background:var(--teal-bg);border-color:var(--teal);color:var(--teal)}}
.ftoggle.sev-high.active{{background:var(--red-bg);border-color:var(--red);color:var(--red)}}
.ftoggle.sev-medium.active{{background:var(--amber-bg);border-color:var(--amber);color:var(--amber)}}
.ftoggle.sev-low.active{{background:var(--green-bg);border-color:var(--green);color:var(--green)}}
.ftoggle.sent-pos.active{{background:var(--pos-bg);border-color:var(--pos-c);color:var(--pos-c)}}
.ftoggle.sent-neu.active{{background:var(--neu-bg);border-color:var(--slate);color:var(--slate)}}
.ftoggle.sent-neg.active{{background:var(--neg-bg);border-color:var(--neg-c);color:var(--neg-c)}}
.divider{{width:1px;height:14px;background:var(--border);flex-shrink:0}}
.sort-sel{{background:var(--navy-mid);border:1px solid var(--border);color:var(--slate);border-radius:6px;padding:3px 8px;font-size:11px;font-family:inherit;outline:none}}

/* ── Main product tab bar ──────────────────────────────── */
#maintabbar{{
  position:sticky;top:50px;z-index:100;
  background:var(--navy);border-bottom:2px solid var(--border);
}}
.maintab-inner{{max-width:1280px;margin:0 auto;padding:0 20px;display:flex;gap:0;overflow-x:auto;justify-content:center}}
.mtab{{
  padding:14px 40px;font-size:15px;font-weight:700;color:var(--slate-dim);
  border-bottom:3px solid transparent;white-space:nowrap;transition:all .15s;
  background:none;letter-spacing:.03em;font-family:Georgia,'Times New Roman',serif;
}}
.mtab:hover{{color:var(--off)}}
.mtab.active{{color:var(--white);border-bottom-color:var(--teal)}}
.mtab .mcnt{{
  background:var(--navy-lite);color:var(--slate);border-radius:99px;
  padding:1px 7px;font-size:10px;margin-left:8px;font-family:sans-serif;font-weight:600;
}}
.mtab.active .mcnt{{background:var(--teal-bg);color:var(--teal)}}
.mtab-vms.active{{border-bottom-color:var(--teal)}}
.mtab-pharma.active{{border-bottom-color:var(--amber)}}
.mtab-retail.active{{border-bottom-color:var(--purple)}}

/* ── VMS local filter row (source + severity + sentiment + sort) */
#vmsFilterRow{{background:var(--navy-mid);border-bottom:1px solid var(--border)}}
#vmsFilterRow.hidden{{display:none}}
.vmsfilter-inner{{
  max-width:1280px;margin:0 auto;padding:6px 20px;
  display:flex;align-items:center;gap:8px;flex-wrap:wrap;
}}

/* ── Signal card highlight glow ────────────────────────── */
.card-highlight{{box-shadow:0 0 0 2px var(--teal),0 0 20px rgba(13,148,136,.25)!important;transition:box-shadow .15s}}

/* ── Section nav ───────────────────────────────────────── */
#secnav{{background:var(--navy-mid);border-bottom:1px solid var(--border);position:sticky;top:94px;z-index:99}}
.secnav-inner{{max-width:1280px;margin:0 auto;padding:0 20px;display:flex;gap:0;overflow-x:auto}}
.snav-tab{{
  padding:10px 16px;font-size:12px;font-weight:600;color:var(--slate-dim);
  border-bottom:2px solid transparent;white-space:nowrap;transition:all .12s;
  background:none;letter-spacing:.01em;
}}
.snav-tab:hover{{color:var(--off)}}
.snav-tab.active{{color:var(--teal);border-bottom-color:var(--teal)}}
.snav-tab .cnt{{
  background:var(--border);color:var(--slate);border-radius:99px;
  padding:1px 7px;font-size:10px;margin-left:6px;font-family:sans-serif;
}}
.snav-tab.active .cnt{{background:var(--teal-bg);color:var(--teal)}}

/* ── Main content ──────────────────────────────────────── */
#content{{max-width:1280px;margin:0 auto;padding:24px 20px 60px}}
.section{{display:none}}
.section.active{{display:block}}

/* ── Section headers ───────────────────────────────────── */
.sec-header{{margin-bottom:20px}}
.sec-title{{font-size:22px;color:var(--white);display:flex;align-items:center;gap:10px}}
.sec-title .ico{{font-size:20px}}
.sec-sub{{font-size:12px;color:var(--slate-dim);margin-top:4px}}

/* ── Dashboard grid ────────────────────────────────────── */
.dash-grid{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:24px}}
@media(max-width:768px){{.dash-grid{{grid-template-columns:1fr}}}}
.dash-grid-3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:20px;margin-bottom:24px}}
@media(max-width:900px){{.dash-grid-3{{grid-template-columns:1fr 1fr}}}}
@media(max-width:600px){{.dash-grid-3{{grid-template-columns:1fr}}}}

/* ── Panels ─────────────────────────────────────────────── */
.panel{{background:var(--navy-mid);border:1px solid var(--border);border-radius:8px;padding:18px 20px}}
.panel-title{{font-size:11px;font-weight:700;color:var(--slate-dim);text-transform:uppercase;letter-spacing:.08em;margin-bottom:14px;display:flex;align-items:center;gap:6px}}
.panel-title .ico{{font-size:13px}}

/* ── Risk watch ─────────────────────────────────────────── */
.risk-banner{{border-radius:8px;margin-bottom:20px;background:var(--navy-mid);border:1px solid rgba(220,38,38,.25)}}
.risk-banner-header{{
  display:flex;align-items:center;justify-content:space-between;
  padding:12px 16px;border-bottom:1px solid rgba(220,38,38,.2);
}}
.risk-banner-title{{font-size:11px;font-weight:700;color:var(--red);text-transform:uppercase;letter-spacing:.08em;display:flex;align-items:center;gap:6px}}
.risk-grid{{
  display:grid;grid-template-columns:1fr 1fr;gap:10px;padding:12px 16px;
}}
@media(max-width:640px){{.risk-grid{{grid-template-columns:1fr}}}}
.risk-card{{
  background:var(--navy);border:1px solid rgba(220,38,38,.2);
  border-left:3px solid var(--red);border-radius:6px;
  padding:10px 12px;height:90px;overflow:hidden;
  display:flex;flex-direction:column;justify-content:space-between;
  cursor:pointer;transition:border-color .12s;
}}
.risk-card:hover{{border-color:rgba(220,38,38,.6)}}
.rc-title{{
  font-weight:700;font-size:13px;color:var(--white);
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;line-height:1.3;
}}
.rc-sub{{
  font-size:11px;color:var(--slate-dim);
  display:flex;align-items:center;gap:5px;white-space:nowrap;overflow:hidden;
  margin-top:3px;
}}
.rc-sev{{
  font-size:9px;font-weight:700;padding:1px 5px;border-radius:3px;
  background:var(--red-bg);color:var(--red);border:1px solid rgba(220,38,38,.3);
  flex-shrink:0;
}}
.rc-sev-med{{background:var(--amber-bg);color:var(--amber);border-color:rgba(217,119,6,.3)}}
.rc-body{{
  font-size:11px;color:var(--off2);margin-top:4px;
  overflow:hidden;display:-webkit-box;
  -webkit-line-clamp:2;-webkit-box-orient:vertical;line-height:1.4;
}}

/* ── Sentiment donut ────────────────────────────────────── */
.donut-wrap{{display:flex;align-items:center;gap:20px}}
.donut{{width:80px;height:80px;border-radius:50%;flex-shrink:0}}
.donut-legend{{flex:1}}
.donut-row{{display:flex;align-items:center;gap:8px;margin-bottom:6px;font-size:12px}}
.donut-dot{{width:10px;height:10px;border-radius:50%;flex-shrink:0}}
.donut-val{{margin-left:auto;font-weight:700;color:var(--white)}}

/* ── Trending bar ───────────────────────────────────────── */
.trend-row{{display:flex;align-items:center;gap:10px;margin-bottom:10px}}
.trend-ing{{font-size:12px;font-weight:600;color:var(--off2);min-width:160px;flex-shrink:0;text-transform:capitalize}}
.trend-bar-wrap{{flex:1;background:var(--navy);border-radius:99px;height:7px;overflow:hidden}}
.trend-bar{{height:100%;background:var(--teal);border-radius:99px;transition:width .4s}}
.trend-meta{{font-size:11px;color:var(--slate-dim);white-space:nowrap;min-width:80px;text-align:right}}
/* compact variant used inside the dashboard panel */
#trendBars .trend-row{{margin-bottom:5px}}
#trendBars .trend-bar-wrap{{height:5px}}
#trendBars .trend-ing{{font-size:11px;min-width:110px}}
#trendBars .trend-meta{{font-size:10px;min-width:100px}}

/* ── Source footer (collapsible) ─────────────────────────── */
.src-footer-panel{{background:var(--navy-mid);border:1px solid var(--border);border-radius:8px;padding:10px 18px;margin-bottom:20px}}
.src-footer-header{{display:flex;align-items:center;justify-content:space-between;cursor:pointer;user-select:none}}
.src-footer-header:hover .panel-title{{color:var(--off)}}
#srcBars .vis-bar-row{{margin-bottom:7px}}

/* ── Cards ──────────────────────────────────────────────── */
.cards{{display:flex;flex-direction:column;gap:10px}}
.card{{
  background:var(--navy-mid);border:1px solid var(--border);
  border-radius:7px;padding:14px 16px;transition:border-color .12s;
  border-left-width:3px;
}}
.card:hover{{border-color:var(--teal)}}
.card.sev-high{{border-left-color:var(--red)}}
.card.sev-medium{{border-left-color:var(--amber)}}
.card.sev-low{{border-left-color:var(--green)}}
.card-top{{display:flex;align-items:flex-start;gap:8px;flex-wrap:wrap;margin-bottom:8px}}
.card-title{{font-size:14px;font-weight:700;color:var(--white);line-height:1.35;flex:1;min-width:0}}
.card-title a{{color:var(--white)}}
.card-title a:hover{{color:var(--teal)}}
.card-badges{{display:flex;gap:5px;flex-wrap:wrap;flex-shrink:0}}
.badge{{font-size:10px;font-weight:700;padding:2px 8px;border-radius:99px;letter-spacing:.03em;white-space:nowrap}}
.badge-src-pubmed{{background:#2D1B69;color:#A78BFA}}
.badge-src-tga{{background:#0D3D35;color:#0D9488}}
.badge-src-tga_consultations{{background:#0D3D30;color:#34D399}}
.badge-src-fda{{background:#0F2A4A;color:#60A5FA}}
.badge-src-fda_australia{{background:#0F2040;color:#93C5FD}}
.badge-src-artg{{background:#0D3D20;color:#6EE7B7}}
.badge-src-adverse_events{{background:#3B0A0A;color:#F87171}}
.badge-src-advisory_committee{{background:#1A1035;color:#818CF8}}
.badge-src-iherb{{background:#1E3A5F;color:#94A3B8}}
.badge-sev-high{{background:var(--red-bg);color:var(--red);border:1px solid rgba(220,38,38,.3)}}
.badge-sev-medium{{background:var(--amber-bg);color:var(--amber);border:1px solid rgba(217,119,6,.3)}}
.badge-sev-low{{background:var(--green-bg);color:var(--green);border:1px solid rgba(5,150,105,.3)}}
.badge-sent-positive{{background:var(--pos-bg);color:var(--pos-c);border:1px solid rgba(5,150,105,.3)}}
.badge-sent-neutral{{background:var(--neu-bg);color:var(--slate);border:1px solid var(--border)}}
.badge-sent-negative{{background:var(--neg-bg);color:var(--red);border:1px solid rgba(220,38,38,.3)}}
.badge-rel-high{{background:#14532D;color:#86EFAC}}
.badge-rel-medium{{background:#1A2C4A;color:#7DD3FC}}
.badge-rel-low{{background:#1A1A2A;color:#64748B}}
.badge-impact-restrictive{{background:#450A0A;color:#FCA5A5}}
.badge-impact-permissive{{background:#0A2D1A;color:#6EE7B7}}
.badge-impact-neutral{{background:#1A1A2A;color:#94A3B8}}
.card-meta{{font-size:12px;color:var(--slate-dim);margin-bottom:6px;display:flex;gap:12px;flex-wrap:wrap}}
.card-summary{{font-size:13px;color:var(--off2);line-height:1.55}}
.card-reason{{font-size:11px;color:var(--slate-dim);margin-top:5px;font-style:italic}}
.card-footer{{margin-top:10px;display:flex;align-items:center;gap:10px;flex-wrap:wrap}}
.card-link{{font-size:11px;font-weight:600;color:var(--teal)}}
.card-conf{{font-size:10px;color:var(--slate-dim)}}
.no-results{{
  text-align:center;padding:48px 20px;color:var(--slate-dim);
  background:var(--navy-mid);border:1px solid var(--border);border-radius:8px;
}}
.no-results .nr-ico{{font-size:32px;margin-bottom:8px}}

/* ── Citation cards ─────────────────────────────────────── */
.cit-card{{
  background:var(--navy-mid);border:1px solid var(--border);border-radius:7px;
  padding:12px 16px;border-left:3px solid var(--border);
}}
.cit-card.sev-high{{border-left-color:var(--red)}}
.cit-card.sev-medium{{border-left-color:var(--amber)}}
.cit-card .cc-top{{display:flex;align-items:flex-start;gap:8px;margin-bottom:6px;flex-wrap:wrap}}
.cit-card .cc-sum{{font-size:13px;color:var(--off2);line-height:1.5}}
.cit-card .cc-foot{{margin-top:8px;display:flex;gap:10px;align-items:center;flex-wrap:wrap}}
.cit-card .cc-meta{{font-size:11px;color:var(--slate-dim);display:flex;gap:10px;flex-wrap:wrap}}
.badge-fda{{background:#0F2A4A;color:#60A5FA}}
.badge-tga{{background:#0D3D35;color:#0D9488}}
.badge-mhra{{background:#1A1035;color:#A78BFA}}
.badge-efsa{{background:#0A2D1A;color:#6EE7B7}}
.badge-bfr{{background:#2D1A0A;color:#FCD34D}}

/* ── Trend analytics ────────────────────────────────────── */
.ing-sent-row{{display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid var(--border)}}
.ing-sent-name{{min-width:160px;font-size:12px;font-weight:600;color:var(--off2);text-transform:capitalize;flex-shrink:0}}
.sent-bar-wrap{{flex:1;display:flex;align-items:center;gap:6px}}
.sent-bar-track{{flex:1;height:6px;background:var(--navy);border-radius:99px;overflow:hidden;position:relative}}
.sent-bar-fill{{height:100%;border-radius:99px;position:absolute;top:0}}
.sent-score{{font-size:11px;font-weight:700;min-width:44px;text-align:right}}
.sent-n{{font-size:10px;color:var(--slate-dim);min-width:32px;text-align:right}}

/* ── Cite controls ──────────────────────────────────────── */
.cite-controls{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px;align-items:center}}
.cite-sel{{background:var(--navy-mid);border:1px solid var(--border);color:var(--slate);border-radius:6px;padding:5px 10px;font-size:12px;font-family:inherit;outline:none;cursor:pointer}}
.cite-sel:focus{{border-color:var(--teal)}}
.result-count{{font-size:11px;color:var(--slate-dim);margin-left:auto}}

/* ── Scrollable card container ──────────────────────────── */
.card-scroll{{}}

/* ── Shift card ─────────────────────────────────────────── */
.shift-card{{
  background:var(--navy-mid);border:1px solid var(--amber);border-radius:7px;
  padding:14px 16px;margin-bottom:10px;
}}
.shift-ing{{font-size:15px;font-weight:700;color:var(--white);text-transform:capitalize;margin-bottom:6px}}
.shift-arrow{{display:flex;align-items:center;gap:8px;font-size:12px}}
.shift-from{{background:var(--navy-lite);color:var(--blue);padding:2px 10px;border-radius:4px}}
.shift-to{{background:var(--red-bg);color:var(--red);padding:2px 10px;border-radius:4px}}
.shift-desc{{font-size:12px;color:var(--slate);margin-top:8px;line-height:1.5}}

/* ── Tab panes ──────────────────────────────────────────── */
.tab-pane{{display:none}}
.tab-pane.active{{display:block}}

/* ── Pharma tab accent ──────────────────────────────────── */
.tab-pharma .snav-tab.active{{color:var(--amber);border-bottom-color:var(--amber)}}
.tab-pharma .snav-tab.active .cnt{{background:rgba(217,119,6,.15);color:var(--amber)}}

/* ── Retail tab accent ──────────────────────────────────── */
.tab-retail .snav-tab.active{{color:var(--purple);border-bottom-color:var(--purple)}}
.tab-retail .snav-tab.active .cnt{{background:rgba(124,58,237,.15);color:var(--purple)}}

/* ── Timeline ───────────────────────────────────────────── */
.timeline{{position:relative;padding-left:28px}}
.timeline::before{{content:"";position:absolute;left:8px;top:0;bottom:0;width:2px;background:var(--border)}}
.tl-item{{position:relative;margin-bottom:16px}}
.tl-dot{{
  position:absolute;left:-24px;top:3px;width:10px;height:10px;
  border-radius:50%;background:var(--teal);border:2px solid var(--navy-mid);
}}
.tl-dot.sev-high{{background:var(--red)}}
.tl-dot.sev-medium{{background:var(--amber)}}
.tl-card{{
  background:var(--navy-mid);border:1px solid var(--border);
  border-radius:6px;padding:10px 14px;
}}
.tl-date{{font-size:10px;color:var(--slate-dim);margin-bottom:3px}}
.tl-title{{font-size:13px;font-weight:700;color:var(--white);margin-bottom:4px}}
.tl-meta{{font-size:11px;color:var(--slate-dim)}}

/* ── Facility table ─────────────────────────────────────── */
.cmp-table{{width:100%;border-collapse:collapse;font-size:12px}}
.cmp-table th{{text-align:left;padding:7px 10px;color:var(--slate-dim);font-weight:600;border-bottom:1px solid var(--border);font-size:11px;text-transform:uppercase;letter-spacing:.05em}}
.cmp-table td{{padding:8px 10px;border-bottom:1px solid rgba(30,58,95,.5);vertical-align:top}}
.cmp-table tr:last-child td{{border-bottom:none}}
.cmp-table tr:hover td{{background:rgba(255,255,255,.03)}}

/* ── Monitor status card ────────────────────────────────── */
.monitor-card{{
  background:var(--navy-mid);border:1px solid var(--border);border-radius:8px;
  padding:20px;margin-bottom:14px;
}}
.monitor-status{{display:flex;align-items:center;gap:8px;margin-bottom:8px}}
.status-dot{{width:8px;height:8px;border-radius:50%;flex-shrink:0}}
.status-blocked{{background:#DC2626}}
.status-ok{{background:#059669}}
.status-warn{{background:#D97706}}

/* ── Ingredient profile banner ─────────────────────────────── */
#ingBanner{{max-width:1280px;margin:0 auto;padding:0 20px}}
.ing-banner-wrap{{background:var(--navy-mid);border:1px solid var(--teal);border-left:3px solid var(--teal);border-radius:8px;overflow:hidden;margin-bottom:16px}}
.ing-banner-head{{display:flex;align-items:center;gap:10px;padding:10px 14px;cursor:pointer;user-select:none;transition:background .12s}}
.ing-banner-head:hover{{background:rgba(13,148,136,.07)}}
.ing-banner-detail{{border-top:1px solid var(--border);padding:14px 16px;background:var(--navy)}}

/* ── Filter chips ───────────────────────────────────────── */
#filterChips{{max-width:1280px;margin:0 auto;padding:0 20px}}
.chip-bar{{display:flex;gap:6px;flex-wrap:wrap;align-items:center;padding:6px 0 8px}}
.filter-chip{{display:inline-flex;align-items:center;gap:3px;background:var(--teal-bg);border:1px solid var(--teal);color:var(--teal);border-radius:99px;padding:2px 10px;font-size:11px;font-weight:600;cursor:pointer;white-space:nowrap;transition:background .12s}}
.filter-chip:hover{{background:rgba(13,148,136,.2)}}
.chip-clear-all{{background:var(--red-bg);border:1px solid var(--red);color:var(--red);border-radius:99px;padding:2px 10px;font-size:11px;font-weight:600;cursor:pointer;white-space:nowrap;transition:background .12s}}
.chip-clear-all:hover{{background:rgba(220,38,38,.2)}}

/* ── Misc ──────────────────────────────────────────────── */
.spacer{{height:12px}}
.row{{display:flex;gap:10px;flex-wrap:wrap}}
.empty-state{{text-align:center;padding:32px;color:var(--slate-dim);font-size:13px}}
.pill-row{{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:16px}}
.cit-load-more{{
  display:block;width:100%;margin-top:14px;padding:10px;text-align:center;
  background:var(--navy-mid);border:1px solid var(--border);border-radius:6px;
  color:var(--slate);font-size:12px;cursor:pointer;
}}
.cit-load-more:hover{{border-color:var(--teal);color:var(--teal)}}
.vis-bar-row{{display:flex;gap:10px;align-items:center;margin-bottom:10px}}
.vis-label{{font-size:11px;color:var(--off2);min-width:90px;flex-shrink:0;font-weight:600}}
.vis-track{{flex:1;height:10px;background:var(--navy);border-radius:99px;overflow:hidden}}
.vis-fill{{height:100%;border-radius:99px}}
.vis-val{{font-size:11px;color:var(--slate);min-width:40px;text-align:right}}
</style>
</head>
<body>

<!-- ═══════════════════════════════════════════════════════════════
     TOP BAR
══════════════════════════════════════════════════════════════════ -->
<div id="topbar">
  <div class="topbar-inner">
    <div class="topbar-row1">
      <div class="brand">
        <svg width="28" height="28" viewBox="0 0 56 56" fill="none">
          <circle cx="28" cy="28" r="26" stroke="#0D9488" stroke-width="2" opacity=".25"/>
          <circle cx="28" cy="28" r="18" stroke="#0D9488" stroke-width="2" opacity=".5"/>
          <polyline stroke="#0D9488" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"
            points="4,28 12,28 16,20 20,36 24,24 27,31 32,28 52,28"/>
          <circle cx="32" cy="28" r="3.5" fill="#0D9488"/>
        </svg>
        <div>
          <div class="brand-name">Signalex</div>
          <div class="brand-sub">Intelligence Hub</div>
        </div>
        <span class="brand-pill">Live</span>
      </div>
      <div class="search-wrap">
        <span class="si">🔍</span>
        <input type="text" id="globalSearch" placeholder="Search across all sections — ingredients, companies, keywords…" oninput="onSearch(this.value)">
      </div>
      <div class="stats-pills" id="statsPills"></div>
    </div>
  </div>
</div>

<!-- ═══════════════════════════════════════════════════════════════
     MAIN PRODUCT TAB BAR
══════════════════════════════════════════════════════════════════ -->
<div id="maintabbar">
  <div class="maintab-inner">
    <button class="mtab mtab-vms active" data-mtab="vms" onclick="showMainTab('vms',this)">
      VMS Intelligence<span class="mcnt" id="mcnt-vms"></span>
    </button>
    <button class="mtab mtab-pharma" data-mtab="pharma" onclick="showMainTab('pharma',this)">
      Pharma Compliance<span class="mcnt" id="mcnt-pharma"></span>
    </button>
    <button class="mtab mtab-retail" data-mtab="retail" onclick="showMainTab('retail',this)">
      Retail &amp; Competitive<span class="mcnt" id="mcnt-retail"></span>
    </button>
  </div>
</div>

<!-- ═══════════════════════════════════════════════════════════════
     SECTION NAV  (content injected by JS per active main tab)
══════════════════════════════════════════════════════════════════ -->
<div id="secnav"><div class="secnav-inner" id="secnavInner"></div></div>

<!-- VMS-only filter row: source · severity · sentiment · sort -->
<div id="vmsFilterRow">
  <div class="vmsfilter-inner">
    <span class="filter-label">Source</span>
    <div class="filter-group" id="srcFilters"></div>
    <div class="divider"></div>
    <span class="filter-label">Severity</span>
    <div class="filter-group" id="sevFilters">
      <button class="ftoggle sev-high active" data-sev="high" onclick="toggleSev(this)">HIGH</button>
      <button class="ftoggle sev-medium active" data-sev="medium" onclick="toggleSev(this)">MED</button>
      <button class="ftoggle sev-low active" data-sev="low" onclick="toggleSev(this)">LOW</button>
    </div>
    <div class="divider"></div>
    <span class="filter-label">Sentiment</span>
    <div class="filter-group" id="sentFilters">
      <button class="ftoggle sent-pos active" data-sent="positive" onclick="toggleSent(this)">▲ Pos</button>
      <button class="ftoggle sent-neu active" data-sent="neutral" onclick="toggleSent(this)">● Neu</button>
      <button class="ftoggle sent-neg active" data-sent="negative" onclick="toggleSent(this)">▼ Neg</button>
    </div>
    <div class="divider"></div>
    <span class="filter-label">Sort</span>
    <select class="sort-sel" id="sortSel" onchange="onSort(this.value)">
      <option value="severity">Severity</option>
      <option value="recent">Most Recent</option>
      <option value="sentiment">Sentiment</option>
      <option value="ingredient">Ingredient</option>
    </select>
  </div>
</div>

<!-- Ingredient profile banner (shown when search matches a known ingredient) -->
<div id="ingBanner"></div>
<!-- Active filter chips -->
<div id="filterChips"></div>

<!-- ═══════════════════════════════════════════════════════════════
     CONTENT
══════════════════════════════════════════════════════════════════ -->
<div id="content">

<!-- ╔══════════════════════════════════════════════════════════╗
     ║  TAB 1 — VMS INTELLIGENCE                               ║
     ╚══════════════════════════════════════════════════════════╝ -->
<div class="tab-pane active" id="tp-vms">

  <!-- 1a. DASHBOARD -->
  <div class="section active" id="sec-dashboard">
    <div class="sec-header">
      <h2 class="sec-title"><span class="ico">🏠</span>VMS Intelligence Dashboard</h2>
      <div class="sec-sub">Risk watch · trending ingredients · sentiment overview &nbsp;·&nbsp; <span id="dash-gen"></span></div>
    </div>
    <div id="dash-risk"></div>
    <div class="dash-grid">
      <div class="panel">
        <div class="panel-title"><span class="ico">💬</span>Sentiment Overview <span style="font-size:10px;font-weight:400;color:var(--slate-dim)">(click to filter)</span></div>
        <div class="donut-wrap">
          <div class="donut" id="donutEl" style="cursor:pointer" onclick="filterBySentiment('negative')" title="Filter to negative signals"></div>
          <div class="donut-legend" id="donutLegend"></div>
        </div>
        <div id="sentBreakdown" style="border-top:1px solid var(--border);margin-top:12px;padding-top:10px"></div>
      </div>
      <div class="panel">
        <div class="panel-title"><span class="ico">📈</span>Trending Ingredients <span style="font-size:10px;font-weight:400;color:var(--slate-dim)">(genuine spikes only · max 5)</span></div>
        <div id="trendBars"></div>
      </div>
    </div>
    <div class="panel" style="margin-bottom:20px">
      <div class="panel-title"><span class="ico">🔴</span>Recent HIGH Severity</div>
      <div id="dashHighCards"></div>
    </div>
    <div class="src-footer-panel" id="srcPanel">
      <div class="src-footer-header" onclick="toggleSrcPanel()">
        <div class="panel-title" style="margin:0"><span class="ico">📡</span>Signal Sources <span id="srcCount" style="font-weight:400;font-size:11px;color:var(--slate-dim)"></span></div>
        <span id="srcChevron" style="font-size:11px;color:var(--slate-dim);font-weight:600">▼ show</span>
      </div>
      <div id="srcBars" style="display:none;margin-top:14px"></div>
    </div>
  </div>

  <!-- 1b. REGULATORY -->
  <div class="section" id="sec-regulatory">
    <div class="sec-header">
      <h2 class="sec-title"><span class="ico">📋</span>Regulatory Signals</h2>
      <div class="sec-sub">TGA safety alerts · FDA recalls · ARTG new listings · TGA consultations · Advisory committee items</div>
    </div>
    <div id="regCards"></div>
  </div>

  <!-- 1c. RESEARCH -->
  <div class="section" id="sec-research">
    <div class="sec-header">
      <h2 class="sec-title"><span class="ico">🔬</span>Research Intelligence</h2>
      <div class="sec-sub">PubMed articles classified for VMS relevance, safety signals, and efficacy claims</div>
    </div>
    <div id="resCards"></div>
  </div>

  <!-- 1d. ADVERSE EVENTS -->
  <div class="section" id="sec-adverse">
    <div class="sec-header">
      <h2 class="sec-title"><span class="ico">⚠️</span>Adverse Events</h2>
      <div class="sec-sub">FDA CAERS · TGA DAEN — supplement adverse event notifications</div>
    </div>
    <div id="aeCards"></div>
  </div>

  <!-- 1e. TRENDS -->
  <div class="section" id="sec-trends">
    <div class="sec-header">
      <h2 class="sec-title"><span class="ico">📈</span>Trends &amp; Analytics</h2>
      <div class="sec-sub">Ingredient spike detection · Claim shift alerts · Sentiment trajectories</div>
    </div>
    <div id="trendsContent"></div>
  </div>

</div><!-- /tp-vms -->

<!-- ╔══════════════════════════════════════════════════════════╗
     ║  TAB 2 — PHARMA COMPLIANCE                              ║
     ╚══════════════════════════════════════════════════════════╝ -->
<div class="tab-pane tab-pharma" id="tp-pharma">

  <!-- 2a. CITATION SEARCH -->
  <div class="section active" id="sec-citations">
    <div class="sec-header">
      <h2 class="sec-title"><span class="ico">📚</span>Citation Search</h2>
      <div class="sec-sub">2,869 regulatory citations — FDA · TGA · MHRA · EFSA · BfR — GMP violations, import alerts, safety findings</div>
    </div>
    <div class="cite-controls">
      <select class="cite-sel" id="citAuth" onchange="renderCitations()">
        <option value="">All Authorities</option>
        <option>FDA</option><option>TGA</option><option>MHRA</option>
        <option>EFSA</option><option>BfR</option>
      </select>
      <select class="cite-sel" id="citFacility" onchange="renderCitations()">
        <option value="">All Facility Types</option>
        <option>Supplement / Nutraceutical</option><option>General Pharma</option>
        <option>Medical Device</option><option>Sterile / Parenteral</option>
        <option>Biologics / Vaccine</option><option>API Manufacturer</option>
        <option>Compounding</option>
      </select>
      <select class="cite-sel" id="citCat" onchange="renderCitations()">
        <option value="">All Categories</option>
        <option>GMP violations</option><option>Ingredient safety</option>
        <option>Labelling &amp; claims</option><option>Contamination &amp; sterility</option>
        <option>Documentation &amp; record keeping</option>
        <option>Quality management system</option>
        <option>Equipment &amp; facilities</option>
        <option>Supply chain &amp; procurement</option>
        <option>Training &amp; competency</option>
        <option>Change control</option>
        <option>Cold chain and storage</option>
        <option>Stability programme</option>
      </select>
      <select class="cite-sel" id="citSort" onchange="renderCitations()">
        <option value="date_desc">Newest first</option>
        <option value="date_asc">Oldest first</option>
        <option value="sev">Severity (HIGH first)</option>
        <option value="auth">Authority A–Z</option>
      </select>
      <span class="result-count" id="citCount"></span>
    </div>
    <div id="citCards"></div>
    <button class="cit-load-more" id="citMore" onclick="loadMoreCitations()" style="display:none">Load more citations…</button>
  </div>

  <!-- 2b. WARNING LETTERS -->
  <div class="section" id="sec-warnings">
    <div class="sec-header">
      <h2 class="sec-title"><span class="ico">⚠️</span>FDA Warning Letters &amp; 483 Observations</h2>
      <div class="sec-sub">GMP violations and enforcement observations for supplement and pharma facilities</div>
    </div>
    <div id="warningsContent"></div>
  </div>

  <!-- 2c. ENFORCEMENT -->
  <div class="section" id="sec-enforcement">
    <div class="sec-header">
      <h2 class="sec-title"><span class="ico">🔒</span>Enforcement Action Tracker</h2>
      <div class="sec-sub">Import alerts, consent decrees, injunctions, and recalls grouped by authority</div>
    </div>
    <div id="enforcementContent"></div>
  </div>

  <!-- 2d. FACILITIES -->
  <div class="section" id="sec-facilities">
    <div class="sec-header">
      <h2 class="sec-title"><span class="ico">🏭</span>Facility-Level Compliance View</h2>
      <div class="sec-sub">Citations grouped by company and facility type — identify repeat offenders</div>
    </div>
    <div id="facilitiesContent"></div>
  </div>

</div><!-- /tp-pharma -->

<!-- ╔══════════════════════════════════════════════════════════╗
     ║  TAB 3 — RETAIL & COMPETITIVE INTEL                     ║
     ╚══════════════════════════════════════════════════════════╝ -->
<div class="tab-pane tab-retail" id="tp-retail">

  <!-- Single view: ARTG Listings + Coming Soon -->
  <div class="section active" id="sec-artg">
    <div class="sec-header">
      <h2 class="sec-title"><span class="ico">📋</span>ARTG New Listings</h2>
      <div class="sec-sub">New complementary medicine entries on the Australian Register of Therapeutic Goods</div>
    </div>
    <div id="artgCards"></div>
  </div>

</div><!-- /tp-retail -->

</div><!-- /content -->

<!-- ═══════════════════════════════════════════════════════════════
     DATA + JS
══════════════════════════════════════════════════════════════════ -->
<script>
// ── Embedded data ──────────────────────────────────────────────
const SIGNALS    = {SIGNALS_JSON};
const CITATIONS  = {CITATIONS_JSON};
const TRENDING   = {TRENDING_JSON};
const SHIFTS     = {SHIFTS_JSON};
const ING_SENT   = {ING_SENT_JSON};
const SENT_DIST  = {SENT_DIST_JSON};
const STATS      = {STATS_JSON};

// ── Section nav definitions ────────────────────────────────────
const SEC_NAVS = {{
  vms: [
    {{id:"dashboard",  label:"🏠 Dashboard",        cnt:"cnt-dashboard"}},
    {{id:"regulatory", label:"📋 Regulatory",        cnt:"cnt-regulatory"}},
    {{id:"research",   label:"🔬 Research",          cnt:"cnt-research"}},
    {{id:"adverse",    label:"⚠️ Adverse Events",    cnt:"cnt-adverse"}},
    {{id:"trends",     label:"📈 Trends",            cnt:"cnt-trends"}},
  ],
  pharma: [
    {{id:"citations",   label:"📚 Citation Search",  cnt:"cnt-citations"}},
    {{id:"warnings",    label:"⚠️ Warning Letters",  cnt:"cnt-warnings"}},
    {{id:"enforcement", label:"🔒 Enforcement",       cnt:"cnt-enforcement"}},
    {{id:"facilities",  label:"🏭 Facilities",        cnt:"cnt-facilities"}},
  ],
  retail: [
    {{id:"artg", label:"📋 ARTG Listings", cnt:"cnt-artg"}},
  ],
}};

// ── State ──────────────────────────────────────────────────────
const state = {{
  search:          "",
  activeMainTab:   "vms",
  activeSecPerTab: {{vms:"dashboard", pharma:"citations", retail:"artg"}},
  activeSrc:   new Set(),
  activeSev:   new Set(["high","medium","low"]),
  activeSent:  new Set(["positive","neutral","negative",""]),
  sort:        "severity",
  citPage:     1,
  citPageSize: 50,
  artgIngFilter: null,
  artgGrouped:   false,
}};

// ── Source labels ──────────────────────────────────────────────
const SRC_LABELS = {{
  pubmed:"PubMed", tga:"TGA", tga_consultations:"TGA Consult.",
  fda:"FDA", fda_australia:"FDA → AU", artg:"ARTG",
  adverse_events:"Adverse Events", advisory_committee:"Advisory Cmte",
  iherb:"iHerb", chemist_warehouse:"Chemist WH",
}};

// ── Helpers ────────────────────────────────────────────────────
function esc(s){{ return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;") }}
function sevClass(s){{ return {{high:"sev-high",medium:"sev-medium",low:"sev-low"}}[s]||"sev-low" }}
function sevBadge(s){{ return `<span class="badge badge-sev-${{s}}">${{(s||"?").toUpperCase()}}</span>` }}
function sentBadge(s){{
  if(!s) return "";
  const icon = {{positive:"▲",neutral:"●",negative:"▼"}}[s]||"";
  return `<span class="badge badge-sent-${{s}}">${{icon}} ${{s}}</span>`;
}}
function srcBadge(src){{
  return `<span class="badge badge-src-${{src}}">${{esc(SRC_LABELS[src]||src)}}</span>`;
}}
function relBadge(r){{
  if(!r) return "";
  return `<span class="badge badge-rel-${{r}}">${{r}} rel.</span>`;
}}
function impactBadge(p){{
  if(!p) return "";
  return `<span class="badge badge-impact-${{p}}">${{p}}</span>`;
}}
function fmtDate(d){{ return d ? d.slice(0,10) : "" }}
function setCnt(id,val){{ const e=document.getElementById(id); if(e) e.textContent=val; }}
function highlight(text, q){{
  if(!q || !text) return esc(text);
  const escaped = esc(text);
  const re = new RegExp("("+q.replace(/[.*+?^${{}}()|[\\]\\\\]/g,"\\\\$&")+")","gi");
  return escaped.replace(re,'<mark style="background:#0D9488;color:#fff;border-radius:2px;padding:0 2px">$1</mark>');
}}

// ── Fuzzy match helpers ────────────────────────────────────────
function editDist1(a,b){{
  if(a===b) return true;
  const la=a.length,lb=b.length;
  if(Math.abs(la-lb)>1) return false;
  if(la===lb){{ let d=0; for(let i=0;i<la;i++){{ if(a[i]!==b[i]){{ if(++d>1) return false; }} }} return d===1; }}
  const [s,l]=la<=lb?[a,b]:[b,a];
  for(let i=0;i<=s.length;i++){{ if(s.slice(0,i)===l.slice(0,i)&&s.slice(i)===l.slice(i+1)) return true; }}
  return false;
}}
function textMatches(q,text){{
  if(!q||!text) return !q;
  if(text.includes(q)) return true;
  if(q.length<4) return false;
  const words=text.match(/[a-z0-9]+/g)||[];
  return words.some(w=>Math.abs(w.length-q.length)<=1&&editDist1(q,w));
}}

// ── Filter signals ─────────────────────────────────────────────
function filteredSignals(){{
  const q = state.search.toLowerCase();
  return SIGNALS.filter(s=>{{
    if(state.activeSrc.size>0 && !state.activeSrc.has(s.source)) return false;
    if(!state.activeSev.has(s.severity)) return false;
    const sent=s.sentiment||"";
    if(!state.activeSent.has(sent) && !(sent===""&&state.activeSent.has(""))) return false;
    if(q){{
      const hay=[s.title,s.ingredient,s.summary,s.source,s.event_type,s.signal_type,s.ctx,s.risk_desc].join(" ").toLowerCase();
      if(!textMatches(q,hay)) return false;
    }}
    return true;
  }});
}}
function sortSignals(arr){{
  const SEV={{high:0,medium:1,low:2}};
  const SENT={{negative:0,neutral:1,positive:2,"":3}};
  const sorted=[...arr];
  if(state.sort==="severity")   sorted.sort((a,b)=>SEV[a.severity]-SEV[b.severity]);
  if(state.sort==="recent")     sorted.sort((a,b)=>b.scraped_at.localeCompare(a.scraped_at));
  if(state.sort==="sentiment")  sorted.sort((a,b)=>SENT[a.sentiment||""]-SENT[b.sentiment||""]);
  if(state.sort==="ingredient") sorted.sort((a,b)=>(a.ingredient||"").localeCompare(b.ingredient||""));
  return sorted;
}}

// ── Signal card ────────────────────────────────────────────────
function signalCard(s, q){{
  const extra = [
    s.relevance        ? relBadge(s.relevance) : "",
    s.potential_impact ? impactBadge(s.potential_impact) : "",
  ].filter(Boolean).join("");
  const meta = [
    s.ctx        ? `<span style="color:var(--teal);font-weight:600">${{esc(s.ctx)}}</span>` : "",
    s.ingredient ? `<span>🧪 ${{esc(s.ingredient)}}</span>` : "",
    s.event_type&&s.event_type!=="other" ? `<span>🏷️ ${{esc(s.event_type.replace(/_/g," "))}}</span>` : "",
    s.signal_type ? `<span>🔖 ${{esc(s.signal_type.replace(/_/g," "))}}</span>` : "",
    fmtDate(s.scraped_at) ? `<span>📅 ${{fmtDate(s.scraped_at)}}</span>` : "",
  ].filter(Boolean).join("");
  return `
<div class="card ${{sevClass(s.severity)}}" data-id="${{esc(s.id)}}">
  <div class="card-top">
    <div class="card-title"><a href="${{esc(s.url)}}" target="_blank" rel="noopener">${{highlight(s.title,q)}}</a></div>
    <div class="card-badges">${{srcBadge(s.source)}}${{sevBadge(s.severity)}}${{sentBadge(s.sentiment)}}${{extra}}</div>
  </div>
  ${{meta ? `<div class="card-meta">${{meta}}</div>` : ""}}
  ${{s.summary ? `<div class="card-summary">${{highlight(s.summary,q)}}</div>` : ""}}
  ${{s.sent_reason ? `<div class="card-reason">💬 ${{esc(s.sent_reason)}}</div>` : ""}}
  <div class="card-footer">
    <a class="card-link" href="${{esc(s.url)}}" target="_blank" rel="noopener">View source →</a>
    ${{s.sent_conf ? `<span class="card-conf">Confidence: ${{Math.round(s.sent_conf*100)}}%</span>` : ""}}
  </div>
</div>`;
}}

// ── Risk card — uniform fixed height ──────────────────────────
function riskCard(s){{
  const title  = s.risk_desc || s.ingredient || s.title;
  const sevCls = s.severity==="medium" ? "rc-sev rc-sev-med" : "rc-sev";
  return `
<div class="risk-card" data-id="${{esc(s.id)}}" data-sent="${{esc(s.sentiment||"")}}"
     onclick="scrollToSignal(this.dataset.id,this.dataset.sent)" title="${{esc(s.title)}}">
  <div class="rc-title">${{esc(title)}}</div>
  <div class="rc-sub">
    <span>${{esc(s.ctx||s.source)}}</span>
    <span class="${{sevCls}}">${{(s.severity||"low").toUpperCase()}}</span>
  </div>
  <div class="rc-body">${{esc(s.summary||"")}}</div>
</div>`;
}}

function noResults(msg){{
  return `<div class="no-results"><div class="nr-ico">🔍</div><div>${{msg||"No results match your filters."}}</div></div>`;
}}

// ── Section: Dashboard ─────────────────────────────────────────
function renderDashboard(){{
  document.getElementById("dash-gen").textContent = "Generated " + STATS.generated_at;
  const fs = filteredSignals();
  const q  = state.search;

  // Risk Watch — uses filtered signals
  const negSigs = fs.filter(s=>s.sentiment==="negative").slice(0,6);
  const riskEl  = document.getElementById("dash-risk");
  if(negSigs.length){{
    riskEl.innerHTML = `
      <div class="risk-banner">
        <div class="risk-banner-header">
          <div class="risk-banner-title">⚠️ Risk Watch — ${{negSigs.length}} negative signal${{negSigs.length!==1?"s":""}}</div>
          <span style="font-size:11px;color:var(--slate-dim);cursor:pointer" onclick="filterBySentiment('negative')">Filter feed →</span>
        </div>
        <div class="risk-grid">${{negSigs.map(riskCard).join("")}}</div>
      </div>`;
  }} else {{
    riskEl.innerHTML = q
      ? `<div style="padding:10px 0;font-size:12px;color:var(--slate-dim)">No negative signals match "<b style="color:var(--off)">${{esc(q)}}</b>".</div>`
      : "";
  }}

  // Sentiment donut — recomputed from filtered signals
  const sentCounts={{positive:0,neutral:0,negative:0}};
  fs.forEach(s=>{{ const k=s.sentiment||"neutral"; if(sentCounts[k]!==undefined) sentCounts[k]++; }});
  const total  = sentCounts.positive+sentCounts.neutral+sentCounts.negative;
  const negPct = total ? sentCounts.negative/total*100 : 0;
  const neuPct = total ? sentCounts.neutral/total*100  : 0;
  const posPct = total ? sentCounts.positive/total*100 : 0;
  const n1=negPct, n2=n1+neuPct;
  document.getElementById("donutEl").style.background =
    total ? `conic-gradient(#DC2626 0% ${{n1}}%, #64748B ${{n1}}% ${{n2}}%, #059669 ${{n2}}% 100%)`
           : "var(--navy-lite)";
  const sentKeys=[
    ["#DC2626","negative","Negative",sentCounts.negative,negPct],
    ["#64748B","neutral","Neutral",  sentCounts.neutral, neuPct],
    ["#059669","positive","Positive",sentCounts.positive,posPct],
  ];
  document.getElementById("donutLegend").innerHTML = sentKeys.map(([c,k,l,n,p])=>`
    <div class="donut-row" style="cursor:pointer;border-radius:4px;padding:3px 5px;margin:-3px -5px;transition:background .12s"
         onmouseover="this.style.background='rgba(255,255,255,.05)'" onmouseout="this.style.background=''"
         onclick="filterBySentiment('${{k}}')" title="Show ${{l.toLowerCase()}} signals">
      <div class="donut-dot" style="background:${{c}}"></div>
      <span style="font-size:12px;color:var(--off2)">${{l}}</span>
      <span class="donut-val" style="color:${{c}}">${{n}}</span>
      <span style="font-size:10px;color:var(--slate-dim)">${{p.toFixed(0)}}%</span>
    </div>`).join("");

  // Named sentiment breakdown — deduplicated by ingredient, with counts
  const sbEl=document.getElementById("sentBreakdown");
  if(sbEl){{
    const buckets={{"negative":"#F87171","neutral":"#94A3B8","positive":"#6EE7B7"}};
    sbEl.innerHTML=Object.entries(buckets).map(([sent,color])=>{{
      const bucket=fs.filter(s=>s.sentiment===sent);
      if(!bucket.length) return "";
      // Group by ingredient to deduplicate ("Consultation, Consultation, ..." → "Consultation (5)")
      const groups={{}};
      bucket.forEach(s=>{{
        const key=(s.ingredient||s.title.slice(0,30)).trim();
        if(!groups[key]) groups[key]=[];
        groups[key].push(s);
      }});
      const sorted=Object.entries(groups).sort((a,b)=>b[1].length-a[1].length);
      const shown=sorted.slice(0,5);
      const items=shown.map(([ing,sigs])=>{{
        const rep=sigs[0];
        const label=sigs.length>1?`${{esc(ing)}} (${{sigs.length}})`:esc(ing);
        return `<span style="cursor:pointer;text-decoration:underline;text-decoration-color:rgba(255,255,255,.3)"
                     data-id="${{esc(rep.id)}}" data-sent="${{esc(sent)}}"
                     onclick="scrollToSignal(this.dataset.id,this.dataset.sent)"
                     title="${{esc(rep.risk_desc||rep.title)}}">${{label}}</span>`;
      }}).join(", ");
      const more=sorted.length>5?` <span style="color:var(--slate-dim)">+${{sorted.length-5}} more</span>`:"";
      return `<div style="margin-top:7px;font-size:11px;line-height:1.6">
        <span style="color:${{color}};font-weight:700;text-transform:capitalize">${{sent}}:</span>
        <span style="color:var(--off2)"> ${{items}}${{more}}</span>
      </div>`;
    }}).join("");
  }}

  // Trending — genuine spikes only, max 5, baseline detection
  const tEl=document.getElementById("trendBars");
  const ql=q.toLowerCase();
  if(!TRENDING.length){{
    tEl.innerHTML=`<div class="empty-state" style="padding:20px 8px">No trending data yet.<br><span style="font-size:11px">Requires multiple pipeline runs.</span></div>`;
  }} else {{
    // Detect "all same ratio" → database too new for meaningful spikes
    const ratios=TRENDING.map(t=>t.ratio);
    const minR=Math.min(...ratios), maxR=Math.max(...ratios);
    const allSame=(maxR-minR)<0.05;
    if(allSame && !q){{
      tEl.innerHTML=`<div style="text-align:center;padding:18px 8px;color:var(--slate-dim);font-size:12px;line-height:1.7">
        <div style="font-size:22px;margin-bottom:4px">📊</div>
        No unusual spikes detected<br>
        <span style="font-size:11px">Monitoring baseline building — ${{TRENDING.length}} ingredient${{TRENDING.length!==1?"s":""}} tracked at ×${{minR.toFixed(1)}}</span>
      </div>`;
    }} else {{
      // Sort by ratio desc, take top 5, bars proportional to each other
      const pool=q ? TRENDING.filter(t=>textMatches(ql,t.ingredient.toLowerCase())) : TRENDING;
      const sorted=[...(pool.length?pool:TRENDING)].sort((a,b)=>b.ratio-a.ratio).slice(0,5);
      const barMax=Math.max(...sorted.map(t=>t.ratio),1);
      tEl.innerHTML=sorted.map(t=>{{
        const pct=Math.min((t.ratio/barMax)*100,100);
        const matched=q && textMatches(ql,t.ingredient.toLowerCase());
        const dimmed=q && !matched;
        return `<div class="trend-row" style="cursor:pointer;opacity:${{dimmed?0.35:1}}" data-ing="${{esc(t.ingredient)}}" onclick="filterByIngredient(this.dataset.ing)" title="${{t.count_7d}} signal${{t.count_7d!==1?"s":""}} this week">
          <div class="trend-ing" style="color:${{matched?"var(--teal)":""}}">${{esc(t.ingredient)}}</div>
          <div class="trend-bar-wrap"><div class="trend-bar" style="width:${{pct}}%;background:${{matched?"var(--teal)":"var(--teal-dim)"}}"></div></div>
          <div class="trend-meta">${{t.count_7d}} signal${{t.count_7d!==1?"s":""}} · ×${{t.ratio.toFixed(1)}}</div>
        </div>`;
      }}).join("");
    }}
  }}

  // Source bars — recomputed from filtered signals (rendered into collapsed panel)
  const srcEl=document.getElementById("srcBars");
  const srcCounts={{}};
  fs.forEach(s=>{{ srcCounts[s.source]=(srcCounts[s.source]||0)+1; }});
  const srcBase=Object.entries(STATS.src_counts||{{}});
  const srcE=srcBase.map(([src,base])=>[src, srcCounts[src]||0, base]).sort((a,b)=>b[1]-a[1]);
  const maxSrc=Math.max(...srcE.map(e=>e[1]),1);
  srcEl.innerHTML=srcE.map(([src,cnt,base])=>{{
    const pct=(cnt/Math.max(maxSrc,1))*100;
    const dimLabel=q&&cnt===0?`style="opacity:.4"`:"";
    return `<div class="vis-bar-row" ${{dimLabel}}>
      <div class="vis-label">${{esc(SRC_LABELS[src]||src)}}</div>
      <div class="vis-track"><div class="vis-fill" style="width:${{pct}}%;background:var(--teal)"></div></div>
      <div class="vis-val">${{cnt}}${{q&&cnt!==base?`<span style="color:var(--slate-dim);font-size:9px">/${{base}}</span>`:""}}</div>
    </div>`;
  }}).join("");
  // Populate the collapsed header summary line
  const srcCountEl=document.getElementById("srcCount");
  if(srcCountEl){{
    const parts=srcE.filter(([,cnt])=>cnt>0).map(([src,cnt])=>`${{SRC_LABELS[src]||src}}: ${{cnt}}`);
    srcCountEl.textContent=parts.length?`(${{parts.join(" · ")}})` : "";
  }}

  // High severity — from filtered signals
  const highSigs=sortSignals(fs.filter(s=>s.severity==="high")).slice(0,5);
  const hEl=document.getElementById("dashHighCards");
  hEl.innerHTML=highSigs.length
    ?`<div class="cards">${{highSigs.map(s=>signalCard(s,state.search)).join("")}}</div>`
    :`<div class="empty-state">No HIGH severity signals match current filters.</div>`;
}}

// ── Section: Regulatory ────────────────────────────────────────
function renderRegulatory(){{
  const regSrc=["tga","fda","fda_australia","artg","tga_consultations","advisory_committee"];
  const sigs=sortSignals(filteredSignals().filter(s=>regSrc.includes(s.source)));
  const el=document.getElementById("regCards");
  setCnt("cnt-regulatory",sigs.length);
  el.innerHTML=sigs.length?`<div class="cards">${{sigs.map(s=>signalCard(s,state.search)).join("")}}</div>`:noResults("No regulatory signals match your filters.");
}}

// ── Section: Research ─────────────────────────────────────────
function renderResearch(){{
  const sigs=sortSignals(filteredSignals().filter(s=>s.source==="pubmed"));
  const el=document.getElementById("resCards");
  setCnt("cnt-research",sigs.length);
  const groups={{}};
  sigs.forEach(s=>{{ const g=s.signal_type||"other"; if(!groups[g]) groups[g]=[]; groups[g].push(s); }});
  const order=["safety_concern","efficacy_claim","regulatory_implication","other"];
  const labels={{safety_concern:"⚠️ Safety Concerns",efficacy_claim:"✅ Efficacy Evidence",regulatory_implication:"⚖️ Regulatory Implications",other:"📄 Other Research"}};
  if(!sigs.length){{ el.innerHTML=noResults("No research articles match your filters."); return; }}
  el.innerHTML=order.filter(g=>groups[g]&&groups[g].length).map(g=>`
    <div style="margin-bottom:24px">
      <div style="font-size:13px;font-weight:700;color:var(--off2);margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--border)">
        ${{labels[g]||g}} <span style="font-size:11px;color:var(--slate-dim);font-weight:400">(${{groups[g].length}})</span>
      </div>
      <div class="cards">${{groups[g].map(s=>signalCard(s,state.search)).join("")}}</div>
    </div>`).join("");
}}

// ── Section: Adverse Events ────────────────────────────────────
function renderAdverse(){{
  const sigs=sortSignals(filteredSignals().filter(s=>s.source==="adverse_events"));
  const el=document.getElementById("aeCards");
  setCnt("cnt-adverse",sigs.length);
  el.innerHTML=sigs.length?`<div class="cards">${{sigs.map(s=>signalCard(s,state.search)).join("")}}</div>`:noResults("No adverse event signals match your filters.");
}}

// ── Section: Citations ─────────────────────────────────────────
let citFiltered = [];

function filteredCitations(){{
  const q    = state.search.toLowerCase();
  const auth = document.getElementById("citAuth")?.value||"";
  const fac  = document.getElementById("citFacility")?.value||"";
  const cat  = document.getElementById("citCat")?.value||"";
  const srt  = document.getElementById("citSort")?.value||"date_desc";

  let arr = CITATIONS.filter(c=>{{
    if(!state.activeSev.has(c.sev)) return false;
    if(auth && c.auth!==auth) return false;
    if(fac  && c.facility!==fac) return false;
    if(cat  && c.cat!==cat) return false;
    if(q){{
      const hay=[c.summary,c.company,c.cat,c.facility,c.product,c.country].join(" ").toLowerCase();
      if(!textMatches(q,hay)) return false;
    }}
    return true;
  }});

  const SEV={{high:0,medium:1}};
  if(srt==="date_desc") arr.sort((a,b)=>b.date.localeCompare(a.date));
  if(srt==="date_asc")  arr.sort((a,b)=>a.date.localeCompare(b.date));
  if(srt==="sev")       arr.sort((a,b)=>(SEV[a.sev]||1)-(SEV[b.sev]||1));
  if(srt==="auth")      arr.sort((a,b)=>a.auth.localeCompare(b.auth));
  return arr;
}}

function citationCard(c, q){{
  const authBadgeClass = "badge-"+c.auth.toLowerCase();
  const sevClass = c.sev==="high"?"sev-high":"sev-medium";
  const sevBadgeHtml = c.sev==="high"
    ? `<span class="badge badge-sev-high">HIGH</span>`
    : `<span class="badge badge-sev-medium">MED</span>`;
  return `
<div class="cit-card ${{sevClass}}">
  <div class="cc-top">
    <span class="badge ${{authBadgeClass}}">${{esc(c.auth)}}</span>
    ${{sevBadgeHtml}}
    ${{c.facility?`<span class="badge" style="background:var(--navy-lite);color:var(--slate)">${{esc(c.facility)}}</span>`:"" }}
    ${{c.cat?`<span class="badge" style="background:var(--navy-lite);color:var(--slate-dim);font-size:10px">${{esc(c.cat)}}</span>`:"" }}
  </div>
  <div class="cc-sum">${{highlight(c.summary,q)}}</div>
  <div class="cc-foot">
    <div class="cc-meta">
      ${{c.company?`<span>🏢 ${{esc(c.company)}}</span>`:"" }}
      ${{c.date?`<span>📅 ${{c.date}}</span>`:"" }}
      ${{c.country?`<span>🌐 ${{esc(c.country)}}</span>`:"" }}
      ${{c.product?`<span>📦 ${{esc(c.product)}}</span>`:"" }}
    </div>
    ${{c.url?`<a class="card-link" href="${{esc(c.url)}}" target="_blank" rel="noopener">View →</a>`:"" }}
  </div>
</div>`;
}}

function renderCitations(){{
  state.citPage=1;
  citFiltered=filteredCitations();
  const el=document.getElementById("citCards");
  const cntEl=document.getElementById("citCount");
  const moreBtn=document.getElementById("citMore");
  setCnt("cnt-citations",citFiltered.length.toLocaleString());
  if(cntEl) cntEl.textContent=`${{citFiltered.length.toLocaleString()}} citations`;
  const slice=citFiltered.slice(0,state.citPageSize);
  if(!slice.length){{ el.innerHTML=noResults("No citations match your filters."); moreBtn.style.display="none"; return; }}
  el.innerHTML=slice.map(c=>citationCard(c,state.search)).join("");
  moreBtn.style.display=citFiltered.length>state.citPageSize?"block":"none";
  moreBtn.textContent=`Load more (${{citFiltered.length-state.citPageSize}} remaining)…`;
}}

function loadMoreCitations(){{
  state.citPage++;
  const start=(state.citPage-1)*state.citPageSize;
  const slice=citFiltered.slice(start,start+state.citPageSize);
  document.getElementById("citCards").insertAdjacentHTML("beforeend",slice.map(c=>citationCard(c,state.search)).join(""));
  const remaining=citFiltered.length-state.citPage*state.citPageSize;
  const btn=document.getElementById("citMore");
  if(remaining<=0) btn.style.display="none";
  else btn.textContent=`Load more (${{remaining}} remaining)…`;
}}

// ── Section: Trends ────────────────────────────────────────────
function renderTrends(){{
  const el=document.getElementById("trendsContent");
  let html=`<div style="margin-bottom:28px">
    <div class="sec-header" style="margin-bottom:14px">
      <h3 style="font-size:17px;color:var(--white);font-family:Georgia,serif">📈 Trending Ingredients</h3>
      <div class="sec-sub">7-day vs 30-day baseline. Spike = 7d ÷ expected 7d.</div>
    </div>`;
  if(TRENDING.length){{
    const maxR=Math.max(...TRENDING.map(t=>t.ratio),1);
    html+=`<div class="panel">`+TRENDING.map(t=>{{
      const pct=Math.min((t.ratio/maxR)*100,100);
      return `<div class="trend-row" style="cursor:pointer" data-ing="${{esc(t.ingredient)}}" onclick="filterByIngredient(this.dataset.ing)" title="Filter to ${{esc(t.ingredient)}} signals">
        <div class="trend-ing" style="min-width:200px">${{esc(t.ingredient)}}</div>
        <div class="trend-bar-wrap"><div class="trend-bar" style="width:${{pct}}%"></div></div>
        <div class="trend-meta" style="min-width:220px;text-align:left;color:var(--slate)">
          7d=${{t.count_7d}} · 30d=${{t.count_30d}} · avg=${{t.avg_daily.toFixed(2)}} · <b style="color:var(--teal)">×${{t.ratio.toFixed(1)}}</b>
        </div>
      </div>`;
    }}).join("")+`</div>`;
  }} else {{
    html+=`<div class="empty-state">No spikes above 2× threshold. Requires multiple pipeline runs.</div>`;
  }}
  html+=`</div><div style="margin-bottom:28px">
    <div class="sec-header" style="margin-bottom:14px">
      <h3 style="font-size:17px;color:var(--white);font-family:Georgia,serif">🔄 Claim Shifts</h3>
      <div class="sec-sub">Dominant signal type changed in the second half of the 30-day window.</div>
    </div>`;
  if(SHIFTS.length){{
    html+=SHIFTS.map(c=>`
      <div class="shift-card">
        <div class="shift-ing">${{esc(c.ingredient)}}</div>
        <div class="shift-arrow">
          <span class="shift-from">${{esc(c.from)}}</span>
          <span style="color:var(--slate)">→</span>
          <span class="shift-to">${{esc(c.to)}}</span>
          <span style="color:var(--slate-dim);font-size:11px">(${{c.old_count}} → ${{c.new_count}})</span>
        </div>
        <div class="shift-desc">${{esc(c.desc)}}</div>
      </div>`).join("");
  }} else {{
    html+=`<div class="empty-state">No claim shifts detected.</div>`;
  }}
  html+=`</div><div style="margin-bottom:28px">
    <div class="sec-header" style="margin-bottom:14px">
      <h3 style="font-size:17px;color:var(--white);font-family:Georgia,serif">💬 Ingredient Sentiment Scores</h3>
      <div class="sec-sub">−1.0 = all negative · 0 = neutral · +1.0 = all positive.</div>
    </div>
    <div class="panel">`;
  if(ING_SENT.length){{
    html+=[...ING_SENT].sort((a,b)=>a.score_30d-b.score_30d).map(s=>{{
      const score=s.score_30d;
      const color=score<-0.3?"#DC2626":score>0.3?"#059669":"#64748B";
      const barOff=50+score*50;
      const riskB=s.rising?`<span class="badge badge-sev-high" style="font-size:9px">RISING RISK</span>`:"";
      return `<div class="ing-sent-row">
        <div class="ing-sent-name">${{esc(s.ingredient)}} ${{riskB}}</div>
        <div class="sent-bar-wrap">
          <div class="sent-bar-track">
            <div style="position:absolute;left:50%;top:0;width:1px;height:100%;background:var(--border)"></div>
            <div class="sent-bar-fill" style="width:${{Math.abs(score)*50}}%;left:${{score<0?barOff:50}}%;background:${{color}}"></div>
          </div>
        </div>
        <div class="sent-score" style="color:${{color}}">${{score>=0?"+":""}}${{score.toFixed(2)}}</div>
        <div class="sent-n">n=${{s.n}}</div>
      </div>`;
    }}).join("");
  }} else {{
    html+=`<div class="empty-state">No ingredient sentiment data yet.</div>`;
  }}
  html+=`</div></div>`;
  setCnt("cnt-trends",TRENDING.length+SHIFTS.length);
  el.innerHTML=html;
}}

// ── TAB 2: Warning Letters ─────────────────────────────────────
function renderWarnings(){{
  const el=document.getElementById("warningsContent");
  const q=state.search.toLowerCase();
  const kw=["warning letter","form 483","483 observation","gmp violation","cgmp","inspection finding"];
  let matches=CITATIONS.filter(c=>{{
    if(c.auth!=="FDA"&&c.auth!=="MHRA"&&c.auth!=="TGA") return false;
    const txt=(c.summary+" "+c.cat+" "+c.company).toLowerCase();
    if(q&&![c.summary,c.company,c.cat,c.country].join(" ").toLowerCase().includes(q)) return false;
    return kw.some(k=>txt.includes(k));
  }}).sort((a,b)=>b.date.localeCompare(a.date));
  setCnt("cnt-warnings",matches.length);
  if(!matches.length){{ el.innerHTML=`<div class="empty-state">No warning letters found matching current search.</div>`; return; }}
  const byAuth={{}};
  matches.forEach(c=>{{ byAuth[c.auth]=(byAuth[c.auth]||0)+1; }});
  el.innerHTML=`<div class="panel" style="margin-bottom:16px">
    <div class="panel-title"><span class="ico">📊</span>Summary</div>
    <div style="display:flex;gap:16px;flex-wrap:wrap">
      ${{Object.entries(byAuth).map(([a,n])=>`
        <div style="text-align:center;padding:8px 16px;background:var(--navy);border-radius:6px">
          <div style="font-size:22px;font-weight:700;color:var(--amber)">${{n}}</div>
          <div style="font-size:11px;color:var(--slate-dim)">${{esc(a)}}</div>
        </div>`).join("")}}
      <div style="text-align:center;padding:8px 16px;background:var(--navy);border-radius:6px">
        <div style="font-size:22px;font-weight:700;color:var(--red)">${{matches.filter(c=>c.sev==="high").length}}</div>
        <div style="font-size:11px;color:var(--slate-dim)">HIGH severity</div>
      </div>
    </div>
  </div>`+matches.map(c=>citationCard(c,q)).join("");
}}

// ── TAB 2: Enforcement Actions ─────────────────────────────────
function renderEnforcement(){{
  const el=document.getElementById("enforcementContent");
  const q=state.search.toLowerCase();
  const kw=["import alert","recall","consent decree","injunction","seizure","enforcement","banned","prohibited"];
  let matches=CITATIONS.filter(c=>{{
    const txt=(c.summary+" "+c.cat).toLowerCase();
    if(q&&![c.summary,c.company,c.cat,c.country].join(" ").toLowerCase().includes(q)) return false;
    return kw.some(k=>txt.includes(k));
  }}).sort((a,b)=>b.date.localeCompare(a.date));
  setCnt("cnt-enforcement",matches.length);
  if(!matches.length){{ el.innerHTML=`<div class="empty-state">No enforcement actions match current search.</div>`; return; }}
  const byAuth={{}};
  matches.forEach(c=>{{ if(!byAuth[c.auth]) byAuth[c.auth]=[]; byAuth[c.auth].push(c); }});
  const authOrder=["FDA","TGA","MHRA","EFSA","BfR"];
  let html="";
  [...authOrder,...Object.keys(byAuth).filter(a=>!authOrder.includes(a))].forEach(auth=>{{
    const group=byAuth[auth]; if(!group||!group.length) return;
    html+=`<div style="margin-bottom:24px">
      <div style="font-size:13px;font-weight:700;color:var(--off2);margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px">
        <span class="badge badge-${{auth.toLowerCase()}}">${{esc(auth)}}</span>
        ${{group.length}} actions
        <span style="font-size:11px;color:var(--red);font-weight:400">${{group.filter(c=>c.sev==="high").length}} HIGH</span>
      </div>
      ${{group.map(c=>citationCard(c,q)).join("")}}
    </div>`;
  }});
  el.innerHTML=html;
}}

// ── TAB 2: Facility Compliance ─────────────────────────────────
function renderFacilities(){{
  const el=document.getElementById("facilitiesContent");
  const q=state.search.toLowerCase();
  let cits=q?CITATIONS.filter(c=>[c.summary,c.company,c.facility,c.cat].join(" ").toLowerCase().includes(q)):CITATIONS;
  const byFac={{}};
  cits.forEach(c=>{{
    const fac=c.facility||"Unknown";
    if(!byFac[fac]) byFac[fac]={{count:0,high:0,companies:{{}}}};
    byFac[fac].count++;
    if(c.sev==="high") byFac[fac].high++;
    if(c.company) byFac[fac].companies[c.company]=(byFac[fac].companies[c.company]||0)+1;
  }});
  setCnt("cnt-facilities",Object.keys(byFac).length);
  const facOrder=["Supplement / Nutraceutical","General Pharma","API Manufacturer","Sterile / Parenteral","Biologics / Vaccine","Medical Device","Compounding","Unknown"];
  const sorted=[...facOrder,...Object.keys(byFac).filter(f=>!facOrder.includes(f))].filter(f=>byFac[f]);
  let html=`<div class="panel" style="margin-bottom:20px">
    <div class="panel-title"><span class="ico">📊</span>Facility Type Overview</div>
    <table class="cmp-table">
      <thead><tr><th>Facility Type</th><th style="text-align:right">Citations</th><th style="text-align:right">HIGH</th><th>Top Companies</th></tr></thead>
      <tbody>`;
  sorted.forEach(fac=>{{
    const d=byFac[fac];
    const topCo=Object.entries(d.companies).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([c,n])=>`${{esc(c)}} (${{n}})`).join(", ");
    html+=`<tr>
      <td style="font-weight:600;color:var(--off)">${{esc(fac)}}</td>
      <td style="text-align:right;color:var(--white);font-weight:700">${{d.count}}</td>
      <td style="text-align:right;color:var(--red);font-weight:700">${{d.high||"—"}}</td>
      <td style="color:var(--slate-dim);font-size:11px">${{topCo||"—"}}</td>
    </tr>`;
  }});
  html+=`</tbody></table></div>`;
  const topFac=sorted[0];
  if(topFac){{
    const topCits=cits.filter(c=>(c.facility||"Unknown")===topFac).sort((a,b)=>b.date.localeCompare(a.date)).slice(0,10);
    html+=`<div style="font-size:13px;font-weight:700;color:var(--off2);margin-bottom:10px">${{esc(topFac)}} — Recent Citations</div>`;
    html+=topCits.map(c=>citationCard(c,q)).join("");
  }}
  el.innerHTML=html;
}}

// ── TAB 3: ARTG Listings ───────────────────────────────────────
function renderArtg(){{
  const allArtg=sortSignals(filteredSignals().filter(s=>s.source==="artg"));
  const el=document.getElementById("artgCards");
  setCnt("cnt-artg",allArtg.length);

  // Apply ingredient filter if set
  const sigs=state.artgIngFilter
    ? allArtg.filter(s=>s.ingredient===state.artgIngFilter)
    : allArtg;

  const ings=[...new Set(allArtg.map(s=>s.ingredient).filter(Boolean))];

  // Stat boxes
  const listingsActive=!state.artgIngFilter&&!state.artgGrouped;
  const groupedActive=state.artgGrouped;
  const statBoxes=`
    <div style="display:flex;gap:16px;flex-wrap:wrap;align-items:flex-start">
      <div onclick="artgShowAll()" style="text-align:center;padding:8px 16px;background:${{listingsActive?"var(--teal-bg)":"var(--navy)"}};border:1px solid ${{listingsActive?"var(--teal)":"var(--border)"}};border-radius:6px;cursor:pointer;transition:all .12s" title="Show all listings">
        <div style="font-size:22px;font-weight:700;color:${{listingsActive?"var(--teal)":"var(--purple)"}}">${{allArtg.length}}</div>
        <div style="font-size:11px;color:var(--slate-dim)">New Listings</div>
      </div>
      <div onclick="artgToggleGrouped()" style="text-align:center;padding:8px 16px;background:${{groupedActive?"rgba(124,58,237,.15)":"var(--navy)"}};border:1px solid ${{groupedActive?"var(--purple)":"var(--border)"}};border-radius:6px;cursor:pointer;transition:all .12s" title="Group by ingredient">
        <div style="font-size:22px;font-weight:700;color:${{groupedActive?"var(--purple)":"var(--teal)"}}">${{ings.length}}</div>
        <div style="font-size:11px;color:var(--slate-dim)">Unique Ingredients</div>
      </div>
      <div style="flex:1;padding-top:4px">
        ${{ings.slice(0,12).map(i=>{{
          const isActive=state.artgIngFilter===i;
          return `<span onclick="artgFilterIng('${{esc(i)}}')" style="cursor:pointer;display:inline-block;margin:2px;padding:2px 8px;border-radius:99px;font-size:11px;font-weight:600;background:${{isActive?"var(--purple)":"var(--navy-lite)"}};color:${{isActive?"#fff":"var(--slate)"}};border:1px solid ${{isActive?"var(--purple)":"var(--border)"}};transition:all .12s">${{esc(i)}}</span>`;
        }}).join("")}}
        ${{state.artgIngFilter?`<span onclick="artgShowAll()" style="cursor:pointer;display:inline-block;margin:2px;padding:2px 8px;border-radius:99px;font-size:11px;font-weight:600;background:var(--navy-lite);color:var(--teal);border:1px solid var(--teal)">✕ Clear filter</span>`:"" }}
      </div>
    </div>`;

  // Cards / grouped view
  let cardsHtml="";
  if(state.artgGrouped){{
    const byIng={{}};
    allArtg.forEach(s=>{{ const i=s.ingredient||"Unknown"; if(!byIng[i]) byIng[i]=[]; byIng[i].push(s); }});
    const sorted=Object.entries(byIng).sort((a,b)=>b[1].length-a[1].length);
    cardsHtml=sorted.map(([ing,items])=>`
      <div style="margin-bottom:24px">
        <div style="font-size:13px;font-weight:700;color:var(--off2);margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px">
          <span onclick="artgFilterIng('${{esc(ing)}}')" style="cursor:pointer;color:var(--teal);text-decoration:underline">${{esc(ing)}}</span>
          <span style="font-size:11px;color:var(--slate-dim);font-weight:400">${{items.length}} listing${{items.length!==1?"s":""}}</span>
        </div>
        <div class="cards">${{items.map(s=>signalCard(s,state.search)).join("")}}</div>
      </div>`).join("");
  }} else if(sigs.length){{
    cardsHtml=`<div class="cards">${{sigs.map(s=>signalCard(s,state.search)).join("")}}</div>`;
  }} else {{
    cardsHtml=noResults("No ARTG listings match your filters.");
  }}

  // Coming Soon section
  const comingSoon=`
    <div style="margin-top:32px;padding:20px;background:var(--navy-mid);border:1px solid var(--border);border-radius:8px;border-left:3px solid var(--purple)">
      <div style="font-size:13px;font-weight:700;color:var(--purple);margin-bottom:12px;text-transform:uppercase;letter-spacing:.06em">🚧 Coming Soon — Retail Intelligence</div>
      <div style="display:flex;gap:16px;flex-wrap:wrap">
        ${{[
          ["🛒","iHerb AU","Supplement product listings, pricing, reviews — blocked by Cloudflare bot protection"],
          ["💊","Chemist Warehouse","Pharmacy VMS product range — requires Playwright (client-side Next.js)"],
          ["📦","Amazon AU","Health & supplement category monitoring — API integration planned"],
        ].map(([ico,name,desc])=>`
          <div style="flex:1;min-width:220px;padding:14px;background:var(--navy);border-radius:6px;border:1px solid var(--border)">
            <div style="font-size:14px;font-weight:700;color:var(--off2);margin-bottom:4px">${{ico}} ${{name}}</div>
            <div style="font-size:12px;color:var(--slate-dim);line-height:1.5">${{desc}}</div>
          </div>`).join("")}}
      </div>
      <div style="font-size:11px;color:var(--slate-dim);margin-top:12px">iHerb AU · Chemist Warehouse · Amazon AU — retail monitoring in development</div>
    </div>`;

  el.innerHTML=`<div class="panel" style="margin-bottom:16px">${{statBoxes}}</div>${{cardsHtml}}${{comingSoon}}`;
}}

function artgShowAll(){{
  state.artgIngFilter=null;
  state.artgGrouped=false;
  renderArtg();
}}
function artgToggleGrouped(){{
  state.artgGrouped=!state.artgGrouped;
  state.artgIngFilter=null;
  renderArtg();
}}
function artgFilterIng(ing){{
  if(state.artgIngFilter===ing){{ state.artgIngFilter=null; }} else {{ state.artgIngFilter=ing; state.artgGrouped=false; }}
  renderArtg();
}}

// ── TAB 3: Product Monitor ─────────────────────────────────────
function renderMonitor(){{
  setCnt("cnt-monitor","");
  document.getElementById("monitorContent").innerHTML=`
    <div class="monitor-card">
      <div class="monitor-status"><div class="status-dot status-blocked"></div>
        <span style="font-weight:700;color:var(--red);font-size:14px">iHerb AU — Blocked by Cloudflare</span></div>
      <div style="font-size:13px;color:var(--off2);margin-bottom:10px">
        iHerb uses Cloudflare bot protection (<code style="background:var(--navy);padding:1px 5px;border-radius:3px;font-size:11px">cf-mitigated: challenge</code>).
        Products load client-side via React; plain HTTP is intercepted at the edge.
      </div>
      <div style="font-size:12px;color:var(--slate-dim)">
        <b style="color:var(--amber)">To enable:</b> Install Playwright + chromium
        (<code style="background:var(--navy);padding:1px 5px;border-radius:3px">playwright install chromium</code>)
        and update <code style="background:var(--navy);padding:1px 5px;border-radius:3px">scrapers/retail.py</code> to use a stealth browser context.
      </div>
    </div>
    <div class="monitor-card">
      <div class="monitor-status"><div class="status-dot status-blocked"></div>
        <span style="font-weight:700;color:var(--red);font-size:14px">Chemist Warehouse — Client-Side Next.js</span></div>
      <div style="font-size:13px;color:var(--off2);margin-bottom:10px">
        Product grid is rendered client-side. The <code style="background:var(--navy);padding:1px 5px;border-radius:3px;font-size:11px">__NEXT_DATA__</code>
        payload contains only category metadata — no product listings.
      </div>
      <div style="font-size:12px;color:var(--slate-dim)">
        <b style="color:var(--amber)">To enable:</b> Use Playwright to load the page fully and extract
        <code style="background:var(--navy);padding:1px 5px;border-radius:3px">div[data-testid="product-grid"]</code> after hydration.
      </div>
    </div>
    <div class="monitor-card">
      <div class="monitor-status"><div class="status-dot status-ok"></div>
        <span style="font-weight:700;color:var(--green);font-size:14px">ARTG Registry — Active</span></div>
      <div style="font-size:13px;color:var(--off2)">
        ARTG new listings scraped successfully via TGA base page + individual entry pages. See ARTG Listings tab for current data.
      </div>
    </div>`;
}}

// ── TAB 3: Launch Timeline ─────────────────────────────────────
function renderTimeline(){{
  const el=document.getElementById("timelineContent");
  const q=state.search.toLowerCase();
  const sigs=SIGNALS.filter(s=>s.source==="artg"||s.source==="tga"||s.source==="advisory_committee")
    .filter(s=>!q||[s.title,s.ingredient,s.summary].join(" ").toLowerCase().includes(q))
    .sort((a,b)=>b.scraped_at.localeCompare(a.scraped_at));
  setCnt("cnt-timeline",sigs.length);
  if(!sigs.length){{ el.innerHTML=`<div class="empty-state">No timeline events match current search.</div>`; return; }}
  const byMonth={{}};
  sigs.forEach(s=>{{ const m=(s.scraped_at||"").slice(0,7)||"Unknown"; if(!byMonth[m]) byMonth[m]=[]; byMonth[m].push(s); }});
  let html="";
  Object.entries(byMonth).sort((a,b)=>b[0].localeCompare(a[0])).forEach(([month,items])=>{{
    html+=`<div style="margin-bottom:24px">
      <div style="font-size:12px;font-weight:700;color:var(--slate-dim);text-transform:uppercase;letter-spacing:.06em;margin-bottom:12px;padding-bottom:6px;border-bottom:1px solid var(--border)">${{month}}</div>
      <div class="timeline">
        ${{items.map(s=>`
          <div class="tl-item">
            <div class="tl-dot ${{sevClass(s.severity)}}"></div>
            <div class="tl-card">
              <div class="tl-date">${{s.scraped_at}} · ${{esc(SRC_LABELS[s.source]||s.source)}}</div>
              <div class="tl-title"><a href="${{esc(s.url)}}" target="_blank" style="color:var(--white)">${{highlight(s.title,q)}}</a></div>
              <div class="tl-meta">🧪 ${{esc(s.ingredient||"—")}} · ${{esc(s.ctx||"")}}</div>
            </div>
          </div>`).join("")}}
      </div>
    </div>`;
  }});
  el.innerHTML=html;
}}

// ── TAB 3: Ingredient Positioning ─────────────────────────────
function renderPositioning(){{
  const el=document.getElementById("positioningContent");
  const artg=SIGNALS.filter(s=>s.source==="artg");
  setCnt("cnt-positioning",artg.length);
  const ingCount={{}};
  artg.forEach(s=>{{ const i=s.ingredient||"Unknown"; ingCount[i]=(ingCount[i]||0)+1; }});
  const ingSorted=Object.entries(ingCount).sort((a,b)=>b[1]-a[1]);
  const typeCount={{}};
  SIGNALS.forEach(s=>{{ if(s.signal_type) typeCount[s.signal_type]=(typeCount[s.signal_type]||0)+1; }});
  const typeSorted=Object.entries(typeCount).sort((a,b)=>b[1]-a[1]);
  const artgSent={{}};
  artg.forEach(s=>{{ const k=s.sentiment||"unknown"; artgSent[k]=(artgSent[k]||0)+1; }});
  el.innerHTML=`<div class="dash-grid-3" style="margin-bottom:24px">
    <div class="panel">
      <div class="panel-title"><span class="ico">🧪</span>Top Ingredients (ARTG)</div>
      ${{ingSorted.slice(0,10).map(([ing,n])=>{{
        const pct=(n/Math.max(ingSorted[0][1],1))*100;
        return `<div class="trend-row">
          <div class="trend-ing" style="min-width:130px;text-transform:capitalize">${{esc(ing)}}</div>
          <div class="trend-bar-wrap"><div class="trend-bar" style="width:${{pct}}%;background:var(--purple)"></div></div>
          <div class="trend-meta">${{n}}</div>
        </div>`;
      }}).join("")}}
    </div>
    <div class="panel">
      <div class="panel-title"><span class="ico">🔖</span>Signal Type Distribution</div>
      ${{typeSorted.slice(0,8).map(([t,n])=>{{
        const pct=(n/Math.max(typeSorted[0][1],1))*100;
        return `<div class="trend-row">
          <div class="trend-ing" style="min-width:130px">${{esc(t.replace(/_/g," "))}}</div>
          <div class="trend-bar-wrap"><div class="trend-bar" style="width:${{pct}}%;background:var(--indigo)"></div></div>
          <div class="trend-meta">${{n}}</div>
        </div>`;
      }}).join("")}}
    </div>
    <div class="panel">
      <div class="panel-title"><span class="ico">💬</span>ARTG Sentiment Split</div>
      ${{Object.entries(artgSent).map(([s,n])=>{{
        const color={{positive:"var(--green)",negative:"var(--red)",neutral:"var(--slate)"}}[s]||"var(--slate-dim)";
        const pct=Math.round(n/Math.max(artg.length,1)*100);
        return `<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
          <div style="width:70px;font-size:12px;color:var(--off2);text-transform:capitalize">${{s}}</div>
          <div style="flex:1;background:var(--navy);border-radius:99px;height:6px;overflow:hidden">
            <div style="width:${{pct}}%;height:100%;background:${{color}};border-radius:99px"></div>
          </div>
          <div style="font-size:11px;color:var(--slate-dim);min-width:44px;text-align:right">${{n}} (${{pct}}%)</div>
        </div>`;
      }}).join("")}}
    </div>
  </div>
  <div class="panel">
    <div class="panel-title"><span class="ico">📋</span>ARTG Listings Detail</div>
    <table class="cmp-table">
      <thead><tr><th>Product/Ingredient</th><th>Sentiment</th><th>Severity</th><th>Date</th><th>Context</th></tr></thead>
      <tbody>
        ${{artg.sort((a,b)=>b.scraped_at.localeCompare(a.scraped_at)).map(s=>`
          <tr>
            <td><a href="${{esc(s.url)}}" target="_blank" style="color:var(--teal)">${{esc(s.ingredient||s.title.slice(0,40))}}</a></td>
            <td>${{sentBadge(s.sentiment)||"—"}}</td>
            <td>${{sevBadge(s.severity)}}</td>
            <td style="color:var(--slate-dim)">${{s.scraped_at}}</td>
            <td style="font-size:11px;color:var(--slate-dim)">${{esc(s.ctx||"")}}</td>
          </tr>`).join("")}}
      </tbody>
    </table>
  </div>`;
}}

// ── Main tab navigation ────────────────────────────────────────
function showMainTab(tabId, btn){{
  if(!btn) btn=document.querySelector(`.mtab[data-mtab="${{tabId}}"]`);
  state.activeMainTab=tabId;
  document.querySelectorAll(".mtab").forEach(b=>b.classList.toggle("active",b.dataset.mtab===tabId));
  document.querySelectorAll(".tab-pane").forEach(p=>p.classList.toggle("active",p.id==="tp-"+tabId));
  document.getElementById("vmsFilterRow").classList.toggle("hidden", tabId!=="vms");
  buildSecNav(tabId);
  renderCurrentSection();
}}

function buildSecNav(tabId){{
  const navEl=document.getElementById("secnavInner");
  const tabs=SEC_NAVS[tabId]||[];
  const activeSec=state.activeSecPerTab[tabId];
  navEl.innerHTML=tabs.map(t=>`
    <button class="snav-tab ${{t.id===activeSec?"active":""}}" data-sec="${{t.id}}" onclick="showSection('${{t.id}}',this)">
      ${{t.label}}<span class="cnt" id="${{t.cnt}}"></span>
    </button>`).join("");
}}

function showSection(secId, btn){{
  const tabId=state.activeMainTab;
  document.querySelectorAll(`#tp-${{tabId}} .section`).forEach(s=>s.classList.remove("active"));
  const target=document.getElementById("sec-"+secId);
  if(target) target.classList.add("active");
  document.querySelectorAll("#secnavInner .snav-tab").forEach(b=>b.classList.toggle("active",b.dataset.sec===secId));
  state.activeSecPerTab[tabId]=secId;
  renderCurrentSection();
}}

function renderCurrentSection(){{
  const tab=state.activeMainTab;
  const sec=state.activeSecPerTab[tab];
  if(tab==="vms"){{
    if(sec==="dashboard")  renderDashboard();
    if(sec==="regulatory") renderRegulatory();
    if(sec==="research")   renderResearch();
    if(sec==="adverse")    renderAdverse();
    if(sec==="trends")     renderTrends();
  }}
  if(tab==="pharma"){{
    if(sec==="citations")   renderCitations();
    if(sec==="warnings")    renderWarnings();
    if(sec==="enforcement") renderEnforcement();
    if(sec==="facilities")  renderFacilities();
  }}
  if(tab==="retail"){{
    if(sec==="artg") renderArtg();
  }}
  updateNavCounts();
  renderFilterChips();
}}

function updateNavCounts(){{
  const regSrc=["tga","fda","fda_australia","artg","tga_consultations","advisory_committee"];
  const fs=filteredSignals();
  setCnt("cnt-dashboard", fs.length);
  setCnt("cnt-regulatory",fs.filter(s=>regSrc.includes(s.source)).length);
  setCnt("cnt-research",  fs.filter(s=>s.source==="pubmed").length);
  setCnt("cnt-adverse",   fs.filter(s=>s.source==="adverse_events").length);
  setCnt("cnt-trends",    TRENDING.length+SHIFTS.length);
  setCnt("cnt-artg",      fs.filter(s=>s.source==="artg").length);
  // Main tab badges — update in real-time as user types (search-only, cross-tab)
  const q=state.search.toLowerCase();
  if(!q){{
    document.getElementById("mcnt-vms").textContent=SIGNALS.length;
    document.getElementById("mcnt-pharma").textContent=CITATIONS.length.toLocaleString();
    document.getElementById("mcnt-retail").textContent=SIGNALS.filter(s=>s.source==="artg").length;
  }} else {{
    const vmsN=SIGNALS.filter(s=>textMatches(q,[s.title,s.ingredient,s.summary,s.source,s.event_type,s.signal_type,s.ctx,s.risk_desc].join(" ").toLowerCase())).length;
    const pharmN=CITATIONS.filter(c=>textMatches(q,[c.summary,c.company,c.cat,c.facility,c.product,c.country].join(" ").toLowerCase())).length;
    const retN=SIGNALS.filter(s=>s.source==="artg"&&textMatches(q,[s.title,s.ingredient,s.summary].join(" ").toLowerCase())).length;
    document.getElementById("mcnt-vms").textContent=vmsN;
    document.getElementById("mcnt-pharma").textContent=pharmN.toLocaleString();
    document.getElementById("mcnt-retail").textContent=retN;
  }}
}}

// ── Stats pills ────────────────────────────────────────────────
function renderStatsPills(){{
  document.getElementById("statsPills").innerHTML=[
    `<div class="stat-pill">Signals <b>${{STATS.total_signals}}</b></div>`,
    `<div class="stat-pill">Citations <b>${{STATS.total_citations.toLocaleString()}}</b></div>`,
    `<div class="stat-pill">🔴 High <b>${{STATS.sev_counts?.high||0}}</b></div>`,
    `<div class="stat-pill">▼ Neg <b style="color:#F87171">${{STATS.sent_counts?.negative||0}}</b></div>`,
    `<div class="stat-pill">▲ Pos <b style="color:#6EE7B7">${{STATS.sent_counts?.positive||0}}</b></div>`,
    `<div class="stat-pill">📅 ${{STATS.generated_at}}</div>`,
  ].join("");
}}

// ── Source filter buttons ──────────────────────────────────────
function buildSourceFilters(){{
  const allSrc=[...new Set(SIGNALS.map(s=>s.source))].sort();
  document.getElementById("srcFilters").innerHTML=allSrc.map(src=>
    `<button class="ftoggle active" data-src="${{src}}" onclick="toggleSrc(this)">${{SRC_LABELS[src]||src}}</button>`
  ).join("");
}}

// ── Event handlers ─────────────────────────────────────────────
let searchTimer;
function onSearch(val){{
  clearTimeout(searchTimer);
  searchTimer=setTimeout(()=>{{ state.search=val.trim(); renderCurrentSection(); updateNavCounts(); renderIngBanner(); renderFilterChips(); }},180);
}}
function toggleSrc(btn){{
  btn.classList.toggle("active");
  const allActive=[...document.querySelectorAll("#srcFilters .ftoggle")].every(b=>b.classList.contains("active"));
  if(allActive) state.activeSrc.clear();
  else state.activeSrc=new Set([...document.querySelectorAll("#srcFilters .ftoggle.active")].map(b=>b.dataset.src));
  renderCurrentSection();
}}
function toggleSev(btn){{
  btn.classList.toggle("active");
  state.activeSev=new Set([...document.querySelectorAll("#sevFilters .ftoggle.active")].map(b=>b.dataset.sev));
  renderCurrentSection();
}}
function toggleSent(btn){{
  btn.classList.toggle("active");
  state.activeSent=new Set([...document.querySelectorAll("#sentFilters .ftoggle.active")].map(b=>b.dataset.sent));
  renderCurrentSection();
}}
function onSort(val){{ state.sort=val; renderCurrentSection(); }}

// ── Sentiment filter (from donut / risk watch) ─────────────────
function filterBySentiment(sent){{
  const onlyThis=state.activeSent.size===1&&state.activeSent.has(sent);
  if(onlyThis){{
    state.activeSent=new Set(["positive","neutral","negative",""]);
    document.querySelectorAll("#sentFilters .ftoggle").forEach(b=>b.classList.add("active"));
  }} else {{
    state.activeSent=new Set([sent,""]);
    document.querySelectorAll("#sentFilters .ftoggle").forEach(b=>b.classList.toggle("active",b.dataset.sent===sent));
  }}
  if(state.activeMainTab!=="vms") showMainTab("vms");
  const regBtn=document.querySelector('.snav-tab[data-sec="regulatory"]');
  if(regBtn) showSection("regulatory",regBtn);
  window.scrollTo({{top:document.getElementById("secnav").offsetTop-10,behavior:"smooth"}});
}}

// ── Scroll to signal card ──────────────────────────────────────
function scrollToSignal(id, sent){{
  // Determine which VMS sub-tab holds this signal
  const sig=SIGNALS.find(s=>s.id===id);
  let targetSec="regulatory";
  if(sig){{
    if(sig.source==="pubmed") targetSec="research";
    else if(sig.source==="adverse_events") targetSec="adverse";
  }}
  // Ensure VMS tab is active
  if(state.activeMainTab!=="vms") showMainTab("vms");
  // Navigate to the right section
  if(state.activeSecPerTab["vms"]!==targetSec) showSection(targetSec,null);
  // Scroll + glow after render
  setTimeout(()=>{{
    const card=document.querySelector(`.card[data-id="${{id}}"]`);
    if(card){{
      card.scrollIntoView({{behavior:"smooth",block:"center"}});
      card.classList.add("card-highlight");
      setTimeout(()=>card.classList.remove("card-highlight"),1600);
    }}
  }},280);
}}

// ── Filter by trending ingredient ─────────────────────────────
function filterByIngredient(ing){{
  const el=document.getElementById("globalSearch");
  if(el) el.value=ing;
  state.search=ing;
  if(state.activeMainTab!=="vms") showMainTab("vms");
  // Smart routing: find which VMS sub-tab has the most matching signals
  const regSrc=["tga","fda","fda_australia","artg","tga_consultations","advisory_committee"];
  const ql=ing.toLowerCase();
  const matched=SIGNALS.filter(s=>{{
    const hay=[s.title,s.ingredient,s.summary,s.ctx,s.risk_desc].join(" ").toLowerCase();
    return textMatches(ql,hay);
  }});
  const cnts={{
    regulatory:matched.filter(s=>regSrc.includes(s.source)).length,
    research:  matched.filter(s=>s.source==="pubmed").length,
    adverse:   matched.filter(s=>s.source==="adverse_events").length,
  }};
  const best=Object.entries(cnts).sort((a,b)=>b[1]-a[1])[0];
  const targetSec=(best&&best[1]>0)?best[0]:"regulatory";
  if(state.activeSecPerTab["vms"]!==targetSec) showSection(targetSec,null);
  else{{ renderCurrentSection(); updateNavCounts(); }}
  renderIngBanner();
  renderFilterChips();
}}

// ── Ingredient profile banner ──────────────────────────────────
const ING_NAMES=[...new Set(SIGNALS.map(s=>s.ingredient).filter(Boolean))];
let ingBannerOpen=false;

function getIngMatch(q){{
  if(!q||q.length<3) return null;
  const ql=q.toLowerCase();
  let m=ING_NAMES.find(i=>i.toLowerCase()===ql);
  if(m) return m;
  m=ING_NAMES.find(i=>i.toLowerCase().startsWith(ql));
  if(m) return m;
  if(q.length>=4){{
    m=ING_NAMES.find(i=>i.toLowerCase().includes(ql));
    if(m) return m;
    // fuzzy: try edit distance 1 on each word of the ingredient name
    m=ING_NAMES.find(i=>i.toLowerCase().split(/\s+/).some(w=>editDist1(ql,w)));
    if(m) return m;
  }}
  return null;
}}

function renderIngBanner(){{
  const el=document.getElementById("ingBanner");
  if(!el) return;
  const ing=getIngMatch(state.search);
  if(!ing){{ el.innerHTML=""; return; }}
  const matches=SIGNALS.filter(s=>s.ingredient===ing);
  const sents={{}};
  matches.forEach(s=>{{ const k=s.sentiment||"neutral"; sents[k]=(sents[k]||0)+1; }});
  const sevs={{}};
  matches.forEach(s=>{{ const k=s.severity||"low"; sevs[k]=(sevs[k]||0)+1; }});
  const srcSet=[...new Set(matches.map(s=>SRC_LABELS[s.source]||s.source))];
  const ql=ing.toLowerCase();
  const citN=CITATIONS.filter(c=>[c.summary,c.company,c.cat,c.product].join(" ").toLowerCase().includes(ql)).length;
  const domE=Object.entries(sents).sort((a,b)=>b[1]-a[1])[0];
  const sIco={{positive:"▲",neutral:"●",negative:"▼"}};
  const sCol={{positive:"var(--green)",neutral:"var(--slate)",negative:"var(--red)"}};
  let domH="";
  if(domE){{ const dc=sCol[domE[0]]||"var(--slate)"; domH=`<span style="color:${{dc}}">${{sIco[domE[0]]||"●"}} ${{domE[0]}} (${{domE[1]}})</span>`; }}
  let sum=`🧪 <b style="color:var(--teal)">${{esc(ing)}}</b> &nbsp;—&nbsp; ${{matches.length}} signal${{matches.length!==1?"s":""}}`;
  if(citN) sum+=` &nbsp;·&nbsp; ${{citN}} citation${{citN!==1?"s":""}}`;
  if(domH) sum+=` &nbsp;·&nbsp; ${{domH}}`;
  sum+=` &nbsp;·&nbsp; <span style="color:var(--slate-dim)">${{srcSet.join(", ")}}</span>`;
  let sentH="";
  ["negative","neutral","positive"].forEach(k=>{{
    const n=sents[k]||0; if(!n) return;
    const c={{negative:"var(--red)",neutral:"var(--slate)",positive:"var(--green)"}}[k];
    const p=Math.round(n/matches.length*100);
    sentH+=`<div style="display:flex;align-items:center;gap:6px;margin-bottom:4px"><span style="color:${{c}};width:60px;text-transform:capitalize">${{k}}</span><div style="width:80px;background:var(--navy);border-radius:99px;height:5px;overflow:hidden"><div style="width:${{p}}%;height:100%;background:${{c}};border-radius:99px"></div></div><span style="color:${{c}};font-weight:700">${{n}}</span></div>`;
  }});
  let sevH="";
  ["high","medium","low"].forEach(k=>{{
    const n=sevs[k]||0; if(!n) return;
    const c={{high:"var(--red)",medium:"var(--amber)",low:"var(--green)"}}[k];
    sevH+=`<div style="display:flex;align-items:center;gap:6px;margin-bottom:4px"><span style="text-transform:capitalize;color:${{c}};width:55px">${{k}}</span><span class="badge badge-sev-${{k}}">${{n}}</span></div>`;
  }});
  let sigH="";
  matches.slice(0,3).forEach(s=>{{
    const t=s.title.length>70?s.title.slice(0,70)+"…":s.title;
    sigH+=`<div style="margin-bottom:6px"><a href="${{esc(s.url)}}" target="_blank" style="color:var(--teal);font-size:12px">${{esc(t)}}</a><span style="color:var(--slate-dim);font-size:11px;margin-left:6px">${{esc(s.ctx||"")}}</span></div>`;
  }});
  const dop=ingBannerOpen?"block":"none";
  el.innerHTML=`<div class="ing-banner-wrap"><div class="ing-banner-head" onclick="toggleIngBanner()"><span style="flex:1;font-size:13px;line-height:1.5">${{sum}}</span><span id="ingBannerChevron" style="color:var(--slate-dim);font-size:11px;flex-shrink:0;margin-left:8px">${{ingBannerOpen?"▲ collapse":"▼ expand"}}</span></div><div id="ingBannerDetail" class="ing-banner-detail" style="display:${{dop}}"><div style="display:flex;gap:24px;flex-wrap:wrap"><div><div class="panel-title" style="margin-bottom:8px">Sentiment</div>${{sentH}}</div><div><div class="panel-title" style="margin-bottom:8px">Severity</div>${{sevH}}</div><div style="flex:1;min-width:200px"><div class="panel-title" style="margin-bottom:8px">Top Signals</div>${{sigH}}</div></div></div></div>`;
}}

function toggleIngBanner(){{
  ingBannerOpen=!ingBannerOpen;
  const det=document.getElementById("ingBannerDetail");
  const chev=document.getElementById("ingBannerChevron");
  if(det) det.style.display=ingBannerOpen?"block":"none";
  if(chev) chev.textContent=ingBannerOpen?"▲ collapse":"▼ expand";
}}

// ── Source panel toggle ────────────────────────────────────────
let srcPanelOpen=false;
function toggleSrcPanel(){{
  srcPanelOpen=!srcPanelOpen;
  const bars=document.getElementById("srcBars");
  const chev=document.getElementById("srcChevron");
  if(bars) bars.style.display=srcPanelOpen?"block":"none";
  if(chev) chev.textContent=srcPanelOpen?"▲ hide":"▼ show";
}}

// ── Filter chips ───────────────────────────────────────────────
function isDefaultFilters(){{
  if(state.search) return false;
  if(state.activeSrc.size>0) return false;
  if(state.activeSev.size!==3||!["high","medium","low"].every(v=>state.activeSev.has(v))) return false;
  if(state.activeSent.size!==4||!["positive","neutral","negative",""].every(v=>state.activeSent.has(v))) return false;
  return true;
}}

function renderFilterChips(){{
  const el=document.getElementById("filterChips");
  if(!el) return;
  if(isDefaultFilters()){{ el.innerHTML=""; return; }}
  const chips=[];
  if(state.search)
    chips.push(`<button class="filter-chip" onclick="applyChipAction('search')" title="Remove search filter">🔍 ${{esc(state.search)}} ✕</button>`);
  [...state.activeSrc].sort().forEach(src=>
    chips.push(`<button class="filter-chip" onclick="applyChipAction('src','${{esc(src)}}')" title="Remove source filter">${{esc(SRC_LABELS[src]||src)}} ✕</button>`)
  );
  const allSev=["high","medium","low"];
  if(state.activeSev.size<3){{
    allSev.filter(v=>state.activeSev.has(v)).forEach(v=>
      chips.push(`<button class="filter-chip" onclick="applyChipAction('sev','${{v}}')" title="Remove severity filter">${{v.toUpperCase()}} ✕</button>`)
    );
  }}
  const allSent=["positive","neutral","negative"];
  if(state.activeSent.size<4){{
    allSent.filter(v=>state.activeSent.has(v)).forEach(v=>
      chips.push(`<button class="filter-chip" onclick="applyChipAction('sent','${{v}}')" title="Remove sentiment filter">${{v}} ✕</button>`)
    );
  }}
  el.innerHTML=`<div class="chip-bar">
    ${{chips.join("")}}
    <button class="chip-clear-all" onclick="clearAllFilters()" title="Reset all filters">✕ Clear all</button>
  </div>`;
}}

function applyChipAction(type, val){{
  if(type==="search"){{
    state.search="";
    const si=document.getElementById("globalSearch");
    if(si) si.value="";
  }} else if(type==="src"){{
    // toggle this source off
    if(state.activeSrc.has(val)){{
      state.activeSrc.delete(val);
      if(state.activeSrc.size===0){{
        // none selected = all active — reset button UI
        document.querySelectorAll("#srcFilters .ftoggle").forEach(b=>b.classList.add("active"));
      }} else {{
        const btn=document.querySelector(`#srcFilters .ftoggle[data-src="${{val}}"]`);
        if(btn) btn.classList.remove("active");
      }}
    }}
  }} else if(type==="sev"){{
    state.activeSev.delete(val);
    const btn=document.querySelector(`#sevFilters .ftoggle[data-sev="${{val}}"]`);
    if(btn) btn.classList.remove("active");
  }} else if(type==="sent"){{
    state.activeSent.delete(val);
    const btn=document.querySelector(`#sentFilters .ftoggle[data-sent="${{val}}"]`);
    if(btn) btn.classList.remove("active");
  }}
  renderCurrentSection();
  renderIngBanner();
  renderFilterChips();
}}

function clearAllFilters(){{
  state.search="";
  state.activeSrc.clear();
  state.activeSev=new Set(["high","medium","low"]);
  state.activeSent=new Set(["positive","neutral","negative",""]);
  const si=document.getElementById("globalSearch");
  if(si) si.value="";
  document.querySelectorAll("#srcFilters .ftoggle").forEach(b=>b.classList.add("active"));
  document.querySelectorAll("#sevFilters .ftoggle").forEach(b=>b.classList.add("active"));
  document.querySelectorAll("#sentFilters .ftoggle").forEach(b=>b.classList.add("active"));
  renderCurrentSection();
  renderIngBanner();
  renderFilterChips();
}}

// ── Init ───────────────────────────────────────────────────────
renderStatsPills();
buildSourceFilters();
buildSecNav("vms");
renderDashboard();
updateNavCounts();
renderIngBanner();
renderFilterChips();
</script>
</body>
</html>"""

# Write output
out_path = BASE / "signals.html"
out_path.write_text(HTML, encoding="utf-8")
print(f"Written: {out_path}  ({len(HTML):,} chars / {len(HTML.encode())//1024} KB)")
