"""
alerts/dispatcher.py — Multi-channel alert delivery system.

Three delivery modes:

  a) Daily digest email
     Sends the standard VMS digest, but now groups signals by SOURCE rather
     than just severity.  Sources: TGA, FDA, ARTG, iHerb, Chemist Warehouse,
     FDA-Australia.  Section headers clearly identify each source.

  b) Instant HIGH severity email
     Fires immediately when any signal has severity="high" OR
     market_significance="high".
     Subject: "[URGENT] Signalex Alert: {ingredient} — {event_type} ({source})"

  c) Slack webhook
     Posts to the URL in SLACK_WEBHOOK_URL env var.
     Format:
       🔴 HIGH | {ingredient} — {event_type}
       {summary}
       Source: {source} | {url}

Usage:
    from alerts.dispatcher import AlertDispatcher
    from classifier.claude import ClassifiedSignal

    dispatcher = AlertDispatcher()

    # Check all new signals and fire instant alerts where warranted
    dispatcher.check_and_dispatch(classified_signals)

    # Send the combined daily digest
    dispatcher.send_daily_digest(classified_signals)

    # Preview without sending
    print(dispatcher.preview_slack(signal))
    print(dispatcher.preview_digest_text(all_signals))
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
from collections import defaultdict
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import TYPE_CHECKING

import requests

import config

if TYPE_CHECKING:
    from classifier.claude import ClassifiedSignal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source display metadata
# ---------------------------------------------------------------------------

_SOURCE_META: dict[str, dict] = {
    "tga":                  {"label": "TGA Safety Alerts",              "flag": "🇦🇺"},
    "artg":                 {"label": "ARTG New Listings",               "flag": "🇦🇺"},
    "tga_consultations":    {"label": "TGA Consultations",               "flag": "🇦🇺"},
    "fda":                  {"label": "FDA Alerts",                      "flag": "🇺🇸"},
    "fda_australia":        {"label": "FDA → Australia Signals",         "flag": "🇺🇸➜🇦🇺"},
    "iherb":                {"label": "iHerb AU New Products",           "flag": "🛒"},
    "chemist_warehouse":    {"label": "Chemist Warehouse New Arrivals",  "flag": "🛒"},
    "pubmed":               {"label": "PubMed Research",                 "flag": "🔬"},
    "advisory_committee":   {"label": "Advisory Committees (FDA/EMA)",   "flag": "🏛️"},
    "adverse_events":       {"label": "Adverse Event Reports (CAERS/DAEN)", "flag": "⚠️"},
}

_DEFAULT_SOURCE_META = {"label": "Unknown Source", "flag": "📋"}

# Severity emoji for Slack
_SLACK_EMOJI = {
    "high":   "🔴",
    "medium": "🟡",
    "low":    "🟢",
}


def _source_label(signal: "ClassifiedSignal") -> str:
    """Return the display label for a signal's source."""
    key = signal.source_label or signal.authority
    return _SOURCE_META.get(key, _DEFAULT_SOURCE_META)["label"]


def _source_flag(signal: "ClassifiedSignal") -> str:
    key = signal.source_label or signal.authority
    return _SOURCE_META.get(key, _DEFAULT_SOURCE_META)["flag"]


def _is_urgent(signal: "ClassifiedSignal") -> bool:
    """Return True if this signal warrants an instant alert."""
    return signal.severity == "high" or signal.market_significance == "high"


# ---------------------------------------------------------------------------
# AlertDispatcher
# ---------------------------------------------------------------------------

class AlertDispatcher:
    """
    Routes classified signals to email and Slack alert channels.

    Instantiate once per pipeline run, call check_and_dispatch() for instant
    alerts, then send_daily_digest() for the grouped digest email.
    """

    def __init__(self) -> None:
        self._slack_url: str = os.getenv("SLACK_WEBHOOK_URL", "")

    # ------------------------------------------------------------------
    # Dispatch entry points
    # ------------------------------------------------------------------

    def check_and_dispatch(self, signals: list["ClassifiedSignal"]) -> int:
        """
        Iterate signals and fire instant alerts for any HIGH severity or HIGH
        market_significance signal.

        Returns the count of instant alerts fired.
        """
        fired = 0
        for signal in signals:
            if _is_urgent(signal):
                self._fire_instant_alert(signal)
                fired += 1
        if fired:
            logger.info("Dispatcher: %d instant alert(s) fired", fired)
        return fired

    def send_daily_digest(self, signals: list["ClassifiedSignal"], trend_report=None, sentiment_report=None, fired_alerts=None) -> None:
        """
        Render and send the enhanced multi-source daily digest email.
        Skips silently if there are no signals.
        """
        if not signals:
            logger.info("Dispatcher: no signals for digest, skipping.")
            return
        html = self.render_digest_html(signals, trend_report=trend_report, sentiment_report=sentiment_report, fired_alerts=fired_alerts)
        text = self.render_digest_text(signals, trend_report=trend_report, sentiment_report=sentiment_report, fired_alerts=fired_alerts)
        self._send_email(
            subject=self._digest_subject(signals),
            html_body=html,
            text_body=text,
        )
        logger.info("Dispatcher: daily digest sent to %d recipient(s)", len(config.EMAIL_RECIPIENTS))

    # ------------------------------------------------------------------
    # Instant HIGH alert
    # ------------------------------------------------------------------

    def _fire_instant_alert(self, signal: "ClassifiedSignal") -> None:
        """Send instant email + Slack notification for a HIGH signal."""
        try:
            self._send_instant_email(signal)
        except Exception as exc:
            logger.error("Instant email failed for %s: %s", signal.source_id, exc)

        if self._slack_url:
            try:
                self._post_slack(signal)
            except Exception as exc:
                logger.error("Slack post failed for %s: %s", signal.source_id, exc)

    def _send_instant_email(self, signal: "ClassifiedSignal") -> None:
        ingredient = signal.ingredient_name or "Unknown ingredient"
        event      = signal.event_type.replace("_", " ").title()
        source     = _source_label(signal)
        subject    = f"[URGENT] Signalex Alert: {ingredient} — {event} ({source})"

        html_body = self._render_instant_html(signal)
        text_body = self._render_instant_text(signal)

        self._send_email(subject=subject, html_body=html_body, text_body=text_body)
        logger.info("Instant email sent: %s", subject)

    def _render_instant_html(self, signal: "ClassifiedSignal") -> str:
        sev_color = {"high": "#dc2626", "medium": "#d97706", "low": "#16a34a"}.get(signal.severity, "#6b7280")
        sig_color = {"high": "#dc2626", "medium": "#d97706", "low": "#6b7280"}.get(signal.market_significance, "#6b7280")
        ingredient = signal.ingredient_name or "Unknown ingredient"
        event      = signal.event_type.replace("_", " ").title()
        source     = _source_label(signal)

        return f"""
<!DOCTYPE html>
<html>
<body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px">
  <div style="background:#dc2626;color:white;padding:16px;border-radius:8px 8px 0 0">
    <h2 style="margin:0">⚠️ URGENT: Signalex Alert</h2>
  </div>
  <div style="border:2px solid #dc2626;border-top:none;padding:20px;border-radius:0 0 8px 8px">
    <table style="width:100%;border-collapse:collapse">
      <tr><td style="padding:6px 0;font-weight:bold;color:#374151">Ingredient:</td>
          <td style="padding:6px 0">{ingredient}</td></tr>
      <tr><td style="padding:6px 0;font-weight:bold;color:#374151">Event Type:</td>
          <td style="padding:6px 0">{event}</td></tr>
      <tr><td style="padding:6px 0;font-weight:bold;color:#374151">Source:</td>
          <td style="padding:6px 0">{source}</td></tr>
      <tr><td style="padding:6px 0;font-weight:bold;color:#374151">Severity:</td>
          <td style="padding:6px 0"><span style="background:{sev_color};color:white;padding:2px 8px;border-radius:4px">{signal.severity.upper()}</span></td></tr>
      <tr><td style="padding:6px 0;font-weight:bold;color:#374151">Market Impact:</td>
          <td style="padding:6px 0"><span style="background:{sig_color};color:white;padding:2px 8px;border-radius:4px">{signal.market_significance.upper()}</span></td></tr>
    </table>
    <div style="margin:16px 0;padding:12px;background:#fef2f2;border-left:4px solid #dc2626;border-radius:4px">
      <strong>Summary:</strong> {signal.summary or "No summary available."}
    </div>
    <p style="margin-top:16px">
      <a href="{signal.url}" style="background:#2563eb;color:white;padding:10px 20px;text-decoration:none;border-radius:6px">View Source →</a>
    </p>
    <p style="color:#6b7280;font-size:12px;margin-top:20px">
      Detected: {signal.scraped_at} | Signalex Intelligence Platform
    </p>
  </div>
</body>
</html>"""

    def _render_instant_text(self, signal: "ClassifiedSignal") -> str:
        lines = [
            "⚠️  URGENT SIGNALIX ALERT",
            "=" * 40,
            f"Ingredient:    {signal.ingredient_name or 'Unknown'}",
            f"Event:         {signal.event_type.replace('_', ' ').title()}",
            f"Source:        {_source_label(signal)}",
            f"Severity:      {signal.severity.upper()}",
            f"Market Impact: {signal.market_significance.upper()}",
            "",
            f"Summary: {signal.summary}",
            "",
            f"URL: {signal.url}",
            "",
            f"Detected: {signal.scraped_at}",
        ]
        if signal.australia_relevance:
            lines.insert(7, f"AU Relevance:  {signal.australia_relevance.upper()} — {signal.australia_reasoning}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Slack
    # ------------------------------------------------------------------

    def _post_slack(self, signal: "ClassifiedSignal") -> None:
        """Post a signal to the configured Slack webhook."""
        message = self.format_slack_message(signal)
        resp = requests.post(
            self._slack_url,
            json={"text": message},
            timeout=10,
        )
        resp.raise_for_status()

    def format_slack_message(self, signal: "ClassifiedSignal") -> str:
        """
        Format a single signal for Slack.

        Output:
          🔴 HIGH | omega-3 — new_listing (ARTG New Listings)
          New high-strength omega-3 product listed in ARTG by Faroson.
          Source: https://www.tga.gov.au/resources/artg/525151
        """
        emoji    = _SLACK_EMOJI.get(signal.severity, "⚪")
        sev      = signal.severity.upper()
        ing      = signal.ingredient_name or "Unknown ingredient"
        event    = signal.event_type.replace("_", " ")
        source   = _source_label(signal)
        summary  = signal.summary or "No summary available."
        url      = signal.url

        extra = ""
        if signal.market_significance == "high":
            extra = " ⚡ HIGH MARKET IMPACT"
        if signal.competitor_signal:
            extra += " 🎯 COMPETITOR SIGNAL"

        return (
            f"{emoji} *{sev}* | {ing} — {event} ({source}){extra}\n"
            f"{summary}\n"
            f"Source: {url}"
        )

    # ------------------------------------------------------------------
    # Daily digest rendering
    # ------------------------------------------------------------------

    def render_digest_html(
        self,
        signals: list["ClassifiedSignal"],
        trend_report=None,
        sentiment_report=None,
        fired_alerts=None,
    ) -> str:
        """Render the multi-source digest as HTML, with optional trend/sentiment sections."""
        grouped = self._group_by_source(signals)
        today   = datetime.now(timezone.utc).strftime("%d %B %Y")
        counts  = self._severity_counts(signals)

        sections_html = ""
        for source_key, source_signals in grouped.items():
            meta = _SOURCE_META.get(source_key, _DEFAULT_SOURCE_META)
            sections_html += self._render_source_section_html(
                source_key,
                meta,
                source_signals,
            )

        _LOGO_SVG = """<svg width="260" height="96" viewBox="0 0 680 280" xmlns="http://www.w3.org/2000/svg">
<circle cx="160" cy="140" r="78" fill="none" stroke="#0D9488" stroke-width="1.5" opacity="0.18"/>
<circle cx="160" cy="140" r="58" fill="none" stroke="#0D9488" stroke-width="1.5" opacity="0.35"/>
<circle cx="160" cy="140" r="38" fill="#0D9488" opacity="0.08"/>
<circle cx="160" cy="140" r="38" fill="none" stroke="#0D9488" stroke-width="2" opacity="0.9"/>
<polyline fill="none" stroke="#0D9488" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"
  points="82,140 106,140 116,114 126,166 136,126 147,154 160,140 238,140"/>
<circle cx="160" cy="140" r="4.5" fill="#0D9488"/>
<line x1="160" y1="140" x2="160" y2="62" stroke="#0D9488" stroke-width="1.2" stroke-dasharray="3 5" opacity="0.4"/>
<path d="M160,62 A78,78 0 0,1 238,140" fill="none" stroke="#0D9488" stroke-width="1.2" stroke-dasharray="3 5" opacity="0.22"/>
<circle cx="212" cy="88" r="6" fill="#0D9488"/>
<circle cx="212" cy="88" r="11" fill="none" stroke="#0D9488" stroke-width="1.5" opacity="0.38"/>
<text x="276" y="158" font-family="Georgia, Cambria, serif" font-size="58" font-weight="700" fill="#F8FAFC" letter-spacing="-2">Signalex</text>
<rect x="276" y="168" width="62" height="3.5" rx="1.75" fill="#0D9488"/>
<text x="277" y="196" font-family="Arial, sans-serif" font-size="12" fill="#64748B" letter-spacing="3">REGULATORY INTELLIGENCE</text>
</svg>"""

        high_banner = ""
        if counts["high"]:
            n = counts["high"]
            label = f"{n} HIGH severity signal{'s' if n != 1 else ''} require{'s' if n == 1 else ''} immediate review."
            high_banner = f"""
  <div style="background:#7F1D1D;color:#fff;padding:14px 28px;display:flex;align-items:center;gap:10px">
    <span style="font-size:20px">⚠️</span>
    <strong style="font-size:15px;letter-spacing:0.01em">{label}</strong>
  </div>"""

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Signalex Intelligence Digest — {today}</title>
</head>
<body style="margin:0;padding:32px 16px;background:#0D1B2A;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif">

<div style="max-width:680px;margin:0 auto">

  <!-- HEADER -->
  <div style="background:#0D1B2A;border-radius:10px 10px 0 0;border-bottom:3px solid #0D9488;padding:18px 28px;min-height:80px;box-sizing:border-box">
    <div style="display:flex;align-items:center;justify-content:space-between;gap:16px">
      <div style="flex-shrink:0">{_LOGO_SVG}</div>
      <div style="text-align:right;flex-shrink:0">
        <div style="font-size:12px;color:#94A3B8;letter-spacing:0.04em;text-transform:uppercase">{today}</div>
        <div style="font-size:36px;font-weight:800;color:#F8FAFC;line-height:1.1;margin-top:4px">{len(signals)}</div>
        <div style="font-size:13px;color:#94A3B8;margin-top:2px">Signal{'s' if len(signals) != 1 else ''} &nbsp;·&nbsp; {len(grouped)} Source{'s' if len(grouped) != 1 else ''}</div>
      </div>
    </div>
  </div>

  <!-- HIGH ALERT BANNER -->
  {high_banner}

  <!-- SEVERITY SUMMARY BAR -->
  <div style="background:#0F2336;padding:14px 28px;border-bottom:1px solid #1E3A5F;display:flex;gap:10px;flex-wrap:wrap;align-items:center">
    <span style="font-size:11px;font-weight:600;color:#64748B;text-transform:uppercase;letter-spacing:0.06em;margin-right:4px">Severity</span>
    <span style="background:#DC2626;color:#fff;padding:5px 14px;border-radius:999px;font-size:12px;font-weight:600">HIGH &nbsp;{counts['high']}</span>
    <span style="background:#D97706;color:#fff;padding:5px 14px;border-radius:999px;font-size:12px;font-weight:600">MEDIUM &nbsp;{counts['medium']}</span>
    <span style="background:#059669;color:#fff;padding:5px 14px;border-radius:999px;font-size:12px;font-weight:600">LOW &nbsp;{counts['low']}</span>
  </div>

  <!-- SIGNAL SECTIONS -->
  <div style="background:#0F2336;padding:28px;border-radius:0 0 10px 10px;border:1px solid #1E3A5F;border-top:none">
    {sections_html}
  </div>

  <!-- ANALYTICS SECTIONS -->
  {self._render_analytics_section(trend_report, sentiment_report, fired_alerts)}

  <!-- FOOTER -->
  <div style="margin-top:24px;text-align:center;color:#475569;font-size:11px;line-height:1.8">
    Generated by <strong style="color:#64748B">Signalex</strong> Intelligence Platform &nbsp;·&nbsp; Powered by Claude AI<br>
    Sources: TGA &nbsp;·&nbsp; FDA &nbsp;·&nbsp; ARTG &nbsp;·&nbsp; PubMed &nbsp;·&nbsp; TGA Consultations &nbsp;·&nbsp; Advisory Committees &nbsp;·&nbsp; CAERS/DAEN
  </div>

</div>
</body>
</html>"""

    def _render_analytics_section(self, trend_report=None, sentiment_report=None, fired_alerts=None) -> str:
        """Render the Trending Ingredients / Risk Watch / Claim Shifts / Custom Alerts analytics block."""
        has_trends    = trend_report and (trend_report.trending_ingredients or trend_report.claim_shift_alerts)
        has_sentiment = sentiment_report and sentiment_report.rising_risk
        has_alerts    = fired_alerts and len(fired_alerts) > 0

        if not has_trends and not has_sentiment and not has_alerts:
            return ""

        blocks = ""

        # ── Trending Ingredients ──────────────────────────────────────
        if trend_report and trend_report.trending_ingredients:
            rows = ""
            for t in trend_report.trending_ingredients[:8]:
                bar_width = min(int(t.spike_ratio / 5 * 100), 100)
                rows += f"""
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
      <div style="width:120px;font-size:13px;color:#CBD5E1;font-weight:600;flex-shrink:0">{t.ingredient.title()}</div>
      <div style="flex:1;background:#0D1B2A;border-radius:999px;height:8px;overflow:hidden">
        <div style="width:{bar_width}%;background:#0D9488;height:100%;border-radius:999px"></div>
      </div>
      <div style="font-size:11px;color:#64748B;min-width:80px;text-align:right">
        7d: {t.count_7d} &nbsp;·&nbsp; ×{t.spike_ratio:.1f} spike
      </div>
    </div>"""

            blocks += f"""
  <div style="background:#0D3D35;border:1px solid #0D9488;border-radius:8px;padding:20px 24px;margin-top:20px">
    <div style="font-size:13px;font-weight:700;color:#0D9488;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:14px">
      📈 Trending Ingredients
    </div>
    {rows}
  </div>"""

        # ── Risk Watch ────────────────────────────────────────────────
        if sentiment_report and sentiment_report.rising_risk:
            risk_items = ""
            for s in sentiment_report.ingredient_summaries:
                if not s.is_rising_risk:
                    continue
                shift_pct = int(abs(s.shift) * 100)
                risk_items += f"""
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
      <div style="width:140px;font-size:13px;color:#CBD5E1;font-weight:600;flex-shrink:0">{s.ingredient.title()}</div>
      <div style="flex:1">
        <div style="font-size:11px;color:#94A3B8">
          30d score: <strong style="color:#F87171">{s.score_30d:+.2f}</strong>
          &nbsp;→&nbsp; recent: <strong style="color:#DC2626">{s.score_recent:+.2f}</strong>
          &nbsp;(shift: {s.shift:+.2f})
        </div>
      </div>
      <div style="background:#DC2626;color:#fff;padding:2px 8px;border-radius:999px;font-size:10px;font-weight:700;flex-shrink:0">
        RISK ↑
      </div>
    </div>"""

            blocks += f"""
  <div style="background:#1A0808;border:1px solid #DC2626;border-radius:8px;padding:20px 24px;margin-top:16px">
    <div style="font-size:13px;font-weight:700;color:#DC2626;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:14px">
      🔴 Risk Watch — Rising Negative Sentiment
    </div>
    {risk_items}
  </div>"""

        # ── Claim Shifts ──────────────────────────────────────────────
        if trend_report and trend_report.claim_shift_alerts:
            shift_items = ""
            for c in trend_report.claim_shift_alerts[:5]:
                shift_items += f"""
    <div style="border-left:3px solid #D97706;padding:8px 12px;margin-bottom:8px;background:#0D1B2A;border-radius:0 4px 4px 0">
      <div style="font-size:13px;font-weight:700;color:#E2E8F0">{c.ingredient.title()}</div>
      <div style="font-size:12px;color:#94A3B8;margin-top:2px">
        <span style="background:#1E3A5F;color:#60A5FA;padding:2px 8px;border-radius:4px;font-size:11px">{c.old_dominant_type}</span>
        &nbsp;→&nbsp;
        <span style="background:#7C2D12;color:#FCA5A5;padding:2px 8px;border-radius:4px;font-size:11px">{c.new_dominant_type}</span>
        &nbsp;&nbsp;({c.old_count} → {c.new_count} signals)
      </div>
    </div>"""

            blocks += f"""
  <div style="background:#0F1A08;border:1px solid #D97706;border-radius:8px;padding:20px 24px;margin-top:16px">
    <div style="font-size:13px;font-weight:700;color:#D97706;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:14px">
      🔄 Claim Shifts
    </div>
    {shift_items}
  </div>"""

        # ── Custom Alert Rules Fired ──────────────────────────────────
        if fired_alerts:
            alert_items = ""
            for alert in fired_alerts[:8]:
                freq_color = {"instant": "#DC2626", "daily_digest": "#D97706", "weekly_summary": "#475569"}.get(
                    alert.frequency, "#475569"
                )
                alert_items += f"""
    <div style="border:1px solid #1E3A5F;border-radius:6px;padding:10px 14px;margin-bottom:8px;background:#0D1B2A">
      <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
        <span style="font-size:13px;font-weight:700;color:#E2E8F0">{alert.rule_name}</span>
        <span style="background:{freq_color};color:#fff;padding:2px 8px;border-radius:999px;font-size:10px;font-weight:700">{alert.frequency.upper().replace("_"," ")}</span>
      </div>
      <div style="font-size:12px;color:#94A3B8;margin-top:4px">
        {alert.trigger_type.replace("_"," ").title()} · Matched: {alert.matched_item[:80]}
      </div>
    </div>"""

            blocks += f"""
  <div style="background:#0A1628;border:1px solid #1E40AF;border-radius:8px;padding:20px 24px;margin-top:16px">
    <div style="font-size:13px;font-weight:700;color:#60A5FA;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:14px">
      🔔 Custom Alert Rules Fired ({len(fired_alerts)})
    </div>
    {alert_items}
  </div>"""

        if not blocks:
            return ""

        return f"""
  <div style="margin-top:28px">
    <div style="font-size:11px;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:0.1em;margin-bottom:12px;padding:0 2px">
      Analytics &amp; Intelligence
    </div>
    {blocks}
  </div>"""

    def _render_source_section_html(self, source_key: str, meta: dict, signals: list["ClassifiedSignal"]) -> str:
        if not signals:
            return ""

        flag  = meta.get("flag", "")
        label = meta.get("label", source_key)

        # Region pill: AU sources get teal, US gets blue, research gets purple, retail gets grey
        if source_key in ("tga", "artg", "tga_consultations"):
            pill_bg, pill_text, pill_label = "#0D3D35", "#0D9488", "🇦🇺 AU"
        elif source_key == "fda_australia":
            pill_bg, pill_text, pill_label = "#0F2A4A", "#60A5FA", "🇺🇸 → 🇦🇺"
        elif source_key == "fda":
            pill_bg, pill_text, pill_label = "#0F2A4A", "#60A5FA", "🇺🇸 US"
        elif source_key == "pubmed":
            pill_bg, pill_text, pill_label = "#2D1B69", "#A78BFA", "🔬 Research"
        elif source_key == "advisory_committee":
            pill_bg, pill_text, pill_label = "#1A1035", "#818CF8", "🏛️ Advisory"
        elif source_key == "adverse_events":
            pill_bg, pill_text, pill_label = "#3B0A0A", "#F87171", "⚠️ AE"
        else:
            pill_bg, pill_text, pill_label = "#1E3A5F", "#94A3B8", "🛒 Retail"

        source_pill = (
            f'<span style="background:{pill_bg};color:{pill_text};'
            f'padding:3px 10px;border-radius:999px;font-size:11px;font-weight:600;'
            f'vertical-align:middle;margin-left:10px">{pill_label}</span>'
        )

        cards = ""
        for s in sorted(signals, key=lambda x: {"high": 0, "medium": 1, "low": 2}.get(x.severity, 3)):
            sev_accent = {"high": "#DC2626", "medium": "#D97706", "low": "#059669"}.get(s.severity, "#475569")

            # Severity badge
            sev_badge = (
                f'<span style="background:{sev_accent};color:#fff;'
                f'padding:4px 12px;border-radius:999px;font-size:11px;font-weight:700;'
                f'letter-spacing:0.04em">{s.severity.upper()}</span>'
            )
            # Competitor badge
            comp_badge = ""
            if s.competitor_signal:
                tier = f" · {s.competitor_tier}" if s.competitor_tier and s.competitor_tier != "none" else ""
                comp_badge = (
                    f' <span style="background:#0D9488;color:#fff;'
                    f'padding:4px 10px;border-radius:999px;font-size:11px;font-weight:600">'
                    f'COMPETITOR{tier}</span>'
                )
            # Market significance badge
            mkt_badge = ""
            if s.market_significance == "high":
                mkt_badge = (
                    ' <span style="background:#7C3AED;color:#fff;'
                    'padding:4px 10px;border-radius:999px;font-size:11px;font-weight:600">'
                    'MKT HIGH</span>'
                )

            # AU relevance note
            au_note = ""
            if s.australia_relevance:
                au_color = {"high": "#0D9488", "medium": "#60A5FA", "low": "#64748B"}.get(s.australia_relevance, "#64748B")
                au_note = (
                    f'<div style="margin-top:8px;font-size:12px;color:#64748B">'
                    f'AU Relevance: <strong style="color:{au_color}">{s.australia_relevance.upper()}</strong>'
                    f' — {s.australia_reasoning}</div>'
                )

            # Ingredient meta line
            meta_parts = []
            if s.ingredient_name and s.ingredient_name not in ("", "unknown"):
                meta_parts.append(f"<strong style='color:#94A3B8'>Ingredient:</strong> {s.ingredient_name}")
            if s.event_type and s.event_type != "other":
                meta_parts.append(f"<strong style='color:#94A3B8'>Type:</strong> {s.event_type.replace('_', ' ').title()}")
            meta_line = (
                f'<div style="font-size:12px;color:#64748B;margin-top:6px">'
                + " &nbsp;·&nbsp; ".join(meta_parts)
                + "</div>"
            ) if meta_parts else ""

            cards += f"""
    <div style="border-left:4px solid {sev_accent};border:1px solid #1E3A5F;border-left:4px solid {sev_accent};
                border-radius:6px;padding:16px 18px;margin-bottom:14px;background:#0D1B2A">
      <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:8px">
        {sev_badge}{comp_badge}{mkt_badge}
      </div>
      <div style="font-size:17px;font-weight:700;line-height:1.3;margin-bottom:4px">
        <a href="{s.url}" style="color:#E2E8F0;text-decoration:none">{s.title}</a>
      </div>
      {meta_line}
      <div style="font-size:15px;color:#CBD5E1;line-height:1.55;margin-top:10px">{s.summary}</div>
      {au_note}
      <div style="margin-top:12px">
        <a href="{s.url}" style="color:#0D9488;font-size:12px;font-weight:600;text-decoration:none">View full notice →</a>
      </div>
    </div>"""

        return f"""
  <div style="margin-bottom:36px">
    <div style="display:flex;align-items:center;margin-bottom:16px;padding-bottom:10px;
                border-bottom:1px solid #1E3A5F">
      <div style="width:4px;height:22px;background:#0D9488;border-radius:2px;margin-right:12px;flex-shrink:0"></div>
      <span style="font-size:15px;font-weight:700;color:#E2E8F0;letter-spacing:0.01em">{label}</span>
      {source_pill}
      <span style="margin-left:auto;font-size:12px;color:#475569;font-weight:500">{len(signals)} signal{'s' if len(signals) != 1 else ''}</span>
    </div>
    {cards}
  </div>"""

    def render_digest_text(self, signals: list["ClassifiedSignal"], trend_report=None, sentiment_report=None, fired_alerts=None) -> str:
        """Render the multi-source digest as plain text."""
        grouped = self._group_by_source(signals)
        today   = datetime.now(timezone.utc).strftime("%d %B %Y, %H:%M UTC")
        counts  = self._severity_counts(signals)

        lines = [
            "=" * 60,
            "  VMS INTELLIGENCE DIGEST",
            f"  {today}",
            f"  {len(signals)} signal(s) · {counts['high']} HIGH · {counts['medium']} MEDIUM · {counts['low']} LOW",
            "=" * 60,
            "",
        ]

        for source_key, source_signals in grouped.items():
            meta = _SOURCE_META.get(source_key, _DEFAULT_SOURCE_META)
            lines.append(f"{'─' * 50}")
            lines.append(f"  {meta['flag']}  {meta['label'].upper()} ({len(source_signals)} signals)")
            lines.append(f"{'─' * 50}")
            for s in sorted(source_signals, key=lambda x: {"high": 0, "medium": 1, "low": 2}.get(x.severity, 3)):
                sev_marker = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(s.severity, "⚪")
                lines.append(f"\n{sev_marker} [{s.severity.upper()}] {s.title}")
                lines.append(f"   Type: {s.event_type.replace('_', ' ').title()}")
                if s.ingredient_name and s.ingredient_name != "unknown":
                    lines.append(f"   Ingredient: {s.ingredient_name}")
                if s.competitor_signal:
                    lines.append(f"   ⚠️  COMPETITOR SIGNAL — tier: {s.competitor_tier or 'unclassified'}")
                if s.market_significance == "high":
                    lines.append(f"   ⚡ HIGH MARKET SIGNIFICANCE")
                if s.australia_relevance:
                    lines.append(f"   AU Relevance: {s.australia_relevance.upper()} — {s.australia_reasoning}")
                lines.append(f"   {s.summary}")
                lines.append(f"   → {s.url}")
            lines.append("")

        lines += [
            "─" * 60,
            "Signalex Intelligence Platform | Powered by Claude AI",
            "Sources: TGA · FDA · ARTG · iHerb · Chemist Warehouse",
        ]
        return "\n".join(lines)

    def preview_slack(self, signal: "ClassifiedSignal") -> str:
        """Return the Slack message string without posting (for preview/testing)."""
        return self.format_slack_message(signal)

    def preview_digest_text(self, signals: list["ClassifiedSignal"]) -> str:
        """Return the plain-text digest without sending (for preview/testing)."""
        return self.render_digest_text(signals)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _group_by_source(signals: list["ClassifiedSignal"]) -> dict[str, list]:
        """Group signals by source_label (or authority if no source_label)."""
        source_order = [
            "tga", "artg", "tga_consultations",
            "fda_australia", "fda",
            "pubmed", "advisory_committee", "adverse_events",
            "iherb", "chemist_warehouse",
        ]
        grouped: dict[str, list] = defaultdict(list)
        for s in signals:
            key = s.source_label or s.authority
            grouped[key].append(s)
        # Return ordered by source_order, then any extras
        result = {}
        for key in source_order:
            if key in grouped:
                result[key] = grouped[key]
        for key, val in grouped.items():
            if key not in result:
                result[key] = val
        return result

    @staticmethod
    def _severity_counts(signals: list["ClassifiedSignal"]) -> dict[str, int]:
        return {
            "high":   sum(1 for s in signals if s.severity == "high"),
            "medium": sum(1 for s in signals if s.severity == "medium"),
            "low":    sum(1 for s in signals if s.severity == "low"),
        }

    @staticmethod
    def _digest_subject(signals: list["ClassifiedSignal"]) -> str:
        today   = datetime.now(timezone.utc).strftime("%d %B %Y")
        n       = len(signals)
        n_high  = sum(1 for s in signals if s.severity == "high")
        prefix  = "[URGENT] " if n_high > 0 else ""
        return f"{prefix}VMS Intelligence Digest — {today} ({n} signal{'s' if n != 1 else ''})"

    def _send_email(self, subject: str, html_body: str, text_body: str) -> None:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = config.EMAIL_FROM
        msg["To"]      = ", ".join(config.EMAIL_RECIPIENTS)

        msg.attach(MIMEText(text_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html",  "utf-8"))

        with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(config.SMTP_USER, config.SMTP_PASSWORD)
            server.sendmail(config.EMAIL_FROM, config.EMAIL_RECIPIENTS, msg.as_string())
