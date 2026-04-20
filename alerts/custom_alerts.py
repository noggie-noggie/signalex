"""
alerts/custom_alerts.py — User-configurable alert rule engine.

Loads rules from config/alert_rules.json and evaluates them against new
signals on each pipeline run.  Rules that match fire their configured
channels (email / slack) at the configured frequency.

Each rule shape:
  {
    "rule_name":    string,
    "trigger_type": "ingredient_mention" | "severity_threshold" | "trend_spike"
                    | "sentiment_shift" | "claim_shift" | "new_source_signal",
    "filters":      { ... condition dict ... },
    "channels":     ["email"] | ["slack"] | ["email", "slack"],
    "frequency":    "instant" | "daily_digest" | "weekly_summary"
  }

Integrates with:
  - alerts/dispatcher.py for email/slack delivery
  - analytics/trends.TrendReport for trend_spike / claim_shift triggers
  - analytics/sentiment.SentimentReport for sentiment_shift triggers
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import config

if TYPE_CHECKING:
    from classifier.claude import ClassifiedSignal
    from analytics.trends import TrendReport
    from analytics.sentiment import SentimentReport

logger = logging.getLogger(__name__)

_RULES_PATH = Path(__file__).parent.parent / "config" / "alert_rules.json"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class FiredAlert:
    rule_name:    str
    trigger_type: str
    matched_item: str        # ingredient name, source, or other match description
    channels:     list[str]
    frequency:    str
    detail:       str        # human-readable match detail
    signals:      list       = field(default_factory=list)  # matching ClassifiedSignals


# ---------------------------------------------------------------------------
# Rule loader
# ---------------------------------------------------------------------------

def load_rules() -> list[dict]:
    """Load alert rules from config/alert_rules.json."""
    if not _RULES_PATH.exists():
        logger.warning("Alert rules file not found: %s", _RULES_PATH)
        return []
    try:
        rules = json.loads(_RULES_PATH.read_text())
        logger.info("Loaded %d alert rule(s) from %s", len(rules), _RULES_PATH)
        return rules
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Failed to load alert rules: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Rule evaluator
# ---------------------------------------------------------------------------

class CustomAlertEvaluator:
    """
    Evaluates all configured alert rules against the current pipeline run.

    Usage:
        evaluator = CustomAlertEvaluator()
        fired = evaluator.evaluate(signals, trend_report, sentiment_report)
        evaluator.dispatch_instant(fired, dispatcher)
    """

    def __init__(self) -> None:
        self.rules = load_rules()

    def evaluate(
        self,
        signals: list["ClassifiedSignal"],
        trend_report: "TrendReport | None" = None,
        sentiment_report: "SentimentReport | None" = None,
    ) -> list[FiredAlert]:
        """
        Evaluate all rules against new signals + analytics reports.
        Returns a list of FiredAlert objects for rules that matched.
        """
        fired: list[FiredAlert] = []

        for rule in self.rules:
            trigger = rule.get("trigger_type", "")
            try:
                result = self._evaluate_rule(rule, signals, trend_report, sentiment_report)
                if result:
                    fired.append(result)
            except Exception:
                logger.exception("Error evaluating rule %r", rule.get("rule_name", "?"))

        if fired:
            logger.info(
                "Custom alerts: %d rule(s) fired: %s",
                len(fired),
                ", ".join(f.rule_name for f in fired),
            )
        return fired

    def _evaluate_rule(
        self,
        rule: dict,
        signals: list["ClassifiedSignal"],
        trend_report,
        sentiment_report,
    ) -> FiredAlert | None:
        trigger  = rule.get("trigger_type", "")
        filters  = rule.get("filters", {})
        name     = rule.get("rule_name", "Unnamed rule")
        channels = rule.get("channels", ["email"])
        freq     = rule.get("frequency", "daily_digest")

        if trigger == "ingredient_mention":
            return self._eval_ingredient_mention(rule, signals, name, channels, freq, filters)

        if trigger == "severity_threshold":
            return self._eval_severity_threshold(rule, signals, name, channels, freq, filters)

        if trigger == "new_source_signal":
            return self._eval_new_source_signal(rule, signals, name, channels, freq, filters)

        if trigger == "trend_spike":
            return self._eval_trend_spike(rule, trend_report, name, channels, freq, filters)

        if trigger == "sentiment_shift":
            return self._eval_sentiment_shift(rule, sentiment_report, name, channels, freq, filters)

        if trigger == "claim_shift":
            return self._eval_claim_shift(rule, trend_report, name, channels, freq, filters)

        logger.warning("Unknown trigger type %r in rule %r", trigger, name)
        return None

    # ------------------------------------------------------------------
    # Trigger implementations
    # ------------------------------------------------------------------

    def _eval_ingredient_mention(self, rule, signals, name, channels, freq, filters) -> FiredAlert | None:
        ingredient_list = [i.lower() for i in filters.get("ingredient_list", [])]
        source_list     = [s.lower() for s in filters.get("source_list", [])]

        matching = []
        for sig in signals:
            ing = (sig.ingredient_name or "").lower()
            src = (sig.source_label or sig.authority or "").lower()

            ing_match = not ingredient_list or any(kw in ing for kw in ingredient_list)
            src_match = not source_list or any(s == src for s in source_list)

            if ing_match and src_match:
                matching.append(sig)

        if not matching:
            return None

        ingredients = list({s.ingredient_name for s in matching if s.ingredient_name})
        return FiredAlert(
            rule_name    = name,
            trigger_type = "ingredient_mention",
            matched_item = ", ".join(ingredients[:5]),
            channels     = channels,
            frequency    = freq,
            detail       = f"{len(matching)} signal(s) matched ingredient mention filter.",
            signals      = matching,
        )

    def _eval_severity_threshold(self, rule, signals, name, channels, freq, filters) -> FiredAlert | None:
        severity_order = {"high": 3, "medium": 2, "low": 1}
        min_sev     = filters.get("min_severity", "low")
        source_list = [s.lower() for s in filters.get("source_list", [])]
        extra_field = filters.get("extra_field")
        extra_value = filters.get("extra_value")

        min_rank = severity_order.get(min_sev, 1)

        matching = []
        for sig in signals:
            sev_rank = severity_order.get(sig.severity, 1)
            if sev_rank < min_rank:
                continue
            src = (sig.source_label or sig.authority or "").lower()
            if source_list and src not in source_list:
                continue
            # Optional extra field filter
            if extra_field and extra_value:
                val = getattr(sig, extra_field, None)
                if str(val).lower() != str(extra_value).lower():
                    continue
            matching.append(sig)

        if not matching:
            return None

        return FiredAlert(
            rule_name    = name,
            trigger_type = "severity_threshold",
            matched_item = ", ".join({s.ingredient_name for s in matching if s.ingredient_name}),
            channels     = channels,
            frequency    = freq,
            detail       = f"{len(matching)} signal(s) met severity >= {min_sev}.",
            signals      = matching,
        )

    def _eval_new_source_signal(self, rule, signals, name, channels, freq, filters) -> FiredAlert | None:
        source_list = [s.lower() for s in filters.get("source_list", [])]
        min_sev     = filters.get("min_severity", "low")
        severity_order = {"high": 3, "medium": 2, "low": 1}
        min_rank = severity_order.get(min_sev, 1)

        matching = []
        for sig in signals:
            src = (sig.source_label or sig.authority or "").lower()
            if source_list and src not in source_list:
                continue
            if severity_order.get(sig.severity, 1) < min_rank:
                continue
            matching.append(sig)

        if not matching:
            return None

        sources = list({(s.source_label or s.authority) for s in matching})
        return FiredAlert(
            rule_name    = name,
            trigger_type = "new_source_signal",
            matched_item = ", ".join(sources),
            channels     = channels,
            frequency    = freq,
            detail       = f"{len(matching)} new signal(s) from {', '.join(sources)}.",
            signals      = matching,
        )

    def _eval_trend_spike(self, rule, trend_report, name, channels, freq, filters) -> FiredAlert | None:
        if not trend_report or not trend_report.trending_ingredients:
            return None

        min_ratio = float(filters.get("min_spike_ratio", 2.0))
        matching  = [t for t in trend_report.trending_ingredients if t.spike_ratio >= min_ratio]

        if not matching:
            return None

        items = ", ".join(f"{t.ingredient} (×{t.spike_ratio:.1f})" for t in matching[:5])
        return FiredAlert(
            rule_name    = name,
            trigger_type = "trend_spike",
            matched_item = items,
            channels     = channels,
            frequency    = freq,
            detail       = (
                f"{len(matching)} ingredient(s) trending: {items}. "
                f"7-day count exceeds 2× 30-day daily average."
            ),
            signals      = [],
        )

    def _eval_sentiment_shift(self, rule, sentiment_report, name, channels, freq, filters) -> FiredAlert | None:
        if not sentiment_report or not sentiment_report.rising_risk:
            return None

        min_shift = float(filters.get("min_shift", 0.3))
        direction = filters.get("direction", "negative")

        if direction == "negative":
            matching = [
                s for s in sentiment_report.ingredient_summaries
                if s.is_rising_risk and abs(s.shift) >= min_shift
            ]
        else:
            matching = []  # Only negative direction currently supported

        if not matching:
            return None

        items = ", ".join(
            f"{s.ingredient} (shift={s.shift:+.2f})" for s in matching[:5]
        )
        return FiredAlert(
            rule_name    = name,
            trigger_type = "sentiment_shift",
            matched_item = items,
            channels     = channels,
            frequency    = freq,
            detail       = (
                f"{len(matching)} ingredient(s) with rising negative sentiment: {items}."
            ),
            signals      = [],
        )

    def _eval_claim_shift(self, rule, trend_report, name, channels, freq, filters) -> FiredAlert | None:
        if not trend_report or not trend_report.claim_shift_alerts:
            return None

        from_type = filters.get("from_type", "")
        to_type   = filters.get("to_type", "")

        matching = trend_report.claim_shift_alerts
        if from_type:
            matching = [c for c in matching if c.old_dominant_type == from_type]
        if to_type:
            matching = [c for c in matching if c.new_dominant_type == to_type]

        if not matching:
            return None

        items = ", ".join(c.ingredient for c in matching[:5])
        return FiredAlert(
            rule_name    = name,
            trigger_type = "claim_shift",
            matched_item = items,
            channels     = channels,
            frequency    = freq,
            detail       = "\n".join(c.description for c in matching[:5]),
            signals      = [],
        )

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def dispatch_instant(self, fired_alerts: list[FiredAlert], dispatcher) -> None:
        """
        Fire instant alerts immediately via the dispatcher.
        daily_digest and weekly_summary alerts are included in the digest rendering.
        """
        for alert in fired_alerts:
            if alert.frequency != "instant":
                continue
            try:
                self._send_alert_email(alert, dispatcher)
                if "slack" in alert.channels and dispatcher._slack_url:
                    self._post_alert_slack(alert, dispatcher)
            except Exception:
                logger.exception("Failed to dispatch instant alert: %s", alert.rule_name)

    def _send_alert_email(self, alert: FiredAlert, dispatcher) -> None:
        subject = f"[Alert] {alert.rule_name}: {alert.matched_item[:60]}"
        html_body = self._render_alert_html(alert)
        text_body = self._render_alert_text(alert)
        dispatcher._send_email(subject=subject, html_body=html_body, text_body=text_body)
        logger.info("Custom alert email sent: %s", alert.rule_name)

    def _post_alert_slack(self, alert: FiredAlert, dispatcher) -> None:
        import requests as _req
        message = (
            f"🔔 *Custom Alert: {alert.rule_name}*\n"
            f"Match: {alert.matched_item}\n"
            f"{alert.detail}"
        )
        resp = _req.post(dispatcher._slack_url, json={"text": message}, timeout=10)
        resp.raise_for_status()

    def _render_alert_html(self, alert: FiredAlert) -> str:
        signal_rows = ""
        for s in alert.signals[:10]:
            sev_color = {"high": "#dc2626", "medium": "#d97706", "low": "#16a34a"}.get(
                getattr(s, "severity", "low"), "#6b7280"
            )
            signal_rows += (
                f'<tr><td style="padding:6px;border-bottom:1px solid #e5e7eb">'
                f'<a href="{getattr(s,"url","")}" style="color:#2563eb">{getattr(s,"title","")[:80]}</a>'
                f'</td><td style="padding:6px;border-bottom:1px solid #e5e7eb">'
                f'<span style="background:{sev_color};color:#fff;padding:2px 8px;border-radius:4px;font-size:12px">'
                f'{getattr(s,"severity","").upper()}</span></td></tr>'
            )

        return f"""<!DOCTYPE html>
<html><body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px">
  <div style="background:#1e40af;color:white;padding:16px;border-radius:8px 8px 0 0">
    <h2 style="margin:0">🔔 Signalex Custom Alert</h2>
  </div>
  <div style="border:2px solid #1e40af;border-top:none;padding:20px;border-radius:0 0 8px 8px">
    <h3 style="color:#1e3a8a">{alert.rule_name}</h3>
    <p><strong>Trigger:</strong> {alert.trigger_type.replace("_"," ").title()}</p>
    <p><strong>Matched:</strong> {alert.matched_item}</p>
    <p style="padding:12px;background:#eff6ff;border-left:4px solid #1e40af;border-radius:4px">
      {alert.detail}
    </p>
    {"<table style='width:100%;border-collapse:collapse;margin-top:16px'><tr><th style='text-align:left;padding:6px;background:#f3f4f6'>Signal</th><th style='text-align:left;padding:6px;background:#f3f4f6'>Severity</th></tr>" + signal_rows + "</table>" if signal_rows else ""}
    <p style="color:#6b7280;font-size:12px;margin-top:20px">
      Generated: {datetime.now(timezone.utc).strftime("%d %B %Y %H:%M UTC")} | Signalex Intelligence Platform
    </p>
  </div>
</body></html>"""

    def _render_alert_text(self, alert: FiredAlert) -> str:
        lines = [
            "🔔 SIGNALEX CUSTOM ALERT",
            "="*40,
            f"Rule:    {alert.rule_name}",
            f"Trigger: {alert.trigger_type}",
            f"Matched: {alert.matched_item}",
            "",
            alert.detail,
        ]
        if alert.signals:
            lines.append("\nMatching signals:")
            for s in alert.signals[:10]:
                lines.append(f"  [{getattr(s,'severity','?').upper()}] {getattr(s,'title','')[:80]}")
                lines.append(f"  → {getattr(s,'url','')}")
        lines.append(f"\nGenerated: {datetime.now(timezone.utc).strftime('%d %B %Y %H:%M UTC')}")
        return "\n".join(lines)
