"""
digest/email_sender.py — Renders and sends the daily VMS regulatory digest.

Flow:
  1. Accept a list of ClassifiedSignals (passed in from the pipeline).
  2. Build a 2-3 sentence narrative summary from the signal data.
  3. Group signals by severity: high → medium → low.
  4. Render HTML + plain-text templates.
  5. Send via SMTP STARTTLS, or return rendered HTML for preview.
"""

from __future__ import annotations

import logging
import smtplib
from collections import Counter
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

import config
from classifier.claude import ClassifiedSignal

logger = logging.getLogger(__name__)


class DigestSender:
    """
    Render and (optionally) send the daily digest.

    Usage:
        sender = DigestSender()
        html = sender.render_html(classified_signals)          # preview
        sender.send(classified_signals)                        # preview + send
    """

    def __init__(self) -> None:
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(config.TEMPLATES_DIR)),
            autoescape=True,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def render_html(self, signals: list[ClassifiedSignal]) -> str:
        """Render and return the HTML email body without sending."""
        ctx = self._build_context(signals)
        return self.jinja_env.get_template("digest.html").render(**ctx)

    def render_text(self, signals: list[ClassifiedSignal]) -> str:
        """Render and return the plain-text email body without sending."""
        ctx = self._build_context(signals)
        return self.jinja_env.get_template("digest.txt").render(**ctx)

    def send(self, signals: list[ClassifiedSignal]) -> None:
        """Render and send the digest to all recipients in config."""
        if not signals:
            logger.info("Digest: no signals to send, skipping.")
            return

        html_body = self.render_html(signals)
        text_body = self.render_text(signals)
        self._send_email(html_body, text_body, len(signals))
        logger.info("Digest sent to %d recipient(s).", len(config.EMAIL_RECIPIENTS))

    # ------------------------------------------------------------------
    # Context builder (shared by HTML and text rendering)
    # ------------------------------------------------------------------

    def _build_context(self, signals: list[ClassifiedSignal]) -> dict:
        artg_listings = [s for s in signals if s.authority.lower() == "artg"]
        reg_signals   = [s for s in signals if s.authority.lower() != "artg"]

        grouped = {"high": [], "medium": [], "low": []}
        for s in reg_signals:
            bucket = s.severity if s.severity in grouped else "low"
            grouped[bucket].append(s)

        return {
            "narrative":      self._build_narrative(reg_signals, grouped),
            "grouped":        grouped,
            "total_count":    len(reg_signals),
            "artg_listings":  artg_listings,
            "generated_at":   datetime.now(timezone.utc).strftime("%d %B %Y, %H:%M UTC"),
            "period_label":   f"Last {config.SIGNAL_LOOKBACK_DAYS} days",
        }

    # ------------------------------------------------------------------
    # Narrative summary (programmatic — no extra API call)
    # ------------------------------------------------------------------

    def _build_narrative(
        self,
        signals: list[ClassifiedSignal],
        grouped: dict,
    ) -> str:
        """
        Compose a 2-3 sentence plain-English intro from the signal data.

        Example output:
          "This week TGA issued 6 regulatory signals, all HIGH severity safety
           alerts around counterfeit and unregistered products. Key ingredients
           flagged include GLP-1 receptor agonist, melatonin, and collagen.
           Immediate review is recommended for products containing these
           substances sold in the Australian market."
        """
        if not signals:
            return "No new regulatory signals were detected in this period."

        n = len(signals)
        n_high   = len(grouped["high"])
        n_medium = len(grouped["medium"])
        n_low    = len(grouped["low"])

        # Authority breakdown
        auth_counts = Counter(s.authority.upper() for s in signals)
        auth_str = ", ".join(f"{a} ({c})" for a, c in auth_counts.most_common())

        # Top ingredients (skip blanks / "unknown", dedupe, cap at 5)
        ingredients = [
            s.ingredient_name for s in signals
            if s.ingredient_name and s.ingredient_name.lower() != "unknown"
        ]
        top = list(dict.fromkeys(ingredients))[:5]   # preserve order, dedupe
        ingredient_str = (
            ", ".join(top[:-1]) + f", and {top[-1]}" if len(top) > 1
            else top[0] if top
            else "various ingredients"
        )

        # Severity description
        if n_high == n:
            sev_str = f"all {n_high} HIGH severity"
        elif n_high > 0:
            parts = []
            if n_high:   parts.append(f"{n_high} HIGH")
            if n_medium: parts.append(f"{n_medium} MEDIUM")
            if n_low:    parts.append(f"{n_low} LOW")
            sev_str = " and ".join(parts) + " severity"
        else:
            sev_str = f"{n} signals across medium and low severity"

        # Event type summary
        event_counts = Counter(s.event_type for s in signals)
        top_event = event_counts.most_common(1)[0][0].replace("_", " ")

        # Sentence 1: volume + authority + severity
        s1 = (
            f"This period saw {n} new regulatory signal{'s' if n != 1 else ''} "
            f"from {auth_str}, {sev_str}."
        )

        # Sentence 2: predominant event type + ingredients
        s2 = (
            f"The predominant event type was {top_event}, "
            f"with key ingredients flagged including {ingredient_str}."
        )

        # Sentence 3: action nudge (only if any HIGH signals)
        s3 = ""
        if n_high > 0:
            s3 = (
                "Immediate review is recommended for any products containing "
                "these ingredients in affected markets."
            )

        return " ".join(filter(None, [s1, s2, s3]))

    # ------------------------------------------------------------------
    # SMTP send
    # ------------------------------------------------------------------

    def _send_email(self, html_body: str, text_body: str, signal_count: int) -> None:
        today = datetime.now(timezone.utc).strftime("%d %B %Y")
        subject = f"VMS Regulatory Digest — {today} ({signal_count} signal{'s' if signal_count != 1 else ''})"

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
            server.sendmail(
                config.EMAIL_FROM,
                config.EMAIL_RECIPIENTS,
                msg.as_string(),
            )
