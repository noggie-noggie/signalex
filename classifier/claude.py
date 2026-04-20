"""
classifier/claude.py — Classifies RawSignals using the Claude API.

Each RawSignal is sent to Claude with a structured prompt requesting a JSON
response containing:
  - ingredient_name: primary supplement/ingredient involved
  - event_type: safety_alert | new_listing | ban | warning | recall | other
  - severity: high | medium | low
  - summary: one-sentence plain-English description

Claude is asked to return JSON only (no prose). The response is validated
with Pydantic; invalid JSON falls back to a default ClassifiedSignal marked
event_type="other" so the pipeline never hard-fails on a bad API response.
"""

from __future__ import annotations

import json
import logging
from typing import Literal

import anthropic
from pydantic import BaseModel, Field, ValidationError

import config
from scrapers.base import RawSignal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

EventType = Literal["safety_alert", "new_listing", "ban", "warning", "recall", "other"]
Severity  = Literal["high", "medium", "low"]


class ClassifiedSignal(BaseModel):
    # Pass-through from RawSignal
    source_id:  str
    authority:  str
    url:        str
    title:      str
    scraped_at: str

    # Claude-produced fields (core)
    ingredient_name: str      = ""
    event_type:      str      = "other"
    severity:        str      = "low"
    summary:         str      = ""
    # Input tokens + output tokens from this API call (for cost tracking)
    input_tokens:    int      = 0
    output_tokens:   int      = 0

    # Extended fields — populated by specialised classifiers
    source_label:        str   = ""    # artg | iherb | chemist_warehouse | tga | fda
    product_category:    str   = ""    # vitamins | minerals | herbal | sports | weight_management | other
    competitor_signal:   bool  = False # True = potential competitor product launch
    market_significance: str   = "low" # high | medium | low
    competitor_tier:     str   = ""    # direct | indirect | none  (retail scrapers)
    brand:               str   = ""    # brand name extracted by retail scrapers
    price:               str   = ""    # price string from retail scrapers
    australia_relevance: str   = ""    # high | medium | low  (FDA Australia filter)
    australia_reasoning: str   = ""    # one-sentence explanation

    # New source-specific fields
    relevance_to_vms:    str   = ""    # high | medium | low (pubmed)
    signal_type:         str   = ""    # safety_concern | efficacy_claim | regulatory_implication (pubmed)
    ingredient_relevance:str   = ""    # high | medium | low (tga_consultations, advisory_committees)
    potential_impact:    str   = ""    # restrictive | permissive | neutral (tga_consultations)
    trend_relevance:     str   = ""    # high | medium | low (adverse_events)

    # Sentiment fields — populated by analytics/sentiment.py
    sentiment:               str   = ""   # positive | neutral | negative
    sentiment_confidence:    float = 0.0
    sentiment_reasoning:     str   = ""


# ---------------------------------------------------------------------------
# System prompt  (set once, reused for every message)
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are a regulatory intelligence analyst specialising in vitamins, minerals,
and dietary supplements (VMS).

You will receive a regulatory notice from a health authority (TGA, FDA, etc.).
Your job is to extract structured data from it.

Respond with a single JSON object — no markdown fences, no prose, just the
raw JSON. Use exactly these keys:

  "ingredient_name" : string  — the primary supplement or ingredient involved
                                (e.g. "melatonin", "collagen", "GLP-1 receptor agonist").
                                Use "unknown" if none is identifiable.
  "event_type"      : string  — one of: safety_alert | new_listing | ban | warning | recall | other
  "severity"        : string  — one of: high | medium | low
                                high   = immediate consumer risk (contamination, undisclosed drug, ban)
                                medium = caution warranted (labelling issue, unverified claims)
                                low    = informational (new listing, minor advisory)
  "summary"         : string  — one sentence in plain English for a non-expert reader

Example output:
{"ingredient_name":"melatonin","event_type":"safety_alert","severity":"high","summary":"The TGA has warned consumers about counterfeit melatonin products imported without regulatory approval, which may contain unknown substances."}
"""

_ARTG_SYSTEM_PROMPT = """\
You are a market intelligence analyst for an Australian vitamins and supplements (VMS) company.

You will receive details of a newly listed product from the Australian Register of Therapeutic Goods (ARTG).
Your job is to assess the product as a potential competitor signal.

Respond with a single JSON object — no markdown, no prose. Use exactly these keys:

  "ingredient_name"     : string  — the primary active ingredient or supplement type
                                    (e.g. "omega-3", "vitamin C", "lactoferrin", "probiotics")
  "event_type"          : string  — always "new_listing" for ARTG entries
  "severity"            : string  — one of: high | medium | low
                                    high   = major competitor entering a crowded market segment
                                    medium = notable new entrant, worth monitoring
                                    low    = minor or unrelated product
  "summary"             : string  — one sentence: what the product is and why it matters
  "product_category"    : string  — one of: vitamins | minerals | herbal | sports | weight_management | other
  "competitor_signal"   : boolean — true if this is a competing VMS consumer product, false otherwise
  "market_significance" : string  — one of: high | medium | low
                                    high   = significant market impact (major brand, novel ingredient, large category)
                                    medium = moderate impact
                                    low    = minimal impact (niche, device, hospital product)

Example output:
{"ingredient_name":"omega-3","event_type":"new_listing","severity":"medium","summary":"Faroson has listed a high-strength triple fish oil product, entering the competitive omega-3 market segment.","product_category":"vitamins","competitor_signal":true,"market_significance":"medium"}
"""

_RETAIL_SYSTEM_PROMPT = """\
You are a market intelligence analyst for an Australian vitamins and supplements (VMS) company.

You will receive details of a product listed on an Australian retail website (iHerb, Chemist Warehouse).
Your job is to assess the product as a competitor intelligence signal.

Respond with a single JSON object — no markdown, no prose. Use exactly these keys:

  "ingredient_name"     : string  — the primary active ingredient (e.g. "magnesium", "collagen")
  "severity"            : string  — one of: high | medium | low
  "summary"             : string  — one sentence describing the product and market relevance
  "product_category"    : string  — one of: vitamins | minerals | herbal | sports | weight_management | other
  "competitor_tier"     : string  — one of: direct | indirect | none
                                    direct   = competes head-to-head in same category and format
                                    indirect = adjacent category, overlapping customer
                                    none     = not a competitor product
  "market_significance" : string  — one of: high | medium | low

Example output:
{"ingredient_name":"magnesium glycinate","severity":"medium","summary":"New high-dose magnesium glycinate capsules from Blackmores targeting sleep and stress, directly competing with our premium magnesium range.","product_category":"minerals","competitor_tier":"direct","market_significance":"high"}
"""

_FDA_AUSTRALIA_SYSTEM_PROMPT = """\
You are a regulatory intelligence analyst specialising in cross-border VMS (vitamins, minerals, supplements) regulation.

You will receive details of a US FDA regulatory notice about a dietary supplement.
Your job is to assess how likely this development is to flow through to the Australian market (via TGA) within the next 12 months.

Respond with a single JSON object — no markdown, no prose. Use exactly these keys:

  "australia_relevance" : string  — one of: high | medium | low
                                    high   = very likely to affect Australian market (ingredient sold here, global brand, regulatory precedent)
                                    medium = possible Australian impact (ingredient present in AU market, similar regulatory environment)
                                    low    = unlikely to affect Australia (US-specific brand, local recall, niche product)
  "reasoning"           : string  — one sentence explaining the relevance assessment

Example output:
{"australia_relevance":"high","reasoning":"The FDA recall of USP-grade melatonin gummies affects a global brand sold in Australian pharmacies, and the TGA is likely to issue a parallel advisory given the shared ingredient and formulation."}
"""


_PUBMED_SYSTEM_PROMPT = """\
You are a regulatory intelligence analyst specialising in vitamins, minerals, and dietary supplements (VMS).

You will receive the title and abstract of a peer-reviewed research article.
Assess its relevance to VMS regulation and market intelligence.

Respond with a single JSON object — no markdown, no prose. Use exactly these keys:

  "ingredient_name"   : string  — primary supplement/ingredient (e.g. "omega-3", "vitamin D"). Use "unknown" if none.
  "event_type"        : string  — one of: safety_alert | warning | other
  "severity"          : string  — one of: high | medium | low
  "summary"           : string  — one sentence plain-English for a non-expert
  "relevance_to_vms"  : string  — one of: high | medium | low
                                  high   = directly about supplement safety, efficacy, or regulatory status
                                  medium = related to an ingredient used in supplements
                                  low    = tangential or academic only
  "signal_type"       : string  — one of: safety_concern | efficacy_claim | regulatory_implication | other

Example:
{"ingredient_name":"melatonin","event_type":"safety_alert","severity":"medium","summary":"Study finds high-dose melatonin supplements may interfere with blood pressure medications in elderly patients.","relevance_to_vms":"high","signal_type":"safety_concern"}
"""

_TGA_CONSULTATION_SYSTEM_PROMPT = """\
You are a regulatory intelligence analyst specialising in Australian VMS (vitamins, minerals, supplements) regulation.

You will receive details of a TGA public consultation.
Assess its relevance to supplement ingredients and the likely regulatory direction.

Respond with a single JSON object — no markdown, no prose. Use exactly these keys:

  "ingredient_name"      : string  — primary ingredient/substance involved. Use "general" if it covers many.
  "event_type"           : string  — one of: warning | ban | new_listing | safety_alert | other
  "severity"             : string  — one of: high | medium | low
  "summary"              : string  — one sentence describing the consultation and its VMS implications
  "ingredient_relevance" : string  — one of: high | medium | low
                                     high   = directly affects VMS ingredients or product categories
                                     medium = could indirectly affect VMS
                                     low    = unrelated or pharmaceutical only
  "potential_impact"     : string  — one of: restrictive | permissive | neutral
                                     restrictive = likely to tighten access/requirements
                                     permissive  = likely to ease requirements
                                     neutral     = informational/procedural change

Example:
{"ingredient_name":"kava","event_type":"warning","severity":"high","summary":"TGA consultation proposes stricter scheduling of kava products following adverse hepatotoxicity reports, which would restrict OTC access.","ingredient_relevance":"high","potential_impact":"restrictive"}
"""

_ADVISORY_COMMITTEE_SYSTEM_PROMPT = """\
You are a regulatory intelligence analyst specialising in global VMS (vitamins, minerals, supplements) regulation.

You will receive details of an FDA or EMA advisory committee meeting agenda item.
Flag any relevance to dietary supplements, botanicals, or VMS ingredients.

Respond with a single JSON object — no markdown, no prose. Use exactly these keys:

  "ingredient_name"      : string  — primary VMS ingredient if any (use "none" if purely pharmaceutical)
  "event_type"           : string  — one of: warning | safety_alert | new_listing | other
  "severity"             : string  — one of: high | medium | low
  "summary"              : string  — one sentence describing the agenda item and its VMS relevance
  "ingredient_relevance" : string  — one of: high | medium | low
                                     high   = directly discusses supplements/botanicals/VMS ingredients
                                     medium = pharmaceutical decision with VMS ingredient implications
                                     low    = minimal or no VMS relevance

Example:
{"ingredient_name":"St. John's Wort","event_type":"warning","severity":"medium","summary":"FDA advisory committee reviewing drug-herb interactions for St. John's Wort, with implications for supplement labelling requirements.","ingredient_relevance":"high"}
"""

_ADVERSE_EVENT_SYSTEM_PROMPT = """\
You are a pharmacovigilance analyst specialising in dietary supplement adverse events.

You will receive a summary of adverse event reports for a supplement product.
Classify the ingredient, severity, and trend relevance.

Respond with a single JSON object — no markdown, no prose. Use exactly these keys:

  "ingredient_name"  : string  — primary active ingredient (e.g. "ephedra", "vitamin E", "green tea extract")
  "severity"         : string  — one of: high | medium | low
                                 high   = life-threatening outcome (hospitalisation, death, serious injury)
                                 medium = significant adverse effect requiring medical attention
                                 low    = mild/moderate, self-resolving
  "summary"          : string  — one sentence: ingredient, adverse effect type, and any pattern observed
  "trend_relevance"  : string  — one of: high | medium | low
                                 high   = multiple reports, emerging pattern, warrants monitoring
                                 medium = isolated reports, ingredient widely used
                                 low    = single report, rare ingredient, no clear pattern

Example:
{"ingredient_name":"green tea extract","severity":"high","summary":"Multiple CAERS reports link high-dose green tea extract supplements to acute liver injury, with 3 hospitalisation outcomes in Q4 2024.","trend_relevance":"high"}
"""


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------

class SignalClassifier:
    """
    Wraps the Anthropic client. Instantiate once; call classify() or
    classify_batch() to classify RawSignals.
    """

    def __init__(self) -> None:
        self.client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def classify(self, signal: RawSignal) -> ClassifiedSignal:
        """
        Classify a single RawSignal. Returns a ClassifiedSignal.
        Never raises — on any API or parse error a default signal is returned
        with event_type="other" so the pipeline can continue.
        """
        base = dict(
            source_id  = signal["source_id"],
            authority  = signal["authority"],
            url        = signal["url"],
            title      = signal["title"],
            scraped_at = signal["scraped_at"],
        )

        try:
            response = self.client.messages.create(
                model      = config.CLAUDE_MODEL,
                max_tokens = config.CLAUDE_MAX_TOKENS,
                system     = _SYSTEM_PROMPT,
                messages   = [{"role": "user", "content": self._build_prompt(signal)}],
            )
        except anthropic.APIError as exc:
            logger.error("Claude API error for signal %s: %s", signal["source_id"], exc)
            return ClassifiedSignal(**base)

        raw_text = response.content[0].text.strip()
        logger.debug("Claude raw response for %s: %s", signal["source_id"], raw_text)

        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            # Try to salvage by extracting the first {...} block
            import re
            m = re.search(r"\{.*\}", raw_text, re.DOTALL)
            if m:
                try:
                    parsed = json.loads(m.group())
                except json.JSONDecodeError:
                    parsed = {}
            else:
                parsed = {}

        if not parsed:
            logger.warning("Could not parse JSON from Claude for signal %s", signal["source_id"])

        return ClassifiedSignal(
            **base,
            ingredient_name = parsed.get("ingredient_name", "unknown"),
            event_type      = parsed.get("event_type", "other"),
            severity        = parsed.get("severity", "low"),
            summary         = parsed.get("summary", ""),
            input_tokens    = response.usage.input_tokens,
            output_tokens   = response.usage.output_tokens,
        )

    def classify_batch(self, signals: list[RawSignal]) -> list[ClassifiedSignal]:
        """Classify a list of signals sequentially, logging progress."""
        results = []
        for i, signal in enumerate(signals, 1):
            logger.info("Classifying signal %d/%d: %s", i, len(signals), signal["title"][:60])
            results.append(self.classify(signal))
        return results

    # ------------------------------------------------------------------
    # Specialised classifiers
    # ------------------------------------------------------------------

    def classify_artg(self, signal: RawSignal) -> ClassifiedSignal:
        """
        Classify an ARTG new-listing signal.

        Returns extended fields: product_category, competitor_signal,
        market_significance, plus the standard core fields.
        """
        base = self._base_dict(signal)
        try:
            response = self.client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=config.CLAUDE_MAX_TOKENS,
                system=_ARTG_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": self._build_prompt(signal)}],
            )
        except anthropic.APIError as exc:
            logger.error("Claude API error (ARTG) for %s: %s", signal["source_id"], exc)
            return ClassifiedSignal(**base, source_label="artg")

        parsed = self._parse_json(response.content[0].text.strip(), signal["source_id"])
        return ClassifiedSignal(
            **base,
            source_label        = "artg",
            ingredient_name     = parsed.get("ingredient_name", "unknown"),
            event_type          = parsed.get("event_type", "new_listing"),
            severity            = parsed.get("severity", "low"),
            summary             = parsed.get("summary", ""),
            product_category    = parsed.get("product_category", "other"),
            competitor_signal   = bool(parsed.get("competitor_signal", False)),
            market_significance = parsed.get("market_significance", "low"),
            input_tokens        = response.usage.input_tokens,
            output_tokens       = response.usage.output_tokens,
        )

    def classify_batch_artg(self, signals: list[RawSignal]) -> list[ClassifiedSignal]:
        results = []
        for i, sig in enumerate(signals, 1):
            logger.info("ARTG classify %d/%d: %s", i, len(signals), sig["title"][:60])
            results.append(self.classify_artg(sig))
        return results

    def classify_retail(self, signal: RawSignal) -> ClassifiedSignal:
        """
        Classify a retail scraper signal (iHerb / Chemist Warehouse).

        Returns: product_category, competitor_tier, market_significance.
        """
        base = self._base_dict(signal)
        source_label = signal.get("authority", "retail")  # type: ignore[call-overload]
        try:
            response = self.client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=config.CLAUDE_MAX_TOKENS,
                system=_RETAIL_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": self._build_prompt(signal)}],
            )
        except anthropic.APIError as exc:
            logger.error("Claude API error (retail) for %s: %s", signal["source_id"], exc)
            return ClassifiedSignal(**base, source_label=source_label)

        parsed = self._parse_json(response.content[0].text.strip(), signal["source_id"])
        return ClassifiedSignal(
            **base,
            source_label        = source_label,
            ingredient_name     = parsed.get("ingredient_name", "unknown"),
            event_type          = "new_listing",
            severity            = parsed.get("severity", "low"),
            summary             = parsed.get("summary", ""),
            product_category    = parsed.get("product_category", "other"),
            competitor_signal   = bool(parsed.get("competitor_tier", "none") != "none"),
            competitor_tier     = parsed.get("competitor_tier", "none"),
            market_significance = parsed.get("market_significance", "low"),
            input_tokens        = response.usage.input_tokens,
            output_tokens       = response.usage.output_tokens,
        )

    def classify_batch_retail(self, signals: list[RawSignal]) -> list[ClassifiedSignal]:
        results = []
        for i, sig in enumerate(signals, 1):
            logger.info("Retail classify %d/%d: %s", i, len(signals), sig["title"][:60])
            results.append(self.classify_retail(sig))
        return results

    def classify_fda_australia(self, signal: RawSignal) -> ClassifiedSignal:
        """
        Two-pass classification: first run the standard classifier, then add
        an Australia-relevance pass.  Returns a single merged ClassifiedSignal.
        """
        classified = self.classify(signal)
        base_dict = classified.model_dump()
        # Stamp source_label now so both the error path and success path use it
        base_dict["source_label"] = "fda_australia"

        try:
            response = self.client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=512,
                system=_FDA_AUSTRALIA_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": self._build_prompt(signal)}],
            )
        except anthropic.APIError as exc:
            logger.error("Claude API error (FDA-AU) for %s: %s", signal["source_id"], exc)
            return ClassifiedSignal(**base_dict)

        parsed = self._parse_json(response.content[0].text.strip(), signal["source_id"])
        base_dict.update(
            australia_relevance = parsed.get("australia_relevance", "low"),
            australia_reasoning = parsed.get("reasoning", ""),
            input_tokens        = classified.input_tokens + response.usage.input_tokens,
            output_tokens       = classified.output_tokens + response.usage.output_tokens,
        )
        return ClassifiedSignal(**base_dict)

    def classify_batch_fda_australia(self, signals: list[RawSignal]) -> list[ClassifiedSignal]:
        results = []
        for i, sig in enumerate(signals, 1):
            logger.info("FDA-AU classify %d/%d: %s", i, len(signals), sig["title"][:60])
            results.append(self.classify_fda_australia(sig))
        return results

    def classify_pubmed(self, signal: RawSignal) -> ClassifiedSignal:
        """Classify a PubMed research signal for VMS relevance and signal type."""
        base = self._base_dict(signal)
        try:
            response = self.client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=config.CLAUDE_MAX_TOKENS,
                system=_PUBMED_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": self._build_prompt(signal)}],
            )
        except anthropic.APIError as exc:
            logger.error("Claude API error (pubmed) for %s: %s", signal["source_id"], exc)
            return ClassifiedSignal(**base, source_label="pubmed")

        parsed = self._parse_json(response.content[0].text.strip(), signal["source_id"])
        return ClassifiedSignal(
            **base,
            source_label       = "pubmed",
            ingredient_name    = parsed.get("ingredient_name", "unknown"),
            event_type         = parsed.get("event_type", "other"),
            severity           = parsed.get("severity", "low"),
            summary            = parsed.get("summary", ""),
            relevance_to_vms   = parsed.get("relevance_to_vms", "low"),
            signal_type        = parsed.get("signal_type", ""),
            input_tokens       = response.usage.input_tokens,
            output_tokens      = response.usage.output_tokens,
        )

    def classify_batch_pubmed(self, signals: list[RawSignal]) -> list[ClassifiedSignal]:
        results = []
        for i, sig in enumerate(signals, 1):
            logger.info("PubMed classify %d/%d: %s", i, len(signals), sig["title"][:60])
            results.append(self.classify_pubmed(sig))
        return results

    def classify_tga_consultation(self, signal: RawSignal) -> ClassifiedSignal:
        """Classify a TGA consultation for ingredient relevance and regulatory impact."""
        base = self._base_dict(signal)
        try:
            response = self.client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=config.CLAUDE_MAX_TOKENS,
                system=_TGA_CONSULTATION_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": self._build_prompt(signal)}],
            )
        except anthropic.APIError as exc:
            logger.error("Claude API error (tga_consultation) for %s: %s", signal["source_id"], exc)
            return ClassifiedSignal(**base, source_label="tga_consultations")

        parsed = self._parse_json(response.content[0].text.strip(), signal["source_id"])
        return ClassifiedSignal(
            **base,
            source_label         = "tga_consultations",
            ingredient_name      = parsed.get("ingredient_name", "unknown"),
            event_type           = parsed.get("event_type", "other"),
            severity             = parsed.get("severity", "low"),
            summary              = parsed.get("summary", ""),
            ingredient_relevance = parsed.get("ingredient_relevance", "low"),
            potential_impact     = parsed.get("potential_impact", "neutral"),
            input_tokens         = response.usage.input_tokens,
            output_tokens        = response.usage.output_tokens,
        )

    def classify_batch_tga_consultation(self, signals: list[RawSignal]) -> list[ClassifiedSignal]:
        results = []
        for i, sig in enumerate(signals, 1):
            logger.info("TGA consult classify %d/%d: %s", i, len(signals), sig["title"][:60])
            results.append(self.classify_tga_consultation(sig))
        return results

    def classify_advisory_committee(self, signal: RawSignal) -> ClassifiedSignal:
        """Classify an advisory committee agenda item for VMS ingredient relevance."""
        base = self._base_dict(signal)
        try:
            response = self.client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=config.CLAUDE_MAX_TOKENS,
                system=_ADVISORY_COMMITTEE_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": self._build_prompt(signal)}],
            )
        except anthropic.APIError as exc:
            logger.error("Claude API error (advisory) for %s: %s", signal["source_id"], exc)
            return ClassifiedSignal(**base, source_label="advisory_committee")

        parsed = self._parse_json(response.content[0].text.strip(), signal["source_id"])
        return ClassifiedSignal(
            **base,
            source_label         = "advisory_committee",
            ingredient_name      = parsed.get("ingredient_name", "unknown"),
            event_type           = parsed.get("event_type", "other"),
            severity             = parsed.get("severity", "low"),
            summary              = parsed.get("summary", ""),
            ingredient_relevance = parsed.get("ingredient_relevance", "low"),
            input_tokens         = response.usage.input_tokens,
            output_tokens        = response.usage.output_tokens,
        )

    def classify_batch_advisory_committee(self, signals: list[RawSignal]) -> list[ClassifiedSignal]:
        results = []
        for i, sig in enumerate(signals, 1):
            logger.info("Advisory classify %d/%d: %s", i, len(signals), sig["title"][:60])
            results.append(self.classify_advisory_committee(sig))
        return results

    def classify_adverse_event(self, signal: RawSignal) -> ClassifiedSignal:
        """Classify an adverse event report for ingredient, severity, and trend relevance."""
        base = self._base_dict(signal)
        try:
            response = self.client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=config.CLAUDE_MAX_TOKENS,
                system=_ADVERSE_EVENT_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": self._build_prompt(signal)}],
            )
        except anthropic.APIError as exc:
            logger.error("Claude API error (adverse_event) for %s: %s", signal["source_id"], exc)
            return ClassifiedSignal(**base, source_label="adverse_events")

        parsed = self._parse_json(response.content[0].text.strip(), signal["source_id"])
        return ClassifiedSignal(
            **base,
            source_label    = "adverse_events",
            ingredient_name = parsed.get("ingredient_name", "unknown"),
            event_type      = "safety_alert",
            severity        = parsed.get("severity", "low"),
            summary         = parsed.get("summary", ""),
            trend_relevance = parsed.get("trend_relevance", "low"),
            input_tokens    = response.usage.input_tokens,
            output_tokens   = response.usage.output_tokens,
        )

    def classify_batch_adverse_event(self, signals: list[RawSignal]) -> list[ClassifiedSignal]:
        results = []
        for i, sig in enumerate(signals, 1):
            logger.info("Adverse event classify %d/%d: %s", i, len(signals), sig["title"][:60])
            results.append(self.classify_adverse_event(sig))
        return results

    # ------------------------------------------------------------------
    # Prompt + JSON helpers
    # ------------------------------------------------------------------

    def _base_dict(self, signal: RawSignal) -> dict:
        return dict(
            source_id  = signal["source_id"],
            authority  = signal["authority"],
            url        = signal["url"],
            title      = signal["title"],
            scraped_at = signal["scraped_at"],
        )

    def _parse_json(self, raw_text: str, source_id: str) -> dict:
        try:
            return json.loads(raw_text)
        except json.JSONDecodeError:
            import re as _re
            m = _re.search(r"\{.*\}", raw_text, _re.DOTALL)
            if m:
                try:
                    return json.loads(m.group())
                except json.JSONDecodeError:
                    pass
        logger.warning("Could not parse JSON from Claude for signal %s", source_id)
        return {}

    def _build_prompt(self, signal: RawSignal) -> str:
        return (
            f"Authority: {signal['authority'].upper()}\n"
            f"Title: {signal['title']}\n"
            f"URL: {signal['url']}\n\n"
            f"{signal['body_text'][:3000]}"
        )
