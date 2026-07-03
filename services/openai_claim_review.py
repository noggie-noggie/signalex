"""
services/openai_claim_review.py — optional OpenAI enhancement for food claims.

All OpenAI-specific code lives here. Imports are safe when the openai package is
not installed, AI is disabled, or API keys are missing.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

_CACHE: dict[str, dict[str, Any]] = {}
_USAGE_DAY: str | None = None
_USAGE_COUNT = 0

_AI_MUTABLE_FIELDS = {
    "assessment",
    "safer_wording",
    "missing_information",
    "recommended_action",
    "matched_themes",
}


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _daily_limit() -> int:
    try:
        return max(0, int(os.getenv("FOOD_CLAIM_REVIEW_AI_MAX_DAILY", "50")))
    except ValueError:
        return 50


def _cache_key(claim_text: str, food_type: str, jurisdiction: str) -> str:
    raw = json.dumps(
        {
            "claim_text": claim_text.strip().lower(),
            "food_type": food_type.strip().lower(),
            "jurisdiction": jurisdiction.strip().lower(),
        },
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _usage_available() -> bool:
    global _USAGE_DAY, _USAGE_COUNT
    today = date.today().isoformat()
    if _USAGE_DAY != today:
        _USAGE_DAY = today
        _USAGE_COUNT = 0
    return _USAGE_COUNT < _daily_limit()


def _increment_usage() -> None:
    global _USAGE_COUNT
    _USAGE_COUNT += 1


def should_attempt_ai(
    deterministic_response: dict[str, Any],
    *,
    use_ai: bool,
    force_ai: bool,
) -> bool:
    """Return True only when request/config/deterministic gating allows AI."""
    if not use_ai:
        return False
    if not _env_bool("FOOD_CLAIM_REVIEW_AI_ENABLED", False):
        return False
    if not os.getenv("OPENAI_API_KEY", "").strip():
        logger.info("Food claim review AI skipped: OPENAI_API_KEY missing")
        return False
    if not _usage_available():
        logger.info("Food claim review AI skipped: daily limit exceeded")
        return False
    if force_ai:
        return True
    if deterministic_response.get("claim_type") == "therapeutic_or_disease_related_claim":
        return False
    return (
        deterministic_response.get("risk_level") == "review_required"
        or deterministic_response.get("claim_type") == "unclassified_food_claim"
    )


def maybe_enhance_claim_review(
    deterministic_response: dict[str, Any],
    *,
    claim_text: str,
    food_type: str,
    jurisdiction: str,
    use_ai: bool,
    force_ai: bool,
) -> dict[str, Any]:
    """
    Optionally enhance deterministic review with OpenAI.

    Falls back to the deterministic response for disabled config, missing keys,
    missing package, cache/limit issues, API errors, or invalid model output.
    """
    response = dict(deterministic_response)
    response["ai_used"] = False

    if not should_attempt_ai(response, use_ai=use_ai, force_ai=force_ai):
        return response

    cache_enabled = _env_bool("FOOD_CLAIM_REVIEW_AI_CACHE_ENABLED", True)
    key = _cache_key(claim_text, food_type, jurisdiction)
    if cache_enabled and key in _CACHE:
        cached = dict(_CACHE[key])
        cached["ai_used"] = True
        return cached

    try:
        ai_payload = _call_openai(response, claim_text, food_type, jurisdiction)
    except ImportError:
        logger.info("Food claim review AI skipped: openai package unavailable")
        return response
    except Exception as exc:
        logger.warning("Food claim review AI failed; deterministic fallback used: %s", exc)
        return response

    merged = _merge_ai_payload(response, ai_payload)
    merged["ai_used"] = True
    _increment_usage()
    if cache_enabled:
        _CACHE[key] = dict(merged)
    return merged


def _call_openai(
    deterministic_response: dict[str, Any],
    claim_text: str,
    food_type: str,
    jurisdiction: str,
) -> dict[str, Any]:
    try:
        from openai import OpenAI
    except Exception as exc:  # package absent or broken install
        raise ImportError("openai package unavailable") from exc

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    model = os.getenv("OPENAI_CLAIM_MODEL", "gpt-5.4-mini")
    prompt = {
        "claim_text": claim_text,
        "food_type": food_type,
        "jurisdiction": jurisdiction,
        "deterministic_response": deterministic_response,
        "instructions": (
            "Return strict JSON only. Improve explanation quality for a food "
            "claim review. Do not provide legal certainty. Do not invent source "
            "citations. Keep the disclaimer concept. Only return keys: "
            "assessment, safer_wording, missing_information, recommended_action, "
            "matched_themes."
        ),
    }
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "You help review AU/NZ food claim wording. You are not a "
                    "lawyer and must not provide final legal or substantiation "
                    "advice. Return strict JSON only."
                ),
            },
            {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
        ],
        response_format={"type": "json_object"},
    )
    raw = response.choices[0].message.content or ""
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("OpenAI response was not a JSON object")
    return parsed


def _merge_ai_payload(base: dict[str, Any], ai_payload: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for field in _AI_MUTABLE_FIELDS:
        value = ai_payload.get(field)
        if value is None:
            continue
        if field in {"safer_wording", "missing_information", "matched_themes"}:
            if isinstance(value, list):
                merged[field] = [str(item) for item in value if str(item).strip()]
        elif isinstance(value, str) and value.strip():
            merged[field] = value.strip()
    return merged


def _reset_ai_state_for_tests() -> None:
    global _USAGE_DAY, _USAGE_COUNT
    _CACHE.clear()
    _USAGE_DAY = None
    _USAGE_COUNT = 0
