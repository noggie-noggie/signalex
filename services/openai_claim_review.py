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
from datetime import date, datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

_CACHE: dict[str, dict[str, Any]] = {}
_USAGE_DAY: str | None = None
_USAGE_COUNT = 0
_USAGE_BY_IP: dict[str, int] = {}

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
        return max(0, int(os.getenv("FOOD_CLAIM_REVIEW_AI_MAX_DAILY", "25")))
    except ValueError:
        return 25


def _daily_ip_limit() -> int:
    try:
        return max(0, int(os.getenv("FOOD_CLAIM_REVIEW_AI_MAX_PER_IP_DAILY", "3")))
    except ValueError:
        return 3


def _max_input_chars() -> int:
    try:
        return max(1, int(os.getenv("FOOD_CLAIM_REVIEW_AI_MAX_INPUT_CHARS", "1000")))
    except ValueError:
        return 1000


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
        _USAGE_BY_IP.clear()
    return _USAGE_COUNT < _daily_limit()


def _quota_remaining(client_ip: str | None = None) -> int:
    _usage_available()
    global_remaining = max(0, _daily_limit() - _USAGE_COUNT)
    if client_ip:
        ip_remaining = max(0, _daily_ip_limit() - _USAGE_BY_IP.get(client_ip, 0))
        return min(global_remaining, ip_remaining)
    return global_remaining


def _quota_reset_iso() -> str:
    tomorrow = datetime.now(timezone.utc).date() + timedelta(days=1)
    return datetime.combine(tomorrow, datetime.min.time(), tzinfo=timezone.utc).isoformat()


def _increment_usage(client_ip: str | None = None) -> None:
    global _USAGE_COUNT
    _usage_available()
    _USAGE_COUNT += 1
    if client_ip:
        _USAGE_BY_IP[client_ip] = _USAGE_BY_IP.get(client_ip, 0) + 1


def _base_status(*, cache_hit: bool = False, ai_available: bool | None = None) -> dict[str, Any]:
    if ai_available is None:
        ai_available = (
            _env_bool("FOOD_CLAIM_REVIEW_AI_ENABLED", False)
            and bool(os.getenv("OPENAI_API_KEY", "").strip())
            and _usage_available()
        )
    return {
        "ai_used": False,
        "ai_available": ai_available,
        "ai_quota_remaining": _quota_remaining(),
        "ai_quota_reset": _quota_reset_iso(),
        "assessment_mode": "deterministic",
        "cache_hit": cache_hit,
    }


def _with_status(response: dict[str, Any], **status_overrides: Any) -> dict[str, Any]:
    out = dict(response)
    status = _base_status()
    status.update(status_overrides)
    out.update(status)
    if out.get("ai_quota_remaining", 0) <= 0 and out.get("ai_available") is False:
        out.setdefault("upgrade_prompt", "AI review quota is exhausted for today. Use the rule-based assessment or try again after the quota reset.")
    return out


def _log_ai_event(
    *,
    claim_text: str,
    ai_used: bool,
    reason: str,
    model: str,
    client_ip: str | None,
    token_usage: Any = None,
) -> None:
    logger.info(
        "food_claim_review_ai %s",
        json.dumps(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "claim_hash": hashlib.sha256(claim_text.strip().lower().encode("utf-8")).hexdigest(),
                "ai_used": ai_used,
                "reason": reason,
                "quota_remaining": _quota_remaining(client_ip),
                "model": model,
                "token_usage": token_usage,
            },
            sort_keys=True,
        ),
    )


def should_attempt_ai(
    deterministic_response: dict[str, Any],
    *,
    use_ai: bool,
    force_ai: bool,
    claim_text: str = "",
    food_type: str = "",
    client_ip: str | None = None,
) -> tuple[bool, str]:
    """Return True only when request/config/deterministic gating allows AI."""
    if not use_ai:
        return False, "use_ai false"
    if not _env_bool("FOOD_CLAIM_REVIEW_AI_ENABLED", False):
        return False, "AI disabled"
    if not os.getenv("OPENAI_API_KEY", "").strip():
        return False, "OPENAI_API_KEY missing"
    if len(claim_text or "") + len(food_type or "") > _max_input_chars():
        return False, "input too long"
    if _quota_remaining(client_ip) <= 0:
        return False, "quota exceeded"
    if force_ai:
        return True, "force_ai"
    if deterministic_response.get("claim_type") == "therapeutic_or_disease_related_claim":
        return False, "deterministic high-risk claim"
    if (
        deterministic_response.get("risk_level") != "review_required"
        and deterministic_response.get("claim_type") != "unclassified_food_claim"
    ):
        return False, "deterministic known pathway"
    return (
        deterministic_response.get("risk_level") == "review_required"
        or deterministic_response.get("claim_type") == "unclassified_food_claim"
    ), "eligible"


def maybe_enhance_claim_review(
    deterministic_response: dict[str, Any],
    *,
    claim_text: str,
    food_type: str,
    jurisdiction: str,
    use_ai: bool,
    force_ai: bool,
    client_ip: str | None = None,
) -> dict[str, Any]:
    """
    Optionally enhance deterministic review with OpenAI.

    Falls back to the deterministic response for disabled config, missing keys,
    missing package, cache/limit issues, API errors, or invalid model output.
    """
    response = dict(deterministic_response)

    attempt, reason = should_attempt_ai(
        response,
        use_ai=use_ai,
        force_ai=force_ai,
        claim_text=claim_text,
        food_type=food_type,
        client_ip=client_ip,
    )
    model = os.getenv("OPENAI_CLAIM_MODEL", "gpt-5.4-mini")
    cache_enabled = _env_bool("FOOD_CLAIM_REVIEW_AI_CACHE_ENABLED", True)
    key = _cache_key(claim_text, food_type, jurisdiction)
    if cache_enabled and reason in {"eligible", "force_ai", "quota exceeded"} and key in _CACHE:
        cached = dict(_CACHE[key])
        _log_ai_event(
            claim_text=claim_text,
            ai_used=True,
            reason="cache hit",
            model=model,
            client_ip=client_ip,
        )
        return _with_status(
            cached,
            ai_used=True,
            ai_available=True,
            ai_quota_remaining=_quota_remaining(client_ip),
            assessment_mode="ai_assisted",
            cache_hit=True,
        )

    if not attempt:
        ai_available = (
            _env_bool("FOOD_CLAIM_REVIEW_AI_ENABLED", False)
            and bool(os.getenv("OPENAI_API_KEY", "").strip())
            and reason not in {"quota exceeded", "input too long"}
            and _quota_remaining(client_ip) > 0
        )
        _log_ai_event(
            claim_text=claim_text,
            ai_used=False,
            reason=reason,
            model=model,
            client_ip=client_ip,
        )
        status = {
            "ai_available": ai_available,
            "ai_quota_remaining": _quota_remaining(client_ip),
            "cache_hit": False,
        }
        if reason == "quota exceeded":
            status["upgrade_prompt"] = "AI review quota is exhausted for today. Use the rule-based assessment or try again after the quota reset."
        return _with_status(response, **status)

    try:
        ai_payload = _call_openai(response, claim_text, food_type, jurisdiction)
    except ImportError:
        _log_ai_event(
            claim_text=claim_text,
            ai_used=False,
            reason="openai package unavailable",
            model=model,
            client_ip=client_ip,
        )
        return _with_status(response, ai_available=False, ai_quota_remaining=_quota_remaining(client_ip), cache_hit=False)
    except Exception as exc:
        _log_ai_event(
            claim_text=claim_text,
            ai_used=False,
            reason=f"openai error: {exc}",
            model=model,
            client_ip=client_ip,
        )
        return _with_status(response, ai_available=False, ai_quota_remaining=_quota_remaining(client_ip), cache_hit=False)

    merged = _merge_ai_payload(response, ai_payload)
    _increment_usage(client_ip)
    token_usage = ai_payload.get("_token_usage")
    _log_ai_event(
        claim_text=claim_text,
        ai_used=True,
        reason="called",
        model=model,
        client_ip=client_ip,
        token_usage=token_usage,
    )
    if cache_enabled:
        _CACHE[key] = dict(merged)
    return _with_status(
        merged,
        ai_used=True,
        ai_available=True,
        ai_quota_remaining=_quota_remaining(client_ip),
        assessment_mode="ai_assisted",
        cache_hit=False,
    )


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
    usage = getattr(response, "usage", None)
    if usage is not None:
        parsed["_token_usage"] = {
            "prompt_tokens": getattr(usage, "prompt_tokens", None),
            "completion_tokens": getattr(usage, "completion_tokens", None),
            "total_tokens": getattr(usage, "total_tokens", None),
        }
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
    _USAGE_BY_IP.clear()
    _USAGE_DAY = None
    _USAGE_COUNT = 0
