"""
config.py — Central settings for the VMS regulatory intelligence platform.

All secrets are read from environment variables (or a .env file).
Non-secret tunables live as plain constants below.
"""

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args, **_kwargs) -> bool:
        return False

load_dotenv()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# Legacy TinyDB path — no longer used (persistence is in analytics/db.py → signals.db).
# Kept as a constant so any remaining references don't cause NameError.
DB_PATH = DATA_DIR / "signals.json"

# Jinja2 template directory used by the email digest.
TEMPLATES_DIR = BASE_DIR / "digest" / "templates"

# ---------------------------------------------------------------------------
# AI enrichment
# ---------------------------------------------------------------------------


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


FOOD_AI_ENRICHMENT_ENABLED: bool = _env_bool("FOOD_AI_ENRICHMENT_ENABLED", False)
AI_PROVIDER: str = os.getenv("AI_PROVIDER", "none").strip().lower() or "none"
ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")


def validate_food_ai_config() -> None:
    """Validate food AI enrichment settings only when enrichment is enabled."""
    if not FOOD_AI_ENRICHMENT_ENABLED:
        return

    if AI_PROVIDER == "anthropic" and not ANTHROPIC_API_KEY:
        raise RuntimeError(
            "FOOD_AI_ENRICHMENT_ENABLED=true with AI_PROVIDER=anthropic requires "
            "ANTHROPIC_API_KEY to be set."
        )
    if AI_PROVIDER == "openai" and not OPENAI_API_KEY:
        raise RuntimeError(
            "FOOD_AI_ENRICHMENT_ENABLED=true with AI_PROVIDER=openai requires "
            "OPENAI_API_KEY to be set."
        )
    if AI_PROVIDER not in {"anthropic", "openai"}:
        raise RuntimeError(
            "FOOD_AI_ENRICHMENT_ENABLED=true requires AI_PROVIDER to be either "
            "'anthropic' or 'openai'."
        )

# Model used for signal classification.  Upgrade to claude-opus-4-6 for higher
# accuracy on complex ingredient/regulatory text if cost allows.
CLAUDE_MODEL: str = "claude-sonnet-4-6"

# Max tokens to return from the classification call.
CLAUDE_MAX_TOKENS: int = 1024

# ---------------------------------------------------------------------------
# Email (SMTP)
# ---------------------------------------------------------------------------
SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER: str = os.getenv("SMTP_USER", "")
SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
EMAIL_FROM: str = os.getenv("EMAIL_FROM", SMTP_USER)

# Comma-separated list of recipient addresses, e.g. "a@co.com,b@co.com"
# Empty string → empty list; digest/alert code checks len(EMAIL_RECIPIENTS) before sending.
EMAIL_RECIPIENTS: list[str] = [
    addr.strip()
    for addr in os.getenv("EMAIL_RECIPIENTS", "").split(",")
    if addr.strip()
]

# ---------------------------------------------------------------------------
# Slack
# ---------------------------------------------------------------------------
# Incoming webhook URL.  Leave blank to disable Slack delivery entirely.
SLACK_WEBHOOK_URL: str = os.getenv("SLACK_WEBHOOK_URL", "")

# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------
# Cron expression for the daily digest run.  Default: 07:00 UTC every day.
DIGEST_CRON: dict = {"hour": 7, "minute": 0}

# Cron expression for scraper runs.  Default: every 6 hours.
SCRAPER_CRON: dict = {"hour": "*/6", "minute": 0}

# ---------------------------------------------------------------------------
# Scrapers
# ---------------------------------------------------------------------------
# How far back (in days) to look when deciding whether a listing is "new".
# Signals older than this are skipped during deduplication.
SIGNAL_LOOKBACK_DAYS: int = 30

# Per-authority scraper config.  Add a new entry here when onboarding a
# new health authority (e.g. EFSA, Health Canada, MHRA).
SCRAPER_CONFIG: dict = {
    "tga": {
        "enabled": True,
        "base_url": "https://www.tga.gov.au",
        # TGA ARTG search endpoint for complementary medicines
        "artg_search_url": "https://www.tga.gov.au/resources/artg",
        # Alerts & advisories feed
        "alerts_url": "https://www.tga.gov.au/safety/safety-monitoring-and-information/safety-alerts",
    },
    "fda": {
        "enabled": True,
        "base_url": "https://www.fda.gov",
        # Recalls / market withdrawals / safety alerts page (DataTables AJAX)
        "recalls_url": "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts",
        # Dietary supplements hub — scraped for safety-related sub-page links
        "hub_url": "https://www.fda.gov/food/dietary-supplements",
    },
    "artg": {
        "enabled": True,
        "base_url": "https://www.tga.gov.au",
        "artg_search_url": "https://www.tga.gov.au/resources/artg",
    },
    "iherb": {
        "enabled": True,
        "base_url": "https://au.iherb.com",
        "new_products_url": "https://au.iherb.com/c/new-products",
    },
    "chemist_warehouse": {
        "enabled": True,
        "base_url": "https://www.chemistwarehouse.com.au",
        "vitamins_url": "https://www.chemistwarehouse.com.au/shop-online/81/vitamins-supplements",
    },
}
