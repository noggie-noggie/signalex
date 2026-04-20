"""
scrapers/base.py — Abstract base class that every authority scraper inherits.

Concrete scrapers only need to implement `fetch_raw()`.  The base class
handles retry logic, deduplication checks, and returning a consistent
RawSignal structure that the classifier expects.
"""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TypedDict

import requests
from tenacity import retry, stop_after_attempt, wait_exponential


class RawSignal(TypedDict):
    """Minimal payload passed from a scraper to the classifier."""
    source_id: str          # stable hash of (authority + url); used for dedup
    authority: str          # e.g. "tga", "fda"
    url: str                # canonical URL of the source page / document
    title: str              # page / document title
    body_text: str          # plain-text content to send to Claude
    scraped_at: str         # ISO-8601 UTC timestamp


class BaseScraper(ABC):
    """
    All scrapers inherit from this class.

    Subclasses must implement `fetch_raw()` which returns a list of RawSignals.
    The base class provides:
      - `run()`: entry point called by the scheduler
      - `_make_source_id()`: deterministic hash for deduplication
      - `_http_get()`: retrying GET with shared headers
    """

    authority: str = ""     # override in subclass, e.g. "tga"

    # Shared session — created once per scraper instance, reused across requests.
    # This keeps TCP connections alive and ensures headers are set consistently.
    _session: requests.Session | None = None

    def __init__(self, config: dict) -> None:
        # config is the relevant slice of SCRAPER_CONFIG[authority]
        self.config = config

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def run(self) -> list[RawSignal]:
        """
        Called by the scheduler.  Returns deduplicated RawSignals ready for
        classification.

        TODO: Add deduplication against storage.SignalStore before returning.
        """
        raw = self.fetch_raw()
        # TODO: filter out source_ids already present in the signal store
        return raw

    # ------------------------------------------------------------------
    # Abstract — implement in each authority scraper
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_raw(self) -> list[RawSignal]:
        """
        Fetch and parse pages from the authority website.
        Return a list of RawSignal dicts (not yet classified).
        """
        ...

    # ------------------------------------------------------------------
    # Helpers available to subclasses
    # ------------------------------------------------------------------

    def _get_session(self) -> requests.Session:
        """Return the shared requests.Session, creating it on first call."""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-AU,en;q=0.9",
                # No Accept-Encoding override — requests handles gzip/deflate
                # automatically. Advertising "br" without a brotli decoder
                # causes servers to return unreadable compressed payloads.
            })
        return self._session

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def _http_get(self, url: str, **kwargs) -> str:
        """
        Retrying GET via the shared session.  Returns response text.
        Raises requests.HTTPError on 4xx/5xx; tenacity retries on network errors.
        """
        resp = self._get_session().get(url, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp.text

    @staticmethod
    def _make_source_id(authority: str, url: str) -> str:
        """Deterministic, collision-resistant ID for a scraped page."""
        key = f"{authority}::{url}"
        return hashlib.sha256(key.encode()).hexdigest()[:16]

    @staticmethod
    def _now_iso() -> str:
        return datetime.utcnow().isoformat() + "Z"
