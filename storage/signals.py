"""
storage/signals.py — Persistence layer for classified signals.

Uses TinyDB (a lightweight JSON file store) during development so there are
no infrastructure dependencies.  The SignalStore class abstracts the backend
so it can be swapped for SQLAlchemy + Postgres by changing only this file.

Responsibilities:
  - Save new ClassifiedSignals (insert-only; signals are immutable once saved).
  - Deduplication check by source_id before insert.
  - Query signals by date range for the daily digest.
  - Mark signals as "digest_sent" after a digest run.

Implementation checklist (fill in during build phase):
  [ ] Replace TinyDB with a proper DB if signal volume exceeds ~100k records
  [ ] Add an index on scraped_at for efficient date-range queries
  [ ] Consider a separate table/collection for "digest runs" (audit trail)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from tinydb import TinyDB, Query

import config

if TYPE_CHECKING:
    from classifier.claude import ClassifiedSignal


class SignalStore:
    """
    Simple insert-and-query store backed by TinyDB.

    Usage:
        store = SignalStore()
        store.save(classified_signal)
        signals = store.get_unsent_signals()
    """

    def __init__(self, db_path=None) -> None:
        path = db_path or config.DB_PATH
        # TODO: initialise TinyDB
        # self.db = TinyDB(path)
        # self.table = self.db.table("signals")
        pass

    def exists(self, source_id: str) -> bool:
        """
        Return True if a signal with this source_id is already stored.
        Used by BaseScraper.run() to skip duplicate fetches.

        TODO: Query self.table for source_id match.
        """
        # PLACEHOLDER
        return False

    def save(self, signal: "ClassifiedSignal") -> None:
        """
        Persist a ClassifiedSignal.  Silently skips if source_id already exists.

        TODO: Convert signal to dict via signal.model_dump() and insert into
        self.table, adding a "digest_sent" flag defaulting to False.
        """
        # PLACEHOLDER
        pass

    def save_batch(self, signals: list["ClassifiedSignal"]) -> int:
        """
        Save a list of signals, skipping duplicates.
        Returns the count of newly inserted signals.
        """
        count = 0
        for signal in signals:
            if not self.exists(signal.source_id):
                self.save(signal)
                count += 1
        return count

    def get_unsent_signals(self, since_hours: int = 24) -> list[dict]:
        """
        Return all signals that have not yet been included in a digest and
        were scraped within the last `since_hours` hours.

        TODO:
          - Compute cutoff = now - timedelta(hours=since_hours).
          - Query table where digest_sent == False and scraped_at >= cutoff.
          - Return list of dicts sorted by severity (high first) then scraped_at.
        """
        # PLACEHOLDER
        return []

    def mark_digest_sent(self, source_ids: list[str]) -> None:
        """
        Flip the digest_sent flag to True for the given source_ids after a
        digest email has been successfully sent.

        TODO: Bulk update in self.table where source_id is in source_ids.
        """
        # PLACEHOLDER
        pass
