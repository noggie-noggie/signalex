"""Tests for deterministic Food signal duplicate detection."""

from __future__ import annotations

import sqlite3
import unittest

from services.food_duplicates import (
    canonical_food_url,
    filter_visible_food_duplicates,
    find_food_duplicate_groups,
    mark_food_duplicates,
)


def _row(
    row_id: int,
    title: str,
    url: str = "",
    *,
    source_label: str = "food_fsanz_updates",
    scraped_at: str = "2026-07-05T00:00:00",
    is_noise: int = 0,
) -> dict:
    return {
        "id": row_id,
        "domain": "food",
        "source_label": source_label,
        "title": title,
        "url": url,
        "scraped_at": scraped_at,
        "created_at": scraped_at,
        "summary": "",
        "is_noise": is_noise,
        "noise_reason": "",
    }


class FoodDuplicateTests(unittest.TestCase):
    def test_fsanz_slug_and_slug_zero_urls_are_duplicates(self):
        self.assertEqual(
            canonical_food_url("https://www.foodstandards.gov.au/media/call-comment-review-young-child-formula"),
            canonical_food_url("https://www.foodstandards.gov.au/media/call-comment-review-young-child-formula-0/"),
        )
        rows = [
            _row(3105, "Call for comment on review of young child formula", "https://www.foodstandards.gov.au/media/call-comment-review-young-child-formula"),
            _row(3104, "Call for comment on review of young child formula", "https://www.foodstandards.gov.au/media/call-comment-review-young-child-formula-0"),
        ]
        groups = find_food_duplicate_groups(rows)
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0].kept_id, 3104)
        self.assertEqual(groups[0].duplicate_ids, [3105])

    def test_duplicate_title_source_date_rows_are_not_both_visible(self):
        rows = [
            _row(1, "Food Standards News", "", scraped_at="2026-07-05T10:00:00"),
            _row(2, " Food   Standards News!!! ", "", scraped_at="2026-07-05T13:00:00"),
        ]
        visible = filter_visible_food_duplicates(rows)
        self.assertEqual([row["id"] for row in visible], [1])

    def test_duplicate_cleanup_marks_one_as_noise(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE signals (
                id INTEGER PRIMARY KEY,
                domain TEXT,
                source_label TEXT,
                title TEXT,
                url TEXT,
                scraped_at TEXT,
                created_at TEXT,
                summary TEXT,
                is_noise INTEGER DEFAULT 0,
                noise_reason TEXT DEFAULT ''
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO signals
                (id, domain, source_label, title, url, scraped_at, created_at, summary, is_noise, noise_reason)
            VALUES
                (:id, :domain, :source_label, :title, :url, :scraped_at, :created_at, :summary, :is_noise, :noise_reason)
            """,
            [
                _row(10, "Call for comment on review of young child formula", "https://www.foodstandards.gov.au/media/call-comment-review-young-child-formula"),
                _row(11, "Call for comment on review of young child formula", "https://www.foodstandards.gov.au/media/call-comment-review-young-child-formula-0"),
            ],
        )
        groups = mark_food_duplicates(conn, apply=True)
        self.assertEqual(len(groups), 1)
        kept = conn.execute("SELECT is_noise, noise_reason FROM signals WHERE id = 10").fetchone()
        duplicate = conn.execute("SELECT is_noise, noise_reason FROM signals WHERE id = 11").fetchone()
        self.assertEqual(kept["is_noise"], 0)
        self.assertEqual(duplicate["is_noise"], 1)
        self.assertEqual(duplicate["noise_reason"], "duplicate food signal: kept id 10")
        conn.close()

    def test_non_duplicate_different_fsanz_items_are_not_collapsed(self):
        rows = [
            _row(1, "Call for comment on infant formula", "https://www.foodstandards.gov.au/media/infant-formula"),
            _row(2, "Call for comment on cadmium levels", "https://www.foodstandards.gov.au/media/cadmium-levels"),
        ]
        self.assertEqual(find_food_duplicate_groups(rows), [])
        self.assertEqual(len(filter_visible_food_duplicates(rows)), 2)

    def test_include_noise_semantics_can_still_show_noise_rows(self):
        rows = [
            _row(1, "Duplicate update", "https://example.test/update"),
            _row(2, "Duplicate update", "https://example.test/update-0", is_noise=1),
        ]
        default_visible = filter_visible_food_duplicates([row for row in rows if row["is_noise"] != 1])
        include_noise_visible = rows
        self.assertEqual([row["id"] for row in default_visible], [1])
        self.assertEqual([row["id"] for row in include_noise_visible], [1, 2])


if __name__ == "__main__":
    unittest.main()
