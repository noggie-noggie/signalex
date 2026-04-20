# VMS Regulatory Intelligence Platform

Automated scraping, classification, and daily digest for vitamins, minerals, and supplements regulatory signals from global health authorities.

---

## Architecture

```
main.py
  └── scheduler/jobs.py          APScheduler — two recurring jobs
        ├── scrape_and_classify  every 6 h
        │     ├── scrapers/tga.py      TGA (Australia) — ARTG + safety alerts
        │     ├── scrapers/fda.py      FDA (USA) — RSS feed + NDI docket
        │     ├── classifier/claude.py  Claude API classification
        │     └── storage/signals.py   TinyDB persistence
        └── send_digest          daily at 07:00 UTC
              └── digest/email_sender.py  Jinja2 render → SMTP send
```

### Data flow

```
Health authority website
        │
        ▼
  [Scraper]  ── fetch_raw() ──►  RawSignal
                                  { source_id, authority, url,
                                    title, body_text, scraped_at }
        │
        ▼
  [Claude API]  classify()  ──►  ClassifiedSignal
                                  { ingredient_names, event_type,
                                    country, severity, summary,
                                    confidence, ... }
        │
        ▼
  [SignalStore]  save_batch()  ──►  signals.json  (TinyDB)
        │
        ▼  (daily cron)
  [DigestSender]  send()  ──►  HTML + text email  ──►  recipients
```

---

## File map

| Path | Purpose |
|------|---------|
| `main.py` | Entry point; CLI flags `--scrape-now`, `--digest-now` |
| `config.py` | All settings; secrets via env vars |
| `scrapers/base.py` | Abstract `BaseScraper`; retry, dedup, `RawSignal` type |
| `scrapers/tga.py` | TGA ARTG listings + safety alerts |
| `scrapers/fda.py` | FDA dietary supplements RSS + NDI docket |
| `classifier/claude.py` | Claude API wrapper; `ClassifiedSignal` Pydantic model |
| `storage/signals.py` | TinyDB insert/query; swappable for Postgres |
| `digest/email_sender.py` | Group signals, render Jinja2, send SMTP |
| `digest/templates/` | `digest.html` + `digest.txt` email templates |
| `scheduler/jobs.py` | APScheduler job definitions |

---

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Playwright browsers (needed for JS-rendered TGA pages)
playwright install chromium

cp .env.example .env
# Edit .env with your API keys and SMTP credentials
```

---

## Running

```bash
# Start the scheduler (runs indefinitely)
python main.py

# One-off scrape and classify
python main.py --scrape-now

# One-off digest send
python main.py --digest-now
```

---

## Adding a new health authority

1. Create `scrapers/<authority>.py` inheriting from `BaseScraper`.
2. Implement `fetch_raw()` returning `list[RawSignal]`.
3. Add an entry to `SCRAPER_CONFIG` in `config.py`.
4. Register the scraper in `scheduler/jobs.py` under `scrape_and_classify()`.

---

## Classification event types

| `event_type` | Meaning |
|---|---|
| `new_listing` | New product registered with the authority |
| `approval` | Ingredient or health claim formally approved |
| `ban` | Ingredient or product prohibited |
| `warning` | Safety advisory issued |
| `label_change` | Mandatory labelling update |
| `adverse_event` | Reported adverse event (e.g. from CAERS) |
| `other` | Anything not fitting the above |

---

## Extending storage

`SignalStore` uses TinyDB by default (no infrastructure required).  To migrate to Postgres:

1. Replace TinyDB calls in `storage/signals.py` with SQLAlchemy.
2. Update `DB_PATH` in `config.py` to a connection string env var.
3. Add a migration tool (Alembic) and define the schema.

---

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | Yes | Claude API key |
| `SMTP_HOST` | No | Default: `smtp.gmail.com` |
| `SMTP_PORT` | No | Default: `587` |
| `SMTP_USER` | Yes | SMTP login username |
| `SMTP_PASSWORD` | Yes | SMTP login password (use an app password for Gmail) |
| `EMAIL_FROM` | No | Default: `SMTP_USER` |
| `EMAIL_RECIPIENTS` | Yes | Comma-separated list of digest recipients |
