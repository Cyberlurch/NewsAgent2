# NewsAgent2

NewsAgent2 is a private, GitHub Actions–driven Python project that automatically generates and emails daily newsletters from curated sources. It is designed for reliability (stateful “memory” to avoid duplicates), low operational friction (runs fully in CI), and safe handling of secrets (no leakage in logs).

## What it does

Each daily run produces (at least) two newsletters:

1. **The Cyberlurch Report**  
   General-interest, primarily based on **YouTube sources** (channels defined in configuration).  
   Goal: a concise daily overview plus a limited number of “Deep Dives”.

2. **Cybermed Report**  
   Medical/clinical, based on **PubMed / journal searches** via NCBI E-Utilities.  
   Goal: “paper-first” output with clinically oriented bottom lines and a separate Deep Dives section.

## Key features

- **Daily automation** via **GitHub Actions** on an Ubuntu runner (Python 3.12).
- **Stateful memory** using `state/processed_items.json` to prevent double-processing across runs.
- **Two independent report profiles** (“Cyberlurch” and “Cybermed”) sharing the same runtime but separated by `REPORT_KEY` to avoid state collisions.
- **Email delivery** via SMTP (secrets are provided via GitHub Secrets).
- **PubMed robustness**: supports NCBI API key configuration and should handle rate limits (429) gracefully.
- **Security-first**: avoid printing secrets or recipient addresses in Actions logs.

## Repository structure (high level)

- `src/newsagent2/`
  - `main.py` — orchestration (load config, collect items, apply state, summarize, generate markdown, email, persist state)
  - `collectors_youtube.py` — YouTube collection + transcript/description retrieval
  - `collectors_pubmed.py` — PubMed queries via NCBI E-Utilities
  - `summarizer.py` — LLM prompting for overview + deep dives
  - `reporter.py` — markdown report rendering
  - `state_manager.py` — load/save state + “already processed” checks
  - `emailer.py` — SMTP delivery of markdown content
- `.github/workflows/` — GitHub Actions workflow(s)
- `data/`
  - `channels.json` — unified configuration for YouTube channels and PubMed queries (topic buckets)
  - `recipients*.json` — recipient lists (should be kept out of git and provided securely)
- `state/processed_items.json` — memory/state file (persisted/committed by CI)

## Configuration

NewsAgent2 is configured through environment variables (GitHub Actions secrets/vars) plus JSON files under `data/`.

Common env vars (names may vary depending on your workflow and secrets setup):

- Report selection:
  - `REPORT_KEY` — e.g. `cyberlurch` or `cybermed`
  - `REPORT_TITLE` — email/report title
  - `REPORT_SUBJECT` — email subject
  - `REPORT_LANGUAGE` — output language (used by summarizer and headings)
  - `REPORT_PROFILE` — e.g. `general` or `medical`
- Runtime behavior:
  - `STATE_PATH` — default `state/processed_items.json`
  - `STATE_RETENTION_DAYS` — prune old state entries
  - `MAX_ITEMS_PER_CHANNEL` — cap items per source/query
  - `DETAIL_ITEMS_PER_DAY` — deep dive count
  - `DETAIL_ITEMS_PER_CHANNEL_MAX` — cap deep dives per channel
- Email (SMTP):
  - `SMTP_HOST`, `SMTP_PORT`, `SMTP_USERNAME`, `SMTP_PASSWORD`
  - `EMAIL_FROM` / `EMAIL_TO` (or recipients JSON-based delivery, depending on your setup)
- PubMed:
  - `NCBI_API_KEY` — optional but recommended for higher rate limits
  - `PUBMED_DATE_TYPE` — `edat` or `pdat` (controls date filtering behavior)

## Data files

### `data/channels.json`
Defines topic buckets and sources. Example (simplified):

```json
{
  "topic_buckets": [
    {
      "name": "General",
      "weight": 1.0,
      "channels": [
        { "name": "Some YouTube Channel", "url": "https://www.youtube.com/@..." },
        { "name": "PubMed: Critical Care", "source": "pubmed", "query": "\"Crit Care\"[jour]" }
      ]
    }
  ]
}
