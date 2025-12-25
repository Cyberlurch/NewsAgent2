# NewsAgent2 – Automated Cybermed & Cyberlurch Newsletters

NewsAgent2 is a private automation project that generates and emails two newsletters via GitHub Actions:

- **Cybermed Report** (medical, English): PubMed journal screening + FOAMed/blog sources (RSS first, HTML fallback).
- **The Cyberlurch Report** (general, German): YouTube/news sources aggregation and summarization.

The system is designed to run reliably on a schedule, avoid duplicate processing, and keep email delivery compatible across many email clients.

### Scheduling (Europe/Stockholm)
- Weekdays (Mon–Fri) at **06:00** local time.
  - Implemented via two UTC crons (04:00, 05:00) plus an early gate step that only proceeds when `TZ=Europe/Stockholm` time is exactly 06:00. This survives DST shifts.
- Automated cadences:
  - **Daily**: every weekday.
  - **Weekly**: additionally on Mondays (after the daily run).
  - **Monthly**: first Monday of each month (after weekly).
- Manual runs (`workflow_dispatch`) still support choosing `report_mode` (daily/weekly/monthly) and `which_report` (both/cybermed/cyberlurch).
  - Only the daily run updates state/commits; weekly/monthly stay read-only.

### Configuration
- PubMed/journal channels:
  - `data/cybermed_channels.json`
- FOAMed sources:
  - `data/cybermed_foamed_sources.json`
- Recipients:
  - Provide via secrets/environment variables (per-report & per-cadence supported).
  - Priority (highest first):
    1. `RECIPIENTS_JSON_<REPORTKEY>_<MODE>` (e.g., `RECIPIENTS_JSON_CYBERMED_DAILY`, `RECIPIENTS_JSON_CYBERLURCH_WEEKLY`)
    2. `RECIPIENTS_JSON_<REPORTKEY>` (legacy per-report list)
    3. `RECIPIENTS_CONFIG_JSON` (single JSON containing all lists; see template below)
    4. `RECIPIENTS_JSON` (accepts nested or flattened keys)
    5. `data/recipients.json` (optional local file; same formats as above)
    6. `EMAIL_TO` (legacy fallback; comma-separated)

#### Recipients JSON template

Use the same shape for `RECIPIENTS_CONFIG_JSON`, `RECIPIENTS_JSON`, or `data/recipients.json`:

```
{
  "cybermed": {
    "daily":   ["alice@example.com", "bob@example.com"],
    "weekly":  ["weekly-reader@example.com"],
    "monthly": ["monthly-reader@example.com", "cfo@example.com"]
  },
  "cyberlurch": {
    "daily":   ["alice@example.com"],
    "weekly":  ["weekly-reader@example.com", "bob@example.com"],
    "monthly": ["monthly-reader@example.com"]
  }
}
```

- **Papers** grouped under clinical categories (e.g., Critical Care / Anesthesia / Pain / AI / Other depending on configuration).
- Each paper includes:
  - a clickable link
  - a short **BOTTOM LINE** takeaway
- **Deep Dives**: a smaller subset of the most relevant items with a longer structured summary.
- **FOAMed & Commentary**: curated educational sources (RSS where possible; HTML fallback when feeds are missing).

### 2) The Cyberlurch Report (General)
Cyberlurch is a YouTube/news-based digest:

- **Executive Summary** and optional **Deep Dives**
- Daily runs are “last 24h” style; weekly/monthly runs provide curated rollups.
- Output format is optimized for email readability and click-through to sources.

---

## How it runs (GitHub Actions)

NewsAgent2 uses a single GitHub Actions workflow:

- **Workflow file:** `.github/workflows/newsagent.yml`
- **Entry point:** `src/newsagent2/main.py`

### Scheduled runs (Europe/Stockholm)
The scheduled job is intended to deliver at **06:00 Europe/Stockholm** on **weekdays (Mon–Fri)**.

Because GitHub cron runs in UTC and Stockholm uses DST, the workflow uses a DST-safe approach:
- GitHub triggers twice (two UTC times)
- The workflow checks Stockholm time at runtime and only continues when local time is exactly **06:00**

This yields one reliable weekday delivery at 06:00 local time year-round.

### Automated weekly + monthly
Weekly and monthly reports are automated in the same workflow:

- **Daily** runs every scheduled weekday.
- **Weekly** runs on **Mondays** (after daily).
- **Monthly** runs on the **first Monday of the month** (after weekly).

### Manual runs (workflow_dispatch)
You can manually run any report cadence using GitHub’s **Run workflow** button:
- Choose cadence: `daily` / `weekly` / `monthly`
- Choose report(s): `cybermed` / `cyberlurch` / `both`
- Optional: override lookback window (hours)

---

## State / “memory” and duplicate prevention

To prevent duplicates across runs, NewsAgent2 keeps a lightweight state file:

- `state/processed_items.json`

**Daily** runs update state (to avoid reprocessing the same content).
**Weekly** and **Monthly** runs are treated as **read-only** and should not mutate state.

This makes rollups reproducible and prevents weekly/monthly runs from “consuming” content meant for daily processing.

---

## Email delivery

NewsAgent2 sends email via SMTP.

- The newsletter body is produced as Markdown and rendered to HTML for email clients.
- A run metadata text file is attached for troubleshooting/audit purposes.
- The project avoids printing secret values or recipient lists into GitHub logs.

---

## Configuration

### GitHub Secrets (required)
You configure secrets under:
**Repo → Settings → Secrets and variables → Actions**

Typical secrets include:
- `OPENAI_API_KEY`
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`
- `EMAIL_FROM`
- `NCBI_API_KEY` (optional but recommended for PubMed rate-limit robustness)

No secret values are stored in the repository.

### GitHub Variables (optional)
Common non-secret variables:
- `OPENAI_MODEL`
- `SEND_EMAIL`
- PubMed throttling parameters (if used by your configuration)

### Recipient configuration (recommended patterns)

NewsAgent2 supports separate recipient lists for daily/weekly/monthly and for each report.

You can configure recipients in one of these ways:

**Option 1: One combined secret (recommended)**
Create a secret named: `RECIPIENTS_CONFIG_JSON`

Format:
```json
{
  "cybermed": {
    "daily":   ["a@example.com"],
    "weekly":  ["b@example.com"],
    "monthly": ["c@example.com"]
  },
  "cyberlurch": {
    "daily":   ["a@example.com"],
    "weekly":  ["b@example.com"],
    "monthly": ["c@example.com"]
  }
}
