# NewsAgent2 – Automated Cybermed & Cyberlurch Newsletters

NewsAgent2 is a private automation project that generates and emails two newsletters via GitHub Actions, keeping delivery reliable and duplicate-free.

- **Cybermed Report** (medical, English): screens PubMed journals and FOAMed/blog sources (RSS preferred, HTML fallback). Results are grouped by clinical domain with concise “bottom line” takeaways and optional deep dives.
- **The Cyberlurch Report** (general, German): aggregates and summarizes YouTube and news sources for a broader audience, with executive summaries and optional deep dives.

---

## Scheduling and automation (Europe/Stockholm)

- Weekdays (Mon–Fri) at **~06:00** local time.
  - Implemented with dual UTC crons (`0 4 * * 1-5` and `0 5 * * 1-5`) plus an early gate step that checks Europe/Stockholm time and only proceeds when the local time is within a delivery window around **06:00** (currently 05:45–06:59). This keeps a single delivery across DST changes while tolerating GitHub's schedule jitter.
- **Jan 1, 06:00** local time: **Year in Review** for each report (cron `0 5 1 1 *`).
- Automated cadences:
  - **Daily**: every weekday.
  - **Weekly**: runs on Mondays after the daily run.
  - **Monthly**: runs on the first Monday of each month after the weekly run.
  - **Yearly**: runs on Jan 1 and compiles the prior year's monthly rollups.
- Manual runs (`workflow_dispatch`):
  - Choose `report_mode` (`daily` / `weekly` / `monthly` / `yearly`).
  - Choose `which_report` (`both` / `cybermed` / `cyberlurch`).
  - Optional: override `lookback_hours`.
  - Optional (yearly only): set `year_in_review_year` to force a specific target year.
  - Only the requested combination runs; weekly/monthly remain read-only.
  - Time gating is **not** applied to manual runs; they proceed immediately regardless of local time.

**Year in Review targeting and safeguards**

- YEAR_IN_REVIEW_YEAR (env or workflow input) wins when set.
- Otherwise, if the local date in Europe/Stockholm is Jan 1 (manual or scheduled), the Year in Review targets the previous year.
- On other days, manual yearly runs target the current year for easier previews.
- Scheduled yearly runs skip sending if no monthly rollups exist for the target year (a short log line is emitted instead).

---

## How it runs (GitHub Actions)

- **Workflow file:** `.github/workflows/newsagent.yml`
- **Entry point:** `src/newsagent2/main.py`
- **State file:** `state/processed_items.json` (daily runs update it; weekly/monthly are read-only to keep rollups reproducible).
- **Rollups state:** `state/rollups.json` (non-secret). Monthly runs append/update a single entry per month and report with the top items, dates/links, and executive-summary bullets. The yearly report uses the prior year's 12 monthly rollups.

---

## Configuration

- PubMed/journal channels: `data/cybermed_channels.json`
- FOAMed sources: `data/cybermed_foamed_sources.json`
- Cyberlurch channels: `data/channels.json` (or `data/youtube_only.json` when present)

### FOAMed runtime toggles

- `FOAMED_AUDIT` (default `0`): when set to `1`, keep normal RSS-first behavior but also run a lightweight HTML sampling pass for RSS-healthy sources. Audit stats are included in the hidden run metadata attachment (not the visible email body).
- `FOAMED_FORCE_FALLBACK_SOURCES` (default empty): comma-separated list of FOAMed source names that should skip RSS and exercise the HTML fallback path, recorded in run metadata.

### Recipient configuration (primary and fallbacks)

Recipients are kept in secrets to avoid leaking addresses. The workflow prioritizes **RECIPIENTS_CONFIG_JSON**; older mechanisms are consulted only if that secret is missing or invalid.

**Preferred (combined) secret**

Create a GitHub Secret named `RECIPIENTS_CONFIG_JSON` with this shape:

```json
{
  "cybermed": {
    "daily":   ["alice@example.com", "bob@example.com"],
    "weekly":  ["weekly@example.com"],
    "monthly": ["monthly@example.com"]
  },
  "cyberlurch": {
    "daily":   ["cl-daily@example.com"],
    "weekly":  ["cl-weekly@example.com"],
    "monthly": ["cl-monthly@example.com"]
  }
}
```

**Fallbacks (used only when the combined secret is absent/invalid)**

- Per-report secrets: `RECIPIENTS_JSON_CYBERMED`, `RECIPIENTS_JSON_CYBERLURCH`
- Mode-specific overrides: `RECIPIENTS_JSON_CYBERMED_DAILY`, `RECIPIENTS_JSON_CYBERMED_WEEKLY`, `RECIPIENTS_JSON_CYBERMED_MONTHLY`, `RECIPIENTS_JSON_CYBERLURCH_DAILY`, `RECIPIENTS_JSON_CYBERLURCH_WEEKLY`, `RECIPIENTS_JSON_CYBERLURCH_MONTHLY`
- Generic nested/flattened: `RECIPIENTS_JSON`
- Local file fallback (private, not committed): `data/recipients.json`
- Legacy single-list fallback: `EMAIL_TO`

### GitHub Secrets (required)

Configure under **Repo → Settings → Secrets and variables → Actions**:

- `OPENAI_API_KEY`
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`
- `EMAIL_FROM`
- `RECIPIENTS_CONFIG_JSON` (see above)
- `NCBI_API_KEY` (optional but recommended for PubMed)

### GitHub Variables (optional)

- `OPENAI_MODEL`
- `SEND_EMAIL`
- PubMed throttling parameters (as needed)
- `ROLLUPS_STATE_PATH` (optional): path to persist monthly rollups; defaults to `state/rollups.json`.
- `ROLLUPS_MAX_MONTHS` (optional): maximum number of monthly rollups to keep per report (default: 24; current month is never pruned).

---

## Security

- Keep all credentials and recipient lists in GitHub Secrets; do **not** commit addresses or passwords to the repository.
- Workflow logs avoid printing secret values and only show recipient counts (not addresses).
- Monthly rollups and the Year in Review use only newsletter content (titles, links, summaries) and never include recipient addresses.

---

## Email delivery

- Reports are generated in Markdown, converted to HTML for email clients, and include a plaintext alternative.
- Run metadata is attached as a `.txt` file for troubleshooting, while the email body omits the metadata block for readability. For Cybermed, the body keeps a minimal “Run Metadata” header as an anchor but the summary lines are removed; the full metadata stays in the attachment.
- Cyberlurch weekly/monthly/yearly reports omit a separate “Sources” section; source links live inside **Top videos (this period)**. The Cyberlurch Daily still includes “Sources”.
- The yearly cadence sends to the union of daily/weekly/monthly recipients for the selected report (deduplicated).

---

## Manual testing

- Run unit tests locally with `python -m pytest` (requires dependencies from `requirements.txt`).
