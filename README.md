# NewsAgent2 – Automated Cybermed & Cyberlurch Newsletters

NewsAgent2 is a private automation project that generates and emails two newsletters via GitHub Actions, keeping delivery reliable and duplicate-free.

- **Cybermed Report** (medical, English): screens PubMed journals and FOAMed/blog sources (RSS preferred, HTML fallback). Results are grouped by clinical domain with concise “bottom line” takeaways and optional deep dives.
- **The Cyberlurch Report** (general, German): aggregates and summarizes YouTube and news sources for a broader audience, with executive summaries and optional deep dives.

---

## Scheduling and automation (Europe/Stockholm)

- Weekdays (Mon–Fri) at **06:00** local time.
  - Implemented with dual UTC crons (`0 4 * * 1-5` and `0 5 * * 1-5`) plus an early gate step that sets `TZ=Europe/Stockholm` and only proceeds when the local time is exactly **06:00**. This keeps a single delivery across DST changes.
- Automated cadences:
  - **Daily**: every weekday.
  - **Weekly**: runs on Mondays after the daily run.
  - **Monthly**: runs on the first Monday of each month after the weekly run.
- Manual runs (`workflow_dispatch`):
  - Choose `report_mode` (`daily` / `weekly` / `monthly`).
  - Choose `which_report` (`both` / `cybermed` / `cyberlurch`).
  - Optional: override `lookback_hours`.
  - Only the requested combination runs; weekly/monthly remain read-only.

---

## How it runs (GitHub Actions)

- **Workflow file:** `.github/workflows/newsagent.yml`
- **Entry point:** `src/newsagent2/main.py`
- **State file:** `state/processed_items.json` (daily runs update it; weekly/monthly are read-only to keep rollups reproducible).

---

## Configuration

- PubMed/journal channels: `data/cybermed_channels.json`
- FOAMed sources: `data/cybermed_foamed_sources.json`
- Cyberlurch channels: `data/channels.json` (or `data/youtube_only.json` when present)

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

---

## Security

- Keep all credentials and recipient lists in GitHub Secrets; do **not** commit addresses or passwords to the repository.
- Workflow logs avoid printing secret values and only show recipient counts (not addresses).

---

## Email delivery

- Reports are generated in Markdown, converted to HTML for email clients, and include a plaintext alternative.
- Run metadata is attached as a `.txt` file for troubleshooting, while the email body omits the metadata block for readability. For Cybermed, the body keeps a minimal “Run Metadata” header as an anchor but the summary lines are removed; the full metadata stays in the attachment.
- Cyberlurch weekly/monthly reports omit a separate “Sources” section; source links live inside **Top videos (this period)**. The Cyberlurch Daily still includes “Sources”.

---

## Manual testing

- Run unit tests locally with `python -m pytest` (requires dependencies from `requirements.txt`).
