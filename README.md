## NewsAgent2 – Cybermed & Cyberlurch newsletters

This repository generates two automated email newsletters via GitHub Actions:

- **Cybermed Report**: PubMed journal screening + FOAMed/blog commentary screening (RSS with HTML fallback), filtered to a strict **last 24 hours** window (with special Monday catch-up logic; see Scheduling).
- **Cyberlurch Report**: YouTube/news aggregation and summarization (project-specific sources and selection rules).

### Key features (Cybermed)
- **Overview (“BOTTOM LINE”)**: short clinical takeaways from newly screened items.
- **Deep Dives**: a smaller subset of the most relevant/impactful items (typically ≤ 8/day).
- **Top picks marking**: best items are highlighted with a **star icon (⭐)** in both Overview and Deep Dives.
- **FOAMed & Commentary**: screens curated education/blog sources using RSS where possible, and an HTML fallback when no RSS is available.

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

### Output
Generated reports are written to:
- `reports/`

A run metadata text attachment is produced for troubleshooting and audit.

### Known limitations
- Some FOAMed sources may block scraping (e.g., HTTP 403) or change URLs (HTTP 404). The collector reports per-source diagnostics in run metadata. If no posts are published (or none qualify in the last 24h), returning **0 FOAMed items is expected**.

### Roadmap
After stable daily operation:
- **Weekly report** (Monday after the daily run): “top tier only” digest for casual readers (Cybermed + Cyberlurch).
- **Monthly report** (after weekly): condensed “top tier only” roll-up (Cybermed + Cyberlurch).
