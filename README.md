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
Both Cybermed and Cyberlurch run at **05:30** local time on:
- **Sunday, Monday, Tuesday, Wednesday, Friday**
- No runs on Thursday and Saturday.

#### Monday “weekend catch-up”
Monday morning reports must include all new items **since the last successful Friday run** (weekend catch-up). On other days, the system uses a standard **24h** lookback.

### Configuration
- PubMed/journal channels:
  - `data/cybermed_channels.json`
- FOAMed sources:
  - `data/cybermed_foamed_sources.json`
- Recipients:
  - provided via environment variables / secrets (see workflow configuration)

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
