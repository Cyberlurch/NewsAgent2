# NewsAgent2 – Automated Cybermed & Cyberlurch Newsletters

NewsAgent2 is a private GitHub Actions automation that generates and distributes two newsletters with stateful deduplication and digest rollups.

## Current architecture

### 1) Cybermed Report (medical, English)
- **Daily:** live PubMed + FOAMed collection, summarization, and digest generation.
- **Weekly:** digest-only from `state/cybermed_daily_digests.json`.
- **Monthly:** digest-only from `state/cybermed_daily_digests.json`.
- Weekly/Monthly **must not** perform live PubMed/FOAMed collection and **must not** regenerate bottom lines.
- Stored `bottom_line`, `top_pick`, and `deep_dive` fields are preserved for digest-only outputs.
- Markdown artifacts and diagnostics are uploaded in workflow artifacts.
- Email supports `none`, `test`, and `real` modes.

### 2) The Cyberlurch Report (general)
- Source-driven from YouTube/news inputs.
- **Daily:** collects current content and updates state.
- **Weekly/Monthly/Yearly:** use stored state and rollups where applicable.

## Workflow and scheduling

- **Main workflow:** `.github/workflows/newsagent.yml`
- Scheduled delivery is gated to approximately **05:30 Europe/Stockholm** using:
  - dual UTC crons, and
  - an explicit Stockholm-time gate.
- `workflow_dispatch` supports:
  - `report_mode`: `daily`, `weekly`, `monthly`, `yearly`, `all`
  - `which_report`: `cyberlurch`, `cybermed`, `both`
  - `email_mode`: `none`, `test`, `real`
  - `lookback_hours` override
  - `year_in_review_year` override

### Email mode behavior
- `email_mode=none`: generate output, send no email.
- `email_mode=test`: send to `TEST_RECIPIENTS_CONFIG_JSON`.
- `email_mode=real`: send to production recipient secrets.
- Recipient addresses must never be printed to logs; logs should show recipient counts only.

## State model

- `state/processed_items.json`
  - Prevents duplicate daily sends.
  - Tracks source health.
- `state/cybermed_daily_digests.json`
  - Stores Cybermed daily digest payloads consumed by Cybermed Weekly/Monthly.
- `state/cyberlurch_digests.json`
  - Stores Cyberlurch digest data.
- `state/rollups.json`
  - Stores monthly rollups used by Year-in-Review.
- Cybermed Weekly/Monthly are **read-only** relative to `processed_items`.
- Maintenance/backfill operations must be manual-only and safe by default.

## Operational runbook

### Cybermed Daily test run
1. Open **Actions → NewsAgent2 workflow → Run workflow**.
2. Set:
   - `which_report=cybermed`
   - `report_mode=daily`
   - `email_mode=none` (or `test` when explicitly validating mail routing)
3. Start run and review logs/artifacts.

### Cybermed Weekly test run
1. Run workflow manually.
2. Set:
   - `which_report=cybermed`
   - `report_mode=weekly`
   - `email_mode=none` (or `test`)
3. Confirm digest-only behavior (no live collection/summarization).

### Cybermed Monthly test run
1. Run workflow manually.
2. Set:
   - `which_report=cybermed`
   - `report_mode=monthly`
   - `email_mode=none` (or `test`)
3. Confirm digest-only behavior and artifact creation.

### How to run `email_mode=test`
- Use `email_mode=test` with any manual run combination.
- Verify sends target `TEST_RECIPIENTS_CONFIG_JSON` only.
- Confirm logs expose recipient **counts**, not addresses.

### What to check in logs
- Correct selected mode/report pair.
- No recipient address output; count-only recipient diagnostics.
- For digest-only Cybermed runs (weekly/monthly), confirm success criteria:
  - `runtime_pubmed_collect_seconds = 0.0`
  - `runtime_foamed_collect_seconds = 0.0`
  - `runtime_summarization_seconds = 0.0`
  - `read_only=True`
  - no traceback
  - artifact exists

## Maintenance and backfill

- Backfill is **not** part of normal newsletter delivery.
- Default mode must be **dry-run / audit-only**.
- Do not use SMTP or recipient secrets in backfill.
- Do not mutate `processed_items` during backfill.
- Do not overwrite non-empty digests unless explicitly requested and audited.
- Review backfill output before any apply step.

## Current roadmap

1. Validate next Friday scheduled run and delivery time.
2. Safe digest-store backfill.
3. Editorial Cybermed Monthly.
4. Editorial Cybermed Year-in-Review.
5. Later: dry-run state-safety, Node/actions maintenance, performance tuning.

## Security

- Keep all credentials and recipient configuration in GitHub Secrets.
- Never commit private email addresses, API keys, or SMTP credentials.
- Use placeholder addresses only in examples.
