# NewsAgent2

NewsAgent2 collects, selects and summarizes sources for two independently operated
email newsletters. It began as a personal briefing tool: reduce the material a
reader must scan, keep original-source links, and make saved reports reusable.
This public repository contains the application, curated configuration and
GitHub Actions workflows.

- **Cybermed Report:** a paper-first briefing for anaesthesia, intensive care and
  emergency medicine. PubMed is the primary research layer; selected FOAMed and
  guideline material provides context and commentary.
- **The Cyberlurch Report:** a personal selection of YouTube news, analysis and
  evergreen material, preserving source attribution and EN/DE/SV language handling.
  Channel priorities and topic buckets express editorial preferences, not
  independent verification of a video's topic or claims.

The newsletters have separate sources, generation, recipients and delivery
histories. They share infrastructure and a writer lock for safe Git-backed state
publication. Summaries support human review; they do not replace reading medical
papers or checking the claims of a source.

## Reading and operating guide

| Area | Details |
| --- | --- |
| Installation, offline tests and CI isolation | [Development](docs/development.md) |
| Schedule, logical editions and observed timing | [Calendar and timing](docs/calendar-and-timing.md) |
| SMTP receipts, failed runs and Git recovery | [Delivery and recovery](docs/delivery-recovery.md) |
| Historical provenance and missing editions | [Coverage audit](docs/history-coverage.md), [dated inventory](docs/history-audit-2026-10-15.md) |
| Reviewed Daily/Weekly reconstruction | [Maintenance](docs/reviewed-maintenance.md) |
| Medical content correction | [Evidence and targeted correction](docs/pubmed-evidence-correction.md) |

## Editions and saved data

Generation, ingestion, delivery and publication are different events. A validated
saved digest can exist without having been accepted by an SMTP server.

| Newsletter | Cadence | Source of the report |
| --- | --- | --- |
| Cybermed | Daily | Current PubMed and configured FOAMed sources |
| Cybermed | Weekly | Saved Daily editions |
| Cybermed | Monthly | Saved Weekly editions, with transitive Daily provenance and month-boundary trimming |
| Cybermed | Yearly | Saved Monthly rollups, following their Weekly/Daily provenance |
| Cyberlurch | Daily | Selected videos and their available source text; saves per-video digests |
| Cyberlurch | Weekly | Saved video digests preferred; collection fallback when absent, configurable supplementation |
| Cyberlurch | Monthly | Persisted video-digest material and explicit source references; production blocks when required material is unavailable |
| Cyberlurch | Yearly | Saved Monthly rollups with namespaced source evidence |

Cybermed aggregates do not fetch live replacement papers when their saved input
chain is missing. New final aggregates require the supported coverage and
provenance checks; a nonempty row or twelve Monthly rows alone is insufficient.
A valid empty edition is present coverage when explicitly referenced, whereas a
missing edition is not. Legacy records remain readable without acquiring invented
calendar policies or successful delivery history.

Cyberlurch video publication dates and digest counts do not establish newsletter
issue coverage. Semantic, enriched legacy and thin legacy rollups have different
evidence value. Known reference defects block production aggregates; unknown
historical issue coverage may remain explicitly limited under Cyberlurch's own
rules. It is not promoted to complete coverage. A bounded manual Monthly audit
can collect sources separately; this is not the production Monthly fallback.
See [coverage semantics](docs/history-coverage.md) and
[source-basis semantics](docs/cyberlurch-source-basis.md).

## Schedule and edition identity

The production workflows use native `Europe/Stockholm` schedules, including
seasonal clock changes: **05:30 Cyberlurch**, **05:40 Cybermed**.

| Newsletter | Scheduled editions |
| --- | --- |
| Cybermed | Daily on eligible Swedish business days with configured holiday/eve exclusions; Weekly on Friday or the last eligible business day before an excluded Friday |
| Cybermed | Monthly on the first Friday of the following month, or next eligible day if excluded; Yearly at the January close after December Monthly |
| Cyberlurch | Daily Monday–Friday; Weekly Friday with its rolling seven-day period |
| Cyberlurch | Monthly on the first calendar day; Yearly January 1, including weekends and independently of Cybermed holidays |

On shared dates Cybermed runs Daily → Weekly → Monthly → Yearly; Cyberlurch runs
Monthly → Yearly → Daily → Weekly. Each planned cadence and period is verified
separately: a Daily success cannot hide a failed Monthly report.

A frozen [edition plan](src/newsagent2/edition_plan.py) binds the logical date,
period, coverage end, calendar policy and stable edition ID across those steps.
Actual execution timestamps remain distinct. Native run creation is only an
inferred date basis: Actions may delay or drop triggers, and arbitrary whole-day
creation delays cannot reliably be detected from its metadata. These slots are
not guaranteed execution or inbox times. No external scheduler is installed.

Bounded catch-up considers the immediately preceding Monthly close and, during
January, the preceding Yearly close, retaining the same identities. Accepted,
finalized closes are filtered out. Missing inputs and unresolved delivery stop
normal processing; catch-up is not an arbitrary historical backfill. Manual
`logical_date` selects an explicit edition date, not additional replay permission.
See [calendar details and holiday examples](docs/calendar-and-timing.md).

## Content, models and limits

### Cybermed

PubMed ingestion uses bounded date-based catch-up, paging and persistent
checkpoints. Found IDs, retrieved records, selected papers, generated editions and
SMTP receipts are separate counts. It cannot certify exhaustive historical
retrieval. [Paper delivery memory](docs/paper-delivery.md) projects known Daily
receipts into durable PMID/DOI-based duplicate decisions and narrowly evidenced
source-update exceptions. [FOAMed adapters](docs/foamed-sources.md) distinguish
valid empty results, access/freshness failures and source Health; a successful
fetch is not a clinical endorsement.

Results and conclusions take priority over background in the bounded PubMed input.
Overview and Deep Dive use a shared validated core statement; contradictory or
uncertain output is reduced or omitted. An objective does not establish benefit;
a nonsignificant result does not prove ineffectiveness. Abstract-only evidence,
missing results, study design, limitations and population applicability matter.
[Clinical labels](docs/clinical-editorial-policy.md) are heuristics, **not GRADE**
or a complete medical validation. Publisher access can limit the source basis.
The configured guideline sentinels are not all activated primary-source adapters.

Cybermed uses the pinned **`gpt-4.1-2025-04-14`** snapshot with validated stage
settings. The dedicated workflow defaults are 96 provider calls, 350,000 input
tokens, 80,000 output tokens and a USD 1.50 **estimated-cost** limit per run,
subject to configured settings and pre-call checks. These are control estimates,
not a billing or output-completeness guarantee. Content-free usage telemetry is
included in diagnostics. The optional manual
[blinded model evaluation](.github/workflows/cybermed-model-eval.yml) uses synthetic
cases, can incur provider costs, sends no newsletter and cannot automatically
promote a candidate model.

### Cyberlurch

The default model is **`gpt-4.1`**, with configurable stage overrides in
[the summarizer](src/newsagent2/summarizer.py). Deterministic metadata selection
balances configured topics and channels before expensive content acquisition.
Daily limits are at most 14 acquisitions, 12 report items and 2 Deep Dives;
lower configured limits apply. API enrichment defaults to 150 requested video
IDs across the application invocation, in batches of at most 50. Failed requests
consume that budget, and no second pass guarantees full utilization.

[Candidate memory](docs/cyberlurch-discovery.md) is bounded: up to 500 records,
72-hour news TTL, explicitly opted-in 14-day evergreen TTL, and at most three
content attempts with one attempt per UTC edition day. Capacity, TTL and access
failures can leave material unprocessed; candidate progress is not delivery.

[Channel identity and text basis](docs/cyberlurch-source-basis.md) distinguish
configured/resolved identity from unverified or conflicting bindings, and a valid
empty listing from fetch or consent/TLS errors. Summaries distinguish description,
transcript, metadata-only and unknown source bases. Coverage describes available
text processed, never proven completeness of the video. Direct extraction defaults
to 80,000 characters; chunking defaults to five chunked items and is capped at
20 total chunks per invocation, with lower settings respected. Structural,
truncation and error-text guards omit unsafe output without unlimited repair
calls. Source attribution distinguishes reporting, forecasts, commentary and
theological interpretation; it is not independent fact-checking. YouTube and
transcript access may change or fail. No live availability or exhaustive coverage
is guaranteed.

## Setup and safe modes

Use Python **3.12** on Linux. From the repository root:

```bash
python3.12 -m venv /tmp/newsagent-dev
source /tmp/newsagent-dev/bin/activate
bash tools/install_dependencies.sh dev
export PYTHONPATH=src
python tools/verify_configs.py
python -m pytest -q --junitxml=.ci-results/pytest.xml
```

The installer uses [Python 3.12 constraints](constraints-python312.txt), pinned pip
and runtime/development requirements, followed by `pip check`. Production uses
`bash tools/install_dependencies.sh runtime`. Installation downloads dependencies;
the normal test suite blocks network, SMTP and provider access unconditionally.
For auditing original file integrity, run checks in the separate filesystem copy
described in [development.md](docs/development.md). No newsletter invocation is
needed to validate installation. CI also checks compilation, imports,
configuration, checksum-installed actionlint and shell syntax.

| Mode | Normal report behavior |
| --- | --- |
| `EMAIL_MODE=none` | Productive paths redirected to scratch; sending skipped |
| `EMAIL_MODE=test` | Productive paths redirected to scratch; any mail uses only explicit `TEST_RECIPIENTS_CONFIG_JSON` |
| `EMAIL_MODE=real` | Production generation/persistence permitted; required SMTP configuration and real SMTP failures fail the run |
| `SEND_EMAIL=0` | Explicit sending skip, including in `real`; never creates SMTP success or Sent/Daily-success markers |

**`none` and `test` do not mean offline or free.** Report generation can still
fetch sources and call models; `test` can send actual test email. They isolate
productive digests, rollups, ingestion/process markers, Health and caches. An
unresolved earlier production handoff remains blocked even with sending disabled.
Only the offline test suite provides the mocked validation boundary above.

Explicit reviewed maintenance has separate permissions. In particular, the manual
Cyberlurch Monthly rollup-write gate is narrowly scoped; ordinary preview mode
does not authorize production writes. Cybermed frozen-candidate Apply is a separate
offline tool. The old `CYBERMED_DIGEST_BACKFILL_APPLY=1` shortcut is refused.

Configure production secrets in Actions settings using the workflow's named
bindings: model credentials, newsletter-specific SMTP/recipient configuration and
`DELIVERY_RECIPIENT_HMAC_KEY` (a stable random raw secret of at least 32 bytes).
Cyberlurch has dedicated SMTP bindings. Test mode never falls back to production
recipients. Do not put secret values, recipient addresses or SMTP exception dumps
in repository files, example commands or public artifacts. Details and recovery
key/audience requirements are in [delivery-recovery.md](docs/delivery-recovery.md).

## Configuration, state and workflows

| Location | Role |
| --- | --- |
| [data/channels.json](data/channels.json), [data/youtube_only.json](data/youtube_only.json) | Cyberlurch defaults to `channels.json`; `youtube_only.json` is an available manual source configuration |
| [data/cybermed_channels.json](data/cybermed_channels.json), [FOAMed registry](data/cybermed_foamed_sources.json), [selection policy](data/cybermed_selection.json) | Independent Cybermed source/editorial configuration |
| [state/processed_items.json](state/processed_items.json) | Processing, source Health and bounded Cyberlurch candidate memory; not a delivery ledger |
| [Cybermed Daily](state/cybermed_daily_digests.json), [Weekly](state/cybermed_weekly_digests.json), [Cyberlurch digests](state/cyberlurch_digests.json), [rollups](state/rollups.json) | Validated saved generation and aggregate provenance |
| [run_context.PATHS](src/newsagent2/run_context.py) | Central path registry, including `delivery_journal.json`, `cybermed_pubmed_ingestion.json` and YouTube identity/content/attempt caches; some are created only when needed |
| [main](src/newsagent2/main.py), [delivery](src/newsagent2/delivery.py), [emailer](src/newsagent2/emailer.py) | Report orchestration, finalization boundary and SMTP result handling |

`NEWSAGENT_STATE_DIR` or individual registered path variables select state paths.
Reader calls do not create, rename or heal stores. Missing-new-store and corrupt,
invalid or unreadable existing stores are distinct; invalid existing bytes remain
unchanged and processing stops. Saves validate preimages/outputs and replace
atomically. Valid additive legacy fields are preserved without invented receipts.

| Workflow | Purpose |
| --- | --- |
| [newsagent.yml](.github/workflows/newsagent.yml) | Cyberlurch production and manual runs |
| [cybermed.yml](.github/workflows/cybermed.yml) | Independent Cybermed production and manual runs |
| [offline-tests.yml](.github/workflows/offline-tests.yml) | Network-blocked tests and integrity evidence |
| [maintenance-backfill.yml](.github/workflows/maintenance-backfill.yml) | General read-only backfill planning |
| [Daily maintenance](.github/workflows/cybermed-daily-backfill.yml), [Weekly maintenance](.github/workflows/cybermed-weekly-backfill.yml) | Audit/candidate generation and separately reviewed offline publication |
| [cybermed-model-eval.yml](.github/workflows/cybermed-model-eval.yml) | Explicit manual blinded comparison, no automatic promotion |

Production and state-writing maintenance retain the shared writer concurrency
boundary and branch-name checkout. Failed multi-cadence runs retain validated
recovery material; they do not commit an unvalidated whole intermediate state.

Production report artifacts retain 14 days; delivery recovery and plan/timing
metadata retain 30 days. Daily/Weekly maintenance audits retain 14 days and safe
Apply results retain 30 days; private backups are excluded. Model-evaluation
artifacts retain 14 days. Generic backfill and `offline-ci-results` use repository
retention defaults, without an explicit workflow override. CI uploads exactly
`pytest.xml` and `integrity.json` after checking their existence and integrity
status. Artifact existence, retention and download permissions are separate;
artifacts in a public repository are not private or permanent archives.

## Delivery, recovery and troubleshooting

Each structured result binds newsletter, cadence, period/edition ID, exact content
SHA-256 and stable Message-ID. [Status definitions](docs/delivery-recovery.md):
`generated` means validated content without acceptance; `intentionally_skipped`
means an explicit non-send; `accepted_by_smtp` means all intended recipients were
accepted by the server; `partially_accepted` means mixed acceptance/refusal and
fails the run; `failed` identifies a known failure category; `outcome_unknown`
means completion cannot be established. **SMTP acceptance is not inbox delivery.**

The journal retains recipient HMAC identifiers rather than plaintext addresses.
Sent/Delivery markers follow a known SMTP result. Local state persistence and Git
publication have separate evidence. SMTP and Git do not share a transaction;
a stable Message-ID does not guarantee deduplication or exactly-once delivery.
Partial or unknown outcomes require reconciliation, not blind resend.

When a run fails, inspect in this order:

1. Trigger, frozen plan, logical date, coverage end and every planned cadence.
2. Source/ingestion status, identity, access and available text basis.
3. Generation, validation and budget diagnostics; saved content is not a receipt.
4. The exact edition/content-version receipt and any unresolved handoff.
5. Validated record/marker persistence, then Git commit/push acknowledgement.

[Delivery recovery](docs/delivery-recovery.md) separates accepted-marker repair
without SMTP, sealed explicit pre-SMTP replay, and operator-reviewed partial/unknown
outcomes. Missing or expired required artifacts can block production. Keep
private SMTP evidence outside the repository; never delete a journal row to make
an edition send. Push failure remains failure. Reconcile hashes against the latest
branch before a normal publication retry; no force-push, state reset or whole-store
rollback over newer records.

For historical problems, start with the bounded, read-only
[coverage audit](docs/history-coverage.md). Its [dated inventory](docs/history-audit-2026-10-15.md)
is evidence of the inspected inputs, not a current Health/SLA certificate or proof
that an unavailable historical original never existed. Do not replace unknown
history with invented original editions or delivery successes.

[Reviewed Daily/Weekly publication](docs/reviewed-maintenance.md) uses frozen
candidates, a whole-plan hash, source hashes, target pre/postimages, insert-only
changes, a shared lock, private backup and atomic save. Matching postimages are
idempotent no-ops. Apply itself has no model/SMTP calls; preceding Daily candidate
generation can use paid providers. The generic audit has no Apply permission.
Backups belong outside state and the checkout and are not uploaded publicly.
Hosted-runner `RUNNER_TEMP` backups are ephemeral; durable protected storage is the
operator's responsibility. Git preimage checks are not a private archive.
Medical content correction, reconstruction and mail replay remain separate scopes.

## Contributing

Use an assessment → one scoped prompt → implementation PR → operator review/merge
→ next iteration workflow. Read the existing code and merged changes before each
iteration. Keep newsletters independent, preserve model/cost boundaries and saved
provenance, and run the appropriate offline checks. Do not use a productive
workflow dispatch or actual send as a test. Documentation rollback is a normal
revert; preserve real state and receipts. Operational recovery needs its own
review and is not implied by reverting documentation.
