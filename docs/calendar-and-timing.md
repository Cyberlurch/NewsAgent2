# Calendar plans and observed timing

Both production workflows use native `Europe/Stockholm` schedules: Cyberlurch
05:30 and Cybermed 05:40. GitHub may delay or drop scheduled events. Neither the
timezone setting nor this implementation guarantees morning execution or inbox
delivery. Actionlint 1.7.12 validates the timezone syntax; its official release
archive is checksum verified by the shared installer.

The official documentation sources checked for this change are
[scheduled workflow syntax](https://github.com/github/docs/blob/main/data/reusables/repositories/actions-scheduled-workflow-example.md),
[timezone product support](https://github.com/github/docs/blob/main/data/features/actions-schedule-timezone.yml),
and [schedule delays](https://github.com/github/docs/blob/main/data/reusables/actions/schedule-delay.md).
Timezone support is enabled for GitHub.com. Direct documentation HTML was
blocked in the cloud environment; these official sources were read via API.

## One plan, multiple reports

After runtime setup, `python -m src.newsagent2.edition_plan --report-key cybermed`
(or `cyberlurch`) reads the actual run's `created_at` using the existing
read-permitted GitHub token. Job execution time is captured separately in the
first step. The plan is saved once in `out/<newsletter>_plan_metadata.json` and
bound to newsletter, effective email mode and workflow run ID. A preview plan
cannot silently inherit the scheduled production default; an unbound internal
logical-date variable is also rejected. `NEWSAGENT_PLAN_PATH`
passes this same plan to each report process and the per-cadence verifier.

The plan provides the logical date, exact period, coverage end, calendar policy
and existing delivery edition ID. It uses the existing journal, receipt,
Message-ID, writer lock and recovery implementation. Monthly/yearly overrides
and weekly coverage bounds are temporarily bound for each invocation. Period
and digest selection use the logical date; generation, source health,
SMTP/state events and filenames retain actual execution timestamps.

A job that waits until the next day or month still uses its plan's date. Actual
run creation is **not** proof of the intended original event date: GitHub's
schedule payload has no reliable original `scheduled_at`. Native plans use an
explicitly labelled `inferred_run_creation_date`, anchored to that local day's
slot. If creation is before that day's slot, or the schedule is unrecognised,
planning fails before collection/models/SMTP with a safe category. A delay of
one or more entire days that creates a run after the same local slot cannot be
detected from this metadata alone. Native scheduling therefore does not provide
a provably original date across arbitrary delays.

For reviewed manual work, the existing `workflow_dispatch` inputs now include
`logical_date` (`YYYY-MM-DD`). An explicit date gives an explicit slot basis;
it does not confer additional delivery/replay permission. Defaults remain
`email_mode=none`. Future production dates and contradictory delivery-period
overrides are rejected. Unknown/partial acceptance and saved ready generation
continue to require the existing operator recovery procedure, rather than
automatic collection or resend. See [delivery-recovery.md](delivery-recovery.md).

## Calendar policy

| Newsletter | Cadence | Productive scheduled rule |
| --- | --- | --- |
| Cybermed | Daily | Swedish eligible business days, including the existing holiday/eve exclusions |
| Cybermed | Weekly | Friday, or the last eligible business day before an excluded Friday |
| Cybermed | Monthly | First Friday of the following month; next eligible day if excluded; complete required saved closing week/provenance |
| Cybermed | Yearly | January close, after the corresponding December Monthly |
| Cyberlurch | Daily / Weekly | Monday–Friday / Friday, independent of Cybermed holidays |
| Cyberlurch | Monthly / Yearly | Month's first day / January 1, including weekends; Monthly before Yearly |

Cybermed's shared dates run Daily → Weekly → Monthly → Yearly. Cyberlurch retains
its existing order: Monthly → Yearly → Daily → Weekly when applicable. The old
first-business-day Cybermed Monthly helper policy is deliberately replaced by
the actual first-Friday base policy. Scheduler compatibility helpers delegate
to the same canonical plan rules.

For Christmas 2026, Cybermed Weekly runs Wednesday December 23, with identity
`cybermed:weekly:2026-12-21:2026-12-25:production`. Its `coverage_end` is December
23. It selects saved Daily records only through that date, and never synthesises
December 24/25 records. New planned Weekly records carry additive
`calendar_policy`, `coverage_end`, `expected_daily_dates`, `coverage_status`
metadata, without rewriting existing stores.

New productive Weekly generation checks unique saved Daily dates for required
eligible days. Planned manual Weekly previews use the same Monday–Friday ID
with coverage to the selected date; early production Weekly is blocked until
the eligible closing day. Older maintenance paths without a plan remain scoped
by their existing explicit authorization. Competing legacy period IDs in stores
or the journal require review, even if a generated store was lost. Monthly uses the existing whole-week and Daily boundary
provenance checks, with coverage checks for v2 weeks and explicit Daily dates
for the closing legacy week. A nonempty row alone is insufficient. Already
stored ambiguous shortened legacy weeks require review, not renaming or a new
send identity. Full retrospective coverage audit and reconstruction remain a
separate task; this change does not certify all historical interior legacy weeks.

On later eligible days after the first-Friday due date, only the immediately
preceding month's close is reconsidered. During January, the preceding year is
also reconsidered. This bounded policy uses identical Monthly/Yearly edition
IDs; it does not traverse arbitrary historical gaps. Fully accepted and
finalized closes are removed from the plan, with their IDs recorded as
`already_finalized_editions`. Missing closing inputs fail before generation;
after restoring validated inputs, the next eligible run may try that same close.
An unaccepted ready generated output instead retains the explicit replay guard.
When a new month begins, older unresolved closes need operator review.

## Timing evidence and failure handling

`out/<newsletter>_<cadence>_timing_metadata.json` records observed application
start, generation, full SMTP acceptance, local state completion and Git
publication acknowledgement. Missing events remain null. `outcome` describes
execution; `delivery_status` preserves the actual receipt status, including
partial/unknown outcomes when the run fails. Already accepted receipts never
acquire a fictitious current SMTP timestamp. Receipt authority stays in the
delivery journal and recovery artifacts, not this timing file.

Latencies distinguish inferred slot-to-creation lateness, creation-to-job,
job-to-application, generation, SMTP/state and publication. Lateness over 15
minutes is a diagnostic warning, not permission to skip or send. A job lacking
a valid plan emits safe failure metadata; calendar no-ops have plans with empty
entries and a reason. Every planned cadence is still verified independently.
Missing reports produce failed receipts and timing outcomes.

Git push failures retain the existing `git_push_failed_or_unknown` failure and
recovery proof. Only a successful push acknowledgement or verified remote SHA
emits a publication observation time. A no-change publication does not invent
a new Git timestamp. Generation and SMTP are not atomic with Git.

Both workflows upload only small plan/failure/timing metadata for 30 days,
including no-op/error runs, after the publication step. Existing report artifacts remain 14 days and
delivery recovery remains 30 days. No recipient addresses, raw exceptions,
credentials, provider hostnames or source fulltexts belong in timing metadata.
Artifact access follows repository settings; do not assume a public repository
makes artifacts secret or durable beyond their configured retention.

## Offline preview and future independent trigger

An example plan preview in an isolated checkout, with no live metadata request:

```bash
EMAIL_MODE=none GITHUB_EVENT_NAME=manual REPORT_MODE=weekly \
  REPORT_DIR=/tmp/newsagent-preview NEWSAGENT_PLAN_PATH=/tmp/newsagent-preview/plan.json \
  PYTHONPATH=src python -m newsagent2.edition_plan --report-key cybermed \
  --created-at 2026-12-23T04:40:00Z --job-started-at 2026-12-23T04:41:00Z \
  --logical-date 2026-12-23
```

`--created-at`/`--job-started-at` are offline inspection/test inputs. Production
workflows fetch run creation from GitHub instead of accepting payload claims.
Planning writes only output metadata; ordinary report none/test isolation still
protects production stores, caches and markers. Use temporary state paths and
the offline tests from [development.md](development.md); do not run a productive
dispatch or provider probe to validate a calendar change.

A future independent timer may use the existing newsletter-specific
`workflow_dispatch` with `logical_date`, cadence and existing month/year
override inputs, pointing at the intended branch. Monthly dispatches must
explicitly set `rollup_month_override` to the preceding month and Yearly must
set `year_in_review_year` to the preceding year, so manual defaults cannot
change the native edition. Use the canonical calendar to select eligible
cadences; an explicit manual `all` request is not a calendar-driven trigger. It must have only the action
dispatch access it needs, pass no secrets in inputs, and use the same edition
for duplicate triggers. A fixed historical replay still requires original
receipt/readiness/audience checks. No external timer, credentials, extra
secrets or timing service are installed by this PR. A native Actions watchdog
does not establish independent punctuality.

Rollback is a normal code/workflow revert. Preserve all produced valid stores
and receipts: never reset state or rename period keys. V2 Weekly identity is
additive and the reverted readers can still read its ordinary fields. Before
resuming an older scheduler across holiday weeks, inspect the saved identity
and delivery history to prevent duplicate publication.
