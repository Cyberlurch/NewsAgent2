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
| Cybermed | Monthly | First Friday of the following month; next eligible day if excluded; valid saved Weekly sources and boundary/provenance checks |
| Cybermed | Yearly | January close, after the corresponding December Monthly |
| Cyberlurch | Daily / Weekly | Monday–Friday / Friday, independent of Cybermed holidays |
| Cyberlurch | Monthly / Yearly | Month's first day / January 1, including weekends; Monthly before Yearly |

Both newsletters use Daily → Weekly → Monthly → Yearly on shared dates, with
Monthly before Yearly and only eligible cadences included. The old
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
send identity. The subsequent [coverage audit](history-coverage.md) now checks the
transitive source chain, including interior weeks. Since the operator's 16 October
decision, valid available evidence can support explicitly labelled partial
Weekly/Monthly/Yearly reviews. Missing historical editions and unestablished
Legacy calendar references are not fabricated. A gap alone is not a permanent
block; corrupt inputs, duplicate/foreign/orphan references, unsafe boundaries,
unclassified upstream failures and contradictory medical facts still block.
An empty archive cannot support a review. [Reviewed maintenance](reviewed-maintenance.md)
is separate from calendar catch-up and does not certify or rewrite Legacy history.

On later eligible days after the first-Friday due date, only the immediately
preceding month's close is reconsidered. During January, the preceding year is
also reconsidered. This bounded policy uses identical Monthly/Yearly edition
IDs; it does not traverse arbitrary historical gaps. Fully accepted and
finalized editions of every cadence are removed from scheduled execution, with
their IDs recorded as `already_finalized_editions`. If no valid closing sources
are available, generation fails;
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

The 23 October run-creation observations were 08:42:23 UTC (Cyberlurch) and
08:50:09 UTC (Cybermed), respectively 5h12m23s and 5h10m09s after their native
Stockholm slots. Creation-to-job and later application/SMTP/publication timings
must be evaluated separately. The current eight-hour readiness grace is an alarm policy, not a punctuality fix. It is not extended in this change.

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

## Independent morning trigger (November 2026)

The production runs on November 24 were created at 16:01:32 and 16:17:03
Stockholm time, despite native 05:30/05:40 slots; they completed at 16:07:14
and 16:20:17. The delay was in GitHub's creation of the scheduled events,
not the model, generation or local State writer. Simply extending grace or
changing native cron cannot guarantee a 07:00–07:30 reading window.

Both producers now accept an explicit `report_mode=scheduled` workflow
dispatch, with today's Stockholm `logical_date`, no month/year overrides and
an explicit email mode. This is different from manual `all`: it uses the
canonical native calendar and preceding-period identities. The plan records
the true workflow-dispatch event and explicit date. It rejects a missing,
future or historical date for this automated mode. Already accepted and
persisted editions are pruned in both native and independent plans, so a
later native fallback is a no-op rather than another generation/send.
All existing recovery and audience checks remain in force.

The producer, recovery and State backfill workflows share the same
`newsagent2-state-writers` concurrency group with `queue: max` and
`cancel-in-progress: false`. GitHub's multi-entry queue is documented at
https://docs.github.com/en/actions/concepts/workflows-and-actions/concurrency.
It avoids replacing an earlier pending run when another writer arrives.
No cross-newsletter ordering is required; within each newsletter the existing
Daily → Weekly → Monthly → Yearly dependency order remains unchanged.

### Deploy on one independent timer host

Use the provided timer on an always-on Mac signed in to the local user
account, with Python 3 and the GitHub CLI installed. The timer needs only
GitHub Actions read/write access to this repository; it never needs SMTP,
recipient data or an OpenAI key. Prefer repository-scoped authorization. Keep
the credentials in the CLI's credential store, never in a plist, script or log.

In an existing trusted clone on the Mac:

```bash
git pull --ff-only
gh auth status --hostname github.com
python3 tools/morning_dispatch.py
bash tools/install_morning_timer_macos.sh
```

The Python command is a dry-run; it makes no network call or State change.
The installer registers a per-user LaunchAgent at local 06:50, 07:00, 07:10
and 07:20, with the Python driver independently checking Europe/Stockholm
06:50–07:30 including DST. The driver records an intent atomically before
each API POST and stores the returned run ID. A repeat timer, local crash
or uncertain API response cannot automatically repeat that day's dispatch.
Both newsletters are submitted individually; the shared writer queue
serializes their work without discarding pending jobs. A producer failure
does not prevent dispatch of the other. The existing native schedules remain
as fallback; the timer does not reset the production journal or replay old dates.

Monitor ~/Library/Logs/NewsAgent2/morning-dispatch.log and the returned Actions
run IDs. The local record is ~/.local/state/newsagent2/morning-dispatch.json.
An uncertain POST requires checking Actions before any operator retry. Do not
delete a same-day intent merely because mail has not yet arrived. The
LaunchAgent requires the Mac to be on, connected and the user session loaded;
GitHub's dispatch API and hosted runners must also be available. The timer
is not installed merely by merging this change, and the target window is
not a guaranteed SLA. Verify the next morning's creation, SMTP and publication
timestamps; compare the two accepted receipts. Remove the timer with
`launchctl bootout gui/$(id -u)/org.newsagent2.morning-dispatch` and then remove
the plist from ~/Library/LaunchAgents if desired.

Rollback is a normal code/workflow revert. Preserve all produced valid stores
and receipts: never reset state or rename period keys. V2 Weekly identity is
additive and the reverted readers can still read its ordinary fields. Before
resuming an older scheduler across holiday weeks, inspect the saved identity
and delivery history to prevent duplicate publication.
