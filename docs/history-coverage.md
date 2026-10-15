# Historical coverage and read-only audit

Coverage describes the stored edition/source chain. It does not validate medical
claims, prove ingestion completeness, or establish SMTP acceptance. SMTP acceptance
is not Inbox delivery. Legacy absence of a receipt proves neither success nor failure.

## Run an audit

Use the existing maintenance entry point, one newsletter and a bounded period:

```bash
PYTHONPATH=src python -m newsagent2.maintenance.backfill_digest_stores \
  --report cybermed --from-date 2026-08-31 --to-date 2026-09-30 \
  --as-of 2026-10-15T10:36:07Z \
  --source-commit "$(git rev-parse HEAD)" \
  --output-dir /tmp/newsagent-history-cybermed
```

Choose `cyberlurch` for its independent audit. Existing central path variables in
`run_context.PATHS` / `NEWSAGENT_STATE_DIR` select the inputs. No `.env`, recipient
configuration, network, source fetches, model calls, locks, or State writes are
needed. The CLI requires an explicit output directory for `--as-of`; State/input
paths are refused as output destinations. JSON and Markdown reports are written
atomically outside State. No Apply is enabled. `--apply`, `--report both` and
OpenAI reconstruction flags are refused in this audit mode.

The original invocation without `--as-of` remains a compatibility-only backfill
plan, with Apply still refused. It is not the provenance-aware audit. Its
Cyberlurch video dates no longer masquerade as edition dates. Reviewed Daily and
Weekly backfill workflows remain separate; no workflow configuration changes here.

Exit codes: **0** no assessed gaps/unknowns; **1** findings or unknown/incomplete
coverage requiring review; **2** corrupt, invalid, unreadable or unsupported
existing input, or invalid request/output. Missing-new-store is distinguished from
corrupted-existing-store; neither is created or repaired by reading. An invalid
record has a store-key/JSON-pointer finding. Unknown additive Legacy metadata is
preserved; unsupported explicit store versions stop that input. Audit outputs
contain identifiers, hashes and status categories, not source text or recipients.

The source commit is supplied by the operator, not independently fetched. Input
SHA-256s bind the actual bytes inspected; per-file change detection prevents a
hash from silently describing a different parsed preimage. This is not a Git/SMTP
transaction or an atomic multi-file snapshot. Use a stable checkout. `as-of` fixes
the calendar horizon, not a historical Git checkout or recovered artifact snapshot.
Historical stored records are inventory evidence, not a promise that they existed
at an earlier hypothetical cutoff.

## Coverage semantics and runtime behavior

- `complete`: all required editions are uniquely present and explicitly referenced
  within the assessed period under a supported, evidenced policy.
- `incomplete`: known missing required coverage, orphan/foreign references,
  conflicting duplicates, overlapping weeks or invalid period references. Known
  defects dominate additional unknowns.
- `unknown`: historical policy or edition references are not established. The
  current calendar is never silently assigned to Legacy records. A labelled
  weekday comparison is an assumption, not evidence of missed sends or operating
  start. A single valid stored Daily (including valid-empty) is a present record;
  its complete **record** scope does not assert a complete reporting interval.

Weekly links identify Daily editions, including valid-empty editions. Item-level
Daily dates are separate and need not mention an empty Daily. Monthly evaluates
all referenced weeks, including interior weeks, and only counts Daily dates inside
the month. Existing boundary-item trimming and closing-week guards remain active.
A Daily that exists but is not referenced by the Weekly does not cover its gap.
Yearly follows Monthly→Weekly→Daily rather than trusting twelve Monthly rows.
Stored `history_coverage` is additive; current checks recompute from sources rather
than trusting a stale `complete` flag. Existing readers and recovery delta
validators validate the optional metadata. New Weekly/Monthly records also bind
the edition/content version to the already generated report when available.

For Cybermed, production Monthly readiness and selection reject unknown/incomplete
source chains before synthesis; Weekly rejects them before publishing its store;
Yearly requires a closed, fully evidenced year before SMTP. Existing scheduled
empty-report guards and strict store readers remain. Legacy stores remain readable;
no history is migrated. Consequently, a new final aggregate over insufficiently
proven Legacy history can require operator review/recovery instead of sending.
There is no new bypass switch. Frozen plans supply the policy for **new** editions,
not an automatic historical migration.

For Cyberlurch, video publication dates cannot establish issue coverage. Monthly
source registries and namespaced Yearly evidence remain authoritative for factual
references; orphan/foreign/out-of-month refs are defects. Existing semantic,
enriched-Legacy and thin-Legacy classification stays separate from calendar
coverage and `source_basis`. Thin Legacy is not promoted to factual evidence.
New Monthly records explicitly retain unknown edition history; known reference defects
block production Monthly before persistence/SMTP. Production Yearly
still requires all Monthly calendar slots and now rejects known reference defects;
unknown issue history is allowed only with a visible limitation and a partial-
evidence guard in the existing synthesis prompt, never “calendar coverage is
complete.” This preserves the existing Monthly-based product without inventing a
Daily issue ledger. Unknown coverage is not a new send-success status.

Previews show coverage limitations; none/test retain the existing scratch-path and
test-recipient policy. An up-to-date partial year is not a final complete year.
The horizon separates months already due, closed months awaiting their next
scheduled closing slot, the open month and future months. Due calculations use
Stockholm slot times and independent newsletter calendars. Historical applicability
of those horizon rules is explicitly unverified unless evidenced separately.

## Recovery boundary / next part 07b

See [the audited inventory](history-audit-2026-10-15.md). Coverage diagnosis does not
establish that a recoverable historical source exists. Before any separate Apply,
review exact target IDs, original source availability and provenance, source and
target byte hashes, backup, proposed diff and idempotence. Preserve valid-empty
and existing valid Daily/Weekly records. Reconstructed output needs truthful
reconstruction timestamps; never invent an original generation time or SMTP receipt.

Existing `.github/workflows/cybermed-daily-backfill.yml` explicitly separates Apply;
`cybermed-weekly-backfill.yml` uses a reviewed candidate hash and insert-only tool.
Their broader source/target hash, backup and conflict protections are work for 07b,
not certified by this PR. `maintenance-backfill.yml` remains audit-only. Existing
sealed pre-SMTP recovery keeps its receipt/HMAC requirements; malformed new
coverage cannot bypass store validation. This audit never authorizes SMTP replay.

Report retention is already 14 days; recovery/timing is 30 days in both newsletter
workflows. Do not upload whole stores/caches, recipients, credentials or full
copyrighted source texts to this public repository/artifacts. Expired artifacts
are not established sources. No new archival workflow or retention increase here.

Rollback: revert these code/documentation changes. No State migration, resets,
regeneration or mail replay is required. It restores the earlier, weaker coverage
checks, which should be considered when choosing rollback.
