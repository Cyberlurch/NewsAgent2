# Newsletter delivery and recovery

Cybermed and Cyberlurch keep separate generation, recipients, model budgets and
Daily→Weekly→Monthly→Yearly provenance. Their production workflows retain the
shared `newsagent2-state-writers-${{ github.ref }}` lock and checkout
`ref: ${{ github.ref_name }}`. The local report CLI also holds a journal lock.
There is no new service/database and no global store migration.

## State meanings

Every result identifies `report_key`, cadence, logical edition dates,
`edition_id`, exact outgoing Markdown `content_version` (SHA-256), and a stable
Message-ID. Daily is identified by its edition date; Cybermed Weekly uses its
stored reporting-period helper; rolling Cyberlurch Weekly covers its date
range; Monthly/Yearly use their selected calendar period. Collection lookback
hours remain separate, in the report/Daily digest. Explicit period overrides
do not authorize a changed content version or a second submission.

| Status | Meaning |
|---|---|
| `generated` | Validated content retained; no SMTP acceptance asserted. |
| `intentionally_skipped` | Sending disabled or an explicit empty-report policy; no delivery success. |
| `accepted_by_smtp` | Known positive SMTP DATA completion for every intended recipient. **Not inbox delivery.** |
| `partially_accepted` | At least one known acceptance and at least one refusal. The run fails; no automatic retry. |
| `failed` | Generation/configuration/persistence failure, or a known SMTP rejection/no acceptance. The category identifies the phase. |
| `outcome_unknown` | SMTP completion cannot be established, including an interrupted in-flight attempt or an unverified legacy edition. No blind retry. |

Validated generated records may be saved before email; generation/ingestion
does not imply sending. Source caches are ingestion data. Sent flags and Daily
success clocks change only after full known SMTP acceptance. Partial acceptance
remains recipient-specific in the journal and never marks a whole report sent.
`state_persisted` means the delivery markers are in the current filesystem;
`git_persistence.json` separately records publication. Older recovery timestamps
never move a newer Daily/sent clock backwards.

The public journal contains recipient HMAC IDs, not addresses, SMTP settings,
raw refusal dictionaries, credentials or SMTP exception strings. Keep a
**32-byte-or-longer random raw secret** in GitHub Actions secret
`DELIVERY_RECIPIENT_HMAC_KEY`. Both workflows bind it explicitly. Keep it stable
for recovery; key/audience changes require review. An HTTP proxy placeholder is
not a suitable local HMAC key. Existing SMTP secrets remain separate as before
(Cyberlurch uses its dedicated secrets). `test` reads only
`TEST_RECIPIENTS_CONFIG_JSON`; it never falls back to production variables or a
recipient file. Default multi-recipient header concealment is retained.

## Mutation and store contracts

`EMAIL_MODE=none/test` redirects **all** report state, digests, rollups, health
data and YouTube caches to private scratch below the report directory. The
original files remain byte-identical, including missing files. Mode, not
cadence, controls production mutation. `real` permits production generation;
`SEND_EMAIL=0` is a documented skip and does not set Sent/Daily success markers.
An unresolved earlier partial/unknown handoff still blocks its production
edition when sending is disabled; it cannot be replaced by a skip. Use a
separate `none` preview or reconcile the original receipt explicitly.

Paths are centrally injectable via `NEWSAGENT_STATE_DIR` or individual variables
listed in `run_context.PATHS`, including the three cache paths and
`DELIVERY_JOURNAL_PATH`. The Cyberlurch Monthly state-write gate preserves only
its specifically approved rollup target. Cybermed Daily no longer grants an in-process
Backfill-Apply exception: frozen Daily/Weekly candidates use the separate
[reviewed maintenance](reviewed-maintenance.md) contract. The legacy
`CYBERMED_DIGEST_BACKFILL_APPLY=1` shortcut is refused. An ordinary none/test run
cannot commit production state.

Readers never create, rename, self-heal or overwrite files. Missing-new-store
returns an in-memory default. Corrupt/empty/duplicate-key/nonfinite JSON fails
with `corrupted_existing_store`; unsupported shapes use
`invalid_existing_store`; unreadable files use `unreadable_existing_store`.
Existing bytes stay unchanged. Saves validate preimages and outputs and use a
unique temporary file, fsync and atomic replacement. Unknown valid legacy
metadata is preserved; no legacy generation/processing marker is invented as
an SMTP receipt. Existing generated editions without known delivery history
stop required production resubmission as `legacy_delivery_history_unknown`.

## Recovery sequence

SMTP and Git do not share a transaction. There is **no exactly-once guarantee**,
and a stable Message-ID does not require servers to deduplicate. Before DATA,
the journal is durably `outcome_unknown`. Known acceptance/refusals are saved
before QUIT and before marker writes. An interrupted handoff remains visible.

Both workflows verify **every planned cadence and period**. A later failure
cannot be hidden by Daily success. Missing/unexecuted planned results are
reported individually as failures. State commits require overall success and
the production/approved-backfill gate; no `always()` state commit exists.
`always()` uploads only reports, validated receipts/content and generated-record
deltas. Deltas identify edition, scope, preimage hash, changed records and
pruned IDs; they do not export the entire intermediate process/health state or
promote none/test candidates into production.

Before a new production sender, `workflow_delivery recover` inspects prior
failed/cancelled/timed-out SMTP-capable runs (up to the API's last 100 completed
runs on that workflow/branch), downloads their recovery artifacts and validates
journals/content hashes. Known published acceptance is never downgraded. An
old runner's filesystem-success flag is not treated as a successful Git push.
Generated records are restored only with matching preimage hashes or an exact
already-applied record match; conflicting newer data requires operator review.
Accepted marker Intents are repaired before new generation, without models or
SMTP. An already accepted current edition bypasses collection/generation and
checks the original audience before acknowledging that receipt.

If a SMTP-capable failed/cancelled run has no usable artifact (including an
expired artifact or a killed runner before upload), production stops. Inspect
SMTP logs and the branch, restore available receipts and generation records,
then acknowledge **only those individually reconciled run IDs** in repository
variable `DELIVERY_RECOVERY_ACK_RUN_IDS` (comma-separated). This is not a generic
"retry anyway" switch. Older history outside the API window, absent legacy
annual receipts, SMTP log availability and artifact retention require operator
judgment; the application cannot prove historical non-delivery.

For a known complete acceptance with unpersisted markers, restore the artifact
to `out/recovery` and its validated journal, then run with the intended original
state paths, under the shared writer lock:

```bash
PYTHONPATH=src python -m newsagent2.workflow_delivery finalize-receipt \
  --edition-id 'cybermed:daily:2026-10-12:2026-10-12:production'
```

This command invokes neither models nor SMTP. It uses the original minimal
marker Intents, not a whole intermediate-state snapshot. Review and ordinarily
commit/push the validated result afterward. Keep backups/private SMTP logs
outside the public checkout and artifacts. A successful original receipt does
not authorize replay of a different audience or content version.

For partial acceptance, no automatic retry occurs. Only after independently
reviewing the original receipt, restoring the exact hashed Markdown and keeping
the original recipient HMAC key/configuration, an operator may explicitly send
the known rejected subset:

```bash
PYTHONPATH=src python -m newsagent2.workflow_delivery retry-saved-rejected \
  --edition-id 'cybermed:daily:2026-10-12:2026-10-12:production' \
  --subject 'Cybermed Report' --confirm-smtp-reviewed
```

Previously accepted recipients are excluded from the envelope, and the
original Message-ID is reused. A new ambiguous timeout retains prior accepted
IDs and blocks any further retry. `retry-saved-failed` similarly uses the saved
body only for specifically known pre-DATA/no-acceptance SMTP categories; it
cannot bypass generation/persistence failure, partial acceptance or uncertainty.

If SMTP logs positively establish that **all pending unknown recipients were
not accepted**, an explicit operator reconciliation is available:

```bash
PYTHONPATH=src python -m newsagent2.workflow_delivery confirm-pending-not-accepted \
  --edition-id 'cybermed:daily:2026-10-12:2026-10-12:production' \
  --confirm-smtp-reviewed
```

It preserves the previous unknown outcome in the attempt history and never
invents acceptance. Previously accepted IDs remain accepted. If logs cannot
establish the outcome, leave it unknown; do not use the command. Manual recovery
and real probes are operator actions after review, not automatic setup steps.

## Explicit replay after a generated-store failure before SMTP

`generation_validated` alone is **not** permission to send. Production generation
now stages Digest/Rollup candidates; its existing read-back checks validate the
staged view. Only after the complete main path returns via its approved queue,
including the existing medical/aggregate/coverage guards and an explicit marker
policy, is a readiness checkpoint saved. It binds the exact Markdown, original
subject, edition/period/Message-ID, audience HMACs/key ID, minimal marker Intents
and a complete hash manifest of required generated-record deltas. The checkpoint
is sealed with the existing `DELIVERY_RECIPIENT_HMAC_KEY`; no new secret is needed.
The Markdown and checkpoint are saved durably before candidate production writes.
Early staging artifacts cannot be restored as approved production records without
that checkpoint. Generation diagnostics from inside main describe staged read-back
validation; they do not establish SMTP or Git persistence.

A candidate write failure at that boundary records
`generated_persistence_failed_before_smtp` with positive `not_handed_off` phase
evidence. The ordinary Daily path still blocks regeneration/resending and prints
the corresponding preview command. A blocked normal run preserves that original
failure category rather than severing the recovery path.

After repairing the storage cause, use the intended checkout/state paths and
original recipient configuration/key. Restore the failed run's recovery artifact
files under `out/recovery/` (or `$REPORT_DIR/recovery/`). Keep all listed deltas,
hashed Markdown and `journal.json` together. A missing branch receipt is read from
the artifact for preview; Apply imports only the explicitly selected edition.
An existing branch receipt takes precedence, so an older artifact cannot override
known acceptance, partial acceptance or uncertainty. Review this **read-only**
preview; it performs no SMTP, providers, store restoration or journal import:

```bash
EMAIL_MODE=real PYTHONPATH=src python -m newsagent2.workflow_delivery replay-pre-smtp \
  --edition-id 'cybermed:daily:2026-10-12:2026-10-12:production'
```

Only after inspecting the preview and the original run, explicitly apply exactly
that edition (replace the example ID with the actual failed edition):

```bash
EMAIL_MODE=real PYTHONPATH=src python -m newsagent2.workflow_delivery replay-pre-smtp \
  --edition-id 'cybermed:daily:2026-10-12:2026-10-12:production' \
  --apply --confirm-replay
```

The same command works for Cyberlurch using its own edition ID and recipient
configuration. There is no subject override. All stores, content/subject/audience
bindings and **every** required delta are checked before mutation or SMTP. Deltas
are restored with their preimage hashes or recognized as already identical;
newer conflicting data stop the operation. Main and this CLI share the same local
writer lock. Preview does not create a missing lock; Apply serializes restoration,
SMTP and marker finalization without nesting that lock.

Before SMTP, the durable phase still becomes `handoff_started`/`outcome_unknown`.
Only full known acceptance finalizes the original marker Intents. A timeout or
partial acceptance blocks further blind Apply. Failed attempts retain safe phase/
category history. Repeating Apply after acceptance sends nothing; an unpersisted
receipt triggers only generated-record/marker repair, while a complete receipt
is acknowledged without another SMTP call. Publish the reviewed state by an
ordinary commit/push afterward; SMTP and Git still have no shared transaction.

Apply is prohibited in none/test mode. Legacy receipts without the complete new
readiness data, missing marker policy/Intents, pre-approval failures, incomplete
deltas, changed body/subject/audience/key and uncertain earlier attempts require
operator review. Empty accepted/unknown arrays are never used as proof of
non-submission. `retry-saved-failed` retains its existing known SMTP-failure scope;
its allowlist has not been broadened. Do not edit/delete journal rows or use
`DELIVERY_RECOVERY_ACK_RUN_IDS` to bypass this guard. Known complete legacy SMTP
receipts retain the existing marker-repair command; unverified legacy generation
does not become a replay authorization.

## Git failure and rollback

The publisher validates selected files, performs one ordinary commit/push and
reports failures. A lost push acknowledgement is checked with a read-only remote
SHA comparison. It never force-pushes, resets or automatically rebases JSON
state. Push/commit failure retains receipts, validated generation deltas and
`git_persistence.json`; restore/reconcile against the latest branch before an
ordinary retry. A cancelled run with missing artifacts is explicitly blocked.

To roll back code, revert this PR while retaining real receipts and reconciling
unfinished editions first. Do not restore an old entire state tree over newer
writes or assume old Sent markers prove delivery. Merge only reviewed record
changes/marker Intents using their hashes and provenance. Existing authorized
maintenance Apply tools remain available for targeted repairs.

Older receipt readers reject the new optional readiness/phase fields. A code
rollback can therefore stop production until a compatible reader is reviewed.
Keep complete journals and recovery artifacts; do not strip fields or reset
history merely to make an older version send again.

## Validation

The suite runs in a separate copied checkout with network/SMTP sockets forbidden:

```bash
NEWSAGENT_OFFLINE_TESTS=1 PYTHONPATH=src SEND_EMAIL=0 python -m pytest -q
```

Regression coverage includes complete/partial rejection, required configuration,
unknown timeouts/crashes, no duplicate retry, explicit rejected-only retry,
acceptance before state failure, missing recovery, generated-record hash
conflicts/idempotence, publication failure, per-cadence verification, both
newsletters in all none/test cadences, corruption preservation and additive
legacy handling. Existing recipient privacy, medical facts, model-budget,
provenance and Maintenance tests remain active. The PR #225/#226 results below are historical. Subsequent offline dependency
and provider repairs resolved those five RSS regressions; they are not current
known failures. See [development.md](development.md) for the current validation
procedure.
No real SMTP, provider, dispatch, global backfill or production state correction
was performed during this change.

PR #225 baseline: **584 passed, 5 known failures, 6 subtests passed**. The 57
new boundary cases passed. Plain `actionlint` passed both production workflows.
All six existing production state files retained their original SHA-256 hashes;
the tests additionally cover missing files and every centrally registered path.

Pre-SMTP replay follow-up: **634 passed, the same 5 known RSS failures, 6 subtests
passed**. All 50 new replay cases passed, including the actual Cybermed Daily and
Cyberlurch Monthly generation paths with provider mocks, fresh-checkout artifacts,
multi-delta completeness, legacy receipts and main/CLI lock serialization. The
targeted Delivery/Recovery and touched Weekly/Monthly tests passed (148 cases).
Production state SHA-256 hashes stayed identical. No workflow, model/budget or
maintenance configuration was changed and no production recovery was performed.
