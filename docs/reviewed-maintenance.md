# Reviewed Cybermed reconstruction and publication

This is **insert-only maintenance**, not E-mail replay or delivery finalization.
A reconstructed edition is not an originally sent edition. The history audit
remains read-only; the generic `backfill_digest_stores --apply` remains refused.
Use the existing Weekly maintenance entry point for both Daily and Weekly
publication. The medical correction CLI and sealed pre-SMTP recovery retain their
independent, narrower contracts.

## Review a bounded Weekly reconstruction

```bash
PYTHONPATH=src python -m newsagent2.maintenance.backfill_cybermed_weekly \
  --from-date 2026-08-31 --to-date 2026-09-04 \
  --output /tmp/review/weekly-plan.json
```

The dry-run reads only the injected Daily/Weekly paths from `run_context.PATHS`;
`--daily-path` / `--weekly-path` explicitly override them. It creates no State,
Health, caches or locks. Choose output outside the input/State directories.
Missing input, invalid records, competing periods and duplicate IDs stop review.
Existing valid records, including valid-empty editions, are preserved.

Read the plan's `actions`, complete candidate records and their source IDs,
`history_coverage` and reconstruction basis; compare with original sources. The
plan includes source file/path SHA-256 bindings, frozen candidate hash, target
preimage/postimage hashes and `plan_sha256` for the **entire plan**. Missing-new-
target uses JSON `null`. The plan contains candidate summaries, not a copy of the
whole target store. No plan requests SMTP or marker repair.

Weekly input dates must contain the whole logical candidate period; partially
selected edge weeks are omitted. Range is bounded to 370 days and 53 candidates.
A Friday alone does not prove completeness. Existing `history_coverage` checks
include every required linked Daily and valid-empty editions. Explicit source
policy supports Swedish shifted closes with Friday identity and earlier actual
coverage; missing historical policy stays unknown. Partial/unknown snapshots are
labelled archival reconstructions, never complete final issues. Existing runtime
07a guards continue to block unsupported finalization.

`generated_at_utc` and `reconstructed_at` are the actual frozen reconstruction
time; original source generation markers stay separate. Re-auditing produces a
new plan/time/hash. Apply never recomputes a timestamp or calls a model.

## Publish the exact reviewed plan locally

The following is a **mutating operator command after review**, not a preview:

```bash
PYTHONPATH=src python -m newsagent2.maintenance.backfill_cybermed_weekly \
  --apply --reviewed-plan /tmp/review/weekly-plan.json \
  --expected-sha256 REVIEWED_PLAN_SHA256 \
  --backup-dir /private/operator-backups/newsagent-change-001 \
  --output /tmp/review/weekly-result.json
```

Replace the placeholder with the reviewed **plan_sha256**, not candidate_sha256.
Choose a new private backup directory outside the checkout and State (and outside
all injected State directories). Parent components/targets may not be symlinks.
Keep that directory on protected storage with retention chosen by the operator;
never commit it or upload it to the public repository. It contains exact preimage
bytes (or an absent-target manifest), the reviewed plan and hash manifest; files
are mode 0600, its directory 0700. Review inputs themselves must also be protected
from unauthorized replacement; a checksum is a review binding, not a signature.

Apply checks scope, candidate content hash, all source hashes and target hashes
under the existing common writer lock. It rechecks after durable backup, validates
and atomically writes one store, then verifies its bytes. Backup failure causes no
State write. An IO error after replacement may leave the exact reviewed postimage
with an incomplete private completion record: inspect the private backup and target
hash; no filesystem/Git transaction is claimed. The plan is never mutated.

An exact postimage with unchanged reviewed sources is an idempotent no-op; it
creates no new backup/timestamp or target write. A record with the same ID but a
different store postimage is not proof of prior success. Any other change needs a
fresh audit/review; do not force or widen the plan. A same-period alternate ID is
a conflict. Existing records are never overwritten, regardless of empty content.

No Sent/Delivery/processed/Health/Paper receipts or rollups are written. Apply
creates/acquires the common writer lock, not a Delivery receipt. The only State
payload target is the chosen Cybermed Daily or Weekly store.

## Daily generation and offline Apply

The existing Daily backfill workflow retains its explicitly requested,
**potentially paid/networked generation** operation (`apply=false`). It collects a
bounded historical reference window using the existing pinned models and budgets.
It does not publish State. Do not use it to claim that current retrieval is an
original historical report.

Generation exports `out/cybermed_daily_candidate.json` and
`out/cybermed_daily_reviewed_plan.json`. The candidate's historical `reference_utc`
and run date are separate from today's actual generation/reconstruction timestamp.
The generated plan binds the real State target, not its none-mode scratch copy.
The legacy `CYBERMED_DIGEST_BACKFILL_APPLY=1` path now fails closed with migration
guidance, before collectors or production mutation; it is not an Apply shortcut.
Other none/test scratch behavior and the separately authorized Cyberlurch Monthly
maintenance allowance remain unchanged.

For offline publication of that reviewed Daily plan:

```bash
PYTHONPATH=src python -m newsagent2.maintenance.backfill_cybermed_weekly \
  --cadence daily --apply \
  --candidate-path out/cybermed_daily_candidate.json \
  --reviewed-plan out/cybermed_daily_reviewed_plan.json \
  --expected-sha256 REVIEWED_DAILY_PLAN_SHA256 \
  --backup-dir /private/operator-backups/newsagent-daily-001 \
  --output /tmp/review/daily-result.json
```

The candidate file must be at the reviewed bound path and match its hash. There is
no collection/model fallback in this CLI. A separately preserved, provable original
edition would need a specific import plan retaining original timestamps and import
provenance; this PR adds no generic historical import/overwrite capability because
no original September source was found. Valid-empty is not evidence of damage.

## Existing workflow review/apply and Git recovery

Both Daily and Weekly workflows use the common writer concurrency and explicit
`github.ref_name` checkout. First run audit, review its artifact, then explicitly
choose Apply with `review_run_id` and `expected_sha256`. The workflows verify a
successful manual audit run from the same repository/branch/workflow, download
only its named audit artifact into runner temporary storage, and copy an exact
small filename allowlist into `out`. No artifact is extracted into State. Unexpected
files, directories, symlinks or oversized downloads stop. Hash/path/range checks
still apply after download. Apply has no provider credentials or generation step.

After the reviewed atomic publication, `tools/publish_maintenance.py` verifies the
postimage and committed Git preimage, rejects unexpected State/data or staged
changes, stages **only** the approved target, and performs normal commit/push with
errors propagated. It never force-pushes, rebases or regenerates. Audit artifacts
are uploaded before any commit attempt; safe reviewed-plan/result artifacts are
retained even after an Apply/push failure. Audit retention is 14 days, safe recovery
result retention 30 days; raw private backups are excluded.

**Hosted-runner limitation:** private backups in `RUNNER_TEMP` exist during the
job and disappear with the runner. They are not durable private archives. The
preimage must therefore also exist in the checked-out Git commit (or be explicitly
absent), and its hash must match the plan; retain the audit run's commit ID and
plan artifact. Git history provides the committed source bytes, not a private
backup service or permanent retention guarantee. For durable private backup
retention use the local command above with protected storage; this PR configures
no private remote storage or encryption keys.

If push fails, inspect current remote main and its target hash before retrying:

1. Remote is exact reviewed preimage and sources still match: the same offline
   plan can publish again on a fresh checkout; no providers or SMTP are involved.
2. Remote is exact postimage and sources still match: Apply is a no-op. Verify the
   branch/commit instead of generating another edition.
3. Anything else, including changed sources: stop and create a new reviewed plan.
   Never replace concurrent changes with an old whole-store backup.

Keep the original reviewed artifacts. A local work directory whose reviewed
postimage is already committed can retry a normal push only after checking the
remote branch history; non-fast-forward failure is visible and must be resolved
by review, not force. To restore a private preimage, review its hash and current
postimage under the common lock and change only the approved target. No automatic
rollback over another writer is supplied.

## CI integrity artifact and rollback

Offline CI explicitly verifies both result files and uploads only
`.ci-results/pytest.xml` and `.ci-results/integrity.json`, with hidden-file support
limited to those exact paths and missing-file failure. No arbitrary hidden tree,
secrets or State bodies are uploaded. Confirm artifact presence via GitHub metadata;
if archive download access is denied, report that independently of CI status and
local integrity evidence.

Rollback is a code revert, without State reset, global regeneration or E-mail
replay. It restores the old weaker maintenance contract, including direct
unreviewed Apply, so evaluate that regression. This work performed no productive
Apply or historical replacement; September source availability remains as recorded
in [the bounded inventory](history-audit-2026-10-15.md#07b-bounded-source-follow-up).
