# All-cadence operation and recovery

On 16 October 2026 the operator explicitly chose **source-backed partial
reviews** over permanent calendar-coverage blocks. Missing saved editions may
limit a Weekly, Monthly or Yearly review; they never become invented editions,
medical facts, complete coverage or successful delivery. Each newsletter keeps
its own sources, recipients, model budgets and calendar.

## Generation policy

New reviews use the existing saved chain: Cybermed Daily → Weekly → Monthly →
Yearly, and Cyberlurch's persisted video/Monthly evidence. No global backfill,
State reset, cache purge or rewrite of valid Legacy records is necessary.

Coverage remains `complete`, `incomplete` or `unknown` in stored diagnostics.
An available-evidence review may proceed with missing editions or an unknown
Legacy calendar/reference policy. A short localized reader note states that it
is partial; known counts are used only when the edition references establish
them. Missing material is omitted. Existing medical facts/conflict validation,
favorite criteria and source-specific perspectives are unchanged.

Malformed JSON/records, duplicate/conflicting periods, orphan/foreign references,
inconsistent date claims, unsafe month-boundary evidence and unclassified
upstream failures remain errors. Child coverage reasons propagate to the parent,
so a structural defect cannot hide behind `upstream_incomplete`. A closed period
with no usable saved source is a problem; it is not a fabricated empty edition.
Valid-empty editions remain distinct from missing editions. Monthly/Yearly
production reports must be closed; previews can inspect an open period.

Production workflows attempt every planned cadence even if an earlier report
fails, then retain a failing overall exit status. Each planned result is still
verified separately. Overall failure prevents State publication; validated
generation deltas and receipts remain recovery artifacts. No `always()` State
commit, blind SMTP retry, force push or automatic JSON rebase is added.

## Daily early check

**Newsletter readiness (all cadences, no sending)** runs at **04:00 and 12:00 Stockholm**
every day, after completed newsletter/state-recovery workflows on `main`, and on
manual request. Its two independent jobs assess all four
cadences, including the next Monthly and Yearly before their due dates. It reads
the current checked-out stores, source references and Actions metadata, checks
required configuration/recipient resolution for every cadence, and writes a
category-only artifact plus an Actions summary. It never creates State or locks,
imports artifacts, collects content, calls models or connects to SMTP.

The configuration check uses the same recipient resolver and newsletter-specific
SMTP bindings as sending. It tests presence/shape, not remote authentication or
server availability. Runtime secrets, addresses and recipient HMAC lists are not
written to its evidence. `none/test` newsletter runs retain their separate scratch
stores and test-only audience.

GitHub created the 19 October morning events more than five hours after their
native slots. An early check therefore allows up to six hours from the existing
05:30/05:40 slot before diagnosing an absent scheduled result: **11:30 for
Cyberlurch, 11:40 for Cybermed**, in Stockholm time including DST. Before that
deadline, yesterday's last business-day result must still be valid and today's
missing result is `awaiting_scheduled_run`, never successful delivery. The noon
check catches service that still has no finalized result. Known generation,
configuration, corruption, partial/unknown SMTP and unpersisted-acceptance errors
remain failures immediately, including before the deadline. Producer schedules,
recipient resolution, delivery guards and model budgets are unchanged.
Failure categories are also emitted as Actions annotations without private
configuration or recipient data, making them visible through metadata-only APIs.

| Status | Meaning |
|---|---|
| `ready` | Static prerequisites checked; not proof of an external service or inbox delivery. |
| `partial_ready` | Available evidence supports a labelled partial review. |
| `awaiting_close` | Future Monthly/Yearly close; source status is reported separately. |
| `awaiting_inputs` | Future sources are not due yet; they are not diagnosed as missed sends. |
| `awaiting_scheduled_run` | Today's result has not arrived within the bounded scheduler grace window; no delivery is inferred. |
| `already_accepted` | A known full SMTP receipt is already persisted; no second submission. |
| `recovery_required` | Saved generation/acceptance needs recovery, or the persisted Daily clock is stale. A Legacy clock is not SMTP proof. |
| `blocked` | Configuration, source or delivery reconciliation needs repair. |

The overall check fails for corrupt inputs, unresolved modern partial/unknown
delivery, stale Daily publication and prior failed SMTP-capable runs requiring
recovery, including failures whose receipt has not reached Git yet, and due modern
editions without a finalized SMTP receipt. Receipt absence is not proof that an
e-mail was unsent and never authorizes a blind retry. A complete
Daily cannot hide a Weekly failure. Read-only Actions metadata is bounded to the
same last 100 completed workflow runs as normal recovery. Positively identified
preview/test jobs and the five exactly reviewed Legacy runs are distinguished.
An inaccessible API is an error, not proof that no recovery is needed.

Pre-transition automatic catch-ups with no structured delivery evidence are
listed in plan `quarantined_editions`, with unknown history and
`automatic_resend=false`. They are not repeatedly scheduled as new work or
reported as finalized. This finite existing boundary is not widened. Structured
partial/unknown receipts always outrank it. Explicit historical manual requests
keep their original reconciliation guards; archive content may still support a
partial Yearly without authorizing replay of its old e-mails.

Local read-only source check (no credentials or network needed):

```bash
PYTHONPATH=src python -m newsagent2.operational_readiness \
  --report-key cybermed --output /tmp/cybermed-readiness.json
PYTHONPATH=src python -m newsagent2.operational_readiness \
  --report-key cyberlurch --output /tmp/cyberlurch-readiness.json
```

Use a destination outside input/State directories. `--as-of` accepts a
timezone-aware timestamp. `--check-config` requires the newsletter's runtime
environment; `--check-actions` uses a scoped GitHub token and reads metadata only.

## Recover the 16 October Daily without sending it again

Merge the fix, then start **Newsletter state recovery (no sending)** on `main`
with `report_key=cybermed`. This manual maintenance workflow is deliberately
separate from newsletter generation. It has no SMTP/model/transcript credentials
or recipient addresses and sets `SEND_EMAIL=0`. It shares the production writer group
and checks out `github.ref_name`.

It receives the existing `DELIVERY_RECIPIENT_HMAC_KEY` solely to verify sealed
generation checkpoints. This verification key contains no recipient addresses
and supplies neither SMTP authentication nor model access. Keep the same key as
the producing run; a missing/changed key must fail validation, never bypass it.
Fixed failure categories are also emitted as Actions error annotations so a
metadata-only API client can diagnose a failed recovery without archive access.

It validates/imports retained production recovery packets, restores only bound
generated deltas/ingestion checkpoints, finalizes known full SMTP acceptances
under the local writer lock, and publishes ordinary validated State. The separate readiness workflow then checks all four cadences again, including
Actions recovery status. State recovery cannot
retry unknown/partially accepted mail, generate new text or submit a newsletter.
Normal future production jobs keep the same pre-sender recovery path.

Run `38874103951` has non-expired recovery artifact
`cybermed-delivery-recovery-38874103951`. The development client's archive download
is denied; only live step/annotation/artifact metadata and the operator's Daily
receipt are established here. The state-only runner must validate the actual
packet and matching preimages. If that fails, keep the failure and its artifacts
for targeted reconciliation; do not ACK, delete receipts or reset State to force
success. Preserve a private packet copy before its 30-day retention expires.

September's old Monthly delivery remains unknown and is not automatically sent
again. The 12–16 October Weekly can use its actual available Daily sources as a
partial review; it does not need invented 14/15 October inputs. Sending an old
edition remains an explicit operator operation, subject to the original receipt
and edition guards. The newly reviewed policy does not dispatch or send it.
For the held Weekly of 16 October, the inspected original run annotations establish
no generation/SMTP handoff. After successful state recovery, the operator can
start **NewsAgent2 Cybermed** on `main` with `report_mode=weekly`,
`logical_date=2026-10-16`, `email_mode=real`; leave the other overrides blank.
This is the actual delayed Weekly and can call models/send mail; it is not part
of state-only recovery or an automatic test. The readiness check remains red
for that due, unfinalized edition until its normal result is reconciled/persisted.

## Verification and rollback

Offline integration exercises all eight actual application paths in `real`,
`none` and `test`: generation/rendering, journals, marker finalization, result
verification, no duplicate real submission and byte-identical production input
paths in preview/test. All external operations are mocked. Daily includes its
valid empty-report route; existing source/content regressions cover ingestion and
clinical facts. A separate CI harness executes the actual workflow Bash loops
with a non-sending Python stub, testing success and each cadence failure for both
newsletters. The full suite retains its unconditional network/SMTP isolation.

Rollback is an ordinary code revert while retaining State, generated records and
real receipts. It restores stricter calendar-coverage blocks. Neither code revert
nor stable Message-ID undoes server acceptance; SMTP and Git are not a transaction.
