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
An inaccessible API is an incomplete external check, not evidence of failed
SMTP or proof that no recovery is needed.

## External checks and trustworthy evidence

The 23 October early Cybermed check reported
`operational_actions_access_failed` at 07:18:52 UTC, while Daily/Weekly were
`awaiting_scheduled_run` and Monthly/Yearly `awaiting_close`. The old code made
one request and discarded HTTP details. Its exact HTTP cause is unknown: a
simulated 503 reproduces the code defect but does not establish that production
received 503. Later producers 39926546423 (Cyberlurch) and 39927473854 (Cybermed),
all four saved Daily/Weekly receipts, and readiness 39927771535 were successful.
Those editions must not be regenerated or resent.

The shared GET-only reader covers readiness, run creation metadata and recovery
metadata/archive downloads. It permits at most four attempts, each logical GET
within a 30-second retry/body deadline and connection/read inactivity timeouts
of at most 3/5 seconds. Requests share an overall 120-second readiness budget
(300 seconds for recovery reads). Waits use exponential backoff with jitter;
valid `Retry-After` dates/seconds and primary rate-reset headers can increase the
wait only when it fits the remaining budget. Timeout, connection failure,
408/429 and selected 5xx are retryable. A plain 401/403/404 is permanent;
403 requires explicit rate-limit header evidence. Invalid JSON or response
shape fails safely, without inventing an empty list. SMTP, model generation,
Git publication and mutation/import functions are never wrapped by these retries.

| Overall result | Meaning / operator action |
|---|---|
| `prerequisites_pass` | Checked static prerequisites and the stated Actions window passed; not a sending authorization, provider guarantee or inbox proof. |
| `externally_unverified` | No known local block, but external verification could not finish. Retry this read-only check later; permanent access/path errors require operator investigation. Exit 1. |
| `blocked` | A known source/configuration/delivery/publication problem exists, even when the API is also unavailable. Repair the stated problem; do not blindly send. Exit 1. |

The JSON artifact, summary, safe log and final annotation include matching
results. Diagnostics expose only fixed operation/category names, HTTP status,
attempt count/limit, elapsed time and exhausted budget. Recovered reads produce
no final error annotation. Full acceptance without persisted State calls for
**State recovery without SMTP**; partial/unknown outcomes call for **SMTP
reconciliation without automatic retry**. API failure cannot hide either.

The existing scope is the **latest 100 completed runs per workflow/branch**,
not the entire archive. Counted responses record `total_available`, `checked`,
`window_limit` and `complete_history`; a larger archive is explicitly
`bounded_window_checked`. The full journal is still checked for unresolved
modern delivery. There is no new historical exception or ACK. Jobs include all
attempts and paginate completely up to 500; artifact lists also paginate up to
500. Missing fields, shifting/duplicate pages or a required list beyond the cap
make the check incomplete, not empty/successful. Older history outside the
existing workflow window remains an operator limitation.

New producers expose a SHA-256 proof of the **actual frozen plan after filtering**
in a successful numbered step before the sender. A failed run is already settled
only when its own production receipts match that whole plan, and every result
is either fully accepted/persisted or proves failure before generation/handoff.
One Daily receipt is insufficient for Daily+Weekly+Monthly. All job attempts are
included; a rerun's pruned subset must also match known receipts. Old or contradictory
planning evidence stays explicitly missing/unresolved; inspect the retained
frozen plan and recovery packets, rather than ACKing or deleting the hold.

The 27 October audit resolved the one documented 16 October
Cybermed publication alarm without resending mail. Production run
[38874103951](https://github.com/Cyberlurch/NewsAgent2/actions/runs/38874103951)
ran a Daily+Weekly+September Monthly plan. Its job log shows the Daily sender
and per-cadence verification succeeded, while existing legacy guards explicitly
held Weekly and Monthly before SMTP. Only the Git State publication step failed
(`publication_newsletter_scope_mismatch`). The Daily SMTP receipt is now
accepted and persisted in the journal; a separately authorized Weekly was
accepted and persisted in run 38894702613. This does **not** establish a
September Monthly send: its historical hold remains intact.

Both readiness and pre-send recovery can close *only this exact historical
publication failure* when its original repository, branch, run ID, commit,
scheduled event, job-step conclusions, legacy hold policies and persisted
Daily/Weekly receipts match. Any mismatch, changed journal entry, other failed
run, partial/unknown SMTP result or missing evidence still blocks. The exception
is narrow; it does not turn old uncertainty into a delivery success or enable
resending. Retain the original run's metadata, reports and recovery artifacts
before their scheduled expiration, for independent audit. Newer runs have
full-plan proof markers and continue through the ordinary checks.

Readiness checks serialize without cancelling a running check when either
newsletter completes. GitHub can still coalesce pending triggers; this is not
an event-delivery guarantee. Each matrix job checks out `github.ref_name` and
records its actual checked commit in JSON, summary and log. A workflow-run
trigger's original producer SHA is not mislabelled as this later State view.
The immutable checkout and SHA explain a result even when a later producer
publishes new State. Readiness does not hold the production writer lock or
mutate that checkout.

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

Rollback is an ordinary code/workflow revert retaining State, generated records
and real receipts. Reverting this reliability fix restores one-shot GitHub reads
and the former Cyberlurch order; it does not revoke the partial-review policy.
Neither code revert
nor stable Message-ID undoes server acceptance; SMTP and Git are not a transaction.
