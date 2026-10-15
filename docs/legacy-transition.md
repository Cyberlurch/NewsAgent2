# Reviewed transition from pre-journal failures

Merging this operational fix explicitly activates a **finite legacy transition**
for Cyberlurch/NewsAgent2. It does not reset State, recreate lost digests or assert
complete historical SMTP delivery. No additional repository variable is required.
The implementation is [legacy_transition.py](../src/newsagent2/legacy_transition.py).

## Reviewed failures

The operator supplied five Actions log archives. Their checked-out source commits
and execution paths were inspected offline. The module binds each exact run ID to
its newsletter, workflow, original commit and archive SHA-256. Raw logs, recipient
addresses and source texts are not committed.

| Run | Finding and disposition |
| --- | --- |
| Cyberlurch `33743120339`, `33752005796`, `33755797975` | Manual Monthly tests failed synthesis validation before SMTP. These are individually reconciled non-handoffs. |
| Cybermed `33186081165` | Two SMTP calls returned (one and nineteen recipients); subsequent Git push failed. At least one acceptance per call is established. Full acceptance of nineteen remains unknown because the legacy sender ignored partial refusal. Archive this uncertainty; never automatically resend. |
| Cybermed `33489323708` | Single-recipient SMTP call and SMTP context completed; Git push failed. SMTP acceptance is established for that call, not inbox delivery or restored State. Original content/marker intents remain unavailable. Never automatically resend. |

Only the five exact identities on `main` in this repository receive the exception.
Cybermed additionally requires a validated production plan dated **15 October 2026
or later**. Both 15 October sender steps were confirmed skipped before SMTP; this
is the boundary for new operations. Recovered structured receipts outrank the
legacy classification. Every other missing/expired artifact, mismatched identity,
new failure, partial/unknown receipt and Git failure retains normal fail-closed
handling. The existing explicit ACK variable remains available, but is unnecessary
for these five reviewed cases and must not be populated with unreviewed failures.

`out/<newsletter>_legacy_plan_metadata.json` retains the per-run findings and policy
hash alongside the existing safe metadata artifacts. No legacy success receipt,
recipient HMAC, content hash or marker is fabricated. The ordinary paper-delivery
projection continues to conservatively retain unknown legacy paper history.

## Held historical attempts

In production, a Cybermed period ending **before 15 October 2026** without a
structured journal receipt is held rather than regenerated or resent. This includes
the September Monthly catch-up. Existing valid generated stores are retained.
Previously recovered receipts still use ordinary acceptance/uncertainty/replay
rules; a hold never replaces a journal row. A previous preflight-only failure
without generated content, recovery data or any SMTP handoff can also be held
while its original failure row remains unchanged. Verified maintenance Apply still uses
its independent offline contract.

The **12–16 October Weekly only** is also held while its required Daily inputs are
missing. This prevents the known 14/15 October gap from stopping new Daily service.
After reviewed source-bound recovery supplies the required Daily inputs, normal
Weekly readiness and generation resume automatically under the same edition ID.
No empty historical edition is fabricated. Newer weeks, October Monthly and Yearly
retain their complete readiness/provenance requirements; this fix does not certify
them or downgrade their future failures.

A hold creates only an explicit **attempt result**, not a delivery-journal row:
`kind=reviewed_legacy_hold`, `status=intentionally_skipped`, exact edition and run
identity, policy hash, unknown historical SMTP outcome, no content version or
Message-ID, no generation, no SMTP and no persisted delivery markers. This says
that **this attempt was skipped**, not that the historical edition was unsent.
Actions emits a warning and job-summary entry for each held cadence; timing has no
generation/SMTP/state timestamps. The verifier checks every planned result and
accepts a hold only when this exact policy, edition, run and current inputs match.
A missing/forged result or a failed generated report still fails the job.

The rest of a successful multi-cadence job can publish only its ordinarily
validated State. There is no `always()` State commit. `none`/`test` remain scratch
modes and do not acquire production holds. Source/model budgets, independent
newsletters, recipient concealment, the shared writer lock and branch checkout
are unchanged.

## Operation and limits

On 16 October the expected outcome is Cyberlurch Daily/Weekly and Cybermed Daily
attempts, plus visible Cybermed Weekly/September-Monthly holds until their evidence
is resolved. These holds are **not SMTP successes**. Live source/model/SMTP access
still requires the operator's configured credentials; the offline PR checks do not
certify it. No productive dispatch, real send, provider/model test or reconstruction
was performed to validate this change.

To release historical periods, use separately reviewed recovery with original
receipts or source-bound maintenance candidates. Do not remove holds to force an
unknown historical resend. October Monthly can remain blocked by the unresolved
Daily/provenance gap; repair that chain before its November due date.

Rollback is a normal code revert preserving all State and receipts. It restores
the earlier global legacy-artifact block. Retain the reviewed log hashes and
operational metadata; never reset the journal or force-push State.
