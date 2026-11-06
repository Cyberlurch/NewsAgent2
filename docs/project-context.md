# NewsAgent2: project context and operating policy

Updated 6 November 2026. Repository evidence and the linked incident audits
take precedence over earlier conversational assumptions.

NewsAgent2 produces Cyberlurch (technology/AI) and Cybermed (medical research
and educational sources). Daily editions feed saved digests used by Weekly,
Monthly and Yearly reviews. Calendar planning, collection, editorial generation,
SMTP handoff and Git publication are separate stages with separate evidence.

The project grew from an automated news collector/newsletter into two products
with multiple cadences. During October 2026 the focus shifted to reliable
delivery, durable source history, calendar handling and duplicate prevention.
PRs #253 and #254 addressed reviewed historical failures and delayed GitHub
scheduling. PR #255 (merged 6 November) added bounded retries for invalid
PubMed search responses. It did not change the strict all-source failure policy
or reconcile the failed 5 November receipt.

## Current user preference

Useful partial editions are preferable to failed runs when individual sources
are unavailable. Missing articles and historical source dates are acceptable;
do not manufacture missing content or automatically resend old editions.
Source outages should produce visible warnings, not disable the newsletter.
When no usable articles remain, the normal configured empty-edition route may
send an honest availability notice. It must not claim that an unavailable
source contained no news. Daily digests label source capture as degraded;
edition-date coverage and article-source coverage are different facts.

Integrity and delivery safety remain strict: unknown or partial SMTP outcomes,
unpersisted acceptance, corrupt state, bad configuration and missing recovery
evidence need reconciliation. A green warning-level readiness result is not
proof of historical delivery or inbox arrival.

## 5–6 November incident

Cybermed run 42029696373 failed on 5 November before generation and SMTP.
Its authenticated GitHub artifact records `failed`, `not_handed_off`, no
accepted/rejected/unknown recipients and no approved generated content.
PubMed diagnostics show 8 invalid-count responses, 5 server errors, 1 timeout
and 12 queries left without retrieval time. The following readiness failures,
including 42204344474 on 6 November, reported that same missing edition and
the failure receipt still outside main. They are not evidence of repeated
SMTP failures or a Git push failure after sending.

Cyberlurch on 5 November and Cybermed on 4 November have finalized SMTP
acceptance receipts. See [the detailed audit](audit-2026-11-05.md).

The repair separates recoverable source failures from delivery/integrity
failures, makes publication gaps warnings, and automatically imports guarded
recovery evidence without any sending credentials after failed producers.
No run-ID exception, ACK, history reset, fabricated success or blanket
`continue-on-error` is used. Recovery deployment on main also reconciles both
newsletters serially using the existing shared writer lock/concurrency group.

No historical backfill is necessary for this incident: the failed run has no
generated Daily content to restore, and Weekly can use its actual partial
sources. PubMed's unresolved query windows remain available for bounded future
collection. The 14/15 October gaps likewise do not justify fabricated digests.

## Validation and limitations

Tests cover partial and total source outages, honest notices, unchanged failed
query frontiers, preserved SMTP safety, warning-only stale publication, and
safe receipt recovery. GitHub Linux CI is authoritative; this Mac has known
temporary-directory cleanup failures in seven unrelated tests.

Monitor the no-send recovery and ensuing readiness runs after deployment.
Historical red runs remain an audit trail; deleting them is not a repair.
External services can remain unavailable and truly unsafe outcomes can still
fail. Neither retrying nor this policy promises exactly-once inbox delivery.
