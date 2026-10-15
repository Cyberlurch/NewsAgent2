# Cyberlurch Daily discovery and delivery memory

Daily discovers inexpensive metadata before acquiring content. Configured topic
buckets describe a **channel's configured role**, not a verified video's subject.
Existing bucket names/weights, personal sources, EN/DE/SV and medical editorial
policy are retained. No model classifies candidates for this step.

## Selection and cost boundaries

The deterministic selector first covers unseen configured topics, then balances
channel counts. Within a balance tier it uses configured priority, newest known
publication and stable ID. Missing topics are `unknown`. The initial channel cap
is `ceil(pool_limit / 3)`; it relaxes only if no remaining channel is below that
cap, so sparse sources do not leave slots needlessly empty. Reasons are
`topic_coverage`, `channel_balance`, `channel_cap_relaxed`. The same rule selects
the final Daily items, preventing a later priority sort from undoing coverage.

Limits remain at most **14 content acquisitions, 12 report items, 2 Deep Dives**;
lower configured limits apply. There is no extra content fallback pool. Empty
content attempts also count. Existing semantic-stage, managed-transcript,
chunking, fallback and model budgets remain in effect. Metadata-only candidates
continue to produce links/status without semantic model calls.

Synthetic comparison (not observed live coverage):

| Input | Previous selection | New selection | Content limit |
|---|---|---|---|
| 40 newer priority-channel news videos + one older small theological source | 14 priority videos | 13 priority + small source, with explicit cap relaxation | 14 both |
| Same pool, final report | 12 priority videos | 11 priority + small source | 12 both |
| Three channels with 100 API metadata IDs each, limit 150 | 300 requested IDs / 6 requests | 150 requested IDs / 3 requests | unchanged |

The API budget is one object per application invocation, across all YouTube
channels (not reset per channel). Each channel reserves `ceil(remaining IDs /
remaining channels)` before its listing. Unused shares pass to later channels;
there is no second discovery pass to reclaim shares from already visited
channels. This intentionally may underuse the budget when later channels are
empty. IDs are deduplicated and charged **before** each request, even when the
API fails or returns no item. Batches contain at most 50 IDs. There is no retry
of an already requested ID in that invocation, nor expensive enrichment as a
replacement. Configuration retains its default of 150; zero disables API
enrichment. Existing explicit operator overrides are not increased by this PR.

Count-only diagnostics distinguish requested IDs, successful enrichment,
requests, budget skips, eligibility reasons, topic count, largest selected
channel count and cap relaxations. They do not expose candidate text or
unbounded channel/ID lists. A short discovery counter record is also logged on
scheduled runs; existing manual diagnostic artifacts remain available.

## Candidate state is not delivery state

`processed_items.json` has an optional, strictly validated
`cyberlurch_candidates: {version: 1, records: {...}}` section. Legacy stores need
no migration. No new production path, publication permission or maintenance
exception is introduced. Existing central `STATE_PATH` injection and
`none`/`test` scratch routing apply, including Health and caches.

| Event | Meaning |
|---|---|
| `screened` | Bounded metadata retained; no content acquisition or SMTP claim |
| eligible / deferred | Computed from publication TTL, attempts and delivery holds; unselected candidates can be reconsidered |
| `selected` | Content attempt recorded before acquisition, including crashes/empty results |
| `content_processed` | Nonempty content returned; not a summary-validation or SMTP claim |
| SMTP receipt | Existing authoritative edition/content-version journal status; acceptance is not inbox delivery |

At most 500 metadata records are retained, newest publication first on overflow.
Titles/descriptions/URLs/topics are bounded; no transcript or new full-text
archive is stored. News expires 72 hours after publication. A channel can opt
into a 14-day TTL using `"candidate_policy": "evergreen"` in its channel config.
Default remains news; no source is automatically opted in, and titles or
theological keywords do not extend TTL. Theological channels may contain timely
news too. Publication dates that are missing/future are not eligible. Rediscovery
does not refresh TTL or attempt history.

Each retained candidate gets at most 3 content attempts, at most once per UTC
edition day. Attempts include failed, empty and interrupted acquisition; a
same-day run therefore may defer a candidate after a pre-SMTP failure. TTLs and
bounded storage prioritize safety/cost over exhaustive coverage. Capacity
eviction can forget attempt counts if an item is later rediscovered, but never
extends its publication-bound TTL. Expired/evicted/attempt-limited candidates
are not claimed delivered. An unselected item can survive beyond the next
discovery window, provided its TTL and capacity allow it.

Only final Daily report IDs enter new delivery finalization intents. Empty
reports consume no candidate IDs. Digests may contain validated generated items
that were not transmitted; generation is not acceptance. Existing legacy
`processed` entries remain conservative suppression evidence without inventing
historical SMTP success. Replaying an older broad Daily intent now marks only
its overview/deep-dive IDs, preserving the original sealed intent itself.

Fresh collection derives cross-edition/cadence holds from valid production
Cyberlurch journal receipts/intents. Known SMTP acceptance, partial acceptance,
unknown outcomes and saved ready reports hold their YouTube IDs. This also
works if SMTP acceptance preceded a process-marker persistence failure. Older
broad intents are conservatively held; they are not rewritten. Saved aggregate
reading remains independent: this guard does not remove the intentional
Daily→Weekly→Monthly→Yearly retrospective references. Cybermed's paper ledger
is unchanged. No recipient index or plaintext addresses are added.

## Persistence and recovery

The candidate section is checkpointed independently of SMTP, before content
attempts and after returned content. Existing strict atomic state saving leaves
corrupt/invalid JSON unchanged and fails categorically. This is bounded ingestion
memory, not permission to commit arbitrary intermediate state on failure.

Production additionally retains `recovery/cyberlurch_candidates.json`: only the
validated candidate section, its original section preimage hash, and output
hash. Existing failure-artifact handling imports this allowlisted file for
Cyberlurch only and restores the section with a matching preimage. Other
process state is retained. Equal output is an idempotent no-op; conflicting
progress stops as `recovery_preimage_conflict`, requiring operator review.
Up to 4096 hashes of previously saved candidate-record checkpoints are retained
in this section. An already published/imported checkpoint is a no-op even
after later progress; the hash list is not a delivery ledger. If its bounded
history has evicted an old hash, conflicting recovery still stops for review.
The artifact travels with the existing journal/generated deltas and survives a
second recovery export. It contains public source metadata, no recipients.

Generation/SMTP/push failures remain visible. Restore failed-run artifacts
through the existing workflow recovery path, then inspect the journal. Known
acceptance can finalize markers without SMTP. Saved ready content uses the
existing explicit pre-SMTP replay command and sealed proof; partial/unknown
SMTP outcomes require operator reconciliation. A new edition is not a license
to resend. SMTP and Git still are not a shared transaction; no exactly-once
guarantee is made. Missing/expired recovery artifacts remain a blocker under
the existing workflow policy.

Rollback: revert code on a reviewed branch and retain journals/receipts and
saved state. Do not reset process markers/caches or resend old editions. Older
code tolerates the additive section but does not apply its eligibility rules;
rolling back also restores the old broad processed-marker behavior and API
budget defect. Review that operational tradeoff before using it.

## Remaining work

Step 06b addresses canonical channel-ID verification, honest empty/error/source
Health, description/full/partial transcript grounding, late transcript facts
and attribution, plus a targeted default-read-only source probe. No live source
health or exhaustive/fair coverage is certified here. Historical coverage and
aggregate recovery remain step 07; README reconciliation is step 08, last.
