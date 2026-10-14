# Cybermed paper delivery and source updates

Cybermed Daily no longer treats the 48-hour cooldown or 20-day processed-state
retention as its paper delivery memory. Delivery decisions are a read-only
projection of the existing journal's production **Daily** receipts. There is
no second success index or new authoritative delivery store. SMTP acceptance
still means server acceptance, not inbox delivery.

## Identity, version and decisions

Paper metadata v1 is captured from collector fields before model enrichment.
It binds PMID, normalized DOI, record role, source-field hashes, reported-primary
result comparison hashes and official CommentsCorrections RefType/PMID links.
It is added to the actually rendered overview/deep-dive finalization rows;
screened-only rows do not carry this payload. Renderer diagnostics enumerate
the papers actually rendered, rather than inferring inclusion from all selected
candidates. Daily digests retain those rendered papers and source notices.

The source version hashes normalized title, abstract, publication types and
relations. It excludes summaries, model choices, query/channel order, fetch and
generation clocks, DOI formatting and publication-date display. Whitespace,
HTML formatting and equivalent decimal spelling are normalized; clinically
sensitive case is preserved. This fingerprint is a practical record comparison,
not proof of semantic equivalence or medical correctness.

`PMID:source_version` identifies the original or update case independently of
the edition's report-wide `content_version` and Message-ID. A DOI alias between
different PMIDs requires the same normalized DOI and identical source version.
Conflicting aliases are held. Notices are separate documents even when they
share the original's DOI. No title similarity matching is used.

| Decision | Meaning |
| --- | --- |
| `include` | New identifiable paper, a source notice, or conservatively identified reported-primary-result/source-status change. Not a delivery claim. |
| `duplicate` | This source version has a known complete production Daily SMTP receipt. It is suppressed before optional model/content work, without a TTL. |
| `review` | Delivery, version, audience, aliases or source-change interpretation remains uncertain. No automatic send or expensive repeated processing. |

Only actually shown papers in `overview_items`/`detail_items` contribute known
acceptance. `items_all_new` does not: it contains screening dispositions too.
Generated, skipped, screened and completely rejected reports are not accepted
papers. A saved generated/failed report remains protected by the existing
explicit recovery policy, even when another Daily edition has started.
Partial/unknown outcomes hold the affected paper across editions. An uncertain
receipt with no reliable paper mapping conservatively holds any new paper until
the original receipt is reconciled. Weekly/Monthly/Yearly receipts do not lock
a Daily; aggregates may reuse saved papers. Newsletter namespaces remain separate.

For a productive enabled send, matching accepted versions must also have the
same existing HMAC audience and recipient key ID. A changed audience/key is a
review case; this mechanism does not automatically send to a changed subset or
new audience. No raw addresses are added to paper metadata or diagnostics.

All accepted versions remain deduplicated, including an older version found
again after a newer acceptance. Comparison with the latest accepted source uses
the edition period, not a later receipt-repair timestamp. Old receipt recovery
therefore cannot roll the selected source history backwards.

## What qualifies as an update

Numerically changed reported **primary** results qualify only when both versions
have usable structured results with an explicit primary endpoint, non-truncated
facts, unchanged methods, endpoint, title and publication-type context. The
existing canonical fact builder supplies this distinction; a positive secondary
result cannot replace an unchanged null primary endpoint. Changed methods,
population/comparator context or ambiguous prose stays review-required.
This is conservative record-change classification, not medical revalidation.
Regular result updates use the existing bounded generation/fact-validation path
and are visibly marked in the Daily.

New official RetractionIn/ErratumIn/ExpressionOfConcernIn/republication relations
can produce a source-status update. RetractionOf, ErratumFor and
ExpressionOfConcernFor notices keep their own PMID and primary reference.
An original labelled `Retracted Publication` is reported as a source-status
record, not presented as an ordinary new intervention study. Contradictory
notice roles are held for review.

Notices use a source-only report section; no original-paper refetch, paid
summary or independent retraction adjudication is performed. At most three
notices are shown, within the existing PubMed overview capacity, with reserved
slots and priority for retraction/EoC. Unrendered notices remain queued. Original
digests/aggregates are not overwritten. Source relations and version metadata
remain in the saved Daily chain.

Create-date capture from #229 is unchanged. It does **not** reliably rediscover
every later change to an old PubMed record. This PR classifies evidence actually
captured/available; it adds no lr search, global revalidation, live aggregate
queries or provider budget. A future bounded change-discovery policy remains
an open limitation. Correction/retraction surveillance is not claimed complete.

## Legacy and held work

An accepted legacy receipt without metadata can establish a paper ID's SMTP
acceptance, but not the source version. A new hash alone does not license resend;
the case is held as `accepted_paper_version_unknown`. Stored generation and old
Sent markers without a trustworthy receipt are `legacy_delivery_unknown`, not
success and not guaranteed non-delivery.

On productive runs, existing unproven IDs are retained in the additive journal
`paper_legacy_unknown` registry before processed-state pruning can erase them.
This registry contains **only uncertainty**, no acceptance or fabricated source
version. Existing fields remain intact; none/test does not add productive
registry data. Recovery merges this uncertainty conservatively. No production
historical migration or mass reconciliation was executed for this PR.

Held candidates move into a `review_required` compartment of their existing
query row, retaining source version, raw collector metadata, captured day,
reason and edition references. Their IDs are `parked`, not falsely generated or
screened out. Other candidate slots and later search days can continue. The
retrieval frontier is separate from `processed_through`, which stops before the
oldest unresolved review day. Review work has no automatic expiry.

After authoritative full acceptance is recovered, a held identical version can
be consumed as a known duplicate without SMTP/provider calls. Merely becoming
`include`, losing a failure marker or changing dates does not release held work
for sending. Ambiguous source/legacy decisions need a targeted reviewed
resolution with source/SMTP evidence; restoring trusted original receipts is
preferred. There is no generic override or guessed historical success.

The default inspection command is read-only and needs no secrets/providers:

```bash
PYTHONPATH=src python -m newsagent2.paper_delivery
```

It shows PMID, captured source version/day, stored hold category and originating
editions. Its separate receipt projection does not verify the current audience
and does not authorize release. Paths use the existing injection registry;
missing stores are not created. For delivery uncertainty use the existing
[delivery recovery](delivery-recovery.md) commands after operator reconciliation.
For source/legacy ambiguity, prepare a narrowly reviewed correction/resolution
with expected preimages rather than editing unknown history automatically.

## Persistence, integrity and examples

The paper payload participates in #226's existing finalization hash and HMAC
seal. New metadata-bearing sealed receipts also validate that finalization hash
when read; structurally plausible tampering fails closed. Old receipt formats
remain readable. No new metadata is injected into already approved old Intents.
Full SMTP acceptance itself is the authoritative durable projection: a later
marker persistence or Git-push failure cannot relicense that paper. Original
receipt finalization remains idempotent and requires no repeat send or provider.

Both journal and ingestion readers are strict/non-mutating; invalid JSON,
invalid version metadata or malformed held work stops with a category and
leaves existing bytes intact. Writes remain validated and atomic. Preview paths,
maintenance grants, writer lock, checkout ref, budgets and model pin are unchanged.
No entire intermediate state is always committed. Git/SMTP/files remain separate
systems, with no exactly-once guarantee or automatic partial-recipient resend.

Mocked examples covered by offline tests:

- PMID 1/source V1 accepted on October 1: same version after 3, 21 or 365 days
  remains a duplicate, even without retained processed markers.
- Yesterday's partial/unknown receipt for PMID 1: today's new edition holds it;
  accepted recipients receive no automatic repeat. Other safely mapped papers
  can proceed; unmapped uncertainty requires broader review.
- Same endpoint/methods, source primary figures change from 18% vs 20%, P=.37
  to 8% vs 20%, P=.01: new versioned source-update case, validated by the existing
  facts path. A secondary-only change remains review-required.
- PMID 2/RetractionOf PMID 1: independent source notice, literal source links,
  no original-paper model/refetch. Once accepted, the same notice version is
  also deduplicated.

Rollback: revert this code commit while retaining journal/ingestion data and
recovery artifacts for review. Do not delete or rewrite histories. The previous
code does not understand new metadata-bearing receipt rows; resuming it on such
data needs a reviewed compatibility deployment, not a blind downgrade or state
reset. FOAMed/Health and further editorial rules remain subsequent parts of
task 05; README reconciliation remains task 08.
