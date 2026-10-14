# Cybermed FOAMed adapters and source diagnostics

FOAMed provides commentary and learning material. A linked PubMed paper remains
an independent primary source, with its own paper delivery receipts. Collection,
selection, generation, and SMTP acceptance are different events. These changes
do not alter Cyberlurch sources or introduce model calls.

## Endpoints and supported roles

The endpoint evidence below is from the supplied **13 October 2026 audit**.
No live feed probe was performed for this change; current availability is unknown.

| Source | Configuration | Basis / limits |
| --- | --- | --- |
| PulmCCM | `https://www.pulmccm.org/feed` (no trailing slash) | Audit found RSS at this replacement; old endpoint returned HTML. |
| The Resus Room | `https://theresusroom.libsyn.com/rss` | Audit found the redirecting podcast feed; old website feed was stale. |
| OpenAnesthesia | `reference` | Empty feed despite active website; no verified dated extraction fixture available. |
| Anesthesiology News | `reference` | HTTP 200 with no entries; requires a reviewed dated adapter before automatic collection. |
| Critical Care Reviews | `ccr_dated_articles_v1` | Conservative per-article extraction described below. |
| ALiEM | `audit_only` | No automatic collection or new failure count. Explicit operator probe available. |
| LITFL / CanadiEM | Existing configured strategies and stored bans | No reset or blanket reactivation. |

Source count is not an operational guarantee. Configuration tests validate unique
names, domains, priorities, strategies, and important profiles, rather than an
exact count of thirty. No new guideline or journal sources are enabled.

## Dated CCR commentary, not a landingpage study

The CCR adapter accepts only non-nested `article` or `itemprop="articleBody"`
blocks containing a heading, an explicit ISO calendar date in `time[datetime]`,
and an HTTPS PubMed or DOI primary link. Conflicting dates, multiple PMIDs or
multiple DOIs in one block, future dates, absent references, and unsupported
layouts are withheld. At most forty blocks are inspected; `entries_truncated`
marks a longer listing. It never substitutes a page-wide date or fallback article.
The fixtures are small synthetic examples, **not evidence that today's live
CCR layout matches**. An operator probe must establish that before relying on it.
Unsupported structure and rejected entry counts are visible diagnostics.

Each item has `source_role: commentary`, additive `primary_references`, and
date precision metadata. Midnight in serialized `published_at` represents a
calendar date, not an observed publication time. Its inclusion window is checked
by calendar day; date-only entries can therefore extend beyond an exact 24-hour
instant window. The identity hashes source name, source calendar day, and the
primary identifier (PMID preferred when both PMID and DOI appear). Retrieval time
and prose are excluded. This supports stable repeated retrieval and multiple
papers on one listing. A changed source day or newly supplied preferred ID can
change identity; uncertain layouts and historical aliases require review.

The original source listing remains the reader link; `legacy_listing_id` records
its old URL identity; `legacy_listing_ids` retains observed configured/redirected/
canonical page aliases. If an observed alias is already marked processed in the active
Cybermed state, normal production collection conservatively holds the new
entries under the existing processed guard. No old ID is rewritten or deleted;
this can suppress genuinely new entries until the legacy guard expires or an
operator reviews the ambiguity. No automatic state migration/republication is
performed. Preview paths retain their existing processing-state bypass.

Daily and Monthly serializers preserve the new provenance; Weekly uses stored
Daily rows, and Yearly uses stored Monthly rows. For these new commentary IDs
only, aggregate deduplication uses the commentary identity before the shared
landing URL. Legacy rows retain their prior matching rules. Linked PMID/DOI
values do not enter the PubMed delivery ledger or collapse commentary into an
original paper. No original paper is refetched to validate a supplied link.

## Health dimensions

Per-source diagnostics distinguish:

- Attempted versus skipped (disabled, audit-only, reference, existing ban).
- HTTP status, final feed/homepage URL, redirect count and reported Content-Type.
- Recognized RSS/Atom, valid empty feed, HTML instead of feed, or parse failure.
- Dated entries and the newest source date, separate from access success.
- Freshness only with an explicit positive `expected_interval_days`; otherwise
  `unknown`, including when dates are missing. This is not proof of inactivity.
- Article fetch attempts/success/failure and text lengths, separate from feed access.
- `selected_items`, populated after selection; the collector alone reports `null`.

Valid XML RSS/Atom feeds are accepted even with an incorrect MIME type. An empty
valid feed has `feed_parse_result: empty_feed`; `ok_rss` means parsing/access
succeeded, not that new material was selected or sent. A successful HTML fetch
with no dated entries is neutral `no_dated_entries`. 403/502 are access outcomes,
not editorial inactivity. Existing compatibility summary fields remain available.

Skipped sources, explicit probe results, and budget-truncated collection do not
increment or clear Health failures or bans. Real failures retain the existing
403/404 threshold rules. Adapter `health_epoch` changes are diagnosed without
resetting history or `disabled_until_utc`; `health_epoch_reset_count` stays zero.
The existing audit-only HTML override is marked `probe_only` for Health updates.
No additional WordPress/sitemap audit is triggered merely because a source is
disabled. Existing expressly enabled audit options remain available.

Health JSON must retain object-shaped source entries and valid known fields.
Missing new stores are inspected without creation. Corrupt/invalid existing
stores raise a categorized store error and remain byte-identical. Unknown legacy
metadata is retained. The existing validated atomic writer and injected paths
remain authoritative; `none` and `test` mutate scratch state only.

## Operator inspection and bounded probe

Run from the checkout with its installed dependencies and `PYTHONPATH=src`.
Every command requires explicit source names. The default is **local inspection**:

```bash
PYTHONPATH=src python -m newsagent2.foamed_probe --source LITFL --source CanadiEM --source ALiEM
```

`--state` optionally supplies a separate copied state file; otherwise the central
`STATE_PATH` / `NEWSAGENT_STATE_DIR` policy selects the path. Inspection does not
create a missing directory or modify any store. Invalid state stops before HTTP.
Output contains a limited configuration/Health view, never configured headers.

After PR review, the operator may explicitly request HTTP diagnostics:

```bash
PYTHONPATH=src python -m newsagent2.foamed_probe --source LITFL --probe --seconds 30 --requests 8 --redirects 3
```

This command was **not executed against a live source** in development. It needs
no SMTP/model secrets, never runs the newsletter, sends nothing, and never writes
state or commits. Source headers are removed. It probes selected audit-only or
disabled strategies without lifting their stored bans. Reference-role sources
remain reference-only; no fabricated adapter is attempted.

Budgets are shared across the invocation, including redirect hops. No retries,
no article enhancement fetches, at most one candidate article page per selected
source, and no heavy audit passes. Defaults: 30 seconds, 8 HTTP requests, 3
redirects. Maximums: 120 seconds, 30 requests, 5 redirects. The hard wall deadline
uses POSIX `SIGALRM` on the main thread, including a stalled/drip-fed response.
An unavailable deadline mechanism fails before HTTP; an already active interval
timer is rejected. The original signal handler is restored afterward.

Exit 0: completed local inspection/probe, which does **not** imply source health
or article success; inspect the individual results. Exit 1: invalid input/store.
Exit 2: request/redirect/time budget ended, so results are incomplete. Later
budget-truncated sources must not be interpreted as checked. Probe output is
limited diagnostics on stdout, with no raw HTML, recipient data or credentials.

Use the result to decide a **separate reviewed maintenance change**. A successful
probe does not grant permission to reset bans, regenerate history or resend an
edition. There is no automatic reactivation or new dispatch workflow.

## Verification and rollback

Offline tests mock HTTP/SMTP/providers and cover parsing, redirects/budgets,
invalid stores, source-state preservation, commentary identity/provenance, and
all centrally registered production paths for both newsletters in none/test.
Full-suite isolation inventories protected files and catches attempted access.
No clinical completeness, live reachability or exact source coverage is claimed.

Rollback is a reviewed compatibility deployment: preserve existing stores and
new additive metadata. The old generic CCR adapter and URL-first aggregate rules
cannot safely interpret these new identities and may collapse contributions or
reintroduce landingpage summaries. Disable CCR collection conservatively before
rolling that code back; do not restore generic extraction automatically. Never
reset state, remove receipts or use a force push. Endpoint availability and the
editorial rules scheduled for 05d remain separate follow-ups.
