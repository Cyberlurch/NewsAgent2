# Cybermed PubMed ingestion

Cybermed Daily previously searched one publication-date page per query. A
retry failure could look like zero results, and the Monday-only 72-hour
lookback could miss longer interruptions. Daily now keeps bounded ingestion
progress independently of SMTP receipts. Cyberlurch retains its existing
publication-date search and selection. Aggregates still read saved digests.

## Search and status semantics

[PubMed Help](https://pubmed.ncbi.nlm.nih.gov/help/#create-date-crdt) defines
Create Date as the date the citation was first created and recommends it for
finding additions since a previous query. It requires `[crdt]`; it is not
the publication date. Daily uses an explicit, parenthesized range:

```text
(ICU[Title] OR sepsis[Title]) AND
("2026/10/11"[crdt] : "2026/10/11"[crdt])
```

The documented date search granularity is a calendar day. The frozen edition
reference from the calendar plan determines the last searchable UTC date;
neither ingestion nor retries change the logical edition. A day range cannot
prove an exact intraday creation timestamp: a delayed job can see additions
later on the same date. We do not invent individual indexing timestamps or
claim a point-in-time medical database snapshot at 05:40. Today's snapshot is
provisional and is searched again after the day closes, or when the reference
changes within that day. Existing publication metadata remains separate; absent publication dates stay unknown in the new ingestion path.
Captured older publications are marked in the Daily paper view and digest.

Query metadata has these statuses:

| Status | Meaning |
| --- | --- |
| `complete` | Valid zero/open-work-free result, with the older closed-day frontier caught up and the current day searched. This does not certify all historical citations or SMTP delivery. |
| `incomplete` | Work remains: candidates awaiting generation, a day/record/request/time budget, or a provisional current day. Its IDs or unresolved search day remain durable. |
| `failed` | Transport/HTTP, response/schema/XML, missing IDs, changing search snapshot, or unsupported API volume prevents trustworthy retrieval. Daily delivery is blocked. |

`count=0` is a success only after validating ESearch. Valid records without an
abstract remain metadata records; optional abstract loading returns no text
for those records, but raises a typed error for a failed fetch. Exceptions
expose categories, never raw responses, recipient/contact parameters, keys or
request URLs. Existing optional content fallback remains separate.

The ESearch count is the actual server count. ID counts also include previously
fetched identities retained when a refreshed result drops them. Fetched/parsed
counts describe the cumulative active window, including already handled IDs;
`fetched_this_run` counts new records. Pages and HTTP attempts are separate.

## Bounded snapshots and progress

The dedicated `CYBERMED_PUBMED_INGESTION_PATH`, normally
`state/cybermed_pubmed_ingestion.json`, contains versioned query rows, an active
day's ID snapshot, bounded fetched candidates, recent handled IDs and generation
receipts. No delivery history is derived from it. A SHA-256 fingerprint of the
trimmed query plus `crdt-v1` separates changed search policies. Old versions and
unknown additive fields survive; they are not silently assigned the new
query's progress.

A missing store begins three days before the frozen UTC date. This is a bounded
initialization, not a reconstruction of older history. `processed_through` is
null until a closed day has actually been handled. A corrupt/invalid existing
store fails without repair, rename, deletion or byte changes. Reads do not
create files; writes validate the preimage and candidate and replace atomically.

Work starts at the oldest unresolved day on every actual Daily run. At most
three empty/already handled days per query are traversed in one run; a day with
new candidates is retained until it is handled. A one-day overlap rechecks
recent closed coverage; recent handled IDs are retained for two frontier days.
This is ingestion overlap memory, **not** a durable paper delivery index.
The separate [paper-delivery projection](paper-delivery.md) now handles durable
Daily receipt-based deduplication and evidenced source notices/updates; ingestion
checkpoints do not establish delivery.

ESearch uses `usehistory=y`; subsequent pages refer to its runtime-only
WebEnv/query key. Only a complete, validated ID list is persisted. An interrupted
discovery is restarted, never resumed using an offset into a mutable search.
Changed counts, duplicated IDs or missing history fail closed. Known fetched
candidates remain protected. EFetch validates the returned unique IDs against
the requested batch, including a syntactically valid but empty response.

[ESearch documentation](https://www.ncbi.nlm.nih.gov/books/NBK25499/#chapter4.ESearch)
limits PubMed retrieval to the first 10,000 IDs. Day windows bound that problem:
more than 10,000 results in one day blocks Daily with `api_result_limit`, keeps
the unresolved day, and requires an operator-reviewed narrower query policy.
There is no silent prefix success or automatic query expansion. This environment
could read PubMed Help; the Books documentation endpoint returned an access
denial. No live ESearch/EFetch was used for development or verification.

| Setting | Default | Permitted range |
| --- | ---: | ---: |
| `CYBERMED_PUBMED_RETRIEVAL_SECONDS` | 120 | 1–120 |
| `CYBERMED_PUBMED_REQUESTS_MAX` | 60 | 1–60 |
| `CYBERMED_PUBMED_RECORDS_MAX` | 650 | 1–650 |
| `CYBERMED_PUBMED_PAGE_SIZE` | 500 | 1–10,000 |
| `CYBERMED_PUBMED_INITIAL_DAYS` | 3 | 1–7 |

Retries count as requests. Rate-limit waits, bounded backoff/Retry-After and
request timeouts consume the shared monotonic retrieval budget. No new request
or sleep begins beyond it, and a response arriving after the deadline is not
accepted. An in-flight synchronous HTTP operation is not forcibly killed;
socket timeout behavior is not a hard wall-clock completion guarantee.
When few requests remain, the ID page grows to the bounded 10,000 maximum so
a small budget does not repeatedly retrieve the same incomplete prefix.
Fetch batches stay within the existing per-channel limit (default 25) and the
global record cap. PubMed query order rotates between runs; other channel
slots retain their order. A successful run with a backlog clearly labels its
limited coverage. Actual source failures block sending.

Model/output budgets are unchanged. Candidate selection sees no more than the
existing per-channel cap. Capacity omissions remain open across restarts;
explicit editorial rejection or existing sent cooldown is distinguishable from
generation. Candidates are consumed only after final generation approval and
generated-store persistence. Processing receipts retain generated/rejected IDs,
edition ID and content version and explicitly set `delivery_claim=false`.
Neither receipt type is an inbox-delivery claim.

## Persistence and recovery

Validated ID snapshots and fetched metadata are atomically saved before model
work. They may survive a later generation/SMTP error without advancing a
processed/Sent/Daily-success marker. The closed-day frontier advances only when
all snapshot IDs have approved generated or terminal screening dispositions.
An editorial capacity omission does not close the day. Generation receipts
remain distinct from `accepted_by_smtp`, `partially_accepted` and
`outcome_unknown` in the unchanged delivery journal.

`none` and `test` use the central path registry and private scratch copies.
Digest backfill apply has no new permission to write ingestion state. Only the
Cybermed publication path includes this store, and the publisher validates its
schema and newsletter scope. A later cadence failure still prevents the normal
Git publication; no entire intermediate application state is always committed.

For production, `out/recovery/cybermed_pubmed_ingestion.json` is written before
each target write. It contains only this validated ingestion store, its initial
preimage hash and canonical content hash. It lives in the existing 30-day
delivery recovery artifact. The existing prior-failure recovery step imports
it with strict hashes and preimages, without providers or SMTP. Restored
artifact identities are recorded additively so importing an older artifact
again cannot roll back subsequent progress. A conflicting preimage stops for
operator review rather than replacing unknown state. Preview artifacts cannot
be promoted. The pre-SMTP generation approval manifest and its guards remain
unchanged; ingestion recovery cannot authorize sending.

Inspect the ingestion diagnostic artifact for each query's frontier/open work,
and the delivery journal separately for the edition's SMTP outcome. For a
failure after server acceptance, reconcile the existing receipt first; do not
rerun collection or resend blindly. For a saved approved pre-SMTP generation,
use the documented explicit replay in
[delivery recovery](delivery-recovery.md), with its existing approval and
hash checks. A Git push error remains a failed run with Git persistence metadata.

An abrupt crash before artifact upload can lose local work; the persisted
remote frontier then remains conservative and work can be rediscovered.
Missing recovery receipts, ambiguous SMTP outcomes or preimage conflicts
still require operator reconciliation. Git, ingestion files and SMTP do not
form a transaction; no exactly-once guarantee is made.

## Mocked catch-up example and rollback

An unresolved Thursday window contains IDs 1–5. Retrieval cap two fetches
1–2. Valid generation includes 1 and omits 2 because of capacity. Monday's
restart uses the stored snapshot, offers 2–3, then 4–5 on the next continuation;
it does not keep fetching IDs 1–2. A closed-day frontier advances only after all
five have explicit dispositions. A Wednesday/holiday interruption follows the
same rule, rather than relying on Monday's date. Today's records are checked
again after closure to catch additions after its provisional snapshot.

Revert this code/workflow commit to restore the prior collector behavior.
Keep the additive ingestion store and recovery artifacts for review; do not
delete, reset or rewrite digest/delivery history. The earlier collector will
ignore the new store. Re-enabling this implementation reuses its saved query
versions. Reverting removes the retrieval safeguards, so review before the next
productive run. No source migration, historical regeneration or real send is
part of this PR.
