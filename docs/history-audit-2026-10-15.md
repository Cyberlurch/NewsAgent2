# Saved-history inventory, 15 October 2026

Read-only audit of State from `a8bde5a0ca40a8ee1f0779c49ff3a25b9413180c`
(merged #234), interval **2025-12-01–2026-10-15**, as-of
**2026-10-15T10:36:07Z**. Both independent audits returned **1** (unknown coverage),
not a clean bill of health. No corrupt/invalid-input or duplicate findings occurred
in this inventory. No E-mails, network calls, models, Apply, workflow dispatches,
artifact downloads or history reconstruction were performed.

Reproduce with the [existing audit CLI](history-coverage.md#run-an-audit), replacing
the example dates with this interval and selecting each newsletter separately.
Use the actual source checkout commit; the underlying State bytes in this PR are
unchanged. JSON and Markdown outputs contain the input hashes and per-record
store-key/JSON pointers. Do not interpret missing policy/receipts as failed sends.

| Input | SHA-256 |
|---|---|
| `state/cybermed_daily_digests.json` | `5b298aaa63dee9a2395b4372f61a41c6e4f4fc850ec165fd4fd3974ffd4b6ef9` |
| `state/cybermed_weekly_digests.json` | `0ece1b186aa799ea8cbda0222afd4beadbf9e771a5dcc5796214ad016269af40` |
| `state/cyberlurch_digests.json` | `72258dcffa2765795d93a68e6f908e75fe688c930a119ad7bed86618c428a5af` |
| `state/rollups.json` | `d6ee970afd849da3929d552cb4722483bf615b3d113e00c25a27ad9a54f65906` |

The configured Delivery journal is absent in this checkout (`missing_new_store`),
so historical SMTP acceptance and delivery-state finalization remain unknown.
Remote runner state/artifacts were not inspected. This does not establish that
those sources never existed. All **14** original State/data files were hashed
before and after development tests and the audit, including detection of new
protected files; their bytes and file inventory were unchanged.

## Cybermed: observed September chain

There are **101 Daily records**, run dates 25 May–13 October 2026, **10 Weeklys** and
**10 Monthlys**, December 2025–September 2026. A stored valid Daily covers its record
presence, not the completeness of its content/ingestion or an entire calendar.
All 20 aggregate rows have **unknown** historical calendar coverage. Missing
operating-start/policy evidence prevents retroactive promises about earlier days.

`cybermed_daily_2026-09-01` / run date `2026-09-01` is absent from the Daily store.
`state/cybermed_weekly_digests.json#/digests/4/source_daily_digest_ids` is exactly:

- `cybermed_daily_2026-08-31`
- `cybermed_daily_2026-09-02`
- `cybermed_daily_2026-09-03`
- `cybermed_daily_2026-09-04`

This is Weekly `cybermed_weekly_2026-08-31_2026-09-04`, generated
`2026-09-04T08:13:51+00:00`. Its source dates also omit 1 September. It has no
explicit historical calendar policy. A labelled weekday comparison exposes a
four-of-five observation; it is not proof of a missed required send.

The dependency on September Monthly is **explicit**, not inferred by adjacent dates:
`state/rollups.json#/reports/cybermed/9/source_weekly_digest_ids` references that
Weekly and the four Weeklys starting 7, 14, 21 and 28 September. The Monthly was
generated `2026-10-02T10:04:03+00:00`. Its traversed September Daily basis starts on
2 September and contains no 1 September edition. Coverage remains **unknown** under
the unestablished historical policy, rather than being upgraded to complete merely
because all five Weekly periods exist. With an evidenced policy requiring
1 September, the same missing/unreferenced Daily makes a new final aggregate
**incomplete** and blocks it. Offline tests cover both interpretations.

No saved Yearly edition is established by this inventory: the Yearly renderer uses
stored Monthlys, and there is no Yearly receipt in the inspected inputs. A future
2026 Yearly would inherit September's uncertainty; an already existing Yearly
publication must not be declared affected without its own provenance/receipt.

## Cyberlurch: evidence is not an issue ledger

The store has **4321 video records**, not Daily editions. Their publication dates
cannot establish newsletter issue dates or missed sends. The ten Monthlys span
December 2025–September 2026. Classification from actual persisted substance is
**four semantic_v2, one enriched_legacy and five thin_legacy**. All ten have unknown
historical edition coverage; substantive classifications and available text basis
remain separate. Thin records are readable archive metadata, not invented factual
or trend evidence. No retrospective ledger or source text was fabricated.

For both newsletters, the current-calendar comparison at the audit cutoff has
January–September as already due, October open and November/December future.
The supplied stores contain those nine 2026 Monthly slots. Their presence proves
neither transitive completeness nor a final complete year. Unknown historical
applicability of the current close schedule is disclosed.

## Targeted recovery assessment for 07b

| Target | Evidence now available | Required next assessment |
|---|---|---|
| Cybermed Daily `2026-09-01` | No edition in the inspected store; no bound Delivery receipt | Identify an actual original report/digest artifact or preserved original source. If none exists, document an unrecoverable gap. No current-source replacement or invented original timestamp. |
| Weekly `2026-08-31_2026-09-04` | Existing valid four-Daily snapshot with explicit IDs | Preserve it. First establish policy and source availability; any separate correction needs source and target hashes, backup, a narrowly reviewed diff and idempotence. Existing insert-only Weekly backfill is not permission to overwrite it. |
| Cybermed Monthly `2026-09` | Explicit reference to that Weekly; existing valid Monthly | Any separately authorized repair must follow this actual dependency, with its own input/target hash and backup. Do not regenerate other Monthlys globally. |
| Older/thin or unbound Legacy aggregates | Stored content/metadata, insufficient edition or policy evidence | Inventory original sources/receipts only where identified. Thin or unknown is not itself authorization to replace history. |
| Yearly publication | No established saved publication/receipt here | Require actual publication provenance before proposing correction or replay. |

Artifact availability is **unestablished**, not confirmed recoverable. Current
report retention is 14 days, recovery/timing 30 days; expired artifacts cannot be
assumed available. Reviewed Daily/Weekly backfill and sealed recovery paths exist,
but source/target-hash and backup hardening belongs to 07b. No Apply was added,
no valid history was overwritten and no SMTP success was inferred.
