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


## 07b bounded source follow-up

Rechecked on 15 October 2026 (11:52 UTC) from main
`3e75ce0e8122af33765f39638cce17a8af9dfb65`. Only the identified 1 September
Daily and the previously established Weekly/Monthly links were in scope. The
following five Daily-store versions changed during 30 August–5 September;
strictly reading their JSON found **zero** target ID/run-date matches in each.

| Git commit | Daily-store SHA-256 |
|---|---|
| `daf5a5acb60d18f75bb1e942fe668051e9d58634` | `127c060189fd463ec646f4de5c2145ec2b9c4352c1c7220bac69e0d3c0ebbf46` |
| `8d8c57655310dac3eca5ab21b51c5fe337d36500` | `d4a15d22e35a3d81e830e038a5d131f0dcbb8b704562434dc416cba87384249a` |
| `b1754daad945eecb5a1ae1ca53e6c47923026b01` | `c2674adb485c00685f90494f85f062dba9a5051eebf743f31f17795e5c33148c` |
| `ae10df6b14abf9f388e9c8a7ee394bc8c163f7e7` | `e43f7814446e05500dd4637d754d1f5b59aef8f9cd6b854f7d07d38df35e7e60` |
| `5f7b5afa5f62209033dd98f07acbe20b5de434b7` | `fe2184446029e6cc19499f7f3f9c1781b3acbfa081b15b3cb3f80d4b250708a1` |

The GitHub artifact API returned total_count=9 (one page, no hidden remaining
pages). All listed artifacts were created 9–15 October, none in September.
Only metadata was inspected: no archive download or source/provider request.

| Artifact ID | Name | Created (UTC) |
|---|---|---|
| `11959340886` | `cybermed-run-metadata-38706781025` | `2026-10-15T08:36:09Z` |
| `11958837259` | `cybermed-reports-38706781025` | `2026-10-15T08:36:08Z` |
| `11957613861` | `cyberlurch-run-metadata-38705847136` | `2026-10-15T08:28:28Z` |
| `11957593918` | `cyberlurch-reports-38705847136` | `2026-10-15T08:28:27Z` |
| `11881168084` | `cyberlurch-reports-38538599641` | `2026-10-14T08:55:59Z` |
| `11881073561` | `cybermed-reports-38539070053` | `2026-10-14T08:59:51Z` |
| `11806483287` | `cybermed-reports-38387351992` | `2026-10-13T11:03:16Z` |
| `11742409260` | `cybermed-reports-38241625348` | `2026-10-12T11:18:50Z` |
| `11612095370` | `cybermed-reports-37920227646` | `2026-10-09T10:54:05Z` |

These checks establish **no recoverable original in the bounded sources inspected**,
not universal nonexistence. External private archives, mailboxes and unlisted
copies were not searched. Expired/unavailable sources are not replaced with
current medical retrieval. With no original source identified, no import diff or
production correction is proposed: Daily 2026-09-01 remains absent, the valid
four-Daily Weekly and explicitly linked September Monthly remain unchanged, and
historical policy/delivery remain unknown. No actual Yearly publication was
established, so none is declared corrected.

The implementation provides [reviewed insert-only maintenance](reviewed-maintenance.md)
with source/target hashes, private backup, Diff/plan and idempotence. It does not
claim to reconstruct the missing original. Existing valid snapshots, including
valid-empty, are preserved; editorial changes to them require a separately
reviewed correction rather than this insert contract. No State, rollup, Health,
processed/Paper/Delivery receipt or Sent marker was changed during this work.
