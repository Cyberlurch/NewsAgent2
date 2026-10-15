# Cybermed PubMed evidence and targeted correction

The 12 October 2026 Daily digest for PMID **42854247**, DOI
**10.1213/ANE.0000000000008318**, incorrectly described successful delirium
prevention and better adherence. Its Deep Dive described a nonsignificant
result. The collector's 2,931-character text lost its results when Bottom Line
input was cut at character 2,000. The former conflict guard also missed a
positive overview versus a negative Deep Dive.

The primary-source audit supplied by the operator reports 7,294 randomized
patients but documented delirium assessment in only 1,255: **225/641 (35.1%)
versus 201/614 (32.7%), P=.37**. Best-practice adherence comparisons were all
**P>.05**. These data do not establish a benefit. Missing assessments limit
interpretation; lack of statistical significance does not prove no effect.
Source: <https://pubmed.ncbi.nlm.nih.gov/42854247/>. The correction file is a
short paraphrase of those audited facts, not a copied article. PubMed access
was blocked by the environment's proxy during development; the operator must
verify the linked primary abstract before applying the prepared correction.

## Evidence boundary

`pubmed_facts.py` extracts bounded source sections, with Results, Conclusions
and limitations before background. It records design, population,
intervention/comparator, endpoint, reported/primary/secondary results,
uncertainty, source basis and a content hash. It never derives result direction
from objectives. Missing results remain unknown. Fulltext is a bounded excerpt;
disagreement with abstract results causes a cautious reduction. Noninferiority
does not establish superiority/equivalence; observational association does not
establish causality. Explicit secondary outcomes cannot replace the primary
outcome.

The same source-derived core appears in the overview and the Deep Dive.
Free-form provider wording cannot reliably prove group/endpoint binding, so
Cybermed's paper assertions and Deep Dive fields are reduced to this extractive
representation. A contradictory Deep Dive is replaced completely, not prefixed
with another Bottom Line. Incomplete provider output is flagged and reduced.
Existing content-safe provider errors and per-run budgets remain active. This
adds no provider calls and no new model/fallback; the pinned Cybermed model is
unchanged. Cyberlurch generation and stored aggregate readers are unchanged.

This guard is deliberately conservative, not perfect medical validation.
Unstructured abstracts, ambiguous endpoints, unlabelled secondary outcomes and
excerpt limitations still need clinical review. It may omit useful model
interpretation rather than assert an unverified benefit. Stored assertions are
not silently reinterpreted from titles. The Daily→Weekly→Monthly→Yearly chain
and its source IDs remain authoritative.

## Reviewed correction of the saved Daily

The saved 12 October Daily still contained the erroneous benefit claim after the
pipeline fix. This follow-up includes the existing targeted correction as a
reviewable State diff. The [field-level reviewed plan](../data/corrections/cybermed_42854247_reviewed_plan.json)
shows all **21 changed fields in exactly two records**: the PMID 42854247 overview
and Deep Dive. The compact store format is retained; use the plan to review the
bounded before/after values without comparing an entire one-line store.

Both views now use the same cautious core: the reported comparison did not
establish a significant delirium-prevention benefit; missing assessments limit
interpretation, and nonsignificance does not prove absence of effect. The
numerical basis is the operator's quoted 13 October primary-abstract audit above.
A fresh independent NCBI EFetch verification was blocked by this environment's
proxy; **no independent online revalidation is claimed**. Review the linked primary
abstract and the field diff before merging this State correction. No secondary
source or model was used.

Input Daily SHA-256: `5b298aaa63dee9a2395b4372f61a41c6e4f4fc850ec165fd4fd3974ffd4b6ef9`.
Reviewed output: `02b0b325ed77244ac6085fab9c3d4aa5cd76b000290119e3cb44bee53ac84a96`.
The plan also binds the unchanged Weekly/Rollup inputs and correction file.
There are no stored aggregate provenance links to this Daily in the inspected
State, so no aggregate is rewritten. Other papers, original edition identities,
generation timestamps, clinical scores, delivery/process markers, Health, caches
and Cyberlurch are unchanged.

The existing CLI was dry-run, reviewed and applied to a temporary State copy with
private preimage backups. All input/output hashes matched; a second Apply was an
idempotent `already_corrected` no-op. Only that reviewed Daily output is included
in this branch. No productive workflow, mail, model or collection was run. After
merge, the same reviewed plan is a no-op; it does not authorize a new edition or
mail replay. A rollback must restore only this bounded correction after checking
its current hashes against newer edits; never reset the complete State tree.

## Historical preparation of the correction


No production store was changed. On main `5fef56b`, the tool identified exactly
two records in `cybermed_daily_2026-10-12`:

| Record | Planned correction |
|---|---|
| `/digests/99/items/pubmed/1` | Replace erroneous Bottom Line; add facts, source basis, version and correction provenance. |
| `/digests/99/deep_dives/1` | Use the identical core; replace the entire Deep Dive and fill the corresponding structured evidence fields. |

All other papers, process/delivery markers, health data and Cyberlurch records
are retained. There were **no existing Weekly/Monthly provenance links** to
this Daily digest in the inspected checkout. No aggregate is regenerated.
Unknown linked editorial prose blocks apply and is reported for manual review.
The reader handles additive facts fields; legacy records remain readable.

Generate a fresh reviewable field-level diff and hashes, without providers,
email, dispatch or state writes:

```bash
cd /workspace/NewsAgent2
PYTHONPATH=src python -m newsagent2.maintenance.correct_pubmed_evidence \
  --correction data/corrections/cybermed_42854247.json \
  --state-dir state --output /tmp/42854247-plan.json
```

Review the source, every `changes` entry, input/output SHA-256 hashes and links.
Run against the intended checked-out branch with competing scheduled writers
paused; do not force-push or use a global backfill. Apply is a separate,
explicit operator maintenance step after review:

```bash
PYTHONPATH=src python -m newsagent2.maintenance.correct_pubmed_evidence \
  --correction data/corrections/cybermed_42854247.json \
  --state-dir state --output /tmp/42854247-apply.json \
  --apply --reviewed-plan /tmp/42854247-plan.json \
  --backup-dir /tmp/42854247-private-backup
```

The backup directory must be new and private, outside the public checkout.
Apply checks all scanned input hashes and the reviewed output hashes before
writing. Every preimage is backed up before atomic file replacement. A retry
with the same plan recognizes the reviewed output hashes and is a no-op when
already corrected. Changed input/correction, corrupt JSON, missing targets or
unresolved aggregate prose fail without overwriting history.

Multi-file replacement is not one filesystem transaction. An I/O failure
records the files already written in the private backup; stop, compare current
hashes, and review recovery. Never restore a backup over a newer writer.
Rollback restores only the affected backed-up file(s) after confirming their
current hashes still match the applied output, then commits that targeted
reversal. Revert the code PR separately if necessary. Neither apply nor rollback
sends mail or creates new editions.

## Offline validation

Regression fixtures are synthetic/paraphrased. Tests cover late results,
both conflict directions, missing results, primary versus secondary outcomes,
noninferiority, observational causality, matching cores, truncated output,
safe provider failures, bounded prompts, read-only correction, hash mismatch,
backup/idempotence, provenance links and corrupt-file preservation. Existing
model-budget and error-leak tests remain in the suite. Tests run in a separate
copied checkout with socket connections blocked; all production `state` files
are hashed before/after. The counts below are historical PR #224 results. The five RSS regressions
were resolved by subsequent dependency/provider repairs; they are not current
known failures. See [development.md](development.md) for current offline checks.

Run from an isolated copy of the checkout (not the production working tree):

```bash
NEWSAGENT_OFFLINE_TESTS=1 PYTHONPATH=src SEND_EMAIL=0 python -m pytest -q
```

Verified result: **527 passed, 5 known failures, 6 subtests passed**, one
feedparser deprecation warning. All five failures are the preexisting cases in
`test_youtube_rss_fallback.py`; no test was deleted or marked xfail. The six
production state files retained their exact before/after SHA-256 hashes.
