# Cybermed clinical routing and editorial priority

This policy adds deterministic source-based hints to Cybermed. It is not a
medical validation service, a formal GRADE assessment, or proof of treatment
benefit. Cyberlurch selection, source configuration, languages, and models are
unchanged. There are no additional model calls or source requests.

## Clinical content before journal name

`clinical_editorial.clinical_text` reuses the existing bounded, results-first
PubMed input builder. Journal and channel names are excluded from clinical
keyword signals. Explicit title/population/setting signals precede incidental
intervention keywords; ICU content can therefore override a Pain journal.
The renderer uses the same `clinical_context` saved with new items.

Population hints distinguish adult, pediatric, neonatal, mixed, and unknown.
They use the source population when available, with title/stored population
fallbacks. Journal titles never establish population. Missing age details stay
unknown, rather than implying adult applicability. Simple explicit exclusion
phrases are recognized; complex eligibility language, background populations,
and competing settings can still defeat these lexical rules. They are not an
exhaustive clinical ontology or a substitute for reading the paper.

When content cannot establish a topic, a journal may choose the layout section.
The displayed topic-placement warning identifies this as a weak fallback.
No new medical finding is inferred from that placement.

For NEJM, Nature, Science, Lancet, JAMA, and BMJ, a local clinical-domain gate
prevents journal prestige alone from promoting an unrelated contribution.
Existing hard exclusions, core journals, and type floors remain. Nature/Science
already had query filters; none of the queries are changed here. The broad NEJM
query can still consume a bounded retrieval slot on an off-topic record before
local filtering. This limitation remains explicit: no query-version reset,
cursor loss, API-budget increase, or new catch-up/backfill is introduced.

ESICM/SCCM/ESAIC guideline sentinels remain unactivated pending reviewed dated
primary-source adapters and provenance. BJA Education would be a separate
learning role, not automatically original research. No sources are expanded in
this policy. Historical coverage and reviewed reconstruction are documented
separately in [history-coverage.md](history-coverage.md) and
[reviewed-maintenance.md](reviewed-maintenance.md).

## Reader labels

- The compact reader line is `⭐ Top pick · Evidence A · Relevance 5/5 ·
  Practice impact 5/5 · Confidence High`, using actual available scores and
  labels. A star appears only for a selected priority; missing values are omitted.
  A–E remains an **article design heuristic, not GRADE**. Source type and trial design do not establish risk of bias,
  recommendation strength, effect certainty, or clinical benefit.
- `Confidence` describes the available source text, not certainty of a treatment
  effect. Source coverage, endpoint mapping and policy reason codes stay in
  structured diagnostics; they are not repeated in each reader header. Existing
  labels are displayed as stored, including legacy labels; this is no new
  assessment of historical evidence. Population belongs in the short clinical
  statement when it is known from the source, not a repeated context line.
- Primary benefit still requires a mapped primary endpoint/result. Unmapped
  primary benefit stays unknown; an explicitly attributed, source-bound
  measured finding may still be described. A selected, clinically
  important null, harm or noninferiority finding may remain an editorial priority
  without an explicit primary label; biomarker-only positive signals do not.
  A source-reported conditional guideline recommendation can support editorial
  priority; it does not become a strong recommendation or a proven effect.
- A clinically important null result can be a favorite. Its validated common
  statement still says that significance was not demonstrated, without treating
  this as proof of no effect/equivalence. Observational statements retain the
  canonical facts guard against causal upgrading.
- FOAMed `Source quality` reflects editorial source configuration, not a formal
  evidence grade. Known primary references remain linked; repetitive missing-
  reference notices are omitted. Learning summaries and source floors remain.

The existing canonical PubMed facts and shared overview/deep-dive core remain
authoritative. These labels do not replace that guard or add a second clinical
model verdict. Unknown endpoints, missing data and uncertainty are not repaired
with invented findings.

The reader projection adds no provider calls. The existing Cybermed Bottom Line
call proposes 2–3 complete sentences: the actual patients and question/comparison,
the reported finding, and a useful clinical implication or specific limitation.
It returns JSON containing that prose and an exact supporting result excerpt.
Local checks bind the excerpt to the canonical primary result (or an explicit
measured result sentence in an unstructured abstract), check scope, direction,
population, named groups, noninferiority and association, and reject truncation,
unbound quotes, generic text and statistical walls. This is bounded lexical
validation, not a second independent medical assessment or a guarantee against
every paraphrase error. The full canonical facts and final consistency guard
remain authoritative. The same pinned model and one existing call are used;
its Cybermed output ceiling is 240 instead of 90 tokens to fit complete prose
plus its short supporting quote. Run-wide token, cost and call caps are unchanged.

The lead sentence is checked too: merely calling something a randomized trial
does not turn a claimed benefit into a study question. A correct second
sentence cannot excuse a contradictory positive/negative lead. Named-arm,
observational-causality and noninferiority checks cover the whole short prose.

Deep Dive labels such as **Key results:** and **Limitations:** are bold. A
limitation label and its text form one row, including older numbered/list
formats. Embedded abstract headings such as **Conclusions and relevance:**
start a separate block rather than running into the result paragraph. Per-item
Source/Source basis metadata is hidden from this view; paper links and actual
reference links remain. The formatter changes reader Markdown only, preserving
canonical facts, source-basis metadata and stored history.

Source/delivery review holds remain enforced and recorded in the ingestion
ledger/diagnostics. Their generic withholding disclaimer is omitted from reader
editions. A genuinely incomplete capture retains its existing coverage notice;
omitting the operational disclaimer does not assert complete coverage.

The optional `clinical_summary` is tied to the facts version and paper identity,
rechecked when rendered, and preserved additively with the canonical facts in
new Daily → Weekly → Monthly → Yearly inputs. A changed source version requires
revalidation during generation; stale or foreign stored proposals are rejected.
Existing history is not rewritten and aggregates make no generation calls.
When a proposal cannot be used, a deterministic fallback names the actual
study question/comparison before describing the known finding or missing result.
It never publishes only “the clinical implication remains uncertain.”
It does not use objectives as
results or let positive secondary results replace a null/unknown primary result.
Statistical estimates, P-values, IQRs, group counts and specific limitations stay
in Deep Dives. Clinically meaningful qualifiers are retained; dense subgroup
findings are not generalized into overall benefit. The overview and Deep Dive
share the same short Bottom Line. Full internal facts, detailed results,
consistency checks and stored records are preserved. This is a bounded editorial
projection, not perfect medical validation or a retroactive history rewrite.
Short legacy statements without a source facts record retain their existing
wording, including a separately stored Deep Dive statement. They are not
silently revalidated or replaced from a title; dense legacy result text remains
in the detailed view when that view is available.

For example, the paraphrased PMID 42854247 regression explains that surgical
patients with cognitive impairment were randomized to AI-guided decision support
or routine care, that a significant reduction in delirium was not demonstrated,
and that incomplete assessments limit interpretation. The counts and P-value
remain in the Deep Dive; nonsignificance is not described as proof of no effect.
This is an offline example, not a new source verification or a sent newsletter.

## Favorites and historical ranking

The selector keeps its existing score/type/content floors. A shared final
decision checks the actual displayed scores: both relevance and practice impact
must be at least 3/5 for a new paper favorite, the design hint must pass the
existing A–C floor, the clinical domain must be source-based, and the primary
result, a reported guideline recommendation, or the bounded null/harm/
noninferiority clinical-priority case above must be available. Clinical
importance is not a claim of benefit. A deep dive cannot force a star afterward;
there is no minimum number of favorites.

Favorites are weighted by clinical relevance and the existing evidence/content
criteria. There is no percentage target, quota or one-star limit; zero or several
papers may qualify in an edition. A desired visual density never lowers the
quality floors or invents an endorsement.

FOAMed retains its usefulness/relevance/source/text floors and needs an actual
learning summary before it can remain a new favorite. Thin/unsafe output does
not earn a star just because the original candidate was promising.

`top_pick_decision` records version, cadence, selected status and reason.
`stored_top_pick` retains the upstream flag/cadence/run date when available.
These are editorial metadata, never delivery receipts. Main and the renderer
apply the same rule; the renderer copies rows and does not mutate a store.

Weekly/Monthly selection still uses persisted inputs and existing caps. For new
rows with `clinical_context`, the same eligibility floors apply. Legacy rows
without that source context retain the existing historical period-ranking
algorithm, including the explicitly authorized historical backfill parity.
Their compatible `top_pick` flag **does not assert current clinical approval**:
it uses the compact **“Top pick”** reader label, while its internal reason stays
`legacy_period_ranking_not_revalidated`. An old
score, guideline label or star is not newly assessed by a model. The original
Daily flag and its provenance are preserved separately; period ranking is not
misrepresented as the original Daily decision.

This compatibility path can retain a low-scored or unscored historical rank.
The stored provenance distinguishes that case. Applying today's rule to rewrite those past
decisions would require a separate reviewed maintenance scope. Missing legacy
facts are not regenerated, silently added to source context, or used to infer
SMTP history. An unproven flag on a new Daily item is rejected.

## Learning summaries and stored limitations

FOAMed uses the existing bottom-line call, model pin, and 120 output-token cap.
Source sentences plus bounded role/reference metadata fit the prior 1500-char
content envelope. Overlong links are omitted from the model payload rather than
truncated into invented URLs; original provenance remains stored and rendered.
Very thin source text is skipped before a paid call. No original article is
refetched or additional validation model invoked.

The prompt asks for a concrete source-supported learning point and attribution,
not a generic “no original research” sentence. Local checks omit truncated
answers, unknown links, unsupported numbers, certain clinical efficacy claims,
and a bare role disclaimer. They are deliberately conservative and can omit a
valid paraphrase; they are not semantic proof that every accepted statement is
correct. Unusable responses do not trigger another model call. The reader sees
a neutral source link/learning-unavailable note rather than provider error text.

New limitation lists remain JSON arrays; valid old strings and arrays remain
readable. No `str(list)` step turns new content into Python list syntax. New
Daily overview/deep-dive metadata, Weekly rows, Monthly `cybermed_items`, and
Yearly views preserve population/context, limitations, and FOAMed references.
Legacy strings already containing a list repr are not mass-migrated. The
Daily→Weekly→Monthly→Yearly chain stays stored-only.

## Verification and rollback

Offline regressions cover conflicting journal/content, late information, age
populations, local journal filtering, incomplete abstracts, null/observational
results, score floors, a real mocked main run that cannot turn a low-score deep
dive into a favorite, all stored cadence boundaries, legacy backfill parity,
FOAMed payload/call budgets, bad outputs and missing learning summaries.
The existing suite covers both newsletters' none/test mutation isolation and
hashes protected state paths; the new main regression additionally verifies
every centrally registered production destination, including absent files.

No production state correction, mail, dispatch, paid test, live source probe or
backfill is performed. JSON/receipt guards, writer lock, workflow checkout,
ingestion watermarks, budgets and pinned models remain unchanged.

Rollback is a reviewed compatible deployment without state reset or receipt
removal. Additive context/decision fields can be retained. Older renderers will
not understand the legacy-ranking label and may present historical flags as
fresh favorites again; older main may force stars for deep dives. Do not treat
that downgrade as equivalent behavior, and do not regenerate/resend history.
README updates remain the final step 08.
