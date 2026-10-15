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
this PR; general historical coverage/recovery remains prompt 07.

## Reader labels

- A–E remains a compatible **article design heuristic**, explicitly labelled
  **not GRADE**. Source type and trial design do not establish risk of bias,
  recommendation strength, effect certainty, or clinical benefit.
- “Text coverage … (not effect certainty)” is separate from the study-design
  hint. New PubMed coverage is based on available results/source omission,
  rather than promoting a long abstract to high confidence. The context line
  distinguishes Abstract, Abstract plus full-text excerpt, bounded excerpts,
  unavailable results, population and source-reported uncertainty.
- An explicit primary endpoint and primary result must be available before a
  result can support a new paper favorite. Unmapped results stay unknown.
  A source-reported conditional guideline recommendation can support editorial
  priority; it does not become a strong recommendation or a proven effect.
- A clinically important null result can be a favorite. Its validated common
  statement still says that significance was not demonstrated, without treating
  this as proof of no effect/equivalence. Observational statements retain the
  canonical facts guard against causal upgrading.
- FOAMed source **priority** is editorial source configuration, not evidence
  quality. The learning/context role and known primary references are visible.
  A missing legacy text basis is shown as unknown even if an old length-based
  `text_confidence_label` says high.

The existing canonical PubMed facts and shared overview/deep-dive core remain
authoritative. These labels do not replace that guard or add a second clinical
model verdict. Unknown endpoints, missing data and uncertainty are not repaired
with invented findings.

## Favorites and historical ranking

The selector keeps its existing score/type/content floors. A shared final
decision checks the actual displayed scores: both relevance and practice impact
must be at least 3/5 for a new paper favorite, the design hint must pass the
existing A–C floor, the clinical domain must be source-based, and the primary
result or a reported guideline recommendation must be available. Clinical
importance is not a claim of benefit. A deep dive cannot force a star afterward;
there is no minimum number of favorites.

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
it is labelled **“Stored ranking (not revalidated)”**, never “Top pick”. An old
score, guideline label or star is not newly assessed by a model. The original
Daily flag and its provenance are preserved separately; period ranking is not
misrepresented as the original Daily decision.

This compatibility path can retain a low-scored or unscored historical rank.
Its explicit label is essential. Applying today's rule to rewrite those past
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
