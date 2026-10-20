# Cyberlurch source identity and text basis

This extends [bounded discovery](cyberlurch-discovery.md), without new production
paths, source-list changes, model changes or delivery permissions. It does not
certify current channel availability or validate every statement in a video.

## Identity and listing

Channel identity distinguishes `configured`, `resolved`, `legacy_unverified`,
`unresolved` and `conflict`. A configured ID must be a full `UC` + 22-character
YouTube channel ID; it is operator configuration, not a live verification.
Conflicting explicit IDs and `/channel/` URLs stop collection for that source.

Resolution can use a unique `forHandle` API result or the root
`ytInitialData.metadata.channelMetadataRenderer` with an owner URL matching the
configured URL. Conflicting owner/canonical IDs remain a conflict. Arbitrary
`channelId`/`browseId` strings in recommendations are never identity evidence.
Conservative parsing may leave real channels unresolved when YouTube changes
its markup. Consent and TLS failures are not evidence of channel inactivity.

The existing ID cache retains its legacy `channels` section, but name-only
entries are hints, not binding evidence. A new `bindings` section (at most 256)
keys identities by normalized configured YouTube URL; equal names do not bind
different URLs. A conflicting saved/configured binding is held rather than
overwritten. No existing cache/history is reset or renamed. A video's API
uploader cannot silently replace the configured channel ID; mismatches against
a known identity exclude that video. Channels without binding evidence remain
unverified; the operator can inspect them explicitly.

RSS and yt-dlp return list-compatible structured listing results. They distinguish
items, a valid empty listing, valid entries outside the edition window,
resolution/conflict, fetch/parse failure, HTML/consent and access/TLS errors.
RSS checks Atom structure and available feed/entry channel IDs. Valid empty RSS
does not trigger yt-dlp. A failed RSS primary is not retried again as a fallback;
an unstructured legacy empty result remains unknown. `channels_success_total`
now counts successful listing paths, including proven empty listings, **not**
content extraction, selection, transcript availability or SMTP acceptance.
Separate fixed-category counters describe listing and identity outcomes.

The application passes its existing edition reference to listing adapters.
Identity request counters are separate from #233's global video-ID budget;
resolution makes at most one forHandle request plus one owner-page fallback.
No extra source-wide verification pass is introduced.

## Text and model output

New rows carry an additive `source_basis` version 1:

| Field | Meaning |
|---|---|
| `kind` | `description`, `transcript`, `metadata_only`, or `unknown` |
| `provider` | Actual extraction provider, or unknown legacy origin |
| `available_chars` | Characters of available supplied source text |
| `processed_unique_chars` | Unique source characters used in a validated extraction |
| `coverage` | `none`, `partial`, `available_text`, or `unknown` |
| `video_completeness` | Always `unknown`; source-text processing does not prove full audio coverage |

Native and managed transcript text is retained in memory before the existing
display/excerpt limit. The Direct/Chunk paths therefore receive later available
text too. Descriptions remain descriptions and retain their bounded acquisition
excerpt; partial processing is labelled. `full_text` remains a compatibility
content-status value for an available transcript; it does not mean a verified
complete video. Display, new diagnostics and new source-basis fields distinguish
those meanings. Legacy `full_text` without new metadata stays unknown.

Daily still has at most 14 acquisitions, 12 items and 2 Deep Dives, with no
additional semantic verification call. Direct extraction respects the existing
80,000-character default and lower operator settings. Chunking respects the
existing enable flag and default limit of 5 chunked items, plus a conservative **20 total
chunks per application invocation** (lower via
`CYBERLURCH_TRANSCRIPT_MAX_CHUNKS_PER_RUN`). Plans exceeding this bound are
skipped before model calls, not silently prefix-summarized as full coverage.
Each standalone chunk helper is also capped at 20. Chunk output is bounded by
the existing chunk-summary character setting before combination. The unique
source-character count is separate from retransmitted/overlapping characters.
Existing compatibility retry remains bounded; no new repair model is added.

Direct/chunk/combine output needs a nonempty string summary and properly typed
bounded arrays when supplied; absent optional arrays become empty. Truncated
finish reasons, invalid structure and recognized technical error text yield no
reader summary. Valid JSON inside a surrounding envelope may be recovered;
unvalidated raw model text is never a prose fallback. This is structural and
heuristic protection, not semantic proof that every generated claim is correct.

Prompts receive the actual text basis. Direct/Chunk extraction is per item;
the configured channel handle is kept in identity, provenance and presentation
rather than supplied as a cue for different narrative treatment. Multi-source
period synthesis retains its named source registry and references. Title is context, not a
reason to override contradictory supplied text. The editorial voice reproduces
each source's content, argument and emphasis without adding a verdict about its
truthfulness or an ideological tone category. Source opinions, religious teaching
and political positions remain source content; they are not rewritten into a
single approved interpretation. The source's own qualifications, forecasts and
negation remain, and quoted guests/denials retain their meaning. EN/DE/SV and
respectful source treatment remain; other source languages use English.

A named channel heading frames a Daily item once. The renderer does not prepend
“According to…” to every claim or repeat “In brief: channel…” after that heading.
Channel-identity metadata remains in diagnostics/stores and still gates collection;
its repetitive status line is omitted from the reader body. The shared writing
rule applies to Direct, Chunk/Combine, Weekly, Monthly and Yearly. Multi-source
prose identifies differing positions when needed to tell who argues what, without
a report/speaker formula in every sentence. Citations and provenance contracts
remain. No additional rewriting/verification model call is introduced.

Cache hits carry new basis metadata; legacy cache text remains unknown rather
than acquiring invented verification. New basis/identity fields survive Daily
serialization/reading and period source registries. Final validated selection
rows are saved before their acquisition-only duplicates, fixing the loss of
generated fields when selection copies a row. New unprocessed acquisition rows
cannot turn titles into Monthly factual evidence. The existing explicit Monthly
collection audit can still supply bounded actual source text to its existing
synthesis call, with its unprocessed/limited basis retained; it does not gain
permission to infer facts from titles. Stored legacy material is
not rewritten; old basis remains unknown in source indexes and model inputs.
Monthly/Yearly references and evidence registries retain their prior identities.
Historical issue coverage and its remaining uncertainty are described in
[the coverage guide](history-coverage.md); source-text coverage does not establish
a complete edition history.

## Reader context

Daily uses the already-saved full summary to introduce the actual subject,
action and context before supporting content. The Executive Snapshot
can include its first complete sentence; the item block retains up to two
complete sentences and 90 words, with the named channel beside the prose.
Sentences are extracted rather than regenerated or clipped into fragments.
New extraction introduces the subject in 1–2 natural sentences, at most 60 words,
then uses existing key points/important details for concrete consequences: who
is affected, what could change and why, as actually described by the source.
“Discusses political and economic implications” does not replace those details;
missing consequences are not invented. Normal items can show up to five useful
statements so three headline points cannot crowd out every consequence.

The same source-scoped reader treatment applies to Daily and Weekly item blocks,
independent of channel names, topics or perceived credibility. A named reporting
voice that introduces the synopsis and repeats in its facts is identified once
in the source line; its direct statements need no repeated “Name argues/claims”.
Third parties merely quoted in a news item do not become the block's narrator.
Generic transcript/sermon metadescriptions yield to concrete saved statements.
Two source sentences can form a fluent introduction; already included facts
count toward the existing five-point limit rather than reappearing as bullets.

Recognized English frames and bounded German `dass` clauses can be rewritten
locally. German recognition uses known finite verbs and explicit clause shapes;
ambiguous genitive/dative or prepositional ownership, unknown grammar, denials,
quotes and compound reporting acts stay unchanged. Numbers, negation, modality,
and existing qualifications are retained. The generation rule independently
requires the same direct voice for every source, without new hedges based on
channel identity. No new model operation or stored-history rewrite is introduced.
Duplicate removal keeps the fuller statement when it literally contains
the shorter one; a high word overlap alone cannot erase different numbers,
signs, ranges, negation or positions. Exact repetition of the introduction is
omitted from its supporting bullets.
The existing 12-item/2-Deep-Dive limits, acquisition/model budgets, source-basis
labels and delivery boundaries are unchanged.

Known old “Unable to summarize” placeholders and recognized technical errors
are not reader facts. They cannot take precedence over a substantive saved
summary in Monthly evidence. A known placeholder-only row cannot acquire a
factual summary from its title. Explicit metadata-only/unprocessed sources do
not supply Daily context or fact bullets; the existing source-basis label shows
that limitation. Missing historical narratives are not reconstructed or
silently revalidated. This change makes no new generation/source calls, edits
no stored history, and does not change Monthly/Yearly synthesis contracts.

## Operator inspection (not executed live for this PR)

Local inspection only, no network or file creation:

```bash
PYTHONPATH=src python -m newsagent2.youtube_probe --channels ThePoplarReport
```

An operator may explicitly allow a public listing probe after review:

```bash
PYTHONPATH=src python -m newsagent2.youtube_probe --channels ThePoplarReport --probe
```

Scope is 1–3 explicitly named, unambiguous configured channels, default shared
30-second hard deadline, 8 HTTP requests and 3 redirect hops. Lower bounds are
available with `--seconds`, `--requests`, `--redirects`; larger values are
rejected. This reuses the existing budgeted HTTP/deadline utility, without
changing FOAMed. TLS checks remain enabled. No forHandle API quota, yt-dlp,
transcript, paid provider, model or SMTP is used by this probe. Transcript stays
`not_probed`; lack of a listing does not classify a source as dead. The probe
does not write state, Health, caches, lock files or reset any ban history, even
with `--probe`. Output goes to stdout; timestamps identify actual observations,
and unresolved stages remain explicit. Invalid existing cache JSON is left
unchanged and stops inspection. No fallback credentials are invented.

## Recovery and rollback

All existing central paths, `none/test` scratch isolation, publication allowlists,
Writer-Lock, frozen editions and HMAC/SMTP/replay proof contracts are retained.
Candidate ingestion accepts an optional bounded validated identity field; old
candidates remain readable. No new delivery status, recipient index, recovery
permission or live probe workflow is added. Generation still is not SMTP
acceptance; SMTP acceptance is not inbox delivery and Git is not its transaction.

Rollback by reviewed code revert while retaining journals, sealed receipts,
caches and generated records. Do not reset state or resend editions. Older code
can ignore additive fields but restores the weaker identity, raw-fallback and
truncation behavior; operationally review that tradeoff. No actual historical
state correction or source activation is included. Step 07 remains historical
coverage/recovery; step 08 reconciles README last.
