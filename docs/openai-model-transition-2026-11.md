# OpenAI model transition — November 2026

## Verified production configuration, November 24

- Cyberlurch run 45420672597: GPT-5 mini for chunk/direct transcript analysis (11 successful direct-digest calls); GPT-4.1 for overview, deep dives and configured fallback.
- Cybermed run 45423698546: GPT-4.1-2025-04-14 for the clinical stages (25 successful calls, about USD 0.16 estimated usage).

The official [deprecation schedule](https://developers.openai.com/api/docs/deprecations) gives December 11, 2026 for GPT-5 mini and January 14, 2027 for GPT-4.1 / GPT-4.1 mini. The [GPT-6 migration guide](https://developers.openai.com/api/docs/guides/latest-model?model=gpt-6-astra) describes reasoning effort and parameter differences.

## Cyberlurch

The default is GPT-6.1 Terra, including direct digest, transcript chunks, overview, deep dives and fallback. Its reasoning effort is explicitly none to preserve the existing focused extraction workload. The existing GPT-5 mini and GPT-4.1 aliases/snapshots are normalized **only in the Cyberlurch job**, so old Actions variables do not silently defeat the transition. Other explicit model choices remain unchanged. JSON validation and report delivery are unchanged.

## Cybermed

The November 25 live A/B review and clinical production-path smoke checks support selecting GPT-6.2 Sol as the workflow default. The existing manual `Cybermed model A-B evaluation` workflow compares the historical GPT-4.1 baseline against GPT-6.2 Sol by default; Terra and Luna are alternative candidates. It uses synthetic cases, a bounded USD 0.50 estimated-cost budget, no SMTP and no production State writes. A successful workflow is not, by itself, a clinical quality approval: inspect the blinded output for numeric fidelity, study design, association vs causation, adverse events, limitations and concise presentation. The three fixtures are a minimal smoke/quality comparison, not comprehensive clinical validation.

The producer and Daily backfill workflows explicitly default `CYBERMED_APPROVED_MODEL` to `gpt-6.2-sol`. An intentional repository Actions variable can select a separately reviewed candidate. The supported IDs are `gpt-6.2-sol`, `gpt-6.1-terra` and `gpt-6-luna`. All clinical stage models switch together, including daily backfill generation; an unapproved alternative/fallback remains blocked. Existing clinical per-run caps are retained; the Sol candidate reserves additional reasoning tokens for bounded requests. Keep the existing evidence guards and inspect the first complete production report, usage and fallback telemetry. Do not replay a previously accepted newsletter to test a model.

## Verification

Offline CI verifies request adaptation, old override normalization, and the clinical model gate. Live tests on November 25: [45607690088](https://github.com/Cyberlurch/NewsAgent2/actions/runs/45607690088) validated Terra JSON and the three-case blinded comparison (Sol keyword coverage 100%, 100%, 100%; GPT-4.1 60%, 100%, 50%). [45608330285](https://github.com/Cyberlurch/NewsAgent2/actions/runs/45608330285) exercised the actual clinical Bottom Line, Deep Dive JSON, FOAMed and Overview request paths with Sol on structured synthetic sources: 6/6 API calls, no budget blocks, USD 0.018856 estimated usage. Required quantitative evidence was retained. These are bounded synthetic checks, not full-report or comprehensive clinical validation. The clinical default was selected explicitly after review; the next full production run still needs observation. The retained smoke workflow is manual-only, no sending and read-only checkout.

No credentials or recipient addresses are changed by this transition.
