# NewsAgent2

**NewsAgent2** is an automated newsletter system that collects, filters, summarizes, and emails curated news reports on a schedule.

The project started as a personal automation experiment: instead of manually checking dozens of sources every morning, NewsAgent2 gathers relevant material, removes duplicates, ranks what matters, and turns it into readable newsletters.

It currently produces two main reports:

1. **Cybermed Report** – a medical newsletter focused on PubMed, critical care, anaesthesia, emergency medicine, and FOAMed sources.
2. **The Cyberlurch Report** – a broader general-interest newsletter based on selected YouTube/news sources and written as a concise situational overview.

The system runs automatically with GitHub Actions and sends the finished reports by email.

---

## What problem does it solve?

Most news feeds are noisy.

Medical literature feeds are especially difficult: many papers are technically new, but not clinically important. Blogs and FOAMed sources can be useful, but they vary widely in quality. YouTube/news channels can contain important signals, but they are mixed with repetition, speculation, and low-value content.

NewsAgent2 tries to solve this by acting as a **curated daily briefing agent**:

- collect from many sources,
- avoid sending the same item twice,
- rank by relevance,
- summarize clearly,
- keep links to the original sources,
- and package everything into a readable email.

The goal is not to replace human judgment. The goal is to reduce the amount of material a human has to manually scan.

---

## The two newsletters

## 1. Cybermed Report

The **Cybermed Report** is the medical newsletter.

It focuses on:

- intensive care,
- anaesthesia,
- emergency medicine,
- resuscitation,
- sepsis and infectious disease,
- ventilation and respiratory medicine,
- clinically relevant AI/methods papers,
- selected FOAMed and medical commentary sources.

The daily Cybermed report is “paper-first”. PubMed literature is treated as the primary evidence layer, while FOAMed/blog material is used as a commentary and interpretation layer.

Typical output includes:

- selected papers,
- concise **BOTTOM LINE** summaries,
- evidence/relevance/practice-impact labels,
- links to PubMed or source pages,
- optional deep dives for selected high-value papers,
- FOAMed/commentary items when they add clinical value.

The weekly and monthly Cybermed reports are built from stored daily digest data. This keeps them reproducible and prevents the weekly/monthly reports from re-collecting live PubMed or FOAMed data.

Current Cybermed status:

- **Daily:** live PubMed + FOAMed collection.
- **Weekly:** digest-only, based on stored daily Cybermed digests.
- **Monthly:** digest-only, based on stored daily Cybermed digests.
- **Year-in-Review:** planned/under improvement as an editorial annual synthesis based on stored monthly/daily data.

---

## 2. The Cyberlurch Report

The **Cyberlurch Report** is a broader general-interest newsletter.

It collects from selected YouTube and news-style sources and creates a readable summary of what is new or important.

It is designed less as a link dump and more as an executive-style overview:

- what happened,
- why it may matter,
- what themes are emerging,
- which sources contributed,
- and which items deserve a closer look.

YouTube metadata and available text are used where possible. Transcript/caption fallbacks can be enabled, but the system tries to avoid unnecessary heavy processing.

---

## How the system runs

NewsAgent2 runs on **GitHub Actions**.

The normal schedule is designed around **Europe/Stockholm** time and aims to produce morning newsletters.

The main workflow is:

```text
.github/workflows/newsagent.yml
