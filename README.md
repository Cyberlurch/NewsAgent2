# NewsAgent2

**NewsAgent2 turns a large stream of articles and videos into two curated email newsletters.** It gathers material from selected sources, chooses what is relevant, summarizes the key findings and links back to the originals. The aim is to spend less time searching and more time reading what matters.

## The newsletters

### Cybermed Report

Medical research for readers interested in **anaesthesia, intensive care and emergency medicine**. PubMed papers come first, supported by selected medical education sites (FOAMed) and commentary. Each selected paper is linked directly and explained in a concise **BOTTOM LINE**. Important studies receive a longer **Deep Dive** covering the findings, limitations and clinical context.

### The Cyberlurch Report

A general news and analysis briefing based on a selected set of YouTube channels and other configured sources. It highlights the main stories, explains why they matter and provides deeper coverage of a few topics. Source links make it possible to check the original reporting or commentary. English, German and Swedish are supported where configured.

Both newsletters are editorial selections, **not exhaustive news feeds**. An AI-generated summary is not independent fact-checking; medical interpretations should be checked against the original research.

## How it works

1. **Collect:** Find recent material from the configured sources.
2. **Select:** Filter and prioritize it to avoid flooding readers with low-value or repeated items.
3. **Explain:** Produce a short briefing with original links and selected Deep Dives.
4. **Deliver:** Send the appropriate edition by email and remember what was already processed.

NewsAgent2 runs automatically through GitHub Actions, usually on weekday mornings in the Europe/Stockholm time zone. Its two newsletters have separate source lists, editorial rules and recipients. Daily reports can also feed **Weekly, Monthly and Yearly** editions, built from saved material rather than presenting old items as new.

The project keeps records of processed content, saved summaries and email-delivery status. These reduce duplication, support longer-period reviews and make failures easier to investigate. A generated report and a successfully accepted email are tracked separately.

## Where the project came from

The original **NewsAgent** was a personal tool for gathering and summarizing online information. **NewsAgent2** developed that idea into two distinct newsletters: a research-focused medical report and a wider news briefing. Over time, it gained scheduled delivery, better source selection, saved material for longer editions, duplicate prevention and checks designed to avoid accidentally sending the same report twice.

## For developers and operators

The application uses **Python 3.12** and GitHub Actions. Newsletter configuration is stored in `data/`, production code in `src/newsagent2/`, and persisted report history in `state/`. Passwords, service keys and mailing lists belong in GitHub Secrets, not repository files.

Technical details are kept in separate guides:

- [Development and tests](docs/development.md)
- [Schedule and edition dates](docs/calendar-and-timing.md)
- [Delivery, failures and recovery](docs/delivery-recovery.md)
- [Operational checks](docs/operational-readiness.md)
- [Historical coverage and evidence](docs/history-coverage.md)
- [Cybermed source and clinical editorial policy](docs/clinical-editorial-policy.md)
- [Cyberlurch source attribution](docs/cyberlurch-source-basis.md)

This repository contains the software and configuration; newsletter delivery requires the operator's own credentials and recipient settings.
