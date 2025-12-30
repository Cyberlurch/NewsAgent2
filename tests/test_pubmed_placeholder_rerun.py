import json
from types import SimpleNamespace

from src.newsagent2 import summarizer


def test_count_pubmed_placeholder_fields_handles_empty_and_language():
    data = {
        "study_type": "Not reported",
        "population_setting": "  ",
        "intervention_comparator": "Placebo-controlled",
        "primary_endpoints": "nicht berichtet",
        "key_results": None,
        "why_this_matters": "   ",
    }

    assert summarizer._count_pubmed_placeholder_fields(data, lang="en") == 5
    assert summarizer._count_pubmed_placeholder_fields(data, lang="de") == 5


def test_placeholder_density_triggers_markdown_rerun(monkeypatch):
    placeholder_json = json.dumps(
        {
            "bottom_line": "Not reported",
            "study_type": "Not reported",
            "population_setting": "Not reported",
            "intervention_comparator": "Not reported",
            "primary_endpoints": "Not reported",
            "key_results": "Not reported",
            "limitations": ["Small sample"],
            "why_this_matters": "Not reported",
        }
    )

    fallback_md = "\n".join(
        [
            "BOTTOM LINE: Clear effect observed",
            "- Study type: RCT",
            "- Population/setting: Adults in ICU",
            "- Intervention/exposure & comparator: Drug vs placebo",
            "- Primary endpoints: 28-day mortality",
            "- Key results: Lower mortality with intervention",
            "- Limitations:",
            "- Single center",
            "- Why this matters: Could guide practice",
        ]
    )

    class DummyCompletions:
        def __init__(self, responses):
            self._responses = responses
            self.calls = []

        def create(self, **kwargs):
            self.calls.append(kwargs)
            content = self._responses[len(self.calls) - 1]
            return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=content))])

    class DummyClient:
        def __init__(self, responses):
            self.completions = DummyCompletions(responses)
            self.chat = SimpleNamespace(completions=self.completions)

    responses = [placeholder_json, fallback_md]
    dummy_client = DummyClient(responses)

    monkeypatch.setattr(summarizer, "_get_client", lambda: dummy_client)

    item = {
        "source": "pubmed",
        "title": "Example",
        "pmid": "12345",
        "abstract": "Example abstract text",
        "text": "Example abstract text",
        "published_at": None,
    }

    detail_md = summarizer.summarize_item_detail(item, language="en", profile="medical")

    assert "BOTTOM LINE" in detail_md
    assert item.get("_deep_dive_placeholder_rerun") is True
    assert item.get("_deep_dive_used_markdown_fallback") is True
    assert item.get("_deep_dive_placeholder_value_count") == 0
    assert len(dummy_client.completions.calls) == 2


def test_best_effort_rescue_improves_placeholder_density(monkeypatch):
    placeholder_json = json.dumps(
        {
            "bottom_line": "Not reported",
            "study_type": "Not reported",
            "population_setting": "Not reported",
            "intervention_comparator": "Not reported",
            "primary_endpoints": "Not reported",
            "key_results": "Not reported",
            "limitations": ["Not reported"],
            "why_this_matters": "Not reported",
        }
    )

    placeholder_md = "\n".join(
        [
            "BOTTOM LINE: Not reported",
            "- Study type: Not reported",
            "- Population/setting: Not reported",
            "- Intervention/exposure & comparator: Not reported",
            "- Primary endpoints: Not reported",
            "- Key results: Not reported",
            "- Limitations:",
            "- Not reported",
            "- Why this matters: Not reported",
        ]
    )

    rescue_md = "\n".join(
        [
            "BOTTOM LINE: Observational data suggest benefit",
            "- Study type: Prospective cohort study at tertiary center",
            "- Population/setting: Adults with acute condition",
            "- Intervention/exposure & comparator: Treatment A vs standard care",
            "- Primary endpoints: 30-day survival",
            "- Key results: Survival higher with Treatment A; no major safety signal reported",
            "- Limitations:",
            "- Single-center design, potential confounding",
            "- Why this matters: Points to possible benefit pending randomized trials",
        ]
    )

    class DummyCompletions:
        def __init__(self, responses):
            self._responses = responses
            self.calls = []

        def create(self, **kwargs):
            self.calls.append(kwargs)
            content = self._responses[len(self.calls) - 1]
            return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=content))])

    class DummyClient:
        def __init__(self, responses):
            self.completions = DummyCompletions(responses)
            self.chat = SimpleNamespace(completions=self.completions)

    responses = [placeholder_json, placeholder_md, rescue_md]
    dummy_client = DummyClient(responses)

    monkeypatch.setattr(summarizer, "_get_client", lambda: dummy_client)

    item = {
        "source": "pubmed",
        "title": "Example",
        "pmid": "54321",
        "abstract": "Signals possible benefit based on cohort data.",
        "text": "Signals possible benefit based on cohort data.",
        "published_at": None,
    }

    detail_md = summarizer.summarize_item_detail(item, language="en", profile="medical")

    assert "BOTTOM LINE" in detail_md
    assert item.get("_deep_dive_best_effort_attempted") is True
    assert item.get("_deep_dive_best_effort_used") is True
    assert item.get("_deep_dive_placeholder_value_count") < 5
    assert len(dummy_client.completions.calls) == 3
