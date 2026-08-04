import json
import pathlib, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
from types import SimpleNamespace
import pytest
from newsagent2 import main, reporter, summarizer
from newsagent2.utils import diagnostics


def item(i, *, deep=False, metadata=False):
    return {"id":str(i),"title":f"Title {i}","url":f"https://example.test/{i}","channel":f"Channel {i}","published_at":"2026-08-04T01:00:00Z","content_status":"metadata_only" if metadata else "full_text","text_source":"metadata_only" if metadata else "managed_transcript","cyberlurch_daily_deep_dive":deep,"transcript_key_points":[f"Alice approved action {i} for 25 sites", f"Decision {i} starts August 5"]}


def test_metadata_preselection_caps_and_orders_deterministically():
    from datetime import datetime, timedelta, timezone
    now = datetime(2026, 8, 4, tzinfo=timezone.utc)
    candidates = [
        {"id": f"v{i:02d}", "channel": "Priority" if i in {20, 21} else "Ordinary", "published_at": now - timedelta(minutes=i)}
        for i in range(30)
    ]
    selected, cap = main._preselect_cyberlurch_daily_metadata(
        candidates, priority_channels={"Priority"}, selected_max=12, deep_dive_max=2
    )
    assert cap == 14
    assert len(selected) == 14
    assert [row["id"] for row in selected[:2]] == ["v20", "v21"]
    assert [row["id"] for row in selected[2:]] == [f"v{i:02d}" for i in range(12)]


def test_daily_renderer_caps_and_deduplicates():
    items=[item(i, deep=i<4) for i in range(15)]
    md=reporter.render_cyberlurch_daily_report(items,title="Cyberlurch",generated_at="now")
    assert md.count("## Executive Snapshot") == 1
    snapshot=md.split("## Selected items")[0]
    assert snapshot.count("- [") == 5
    assert md.count("## Deep Dives") == 1
    assert len(md.split()) <= 1600
    assert "Top videos" not in md and "Themenbereiche" not in md and "Why it matters" not in md
    assert sum(md.count(f"### [Title {i}]") for i in range(15)) == 12


def test_metadata_only_render_is_compact():
    md=reporter.render_cyberlurch_daily_report([item(1,metadata=True)],title="Cyberlurch",generated_at="now")
    assert md.count("Metadata only.") == 1


def response(content='{"transcript_full_summary":"Alice approved 25 sites.","transcript_key_points":["Alice approved 25 sites on August 5"],"editorial_relevance":""}'):
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=content))],usage=SimpleNamespace(prompt_tokens=10,completion_tokens=5,total_tokens=15))


def test_one_direct_call_and_count_only_diagnostics(monkeypatch):
    calls=[]
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw): calls.append(kw); return response()
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    d=diagnostics.reset_cyberlurch_openai_diagnostics()
    out=summarizer.summarize_youtube_transcript_direct({"text":"Alice approved 25 sites on August 5."},language="en")
    assert len(calls)==1 and d.call_success_total==1
    assert "Alice" in str(out["transcript_key_points"])
    payload=json.dumps(d.to_dict())
    assert "Alice" not in payload and "example.test" not in payload
    assert d.to_dict()["calls_by_stage_model"]


def test_gpt5_mini_request_omits_temperature_and_keeps_json(monkeypatch):
    calls=[]
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw): calls.append(kw); return response()
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    monkeypatch.setattr(summarizer,"OPENAI_MODEL_CYBERLURCH_DIRECT_DIGEST","gpt-5-mini")
    d=diagnostics.reset_cyberlurch_openai_diagnostics()
    summarizer.summarize_youtube_transcript_direct({"title":"Original title","text":"facts"},language="en")
    assert len(calls)==1
    assert calls[0]["model"]=="gpt-5-mini"
    assert "temperature" not in calls[0]
    assert calls[0]["response_format"]=={"type":"json_object"}
    assert calls[0]["reasoning_effort"]=="minimal"
    assert d.fallback_attempts_total==0


def test_prompt_uses_title_and_explicit_item_language(monkeypatch):
    prompts=[]
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw): prompts.append(kw["messages"][1]["content"]); return response()
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    for code, label in (("de","German"),("sv","Swedish"),("en","English")):
        summarizer.summarize_youtube_transcript_direct({"title":f"Title {code}","text":"facts","language":code},language="en")
        assert f"Original title (context only): Title {code}" in prompts[-1]
        assert f"Output language: {label}" in prompts[-1]


@pytest.mark.parametrize("code", ["he", "heb", "ar", "es", "fr"])
def test_explicit_unsupported_item_language_forces_english(monkeypatch, code):
    calls=[]
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw): calls.append(kw); return response()
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    summarizer.summarize_youtube_transcript_direct({"title":"Title","text":"facts","language":code})
    assert "Output language: English" in calls[0]["messages"][1]["content"]


@pytest.mark.parametrize("text", [
    "היחידה המובחרת של צהל התגייסה היום מתוך אמונה ומוטיבציה גבוהה לשירות משמעותי ולהגנת המדינה",
    "ناقش التقرير التطورات السياسية والأمنية الجديدة وتأثيرها على السكان في المنطقة خلال الأيام المقبلة",
    "В репортаже подробно обсуждаются новые политические решения и их последствия для жителей региона",
])
def test_dominant_unsupported_script_forces_english(monkeypatch, text):
    calls=[]
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw): calls.append(kw); return response()
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    summarizer.summarize_youtube_transcript_direct({"title":"An English report title","text":text})
    assert "Output language: English" in calls[0]["messages"][1]["content"]


def test_short_non_latin_fragment_does_not_override_same_call_detection(monkeypatch):
    calls=[]
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw): calls.append(kw); return response()
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    text="The report discusses the Hebrew name ירושלים in an otherwise entirely English factual account."
    summarizer.summarize_youtube_transcript_direct({"title":"English title","text":text})
    prompt=calls[0]["messages"][1]["content"]
    assert "detect the dominant language from the original title" in prompt
    assert "Output language: English" not in prompt


def test_extraction_language_contract_is_in_system_and_user_messages(monkeypatch):
    calls=[]
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw): calls.append(kw); return response()
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    summarizer.summarize_youtube_transcript_direct({"title":"Title","text":"facts"})
    system, user = (message["content"] for message in calls[0]["messages"])
    for prompt in (system, user):
        assert "only English, German, or Swedish" in prompt
        assert "anything else" in prompt and "in English" in prompt
        assert "Hebrew, Arabic, Cyrillic, Chinese" in prompt
        assert "original title unchanged" in prompt


@pytest.mark.parametrize("title,text", [
    ("Rostock: Immer mehr Schiffswracks", "Die Stadt sucht eine konkrete Lösung."),
    ("AIK & Hammarby gick i Pride", "Fotbollen diskuterades som politisk."),
])
def test_production_shaped_item_auto_detects_language_in_same_call(monkeypatch,title,text):
    calls=[]
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw): calls.append(kw); return response()
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    summarizer.summarize_youtube_transcript_direct({"title":title,"text":text},language="en")
    prompt=calls[0]["messages"][1]["content"]
    assert "detect the dominant language from the original title and supplied transcript/description" in prompt
    assert "Output language: English" not in prompt
    assert len(calls)==1


def test_extraction_prompt_requires_complete_facts_and_filters_promotion(monkeypatch):
    calls=[]
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw): calls.append(kw); return response()
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    summarizer.summarize_youtube_transcript_direct({"title":"Rostock ship decision","text":"Three resolves were listed."})
    system=calls[0]["messages"][0]["content"]
    prompt=calls[0]["messages"][1]["content"]
    assert "complete self-contained sentence" in system
    assert "its own explicit subject or named noun phrase" in system
    assert "Exclude routine promotion and source publication or upload metadata" in system
    assert "standalone grammatical factual sentences" in prompt
    assert "its own explicit subject or named noun phrase" in prompt
    assert "Grew up ..." in prompt and "Started ..." in prompt
    assert "omit a fact if it cannot be written as a complete self-contained sentence" in prompt
    assert "Exclude broadcast or programme times" in prompt
    assert "routine programme housekeeping" in prompt
    assert "unless that information is itself the substantive subject" in prompt
    assert "funding or financial-support information" in prompt
    assert "donation links or financial information are provided" in prompt
    assert "upload, publication, or release date" in prompt
    assert "Keep dates for substantive events, actions, decisions, or deadlines" in prompt
    assert "preserve the title's spelling when the transcript conflicts" in prompt


@pytest.mark.parametrize("message,status", [("insufficient_quota",429),("authentication failed",401),("timed out",429)])
def test_protected_errors_do_not_fallback(monkeypatch,message,status):
    calls=[]
    class E(Exception):
        status_code=status
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw): calls.append(kw); raise E(message)
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    diagnostics.reset_cyberlurch_openai_diagnostics()
    with pytest.raises(E): summarizer.summarize_youtube_transcript_direct({"text":"facts"})
    assert len(calls)==1


def test_unsupported_reasoning_effort_has_one_same_model_retry(monkeypatch):
    calls=[]
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw):
                    calls.append(kw)
                    if len(calls)==1: raise RuntimeError("reasoning_effort is unsupported")
                    return response()
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    d=diagnostics.reset_cyberlurch_openai_diagnostics()
    summarizer.summarize_youtube_transcript_direct({"text":"facts"})
    assert len(calls)==2 and d.fallback_attempts_total==1
    assert "reasoning_effort" not in calls[1]
    assert calls[1]["response_format"]=={"type":"json_object"}
    assert calls[1]["model"] == calls[0]["model"]


def test_generic_http_400_does_not_fallback_or_leak(monkeypatch):
    calls=[]
    secret="raw-title-and-body-secret"
    class E(Exception): status_code=400
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw): calls.append(kw); raise E(secret)
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    d=diagnostics.reset_cyberlurch_openai_diagnostics()
    with pytest.raises(E):
        summarizer.summarize_youtube_transcript_direct({"text":"facts"})
    assert len(calls)==1 and d.fallback_attempts_total==0
    assert secret not in json.dumps(d.to_dict())


def test_direct_character_count_counts_each_transmission(monkeypatch):
    calls=[]
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw):
                    calls.append(kw)
                    if len(calls)==1: raise RuntimeError("reasoning_effort is unsupported")
                    return response()
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    assert summarizer.summarize_youtube_transcript_direct({"text":"facts"})["chars_processed_total"]==10


def test_arrays_and_reasoning_usage_are_preserved(monkeypatch):
    calls=[]
    payload='{"transcript_full_summary":"Zusammenfassung.","transcript_key_points":["Erster Fakt.","Zweiter Fakt."],"important_details":["Detail."],"editorial_relevance":""}'
    result=response(payload)
    result.usage.completion_tokens_details=SimpleNamespace(reasoning_tokens=2)
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw): calls.append(kw); return result
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    monkeypatch.setattr(summarizer,"OPENAI_MODEL_CYBERLURCH_DIRECT_DIGEST","gpt-5-mini")
    d=diagnostics.reset_cyberlurch_openai_diagnostics()
    out=summarizer.summarize_youtube_transcript_direct({"text":"Fakten","language":"de"})
    assert out["transcript_key_points"]==["Erster Fakt.","Zweiter Fakt."]
    totals=d.to_dict()
    assert totals["output_tokens"]==5
    assert totals["reasoning_tokens"]==2
    assert totals["non_reasoning_output_tokens"]==3


def test_usage_without_reasoning_details_records_zero(monkeypatch):
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw): return response()
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    d=diagnostics.reset_cyberlurch_openai_diagnostics()
    summarizer.summarize_youtube_transcript_direct({"text":"facts"})
    totals=d.to_dict()
    assert totals["reasoning_tokens"]==0
    assert totals["output_tokens"]==totals["non_reasoning_output_tokens"]


def test_deep_dive_deduplicates_internal_fact():
    it=item(1,deep=True)
    repeated="Alice approved 25 sites on August 5."
    it.update(transcript_full_summary=repeated, transcript_key_points=[repeated], important_details=["- alice approved 25 sites on August 5"], transcript_notable_claims=[repeated])
    md=reporter.render_cyberlurch_daily_report([it],title="Cyberlurch",generated_at="now")
    deep=md.split("## Deep Dives",1)[1]
    assert deep.casefold().count("alice approved 25 sites on august 5") == 1


def test_deep_dive_prefers_details_and_summary_only_is_fallback():
    it=item(1,deep=True)
    broad="The council approved 25 sites, selected Berlin, and starts work on August 5."
    it.update(transcript_full_summary=broad, transcript_key_points=["The council approved 25 sites.", "The council selected Berlin."], important_details=["Work starts on August 5."], transcript_notable_claims=[])
    md=reporter.render_cyberlurch_daily_report([it],title="Cyberlurch",generated_at="now")
    deep=md.split("## Deep Dives",1)[1]
    assert broad not in deep
    assert deep.count("approved 25 sites") == deep.count("selected Berlin") == deep.count("starts on August 5") == 1
    assert sum(line.startswith("- ") for line in deep.splitlines()) <= 5
    legacy=item(2,deep=True); legacy["transcript_key_points"]=[]; legacy["transcript_full_summary"]="Legacy fallback summary."
    legacy_md=reporter.render_cyberlurch_daily_report([legacy],title="Cyberlurch",generated_at="now")
    assert "- Legacy fallback summary." in legacy_md


def test_daily_renderer_drops_only_clear_boilerplate_and_subjectless_fragments():
    it=item(1,deep=True)
    it.update(
        transcript_full_summary="LIONMedia reported two concrete biographical facts.",
        transcript_key_points=[
            "LIONMedia documented the subject's work in New York.",
            "Finanzinformationen und Spendenlinks werden angegeben.",
            "Datum der Veröffentlichung ist 04. August 2026.",
        ],
        important_details=[
            "Grew up in Queens before moving to Manhattan.",
            "Started work at the firm in 1998.",
            "The subject moved to Manhattan in 1995.",
        ],
        transcript_notable_claims=[],
    )
    md=reporter.render_cyberlurch_daily_report([it],title="Cyberlurch",generated_at="now")
    assert "LIONMedia documented the subject's work in New York." in md
    assert "The subject moved to Manhattan in 1995." in md
    assert "Finanzinformationen und Spendenlinks" not in md
    assert "Datum der Veröffentlichung" not in md
    assert "Grew up" not in md and "Started work" not in md


def test_daily_renderer_keeps_substantive_donations_and_event_dates():
    it=item(1)
    it["transcript_key_points"]=[
        "The charity raised $2 million for emergency shelters.",
        "The council starts construction on August 5.",
        "The publication date is August 4, which triggered the statutory review period.",
    ]
    md=reporter.render_cyberlurch_daily_report([it],title="Cyberlurch",generated_at="now")
    assert "The charity raised $2 million for emergency shelters." in md
    assert "The council starts construction on August 5." in md
    assert "The publication date is August 4, which triggered the statutory review period." in md


@pytest.mark.parametrize("fact", [
    "Financial information and donation links are provided.",
    "Finansieringsinformation och donationslänkar anges.",
    "The publication date is August 4, 2026.",
    "Publiceringsdatumet är den 4 augusti 2026.",
])
def test_daily_fact_filter_covers_supported_output_languages(fact):
    assert not reporter._is_usable_cyberlurch_fact(fact)


def test_legacy_fact_formats_render_without_python_list_repr():
    it=item(1); it["transcript_key_points"]="['Erster vollständiger Satz.', 'Zweiter vollständiger Satz.']"
    md=reporter.render_cyberlurch_daily_report([it],title="Cyberlurch",generated_at="now")
    assert "- Erster vollständiger Satz." in md and "- Zweiter vollständiger Satz." in md
    assert "['" not in md


def test_german_and_swedish_values_render_unchanged():
    items=[item(1),item(2)]
    items[0]["transcript_key_points"]=["Die Stadt sucht eine konkrete Lösung."]
    items[1]["transcript_key_points"]=["Fotbollen diskuterades som politisk."]
    md=reporter.render_cyberlurch_daily_report(items,title="Cyberlurch",generated_at="now")
    assert "Die Stadt sucht eine konkrete Lösung." in md
    assert "Fotbollen diskuterades som politisk." in md


def test_twelve_non_chunked_items_make_at_most_twelve_attempts(monkeypatch):
    calls=[]
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw): calls.append(kw); return response()
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    d=diagnostics.reset_cyberlurch_openai_diagnostics()
    for i in range(12):
        summarizer.summarize_youtube_transcript_direct({"title":f"Title {i}","text":f"fact {i}"},language="en")
    assert len(calls)==12
    assert d.call_attempts_total==d.call_success_total==12
    assert d.call_error_total==d.fallback_attempts_total==0
