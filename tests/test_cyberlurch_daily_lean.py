import json
import pathlib, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
from types import SimpleNamespace
import pytest
from newsagent2 import reporter, summarizer
from newsagent2.utils import diagnostics


def item(i, *, deep=False, metadata=False):
    return {"id":str(i),"title":f"Title {i}","url":f"https://example.test/{i}","channel":f"Channel {i}","published_at":"2026-08-04T01:00:00Z","content_status":"metadata_only" if metadata else "full_text","text_source":"metadata_only" if metadata else "managed_transcript","cyberlurch_daily_deep_dive":deep,"transcript_key_points":[f"Alice approved action {i} for 25 sites", f"Decision {i} starts August 5"]}


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


def test_compatibility_has_one_retry(monkeypatch):
    calls=[]
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(**kw):
                    calls.append(kw)
                    if len(calls)==1: raise RuntimeError("response_format unsupported")
                    return response()
    monkeypatch.setattr(summarizer,"_get_client",lambda:C())
    d=diagnostics.reset_cyberlurch_openai_diagnostics()
    summarizer.summarize_youtube_transcript_direct({"text":"facts"})
    assert len(calls)==2 and d.fallback_attempts_total==1
