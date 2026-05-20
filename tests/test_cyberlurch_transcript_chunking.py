import pathlib
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
from newsagent2 import summarizer


def test_chunking_ordered_overlap():
    text = "".join(str(i % 10) for i in range(15000))
    chunks = summarizer._chunk_text_ordered(text, chunk_chars=6000, overlap_chars=500)
    assert len(chunks) >= 3
    assert chunks[0][-500:] == chunks[1][:500]


def test_overview_prefers_transcript_full_summary(monkeypatch):
    items = [{"source": "youtube", "title": "t", "text": "RAW", "transcript_full_summary": "FULLSUM", "content_status": "full_text", "text_source": "managed_transcript"}]
    slim = summarizer._slim_items(items, max_text_chars=6000)
    assert slim[0]["text"].startswith("FULLSUM")


def test_deep_dive_fallback_uses_env_limit(monkeypatch):
    captured = {}
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(model, messages, temperature):
                    captured['payload']=messages[-1]['content']
                    class R:
                        class choice:
                            class msg:
                                content='ok'
                        choices=[type('x',(object,),{'message':type('m',(object,),{'content':'ok'})()})()]
                    return R()
    monkeypatch.setattr(summarizer, '_get_client', lambda: C())
    monkeypatch.setenv('REPORT_KEY','cyberlurch')
    monkeypatch.setenv('CYBERLURCH_DEEPDIVE_MAX_TRANSCRIPT_CHARS','18000')
    txt='A'*25000
    summarizer.summarize_item_detail({'source':'youtube','text_source':'managed_transcript','text':txt,'title':'t','url':'u'}, language='en')
    assert 'A'*7000 in captured['payload']
    assert 'A'*20000 not in captured['payload']


def test_deep_dive_prefers_chunk_summary_fields(monkeypatch):
    captured = {}
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(model, messages, temperature):
                    captured["payload"] = messages[-1]["content"]
                    class R:
                        choices=[type('x',(object,),{'message':type('m',(object,),{'content':'ok'})()})()]
                    return R()
    monkeypatch.setattr(summarizer, "_get_client", lambda: C())
    monkeypatch.setenv("REPORT_KEY", "cyberlurch")
    raw = "RAWTRANSCRIPT " * 3000
    summarizer.summarize_item_detail(
        {
            "source": "youtube",
            "text_source": "managed_transcript",
            "text": raw,
            "title": "t",
            "url": "u",
            "transcript_full_summary": "FULL SUMMARY",
            "transcript_key_points": "- p1",
            "transcript_notable_claims": "- c1",
            "transcript_uncertainties": "- u1",
        },
        language="en",
    )
    assert "Full transcript summary:" in captured["payload"]
    assert "Notable claims:" in captured["payload"]
    assert "RAWTRANSCRIPT RAWTRANSCRIPT RAWTRANSCRIPT" not in captured["payload"]


def test_chunk_model_env_used(monkeypatch):
    captured = {}
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(model, messages, temperature):
                    captured.setdefault("models", []).append(model)
                    return type("R",(object,),{"choices":[type("x",(object,),{"message":type("m",(object,),{"content":"{}"})()})()]})()
    monkeypatch.setenv("OPENAI_MODEL", "base-model")
    monkeypatch.setenv("OPENAI_MODEL_CYBERLURCH_CHUNKS", "chunk-model")
    import importlib
    importlib.reload(summarizer)
    monkeypatch.setattr(summarizer, "_get_client", lambda: C())
    summarizer.summarize_youtube_transcript_chunks({"text":"X"*2000})
    assert captured["models"] and all(m == "chunk-model" for m in captured["models"])


def test_overview_and_deepdive_model_env_used(monkeypatch):
    captured = []
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(model, messages, temperature, **kwargs):
                    captured.append(model)
                    return type("R",(object,),{"choices":[type("x",(object,),{"message":type("m",(object,),{"content":"ok"})()})()]})()
    monkeypatch.setenv("REPORT_KEY", "cyberlurch")
    monkeypatch.setenv("OPENAI_MODEL", "base-model")
    monkeypatch.setenv("OPENAI_MODEL_CYBERLURCH_OVERVIEW", "overview-model")
    monkeypatch.setenv("OPENAI_MODEL_CYBERLURCH_DEEPDIVE", "deepdive-model")
    import importlib
    importlib.reload(summarizer)
    monkeypatch.setattr(summarizer, "_get_client", lambda: C())
    summarizer.summarize([{"source":"youtube","title":"t","text":"hello"}], language="en", profile="general")
    summarizer.summarize_item_detail({"source":"youtube","title":"t","text":"hello world "*20}, language="en", profile="general")
    assert "overview-model" in captured
    assert "deepdive-model" in captured


def test_cybermed_model_behavior_unchanged(monkeypatch):
    captured = []
    class C:
        class chat:
            class completions:
                @staticmethod
                def create(model, messages, temperature, **kwargs):
                    captured.append(model)
                    return type("R",(object,),{"choices":[type("x",(object,),{"message":type("m",(object,),{"content":"ok"})()})()]})()
    monkeypatch.setenv("REPORT_KEY", "cybermed")
    monkeypatch.setenv("OPENAI_MODEL", "base-model")
    monkeypatch.setenv("OPENAI_MODEL_CYBERLURCH_DEEPDIVE", "deepdive-model")
    import importlib
    importlib.reload(summarizer)
    monkeypatch.setattr(summarizer, "_get_client", lambda: C())
    summarizer.summarize_item_detail({"source":"youtube","title":"t","text":"hello world "*20}, language="en", profile="medical")
    assert "base-model" in captured
