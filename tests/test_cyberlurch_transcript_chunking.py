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
