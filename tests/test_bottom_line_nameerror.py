from src.newsagent2 import summarizer


class _DummyResponse:
    class Choice:
        class Message:
            content = "Bottom line"

        message = Message()

    choices = [Choice()]


class _DummyClient:
    class Chat:
        class Completions:
            @staticmethod
            def create(**kwargs):
                return _DummyResponse()

        completions = Completions()

    chat = Chat()


def test_summarize_pubmed_bottom_line_no_nameerror(monkeypatch):
    monkeypatch.setattr(summarizer, "_get_client", lambda: _DummyClient())
    item = {
        "source": "pubmed",
        "title": "Study",
        "journal": "J",
        "pmid": "123",
        "doi": "10.1/x",
        "published_at": "2026-05-20T12:00:00+00:00",
        "text": "This is a sufficiently detailed abstract text for safe summarization.",
    }
    out = summarizer.summarize_pubmed_bottom_line(item, language="en")
    assert "NameError" not in out


def test_summarize_cyberlurch_bottom_line_no_nameerror(monkeypatch):
    monkeypatch.setattr(summarizer, "_get_client", lambda: _DummyClient())
    item = {
        "type": "youtube",
        "title": "Video",
        "text": "placeholder",
        "transcript_full_summary": "This is a full transcript summary with enough length to exceed minimum threshold for summarization output.",
        "transcript_key_points": "Point A; Point B",
    }
    out = summarizer.summarize_cyberlurch_bottom_line(item, language="en")
    assert "NameError" not in out
