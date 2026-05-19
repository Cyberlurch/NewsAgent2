import pathlib, sys
SRC = pathlib.Path(__file__).resolve().parents[1] / 'src'
if str(SRC) not in sys.path: sys.path.insert(0, str(SRC))
from newsagent2.collectors_youtube import fetch_transcript

class FakeSnippet:
    def __init__(self,t): self.text=t
class FakeFetchedRaw:
    def to_raw_data(self): return [{'text':'hello'},{'text':'world'}]
class FakeFetchedSnippets:
    snippets=[FakeSnippet('foo'),FakeSnippet('bar')]
class FakeTranscript:
    def __init__(self,payload,is_generated=False): self.payload=payload; self.is_generated=is_generated
    def fetch(self): return self.payload
class FakeList:
    def __init__(self,t=None): self.t=t
    def find_manually_created_transcript(self,langs): return self.t
    def find_generated_transcript(self,langs): return self.t
    def __iter__(self): return iter([self.t] if self.t else [])

class FakeApi:
    def __init__(self, t): self.t=t
    def list(self, video_id): return FakeList(self.t)

def test_fetch_transcript_to_raw_data(monkeypatch):
    import newsagent2.collectors_youtube as m
    monkeypatch.setattr(m, 'YouTubeTranscriptApi', lambda: FakeApi(FakeTranscript(FakeFetchedRaw())))
    d={}
    assert fetch_transcript('x', diagnostics=d) == 'hello world'
    assert d['transcript_attempted_total']==1 and d['transcript_success_total']==1

def test_fetch_transcript_snippets(monkeypatch):
    import newsagent2.collectors_youtube as m
    monkeypatch.setattr(m, 'YouTubeTranscriptApi', lambda: FakeApi(FakeTranscript(FakeFetchedSnippets())))
    assert fetch_transcript('x') == 'foo bar'

def test_fetch_transcript_list_dict(monkeypatch):
    import newsagent2.collectors_youtube as m
    monkeypatch.setattr(m, 'YouTubeTranscriptApi', lambda: FakeApi(FakeTranscript([{'text':'a'},{'text':'b'}])))
    assert fetch_transcript('x') == 'a b'

def test_fetch_transcript_api_incompatible(monkeypatch, capsys):
    import newsagent2.collectors_youtube as m
    class BrokenApi:
        def list(self, video_id): raise AttributeError('list_transcripts removed for x')
    monkeypatch.setattr(m, 'YouTubeTranscriptApi', lambda: BrokenApi())
    d={}
    assert fetch_transcript('x', diagnostics=d) is None
    assert d['transcript_error_by_kind']['api_removed_or_incompatible']==1
    assert 'list_transcripts removed' not in capsys.readouterr().out
