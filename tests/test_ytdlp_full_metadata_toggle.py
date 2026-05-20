import datetime as dt
from unittest.mock import patch
import pathlib
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
from newsagent2 import collectors_youtube as cy


def _fake_ytdlp(entries):
    class Y:
        def __init__(self,*a,**k): pass
        def __enter__(self): return self
        def __exit__(self,*a): return False
        def extract_info(self, url, download=False):
            return {"entries": entries}
    return Y


def test_skip_full_metadata_by_default(monkeypatch):
    entries=[{"id":"v1","title":"t","timestamp": int(dt.datetime(2026,5,20,tzinfo=dt.timezone.utc).timestamp()),"description":"x"*200}]
    diag={}
    with patch.object(cy.yt_dlp,'YoutubeDL',_fake_ytdlp(entries)), patch.object(cy,'_fetch_full_video_metadata', return_value=None) as ff:
        cy.list_recent_videos('u',hours=48,max_items=1,diagnostics=diag, now_utc=dt.datetime(2026,5,20,tzinfo=dt.timezone.utc), force_full_metadata=False)
    assert ff.call_count == 0


def test_force_full_metadata_keeps_old_behavior(monkeypatch):
    entries=[{"id":"v1","title":"t","upload_date":"20260520","description":"x"*200}]
    diag={}
    with patch.object(cy.yt_dlp,'YoutubeDL',_fake_ytdlp(entries)), patch.object(cy,'_fetch_full_video_metadata', return_value={}) as ff:
        cy.list_recent_videos('u',hours=48,max_items=1,diagnostics=diag, now_utc=dt.datetime(2026,5,20,tzinfo=dt.timezone.utc), force_full_metadata=True)
    assert ff.call_count >= 1


def test_env_full_metadata_toggle_enabled(monkeypatch):
    monkeypatch.setenv("YTDLP_FULL_METADATA_ENRICHMENT", "1")
    entries=[{"id":"v1","title":"t","upload_date":"20260520","description":"x"*200}]
    with patch.object(cy.yt_dlp,'YoutubeDL',_fake_ytdlp(entries)), patch.object(cy,'_fetch_full_video_metadata', return_value={}) as ff:
        cy.list_recent_videos('u',hours=48,max_items=1,diagnostics={}, now_utc=dt.datetime(2026,5,20,tzinfo=dt.timezone.utc))
    assert ff.call_count >= 1
