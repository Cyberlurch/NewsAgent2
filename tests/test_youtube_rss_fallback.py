from __future__ import annotations

import datetime as dt
import json
import pathlib
import sys

SRC = pathlib.Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from newsagent2.collectors_youtube_rss import parse_youtube_rss
from newsagent2 import main as main_mod


SAMPLE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns:yt="http://www.youtube.com/xml/schemas/2015" xmlns="http://www.w3.org/2005/Atom">
  <title>Example Channel</title>
  <entry>
    <yt:videoId>abc123XYZ</yt:videoId>
    <title>Example Video</title>
    <link rel="alternate" href="https://www.youtube.com/watch?v=abc123XYZ"/>
    <author><name>Example Channel</name></author>
    <published>2026-05-14T12:00:00+00:00</published>
    <updated>2026-05-14T13:00:00+00:00</updated>
    <media:group xmlns:media="http://search.yahoo.com/mrss/"><media:description>Short summary</media:description></media:group>
  </entry>
  <entry>
    <yt:videoId>old456XYZ</yt:videoId>
    <title>Old Video</title>
    <link rel="alternate" href="https://www.youtube.com/watch?v=old456XYZ"/>
    <published>2026-05-10T12:00:00+00:00</published>
  </entry>
</feed>
"""


def test_youtube_rss_parser_parses_and_filters_by_lookback():
    now = dt.datetime(2026, 5, 15, 0, 0, tzinfo=dt.timezone.utc)
    items = parse_youtube_rss(SAMPLE_RSS, hours=36, now_utc=now)

    assert [it["id"] for it in items] == ["abc123XYZ"]
    assert items[0]["title"] == "Example Video"
    assert items[0]["url"] == "https://www.youtube.com/watch?v=abc123XYZ"
    assert items[0]["published_at"] == dt.datetime(2026, 5, 14, 12, 0, tzinfo=dt.timezone.utc)
    assert items[0]["_metadata_source"] == "youtube_rss"


def test_main_rss_fallback_retains_metadata_only_and_count_only_diagnostics(tmp_path, monkeypatch, capsys):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(
        json.dumps(
            {
                "topic_buckets": [
                    {
                        "topic": "test",
                        "channels": [
                            {"name": "Test Channel", "url": "https://www.youtube.com/channel/UCabcdefghijklmnopqrstuv"}
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    state_path = tmp_path / "state.json"
    report_dir = tmp_path / "out"
    video = {
        "id": "fallback123",
        "title": "Metadata Only Title",
        "channel": "Test Channel",
        "published_at": dt.datetime(2026, 5, 14, 12, 0, tzinfo=dt.timezone.utc),
        "url": "https://www.youtube.com/watch?v=fallback123",
        "description": "",
        "_metadata_source": "youtube_rss",
    }

    monkeypatch.setattr(main_mod, "list_recent_videos", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("blocked")))
    def fake_rss(*a, **k):
        diagnostics = k.get("diagnostics") or {}
        diagnostics["rss_fallback_attempted_total"] = diagnostics.get("rss_fallback_attempted_total", 0) + 1
        diagnostics["rss_fallback_success_total"] = diagnostics.get("rss_fallback_success_total", 0) + 1
        return [video]
    monkeypatch.setattr(main_mod, "list_recent_videos_rss", fake_rss)
    monkeypatch.setattr(main_mod, "fetch_transcript", lambda vid: None)
    monkeypatch.setattr(main_mod, "fetch_captions_via_timedtext", lambda *a, **k: ("", "empty"))
    monkeypatch.setattr(main_mod, "fetch_captions_text", lambda *a, **k: ("", "empty", ""))
    monkeypatch.setattr(main_mod, "summarize", lambda items, **k: "## Executive Summary\n\nTranscript content was unavailable for metadata-only items.")
    monkeypatch.setattr(main_mod, "summarize_item_detail", lambda item, **k: "Key takeaways:\n- Metadata only; no transcript available.")
    monkeypatch.setattr(main_mod, "send_markdown", lambda *a, **k: None)
    monkeypatch.setenv("REPORT_KEY", "cyberlurch")
    monkeypatch.setenv("REPORT_TITLE", "The Cyberlurch Report")
    monkeypatch.setenv("REPORT_SUBJECT", "The Cyberlurch Report")
    monkeypatch.setenv("REPORT_LANGUAGE", "en")
    monkeypatch.setenv("REPORT_PROFILE", "general")
    monkeypatch.setenv("REPORT_MODE", "daily")
    monkeypatch.setenv("REPORT_DIR", str(report_dir))
    monkeypatch.setenv("STATE_PATH", str(state_path))
    monkeypatch.setenv("SEND_EMAIL", "0")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(channels_path), "--hours", "36"])

    main_mod.main()
    captured = capsys.readouterr()

    assert "Metadata Only Title" not in captured.out
    assert "https://www.youtube.com/watch?v=fallback123" not in captured.out
    md = next(report_dir.glob("*.md")).read_text(encoding="utf-8")
    assert "Metadata Only Title" in md
    assert "Transcript/caption text unavailable; listed from metadata only." in md
    diag = json.loads((report_dir / "cyberlurch_youtube_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["metadata_only_total"] == 1
    assert diag["videos_skipped_empty_text_total"] == 0
    assert diag["rss_fallback_attempted_total"] >= 1
    forbidden_keys = {"title", "url", "description", "transcript", "captions"}
    assert forbidden_keys.isdisjoint(diag.keys())


def test_empty_cyberlurch_report_keeps_metadata_attachment_and_diagnostics_file(tmp_path, monkeypatch):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(
        json.dumps({"topic_buckets": [{"topic": "test", "channels": [{"name": "Empty", "url": "https://www.youtube.com/channel/UCabcdefghijklmnopqrstuv"}]}]}),
        encoding="utf-8",
    )
    report_dir = tmp_path / "out_empty"
    monkeypatch.setattr(main_mod, "list_recent_videos", lambda *a, **k: [])
    monkeypatch.setattr(main_mod, "list_recent_videos_rss", lambda *a, **k: [])
    monkeypatch.setattr(main_mod, "send_markdown", lambda *a, **k: None)
    monkeypatch.setenv("REPORT_KEY", "cyberlurch")
    monkeypatch.setenv("REPORT_TITLE", "The Cyberlurch Report")
    monkeypatch.setenv("REPORT_SUBJECT", "The Cyberlurch Report")
    monkeypatch.setenv("REPORT_LANGUAGE", "en")
    monkeypatch.setenv("REPORT_PROFILE", "general")
    monkeypatch.setenv("REPORT_MODE", "daily")
    monkeypatch.setenv("REPORT_DIR", str(report_dir))
    monkeypatch.setenv("STATE_PATH", str(tmp_path / "state_empty.json"))
    monkeypatch.setenv("SEND_EMAIL", "0")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(channels_path), "--hours", "36"])

    main_mod.main()

    md = next(report_dir.glob("*.md")).read_text(encoding="utf-8")
    assert "<!-- RUN_METADATA_ATTACHMENT_START -->" in md
    assert "## YouTube Diagnostics" in md
    diag = json.loads((report_dir / "cyberlurch_youtube_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["channels_attempted_total"] == 1
    assert diag["videos_listed_total"] == 0
    assert "captions_error_by_kind" in diag
