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
    monkeypatch.setattr(main_mod, "fetch_video_content", lambda **k: type("R", (), {"status":"empty","text":"","source":"metadata_only"})())
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


def test_rss_primary_with_channel_id_skips_ytdlp_listing(tmp_path, monkeypatch):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps({"topic_buckets": [{"topic": "t", "channels": [{"name": "C", "url": "https://youtube.com/@c", "channel_id": "UCabc"}]}]}), encoding="utf-8")
    report_dir = tmp_path / "out_rss_primary"
    calls = {"ytdlp": 0}
    monkeypatch.setattr(main_mod, "list_recent_videos", lambda *a, **k: calls.__setitem__("ytdlp", calls["ytdlp"] + 1) or [])
    monkeypatch.setattr(main_mod, "list_recent_videos_rss", lambda *a, **k: [{"id": "v1", "title": "t", "channel": "c", "published_at": dt.datetime(2026, 5, 14, 12, 0, tzinfo=dt.timezone.utc), "url": "https://www.youtube.com/watch?v=v1", "description": ""}])
    monkeypatch.setattr(main_mod, "fetch_video_content", lambda **k: type("R", (), {"status":"empty","text":"","source":"metadata_only"})())
    monkeypatch.setattr(main_mod, "summarize", lambda *a, **k: "sum")
    monkeypatch.setattr(main_mod, "summarize_item_detail", lambda *a, **k: "detail")
    monkeypatch.setattr(main_mod, "send_markdown", lambda *a, **k: None)
    monkeypatch.setenv("REPORT_KEY", "cyberlurch"); monkeypatch.setenv("REPORT_MODE", "daily"); monkeypatch.setenv("REPORT_DIR", str(report_dir)); monkeypatch.setenv("STATE_PATH", str(tmp_path / "s.json")); monkeypatch.setenv("SEND_EMAIL", "0"); monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(channels_path), "--hours", "36"])
    main_mod.main()
    assert calls["ytdlp"] == 0


def test_bot_check_disables_subsequent_captions_calls(tmp_path, monkeypatch, capsys):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps({"topic_buckets": [{"topic": "t", "channels": [{"name": "C", "url": "https://youtube.com/@c"}]}]}), encoding="utf-8")
    report_dir = tmp_path / "out_bot"
    vids = [{"id": "id1", "title": "A", "channel": "c", "published_at": dt.datetime(2026, 5, 14, 12, 0, tzinfo=dt.timezone.utc), "url": "https://www.youtube.com/watch?v=id1", "description": ""}, {"id": "id2", "title": "B", "channel": "c", "published_at": dt.datetime(2026, 5, 14, 12, 0, tzinfo=dt.timezone.utc), "url": "https://www.youtube.com/watch?v=id2", "description": ""}]
    monkeypatch.setattr(main_mod, "list_recent_videos", lambda *a, **k: vids)
    counter = {"n": 0}
    def _provider(**k):
        counter["n"] += 1
        if counter["n"] == 1:
            return type("R", (), {"status":"error","text":"","source":"yt_dlp_captions","error_kind":"bot_check"})()
        return type("R", (), {"status":"empty","text":"","source":"metadata_only","error_kind":""})()
    monkeypatch.setattr(main_mod, "fetch_video_content", _provider)
    monkeypatch.setattr(main_mod, "summarize", lambda *a, **k: "sum")
    monkeypatch.setattr(main_mod, "summarize_item_detail", lambda *a, **k: "detail")
    monkeypatch.setattr(main_mod, "send_markdown", lambda *a, **k: None)
    monkeypatch.setenv("REPORT_KEY", "cyberlurch"); monkeypatch.setenv("REPORT_MODE", "daily"); monkeypatch.setenv("REPORT_DIR", str(report_dir)); monkeypatch.setenv("STATE_PATH", str(tmp_path / "s2.json")); monkeypatch.setenv("SEND_EMAIL", "0"); monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(channels_path), "--hours", "36"])
    main_mod.main()
    out = capsys.readouterr().out
    assert counter["n"] == 2
    assert "sign in to confirm" not in out.lower()
    assert "watch?v=id1" not in out and "id1" not in out


def test_transcript_success_sets_full_text_source(tmp_path, monkeypatch):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps({"topic_buckets":[{"topic":"t","channels":[{"name":"C","url":"https://youtube.com/@c"}]}]}), encoding="utf-8")
    report_dir = tmp_path / "out_tr"
    vids=[{"id":"ok1","title":"T","channel":"c","published_at":dt.datetime(2026,5,14,12,0,tzinfo=dt.timezone.utc),"url":"https://www.youtube.com/watch?v=ok1","description":""}]
    monkeypatch.setattr(main_mod, "list_recent_videos", lambda *a, **k: vids)
    monkeypatch.setattr(main_mod, "fetch_video_content", lambda **k: type("R", (), {"status":"success","text":"real transcript "*500,"source":"youtube_transcript_api"})())
    monkeypatch.setattr(main_mod, "summarize", lambda *a, **k: "sum")
    monkeypatch.setattr(main_mod, "summarize_item_detail", lambda *a, **k: "detail")
    monkeypatch.setattr(main_mod, "send_markdown", lambda *a, **k: None)
    monkeypatch.setenv("REPORT_KEY", "cyberlurch"); monkeypatch.setenv("REPORT_MODE", "daily"); monkeypatch.setenv("REPORT_DIR", str(report_dir)); monkeypatch.setenv("STATE_PATH", str(tmp_path / "s3.json")); monkeypatch.setenv("SEND_EMAIL", "0"); monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(channels_path), "--hours", "36"])
    main_mod.main()
    md = next(report_dir.glob("*.md")).read_text(encoding="utf-8")
    assert "Deep dives skipped" not in md


def test_metadata_only_skips_deep_dives(tmp_path, monkeypatch):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps({"topic_buckets":[{"topic":"t","channels":[{"name":"C","url":"https://youtube.com/@c"}]}]}), encoding="utf-8")
    report_dir = tmp_path / "out_meta"
    vids=[{"id":"m1","title":"Meta","channel":"c","published_at":dt.datetime(2026,5,14,12,0,tzinfo=dt.timezone.utc),"url":"https://www.youtube.com/watch?v=m1","description":""}]
    monkeypatch.setattr(main_mod, "list_recent_videos", lambda *a, **k: vids)
    monkeypatch.setattr(main_mod, "fetch_video_content", lambda **k: type("R", (), {"status":"empty","text":"","source":"metadata_only"})())
    monkeypatch.setattr(main_mod, "summarize", lambda *a, **k: "## Executive Summary\n\nmeta")
    calls={"n":0}
    def _detail(*a, **k): calls["n"] += 1; return "detail"
    monkeypatch.setattr(main_mod, "summarize_item_detail", _detail)
    monkeypatch.setattr(main_mod, "send_markdown", lambda *a, **k: None)
    monkeypatch.setenv("REPORT_KEY", "cyberlurch"); monkeypatch.setenv("REPORT_MODE", "daily"); monkeypatch.setenv("REPORT_DIR", str(report_dir)); monkeypatch.setenv("STATE_PATH", str(tmp_path / "s4.json")); monkeypatch.setenv("SEND_EMAIL", "0"); monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(channels_path), "--hours", "36"])
    main_mod.main()
    md = next(report_dir.glob("*.md")).read_text(encoding="utf-8")
    assert "Deep dives skipped because no transcript, caption, or description content was available." in md
    assert "Details & reasoning" not in md
    assert calls["n"] == 0


def test_chunking_failure_does_not_break_overview(tmp_path, monkeypatch):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps({"topic_buckets":[{"topic":"t","channels":[{"name":"C","url":"https://youtube.com/@c"}]}]}), encoding="utf-8")
    report_dir = tmp_path / "out_chunk"
    long_text = "X" * 9000
    vids=[{"id":"c1","title":"Chunk","channel":"c","published_at":dt.datetime(2026,5,14,12,0,tzinfo=dt.timezone.utc),"url":"https://www.youtube.com/watch?v=c1","description":""}]
    monkeypatch.setattr(main_mod, "list_recent_videos", lambda *a, **k: vids)
    monkeypatch.setattr(main_mod, "fetch_video_content", lambda **k: type("R", (), {"status":"success","text":long_text,"source":"managed_transcript"})())
    monkeypatch.setenv("CYBERLURCH_DIRECT_TRANSCRIPT_MAX_CHARS", "8000")
    monkeypatch.setenv("CYBERLURCH_TRANSCRIPT_CHUNKING_MIN_CHARS", "8000")
    monkeypatch.setattr(main_mod, "summarize_youtube_transcript_chunks", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(main_mod, "summarize", lambda *a, **k: "## Executive Summary\n\nok")
    monkeypatch.setattr(main_mod, "summarize_item_detail", lambda *a, **k: "detail")
    monkeypatch.setattr(main_mod, "send_markdown", lambda *a, **k: None)
    monkeypatch.setenv("REPORT_KEY", "cyberlurch"); monkeypatch.setenv("REPORT_MODE", "daily"); monkeypatch.setenv("REPORT_DIR", str(report_dir)); monkeypatch.setenv("STATE_PATH", str(tmp_path / "s5.json")); monkeypatch.setenv("SEND_EMAIL", "0"); monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(channels_path), "--hours", "36"])
    main_mod.main()
    md = next(report_dir.glob("*.md")).read_text(encoding="utf-8")
    assert "Failed to generate overview" not in md
    diag = json.loads((report_dir / "cyberlurch_youtube_diagnostics.json").read_text(encoding="utf-8"))
    assert "transcript_chunking_attempted_total" in diag
    assert "transcript_chunking_error_total" in diag


def test_chunking_not_needed_counter_for_short_transcript(tmp_path, monkeypatch):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps({"topic_buckets":[{"topic":"t","channels":[{"name":"C","url":"https://youtube.com/@c"}]}]}), encoding="utf-8")
    report_dir = tmp_path / "out_short"
    short_text = "X" * 3000
    vids=[{"id":"c2","title":"Short","channel":"c","published_at":dt.datetime(2026,5,14,12,0,tzinfo=dt.timezone.utc),"url":"https://www.youtube.com/watch?v=c2","description":""}]
    monkeypatch.setattr(main_mod, "list_recent_videos", lambda *a, **k: vids)
    monkeypatch.setattr(main_mod, "fetch_video_content", lambda **k: type("R", (), {"status":"success","text":short_text,"source":"managed_transcript"})())
    monkeypatch.setattr(main_mod, "summarize_youtube_transcript_direct", lambda *a, **k: {"transcript_full_summary": "FULL", "chars_processed_total": len(short_text)})
    monkeypatch.setattr(main_mod, "summarize", lambda *a, **k: "## Executive Summary\n\nok")
    monkeypatch.setattr(main_mod, "summarize_item_detail", lambda *a, **k: "detail")
    monkeypatch.setattr(main_mod, "send_markdown", lambda *a, **k: None)
    monkeypatch.setenv("REPORT_KEY", "cyberlurch"); monkeypatch.setenv("REPORT_MODE", "daily"); monkeypatch.setenv("REPORT_DIR", str(report_dir)); monkeypatch.setenv("STATE_PATH", str(tmp_path / "s6.json")); monkeypatch.setenv("SEND_EMAIL", "0"); monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(channels_path), "--hours", "36"])
    main_mod.main()
    diag = json.loads((report_dir / "cyberlurch_youtube_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["transcript_direct_attempted_total"] >= 1
    assert diag["transcript_direct_success_total"] >= 1
    assert diag["transcript_processing_direct_total"] >= 1


def test_long_managed_transcript_uses_chunking_path(tmp_path, monkeypatch):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps({"topic_buckets":[{"topic":"t","channels":[{"name":"C","url":"https://youtube.com/@c"}]}]}), encoding="utf-8")
    report_dir = tmp_path / "out_long_chunk"
    long_text = "Y" * 90000
    vids=[{"id":"c3","title":"Long","channel":"c","published_at":dt.datetime(2026,5,14,12,0,tzinfo=dt.timezone.utc),"url":"https://www.youtube.com/watch?v=c3","description":""}]
    monkeypatch.setattr(main_mod, "list_recent_videos", lambda *a, **k: vids)
    monkeypatch.setattr(main_mod, "fetch_video_content", lambda **k: type("R", (), {"status":"success","text":long_text,"source":"managed_transcript"})())
    monkeypatch.setattr(main_mod, "summarize_youtube_transcript_chunks", lambda *a, **k: {"transcript_full_summary": "chunked", "chunks_total": 2, "chars_processed_total": len(long_text)})
    monkeypatch.setattr(main_mod, "summarize", lambda *a, **k: "## Executive Summary\n\nok")
    monkeypatch.setattr(main_mod, "summarize_item_detail", lambda *a, **k: "detail")
    monkeypatch.setattr(main_mod, "send_markdown", lambda *a, **k: None)
    monkeypatch.setenv("REPORT_KEY", "cyberlurch"); monkeypatch.setenv("REPORT_MODE", "daily"); monkeypatch.setenv("REPORT_DIR", str(report_dir)); monkeypatch.setenv("STATE_PATH", str(tmp_path / "s7.json")); monkeypatch.setenv("SEND_EMAIL", "0"); monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(channels_path), "--hours", "36"])
    main_mod.main()
    diag = json.loads((report_dir / "cyberlurch_youtube_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["transcript_chunking_attempted_total"] >= 1
    assert diag["transcript_processing_chunked_total"] >= 1


def test_direct_digest_exception_keeps_full_text_and_excerpt_fallback(tmp_path, monkeypatch):
    channels_path = tmp_path / "channels.json"
    channels_path.write_text(json.dumps({"topic_buckets":[{"topic":"t","channels":[{"name":"C","url":"https://youtube.com/@c"}]}]}), encoding="utf-8")
    report_dir = tmp_path / "out_direct_fail"
    txt = "X" * 3000
    vids=[{"id":"c9","title":"FailDirect","channel":"c","published_at":dt.datetime(2026,5,14,12,0,tzinfo=dt.timezone.utc),"url":"https://www.youtube.com/watch?v=c9","description":""}]
    monkeypatch.setattr(main_mod, "list_recent_videos", lambda *a, **k: vids)
    monkeypatch.setattr(main_mod, "fetch_video_content", lambda **k: type("R", (), {"status":"success","text":txt,"source":"managed_transcript"})())
    monkeypatch.setattr(main_mod, "summarize_youtube_transcript_direct", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("empty_output")))
    monkeypatch.setattr(main_mod, "summarize", lambda *a, **k: "## Executive Summary\n\nok")
    monkeypatch.setattr(main_mod, "summarize_item_detail", lambda *a, **k: "detail")
    monkeypatch.setattr(main_mod, "send_markdown", lambda *a, **k: None)
    monkeypatch.setenv("REPORT_KEY", "cyberlurch"); monkeypatch.setenv("REPORT_MODE", "daily"); monkeypatch.setenv("REPORT_DIR", str(report_dir)); monkeypatch.setenv("STATE_PATH", str(tmp_path / "s9.json")); monkeypatch.setenv("SEND_EMAIL", "0"); monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(channels_path), "--hours", "36"])
    main_mod.main()
    diag = json.loads((report_dir / "cyberlurch_youtube_diagnostics.json").read_text(encoding="utf-8"))
    assert diag["transcript_direct_error_by_kind"]["empty_output"] >= 1
    md = next(report_dir.glob("*.md")).read_text(encoding="utf-8")
    assert "Source: TranscriptAPI, transcript excerpt fallback" in md
