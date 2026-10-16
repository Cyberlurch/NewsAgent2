"""Offline identity, listing, source basis and complete CLI regressions."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from newsagent2 import main, reporter, summarizer
from newsagent2 import collectors_youtube_rss as rss
from newsagent2 import youtube_source as source
from newsagent2 import youtube_probe as probe
from newsagent2.run_context import PATHS, path_for
from newsagent2.youtube_generation import parsed_output
from test_cyberlurch_candidates import configure_cli

CID = "UC" + "a" * 22
OTHER = "UC" + "b" * 22
CHANNEL = {"name": "Selected", "url": "https://www.youtube.com/@selected"}
NOW = datetime(2026, 10, 15, 3, tzinfo=timezone.utc)


def owner_html(cid=CID, *, canonical=None):
    return ('<html><script>var recommendations = {"channelId":"' + OTHER + '"};</script>' +
        (f'<link rel="canonical" href="{canonical}">' if canonical else '') +
        '<script>var ytInitialData = ' + json.dumps({"metadata": {"channelMetadataRenderer": {
            "externalId": cid, "vanityChannelUrl": CHANNEL["url"]}}}) + ';</script></html>')


def response(body, *, status=200):
    return NS(text=body, status_code=status, url=CHANNEL["url"], raise_for_status=lambda: None)


def model_response(value, finish="stop"):
    return NS(choices=[NS(message=NS(content=json.dumps(value) if isinstance(value, dict) else value), finish_reason=finish)])


def digest(summary="The source predicts a change; the change has not occurred."):
    return {"transcript_full_summary": summary, "transcript_key_points": [summary],
            "transcript_notable_claims": ["The source interprets the passage as a prediction."],
            "transcript_uncertainties": ["The outcome is unknown."], "important_details": [], "editorial_relevance": ""}


def test_owner_identity_ignores_early_recommendations_and_detects_conflict():
    assert source.owner_identity(owner_html(), CHANNEL["url"]).channel_id == CID
    assert source.owner_identity(owner_html(canonical="https://www.youtube.com/channel/" + OTHER), CHANNEL["url"]).status == "conflict"
    assert source.owner_identity('<html>consent<script>{"channelId":"' + CID + '"}</script></html>', CHANNEL["url"]).status == "unresolved"
    assert source.owner_identity(owner_html(), "https://youtube.com/@someoneelse").status == "unresolved"


def test_configured_url_conflict_and_name_cache_never_verify_a_different_source():
    cache = {"channels": {"Selected": {"channel_id": CID}}}
    assert source.cached_identity(cache, CHANNEL).status == "legacy_unverified"
    bound = source.Identity("resolved", CID, "for_handle", CHANNEL["url"])
    source.cache_identity(cache, bound)
    assert source.cached_identity(cache, CHANNEL).channel_id == CID
    changed = {**CHANNEL, "url": "https://www.youtube.com/@different"}
    assert source.cached_identity(cache, changed).status == "legacy_unverified"
    assert source.cached_identity(cache, {**CHANNEL, "channel_id": OTHER}).status == "conflict"
    assert source.configured_identity({**CHANNEL, "channel_id": "UCshort"}).status == "conflict"
    assert source.configured_identity({"url": "https://youtube.com/channel/" + CID, "channel_id": OTHER}).status == "conflict"


def test_for_handle_binding_and_resolution_request_count(monkeypatch):
    monkeypatch.setenv("YOUTUBE_API_KEY", "dummy")
    calls = []
    def get(url, **kw):
        calls.append((url, kw))
        return NS(raise_for_status=lambda: None, json=lambda: {"items": [{"id": CID}]})
    diag = {}
    result = rss.resolve_identity(CHANNEL, client=NS(get=get), diagnostics=diag)
    assert result.channel_id == CID and result.evidence == "for_handle"
    assert calls[0][1]["params"]["forHandle"] == "selected"
    assert diag["youtube_identity_requests_total"] == 1


@pytest.mark.parametrize("xml,status", [
    ('<feed xmlns="http://www.w3.org/2005/Atom"/>', "empty_valid"),
    ('<html>Consent</html>', "consent_or_html"),
    ('<feed', "parse_error"),
    (f'<feed xmlns="http://www.w3.org/2005/Atom"><id>yt:channel:{OTHER}</id></feed>', "identity_conflict"),
    ('<feed xmlns="http://www.w3.org/2005/Atom"><entry><id>yt:video:abcdefghijk</id><published>2020-01-01T00:00:00Z</published></entry></feed>', "outside_window"),
])
def test_listing_distinguishes_valid_empty_old_invalid_and_wrong_channel(xml, status):
    result = rss.list_recent_videos_rss({**CHANNEL, "channel_id": CID}, client=NS(get=lambda *a, **k: response(xml)), now_utc=NOW)
    assert result.status == status and not result
    assert source.listing_valid(result) == (status in {"empty_valid", "outside_window"})


def test_fetch_error_is_not_valid_empty():
    def fail(*a, **k):
        raise TimeoutError("private details must never escape")
    result = rss.list_recent_videos_rss({**CHANNEL, "channel_id": CID}, client=NS(get=fail), now_utc=NOW)
    assert result.status == "fetch_error" and not source.listing_valid(result)
    assert "private" not in str(result.__dict__)


@pytest.mark.parametrize("value,finish", [
    ("raw technical failure", "stop"), (digest(), "length"), ({"transcript_full_summary": "cut", "transcript_key_points": "wrong type"}, "stop"),
    ({"transcript_full_summary": "Traceback (most recent call last): API error"}, "stop"), ("{\"transcript_full_summary\":\"truncated", "stop")])
def test_unvalidated_model_outputs_never_become_reader_text(value, finish):
    assert parsed_output(model_response(value, finish))[0] is None


def test_safe_json_recovery_keeps_typed_lists_and_negation():
    obj, recovered = parsed_output(model_response("prefix ```json\n" + json.dumps(digest()) + "\n``` suffix"))
    assert recovered and isinstance(obj["transcript_key_points"], list)
    assert "has not occurred" in obj["transcript_key_points"][0]
    assert len(reporter._deduplicate_cyberlurch_facts(["The change has occurred.", "The change has not occurred."])) == 2


@pytest.mark.parametrize("provider", ["youtube_transcript_api", "timedtext", "yt_dlp_captions", "managed_transcript", "description"])
def test_cli_source_basis_late_information_and_both_reader_views(monkeypatch, tmp_path, freeze_clock, provider):
    configure_cli(monkeypatch, tmp_path, freeze_clock, channels=1, count=1, mode="real")
    text = "Source discussion. " * 800 + "Late finding: the planned change has not occurred."
    assert text.index("Late finding") > 12000
    monkeypatch.setattr(main, "fetch_video_content", lambda **kw: NS(text=text, source=provider))
    prompts = []
    def create(**kw):
        prompts.append(kw["messages"][-1]["content"])
        return model_response(digest())
    monkeypatch.setattr(summarizer, "_get_client", lambda: NS(chat=NS(completions=NS(create=create))))
    # main retains the direct helper import; calls still reach the mocked client.
    main.main()
    assert len(prompts) == 1
    if provider != "description":
        assert "Late finding: the planned change has not occurred." in prompts[0]
    else:
        # Description remains bounded by the existing acquisition prefix; it
        # cannot be promoted to a transcript or claim full-video analysis.
        assert "Description" in prompts[0] and "FULL transcript" not in prompts[0]
    store = main._load_cyberlurch_digest_state(str(path_for("CYBERLURCH_DIGEST_STATE_PATH")))
    row = main._item_from_digest_record(store["digests"][0])
    assert row["source_basis"]["kind"] == ("description" if provider == "description" else "transcript")
    assert row["source_basis"]["video_completeness"] == "unknown"
    assert row["source_basis"]["coverage"] == ("partial" if provider == "description" else "available_text")
    for deep in (False, True):
        md = reporter.render_cyberlurch_daily_report([{**row, "cyberlurch_daily_deep_dive": deep}], title="Cyberlurch", generated_at="now")
        assert "has not occurred" in md and "*Channel0 ·" in md
        assert "According to Channel0" not in md
        assert source.basis_label(row) in md and "['" not in md


def test_cli_valid_empty_rss_never_falls_back_to_ytdlp(monkeypatch, tmp_path, freeze_clock):
    configure_cli(monkeypatch, tmp_path, freeze_clock, channels=1, count=0)
    calls = []
    def empty(ch, **kw):
        calls.append(kw)
        return source.Listing(status="empty_valid", identity=source.configured_identity(ch))
    monkeypatch.setattr(main, "list_recent_videos_rss", empty)
    main.main()
    assert len(calls) == 1 and calls[0]["now_utc"] == NOW
    diag = json.loads((Path(os.environ["REPORT_DIR"]) / "cyberlurch_youtube_diagnostics.json").read_text())
    assert diag["channels_success_total"] == 1 and diag["daily_content_preselected_total"] == 0
    assert diag["youtube_listing_status_counts"] == {"empty_valid": 1}


@pytest.mark.parametrize("origin", ["api", "listing"])
def test_cli_uploader_mismatch_does_not_poison_channel_cache(monkeypatch, tmp_path, freeze_clock, origin):
    api, content = configure_cli(monkeypatch, tmp_path, freeze_clock, channels=1, count=1, mode="real")
    if origin == "api":
        monkeypatch.setattr(main, "fetch_video_snippets", lambda ids, *a: {i: {"channel_id": OTHER} for i in ids})
    else:
        original = main.list_recent_videos_rss
        monkeypatch.setattr(main, "list_recent_videos_rss", lambda *a, **k: [{**v, "uploader_channel_id": OTHER} for v in original(*a, **k)])
    main.main()
    assert not content
    cache = main._load_youtube_channel_id_cache()
    assert all(row["channel_id"] != OTHER for row in cache.get("bindings", {}).values())


def test_chunk_plan_exhaustion_no_paid_calls_and_unique_vs_overlapping_chars(monkeypatch):
    text = "Source text. " * 1500
    prompts = []
    def create(**kw):
        prompts.append(kw["messages"][-1]["content"])
        if "Chunk " in prompts[-1]:
            return model_response({"summary": "The source predicts a change.", "key_points": [], "notable_claims": [], "uncertainties": []})
        return model_response(digest())
    monkeypatch.setattr(summarizer, "_get_client", lambda: NS(chat=NS(completions=NS(create=create))))
    result = summarizer.summarize_youtube_transcript_chunks({"text": text})
    assert result["unique_chars_processed"] == len(text.strip())
    assert result["chars_processed_total"] > result["unique_chars_processed"]
    monkeypatch.setenv("CYBERLURCH_TRANSCRIPT_MAX_CHUNKS_PER_RUN", "0")
    before = len(prompts)
    assert summarizer.summarize_youtube_transcript_chunks({"text": text})["transcript_generation_status"] == "budget_skipped"
    assert len(prompts) == before


def test_source_basis_through_monthly_and_yearly_registry_and_legacy_unknown():
    from newsagent2.cyberlurch_monthly import build_source_registry
    from newsagent2.cyberlurch_yearly import build_annual_evidence
    b = source.basis("description", "Available description.")
    b.update(processed_unique_chars=b["available_chars"], coverage="available_text")
    item = dict(id="id", source="youtube", channel="Channel", title="A specific source claim", url="https://youtube.com/watch?v=id",
                published_at=NOW, source_basis=b, transcript_full_summary="The source predicts a change.")
    registry = build_source_registry([item])
    assert registry[0]["source_basis"] == b
    legacy = main._item_from_digest_record({"video_id": "legacy", "text_source": "managed_transcript", "content_status": "full_text"})
    assert legacy["source_basis"]["kind"] == "unknown"
    # Monthly semantic-v2 fixtures and annual full rendering are covered by the
    # existing suite; check additive registry preservation with its actual shape.
    from test_cyberlurch_yearly_semantic import _semantic
    rollup = _semantic("2026-10", "Channel", "https://youtube.com/watch?v=id")
    for row in rollup["source_registry"]:
        row["source_basis"] = b
    _, annual_registry, _ = build_annual_evidence([rollup], 2026)
    assert annual_registry and all(row["source_basis"] == b for row in annual_registry)


def test_cache_hits_preserve_new_basis_and_keep_legacy_basis_unknown(monkeypatch):
    from newsagent2 import youtube_content_providers as providers
    def forbidden(*a, **kw):
        pytest.fail("A full cached text must not call providers")
    monkeypatch.setattr(providers.YouTubeTranscriptApiProvider, "fetch", forbidden)
    b = source.basis("youtube_transcript_api", "Saved transcript.")
    cache = {"new": {"status": "success", "source": "youtube_transcript_api", "fetched_at_utc": "2999-01-01T00:00:00+00:00", "text": "Saved transcript.", "source_basis": b},
             "old": {"status": "success", "source": "managed_transcript", "fetched_at_utc": "2999-01-01T00:00:00+00:00", "text": "Old saved text."}}
    path_for("YOUTUBE_CONTENT_CACHE_PATH").write_text(json.dumps(cache))
    for key in ("new", "old"):
        result = providers.fetch_video_content(video_id=key, video_url="https://youtube.com/watch?v=" + key, description="", diagnostics={})
        assert result.source_basis["kind"] == ("transcript" if key == "new" else "unknown")
    assert json.loads(path_for("YOUTUBE_CONTENT_CACHE_PATH").read_text()) == cache


def test_new_unprocessed_digest_rows_cannot_supply_a_monthly_fact_from_title():
    from newsagent2.cyberlurch_monthly import build_source_registry, select_evidence_pack
    item = dict(id="id", channel="Channel", title="An unsupported positive title", url="https://youtube.com/watch?v=id", published_at=NOW,
                content_status="full_text", source_basis=source.basis("youtube_transcript_api", "Unprocessed text."))
    assert select_evidence_pack(build_source_registry([item])) == []


def test_invalid_new_digest_basis_preserves_existing_bytes():
    from newsagent2.store_io import StoreError
    path = path_for("CYBERLURCH_DIGEST_STATE_PATH")
    path.write_text(json.dumps({"version": 1, "digests": [{"video_id": "validid", "source_basis": {"version": 1, "coverage": "invented"}}]}))
    before = path.read_bytes()
    with pytest.raises(StoreError):
        main._load_cyberlurch_digest_state(str(path))
    assert path.read_bytes() == before


def test_metadata_only_helpers_do_not_call_a_model(monkeypatch):
    monkeypatch.setattr(summarizer, "_get_client", lambda: pytest.fail("No model for metadata"))
    for helper in (summarizer.summarize_youtube_transcript_direct, summarizer.summarize_youtube_transcript_chunks):
        assert helper({"text": "Only a title", "content_status": "metadata_only"})["transcript_generation_status"] == "unavailable"


def test_actual_monthly_persistence_retains_basis_for_yearly_without_touching_digests(monkeypatch, tmp_path, freeze_clock):
    from test_cyberlurch_mode_artifacts_and_digest_primary import _channels, _digest_state_mixed_temporality, _common
    from newsagent2.cyberlurch_yearly import build_annual_evidence
    import sys
    cfg = tmp_path / "channels.json"
    _channels(cfg)
    store = tmp_path / "state" / "cyberlurch_digests.json"
    _digest_state_mixed_temporality(store)
    data = json.loads(store.read_text())
    b = source.basis("description", "Persisted source description.")
    b.update(processed_unique_chars=b["available_chars"], coverage="available_text")
    for row in data["digests"]:
        row["source_basis"] = b
    store.write_text(json.dumps(data))
    before = store.read_bytes()
    _common(monkeypatch, tmp_path, "monthly")
    monkeypatch.setenv("EMAIL_MODE", "real")
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    monkeypatch.setenv('ROLLUP_MONTH_OVERRIDE', now.strftime('%Y-%m'))
    freeze_clock(((now.replace(day=28)+timedelta(days=4)).replace(day=6)).isoformat())
    rollups = tmp_path / "rollups.json"
    monkeypatch.setenv("ROLLUPS_STATE_PATH", str(rollups))
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(cfg)])
    monkeypatch.setattr(main, "list_recent_videos", lambda *a, **k: pytest.fail("Stored Monthly must not collect"))
    main.main()
    stored = json.loads(rollups.read_text())["reports"]["cyberlurch"][-1]
    assert stored["source_registry"] and all(row["source_basis"] == b for row in stored["source_registry"])
    _, annual, _ = build_annual_evidence([stored], int(stored["month"][:4]))
    assert annual and all(row["source_basis"] == b for row in annual)
    assert store.read_bytes() == before


def test_foreign_rss_entry_is_not_accepted_under_the_configured_owner():
    xml = f'<feed xmlns="http://www.w3.org/2005/Atom" xmlns:yt="http://www.youtube.com/xml/schemas/2015"><id>yt:channel:{CID}</id><entry><yt:channelId>{OTHER}</yt:channelId></entry></feed>'
    result = rss.list_recent_videos_rss({**CHANNEL, "channel_id": CID}, client=NS(get=lambda *a, **k: response(xml)), now_utc=NOW)
    assert result.status == "identity_conflict" and not result
