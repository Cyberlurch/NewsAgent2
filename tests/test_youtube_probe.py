import hashlib
import json
from pathlib import Path
from types import SimpleNamespace as NS

import pytest
import requests

from newsagent2 import youtube_probe as probe
from newsagent2.run_context import PATHS, path_for
from newsagent2.store_io import StoreError

CID = "UC" + "a" * 22


def config(tmp_path):
    path = tmp_path / "channels.json"
    path.write_text(json.dumps({"topic_buckets": [{"topic": "Selected topic", "channels": [
        {"name": name, "url": f"https://youtube.com/@{name}", "channel_id": CID} for name in ("One", "Two", "Other")]}]}))
    return path


def hashes():
    return {key: hashlib.sha256(path_for(key).read_bytes()).hexdigest() if path_for(key).exists() else None for key in PATHS}


def test_default_probe_is_local_only_and_does_not_create_cache_or_lock(tmp_path, monkeypatch):
    def forbidden(*args, **kw):
        pytest.fail("Local inspection must not access any network")
    monkeypatch.setattr(requests, "get", forbidden)
    monkeypatch.setattr(requests.Session, "get", forbidden)
    before = hashes()
    result = probe.inspect_channels(["One"], config=config(tmp_path))
    assert result["cache_status"] == "missing_new_store"
    assert result["channels"][0]["listing_status"] == "not_probed"
    assert result["channels"][0]["transcript_status"] == "not_probed"
    assert hashes() == before and not path_for("DELIVERY_JOURNAL_PATH").with_suffix(".lock").exists()


@pytest.mark.parametrize("raw", [b'{"truncated":', b'{"channels":[]}'])
def test_local_probe_preserves_corrupted_or_invalid_cache(tmp_path, raw):
    path_for("YOUTUBE_CHANNEL_ID_CACHE_PATH").write_bytes(raw)
    before = hashes()
    with pytest.raises(StoreError):
        probe.inspect_channels(["One"], config=config(tmp_path))
    assert hashes() == before


def test_explicit_probe_shared_request_budget_only_selected_channels_and_readonly(tmp_path, monkeypatch):
    calls = []
    def get(self, url, **kw):
        calls.append(url)
        return NS(text='<feed xmlns="http://www.w3.org/2005/Atom"/>', status_code=200, headers={},
                  history=[], close=lambda: None, raise_for_status=lambda: None)
    monkeypatch.setattr(requests.Session, "get", get)
    before = hashes()
    result = probe.inspect_channels(["One", "Two"], config=config(tmp_path), probe=True, requests_max=1)
    assert len(calls) == result["requests_used"] == 1
    assert [row["name"] for row in result["channels"]] == ["One", "Two"]
    assert result["channels"][0]["listing_status"] == "empty_valid"
    assert result["channels"][1]["listing_status"] == "budget_exhausted"
    assert all(row["transcript_status"] == "not_probed" for row in result["channels"])
    assert hashes() == before


def test_probe_redirect_budget_is_global_and_does_not_disable_tls(tmp_path, monkeypatch):
    calls = []
    def get(self, url, **kw):
        calls.append(kw)
        return NS(status_code=302, headers={"Location": "https://www.youtube.com/next"}, close=lambda: None)
    monkeypatch.setattr(requests.Session, "get", get)
    result = probe.inspect_channels(["One"], config=config(tmp_path), probe=True, requests_max=8, redirects_max=1)
    assert len(calls) == 2 and result["redirects_used"] == 1
    assert result["channels"][0]["listing_status"] == "budget_exhausted"
    assert all(call.get("verify", True) and call["allow_redirects"] is False for call in calls)


def test_probe_hard_deadline_is_categorized_and_state_unchanged(tmp_path, monkeypatch):
    from newsagent2.foamed_probe import ProbeDeadline
    def expire(*a, **k):
        raise ProbeDeadline()
    monkeypatch.setattr(probe, "list_recent_videos_rss", expire)
    before = hashes()
    result = probe.inspect_channels(["One"], config=config(tmp_path), probe=True)
    assert result["channels"][0]["listing_status"] == "deadline_exhausted"
    assert hashes() == before


@pytest.mark.parametrize("names", [[], ["Missing"], ["One", "One"], ["One", "Two", "Other", "Fourth"]])
def test_probe_requires_a_small_explicit_scope(tmp_path, names):
    with pytest.raises(ValueError):
        probe.inspect_channels(names, config=config(tmp_path))


def test_probe_cli_default_remains_readonly(tmp_path, monkeypatch, capsys):
    import sys
    monkeypatch.setattr(sys, "argv", ["youtube_probe", "--channels", "One", "--config", str(config(tmp_path))])
    before = hashes()
    probe.main()
    value = json.loads(capsys.readouterr().out)
    assert value["mode"] == "local_inspection" and value["channels"][0]["listing_status"] == "not_probed"
    assert hashes() == before
