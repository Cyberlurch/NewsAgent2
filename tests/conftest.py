from __future__ import annotations
import pytest


@pytest.fixture
def safe_main_state_paths(monkeypatch, tmp_path):
    monkeypatch.setenv("STATE_PATH", str(tmp_path / "state" / "processed_items.json"))
    monkeypatch.setenv("ROLLUPS_STATE_PATH", str(tmp_path / "state" / "rollups.json"))
    monkeypatch.setenv("CYBERLURCH_DIGEST_STATE_PATH", str(tmp_path / "state" / "cyberlurch_digests.json"))
    monkeypatch.setenv("REPORT_DIR", str(tmp_path / "out"))
