from __future__ import annotations
import pytest
import os
import socket


def accepted_sender_stub(subject, body, *, coordinator=None, finalization=None):
    """A structured SMTP mock, without connections or production credentials."""
    from newsagent2.delivery import Coordinator
    from newsagent2.emailer import _extract_run_metadata_for_email
    coordinator = coordinator or Coordinator(os.getenv("REPORT_KEY") or "cyberlurch", os.getenv("REPORT_MODE") or "yearly")
    clean, _, _ = _extract_run_metadata_for_email(body)
    result = coordinator.generated(clean, finalization)
    result.status = "accepted_by_smtp"
    result.accepted = ["a" * 64]
    result.recipient_key_id = "b" * 64
    coordinator._persist(result)
    return result


@pytest.fixture(autouse=True)
def offline_connections(monkeypatch):
    """Opt-in offline suite guard; provider mocks still exercise their budgets."""
    if os.getenv("NEWSAGENT_OFFLINE_TESTS") == "1":
        def blocked(*args, **kwargs):
            raise AssertionError("Offline tests forbid network/SMTP connections")
        monkeypatch.setattr(socket.socket, "connect", blocked)
        monkeypatch.setattr(socket.socket, "connect_ex", blocked)


@pytest.fixture
def safe_main_state_paths(monkeypatch, tmp_path):
    monkeypatch.setenv("STATE_PATH", str(tmp_path / "state" / "processed_items.json"))
    monkeypatch.setenv("ROLLUPS_STATE_PATH", str(tmp_path / "state" / "rollups.json"))
    monkeypatch.setenv("CYBERLURCH_DIGEST_STATE_PATH", str(tmp_path / "state" / "cyberlurch_digests.json"))
    monkeypatch.setenv("REPORT_DIR", str(tmp_path / "out"))
