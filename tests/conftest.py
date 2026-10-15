from __future__ import annotations
import pytest
import os
from pathlib import Path
import tempfile
import sys

from _isolation import Policy, install

ROOT = Path(__file__).resolve().parents[1]
SESSION = tempfile.TemporaryDirectory(prefix="newsagent-offline-")
SESSION_ROOT = Path(SESSION.name)


def private_environment_name(name):
    return (name.startswith(("SMTP_", "EMAIL_", "RECIPIENTS_", "TEST_RECIPIENTS_", "OPENAI_", "GITHUB_"))
            or name.endswith(("_TOKEN", "_API_KEY", "_SECRET", "_PASSWORD"))
            or name in {"NCBI_EMAIL", "UNPAYWALL_EMAIL", "DELIVERY_RECIPIENT_HMAC_KEY"})


def clean_environment():
    for name in list(os.environ):
        if private_environment_name(name):
            os.environ.pop(name, None)


clean_environment()  # Before test-module imports and their bound defaults/clients.
sys.path.insert(0, str(ROOT / "src"))
from newsagent2.run_context import PATHS, path_for
for name, filename in PATHS.items():
    os.environ[name] = str(SESSION_ROOT / filename)
os.environ.update(NEWSAGENT_STATE_DIR=str(SESSION_ROOT), REPORT_DIR=str(SESSION_ROOT / "out"), EMAIL_MODE="none", SEND_EMAIL="0")
SESSION_POLICY = Policy(ROOT, SESSION_ROOT / "collection-audit.log")
install(SESSION_POLICY)
BEFORE = SESSION_POLICY.snapshot()
# Existing saved-store regressions use byte-identical temporary snapshots.
# This is the only explicit source-store read, before any test executes.
STORE_SNAPSHOTS = SESSION_ROOT / "saved-store-snapshots"
STORE_SNAPSHOTS.mkdir()
SESSION_POLICY.snapshot_read = True
try:
    for filename in ("cyberlurch_digests.json", "cybermed_daily_digests.json"):
        (STORE_SNAPSHOTS / filename).write_bytes((ROOT / "state" / filename).read_bytes())
finally:
    SESSION_POLICY.snapshot_read = False


@pytest.fixture
def saved_store_snapshot(tmp_path):
    def copy(filename):
        source = STORE_SNAPSHOTS / filename
        if not source.is_file():
            raise ValueError("Unknown saved-store fixture")
        target = tmp_path / filename
        target.write_bytes(source.read_bytes())
        return target
    return copy


def accepted_sender_stub(subject, body, *, coordinator=None, finalization=None):
    """A structured SMTP mock, without connections or production credentials."""
    from newsagent2.delivery import Coordinator
    from newsagent2.emailer import _extract_run_metadata_for_email
    coordinator = coordinator or Coordinator(os.getenv("REPORT_KEY") or "cyberlurch", os.getenv("REPORT_MODE") or "yearly")
    clean, _, _ = _extract_run_metadata_for_email(body)
    result = coordinator.generated(clean, finalization)
    result.status = "accepted_by_smtp"
    result.accepted = result.recovery.get("audience", ["a" * 64])
    result.recipient_key_id = result.recovery.get("recipient_key_id", "b" * 64)
    result.smtp_phase = "completed"
    coordinator._persist(result)
    return result


@pytest.fixture(autouse=True)
def offline_connections(tmp_path):
    policy = Policy(ROOT, tmp_path / "isolation-audit.log")
    install(policy)
    yield
    install(SESSION_POLICY)
    if policy.log.exists():
        pytest.fail("Forbidden access was attempted (even if caught): " + policy.log.read_text().strip())


@pytest.fixture(autouse=True)
def isolated_paths(monkeypatch, tmp_path, offline_connections):
    # Remove inherited secrets per test; individual mocks may explicitly supply
    # dummy settings. Central registry covers caches, journal and lock location.
    for name in list(os.environ):
        if private_environment_name(name):
            monkeypatch.delenv(name, raising=False)
    for name, filename in PATHS.items():
        monkeypatch.setenv(name, str(tmp_path / filename))
        assert path_for(name).parent == tmp_path
    monkeypatch.setenv("NEWSAGENT_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("REPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("EMAIL_MODE", "none")
    monkeypatch.setenv("SEND_EMAIL", "0")
    # Historical fixtures do not inherit the real operator's finite transition.
    # Transition regressions explicitly select the actual repository binding.
    monkeypatch.setenv("GITHUB_REPOSITORY", "offline-tests/fixtures")
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    import dotenv
    original = dotenv.load_dotenv
    for name, module in list(sys.modules.items()):
        if (name == "dotenv" or name.startswith(("newsagent2.", "src.newsagent2."))) and getattr(module, "load_dotenv", None) is original:
            monkeypatch.setattr(module, "load_dotenv", lambda *a, **k: False)
    yield


@pytest.fixture
def safe_main_state_paths(isolated_paths):
    """Compatibility alias; isolation is now unconditional and complete."""


@pytest.fixture
def freeze_clock(monkeypatch):
    from datetime import datetime, timezone
    from types import SimpleNamespace
    def freeze(instant, *, aliases=()):
        fixed = datetime.fromisoformat(instant)
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                value = fixed.astimezone(tz or timezone.utc)
                return cls.fromisoformat(value.isoformat()) if tz else cls.fromisoformat(value.replace(tzinfo=None).isoformat())
        from newsagent2 import main, delivery, reporter
        for module in (main, delivery, reporter):
            monkeypatch.setattr(module, "datetime", Clock)
        for module, name in aliases:
            original = getattr(module, name)
            monkeypatch.setattr(module, name, SimpleNamespace(**{k: getattr(original, k) for k in dir(original) if k != "datetime"}, datetime=Clock))
        return Clock
    return freeze


def pytest_sessionfinish(session, exitstatus):
    import json
    after = SESSION_POLICY.snapshot()
    output = ROOT / ".ci-results"
    output.mkdir(exist_ok=True)
    violations = SESSION_POLICY.log.read_text().splitlines() if SESSION_POLICY.log.exists() else []
    (output / "integrity.json").write_text(json.dumps({"before": BEFORE, "after": after, "unchanged": BEFORE == after, "session_violations": violations}, indent=2) + "\n")
    if after != BEFORE or SESSION_POLICY.log.exists():
        session.exitstatus = 1
        session.config.get_terminal_writer().line("FAIL: production integrity or collection isolation violation", red=True)
