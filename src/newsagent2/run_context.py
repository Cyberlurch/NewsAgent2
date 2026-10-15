"""Small scoped mutation policy; paths are injectable, cadence is not policy."""
from __future__ import annotations

import os
import re
import shutil
import tempfile
import fcntl
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable


PATHS = {
    "STATE_PATH": "processed_items.json", "ROLLUPS_STATE_PATH": "rollups.json",
    "CYBERLURCH_DIGEST_STATE_PATH": "cyberlurch_digests.json",
    "CYBERMED_DAILY_DIGEST_STATE_PATH": "cybermed_daily_digests.json",
    "CYBERMED_WEEKLY_DIGEST_STATE_PATH": "cybermed_weekly_digests.json",
    "YOUTUBE_CHANNEL_ID_CACHE_PATH": "youtube_channel_ids.json",
    "YOUTUBE_CONTENT_CACHE_PATH": "youtube_content_cache.json",
    "YOUTUBE_TRANSCRIPT_ATTEMPTS_PATH": "youtube_transcript_attempts.json",
    "DELIVERY_JOURNAL_PATH": "delivery_journal.json",
    "CYBERMED_PUBMED_INGESTION_PATH": "cybermed_pubmed_ingestion.json",
}


def path_for(name: str, fallback: str | Path | None = None) -> Path:
    if os.getenv(name):
        return Path(os.environ[name])
    if os.getenv("NEWSAGENT_STATE_DIR"):
        return Path(os.environ["NEWSAGENT_STATE_DIR"]) / PATHS[name]
    if name != "STATE_PATH" and os.getenv("STATE_PATH") and fallback is None:
        return Path(os.environ["STATE_PATH"]).parent / PATHS[name]
    return Path(fallback if fallback is not None else Path("state") / PATHS[name])


def email_mode() -> str:
    mode = (os.getenv("EMAIL_MODE") or "").strip().lower()
    if not mode:
        mode = "real" if os.getenv("GITHUB_EVENT_NAME") == "schedule" else "none"
    if mode not in {"real", "none", "test"}:
        raise RuntimeError("delivery_configuration:invalid_email_mode")
    return mode


def send_enabled() -> bool:
    value = (os.getenv("SEND_EMAIL") or "1").strip()
    if value not in {"0", "1"}:
        raise RuntimeError("delivery_configuration:invalid_send_email")
    return value == "1"


def backfill_paths() -> set[str]:
    def enabled(name):
        return (os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}
    manual = os.getenv("GITHUB_EVENT_NAME", "") in {"workflow_dispatch", "manual", ""}
    safe = manual and email_mode() == "none" and os.getenv("SEND_EMAIL") == "0"
    allowed: set[str] = set()
    if (safe and os.getenv("GITHUB_EVENT_NAME") == "workflow_dispatch" and os.getenv("REPORT_KEY") == "cyberlurch"
            and os.getenv("REPORT_MODE") == "monthly" and enabled("CYBERLURCH_MONTHLY_MANUAL_STATE_WRITE")
            and re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", os.getenv("ROLLUP_MONTH_OVERRIDE", ""))):
        allowed.add("ROLLUPS_STATE_PATH")
    return allowed


@dataclass
class RunContext:
    mode: str
    pending: list[Callable] = field(default_factory=list)
    result: object | None = None
    coordinator: object | None = None
    production_paths: set[Path] = field(default_factory=set)
    defer_generation: bool = False
    generated_candidates: dict = field(default_factory=dict)
    input_paths: dict[str, Path] = field(default_factory=dict)

    @property
    def production(self) -> bool:
        return self.mode == "real"


_CURRENT: ContextVar[RunContext | None] = ContextVar("newsagent_run", default=None)


def current() -> RunContext | None:
    return _CURRENT.get()


@contextmanager
def writer_lock(*, write: bool = True):
    """Same local lock for the report CLI and explicit recovery commands.

    A preview never creates a missing lock. Only mutating entry points need a
    new lock; helpers called under an existing lock do not acquire it again.
    """
    lock_path = path_for("DELIVERY_JOURNAL_PATH").with_suffix(".lock")
    if not write and not lock_path.exists():
        yield
        return
    if write:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a" if write else "r") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


@contextmanager
def execution_context():
    ctx = RunContext(email_mode())
    original = {name: os.environ.get(name) for name in PATHS}
    token = _CURRENT.set(ctx)
    try:
        destinations = {name: path_for(name) for name in PATHS}
        ctx.input_paths = dict(destinations)
        allowed = backfill_paths()
        ctx.production_paths = {target.resolve() for name, target in destinations.items() if ctx.production or name in allowed}
        if not ctx.production:
            report_dir = Path(os.getenv("REPORT_DIR") or "reports")
            report_dir.mkdir(parents=True, exist_ok=True)
            scratch = Path(tempfile.mkdtemp(prefix="local-state-", dir=report_dir))
            for name, source in destinations.items():
                if name in allowed:
                    continue
                target = scratch / PATHS[name]
                # Copy bytes without healing or rewriting the production source.
                if source.is_file():
                    shutil.copyfile(source, target)
                destinations[name] = target
        for name, target in destinations.items():
            os.environ[name] = str(target)
        with writer_lock():
            yield ctx
    finally:
        _CURRENT.reset(token)
        for name, value in original.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
