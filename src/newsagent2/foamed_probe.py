"""Read-only, explicitly selected operator diagnostics. No newsletter execution."""
from __future__ import annotations

import argparse
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import signal
import time
from urllib.parse import urljoin, urlsplit

import requests

from . import collector_foamed as collector
from .run_context import path_for
from .store_io import read_store, processed_store, StoreError
from .foamed_sources import load_sources


class BudgetSession(requests.Session):
    """No retries; count every redirect hop against one shared request budget."""
    def __init__(self, seconds: int, requests_max: int, redirects_max: int):
        super().__init__()
        self.requests_left = requests_max
        self.redirects_left = redirects_max
        setattr(self, collector._COLLECTION_DEADLINE_ATTR, time.monotonic() + seconds)

    def get(self, url, **kwargs):
        kwargs.pop("allow_redirects", None)
        history = []
        while True:
            p = urlsplit(url)
            if p.scheme not in {"http", "https"} or not p.hostname or p.username or p.password:
                raise requests.RequestException("invalid_probe_url")
            remaining = collector._collection_budget_remaining_seconds(self)
            if self.requests_left <= 0 or remaining <= .25:
                collector._note_collection_budget_exhausted(self)
                raise requests.RequestException("probe_budget_exhausted")
            self.requests_left -= 1
            kwargs["timeout"] = max(.1, min(float(kwargs.get("timeout", 10)), remaining / 2))
            response = super().get(url, allow_redirects=False, **kwargs)
            if response.status_code not in {301, 302, 303, 307, 308}:
                response.history = history
                return response
            if self.redirects_left <= 0:
                response.close()
                collector._note_collection_budget_exhausted(self)
                raise requests.RequestException("probe_redirect_budget_exhausted")
            self.redirects_left -= 1
            location = response.headers.get("Location")
            response.close()
            if not location:
                raise requests.RequestException("probe_redirect_without_location")
            history.append(response)
            url = urljoin(url, location)


def _expired(_signum, _frame):
    # BaseException escapes the collector's exception handlers and ensures a
    # hard wall deadline even if a server drip-feeds the response body.
    raise ProbeDeadline()


class ProbeDeadline(BaseException):
    pass


def inspect_sources(names: list[str], *, config: Path, state_path: Path,
                    probe: bool = False, seconds: int = 30, requests_max: int = 8,
                    redirects_max: int = 3) -> dict:
    if not names or not (1 <= seconds <= 120 and 1 <= requests_max <= 30 and 0 <= redirects_max <= 5):
        raise ValueError("invalid_probe_scope_or_budget")
    sources = load_sources(config)
    selected = [s for s in sources if s["name"] in names]
    if set(names) != {s["name"] for s in selected}:
        raise ValueError("unknown_probe_source")
    state = read_store(state_path, {}, kind="processed", validate=processed_store)
    health = state.get("foamed_source_health", {})
    if not isinstance(health, dict) or any(not isinstance(v, dict) for v in health.values()):
        raise StoreError("foamed_health", "invalid_existing_store")
    result = {"mode": "probe" if probe else "local_inspection", "production_mutation": False,
              "state_status": "existing" if state_path.exists() else "missing_new_store", "sources": []}
    for source in selected:
        row = {key: source.get(key) for key in ("name", "extraction_strategy", "listing_adapter", "endpoint_basis")}
        row["stored_health"] = {key: health.get(source["name"], {}).get(key)
                                for key in ("last_health", "consecutive_failures", "disabled_until_utc", "last_ok_utc")}
        result["sources"].append(row)
    if not probe:
        return result
    # This path intentionally supports POSIX/main-thread execution only. If a
    # hard deadline cannot be installed it fails before any HTTP request.
    handler = signal.getsignal(signal.SIGALRM)
    pending = signal.getitimer(signal.ITIMER_REAL)
    if pending != (0.0, 0.0):
        raise ValueError("probe_requires_exclusive_deadline")
    signal.signal(signal.SIGALRM, _expired)
    session = BudgetSession(seconds, requests_max, redirects_max)
    try:
        signal.setitimer(signal.ITIMER_REAL, seconds)
        configs = []
        for source in selected:
            if source.get("extraction_strategy") == "reference":
                configs.append(dict(source))
                continue
            # Explicit probe grants HTTP only, not deactivation or Health writes.
            strategy = source.get("extraction_strategy")
            if strategy in {"disabled", "audit_only"}:
                strategy = "rss_only" if source.get("feed_url") else "html_only"
            configs.append({**source, "extraction_strategy": strategy,
                "headers": {}, "user_agent": collector.DEFAULT_USER_AGENT,
                "max_pages": 1, "max_candidates": 1, "fallback_urls": [source.get("homepage")],
                "wp_rest_url": "", "sitemap_url": ""})
        from datetime import datetime, timezone
        with redirect_stdout(io.StringIO()):
            _, stats = collector.collect_foamed_items(configs, datetime.now(timezone.utc), session=session, probe=True)
        allowed = ("attempted", "probe_only", "skipped_reason", "health", "feed_status_code", "homepage_status_code",
                   "feed_content_type", "homepage_content_type", "feed_redirects", "homepage_redirects",
                   "feed_parse_result", "entries_total", "entries_with_date", "newest_entry_datetime",
                   "freshness", "adapter_result", "rejected_entries", "entries_truncated", "collection_incomplete_due_to_budget")
        result["diagnostics"] = {name: {k: row.get(k) for k in allowed}
                                 for name, row in stats["per_source"].items()}
        result["incomplete"] = bool(stats["collection_budget_exhausted"])
    except ProbeDeadline:
        result.update(incomplete=True, outcome="wall_deadline_exceeded")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, handler)
        session.close()
    result["requests_used"] = requests_max - session.requests_left
    result["redirects_used"] = redirects_max - session.redirects_left
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", action="append", required=True)
    parser.add_argument("--config", type=Path, default=Path(__file__).resolve().parents[2] / "data/cybermed_foamed_sources.json")
    parser.add_argument("--state", type=Path, default=path_for("STATE_PATH"))
    parser.add_argument("--probe", action="store_true", help="Explicit bounded HTTP probe; never changes state")
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--requests", type=int, default=8)
    parser.add_argument("--redirects", type=int, default=3)
    args = parser.parse_args(argv)
    try:
        result = inspect_sources(args.source, config=args.config, state_path=args.state, probe=args.probe,
                                 seconds=args.seconds, requests_max=args.requests, redirects_max=args.redirects)
    except StoreError as error:
        print(json.dumps({"outcome": str(error)}))
        return 1
    except ValueError:
        print(json.dumps({"outcome": "invalid_input_or_store"}))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 2 if result.get("incomplete") else 0


if __name__ == "__main__":
    raise SystemExit(main())
