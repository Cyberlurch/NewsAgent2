"""Explicitly selected public listing diagnostics, never a newsletter run."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import signal

from .foamed_probe import BudgetSession, ProbeDeadline, _expired
from .youtube_source import cached_identity, cache_valid
from .collectors_youtube_rss import list_recent_videos_rss
from .run_context import path_for
from .store_io import read_store


def inspect_channels(names, *, config, probe=False, seconds=30, requests_max=8, redirects_max=3, session_factory=None):
    if not names or len(names) > 3 or len(set(names)) != len(names):
        raise ValueError("probe_select_one_to_three_unique_channels")
    if not (1 <= seconds <= 30 and 1 <= requests_max <= 8 and 0 <= redirects_max <= 3):
        raise ValueError("invalid_probe_budget")
    from .main import load_channels_config
    channels, _, _ = load_channels_config(str(config))
    selected = [c for c in channels if c["name"] in names and c["source"] == "youtube"]
    selected = list({(c["name"], c["url"]): c for c in selected}.values())
    if set(names) != {c["name"] for c in selected} or len(selected) != len(names):
        raise ValueError("unknown_or_ambiguous_probe_channel")
    cache_path = path_for("YOUTUBE_CHANNEL_ID_CACHE_PATH")
    cache = read_store(cache_path, {"channels": {}}, kind="youtube_channel_ids", validate=cache_valid)
    output = {"mode": "probe" if probe else "local_inspection", "production_mutation": False,
              "cache_status": "existing" if cache_path.exists() else "missing_new_store", "channels": []}
    for channel in selected:
        output["channels"].append({"name": channel["name"], "identity": cached_identity(cache, channel).public(),
            "listing_status": "not_probed", "transcript_status": "not_probed", "observed_at": None})
    if not probe:
        return output
    if signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
        raise ValueError("probe_requires_exclusive_deadline")
    handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _expired)
    session = (session_factory or BudgetSession)(seconds, requests_max, redirects_max)
    try:
        signal.setitimer(signal.ITIMER_REAL, seconds)
        now = datetime.now(timezone.utc)
        for channel, row in zip(selected, output["channels"]):
            try:
                identity = cached_identity(cache, channel)
                bound = {**channel, "channel_id": identity.channel_id} if identity.channel_id else channel
                result = list_recent_videos_rss(bound, max_items=1, now_utc=now, client=session, allow_api=False)
                status = "budget_exhausted" if getattr(session, "_newsagent_collection_budget_exhausted", False) else result.status
                observed_identity = identity if identity.status == "resolved" and result.identity.status == "configured" else result.identity
                row.update(identity=observed_identity.public(), listing_status=status, items_in_window=len(result),
                           http_status=result.http_status, observed_at=datetime.now(timezone.utc).isoformat())
            except ProbeDeadline:
                row["listing_status"] = "deadline_exhausted"
                break
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, handler)
        session.close()
    output["requests_used"] = requests_max - session.requests_left
    output["redirects_used"] = redirects_max - session.redirects_left
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--channels", nargs="+", required=True)
    parser.add_argument("--config", type=Path, default=Path("data/channels.json"))
    parser.add_argument("--probe", action="store_true", help="Allow bounded public HTTP listing only; still no state writes")
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--requests", type=int, default=8)
    parser.add_argument("--redirects", type=int, default=3)
    args = parser.parse_args()
    result = inspect_channels(args.channels, config=args.config, probe=args.probe, seconds=args.seconds,
                              requests_max=args.requests, redirects_max=args.redirects)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
