from __future__ import annotations

import datetime as dt
import pathlib
import sys

SRC = pathlib.Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from newsagent2 import collectors_youtube as cy


class DummyYoutubeDL:
    listing = []
    metadata_by_url = {}
    metadata_errors = set()
    opts_seen = []

    def __init__(self, opts):
        self.opts = opts
        type(self).opts_seen.append(opts)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def extract_info(self, url, download=False):
        if url.endswith("/videos"):
            return {"entries": list(type(self).listing)}
        if url in type(self).metadata_errors:
            raise RuntimeError("metadata failed")
        return dict(type(self).metadata_by_url.get(url, {}))


def setup_dummy(monkeypatch, *, listing, metadata_by_url=None, metadata_errors=None):
    DummyYoutubeDL.listing = listing
    DummyYoutubeDL.metadata_by_url = metadata_by_url or {}
    DummyYoutubeDL.metadata_errors = metadata_errors or set()
    DummyYoutubeDL.opts_seen = []
    monkeypatch.setattr(cy.yt_dlp, "YoutubeDL", DummyYoutubeDL)


def fixed_now():
    return dt.datetime(2026, 5, 12, 3, 30, tzinfo=dt.timezone.utc)


def test_date_only_yesterday_is_kept_when_cutoff_is_yesterday_0330(monkeypatch):
    setup_dummy(monkeypatch, listing=[{"id": "abc123", "upload_date": "20260511", "title": ""}])
    diag = {}

    videos = cy.list_recent_videos(
        "https://www.youtube.com/@example",
        hours=24,
        max_items=10,
        diagnostics=diag,
        now_utc=fixed_now(),
    )

    assert [v["id"] for v in videos] == ["abc123"]
    assert videos[0]["published_at"] == dt.datetime(2026, 5, 11, 12, 0, tzinfo=dt.timezone.utc)
    assert diag["videos_kept_after_date_total"] == 1
    assert DummyYoutubeDL.opts_seen[0]["playlistend"] >= 50


def test_real_timestamp_inside_last_24h_is_kept(monkeypatch):
    ts = int(dt.datetime(2026, 5, 11, 4, 0, tzinfo=dt.timezone.utc).timestamp())
    setup_dummy(monkeypatch, listing=[{"id": "inside", "timestamp": ts, "title": ""}])

    videos = cy.list_recent_videos("https://www.youtube.com/@example", hours=24, now_utc=fixed_now())

    assert [v["id"] for v in videos] == ["inside"]
    assert videos[0]["published_at"] == dt.datetime(2026, 5, 11, 4, 0, tzinfo=dt.timezone.utc)


def test_real_timestamp_older_than_cutoff_is_skipped(monkeypatch):
    ts = int(dt.datetime(2026, 5, 11, 3, 0, tzinfo=dt.timezone.utc).timestamp())
    diag = {}
    setup_dummy(monkeypatch, listing=[{"id": "old", "timestamp": ts, "title": ""}])

    videos = cy.list_recent_videos(
        "https://www.youtube.com/@example", hours=24, diagnostics=diag, now_utc=fixed_now()
    )

    assert videos == []
    assert diag["videos_skipped_by_date_total"] == 1


def test_upload_date_before_cutoff_date_is_skipped(monkeypatch):
    diag = {}
    setup_dummy(monkeypatch, listing=[{"id": "too-old", "upload_date": "20260510", "title": ""}])

    videos = cy.list_recent_videos(
        "https://www.youtube.com/@example", hours=24, diagnostics=diag, now_utc=fixed_now()
    )

    assert videos == []
    assert diag["videos_skipped_by_date_total"] == 1


def test_metadata_enrichment_uses_better_timestamp_and_description(monkeypatch):
    monkeypatch.setenv("YTDLP_FULL_METADATA_ENRICHMENT", "1")
    url = "https://www.youtube.com/watch?v=enriched"
    better_ts = int(dt.datetime(2026, 5, 11, 5, 0, tzinfo=dt.timezone.utc).timestamp())
    setup_dummy(
        monkeypatch,
        listing=[{"id": "enriched", "upload_date": "20260511", "title": "flat", "description": ""}],
        metadata_by_url={
            url: {
                "id": "enriched",
                "timestamp": better_ts,
                "title": "full",
                "description": "full description",
                "uploader": "full channel",
            }
        },
    )
    diag = {}

    videos = cy.list_recent_videos(
        "https://www.youtube.com/@example", hours=24, diagnostics=diag, now_utc=fixed_now()
    )

    assert len(videos) == 1
    assert videos[0]["published_at"] == dt.datetime(2026, 5, 11, 5, 0, tzinfo=dt.timezone.utc)
    assert videos[0]["description"] == "full description"
    assert videos[0]["channel"] == "full channel"
    assert diag["metadata_enrichment_attempted_total"] == 1
    assert diag["metadata_enrichment_success_total"] == 1


def test_metadata_enrichment_failure_keeps_plausibly_recent_date_only_entry(monkeypatch):
    monkeypatch.setenv("YTDLP_FULL_METADATA_ENRICHMENT", "1")
    url = "https://www.youtube.com/watch?v=fallback"
    setup_dummy(
        monkeypatch,
        listing=[{"id": "fallback", "upload_date": "20260511", "title": ""}],
        metadata_errors={url},
    )
    diag = {}

    videos = cy.list_recent_videos(
        "https://www.youtube.com/@example", hours=24, diagnostics=diag, now_utc=fixed_now()
    )

    assert [v["id"] for v in videos] == ["fallback"]
    assert diag["metadata_enrichment_attempted_total"] == 1
    assert diag["metadata_enrichment_error_total"] == 1


def test_precise_timestamp_suppresses_full_enrichment_without_force(monkeypatch):
    ts = int(dt.datetime(2026, 5, 11, 10, 0, tzinfo=dt.timezone.utc).timestamp())
    setup_dummy(monkeypatch, listing=[{"id": "clean", "timestamp": ts, "title": "t", "description": ""}])
    diag = {}
    videos = cy.list_recent_videos(
        "https://www.youtube.com/@example", hours=24, diagnostics=diag, now_utc=fixed_now(), force_full_metadata=False
    )
    assert [v["id"] for v in videos] == ["clean"]
    assert diag.get("metadata_enrichment_attempted_total", 0) == 0


def test_force_full_enrichment_reenabled(monkeypatch):
    ts = int(dt.datetime(2026, 5, 11, 10, 0, tzinfo=dt.timezone.utc).timestamp())
    url = "https://www.youtube.com/watch?v=clean2"
    setup_dummy(
        monkeypatch,
        listing=[{"id": "clean2", "timestamp": ts, "title": "t", "description": ""}],
        metadata_by_url={url: {"id": "clean2", "description": "full description"}},
    )
    diag = {}
    videos = cy.list_recent_videos(
        "https://www.youtube.com/@example", hours=24, diagnostics=diag, now_utc=fixed_now(), force_full_metadata=True
    )
    assert [v["id"] for v in videos] == ["clean2"]
    assert diag.get("metadata_enrichment_attempted_total", 0) >= 1
