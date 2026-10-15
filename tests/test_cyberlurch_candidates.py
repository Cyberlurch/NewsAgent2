"""Synthetic offline discovery, recovery and actual CLI wiring regressions."""
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

from newsagent2 import main, workflow_delivery
from newsagent2.cyberlurch_candidates import Candidates, MetadataBudget, FIELD, held_ids, preselect, restore
from newsagent2.delivery import Coordinator, DeliveryError, load_journal
from newsagent2.run_context import PATHS, execution_context, path_for
from newsagent2.state_manager import is_processed, load_state, mark_processed, save_state
from newsagent2.store_io import StoreError

NOW = datetime(2026, 10, 15, 3, tzinfo=timezone.utc)


def video(i, channel="Flood", topic="News", age=0):
    return dict(id=str(i), source="youtube", channel=channel, title="Synthetic title " + str(i),
                topics=[topic], published_at=NOW - timedelta(hours=age),
                url="https://www.youtube.com/watch?v=" + str(i), description="Synthetic source text.")


def select(rows, n=12, deep=2):
    return preselect(rows, priority_channels={"Flood"}, selected_max=n, deep_dive_max=deep)[0]


def test_flood_small_topic_priority_and_order_independence():
    rows = [video(i, age=i / 100) for i in range(40)] + [video("small", "Small", "Theology", age=5)]
    picked = select(rows)
    assert len(picked) == 14 and "small" in {r["id"] for r in picked}
    assert picked == select(list(reversed(rows)))
    assert picked[0]["channel"] == "Flood"  # Priority within the balance tier.
    assert any(r["selection_reason"] == "channel_cap_relaxed" for r in picked)


def test_many_channels_share_cap_and_topics_with_unknowns():
    rows = [video(f"{c}-{i}", str(c), "unknown" if c == 3 else str(c % 3), i) for c in range(5) for i in range(20)]
    rows += [video("priority", "Flood", "unknown")]
    picked = select(rows)
    assert len(picked) == 14 and max(Counter(r["channel"] for r in picked).values()) <= 5
    assert {"0", "1", "2", "unknown"} <= {t for r in picked for t in r["topics"]}
    assert len(select(rows, 2, 0)) == 2 and len(select(rows, 1, 1)) == 2
    assert len(select([video("one")])) == 1


def test_candidate_deferred_survives_discovery_window_and_no_ttl_extension():
    state = {"reports": {}, "legacy_extra": {"preserve": True}}
    first = Candidates(state, NOW, {"editions": {}})
    first.collect([video("later", age=23)])
    first.checkpoint()
    assert not is_processed(state, "cyberlurch", "youtube", "later")
    nxt = Candidates(load_state(str(path_for("STATE_PATH"))), NOW + timedelta(days=1), {"editions": {}})
    assert [r["id"] for r in nxt.collect([])] == ["later"]
    expires = nxt.value["records"]["later"]["expires_at"]
    nxt.collect([video("later")])
    assert nxt.value["records"]["later"]["expires_at"] == expires
    assert nxt.state["legacy_extra"] == {"preserve": True}


def test_dates_news_evergreen_expiry_retry_and_storage_bounds():
    state = {"reports": {}}
    first = Candidates(state, NOW, {"editions": {}})
    rows = [video("old_news", age=96), {**video("old_theology", age=96), "candidate_policy": "evergreen"},
            {**video("unknown"), "published_at": None}, video("future", age=-1),
            {**video("attempt"), "candidate_policy": "evergreen"}]
    assert {r["id"] for r in first.collect(rows)} == {"old_theology", "attempt"}
    first.attempted("attempt")
    assert "attempt" not in {r["id"] for r in first.collect([])}
    for d in (1, 2):
        nxt = Candidates(state, NOW + timedelta(days=d), {"editions": {}})
        assert "attempt" in {r["id"] for r in nxt.collect([])}
        nxt.attempted("attempt")
    nxt = Candidates(state, NOW + timedelta(days=3), {"editions": {}})
    assert "attempt" not in {r["id"] for r in nxt.collect([])}
    assert not Candidates(state, NOW + timedelta(days=15), {"editions": {}}).collect([])
    many = Candidates({"reports": {}}, NOW, {"editions": {}})
    assert len(many.collect([video(i) for i in range(700)])) == 500
    assert many.counts["capacity_dropped"] == 200


@pytest.mark.parametrize("status", ["accepted_by_smtp", "partially_accepted", "outcome_unknown", "ready"])
def test_cross_edition_receipts_hold_candidates_without_new_recipient_index(monkeypatch, status):
    monkeypatch.setenv("EMAIL_MODE", "real")
    co = Coordinator("cyberlurch", "daily", period_dates=("2026-10-14", "2026-10-14"))
    intents = dict(items_all_new=[video("sent")], overview_items=[], detail_items=[])
    # Keep only existing approved finalization fields, as the real main does.
    intents["items_all_new"] = [{k: r[k] for k in ("source", "id", "title", "channel", "url")} for r in intents["items_all_new"]]
    row = co.generated("Saved synthetic report", intents)
    row.status = "generated" if status == "ready" else status
    if status == "ready":
        # Existing sealed-ready coverage is also in test_pre_smtp_recovery;
        # this test exercises the hold predicate, not proof construction.
        row.recovery = {"held": True}
        journal = {"editions": {row.edition_id: row.public()}}
    else:
        row.accepted = ["a" * 64] if status != "outcome_unknown" else []
        row.rejected = ["b" * 64] if status == "partially_accepted" else []
        row.unknown = ["b" * 64] if status == "outcome_unknown" else []
        co._persist(row)
        journal = load_journal(co.path)
    state = {"reports": {}}
    mark_processed(state, "cyberlurch", "youtube", "legacy")
    candidates = Candidates(state, NOW, journal)
    assert {r["id"] for r in candidates.collect([video("sent"), video("legacy"), video("fresh")])} == {"fresh"}
    assert is_processed(state, "cyberlurch", "youtube", "legacy")


def test_generation_and_complete_rejection_do_not_consume_delivery(monkeypatch):
    monkeypatch.setenv("EMAIL_MODE", "real")
    co = Coordinator("cyberlurch", "daily")
    row = co.generated("Saved", {"items_all_new": [{"source": "youtube", "id": "x"}]})
    assert not held_ids(co.journal)
    row.status = "failed"; row.rejected = ["b" * 64]; co._persist(row)
    assert not held_ids(co.journal)


def test_only_rendered_markers_and_persistence_failure_receipt_hold(monkeypatch):
    monkeypatch.setenv("EMAIL_MODE", "real")
    co = Coordinator("cyberlurch", "daily")
    intents = dict(items_all_new=[{"source": "youtube", "id": i} for i in ("shown", "screened")],
                   overview_items=[{"source": "youtube", "id": "shown"}], detail_items=[], foamed_overview_items=[],
                   report_key="cyberlurch", report_mode="daily", now_utc_iso=NOW.isoformat())
    row = co.generated("Saved", intents); row.status = "accepted_by_smtp"; row.accepted = ["a" * 64]; co._persist(row)
    state = {"reports": {}}
    main._update_state_after_run(state_path=str(path_for("STATE_PATH")), state=state,
                                read_only=False, delivery_result=row, **intents)
    assert is_processed(state, "cyberlurch", "youtube", "shown")
    assert not is_processed(state, "cyberlurch", "youtube", "screened")
    monkeypatch.setattr(main, "save_state", lambda *a: (_ for _ in ()).throw(StoreError("processed_items", "persistence_failed")))
    with pytest.raises(DeliveryError):
        co.finalize([lambda result: main._update_state_after_run(state_path=str(path_for("STATE_PATH")), state={"reports": {}},
                            read_only=False, delivery_result=result, **intents)])
    assert co.result.status == "accepted_by_smtp" and not co.result.state_persisted
    assert "shown" in held_ids(load_journal(co.path))


def test_candidate_recovery_is_section_scoped_idempotent_and_hash_guarded(monkeypatch, tmp_path):
    monkeypatch.setenv("EMAIL_MODE", "real")
    original = {"reports": {}, "unrelated": "keep"}
    save_state(str(path_for("STATE_PATH")), original)
    with execution_context() as ctx:
        ctx.coordinator = Coordinator("cyberlurch", "daily")
        c = Candidates(original, NOW, ctx.coordinator.journal)
        c.collect([video("x")]); c.checkpoint()
        directory = ctx.coordinator.recovery
    # Simulate lost Git publication: checkout still has the old section.
    save_state(str(path_for("STATE_PATH")), {"reports": {}, "unrelated": "newer_other_state"})
    restore(directory)
    assert load_state(str(path_for("STATE_PATH")))["unrelated"] == "newer_other_state"
    before = path_for("STATE_PATH").read_bytes(); restore(directory)
    assert path_for("STATE_PATH").read_bytes() == before
    changed = load_state(str(path_for("STATE_PATH")))
    changed[FIELD]["records"]["x"]["attempts"] = 1
    changed[FIELD]["records"]["x"]["last_attempt_day"] = NOW.date().isoformat()
    changed[FIELD]["records"]["x"]["stage"] = "selected"
    save_state(str(path_for("STATE_PATH")), changed)
    # The same older checkpoint must now be a no-op, preserving later progress.
    before = path_for("STATE_PATH").read_bytes()
    restore(directory)
    assert path_for("STATE_PATH").read_bytes() == before
    # Unrelated progress with no evidence of having seen the checkpoint stops.
    changed[FIELD]["checkpoints"] = []
    save_state(str(path_for("STATE_PATH")), changed)
    with pytest.raises(StoreError, match="recovery_preimage_conflict"):
        restore(directory)


def test_published_checkpoint_survives_later_progress_and_failure_import(monkeypatch, tmp_path):
    monkeypatch.setenv("EMAIL_MODE", "real")
    with execution_context() as ctx:
        ctx.coordinator = Coordinator("cyberlurch", "daily")
        c = Candidates({"reports": {}}, NOW, ctx.coordinator.journal)
        c.collect([video("x")]); c.checkpoint()
        ctx.coordinator.failure("synthetic_later_failure")
        first = tmp_path / "old-recovery"
        import shutil
        shutil.copytree(ctx.coordinator.recovery, first)
        c.attempted("x")  # Later checkpoint changes the records.
        before = path_for("STATE_PATH").read_bytes()
        exported = tmp_path / "new-export"
        workflow_delivery.import_recovery(first, recovery_dir=exported)
        assert (exported / "cyberlurch_candidates.json").read_bytes() == (first / "cyberlurch_candidates.json").read_bytes()
        restore(exported)
        assert path_for("STATE_PATH").read_bytes() == before


@pytest.mark.parametrize("raw", [b'{"broken":', b'{"reports":{},"cyberlurch_candidates":{"version":1,"records":[]}}'])
def test_corrupt_or_invalid_candidate_store_is_byte_preserved(raw):
    path_for("STATE_PATH").write_bytes(raw)
    with pytest.raises(StoreError):
        load_state(str(path_for("STATE_PATH")))
    assert path_for("STATE_PATH").read_bytes() == raw


def test_global_budget_batches_duplicates_failures_and_reservations():
    diag, calls = {}, []
    b = MetadataBudget(150, 3, diag)
    def fetch(ids, key, diagnostics):
        calls.append(list(ids))
        return {} if len(calls) == 1 else {i: {} for i in ids}
    for channel in range(3):
        quota = b.start_channel()
        b.fetch([f"{channel}-{i}" for i in range(100)] * 2, quota, "dummy", fetch)
    assert [len(c) for c in calls] == [50, 50, 50]
    assert diag["youtube_api_budget_requested_ids_total"] == 150
    assert diag["youtube_api_budget_enriched_ids_total"] == 100
    assert diag["youtube_api_budget_skipped_total"] == 150
    b.fetch(calls[0], 150, "dummy", fetch)
    assert len(calls) == 3  # No free retry of the failed batch.
    small = MetadataBudget(2, 3, {})
    small_calls = []
    for i in range(3):
        small.fetch([str(i)], small.start_channel(), "dummy", lambda ids, *a: small_calls.extend(ids) or {})
    assert small_calls == ["0", "1"]


def configure_cli(monkeypatch, tmp_path, freeze_clock, *, channels=4, count=30, mode="none"):
    freeze_clock(NOW.isoformat())
    cfg = tmp_path / "channels.json"
    cfg.write_text(json.dumps({"topic_buckets": [{"topic": "topic" + str(c), "channels": [
        {"name": f"Channel{c}", "url": f"https://youtube.com/@channel{c}", "channel_id": f"UC{c}"}]} for c in range(channels)]}))
    for key, value in dict(REPORT_KEY="cyberlurch", REPORT_MODE="daily", REPORT_PROFILE="general", EMAIL_MODE=mode,
            SEND_EMAIL="0", GITHUB_EVENT_NAME="workflow_dispatch", YOUTUBE_API_KEY="dummy", YOUTUBE_METADATA_FALLBACK="0",
            YOUTUBE_API_MAX_VIDEOS_PER_RUN="7", YOUTUBE_API_METADATA="1", REPORT_LANGUAGE="en").items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(sys, "argv", ["main", "--channels", str(cfg)])
    monkeypatch.setattr(main, "list_recent_videos_rss", lambda ch, **kw: [video(f'{ch["name"]}-{i}', ch["name"], age=i/100) for i in range(count)])
    monkeypatch.setattr(main, "list_recent_videos", lambda *a, **k: pytest.fail("Unplanned listing"))
    api, content = [], []
    monkeypatch.setattr(main, "fetch_video_snippets", lambda ids, *a: api.append(list(ids)) or {})
    monkeypatch.setattr(main, "fetch_video_content", lambda **kw: content.append(kw["video_id"]) or SimpleNamespace(text="", source="metadata_only"))
    monkeypatch.setattr(main, "summarize", lambda *a, **k: pytest.fail("No semantic call for metadata"))
    return api, content


@pytest.mark.parametrize("mode", ["none", "test"])
def test_real_cli_global_budget_content_caps_and_all_production_hashes(monkeypatch, tmp_path, freeze_clock, mode):
    api, content = configure_cli(monkeypatch, tmp_path, freeze_clock, mode=mode)
    def snapshot():
        return {name: hashlib.sha256(path_for(name).read_bytes()).hexdigest() if path_for(name).exists() else None for name in PATHS}
    # Existing valid legacy process state plus absent remaining paths.
    save_state(str(path_for("STATE_PATH")), {"reports": {}, "legacy": "untouched"})
    before = snapshot()
    main.main()
    assert snapshot() == before
    assert sum(map(len, api)) == 7 and max(map(len, api)) <= 50
    assert len(content) == 14 and len(set(content)) == 14
    diag = json.loads((Path(os.environ["REPORT_DIR"]) / "cyberlurch_youtube_diagnostics.json").read_text())
    assert diag["youtube_api_budget_requested_ids_total"] == 7
    assert diag["daily_selected_topics_total"] == 4


def test_real_cli_production_finalization_uses_only_rendered_ids(monkeypatch, tmp_path, freeze_clock):
    api, content = configure_cli(monkeypatch, tmp_path, freeze_clock, mode="real")
    main.main()  # SEND_EMAIL=0: valid generation, explicit skip, no delivery.
    journal = load_journal(path_for("DELIVERY_JOURNAL_PATH"))
    row = next(iter(journal["editions"].values()))
    assert row["status"] == "intentionally_skipped"
    ids = {r["id"] for r in row["finalization"]["items_all_new"]}
    assert len(ids) == 12 and ids <= set(content)
    state = load_state(str(path_for("STATE_PATH")))
    assert len(state[FIELD]["records"]) == 120
    assert not state["reports"].get("cyberlurch", {}).get("youtube", {}).get("processed")
    assert Counter(r["stage"] for r in state[FIELD]["records"].values())["selected"] == 14


def test_real_cli_empty_discovery_uses_saved_candidates(monkeypatch, tmp_path, freeze_clock):
    api, content = configure_cli(monkeypatch, tmp_path, freeze_clock, channels=1, count=0, mode="real")
    state = {"reports": {}}
    c = Candidates(state, NOW - timedelta(days=1), {"editions": {}})
    c.collect([video("backlog", "Channel0", age=25)]); c.checkpoint()
    monkeypatch.setattr(main, "list_recent_videos", lambda *a, **k: [])
    main.main()
    assert content == ["backlog"] and not api


def test_real_cli_lower_caps_and_empty_report_intents(monkeypatch, tmp_path, freeze_clock):
    api, content = configure_cli(monkeypatch, tmp_path, freeze_clock, mode="real")
    monkeypatch.setenv("CYBERLURCH_DAILY_SELECTED_ITEMS_MAX", "2")
    monkeypatch.setenv("CYBERLURCH_DAILY_DEEP_DIVES_MAX", "0")
    main.main()
    assert len(content) == 2
    row = next(iter(load_journal(path_for("DELIVERY_JOURNAL_PATH"))["editions"].values()))
    assert len(row["finalization"]["items_all_new"]) == 2 and not row["finalization"]["detail_items"]


def test_real_cli_empty_report_never_marks_screened_candidates(monkeypatch, tmp_path, freeze_clock):
    configure_cli(monkeypatch, tmp_path, freeze_clock, count=0, mode="real")
    monkeypatch.setattr(main, "list_recent_videos", lambda *a, **k: [])
    main.main()
    row = next(iter(load_journal(path_for("DELIVERY_JOURNAL_PATH"))["editions"].values()))
    assert row["finalization"]["items_all_new"] == []
    assert not load_state(str(path_for("STATE_PATH")))["reports"]


def test_cli_accepted_receipt_retry_and_new_edition_do_not_fetch_sent_videos(monkeypatch, tmp_path, freeze_clock):
    from conftest import accepted_sender_stub
    api, content = configure_cli(monkeypatch, tmp_path, freeze_clock, channels=1, count=1, mode="real")
    monkeypatch.setenv("SEND_EMAIL", "1")
    monkeypatch.setenv("DELIVERY_RECIPIENT_HMAC_KEY", "offline-recipient-key-for-testing-only-32")
    monkeypatch.setattr(main, "current_audience_refs", lambda *a: {"a" * 64})
    from newsagent2 import emailer
    monkeypatch.setattr(emailer, "current_audience_refs", lambda *a: {"a" * 64})
    calls = []
    def sender(*args, **kw):
        calls.append(1)
        return accepted_sender_stub(*args, **kw)
    monkeypatch.setattr(main, "send_markdown", sender)
    main.main()
    assert len(calls) == len(content) == 1
    main.main()  # Same edition: receipt finalization, no generation/SMTP.
    assert len(calls) == len(content) == 1
    # Another edition may send an empty report but cannot submit this video.
    freeze_clock((NOW + timedelta(days=1)).isoformat())
    main.main()
    assert len(content) == 1
    newest = max(load_journal(path_for("DELIVERY_JOURNAL_PATH"))["editions"].values(), key=lambda r: r["period_start"])
    assert not newest["finalization"]["items_all_new"]


@pytest.mark.parametrize("status", ["partially_accepted", "outcome_unknown"])
def test_cli_uncertain_receipt_blocks_video_in_another_edition(monkeypatch, tmp_path, freeze_clock, status):
    configure_cli(monkeypatch, tmp_path, freeze_clock, channels=1, count=1, mode="real")
    content = []
    monkeypatch.setattr(main, "fetch_video_content", lambda **kw: content.append(kw["video_id"]) or SimpleNamespace(text="", source="metadata_only"))
    monkeypatch.setenv("SEND_EMAIL", "1")
    monkeypatch.setenv("DELIVERY_RECIPIENT_HMAC_KEY", "offline-recipient-key-for-testing-only-32")
    from newsagent2 import emailer
    monkeypatch.setattr(emailer, "current_audience_refs", lambda *a: {"a" * 64, "b" * 64})
    def uncertain(subject, body, *, coordinator, finalization):
        result = coordinator.generated(body, finalization)
        result.status = status; result.smtp_phase = "completed" if status == "partially_accepted" else "handoff_started"
        result.accepted = ["a" * 64] if status == "partially_accepted" else []
        result.rejected = ["b" * 64] if status == "partially_accepted" else []
        result.unknown = ["a" * 64, "b" * 64] if status == "outcome_unknown" else []
        coordinator._persist(result)
        raise DeliveryError(result)
    monkeypatch.setattr(main, "send_markdown", uncertain)
    with pytest.raises(RuntimeError, match="report_run_failed"):
        main.main()
    assert len(content) == 1
    original = next(iter(load_journal(path_for("DELIVERY_JOURNAL_PATH"))["editions"].values()))
    assert original["recovery"] and original["finalization"]["items_all_new"]
    freeze_clock((NOW + timedelta(days=1)).isoformat())
    monkeypatch.setenv("SEND_EMAIL", "0")
    main.main()
    assert len(content) == 1
    journal = load_journal(path_for("DELIVERY_JOURNAL_PATH"))
    assert journal["editions"][original["edition_id"]] == original
    newest = max(journal["editions"].values(), key=lambda r: r["period_start"])
    assert not newest["finalization"]["items_all_new"]


def test_cli_many_channels_actually_share_default_150_budget(monkeypatch, tmp_path, freeze_clock):
    api, content = configure_cli(monkeypatch, tmp_path, freeze_clock, channels=3, count=100)
    monkeypatch.setenv("YOUTUBE_API_MAX_VIDEOS_PER_RUN", "150")
    main.main()
    assert [len(batch) for batch in api] == [50, 50, 50]
    assert len(content) == 14


def test_api_failure_is_charged_before_actual_request(monkeypatch):
    from newsagent2 import collectors_youtube_api as api
    calls = []
    def fail(*args, **kwargs):
        calls.append(kwargs["params"]["id"])
        raise TimeoutError("synthetic failure")
    monkeypatch.setattr(api.requests, "get", fail)
    diag = {}
    budget = MetadataBudget(3, 1, diag)
    assert budget.fetch(["one", "two", "three", "four"], budget.start_channel(), "dummy", api.fetch_video_snippets) == {}
    budget.fetch(["one", "two", "three"], 3, "dummy", api.fetch_video_snippets)
    assert calls == ["one,two,three"]
    assert diag["youtube_api_budget_requested_ids_total"] == diag["youtube_api_metadata_error_total"] == 3
