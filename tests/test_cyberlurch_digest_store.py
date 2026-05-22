from datetime import datetime, timezone
from newsagent2 import main


def test_digest_sanitize_excludes_full_text_fields():
    item = {"video_id":"v1","title":"t","published_at":datetime(2026,1,1,tzinfo=timezone.utc),"_full_text_for_processing":"secret","description":"long","transcript_full_summary":"sum"}
    out = main._sanitize_cyberlurch_digest_record(item)
    assert "_full_text_for_processing" not in out
    assert "description" not in out
    assert out["video_id"] == "v1"
    assert out["transcript_full_summary"] == "sum"


def test_determine_monthly_rollup_month_day1_schedule_prev_month():
    now = datetime(2026,1,1,10,0,tzinfo=main.STO)
    assert main.determine_monthly_rollup_month(now, "schedule", None) == "2025-12"


def test_determine_monthly_rollup_month_override_wins():
    now = datetime(2026,1,1,10,0,tzinfo=main.STO)
    assert main.determine_monthly_rollup_month(now, "schedule", "2024-11") == "2024-11"
