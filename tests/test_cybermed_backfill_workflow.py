from pathlib import Path


WORKFLOW = Path(".github/workflows/cybermed-daily-backfill.yml")


def test_cybermed_backfill_is_manual_only_and_email_safe():
    text = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in text
    assert "schedule:" not in text
    assert 'SEND_EMAIL: "0"' in text
    assert "EMAIL_MODE: none" in text
    assert "RECIPIENTS_CONFIG_JSON" not in text
    assert "SMTP_" not in text


def test_cybermed_backfill_defaults_to_audit_and_bounded_august_replay():
    text = WORKFLOW.read_text(encoding="utf-8")

    assert 'default: "2026-08-10"' in text
    assert 'default: "2026-08-10T05:11:00+00:00"' in text
    assert 'default: "72"' in text
    assert "default: false" in text
    assert 'CYBERMED_DIGEST_BACKFILL_APPLY: "0"' in text
    assert "--reviewed-plan" in text and "--backup-dir" in text and "--expected-sha256" in text
    assert "if: ${{ !inputs.apply }}" in text
    assert "out/cybermed_daily_candidate.json" in text


def test_cybermed_backfill_commits_only_daily_digest_store():
    text = WORKFLOW.read_text(encoding="utf-8")

    assert "state/cybermed_daily_digests.json" in text
    assert "tools/publish_maintenance.py --cadence daily" in text
    assert "state/processed_items.json" not in text
    assert "cyberlurch" not in text.lower()
