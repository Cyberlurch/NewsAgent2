from pathlib import Path


CYBERLURCH_WORKFLOW = Path(".github/workflows/newsagent.yml")
CYBERMED_WORKFLOW = Path(".github/workflows/cybermed.yml")


def test_cybermed_has_independent_schedule_concurrency_and_timeout():
    text = CYBERMED_WORKFLOW.read_text(encoding="utf-8")

    assert 'name: NewsAgent2 Cybermed' in text
    assert 'cron: "40 3 * * *"' in text
    assert 'cron: "40 4 * * *"' in text
    assert 'group: newsagent2-state-writers-${{ github.ref }}' in text
    assert 'timeout-minutes: 50' in text
    assert 'Gate scheduled runs to 05:40 Europe/Stockholm' in text


def test_cybermed_workflow_has_no_cyberlurch_runtime_or_youtube_dependencies():
    text = CYBERMED_WORKFLOW.read_text(encoding="utf-8")

    assert "REPORT_KEY=cyberlurch" not in text
    assert "data/channels.json" not in text
    assert "setup-deno" not in text
    assert "YOUTUBE_" not in text
    assert "MANAGED_TRANSCRIPT" not in text
    assert "data/cybermed_channels.json" in text


def test_existing_workflow_no_longer_invokes_or_commits_cybermed():
    text = CYBERLURCH_WORKFLOW.read_text(encoding="utf-8")

    run_step = text.split("- name: Run reports", 1)[1].split("- name: Verify reports", 1)[0]
    assert "REPORT_KEY=cybermed" not in run_step
    assert 'run_which_report="cyberlurch"' in text
    assert "state/cybermed_daily_digests.json" not in text


def test_cybermed_artifacts_are_uploaded_for_scheduled_and_manual_runs():
    text = CYBERMED_WORKFLOW.read_text(encoding="utf-8")
    upload_header = "- name: Upload Cybermed audit artifacts"

    assert upload_header in text
    upload = text.split(upload_header, 1)[1].split("- name: Commit Cybermed", 1)[0]
    assert "github.event_name == 'workflow_dispatch'" not in upload
    assert "out/*diagnostics*.json" in upload


def test_cybermed_commits_dedicated_weekly_store():
    text = CYBERMED_WORKFLOW.read_text(encoding="utf-8")

    assert "state/cybermed_daily_digests.json" in text
    assert "state/cybermed_weekly_digests.json" in text
    assert "contains(steps.plan.outputs.modes, 'weekly')" in text


def test_weekly_backfill_is_manual_audit_then_hash_gated_apply():
    text = Path(".github/workflows/cybermed-weekly-backfill.yml").read_text(encoding="utf-8")

    assert "workflow_dispatch:" in text
    assert "expected_sha256:" in text
    assert "--expected-sha256" in text
    assert "if: inputs.action == 'apply'" in text
    assert "state/cybermed_weekly_digests.json" in text
    assert "RECIPIENTS" not in text
    assert "SEND_EMAIL" not in text
