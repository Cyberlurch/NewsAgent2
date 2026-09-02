"""Static guards for workflows that commit overlapping repository state."""

from pathlib import Path
import re


STATE_WRITER_WORKFLOWS = (
    Path(".github/workflows/newsagent.yml"),
    Path(".github/workflows/cybermed.yml"),
    Path(".github/workflows/cybermed-daily-backfill.yml"),
    Path(".github/workflows/cybermed-weekly-backfill.yml"),
)
EXPECTED_GROUP = "newsagent2-state-writers-${{ github.ref }}"
PRODUCTION_WORKFLOWS = STATE_WRITER_WORKFLOWS[:2]


def _concurrency_value(workflow: Path, key: str) -> str:
    text = workflow.read_text(encoding="utf-8")
    match = re.search(
        rf"(?m)^concurrency:\s*\n(?:^[ \t]+.*\n)*?^[ \t]+{re.escape(key)}:\s*(.+?)\s*$",
        text,
    )
    assert match is not None, f"{workflow} has no concurrency.{key}"
    return match.group(1)


def test_all_overlapping_state_writers_share_ref_scoped_concurrency_group():
    groups = {
        workflow: _concurrency_value(workflow, "group")
        for workflow in STATE_WRITER_WORKFLOWS
    }

    assert set(groups.values()) == {EXPECTED_GROUP}


def test_state_writer_runs_queue_instead_of_cancelling_each_other():
    for workflow in STATE_WRITER_WORKFLOWS:
        assert _concurrency_value(workflow, "cancel-in-progress") == "false"


def test_production_state_writers_checkout_current_branch_head():
    for workflow in PRODUCTION_WORKFLOWS:
        text = workflow.read_text(encoding="utf-8")
        checkout = re.search(
            r"(?ms)^\s*- name: Checkout\s*$.*?(?=^\s*- name:)", text
        )
        assert checkout is not None, f"{workflow} has no Checkout step"
        assert "uses: actions/checkout@v5" in checkout.group(0)
        assert "fetch-depth: 0" in checkout.group(0)
        assert "ref: ${{ github.ref_name }}" in checkout.group(0)
