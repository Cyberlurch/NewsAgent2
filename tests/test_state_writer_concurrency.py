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
