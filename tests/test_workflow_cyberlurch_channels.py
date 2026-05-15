from __future__ import annotations

import pathlib
import re

WORKFLOW = pathlib.Path(__file__).resolve().parents[1] / ".github" / "workflows" / "newsagent.yml"


def _workflow_text() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def test_workflow_has_manual_cyberlurch_channels_override_input():
    text = _workflow_text()
    assert "cyberlurch_channels_file:" in text
    assert 'description: "Cyberlurch channels file override"' in text
    assert 'default: ""' in text


def test_scheduled_cyberlurch_defaults_to_full_channels_not_youtube_only():
    text = _workflow_text()
    assert 'CHFILE="data/channels.json"' in text
    assert "if [ -f data/youtube_only.json ]" not in text
    assert 'CHFILE="data/youtube_only.json"' not in text


def test_manual_override_allows_youtube_only_only_when_explicit_and_validated():
    text = _workflow_text()
    run_step = re.search(r"- name: Run reports(?P<body>.*?)- name: Verify reports", text, re.S).group("body")
    assert 'github.event_name }}" = "workflow_dispatch"' in run_step
    assert "CYBERLURCH_CHANNELS_FILE_OVERRIDE" in run_step
    assert "data/*.json" in run_step
    assert "*..*" in run_step
    assert 'CHFILE="${CYBERLURCH_CHANNELS_FILE_OVERRIDE}"' in run_step
