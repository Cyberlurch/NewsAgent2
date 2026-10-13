import copy
import hashlib
import json
from pathlib import Path

import pytest

from newsagent2.maintenance.correct_pubmed_evidence import (
    CorrectionError, apply_correction, plan_correction,
)


CORRECTION = json.loads((Path(__file__).parents[1] / "data/corrections/cybermed_42854247.json").read_text())


def setup_state(root):
    root.mkdir()
    paper = {"item_id": "42854247", "pmid": "42854247", "bottom_line": "AI reduced delirium.", "legacy_extra": {"retain": True}}
    data = {"schema_version": 1, "legacy_meta": "retain", "digests": [{
        "digest_id": "cybermed_daily_2026-10-12", "items": {"pubmed": [paper, {"pmid": "99", "bottom_line": "Untouched"}]},
        "deep_dives": [{**paper, "deep_dive_markdown": "No significant difference."}]}]}
    (root / "cybermed_daily_digests.json").write_text(json.dumps(data))
    (root / "processed_items.json").write_text('{"reports":{}}')
    return data


def hashes(root):
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir() if p.is_file()}


def test_dry_run_is_readonly_and_shows_field_diff(tmp_path):
    state = tmp_path / "state"; setup_state(state)
    before = hashes(state)
    plan, outputs = plan_correction(state, CORRECTION)
    assert before == hashes(state)
    assert not plan["apply"] and plan["changes"] and len(outputs) == 1
    assert any(c["pointer"].endswith("deep_dive_markdown") for c in plan["changes"])


def test_apply_requires_hashes_backup_and_is_idempotent(tmp_path):
    state = tmp_path / "state"; initial = setup_state(state)
    raw = (state / "cybermed_daily_digests.json").read_bytes()
    plan, _ = plan_correction(state, CORRECTION)
    result = apply_correction(state, CORRECTION, plan, tmp_path / "backup")
    assert result["status"] == "applied"
    assert (tmp_path / "backup/cybermed_daily_digests.json").read_bytes() == raw
    new = json.loads((state / "cybermed_daily_digests.json").read_text())
    assert new["legacy_meta"] == initial["legacy_meta"]
    assert new["digests"][0]["items"]["pubmed"][1] == initial["digests"][0]["items"]["pubmed"][1]
    assert new["digests"][0]["items"]["pubmed"][0]["legacy_extra"] == {"retain": True}
    before = hashes(state)
    assert apply_correction(state, CORRECTION, plan, tmp_path / "unused-backup")["status"] == "already_corrected"
    assert hashes(state) == before and not (tmp_path / "unused-backup").exists()


def test_hash_mismatch_refuses_before_any_write(tmp_path):
    state = tmp_path / "state"; setup_state(state)
    plan, _ = plan_correction(state, CORRECTION)
    path = state / "cybermed_daily_digests.json"; path.write_bytes(path.read_bytes() + b" ")
    before = hashes(state)
    with pytest.raises(CorrectionError, match="input_hash_mismatch"):
        apply_correction(state, CORRECTION, plan, tmp_path / "backup")
    assert before == hashes(state) and not (tmp_path / "backup").exists()


def test_only_explicit_aggregate_provenance_links_are_eligible(tmp_path):
    state = tmp_path / "state"; setup_state(state)
    paper = {"pmid": "42854247", "bottom_line": "Wrong", "source_daily_run_dates": ["2026-10-12"]}
    rows = [{"digest_id": "w1", "source_daily_digest_ids": CORRECTION["digest_ids"], "items": {"pubmed": [paper]}},
            {"digest_id": "w2", "source_daily_digest_ids": ["other"], "items": {"pubmed": [copy.deepcopy(paper)]}}]
    (state / "cybermed_weekly_digests.json").write_text(json.dumps({"digests": rows}))
    plan, outputs = plan_correction(state, CORRECTION)
    weekly = json.loads(outputs["cybermed_weekly_digests.json"])
    assert weekly["digests"][0]["items"]["pubmed"][0]["bottom_line"] != "Wrong"
    assert weekly["digests"][1] == rows[1]
    assert len(plan["links"]) == 1


def test_unknown_linked_editorial_prose_blocks_apply(tmp_path):
    state = tmp_path / "state"; setup_state(state)
    (state / "cybermed_weekly_digests.json").write_text(json.dumps({"digests": [{"digest_id": "w1",
        "source_daily_digest_ids": CORRECTION["digest_ids"], "executive_summary": ["A narrative with unknown attribution"]}]}))
    plan, _ = plan_correction(state, CORRECTION)
    before = hashes(state)
    with pytest.raises(CorrectionError, match="unresolved_target_or_aggregate_review"):
        apply_correction(state, CORRECTION, plan, tmp_path / "backup")
    assert hashes(state) == before


def test_corruption_is_preserved_byte_for_byte(tmp_path):
    state = tmp_path / "state"; setup_state(state)
    broken = state / "cybermed_daily_digests.json"; broken.write_bytes(b'{"digests":truncated')
    before = hashes(state)
    with pytest.raises(CorrectionError, match="corrupted_existing_store"):
        plan_correction(state, CORRECTION)
    assert hashes(state) == before
