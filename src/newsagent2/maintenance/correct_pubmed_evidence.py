"""Targeted, offline evidence correction. Dry run by default; no providers/mail.

Only identified Daily digests and explicitly linked aggregate paper records are
eligible. Ambiguous aggregate prose is reported for review, never regenerated.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from ..pubmed_facts import build_pubmed_facts


FILES = ("cybermed_daily_digests.json", "cybermed_weekly_digests.json", "rollups.json")


class CorrectionError(RuntimeError):
    pass


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read(path: Path) -> tuple[bytes | None, dict]:
    if not path.exists():
        return None, {}
    raw = path.read_bytes()
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeError):
        raise CorrectionError("corrupted_existing_store") from None
    if not isinstance(data, dict):
        raise CorrectionError("invalid_existing_store")
    return raw, data


def _paper(row: dict, correction: dict) -> bool:
    return (str(row.get("pmid") or row.get("item_id") or row.get("id") or "") == correction["pmid"]
            or bool(correction.get("doi") and row.get("doi") == correction["doi"]))


def _encode(data: dict, original: bytes) -> bytes:
    # Preserve compact versus pretty store convention; do not reorder keys.
    return (json.dumps(data, ensure_ascii=False, indent=2 if b"\n " in original else None,
                       separators=None if b"\n " in original else (",", ":")) + "\n").encode()


def plan_correction(state_dir: Path, correction: dict) -> tuple[dict, dict[str, bytes]]:
    if not correction.get("pmid") or not correction.get("digest_ids") or correction.get("source_basis") != "abstract":
        raise CorrectionError("invalid_correction_scope")
    facts = build_pubmed_facts({"pmid": correction["pmid"], "abstract": correction["abstract"]})
    if not facts.primary_results:
        raise CorrectionError("correction_requires_reported_results")
    plan: dict[str, Any] = {"pmid": correction["pmid"], "source_url": correction["source_url"],
        "source_basis": facts.source_basis, "evidence_review": correction.get("evidence_review", ""),
        "correction_sha256": sha(json.dumps(correction, sort_keys=True).encode()),
        "input_sha256": {}, "output_sha256": {}, "changes": [], "links": [], "manual_review": [], "apply": False}
    stores: dict[str, dict] = {}
    original: dict[str, bytes] = {}
    for name in FILES:
        raw, data = _read(state_dir / name)
        if raw is not None:
            if name in FILES[:2] and not isinstance(data.get("digests"), list):
                raise CorrectionError("invalid_existing_store")
            if name == FILES[2] and not isinstance(data.get("reports"), dict):
                raise CorrectionError("invalid_existing_store")
            original[name] = raw
            plan["input_sha256"][name] = sha(raw)
            stores[name] = copy.deepcopy(data)
    daily = stores.get(FILES[0], {}).get("digests", [])
    weekly = stores.get(FILES[1], {}).get("digests", [])
    monthly = stores.get(FILES[2], {}).get("reports", {}).get("cybermed", [])
    if not all(isinstance(rows, list) and all(isinstance(row, dict) for row in rows) for rows in (daily, weekly, monthly)):
        raise CorrectionError("invalid_existing_store")
    requested = set(correction["digest_ids"])
    # No dates/title guesses: only explicit target IDs and provenance links.
    targets: list[tuple[str, str, dict]] = []
    found: set[str] = set()
    for i, row in enumerate(daily):
        if row.get("digest_id") in requested:
            found.add(row["digest_id"])
            targets.append((FILES[0], f"/digests/{i}", row))
    linked_weekly: set[str] = set()
    for i, row in enumerate(weekly):
        sources = row.get("source_daily_digest_ids") or []
        if isinstance(sources, list) and requested.intersection(sources):
            linked_weekly.add(row.get("digest_id", ""))
            targets.append((FILES[1], f"/digests/{i}", row))
            plan["links"].append({"cadence": "weekly", "id": row.get("digest_id"), "source_ids": sources})
    for i, row in enumerate(monthly):
        sources = row.get("source_weekly_digest_ids") or []
        if isinstance(sources, list) and linked_weekly.intersection(sources):
            targets.append((FILES[2], f"/reports/cybermed/{i}", row))
            plan["links"].append({"cadence": "monthly", "id": row.get("month"), "source_ids": sources})
    plan["missing_target_ids"] = sorted(requested - found)

    def update(row: dict, name: str, pointer: str) -> None:
        if not _paper(row, correction):
            return
        values = {"bottom_line": facts.core(), "pubmed_facts": facts.to_dict(), "facts_version": facts.version,
                  "source_basis": facts.source_basis,
                  "evidence_correction": {"pmid": correction["pmid"], "source_url": correction["source_url"],
                                          "correction_sha256": plan["correction_sha256"]}}
        if "deep_dive_markdown" in row:
            values.update({"deep_dive_markdown": facts.deep_dive(), "study_type": facts.design,
                "population_setting": facts.population, "intervention_or_exposure": facts.intervention_comparator,
                "primary_endpoint": facts.primary_endpoint, "key_results": facts.primary_results,
                "limitations": facts.limitations, "key_secondary_results": facts.secondary_results,
                "primary_result_direction": facts.direction,
                "primary_result_significance": "not_significant" if facts.direction == "negative_or_null" else "not_reported",
                "clinical_interpretation": facts.core()})
        for key, value in values.items():
            if row.get(key) != value:
                plan["changes"].append({"file": name, "pointer": pointer + "/" + key,
                                        "before": row.get(key), "after": value})
                row[key] = value

    def visit(value: Any, name: str, pointer: str) -> None:
        if isinstance(value, dict):
            update(value, name, pointer)
            for key, child in list(value.items()):
                if key not in {"pubmed_facts", "evidence_correction"}:
                    visit(child, name, pointer + "/" + str(key))
        elif isinstance(value, list):
            for i, child in enumerate(value):
                visit(child, name, pointer + "/" + str(i))

    for name, pointer, row in targets:
        visit(row, name, pointer)
        if name != FILES[0]:
            # The source IDs establish a link, not which sentence in an
            # editorial synthesis belongs to the paper. Never rewrite it blind.
            if row.get("executive_summary") or row.get("themes") or row.get("month_in_brief"):
                plan["manual_review"].append({"file": name, "pointer": pointer,
                    "reason": "linked_aggregate_editorial_prose_requires_review; no Yearly regeneration"})
    outputs: dict[str, bytes] = {}
    for name, data in stores.items():
        changed = any(c["file"] == name for c in plan["changes"])
        encoded = _encode(data, original[name]) if changed else original[name]
        # Roundtrip and retain every untouched field, including legacy metadata.
        if json.loads(encoded) != data:
            raise CorrectionError("output_validation_failed")
        plan["output_sha256"][name] = sha(encoded)
        if changed:
            outputs[name] = encoded
    return plan, outputs


def _atomic(path: Path, data: bytes) -> None:
    fd, tmp = tempfile.mkstemp(prefix=".evidence-correction-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def apply_correction(state_dir: Path, correction: dict, reviewed: dict, backup_dir: Path) -> dict:
    protected_roots = [state_dir.resolve()]
    protected_roots.extend(parent for parent in state_dir.resolve().parents
                           if (parent / ".git" / "HEAD").is_file() or (parent / ".git").is_file())
    if any(backup_dir.resolve().is_relative_to(root) for root in protected_roots):
        raise CorrectionError("backup_must_be_outside_state_and_checkout")
    plan, outputs = plan_correction(state_dir, correction)
    if reviewed.get("correction_sha256") != plan["correction_sha256"]:
        raise CorrectionError("correction_hash_mismatch")
    if set(reviewed.get("input_sha256", {})) != set(plan["input_sha256"]):
        raise CorrectionError("input_file_set_mismatch")
    for name, digest in plan["input_sha256"].items():
        if digest not in {reviewed["input_sha256"].get(name), reviewed.get("output_sha256", {}).get(name)}:
            raise CorrectionError("input_hash_mismatch")
        if digest == reviewed["input_sha256"].get(name) and plan["output_sha256"][name] != reviewed.get("output_sha256", {}).get(name):
            raise CorrectionError("reviewed_output_hash_mismatch")
    if plan["missing_target_ids"] or plan["manual_review"]:
        raise CorrectionError("unresolved_target_or_aggregate_review")
    if not outputs:
        return {**plan, "apply": True, "status": "already_corrected"}
    # Each preimage is recoverable before the first replacement. Use a new,
    # private directory outside the checkout, never overwrite an earlier backup.
    backup_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
    for name in outputs:
        (backup_dir / name).write_bytes((state_dir / name).read_bytes())
    (backup_dir / "reviewed-plan.json").write_text(json.dumps(reviewed, indent=2) + "\n")
    written: list[str] = []
    try:
        for name, data in outputs.items():
            if sha((state_dir / name).read_bytes()) != plan["input_sha256"][name]:
                raise CorrectionError("concurrent_state_change")
            _atomic(state_dir / name, data)
            written.append(name)
    except Exception:
        # Do not roll back over a concurrent writer. The complete backups and
        # listed writes support an operator review after a partial filesystem IO.
        (backup_dir / "partial-write.json").write_text(json.dumps({"written": written}))
        raise CorrectionError("correction_persistence_failed; inspect_private_backup") from None
    return {**plan, "apply": True, "status": "applied"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("state"))
    parser.add_argument("--correction", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--reviewed-plan", type=Path)
    parser.add_argument("--backup-dir", type=Path)
    args = parser.parse_args()
    if args.output.resolve().is_relative_to(args.state_dir.resolve()):
        parser.error("output must be outside the state directory")
    correction = json.loads(args.correction.read_text())
    if args.apply:
        if not args.reviewed_plan or not args.backup_dir:
            parser.error("apply requires --reviewed-plan and --backup-dir")
        plan = apply_correction(args.state_dir, correction, json.loads(args.reviewed_plan.read_text()), args.backup_dir)
    else:
        plan, _ = plan_correction(args.state_dir, correction)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n")
    print(f"evidence correction: changes={len(plan['changes'])} linked_aggregates={len(plan['links'])} apply={plan['apply']}")


if __name__ == "__main__":
    main()
