#!/usr/bin/env python3
"""Independent morning trigger; GitHub remains the only newsletter producer.

Requires an authenticated gh CLI with Actions write access to this repository.
No OpenAI/SMTP credentials are needed. A dispatch with an uncertain outcome
is never automatically repeated. Native cron remains an independent fallback.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from datetime import datetime, time
from pathlib import Path
import subprocess
import tempfile
from zoneinfo import ZoneInfo

REPOSITORY = "Cyberlurch/NewsAgent2"
WORKFLOWS = ("newsagent.yml", "cybermed.yml")
STOCKHOLM = ZoneInfo("Europe/Stockholm")
WINDOW_START = time(6, 50)
WINDOW_END = time(7, 30)


def in_window(now):
    local = now.astimezone(STOCKHOLM)
    return WINDOW_START <= local.time() <= WINDOW_END


def payload(now):
    return {"ref": "main", "return_run_details": True,
            "inputs": {"report_mode": "scheduled", "email_mode": "real",
                       "logical_date": now.astimezone(STOCKHOLM).date().isoformat()}}


def gh_dispatch(workflow, data):
    command = ["gh", "api", "--method", "POST",
               "-H", "X-GitHub-Api-Version: 2026-03-10",
               f"repos/{REPOSITORY}/actions/workflows/{workflow}/dispatches",
               "--input", "-"]
    try:
        result = subprocess.run(command, input=json.dumps(data), text=True,
                                capture_output=True, timeout=45, check=True)
        value = json.loads(result.stdout)
        run_id = value.get("workflow_run_id")
        if not isinstance(run_id, int) or isinstance(run_id, bool) or run_id <= 0:
            raise ValueError("missing run ID")
        return run_id
    except (OSError, subprocess.SubprocessError, ValueError, AttributeError):
        # A POST may have been accepted despite a client/network error.
        raise RuntimeError("dispatch_outcome_unverified") from None


def read_record(path, day):
    if not path.exists():
        return {"date": day, "workflows": {}}
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or set(value) != {"date", "workflows"}:
        raise ValueError("invalid_local_dispatch_record")
    if not isinstance(value["date"], str) or not isinstance(value["workflows"], dict):
        raise ValueError("invalid_local_dispatch_record")
    for name, record in value["workflows"].items():
        if name not in WORKFLOWS or not isinstance(record, dict):
            raise ValueError("invalid_local_dispatch_record")
        if record not in ({"status": "intent"}, {"status": "unverified"}) and (
            set(record) != {"status", "run_id"} or record["status"] != "dispatched"
            or type(record["run_id"]) is not int or record["run_id"] <= 0
        ):
            raise ValueError("invalid_local_dispatch_record")
    if value["date"] > day:
        raise ValueError("future_local_dispatch_record")
    return value if value["date"] == day else {"date": day, "workflows": {}}


def save_record(path, value):
    # Atomic write + fsync before the POST. Never put credentials in this file.
    fd, temp = tempfile.mkstemp(prefix=".dispatch-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def dispatch_morning(now, path, *, submit=gh_dispatch):
    if not in_window(now):
        return {}
    data = payload(now)
    day = data["inputs"]["logical_date"]
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.with_suffix(".lock").open("a", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {}
        record = read_record(path, day)
        for workflow in WORKFLOWS:
            # Any existing intent is treated as potentially dispatched: never
            # retry an ambiguous POST, even following a local process crash.
            if workflow in record["workflows"]:
                continue
            record["workflows"][workflow] = {"status": "intent"}
            save_record(path, record)
            try:
                run_id = submit(workflow, data)
            except RuntimeError:
                record["workflows"][workflow] = {"status": "unverified"}
            else:
                record["workflows"][workflow] = {"status": "dispatched", "run_id": run_id}
            save_record(path, record)
        return record["workflows"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dispatch", action="store_true",
                        help="Enable real workflow dispatches, only during the morning window")
    parser.add_argument("--state-file", type=Path,
                        default=Path.home() / ".local/state/newsagent2/morning-dispatch.json")
    args = parser.parse_args()
    now = datetime.now(STOCKHOLM)
    if not args.dispatch:
        print(json.dumps({"dry_run": True, "in_window": in_window(now),
                          "workflows": WORKFLOWS, "payload": payload(now)}))
        return 0
    try:
        result = dispatch_morning(now, args.state_file)
    except (OSError, ValueError):
        print("Morning dispatch blocked: invalid or unavailable local state; review before retry.")
        return 1
    print(json.dumps(result, sort_keys=True))
    return int(any(row["status"] != "dispatched" for row in result.values()))


if __name__ == "__main__":
    raise SystemExit(main())
