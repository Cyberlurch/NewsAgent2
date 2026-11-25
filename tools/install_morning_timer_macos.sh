#!/usr/bin/env bash
# Install an independent per-user macOS timer (no production API keys).
# Run from a trusted checkout after authenticating gh for NewsAgent2.
set -euo pipefail
if [[ "$(uname -s)" != Darwin ]]; then
  echo "This installer is for macOS." >&2; exit 1
fi
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python="$(command -v python3)"
gh="$(command -v gh)"
"$gh" auth status --hostname github.com >/dev/null
"$python" "$root/tools/morning_dispatch.py"
label="org.newsagent2.morning-dispatch"
agent="$HOME/Library/LaunchAgents/$label.plist"
logs="$HOME/Library/Logs/NewsAgent2"
mkdir -p "$(dirname "$agent")" "$logs"
"$python" - "$root" "$python" "$gh" "$agent" "$logs" "$label" <<'PY'
import os, plistlib, sys
root, python, gh, agent, logs, label = sys.argv[1:]
from pathlib import Path
config = {
    "Label": label,
    "ProgramArguments": [python, str(Path(root) / "tools/morning_dispatch.py"), "--dispatch"],
    "StartCalendarInterval": [{"Hour": 6, "Minute": 50}] +
        [{"Hour": 7, "Minute": m} for m in (0, 10, 20)],
    "EnvironmentVariables": {"PATH": ":".join([str(Path(gh).parent), "/usr/bin", "/bin", "/usr/sbin", "/sbin"])},
    "StandardOutPath": str(Path(logs) / "morning-dispatch.log"),
    "StandardErrorPath": str(Path(logs) / "morning-dispatch-error.log"),
}
with open(agent, "wb") as stream:
    plistlib.dump(config, stream)
os.chmod(agent, 0o600)
PY
plutil -lint "$agent"
launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$agent"
launchctl print "gui/$(id -u)/$label" >/dev/null
echo "Morning timer installed. First eligible window: 06:50–07:30 Stockholm."
echo "GitHub native schedule stays enabled as fallback."
