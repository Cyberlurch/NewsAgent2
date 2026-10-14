"""Refresh the Python 3.12 lock in a fresh env; updates require a package allowlist."""
import argparse
from pathlib import Path
import re
import subprocess
import sys
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-update", action="append", default=[])
    parser.add_argument("--output", type=Path, default=Path("constraints-python312.txt"))
    args = parser.parse_args()
    if sys.version_info[:2] != (3, 12): parser.error("Python 3.12 required")
    if any(not re.fullmatch(r"[a-zA-Z0-9_.-]+", name) for name in args.allow_update): parser.error("package names only")
    normalize = lambda name: re.sub(r"[-_.]+", "-", name.lower())
    allowed = {normalize(name) for name in args.allow_update}
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix="newsagent-lock-") as folder:
        env = Path(folder) / "venv"
        subprocess.run([sys.executable, "-m", "venv", str(env)], check=True)
        python = str(env / "bin/python")
        constraints = Path(folder) / "constraints.txt"
        constraints.write_text("\n".join(line for line in (root / "constraints-python312.txt").read_text().splitlines()
            if not line or line.startswith("#") or normalize(line.split("==")[0]) not in allowed) + "\n")
        subprocess.run([python, "-m", "pip", "install", "pip==25.0.1"], check=True)
        subprocess.run([python, "-m", "pip", "install", "-c", str(constraints), "-r", str(root / "requirements-dev.txt")], check=True)
        subprocess.run([python, "-m", "pip", "check"], check=True)
        frozen = subprocess.check_output([python, "-m", "pip", "freeze", "--all"], text=True)
        args.output.write_text("# Python 3.12 / Linux; generated in a fresh venv by tools/update_dependency_lock.py.\n"
            "# Reviewed update allowlist: " + (", ".join(sorted(allowed)) or "none") + "\n" + frozen)


if __name__ == "__main__": main()
