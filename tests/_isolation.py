"""Process-wide offline audit, including swallowed failures and Python children."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import subprocess
import sys
import socket


class IsolationViolation(RuntimeError):
    pass


class Policy:
    def __init__(self, root: Path, log: Path):
        self.root, self.log = root.resolve(), log
        self.snapshot_read = False

    def reject(self, event):
        self.log.parent.mkdir(parents=True, exist_ok=True)
        with self.log.open("a", encoding="utf-8") as stream:
            if self.log.name == "collection-audit.log":
                import traceback
                functions = [frame.name for frame in traceback.extract_stack(limit=8)]
                stream.write(event + ":" + "/".join(functions) + "\n")
            else:
                stream.write(event + "\n")  # Categories only: no URLs, addresses or credentials.
        raise IsolationViolation("Offline test isolation forbids " + event)

    def protected(self, path):
        if not isinstance(path, (str, bytes, os.PathLike)):
            return False
        target = Path(os.fsdecode(path)).resolve()
        return any(target.is_relative_to(self.root / name) for name in
                   ("state", "data", "out", "reports", "health", ".cache"))

    def audit(self, event, args):
        if event.startswith("socket.") and event not in {"socket.__new__", "socket.gethostname"}:
            self.reject(event)
        if event in {"os.system", "os.exec", "os.posix_spawn"}:
            self.reject(event)
        if event == "subprocess.Popen":
            executable, argv = args[:2]
            name = Path(os.fsdecode(executable)).name
            startup = []
            for value in argv[1:]:
                if value in {"-c", "-m"} or not value.startswith("-"): break
                startup.append(value)
            if not name.startswith("python") or any(v.startswith("-") and not v.startswith("--") and set(v[1:]) & {"I", "S", "E"} for v in startup):
                self.reject("external_process")
        if event == "open":
            path, mode, flags = args
            if not isinstance(path, (str, bytes, os.PathLike)):
                return
            target = Path(os.fsdecode(path)).resolve()
            writing = bool(flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND))
            if target in {self.root / ".env", self.root / "data/recipients.json"} and not self.snapshot_read:
                self.reject("private_configuration_read")
            if not self.snapshot_read and ((target.is_relative_to(self.root / "state") and target.is_file()) or (writing and self.protected(target))):
                self.reject("production_path_access")
        if event in {"os.mkdir", "os.remove", "os.rmdir", "os.rename", "os.link", "os.symlink"}:
            def absolute(path, fd):
                if isinstance(path, (str, bytes, os.PathLike)) and fd not in {None, -1} and not Path(os.fsdecode(path)).is_absolute():
                    return Path(os.readlink(f"/proc/self/fd/{fd}")) / os.fsdecode(path)
                return path
            if event in {"os.rename", "os.link"}:
                paths = [absolute(args[0], args[2]), absolute(args[1], args[3])]
            elif event == "os.symlink":
                paths = [absolute(args[1], args[2])]
            else:
                paths = [absolute(args[0], args[2] if event == "os.mkdir" else args[1])]
            if any(self.protected(path) for path in paths):
                self.reject("production_path_mutation")

    def snapshot(self):
        self.snapshot_read = True
        try:
            result = {}
            for name in ("state", "data", "out", "reports", "health", ".cache"):
                directory = self.root / name
                if directory.exists():
                    result[name + "/"] = "directory"
                    for path in directory.rglob("*"):
                        result[str(path.relative_to(self.root))] = (
                            hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else "directory")
            return result
        finally:
            self.snapshot_read = False


ACTIVE = None
_INSTALLED = False
_POPEN = subprocess.Popen


def install(policy: Policy):
    global ACTIVE, _INSTALLED
    ACTIVE = policy
    if _INSTALLED:
        return
    _INSTALLED = True
    # urllib3 probes IPv6 by binding a socket at import time. Offline tests
    # use a fixed capability instead, including Python CLI children; the
    # socket audit guard remains strict for all actual attempts.
    socket.has_ipv6 = False
    sys.addaudithook(lambda event, args: ACTIVE.audit(event, args) if ACTIVE else None)

    class OfflinePopen(_POPEN):
        def __init__(self, *args, **kwargs):
            # Keep a class: yt-dlp subclasses Popen during import. Python CLI
            # children inherit the guard even when they replace PYTHONPATH.
            env = dict(os.environ if kwargs.get("env") is None else kwargs["env"])
            bootstrap = str(Path(__file__).parent / "offline_bootstrap")
            env["PYTHONPATH"] = bootstrap + os.pathsep + env.get("PYTHONPATH", "")
            env["NEWSAGENT_TEST_ROOT"] = str(ACTIVE.root)
            env["NEWSAGENT_TEST_AUDIT"] = str(ACTIVE.log)
            kwargs["env"] = env
            super().__init__(*args, **kwargs)
    subprocess.Popen = OfflinePopen
