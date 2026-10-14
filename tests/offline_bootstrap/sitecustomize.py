"""Offline guard inherited by subprocess CLI tests; no runtime package change."""
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _isolation import Policy, install

install(Policy(Path(os.environ["NEWSAGENT_TEST_ROOT"]), Path(os.environ["NEWSAGENT_TEST_AUDIT"])))
