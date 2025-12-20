import pathlib
import sys
from datetime import datetime, timezone

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2.main import _parse_iso_utc


def test_parse_iso_utc_with_z_suffix():
    dt = _parse_iso_utc("2025-12-20T05:30:00Z")
    assert isinstance(dt, datetime)
    assert dt.tzinfo is not None
    assert dt == datetime(2025, 12, 20, 5, 30, tzinfo=timezone.utc)


def test_parse_iso_utc_empty_string():
    assert _parse_iso_utc("") is None
