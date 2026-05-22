import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2.utils.diagnostics import YouTubeDiagnosticsCounters


def test_to_count_only_dict_includes_digest_counters():
    d = YouTubeDiagnosticsCounters().to_count_only_dict()
    assert "cyberlurch_digest_upserted_total" in d
    assert "cyberlurch_digest_pruned_total" in d
    assert "cyberlurch_digest_store_total" in d
    assert "cyberlurch_digest_invalid_records_removed_total" in d
    assert "cyberlurch_digest_invalid_records_skipped_total" in d

