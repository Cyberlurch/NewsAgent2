import json
import pathlib
from datetime import datetime
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2 import rollups


def test_upsert_monthly_rollup_overwrites_month():
    state = {}
    rollups.upsert_monthly_rollup(
        state,
        report_key="cybermed",
        month="2024-01",
        generated_at="2024-02-01T00:00:00Z",
        executive_summary=["initial"],
        top_items=[
            {"title": "One", "url": "https://a", "channel": "ch", "source": "youtube", "published_at": "2024-01-05"},
        ],
    )
    rollups.upsert_monthly_rollup(
        state,
        report_key="cybermed",
        month="2024-01",
        generated_at="2024-02-02T00:00:00Z",
        executive_summary=["updated"],
        top_items=[
            {"title": "Two", "url": "https://b", "channel": "ch2", "source": "youtube", "published_at": "2024-01-06"},
        ],
    )

    entries = state["reports"]["cybermed"]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["generated_at"] == "2024-02-02T00:00:00Z"
    assert entry["executive_summary"] == ["updated"]
    assert entry["top_items"][0]["title"] == "Two"
    assert entry["top_items"][0]["date"] == "2024-01-06"


def test_sanitize_item_preserves_bottom_line_and_truncates():
    long_bl = " Key point\nNext\tLine " + "x" * 700
    sanitized = rollups._sanitize_item(
        {
            "title": "One",
            "url": "https://a",
            "channel": "ch",
            "source": "youtube",
            "published_at": "2024-01-05",
            "bottom_line": long_bl,
        }
    )
    assert sanitized["bottom_line"].startswith("Key point Next Line")
    assert len(sanitized["bottom_line"]) == 600
    assert "\n" not in sanitized["bottom_line"]


def test_yearly_markdown_uses_previous_year_rollups():
    base_state = {
        "reports": {
            "cyberlurch": [
                {"month": "2023-12", "generated_at": "2024-01-01T00:00:00Z", "executive_summary": ["wrap"], "top_items": []},
                {"month": "2024-01", "generated_at": "2024-02-01T00:00:00Z", "executive_summary": ["jan"], "top_items": []},
            ]
        }
    }

    entries = rollups.rollups_for_year(base_state, "cyberlurch", 2024)
    assert len(entries) == 1
    assert entries[0]["month"] == "2024-01"

    md = rollups.render_yearly_markdown(
        report_title="The Cyberlurch Year in Review — 2024",
        report_language="en",
        year=2024,
        rollups=[
            {"month": f"2024-{i:02d}", "generated_at": "2025-01-01T00:00:00Z", "executive_summary": [f"m{i}"], "top_items": []}
            for i in range(1, 13)
        ],
    )

    for i in range(1, 13):
        label = datetime(year=2024, month=i, day=1).strftime("%B 2024")
        assert label in md
        assert f"m{i}" in md


def test_prune_rollups_keeps_newest_and_current_month():
    state = {
        "reports": {
            "cyberlurch": [
                {"month": "2023-11"},
                {"month": "2023-12"},
                {"month": "2024-01"},
                {"month": "2024-02"},
                {"month": "2024-03"},
            ]
        }
    }
    rollups.prune_rollups(state, report_key="cyberlurch", max_months=2, keep_month="2024-01")
    months = [entry["month"] for entry in state["reports"]["cyberlurch"]]
    assert months == ["2024-01", "2024-02", "2024-03"]


def test_prune_rollups_handles_missing_and_empty(tmp_path):
    missing_path = tmp_path / "missing.json"
    state_missing = rollups.load_rollups_state(str(missing_path))
    rollups.prune_rollups(state_missing, report_key="cybermed", max_months=12, keep_month="2024-01")
    assert state_missing["reports"] == {}
    assert missing_path.exists()

    empty_path = tmp_path / "empty.json"
    empty_path.write_text("", encoding="utf-8")
    state_empty = rollups.load_rollups_state(str(empty_path))
    rollups.prune_rollups(state_empty, report_key="cybermed", max_months=12, keep_month="2024-01")
    assert state_empty["reports"] == {}


def test_yearly_markdown_guardrails_and_limits():
    rollup_entries = []
    for month in range(1, 5):
        rollup_entries.append(
            {
                "month": f"2024-{month:02d}",
                "executive_summary": [f"summary {month}-{i}" for i in range(1, 5)],
                "top_items": [
                    {
                        "title": f"Starred {month}",
                        "url": f"https://example.com/star-{month}",
                        "channel": "ch",
                        "source": "yt",
                        "date": f"2024-{month:02d}-01",
                        "top_pick": True,
                    },
                ]
                + [
                    {
                        "title": f"Regular {month}-{i}",
                        "url": f"https://example.com/{month}-{i}",
                        "channel": "ch",
                        "source": "yt",
                        "date": f"2024-{month:02d}-0{i}",
                    }
                    for i in range(1, 5)
                ],
            }
        )

    md = rollups.render_yearly_markdown(
        report_title="Year in Review",
        report_language="en",
        year=2024,
        rollups=rollup_entries,
    )

    assert "Coverage note: only 4 monthly editions were available for this year." in md
    assert md.index("Coverage note:") < md.index("## Executive Summary")

    jan_idx = md.index("January 2024")
    mar_idx = md.index("March 2024")
    assert jan_idx < mar_idx

    top_section_start = md.index("## Top 10 items")
    first_star = md.index("⭐ [Starred 1]", top_section_start)
    first_regular = md.index("Regular 1-1", top_section_start)
    assert first_star < first_regular

    lines = md.splitlines()
    top_section_end = lines.index("## By month")
    top_section_lines = lines[lines.index("## Top 10 items") : top_section_end]
    assert sum(1 for ln in top_section_lines if "https://example.com/" in ln) == 10

    for heading in ("### January 2024", "### February 2024", "### March 2024", "### April 2024"):
        idx = lines.index(heading)
        end = next((i for i in range(idx + 1, len(lines)) if lines[i].startswith("### ")), len(lines))
        bullet_count = len([ln for ln in lines[idx + 1 : end] if ln.startswith("- ")])
        assert 1 <= bullet_count <= 3


def test_monthly_summary_ignores_run_metadata_block():
    markdown = """## Run Metadata
Run metadata is attached as a text file.

## Details
- Other content
"""
    top_items = [
        {"title": "Star pick", "url": "https://example.com/a", "channel": "ch", "source": "yt", "published_at": "2024-12-01", "top_pick": True},
        {"title": "Second pick", "url": "https://example.com/b", "channel": "ch", "source": "yt", "published_at": "2024-11-30"},
    ]

    summary = rollups.derive_monthly_summary(markdown, top_items=top_items, max_bullets=5)
    assert summary[0] == "Highlights derived from top items."
    assert all("metadata" not in s.lower() for s in summary)
    assert "Star pick" in summary[1]
    assert "Second pick" in summary[2]


def test_monthly_summary_skips_cybermed_metadata_block():
    markdown = """**Cybermed report metadata**
- diagnostics
## Executive Summary
- Real summary line
- Another
"""
    top_items = [{"title": "Fallback item", "url": "https://example.com/1", "channel": "ch", "source": "yt", "published_at": "2024-01-01"}]
    summary = rollups.derive_monthly_summary(markdown, top_items=top_items, max_bullets=5)
    assert any("real summary line" in s.lower() for s in summary)
    assert all("metadata" not in s.lower() for s in summary)


def test_metadata_placeholder_collapses_to_no_summary():
    state = {}
    rollups.upsert_monthly_rollup(
        state,
        report_key="cybermed",
        month="2025-12",
        generated_at="2026-01-01T00:00:00Z",
        executive_summary=["Cybermed report metadata**"],
        top_items=[],
    )

    entry = state["reports"]["cybermed"][0]
    assert entry["executive_summary"] == ["(no summary captured)"]


def test_yearly_markdown_uses_sanitized_rollup_summary():
    md = rollups.render_yearly_markdown(
        report_title="Year in Review",
        report_language="en",
        year=2025,
        rollups=[
            {
                "month": "2025-12",
                "executive_summary": ["Cybermed report metadata**"],
                "top_items": [
                    {"title": "First highlight", "url": "https://example.com/1", "channel": "ch", "source": "yt", "date": "2025-12-01", "top_pick": True},
                    {"title": "Second highlight", "url": "https://example.com/2", "channel": "ch", "source": "yt", "date": "2025-11-30"},
                ],
            }
        ],
    )

    assert "metadata" not in md.lower()
    assert "Highlights derived from top items." in md
    assert "December 2025" in md
    assert "Highlights derived from top items." in md


def test_sanitize_rollup_summary_filters_metadata_and_falls_back():
    sanitized = rollups.sanitize_rollup_summary(["Cybermed report metadata**"])
    assert sanitized == ["Highlights derived from top items."]
    assert all("metadata" not in line.lower() for line in sanitized)


def test_sanitize_rollup_summary_strips_attachment_line():
    sanitized = rollups.sanitize_rollup_summary(["Run metadata is attached as a text file."])
    assert sanitized == ["Highlights derived from top items."]


def test_sanitize_rollup_summary_preserves_normal_bullet():
    sanitized = rollups.sanitize_rollup_summary(["* Key finding **"])
    assert sanitized == ["Key finding"]


def test_yearly_markdown_includes_bottom_lines():
    md = rollups.render_yearly_markdown(
        report_title="Year in Review",
        report_language="en",
        year=2025,
        rollups=[
            {
                "month": "2025-12",
                "executive_summary": ["Summary line"],
                "top_items": [
                    {
                        "title": "Critical airway update",
                        "url": "https://example.com/airway",
                        "channel": "ch",
                        "source": "yt",
                        "date": "2025-12-01",
                        "top_pick": True,
                        "bottom_line": "Use video laryngoscopy for improved first-pass success.\nConsider device availability.",
                    },
                    {
                        "title": "Backup item",
                        "url": "https://example.com/backup",
                        "channel": "ch",
                        "source": "yt",
                        "date": "2025-11-30",
                        "bottom_line": "Helpful but less urgent.",
                    },
                ],
            }
        ],
    )

    assert "**BOTTOM LINE:**" in md
    assert "Use video laryngoscopy for improved first-pass success. Consider device availability." in md


def test_load_rollups_state_self_heals_existing_file(tmp_path):
    path = tmp_path / "rollups.json"
    raw_state = {
        "reports": {
            "cybermed": [
                {
                    "month": "2025-12",
                    "generated_at": "2026-01-01T00:00:00Z",
                    "executive_summary": ["Cybermed report metadata**"],
                    "top_items": [
                        {"title": "First highlight", "url": "https://example.com/1", "channel": "ch", "source": "yt", "published_at": "2025-12-01"}
                    ],
                }
            ]
        }
    }
    path.write_text(json.dumps(raw_state), encoding="utf-8")

    state = rollups.load_rollups_state(str(path))
    summary = state["reports"]["cybermed"][0]["executive_summary"]
    assert "metadata" not in " ".join(summary).lower()
    assert summary[0] == "Highlights derived from top items."

    cleaned = json.loads(path.read_text(encoding="utf-8"))
    cleaned_summary = cleaned["reports"]["cybermed"][0]["executive_summary"]
    assert "metadata" not in " ".join(cleaned_summary).lower()


def test_load_rollups_state_dedupes_latest_month(tmp_path):
    path = tmp_path / "rollups.json"
    payload = {
        "reports": {
            "cybermed": [
                {"month": "2025-12", "generated_at": "2025-12-31T00:00:00Z", "executive_summary": ["Old"], "top_items": []},
                {"month": "2025-12", "generated_at": "2026-01-01T00:00:00Z", "executive_summary": ["New"], "top_items": []},
            ]
        }
    }
    path.write_text(json.dumps(payload), encoding="utf-8")

    state = rollups.load_rollups_state(str(path))
    entries = state["reports"]["cybermed"]
    assert len(entries) == 1
    assert entries[0]["generated_at"] == "2026-01-01T00:00:00Z"
    assert entries[0]["executive_summary"] == ["New"]
