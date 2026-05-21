import os
import pathlib
import sys
import unittest
from datetime import datetime
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from newsagent2 import reporter


class CyberlurchPeriodicRenderingTests(unittest.TestCase):
    def test_weekly_top_videos_are_links_and_sources_removed(self):
        items = [
            {
                "id": "v1",
                "title": "Video One",
                "url": "https://example.com/one",
                "channel": "Channel A",
                "published_at": datetime(2024, 1, 2),
                "top_pick": True,
            },
            {
                "id": "v2",
                "title": "Video Two",
                "url": "https://example.com/two",
                "channel": "Channel B",
                "published_at": datetime(2024, 1, 3),
            },
        ]

        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(
                items,
                overview_markdown="",
                details_by_id={},
                report_title="Cyberlurch Weekly",
                report_language="en",
                report_mode="weekly",
            )

        self.assertIn("## Top videos (this period)", md)
        self.assertIn("[Video One](https://example.com/one)", md)
        self.assertIn("[Video Two](https://example.com/two)", md)
        self.assertNotIn("## Sources", md)


    def test_monthly_sources_removed_but_top_videos_present(self):
        items = [
            {
                "id": "v3",
                "title": "Video Three",
                "url": "https://example.com/three",
                "channel": "Channel C",
                "published_at": datetime(2024, 2, 1),
            }
        ]

        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(
                items,
                overview_markdown="",
                details_by_id={},
                report_title="Cyberlurch — Monthly",
                report_language="en",
                report_mode="monthly",
            )

        self.assertIn("## Top videos (this period)", md)
        self.assertIn("[Video Three](https://example.com/three)", md)
        self.assertNotIn("## Sources", md)


    def test_daily_top_videos_present_and_sources_deduped(self):
        items = [
            {
                "id": "d1",
                "title": "Daily Video",
                "url": "https://example.com/daily",
                "channel": "Channel D",
                "published_at": datetime(2024, 3, 1),
            }
        ]

        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(
                items,
                overview_markdown="",
                details_by_id={},
                report_title="Cyberlurch Daily",
                report_language="en",
                report_mode="daily",
            )

        self.assertIn("## Top videos", md)
        self.assertIn("[Daily Video](https://example.com/daily)", md)
        self.assertNotIn("## Sources", md)


    def test_daily_metadata_only_item_is_marked(self):
        items = [
            {
                "id": "d2",
                "title": "Metadata Video",
                "url": "https://example.com/meta",
                "channel": "Channel M",
                "published_at": datetime(2024, 3, 2),
                "content_status": "metadata_only",
            }
        ]

        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(
                items,
                overview_markdown="",
                details_by_id={},
                report_title="Cyberlurch Daily",
                report_language="en",
                report_mode="daily",
            )

        self.assertIn("## Top videos", md)
        self.assertIn("Source: metadata only", md)
        self.assertIn("Transcript/caption text unavailable; listed from metadata only.", md)

    def test_daily_source_label_for_description(self):
        items = [{
            "id": "d3", "title": "Desc Video", "url": "https://example.com/desc",
            "channel": "Channel D", "published_at": datetime(2024, 3, 3),
            "text_source": "description", "content_status": "full_text",
        }]
        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(items, overview_markdown="", details_by_id={}, report_title="Cyberlurch Daily", report_language="en", report_mode="daily")
        self.assertIn("Source: YouTube description", md)

    def test_daily_source_label_for_chunked_transcript(self):
        items = [{
            "id": "d4", "title": "Chunked Video", "url": "https://example.com/chunked",
            "channel": "Channel D", "published_at": datetime(2024, 3, 3),
            "text_source": "managed_transcript", "content_status": "full_text",
            "transcript_processing": "chunked_full_transcript",
            "transcript_chunking_success": True,
        }]
        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(items, overview_markdown="", details_by_id={}, report_title="Cyberlurch Daily", report_language="en", report_mode="daily")
        self.assertIn("Source: TranscriptAPI, full transcript chunked", md)

    def test_daily_source_label_for_full_transcript_within_limit(self):
        items = [{
            "id": "d5", "title": "Short Transcript", "url": "https://example.com/short",
            "channel": "Channel E", "published_at": datetime(2024, 3, 4),
            "text_source": "managed_transcript", "content_status": "full_text",
            "transcript_processing": "direct_full_transcript", "transcript_direct_success": True,
        }]
        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(items, overview_markdown="", details_by_id={}, report_title="Cyberlurch Daily", report_language="en", report_mode="daily")
        self.assertIn("Source: TranscriptAPI, full transcript analyzed", md)

    def test_daily_source_label_for_transcript_excerpt(self):
        items = [{
            "id": "d6", "title": "Truncated Transcript", "url": "https://example.com/excerpt",
            "channel": "Channel E", "published_at": datetime(2024, 3, 4),
            "text_source": "managed_transcript", "content_status": "full_text",
            "transcript_full_chars_available": 12000, "transcript_chars_used_for_summary": 6000, "transcript_was_truncated": True,
        }]
        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(items, overview_markdown="", details_by_id={}, report_title="Cyberlurch Daily", report_language="en", report_mode="daily")
        self.assertIn("Source: TranscriptAPI, transcript excerpt fallback", md)

    def test_daily_renders_topic_sections_and_deep_dives(self):
        items = [{
            "id": "d7", "title": "Topic Video", "url": "https://example.com/topic",
            "channel": "Channel T", "published_at": datetime(2024, 3, 4),
            "topic": "Geopolitik, Krieg & Machtblöcke",
        }]
        details = {"d7": "Key takeaways:\n- point\n\n**BOTTOM LINE:** topic detail"}
        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(items, overview_markdown="## Executive Summary\n\nOverview", details_by_id=details, report_title="Cyberlurch Daily", report_language="en", report_mode="daily")
        self.assertIn("## Executive Summary", md)
        self.assertIn("## Themenbereiche / Topic sections", md)
        self.assertIn("## Deep Dives", md)
        self.assertIn("## Top videos", md)

    def test_daily_source_label_for_excerpt_fallback_processing(self):
        items = [{
            "id": "d6b", "title": "Fallback Transcript", "url": "https://example.com/fallback",
            "channel": "Channel E", "published_at": datetime(2024, 3, 4),
            "text_source": "managed_transcript", "content_status": "full_text",
            "transcript_processing": "excerpt_fallback",
        }]
        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(items, overview_markdown="", details_by_id={}, report_title="Cyberlurch Daily", report_language="en", report_mode="daily")
        self.assertIn("Source: TranscriptAPI, transcript excerpt fallback", md)

    def test_daily_source_label_for_direct_fallback_digest_processing(self):
        items = [{
            "id": "d6c", "title": "Fallback Digest", "url": "https://example.com/fallback-digest",
            "channel": "Channel E", "published_at": datetime(2024, 3, 4),
            "text_source": "managed_transcript", "content_status": "full_text",
            "transcript_processing": "direct_full_transcript_fallback", "transcript_direct_success": True,
        }]
        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(items, overview_markdown="", details_by_id={}, report_title="Cyberlurch Daily", report_language="en", report_mode="daily")
        self.assertIn("Source: TranscriptAPI, full transcript analyzed (fallback digest)", md)

    def test_topic_sections_avoid_pubmed_wording(self):
        items = [{
            "id": "d8", "title": "Topic Video", "url": "https://example.com/topic2",
            "channel": "Channel T", "published_at": datetime(2024, 3, 4),
            "topic": "Ops", "transcript_full_summary": "Key infrastructure changes discussed"
        }]
        details = {"d8": "detail"}
        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(items, overview_markdown="## Executive Summary\n\nOverview", details_by_id=details, report_title="Cyberlurch Daily", report_language="en", report_mode="daily")
        for bad in ["This paper", "clinical implication", "Evidence strength", "methods not classified", "abstract keywords"]:
            self.assertNotIn(bad, md)
        self.assertNotIn("What it says:", md)

    def test_topic_sections_keep_real_bucket_name_and_clean_list_formatting(self):
        items = [{
            "id": "d9", "title": "Topic Video", "url": "https://example.com/topic3",
            "channel": "Channel T", "published_at": datetime(2024, 3, 4),
            "topic": "Israel, Nahost & Sicherheitslage",
            "transcript_full_summary": ["Point A", "Point B"],
            "important_details": ["Detail 1", "Detail 2"],
        }]
        details = {"d9": "detail"}
        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(items, overview_markdown="## Executive Summary\n\nOverview", details_by_id=details, report_title="Cyberlurch Daily", report_language="en", report_mode="daily")
        self.assertIn("### Israel, Nahost & Sicherheitslage", md)
        self.assertNotIn('["', md)
        self.assertNotIn("Content point:", md)
        self.assertNotIn("- Why it matters:", md)

    def test_deep_dive_internal_headings_demoted_to_h4(self):
        items = [{"id": "d10", "title": "Topic Video", "url": "https://example.com/topic4", "channel": "Channel T", "published_at": datetime(2024, 3, 4), "topic": "Ops"}]
        details = {"d10": "# Key takeaways\n- a\n## Details & reasoning\n- b"}
        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(items, overview_markdown="overview", details_by_id=details, report_title="Cyberlurch Daily", report_language="en", report_mode="daily")
        self.assertIn("#### Key takeaways", md)
        self.assertIn("#### Details & reasoning", md)
        self.assertNotIn("\n# Key takeaways", md)
        self.assertNotIn("\n## Details & reasoning", md)

    def test_deep_dive_removes_duplicate_title_channel_block(self):
        items = [{"id": "d11", "title": "Topic Video", "url": "https://example.com/topic5", "channel": "Channel T", "published_at": datetime(2024, 3, 4), "topic": "Ops"}]
        details = {"d11": "Title: Topic Video\nChannel: Channel T\nPublished: 2024-03-04\nWatch on YouTube: https://example.com/topic5\n\n# Title\n## Uncertainties\n- unsure"}
        with patch.dict(os.environ, {"REPORT_KEY": "cyberlurch"}):
            md = reporter.to_markdown(items, overview_markdown="overview", details_by_id=details, report_title="Cyberlurch Daily", report_language="en", report_mode="daily")
        self.assertNotIn("Title: Topic Video", md)
        self.assertNotIn("Channel: Channel T", md)
        self.assertNotIn("Published: 2024-03-04", md)
        self.assertNotIn("Watch on YouTube", md)
        self.assertNotIn("# Title", md)
        self.assertIn("#### Uncertainties", md)


if __name__ == "__main__":
    unittest.main()
