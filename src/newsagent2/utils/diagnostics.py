from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class YouTubeDiagnosticsCounters:
    yt_dlp_version: str = "unknown"
    channels_attempted_total: int = 0
    channels_success_total: int = 0
    channels_error_total: int = 0
    videos_listed_total: int = 0
    videos_kept_after_date_total: int = 0
    videos_skipped_by_date_total: int = 0
    videos_skipped_empty_text_total: int = 0
    metadata_only_total: int = 0
    rss_fallback_attempted_total: int = 0
    rss_fallback_success_total: int = 0
    rss_fallback_error_total: int = 0
    rss_fallback_resolution_failed_total: int = 0
    channels_file_used: str = ""
    metadata_enrichment_attempted_total: int = 0
    metadata_enrichment_success_total: int = 0
    metadata_enrichment_error_total: int = 0
    videos_total: int = 0
    low_signal_total: int = 0
    captions_attempted_total: int = 0
    captions_success_total: int = 0
    captions_empty_total: int = 0
    captions_error_total: int = 0
    timedtext_attempted_total: int = 0
    timedtext_success_total: int = 0
    timedtext_empty_total: int = 0
    timedtext_error_total: int = 0
    transcript_success_total: int = 0
    transcript_empty_total: int = 0
    ytdlp_disabled_due_to_bot_check: bool = False
    ytdlp_skipped_due_to_bot_check_total: int = 0
    captions_error_by_kind: dict[str, int] = field(
        default_factory=lambda: {
            "timeout": 0,
            "no_subtitles": 0,
            "http_403": 0,
            "http_429": 0,
            "bot_check": 0,
            "cli_option_error": 0,
            "extract_failed": 0,
            "unknown": 0,
        }
    )
    low_signal_reason_counts: dict[str, int] = field(
        default_factory=lambda: {
            "too_short": 0,
            "link_dense": 0,
            "promo_keywords": 0,
            "empty": 0,
        }
    )
    poplar_total: int = 0
    poplar_low_signal: int = 0
    poplar_captions_attempted: int = 0
    poplar_captions_success: int = 0
    poplar_captions_empty: int = 0
    poplar_captions_error: int = 0
    poplar_timedtext_attempted: int = 0
    poplar_timedtext_success: int = 0
    poplar_timedtext_empty: int = 0
    poplar_timedtext_error: int = 0
    poplar_ytdlp_skipped_due_to_bot_check: int = 0
    blackscout_total: int = 0
    blackscout_low_signal: int = 0
    blackscout_captions_attempted: int = 0
    blackscout_captions_success: int = 0
    blackscout_captions_empty: int = 0
    blackscout_captions_error: int = 0
    blackscout_timedtext_attempted: int = 0
    blackscout_timedtext_success: int = 0
    blackscout_timedtext_empty: int = 0
    blackscout_timedtext_error: int = 0
    blackscout_ytdlp_skipped_due_to_bot_check: int = 0

    def to_log_line(self) -> str:
        return (
            "yt_dlp_version={yt_dlp_version} "
            "channels_attempted_total={channels_attempted_total} "
            "channels_success_total={channels_success_total} "
            "channels_error_total={channels_error_total} "
            "videos_listed_total={videos_listed_total} "
            "videos_kept_after_date_total={videos_kept_after_date_total} "
            "videos_skipped_by_date_total={videos_skipped_by_date_total} "
            "videos_skipped_empty_text_total={videos_skipped_empty_text_total} "
            "metadata_only_total={metadata_only_total} "
            "rss_fallback_attempted_total={rss_fallback_attempted_total} "
            "rss_fallback_success_total={rss_fallback_success_total} "
            "rss_fallback_error_total={rss_fallback_error_total} "
            "rss_fallback_resolution_failed_total={rss_fallback_resolution_failed_total} "
            "metadata_enrichment_attempted_total={metadata_enrichment_attempted_total} "
            "metadata_enrichment_success_total={metadata_enrichment_success_total} "
            "metadata_enrichment_error_total={metadata_enrichment_error_total} "
            "videos_total={videos_total} "
            "low_signal_total={low_signal_total} "
            "captions_attempted_total={captions_attempted_total} "
            "captions_success_total={captions_success_total} "
            "captions_empty_total={captions_empty_total} "
            "captions_error_total={captions_error_total} "
            "timedtext_attempted_total={timedtext_attempted_total} "
            "timedtext_success_total={timedtext_success_total} "
            "timedtext_empty_total={timedtext_empty_total} "
            "timedtext_error_total={timedtext_error_total} "
            "transcript_success_total={transcript_success_total} "
            "transcript_empty_total={transcript_empty_total} "
            "ytdlp_disabled_due_to_bot_check={ytdlp_disabled_due_to_bot_check} "
            "ytdlp_skipped_due_to_bot_check_total={ytdlp_skipped_due_to_bot_check_total} "
            "poplar_total={poplar_total} poplar_low_signal={poplar_low_signal} "
            "poplar_captions_attempted={poplar_captions_attempted} "
            "poplar_captions_success={poplar_captions_success} "
            "poplar_captions_empty={poplar_captions_empty} "
            "poplar_captions_error={poplar_captions_error} "
            "poplar_timedtext_attempted={poplar_timedtext_attempted} "
            "poplar_timedtext_success={poplar_timedtext_success} "
            "poplar_timedtext_empty={poplar_timedtext_empty} "
            "poplar_timedtext_error={poplar_timedtext_error} "
            "poplar_ytdlp_skipped_due_to_bot_check={poplar_ytdlp_skipped_due_to_bot_check} "
            "blackscout_total={blackscout_total} blackscout_low_signal={blackscout_low_signal} "
            "blackscout_captions_attempted={blackscout_captions_attempted} "
            "blackscout_captions_success={blackscout_captions_success} "
            "blackscout_captions_empty={blackscout_captions_empty} "
            "blackscout_captions_error={blackscout_captions_error} "
            "blackscout_timedtext_attempted={blackscout_timedtext_attempted} "
            "blackscout_timedtext_success={blackscout_timedtext_success} "
            "blackscout_timedtext_empty={blackscout_timedtext_empty} "
            "blackscout_timedtext_error={blackscout_timedtext_error} "
            "blackscout_ytdlp_skipped_due_to_bot_check={blackscout_ytdlp_skipped_due_to_bot_check} "
            "captions_error_by_kind={captions_error_by_kind} "
            "low_signal_reason_counts={low_signal_reason_counts}"
        ).format(**self.__dict__)

    def to_metadata_section(self) -> str:
        lines = [
            "## YouTube Diagnostics",
            f"- yt_dlp_version: {self.yt_dlp_version}",
            f"- channels_attempted_total: {self.channels_attempted_total}",
            f"- channels_success_total: {self.channels_success_total}",
            f"- channels_error_total: {self.channels_error_total}",
            f"- videos_listed_total: {self.videos_listed_total}",
            f"- videos_kept_after_date_total: {self.videos_kept_after_date_total}",
            f"- videos_skipped_by_date_total: {self.videos_skipped_by_date_total}",
            f"- videos_skipped_empty_text_total: {self.videos_skipped_empty_text_total}",
            f"- metadata_only_total: {self.metadata_only_total}",
            f"- rss_fallback_attempted_total: {self.rss_fallback_attempted_total}",
            f"- rss_fallback_success_total: {self.rss_fallback_success_total}",
            f"- rss_fallback_error_total: {self.rss_fallback_error_total}",
            f"- rss_fallback_resolution_failed_total: {self.rss_fallback_resolution_failed_total}",
            f"- channels_file_used: {self.channels_file_used}",
            f"- metadata_enrichment_attempted_total: {self.metadata_enrichment_attempted_total}",
            f"- metadata_enrichment_success_total: {self.metadata_enrichment_success_total}",
            f"- metadata_enrichment_error_total: {self.metadata_enrichment_error_total}",
            f"- videos_total: {self.videos_total}",
            f"- low_signal_total: {self.low_signal_total}",
            f"- captions_attempted_total: {self.captions_attempted_total}",
            f"- captions_success_total: {self.captions_success_total}",
            f"- captions_empty_total: {self.captions_empty_total}",
            f"- captions_error_total: {self.captions_error_total}",
            f"- timedtext_attempted_total: {self.timedtext_attempted_total}",
            f"- timedtext_success_total: {self.timedtext_success_total}",
            f"- timedtext_empty_total: {self.timedtext_empty_total}",
            f"- timedtext_error_total: {self.timedtext_error_total}",
            f"- transcript_success_total: {self.transcript_success_total}",
            f"- transcript_empty_total: {self.transcript_empty_total}",
            f"- ytdlp_disabled_due_to_bot_check: {self.ytdlp_disabled_due_to_bot_check}",
            f"- ytdlp_skipped_due_to_bot_check_total: {self.ytdlp_skipped_due_to_bot_check_total}",
            "- captions_error_by_kind:",
            f"  - timeout: {self.captions_error_by_kind.get('timeout', 0)}",
            f"  - no_subtitles: {self.captions_error_by_kind.get('no_subtitles', 0)}",
            f"  - http_403: {self.captions_error_by_kind.get('http_403', 0)}",
            f"  - http_429: {self.captions_error_by_kind.get('http_429', 0)}",
            f"  - bot_check: {self.captions_error_by_kind.get('bot_check', 0)}",
            f"  - cli_option_error: {self.captions_error_by_kind.get('cli_option_error', 0)}",
            f"  - extract_failed: {self.captions_error_by_kind.get('extract_failed', 0)}",
            f"  - unknown: {self.captions_error_by_kind.get('unknown', 0)}",
            "- low_signal_reason_counts:",
            f"  - too_short: {self.low_signal_reason_counts.get('too_short', 0)}",
            f"  - link_dense: {self.low_signal_reason_counts.get('link_dense', 0)}",
            f"  - promo_keywords: {self.low_signal_reason_counts.get('promo_keywords', 0)}",
            f"  - empty: {self.low_signal_reason_counts.get('empty', 0)}",
            f"- poplar_total: {self.poplar_total}",
            f"- poplar_low_signal: {self.poplar_low_signal}",
            f"- poplar_captions_attempted: {self.poplar_captions_attempted}",
            f"- poplar_captions_success: {self.poplar_captions_success}",
            f"- poplar_captions_empty: {self.poplar_captions_empty}",
            f"- poplar_captions_error: {self.poplar_captions_error}",
            f"- poplar_timedtext_attempted: {self.poplar_timedtext_attempted}",
            f"- poplar_timedtext_success: {self.poplar_timedtext_success}",
            f"- poplar_timedtext_empty: {self.poplar_timedtext_empty}",
            f"- poplar_timedtext_error: {self.poplar_timedtext_error}",
            f"- poplar_ytdlp_skipped_due_to_bot_check: {self.poplar_ytdlp_skipped_due_to_bot_check}",
            f"- blackscout_total: {self.blackscout_total}",
            f"- blackscout_low_signal: {self.blackscout_low_signal}",
            f"- blackscout_captions_attempted: {self.blackscout_captions_attempted}",
            f"- blackscout_captions_success: {self.blackscout_captions_success}",
            f"- blackscout_captions_empty: {self.blackscout_captions_empty}",
            f"- blackscout_captions_error: {self.blackscout_captions_error}",
            f"- blackscout_timedtext_attempted: {self.blackscout_timedtext_attempted}",
            f"- blackscout_timedtext_success: {self.blackscout_timedtext_success}",
            f"- blackscout_timedtext_empty: {self.blackscout_timedtext_empty}",
            f"- blackscout_timedtext_error: {self.blackscout_timedtext_error}",
            f"- blackscout_ytdlp_skipped_due_to_bot_check: {self.blackscout_ytdlp_skipped_due_to_bot_check}",
        ]
        return "\n".join(lines)

    def to_count_only_dict(self) -> dict[str, object]:
        keys = [
            "yt_dlp_version",
            "channels_file_used",
            "channels_attempted_total",
            "channels_success_total",
            "channels_error_total",
            "videos_listed_total",
            "videos_kept_after_date_total",
            "videos_skipped_by_date_total",
            "videos_skipped_empty_text_total",
            "metadata_only_total",
            "rss_fallback_attempted_total",
            "rss_fallback_success_total",
            "rss_fallback_error_total",
            "rss_fallback_resolution_failed_total",
            "transcript_success_total",
            "transcript_empty_total",
            "timedtext_success_total",
            "timedtext_error_total",
            "captions_success_total",
            "captions_error_total",
        ]
        data = {key: getattr(self, key, 0) for key in keys}
        data["captions_error_by_kind"] = dict(self.captions_error_by_kind)
        return data

