from __future__ import annotations

from dataclasses import dataclass, field
from collections import Counter, defaultdict
from typing import Any


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
    ytdlp_js_runtime_configured: str = "unknown"
    ytdlp_remote_components_enabled: bool = False
    rss_primary_attempted_total: int = 0
    rss_primary_success_total: int = 0
    rss_primary_empty_total: int = 0
    rss_primary_error_total: int = 0
    ytdlp_warning_by_kind: dict[str, int] = field(default_factory=lambda: {"bot_check": 0, "no_js_runtime": 0, "http_403": 0, "http_429": 0, "timeout": 0, "extract_failed": 0, "no_subtitles": 0, "unknown": 0})
    ytdlp_error_by_kind: dict[str, int] = field(default_factory=lambda: {"bot_check": 0, "no_js_runtime": 0, "http_403": 0, "http_429": 0, "timeout": 0, "extract_failed": 0, "no_subtitles": 0, "unknown": 0})
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
    provider_attempted_by_name: dict[str, int] = field(default_factory=dict)
    provider_success_by_name: dict[str, int] = field(default_factory=dict)
    provider_empty_by_name: dict[str, int] = field(default_factory=dict)
    provider_error_by_name: dict[str, int] = field(default_factory=dict)
    provider_error_kind_by_name: dict[str, dict[str, int]] = field(default_factory=dict)
    cache_hit_total: int = 0
    cache_metadata_hit_no_text_total: int = 0
    cache_miss_total: int = 0
    cache_write_total: int = 0
    external_api_attempted_total: int = 0
    external_api_success_total: int = 0
    external_api_empty_total: int = 0
    external_api_error_total: int = 0
    youtube_api_metadata_attempted_total: int = 0
    youtube_api_metadata_success_total: int = 0
    youtube_api_metadata_empty_total: int = 0
    youtube_api_metadata_error_total: int = 0
    youtube_api_metadata_items_returned_total: int = 0
    youtube_api_channel_ids_discovered_total: int = 0
    youtube_transcript_provider: str = "none"
    managed_transcript_configured: bool = False
    managed_transcript_api_key_present: bool = False
    managed_transcript_base_url_present: bool = False
    managed_transcript_attempted_total: int = 0
    managed_transcript_success_total: int = 0
    managed_transcript_empty_total: int = 0
    managed_transcript_rate_limited_total: int = 0
    managed_transcript_skipped_budget_total: int = 0
    managed_transcript_chars_min: int = 0
    managed_transcript_chars_median: int = 0
    managed_transcript_chars_max: int = 0
    managed_transcript_error_total: int = 0
    managed_transcript_auth_error_total: int = 0
    managed_transcript_misconfigured_total: int = 0
    managed_transcript_budget_total: int = 0
    managed_transcript_budget_remaining: int = 0
    managed_transcript_skipped_previous_success_total: int = 0
    managed_transcript_skipped_retry_cooldown_total: int = 0
    managed_transcript_retry_due_total: int = 0
    managed_transcript_billable_success_estimate: int = 0
    managed_transcript_skipped_force_reprocess_cost_guard_total: int = 0
    full_text_items_total: int = 0
    metadata_only_items_total: int = 0
    full_text_ratio: float = 0.0
    transcript_chunking_attempted_total: int = 0
    transcript_chunking_success_total: int = 0
    transcript_chunking_error_total: int = 0
    transcript_chunking_skipped_budget_total: int = 0
    transcript_chunking_not_needed_total: int = 0
    transcript_direct_attempted_total: int = 0
    transcript_direct_success_total: int = 0
    transcript_direct_error_total: int = 0
    transcript_direct_chars_processed_total: int = 0
    transcript_direct_json_parse_error_total: int = 0
    transcript_direct_json_recovered_total: int = 0
    transcript_direct_fallback_text_total: int = 0
    transcript_direct_response_format_used_total: int = 0
    transcript_direct_response_format_rejected_total: int = 0
    transcript_direct_error_by_kind: dict[str, int] = field(default_factory=lambda: {"openai_error": 0, "response_format_unsupported": 0, "json_parse_error": 0, "empty_output": 0, "timeout": 0, "unknown": 0})
    transcript_processing_direct_total: int = 0
    transcript_processing_chunked_total: int = 0
    transcript_processing_excerpt_total: int = 0
    transcript_processing_not_needed_total: int = 0
    managed_transcript_full_within_limit_total: int = 0
    managed_transcript_chunked_total: int = 0
    managed_transcript_excerpt_total: int = 0
    transcript_chunks_total: int = 0
    transcript_chars_processed_total: int = 0
    transcript_full_chars_available_max: int = 0
    transcript_full_chars_available_median: int = 0
    cyberlurch_digest_upserted_total: int = 0
    cyberlurch_digest_pruned_total: int = 0
    cyberlurch_digest_store_total: int = 0
    cyberlurch_digest_invalid_records_removed_total: int = 0
    cyberlurch_digest_invalid_records_skipped_total: int = 0

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
            " youtube_api_metadata_attempted_total={youtube_api_metadata_attempted_total}"
            " youtube_api_metadata_success_total={youtube_api_metadata_success_total}"
            " youtube_api_metadata_empty_total={youtube_api_metadata_empty_total}"
            " youtube_api_metadata_error_total={youtube_api_metadata_error_total}"
            " youtube_api_metadata_items_returned_total={youtube_api_metadata_items_returned_total}"
            " youtube_api_channel_ids_discovered_total={youtube_api_channel_ids_discovered_total}"
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
            f"- youtube_api_metadata_attempted_total: {self.youtube_api_metadata_attempted_total}",
            f"- youtube_api_metadata_success_total: {self.youtube_api_metadata_success_total}",
            f"- youtube_api_metadata_empty_total: {self.youtube_api_metadata_empty_total}",
            f"- youtube_api_metadata_error_total: {self.youtube_api_metadata_error_total}",
            f"- youtube_api_metadata_items_returned_total: {self.youtube_api_metadata_items_returned_total}",
            f"- youtube_api_channel_ids_discovered_total: {self.youtube_api_channel_ids_discovered_total}",
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
            "ytdlp_disabled_due_to_bot_check",
            "ytdlp_skipped_due_to_bot_check_total",
            "ytdlp_js_runtime_configured",
            "ytdlp_remote_components_enabled",
            "rss_primary_attempted_total",
            "rss_primary_success_total",
            "rss_primary_empty_total",
            "rss_primary_error_total",
            "youtube_api_metadata_attempted_total",
            "youtube_api_metadata_success_total",
            "youtube_api_metadata_empty_total",
            "youtube_api_metadata_error_total",
            "youtube_api_metadata_items_returned_total",
            "youtube_api_channel_ids_discovered_total",
            "youtube_transcript_provider",
            "managed_transcript_configured",
            "managed_transcript_api_key_present",
            "managed_transcript_base_url_present",
            "managed_transcript_attempted_total",
            "managed_transcript_success_total",
            "managed_transcript_empty_total",
            "managed_transcript_error_total",
            "managed_transcript_auth_error_total",
            "managed_transcript_rate_limited_total",
            "managed_transcript_misconfigured_total",
            "managed_transcript_skipped_budget_total",
            "managed_transcript_chars_min",
            "managed_transcript_chars_median",
            "managed_transcript_chars_max",
            "managed_transcript_budget_total",
            "managed_transcript_budget_remaining",
            "managed_transcript_skipped_previous_success_total",
            "managed_transcript_skipped_retry_cooldown_total",
            "managed_transcript_retry_due_total",
            "managed_transcript_billable_success_estimate",
            "managed_transcript_skipped_force_reprocess_cost_guard_total",
            "transcript_chunking_attempted_total",
            "transcript_chunking_success_total",
            "transcript_chunking_error_total",
            "transcript_chunking_skipped_budget_total",
            "transcript_chunking_not_needed_total",
            "transcript_direct_attempted_total",
            "transcript_direct_success_total",
            "transcript_direct_error_total",
            "transcript_direct_chars_processed_total",
            "transcript_direct_json_parse_error_total",
            "transcript_direct_json_recovered_total",
            "transcript_direct_fallback_text_total",
            "transcript_direct_response_format_used_total",
            "transcript_direct_response_format_rejected_total",
            "transcript_processing_direct_total",
            "transcript_processing_chunked_total",
            "transcript_processing_excerpt_total",
            "transcript_processing_not_needed_total",
            "transcript_chunks_total",
            "transcript_chars_processed_total",
            "transcript_full_chars_available_max",
            "transcript_full_chars_available_median",
            "managed_transcript_full_within_limit_total",
            "managed_transcript_chunked_total",
            "managed_transcript_excerpt_total",
            "cyberlurch_digest_upserted_total",
            "cyberlurch_digest_pruned_total",
            "cyberlurch_digest_store_total",
            "cyberlurch_digest_invalid_records_removed_total",
            "cyberlurch_digest_invalid_records_skipped_total",
            "full_text_items_total",
            "metadata_only_items_total",
            "full_text_ratio",
            "metadata_enrichment_attempted_total",
            "metadata_enrichment_success_total",
            "metadata_enrichment_error_total",
        ]
        data = {key: getattr(self, key, 0) for key in keys}
        data["captions_error_by_kind"] = dict(self.captions_error_by_kind)
        data["ytdlp_warning_by_kind"] = dict(self.ytdlp_warning_by_kind)
        data["ytdlp_error_by_kind"] = dict(self.ytdlp_error_by_kind)
        data["provider_attempted_by_name"] = dict(self.provider_attempted_by_name)
        data["provider_success_by_name"] = dict(self.provider_success_by_name)
        data["transcript_direct_error_by_kind"] = dict(self.transcript_direct_error_by_kind)
        data["provider_empty_by_name"] = dict(self.provider_empty_by_name)
        data["provider_error_by_name"] = dict(self.provider_error_by_name)
        data["provider_error_kind_by_name"] = dict(self.provider_error_kind_by_name)
        data["cache_hit_total"] = self.cache_hit_total
        data["cache_miss_total"] = self.cache_miss_total
        data["cache_metadata_hit_no_text_total"] = self.cache_metadata_hit_no_text_total
        data["cache_write_total"] = self.cache_write_total
        data["external_api_attempted_total"] = self.external_api_attempted_total
        data["external_api_success_total"] = self.external_api_success_total
        data["external_api_empty_total"] = self.external_api_empty_total
        data["external_api_error_total"] = self.external_api_error_total
        return data
@dataclass
class CyberlurchOpenAIDiagnostics:
    """Count-only telemetry. Never accepts request content or exception text."""
    call_attempts_total: int = 0
    call_success_total: int = 0
    call_error_total: int = 0
    fallback_attempts_total: int = 0
    metadata_only_openai_calls: int = 0
    chunked_items: int = 0
    chunk_calls: int = 0
    by_stage_model: dict[str, dict[str, int]] = field(default_factory=dict)
    error_categories: Counter = field(default_factory=Counter)
    fallback_reasons: Counter = field(default_factory=Counter)
    safe_status_codes: Counter = field(default_factory=Counter)

    def record_attempt(self, stage: str, model: str, *, metadata_only: bool = False) -> None:
        self.call_attempts_total += 1
        if metadata_only:
            self.metadata_only_openai_calls += 1
        row = self.by_stage_model.setdefault(f"{stage}|{model}", {"calls": 0, "successes": 0, "errors": 0, "input_tokens": 0, "output_tokens": 0, "total_tokens": 0, "reasoning_tokens": 0, "non_reasoning_output_tokens": 0})
        row["calls"] += 1

    def record_success(self, stage: str, model: str, response: Any) -> None:
        self.call_success_total += 1
        row = self.by_stage_model[f"{stage}|{model}"]
        row["successes"] += 1
        usage = getattr(response, "usage", None)
        current: dict[str, int] = {}
        for target, names in (("input_tokens", ("prompt_tokens", "input_tokens")), ("output_tokens", ("completion_tokens", "output_tokens")), ("total_tokens", ("total_tokens",))):
            current[target] = next((int(getattr(usage, n, 0) or 0) for n in names if getattr(usage, n, None) is not None), 0)
            row[target] += current[target]
        details = getattr(usage, "completion_tokens_details", None)
        reasoning = min(current["output_tokens"], max(0, int(getattr(details, "reasoning_tokens", 0) or 0)))
        row["reasoning_tokens"] += reasoning
        row["non_reasoning_output_tokens"] += max(0, current["output_tokens"] - reasoning)

    def record_error(self, stage: str, model: str, category: str, exc: Exception) -> None:
        self.call_error_total += 1
        self.by_stage_model[f"{stage}|{model}"]["errors"] += 1
        self.error_categories[category] += 1
        status = getattr(exc, "status_code", None)
        code = getattr(exc, "code", None)
        safe = str(status if isinstance(status, int) else code or "")
        if safe and len(safe) <= 40 and all(c.isalnum() or c in "_-" for c in safe):
            self.safe_status_codes[safe] += 1

    def to_dict(self) -> dict[str, Any]:
        token_totals = {key: sum(row.get(key, 0) for row in self.by_stage_model.values()) for key in ("input_tokens", "output_tokens", "total_tokens", "reasoning_tokens", "non_reasoning_output_tokens")}
        return {"call_attempts_total": self.call_attempts_total, "call_success_total": self.call_success_total, "call_error_total": self.call_error_total, "calls_by_stage_model": dict(self.by_stage_model), **token_totals, "fallback_attempts_total": self.fallback_attempts_total, "fallback_reasons": dict(self.fallback_reasons), "errors_by_category": dict(self.error_categories), "safe_status_codes": dict(self.safe_status_codes), "metadata_only_openai_calls": self.metadata_only_openai_calls, "chunked_items": self.chunked_items, "chunk_calls": self.chunk_calls}


CYBERLURCH_OPENAI_DIAGNOSTICS = CyberlurchOpenAIDiagnostics()

def reset_cyberlurch_openai_diagnostics() -> CyberlurchOpenAIDiagnostics:
    global CYBERLURCH_OPENAI_DIAGNOSTICS
    CYBERLURCH_OPENAI_DIAGNOSTICS = CyberlurchOpenAIDiagnostics()
    return CYBERLURCH_OPENAI_DIAGNOSTICS
