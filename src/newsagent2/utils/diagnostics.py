from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class YouTubeDiagnosticsCounters:
    yt_dlp_version: str = "unknown"
    videos_total: int = 0
    low_signal_total: int = 0
    captions_attempted_total: int = 0
    captions_success_total: int = 0
    captions_empty_total: int = 0
    captions_error_total: int = 0
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
    blackscout_total: int = 0
    blackscout_low_signal: int = 0
    blackscout_captions_attempted: int = 0
    blackscout_captions_success: int = 0
    blackscout_captions_empty: int = 0
    blackscout_captions_error: int = 0

    def to_log_line(self) -> str:
        return (
            "yt_dlp_version={yt_dlp_version} videos_total={videos_total} "
            "low_signal_total={low_signal_total} "
            "captions_attempted_total={captions_attempted_total} "
            "captions_success_total={captions_success_total} "
            "captions_empty_total={captions_empty_total} "
            "captions_error_total={captions_error_total} "
            "poplar_total={poplar_total} poplar_low_signal={poplar_low_signal} "
            "poplar_captions_attempted={poplar_captions_attempted} "
            "poplar_captions_success={poplar_captions_success} "
            "poplar_captions_empty={poplar_captions_empty} "
            "poplar_captions_error={poplar_captions_error} "
            "blackscout_total={blackscout_total} blackscout_low_signal={blackscout_low_signal} "
            "blackscout_captions_attempted={blackscout_captions_attempted} "
            "blackscout_captions_success={blackscout_captions_success} "
            "blackscout_captions_empty={blackscout_captions_empty} "
            "blackscout_captions_error={blackscout_captions_error} "
            "captions_error_by_kind={captions_error_by_kind} "
            "low_signal_reason_counts={low_signal_reason_counts}"
        ).format(**self.__dict__)

    def to_metadata_section(self) -> str:
        lines = [
            "## YouTube Diagnostics",
            f"- yt_dlp_version: {self.yt_dlp_version}",
            f"- videos_total: {self.videos_total}",
            f"- low_signal_total: {self.low_signal_total}",
            f"- captions_attempted_total: {self.captions_attempted_total}",
            f"- captions_success_total: {self.captions_success_total}",
            f"- captions_empty_total: {self.captions_empty_total}",
            f"- captions_error_total: {self.captions_error_total}",
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
            f"- blackscout_total: {self.blackscout_total}",
            f"- blackscout_low_signal: {self.blackscout_low_signal}",
            f"- blackscout_captions_attempted: {self.blackscout_captions_attempted}",
            f"- blackscout_captions_success: {self.blackscout_captions_success}",
            f"- blackscout_captions_empty: {self.blackscout_captions_empty}",
            f"- blackscout_captions_error: {self.blackscout_captions_error}",
        ]
        return "\n".join(lines)
