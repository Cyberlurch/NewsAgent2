from __future__ import annotations

from dataclasses import dataclass


@dataclass
class YouTubeDiagnosticsCounters:
    videos_total: int = 0
    low_signal_total: int = 0
    captions_attempted_total: int = 0
    captions_success_total: int = 0
    captions_empty_total: int = 0
    captions_error_total: int = 0
    poplar_total: int = 0
    poplar_low_signal: int = 0
    poplar_captions_attempted: int = 0
    poplar_captions_success: int = 0
    poplar_captions_empty: int = 0

    def to_log_line(self) -> str:
        return (
            "videos_total={videos_total} low_signal_total={low_signal_total} "
            "captions_attempted_total={captions_attempted_total} "
            "captions_success_total={captions_success_total} "
            "captions_empty_total={captions_empty_total} "
            "captions_error_total={captions_error_total} "
            "poplar_total={poplar_total} poplar_low_signal={poplar_low_signal} "
            "poplar_captions_attempted={poplar_captions_attempted} "
            "poplar_captions_success={poplar_captions_success} "
            "poplar_captions_empty={poplar_captions_empty}"
        ).format(**self.__dict__)

    def to_metadata_section(self) -> str:
        lines = [
            "## YouTube Diagnostics",
            f"- videos_total: {self.videos_total}",
            f"- low_signal_total: {self.low_signal_total}",
            f"- captions_attempted_total: {self.captions_attempted_total}",
            f"- captions_success_total: {self.captions_success_total}",
            f"- captions_empty_total: {self.captions_empty_total}",
            f"- captions_error_total: {self.captions_error_total}",
            f"- poplar_total: {self.poplar_total}",
            f"- poplar_low_signal: {self.poplar_low_signal}",
            f"- poplar_captions_attempted: {self.poplar_captions_attempted}",
            f"- poplar_captions_success: {self.poplar_captions_success}",
            f"- poplar_captions_empty: {self.poplar_captions_empty}",
        ]
        return "\n".join(lines)
