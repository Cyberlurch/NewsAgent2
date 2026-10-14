from __future__ import annotations

from datetime import date, timedelta

from .swedish_calendar import (
    cybermed_holiday_greeting_for_date,
    is_swedish_no_send_day,
    swedish_no_send_days,
)


def compute_cyberlurch_modes_for_date(d: date, report_mode_request: str = "scheduled") -> list[str]:
    from .edition_plan import planned_modes
    return planned_modes("cyberlurch", d) if report_mode_request == "scheduled" else [report_mode_request]


def _weekly_target_friday(d: date) -> date:
    return d + timedelta(days=(4 - d.weekday()))


def compute_cybermed_modes_for_date(d: date, report_mode_request: str = "scheduled") -> list[str]:
    from .edition_plan import planned_modes
    return planned_modes("cybermed", d) if report_mode_request == "scheduled" else [report_mode_request]


def compute_scheduled_run_plan(d: date) -> dict:
    no_send_days = swedish_no_send_days(d.year)
    holiday_name = no_send_days.get(d)
    cybermed_modes = compute_cybermed_modes_for_date(d)
    cyberlurch_modes = compute_cyberlurch_modes_for_date(d)
    shifted = []
    if "weekly" in cybermed_modes and d.weekday() != 4:
        shifted.append("weekly")
    first = date(d.year, d.month, 1)
    base_friday = first + timedelta(days=(4-first.weekday()) % 7)
    if "monthly" in cybermed_modes and d != base_friday:
        shifted.append("monthly")
    if "yearly" in cybermed_modes and d != base_friday:
        shifted.append("yearly")
    return {
        "cybermed_modes": cybermed_modes,
        "cyberlurch_modes": cyberlurch_modes,
        "skipped_reasons": [] if cybermed_modes else (["cybermed_no_send"] if is_swedish_no_send_day(d) else []),
        "holiday_name": holiday_name,
        "greeting": cybermed_holiday_greeting_for_date(d),
        "shifted_from": str(_weekly_target_friday(d)) if "weekly" in shifted else None,
        "shifted_to": str(d) if shifted else None,
        "shift_direction": "previous" if "weekly" in shifted else ("next" if shifted else None),
        "cybermed_shifted_modes": shifted,
    }
