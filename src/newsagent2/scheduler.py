from __future__ import annotations

from datetime import date, timedelta

from .swedish_calendar import (
    cybermed_holiday_greeting_for_date,
    is_swedish_no_send_day,
    next_swedish_business_day,
    previous_swedish_business_day,
    swedish_no_send_days,
)


def compute_cyberlurch_modes_for_date(d: date, report_mode_request: str = "scheduled") -> list[str]:
    if report_mode_request != "scheduled":
        return [report_mode_request]
    modes: list[str] = []
    if d.day == 1:
        modes.append("monthly")
        if d.month == 1:
            modes.append("yearly")
    modes.append("daily")
    if d.weekday() == 4:
        modes.append("weekly")
    return modes


def _weekly_target_friday(d: date) -> date:
    return d + timedelta(days=(4 - d.weekday()))


def compute_cybermed_modes_for_date(d: date, report_mode_request: str = "scheduled") -> list[str]:
    if report_mode_request != "scheduled":
        return [report_mode_request]
    modes: list[str] = []
    if not is_swedish_no_send_day(d):
        modes.append("daily")

    friday = _weekly_target_friday(d)
    if d <= friday:
        weekly_run = previous_swedish_business_day(friday + timedelta(days=1)) if is_swedish_no_send_day(friday) else friday
        if d == weekly_run:
            modes.append("weekly")

    if d.day == 1:
        if not is_swedish_no_send_day(d):
            modes.append("monthly")
            if d.month == 1:
                modes.append("yearly")
    else:
        first = date(d.year, d.month, 1)
        if is_swedish_no_send_day(first) and d == next_swedish_business_day(first):
            modes.append("monthly")
        if d.month == 1:
            jan1 = date(d.year, 1, 1)
            if is_swedish_no_send_day(jan1) and d == next_swedish_business_day(jan1):
                modes.append("yearly")
    return modes


def compute_scheduled_run_plan(d: date) -> dict:
    no_send_days = swedish_no_send_days(d.year)
    holiday_name = no_send_days.get(d)
    cybermed_modes = compute_cybermed_modes_for_date(d)
    cyberlurch_modes = compute_cyberlurch_modes_for_date(d)
    shifted = []
    if "weekly" in cybermed_modes and d.weekday() != 4:
        shifted.append("weekly")
    first = date(d.year, d.month, 1)
    if "monthly" in cybermed_modes and d != first:
        shifted.append("monthly")
    if "yearly" in cybermed_modes and d != date(d.year, 1, 1):
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
