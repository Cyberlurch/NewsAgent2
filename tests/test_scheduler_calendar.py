from datetime import date

from src.newsagent2.scheduler import compute_cyberlurch_modes_for_date, compute_cybermed_modes_for_date


def test_cybermed_daily_blocked_weekend_and_midsummer_eve():
    assert "daily" not in compute_cybermed_modes_for_date(date(2026, 6, 20))
    assert "daily" not in compute_cybermed_modes_for_date(date(2026, 6, 19))


def test_cybermed_weekly_shifts_from_midsummer_friday_to_thursday():
    assert "weekly" in compute_cybermed_modes_for_date(date(2026, 6, 18))


def test_monthly_and_yearly_shift_forward_from_jan1():
    modes = compute_cybermed_modes_for_date(date(2022, 1, 3))
    assert "monthly" in modes and "yearly" in modes


def test_cyberlurch_independent_of_swedish_no_send():
    modes = compute_cyberlurch_modes_for_date(date(2026, 6, 20))
    assert "daily" in modes
