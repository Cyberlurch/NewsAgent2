from datetime import date

from src.newsagent2.swedish_calendar import (
    cybermed_holiday_greeting_for_date,
    is_swedish_no_send_day,
    swedish_no_send_days,
    swedish_public_holidays,
)


def test_public_holidays_known_dates():
    h = swedish_public_holidays(2026)
    assert h[date(2026, 1, 1)] == "Nyårsdagen"
    assert h[date(2026, 6, 20)] == "Midsommardagen"


def test_easter_derived_dates_2026():
    h = swedish_public_holidays(2026)
    assert h[date(2026, 4, 3)] == "Långfredagen"
    assert h[date(2026, 4, 5)] == "Påskdagen"


def test_eves_and_weekend_no_send():
    ns = swedish_no_send_days(2026)
    assert ns[date(2026, 6, 19)] == "Midsommarafton"
    assert ns[date(2026, 12, 24)] == "Julafton"
    assert ns[date(2026, 12, 31)] == "Nyårsafton"
    assert is_swedish_no_send_day(date(2026, 6, 20))


def test_greetings():
    assert cybermed_holiday_greeting_for_date(date(2026, 6, 19)) == "Glad midsommar!"
    assert cybermed_holiday_greeting_for_date(date(2026, 12, 24)) == "God jul!"
