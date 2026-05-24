from __future__ import annotations

from datetime import date, timedelta


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _weekday_in_range(year: int, month: int, day_start: int, day_end: int, weekday: int) -> date:
    for d in range(day_start, day_end + 1):
        cand = date(year, month, d)
        if cand.weekday() == weekday:
            return cand
    raise ValueError("No matching weekday in range")


def swedish_public_holidays(year: int) -> dict[date, str]:
    easter = _easter_sunday(year)
    return {
        date(year, 1, 1): "Nyårsdagen",
        date(year, 1, 6): "Trettondedag jul",
        easter - timedelta(days=2): "Långfredagen",
        easter: "Påskdagen",
        easter + timedelta(days=1): "Annandag påsk",
        date(year, 5, 1): "Första maj",
        easter + timedelta(days=39): "Kristi himmelsfärds dag",
        easter + timedelta(days=49): "Pingstdagen",
        date(year, 6, 6): "Sveriges nationaldag",
        _weekday_in_range(year, 6, 20, 26, 5): "Midsommardagen",
        _weekday_in_range(year, 10, 31, 31, 5) if date(year, 10, 31).weekday() == 5 else _weekday_in_range(year, 11, 1, 6, 5): "Alla helgons dag",
        date(year, 12, 25): "Juldagen",
        date(year, 12, 26): "Annandag jul",
    }


def swedish_no_send_days(year: int) -> dict[date, str]:
    easter = _easter_sunday(year)
    no_send = dict(swedish_public_holidays(year))
    no_send[_weekday_in_range(year, 6, 19, 25, 4)] = "Midsommarafton"
    no_send[date(year, 12, 24)] = "Julafton"
    no_send[date(year, 12, 31)] = "Nyårsafton"
    no_send[easter - timedelta(days=1)] = "Påskafton"
    return no_send


def is_swedish_weekend(d: date) -> bool:
    return d.weekday() >= 5


def is_swedish_no_send_day(d: date) -> bool:
    return is_swedish_weekend(d) or d in swedish_no_send_days(d.year)


def previous_swedish_business_day(d: date) -> date:
    cur = d - timedelta(days=1)
    while is_swedish_no_send_day(cur):
        cur -= timedelta(days=1)
    return cur


def next_swedish_business_day(d: date) -> date:
    cur = d + timedelta(days=1)
    while is_swedish_no_send_day(cur):
        cur += timedelta(days=1)
    return cur


def cybermed_holiday_greeting_for_date(d: date) -> str | None:
    names = swedish_no_send_days(d.year)
    name = names.get(d)
    if name == "Midsommarafton":
        return "Glad midsommar!"
    if name in {"Långfredagen", "Påskdagen", "Annandag påsk", "Påskafton"}:
        return "Glad påsk!"
    if name in {"Julafton", "Juldagen", "Annandag jul"}:
        return "God jul!"
    if name in {"Nyårsafton", "Nyårsdagen"}:
        return "Gott nytt år!"
    if name == "Sveriges nationaldag":
        return "Trevlig nationaldag!"
    return None
